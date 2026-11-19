//! Serialization helpers.
//!
//! `serde_json` is the one value codec (matching `storage-sqlite`, whose
//! records are self-describing JSON documents — and which the hand-written
//! `Deserialize` impls in `OpenMLS` require). `Vec<u8>` fields on upstream
//! types serialize as decimal arrays; mk-core-owned row structs
//! ([`MessageRow`], and the snapshot blob structs' byte fields) carry them
//! as base64 strings instead — the compact representation the one codec
//! allows without touching upstream derives. Records that do not implement
//! serde use explicit tuple codecs built from `put_field`/`take_*`.

use cgka_traits::message::{DeferredPeelLifecycle, MessageRecord, MessageState};
use cgka_traits::storage::{StorageError, StorageResult};
use cgka_traits::types::{EpochId, GroupId, MessageId};
use serde::{Deserialize, Serialize};

/// `serde_json` encode → [`StorageError::Serialization`] on failure. The
/// error message is fixed: serde errors can echo payload fragments, and row
/// contents never belong in diagnostics.
pub(crate) fn ser<T: Serialize + ?Sized>(value: &T) -> StorageResult<Vec<u8>> {
    serde_json::to_vec(value).map_err(|_| StorageError::Serialization("kv serde encode".into()))
}

/// `serde_json` decode → [`StorageError::Serialization`] on failure; the
/// message is fixed for the same reason as [`ser`].
pub(crate) fn de<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> StorageResult<T> {
    serde_json::from_slice(bytes).map_err(|_| StorageError::Serialization("kv serde decode".into()))
}

/// `serde_json` encode into a zeroizing buffer, for rows that may hold key
/// material.
pub(crate) fn ser_sensitive<T: Serialize + ?Sized>(
    value: &T,
) -> StorageResult<zeroize::Zeroizing<Vec<u8>>> {
    serde_json::to_vec(value)
        .map(zeroize::Zeroizing::new)
        .map_err(|_| StorageError::Serialization("kv serde encode".into()))
}

/// The stored form of one `cgka_messages` row: mk-core-owned so its byte
/// fields carry the compact base64 representation upstream's derive cannot
/// take.
#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct MessageRow {
    #[serde(with = "blob")]
    id: Vec<u8>,
    #[serde(with = "blob")]
    group_id: Vec<u8>,
    epoch: u64,
    state: MessageState,
    #[serde(with = "blob")]
    payload: Vec<u8>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    deferred_peel: Option<DeferredPeelLifecycle>,
}

impl From<&MessageRecord> for MessageRow {
    fn from(record: &MessageRecord) -> Self {
        Self {
            id: record.id.as_slice().to_vec(),
            group_id: record.group_id.as_slice().to_vec(),
            epoch: record.epoch.0,
            state: record.state,
            payload: record.payload.clone(),
            deferred_peel: record.deferred_peel.clone(),
        }
    }
}

impl From<MessageRow> for MessageRecord {
    fn from(row: MessageRow) -> Self {
        Self {
            id: MessageId::new(row.id),
            group_id: GroupId::new(row.group_id),
            epoch: EpochId(row.epoch),
            state: row.state,
            payload: row.payload,
            deferred_peel: row.deferred_peel,
        }
    }
}

/// Message-row encode: [`MessageRow`] through `serde_json`, with byte
/// fields as base64 strings.
pub(crate) fn ser_message(record: &MessageRecord) -> StorageResult<Vec<u8>> {
    ser(&MessageRow::from(record))
}

/// Message-row decode: the [`MessageRow`] mirror of [`ser_message`]. A
/// malformed base64 field surfaces as the same fixed
/// [`StorageError::Serialization`] message as any other decode failure.
pub(crate) fn de_message(bytes: &[u8]) -> StorageResult<MessageRecord> {
    de::<MessageRow>(bytes).map(MessageRecord::from)
}

/// `Vec<u8>` field: a base64 string (the compact byte representation
/// `serde_json` allows without a second codec).
pub(crate) mod blob {
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    /// Borrowed byte field encoding as base64 — the shared `with`
    /// element for every byte-field module.
    pub(super) struct Ref<'a>(pub(super) &'a [u8]);

    impl Serialize for Ref<'_> {
        fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
            use base64::Engine as _;
            serializer.serialize_str(&base64::engine::general_purpose::STANDARD.encode(self.0))
        }
    }

    /// Owned byte field decoding from base64.
    pub(super) struct Owned(pub(super) Vec<u8>);

    impl<'de> Deserialize<'de> for Owned {
        fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
            deserialize(deserializer).map(Self)
        }
    }

    pub(crate) fn serialize<S: Serializer>(bytes: &[u8], serializer: S) -> Result<S::Ok, S::Error> {
        Ref(bytes).serialize(serializer)
    }

    pub(crate) fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Vec<u8>, D::Error> {
        use base64::Engine as _;
        let text = String::deserialize(deserializer)?;
        base64::engine::general_purpose::STANDARD
            .decode(text)
            .map_err(serde::de::Error::custom)
    }
}

/// `Option<Vec<u8>>` field; same representation as [`blob`].
#[allow(clippy::ref_option, reason = "serde `with` passes `&self.field`")]
pub(crate) mod opt_blob {
    use super::blob::{Owned, Ref};
    use serde::{Deserialize, Deserializer, Serializer};

    pub(crate) fn serialize<S: Serializer>(
        value: &Option<Vec<u8>>,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        match value {
            Some(bytes) => serializer.serialize_some(&Ref(bytes)),
            None => serializer.serialize_none(),
        }
    }

    pub(crate) fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Option<Vec<u8>>, D::Error> {
        Ok(Option::<Owned>::deserialize(deserializer)?.map(|owned| owned.0))
    }
}

/// `Vec<(Vec<u8>, Vec<u8>)>` field; elements use the [`blob`] form.
#[allow(clippy::type_complexity, reason = "serde `with` field type")]
pub(crate) mod blob_pairs {
    use super::blob::{Owned, Ref};
    use serde::{Deserialize, Deserializer, Serializer};

    pub(crate) fn serialize<S: Serializer>(
        pairs: &[(Vec<u8>, Vec<u8>)],
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        serializer.collect_seq(pairs.iter().map(|(a, b)| (Ref(a), Ref(b))))
    }

    pub(crate) fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, D::Error> {
        Ok(Vec::<(Owned, Owned)>::deserialize(deserializer)?
            .into_iter()
            .map(|(a, b)| (a.0, b.0))
            .collect())
    }
}

/// `Option<Vec<(u64, Vec<u8>)>>` field; elements use the [`blob`] form.
#[allow(
    clippy::ref_option,
    clippy::type_complexity,
    reason = "serde `with` field type"
)]
pub(crate) mod opt_u64_pairs {
    use super::blob::{Owned, Ref};
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub(crate) fn serialize<S: Serializer>(
        pairs: &Option<Vec<(u64, Vec<u8>)>>,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        match pairs {
            Some(pairs) => serializer.serialize_some(&SerializeSeq(pairs)),
            None => serializer.serialize_none(),
        }
    }

    struct SerializeSeq<'a>(&'a [(u64, Vec<u8>)]);

    impl Serialize for SerializeSeq<'_> {
        fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
            serializer.collect_seq(self.0.iter().map(|(n, bytes)| (*n, Ref(bytes))))
        }
    }

    pub(crate) fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Option<Vec<(u64, Vec<u8>)>>, D::Error> {
        Ok(Option::<Vec<(u64, Owned)>>::deserialize(deserializer)?
            .map(|pairs| pairs.into_iter().map(|(n, bytes)| (n, bytes.0)).collect()))
    }
}

/// Append a length-prefixed field (`u32 BE` + bytes) to `out`.
pub(crate) fn put_field(out: &mut Vec<u8>, field: &[u8]) {
    out.extend_from_slice(&u32::try_from(field.len()).unwrap_or(u32::MAX).to_be_bytes());
    out.extend_from_slice(field);
}

/// Cursor over a tuple-decoded buffer.
pub(crate) struct Tuple<'a> {
    rest: &'a [u8],
}

impl<'a> Tuple<'a> {
    /// Start decoding `bytes`.
    pub(crate) const fn new(bytes: &'a [u8]) -> Self {
        Self { rest: bytes }
    }

    /// Read one length-prefixed field.
    pub(crate) fn field(&mut self) -> StorageResult<&'a [u8]> {
        if self.rest.len() < 4 {
            return Err(corrupt());
        }
        let (len_bytes, tail) = self.rest.split_at(4);
        let len = u32::from_be_bytes(len_bytes.try_into().map_err(|_| corrupt())?) as usize;
        if tail.len() < len {
            return Err(corrupt());
        }
        let (field, next) = tail.split_at(len);
        self.rest = next;
        Ok(field)
    }

    /// Read a `u64` field.
    pub(crate) fn u64(&mut self) -> StorageResult<u64> {
        let bytes: [u8; 8] = self.field()?.try_into().map_err(|_| corrupt())?;
        Ok(u64::from_be_bytes(bytes))
    }

    /// Read a `u16` field.
    pub(crate) fn u16(&mut self) -> StorageResult<u16> {
        let bytes: [u8; 2] = self.field()?.try_into().map_err(|_| corrupt())?;
        Ok(u16::from_be_bytes(bytes))
    }

    /// Read a single-byte field.
    pub(crate) fn u8(&mut self) -> StorageResult<u8> {
        let field = self.field()?;
        let (&byte, _) = field.split_first().ok_or_else(corrupt)?;
        if field.len() != 1 {
            return Err(corrupt());
        }
        Ok(byte)
    }

    /// Require all input consumed.
    pub(crate) fn finish(&self) -> StorageResult<()> {
        if self.rest.is_empty() {
            Ok(())
        } else {
            Err(corrupt())
        }
    }
}

fn corrupt() -> StorageError {
    StorageError::Serialization("malformed kv record".into())
}

pub(crate) fn put_u64_field(out: &mut Vec<u8>, value: u64) {
    put_field(out, &value.to_be_bytes());
}

pub(crate) fn put_u16_field(out: &mut Vec<u8>, value: u16) {
    put_field(out, &value.to_be_bytes());
}

pub(crate) fn put_u8_field(out: &mut Vec<u8>, value: u8) {
    put_field(out, &[value]);
}

/// Encode/decode an `Option<Vec<u8>>` as presence byte + field.
pub(crate) fn put_opt_field(out: &mut Vec<u8>, value: Option<&[u8]>) {
    match value {
        Some(bytes) => {
            put_u8_field(out, 1);
            put_field(out, bytes);
        }
        None => put_u8_field(out, 0),
    }
}

pub(crate) fn take_opt_field<'a>(tuple: &mut Tuple<'a>) -> StorageResult<Option<&'a [u8]>> {
    match tuple.u8()? {
        0 => Ok(None),
        1 => tuple.field().map(Some),
        _ => Err(corrupt()),
    }
}

#[cfg(test)]
#[expect(clippy::panic, reason = "test assertion")]
mod tests {
    use super::*;

    #[test]
    fn decode_error_never_echoes_row_contents() {
        // serde_json embeds the offending value in its Display; a distinctive
        // marker in the row must not leak into the storage error's message.
        const MARKER: &str = "zzz-distinctive-payload-marker-zzz";
        let row = format!("\"{MARKER}\"").into_bytes();
        let error = de::<u32>(&row).map_or_else(|e| e, |_| panic!("decode must fail"));
        let message = error.to_string();
        assert!(!message.contains(MARKER), "error leaked payload: {message}");
    }
}
