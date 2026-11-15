//! Serialization helpers.
//!
//! `serde_json` is the value codec throughout (matching `storage-sqlite`,
//! whose records are self-describing JSON documents — and which the
//! hand-written `Deserialize` impls in `OpenMLS` require). Records that do not
//! implement serde use explicit tuple codecs built from `put_field`/`take_*`.

use cgka_traits::storage::{StorageError, StorageResult};

/// `serde_json` encode → [`StorageError::Serialization`] on failure. The
/// error message is fixed: serde errors can echo payload fragments, and row
/// contents never belong in diagnostics.
pub(crate) fn ser<T: serde::Serialize>(value: &T) -> StorageResult<Vec<u8>> {
    serde_json::to_vec(value).map_err(|_| StorageError::Serialization("kv serde encode".into()))
}

/// `serde_json` decode → [`StorageError::Serialization`] on failure; the
/// message is fixed for the same reason as [`ser`].
pub(crate) fn de<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> StorageResult<T> {
    serde_json::from_slice(bytes).map_err(|_| StorageError::Serialization("kv serde decode".into()))
}

/// `serde_json` encode into a zeroizing buffer, for rows that may hold key
/// material.
pub(crate) fn ser_sensitive<T: serde::Serialize + ?Sized>(
    value: &T,
) -> StorageResult<zeroize::Zeroizing<Vec<u8>>> {
    serde_json::to_vec(value)
        .map(zeroize::Zeroizing::new)
        .map_err(|_| StorageError::Serialization("kv serde encode".into()))
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
