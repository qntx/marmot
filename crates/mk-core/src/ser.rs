//! Serialization helpers.
//!
//! `serde_json` is the one value codec (matching `storage-sqlite`, whose
//! records are self-describing JSON documents — and which the hand-written
//! `Deserialize` impls in `OpenMLS` require). `Vec<u8>` fields on upstream
//! types serialize as decimal arrays; mk-core-owned row structs
//! ([`MessageRow`], [`GroupRow`], and the snapshot blob structs' byte
//! fields) carry them as base64 strings instead — the compact
//! representation the one codec allows without touching upstream derives.
//! Records that do not implement serde use explicit tuple codecs built
//! from `put_field`/`take_*`.

use cgka_traits::capabilities::GroupCapabilities;
use cgka_traits::group::{
    DeviceRosterDeparture, DeviceRosterDepartureCause, DisbandTombstone, Group, Member,
    ProtocolProfile,
};
use cgka_traits::message::{DeferredPeelLifecycle, MessageRecord, MessageState};
use cgka_traits::storage::{StorageError, StorageResult};
use cgka_traits::transport::Timestamp;
use cgka_traits::types::{EpochId, GroupId, MemberId, MessageId};
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

/// The stored form of one `cgka_groups` row: mk-core-owned for the same
/// reason as [`MessageRow`] — upstream `Group`'s serde derive writes every
/// byte field as a decimal array, which is most of the row at roster
/// sizes. Mirror every upstream field so a pin bump that adds one fails
/// to compile here, not silently drops it.
#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct GroupRow {
    #[serde(with = "blob")]
    id: Vec<u8>,
    name: String,
    description: String,
    epoch: u64,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    members: Vec<MemberRow>,
    required_capabilities: GroupCapabilities,
    #[serde(default)]
    protocol_profile: ProtocolProfile,
    #[serde(default)]
    removed: bool,
    #[serde(default)]
    unrecoverable: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    disbanded: Option<TombstoneRow>,
    #[serde(default)]
    join_epoch: u64,
    #[serde(default)]
    local_copy_install_epoch: u64,
    #[serde(default, skip_serializing_if = "Vec::is_empty", with = "fixed_seq")]
    self_removed_sibling_slots: Vec<[u8; 32]>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    local_copy_welcome_created_at: Option<u64>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    device_roster_departures: Vec<DepartureRow>,
}

impl From<&Group> for GroupRow {
    fn from(group: &Group) -> Self {
        let Group {
            id,
            name,
            description,
            epoch,
            members,
            required_capabilities,
            protocol_profile,
            removed,
            unrecoverable,
            disbanded,
            join_epoch,
            local_copy_install_epoch,
            self_removed_sibling_slots,
            local_copy_welcome_created_at,
            device_roster_departures,
        } = group;
        Self {
            id: id.as_slice().to_vec(),
            name: name.clone(),
            description: description.clone(),
            epoch: epoch.0,
            members: members.iter().map(MemberRow::from).collect(),
            required_capabilities: required_capabilities.clone(),
            protocol_profile: *protocol_profile,
            removed: *removed,
            unrecoverable: *unrecoverable,
            disbanded: disbanded.as_ref().map(TombstoneRow::from),
            join_epoch: join_epoch.0,
            local_copy_install_epoch: local_copy_install_epoch.0,
            self_removed_sibling_slots: self_removed_sibling_slots.iter().copied().collect(),
            local_copy_welcome_created_at: local_copy_welcome_created_at.map(|stamp| stamp.0),
            device_roster_departures: device_roster_departures
                .iter()
                .map(DepartureRow::from)
                .collect(),
        }
    }
}

impl From<GroupRow> for Group {
    fn from(row: GroupRow) -> Self {
        let GroupRow {
            id,
            name,
            description,
            epoch,
            members,
            required_capabilities,
            protocol_profile,
            removed,
            unrecoverable,
            disbanded,
            join_epoch,
            local_copy_install_epoch,
            self_removed_sibling_slots,
            local_copy_welcome_created_at,
            device_roster_departures,
        } = row;
        Self {
            id: GroupId::new(id),
            name,
            description,
            epoch: EpochId(epoch),
            members: members.into_iter().map(Member::from).collect(),
            required_capabilities,
            protocol_profile,
            removed,
            unrecoverable,
            disbanded: disbanded.map(DisbandTombstone::from),
            join_epoch: EpochId(join_epoch),
            local_copy_install_epoch: EpochId(local_copy_install_epoch),
            self_removed_sibling_slots: self_removed_sibling_slots.into_iter().collect(),
            local_copy_welcome_created_at: local_copy_welcome_created_at.map(Timestamp),
            device_roster_departures: device_roster_departures
                .into_iter()
                .map(DeviceRosterDeparture::from)
                .collect(),
        }
    }
}

/// `Group.members` / `DisbandTombstone.former_members` element.
#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct MemberRow {
    #[serde(with = "blob")]
    id: Vec<u8>,
    #[serde(with = "blob")]
    credential: Vec<u8>,
}

impl From<&Member> for MemberRow {
    fn from(member: &Member) -> Self {
        let Member { id, credential } = member;
        Self {
            id: id.as_slice().to_vec(),
            credential: credential.clone(),
        }
    }
}

impl From<MemberRow> for Member {
    fn from(row: MemberRow) -> Self {
        let MemberRow { id, credential } = row;
        Self {
            id: MemberId::new(id),
            credential,
        }
    }
}

/// `Group.disbanded` element.
#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct TombstoneRow {
    epoch: u64,
    #[serde(with = "blob")]
    actor: Vec<u8>,
    #[serde(default, skip_serializing_if = "Option::is_none", with = "opt_blob")]
    origin_commit_id: Option<Vec<u8>>,
    #[serde(with = "fixed")]
    commit_digest: [u8; 32],
    local_was_committer_leaf: bool,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    former_members: Vec<MemberRow>,
    #[serde(default)]
    announced: bool,
}

impl From<&DisbandTombstone> for TombstoneRow {
    fn from(tombstone: &DisbandTombstone) -> Self {
        let DisbandTombstone {
            epoch,
            actor,
            origin_commit_id,
            commit_digest,
            local_was_committer_leaf,
            former_members,
            announced,
        } = tombstone;
        Self {
            epoch: epoch.0,
            actor: actor.as_slice().to_vec(),
            origin_commit_id: origin_commit_id.as_ref().map(|id| id.as_slice().to_vec()),
            commit_digest: *commit_digest,
            local_was_committer_leaf: *local_was_committer_leaf,
            former_members: former_members.iter().map(MemberRow::from).collect(),
            announced: *announced,
        }
    }
}

impl From<TombstoneRow> for DisbandTombstone {
    fn from(row: TombstoneRow) -> Self {
        let TombstoneRow {
            epoch,
            actor,
            origin_commit_id,
            commit_digest,
            local_was_committer_leaf,
            former_members,
            announced,
        } = row;
        Self {
            epoch: EpochId(epoch),
            actor: MemberId::new(actor),
            origin_commit_id: origin_commit_id.map(MessageId::new),
            commit_digest,
            local_was_committer_leaf,
            former_members: former_members.into_iter().map(Member::from).collect(),
            announced,
        }
    }
}

/// `Group.device_roster_departures` element.
#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct DepartureRow {
    label: String,
    #[serde(with = "fixed")]
    slot: [u8; 32],
    #[serde(default, skip_serializing_if = "Option::is_none", with = "opt_blob")]
    tag_signature_key: Option<Vec<u8>>,
    linked_at: u64,
    #[serde(default, skip_serializing_if = "Option::is_none", with = "opt_fixed")]
    entry_rename_id: Option<[u8; 16]>,
    source_epoch: u64,
    #[serde(with = "blob")]
    commit_id: Vec<u8>,
    cause: DeviceRosterDepartureCause,
}

impl From<&DeviceRosterDeparture> for DepartureRow {
    fn from(departure: &DeviceRosterDeparture) -> Self {
        let DeviceRosterDeparture {
            label,
            slot,
            tag_signature_key,
            linked_at,
            entry_rename_id,
            source_epoch,
            commit_id,
            cause,
        } = departure;
        Self {
            label: label.clone(),
            slot: *slot,
            tag_signature_key: tag_signature_key.clone(),
            linked_at: *linked_at,
            entry_rename_id: *entry_rename_id,
            source_epoch: source_epoch.0,
            commit_id: commit_id.as_slice().to_vec(),
            cause: *cause,
        }
    }
}

impl From<DepartureRow> for DeviceRosterDeparture {
    fn from(row: DepartureRow) -> Self {
        let DepartureRow {
            label,
            slot,
            tag_signature_key,
            linked_at,
            entry_rename_id,
            source_epoch,
            commit_id,
            cause,
        } = row;
        Self {
            label,
            slot,
            tag_signature_key,
            linked_at,
            entry_rename_id,
            source_epoch: EpochId(source_epoch),
            commit_id: MessageId::new(commit_id),
            cause,
        }
    }
}

/// Group-row encode: [`GroupRow`] through `serde_json`, with byte fields as
/// base64 strings.
pub(crate) fn ser_group(group: &Group) -> StorageResult<Vec<u8>> {
    ser(&GroupRow::from(group))
}

/// Group-row decode: the [`GroupRow`] mirror of [`ser_group`]. A malformed
/// base64 field — including a fixed-size field of the wrong length —
/// surfaces as the same fixed [`StorageError::Serialization`] message as
/// any other decode failure.
pub(crate) fn de_group(bytes: &[u8]) -> StorageResult<Group> {
    de::<GroupRow>(bytes).map(Group::from)
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

/// `[u8; N]` field: a base64 string of exactly `N` bytes — any other
/// decoded length is a decode error.
pub(crate) mod fixed {
    use super::blob::Ref;
    use serde::{Deserializer, Serialize, Serializer};

    pub(crate) fn serialize<S: Serializer, const N: usize>(
        bytes: &[u8; N],
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        Ref(bytes).serialize(serializer)
    }

    pub(crate) fn deserialize<'de, D: Deserializer<'de>, const N: usize>(
        deserializer: D,
    ) -> Result<[u8; N], D::Error> {
        super::blob::deserialize(deserializer)?
            .try_into()
            .map_err(|_| serde::de::Error::custom("fixed blob length mismatch"))
    }
}

/// `Option<[u8; N]>` field; same representation and length rule as
/// [`fixed`].
#[allow(clippy::ref_option, reason = "serde `with` passes `&self.field`")]
pub(crate) mod opt_fixed {
    use super::blob::{Owned, Ref};
    use serde::{Deserialize, Deserializer, Serializer};

    pub(crate) fn serialize<S: Serializer, const N: usize>(
        value: &Option<[u8; N]>,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        match value {
            Some(bytes) => serializer.serialize_some(&Ref(bytes)),
            None => serializer.serialize_none(),
        }
    }

    pub(crate) fn deserialize<'de, D: Deserializer<'de>, const N: usize>(
        deserializer: D,
    ) -> Result<Option<[u8; N]>, D::Error> {
        Option::<Owned>::deserialize(deserializer)?
            .map(|owned| owned.0.try_into())
            .transpose()
            .map_err(|_| serde::de::Error::custom("fixed blob length mismatch"))
    }
}

/// `Vec<[u8; N]>` field; elements use the [`fixed`] form and length rule.
pub(crate) mod fixed_seq {
    use super::blob::{Owned, Ref};
    use serde::{Deserialize, Deserializer, Serializer};

    pub(crate) fn serialize<S: Serializer, const N: usize>(
        values: &[[u8; N]],
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        serializer.collect_seq(values.iter().map(|bytes| Ref(bytes)))
    }

    pub(crate) fn deserialize<'de, D: Deserializer<'de>, const N: usize>(
        deserializer: D,
    ) -> Result<Vec<[u8; N]>, D::Error> {
        Vec::<Owned>::deserialize(deserializer)?
            .into_iter()
            .map(|owned| owned.0.try_into())
            .collect::<Result<Vec<[u8; N]>, _>>()
            .map_err(|_| serde::de::Error::custom("fixed blob length mismatch"))
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
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "test code"
)]
mod tests {
    use super::*;
    use cgka_traits::app_components::AppComponentSet;
    use proptest::prelude::*;
    use std::collections::BTreeSet;

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

    fn any_bytes() -> impl Strategy<Value = Vec<u8>> {
        prop::collection::vec(any::<u8>(), 0..64)
    }

    fn member_strategy() -> impl Strategy<Value = Member> {
        (any_bytes(), any_bytes()).prop_map(|(id, credential)| Member {
            id: MemberId::new(id),
            credential,
        })
    }

    fn capabilities_strategy() -> impl Strategy<Value = GroupCapabilities> {
        (
            prop::collection::btree_set(any::<u16>(), 0..8),
            prop::collection::btree_set(any::<u16>(), 0..8),
            prop::collection::btree_set(any::<u16>(), 0..8),
        )
            .prop_map(|(proposals, extensions, ids)| GroupCapabilities {
                proposals,
                extensions,
                app_components: AppComponentSet { ids },
            })
    }

    fn tombstone_strategy() -> impl Strategy<Value = DisbandTombstone> {
        (
            any::<u64>(),
            any_bytes(),
            prop::option::of(any_bytes()),
            any::<[u8; 32]>(),
            any::<bool>(),
            prop::collection::vec(member_strategy(), 0..6),
            any::<bool>(),
        )
            .prop_map(
                |(
                    epoch,
                    actor,
                    origin_commit_id,
                    commit_digest,
                    local_was_committer_leaf,
                    former_members,
                    announced,
                )| {
                    DisbandTombstone {
                        epoch: EpochId(epoch),
                        actor: MemberId::new(actor),
                        origin_commit_id: origin_commit_id.map(MessageId::new),
                        commit_digest,
                        local_was_committer_leaf,
                        former_members,
                        announced,
                    }
                },
            )
    }

    fn departure_strategy() -> impl Strategy<Value = DeviceRosterDeparture> {
        (
            ".{0,64}",
            any::<[u8; 32]>(),
            prop::option::of(any_bytes()),
            any::<u64>(),
            prop::option::of(any::<[u8; 16]>()),
            any::<u64>(),
            any_bytes(),
            prop_oneof![
                Just(DeviceRosterDepartureCause::Moved),
                Just(DeviceRosterDepartureCause::Ordinary)
            ],
        )
            .prop_map(
                |(
                    label,
                    slot,
                    tag_signature_key,
                    linked_at,
                    entry_rename_id,
                    source_epoch,
                    commit_id,
                    cause,
                )| {
                    DeviceRosterDeparture {
                        label,
                        slot,
                        tag_signature_key,
                        linked_at,
                        entry_rename_id,
                        source_epoch: EpochId(source_epoch),
                        commit_id: MessageId::new(commit_id),
                        cause,
                    }
                },
            )
    }

    fn group_strategy() -> impl Strategy<Value = Group> {
        (
            any_bytes(),
            ".{0,64}",
            ".{0,64}",
            any::<u64>(),
            prop::collection::vec(member_strategy(), 1..8),
            capabilities_strategy(),
            prop_oneof![
                Just(ProtocolProfile::Legacy),
                Just(ProtocolProfile::Current)
            ],
            any::<bool>(),
            any::<bool>(),
            any::<u64>(),
            any::<u64>(),
            (
                prop::option::of(tombstone_strategy()),
                prop::collection::btree_set(any::<[u8; 32]>(), 0..8),
                prop::option::of(any::<u64>()),
                prop::collection::vec(departure_strategy(), 0..6),
            ),
        )
            .prop_map(
                |(
                    id,
                    name,
                    description,
                    epoch,
                    members,
                    required_capabilities,
                    protocol_profile,
                    removed,
                    unrecoverable,
                    join_epoch,
                    local_copy_install_epoch,
                    (
                        disbanded,
                        self_removed_sibling_slots,
                        local_copy_welcome_created_at,
                        device_roster_departures,
                    ),
                )| {
                    Group {
                        id: GroupId::new(id),
                        name,
                        description,
                        epoch: EpochId(epoch),
                        members,
                        required_capabilities,
                        protocol_profile,
                        removed,
                        unrecoverable,
                        disbanded,
                        join_epoch: EpochId(join_epoch),
                        local_copy_install_epoch: EpochId(local_copy_install_epoch),
                        self_removed_sibling_slots,
                        local_copy_welcome_created_at: local_copy_welcome_created_at.map(Timestamp),
                        device_roster_departures,
                    }
                },
            )
    }

    proptest! {
        /// Every field of every nested struct round-trips: non-empty member
        /// lists, disbanded Some/None, sibling slots, departures with
        /// Some/None option fields.
        #[test]
        fn group_row_round_trip(group in group_strategy()) {
            let bytes = ser_group(&group).expect("encode");
            prop_assert_eq!(de_group(&bytes).expect("decode"), group);
        }
    }

    /// A `Group` with every field non-default — the literal lists all of
    /// them (no `..`), so an upstream field addition fails to compile here.
    fn exhaustive_group() -> Group {
        Group {
            id: GroupId::new(vec![1; 32]),
            name: "name".to_owned(),
            description: "description".to_owned(),
            epoch: EpochId(7),
            members: vec![
                Member {
                    id: MemberId::new(vec![2; 32]),
                    credential: vec![3; 48],
                },
                Member {
                    id: MemberId::new(vec![20; 32]),
                    credential: vec![21; 48],
                },
            ],
            required_capabilities: GroupCapabilities {
                proposals: BTreeSet::from([1, 2]),
                extensions: BTreeSet::from([3]),
                app_components: AppComponentSet::new([0x8001]),
            },
            protocol_profile: ProtocolProfile::Current,
            removed: true,
            unrecoverable: true,
            disbanded: Some(DisbandTombstone {
                epoch: EpochId(9),
                actor: MemberId::new(vec![4; 32]),
                origin_commit_id: Some(MessageId::new(vec![5; 32])),
                commit_digest: [6; 32],
                local_was_committer_leaf: true,
                former_members: vec![Member {
                    id: MemberId::new(vec![7; 32]),
                    credential: vec![8; 48],
                }],
                announced: true,
            }),
            join_epoch: EpochId(4),
            local_copy_install_epoch: EpochId(5),
            self_removed_sibling_slots: BTreeSet::from([[9; 32], [10; 32]]),
            local_copy_welcome_created_at: Some(Timestamp(1_700_000_000)),
            device_roster_departures: vec![
                DeviceRosterDeparture {
                    label: "laptop".to_owned(),
                    slot: [11; 32],
                    tag_signature_key: Some(vec![12; 32]),
                    linked_at: 42,
                    entry_rename_id: Some([13; 16]),
                    source_epoch: EpochId(6),
                    commit_id: MessageId::new(vec![14; 32]),
                    cause: DeviceRosterDepartureCause::Moved,
                },
                DeviceRosterDeparture {
                    label: "phone".to_owned(),
                    slot: [15; 32],
                    tag_signature_key: None,
                    linked_at: 43,
                    entry_rename_id: None,
                    source_epoch: EpochId(7),
                    commit_id: MessageId::new(vec![16; 32]),
                    cause: DeviceRosterDepartureCause::Ordinary,
                },
            ],
        }
    }

    #[test]
    fn group_row_every_field_round_trips() {
        let group = exhaustive_group();
        let bytes = ser_group(&group).expect("encode");
        assert_eq!(de_group(&bytes).expect("decode"), group);
    }

    /// Byte fields land as base64 strings, not decimal arrays.
    #[test]
    fn group_row_byte_fields_are_base64_strings() {
        let row = serde_json::to_value(GroupRow::from(&exhaustive_group())).expect("row json");
        for pointer in [
            "/id",
            "/members/0/id",
            "/members/0/credential",
            "/disbanded/actor",
            "/disbanded/origin_commit_id",
            "/disbanded/commit_digest",
            "/disbanded/former_members/0/credential",
            "/self_removed_sibling_slots/0",
            "/device_roster_departures/0/slot",
            "/device_roster_departures/0/tag_signature_key",
            "/device_roster_departures/0/entry_rename_id",
            "/device_roster_departures/0/commit_id",
        ] {
            assert!(
                row.pointer(pointer)
                    .is_some_and(serde_json::Value::is_string),
                "{pointer} must be a base64 string"
            );
        }
    }

    /// A fixed-size field decoded at the wrong length is the fixed
    /// serialization error — like any other malformed row.
    #[test]
    fn malformed_fixed_length_field_is_serialization_error() {
        use base64::Engine as _;
        let field = "/disbanded/commit_digest";
        for len in [31usize, 33] {
            let mut row =
                serde_json::to_value(GroupRow::from(&exhaustive_group())).expect("row json");
            *row.pointer_mut(field).expect("fixed field") = serde_json::Value::String(
                base64::engine::general_purpose::STANDARD.encode(vec![0u8; len]),
            );
            let bytes = serde_json::to_vec(&row).expect("json");
            assert!(
                matches!(de_group(&bytes), Err(StorageError::Serialization(_))),
                "a {len}-byte digest must fail to decode"
            );
        }
    }
}
