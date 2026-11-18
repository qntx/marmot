//! The `OpenMLS` storage adapter: `openmls_traits::storage::StorageProvider`
//! over the KV store, mirroring `storage-sqlite`'s `openmls_storage` module.
//!
//! - Row keys are `key(OpenMlsValue, [label, entity_key])` where `entity_key`
//!   is the `serde_json` bytes of the entity's key type.
//! - Group-scoped rows also write an index row under
//!   `key(OpenMlsGroupIndex, [raw_gid, row_key])`; `raw_gid` is derived by
//!   [`index_segment`], the one place group ownership is computed.
//! - Values are `serde_json`, the one value codec for the whole crate; secret
//!   labels encode into zeroizing buffers.

use openmls_traits::storage::{CURRENT_VERSION, Entity, StorageProvider, traits};
use serde::{Serialize, de::DeserializeOwned};

use crate::keys::{self, Table};
use crate::ser::{self, Tuple};
use crate::store::KvStore;
use cgka_traits::storage::{StorageError, StorageResult};

/// A stable `OpenMLS` value label with its sensitivity class (`labels.rs`).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct Label {
    bytes: &'static [u8],
    secret: bool,
}

impl Label {
    const fn public(bytes: &'static [u8]) -> Self {
        Self {
            bytes,
            secret: false,
        }
    }

    const fn secret(bytes: &'static [u8]) -> Self {
        Self {
            bytes,
            secret: true,
        }
    }
}

const KEY_PACKAGE_LABEL: Label = Label::secret(b"KeyPackage");
const PSK_LABEL: Label = Label::secret(b"Psk");
const ENCRYPTION_KEY_PAIR_LABEL: Label = Label::secret(b"EncryptionKeyPair");
const SIGNATURE_KEY_PAIR_LABEL: Label = Label::secret(b"SignatureKeyPair");
const EPOCH_KEY_PAIRS_LABEL: Label = Label::secret(b"EpochKeyPairs");
const TREE_LABEL: Label = Label::public(b"Tree");
const GROUP_CONTEXT_LABEL: Label = Label::public(b"GroupContext");
const APPLICATION_EXPORT_TREE_LABEL: Label = Label::secret(b"ApplicationExportTree");
const INTERIM_TRANSCRIPT_HASH_LABEL: Label = Label::public(b"InterimTranscriptHash");
const CONFIRMATION_TAG_LABEL: Label = Label::public(b"ConfirmationTag");
const JOIN_CONFIG_LABEL: Label = Label::public(b"MlsGroupJoinConfig");
const OWN_LEAF_NODES_LABEL: Label = Label::public(b"OwnLeafNodes");
const GROUP_STATE_LABEL: Label = Label::secret(b"GroupState");
const QUEUED_PROPOSAL_LABEL: Label = Label::public(b"QueuedProposal");
const PROPOSAL_QUEUE_REFS_LABEL: Label = Label::public(b"ProposalQueueRefs");
const OWN_LEAF_NODE_INDEX_LABEL: Label = Label::public(b"OwnLeafNodeIndex");
const EPOCH_SECRETS_LABEL: Label = Label::secret(b"EpochSecrets");
const RESUMPTION_PSK_STORE_LABEL: Label = Label::secret(b"ResumptionPsk");
const MESSAGE_SECRETS_LABEL: Label = Label::secret(b"MessageSecrets");

/// The label bytes `provider.rs` scans under for
/// `stored_key_package_bundles`.
pub(crate) const KEY_PACKAGE_LABEL_BYTES: &[u8] = b"KeyPackage";

/// `epoch_key_pairs_id`: the unambiguous JSON tuple key — `GroupEpoch` and
/// `leaf_index` both serialize to undelimited digits, so concatenation
/// collides (`(3, 45)` vs `(34, 5)`).
fn epoch_key_pairs_id(
    group_id: &impl traits::GroupId<CURRENT_VERSION>,
    epoch: &impl traits::EpochKey<CURRENT_VERSION>,
    leaf_index: u32,
) -> StorageResult<Vec<u8>> {
    ser::ser(&(group_id, epoch, leaf_index))
}

/// `labels.rs:96`: the `mdk.openmls.value-key.v1` storage key —
/// `v1` + u64-BE label length + label + u64-BE key length + key +
/// u16-BE provider version. These bytes are observable:
/// `stored_key_package_bundles` hands them out as
/// `StoredKeyPackageBundle::storage_key`, so the segment must be
/// byte-identical to sqlite's storage key.
const VALUE_STORAGE_KEY_V1: &[u8] = b"mdk.openmls.value-key.v1";

fn build_key(label: &[u8], entity_key: &[u8]) -> Vec<u8> {
    let mut out =
        Vec::with_capacity(VALUE_STORAGE_KEY_V1.len() + 8 + label.len() + 8 + entity_key.len() + 2);
    out.extend_from_slice(VALUE_STORAGE_KEY_V1);
    out.extend_from_slice(&(label.len() as u64).to_be_bytes());
    out.extend_from_slice(label);
    out.extend_from_slice(&(entity_key.len() as u64).to_be_bytes());
    out.extend_from_slice(entity_key);
    out.extend_from_slice(&CURRENT_VERSION.to_be_bytes());
    out
}

/// The label half of a `build_key` storage key, or `None` when malformed.
pub(crate) fn storage_key_label(storage_key: &[u8]) -> Option<&[u8]> {
    let rest = storage_key.strip_prefix(VALUE_STORAGE_KEY_V1)?;
    let (len_bytes, rest) = rest.split_at_checked(8)?;
    let len = usize::try_from(u64::from_be_bytes(len_bytes.try_into().ok()?)).ok()?;
    rest.get(..len)
}

/// The full KV row key for an `OpenMLS` value row: label first so
/// label-prefix scans (bundle listing, group-index deletes) stay prefix
/// scans; the `build_key` storage key as the entity segment.
pub(crate) fn row_key(label: &[u8], entity_key: &[u8]) -> Vec<u8> {
    keys::key(Table::OpenMlsValue, &[label, &build_key(label, entity_key)])
}

/// The one function that maps a stored `group_key` (`serde_json` of the
/// OpenMLS-side group id) to the group-scope index segment: the RAW MLS
/// group-id bytes, recovered by serde round-trip into
/// `openmls::group::GroupId` and `.as_slice()`. The Marmot side scans
/// `key(OpenMlsGroupIndex, [marmot GroupId::as_slice()])` — the same bytes —
/// so both directions land on one segment. A `group_key` that is not an
/// `OpenMLS` `GroupId` is an error, never a fallback segment: a silent second
/// indexing scheme is exactly the divergence class that loses rows on group
/// delete and snapshot restore.
fn index_segment(group_key: &[u8]) -> StorageResult<Vec<u8>> {
    serde_json::from_slice::<openmls::group::GroupId>(group_key)
        .map(|group_id| group_id.as_slice().to_vec())
        .map_err(|_| StorageError::Serialization("kv decode: openmls group key".into()))
}

/// `serde_json` of the OpenMLS-side group id — the `group_key` column value
/// (`SqliteOpenMlsStorage::group_key`).
fn group_key_of<G: traits::GroupId<CURRENT_VERSION>>(group_id: &G) -> StorageResult<Vec<u8>> {
    ser::ser(group_id)
}

/// The row value layout: the stored `group_key` (or none) plus the encoded
/// entity bytes — sqlite's `group_key`/`value` columns flattened.
fn encode_row(group_key: Option<&[u8]>, encoded: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    ser::put_opt_field(&mut out, group_key);
    ser::put_field(&mut out, encoded);
    out
}

pub(crate) fn decode_row(row: &[u8]) -> StorageResult<(Option<Vec<u8>>, Vec<u8>)> {
    let mut tuple = Tuple::new(row);
    let group_key = ser::take_opt_field(&mut tuple)?.map(<[u8]>::to_vec);
    let encoded = tuple.field()?.to_vec();
    tuple.finish()?;
    Ok((group_key, encoded))
}

fn encode<T: Serialize>(label: Label, value: &T) -> Result<Vec<u8>, StorageError> {
    if label.secret {
        // Secret labels serialize inside a zeroizing buffer so the plaintext
        // intermediates do not linger past the copy into the row.
        return ser::ser_sensitive(value).map(|buffer| buffer.to_vec());
    }
    ser::ser(value)
}

fn decode<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, StorageError> {
    ser::de(bytes)
}

/// Remove the first list element matching the entity's encoding.
fn remove_encoded<T: Entity<CURRENT_VERSION>>(
    list: &mut Vec<Vec<u8>>,
    label: Label,
    entity: &T,
) -> StorageResult<()> {
    let encoded = encode(label, entity)?;
    if let Some(pos) = list.iter().position(|element| *element == encoded) {
        list.remove(pos);
    }
    Ok(())
}

/// Decode a group-index row back into `(row_key, label)`.
fn indexed_row(index_key: &[u8]) -> StorageResult<(Vec<u8>, Vec<u8>)> {
    let row = keys::segments(index_key)
        .and_then(|segments| segments.last().copied())
        .ok_or_else(|| StorageError::Backend("kv group index malformed".into()))?
        .to_vec();
    let label = keys::segments(&row)
        .and_then(|segments| segments.first().copied())
        .ok_or_else(|| StorageError::Backend("kv openmls row key malformed".into()))?
        .to_vec();
    Ok((row, label))
}

/// sqlite's `is_undecodable_value`: a decode failure on either era's
/// encoding recovers on the queued-proposals path; every other error class
/// propagates.
pub(crate) const fn is_undecodable(error: &StorageError) -> bool {
    matches!(error, StorageError::Serialization(_))
}

/// The `OpenMLS` storage provider: one `KvStore` behind
/// `openmls_traits`'s `StorageProvider<CURRENT_VERSION>`.
#[derive(Clone, Debug)]
pub struct KvOpenMlsStorage {
    store: KvStore,
}

impl KvOpenMlsStorage {
    /// Delete the indexed row and its index entry when its label is listed.
    fn delete_indexed(&self, labels: &[Label], index_key: &[u8]) -> StorageResult<()> {
        self.store.ensure_writable()?;
        let (row, label) = indexed_row(index_key)?;
        if labels.iter().any(|listed| listed.bytes == label.as_slice()) {
            self.store.delete(&row)?;
            self.store.delete(index_key)?;
        }
        Ok(())
    }

    pub(crate) const fn new(store: KvStore) -> Self {
        Self { store }
    }

    /// Write `encoded` under the entity key and register the group index —
    /// the whole thing one committed unit.
    fn write_row(
        &self,
        label: Label,
        key: &[u8],
        group_key: Option<&[u8]>,
        encoded: &[u8],
    ) -> StorageResult<()> {
        self.store.transact(|| -> StorageResult<()> {
            let row = row_key(label.bytes, key);
            self.store
                .put(row.clone(), encode_row(group_key, encoded))?;
            if let Some(group_key) = group_key {
                let segment = index_segment(group_key)?;
                self.store.put(
                    keys::key(Table::OpenMlsGroupIndex, &[&segment, &row]),
                    Vec::new(),
                )?;
            }
            Ok(())
        })
    }

    /// Delete the value row for one entity key and its group index entry.
    fn delete_row_for_key(&self, label: &[u8], entity_key: &[u8]) -> StorageResult<()> {
        let row = row_key(label, entity_key);
        if let Some(value) = self.store.get(&row)? {
            let (group_key, _) = decode_row(&value)?;
            if let Some(group_key) = group_key {
                let segment = index_segment(&group_key)?;
                self.store
                    .delete(&keys::key(Table::OpenMlsGroupIndex, &[&segment, &row]))?;
            }
        }
        self.store.delete(&row)
    }

    /// `read_json`: the row under the entity key, or `None`.
    fn read_json<T: DeserializeOwned>(&self, label: Label, key: &[u8]) -> StorageResult<Option<T>> {
        let Some(row) = self.store.get(&row_key(label.bytes, key))? else {
            return Ok(None);
        };
        let (_, encoded) = decode_row(&row)?;
        decode(&encoded).map(Some)
    }

    /// `delete_value`: the row under the entity key.
    fn delete_value(&self, label: Label, key: &[u8]) -> StorageResult<()> {
        self.store
            .transact(|| self.delete_row_for_key(label.bytes, key))
    }

    /// The stored list (each element a self-contained encoding); absent →
    /// empty.
    fn read_raw_list(&self, label: Label, key: &[u8]) -> StorageResult<Vec<Vec<u8>>> {
        let Some(row) = self.store.get(&row_key(label.bytes, key))? else {
            return Ok(Vec::new());
        };
        let (_, encoded) = decode_row(&row)?;
        decode::<Vec<Vec<u8>>>(&encoded)
    }

    fn read_list<T: Entity<CURRENT_VERSION>>(
        &self,
        label: Label,
        key: &[u8],
    ) -> StorageResult<Vec<T>> {
        self.read_raw_list(label, key)?
            .iter()
            .map(|element| decode(element))
            .collect()
    }

    /// `append_entity` / `remove_entity`: read the list inside the write
    /// unit, mutate, and rewrite under the entity key.
    fn mutate_list<T: Entity<CURRENT_VERSION>>(
        &self,
        label: Label,
        key: &[u8],
        group_key: Option<&[u8]>,
        remove: bool,
        entity: &T,
    ) -> StorageResult<()> {
        self.store.transact(|| -> StorageResult<()> {
            let mut list = self.read_raw_list(label, key)?;
            if remove {
                remove_encoded(&mut list, label, entity)?;
            } else {
                list.push(encode(label, entity)?);
            }
            let row = row_key(label.bytes, key);
            let encoded = encode(label, &list)?;
            self.store
                .put(row.clone(), encode_row(group_key, &encoded))?;
            if let Some(group_key) = group_key {
                let segment = index_segment(group_key)?;
                self.store.put(
                    keys::key(Table::OpenMlsGroupIndex, &[&segment, &row]),
                    Vec::new(),
                )?;
            }
            Ok(())
        })
    }

    fn write_entity<T: Entity<CURRENT_VERSION>>(
        &self,
        label: Label,
        key: &[u8],
        group_key: Option<&[u8]>,
        entity: &T,
    ) -> StorageResult<()> {
        self.write_serialized(label, key, group_key, entity)
    }

    /// `write_json` — the same write for any serializable value (entity
    /// lists such as the epoch key-pair rows).
    fn write_serialized<T: Serialize>(
        &self,
        label: Label,
        key: &[u8],
        group_key: Option<&[u8]>,
        value: &T,
    ) -> StorageResult<()> {
        self.write_row(label, key, group_key, &encode(label, value)?)
    }

    fn write_group_entity<G: traits::GroupId<CURRENT_VERSION>, T: Entity<CURRENT_VERSION>>(
        &self,
        label: Label,
        group_id: &G,
        entity: &T,
    ) -> StorageResult<()> {
        let group_key = group_key_of(group_id)?;
        self.write_entity(label, &group_key, Some(&group_key), entity)
    }

    fn read_group_entity<G: traits::GroupId<CURRENT_VERSION>, T: Entity<CURRENT_VERSION>>(
        &self,
        label: Label,
        group_id: &G,
    ) -> StorageResult<Option<T>> {
        self.read_json(label, &group_key_of(group_id)?)
    }

    fn delete_group_value<G: traits::GroupId<CURRENT_VERSION>>(
        &self,
        label: Label,
        group_id: &G,
    ) -> StorageResult<()> {
        self.delete_value(label, &group_key_of(group_id)?)
    }

    /// `delete_group_labels`: every group-indexed row whose label is listed,
    /// deleted with its index entry in one unit.
    fn delete_group_labels<G: traits::GroupId<CURRENT_VERSION>>(
        &self,
        group_id: &G,
        labels: &[Label],
    ) -> StorageResult<()> {
        let group_key = group_key_of(group_id)?;
        let segment = index_segment(&group_key)?;
        self.store.transact(|| -> StorageResult<()> {
            self.store.ensure_writable()?;
            for (index_key, _) in self
                .store
                .scan(&keys::key(Table::OpenMlsGroupIndex, &[&segment]))?
            {
                self.delete_indexed(labels, &index_key)?;
            }
            Ok(())
        })
    }

    /// `recover_corrupt_proposal_queue`: clear both queue labels so a
    /// dangling or undecodable row cannot brick group load — commits after
    /// recovery start from current group state and re-enqueue.
    fn recover_corrupt_proposal_queue<G: traits::GroupId<CURRENT_VERSION>>(
        &self,
        group_id: &G,
    ) -> StorageResult<()> {
        self.delete_group_labels(
            group_id,
            &[QUEUED_PROPOSAL_LABEL, PROPOSAL_QUEUE_REFS_LABEL],
        )
    }
}

impl StorageProvider<CURRENT_VERSION> for KvOpenMlsStorage {
    type Error = StorageError;

    fn write_mls_join_config<
        GroupId: traits::GroupId<CURRENT_VERSION>,
        MlsGroupJoinConfig: traits::MlsGroupJoinConfig<CURRENT_VERSION>,
    >(
        &self,
        group_id: &GroupId,
        config: &MlsGroupJoinConfig,
    ) -> Result<(), Self::Error> {
        self.write_group_entity(JOIN_CONFIG_LABEL, group_id, config)
    }

    fn append_own_leaf_node<
        GroupId: traits::GroupId<CURRENT_VERSION>,
        LeafNode: traits::LeafNode<CURRENT_VERSION>,
    >(
        &self,
        group_id: &GroupId,
        leaf_node: &LeafNode,
    ) -> Result<(), Self::Error> {
        let group_key = group_key_of(group_id)?;
        self.mutate_list(
            OWN_LEAF_NODES_LABEL,
            &group_key.clone(),
            Some(&group_key),
            false,
            leaf_node,
        )
    }

    fn queue_proposal<
        GroupId: traits::GroupId<CURRENT_VERSION>,
        ProposalRef: traits::ProposalRef<CURRENT_VERSION>,
        QueuedProposal: traits::QueuedProposal<CURRENT_VERSION>,
    >(
        &self,
        group_id: &GroupId,
        proposal_ref: &ProposalRef,
        proposal: &QueuedProposal,
    ) -> Result<(), Self::Error> {
        // One unit: the entity write rolls back with a failed ref append.
        self.store.transact(|| -> StorageResult<()> {
            let group_key = group_key_of(group_id)?;
            let proposal_key = ser::ser(&(group_id, proposal_ref))?;
            self.write_entity(
                QUEUED_PROPOSAL_LABEL,
                &proposal_key,
                Some(&group_key),
                proposal,
            )?;
            self.mutate_list(
                PROPOSAL_QUEUE_REFS_LABEL,
                &group_key.clone(),
                Some(&group_key),
                false,
                proposal_ref,
            )
        })
    }

    fn write_tree<
        GroupId: traits::GroupId<CURRENT_VERSION>,
        TreeSync: traits::TreeSync<CURRENT_VERSION>,
    >(
        &self,
        group_id: &GroupId,
        tree: &TreeSync,
    ) -> Result<(), Self::Error> {
        self.write_group_entity(TREE_LABEL, group_id, tree)
    }

    fn write_interim_transcript_hash<
        GroupId: traits::GroupId<CURRENT_VERSION>,
        InterimTranscriptHash: traits::InterimTranscriptHash<CURRENT_VERSION>,
    >(
        &self,
        group_id: &GroupId,
        interim_transcript_hash: &InterimTranscriptHash,
    ) -> Result<(), Self::Error> {
        self.write_group_entity(
            INTERIM_TRANSCRIPT_HASH_LABEL,
            group_id,
            interim_transcript_hash,
        )
    }

    fn write_context<
        GroupId: traits::GroupId<CURRENT_VERSION>,
        GroupContext: traits::GroupContext<CURRENT_VERSION>,
    >(
        &self,
        group_id: &GroupId,
        group_context: &GroupContext,
    ) -> Result<(), Self::Error> {
        self.write_group_entity(GROUP_CONTEXT_LABEL, group_id, group_context)
    }

    fn write_confirmation_tag<
        GroupId: traits::GroupId<CURRENT_VERSION>,
        ConfirmationTag: traits::ConfirmationTag<CURRENT_VERSION>,
    >(
        &self,
        group_id: &GroupId,
        confirmation_tag: &ConfirmationTag,
    ) -> Result<(), Self::Error> {
        self.write_group_entity(CONFIRMATION_TAG_LABEL, group_id, confirmation_tag)
    }

    fn write_group_state<
        GroupState: traits::GroupState<CURRENT_VERSION>,
        GroupId: traits::GroupId<CURRENT_VERSION>,
    >(
        &self,
        group_id: &GroupId,
        group_state: &GroupState,
    ) -> Result<(), Self::Error> {
        self.write_group_entity(GROUP_STATE_LABEL, group_id, group_state)
    }

    fn write_message_secrets<
        GroupId: traits::GroupId<CURRENT_VERSION>,
        MessageSecrets: traits::MessageSecrets<CURRENT_VERSION>,
    >(
        &self,
        group_id: &GroupId,
        message_secrets: &MessageSecrets,
    ) -> Result<(), Self::Error> {
        self.write_group_entity(MESSAGE_SECRETS_LABEL, group_id, message_secrets)
    }

    fn write_resumption_psk_store<
        GroupId: traits::GroupId<CURRENT_VERSION>,
        ResumptionPskStore: traits::ResumptionPskStore<CURRENT_VERSION>,
    >(
        &self,
        group_id: &GroupId,
        resumption_psk_store: &ResumptionPskStore,
    ) -> Result<(), Self::Error> {
        self.write_group_entity(RESUMPTION_PSK_STORE_LABEL, group_id, resumption_psk_store)
    }

    fn write_own_leaf_index<
        GroupId: traits::GroupId<CURRENT_VERSION>,
        LeafNodeIndex: traits::LeafNodeIndex<CURRENT_VERSION>,
    >(
        &self,
        group_id: &GroupId,
        own_leaf_index: &LeafNodeIndex,
    ) -> Result<(), Self::Error> {
        self.write_group_entity(OWN_LEAF_NODE_INDEX_LABEL, group_id, own_leaf_index)
    }

    fn write_group_epoch_secrets<
        GroupId: traits::GroupId<CURRENT_VERSION>,
        GroupEpochSecrets: traits::GroupEpochSecrets<CURRENT_VERSION>,
    >(
        &self,
        group_id: &GroupId,
        group_epoch_secrets: &GroupEpochSecrets,
    ) -> Result<(), Self::Error> {
        self.write_group_entity(EPOCH_SECRETS_LABEL, group_id, group_epoch_secrets)
    }

    fn write_signature_key_pair<
        SignaturePublicKey: traits::SignaturePublicKey<CURRENT_VERSION>,
        SignatureKeyPair: traits::SignatureKeyPair<CURRENT_VERSION>,
    >(
        &self,
        public_key: &SignaturePublicKey,
        signature_key_pair: &SignatureKeyPair,
    ) -> Result<(), Self::Error> {
        self.write_entity(
            SIGNATURE_KEY_PAIR_LABEL,
            &ser::ser(public_key)?,
            None,
            signature_key_pair,
        )
    }

    fn write_encryption_key_pair<
        EncryptionKey: traits::EncryptionKey<CURRENT_VERSION>,
        HpkeKeyPair: traits::HpkeKeyPair<CURRENT_VERSION>,
    >(
        &self,
        public_key: &EncryptionKey,
        key_pair: &HpkeKeyPair,
    ) -> Result<(), Self::Error> {
        self.write_entity(
            ENCRYPTION_KEY_PAIR_LABEL,
            &ser::ser(public_key)?,
            None,
            key_pair,
        )
    }

    fn write_encryption_epoch_key_pairs<
        GroupId: traits::GroupId<CURRENT_VERSION>,
        EpochKey: traits::EpochKey<CURRENT_VERSION>,
        HpkeKeyPair: traits::HpkeKeyPair<CURRENT_VERSION>,
    >(
        &self,
        group_id: &GroupId,
        epoch: &EpochKey,
        leaf_index: u32,
        key_pairs: &[HpkeKeyPair],
    ) -> Result<(), Self::Error> {
        let group_key = group_key_of(group_id)?;
        self.write_serialized(
            EPOCH_KEY_PAIRS_LABEL,
            &epoch_key_pairs_id(group_id, epoch, leaf_index)?,
            Some(&group_key),
            &key_pairs,
        )
    }

    fn write_key_package<
        HashReference: traits::HashReference<CURRENT_VERSION>,
        KeyPackage: traits::KeyPackage<CURRENT_VERSION>,
    >(
        &self,
        hash_ref: &HashReference,
        key_package: &KeyPackage,
    ) -> Result<(), Self::Error> {
        self.write_entity(KEY_PACKAGE_LABEL, &ser::ser(hash_ref)?, None, key_package)
    }

    fn write_psk<
        PskId: traits::PskId<CURRENT_VERSION>,
        PskBundle: traits::PskBundle<CURRENT_VERSION>,
    >(
        &self,
        psk_id: &PskId,
        psk: &PskBundle,
    ) -> Result<(), Self::Error> {
        self.write_entity(PSK_LABEL, &ser::ser(psk_id)?, None, psk)
    }

    fn mls_group_join_config<
        GroupId: traits::GroupId<CURRENT_VERSION>,
        MlsGroupJoinConfig: traits::MlsGroupJoinConfig<CURRENT_VERSION>,
    >(
        &self,
        group_id: &GroupId,
    ) -> Result<Option<MlsGroupJoinConfig>, Self::Error> {
        self.read_group_entity(JOIN_CONFIG_LABEL, group_id)
    }

    fn own_leaf_nodes<
        GroupId: traits::GroupId<CURRENT_VERSION>,
        LeafNode: traits::LeafNode<CURRENT_VERSION>,
    >(
        &self,
        group_id: &GroupId,
    ) -> Result<Vec<LeafNode>, Self::Error> {
        self.read_list(OWN_LEAF_NODES_LABEL, &group_key_of(group_id)?)
    }

    fn queued_proposal_refs<
        GroupId: traits::GroupId<CURRENT_VERSION>,
        ProposalRef: traits::ProposalRef<CURRENT_VERSION>,
    >(
        &self,
        group_id: &GroupId,
    ) -> Result<Vec<ProposalRef>, Self::Error> {
        self.read_list(PROPOSAL_QUEUE_REFS_LABEL, &group_key_of(group_id)?)
    }

    /// Dangling refs and undecodable blobs recover by clearing the queue;
    /// operational errors propagate (`provider.rs` in storage-sqlite).
    fn queued_proposals<
        GroupId: traits::GroupId<CURRENT_VERSION>,
        ProposalRef: traits::ProposalRef<CURRENT_VERSION>,
        QueuedProposal: traits::QueuedProposal<CURRENT_VERSION>,
    >(
        &self,
        group_id: &GroupId,
    ) -> Result<Vec<(ProposalRef, QueuedProposal)>, Self::Error> {
        let group_key = group_key_of(group_id)?;
        let refs: Vec<ProposalRef> = match self.read_list(PROPOSAL_QUEUE_REFS_LABEL, &group_key) {
            Ok(refs) => refs,
            Err(error) if is_undecodable(&error) => {
                self.recover_corrupt_proposal_queue(group_id)?;
                return Ok(Vec::new());
            }
            Err(error) => return Err(error),
        };
        let mut proposals = Vec::with_capacity(refs.len());
        for proposal_ref in refs {
            let key = ser::ser(&(group_id, &proposal_ref))?;
            match self.read_json::<QueuedProposal>(QUEUED_PROPOSAL_LABEL, &key) {
                Ok(Some(proposal)) => proposals.push((proposal_ref, proposal)),
                Ok(None) => {
                    self.recover_corrupt_proposal_queue(group_id)?;
                    return Ok(Vec::new());
                }
                Err(error) if is_undecodable(&error) => {
                    self.recover_corrupt_proposal_queue(group_id)?;
                    return Ok(Vec::new());
                }
                Err(error) => return Err(error),
            }
        }
        Ok(proposals)
    }

    fn tree<
        GroupId: traits::GroupId<CURRENT_VERSION>,
        TreeSync: traits::TreeSync<CURRENT_VERSION>,
    >(
        &self,
        group_id: &GroupId,
    ) -> Result<Option<TreeSync>, Self::Error> {
        self.read_group_entity(TREE_LABEL, group_id)
    }

    fn group_context<
        GroupId: traits::GroupId<CURRENT_VERSION>,
        GroupContext: traits::GroupContext<CURRENT_VERSION>,
    >(
        &self,
        group_id: &GroupId,
    ) -> Result<Option<GroupContext>, Self::Error> {
        self.read_group_entity(GROUP_CONTEXT_LABEL, group_id)
    }

    fn interim_transcript_hash<
        GroupId: traits::GroupId<CURRENT_VERSION>,
        InterimTranscriptHash: traits::InterimTranscriptHash<CURRENT_VERSION>,
    >(
        &self,
        group_id: &GroupId,
    ) -> Result<Option<InterimTranscriptHash>, Self::Error> {
        self.read_group_entity(INTERIM_TRANSCRIPT_HASH_LABEL, group_id)
    }

    fn confirmation_tag<
        GroupId: traits::GroupId<CURRENT_VERSION>,
        ConfirmationTag: traits::ConfirmationTag<CURRENT_VERSION>,
    >(
        &self,
        group_id: &GroupId,
    ) -> Result<Option<ConfirmationTag>, Self::Error> {
        self.read_group_entity(CONFIRMATION_TAG_LABEL, group_id)
    }

    fn group_state<
        GroupState: traits::GroupState<CURRENT_VERSION>,
        GroupId: traits::GroupId<CURRENT_VERSION>,
    >(
        &self,
        group_id: &GroupId,
    ) -> Result<Option<GroupState>, Self::Error> {
        self.read_group_entity(GROUP_STATE_LABEL, group_id)
    }

    fn message_secrets<
        GroupId: traits::GroupId<CURRENT_VERSION>,
        MessageSecrets: traits::MessageSecrets<CURRENT_VERSION>,
    >(
        &self,
        group_id: &GroupId,
    ) -> Result<Option<MessageSecrets>, Self::Error> {
        self.read_group_entity(MESSAGE_SECRETS_LABEL, group_id)
    }

    fn resumption_psk_store<
        GroupId: traits::GroupId<CURRENT_VERSION>,
        ResumptionPskStore: traits::ResumptionPskStore<CURRENT_VERSION>,
    >(
        &self,
        group_id: &GroupId,
    ) -> Result<Option<ResumptionPskStore>, Self::Error> {
        self.read_group_entity(RESUMPTION_PSK_STORE_LABEL, group_id)
    }

    fn own_leaf_index<
        GroupId: traits::GroupId<CURRENT_VERSION>,
        LeafNodeIndex: traits::LeafNodeIndex<CURRENT_VERSION>,
    >(
        &self,
        group_id: &GroupId,
    ) -> Result<Option<LeafNodeIndex>, Self::Error> {
        self.read_group_entity(OWN_LEAF_NODE_INDEX_LABEL, group_id)
    }

    fn group_epoch_secrets<
        GroupId: traits::GroupId<CURRENT_VERSION>,
        GroupEpochSecrets: traits::GroupEpochSecrets<CURRENT_VERSION>,
    >(
        &self,
        group_id: &GroupId,
    ) -> Result<Option<GroupEpochSecrets>, Self::Error> {
        self.read_group_entity(EPOCH_SECRETS_LABEL, group_id)
    }

    fn signature_key_pair<
        SignaturePublicKey: traits::SignaturePublicKey<CURRENT_VERSION>,
        SignatureKeyPair: traits::SignatureKeyPair<CURRENT_VERSION>,
    >(
        &self,
        public_key: &SignaturePublicKey,
    ) -> Result<Option<SignatureKeyPair>, Self::Error> {
        self.read_json(SIGNATURE_KEY_PAIR_LABEL, &ser::ser(public_key)?)
    }

    fn encryption_key_pair<
        HpkeKeyPair: traits::HpkeKeyPair<CURRENT_VERSION>,
        EncryptionKey: traits::EncryptionKey<CURRENT_VERSION>,
    >(
        &self,
        public_key: &EncryptionKey,
    ) -> Result<Option<HpkeKeyPair>, Self::Error> {
        self.read_json(ENCRYPTION_KEY_PAIR_LABEL, &ser::ser(public_key)?)
    }

    fn encryption_epoch_key_pairs<
        GroupId: traits::GroupId<CURRENT_VERSION>,
        EpochKey: traits::EpochKey<CURRENT_VERSION>,
        HpkeKeyPair: traits::HpkeKeyPair<CURRENT_VERSION>,
    >(
        &self,
        group_id: &GroupId,
        epoch: &EpochKey,
        leaf_index: u32,
    ) -> Result<Vec<HpkeKeyPair>, Self::Error> {
        let Some(row) = self.store.get(&row_key(
            EPOCH_KEY_PAIRS_LABEL.bytes,
            &epoch_key_pairs_id(group_id, epoch, leaf_index)?,
        ))?
        else {
            return Ok(Vec::new());
        };
        let (_, encoded) = decode_row(&row)?;
        decode::<Vec<HpkeKeyPair>>(&encoded)
    }

    fn key_package<
        KeyPackageRef: traits::HashReference<CURRENT_VERSION>,
        KeyPackage: traits::KeyPackage<CURRENT_VERSION>,
    >(
        &self,
        hash_ref: &KeyPackageRef,
    ) -> Result<Option<KeyPackage>, Self::Error> {
        self.read_json(KEY_PACKAGE_LABEL, &ser::ser(hash_ref)?)
    }

    fn psk<PskBundle: traits::PskBundle<CURRENT_VERSION>, PskId: traits::PskId<CURRENT_VERSION>>(
        &self,
        psk_id: &PskId,
    ) -> Result<Option<PskBundle>, Self::Error> {
        self.read_json(PSK_LABEL, &ser::ser(psk_id)?)
    }

    fn application_export_tree<
        GroupId: traits::GroupId<CURRENT_VERSION>,
        ApplicationExportTree: traits::ApplicationExportTree<CURRENT_VERSION>,
    >(
        &self,
        group_id: &GroupId,
    ) -> Result<Option<ApplicationExportTree>, Self::Error> {
        self.read_group_entity(APPLICATION_EXPORT_TREE_LABEL, group_id)
    }

    fn remove_proposal<
        GroupId: traits::GroupId<CURRENT_VERSION>,
        ProposalRef: traits::ProposalRef<CURRENT_VERSION>,
    >(
        &self,
        group_id: &GroupId,
        proposal_ref: &ProposalRef,
    ) -> Result<(), Self::Error> {
        self.store.transact(|| -> StorageResult<()> {
            let group_key = group_key_of(group_id)?;
            self.mutate_list(
                PROPOSAL_QUEUE_REFS_LABEL,
                &group_key.clone(),
                Some(&group_key),
                true,
                proposal_ref,
            )?;
            self.delete_value(QUEUED_PROPOSAL_LABEL, &ser::ser(&(group_id, proposal_ref))?)
        })
    }

    fn delete_own_leaf_nodes<GroupId: traits::GroupId<CURRENT_VERSION>>(
        &self,
        group_id: &GroupId,
    ) -> Result<(), Self::Error> {
        self.delete_group_value(OWN_LEAF_NODES_LABEL, group_id)
    }

    fn delete_group_config<GroupId: traits::GroupId<CURRENT_VERSION>>(
        &self,
        group_id: &GroupId,
    ) -> Result<(), Self::Error> {
        self.delete_group_value(JOIN_CONFIG_LABEL, group_id)
    }

    fn delete_tree<GroupId: traits::GroupId<CURRENT_VERSION>>(
        &self,
        group_id: &GroupId,
    ) -> Result<(), Self::Error> {
        self.delete_group_value(TREE_LABEL, group_id)
    }

    fn delete_confirmation_tag<GroupId: traits::GroupId<CURRENT_VERSION>>(
        &self,
        group_id: &GroupId,
    ) -> Result<(), Self::Error> {
        self.delete_group_value(CONFIRMATION_TAG_LABEL, group_id)
    }

    fn delete_group_state<GroupId: traits::GroupId<CURRENT_VERSION>>(
        &self,
        group_id: &GroupId,
    ) -> Result<(), Self::Error> {
        self.delete_group_value(GROUP_STATE_LABEL, group_id)
    }

    fn delete_context<GroupId: traits::GroupId<CURRENT_VERSION>>(
        &self,
        group_id: &GroupId,
    ) -> Result<(), Self::Error> {
        self.delete_group_value(GROUP_CONTEXT_LABEL, group_id)
    }

    fn delete_interim_transcript_hash<GroupId: traits::GroupId<CURRENT_VERSION>>(
        &self,
        group_id: &GroupId,
    ) -> Result<(), Self::Error> {
        self.delete_group_value(INTERIM_TRANSCRIPT_HASH_LABEL, group_id)
    }

    fn delete_message_secrets<GroupId: traits::GroupId<CURRENT_VERSION>>(
        &self,
        group_id: &GroupId,
    ) -> Result<(), Self::Error> {
        self.delete_group_value(MESSAGE_SECRETS_LABEL, group_id)
    }

    fn delete_all_resumption_psk_secrets<GroupId: traits::GroupId<CURRENT_VERSION>>(
        &self,
        group_id: &GroupId,
    ) -> Result<(), Self::Error> {
        self.delete_group_value(RESUMPTION_PSK_STORE_LABEL, group_id)
    }

    fn delete_own_leaf_index<GroupId: traits::GroupId<CURRENT_VERSION>>(
        &self,
        group_id: &GroupId,
    ) -> Result<(), Self::Error> {
        self.delete_group_value(OWN_LEAF_NODE_INDEX_LABEL, group_id)
    }

    fn delete_group_epoch_secrets<GroupId: traits::GroupId<CURRENT_VERSION>>(
        &self,
        group_id: &GroupId,
    ) -> Result<(), Self::Error> {
        self.delete_group_value(EPOCH_SECRETS_LABEL, group_id)
    }

    /// Both queue labels in one unit — a crash between them would leave
    /// refs to missing entities and brick `MlsGroup::load` (issue #148).
    fn clear_proposal_queue<
        GroupId: traits::GroupId<CURRENT_VERSION>,
        ProposalRef: traits::ProposalRef<CURRENT_VERSION>,
    >(
        &self,
        group_id: &GroupId,
    ) -> Result<(), Self::Error> {
        self.delete_group_labels(
            group_id,
            &[QUEUED_PROPOSAL_LABEL, PROPOSAL_QUEUE_REFS_LABEL],
        )
    }

    fn delete_signature_key_pair<
        SignaturePublicKey: traits::SignaturePublicKey<CURRENT_VERSION>,
    >(
        &self,
        public_key: &SignaturePublicKey,
    ) -> Result<(), Self::Error> {
        self.delete_value(SIGNATURE_KEY_PAIR_LABEL, &ser::ser(public_key)?)
    }

    fn delete_encryption_key_pair<EncryptionKey: traits::EncryptionKey<CURRENT_VERSION>>(
        &self,
        public_key: &EncryptionKey,
    ) -> Result<(), Self::Error> {
        self.delete_value(ENCRYPTION_KEY_PAIR_LABEL, &ser::ser(public_key)?)
    }

    fn delete_encryption_epoch_key_pairs<
        GroupId: traits::GroupId<CURRENT_VERSION>,
        EpochKey: traits::EpochKey<CURRENT_VERSION>,
    >(
        &self,
        group_id: &GroupId,
        epoch: &EpochKey,
        leaf_index: u32,
    ) -> Result<(), Self::Error> {
        self.delete_value(
            EPOCH_KEY_PAIRS_LABEL,
            &epoch_key_pairs_id(group_id, epoch, leaf_index)?,
        )
    }

    fn delete_key_package<KeyPackageRef: traits::HashReference<CURRENT_VERSION>>(
        &self,
        hash_ref: &KeyPackageRef,
    ) -> Result<(), Self::Error> {
        self.delete_value(KEY_PACKAGE_LABEL, &ser::ser(hash_ref)?)
    }

    fn delete_psk<PskKey: traits::PskId<CURRENT_VERSION>>(
        &self,
        psk_id: &PskKey,
    ) -> Result<(), Self::Error> {
        self.delete_value(PSK_LABEL, &ser::ser(psk_id)?)
    }

    fn write_application_export_tree<
        GroupId: traits::GroupId<CURRENT_VERSION>,
        ApplicationExportTree: traits::ApplicationExportTree<CURRENT_VERSION>,
    >(
        &self,
        group_id: &GroupId,
        application_export_tree: &ApplicationExportTree,
    ) -> Result<(), Self::Error> {
        self.write_group_entity(
            APPLICATION_EXPORT_TREE_LABEL,
            group_id,
            application_export_tree,
        )
    }

    fn delete_application_export_tree<
        GroupId: traits::GroupId<CURRENT_VERSION>,
        ApplicationExportTree: traits::ApplicationExportTree<CURRENT_VERSION>,
    >(
        &self,
        group_id: &GroupId,
    ) -> Result<(), Self::Error> {
        self.delete_group_value(APPLICATION_EXPORT_TREE_LABEL, group_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn index_segment_recovers_raw_group_id_bytes() {
        let group_id = openmls::group::GroupId::from_slice(&[9, 8, 7, 6, 5]);
        let group_key = serde_json::to_vec(&group_id).unwrap();
        assert_eq!(index_segment(&group_key).unwrap(), vec![9, 8, 7, 6, 5]);
    }

    #[test]
    fn index_segment_rejects_undecodable_group_key() {
        assert!(index_segment(b"not-a-group-id").is_err());
    }

    #[test]
    fn value_encoding_round_trips() {
        #[derive(Debug, PartialEq, Serialize, serde::Deserialize)]
        struct Blob {
            bytes: Vec<u8>,
            epoch: u64,
        }
        let blob = Blob {
            bytes: vec![1, 2, 3],
            epoch: 7,
        };
        for label in [TREE_LABEL, KEY_PACKAGE_LABEL] {
            let encoded = encode(label, &blob).unwrap();
            assert_eq!(decode::<Blob>(&encoded).unwrap(), blob);
        }
    }
}
