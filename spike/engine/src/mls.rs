//! OpenMLS `StorageProvider` over the shared `KvStore`.
//!
//! Mirrors storage-sqlite's layout: each entity is stored under
//! `build_key(label, entity_key)` inside the `mls/` namespace; group-scoped
//! rows are registered in a `gml/<group_key>` index so group delete and
//! snapshots can enumerate them. Values are serde_json — a self-describing
//! format is required because OpenMLS's entities use hand-written
//! `Deserialize` impls (storage-sqlite's value_store.rs makes the same point:
//! positional formats like postcard cannot serve).

use std::error::Error as StdError;
use std::fmt;
use std::sync::Arc;

use openmls_traits::storage::{CURRENT_VERSION, StorageProvider, traits};
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::provider::{GML_NS, MLS_NS};
use crate::store::KvStore;

pub const KEY_PACKAGE_LABEL: &[u8] = b"KeyPackage";
const PSK_LABEL: &[u8] = b"Psk";
const ENCRYPTION_KEY_PAIR_LABEL: &[u8] = b"EncryptionKeyPair";
const SIGNATURE_KEY_PAIR_LABEL: &[u8] = b"SignatureKeyPair";
const EPOCH_KEY_PAIRS_LABEL: &[u8] = b"EpochKeyPairs";
const TREE_LABEL: &[u8] = b"Tree";
const GROUP_CONTEXT_LABEL: &[u8] = b"GroupContext";
const APPLICATION_EXPORT_TREE_LABEL: &[u8] = b"ApplicationExportTree";
const INTERIM_TRANSCRIPT_HASH_LABEL: &[u8] = b"InterimTranscriptHash";
const CONFIRMATION_TAG_LABEL: &[u8] = b"ConfirmationTag";
const JOIN_CONFIG_LABEL: &[u8] = b"MlsGroupJoinConfig";
const OWN_LEAF_NODES_LABEL: &[u8] = b"OwnLeafNodes";
const GROUP_STATE_LABEL: &[u8] = b"GroupState";
const QUEUED_PROPOSAL_LABEL: &[u8] = b"QueuedProposal";
const PROPOSAL_QUEUE_REFS_LABEL: &[u8] = b"ProposalQueueRefs";
const OWN_LEAF_NODE_INDEX_LABEL: &[u8] = b"OwnLeafNodeIndex";
const EPOCH_SECRETS_LABEL: &[u8] = b"EpochSecrets";
const RESUMPTION_PSK_LABEL: &[u8] = b"ResumptionPsk";
const MESSAGE_SECRETS_LABEL: &[u8] = b"MessageSecrets";

const VALUE_STORAGE_KEY_V1: &[u8] = b"mdk.openmls.value-key.v1";

/// MDK-compatible storage key so `StoredKeyPackageBundle::storage_key` values
/// interoperate with MDK tooling semantics.
fn build_key(label: &[u8], key: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(VALUE_STORAGE_KEY_V1);
    out.extend_from_slice(&(label.len() as u64).to_be_bytes());
    out.extend_from_slice(label);
    out.extend_from_slice(&(key.len() as u64).to_be_bytes());
    out.extend_from_slice(key);
    out.extend_from_slice(&CURRENT_VERSION.to_be_bytes());
    out
}

/// `mls/<label>/<storage-key>` — label readable at a fixed offset so
/// `stored_key_package_bundles` can prefix-scan.
pub fn label_prefix(label: &[u8]) -> Vec<u8> {
    let mut out = MLS_NS.to_vec();
    out.extend_from_slice(&(label.len() as u32).to_be_bytes());
    out.extend_from_slice(label);
    out
}

fn row_key(label: &[u8], entity_key: &[u8]) -> Vec<u8> {
    let mut out = label_prefix(label);
    out.extend_from_slice(&build_key(label, entity_key));
    out
}

/// serde_json encoding of a group id — the same value sqlite uses as
/// `group_key` for row indexing. This MUST byte-match
/// `serde_json::to_vec` of the OpenMLS `GroupId` used in
/// `KvOpenMlsStorage::group_key`, or the `gml/` index prefix scans in
/// `provider.rs` silently match nothing and snapshot/restore leaks every
/// OpenMLS row (the replay-then-WrongEpoch livelock this bug produced).
/// OpenMLS `GroupId` is a struct `{ value: VLBytes }`, not a byte array.
pub fn group_key_json(gid: &[u8]) -> Vec<u8> {
    serde_json::to_vec(&openmls::group::GroupId::from_slice(gid)).unwrap_or_default()
}

fn group_index_key(group_key: &[u8], storage_key: &[u8]) -> Vec<u8> {
    let mut out = GML_NS.to_vec();
    out.extend_from_slice(&(group_key.len() as u32).to_be_bytes());
    out.extend_from_slice(group_key);
    out.extend_from_slice(storage_key);
    out
}

#[derive(Debug)]
pub struct KvOpenMlsStorageError(String);

impl fmt::Display for KvOpenMlsStorageError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl StdError for KvOpenMlsStorageError {}

impl From<serde_json::Error> for KvOpenMlsStorageError {
    fn from(e: serde_json::Error) -> Self {
        Self(format!("serde: {e}"))
    }
}

type Res<T> = Result<T, KvOpenMlsStorageError>;

/// OpenMLS storage facade over the same `KvStore` as the Marmot tables.
#[derive(Clone)]
pub struct KvOpenMlsStorage {
    store: Arc<KvStore>,
}

impl KvOpenMlsStorage {
    pub fn new(store: Arc<KvStore>) -> Self {
        Self { store }
    }

    fn group_key<GroupId: traits::GroupId<CURRENT_VERSION>>(group_id: &GroupId) -> Res<Vec<u8>> {
        Ok(serde_json::to_vec(group_id)?)
    }

    fn register_group_row(&self, group_key: &[u8], storage_key: &[u8]) {
        self.store
            .put(group_index_key(group_key, storage_key), Vec::new());
    }

    fn unregister_group_row(&self, group_key: &[u8], storage_key: &[u8]) {
        self.store.delete(&group_index_key(group_key, storage_key));
    }

    /// Single-row group-scoped entity (`Tree`, `GroupContext`, …).
    fn write_group_entity<GroupId, V>(&self, label: &[u8], group_id: &GroupId, value: &V) -> Res<()>
    where
        GroupId: traits::GroupId<CURRENT_VERSION>,
        V: Serialize + ?Sized,
    {
        let gk = Self::group_key(group_id)?;
        let key = row_key(label, &gk);
        self.store.put(key.clone(), serde_json::to_vec(value)?);
        self.register_group_row(&gk, &key);
        Ok(())
    }

    fn read_group_entity<GroupId, V>(&self, label: &[u8], group_id: &GroupId) -> Res<Option<V>>
    where
        GroupId: traits::GroupId<CURRENT_VERSION>,
        V: DeserializeOwned,
    {
        let gk = Self::group_key(group_id)?;
        match self.store.get(&row_key(label, &gk)) {
            Some(b) => Ok(Some(serde_json::from_slice(&b)?)),
            None => Ok(None),
        }
    }

    fn delete_group_entity<GroupId>(&self, label: &[u8], group_id: &GroupId) -> Res<()>
    where
        GroupId: traits::GroupId<CURRENT_VERSION>,
    {
        let gk = Self::group_key(group_id)?;
        let key = row_key(label, &gk);
        self.store.delete(&key);
        self.unregister_group_row(&gk, &key);
        Ok(())
    }

    /// Ungrouped point entity (`SignatureKeyPair`, `KeyPackage`, `Psk`).
    fn write_entity<V: Serialize + ?Sized>(
        &self,
        label: &[u8],
        entity_key: Vec<u8>,
        group_key: Option<Vec<u8>>,
        value: &V,
    ) -> Res<()> {
        let key = row_key(label, &entity_key);
        self.store.put(key.clone(), serde_json::to_vec(value)?);
        if let Some(gk) = group_key {
            self.register_group_row(&gk, &key);
        }
        Ok(())
    }

    fn read_entity<V: DeserializeOwned>(
        &self,
        label: &[u8],
        entity_key: Vec<u8>,
    ) -> Res<Option<V>> {
        match self.store.get(&row_key(label, &entity_key)) {
            Some(b) => Ok(Some(serde_json::from_slice(&b)?)),
            None => Ok(None),
        }
    }

    fn delete_entity(&self, label: &[u8], entity_key: Vec<u8>) -> Res<()> {
        self.store.delete(&row_key(label, &entity_key));
        Ok(())
    }

    /// List-valued group entity (`OwnLeafNodes`, `ProposalQueueRefs`).
    fn read_list<V: DeserializeOwned>(&self, label: &[u8], group_key: Vec<u8>) -> Res<Vec<V>> {
        match self.store.get(&row_key(label, &group_key)) {
            Some(b) => Ok(serde_json::from_slice(&b)?),
            None => Ok(Vec::new()),
        }
    }

    fn append_entity<V: Serialize>(
        &self,
        label: &[u8],
        entity_key: Vec<u8>,
        group_key: Option<Vec<u8>>,
        value: &V,
    ) -> Res<()> {
        let key = row_key(label, &entity_key);
        let mut list: Vec<serde_json::Value> = match self.store.get(&key) {
            Some(b) => serde_json::from_slice(&b)?,
            None => Vec::new(),
        };
        list.push(serde_json::to_value(value)?);
        self.store.put(key.clone(), serde_json::to_vec(&list)?);
        if let Some(gk) = group_key {
            self.register_group_row(&gk, &key);
        }
        Ok(())
    }
}

fn epoch_key_pairs_id<GroupId, EpochKey>(
    group_id: &GroupId,
    epoch: &EpochKey,
    leaf_index: u32,
) -> Res<Vec<u8>>
where
    GroupId: traits::GroupId<CURRENT_VERSION>,
    EpochKey: traits::EpochKey<CURRENT_VERSION>,
{
    Ok(serde_json::to_vec(&(group_id, epoch, leaf_index))?)
}

impl StorageProvider<CURRENT_VERSION> for KvOpenMlsStorage {
    type Error = KvOpenMlsStorageError;

    fn write_mls_join_config<
        GroupId: traits::GroupId<CURRENT_VERSION>,
        MlsGroupJoinConfig: traits::MlsGroupJoinConfig<CURRENT_VERSION>,
    >(
        &self,
        group_id: &GroupId,
        config: &MlsGroupJoinConfig,
    ) -> Res<()> {
        self.write_group_entity(JOIN_CONFIG_LABEL, group_id, config)
    }

    fn append_own_leaf_node<
        GroupId: traits::GroupId<CURRENT_VERSION>,
        LeafNode: traits::LeafNode<CURRENT_VERSION>,
    >(
        &self,
        group_id: &GroupId,
        leaf_node: &LeafNode,
    ) -> Res<()> {
        let gk = Self::group_key(group_id)?;
        self.append_entity(OWN_LEAF_NODES_LABEL, gk.clone(), Some(gk), leaf_node)
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
    ) -> Res<()> {
        let gk = Self::group_key(group_id)?;
        let proposal_key = serde_json::to_vec(&(group_id, proposal_ref))?;
        self.store.transact::<(), KvOpenMlsStorageError, _>(|| {
            self.write_entity(
                QUEUED_PROPOSAL_LABEL,
                proposal_key,
                Some(gk.clone()),
                proposal,
            )?;
            self.append_entity(
                PROPOSAL_QUEUE_REFS_LABEL,
                gk.clone(),
                Some(gk),
                proposal_ref,
            )?;
            Ok(())
        })
    }

    fn write_tree<
        GroupId: traits::GroupId<CURRENT_VERSION>,
        TreeSync: traits::TreeSync<CURRENT_VERSION>,
    >(
        &self,
        group_id: &GroupId,
        tree: &TreeSync,
    ) -> Res<()> {
        self.write_group_entity(TREE_LABEL, group_id, tree)
    }

    fn write_interim_transcript_hash<
        GroupId: traits::GroupId<CURRENT_VERSION>,
        InterimTranscriptHash: traits::InterimTranscriptHash<CURRENT_VERSION>,
    >(
        &self,
        group_id: &GroupId,
        hash: &InterimTranscriptHash,
    ) -> Res<()> {
        self.write_group_entity(INTERIM_TRANSCRIPT_HASH_LABEL, group_id, hash)
    }

    fn write_context<
        GroupId: traits::GroupId<CURRENT_VERSION>,
        GroupContext: traits::GroupContext<CURRENT_VERSION>,
    >(
        &self,
        group_id: &GroupId,
        context: &GroupContext,
    ) -> Res<()> {
        self.write_group_entity(GROUP_CONTEXT_LABEL, group_id, context)
    }

    fn write_confirmation_tag<
        GroupId: traits::GroupId<CURRENT_VERSION>,
        ConfirmationTag: traits::ConfirmationTag<CURRENT_VERSION>,
    >(
        &self,
        group_id: &GroupId,
        tag: &ConfirmationTag,
    ) -> Res<()> {
        self.write_group_entity(CONFIRMATION_TAG_LABEL, group_id, tag)
    }

    fn write_group_state<
        GroupState: traits::GroupState<CURRENT_VERSION>,
        GroupId: traits::GroupId<CURRENT_VERSION>,
    >(
        &self,
        group_id: &GroupId,
        state: &GroupState,
    ) -> Res<()> {
        self.write_group_entity(GROUP_STATE_LABEL, group_id, state)
    }

    fn write_message_secrets<
        GroupId: traits::GroupId<CURRENT_VERSION>,
        MessageSecrets: traits::MessageSecrets<CURRENT_VERSION>,
    >(
        &self,
        group_id: &GroupId,
        secrets: &MessageSecrets,
    ) -> Res<()> {
        self.write_group_entity(MESSAGE_SECRETS_LABEL, group_id, secrets)
    }

    fn write_resumption_psk_store<
        GroupId: traits::GroupId<CURRENT_VERSION>,
        ResumptionPskStore: traits::ResumptionPskStore<CURRENT_VERSION>,
    >(
        &self,
        group_id: &GroupId,
        store: &ResumptionPskStore,
    ) -> Res<()> {
        self.write_group_entity(RESUMPTION_PSK_LABEL, group_id, store)
    }

    fn write_own_leaf_index<
        GroupId: traits::GroupId<CURRENT_VERSION>,
        LeafNodeIndex: traits::LeafNodeIndex<CURRENT_VERSION>,
    >(
        &self,
        group_id: &GroupId,
        index: &LeafNodeIndex,
    ) -> Res<()> {
        self.write_group_entity(OWN_LEAF_NODE_INDEX_LABEL, group_id, index)
    }

    fn write_group_epoch_secrets<
        GroupId: traits::GroupId<CURRENT_VERSION>,
        GroupEpochSecrets: traits::GroupEpochSecrets<CURRENT_VERSION>,
    >(
        &self,
        group_id: &GroupId,
        secrets: &GroupEpochSecrets,
    ) -> Res<()> {
        self.write_group_entity(EPOCH_SECRETS_LABEL, group_id, secrets)
    }

    fn write_application_export_tree<
        GroupId: traits::GroupId<CURRENT_VERSION>,
        ApplicationExportTree: traits::ApplicationExportTree<CURRENT_VERSION>,
    >(
        &self,
        group_id: &GroupId,
        tree: &ApplicationExportTree,
    ) -> Res<()> {
        self.write_group_entity(APPLICATION_EXPORT_TREE_LABEL, group_id, tree)
    }

    fn write_signature_key_pair<
        SignaturePublicKey: traits::SignaturePublicKey<CURRENT_VERSION>,
        SignatureKeyPair: traits::SignatureKeyPair<CURRENT_VERSION>,
    >(
        &self,
        public_key: &SignaturePublicKey,
        signature_key_pair: &SignatureKeyPair,
    ) -> Res<()> {
        self.write_entity(
            SIGNATURE_KEY_PAIR_LABEL,
            serde_json::to_vec(public_key)?,
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
    ) -> Res<()> {
        self.write_entity(
            ENCRYPTION_KEY_PAIR_LABEL,
            serde_json::to_vec(public_key)?,
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
    ) -> Res<()> {
        let gk = Self::group_key(group_id)?;
        self.write_entity(
            EPOCH_KEY_PAIRS_LABEL,
            epoch_key_pairs_id(group_id, epoch, leaf_index)?,
            Some(gk),
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
    ) -> Res<()> {
        self.write_entity(
            KEY_PACKAGE_LABEL,
            serde_json::to_vec(hash_ref)?,
            None,
            key_package,
        )
    }

    fn write_psk<
        PskId: traits::PskId<CURRENT_VERSION>,
        PskBundle: traits::PskBundle<CURRENT_VERSION>,
    >(
        &self,
        psk_id: &PskId,
        psk: &PskBundle,
    ) -> Res<()> {
        self.write_entity(PSK_LABEL, serde_json::to_vec(psk_id)?, None, psk)
    }

    fn mls_group_join_config<
        GroupId: traits::GroupId<CURRENT_VERSION>,
        MlsGroupJoinConfig: traits::MlsGroupJoinConfig<CURRENT_VERSION>,
    >(
        &self,
        group_id: &GroupId,
    ) -> Res<Option<MlsGroupJoinConfig>> {
        self.read_group_entity(JOIN_CONFIG_LABEL, group_id)
    }

    fn own_leaf_nodes<
        GroupId: traits::GroupId<CURRENT_VERSION>,
        LeafNode: traits::LeafNode<CURRENT_VERSION>,
    >(
        &self,
        group_id: &GroupId,
    ) -> Res<Vec<LeafNode>> {
        self.read_list(OWN_LEAF_NODES_LABEL, Self::group_key(group_id)?)
    }

    fn queued_proposal_refs<
        GroupId: traits::GroupId<CURRENT_VERSION>,
        ProposalRef: traits::ProposalRef<CURRENT_VERSION>,
    >(
        &self,
        group_id: &GroupId,
    ) -> Res<Vec<ProposalRef>> {
        self.read_list(PROPOSAL_QUEUE_REFS_LABEL, Self::group_key(group_id)?)
    }

    fn queued_proposals<
        GroupId: traits::GroupId<CURRENT_VERSION>,
        ProposalRef: traits::ProposalRef<CURRENT_VERSION>,
        QueuedProposal: traits::QueuedProposal<CURRENT_VERSION>,
    >(
        &self,
        group_id: &GroupId,
    ) -> Res<Vec<(ProposalRef, QueuedProposal)>> {
        let refs: Vec<ProposalRef> =
            self.read_list(PROPOSAL_QUEUE_REFS_LABEL, Self::group_key(group_id)?)?;
        let mut out = Vec::with_capacity(refs.len());
        for proposal_ref in refs {
            let key = serde_json::to_vec(&(group_id, &proposal_ref))?;
            if let Some(p) = self.read_entity::<QueuedProposal>(QUEUED_PROPOSAL_LABEL, key)? {
                out.push((proposal_ref, p));
            }
        }
        Ok(out)
    }

    fn tree<
        GroupId: traits::GroupId<CURRENT_VERSION>,
        TreeSync: traits::TreeSync<CURRENT_VERSION>,
    >(
        &self,
        group_id: &GroupId,
    ) -> Res<Option<TreeSync>> {
        self.read_group_entity(TREE_LABEL, group_id)
    }

    fn group_context<
        GroupId: traits::GroupId<CURRENT_VERSION>,
        GroupContext: traits::GroupContext<CURRENT_VERSION>,
    >(
        &self,
        group_id: &GroupId,
    ) -> Res<Option<GroupContext>> {
        self.read_group_entity(GROUP_CONTEXT_LABEL, group_id)
    }

    fn interim_transcript_hash<
        GroupId: traits::GroupId<CURRENT_VERSION>,
        InterimTranscriptHash: traits::InterimTranscriptHash<CURRENT_VERSION>,
    >(
        &self,
        group_id: &GroupId,
    ) -> Res<Option<InterimTranscriptHash>> {
        self.read_group_entity(INTERIM_TRANSCRIPT_HASH_LABEL, group_id)
    }

    fn confirmation_tag<
        GroupId: traits::GroupId<CURRENT_VERSION>,
        ConfirmationTag: traits::ConfirmationTag<CURRENT_VERSION>,
    >(
        &self,
        group_id: &GroupId,
    ) -> Res<Option<ConfirmationTag>> {
        self.read_group_entity(CONFIRMATION_TAG_LABEL, group_id)
    }

    fn group_state<
        GroupState: traits::GroupState<CURRENT_VERSION>,
        GroupId: traits::GroupId<CURRENT_VERSION>,
    >(
        &self,
        group_id: &GroupId,
    ) -> Res<Option<GroupState>> {
        self.read_group_entity(GROUP_STATE_LABEL, group_id)
    }

    fn message_secrets<
        GroupId: traits::GroupId<CURRENT_VERSION>,
        MessageSecrets: traits::MessageSecrets<CURRENT_VERSION>,
    >(
        &self,
        group_id: &GroupId,
    ) -> Res<Option<MessageSecrets>> {
        self.read_group_entity(MESSAGE_SECRETS_LABEL, group_id)
    }

    fn resumption_psk_store<
        GroupId: traits::GroupId<CURRENT_VERSION>,
        ResumptionPskStore: traits::ResumptionPskStore<CURRENT_VERSION>,
    >(
        &self,
        group_id: &GroupId,
    ) -> Res<Option<ResumptionPskStore>> {
        self.read_group_entity(RESUMPTION_PSK_LABEL, group_id)
    }

    fn own_leaf_index<
        GroupId: traits::GroupId<CURRENT_VERSION>,
        LeafNodeIndex: traits::LeafNodeIndex<CURRENT_VERSION>,
    >(
        &self,
        group_id: &GroupId,
    ) -> Res<Option<LeafNodeIndex>> {
        self.read_group_entity(OWN_LEAF_NODE_INDEX_LABEL, group_id)
    }

    fn group_epoch_secrets<
        GroupId: traits::GroupId<CURRENT_VERSION>,
        GroupEpochSecrets: traits::GroupEpochSecrets<CURRENT_VERSION>,
    >(
        &self,
        group_id: &GroupId,
    ) -> Res<Option<GroupEpochSecrets>> {
        self.read_group_entity(EPOCH_SECRETS_LABEL, group_id)
    }

    fn signature_key_pair<
        SignaturePublicKey: traits::SignaturePublicKey<CURRENT_VERSION>,
        SignatureKeyPair: traits::SignatureKeyPair<CURRENT_VERSION>,
    >(
        &self,
        public_key: &SignaturePublicKey,
    ) -> Res<Option<SignatureKeyPair>> {
        self.read_entity(SIGNATURE_KEY_PAIR_LABEL, serde_json::to_vec(public_key)?)
    }

    fn encryption_key_pair<
        HpkeKeyPair: traits::HpkeKeyPair<CURRENT_VERSION>,
        EncryptionKey: traits::EncryptionKey<CURRENT_VERSION>,
    >(
        &self,
        public_key: &EncryptionKey,
    ) -> Res<Option<HpkeKeyPair>> {
        self.read_entity(ENCRYPTION_KEY_PAIR_LABEL, serde_json::to_vec(public_key)?)
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
    ) -> Res<Vec<HpkeKeyPair>> {
        let key = epoch_key_pairs_id(group_id, epoch, leaf_index)?;
        Ok(self
            .read_entity::<Vec<HpkeKeyPair>>(EPOCH_KEY_PAIRS_LABEL, key)?
            .unwrap_or_default())
    }

    fn key_package<
        KeyPackageRef: traits::HashReference<CURRENT_VERSION>,
        KeyPackage: traits::KeyPackage<CURRENT_VERSION>,
    >(
        &self,
        hash_ref: &KeyPackageRef,
    ) -> Res<Option<KeyPackage>> {
        self.read_entity(KEY_PACKAGE_LABEL, serde_json::to_vec(hash_ref)?)
    }

    fn psk<PskBundle: traits::PskBundle<CURRENT_VERSION>, PskId: traits::PskId<CURRENT_VERSION>>(
        &self,
        psk_id: &PskId,
    ) -> Res<Option<PskBundle>> {
        self.read_entity(PSK_LABEL, serde_json::to_vec(psk_id)?)
    }

    fn application_export_tree<
        GroupId: traits::GroupId<CURRENT_VERSION>,
        ApplicationExportTree: traits::ApplicationExportTree<CURRENT_VERSION>,
    >(
        &self,
        group_id: &GroupId,
    ) -> Res<Option<ApplicationExportTree>> {
        self.read_group_entity(APPLICATION_EXPORT_TREE_LABEL, group_id)
    }

    fn remove_proposal<
        GroupId: traits::GroupId<CURRENT_VERSION>,
        ProposalRef: traits::ProposalRef<CURRENT_VERSION>,
    >(
        &self,
        group_id: &GroupId,
        proposal_ref: &ProposalRef,
    ) -> Res<()> {
        let gk = Self::group_key(group_id)?;
        let mut refs: Vec<serde_json::Value> = self
            .store
            .get(&row_key(PROPOSAL_QUEUE_REFS_LABEL, &gk))
            .map(|b| serde_json::from_slice(&b))
            .transpose()?
            .unwrap_or_default();
        let target = serde_json::to_value(proposal_ref)?;
        refs.retain(|r| r != &target);
        let refs_key = row_key(PROPOSAL_QUEUE_REFS_LABEL, &gk);
        self.store.put(refs_key, serde_json::to_vec(&refs)?);
        self.delete_entity(
            QUEUED_PROPOSAL_LABEL,
            serde_json::to_vec(&(group_id, proposal_ref))?,
        )
    }

    fn delete_own_leaf_nodes<GroupId: traits::GroupId<CURRENT_VERSION>>(
        &self,
        group_id: &GroupId,
    ) -> Res<()> {
        self.delete_group_entity(OWN_LEAF_NODES_LABEL, group_id)
    }

    fn delete_group_config<GroupId: traits::GroupId<CURRENT_VERSION>>(
        &self,
        group_id: &GroupId,
    ) -> Res<()> {
        self.delete_group_entity(JOIN_CONFIG_LABEL, group_id)
    }

    fn delete_tree<GroupId: traits::GroupId<CURRENT_VERSION>>(
        &self,
        group_id: &GroupId,
    ) -> Res<()> {
        self.delete_group_entity(TREE_LABEL, group_id)
    }

    fn delete_confirmation_tag<GroupId: traits::GroupId<CURRENT_VERSION>>(
        &self,
        group_id: &GroupId,
    ) -> Res<()> {
        self.delete_group_entity(CONFIRMATION_TAG_LABEL, group_id)
    }

    fn delete_group_state<GroupId: traits::GroupId<CURRENT_VERSION>>(
        &self,
        group_id: &GroupId,
    ) -> Res<()> {
        self.delete_group_entity(GROUP_STATE_LABEL, group_id)
    }

    fn delete_context<GroupId: traits::GroupId<CURRENT_VERSION>>(
        &self,
        group_id: &GroupId,
    ) -> Res<()> {
        self.delete_group_entity(GROUP_CONTEXT_LABEL, group_id)
    }

    fn delete_interim_transcript_hash<GroupId: traits::GroupId<CURRENT_VERSION>>(
        &self,
        group_id: &GroupId,
    ) -> Res<()> {
        self.delete_group_entity(INTERIM_TRANSCRIPT_HASH_LABEL, group_id)
    }

    fn delete_message_secrets<GroupId: traits::GroupId<CURRENT_VERSION>>(
        &self,
        group_id: &GroupId,
    ) -> Res<()> {
        self.delete_group_entity(MESSAGE_SECRETS_LABEL, group_id)
    }

    fn delete_all_resumption_psk_secrets<GroupId: traits::GroupId<CURRENT_VERSION>>(
        &self,
        group_id: &GroupId,
    ) -> Res<()> {
        self.delete_group_entity(RESUMPTION_PSK_LABEL, group_id)
    }

    fn delete_own_leaf_index<GroupId: traits::GroupId<CURRENT_VERSION>>(
        &self,
        group_id: &GroupId,
    ) -> Res<()> {
        self.delete_group_entity(OWN_LEAF_NODE_INDEX_LABEL, group_id)
    }

    fn delete_group_epoch_secrets<GroupId: traits::GroupId<CURRENT_VERSION>>(
        &self,
        group_id: &GroupId,
    ) -> Res<()> {
        self.delete_group_entity(EPOCH_SECRETS_LABEL, group_id)
    }

    fn clear_proposal_queue<
        GroupId: traits::GroupId<CURRENT_VERSION>,
        ProposalRef: traits::ProposalRef<CURRENT_VERSION>,
    >(
        &self,
        group_id: &GroupId,
    ) -> Res<()> {
        let gk = Self::group_key(group_id)?;
        let refs: Vec<serde_json::Value> = self
            .store
            .get(&row_key(PROPOSAL_QUEUE_REFS_LABEL, &gk))
            .map(|b| serde_json::from_slice(&b))
            .transpose()?
            .unwrap_or_default();
        self.store.transact::<(), KvOpenMlsStorageError, _>(|| {
            for r in &refs {
                let pref: ProposalRef = serde_json::from_value(r.clone())?;
                self.delete_entity(
                    QUEUED_PROPOSAL_LABEL,
                    serde_json::to_vec(&(group_id, &pref))?,
                )?;
            }
            self.delete_group_entity(PROPOSAL_QUEUE_REFS_LABEL, group_id)?;
            Ok(())
        })
    }

    fn delete_signature_key_pair<
        SignaturePublicKey: traits::SignaturePublicKey<CURRENT_VERSION>,
    >(
        &self,
        public_key: &SignaturePublicKey,
    ) -> Res<()> {
        self.delete_entity(SIGNATURE_KEY_PAIR_LABEL, serde_json::to_vec(public_key)?)
    }

    fn delete_encryption_key_pair<EncryptionKey: traits::EncryptionKey<CURRENT_VERSION>>(
        &self,
        public_key: &EncryptionKey,
    ) -> Res<()> {
        self.delete_entity(ENCRYPTION_KEY_PAIR_LABEL, serde_json::to_vec(public_key)?)
    }

    fn delete_encryption_epoch_key_pairs<
        GroupId: traits::GroupId<CURRENT_VERSION>,
        EpochKey: traits::EpochKey<CURRENT_VERSION>,
    >(
        &self,
        group_id: &GroupId,
        epoch: &EpochKey,
        leaf_index: u32,
    ) -> Res<()> {
        self.delete_entity(
            EPOCH_KEY_PAIRS_LABEL,
            epoch_key_pairs_id(group_id, epoch, leaf_index)?,
        )
    }

    fn delete_key_package<KeyPackageRef: traits::HashReference<CURRENT_VERSION>>(
        &self,
        hash_ref: &KeyPackageRef,
    ) -> Res<()> {
        self.delete_entity(KEY_PACKAGE_LABEL, serde_json::to_vec(hash_ref)?)
    }

    fn delete_psk<PskKey: traits::PskId<CURRENT_VERSION>>(&self, psk_id: &PskKey) -> Res<()> {
        self.delete_entity(PSK_LABEL, serde_json::to_vec(psk_id)?)
    }

    fn delete_application_export_tree<
        GroupId: traits::GroupId<CURRENT_VERSION>,
        ApplicationExportTree: traits::ApplicationExportTree<CURRENT_VERSION>,
    >(
        &self,
        group_id: &GroupId,
    ) -> Res<()> {
        self.delete_group_entity(APPLICATION_EXPORT_TREE_LABEL, group_id)
    }
}
