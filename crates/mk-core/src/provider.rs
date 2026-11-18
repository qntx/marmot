//! `KvStorageProvider`: the Marmot storage traits over the KV store.
//!
//! Every method mirrors `storage-sqlite` v0.17.0 semantics: insertion-order
//! listings, upsert-keeps-position, `INSERT OR IGNORE`/`ON CONFLICT DO UPDATE`
//! idempotency, `SnapshotMissing`/`NotFound`/`AlreadyExists` error classes,
//! the 4,096 ingress-dedup FIFO bound, the outbound-fanout successor guard,
//! and full/group-state/checkpoint snapshot scopes.
//!
//! Multi-row writes run inside one store unit so a mid-method error cannot
//! leave a torn row set — the same guarantee SQLite's savepoint/autocommit
//! grouping gives.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use cgka_traits::app_event::PendingAppMessageAuthority;
use cgka_traits::capabilities::{CapabilityRequirement, Feature};
use cgka_traits::convergence_pass::DurableConvergencePass;
use cgka_traits::engine::GroupEvent;
use cgka_traits::group::{DisbandTombstone, InvitationAdmission};
use cgka_traits::maintenance::{
    DurableGroupEvolution, DurableTransportFanout, GroupMaintenanceState, KeyPackageLifecycleState,
    MaintenanceObligation, PeriodicMaintenancePolicy,
};
use cgka_traits::message::{DeferredMessageMetadata, MessageRecord, MessageState};
use cgka_traits::storage::{
    AccountDeviceHistoryKey, AccountDeviceHistoryKeyStorage, AccountDeviceSignerBinding,
    AccountDeviceSignerStorage, CapabilityStorage, CommittedMembers, CommittedRoster,
    CommittedRosterStorage, CommittedRosterWrite, ConvergencePassStorage, ConvergencePolicyStorage,
    DeferredPeelGeneration, DeferredPeelGenerationStorage, DeviceGroupWelcomeExpectation,
    DeviceGroupWelcomeStorage, DisbandCandidateStorage, DisbandRequestStorage,
    DisbandTombstoneStorage, GroupStateCheckpointRef, GroupStorage, KeyPackageBundleStorage,
    LeaveRequestStorage, MaintenanceStorage, MemberValidationCacheStorage, MessageStorage,
    MlsStorageError, OpenMlsContentFilter, OutboundFanoutStorage, OutboundIntentStorage,
    RosterRevision, StorageError, StorageProvider, StorageResult, StoredKeyPackageBundle,
    TransportGroupRoute, WelcomeStorage,
};
use cgka_traits::transport_adapter::OutboundFanout;
use cgka_traits::types::{Backend, EpochId, GroupId, MemberId, MessageId};
use cgka_traits::welcome::PendingWelcome;
use cgka_traits::{DisbandCandidate, DisbandRequest, Group, LeaveRequest, Timestamp};
use sha2::Digest as _;
use zeroize::Zeroizing;

use crate::error::OpenError;
use crate::keys::{self, Table};
use crate::kv::KvHost;
use crate::ser::{self, Tuple};
use crate::snapshot;
use crate::store::KvStore;

/// `storage-sqlite`'s ingress dedup FIFO bound
/// (`messages.rs`: `INGRESS_DEDUP_MARKER_CAPACITY`). Processed transport ids
/// are deliberately NOT bounded — `messages.rs` has no capacity there.
pub const INGRESS_DEDUP_MARKER_CAPACITY: usize = 4096;

/// `openmls_content` classes, mirroring `messages.rs`.
const OPENMLS_CONTENT_PENDING: i64 = -1;
const OPENMLS_CONTENT_NONE: i64 = 0;
const OPENMLS_CONTENT_COMMIT: i64 = 1;
const OPENMLS_CONTENT_PROPOSAL: i64 = 2;
const OPENMLS_CONTENT_APPLICATION: i64 = 3;
const OPENMLS_CONTENT_UNCLASSIFIED: i64 = 4;

/// The storage provider: one `KvStore` behind `cgka_traits`'s aggregate
/// [`StorageProvider`] plus the `OpenMLS` storage trait (via
/// [`Self::mls_storage`]).
///
/// `backend()` reports [`Backend::Sqlite`] because MDK v0.17.0's `Backend`
/// enum has no generic KV variant — an upstream gap, recorded here rather
/// than papered over with a wrong answer.
///
/// The three optional device-linking stores
/// (`private_key_package_storage`, `roster_tag_key_storage`,
/// `sibling_add_storage`) stay at the trait default `None` until the device
/// linking milestone; the engine degrades gracefully without them.
#[derive(Clone, Debug)]
pub struct KvStorageProvider {
    store: KvStore,
    mls: crate::mls::KvOpenMlsStorage,
    /// Per-store interner for capability descriptions: at most one leaked
    /// allocation per distinct unknown string, bounded by count and length.
    descriptions: Arc<Mutex<HashMap<String, &'static str>>>,
}

impl KvStorageProvider {
    /// Open a provider over `rows` (everything the host has durable), writing
    /// committed units to `host`. See [`crate::kv`] for the host contract.
    ///
    /// # Errors
    ///
    /// [`OpenError`] for schema-version and meta-row failures.
    pub fn open(rows: Vec<(Vec<u8>, Vec<u8>)>, host: Arc<dyn KvHost>) -> Result<Self, OpenError> {
        let store = KvStore::open(rows, host)?;
        Ok(Self {
            mls: crate::mls::KvOpenMlsStorage::new(store.clone()),
            store,
            descriptions: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    /// Shared store handle for the `OpenMLS` side and snapshot module.
    pub(crate) const fn store(&self) -> &KvStore {
        &self.store
    }

    /// The last committed batch sequence, for the session durability gate a
    /// later milestone adds.
    pub(crate) fn last_seq(&self) -> Option<crate::kv::CommitSeq> {
        self.store.last_seq()
    }

    /// Resolve a persisted capability description to a `&'static str` without
    /// leaking a fresh allocation per read:
    ///
    /// - a string equal to a known production description resolves to that
    ///   static (pointer-equal, no allocation);
    /// - any other string is interned once per store instance
    ///   (`Box::leak`), deduped by value — the only permitted `Box::leak` in
    ///   the crate, bounded by [`INTERN_CAP`] distinct strings of at most
    ///   [`INTERN_LEN_CAP`] bytes; beyond that the row is reported corrupt.
    ///
    /// `CapabilityRequirement::description` is `&'static str` because registry
    /// constants are static; rows carry owned strings, so reconstruction needs
    /// this intern.
    fn intern_description(&self, description: String) -> StorageResult<&'static str> {
        for known in KNOWN_DESCRIPTIONS {
            if description == *known {
                return Ok(known);
            }
        }
        let mut interner = self
            .descriptions
            .lock()
            .map_err(|_| StorageError::Backend("kv description interner poisoned".into()))?;
        if let Some(interned) = interner.get(description.as_str()) {
            return Ok(interned);
        }
        if interner.len() >= INTERN_CAP || description.len() > INTERN_LEN_CAP {
            return Err(StorageError::Serialization(
                "capability description out of bounds".into(),
            ));
        }
        let interned: &'static str = Box::leak(description.clone().into_boxed_str());
        interner.insert(description, interned);
        drop(interner);
        Ok(interned)
    }

    /// Distinct unknown descriptions interned so far — the `testing`
    /// surface uses it to prove known descriptions never intern.
    #[cfg(feature = "testing")]
    pub(crate) fn interned_count(&self) -> usize {
        self.descriptions
            .lock()
            .map_or(0, |interner| interner.len())
    }
}

/// The only permitted `Box::leak` target bounds.
const INTERN_CAP: usize = 128;
const INTERN_LEN_CAP: usize = 1024;

/// Descriptions this crate registers through `register_feature`:
/// `cgka-engine` v0.17.0's own `FeatureRegistry::new()` is empty — every
/// registration arrives from the app layer — and this crate registers
/// `SelfRemove` only. Resolution is by content equality; the static is
/// returned so a known string costs no allocation. Unknown strings intern
/// instead.
const KNOWN_DESCRIPTIONS: &[&str] = &["MIP-03 SelfRemove group departure"];

pub(crate) fn gid_seg(id: &GroupId) -> &[u8] {
    id.as_slice()
}

fn mid_seg(id: &MessageId) -> &[u8] {
    id.as_slice()
}

/// The message-state index fragment: the serde of the variant name is
/// deterministic and survives new variants.
fn state_frag(state: MessageState) -> StorageResult<Vec<u8>> {
    ser::ser(&state)
}

fn malformed_row() -> StorageError {
    StorageError::Backend("kv row key malformed".into())
}

fn missing_row() -> StorageError {
    StorageError::Backend("kv index points at a missing row".into())
}

impl KvStorageProvider {
    pub(crate) fn row(&self, table: Table, segments: &[&[u8]]) -> StorageResult<Option<Vec<u8>>> {
        self.store.get(&keys::key(table, segments))
    }

    /// `PRAGMA foreign_keys = ON` in sqlite: a write to a group-scoped table
    /// whose `group_id` row is missing fails the statement. Every `Table`
    /// mapped to a `REFERENCES cgka_groups` column gates on this.
    fn require_group(&self, group_id: &GroupId) -> StorageResult<()> {
        if self.row(Table::Group, &[gid_seg(group_id)])?.is_some() {
            Ok(())
        } else {
            Err(StorageError::Backend(
                "FOREIGN KEY constraint failed".into(),
            ))
        }
    }

    fn put_row(&self, table: Table, segments: &[&[u8]], value: Vec<u8>) -> StorageResult<()> {
        self.store.put(keys::key(table, segments), value)
    }

    fn del_row(&self, table: Table, segments: &[&[u8]]) -> StorageResult<()> {
        self.store.delete(&keys::key(table, segments))
    }

    pub(crate) fn scan_rows(
        &self,
        table: Table,
        segments: &[&[u8]],
    ) -> StorageResult<Vec<(Vec<u8>, Vec<u8>)>> {
        self.store.scan(&keys::key(table, segments))
    }

    /// Prefix delete: sqlite's `DELETE WHERE` fails under `query_only`
    /// even when nothing matches, so declare write intent first.
    fn del_prefix(&self, table: Table, segments: &[&[u8]]) -> StorageResult<()> {
        self.store.ensure_writable()?;
        for row_key in self
            .store
            .keys_in(keys::prefix_range(&keys::key(table, segments)))?
        {
            self.store.delete(&row_key)?;
        }
        Ok(())
    }

    fn de_row<T: serde::de::DeserializeOwned>(
        &self,
        table: Table,
        segments: &[&[u8]],
    ) -> StorageResult<Option<T>> {
        self.row(table, segments)?.map(|v| ser::de(&v)).transpose()
    }

    fn put_ser<T: serde::Serialize>(
        &self,
        table: Table,
        segments: &[&[u8]],
        value: &T,
    ) -> StorageResult<()> {
        self.put_row(table, segments, ser::ser(value)?)
    }

    /// The `(insert_order, record)` codec for every ordered table whose
    /// upsert keeps its position (`ON CONFLICT DO UPDATE` keeps
    /// `insert_order`).
    pub(crate) fn read_ordered<T: serde::de::DeserializeOwned>(
        value: &[u8],
    ) -> StorageResult<(u64, T)> {
        let mut tuple = Tuple::new(value);
        let order = tuple.u64()?;
        let record: T = ser::de(tuple.field()?)?;
        tuple.finish()?;
        Ok((order, record))
    }

    pub(crate) fn read_order(value: &[u8]) -> StorageResult<u64> {
        Tuple::new(value).u64()
    }

    pub(crate) fn write_ordered<T: serde::Serialize>(
        order: u64,
        record: &T,
    ) -> StorageResult<Vec<u8>> {
        let mut out = Vec::new();
        ser::put_u64_field(&mut out, order);
        ser::put_field(&mut out, &ser::ser(record)?);
        Ok(out)
    }

    /// Ordered-table upsert: an existing row keeps its position; a new row
    /// gets the next durable sequence.
    fn put_ordered<T: serde::Serialize>(
        &self,
        table: Table,
        segments: &[&[u8]],
        record: &T,
    ) -> StorageResult<()> {
        self.store.transact(|| -> StorageResult<()> {
            let order = match self.row(table, segments)? {
                Some(existing) => Self::read_order(&existing)?,
                None => self.store.next_sequence()?,
            };
            self.put_row(table, segments, Self::write_ordered(order, record)?)
        })
    }

    /// Ordered-table upsert that always takes a fresh position —
    /// `INSERT OR REPLACE`'s delete+insert moves the row to the end
    /// (welcomes, disband candidates are `ORDER BY rowid` tables).
    fn put_ordered_fresh<T: serde::Serialize>(
        &self,
        table: Table,
        segments: &[&[u8]],
        record: &T,
    ) -> StorageResult<()> {
        self.store.transact(|| -> StorageResult<()> {
            let order = self.store.next_sequence()?;
            self.put_row(table, segments, Self::write_ordered(order, record)?)
        })
    }

    fn list_ordered<T: serde::de::DeserializeOwned>(
        &self,
        table: Table,
        segments: &[&[u8]],
    ) -> StorageResult<Vec<T>> {
        let mut rows: Vec<(u64, T)> = self
            .scan_rows(table, segments)?
            .iter()
            .map(|(_, v)| Self::read_ordered(v))
            .collect::<StorageResult<_>>()?;
        rows.sort_by_key(|(order, _)| *order);
        Ok(rows.into_iter().map(|(_, record)| record).collect())
    }

    pub(crate) fn message_row_key(&self, id: &MessageId) -> StorageResult<Option<Vec<u8>>> {
        self.row(Table::MessageIndex, &[mid_seg(id)])
    }

    pub(crate) fn message_record(&self, id: &MessageId) -> StorageResult<Option<MessageRecord>> {
        match self.message_row_key(id)? {
            Some(row_key) => self
                .store
                .get(&row_key)?
                .map_or(Ok(None), |value| ser::de(&value).map(Some)),
            None => Ok(None),
        }
    }

    /// Insert `record`, keeping an existing row's position when `id` repeats —
    /// `INSERT .. ON CONFLICT(id) DO UPDATE` keeps `insert_order`.
    fn put_message_record(&self, record: &MessageRecord) -> StorageResult<()> {
        self.require_group(&record.group_id)?;
        let mid = mid_seg(&record.id);
        if let Some(row_key) = self.message_row_key(&record.id)? {
            let old: MessageRecord = ser::de(&self.store.get(&row_key)?.ok_or_else(missing_row)?)?;
            let old_state = state_frag(old.state)?;
            let seq = keys::segments(&row_key)
                .ok_or_else(malformed_row)?
                .get(1)
                .copied()
                .ok_or_else(malformed_row)?;
            self.store.delete(&row_key)?;
            self.del_row(
                Table::MessageState,
                &[gid_seg(&old.group_id), &old_state, mid],
            )?;
            let new_row_key = keys::key(Table::Message, &[gid_seg(&record.group_id), seq, mid]);
            self.store.put(new_row_key.clone(), ser::ser(record)?)?;
            self.store
                .put(keys::key(Table::MessageIndex, &[mid]), new_row_key.clone())?;
            let state = state_frag(record.state)?;
            self.put_row(
                Table::MessageState,
                &[gid_seg(&record.group_id), &state, mid],
                new_row_key,
            )
        } else {
            let seq = self.store.next_sequence()?;
            let row_key = keys::key(
                Table::Message,
                &[gid_seg(&record.group_id), &keys::u64_seg(seq), mid],
            );
            self.store.put(row_key.clone(), ser::ser(record)?)?;
            self.store
                .put(keys::key(Table::MessageIndex, &[mid]), row_key.clone())?;
            let state = state_frag(record.state)?;
            self.put_row(
                Table::MessageState,
                &[gid_seg(&record.group_id), &state, mid],
                row_key,
            )
        }
    }

    /// `put_message_on_connection` at an explicit position, for snapshot
    /// restore.
    pub(crate) fn put_message_at(&self, seq: u64, record: &MessageRecord) -> StorageResult<()> {
        let mid = mid_seg(&record.id);
        let row_key = keys::key(
            Table::Message,
            &[gid_seg(&record.group_id), &keys::u64_seg(seq), mid],
        );
        self.store.put(row_key.clone(), ser::ser(record)?)?;
        self.store
            .put(keys::key(Table::MessageIndex, &[mid]), row_key.clone())?;
        let state = state_frag(record.state)?;
        self.put_row(
            Table::MessageState,
            &[gid_seg(&record.group_id), &state, mid],
            row_key,
        )
    }

    /// `delete_message_on_connection` minus its pending-application
    /// deletes: sqlite's group-delete removes those `WHERE group_id = ?`
    /// only (groups.rs:217-222), while `delete_message` removes them by
    /// `message_id` (messages.rs:1325-1336) — the `delete_message` caller
    /// runs those deletes itself before calling this. A pending event
    /// whose source message leaves its group outlives the message, as
    /// sqlite's group-scoped rows do.
    pub(crate) fn delete_message_row(&self, record: &MessageRecord) -> StorageResult<()> {
        let mid = mid_seg(&record.id);
        if let Some(row_key) = self.message_row_key(&record.id)? {
            self.store.delete(&row_key)?;
        }
        self.del_row(Table::MessageIndex, &[mid])?;
        let state = state_frag(record.state)?;
        self.del_row(
            Table::MessageState,
            &[gid_seg(&record.group_id), &state, mid],
        )
    }

    pub(crate) fn ordered_messages(
        &self,
        group_id: &GroupId,
    ) -> StorageResult<Vec<(u64, MessageRecord)>> {
        let mut out = Vec::new();
        for (row_key, value) in self.scan_rows(Table::Message, &[gid_seg(group_id)])? {
            let seq_bytes = keys::segments(&row_key)
                .ok_or_else(malformed_row)?
                .get(1)
                .copied()
                .ok_or_else(malformed_row)?;
            let seq = u64::from_be_bytes(seq_bytes.try_into().map_err(|_| malformed_row())?);
            out.push((seq, ser::de::<MessageRecord>(&value)?));
        }
        out.sort_by_key(|(seq, _)| *seq);
        Ok(out)
    }

    /// `(openmls_content, openmls_epoch)` derived from a stored payload —
    /// `openmls_content_columns` (`messages.rs:1060`): parse the
    /// `StoredMessagePayload`, take the `OpenMLS` wire bytes, TLS-deserialize
    /// to a `ProtocolMessage`, and read its content type and epoch. Anything
    /// unparseable is `UNCLASSIFIED`, which every listing returns for the
    /// engine to judge.
    fn openmls_content_columns(payload: &[u8]) -> (i64, Option<u64>) {
        use cgka_traits::message::StoredMessagePayload;
        use openmls::prelude::tls_codec::Deserialize as _;
        let Ok(payload) = StoredMessagePayload::decode(payload) else {
            return (OPENMLS_CONTENT_UNCLASSIFIED, None);
        };
        let Some(message) = payload.as_openmls_wire() else {
            return (OPENMLS_CONTENT_NONE, None);
        };
        let Ok(mls) = openmls::prelude::MlsMessageIn::tls_deserialize_exact(&message.payload)
        else {
            return (OPENMLS_CONTENT_UNCLASSIFIED, None);
        };
        let protocol: openmls::prelude::ProtocolMessage = match mls.extract() {
            openmls::prelude::MlsMessageBodyIn::PrivateMessage(private) => private.into(),
            openmls::prelude::MlsMessageBodyIn::PublicMessage(public) => public.into(),
            _ => return (OPENMLS_CONTENT_UNCLASSIFIED, None),
        };
        let content = match protocol.content_type() {
            openmls::prelude::ContentType::Commit => OPENMLS_CONTENT_COMMIT,
            openmls::prelude::ContentType::Proposal => OPENMLS_CONTENT_PROPOSAL,
            openmls::prelude::ContentType::Application => OPENMLS_CONTENT_APPLICATION,
        };
        (content, Some(protocol.epoch().as_u64()))
    }

    /// Whether `record` matches one arm of `openmls_filtered_message_sql`.
    /// Pending and unclassified rows match every arm, exactly as sqlite's
    /// `openmls_content IN (-1, 4)` clauses.
    fn openmls_filter_match(record: &MessageRecord, filter: &OpenMlsContentFilter) -> bool {
        let (content, epoch) = Self::openmls_content_columns(&record.payload);
        if matches!(
            content,
            OPENMLS_CONTENT_PENDING | OPENMLS_CONTENT_UNCLASSIFIED
        ) {
            return true;
        }
        let epoch = epoch.unwrap_or(0);
        match filter {
            OpenMlsContentFilter::ApplicationsAt(at) => {
                content == OPENMLS_CONTENT_APPLICATION && epoch == at.0
            }
            OpenMlsContentFilter::CommitsAndProposalsBetween { from, through } => {
                matches!(content, OPENMLS_CONTENT_COMMIT | OPENMLS_CONTENT_PROPOSAL)
                    && epoch >= from.0
                    && epoch <= through.0
            }
            OpenMlsContentFilter::Commits => content == OPENMLS_CONTENT_COMMIT,
            OpenMlsContentFilter::WithoutProcessedApplicationsThrough(_)
            | OpenMlsContentFilter::WithoutProcessedApplications => {
                !matches!(content, OPENMLS_CONTENT_APPLICATION)
                    || record.state != MessageState::Processed
            }
        }
    }

    /// Every `OpenMLS` row the group index attributes to `gid`, as
    /// `(storage_key, value)` pairs in storage-key order.
    pub(crate) fn group_openmls_rows(
        &self,
        gid: &GroupId,
    ) -> StorageResult<Vec<(Vec<u8>, Vec<u8>)>> {
        let mut out = Vec::new();
        for (index_key, _) in self.scan_rows(Table::OpenMlsGroupIndex, &[gid_seg(gid)])? {
            let row_key = keys::segments(&index_key)
                .and_then(|segments| segments.last().copied())
                .ok_or_else(malformed_row)?
                .to_vec();
            if let Some(value) = self.store.get(&row_key)? {
                out.push((row_key, value));
            }
        }
        out.sort();
        Ok(out)
    }

    /// Delete every group-scoped `OpenMLS` row and its index entries.
    pub(crate) fn delete_group_openmls_rows(&self, gid: &GroupId) -> StorageResult<()> {
        self.store.ensure_writable()?;
        for (index_key, _) in self.scan_rows(Table::OpenMlsGroupIndex, &[gid_seg(gid)])? {
            let row_key = keys::segments(&index_key)
                .and_then(|segments| segments.last().copied())
                .ok_or_else(malformed_row)?
                .to_vec();
            self.store.delete(&row_key)?;
            self.store.delete(&index_key)?;
        }
        Ok(())
    }

    /// Restore `rows` as the group's complete `OpenMLS` state.
    pub(crate) fn restore_group_openmls_rows(
        &self,
        gid: &GroupId,
        rows: &[(Vec<u8>, Vec<u8>)],
    ) -> StorageResult<()> {
        self.delete_group_openmls_rows(gid)?;
        for (row_key, value) in rows {
            self.store.put(row_key.clone(), value.clone())?;
            self.put_row(
                Table::OpenMlsGroupIndex,
                &[gid_seg(gid), row_key],
                Vec::new(),
            )?;
        }
        Ok(())
    }

    /// The canonical group-state image (group record + validation marker +
    /// group-scoped `OpenMLS` rows), shared by snapshots, checkpoints, and the
    /// replay fingerprint.
    pub(crate) fn capture_group_state(
        &self,
        gid: &GroupId,
    ) -> StorageResult<snapshot::GroupStateImage> {
        let group = self
            .row(Table::Group, &[gid_seg(gid)])?
            .ok_or(StorageError::NotFound)?;
        Ok(snapshot::GroupStateImage {
            group,
            validated_tree_marker: self.row(Table::MemberValidation, &[gid_seg(gid)])?,
            openmls: self.group_openmls_rows(gid)?,
        })
    }
}

// GroupStorage ----------------------------------------------------------

impl GroupStorage for KvStorageProvider {
    fn invitation_admission(&self, id: &GroupId) -> StorageResult<Option<InvitationAdmission>> {
        self.row(Table::InvitationAdmission, &[gid_seg(id)])?
            .map(|value| {
                let mut tuple = Tuple::new(&value);
                let generation: [u8; 32] =
                    tuple.field()?.try_into().map_err(|_| malformed_row())?;
                let created_locally = tuple.u8()? != 0;
                let via_welcome =
                    ser::take_opt_field(&mut tuple)?.map(|bytes| MessageId::new(bytes.to_vec()));
                tuple.finish()?;
                Ok(InvitationAdmission {
                    generation,
                    created_locally,
                    via_welcome,
                })
            })
            .transpose()
    }

    fn put_invitation_admission(
        &self,
        id: &GroupId,
        admission: Option<&InvitationAdmission>,
    ) -> StorageResult<()> {
        self.store.ensure_writable()?;
        admission.map_or_else(
            || self.del_row(Table::InvitationAdmission, &[gid_seg(id)]),
            |admission| {
                self.require_group(id)?;
                let mut out = Vec::new();
                ser::put_field(&mut out, &admission.generation);
                ser::put_u8_field(&mut out, u8::from(admission.created_locally));
                ser::put_opt_field(
                    &mut out,
                    admission.via_welcome.as_ref().map(MessageId::as_slice),
                );
                self.put_row(Table::InvitationAdmission, &[gid_seg(id)], out)
            },
        )
    }

    /// Joins a caller's `with_transaction` rather than beginning its own
    /// (sqlite `connection.with_transaction` passthrough): the record write
    /// and the committed-roster write are one unit, so a failure leaves
    /// neither.
    fn put_group_with_roster(
        &self,
        group: &Group,
        committed: &CommittedMembers,
    ) -> StorageResult<()> {
        self.store.transact(|| -> StorageResult<()> {
            self.put_group_record(group)?;
            self.write_committed_roster(&group.id, committed)?;
            Ok(())
        })
    }

    fn put_group_keeping_roster(&self, group: &Group) -> StorageResult<()> {
        self.put_group_record(group)
    }

    fn get_group(&self, id: &GroupId) -> StorageResult<Group> {
        self.de_row(Table::Group, &[gid_seg(id)])?
            .ok_or(StorageError::NotFound)
    }

    /// One unit: the group row plus every table that references it — the
    /// `delete_group_tx` cascade list — minus `cgka_welcomes`,
    /// `cgka_disband_tombstones`, `cgka_ingress_dedup`,
    /// `processed_transport_ids`, and `cgka_member_capabilities`, which
    /// sqlite deliberately keeps (the tombstone is the anti-resurrection
    /// guard; welcomes stay for late delivery). Snapshots and checkpoints
    /// for the group are dropped by cascade.
    ///
    /// `delete_group_tx` deletes every group-scoped row *before* the missing
    /// group reports `NotFound` (`groups.rs:202-231`): calling `delete_group`
    /// on an absent group still wipes that id's `OpenMLS` rows, pending
    /// application rows and route state.
    fn delete_group(&self, id: &GroupId) -> StorageResult<()> {
        self.store.transact(|| -> StorageResult<()> {
            self.store.ensure_writable()?;
            let existed = self.row(Table::Group, &[gid_seg(id)])?.is_some();
            self.delete_group_tx(id)?;
            if existed {
                Ok(())
            } else {
                Err(StorageError::NotFound)
            }
        })
    }

    /// `cross::wipe` opens its own `BEGIN IMMEDIATE` unconditionally (no
    /// `is_current_thread_transaction_owner` join like the other write paths),
    /// so sqlite fails it inside any open unit.
    fn forget_group_local(&self, id: &GroupId, at: Timestamp) -> StorageResult<bool> {
        self.store.ensure_writable()?;
        if self.store.in_unit() {
            return Err(StorageError::Backend(
                "kv cannot run forget_group_local inside a transaction".into(),
            ));
        }
        self.store.transact(|| -> StorageResult<bool> {
            self.store.ensure_writable()?;
            let already_forgotten = self.is_group_forgotten(id)?;
            // `ON CONFLICT DO UPDATE SET forgotten_at = MAX(old, new)` and
            // `awaiting_welcome = 1 WHERE awaiting_welcome = 0` — the net
            // durable state after forget is always (max cutoff, awaiting).
            let forgotten_at = match self.row(Table::ForgottenGroup, &[gid_seg(id)])? {
                Some(value) => Tuple::new(&value).u64()?.max(at.0),
                None => at.0,
            };
            let mut out = Vec::new();
            ser::put_u64_field(&mut out, forgotten_at);
            ser::put_u8_field(&mut out, 1);
            self.put_row(Table::ForgottenGroup, &[gid_seg(id)], out)?;
            // Same wipe as delete_group_tx, plus the two tables sqlite clears
            // only on forget (welcomes, disband tombstones).
            self.delete_group_tx(id)?;
            self.delete_group_welcomes(id)?;
            self.del_row(Table::DisbandTombstone, &[gid_seg(id)])?;
            Ok(!already_forgotten)
        })
    }

    fn is_group_forgotten(&self, id: &GroupId) -> StorageResult<bool> {
        match self.row(Table::ForgottenGroup, &[gid_seg(id)])? {
            Some(value) => {
                let mut tuple = Tuple::new(&value);
                let _cutoff = tuple.u64()?;
                let awaiting = tuple.u8()?;
                tuple.finish()?;
                Ok(awaiting != 0)
            }
            None => Ok(false),
        }
    }

    fn group_local_reset_cutoff(&self, id: &GroupId) -> StorageResult<Option<Timestamp>> {
        self.row(Table::ForgottenGroup, &[gid_seg(id)])?
            .map(|value| Tuple::new(&value).u64().map(Timestamp))
            .transpose()
    }

    /// `UPDATE .. SET awaiting_welcome = 0`: the cutoff row stays; a missing
    /// row is a no-op success.
    fn complete_group_local_reset(&self, id: &GroupId) -> StorageResult<()> {
        self.store.ensure_writable()?;
        match self.row(Table::ForgottenGroup, &[gid_seg(id)])? {
            Some(value) => {
                let cutoff = Tuple::new(&value).u64()?;
                let mut out = Vec::new();
                ser::put_u64_field(&mut out, cutoff);
                ser::put_u8_field(&mut out, 0);
                self.put_row(Table::ForgottenGroup, &[gid_seg(id)], out)
            }
            None => Ok(()),
        }
    }

    fn list_groups(&self) -> StorageResult<Vec<GroupId>> {
        // `SELECT id FROM cgka_groups ORDER BY id` — byte order, which the
        // row-key scan already yields.
        self.scan_rows(Table::Group, &[])?
            .iter()
            .map(|(row_key, _)| {
                keys::segments(row_key)
                    .and_then(|segments| segments.last().copied())
                    .map(|seg| GroupId::new(seg.to_vec()))
                    .ok_or_else(malformed_row)
            })
            .collect()
    }

    fn list_group_records(&self) -> StorageResult<Vec<Group>> {
        self.scan_rows(Table::Group, &[])?
            .iter()
            .map(|(_, value)| ser::de(value))
            .collect()
    }

    fn put_transport_group_route(
        &self,
        transport_group_id: &[u8],
        group_id: &GroupId,
        source_epoch: EpochId,
    ) -> StorageResult<()> {
        self.store.transact(|| -> StorageResult<()> {
            self.store.ensure_writable()?;
            self.require_group(group_id)?;
            // `ON CONFLICT DO UPDATE` keeps the rowid: first-insertion order.
            let order = match self.row(Table::TransportRoute, &[transport_group_id])? {
                Some(existing) => Tuple::new(&existing).u64()?,
                None => self.store.next_sequence()?,
            };
            let mut out = Vec::new();
            ser::put_u64_field(&mut out, order);
            ser::put_field(&mut out, gid_seg(group_id));
            ser::put_u64_field(&mut out, source_epoch.0);
            self.put_row(Table::TransportRoute, &[transport_group_id], out)
        })
    }

    fn list_transport_group_routes(&self) -> StorageResult<Vec<TransportGroupRoute>> {
        let mut routes = Vec::new();
        for (row_key, value) in self.scan_rows(Table::TransportRoute, &[])? {
            let mut tuple = Tuple::new(&value);
            let order = tuple.u64()?;
            let group = tuple.field()?.to_vec();
            let epoch = tuple.u64()?;
            tuple.finish()?;
            let transport_group_id = keys::segments(&row_key)
                .and_then(|segments| segments.last().copied())
                .ok_or_else(malformed_row)?
                .to_vec();
            routes.push((
                order,
                TransportGroupRoute {
                    transport_group_id,
                    group_id: GroupId::new(group),
                    source_epoch: EpochId(epoch),
                },
            ));
        }
        routes.sort_by_key(|(order, _)| *order);
        Ok(routes.into_iter().map(|(_, route)| route).collect())
    }

    fn delete_transport_group_route(&self, transport_group_id: &[u8]) -> StorageResult<()> {
        self.del_row(Table::TransportRoute, &[transport_group_id])
    }

    fn delete_transport_group_routes_below_epoch(
        &self,
        group_id: &GroupId,
        cutoff: EpochId,
    ) -> StorageResult<()> {
        self.store
            .transact(|| self.delete_transport_routes(group_id, Some(cutoff.0)))
    }

    fn delete_transport_group_routes_for_group(&self, group_id: &GroupId) -> StorageResult<()> {
        self.store
            .transact(|| self.delete_transport_routes(group_id, None))
    }
}

// CommittedRosterStorage -----------------------------------------------------

/// The `cgka_committed_rosters` + `cgka_committed_roster_members` pair in one
/// row value: revision and normalized member list together, so a read can
/// never pair one roster's revision with another's members (the guarantee
/// sqlite's `committed_roster` takes a `with_deferred_read` snapshot for).
/// The member list is `CommittedMembers`-normalized (distinct, byte order)
/// at write, matching `members_on`'s `BTreeSet`.
#[derive(serde::Serialize, serde::Deserialize)]
struct RosterRow {
    revision: u64,
    members: Vec<MemberId>,
}

/// The `cgka_committed_roster_revision` counter row's single segment.
const ROSTER_REVISION_ROW: &[u8] = b"last";

impl CommittedRosterStorage for KvStorageProvider {
    fn committed_roster(&self, group_id: &GroupId) -> StorageResult<Option<CommittedRoster>> {
        Ok(self
            .de_row::<RosterRow>(Table::CommittedRoster, &[gid_seg(group_id)])?
            .map(|row| CommittedRoster {
                revision: RosterRevision(row.revision),
                members: CommittedMembers::from_ids(row.members),
            }))
    }

    fn committed_roster_revision(
        &self,
        group_id: &GroupId,
    ) -> StorageResult<Option<RosterRevision>> {
        Ok(self
            .de_row::<RosterRow>(Table::CommittedRoster, &[gid_seg(group_id)])?
            .map(|row| RosterRevision(row.revision)))
    }

    /// Its own savepoint, like sqlite's `with_savepoint`: inside a caller's
    /// transaction a refused write undoes its own rows — and the revision it
    /// consumed — while the caller can still commit.
    fn put_committed_roster(
        &self,
        group_id: &GroupId,
        members: &CommittedMembers,
    ) -> StorageResult<CommittedRosterWrite> {
        self.store
            .savepoint(|| self.write_committed_roster(group_id, members))
    }
}

impl KvStorageProvider {
    /// The `reject_forgotten_group_insert` trigger (migrations
    /// `0070`/`0071`) aborts any `cgka_groups` insert while the group is
    /// forgotten and awaiting a fresh Welcome — sqlite's `put_group_record`.
    fn put_group_record(&self, group: &Group) -> StorageResult<()> {
        self.store.ensure_writable()?;
        if self.is_group_forgotten(&group.id)? {
            return Err(StorageError::Backend(
                "kv group awaiting fresh welcome".into(),
            ));
        }
        self.put_ser(Table::Group, &[gid_seg(&group.id)], group)
    }

    /// `UPDATE cgka_committed_roster_revision SET last = last + 1 RETURNING
    /// last`: the account-wide counter every revision is drawn from — a
    /// missing row is the seeded `last = 0`; it is never reset or deleted.
    fn next_roster_revision(&self) -> StorageResult<RosterRevision> {
        let last = self
            .row(Table::CommittedRosterRevision, &[ROSTER_REVISION_ROW])?
            .map_or(Ok(0), |value| {
                value
                    .as_slice()
                    .try_into()
                    .map(u64::from_be_bytes)
                    .map_err(|_| malformed_row())
            })?;
        let next = last
            .checked_add(1)
            .ok_or_else(|| StorageError::Backend("kv committed-roster revision overflow".into()))?;
        self.put_row(
            Table::CommittedRosterRevision,
            &[ROSTER_REVISION_ROW],
            next.to_be_bytes().to_vec(),
        )?;
        Ok(RosterRevision(next))
    }

    /// `write_committed_roster` (`committed_rosters.rs`), in the caller's unit:
    /// writes nothing when the stored members already equal `members`;
    /// otherwise takes the next account-wide revision and replaces the row.
    /// The `INSERT INTO cgka_committed_rosters` foreign key makes a missing
    /// group record refuse the write — after the counter move, exactly as
    /// sqlite (a savepoint undoes both).
    fn write_committed_roster(
        &self,
        group_id: &GroupId,
        members: &CommittedMembers,
    ) -> StorageResult<CommittedRosterWrite> {
        let previous: Option<RosterRow> =
            self.de_row(Table::CommittedRoster, &[gid_seg(group_id)])?;
        if let Some(previous) = &previous
            && previous.members.as_slice() == members.ids()
        {
            return Ok(CommittedRosterWrite::Unchanged(RosterRevision(
                previous.revision,
            )));
        }
        let revision = self.next_roster_revision()?;
        self.require_group(group_id)?;
        self.put_ser(
            Table::CommittedRoster,
            &[gid_seg(group_id)],
            &RosterRow {
                revision: revision.0,
                members: members.ids().to_vec(),
            },
        )?;
        Ok(match previous {
            Some(_) => CommittedRosterWrite::Replaced(revision),
            None => CommittedRosterWrite::Recorded(revision),
        })
    }
}

impl KvStorageProvider {
    /// Welcome rows carry their group inside the record; scan and drop the
    /// ones matching `id` (forget-only cascade).
    fn delete_group_welcomes(&self, id: &GroupId) -> StorageResult<()> {
        self.store.ensure_writable()?;
        for (row_key, value) in self.scan_rows(Table::Welcome, &[])? {
            let (_, welcome): (_, PendingWelcome) = Self::read_ordered(&value)?;
            if welcome.group_id.as_slice() == gid_seg(id) {
                self.store.delete(&row_key)?;
            }
        }
        Ok(())
    }

    /// Route rows carry `(insert_order, group, epoch)` tuple values; `cutoff`
    /// limits the delete to epochs below it.
    fn delete_transport_routes(
        &self,
        group_id: &GroupId,
        cutoff: Option<u64>,
    ) -> StorageResult<()> {
        self.store.ensure_writable()?;
        for (row_key, value) in self.scan_rows(Table::TransportRoute, &[])? {
            let mut tuple = Tuple::new(&value);
            let _order = tuple.u64()?;
            let group = tuple.field()?;
            let epoch = tuple.u64()?;
            tuple.finish()?;
            if group == gid_seg(group_id) && cutoff.is_none_or(|limit| epoch < limit) {
                self.store.delete(&row_key)?;
            }
        }
        Ok(())
    }

    /// Drop every maintenance sub-row scoped to `gid`.
    fn delete_maintenance_rows(&self, gid: &[u8]) -> StorageResult<()> {
        self.store.ensure_writable()?;
        let mut scoped = Vec::new();
        for sub in [&b"obl"[..], &b"evo"[..], &b"fan"[..]] {
            scoped.extend(
                self.scan_rows(Table::Maintenance, &[sub])?
                    .into_iter()
                    .map(|row| (sub, row)),
            );
        }
        for (sub, (row_key, value)) in scoped {
            if Self::maintenance_row_group(sub, &value)?.as_deref() == Some(gid) {
                self.store.delete(&row_key)?;
            }
        }
        Ok(())
    }

    /// Decode the group a maintenance sub-row belongs to; `fan` rows may be
    /// unscoped (`group_id` optional).
    fn maintenance_row_group(sub: &[u8], value: &[u8]) -> StorageResult<Option<Vec<u8>>> {
        match sub {
            b"obl" => {
                let (_, record): (_, MaintenanceObligation) = Self::read_ordered(value)?;
                Ok(Some(record.group_id.as_slice().to_vec()))
            }
            b"evo" => {
                let (_, record): (_, DurableGroupEvolution) = Self::read_ordered(value)?;
                Ok(Some(record.group_id.as_slice().to_vec()))
            }
            _ => {
                let (_, record): (_, DurableTransportFanout) = Self::read_ordered(value)?;
                Ok(record.group_id.as_ref().map(|g| g.as_slice().to_vec()))
            }
        }
    }

    /// Reports without sender authority need a retained authority request;
    /// everything else clears the pending slot (INSERT OR IGNORE parity).
    fn update_pending_authority(&self, event: &GroupEvent) -> StorageResult<()> {
        self.store.ensure_writable()?;
        let GroupEvent::MessageReceived {
            group_id,
            message_id,
            epoch,
            sender,
            payload,
            authority,
            retention,
            ..
        } = event
        else {
            return Ok(());
        };
        let mid = mid_seg(message_id);
        let needs = authority.is_none()
            && cgka_traits::MarmotAppEvent::decode(payload)
                .is_ok_and(|app| cgka_traits::reporting::requires_source_authority(app.kind));
        if !needs {
            return self.del_row(Table::PendingAppAuthority, &[mid]);
        }
        if self
            .store
            .contains(&keys::key(Table::PendingAppAuthority, &[mid]))?
        {
            return Ok(());
        }
        let request = PendingAppMessageAuthority {
            group_id: group_id.clone(),
            message_id: message_id.clone(),
            epoch: *epoch,
            sender: sender.clone(),
            payload_digest: sha2::Sha256::digest(payload).into(),
            retention: *retention,
        };
        self.put_ser(Table::PendingAppAuthority, &[mid], &request)
    }

    /// `Ok(true)` when an existing row consumed the put: identical content is
    /// a no-op, an authority upgrade rewrites in place, anything else is the
    /// sqlite "reused with different content" rejection.
    fn merge_pending_app_event(
        &self,
        mid: &[u8],
        record: &[u8],
        event: &GroupEvent,
    ) -> StorageResult<bool> {
        self.store.ensure_writable()?;
        let Some(existing) = self.row(Table::PendingAppEvent, &[mid])? else {
            return Ok(false);
        };
        // The row is the tuple `[group segment, order, record]` — decode it
        // before comparing; sqlite's `record` column is the bare value.
        let mut tuple = Tuple::new(&existing);
        let group = tuple.field()?.to_vec();
        let order = tuple.u64()?;
        let existing_record = tuple.field()?.to_vec();
        tuple.finish()?;
        if existing_record == record {
            return Ok(true);
        }
        if pending_authority_upgrade(&existing_record, event)? {
            // The authority/retention upgrade rewrites the record but keeps
            // its position and the group check is already proven.
            let upgraded = ser::ser(&merged_pending_event(&existing_record, event)?)?;
            let mut out = Vec::new();
            ser::put_field(&mut out, &group);
            ser::put_u64_field(&mut out, order);
            ser::put_field(&mut out, &upgraded);
            self.put_row(Table::PendingAppEvent, &[mid], out)?;
            return Ok(true);
        }
        Err(StorageError::Backend(
            "pending application event id reused with different content".to_owned(),
        ))
    }

    /// Keep only the newest `INGRESS_DEDUP_MARKER_CAPACITY` markers (FIFO
    /// eviction by the stored sequence value).
    fn prune_ingress_dedup(&self) -> StorageResult<()> {
        self.store.ensure_writable()?;
        let mut rows = self.scan_rows(Table::IngressDedup, &[])?;
        if rows.len() <= INGRESS_DEDUP_MARKER_CAPACITY {
            return Ok(());
        }
        rows.sort_by_key(|(_, v)| u64::from_be_bytes(v.as_slice().try_into().unwrap_or([0; 8])));
        for (row_key, _) in rows.iter().take(rows.len() - INGRESS_DEDUP_MARKER_CAPACITY) {
            self.store.delete(row_key)?;
        }
        Ok(())
    }

    /// Existing fanout rows keep position and must be a valid successor;
    /// new rows take the next insert order.
    fn fanout_order(&self, mid: &[u8], fanout: &OutboundFanout) -> StorageResult<u64> {
        self.store.ensure_writable()?;
        match self.row(Table::OutboundFanout, &[mid])? {
            Some(existing) => {
                let (order, previous) = Self::read_ordered::<OutboundFanout>(&existing)?;
                fanout.validate_successor_of(&previous).map_err(|_| {
                    StorageError::Backend(
                        "outbound fanout update would alter or regress durable state".into(),
                    )
                })?;
                Ok(order)
            }
            None => self.store.next_sequence(),
        }
    }

    pub(crate) fn delete_group_tx(&self, id: &GroupId) -> StorageResult<()> {
        let gid = gid_seg(id);
        self.del_row(Table::Group, &[gid])?;
        // `cgka_committed_rosters` cascades with the group record; the
        // account-wide revision counter is never deleted.
        self.del_row(Table::CommittedRoster, &[gid])?;
        self.del_row(Table::InvitationAdmission, &[gid])?;
        self.del_row(Table::ConvergencePolicy, &[gid])?;
        self.del_row(Table::MemberValidation, &[gid])?;
        self.del_row(Table::ConvergencePass, &[gid])?;
        self.del_row(Table::DeferredPeelGeneration, &[gid])?;
        self.del_row(Table::LeaveRequest, &[gid])?;
        self.del_row(Table::DisbandRequest, &[gid])?;
        self.del_prefix(Table::DisbandCandidate, &[gid])?;

        self.del_row(Table::Maintenance, &[&b"gm"[..], gid])?;
        // Id-keyed ordered tables: scan and match the record's group, like
        // the `FOREIGN KEY(group_id)` rows.
        for (row_key, value) in self.scan_rows(Table::QueuedIntent, &[])? {
            let (_, record): (_, cgka_traits::storage::QueuedOutboundIntent) =
                Self::read_ordered(&value)?;
            if record.group_id.as_slice() == gid {
                self.store.delete(&row_key)?;
            }
        }
        for (row_key, value) in self.scan_rows(Table::OwnCommitIntent, &[])? {
            let (_, record): (_, cgka_traits::storage::OwnCommitIntent) =
                Self::read_ordered(&value)?;
            if record.group_id.as_slice() == gid {
                self.store.delete(&row_key)?;
            }
        }
        for (row_key, value) in self.scan_rows(Table::OutboundFanout, &[])? {
            let (_, record): (_, OutboundFanout) = Self::read_ordered(&value)?;
            if record.group_id().map(GroupId::as_slice) == Some(gid) {
                self.store.delete(&row_key)?;
            }
        }
        for (row_key, value) in self.scan_rows(Table::PendingAppEvent, &[])? {
            let mut tuple = Tuple::new(&value);
            let group = tuple.field()?;
            if group == gid {
                self.store.delete(&row_key)?;
            }
        }
        for (row_key, value) in self.scan_rows(Table::PendingAppAuthority, &[])? {
            let record: PendingAppMessageAuthority = ser::de(&value)?;
            if record.group_id.as_slice() == gid {
                self.store.delete(&row_key)?;
            }
        }
        for (row_key, value) in self.scan_rows(Table::TransportRoute, &[])? {
            let mut tuple = Tuple::new(&value);
            let _order = tuple.u64()?;
            let group = tuple.field()?;
            let _epoch = tuple.u64()?;
            tuple.finish()?;
            if group == gid {
                self.store.delete(&row_key)?;
            }
        }
        // `cgka_processed_transport_ids` has `ON DELETE CASCADE` (migration
        // `0058`); `cgka_ingress_dedup` is group-less and stays.
        for (row_key, value) in self.scan_rows(Table::ProcessedTransportId, &[])? {
            if value.as_slice() == gid {
                self.store.delete(&row_key)?;
            }
        }
        self.delete_maintenance_rows(gid)?;
        // Messages: row set, both indexes, and per-message pending rows.
        for (_, record) in self.ordered_messages(id)? {
            self.delete_message_row(&record)?;
        }
        self.del_prefix(Table::Snapshot, &[gid])?;
        self.del_prefix(Table::Checkpoint, &[gid])?;
        self.del_prefix(Table::MediaSecret, &[gid])?;
        self.delete_group_openmls_rows(id)
    }
}

// MessageStorage --------------------------------------------------------

impl MessageStorage for KvStorageProvider {
    fn put_message(&self, record: &MessageRecord) -> StorageResult<()> {
        self.store.transact(|| self.put_message_record(record))
    }

    fn get_message(&self, id: &MessageId) -> StorageResult<MessageRecord> {
        self.message_record(id)?.ok_or(StorageError::NotFound)
    }

    /// `delete_message_on_connection`: pending rows go even when the message
    /// itself is absent.
    fn delete_message(&self, id: &MessageId) -> StorageResult<()> {
        self.store.ensure_writable()?;
        self.store.transact(|| -> StorageResult<()> {
            let record = self.message_record(id)?;
            self.del_row(Table::PendingAppEvent, &[mid_seg(id)])?;
            self.del_row(Table::PendingAppAuthority, &[mid_seg(id)])?;
            if let Some(record) = record {
                self.delete_message_row(&record)?;
            }
            Ok(())
        })
    }

    /// Standalone-engine semantics: no app consumer owns released messages,
    /// so this is exactly `delete_message` (`messages.rs`:
    /// `app_owns_released_messages` is false without an app projection).
    fn release_message_for_replay(&self, record: &MessageRecord) -> StorageResult<()> {
        self.delete_message(&record.id)
    }

    fn update_message_state(&self, id: &MessageId, new_state: MessageState) -> StorageResult<()> {
        self.store.transact(|| -> StorageResult<()> {
            let row_key = self.message_row_key(id)?.ok_or(StorageError::NotFound)?;
            let value = self.store.get(&row_key)?.ok_or(StorageError::NotFound)?;
            let mut record: MessageRecord = ser::de(&value)?;
            let old_state = state_frag(record.state)?;
            record.state = new_state;
            self.store.put(row_key.clone(), ser::ser(&record)?)?;
            let mid = mid_seg(id);
            self.del_row(
                Table::MessageState,
                &[gid_seg(&record.group_id), &old_state, mid],
            )?;
            let state = state_frag(new_state)?;
            self.put_row(
                Table::MessageState,
                &[gid_seg(&record.group_id), &state, mid],
                row_key,
            )
        })
    }

    fn list_messages(
        &self,
        group_id: &GroupId,
        at_or_after_epoch: EpochId,
    ) -> StorageResult<Vec<MessageRecord>> {
        Ok(self
            .ordered_messages(group_id)?
            .into_iter()
            .filter(|(_, record)| record.epoch >= at_or_after_epoch)
            .map(|(_, record)| record)
            .collect())
    }

    /// `ORDER BY insert_order` over `state IN .. AND epoch >= ?` — the mst
    /// index orders by position within each state; merging by row key (which
    /// embeds the position) gives the global insertion order.
    fn list_messages_in_states(
        &self,
        group_id: &GroupId,
        states: &[MessageState],
        at_or_after_epoch: EpochId,
    ) -> StorageResult<Vec<MessageRecord>> {
        if states.is_empty() {
            return Ok(Vec::new());
        }
        let mut rows = Vec::new();
        for state in states {
            let frag = state_frag(*state)?;
            for (index_key, row_key) in
                self.scan_rows(Table::MessageState, &[gid_seg(group_id), &frag])?
            {
                let _ = index_key;
                rows.push(row_key);
            }
        }
        rows.sort();
        rows.dedup();
        let mut out = Vec::with_capacity(rows.len());
        for row_key in rows {
            let Some(value) = self.store.get(&row_key)? else {
                continue;
            };
            let record: MessageRecord = ser::de(&value)?;
            if record.epoch >= at_or_after_epoch {
                out.push(record);
            }
        }
        Ok(out)
    }

    /// The content-filtered listing: same scan, then the
    /// `openmls_content`/`openmls_epoch` arms of `openmls_filtered_message_sql`
    /// evaluated per row (the classifier is a pure function of the payload,
    /// so a stored column is unnecessary).
    fn list_openmls_messages_in_states(
        &self,
        group_id: &GroupId,
        states: &[MessageState],
        at_or_after_epoch: EpochId,
        filter: OpenMlsContentFilter,
    ) -> StorageResult<Vec<MessageRecord>> {
        if states.is_empty() {
            return Ok(Vec::new());
        }
        Ok(self
            .list_messages_in_states(group_id, states, at_or_after_epoch)?
            .into_iter()
            .filter(|record| Self::openmls_filter_match(record, &filter))
            .collect())
    }

    /// Peel-deferred rows in insertion order — sqlite's SQL fixes
    /// `state = PeelDeferred AND epoch >= 0`.
    fn list_deferred_message_metadata(
        &self,
        group_id: &GroupId,
    ) -> StorageResult<Vec<DeferredMessageMetadata>> {
        Ok(self
            .ordered_messages(group_id)?
            .into_iter()
            .filter(|(_, record)| record.state == MessageState::PeelDeferred)
            .map(|(_, record)| DeferredMessageMetadata::from(record))
            .collect())
    }

    /// `put_pending_application_event`: `MessageReceived` keyed by its source
    /// message, `GroupJoined` by `via_welcome`; anything else is rejected.
    /// The pending row lands only when the source message exists for the
    /// event's group (`INSERT .. SELECT insert_order FROM cgka_messages`);
    /// a conflicting same-id row upgrades in place when it is the same
    /// record with resolved authority.
    fn put_pending_application_event(&self, event: &GroupEvent) -> StorageResult<()> {
        self.store.ensure_writable()?;
        let (group_id, message_id) = match event {
            GroupEvent::MessageReceived {
                group_id,
                message_id,
                ..
            } => (group_id, message_id),
            GroupEvent::GroupJoined {
                group_id,
                via_welcome,
                ..
            } => (group_id, via_welcome),
            _ => {
                return Err(StorageError::Backend(
                    "pending application outbox accepts only MessageReceived and GroupJoined events"
                        .to_owned(),
                ));
            }
        };
        let record = ser::ser(event)?;
        self.store.transact(|| -> StorageResult<()> {
            self.update_pending_authority(event)?;
            let mid = mid_seg(message_id);
            if self.merge_pending_app_event(mid, &record, event)? {
                return Ok(());
            }
            // No existing row: the source message must back the event's group.
            let source = self.message_record(message_id)?;
            match source {
                Some(source) if source.group_id == *group_id => {
                    let seq =
                        keys::segments(&self.message_row_key(message_id)?.ok_or_else(missing_row)?)
                            .ok_or_else(malformed_row)?
                            .get(1)
                            .copied()
                            .ok_or_else(malformed_row)?
                            .to_vec();
                    let mut out = Vec::new();
                    ser::put_field(&mut out, gid_seg(group_id));
                    let order = u64::from_be_bytes(seq.try_into().map_err(|_| malformed_row())?);
                    ser::put_u64_field(&mut out, order);
                    ser::put_field(&mut out, &record);
                    self.put_row(Table::PendingAppEvent, &[mid], out)
                }
                _ => Err(StorageError::NotFound),
            }
        })
    }

    /// `ORDER BY message_insert_order, message_id`.
    fn list_pending_application_events(&self) -> StorageResult<Vec<GroupEvent>> {
        let mut rows = Vec::new();
        for (row_key, value) in self.scan_rows(Table::PendingAppEvent, &[])? {
            let mid = keys::segments(&row_key)
                .and_then(|segments| segments.last().copied())
                .ok_or_else(malformed_row)?
                .to_vec();
            let mut tuple = Tuple::new(&value);
            let _group = tuple.field()?;
            let order = tuple.u64()?;
            let record = tuple.field()?.to_vec();
            tuple.finish()?;
            rows.push((order, mid, record));
        }
        rows.sort_by(|(oa, ma, _), (ob, mb, _)| oa.cmp(ob).then(ma.cmp(mb)));
        rows.iter().map(|(_, _, record)| ser::de(record)).collect()
    }

    /// `ORDER BY message_id` with the `?1 IS NULL OR message_id > ?1`
    /// pagination and `LIMIT min(100, limit)`.
    fn pending_application_authority_batch(
        &self,
        after: Option<&MessageId>,
        limit: usize,
    ) -> StorageResult<Vec<PendingAppMessageAuthority>> {
        let mut out = Vec::new();
        for (row_key, value) in self.scan_rows(Table::PendingAppAuthority, &[])? {
            let mid = keys::segments(&row_key)
                .and_then(|segments| segments.last().copied())
                .ok_or_else(malformed_row)?;
            if after.is_some_and(|after| mid <= mid_seg(after)) {
                continue;
            }
            out.push(ser::de::<PendingAppMessageAuthority>(&value)?);
            if out.len() >= limit.min(100) {
                break;
            }
        }
        Ok(out)
    }

    /// Only the pending-event rows go — authority rows retire through
    /// `delete_message` or a resolved-authority rewrite, as in sqlite.
    fn delete_pending_application_events(&self, ids: &[MessageId]) -> StorageResult<()> {
        self.store.transact(|| -> StorageResult<()> {
            for id in ids {
                self.del_row(Table::PendingAppEvent, &[mid_seg(id)])?;
            }
            Ok(())
        })
    }

    /// `INSERT OR IGNORE` then prune to the 4,096-marker FIFO ring: the
    /// oldest `insert_order`-equivalents go first.
    fn put_ingress_dedup_marker(&self, id: &MessageId) -> StorageResult<()> {
        self.store.transact(|| -> StorageResult<()> {
            let mid = mid_seg(id);
            if !self
                .store
                .contains(&keys::key(Table::IngressDedup, &[mid]))?
            {
                let seq = self.store.next_sequence()?;
                self.put_row(Table::IngressDedup, &[mid], seq.to_be_bytes().to_vec())?;
            }
            self.prune_ingress_dedup()?;
            Ok(())
        })
    }

    fn has_ingress_dedup_marker(&self, id: &MessageId) -> StorageResult<bool> {
        self.store
            .contains(&keys::key(Table::IngressDedup, &[mid_seg(id)]))
    }

    /// `INSERT OR IGNORE` into `cgka_processed_transport_ids`, then the
    /// group-consistency check a second owner of the same id fails.
    fn put_processed_transport_id(
        &self,
        group_id: &GroupId,
        transport_id: &MessageId,
    ) -> StorageResult<()> {
        self.store.transact(|| -> StorageResult<()> {
            self.store.ensure_writable()?;
            self.require_group(group_id)?;
            match self.row(Table::ProcessedTransportId, &[mid_seg(transport_id)])? {
                Some(existing) if existing != gid_seg(group_id) => Err(StorageError::Backend(
                    "processed transport id is already assigned to another group".into(),
                )),
                Some(_) => Ok(()),
                None => self.put_row(
                    Table::ProcessedTransportId,
                    &[mid_seg(transport_id)],
                    gid_seg(group_id).to_vec(),
                ),
            }
        })
    }

    fn has_processed_transport_id(&self, transport_id: &MessageId) -> StorageResult<bool> {
        self.store.contains(&keys::key(
            Table::ProcessedTransportId,
            &[mid_seg(transport_id)],
        ))
    }

    fn create_group_snapshot(&self, group_id: &GroupId, name: &str) -> StorageResult<()> {
        snapshot::create_group_snapshot(self, group_id, name)
    }

    fn create_group_state_snapshot(&self, group_id: &GroupId, name: &str) -> StorageResult<()> {
        snapshot::create_group_state_snapshot(self, group_id, name)
    }

    fn list_group_snapshots(&self, group_id: &GroupId) -> StorageResult<Vec<String>> {
        // sqlite: `ORDER BY name` — byte order of the name, not of the
        // length-prefixed row-key segment.
        let mut names = self
            .scan_rows(Table::Snapshot, &[gid_seg(group_id)])?
            .iter()
            .map(|(row_key, _)| {
                keys::segments(row_key)
                    .and_then(|segments| segments.last().copied())
                    .and_then(|seg| String::from_utf8(seg.to_vec()).ok())
                    .ok_or_else(malformed_row)
            })
            .collect::<StorageResult<Vec<_>>>()?;
        names.sort();
        Ok(names)
    }

    fn group_snapshot_fingerprint(
        &self,
        group_id: &GroupId,
        name: &str,
    ) -> StorageResult<Option<[u8; 32]>> {
        Ok(self
            .row(Table::Snapshot, &[gid_seg(group_id), name.as_bytes()])?
            .map(|blob| sha2::Sha256::digest(&blob).into()))
    }

    fn rollback_group_to_snapshot(&self, group_id: &GroupId, name: &str) -> StorageResult<()> {
        snapshot::rollback(self, group_id, name)
    }

    /// Canonical-state-only restore, even from a full image.
    fn rollback_group_state_to_snapshot(
        &self,
        group_id: &GroupId,
        name: &str,
    ) -> StorageResult<()> {
        snapshot::rollback_group_state(self, group_id, name)
    }

    fn release_group_snapshot(&self, group_id: &GroupId, name: &str) -> StorageResult<()> {
        snapshot::release(self, group_id, name)
    }

    fn create_group_state_checkpoint(
        &self,
        group_id: &GroupId,
        checkpoint: &GroupStateCheckpointRef,
    ) -> StorageResult<()> {
        snapshot::create_checkpoint(self, group_id, checkpoint)
    }

    fn restore_group_state_checkpoint(
        &self,
        group_id: &GroupId,
        checkpoint_id: &str,
    ) -> StorageResult<()> {
        snapshot::restore_checkpoint(self, group_id, checkpoint_id)
    }

    fn list_group_state_checkpoints(
        &self,
        group_id: &GroupId,
    ) -> StorageResult<Vec<GroupStateCheckpointRef>> {
        snapshot::list_checkpoints(self, group_id)
    }

    fn release_group_state_checkpoint(
        &self,
        group_id: &GroupId,
        checkpoint_id: &str,
    ) -> StorageResult<()> {
        snapshot::release_checkpoint(self, group_id, checkpoint_id)
    }
}

/// `pending_authority_upgrade` (`messages.rs:1372`): an existing pending row
/// upgrades in place exactly when it is the same event with `authority` newly
/// resolved (and `retention` still unset, or equal after the upgrade).
fn pending_authority_upgrade(existing: &[u8], incoming: &GroupEvent) -> StorageResult<bool> {
    let mut existing: GroupEvent = ser::de(existing)?;
    if let (
        GroupEvent::MessageReceived {
            authority: old,
            retention: old_retention,
            ..
        },
        GroupEvent::MessageReceived {
            authority: Some(new),
            retention: new_retention,
            ..
        },
    ) = (&mut existing, incoming)
        && old.is_none()
    {
        *old = Some(*new);
        if old_retention.is_none() {
            *old_retention = *new_retention;
        }
        return Ok(&existing == incoming);
    }
    Ok(false)
}

fn merged_pending_event(existing: &[u8], incoming: &GroupEvent) -> StorageResult<GroupEvent> {
    let mut existing: GroupEvent = ser::de(existing)?;
    if let (
        GroupEvent::MessageReceived {
            authority: old,
            retention: old_retention,
            ..
        },
        GroupEvent::MessageReceived {
            authority: Some(new),
            retention: new_retention,
            ..
        },
    ) = (&mut existing, incoming)
        && old.is_none()
    {
        *old = Some(*new);
        if old_retention.is_none() {
            *old_retention = *new_retention;
        }
    }
    Ok(existing)
}

// OutboundIntentStorage ---------------------------------------------------

impl OutboundIntentStorage for KvStorageProvider {
    fn put_queued_outbound_intent(
        &self,
        record: &cgka_traits::storage::QueuedOutboundIntent,
    ) -> StorageResult<()> {
        self.store.ensure_writable()?;
        self.require_group(&record.group_id)?;
        self.put_ordered(Table::QueuedIntent, &[mid_seg(&record.id)], record)
    }

    fn list_queued_outbound_intents(
        &self,
        group_id: &GroupId,
    ) -> StorageResult<Vec<cgka_traits::storage::QueuedOutboundIntent>> {
        let all: Vec<cgka_traits::storage::QueuedOutboundIntent> =
            self.list_ordered(Table::QueuedIntent, &[])?;
        Ok(all
            .into_iter()
            .filter(|record| &record.group_id == group_id)
            .collect())
    }

    /// `DELETE ... WHERE id`, `changed == 0` → `NotFound`
    /// (`outbound.rs:80-99`).
    fn delete_queued_outbound_intent(&self, id: &MessageId) -> StorageResult<()> {
        self.store.ensure_writable()?;
        if self.row(Table::QueuedIntent, &[mid_seg(id)])?.is_none() {
            return Err(StorageError::NotFound);
        }
        self.del_row(Table::QueuedIntent, &[mid_seg(id)])
    }

    fn put_own_commit_intent(
        &self,
        record: &cgka_traits::storage::OwnCommitIntent,
    ) -> StorageResult<()> {
        self.store.ensure_writable()?;
        self.require_group(&record.group_id)?;
        self.put_ordered(
            Table::OwnCommitIntent,
            &[mid_seg(&record.commit_id)],
            record,
        )
    }

    fn own_commit_intent(
        &self,
        commit_id: &MessageId,
    ) -> StorageResult<Option<cgka_traits::storage::OwnCommitIntent>> {
        self.row(Table::OwnCommitIntent, &[mid_seg(commit_id)])?
            .map(|v| Self::read_ordered(&v).map(|(_, record)| record))
            .transpose()
    }

    fn list_own_commit_intents(
        &self,
        group_id: Option<&GroupId>,
    ) -> StorageResult<Vec<cgka_traits::storage::OwnCommitIntent>> {
        let all: Vec<cgka_traits::storage::OwnCommitIntent> =
            self.list_ordered(Table::OwnCommitIntent, &[])?;
        Ok(all
            .into_iter()
            .filter(|record| group_id.is_none_or(|gid| &record.group_id == gid))
            .collect())
    }

    fn delete_own_commit_intent(&self, commit_id: &MessageId) -> StorageResult<()> {
        self.del_row(Table::OwnCommitIntent, &[mid_seg(commit_id)])
    }
}

// OutboundFanoutStorage ---------------------------------------------------

impl OutboundFanoutStorage for KvStorageProvider {
    /// Replace with the `validate_successor_of` guard, and raise the
    /// pending-reference high-water inside the same unit — `put_outbound_fanout`
    /// in `outbound.rs`.
    fn put_outbound_fanout(&self, fanout: &OutboundFanout) -> StorageResult<()> {
        self.store.transact(|| -> StorageResult<()> {
            if let Some(group_id) = fanout.group_id() {
                self.require_group(group_id)?;
            }
            let mid = mid_seg(fanout.message_id());
            let order = self.fanout_order(mid, fanout)?;
            if let Some(pending) = fanout.pending_ref() {
                self.store.raise_pending_high_water(pending.as_u64())?;
            }
            self.put_row(
                Table::OutboundFanout,
                &[mid],
                Self::write_ordered(order, fanout)?,
            )
        })
    }

    fn outbound_fanout(&self, message_id: &MessageId) -> StorageResult<Option<OutboundFanout>> {
        self.row(Table::OutboundFanout, &[mid_seg(message_id)])?
            .map(|v| Self::read_ordered(&v).map(|(_, record)| record))
            .transpose()
    }

    fn list_outbound_fanouts(&self) -> StorageResult<Vec<OutboundFanout>> {
        self.list_ordered(Table::OutboundFanout, &[])
    }

    fn list_outbound_fanouts_for_group(
        &self,
        group_id: &GroupId,
    ) -> StorageResult<Vec<OutboundFanout>> {
        Ok(self
            .list_ordered::<OutboundFanout>(Table::OutboundFanout, &[])?
            .into_iter()
            .filter(|record| record.group_id() == Some(group_id))
            .collect())
    }

    fn delete_outbound_fanout(&self, message_id: &MessageId) -> StorageResult<()> {
        self.del_row(Table::OutboundFanout, &[mid_seg(message_id)])
    }
}

// Leave / Disband ---------------------------------------------------------

impl LeaveRequestStorage for KvStorageProvider {
    fn put_leave_request(&self, request: &LeaveRequest) -> StorageResult<()> {
        self.store.ensure_writable()?;
        self.require_group(&request.group_id)?;
        self.put_ser(Table::LeaveRequest, &[gid_seg(&request.group_id)], request)
    }

    fn leave_request(&self, group_id: &GroupId) -> StorageResult<Option<LeaveRequest>> {
        self.de_row(Table::LeaveRequest, &[gid_seg(group_id)])
    }

    fn clear_leave_request(&self, group_id: &GroupId) -> StorageResult<()> {
        self.del_row(Table::LeaveRequest, &[gid_seg(group_id)])
    }
}

impl DisbandRequestStorage for KvStorageProvider {
    fn put_disband_request(&self, request: &DisbandRequest) -> StorageResult<()> {
        self.store.ensure_writable()?;
        self.require_group(&request.group_id)?;
        self.put_ser(
            Table::DisbandRequest,
            &[gid_seg(&request.group_id)],
            request,
        )
    }

    fn disband_request(&self, group_id: &GroupId) -> StorageResult<Option<DisbandRequest>> {
        self.de_row(Table::DisbandRequest, &[gid_seg(group_id)])
    }

    fn clear_disband_request(&self, group_id: &GroupId) -> StorageResult<()> {
        self.del_row(Table::DisbandRequest, &[gid_seg(group_id)])
    }
}

impl DisbandCandidateStorage for KvStorageProvider {
    /// `INSERT OR REPLACE` + `ORDER BY rowid`: a re-put moves the candidate
    /// to the end of its group's list.
    fn put_disband_candidate(&self, candidate: &DisbandCandidate) -> StorageResult<()> {
        self.store.ensure_writable()?;
        self.require_group(&candidate.group_id)?;
        self.put_ordered_fresh(
            Table::DisbandCandidate,
            &[gid_seg(&candidate.group_id), mid_seg(&candidate.commit_id)],
            candidate,
        )
    }

    fn disband_candidate(
        &self,
        group_id: &GroupId,
        commit_id: &MessageId,
    ) -> StorageResult<Option<DisbandCandidate>> {
        self.row(
            Table::DisbandCandidate,
            &[gid_seg(group_id), mid_seg(commit_id)],
        )?
        .map(|v| Self::read_ordered(&v).map(|(_, record)| record))
        .transpose()
    }

    fn list_disband_candidates(&self, group_id: &GroupId) -> StorageResult<Vec<DisbandCandidate>> {
        self.list_ordered(Table::DisbandCandidate, &[gid_seg(group_id)])
    }

    fn clear_disband_candidates(&self, group_id: &GroupId) -> StorageResult<()> {
        self.del_prefix(Table::DisbandCandidate, &[gid_seg(group_id)])
    }
}

impl DisbandTombstoneStorage for KvStorageProvider {
    /// Preserve-on-rewrite: `announced` is owned by
    /// `mark_disband_tombstone_announced`; OR it forward from the stored row.
    fn put_disband_tombstone(
        &self,
        group_id: &GroupId,
        tombstone: &DisbandTombstone,
    ) -> StorageResult<()> {
        self.store.transact(|| -> StorageResult<()> {
            let previously_announced = self
                .de_row::<DisbandTombstone>(Table::DisbandTombstone, &[gid_seg(group_id)])?
                .is_some_and(|existing| existing.announced);
            if previously_announced && !tombstone.announced {
                self.put_ser(
                    Table::DisbandTombstone,
                    &[gid_seg(group_id)],
                    &DisbandTombstone {
                        announced: true,
                        ..tombstone.clone()
                    },
                )
            } else {
                self.put_ser(Table::DisbandTombstone, &[gid_seg(group_id)], tombstone)
            }
        })
    }

    fn disband_tombstone(&self, group_id: &GroupId) -> StorageResult<Option<DisbandTombstone>> {
        self.de_row(Table::DisbandTombstone, &[gid_seg(group_id)])
    }

    /// `ORDER BY group_id` — row-key order.
    fn list_disband_tombstones(&self) -> StorageResult<Vec<(GroupId, DisbandTombstone)>> {
        self.scan_rows(Table::DisbandTombstone, &[])?
            .iter()
            .map(|(row_key, value)| {
                let gid = keys::segments(row_key)
                    .and_then(|segments| segments.last().copied())
                    .map(|seg| GroupId::new(seg.to_vec()))
                    .ok_or_else(malformed_row)?;
                Ok((gid, ser::de::<DisbandTombstone>(value)?))
            })
            .collect()
    }

    fn mark_disband_tombstone_announced(&self, group_id: &GroupId) -> StorageResult<()> {
        self.store.transact(|| -> StorageResult<()> {
            let Some(mut tombstone) =
                self.de_row::<DisbandTombstone>(Table::DisbandTombstone, &[gid_seg(group_id)])?
            else {
                return Ok(());
            };
            if tombstone.announced {
                return Ok(());
            }
            tombstone.announced = true;
            self.put_ser(Table::DisbandTombstone, &[gid_seg(group_id)], &tombstone)
        })
    }
}

// Welcome -----------------------------------------------------------------

impl WelcomeStorage for KvStorageProvider {
    /// `INSERT OR REPLACE` + `ORDER BY rowid`: a re-put moves the welcome to
    /// the end. Rows key on `message_id`, the sqlite primary key.
    fn put_welcome(&self, welcome: &PendingWelcome) -> StorageResult<()> {
        self.put_ordered_fresh(Table::Welcome, &[mid_seg(&welcome.message_id)], welcome)
    }

    /// Missing → `NotFound`; undecodable → the row stays, matching
    /// `take_welcome_on_connection` (read, decode, then delete).
    fn take_welcome(&self, id: &MessageId) -> StorageResult<PendingWelcome> {
        self.store.transact(|| -> StorageResult<PendingWelcome> {
            let value = self
                .row(Table::Welcome, &[mid_seg(id)])?
                .ok_or(StorageError::NotFound)?;
            let (_, welcome) = Self::read_ordered::<PendingWelcome>(&value)?;
            self.del_row(Table::Welcome, &[mid_seg(id)])?;
            Ok(welcome)
        })
    }

    /// `ORDER BY rowid`; undecodable rows are skipped (sqlite warns; the warn
    /// carries no sensitive data and there is no logging channel here).
    fn list_welcomes(&self) -> StorageResult<Vec<PendingWelcome>> {
        let mut rows = Vec::new();
        for (_, value) in self.scan_rows(Table::Welcome, &[])? {
            let (order, record) = match Self::read_ordered::<PendingWelcome>(&value) {
                Ok(entry) => entry,
                Err(StorageError::Serialization(_)) => continue,
                Err(error) => return Err(error),
            };
            rows.push((order, record));
        }
        rows.sort_by_key(|(order, _)| *order);
        Ok(rows.into_iter().map(|(_, record)| record).collect())
    }
}

// Capability --------------------------------------------------------------

impl CapabilityStorage for KvStorageProvider {
    fn register_feature(&self, feature: Feature, req: CapabilityRequirement) -> StorageResult<()> {
        let row = serde_json::json!({
            "requires": req.requires,
            "level": req.level,
            "description": req.description,
        });
        self.put_row(Table::Capability, &[feature.0.as_bytes()], ser::ser(&row)?)
    }

    fn feature_requirement(
        &self,
        feature: &Feature,
    ) -> StorageResult<Option<CapabilityRequirement>> {
        self.row(Table::Capability, &[feature.0.as_bytes()])?
            .map(|value| {
                let row: serde_json::Value = ser::de(&value)?;
                let requires = ser::de::<cgka_traits::capabilities::Capability>(&ser::ser(
                    row.get("requires").ok_or_else(malformed_row)?,
                )?)?;
                let level = ser::de::<cgka_traits::capabilities::RequirementLevel>(&ser::ser(
                    row.get("level").ok_or_else(malformed_row)?,
                )?)?;
                let description = row
                    .get("description")
                    .and_then(serde_json::Value::as_str)
                    .ok_or_else(malformed_row)?
                    .to_owned();
                Ok(CapabilityRequirement {
                    requires,
                    level,
                    description: self.intern_description(description)?,
                })
            })
            .transpose()
    }
}

// Convergence --------------------------------------------------------------

impl ConvergencePolicyStorage for KvStorageProvider {
    fn put_convergence_policy(&self, group_id: &GroupId, policy: &[u8]) -> StorageResult<()> {
        self.store.ensure_writable()?;
        self.require_group(group_id)?;
        self.put_row(
            Table::ConvergencePolicy,
            &[gid_seg(group_id)],
            policy.to_vec(),
        )
    }

    fn convergence_policy(&self, group_id: &GroupId) -> StorageResult<Option<Vec<u8>>> {
        self.row(Table::ConvergencePolicy, &[gid_seg(group_id)])
    }
}

impl MemberValidationCacheStorage for KvStorageProvider {
    fn put_validated_tree_marker(&self, group_id: &GroupId, marker: &[u8]) -> StorageResult<()> {
        self.store.ensure_writable()?;
        self.require_group(group_id)?;
        self.put_row(
            Table::MemberValidation,
            &[gid_seg(group_id)],
            marker.to_vec(),
        )
    }

    fn validated_tree_marker(&self, group_id: &GroupId) -> StorageResult<Option<Vec<u8>>> {
        self.row(Table::MemberValidation, &[gid_seg(group_id)])
    }
}

impl ConvergencePassStorage for KvStorageProvider {
    fn convergence_pass(
        &self,
        group_id: &GroupId,
    ) -> StorageResult<Option<DurableConvergencePass>> {
        self.de_row(Table::ConvergencePass, &[gid_seg(group_id)])
    }

    fn put_convergence_pass(&self, pass: &DurableConvergencePass) -> StorageResult<()> {
        self.store.ensure_writable()?;
        self.require_group(&pass.group_id)?;
        self.put_ser(Table::ConvergencePass, &[gid_seg(&pass.group_id)], pass)
    }

    /// `ORDER BY group_id` — row-key order.
    fn list_convergence_passes(&self) -> StorageResult<Vec<DurableConvergencePass>> {
        self.scan_rows(Table::ConvergencePass, &[])?
            .iter()
            .map(|(_, value)| ser::de(value))
            .collect()
    }

    fn delete_convergence_pass(&self, group_id: &GroupId) -> StorageResult<()> {
        self.del_row(Table::ConvergencePass, &[gid_seg(group_id)])
    }
}

impl DeferredPeelGenerationStorage for KvStorageProvider {
    fn deferred_peel_generation(
        &self,
        group_id: &GroupId,
    ) -> StorageResult<Option<DeferredPeelGeneration>> {
        self.de_row(Table::DeferredPeelGeneration, &[gid_seg(group_id)])
    }

    fn put_deferred_peel_generation(
        &self,
        generation: &DeferredPeelGeneration,
    ) -> StorageResult<()> {
        self.store.ensure_writable()?;
        self.require_group(&generation.group_id)?;
        self.put_ser(
            Table::DeferredPeelGeneration,
            &[gid_seg(&generation.group_id)],
            generation,
        )
    }

    fn delete_deferred_peel_generation(&self, group_id: &GroupId) -> StorageResult<()> {
        self.del_row(Table::DeferredPeelGeneration, &[gid_seg(group_id)])
    }
}

// Account device -----------------------------------------------------------

impl AccountDeviceSignerStorage for KvStorageProvider {
    fn put_account_device_signer(&self, binding: &AccountDeviceSignerBinding) -> StorageResult<()> {
        self.put_ser(
            Table::AccountDeviceSigner,
            &[binding.marmot_identity.as_slice()],
            binding,
        )
    }

    fn account_device_signer(
        &self,
        marmot_identity: &MemberId,
    ) -> StorageResult<Option<AccountDeviceSignerBinding>> {
        self.de_row(Table::AccountDeviceSigner, &[marmot_identity.as_slice()])
    }
}

impl AccountDeviceHistoryKeyStorage for KvStorageProvider {
    /// `INSERT OR IGNORE` — the first key wins; the stored row is returned.
    fn put_account_device_history_key_if_absent(
        &self,
        key: &AccountDeviceHistoryKey,
    ) -> StorageResult<AccountDeviceHistoryKey> {
        self.store.ensure_writable()?;
        self.store
            .transact(|| -> StorageResult<AccountDeviceHistoryKey> {
                self.store.ensure_writable()?;
                let member = key.marmot_identity.as_slice();
                if self
                    .row(Table::AccountDeviceHistoryKey, &[member])?
                    .is_none()
                {
                    let mut out = Vec::new();
                    ser::put_u16_field(&mut out, key.ciphersuite);
                    ser::put_field(&mut out, &key.public_key);
                    ser::put_field(&mut out, key.private_key.as_slice());
                    self.put_row(Table::AccountDeviceHistoryKey, &[member], out)?;
                }
                self.account_device_history_key(&key.marmot_identity)?
                    .ok_or(StorageError::NotFound)
            })
    }

    fn account_device_history_key(
        &self,
        marmot_identity: &MemberId,
    ) -> StorageResult<Option<AccountDeviceHistoryKey>> {
        self.row(
            Table::AccountDeviceHistoryKey,
            &[marmot_identity.as_slice()],
        )?
        .map(|value| {
            let mut tuple = Tuple::new(&value);
            let ciphersuite = tuple.u16()?;
            let public_key = tuple.field()?.to_vec();
            let private_key = tuple.field()?.to_vec();
            tuple.finish()?;
            Ok(AccountDeviceHistoryKey {
                marmot_identity: marmot_identity.clone(),
                ciphersuite,
                public_key,
                private_key: Zeroizing::new(private_key),
            })
        })
        .transpose()
    }
}

// Device group welcome ------------------------------------------------------

impl DeviceGroupWelcomeStorage for KvStorageProvider {
    fn put_device_group_welcome_expectation(
        &self,
        expectation: &DeviceGroupWelcomeExpectation,
    ) -> StorageResult<()> {
        self.put_ser(
            Table::DeviceGroupWelcome,
            &[gid_seg(&expectation.group_id)],
            expectation,
        )
    }

    fn device_group_welcome_expectation(
        &self,
        group_id: &GroupId,
    ) -> StorageResult<Option<DeviceGroupWelcomeExpectation>> {
        self.de_row(Table::DeviceGroupWelcome, &[gid_seg(group_id)])
    }

    /// `ORDER BY group_id` — row-key order.
    fn device_group_welcome_expectations(
        &self,
    ) -> StorageResult<Vec<DeviceGroupWelcomeExpectation>> {
        self.scan_rows(Table::DeviceGroupWelcome, &[])?
            .iter()
            .map(|(_, value)| ser::de(value))
            .collect()
    }

    /// Standalone-engine semantics: the trait default — compare and consume.
    /// sqlite's move-owner bookkeeping lives on the app side, which a
    /// protocol-only store has no rows for.
    fn accept_device_group_welcome(
        &self,
        expected: &DeviceGroupWelcomeExpectation,
        _joined_at: Timestamp,
    ) -> StorageResult<bool> {
        if self
            .device_group_welcome_expectation(&expected.group_id)?
            .as_ref()
            != Some(expected)
        {
            return Ok(false);
        }
        self.delete_device_group_welcome_expectation(&expected.group_id)
    }

    fn delete_device_group_welcome_expectation(&self, group_id: &GroupId) -> StorageResult<bool> {
        self.store.transact(|| -> StorageResult<bool> {
            let existed = self
                .store
                .contains(&keys::key(Table::DeviceGroupWelcome, &[gid_seg(group_id)]))?;
            self.del_row(Table::DeviceGroupWelcome, &[gid_seg(group_id)])?;
            Ok(existed)
        })
    }
}

// KeyPackageBundleStorage ---------------------------------------------------

impl KeyPackageBundleStorage for KvStorageProvider {
    /// `WHERE label = 'KeyPackage' ORDER BY storage_key` — the raw row value
    /// is the bundle bytes the engine decodes (`StoredKeyPackageBundle::value`
    /// stays JSON).
    fn stored_key_package_bundles(&self) -> StorageResult<Vec<StoredKeyPackageBundle>> {
        let mut bundles = Vec::new();
        for (row_key, value) in
            self.scan_rows(Table::OpenMlsValue, &[crate::mls::KEY_PACKAGE_LABEL_BYTES])?
        {
            let storage_key = keys::segments(&row_key)
                .and_then(|segments| segments.last().copied())
                .ok_or_else(malformed_row)?
                .to_vec();
            bundles.push(StoredKeyPackageBundle {
                storage_key,
                value: Zeroizing::new(value),
            });
        }
        bundles.sort_by(|a, b| a.storage_key.cmp(&b.storage_key));
        Ok(bundles)
    }

    fn delete_stored_key_package_bundle(&self, storage_key: &[u8]) -> StorageResult<()> {
        let label = crate::mls::storage_key_label(storage_key).ok_or_else(malformed_row)?;
        self.store
            .delete(&keys::key(Table::OpenMlsValue, &[label, storage_key]))
    }
}

// Maintenance ----------------------------------------------------------------

impl MaintenanceStorage for KvStorageProvider {
    fn key_package_lifecycle(&self) -> StorageResult<Option<KeyPackageLifecycleState>> {
        self.de_row(Table::Maintenance, &[b"kpl"])
    }

    fn put_key_package_lifecycle(&self, state: &KeyPackageLifecycleState) -> StorageResult<()> {
        self.put_ser(Table::Maintenance, &[b"kpl"], state)
    }

    fn group_maintenance(
        &self,
        group_id: &GroupId,
    ) -> StorageResult<Option<GroupMaintenanceState>> {
        self.de_row(Table::Maintenance, &[b"gm", gid_seg(group_id)])
    }

    fn put_group_maintenance(&self, state: &GroupMaintenanceState) -> StorageResult<()> {
        self.store.ensure_writable()?;
        self.require_group(&state.group_id)?;
        self.put_ser(
            Table::Maintenance,
            &[b"gm", gid_seg(&state.group_id)],
            state,
        )
    }

    fn delete_group_maintenance(&self, group_id: &GroupId) -> StorageResult<()> {
        self.del_row(Table::Maintenance, &[b"gm", gid_seg(group_id)])
    }

    fn put_maintenance_obligation(&self, record: &MaintenanceObligation) -> StorageResult<()> {
        self.store.ensure_writable()?;
        self.require_group(&record.group_id)?;
        self.put_ordered(Table::Maintenance, &[b"obl", mid_seg(&record.id)], record)
    }

    fn maintenance_obligation(
        &self,
        id: &MessageId,
    ) -> StorageResult<Option<MaintenanceObligation>> {
        self.row(Table::Maintenance, &[b"obl", mid_seg(id)])?
            .map(|v| Self::read_ordered(&v).map(|(_, record)| record))
            .transpose()
    }

    fn list_maintenance_obligations(&self) -> StorageResult<Vec<MaintenanceObligation>> {
        self.list_ordered(Table::Maintenance, &[b"obl"])
    }

    fn list_maintenance_obligations_for_group(
        &self,
        group_id: &GroupId,
    ) -> StorageResult<Vec<MaintenanceObligation>> {
        Ok(self
            .list_ordered::<MaintenanceObligation>(Table::Maintenance, &[b"obl"])?
            .into_iter()
            .filter(|record| &record.group_id == group_id)
            .collect())
    }

    fn delete_maintenance_obligation(&self, id: &MessageId) -> StorageResult<()> {
        self.del_row(Table::Maintenance, &[b"obl", mid_seg(id)])
    }

    /// The high-water raise and the record write are one unit, like
    /// `with_savepoint` in `maintenance.rs`.
    fn put_group_evolution(&self, record: &DurableGroupEvolution) -> StorageResult<()> {
        self.store.transact(|| -> StorageResult<()> {
            self.store.ensure_writable()?;
            self.require_group(&record.group_id)?;
            if let Some(pending) = record.pending_ref {
                self.store.raise_pending_high_water(pending.as_u64())?;
            }
            let mid = mid_seg(&record.id);
            let order = match self.row(Table::Maintenance, &[b"evo", mid])? {
                Some(existing) => Self::read_order(&existing)?,
                None => self.store.next_sequence()?,
            };
            self.put_row(
                Table::Maintenance,
                &[b"evo", mid],
                Self::write_ordered(order, record)?,
            )
        })
    }

    fn group_evolution(&self, id: &MessageId) -> StorageResult<Option<DurableGroupEvolution>> {
        self.row(Table::Maintenance, &[b"evo", mid_seg(id)])?
            .map(|v| Self::read_ordered(&v).map(|(_, record)| record))
            .transpose()
    }

    fn list_group_evolutions(&self) -> StorageResult<Vec<DurableGroupEvolution>> {
        self.list_ordered(Table::Maintenance, &[b"evo"])
    }

    fn list_group_evolutions_for_group(
        &self,
        group_id: &GroupId,
    ) -> StorageResult<Vec<DurableGroupEvolution>> {
        Ok(self
            .list_ordered::<DurableGroupEvolution>(Table::Maintenance, &[b"evo"])?
            .into_iter()
            .filter(|record| &record.group_id == group_id)
            .collect())
    }

    fn delete_group_evolution(&self, id: &MessageId) -> StorageResult<()> {
        self.del_row(Table::Maintenance, &[b"evo", mid_seg(id)])
    }

    fn put_transport_fanout(&self, record: &DurableTransportFanout) -> StorageResult<()> {
        self.store.ensure_writable()?;
        if let Some(group_id) = &record.group_id {
            self.require_group(group_id)?;
        }
        self.put_ordered(Table::Maintenance, &[b"fan", mid_seg(&record.id)], record)
    }

    fn transport_fanout(&self, id: &MessageId) -> StorageResult<Option<DurableTransportFanout>> {
        self.row(Table::Maintenance, &[b"fan", mid_seg(id)])?
            .map(|v| Self::read_ordered(&v).map(|(_, record)| record))
            .transpose()
    }

    fn list_transport_fanouts(&self) -> StorageResult<Vec<DurableTransportFanout>> {
        self.list_ordered(Table::Maintenance, &[b"fan"])
    }

    fn delete_transport_fanout(&self, id: &MessageId) -> StorageResult<()> {
        self.del_row(Table::Maintenance, &[b"fan", mid_seg(id)])
    }

    /// The singleton row is always present in sqlite (migration
    /// `0034_maintenance_publication.rs` inserts `periodic_policy = 1`); a
    /// missing row reads as `EnabledForNewGroups` here.
    fn periodic_maintenance_policy(&self) -> StorageResult<PeriodicMaintenancePolicy> {
        self.row(Table::Maintenance, &[b"settings"])?.map_or(
            Ok(PeriodicMaintenancePolicy::EnabledForNewGroups),
            |value| match value.as_slice() {
                [0] => Ok(PeriodicMaintenancePolicy::Disabled),
                [1] => Ok(PeriodicMaintenancePolicy::EnabledForNewGroups),
                _ => Err(StorageError::Serialization(
                    "invalid persisted periodic maintenance policy".into(),
                )),
            },
        )
    }

    fn put_periodic_maintenance_policy(
        &self,
        policy: PeriodicMaintenancePolicy,
    ) -> StorageResult<()> {
        let value = match policy {
            PeriodicMaintenancePolicy::Disabled => 0,
            PeriodicMaintenancePolicy::EnabledForNewGroups => 1,
        };
        self.put_row(Table::Maintenance, &[b"settings"], vec![value])
    }
}

// StorageProvider aggregate --------------------------------------------------

impl StorageProvider for KvStorageProvider {
    type Mls = crate::mls::KvOpenMlsStorage;

    fn mls_storage(&self) -> &Self::Mls {
        &self.mls
    }

    /// `None` when the error is a stored value that did not decode (sqlite's
    /// `is_undecodable_value`); otherwise the storage failure itself — the
    /// `Self::Storage(error) => error.clone()` arm of sqlite's
    /// `storage_failure`.
    fn mls_storage_failure(&self, error: &MlsStorageError<Self>) -> Option<StorageError> {
        if crate::mls::is_undecodable(error) {
            return None;
        }
        Some(error.clone())
    }

    fn pending_ref_high_water(&self) -> StorageResult<u64> {
        self.store.pending_high_water()
    }

    /// The committed-batch count: a strict over-approximation of sqlite's
    /// OpenMLS-write generation (which also moves on unrelated commits via
    /// `PRAGMA data_version`); over-invalidation is always safe.
    fn mls_write_generation(&self) -> Option<u64> {
        self.store.write_generation().ok()
    }

    /// A frozen read view for the duration of `f`; writes inside fail.
    fn with_read_snapshot<T, E, F>(&self, f: F) -> Result<T, E>
    where
        E: From<StorageError>,
        F: FnOnce(&Self) -> Result<T, E>,
    {
        self.store.read_snapshot(|| f(self))
    }

    fn maintenance_storage(&self) -> Option<&dyn MaintenanceStorage> {
        Some(self)
    }

    fn group_replay_state_fingerprint(
        &self,
        group_id: &GroupId,
    ) -> StorageResult<Option<[u8; 32]>> {
        snapshot::replay_fingerprint(self, group_id).map(Some)
    }

    fn with_transaction<T, E, F>(&self, f: F) -> Result<T, E>
    where
        E: From<StorageError>,
        F: FnOnce(&Self) -> Result<T, E>,
    {
        self.store.transact(|| f(self))
    }

    /// Outside a unit the callback runs immediately; inside one it queues
    /// behind the outermost commit and dies with a rollback.
    fn after_commit(&self, callback: Box<dyn FnOnce() + Send + 'static>) {
        self.store.after_commit(callback);
    }

    /// A savepoint, not the snapshot fallback: the contract permits undoing
    /// more than the group's writes on failure, and an overlay rollback is
    /// exactly SQLite's `SAVEPOINT` — `with_group_savepoint` in `provider.rs`.
    fn with_group_savepoint<T, E, F>(
        &self,
        _group_id: &GroupId,
        _fallback_snapshot: &str,
        f: F,
    ) -> Result<T, E>
    where
        E: From<StorageError>,
        F: FnOnce(&Self) -> Result<T, E>,
    {
        self.store.savepoint(|| f(self))
    }

    /// The overlay savepoint above is a real savepoint: it writes no
    /// snapshot, and only the outermost commit emits a batch, so a crash
    /// inside `f` leaves none of `f`'s writes — same as sqlite's `SAVEPOINT`.
    fn has_atomic_group_savepoints(&self) -> bool {
        true
    }

    /// Canonical-state-only restore, even from a full image — sqlite's
    /// override delegates to `rollback_group_state_to_snapshot`.
    fn rollback_group_state_keeping_live_records(
        &self,
        group_id: &GroupId,
        name: &str,
    ) -> StorageResult<()> {
        snapshot::rollback_group_state(self, group_id, name)
    }

    fn list_snapshots_with_prefixes(
        &self,
        prefixes: &[&str],
    ) -> StorageResult<Vec<(GroupId, String)>> {
        snapshot::list_with_prefixes(self, prefixes)
    }

    /// MDK v0.17.0's `Backend` enum has no generic KV variant; `Sqlite` is
    /// the only honest answer the enum admits. Upstream gap.
    fn backend(&self) -> Backend {
        Backend::Sqlite
    }
}
