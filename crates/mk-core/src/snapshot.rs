//! Named group snapshots, group-state checkpoints, and the replay-state
//! fingerprint — `storage-sqlite`'s `snapshots/` module over the KV store.
//!
//! Scope parity (`snapshots/AGENTS.md`):
//! - a full snapshot captures the canonical group state (group record,
//!   validated-tree marker, group-scoped `OpenMLS` rows), the convergence
//!   policy, the message ledger, and the queued-outbound intents;
//! - a group-state snapshot captures everything but the ledger and queue;
//! - a checkpoint captures canonical state only and its restore never
//!   touches the convergence policy — asymmetric to a state-scoped snapshot
//!   restore, which does restore the policy.
//!
//! Restores keep captured insertion positions and discard pending
//! application events whose source message the rollback removed
//! (`restore.rs`). Snapshot creation is `INSERT OR REPLACE`; checkpoint
//! creation is insert-only with an exact-retry idempotent success and
//! `AlreadyExists` on any other conflict.
//!
//! Blobs are versioned (`[u8 tag]` + a self-describing `serde_json` document)
//! and opaque outside this module. Fingerprint bytes differ from sqlite's
//! by construction — the fingerprint is a memory-only validation value
//! (`snapshots/AGENTS.md`), so what carries across backends is the
//! structure: live canonical image plus every retained snapshot and
//! checkpoint blob, in name order.

use cgka_traits::storage::{
    ConvergencePolicyStorage, GroupStateCheckpointRef, QueuedOutboundIntent, StorageError,
    StorageResult,
};
use cgka_traits::types::GroupId;
use serde::{Deserialize, Serialize};
use sha2::Digest as _;

use crate::keys::{self, Table};
use crate::provider::{KvStorageProvider, gid_seg};
use crate::ser;

/// Current snapshot/checkpoint blob tag.
const BLOB_V1: u8 = 1;

/// The canonical group-state image, shared by snapshots, checkpoints, and
/// the replay fingerprint.
#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct GroupStateImage {
    /// The serialized `cgka_groups` row bytes.
    #[serde(with = "crate::ser::blob")]
    pub(crate) group: Vec<u8>,
    #[serde(with = "crate::ser::opt_blob")]
    pub(crate) validated_tree_marker: Option<Vec<u8>>,
    /// `(row_key, value)` pairs, sorted by row key.
    #[serde(with = "crate::ser::blob_pairs")]
    pub(crate) openmls: Vec<(Vec<u8>, Vec<u8>)>,
}

#[derive(Debug, Serialize, Deserialize)]
struct SnapshotBlob {
    /// `true` — a full snapshot that captured ledger and queue.
    full: bool,
    state: GroupStateImage,
    #[serde(with = "crate::ser::opt_blob")]
    convergence_policy: Option<Vec<u8>>,
    /// `(insert_order, serialized MessageRecord)` in order; full only.
    #[serde(with = "crate::ser::opt_u64_pairs")]
    messages: Option<Vec<(u64, Vec<u8>)>>,
    /// `(insert_order, serialized QueuedOutboundIntent)` in order; full only.
    #[serde(with = "crate::ser::opt_u64_pairs")]
    queued_outbound: Option<Vec<(u64, Vec<u8>)>>,
}

fn encode<T: Serialize>(value: &T) -> StorageResult<Vec<u8>> {
    let mut out = vec![BLOB_V1];
    out.extend_from_slice(&ser::ser(value)?);
    Ok(out)
}

fn decode<T: serde::de::DeserializeOwned>(blob: &[u8]) -> StorageResult<T> {
    let (tag, body) = blob
        .split_first()
        .ok_or_else(|| StorageError::Serialization("empty snapshot blob".into()))?;
    if *tag != BLOB_V1 {
        return Err(StorageError::Serialization(
            "unsupported snapshot blob version".into(),
        ));
    }
    ser::de(body)
}

fn snapshot_row(group_id: &GroupId, name: &str) -> Vec<u8> {
    keys::key(Table::Snapshot, &[gid_seg(group_id), name.as_bytes()])
}

fn checkpoint_row(group_id: &GroupId, id: &str) -> Vec<u8> {
    keys::key(Table::Checkpoint, &[gid_seg(group_id), id.as_bytes()])
}

fn capture_state(
    provider: &KvStorageProvider,
    group_id: &GroupId,
) -> StorageResult<GroupStateImage> {
    provider.capture_group_state(group_id)
}

fn capture(
    provider: &KvStorageProvider,
    group_id: &GroupId,
    full: bool,
) -> StorageResult<SnapshotBlob> {
    let state = capture_state(provider, group_id)?;
    let convergence_policy = provider.convergence_policy(group_id)?;
    let (messages, queued_outbound) = if full {
        // `Message[gid]` keys are `(gid, seq8, mid)` so the prefix scan is
        // already in insertion order; raw row bytes are copied as-is.
        let messages = provider
            .scan_rows(Table::Message, &[gid_seg(group_id)])?
            .into_iter()
            .map(|(row_key, value)| {
                let seq = row_key_seq(&row_key)?;
                Ok((seq, value))
            })
            .collect::<StorageResult<Vec<(u64, Vec<u8>)>>>()?;
        let queued = provider
            .scan_rows(Table::QueuedIntent, &[])?
            .iter()
            .map(|(_, value)| KvStorageProvider::read_ordered::<QueuedOutboundIntent>(value))
            .collect::<StorageResult<Vec<_>>>()?
            .into_iter()
            .filter(|(_, record)| &record.group_id == group_id)
            .map(|(order, record)| Ok((order, ser::ser(&record)?)))
            .collect::<StorageResult<Vec<_>>>()?;
        (Some(messages), Some(queued))
    } else {
        (None, None)
    };
    Ok(SnapshotBlob {
        full,
        state,
        convergence_policy,
        messages,
        queued_outbound,
    })
}

fn create(
    provider: &KvStorageProvider,
    group_id: &GroupId,
    name: &str,
    full: bool,
) -> StorageResult<()> {
    provider.store().transact(|| -> StorageResult<()> {
        let blob = encode(&capture(provider, group_id, full)?)?;
        // `INSERT OR REPLACE` (`capture.rs:create_on_connection`): retaking a
        // name replaces the snapshot.
        provider.store().put(snapshot_row(group_id, name), blob)
    })
}

pub(crate) fn create_group_snapshot(
    provider: &KvStorageProvider,
    group_id: &GroupId,
    name: &str,
) -> StorageResult<()> {
    create(provider, group_id, name, true)
}

pub(crate) fn create_group_state_snapshot(
    provider: &KvStorageProvider,
    group_id: &GroupId,
    name: &str,
) -> StorageResult<()> {
    create(provider, group_id, name, false)
}

fn snapshot_blob(
    provider: &KvStorageProvider,
    group_id: &GroupId,
    name: &str,
) -> StorageResult<SnapshotBlob> {
    let row = provider
        .store()
        .get(&snapshot_row(group_id, name))?
        .ok_or_else(|| StorageError::SnapshotMissing(name.to_owned()))?;
    decode(&row)
}

/// Canonical-state restore shared by both snapshot scopes, in
/// `restore.rs:restore_snapshot` order — convergence policy, validation
/// marker, then `OpenMLS` rows (the policy applies unconditionally). The
/// order is observable: an error mid-restore inside a flattened nested unit
/// leaves the earlier writes in place.
fn restore_canonical(
    provider: &KvStorageProvider,
    group_id: &GroupId,
    image: &GroupStateImage,
    convergence_policy: Option<&[u8]>,
) -> StorageResult<()> {
    let gid = gid_seg(group_id);
    provider
        .store()
        .delete(&keys::key(Table::ConvergencePolicy, &[gid]))?;
    if let Some(policy) = convergence_policy {
        provider
            .store()
            .put(keys::key(Table::ConvergencePolicy, &[gid]), policy.to_vec())?;
    }
    provider
        .store()
        .delete(&keys::key(Table::MemberValidation, &[gid]))?;
    if let Some(marker) = &image.validated_tree_marker {
        provider
            .store()
            .put(keys::key(Table::MemberValidation, &[gid]), marker.clone())?;
    }
    provider.restore_group_openmls_rows(group_id, &image.openmls)
}

/// Checkpoint restore: canonical state without the policy column
/// (`restore.rs:restore_group_state`).
fn restore_checkpoint_state(
    provider: &KvStorageProvider,
    group_id: &GroupId,
    image: &GroupStateImage,
) -> StorageResult<()> {
    let gid = gid_seg(group_id);
    provider
        .store()
        .put(keys::key(Table::Group, &[gid]), image.group.clone())?;
    provider
        .store()
        .delete(&keys::key(Table::MemberValidation, &[gid]))?;
    if let Some(marker) = &image.validated_tree_marker {
        provider
            .store()
            .put(keys::key(Table::MemberValidation, &[gid]), marker.clone())?;
    }
    provider.restore_group_openmls_rows(group_id, &image.openmls)
}

/// `rollback` — a full restore. A state-scoped blob restores canonical
/// state only: `restore_snapshot` skips the absent ledger/queue fields
/// rather than failing on the narrower image. Step order mirrors
/// `restore.rs:restore_snapshot` (group, messages, queue, canonical tail) so
/// a mid-restore error inside a flattened nested unit leaves the same
/// partial writes as sqlite.
pub(crate) fn rollback(
    provider: &KvStorageProvider,
    group_id: &GroupId,
    name: &str,
) -> StorageResult<()> {
    provider.store().transact(|| -> StorageResult<()> {
        let snapshot = snapshot_blob(provider, group_id, name)?;
        provider.store().put(
            keys::key(Table::Group, &[gid_seg(group_id)]),
            snapshot.state.group.clone(),
        )?;
        if snapshot.full {
            if let Some(messages) = &snapshot.messages {
                restore_messages(provider, group_id, messages)?;
            }
            if let Some(queued) = &snapshot.queued_outbound {
                restore_queued(provider, group_id, queued)?;
            }
        }
        restore_canonical(
            provider,
            group_id,
            &snapshot.state,
            snapshot.convergence_policy.as_deref(),
        )
    })
}

/// `rollback_group_state` / `rollback_group_state_keeping_live_records`:
/// canonical state only, whatever the blob's scope.
pub(crate) fn rollback_group_state(
    provider: &KvStorageProvider,
    group_id: &GroupId,
    name: &str,
) -> StorageResult<()> {
    provider.store().transact(|| -> StorageResult<()> {
        let snapshot = snapshot_blob(provider, group_id, name)?;
        provider.store().put(
            keys::key(Table::Group, &[gid_seg(group_id)]),
            snapshot.state.group.clone(),
        )?;
        restore_canonical(
            provider,
            group_id,
            &snapshot.state,
            snapshot.convergence_policy.as_deref(),
        )
    })
}

/// The `seq8` segment of a `Table::Message` row key, decoded.
fn row_key_seq(row_key: &[u8]) -> StorageResult<u64> {
    let seq = keys::segments(row_key)
        .and_then(|segments| segments.get(1).copied())
        .ok_or_else(|| StorageError::Backend("kv row key malformed".into()))?;
    Ok(u64::from_be_bytes(seq.try_into().map_err(|_| {
        StorageError::Backend("kv row key malformed".into())
    })?))
}

/// `restore.rs:messages` — replace the ledger at captured positions, then
/// drop pending application events whose source message the rollback
/// discarded.
fn restore_messages(
    provider: &KvStorageProvider,
    group_id: &GroupId,
    messages: &[(u64, Vec<u8>)],
) -> StorageResult<()> {
    // `cgka_messages` carries `insert_order INTEGER PRIMARY KEY` and
    // `id UNIQUE`; the restore's explicit-order INSERT
    // (`restore.rs:messages` → `put_message_on_connection(Some(..))`) fails
    // when a surviving row of another group holds either, so the same
    // conflict must fail here.
    let mut survivor_orders = std::collections::BTreeSet::new();
    for (row_key, _) in provider.scan_rows(Table::Message, &[])? {
        let segments = keys::segments(&row_key)
            .ok_or_else(|| StorageError::Backend("kv row key malformed".into()))?;
        if segments.first().copied() == Some(gid_seg(group_id)) {
            continue;
        }
        survivor_orders.insert(row_key_seq(&row_key)?);
    }
    provider.delete_group_message_rows(group_id)?;
    for (seq, bytes) in messages {
        let record: cgka_traits::message::MessageRecord = ser::de_message(bytes)?;
        if provider.message_row_key(&record.id)?.is_some() || survivor_orders.contains(seq) {
            return Err(StorageError::Backend(
                "kv snapshot restore message conflict".into(),
            ));
        }
        provider.put_message_at(*seq, &record)?;
    }
    // Mirror `restore.rs:messages`' prune: pending deliveries of this
    // group survive only while their source message stands in the same
    // group (`NOT EXISTS` pairs id AND group_id).
    for (row_key, value) in provider.scan_rows(Table::PendingAppEvent, &[])? {
        let mut tuple = ser::Tuple::new(&value);
        let group = tuple.field()?;
        if group != gid_seg(group_id) {
            continue;
        }
        let mid = keys::segments(&row_key)
            .and_then(|segments| segments.last().copied())
            .ok_or_else(|| StorageError::Backend("kv row key malformed".into()))?
            .to_vec();
        let survives = provider
            .message_record(&cgka_traits::types::MessageId::new(mid))?
            .is_some_and(|record| record.group_id.as_slice() == group);
        if !survives {
            provider.store().delete(&row_key)?;
        }
    }
    Ok(())
}

/// `restore.rs:queued_outbound` — replace the group's queued intents at
/// their captured positions. `cgka_queued_outbound` carries
/// `insert_order INTEGER PRIMARY KEY` and `id UNIQUE`, and the restore's
/// plain `INSERT` fails when a surviving row of another group holds either
/// (e.g. an intent re-queued under a different group after the capture) —
/// mirror that conflict instead of overwriting the survivor.
fn restore_queued(
    provider: &KvStorageProvider,
    group_id: &GroupId,
    queued: &[(u64, Vec<u8>)],
) -> StorageResult<()> {
    let mut survivors: Vec<(u64, Vec<u8>)> = Vec::new();
    for (row_key, value) in provider.scan_rows(Table::QueuedIntent, &[])? {
        let (order, record) = KvStorageProvider::read_ordered::<QueuedOutboundIntent>(&value)?;
        if &record.group_id == group_id {
            provider.store().delete(&row_key)?;
        } else {
            survivors.push((order, record.id.as_slice().to_vec()));
        }
    }
    for (order, bytes) in queued {
        let record: QueuedOutboundIntent = ser::de(bytes)?;
        if survivors.iter().any(|(surviving_order, id)| {
            surviving_order == order || id.as_slice() == record.id.as_slice()
        }) {
            return Err(StorageError::Backend(
                "kv snapshot restore queued intent conflict".into(),
            ));
        }
        let mid = record.id.as_slice().to_vec();
        let mut out = Vec::new();
        ser::put_u64_field(&mut out, *order);
        ser::put_field(&mut out, bytes);
        provider
            .store()
            .put(keys::key(Table::QueuedIntent, &[&mid]), out)?;
    }
    Ok(())
}

pub(crate) fn release(
    provider: &KvStorageProvider,
    group_id: &GroupId,
    name: &str,
) -> StorageResult<()> {
    provider.store().ensure_writable()?;
    let row = snapshot_row(group_id, name);
    if !provider.store().contains(&row)? {
        return Err(StorageError::SnapshotMissing(name.to_owned()));
    }
    provider.store().delete(&row)
}

// Checkpoints -----------------------------------------------------------

/// The checkpoint row value: `resulting_epoch` plus the state blob.
fn encode_checkpoint_row(epoch: u64, blob: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    ser::put_u64_field(&mut out, epoch);
    ser::put_field(&mut out, blob);
    out
}

fn decode_checkpoint_row(row: &[u8]) -> StorageResult<(u64, Vec<u8>)> {
    let mut tuple = ser::Tuple::new(row);
    let epoch = tuple.u64()?;
    let blob = tuple.field()?.to_vec();
    tuple.finish()?;
    Ok((epoch, blob))
}

/// `create_on_connection` (`checkpoints.rs`): an existing row is an exact
/// retry only when the epoch and the captured state both match; any other
/// conflict is `AlreadyExists`.
pub(crate) fn create_checkpoint(
    provider: &KvStorageProvider,
    group_id: &GroupId,
    checkpoint: &GroupStateCheckpointRef,
) -> StorageResult<()> {
    provider.store().transact(|| -> StorageResult<()> {
        let blob = encode(&capture_state(provider, group_id)?)?;
        let row = checkpoint_row(group_id, &checkpoint.id);
        if let Some(existing) = provider.store().get(&row)? {
            let (epoch, existing_blob) = decode_checkpoint_row(&existing)?;
            if epoch == checkpoint.resulting_epoch.0 && existing_blob == blob {
                return Ok(());
            }
            return Err(StorageError::AlreadyExists);
        }
        provider.store().put(
            row,
            encode_checkpoint_row(checkpoint.resulting_epoch.0, &blob),
        )
    })
}

pub(crate) fn restore_checkpoint(
    provider: &KvStorageProvider,
    group_id: &GroupId,
    checkpoint_id: &str,
) -> StorageResult<()> {
    provider.store().transact(|| -> StorageResult<()> {
        let row = provider
            .store()
            .get(&checkpoint_row(group_id, checkpoint_id))?
            .ok_or_else(|| StorageError::SnapshotMissing(checkpoint_id.to_owned()))?;
        let (_, blob) = decode_checkpoint_row(&row)?;
        let image: GroupStateImage = decode(&blob)?;
        restore_checkpoint_state(provider, group_id, &image)
    })
}

/// `ORDER BY resulting_epoch, checkpoint_id` (`checkpoints.rs`).
pub(crate) fn list_checkpoints(
    provider: &KvStorageProvider,
    group_id: &GroupId,
) -> StorageResult<Vec<GroupStateCheckpointRef>> {
    let mut out = Vec::new();
    for (row_key, value) in provider
        .store()
        .scan(&keys::key(Table::Checkpoint, &[gid_seg(group_id)]))?
    {
        let id = String::from_utf8(
            keys::segments(&row_key)
                .and_then(|segments| segments.last().copied())
                .ok_or_else(|| StorageError::Backend("kv row key malformed".into()))?
                .to_vec(),
        )
        .map_err(|_| StorageError::Serialization("kv checkpoint id invalid utf-8".into()))?;
        let (epoch, _) = decode_checkpoint_row(&value)?;
        out.push(GroupStateCheckpointRef {
            id,
            resulting_epoch: cgka_traits::types::EpochId(epoch),
        });
    }
    out.sort_by(|a, b| {
        a.resulting_epoch
            .0
            .cmp(&b.resulting_epoch.0)
            .then_with(|| a.id.cmp(&b.id))
    });
    Ok(out)
}

pub(crate) fn release_checkpoint(
    provider: &KvStorageProvider,
    group_id: &GroupId,
    checkpoint_id: &str,
) -> StorageResult<()> {
    provider.store().ensure_writable()?;
    let row = checkpoint_row(group_id, checkpoint_id);
    if !provider.store().contains(&row)? {
        return Err(StorageError::SnapshotMissing(checkpoint_id.to_owned()));
    }
    provider.store().delete(&row)
}

/// `list_with_prefixes`: one scan of the snapshot keys — row-key order is
/// `(group_id, name)`, the same order sqlite's covering-index scan yields.
pub(crate) fn list_with_prefixes(
    provider: &KvStorageProvider,
    prefixes: &[&str],
) -> StorageResult<Vec<(GroupId, String)>> {
    let mut found = Vec::new();
    for (row_key, _) in provider.store().scan(&keys::key(Table::Snapshot, &[]))? {
        let Some(segments) = keys::segments(&row_key) else {
            continue;
        };
        let (Some(gid), Some(name)) = (segments.first(), segments.get(1)) else {
            continue;
        };
        let Ok(name) = String::from_utf8(name.to_vec()) else {
            continue;
        };
        if prefixes.iter().any(|prefix| name.starts_with(prefix)) {
            found.push((GroupId::new(gid.to_vec()), name));
        }
    }
    // The covering primary-key index sqlite scans yields (group_id, name)
    // byte order; the length-prefixed name segment does not.
    found.sort_by(|a, b| a.0.as_slice().cmp(b.0.as_slice()).then(a.1.cmp(&b.1)));
    Ok(found)
}

/// `replay_fingerprint_on_connection` (`capture.rs`): the live canonical
/// image, then every retained snapshot blob by name and every checkpoint by
/// id with its epoch — the same structure over this backend's encodings.
/// The domain tag differs from sqlite's because the bytes are
/// backend-local; the fingerprint is a memory-only value.
pub(crate) fn replay_fingerprint(
    provider: &KvStorageProvider,
    group_id: &GroupId,
) -> StorageResult<[u8; 32]> {
    let mut hash = sha2::Sha256::new();
    hash.update(b"mk-core-group-replay-state/v1");
    let live = encode(&capture_state(provider, group_id)?)?;
    hash.update((live.len() as u64).to_be_bytes());
    hash.update(&live);
    for domain in [Table::Snapshot, Table::Checkpoint] {
        hash.update(match domain {
            Table::Snapshot => b"snapshots".as_slice(),
            _ => b"checkpoints".as_slice(),
        });
        let mut rows: Vec<(String, u64, Vec<u8>)> = Vec::new();
        for (row_key, value) in provider
            .store()
            .scan(&keys::key(domain, &[gid_seg(group_id)]))?
        {
            let name = String::from_utf8(
                keys::segments(&row_key)
                    .and_then(|segments| segments.last().copied())
                    .ok_or_else(|| StorageError::Backend("kv row key malformed".into()))?
                    .to_vec(),
            )
            .map_err(|_| StorageError::Serialization("kv snapshot name invalid utf-8".into()))?;
            let (epoch, blob) = if domain == Table::Checkpoint {
                decode_checkpoint_row(&value)?
            } else {
                (0, value)
            };
            rows.push((name, epoch, blob));
        }
        rows.sort_by(|a, b| a.0.cmp(&b.0));
        for (name, epoch, blob) in rows {
            hash.update([1]);
            hash.update(epoch.to_be_bytes());
            hash.update((name.len() as u64).to_be_bytes());
            hash.update(name.as_bytes());
            hash.update((blob.len() as u64).to_be_bytes());
            hash.update(&blob);
        }
        hash.update([0]);
    }
    Ok(hash.finalize().into())
}
