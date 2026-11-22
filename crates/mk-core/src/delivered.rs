//! The delivered-message index, modifier holds and the retention sweep.
//!
//! [`DeliveredRow`] is the durable half of the session's per-message fact
//! table ([`Table::DeliveredMessage`], keyed `group/inner_event_id`): one
//! row for every delivered or own-sent application message carrying its
//! author, kind, transport id and retention expiry. Kind 1009/5 modifiers
//! authorize against it, the lazily-built expiry heap sweeps it, and group
//! deletion removes every row with the group. [`PacingRow`]
//! ([`Table::SendPacing`]) holds the last `created_at` second the send
//! allocator issued.
//!
//! Held modifiers and the heap itself are memory-only: an unacked pending
//! row re-presents a held modifier after restart, and the heap rebuilds
//! from the table on the first `tick`.

use std::cmp::Reverse;
use std::collections::{BinaryHeap, VecDeque};

use cgka_traits::app_event::MARMOT_APP_EVENT_KIND_AGENT_STREAM_START;
use cgka_traits::storage::{MessageStorage, StorageError, StorageResult};
use cgka_traits::{GroupId as EngineGroupId, MessageId};
use serde::{Deserialize, Serialize};

use crate::events::{AgentStreamAnchor, ReceivedMessage, SessionEvent};
use crate::keys::Table;
use crate::provider::{KvStorageProvider, gid_seg};
use crate::semantics::{self, Modifier, Resolution, TargetFacts};
use crate::session::{Session, SessionError};
use crate::types::{AccountId, EventId, GroupId};

/// Held modifiers per group; beyond it the oldest drops `unverifiable`.
pub(crate) const MAX_HELD_PER_GROUP: usize = 128;
/// Held modifier bytes per group; beyond it the oldest drops
/// `unverifiable` — a peer cannot pin unbounded memory with large edits.
pub(crate) const MAX_HELD_BYTES_PER_GROUP: usize = 4 * 1024 * 1024;
/// How long a held modifier waits for its targets before expiring
/// `unverifiable` — a bound on acceptable relay reordering.
pub(crate) const HOLD_WINDOW_MS: u64 = 300_000;
/// Expired index rows retired per `tick`; a remainder wakes it immediately.
pub(crate) const MAX_EXPIRY_SWEEP_PER_TICK: usize = 256;

const DELIVERED_ENCODE: &str = "kv serde encode: delivered";
const DELIVERED_DECODE: &str = "kv serde decode: delivered";
const DELIVERED_KEY: &str = "kv decode: delivered key";
const PACING_ENCODE: &str = "kv serde encode: pacing";
const PACING_DECODE: &str = "kv serde decode: pacing";

fn storage_err(message: &'static str) -> StorageError {
    StorageError::Backend(message.to_owned())
}

/// One `DeliveredMessage` row: the facts a modifier checks its target
/// against, plus the retention expiry the sweep reads.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct DeliveredRow {
    /// Inner event author.
    #[serde(with = "crate::ser::fixed")]
    pub author: [u8; 32],
    /// Inner event kind.
    pub kind: u64,
    /// The kind-445 transport event id — also the `MediaSecret` lookup key.
    #[serde(with = "crate::ser::fixed")]
    pub transport_id: [u8; 32],
    /// Retention expiry, unix seconds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires_at: Option<u64>,
}

/// One `SendPacing` row: the last `created_at` second issued for the group.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct PacingRow {
    /// Allocated `created_at` second; a restarted session treats it as full.
    pub second: u64,
}

/// A modifier mapped and held until its targets land in the index. The
/// pending row keeps it restart-safe: it re-presents as a fresh modifier
/// with a fresh window.
#[derive(Debug)]
pub(crate) struct HeldModifier {
    /// The mapped message; `semantics` is a placeholder until resolution.
    pub message: ReceivedMessage,
    /// The parsed modifier (edit content / delete targets).
    pub modifier: Modifier,
    /// Hold deadline on the session clock, ms.
    pub deadline_ms: u64,
}

impl HeldModifier {
    /// Its transport id as the pending-row key.
    fn pending_id(&self) -> MessageId {
        MessageId::new(self.message.transport_id.0.to_vec())
    }

    /// The bytes the hold counts toward `MAX_HELD_BYTES_PER_GROUP`: the
    /// message content plus every tag string.
    fn held_bytes(&self) -> usize {
        self.message.content.len()
            + self
                .message
                .tags
                .iter()
                .flatten()
                .map(String::len)
                .sum::<usize>()
    }
}

/// One group's hold queue with its running byte total. `bytes` is the
/// sum of every held entry's `held_bytes`, maintained on push and on
/// every removal path (release, expiry, eviction, invalidation) so both
/// caps check in O(1).
#[derive(Debug, Default)]
pub(crate) struct HeldQueue {
    /// Arrival-order hold queue — also the eviction order.
    pub(crate) queue: VecDeque<HeldModifier>,
    /// Total `held_bytes` of every entry in `queue`.
    pub(crate) bytes: usize,
}

impl HeldQueue {
    /// Push `entry`, adding its size to `bytes`.
    fn push_back(&mut self, entry: HeldModifier) {
        self.bytes += entry.held_bytes();
        self.queue.push_back(entry);
    }

    /// Pop the oldest entry, subtracting its size.
    fn pop_front(&mut self) -> Option<HeldModifier> {
        let entry = self.queue.pop_front()?;
        self.bytes -= entry.held_bytes();
        Some(entry)
    }

    /// Remove the entry at `index`, subtracting its size.
    fn remove(&mut self, index: usize) -> Option<HeldModifier> {
        let entry = self.queue.remove(index)?;
        self.bytes -= entry.held_bytes();
        Some(entry)
    }
}

/// Min-heap entry: `(expires_at, group, inner)` — lazily validated on pop
/// because index-row deletions do not retract heap entries.
pub(crate) type ExpiryHeap = BinaryHeap<Reverse<(u64, Vec<u8>, [u8; 32])>>;

impl KvStorageProvider {
    /// Write one delivered-message index row.
    pub(crate) fn put_delivered(
        &self,
        group_id: &EngineGroupId,
        inner: &EventId,
        row: &DeliveredRow,
    ) -> StorageResult<()> {
        let value = crate::ser::ser(row).map_err(|_| storage_err(DELIVERED_ENCODE))?;
        self.put_row(
            Table::DeliveredMessage,
            &[gid_seg(group_id), &inner.0],
            value,
        )
    }

    /// The index row for `inner`, if any.
    pub(crate) fn delivered_row(
        &self,
        group_id: &EngineGroupId,
        inner: &EventId,
    ) -> StorageResult<Option<DeliveredRow>> {
        self.row(Table::DeliveredMessage, &[gid_seg(group_id), &inner.0])?
            .map(|value| crate::ser::de(&value).map_err(|_| storage_err(DELIVERED_DECODE)))
            .transpose()
    }

    /// Delete `inner`'s index row.
    pub(crate) fn delete_delivered(
        &self,
        group_id: &EngineGroupId,
        inner: &EventId,
    ) -> StorageResult<()> {
        self.del_row(Table::DeliveredMessage, &[gid_seg(group_id), &inner.0])
    }

    /// Delete the group row whose `transport_id` matches — invalidation is
    /// transport-keyed while the index is inner-keyed.
    pub(crate) fn delete_delivered_by_transport(
        &self,
        group_id: &EngineGroupId,
        transport_id: &[u8; 32],
    ) -> StorageResult<()> {
        for (key, value) in self.scan_rows(Table::DeliveredMessage, &[gid_seg(group_id)])? {
            let row: DeliveredRow =
                crate::ser::de(&value).map_err(|_| storage_err(DELIVERED_DECODE))?;
            if &row.transport_id == transport_id {
                self.store().delete(&key)?;
            }
        }
        Ok(())
    }

    /// Every index row across all groups, as `(group, inner, row)` — the
    /// expiry-heap seed.
    pub(crate) fn delivered_all(&self) -> StorageResult<Vec<(Vec<u8>, EventId, DeliveredRow)>> {
        let mut out = Vec::new();
        for (key, value) in self.scan_rows(Table::DeliveredMessage, &[])? {
            let Some(segments) = crate::keys::segments(&key) else {
                continue;
            };
            let [gid, inner] = segments.as_slice() else {
                continue;
            };
            let inner: [u8; 32] =
                <[u8; 32]>::try_from(*inner).map_err(|_| storage_err(DELIVERED_KEY))?;
            let row: DeliveredRow =
                crate::ser::de(&value).map_err(|_| storage_err(DELIVERED_DECODE))?;
            out.push((gid.to_vec(), EventId(inner), row));
        }
        Ok(out)
    }

    /// The group's pacing row, if any.
    pub(crate) fn pacing_row(&self, group_id: &EngineGroupId) -> StorageResult<Option<PacingRow>> {
        self.row(Table::SendPacing, &[gid_seg(group_id)])?
            .map(|value| crate::ser::de(&value).map_err(|_| storage_err(PACING_DECODE)))
            .transpose()
    }

    /// Write the group's pacing row.
    pub(crate) fn put_pacing_row(
        &self,
        group_id: &EngineGroupId,
        row: &PacingRow,
    ) -> StorageResult<()> {
        let value = crate::ser::ser(row).map_err(|_| storage_err(PACING_ENCODE))?;
        self.put_row(Table::SendPacing, &[gid_seg(group_id)], value)
    }
}

impl Session {
    /// Resolve `entry` against the index: `Hold` pushes it to the group's
    /// queue (evicting the oldest on overflow), `Deliver` patches the
    /// placeholder semantics, indexes the message, surfaces it and runs the
    /// release loop; `Drop` emits `AppEventDropped` and releases the pending
    /// row. Runs inside the caller's storage unit.
    pub(crate) fn resolve_modifier(
        &mut self,
        group: &GroupId,
        entry: HeldModifier,
        final_attempt: bool,
        out: &mut Vec<SessionEvent>,
    ) -> StorageResult<()> {
        let group_id = EngineGroupId::new(group.0.clone());
        let mut read_error = None;
        let resolution = semantics::resolve(
            &entry.modifier,
            &entry.message.sender,
            |target| match self.provider.delivered_row(&group_id, target) {
                Ok(row) => row.map(|row| TargetFacts {
                    author: AccountId(row.author),
                    kind: row.kind,
                }),
                Err(error) => {
                    read_error = Some(error);
                    None
                }
            },
            final_attempt,
        );
        if let Some(error) = read_error {
            return Err(error);
        }
        match resolution {
            Resolution::Hold => self.hold_modifier(group, entry, out),
            Resolution::Deliver(semantics) => {
                let mut message = entry.message;
                message.semantics = semantics;
                self.index_received(group, &message)?;
                out.push(SessionEvent::MessageReceived {
                    group: group.clone(),
                    message,
                });
                self.release_held(group, out)
            }
            Resolution::Drop(reason) => {
                out.push(SessionEvent::AppEventDropped {
                    group: group.clone(),
                    reason,
                });
                self.provider
                    .delete_pending_application_events(&[entry.pending_id()])
            }
        }
    }

    /// Index a surfaced (or released) application message and enqueue its
    /// expiry.
    pub(crate) fn index_received(
        &mut self,
        group: &GroupId,
        message: &ReceivedMessage,
    ) -> StorageResult<()> {
        let inner =
            semantics::event_id_of_hex(&message.id).ok_or_else(|| storage_err(DELIVERED_KEY))?;
        self.index_write(
            group,
            &inner,
            &DeliveredRow {
                author: message.sender.0,
                kind: message.kind,
                transport_id: message.transport_id.0,
                expires_at: message.expires_at,
            },
        )
    }

    /// Index an agent-stream anchor the same way — it authorizes nothing
    /// but sweeps like any row.
    pub(crate) fn index_anchor(
        &mut self,
        group: &GroupId,
        start: &AgentStreamAnchor,
    ) -> StorageResult<()> {
        let inner =
            semantics::event_id_of_hex(&start.id).ok_or_else(|| storage_err(DELIVERED_KEY))?;
        self.index_write(
            group,
            &inner,
            &DeliveredRow {
                author: start.sender.0,
                kind: MARMOT_APP_EVENT_KIND_AGENT_STREAM_START,
                transport_id: start.transport_id.0,
                expires_at: start.expires_at,
            },
        )
    }

    /// Record a message this device sent — its own row is what authorizes
    /// a sibling device's modifier against it.
    pub(crate) fn index_own_send(
        &mut self,
        group: &GroupId,
        inner: &EventId,
        kind: u64,
        transport_id: [u8; 32],
        expires_at: Option<u64>,
    ) -> StorageResult<()> {
        self.index_write(
            group,
            inner,
            &DeliveredRow {
                author: self.account().0,
                kind,
                transport_id,
                expires_at,
            },
        )
    }

    /// The actual row write plus the heap push — the one place both happen.
    fn index_write(
        &mut self,
        group: &GroupId,
        inner: &EventId,
        row: &DeliveredRow,
    ) -> StorageResult<()> {
        self.provider
            .put_delivered(&EngineGroupId::new(group.0.clone()), inner, row)?;
        if let (Some(heap), Some(expires_at)) = (&mut self.expiry_heap, row.expires_at) {
            heap.push(Reverse((expires_at, group.0.clone(), inner.0)));
        }
        Ok(())
    }

    /// Push `entry` to the group's hold queue (deduped by transport id) and
    /// resolve the oldest finally while either bound overflows — entry
    /// count or held bytes. An entry that alone exceeds the byte cap
    /// resolves immediately once the queue in front of it drains.
    fn hold_modifier(
        &mut self,
        group: &GroupId,
        entry: HeldModifier,
        out: &mut Vec<SessionEvent>,
    ) -> StorageResult<()> {
        {
            let queue = self.held_modifiers.entry(group.0.clone()).or_default();
            if queue
                .queue
                .iter()
                .any(|held| held.message.transport_id == entry.message.transport_id)
            {
                return Ok(());
            }
            queue.push_back(entry);
        }
        while self.held_modifiers.get(&group.0).is_some_and(|queue| {
            queue.queue.len() > MAX_HELD_PER_GROUP || queue.bytes > MAX_HELD_BYTES_PER_GROUP
        }) {
            let Some(oldest) = self
                .held_modifiers
                .get_mut(&group.0)
                .and_then(HeldQueue::pop_front)
            else {
                break;
            };
            self.resolve_modifier(group, oldest, true, out)?;
        }
        Ok(())
    }

    /// The earliest held entry whose targets are all indexed, or `None`.
    fn first_releasable(&self, group: &GroupId) -> StorageResult<Option<usize>> {
        let Some(queue) = self.held_modifiers.get(&group.0) else {
            return Ok(None);
        };
        let group_id = EngineGroupId::new(group.0.clone());
        for (index, held) in queue.queue.iter().enumerate() {
            if self.all_indexed(&group_id, held.modifier.targets())? {
                return Ok(Some(index));
            }
        }
        Ok(None)
    }

    /// Every target has a durable index row.
    fn all_indexed(&self, group_id: &EngineGroupId, targets: &[EventId]) -> StorageResult<bool> {
        for target in targets {
            if self.provider.delivered_row(group_id, target)?.is_none() {
                return Ok(false);
            }
        }
        Ok(true)
    }

    /// Release every held entry whose targets are now all indexed, in
    /// arrival order, to a fixpoint — a delivered entry may itself release
    /// another (a kind 5 voiding a held edit).
    pub(crate) fn release_held(
        &mut self,
        group: &GroupId,
        out: &mut Vec<SessionEvent>,
    ) -> StorageResult<()> {
        while let Some(index) = self.first_releasable(group)? {
            let Some(entry) = self
                .held_modifiers
                .get_mut(&group.0)
                .and_then(|queue| queue.remove(index))
            else {
                return Ok(());
            };
            self.resolve_modifier(group, entry, true, out)?;
        }
        Ok(())
    }

    /// Invalidation: drop held entries carrying the invalidated transport
    /// id (releasing their pending rows) and delete the index row the
    /// transport id maps to. Runs inside the caller's storage unit.
    pub(crate) fn invalidate_transport(
        &mut self,
        group: &GroupId,
        transport: &EventId,
    ) -> StorageResult<()> {
        let mut pending = Vec::new();
        if let Some(queue) = self.held_modifiers.get_mut(&group.0) {
            let (dropped, kept): (VecDeque<HeldModifier>, VecDeque<HeldModifier>) = queue
                .queue
                .drain(..)
                .partition(|held| held.message.transport_id == *transport);
            pending.extend(dropped.iter().map(HeldModifier::pending_id));
            queue.bytes -= dropped.iter().map(HeldModifier::held_bytes).sum::<usize>();
            queue.queue = kept;
        }
        self.provider.delete_pending_application_events(&pending)?;
        self.provider
            .delete_delivered_by_transport(&EngineGroupId::new(group.0.clone()), &transport.0)
    }

    /// `tick`'s durable housekeeping in one storage unit: held modifiers
    /// whose window closed resolve finally, then expired index rows sweep
    /// out (bounded). Produced events queue into `self.tick_events`;
    /// returns the wake contribution — `Some(0)` while sweepable entries
    /// remain past the sweep bound.
    pub(crate) fn process_expiry_and_holds(
        &mut self,
        now_ms: u64,
    ) -> Result<Option<u64>, SessionError> {
        if self.expiry_heap.is_none() {
            self.expiry_heap = Some(self.build_expiry_heap().map_err(SessionError::Storage)?);
        }
        let now_secs = now_ms / 1_000;
        let store = self.provider.store().clone();
        let mut popped: Vec<(u64, Vec<u8>, [u8; 32])> = Vec::new();
        let produced = store.transact(|| -> StorageResult<Vec<SessionEvent>> {
            let mut out = Vec::new();
            self.expire_due_holds(now_ms, &mut out)?;
            self.sweep_due_rows(now_secs, &mut popped, &mut out)?;
            Ok(out)
        });
        match produced {
            Ok(events) => self.tick_events.extend(events),
            Err(error) => {
                // Heap pops rolled back with the unit: restore them — the
                // rows still exist, so the entries must too.
                if let Some(heap) = &mut self.expiry_heap {
                    heap.extend(popped.into_iter().map(Reverse));
                }
                return Err(SessionError::Storage(error));
            }
        }
        Ok(self.expiry_wake(now_ms))
    }

    /// Held modifiers past `now_ms` resolve finally, in group-then-arrival
    /// order. Runs inside the caller's storage unit.
    fn expire_due_holds(&mut self, now_ms: u64, out: &mut Vec<SessionEvent>) -> StorageResult<()> {
        let gids: Vec<Vec<u8>> = self.held_modifiers.keys().cloned().collect();
        for gid in gids {
            while let Some(entry) = self.pop_due_hold(&gid, now_ms) {
                self.resolve_modifier(&GroupId(gid.clone()), entry, true, out)?;
            }
        }
        Ok(())
    }

    /// The queue's front entry iff its deadline passed.
    fn pop_due_hold(&mut self, gid: &[u8], now_ms: u64) -> Option<HeldModifier> {
        self.held_modifiers
            .get_mut(gid)
            .filter(|queue| {
                queue
                    .queue
                    .front()
                    .is_some_and(|held| held.deadline_ms <= now_ms)
            })
            .and_then(HeldQueue::pop_front)
    }

    /// Up to `MAX_EXPIRY_SWEEP_PER_TICK` due rows: delete index row and
    /// media secret, emit `MessageExpired`, record each pop in `popped`
    /// so a failed unit can restore the heap.
    fn sweep_due_rows(
        &mut self,
        now_secs: u64,
        popped: &mut Vec<(u64, Vec<u8>, [u8; 32])>,
        out: &mut Vec<SessionEvent>,
    ) -> StorageResult<()> {
        for _ in 0..MAX_EXPIRY_SWEEP_PER_TICK {
            let Some(heap) = &mut self.expiry_heap else {
                break;
            };
            if heap.peek().is_none_or(|entry| entry.0.0 > now_secs) {
                break;
            }
            let Some(entry) = heap.pop() else { break };
            let Reverse((expires_at, gid, inner)) = entry;
            popped.push((expires_at, gid.clone(), inner));
            self.sweep_row(expires_at, &gid, EventId(inner), out)?;
        }
        Ok(())
    }

    /// One lazy-validated sweep: the row must still exist with the same
    /// expiry, else the heap entry was stale.
    fn sweep_row(
        &self,
        expires_at: u64,
        gid: &[u8],
        inner_id: EventId,
        out: &mut Vec<SessionEvent>,
    ) -> StorageResult<()> {
        let group_id = EngineGroupId::new(gid.to_vec());
        let Some(row) = self.provider.delivered_row(&group_id, &inner_id)? else {
            return Ok(());
        };
        if row.expires_at != Some(expires_at) {
            return Ok(());
        }
        self.provider.delete_delivered(&group_id, &inner_id)?;
        self.provider
            .delete_media_secret(&group_id, &MessageId::new(row.transport_id.to_vec()))?;
        out.push(SessionEvent::MessageExpired {
            group: GroupId(gid.to_vec()),
            message: inner_id,
            transport_id: EventId(row.transport_id),
        });
        Ok(())
    }

    /// Seed the heap from every indexed row carrying an expiry.
    fn build_expiry_heap(&self) -> StorageResult<ExpiryHeap> {
        let mut heap = BinaryHeap::new();
        for (gid, inner, row) in self.provider.delivered_all()? {
            if let Some(expires_at) = row.expires_at {
                heap.push(Reverse((expires_at, gid, inner.0)));
            }
        }
        Ok(heap)
    }

    /// The wake contribution of the earliest hold deadline and heap top.
    fn expiry_wake(&self, now_ms: u64) -> Option<u64> {
        let now_secs = now_ms / 1_000;
        let mut wake: Option<u64> = None;
        for queue in self.held_modifiers.values() {
            if let Some(held) = queue.queue.front() {
                let delay = held.deadline_ms.saturating_sub(now_ms);
                wake = Some(wake.map_or(delay, |current| current.min(delay)));
            }
        }
        if let Some(entry) = self.expiry_heap.as_ref().and_then(BinaryHeap::peek) {
            let expires_at = entry.0.0;
            let delay = if expires_at <= now_secs {
                0
            } else {
                expires_at.saturating_mul(1_000).saturating_sub(now_ms)
            };
            wake = Some(wake.map_or(delay, |current| current.min(delay)));
        }
        wake
    }

    /// Drop all in-memory holds and pacing state of a forgotten group.
    pub(crate) fn drop_group_memory(&mut self, group: &GroupId) {
        self.held_modifiers.remove(&group.0);
        self.pacing.remove(&group.0);
    }
}
