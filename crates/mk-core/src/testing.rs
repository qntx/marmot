//! Test and harness support for the KV host boundary.
//!
//! [`RecordingHost`] is an in-memory [`crate::kv::KvHost`] that applies every
//! committed batch to a durable row map, recording each batch as it arrives.
//! It can replay what a real host would hold (`durable_rows`), drop future
//! commits (`fail_after`), or pretend an earlier commit was the last one the
//! host persisted (`cut_after`) — the fixtures reopen and migration tests,
//! the session durability gate, and the interop harness all need.

use std::collections::BTreeMap;
use std::sync::Mutex;

use crate::kv::{CommitSeq, KvBatch, KvHost, KvOp};
use crate::provider::KvStorageProvider;

/// An in-memory recording [`KvHost`].
///
/// `commit` applies the batch to the durable map strictly in order. The host
/// never mutates rows outside `commit` and never calls back into the store.
///
/// Failure injection is by commit count: [`Self::fail_after`] drops every
/// commit after the `n`th (the store keeps running against a silently stale
/// host — the crash window before the durability gate exists), and
/// [`Self::durable_rows`] reports exactly what a reopen would load.
#[derive(Debug, Default)]
pub struct RecordingHost {
    inner: Mutex<Inner>,
}

#[derive(Debug, Default)]
struct Inner {
    rows: BTreeMap<Vec<u8>, Vec<u8>>,
    batches: Vec<KvBatch>,
    /// Commits after this many are dropped instead of applied.
    fail_after: Option<usize>,
}

impl RecordingHost {
    /// A host with nothing durable yet.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Drop every commit after the `n`th. `0` drops them all.
    pub fn fail_after(&self, commits: usize) {
        let Ok(mut inner) = self.inner.lock() else {
            return;
        };
        inner.fail_after = Some(commits);
    }

    /// Accept all commits again.
    pub fn resume(&self) {
        let Ok(mut inner) = self.inner.lock() else {
            return;
        };
        inner.fail_after = None;
    }

    /// The batches this host has been asked to apply, in order.
    #[must_use]
    pub fn batches(&self) -> Vec<KvBatch> {
        self.inner
            .lock()
            .map(|inner| inner.batches.clone())
            .unwrap_or_default()
    }

    /// The committed batch sequence numbers, in order — the monotonicity
    /// probe for `seq` semantics.
    #[must_use]
    pub fn seqs(&self) -> Vec<u64> {
        self.inner
            .lock()
            .map(|inner| inner.batches.iter().map(|batch| batch.seq.get()).collect())
            .unwrap_or_default()
    }

    /// Every durable row, as a reopen hands them to
    /// [`crate::KvStorageProvider::open`].
    #[must_use]
    pub fn durable_rows(&self) -> Vec<(Vec<u8>, Vec<u8>)> {
        self.inner
            .lock()
            .map(|inner| {
                inner
                    .rows
                    .iter()
                    .map(|(key, value)| (key.clone(), value.clone()))
                    .collect()
            })
            .unwrap_or_default()
    }

    /// The durable rows as they would be had only the first `n` batches
    /// applied — a reopen point mid-history. `n` past the end clamps.
    #[must_use]
    pub fn rows_after_batches(&self, n: usize) -> Vec<(Vec<u8>, Vec<u8>)> {
        self.inner
            .lock()
            .map(|inner| {
                let mut rows = BTreeMap::new();
                for batch in inner.batches.iter().take(n) {
                    apply(&mut rows, batch);
                }
                rows.iter()
                    .map(|(key, value)| (key.clone(), value.clone()))
                    .collect()
            })
            .unwrap_or_default()
    }

    /// The seq of the last applied commit, for the session's durability ack.
    #[must_use]
    pub fn last_seq(&self) -> Option<CommitSeq> {
        self.inner
            .lock()
            .ok()
            .and_then(|inner| inner.batches.last().map(|batch| batch.seq))
    }
}

impl KvHost for RecordingHost {
    fn commit(&self, batch: KvBatch) {
        let Ok(mut inner) = self.inner.lock() else {
            return;
        };
        if inner
            .fail_after
            .is_some_and(|limit| inner.batches.len() >= limit)
        {
            inner.batches.push(batch);
            return;
        }
        apply(&mut inner.rows, &batch);
        inner.batches.push(batch);
    }
}

fn apply(rows: &mut BTreeMap<Vec<u8>, Vec<u8>>, batch: &KvBatch) {
    for op in &batch.ops {
        match op {
            KvOp::Put { key, value } => {
                rows.insert(key.clone(), value.clone());
            }
            KvOp::Delete { key } => {
                rows.remove(key);
            }
        }
    }
}

impl KvStorageProvider {
    /// Distinct capability descriptions interned so far. `KNOWN`
    /// descriptions resolve to statics without interning, so a provider
    /// that only ever saw registered descriptions reports `0`.
    #[must_use]
    pub fn interned_description_count(&self) -> usize {
        self.interned_count()
    }
}
