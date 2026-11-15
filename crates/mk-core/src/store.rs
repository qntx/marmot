//! The in-memory mirror plus transaction overlays.
//!
//! `KvStore` holds a committed `BTreeMap` mirror of the durable rows and a
//! stack of overlay layers — one per open `with_transaction` or
//! `with_group_savepoint` scope; `with_read_snapshot` is a write-forbidding
//! flag over the live view, not a layer. Reads see through
//! the top of the stack into committed state; writes land in the top overlay
//! (or, with no overlay, go straight to committed and emit one batch). A
//! scope's closure returning `Err` — or unwinding — drops its overlay through
//! the guard, so rollback needs no `catch_unwind`: the layer is discarded on
//! drop either way. Only the outermost successful scope merges into committed
//! state and emits exactly one [`KvBatch`].
//!
//! `after_commit` callbacks queue per layer, so a rolled-back layer's
//! callbacks die with it and a nested layer's callbacks only run when the
//! outermost commit lands.
//!
//! Lock poisoning maps to [`StorageError::Backend`]; it never panics.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, MutexGuard};

use cgka_traits::storage::{StorageError, StorageResult};

use crate::error::{MetaRow, OpenError};
use crate::keys::{self, Table};
use crate::kv::{CommitSeq, KvBatch, KvHost, KvOp};

type Overlay = BTreeMap<Vec<u8>, Option<Vec<u8>>>;

struct Inner {
    /// Committed mirror of every durable row.
    rows: BTreeMap<Vec<u8>, Vec<u8>>,
    /// Uncommitted layers, bottom to top.
    overlays: Vec<Overlay>,
    /// Per-layer after-commit callbacks, parallel to `overlays`.
    callbacks: Vec<Vec<Box<dyn FnOnce() + Send + 'static>>>,
    /// Batches emitted so far; `seq` of the next batch is `batches + 1`.
    batches: u64,
    /// Count of committed batches (the engine's cache-invalidation counter).
    write_gen: u64,
    /// Monotonic insertion-order counter, mirrored from `META_SEQUENCE`.
    sequence: u64,
    /// While `true`, every write path fails: `with_read_snapshot`'s
    /// read-only scope. The flag freezes the view by enforcement — no clone.
    read_only: bool,
}

/// The mirror plus transactional units over a host KV port.
#[derive(Clone)]
pub(crate) struct KvStore {
    host: Arc<dyn KvHost>,
    inner: Arc<Mutex<Inner>>,
}

impl KvStore {
    /// Open a store over `rows`, applying the schema-version contract:
    ///
    /// - no rows → a fresh store; the version row, sequence counter, and
    ///   pending-reference high-water are written in the first emitted batch;
    /// - a version row equal to this build's version → open;
    /// - a different version → [`OpenError::UnsupportedSchemaVersion`];
    /// - rows but no version row → [`OpenError::MissingSchemaVersion`];
    /// - a version/meta row that will not decode → [`OpenError::MalformedMeta`].
    ///
    /// There is deliberately no migration: a store this build did not write is
    /// refused, not guessed at.
    ///
    /// # Errors
    ///
    /// [`OpenError`] per the cases above.
    pub(crate) fn open(
        rows: Vec<(Vec<u8>, Vec<u8>)>,
        host: Arc<dyn KvHost>,
    ) -> Result<Self, OpenError> {
        let rows: BTreeMap<Vec<u8>, Vec<u8>> = rows.into_iter().collect();
        let version_key = keys::key(Table::Meta, &[keys::META_SCHEMA_VERSION]);
        if rows.is_empty() {
            let store = Self {
                host,
                inner: Arc::new(Mutex::new(Inner {
                    rows: BTreeMap::new(),
                    overlays: Vec::new(),
                    callbacks: Vec::new(),
                    batches: 0,
                    write_gen: 0,
                    sequence: 0,
                    read_only: false,
                })),
            };
            let mut pending: Overlay = BTreeMap::new();
            pending.insert(
                version_key,
                Some(keys::SCHEMA_VERSION.to_be_bytes().to_vec()),
            );
            pending.insert(
                keys::key(Table::Meta, &[keys::META_SEQUENCE]),
                Some(0_u64.to_be_bytes().to_vec()),
            );
            pending.insert(
                keys::key(Table::Meta, &[keys::META_PENDING_HIGH_WATER]),
                Some(0_u64.to_be_bytes().to_vec()),
            );
            {
                let mut inner = store.inner.lock().map_err(|_| OpenError::LockPoisoned)?;
                inner.commit_pending(store.host.as_ref(), pending);
            }
            return Ok(store);
        }
        let version = rows
            .get(&version_key)
            .ok_or(OpenError::MissingSchemaVersion)?;
        let found = read_u32(version).ok_or(OpenError::MalformedMeta {
            row: MetaRow::SchemaVersion,
        })?;
        if found != keys::SCHEMA_VERSION {
            return Err(OpenError::UnsupportedSchemaVersion { found });
        }
        let sequence = rows
            .get(&keys::key(Table::Meta, &[keys::META_SEQUENCE]))
            .and_then(|v| read_u64(v))
            .ok_or(OpenError::MalformedMeta {
                row: MetaRow::Sequence,
            })?;
        rows.get(&keys::key(Table::Meta, &[keys::META_PENDING_HIGH_WATER]))
            .and_then(|v| read_u64(v))
            .ok_or(OpenError::MalformedMeta {
                row: MetaRow::PendingHighWater,
            })?;
        Ok(Self {
            host,
            inner: Arc::new(Mutex::new(Inner {
                rows,
                overlays: Vec::new(),
                callbacks: Vec::new(),
                batches: 0,
                write_gen: 0,
                sequence,
                read_only: false,
            })),
        })
    }

    /// Lock poisoning recovery: a poisoned lock means a panic happened while a
    /// writer held it; mirror integrity is not guaranteed — surface it as a
    /// backend error, never a panic.
    fn lock(&self) -> StorageResult<MutexGuard<'_, Inner>> {
        self.inner
            .lock()
            .map_err(|_| StorageError::Backend("kv store lock poisoned".into()))
    }

    /// Run `f` as one unit: a fresh overlay on the stack. `Ok` merges the layer
    /// into the parent (or, for the outermost scope, commits one batch to the
    /// host); `Err` — and a panic unwinding through `f` — discards the layer
    /// and every callback it queued.
    ///
    /// # Errors
    ///
    /// `f`'s error, or [`StorageError`] on lock poisoning.
    /// `with_transaction`: the outermost call owns the commit; a nested call
    /// runs `f` inline in the open unit — SQLite's nested-transaction
    /// passthrough (`connection.rs` — the outer SQL transaction owns
    /// rollback/commit), so an inner `Err` leaves its writes in place and it
    /// is the enclosing unit that decides whether anything undoes.
    pub(crate) fn transact<T, E, F>(&self, f: F) -> Result<T, E>
    where
        E: From<StorageError>,
        F: FnOnce() -> Result<T, E>,
    {
        {
            let mut inner = self.lock().map_err(E::from)?;
            if !inner.overlays.is_empty() {
                drop(inner);
                return f();
            }
            // Under `read_only` the layer still opens — sqlite's nested
            // `with_transaction` inside `query_only` runs the callback too;
            // each individual write inside fails at `Inner::write` (the
            // `query_only` equivalent) rather than the scope itself erroring.
            inner.overlays.push(BTreeMap::new());
            inner.callbacks.push(Vec::new());
        }
        // The guard pops the overlay on drop — covering `Err` below *and* any
        // unwind through `f` — unless `commit` disarms it first.
        let mut guard = LayerGuard {
            store: self,
            armed: true,
        };
        let value = f()?;
        guard.commit().map_err(E::from)?;
        Ok(value)
    }

    /// Whether a write unit (transaction or savepoint) is currently open.
    pub(crate) fn in_unit(&self) -> bool {
        self.inner
            .lock()
            .is_ok_and(|inner| !inner.overlays.is_empty())
    }

    /// `with_group_savepoint`: always pushes a layer — a `SAVEPOINT` is a real
    /// rollback scope even inside an open transaction.
    pub(crate) fn savepoint<T, E, F>(&self, f: F) -> Result<T, E>
    where
        E: From<StorageError>,
        F: FnOnce() -> Result<T, E>,
    {
        {
            let mut inner = self.lock().map_err(E::from)?;
            inner.overlays.push(BTreeMap::new());
            inner.callbacks.push(Vec::new());
        }
        let mut guard = LayerGuard {
            store: self,
            armed: true,
        };
        let value = f()?;
        guard.commit().map_err(E::from)?;
        Ok(value)
    }

    /// Run `f` over a consistent view of current state; every write inside
    /// fails. Reads in `f` see the row set at entry, including any enclosing
    /// unit's uncommitted writes — the read-your-writes part of SQLite's
    /// read-only deferred transaction inside a write transaction.
    ///
    /// No rows are copied: the view is the live mirror held still by
    /// enforcement. While the scope is open, every path that could mutate —
    /// `put`/`delete`, `transact`, `savepoint` — fails under the lock before
    /// touching state, so committed rows and the overlay stack cannot move
    /// beneath the reader. The store has a single writer (whoever holds the
    /// lock for a write), and that writer is refused for the scope's
    /// duration; the layered view it would have mutated is therefore exactly
    /// the snapshot. Cost is O(1) per call.
    ///
    /// # Errors
    ///
    /// `f`'s error, or [`StorageError`] on lock poisoning.
    pub(crate) fn read_snapshot<T, E, F>(&self, f: F) -> Result<T, E>
    where
        E: From<StorageError>,
        F: FnOnce() -> Result<T, E>,
    {
        {
            let mut inner = self.lock().map_err(E::from)?;
            if inner.read_only {
                // Nested read snapshot: sqlite's `with_read_snapshot` joins
                // the thread-owned deferred transaction and just runs the
                // callback. The outer scope's `ReadGuard` owns the flag —
                // nothing to track for the inner scope.
                drop(inner);
                return f();
            }
            inner.read_only = true;
        }
        let guard = ReadGuard { store: self };
        let out = f();
        drop(guard);
        out
    }

    /// Queue `callback` to run after the outermost commit, or run it now when
    /// no unit is open. A rolled-back layer's callbacks are discarded with it.
    pub(crate) fn after_commit(&self, callback: Box<dyn FnOnce() + Send + 'static>) {
        let mut callback = Some(callback);
        if let Ok(mut inner) = self.inner.lock()
            && let Some(queue) = inner.callbacks.last_mut()
            && let Some(callback) = callback.take()
        {
            queue.push(callback);
        }
        if let Some(callback) = callback {
            callback();
        }
    }

    /// Fail when a read snapshot forbids writes — the `query_only`
    /// equivalent, checked at mutation entry so a read-then-write method
    /// cannot short-circuit to `NotFound` before the write that sqlite would
    /// have failed first.
    pub(crate) fn ensure_writable(&self) -> StorageResult<()> {
        if self.lock()?.read_only {
            return Err(StorageError::Backend(
                "kv write inside a read snapshot".into(),
            ));
        }
        Ok(())
    }

    /// A single read through all layers.
    pub(crate) fn get(&self, key: &[u8]) -> StorageResult<Option<Vec<u8>>> {
        Ok(self.lock()?.get(key))
    }

    /// Does `key` resolve to a live row.
    pub(crate) fn contains(&self, key: &[u8]) -> StorageResult<bool> {
        Ok(self.get(key)?.is_some())
    }

    /// Insert or replace one row inside the current unit (or as a bare unit).
    pub(crate) fn put(&self, key: Vec<u8>, value: Vec<u8>) -> StorageResult<()> {
        let mut inner = self.lock()?;
        inner.write(self.host.as_ref(), key, Some(value))
    }

    /// Delete one row inside the current unit (or as a bare unit).
    pub(crate) fn delete(&self, key: &[u8]) -> StorageResult<()> {
        let mut inner = self.lock()?;
        inner.write(self.host.as_ref(), key.to_vec(), None)
    }

    /// All `(key, value)` rows under `prefix`, in key order. Overlays merge
    /// over committed state; results are always sorted by key bytes.
    pub(crate) fn scan(&self, prefix: &[u8]) -> StorageResult<Vec<(Vec<u8>, Vec<u8>)>> {
        let inner = self.lock()?;
        let (start, end) = keys::prefix_range(prefix);
        let mut out: BTreeMap<Vec<u8>, Vec<u8>> = inner
            .rows
            .range((start, end))
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect();
        for overlay in &inner.overlays {
            merge_overlay(&mut out, overlay, prefix);
        }
        drop(inner);
        Ok(out.into_iter().collect())
    }

    /// Allocate the next durable insertion-order value. The counter lives in a
    /// meta row so it survives restart and is written inside the same unit as
    /// the row that consumes it.
    pub(crate) fn next_sequence(&self) -> StorageResult<u64> {
        let mut inner = self.lock()?;
        if inner.read_only {
            return Err(StorageError::Backend(
                "kv write inside a read snapshot".into(),
            ));
        }
        let current = inner.sequence;
        let next = current
            .checked_add(1)
            .ok_or_else(|| StorageError::Backend("kv insertion-order counter overflow".into()))?;
        inner.sequence = next;
        inner.write(
            self.host.as_ref(),
            keys::key(Table::Meta, &[keys::META_SEQUENCE]),
            Some(next.to_be_bytes().to_vec()),
        )?;
        drop(inner);
        Ok(next)
    }

    /// The pending-reference high-water (monotone; never regresses).
    pub(crate) fn pending_high_water(&self) -> StorageResult<u64> {
        let key = keys::key(Table::Meta, &[keys::META_PENDING_HIGH_WATER]);
        let raw = self.get(&key)?.ok_or_else(|| {
            StorageError::Backend("kv pending-reference high-water row missing".into())
        })?;
        read_u64(&raw).ok_or_else(|| {
            StorageError::Backend("kv pending-reference high-water row corrupt".into())
        })
    }

    /// Raise the high-water to at least `value`. Must run inside the same unit
    /// as the artifact that carries the reference.
    pub(crate) fn raise_pending_high_water(&self, value: u64) -> StorageResult<()> {
        if value <= self.pending_high_water()? {
            return Ok(());
        }
        self.put(
            keys::key(Table::Meta, &[keys::META_PENDING_HIGH_WATER]),
            value.to_be_bytes().to_vec(),
        )
    }

    /// Committed-batch count: the engine's cache-invalidation counter.
    /// Bumped once per emitted batch — a strict over-approximation of
    /// `storage-sqlite`'s OpenMLS-write counter (which also counts unrelated
    /// commits through `PRAGMA data_version`), and over-invalidation is always
    /// safe.
    pub(crate) fn write_generation(&self) -> StorageResult<u64> {
        Ok(self.lock()?.write_gen)
    }

    /// The last emitted batch sequence, for the session durability gate.
    pub(crate) fn last_seq(&self) -> Option<CommitSeq> {
        let batches = self.inner.lock().ok()?.batches;
        (batches > 0).then(|| CommitSeq::from_raw(batches))
    }
}

impl Inner {
    /// Read `key` through the overlay stack into committed state.
    fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        for overlay in self.overlays.iter().rev() {
            if let Some(entry) = overlay.get(key) {
                return entry.clone();
            }
        }
        self.rows.get(key).cloned()
    }

    /// Write into the top overlay, or commit a bare unit when the stack is
    /// empty.
    fn write(
        &mut self,
        host: &dyn KvHost,
        key: Vec<u8>,
        value: Option<Vec<u8>>,
    ) -> StorageResult<()> {
        if self.read_only {
            return Err(StorageError::Backend(
                "kv write inside a read snapshot".into(),
            ));
        }
        if let Some(overlay) = self.overlays.last_mut() {
            overlay.insert(key, value);
        } else {
            let mut pending = BTreeMap::new();
            pending.insert(key, value);
            self.commit_pending(host, pending);
        }
        Ok(())
    }

    /// Fold a popped layer into the parent, or commit it as one batch when
    /// it was the outermost scope. Returns callbacks to run post-commit.
    fn absorb(
        &mut self,
        host: &dyn KvHost,
        overlay: Overlay,
        queued: Vec<Box<dyn FnOnce() + Send + 'static>>,
    ) -> Vec<Box<dyn FnOnce() + Send + 'static>> {
        if let Some(parent) = self.overlays.last_mut() {
            parent.extend(overlay);
            if let Some(queue) = self.callbacks.last_mut() {
                queue.extend(queued);
            }
            return Vec::new();
        }
        self.commit_pending(host, overlay);
        queued
    }

    /// Merge `pending` into committed state and emit one batch. Emits nothing
    /// when `pending` is empty — empty units produce no batch.
    fn commit_pending(&mut self, host: &dyn KvHost, pending: Overlay) {
        if pending.is_empty() {
            return;
        }
        let mut ops = Vec::with_capacity(pending.len());
        for (key, value) in pending {
            if let Some(value) = value {
                self.rows.insert(key.clone(), value.clone());
                ops.push(KvOp::Put { key, value });
            } else {
                self.rows.remove(&key);
                ops.push(KvOp::Delete { key });
            }
        }
        self.batches += 1;
        self.write_gen += 1;
        // `commit` runs while the lock is held: that is what guarantees the
        // host observes batches strictly in seq order.
        host.commit(KvBatch {
            seq: CommitSeq::from_raw(self.batches),
            ops,
        });
    }
}

/// Pops the top overlay unless `commit` disarmed it. Covers both the `Err`
/// path (the guard drops during `?` propagation) and unwinding through `f`.
struct LayerGuard<'a> {
    store: &'a KvStore,
    armed: bool,
}

impl LayerGuard<'_> {
    /// Merge this layer into the parent — or commit one batch and run the
    /// queued callbacks, for the outermost scope.
    fn commit(&mut self) -> StorageResult<()> {
        self.armed = false;
        let callbacks = {
            let mut inner = self.store.lock()?;
            let Some(overlay) = inner.overlays.pop() else {
                return Err(StorageError::Backend("kv transaction layer missing".into()));
            };
            let queued = inner.callbacks.pop().unwrap_or_default();
            inner.absorb(self.store.host.as_ref(), overlay, queued)
        };
        // Callbacks run after the batch is committed and the lock released.
        for callback in callbacks {
            callback();
        }
        Ok(())
    }
}

impl Drop for LayerGuard<'_> {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        if let Ok(mut inner) = self.store.inner.lock() {
            inner.overlays.pop();
            inner.callbacks.pop();
        }
    }
}

/// Clears the read-only flag on scope exit.
struct ReadGuard<'a> {
    store: &'a KvStore,
}

impl Drop for ReadGuard<'_> {
    fn drop(&mut self) {
        if let Ok(mut inner) = self.store.inner.lock() {
            inner.read_only = false;
        }
    }
}

fn read_u32(bytes: &[u8]) -> Option<u32> {
    let bytes: [u8; 4] = bytes.try_into().ok()?;
    Some(u32::from_be_bytes(bytes))
}

fn read_u64(bytes: &[u8]) -> Option<u64> {
    let bytes: [u8; 8] = bytes.try_into().ok()?;
    Some(u64::from_be_bytes(bytes))
}

impl std::fmt::Debug for KvStore {
    /// The host identity and nothing else: the mirror holds row data and the
    /// after-commit queue holds closures — neither belongs in diagnostics.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KvStore")
            .field("host", &self.host)
            .finish_non_exhaustive()
    }
}

/// Fold one overlay layer's `range(prefix)` edits into `out`.
fn merge_overlay(out: &mut BTreeMap<Vec<u8>, Vec<u8>>, overlay: &Overlay, prefix: &[u8]) {
    let (scan_start, scan_end) = keys::prefix_range(prefix);
    for (key, entry) in overlay.range((scan_start, scan_end)) {
        match entry {
            Some(value) => {
                out.insert(key.clone(), value.clone());
            }
            None => {
                out.remove(key);
            }
        }
    }
}
