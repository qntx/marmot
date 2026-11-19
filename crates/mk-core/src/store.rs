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

use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
use std::ops::Bound;
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
    /// Depth at which `transact` stops passing through: while a `Savepoint`
    /// is open (`floor > 0`), a `transact` at or below it pushes its own
    /// layer — rolling back on `Err`, merging into the savepoint on `Ok` —
    /// so engine code inside the span keeps outermost-unit semantics.
    floor: usize,
    /// While `true`, every write fails: armed by `fence_writes` for the
    /// duration of an open `Savepoint`, cleared when it ends.
    write_fence: bool,
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
                    floor: 0,
                    write_fence: false,
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
                floor: 0,
                write_fence: false,
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
            if inner.overlays.len() > inner.floor {
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

    /// Open an owning savepoint that can be held across `.await` — the
    /// async-spanning counterpart of [`KvStore::savepoint`], whose guard borrows
    /// the store and therefore cannot outlive its closure. While the guard is
    /// open, `transact` scopes behave as if outermost (each pushes its own
    /// layer, rolls back on `Err`, merges into the savepoint on `Ok`), and
    /// [`KvStore::fence_writes`] can refuse every later write for the span.
    /// [`Savepoint::commit`] merges/commits the layer exactly like an outermost
    /// unit; dropping without commit rolls it back and bumps `write_gen`, so
    /// group state the engine cached inside the rolled-back unit is
    /// invalidated.
    ///
    /// # Errors
    /// [`StorageError`] on lock poisoning.
    pub(crate) fn begin_savepoint(&self) -> StorageResult<Savepoint> {
        let mut inner = self.lock()?;
        inner.overlays.push(BTreeMap::new());
        inner.callbacks.push(Vec::new());
        let prev_floor = inner.floor;
        inner.floor = inner.overlays.len();
        Ok(Savepoint {
            store: self.clone(),
            depth: inner.overlays.len(),
            prev_floor,
            armed: true,
        })
    }

    /// Refuse every subsequent write until the open `Savepoint` ends —
    /// `put`/`delete`/`next_sequence` all fail with a fixed
    /// [`StorageError::Backend`] text. Committing or dropping the savepoint
    /// clears the fence. A no-op when no savepoint is open.
    pub(crate) fn fence_writes(&self) {
        if let Ok(mut inner) = self.inner.lock()
            && inner.floor > 0
        {
            inner.write_fence = true;
        }
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
        self.any_key_in((Bound::Included(key.to_vec()), Bound::Included(key.to_vec())))
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
        let range = keys::prefix_range(prefix);
        let rows = inner
            .live_keys(&range)
            .into_iter()
            .filter_map(|key| inner.get(&key).map(|value| (key, value)))
            .collect();
        drop(inner);
        Ok(rows)
    }

    /// All live keys in `range`, in key order, with the same overlay-merge
    /// semantics as `scan` — overlay tombstones hide committed keys and
    /// overlay puts contribute their keys. Values are never cloned.
    pub(crate) fn keys_in(
        &self,
        range: (Bound<Vec<u8>>, Bound<Vec<u8>>),
    ) -> StorageResult<Vec<Vec<u8>>> {
        Ok(self.lock()?.live_keys(&range))
    }

    /// Whether `range` holds at least one live key, under the same
    /// overlay-merge semantics as `keys_in`, exiting on the first hit.
    pub(crate) fn any_key_in(
        &self,
        range: (Bound<Vec<u8>>, Bound<Vec<u8>>),
    ) -> StorageResult<bool> {
        Ok(self.lock()?.any_key_in(&range))
    }

    /// Allocate the next durable insertion-order value. The counter lives in a
    /// meta row so it survives restart and is written inside the same unit as
    /// the row that consumes it.
    pub(crate) fn next_sequence(&self) -> StorageResult<u64> {
        let mut inner = self.lock()?;
        if inner.write_fence {
            return Err(StorageError::Backend("kv write fenced by savepoint".into()));
        }
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

/// Whether `range` names no key: start past end, or equal bounds that
/// exclude an endpoint. `BTreeMap::range` panics on inverted and on
/// excluded-excluded equal bounds, so the callers check before probing.
fn range_is_empty(range: &(Bound<Vec<u8>>, Bound<Vec<u8>>)) -> bool {
    let (start, end) = match range {
        (
            Bound::Included(start) | Bound::Excluded(start),
            Bound::Included(end) | Bound::Excluded(end),
        ) => (start, end),
        (Bound::Unbounded, _) | (_, Bound::Unbounded) => return false,
    };
    match start.cmp(end) {
        Ordering::Greater => true,
        Ordering::Equal => {
            matches!(range.0, Bound::Excluded(_)) || matches!(range.1, Bound::Excluded(_))
        }
        Ordering::Less => false,
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

    /// All live keys in `range`, in key order: committed keys merged with
    /// every overlay layer's edits, tombstones hiding committed keys.
    fn live_keys(&self, range: &(Bound<Vec<u8>>, Bound<Vec<u8>>)) -> Vec<Vec<u8>> {
        if range_is_empty(range) {
            return Vec::new();
        }
        let mut out: BTreeSet<Vec<u8>> = self
            .rows
            .range(range.clone())
            .map(|(key, _)| key.clone())
            .collect();
        for overlay in &self.overlays {
            merge_overlay_keys(&mut out, overlay, range);
        }
        out.into_iter().collect()
    }

    /// The merged verdict for a committed key: the topmost overlay entry for
    /// it wins, otherwise the committed row is live.
    fn key_is_live(&self, key: &[u8]) -> bool {
        self.overlays
            .iter()
            .rev()
            .find_map(|overlay| overlay.get(key))
            .is_none_or(Option::is_some)
    }

    /// Whether `range` holds at least one live key, exiting on the first hit.
    fn any_key_in(&self, range: &(Bound<Vec<u8>>, Bound<Vec<u8>>)) -> bool {
        if range_is_empty(range) {
            return false;
        }
        if self
            .rows
            .range(range.clone())
            .any(|(key, _)| self.key_is_live(key))
        {
            return true;
        }
        self.overlay_only_live_key(range)
    }

    /// Whether any key present only in overlays resolves to a live put. Each
    /// key's verdict is its topmost entry, so layers are walked top-down and
    /// each key is settled once.
    fn overlay_only_live_key(&self, range: &(Bound<Vec<u8>>, Bound<Vec<u8>>)) -> bool {
        let mut resolved = BTreeSet::new();
        self.overlays
            .iter()
            .rev()
            .flat_map(|overlay| overlay.range(range.clone()))
            .any(|(key, entry)| {
                !self.rows.contains_key(key) && resolved.insert(key) && entry.is_some()
            })
    }

    /// Write into the top overlay, or commit a bare unit when the stack is
    /// empty.
    fn write(
        &mut self,
        host: &dyn KvHost,
        key: Vec<u8>,
        value: Option<Vec<u8>>,
    ) -> StorageResult<()> {
        if self.write_fence {
            return Err(StorageError::Backend("kv write fenced by savepoint".into()));
        }
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

/// The owning guard [`KvStore::begin_savepoint`] returns: pops or commits its
/// layer on exit, restoring the transaction floor and the write fence. It owns
/// a `KvStore` clone, so it can be held across `.await`.
pub(crate) struct Savepoint {
    store: KvStore,
    /// `overlays.len()` after this savepoint's layer was pushed; the layer
    /// sits at index `depth - 1`.
    depth: usize,
    /// The `transact` floor in effect before this savepoint opened.
    prev_floor: usize,
    armed: bool,
}

impl Savepoint {
    /// Merge the layer into the parent unit — or commit one batch and run the
    /// queued callbacks, for an outermost savepoint. Exactly
    /// [`LayerGuard::commit`]'s semantics.
    ///
    /// # Errors
    /// [`StorageError::Backend`] when inner scopes left the layer stack
    /// unbalanced (the whole unit then rolls back through `Drop`), or on
    /// lock poisoning.
    pub(crate) fn commit(mut self) -> StorageResult<()> {
        let callbacks = {
            let mut inner = self.store.lock()?;
            if inner.overlays.len() != self.depth {
                return Err(StorageError::Backend(
                    "kv savepoint layers unbalanced".into(),
                ));
            }
            let Some(overlay) = inner.overlays.pop() else {
                return Err(StorageError::Backend("kv savepoint layer missing".into()));
            };
            let queued = inner.callbacks.pop().unwrap_or_default();
            let run = inner.absorb(self.store.host.as_ref(), overlay, queued);
            inner.floor = self.prev_floor;
            inner.write_fence = false;
            run
        };
        self.armed = false;
        for callback in callbacks {
            callback();
        }
        Ok(())
    }
}

impl Drop for Savepoint {
    /// Rollback: discard the savepoint's layer plus anything left above it,
    /// and bump `write_gen` so group state cached inside the rolled-back
    /// unit is invalidated. Covers `Err` propagation and unwinding alike.
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        if let Ok(mut inner) = self.store.inner.lock() {
            while inner.overlays.len() >= self.depth {
                inner.overlays.pop();
                inner.callbacks.pop();
            }
            inner.write_gen += 1;
            inner.floor = self.prev_floor;
            inner.write_fence = false;
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

/// Fold one overlay layer's edits to keys in `range` into `out`: puts add
/// their key, tombstones remove it.
fn merge_overlay_keys(
    out: &mut BTreeSet<Vec<u8>>,
    overlay: &Overlay,
    range: &(Bound<Vec<u8>>, Bound<Vec<u8>>),
) {
    for (key, entry) in overlay.range(range.clone()) {
        match entry {
            Some(_) => {
                out.insert(key.clone());
            }
            None => {
                out.remove(key);
            }
        }
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

    #[derive(Debug)]
    struct NoopHost;

    impl KvHost for NoopHost {
        fn commit(&self, _batch: KvBatch) {}
    }

    fn open() -> KvStore {
        KvStore::open(Vec::new(), Arc::new(NoopHost)).unwrap()
    }

    fn all_range() -> (Bound<Vec<u8>>, Bound<Vec<u8>>) {
        (
            Bound::Included(b"k".to_vec()),
            Bound::Excluded(b"l".to_vec()),
        )
    }

    fn put(store: &KvStore, key: &[u8]) {
        store.put(key.to_vec(), b"v".to_vec()).unwrap();
    }

    #[test]
    fn keys_in_reports_committed_keys_only() {
        let store = open();
        put(&store, b"k1");
        put(&store, b"k2");
        put(&store, b"out");
        assert_eq!(
            store.keys_in(all_range()).unwrap(),
            vec![b"k1".to_vec(), b"k2".to_vec()]
        );
        assert!(store.any_key_in(all_range()).unwrap());
    }

    #[test]
    fn any_key_in_sees_committed_keys() {
        let store = open();
        assert!(!store.any_key_in(all_range()).unwrap());
        put(&store, b"k1");
        assert!(store.any_key_in(all_range()).unwrap());
        assert!(
            !store
                .any_key_in((Bound::Included(b"z".to_vec()), Bound::Unbounded))
                .unwrap()
        );
    }

    #[test]
    fn open_transaction_delete_hides_committed_key() {
        let store = open();
        put(&store, b"k1");
        put(&store, b"k2");
        store
            .transact::<_, StorageError, _>(|| {
                store.delete(b"k1")?;
                assert_eq!(
                    store.keys_in(all_range())?,
                    vec![b"k2".to_vec()],
                    "deleted key must be hidden inside its unit"
                );
                assert!(!store.any_key_in((
                    Bound::Included(b"k1".to_vec()),
                    Bound::Included(b"k1".to_vec())
                ))?);
                Ok(())
            })
            .unwrap();
        assert_eq!(
            store.keys_in(all_range()).unwrap(),
            vec![b"k2".to_vec()],
            "committed delete must stay hidden after the unit lands"
        );
    }

    #[test]
    fn nested_savepoint_puts_are_visible() {
        let store = open();
        put(&store, b"k1");
        store
            .transact::<_, StorageError, _>(|| {
                store
                    .savepoint::<_, StorageError, _>(|| {
                        put(&store, b"k9");
                        assert!(store.any_key_in((
                            Bound::Included(b"k9".to_vec()),
                            Bound::Included(b"k9".to_vec())
                        ))?);
                        Ok(())
                    })
                    .unwrap();
                assert_eq!(
                    store.keys_in(all_range())?,
                    vec![b"k1".to_vec(), b"k9".to_vec()]
                );
                Ok(())
            })
            .unwrap();
    }

    #[test]
    fn rolled_back_layer_leaves_no_trace() {
        let store = open();
        put(&store, b"k1");
        let result = store.transact::<(), StorageError, _>(|| {
            store.delete(b"k1")?;
            put(&store, b"k9");
            Err(StorageError::Backend("abort".into()))
        });
        assert!(result.is_err());
        assert_eq!(store.keys_in(all_range()).unwrap(), vec![b"k1".to_vec()]);
        assert!(
            !store
                .any_key_in((
                    Bound::Included(b"k9".to_vec()),
                    Bound::Included(b"k9".to_vec())
                ))
                .unwrap()
        );
    }

    #[test]
    fn reads_inside_snapshot_see_live_keys() {
        let store = open();
        put(&store, b"k1");
        store
            .read_snapshot::<_, StorageError, _>(|| {
                assert_eq!(store.keys_in(all_range())?, vec![b"k1".to_vec()]);
                assert!(store.any_key_in(all_range())?);
                Ok(())
            })
            .unwrap();
    }

    #[test]
    fn empty_range_finds_nothing() {
        let store = open();
        put(&store, b"k1");
        let empty = (
            Bound::Included(b"x".to_vec()),
            Bound::Excluded(b"y".to_vec()),
        );
        assert_eq!(store.keys_in(empty.clone()).unwrap(), Vec::<Vec<u8>>::new());
        assert!(!store.any_key_in(empty).unwrap());
    }

    #[test]
    fn inverted_and_excluded_ranges_find_nothing() {
        let store = open();
        put(&store, b"k1");
        for range in [
            // start > end: `BTreeMap::range` panics.
            (
                Bound::Included(b"z".to_vec()),
                Bound::Included(b"a".to_vec()),
            ),
            // start == end, both excluded: also a panic range.
            (
                Bound::Excluded(b"k1".to_vec()),
                Bound::Excluded(b"k1".to_vec()),
            ),
            // start == end, one excluded: empty either way.
            (
                Bound::Included(b"k1".to_vec()),
                Bound::Excluded(b"k1".to_vec()),
            ),
        ] {
            assert_eq!(store.keys_in(range.clone()).unwrap(), Vec::<Vec<u8>>::new());
            assert!(!store.any_key_in(range).unwrap());
        }
    }

    #[test]
    fn committed_savepoint_makes_writes_durable() {
        let store = open();
        let savepoint = store.begin_savepoint().unwrap();
        put(&store, b"k1");
        savepoint.commit().unwrap();
        assert_eq!(store.keys_in(all_range()).unwrap(), vec![b"k1".to_vec()]);
        assert!(!store.in_unit());
    }

    #[test]
    fn dropped_savepoint_rolls_back_and_bumps_generation() {
        let store = open();
        put(&store, b"k1");
        let before = store.write_generation().unwrap();
        {
            let savepoint = store.begin_savepoint().unwrap();
            put(&store, b"k9");
            assert_eq!(
                store.keys_in(all_range()).unwrap(),
                vec![b"k1".to_vec(), b"k9".to_vec()],
                "writes are visible inside the span"
            );
            drop(savepoint);
        }
        assert_eq!(
            store.keys_in(all_range()).unwrap(),
            vec![b"k1".to_vec()],
            "a dropped savepoint leaves no trace"
        );
        assert_eq!(
            store.write_generation().unwrap(),
            before + 1,
            "rollback bumps the generation to invalidate cached group state"
        );
        assert!(!store.in_unit());
    }

    #[test]
    fn transact_inside_savepoint_behaves_outermost() {
        let store = open();
        let savepoint = store.begin_savepoint().unwrap();
        put(&store, b"k1");
        // A failed inner transact rolls back only its own layer: the
        // savepoint's earlier writes survive and the span stays usable.
        let failed = store.transact::<(), StorageError, _>(|| {
            put(&store, b"k2");
            Err(StorageError::Backend("abort".into()))
        });
        assert!(failed.is_err());
        assert_eq!(store.keys_in(all_range()).unwrap(), vec![b"k1".to_vec()]);
        // A succeeding inner transact merges into the savepoint; a
        // passthrough-depth transact inside it still passes through.
        store
            .transact::<_, StorageError, _>(|| {
                put(&store, b"k3");
                let inner = store.transact::<(), StorageError, _>(|| {
                    put(&store, b"k4");
                    Err(StorageError::Backend("abort".into()))
                });
                assert!(inner.is_err());
                assert_eq!(
                    store.keys_in(all_range())?,
                    vec![b"k1".to_vec(), b"k3".to_vec(), b"k4".to_vec()],
                    "passthrough writes stay in the enclosing unit on Err"
                );
                Ok(())
            })
            .unwrap();
        assert_eq!(
            store.keys_in(all_range()).unwrap(),
            vec![b"k1".to_vec(), b"k3".to_vec(), b"k4".to_vec()]
        );
        savepoint.commit().unwrap();
        // After the span, transact passthrough rules are the old ones: a
        // failed outermost transact still rolls back.
        let aborted = store.transact::<(), StorageError, _>(|| {
            put(&store, b"k5");
            Err(StorageError::Backend("abort".into()))
        });
        assert!(aborted.is_err());
        assert_eq!(
            store.keys_in(all_range()).unwrap(),
            vec![b"k1".to_vec(), b"k3".to_vec(), b"k4".to_vec()]
        );
    }

    #[test]
    fn fenced_writes_fail_until_the_savepoint_ends() {
        let store = open();
        let savepoint = store.begin_savepoint().unwrap();
        store.fence_writes();
        for attempt in [
            store.put(b"k1".to_vec(), b"v".to_vec()),
            store.delete(b"k1"),
            store.next_sequence().map(|_| ()),
        ] {
            assert_eq!(
                attempt.unwrap_err().to_string(),
                "backend failure: kv write fenced by savepoint"
            );
        }
        drop(savepoint);
        put(&store, b"k1");
        store.next_sequence().unwrap();
        // The fence is a per-span flag: a new savepoint is not fenced.
        let reopened = store.begin_savepoint().unwrap();
        put(&store, b"k2");
        reopened.commit().unwrap();
    }

    #[test]
    fn fence_is_a_noop_without_an_open_savepoint() {
        let store = open();
        store.fence_writes();
        put(&store, b"k1");
        store.next_sequence().unwrap();
    }

    #[test]
    fn committed_savepoint_also_clears_the_fence() {
        let store = open();
        let savepoint = store.begin_savepoint().unwrap();
        store.fence_writes();
        savepoint.commit().unwrap();
        put(&store, b"k1");
    }
}
