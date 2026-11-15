//! In-memory BTreeMap mirror with layered overlays for `with_transaction` /
//! `with_group_savepoint` atomicity, and one `KvHost::commit` batch per
//! outermost unit.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use crate::kv::{KvHost, KvOp};

type Map = BTreeMap<Vec<u8>, Vec<u8>>;
/// `None` = delete.
type Overlay = BTreeMap<Vec<u8>, Option<Vec<u8>>>;

struct Inner {
    /// Committed state, identical to what the host holds after every commit.
    map: Map,
    /// Stacked write units. `with_transaction` pushes a layer; the outermost
    /// merge applies every op to `map` and hands one batch to the host.
    /// `with_group_savepoint` pushes another layer on top; dropping that
    /// layer on `Err` undoes every write the closure made (group-scoped or
    /// not — a superset of the contract's allowed undo).
    overlays: Vec<Overlay>,
    /// Monotonic counter bumped on every committed batch; feeds
    /// `mls_write_generation` so the engine's `MlsGroup` reuse cache
    /// invalidates on any real write.
    write_gen: u64,
    /// `after_commit` callbacks queued until the outermost unit commits.
    after_commit: Vec<Box<dyn FnOnce() + Send + 'static>>,
    /// Insertion counter for seq-ordered tables (messages, pending events).
    seq: u64,
}

fn seq_key() -> Vec<u8> {
    b"meta/seq".to_vec()
}

/// Shared mutable store behind the provider. All locking is confined here.
pub struct KvStore {
    inner: Mutex<Inner>,
    host: Arc<dyn KvHost>,
}

impl KvStore {
    pub fn open(host: Arc<dyn KvHost>) -> Self {
        let map: Map = host.load_all().into_iter().collect();
        let seq = map.get(&seq_key()).map_or(0, |v| {
            let b: &[u8] = v.as_slice();
            b.try_into().map(u64::from_be_bytes).unwrap_or(0)
        });
        Self {
            inner: Mutex::new(Inner {
                map,
                overlays: Vec::new(),
                write_gen: 0,
                after_commit: Vec::new(),
                seq,
            }),
            host,
        }
    }

    /// Allocate the next insertion sequence. The counter row travels in the
    /// caller's unit so a rolled-back put also rolls back the bump.
    pub fn next_seq(&self) -> u64 {
        let mut inner = self.inner.lock().unwrap();
        inner.seq += 1;
        let seq = inner.seq;
        let sk = seq_key();
        self.put_locked(&mut inner, sk, seq.to_be_bytes().to_vec());
        seq
    }

    fn put_locked(&self, inner: &mut Inner, key: Vec<u8>, value: Vec<u8>) {
        if let Some(top) = inner.overlays.last_mut() {
            top.insert(key, Some(value));
        } else {
            let batch = vec![KvOp::Put(key.clone(), value.clone())];
            inner.map.insert(key, value);
            inner.write_gen += 1;
            let host = self.host.clone();
            // Lock released by caller? No — keep the lock; host commit is a
            // plain map write and cannot reenter this store.
            host.commit(batch);
        }
    }

    fn get_locked(inner: &Inner, key: &[u8]) -> Option<Vec<u8>> {
        for overlay in inner.overlays.iter().rev() {
            if let Some(entry) = overlay.get(key) {
                return entry.clone();
            }
        }
        inner.map.get(key).cloned()
    }

    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        let inner = self.inner.lock().unwrap();
        Self::get_locked(&inner, key)
    }

    pub fn put(&self, key: Vec<u8>, value: Vec<u8>) {
        let mut inner = self.inner.lock().unwrap();
        self.put_locked(&mut inner, key, value);
    }

    pub fn delete(&self, key: &[u8]) {
        let mut inner = self.inner.lock().unwrap();
        if let Some(top) = inner.overlays.last_mut() {
            top.insert(key.to_vec(), None);
        } else if inner.map.remove(key).is_some() {
            inner.write_gen += 1;
            self.host.commit(vec![KvOp::Delete(key.to_vec())]);
        }
    }

    /// Every live (visible) key with `prefix`, in key order.
    pub fn scan_prefix(&self, prefix: &[u8]) -> Vec<(Vec<u8>, Vec<u8>)> {
        let inner = self.inner.lock().unwrap();
        let mut end = prefix.to_vec();
        end.push(0xff);
        let range = prefix.to_vec()..end;
        let mut rows: Vec<(Vec<u8>, Option<Vec<u8>>)> = inner
            .map
            .range(range.clone())
            .map(|(k, v)| (k.clone(), Some(v.clone())))
            .collect();
        for overlay in &inner.overlays {
            for (k, v) in overlay.range(range.clone()) {
                if let Some(slot) = rows.iter_mut().find(|(rk, _)| rk == k) {
                    slot.1 = v.clone();
                } else {
                    rows.push((k.clone(), v.clone()));
                }
            }
        }
        rows.sort_by(|a, b| a.0.cmp(&b.0));
        rows.into_iter()
            .filter_map(|(k, v)| v.map(|value| (k, value)))
            .collect()
    }

    /// Delete every live key under `prefix`; returns removed keys.
    pub fn delete_prefix(&self, prefix: &[u8]) -> Vec<Vec<u8>> {
        let keys: Vec<Vec<u8>> = self
            .scan_prefix(prefix)
            .into_iter()
            .map(|(k, _)| k)
            .collect();
        for k in &keys {
            self.delete(k);
        }
        keys
    }

    /// Copy every live row under `src` to `dst`, preserving key tails.
    pub fn copy_prefix(&self, src: &[u8], dst: &[u8]) {
        for (k, v) in self.scan_prefix(src) {
            let mut new_key = dst.to_vec();
            new_key.extend_from_slice(&k[src.len()..]);
            self.put(new_key, v);
        }
    }

    pub fn write_generation(&self) -> u64 {
        self.inner.lock().unwrap().write_gen
    }

    /// Push a new overlay layer; run `f`; merge on success or drop on error.
    /// The outermost merge flushes one batch to the host and then runs every
    /// queued `after_commit` callback.
    pub fn transact<T, E, F>(&self, f: F) -> Result<T, E>
    where
        F: FnOnce() -> Result<T, E>,
    {
        {
            let mut inner = self.inner.lock().unwrap();
            inner.overlays.push(Overlay::new());
        }
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(f));
        let mut inner = self.inner.lock().unwrap();
        let layer = inner.overlays.pop().unwrap_or_default();
        match result {
            Ok(Ok(value)) => {
                if let Some(parent) = inner.overlays.last_mut() {
                    parent.extend(layer);
                    drop(inner);
                } else {
                    let mut batch = Vec::with_capacity(layer.len());
                    for (k, v) in layer {
                        match v {
                            Some(value) => {
                                inner.map.insert(k.clone(), value.clone());
                                batch.push(KvOp::Put(k, value));
                            }
                            None => {
                                inner.map.remove(&k);
                                batch.push(KvOp::Delete(k));
                            }
                        }
                    }
                    if !batch.is_empty() {
                        inner.write_gen += 1;
                    }
                    let callbacks: Vec<_> = std::mem::take(&mut inner.after_commit);
                    let host = self.host.clone();
                    drop(inner);
                    if !batch.is_empty() {
                        host.commit(batch);
                    }
                    for cb in callbacks {
                        cb();
                    }
                }
                Ok(value)
            }
            Ok(Err(err)) => {
                if inner.overlays.is_empty() {
                    inner.after_commit.clear();
                }
                drop(inner);
                Err(err)
            }
            Err(panic) => {
                // A panic inside the closure discards the whole layer and its
                // queued callbacks, then propagates.
                if inner.overlays.is_empty() {
                    inner.after_commit.clear();
                }
                drop(inner);
                std::panic::resume_unwind(panic);
            }
        }
    }

    /// Queue a callback for the outermost unit's commit; outside any unit the
    /// write that owns it commits immediately, so run it now.
    pub fn on_after_commit(&self, callback: Box<dyn FnOnce() + Send + 'static>) {
        let mut inner = self.inner.lock().unwrap();
        if inner.overlays.is_empty() {
            drop(inner);
            callback();
        } else {
            inner.after_commit.push(callback);
        }
    }
}
