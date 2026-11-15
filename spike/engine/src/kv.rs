//! Host key/value boundary for the spike storage provider.
//!
//! `KvHost` is the one place the provider talks to the outside world. The
//! engine's storage API is synchronous, so the provider mirrors every byte in
//! memory; `commit` hands the host one atomic write batch per committed unit.

use std::collections::BTreeMap;
use std::sync::Mutex;

/// One durable write operation inside a committed unit.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum KvOp {
    Put(Vec<u8>, Vec<u8>),
    Delete(Vec<u8>),
}

/// Host persistence boundary. Implementations must apply each batch
/// atomically; the provider never splits a committed unit across calls.
pub trait KvHost: Send + Sync {
    /// Full contents at open. Called exactly once per provider construction.
    fn load_all(&self) -> Vec<(Vec<u8>, Vec<u8>)>;

    /// Apply one atomic write batch.
    fn commit(&self, batch: Vec<KvOp>);
}

/// In-memory host used by the harness. Keeps both the live map and the batch
/// log so a test can dump the state and rebuild a provider from it, which
/// simulates a process restart.
#[derive(Default)]
pub struct InMemoryHost {
    map: Mutex<BTreeMap<Vec<u8>, Vec<u8>>>,
    batches: Mutex<Vec<Vec<KvOp>>>,
}

impl InMemoryHost {
    pub fn new() -> Self {
        Self::default()
    }

    /// Serialize the full state (postcard of the ordered map).
    pub fn dump(&self) -> Vec<u8> {
        let map = self.map.lock().unwrap();
        let rows: Vec<(Vec<u8>, Vec<u8>)> =
            map.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
        postcard::to_allocvec(&rows).unwrap_or_else(|e| unreachable!("map dump: {e}"))
    }

    /// Rebuild a host from a `dump` image.
    pub fn load(bytes: &[u8]) -> Self {
        let rows: Vec<(Vec<u8>, Vec<u8>)> =
            postcard::from_bytes(bytes).unwrap_or_else(|e| panic!("corrupt dump: {e}"));
        Self {
            map: Mutex::new(rows.into_iter().collect()),
            batches: Mutex::new(Vec::new()),
        }
    }

    pub fn batch_count(&self) -> usize {
        self.batches.lock().unwrap().len()
    }
}

impl KvHost for InMemoryHost {
    fn load_all(&self) -> Vec<(Vec<u8>, Vec<u8>)> {
        self.map
            .lock()
            .unwrap()
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }

    fn commit(&self, batch: Vec<KvOp>) {
        let mut map = self.map.lock().unwrap();
        for op in &batch {
            match op {
                KvOp::Put(k, v) => {
                    map.insert(k.clone(), v.clone());
                }
                KvOp::Delete(k) => {
                    map.remove(k);
                }
            }
        }
        self.batches.lock().unwrap().push(batch);
    }
}
