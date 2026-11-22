//! The host storage port: `mk-core`'s infallible `KvHost` batches queue
//! inside the bridge and are flushed to the host's fallible byte API.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex, MutexGuard};

use mk_core::{KvBatch, KvHost};

/// The embedder's durable KV port. Not `Send`/`Sync`: wasm hosts implement
/// it over JavaScript callbacks.
pub trait Host {
    /// Every durable row the host holds, encoded by
    /// [`crate::encode_rows`]. Called once by `open`.
    ///
    /// # Errors
    /// [`HostError`] on any load failure; the bridge is poisoned.
    fn load(&mut self) -> Result<Vec<u8>, HostError>;

    /// Accept one committed `KvBatch` (`KvBatch::encode` bytes) for
    /// persistence. Batches arrive strictly in `seq` order.
    ///
    /// # Errors
    /// [`HostError`] on any commit failure; the bridge is poisoned and the
    /// session must be reopened from durable rows.
    fn commit(&mut self, batch: &[u8]) -> Result<(), HostError>;
}

/// The host storage failed. Unit on purpose: diagnostics about the host's
/// own failure are the host's business.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, thiserror::Error)]
#[error("host storage failed")]
#[non_exhaustive]
pub struct HostError;

impl HostError {
    /// A storage failure.
    #[must_use]
    pub const fn new() -> Self {
        Self
    }
}

/// The `mk-core` side of the port: queues committed batches for the bridge
/// to flush through [`Host::commit`] in order. `Send + Sync` per
/// [`KvHost`]'s contract.
#[derive(Default)]
pub(crate) struct QueueHost {
    queue: Arc<Mutex<VecDeque<KvBatch>>>,
}

impl std::fmt::Debug for QueueHost {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QueueHost")
            .field("queue_len", &lock(&self.queue).len())
            .finish()
    }
}

impl QueueHost {
    pub(crate) const fn new(queue: Arc<Mutex<VecDeque<KvBatch>>>) -> Self {
        Self { queue }
    }
}

impl KvHost for QueueHost {
    fn commit(&self, batch: KvBatch) {
        let mut guard = lock(&self.queue);
        guard.push_back(batch);
    }
}

/// Lock a mutex, recovering the guard on poisoning (a panicking writer may
/// have left the queue partial; the bytes still queue in order).
pub(crate) fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}
