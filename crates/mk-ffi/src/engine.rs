//! The safe half of the handle: the job channel, the engine thread and
//! the lifecycle phase the ABI layer reads. Nothing here touches `unsafe`
//! or the `Bridge` itself — the thread body lives in `crate::abi`.

use std::io;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::mpsc::{Receiver, Sender, channel};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::thread::{Builder, JoinHandle};

/// Raw `AtomicU8` phase values; [`Phase`] is the typed view. Exported so
/// the engine thread body can store `POISONED` without a typed `Engine`.
pub(crate) const PHASE_RUNNING: u8 = 0;
pub(crate) const PHASE_POISONED: u8 = 1;
pub(crate) const PHASE_CLOSED: u8 = 2;

/// The handle's lifecycle, observed by ABI calls.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Phase {
    /// The engine thread accepts jobs.
    Running,
    /// A job panicked or the init failed; calls answer `poisoned`.
    Poisoned,
    /// `mk_bridge_close` ran the engine thread down; calls answer
    /// `closed`.
    Closed,
}

impl Phase {
    const fn from_raw(raw: u8) -> Self {
        match raw {
            PHASE_POISONED => Self::Poisoned,
            PHASE_CLOSED => Self::Closed,
            _ => Self::Running,
        }
    }
}

/// One unit of work for the engine thread. Request jobs always carry a
/// reply sender; the thread answers every job it receives before
/// shutting down, so a caller that successfully queued a job never waits
/// forever on a live thread.
#[derive(Debug)]
pub(crate) enum Job {
    /// `mk_bridge_open`: the config bytes.
    Open {
        /// The config envelope.
        request: Vec<u8>,
        /// Per-job reply channel.
        reply: Sender<Vec<u8>>,
    },
    /// `mk_bridge_call`: the request envelope.
    Call {
        /// The request envelope.
        request: Vec<u8>,
        /// Per-job reply channel.
        reply: Sender<Vec<u8>>,
    },
    /// `mk_bridge_drain`.
    Drain {
        /// Per-job reply channel.
        reply: Sender<Vec<u8>>,
    },
    /// Feature `test-panic`: run a panicking job on the engine thread.
    #[cfg(feature = "test-panic")]
    Panic {
        /// Per-job reply channel.
        reply: Sender<Vec<u8>>,
    },
    /// Run `Bridge::close`, then exit the thread. `close` passes a reply
    /// it waits on; `free` passes `None` and detaches.
    Shutdown {
        /// Signalled once `Bridge::close` ran, when present.
        reply: Option<Sender<()>>,
    },
}

impl Job {
    /// The byte-reply channel of a request job; `None` for `Shutdown`.
    pub(crate) fn reply_sender(&self) -> Option<Sender<Vec<u8>>> {
        match self {
            Self::Open { reply, .. } | Self::Call { reply, .. } | Self::Drain { reply } => {
                Some(reply.clone())
            }
            #[cfg(feature = "test-panic")]
            Self::Panic { reply } => Some(reply.clone()),
            Self::Shutdown { .. } => None,
        }
    }
}

/// A lock that tolerates a panicked holder: poisoned mutexes here guard
/// only channel/thread handles, never bridge state.
fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

/// The handle state every ABI call shares. `Sync`: the mutexes serialize
/// job submission and shutdown, the phase is atomic.
#[derive(Debug)]
pub(crate) struct Engine {
    /// Lifecycle the ABI layer reads before and after dispatch.
    phase: Arc<AtomicU8>,
    /// The job channel. Held under a mutex across send-and-reply-wait so
    /// calls serialize FIFO and `close` cannot slip a `Shutdown` behind an
    /// unanswered job. `None` once `close` consumed it.
    jobs: Mutex<Option<Sender<Job>>>,
    /// A second sender for the non-blocking `free` path, which must not
    /// wait on `jobs` while a call is in flight.
    shutdown: Sender<Job>,
    /// The engine thread join handle, taken by `close` (join) or `free`
    /// (detach).
    thread: Mutex<Option<JoinHandle<()>>>,
}

impl Engine {
    /// Spawn the engine thread with `name` and `stack_bytes`, running
    /// `main(receiver, phase)`.
    ///
    /// # Errors
    ///
    /// Returns the spawn `io::Error`; no thread exists then.
    pub(crate) fn spawn(
        name: &str,
        stack_bytes: usize,
        main: impl FnOnce(Receiver<Job>, Arc<AtomicU8>) + Send + 'static,
    ) -> io::Result<Self> {
        let (tx, rx) = channel::<Job>();
        let phase = Arc::new(AtomicU8::new(PHASE_RUNNING));
        let thread_phase = Arc::clone(&phase);
        let thread = Builder::new()
            .name(name.to_owned())
            .stack_size(stack_bytes)
            .spawn(move || {
                let _outcome = catch_unwind(AssertUnwindSafe(|| main(rx, thread_phase)));
            })?;
        Ok(Self {
            phase,
            jobs: Mutex::new(Some(tx.clone())),
            shutdown: tx,
            thread: Mutex::new(Some(thread)),
        })
    }

    /// The current lifecycle phase.
    pub(crate) fn phase(&self) -> Phase {
        Phase::from_raw(self.phase.load(Ordering::Acquire))
    }

    /// Mark the handle poisoned. Idempotent; never downgrades `Closed`.
    pub(crate) fn poison(&self) {
        let _was_running = self.phase.compare_exchange(
            PHASE_RUNNING,
            PHASE_POISONED,
            Ordering::AcqRel,
            Ordering::Acquire,
        );
    }

    /// Queue one request job and block the caller on its reply.
    ///
    /// Holding the `jobs` mutex across the reply wait serializes callers
    /// into the channel's FIFO order and guarantees that once `close`
    /// consumed the sender, no later job can strand behind `Shutdown`.
    ///
    /// # Errors
    ///
    /// `Err(phase)` when no answer can come: the phase observed before
    /// dispatch, or `Poisoned` when the channel died mid-flight (the
    /// engine thread is gone).
    #[allow(
        clippy::significant_drop_tightening,
        reason = "the jobs guard must live until the reply arrives — early release breaks FIFO"
    )]
    pub(crate) fn submit(
        &self,
        make: impl FnOnce(Sender<Vec<u8>>) -> Job,
    ) -> Result<Vec<u8>, Phase> {
        let phase = self.phase();
        if phase != Phase::Running {
            return Err(phase);
        }
        let guard = lock(&self.jobs);
        let Some(jobs) = guard.as_ref() else {
            return Err(Phase::Closed);
        };
        let (reply, wait) = channel();
        if jobs.send(make(reply)).is_err() {
            self.poison();
            return Err(Phase::Poisoned);
        }
        wait.recv().map_or_else(
            |_| {
                self.poison();
                Err(Phase::Poisoned)
            },
            Ok,
        )
    }

    /// `mk_bridge_close`: consume the job channel, wait for the thread to
    /// run `Bridge::close` and exit, then join it. Idempotent; on a
    /// poisoned handle only the join remains.
    pub(crate) fn close(&self) {
        self.signal_shutdown();
        let thread = lock(&self.thread).take();
        if let Some(thread) = thread {
            let _joined = thread.join();
        }
        let _was_running = self.phase.compare_exchange(
            PHASE_RUNNING,
            PHASE_CLOSED,
            Ordering::AcqRel,
            Ordering::Acquire,
        );
    }

    /// Consume the job channel and send `Shutdown` with a reply the
    /// caller waits on; afterwards later submits answer `closed`.
    fn signal_shutdown(&self) {
        let jobs = lock(&self.jobs).take();
        let Some(jobs) = jobs else {
            return;
        };
        let (reply, wait) = channel();
        if jobs.send(Job::Shutdown { reply: Some(reply) }).is_ok() {
            let _answered = wait.recv();
        }
    }

    /// `mk_bridge_free`: enqueue shutdown without waiting and detach the
    /// thread. Never blocks: the shutdown send goes on a channel the
    /// in-flight calls do not hold, and the join handle is dropped, not
    /// joined. A racing `close` already owns the shutdown — then this is a
    /// no-op beyond freeing the box.
    pub(crate) fn detach(&self) {
        let _sent = self.shutdown.send(Job::Shutdown { reply: None });
        if let Ok(mut guard) = self.thread.try_lock()
            && let Some(thread) = guard.take()
        {
            drop(thread);
        }
    }
}
