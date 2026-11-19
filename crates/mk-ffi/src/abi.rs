//! The C ABI: exported `mk_bridge_*` functions, the `MkHostVtable`
//! callbacks and the engine thread body. This module is the crate's only
//! `unsafe` site; every other module is safe Rust.
//!
//! Caller contract (also written into `include/marmot_ffi.h`): a handle
//! comes from `mk_bridge_new`, any thread may call the API (calls
//! serialize), input pointers stay valid for the call's duration, `out`
//! may be null, and `mk_bridge_free` runs at most once per handle —
//! calls already in flight finish on the detached engine's state. Host
//! callbacks must not reenter the API for the same handle — the engine
//! thread is busy inside the callback, so a reentrant call would
//! deadlock.
#![allow(
    unsafe_code,
    reason = "the C ABI boundary: raw pointers, host callback invocations and owned buffers cross here"
)]

use std::ffi::c_void;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::ptr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::mpsc::{Receiver, Sender, channel};

use mk_bridge::{Bridge, BridgeError, Host, HostError, MAX_REQUEST_BYTES, encode_error};

use crate::engine::{self, Engine, Job, Phase};

/// The engine thread's stack. The poll model keeps `Box`ed operation
/// futures on the heap, but a single poll still descends the whole
/// `Session`/`cgka-engine`/`OpenMLS` poll chain; 4 MiB measured comfortable
/// for a create→invite→join→send flow (2 MiB overflows in debug builds),
/// so 8 MiB leaves the same headroom as a platform main-thread stack.
const ENGINE_STACK_BYTES: usize = 8 * 1024 * 1024;

/// A Rust-owned byte buffer handed to C.
///
/// The buffer is heap-allocated by this library; release it exactly once
/// with `mk_bytes_free`. `ptr` may be a non-null dangling value when
/// `len` is 0 — passing it to `mk_bytes_free` remains correct.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct MkBytes {
    /// Buffer start.
    pub ptr: *mut u8,
    /// Buffer length in bytes.
    pub len: usize,
}

/// The host storage callbacks every handle drives on its engine thread.
///
/// `mk_bridge_new` copies the vtable; `ctx` is passed back verbatim to
/// every callback. All callbacks run synchronously on the engine thread
/// inside the `mk_bridge_*` call that triggered them, so a `commit`
/// produced by a call is always delivered before that call returns.
/// `load`/`commit` return 0 on success; any other value fails the bridge
/// (`host_failed`, then `poisoned`). A null `load`/`commit` fails the
/// same way; a null `release` is skipped.
///
/// `load` writes a buffer the bridge copies synchronously: it must stay
/// valid until the `mk_bridge_open` call that triggered it returns.
/// `commit` bytes are valid only during the callback. `release` runs
/// exactly once, on the engine thread, after the bridge was dropped —
/// except when `mk_bridge_new` itself failed, where it never runs.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct MkHostVtable {
    /// Serve the durable rows blob; see the type docs.
    pub load:
        Option<unsafe extern "C" fn(ctx: *mut c_void, ptr: *mut *const u8, len: *mut usize) -> i32>,
    /// Accept one committed batch; see the type docs.
    pub commit: Option<unsafe extern "C" fn(ctx: *mut c_void, ptr: *const u8, len: usize) -> i32>,
    /// Release `ctx`; see the type docs.
    pub release: Option<unsafe extern "C" fn(ctx: *mut c_void)>,
}

/// One account-device bridge behind the C ABI. Opaque; create with
/// `mk_bridge_new`, destroy with `mk_bridge_free`.
///
/// `open`/`call`/`drain` may be called from any thread — they serialize
/// on the handle's job channel. `mk_bridge_free` may run while calls
/// are in flight: every entry clones this `Arc` first, so the engine
/// state outlives the box until the last in-flight call returns.
#[derive(Debug)]
pub struct MkBridge {
    engine: Arc<Engine>,
}

/// The host parts moved onto the engine thread: the copied vtable plus
/// the opaque context the callbacks receive.
#[derive(Clone, Copy)]
struct HostParts {
    vtable: MkHostVtable,
    ctx: *mut c_void,
}

// SAFETY: `ctx` is host-owned state the contract keeps valid until
// `release` runs; every callback — including `release` — fires on the
// one engine thread that owns `HostParts`, so no data crosses threads
// concurrently.
unsafe impl Send for HostParts {}

/// The `Host` impl the engine thread runs: forwards `load`/`commit`
/// into the host vtable on that thread.
struct FfiHost {
    vtable: MkHostVtable,
    ctx: *mut c_void,
}

impl Host for FfiHost {
    fn load(&mut self) -> Result<Vec<u8>, HostError> {
        let Some(load) = self.vtable.load else {
            return Err(HostError::new());
        };
        let mut ptr: *const u8 = ptr::null();
        let mut len = 0usize;
        // SAFETY: `load` is a live host callback and both out-pointers
        // name valid locals. The contract keeps the returned buffer
        // readable until the `mk_bridge_open` that triggered this call
        // returns; it is copied before that.
        let result = unsafe { load(self.ctx, &raw mut ptr, &raw mut len) };
        if result != 0 {
            return Err(HostError::new());
        }
        if ptr.is_null() {
            return if len == 0 {
                Ok(Vec::new())
            } else {
                Err(HostError::new())
            };
        }
        // SAFETY: per the contract above, [ptr, ptr+len) is a readable
        // host buffer for this call's duration.
        Ok(unsafe { std::slice::from_raw_parts(ptr, len) }.to_vec())
    }

    fn commit(&mut self, batch: &[u8]) -> Result<(), HostError> {
        let Some(commit) = self.vtable.commit else {
            return Err(HostError::new());
        };
        // SAFETY: `commit` is a live host callback; the batch slice stays
        // readable for the call's duration, which is all the contract
        // grants the host.
        let result = unsafe { commit(self.ctx, batch.as_ptr(), batch.len()) };
        if result == 0 {
            Ok(())
        } else {
            Err(HostError::new())
        }
    }
}

/// The engine thread body: build the runtime, report init, run jobs
/// FIFO, then close out the bridge and `release` the host context.
fn engine_main(
    rx: Receiver<Job>,
    phase: &AtomicU8,
    host: HostParts,
    init: &Sender<Result<(), ()>>,
) {
    // The bridge polls its futures with a noop waker; a `current_thread`
    // runtime in context supplies the timer registration MDK's deferred
    // work needs, and one driver turn per job (below) lets an elapsed
    // deadline fire on the next poll — the native parity of the wasm
    // shell's gloo timers firing between calls.
    let Ok(runtime) = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
    else {
        let _sent = init.send(Err(()));
        return;
    };
    let _init_sent = init.send(Ok(()));
    let mut bridge = Some(Bridge::new(FfiHost {
        vtable: host.vtable,
        ctx: host.ctx,
    }));
    for job in rx {
        match job {
            Job::Shutdown { reply } => {
                close_and_drop(&mut bridge);
                if let Some(reply) = reply {
                    let _shutdown_sent = reply.send(());
                }
                break;
            }
            job => {
                let reply = job.reply_sender();
                let outcome =
                    catch_unwind(AssertUnwindSafe(|| execute(&runtime, &mut bridge, &job)));
                let bytes = outcome.unwrap_or_else(|_| poison_job(&mut bridge, phase));
                if let Some(reply) = reply {
                    let _reply_sent = reply.send(bytes);
                }
            }
        }
    }
    if let Some(release) = host.vtable.release {
        // SAFETY: the contract — exactly once, on this engine thread,
        // after the bridge was dropped.
        unsafe { release(host.ctx) }
    }
}

/// `Bridge::close` then drop, each under its own `catch_unwind` so a
/// failing close still lets the drop (and `release`) run.
fn close_and_drop(bridge: &mut Option<Bridge<FfiHost>>) {
    let Some(mut taken) = bridge.take() else {
        return;
    };
    let _closed = catch_unwind(AssertUnwindSafe(|| taken.close()));
    let _dropped = catch_unwind(AssertUnwindSafe(|| drop(taken)));
}

/// A panicking job leaves the bridge in an unknown state: drop it,
/// poison the handle and answer `poisoned`.
fn poison_job(bridge: &mut Option<Bridge<FfiHost>>, phase: &AtomicU8) -> Vec<u8> {
    if let Some(bridge) = bridge.take() {
        let _dropped = catch_unwind(AssertUnwindSafe(|| drop(bridge)));
    }
    phase.store(engine::PHASE_POISONED, Ordering::Release);
    encode_error(&BridgeError::Poisoned)
}

/// Run one request job inside the runtime: `block_on` puts the bridge's
/// polls in runtime context, and the leading `yield_now` turns the time
/// driver once so a deadline that elapsed while the job was queued fires
/// on the first poll below.
#[cfg_attr(
    feature = "test-panic",
    allow(
        clippy::panic,
        reason = "the test-panic job exists to panic on the engine thread"
    )
)]
fn execute(
    runtime: &tokio::runtime::Runtime,
    bridge: &mut Option<Bridge<FfiHost>>,
    job: &Job,
) -> Vec<u8> {
    runtime.block_on(async {
        tokio::task::yield_now().await;
        match (bridge.as_mut(), job) {
            (Some(bridge), Job::Open { request, .. }) => bridge.open(request),
            (Some(bridge), Job::Call { request, .. }) => bridge.call(request),
            (Some(bridge), Job::Drain { .. }) => bridge.drain(),
            #[cfg(feature = "test-panic")]
            (Some(_), Job::Panic { .. }) => {
                panic!("mk-ffi test-panic job")
            }
            // Bridge already dropped (poisoned): answer at this layer.
            (None, _) => encode_error(&BridgeError::Poisoned),
            // Only `Shutdown` remains; the loop answers it directly.
            (Some(_), _) => encode_error(&BridgeError::Internal {
                site: "job".to_owned(),
            }),
        }
    })
}

/// The ABI-side answer when the catch-all unwinding guard caught a
/// panic: poison the handle when there is one, then encode `poisoned`.
fn poison_outside(engine: Option<&Arc<Engine>>) -> Vec<u8> {
    if let Some(engine) = engine {
        engine.poison();
    }
    encode_error(&BridgeError::Poisoned)
}

/// Clone the engine `Arc` behind an ABI pointer, keeping the engine
/// state alive for the whole call even when `free` drops the box.
fn engine_of(handle: *mut MkBridge) -> Option<Arc<Engine>> {
    if handle.is_null() {
        return None;
    }
    // SAFETY: caller contract — a non-null handle came from
    // `mk_bridge_new` and is valid at entry.
    Some(unsafe { Arc::clone(&(*handle).engine) })
}

/// Validate a request pointer/length pair without touching the bridge.
/// Oversized inputs are refused before the pointer is ever read.
fn input_slice<'a>(ptr: *const u8, len: usize) -> Result<&'a [u8], BridgeError> {
    if len > MAX_REQUEST_BYTES {
        return Err(BridgeError::TooLarge {
            limit: MAX_REQUEST_BYTES as u64,
        });
    }
    if ptr.is_null() {
        return if len == 0 {
            Ok(&[])
        } else {
            Err(BridgeError::InvalidRequest {
                reason: "shape".to_owned(),
            })
        };
    }
    // SAFETY: caller contract — a non-null input pointer names a
    // readable buffer of `len` bytes for the call's duration; `len` was
    // capped above so absurd sizes never reach here.
    Ok(unsafe { std::slice::from_raw_parts(ptr, len) })
}

/// Hand `bytes` to the caller as an owned `MkBytes`; a null `out` drops
/// the result.
fn write_out(out: *mut MkBytes, bytes: Vec<u8>) {
    if out.is_null() {
        return;
    }
    let mut boxed = std::mem::ManuallyDrop::new(bytes.into_boxed_slice());
    let leaked = MkBytes {
        ptr: boxed.as_mut_ptr(),
        len: boxed.len(),
    };
    // SAFETY: caller contract — `out` names writable `MkBytes` storage.
    unsafe { ptr::write(out, leaked) }
}

/// The shared open/call path: validate, dispatch, map the outcome to an
/// encoded envelope.
fn request_bytes(engine: Option<Arc<Engine>>, ptr: *const u8, len: usize, call: bool) -> Vec<u8> {
    let Some(engine) = engine else {
        return encode_error(&BridgeError::NotOpen);
    };
    let request = match input_slice(ptr, len) {
        Ok(slice) => slice.to_vec(),
        Err(error) => return encode_error(&error),
    };
    let make = |reply: Sender<Vec<u8>>| {
        if call {
            Job::Call { request, reply }
        } else {
            Job::Open { request, reply }
        }
    };
    match engine.submit(make) {
        Ok(bytes) => bytes,
        Err(Phase::Poisoned) => encode_error(&BridgeError::Poisoned),
        Err(_) => encode_error(&BridgeError::Closed),
    }
}

/// Create a bridge handle and its engine thread; NULL on a null vtable,
/// a failed spawn or a failed runtime build (`release` never runs then).
///
/// # Safety
///
/// `vtable`, when non-null, must name a readable `MkHostVtable`. The
/// callbacks it carries and the `ctx` ownership follow the
/// `MkHostVtable` contract.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn mk_bridge_new(
    vtable: *const MkHostVtable,
    ctx: *mut c_void,
) -> *mut MkBridge {
    catch_unwind(AssertUnwindSafe(|| {
        if vtable.is_null() {
            return ptr::null_mut();
        }
        // SAFETY: caller contract — `vtable` names a readable
        // `MkHostVtable`; copying it is what the ABI specifies.
        let vtable = unsafe { ptr::read(vtable) };
        let host = HostParts { vtable, ctx };
        let (init, wait) = channel();
        let Ok(engine) = Engine::spawn("mk-engine", ENGINE_STACK_BYTES, move |rx, phase| {
            engine_main(rx, &phase, host, &init);
        }) else {
            return ptr::null_mut();
        };
        if matches!(wait.recv(), Ok(Ok(()))) {
            Box::into_raw(Box::new(MkBridge {
                engine: Arc::new(engine),
            }))
        } else {
            // Init failed (or the thread died): `release` must not run —
            // the engine main returned before the loop.
            engine.close();
            ptr::null_mut()
        }
    }))
    .unwrap_or(ptr::null_mut())
}

/// `open` the session; writes the encoded open response to `out`.
///
/// # Safety
///
/// `handle` must come from `mk_bridge_new` (null answers `not_open`),
/// the input must follow the `input_slice` contract, and `out`, when
/// non-null, must name writable `MkBytes` storage.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn mk_bridge_open(
    handle: *mut MkBridge,
    ptr: *const u8,
    len: usize,
    out: *mut MkBytes,
) {
    let engine = engine_of(handle);
    let bytes = catch_unwind(AssertUnwindSafe(|| {
        request_bytes(engine.clone(), ptr, len, false)
    }))
    .unwrap_or_else(|_| poison_outside(engine.as_ref()));
    write_out(out, bytes);
}

/// Dispatch one `call` envelope; writes the encoded response to `out`.
///
/// # Safety
///
/// Same contract as `mk_bridge_open`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn mk_bridge_call(
    handle: *mut MkBridge,
    ptr: *const u8,
    len: usize,
    out: *mut MkBytes,
) {
    let engine = engine_of(handle);
    let bytes = catch_unwind(AssertUnwindSafe(|| {
        request_bytes(engine.clone(), ptr, len, true)
    }))
    .unwrap_or_else(|_| poison_outside(engine.as_ref()));
    write_out(out, bytes);
}

/// Drain queued session events; writes the encoded drain response to
/// `out`.
///
/// # Safety
///
/// Same handle/`out` contract as `mk_bridge_open`; takes no input.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn mk_bridge_drain(handle: *mut MkBridge, out: *mut MkBytes) {
    let engine = engine_of(handle);
    let bytes = catch_unwind(AssertUnwindSafe(|| {
        let Some(engine) = engine.clone() else {
            return encode_error(&BridgeError::NotOpen);
        };
        match engine.submit(|reply| Job::Drain { reply }) {
            Ok(bytes) => bytes,
            Err(Phase::Poisoned) => encode_error(&BridgeError::Poisoned),
            Err(_) => encode_error(&BridgeError::Closed),
        }
    }))
    .unwrap_or_else(|_| poison_outside(engine.as_ref()));
    write_out(out, bytes);
}

/// Close the session: blocks until the engine thread ran `Bridge::close`
/// and exited. Idempotent; null is a no-op. Afterwards `open`/`call`/
/// `drain` answer `closed` (or `poisoned` on a poisoned handle).
///
/// # Safety
///
/// `handle` must come from `mk_bridge_new` and not have been freed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn mk_bridge_close(handle: *mut MkBridge) {
    let _outcome = catch_unwind(AssertUnwindSafe(|| {
        if let Some(engine) = engine_of(handle) {
            engine.close();
        }
    }));
}

/// Destroy the handle. Never blocks; null is a no-op.
///
/// Without a prior `mk_bridge_close` it enqueues close+shutdown and
/// detaches the engine thread, which still runs `release` once. `free`
/// may run at most once; calls already in flight keep the engine state
/// alive through their own `Arc` and finish (or answer `poisoned` if
/// their job lands behind the shutdown).
///
/// # Safety
///
/// `handle` must come from `mk_bridge_new` and not have been freed;
/// `free` may run at most once.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn mk_bridge_free(handle: *mut MkBridge) {
    let _outcome = catch_unwind(AssertUnwindSafe(|| {
        if !handle.is_null() {
            // SAFETY: caller contract — `handle` came from
            // `mk_bridge_new` and is freed exactly once.
            let bridge = unsafe { Box::from_raw(handle) };
            bridge.engine.detach();
        }
    }));
}

/// Release an `MkBytes` produced by this library. `{NULL, 0}` and any
/// zero-length buffer are safe; freeing a buffer not produced here is a
/// caller error.
///
/// # Safety
///
/// `bytes` must have been produced by this library and not freed before.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn mk_bytes_free(bytes: MkBytes) {
    let _outcome = catch_unwind(AssertUnwindSafe(|| {
        if !bytes.ptr.is_null() {
            // SAFETY: `write_out` leaks a `Box<[u8]>` and hands out its
            // `ptr`/`len`; reconstructing that same `Box<[u8]>` is the
            // exact inverse.
            drop(unsafe { Box::from_raw(ptr::slice_from_raw_parts_mut(bytes.ptr, bytes.len)) });
        }
    }));
}

/// Feature `test-panic` only: run a panicking job on the engine thread
/// so tests can prove panic containment. Never in shipped builds.
///
/// # Safety
///
/// Same handle/`out` contract as `mk_bridge_drain`.
#[cfg(feature = "test-panic")]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn mk_bridge_test_panic(handle: *mut MkBridge, out: *mut MkBytes) {
    let engine = engine_of(handle);
    let bytes = catch_unwind(AssertUnwindSafe(|| {
        let Some(engine) = engine.clone() else {
            return encode_error(&BridgeError::NotOpen);
        };
        match engine.submit(|reply| Job::Panic { reply }) {
            Ok(bytes) => bytes,
            Err(Phase::Poisoned) => encode_error(&BridgeError::Poisoned),
            Err(_) => encode_error(&BridgeError::Closed),
        }
    }))
    .unwrap_or_else(|_| poison_outside(engine.as_ref()));
    write_out(out, bytes);
}
