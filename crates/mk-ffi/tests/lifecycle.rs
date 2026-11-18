//! Handle lifecycle: use-after-close answers `closed`, double close is a
//! no-op, `free` without `close` returns promptly even while a job runs
//! on the engine thread, and `release` fires exactly once.
//!
//! Every test except `free_does_not_block_on_in_flight_work` stays
//! session-free — raw handles, no `LocalSigner` — so they run under
//! Miri.

#![allow(unsafe_code, reason = "the tests exercise the C entry points")]
#![allow(
    unused_crate_dependencies,
    clippy::borrow_as_ptr,
    clippy::cast_lossless,
    clippy::cast_possible_truncation,
    clippy::clone_on_ref_ptr,
    clippy::disallowed_methods,
    clippy::doc_markdown,
    clippy::excessive_nesting,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::manual_dangling_ptr,
    clippy::manual_let_else,
    clippy::map_unwrap_or,
    clippy::match_wildcard_for_single_variants,
    clippy::missing_const_for_fn,
    clippy::missing_docs_in_private_items,
    clippy::missing_fields_in_debug,
    clippy::missing_panics_doc,
    clippy::multiple_unsafe_ops_per_block,
    clippy::needless_pass_by_value,
    clippy::option_if_let_else,
    clippy::panic,
    clippy::shadow_reuse,
    clippy::shadow_unrelated,
    clippy::significant_drop_tightening,
    clippy::tests_outside_test_module,
    clippy::too_many_lines,
    clippy::undocumented_unsafe_blocks,
    clippy::unnecessary_wraps,
    clippy::unwrap_in_result,
    clippy::unwrap_used,
    unreachable_pub,
    dead_code,
    reason = "test code"
)]

mod support;

use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use marmot_ffi::{
    MkBytes, mk_bridge_call, mk_bridge_close, mk_bridge_drain, mk_bridge_free, mk_bridge_new,
    mk_bridge_open,
};
use mk_bridge::protocol::*;
use support::*;

/// After `mk_bridge_close`, `open`/`call`/`drain` answer `closed` from
/// the ABI layer (the engine thread is gone).
#[test]
fn use_after_close_answers_closed() {
    let (_state, releases, ctx) = host_ctx();
    let vtable = vtable();
    let handle = unsafe { mk_bridge_new(&vtable, ctx) };
    assert!(!handle.is_null());
    unsafe { mk_bridge_close(handle) };

    let mut out = MkBytes {
        ptr: std::ptr::null_mut(),
        len: 0,
    };
    let envelope = call_envelope(
        Time {
            unix_ms: 1_700_000_000_000,
            monotonic_ms: 2_000,
        },
        &Request::Groups(NoArgs {}),
    );
    unsafe {
        mk_bridge_call(handle, envelope.as_ptr(), envelope.len(), &mut out);
        assert_eq!(error_kind_of(&take_bytes(out)), "closed");
        mk_bridge_open(handle, envelope.as_ptr(), envelope.len(), &mut out);
        assert_eq!(error_kind_of(&take_bytes(out)), "closed");
        mk_bridge_drain(handle, &mut out);
        assert_eq!(error_kind_of(&take_bytes(out)), "closed");
        mk_bridge_free(handle);
    }
    wait_for_release(&releases);
}

/// A second `mk_bridge_close` is a no-op.
#[test]
fn double_close_is_noop() {
    let (_state, releases, ctx) = host_ctx();
    let vtable = vtable();
    let handle = unsafe { mk_bridge_new(&vtable, ctx) };
    assert!(!handle.is_null());
    unsafe {
        mk_bridge_close(handle);
        mk_bridge_close(handle);
        mk_bridge_free(handle);
    }
    wait_for_release(&releases);
    assert_eq!(releases.load(Ordering::SeqCst), 1);
}

/// `close` then `free`: `release` runs exactly once, on the engine
/// thread's exit path.
#[test]
fn close_then_free_releases_once() {
    let (_state, releases, ctx) = host_ctx();
    let vtable = vtable();
    let handle = unsafe { mk_bridge_new(&vtable, ctx) };
    assert!(!handle.is_null());
    unsafe {
        mk_bridge_close(handle);
        // After the join the engine thread is gone — release already ran.
        mk_bridge_free(handle);
    }
    assert_eq!(releases.load(Ordering::SeqCst), 1);
}

/// `mk_bridge_free` without a prior `close` enqueues shutdown and
/// detaches: `release` still runs exactly once, asynchronously.
#[test]
fn free_without_close_releases_once() {
    let (_state, releases, ctx) = host_ctx();
    let vtable = vtable();
    let handle = unsafe { mk_bridge_new(&vtable, ctx) };
    assert!(!handle.is_null());
    unsafe { mk_bridge_free(handle) };
    wait_for_release(&releases);
    assert_eq!(releases.load(Ordering::SeqCst), 1);
}

/// `mk_bridge_free` never blocks on engine work: with a call in flight
/// whose `commit` sleeps, `free` still returns immediately while the
/// detached thread finishes the job and runs `release`.
#[test]
#[cfg_attr(miri, ignore = "miri cannot run secp256k1-sys C code (session open)")]
fn free_does_not_block_on_in_flight_work() {
    let member = Member::new();
    member.state.lock().unwrap().commit_delay_ms = 400;
    let config = member.config();
    let handle = member.handle as usize;
    // `open` always commits (the session's schema/version rows): its job
    // parks inside `commit` for ~400 ms on the engine thread.
    let worker = std::thread::spawn(move || {
        let handle = handle as *mut marmot_ffi::MkBridge;
        ffi_open(handle, &config)
    });
    // Let the job reach `commit`, then free: it must return well before
    // the 400 ms callback ends.
    std::thread::sleep(Duration::from_millis(100));
    let releases = member.releases.clone();
    let started = Instant::now();
    member.free();
    let elapsed = started.elapsed();
    assert!(
        elapsed < Duration::from_millis(200),
        "mk_bridge_free blocked for {elapsed:?}"
    );
    // The detached engine thread finishes the job, then shuts down and
    // releases the host context.
    let reply = worker.join().expect("worker");
    assert!(
        serde_json::from_slice::<serde_json::Value>(&reply).is_ok(),
        "in-flight call still answered: {reply:?}"
    );
    wait_for_release(&releases);
    assert_eq!(releases.load(Ordering::SeqCst), 1);
}

/// A host `load` failure poisons the handle: later calls answer
/// `poisoned`, `close` still joins the engine thread, `release` runs
/// once.
#[test]
fn host_failure_poisons_then_close_releases() {
    let (state, releases, ctx) = host_ctx();
    state.lock().unwrap().fail_load = true;
    let vtable = vtable();
    let handle = unsafe { mk_bridge_new(&vtable, ctx) };
    assert!(!handle.is_null());
    let config = fixed_config();
    let bytes = ffi_open(handle, &config);
    assert_eq!(error_kind_of(&bytes), "host_failed");
    let envelope = call_envelope(
        Time {
            unix_ms: 1_700_000_000_000,
            monotonic_ms: 2_000,
        },
        &Request::Groups(NoArgs {}),
    );
    let bytes = ffi_call(handle, &envelope);
    assert_eq!(error_kind_of(&bytes), "poisoned");
    unsafe {
        mk_bridge_close(handle);
        mk_bridge_free(handle);
    }
    wait_for_release(&releases);
    assert_eq!(releases.load(Ordering::SeqCst), 1);
}
