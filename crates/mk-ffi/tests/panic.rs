//! Panic containment: a job that panics on the engine thread poisons
//! only its own handle — later calls answer `poisoned`, `close`/`free`
//! still work, `release` runs exactly once, and other handles are
//! unaffected. Requires the test-only `test-panic` feature.
//!
//! These tests stay session-free on purpose: no `open` means no
//! secp256k1, so the whole file runs under Miri.

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

#[cfg(feature = "test-panic")]
use std::sync::atomic::Ordering;

#[cfg(feature = "test-panic")]
use marmot_ffi::mk_bridge_test_panic;
#[cfg(feature = "test-panic")]
use marmot_ffi::{
    MkBytes, mk_bridge_call, mk_bridge_close, mk_bridge_drain, mk_bridge_free, mk_bridge_new,
    mk_bridge_open,
};
#[cfg(feature = "test-panic")]
use mk_bridge::protocol::*;
#[cfg(feature = "test-panic")]
use support::*;

/// After a panicking job, `open`/`call`/`drain` answer `poisoned` and
/// `release` still runs exactly once on `free`.
#[cfg(feature = "test-panic")]
#[test]
fn panicking_job_poisons_the_handle() {
    let (_state, releases, ctx) = host_ctx();
    let vtable = vtable();
    let handle = unsafe { mk_bridge_new(&vtable, ctx) };
    assert!(!handle.is_null());

    let mut out = MkBytes {
        ptr: std::ptr::null_mut(),
        len: 0,
    };
    let envelope = call_envelope(
        Time {
            unix_ms: 1_700_000_000_000,
            monotonic_ms: 1_000,
        },
        &Request::Groups(NoArgs {}),
    );
    unsafe {
        mk_bridge_test_panic(handle, &mut out);
        assert_eq!(error_kind_of(&take_bytes(out)), "poisoned");

        mk_bridge_call(handle, envelope.as_ptr(), envelope.len(), &mut out);
        assert_eq!(error_kind_of(&take_bytes(out)), "poisoned");
        mk_bridge_drain(handle, &mut out);
        assert_eq!(error_kind_of(&take_bytes(out)), "poisoned");
        mk_bridge_open(handle, envelope.as_ptr(), envelope.len(), &mut out);
        assert_eq!(error_kind_of(&take_bytes(out)), "poisoned");

        mk_bridge_free(handle);
    }
    wait_for_release(&releases);
    assert_eq!(releases.load(Ordering::SeqCst), 1);
}

/// A poisoned handle does not affect a second handle on the same host.
#[cfg(feature = "test-panic")]
#[test]
fn panic_isolates_handles() {
    let (_state, releases, ctx) = host_ctx();
    let (_other_state, other_releases, other_ctx) = host_ctx();
    let vtable = vtable();
    let (handle, other) = unsafe {
        (
            mk_bridge_new(&vtable, ctx),
            mk_bridge_new(&vtable, other_ctx),
        )
    };
    assert!(!handle.is_null() && !other.is_null());

    let mut out = MkBytes {
        ptr: std::ptr::null_mut(),
        len: 0,
    };
    unsafe {
        mk_bridge_test_panic(handle, &mut out);
        assert_eq!(error_kind_of(&take_bytes(out)), "poisoned");
    }
    // The other handle still answers normally — anything but
    // `poisoned` proves the panic stayed on its own handle.
    let bytes = ffi_call(other, b"{}");
    assert_ne!(error_kind_of(&bytes), "poisoned");

    // `close` on the poisoned handle still joins the engine thread.
    unsafe {
        mk_bridge_close(handle);
        mk_bridge_close(other);
        mk_bridge_free(handle);
        mk_bridge_free(other);
    }
    wait_for_release(&releases);
    wait_for_release(&other_releases);
    assert_eq!(releases.load(Ordering::SeqCst), 1);
    assert_eq!(other_releases.load(Ordering::SeqCst), 1);
}

/// A null handle answers `not_open` from the test entry point too.
#[cfg(feature = "test-panic")]
#[test]
fn test_panic_null_handle_answers_not_open() {
    let mut out = MkBytes {
        ptr: std::ptr::null_mut(),
        len: 0,
    };
    unsafe {
        mk_bridge_test_panic(std::ptr::null_mut(), &mut out);
        assert_eq!(error_kind_of(&take_bytes(out)), "not_open");
    }
}
