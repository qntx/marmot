//! Input validation at the ABI boundary: null/oversized inputs answer
//! the existing encoded errors without ever touching the bridge, and a
//! null `out` parameter is tolerated.

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

use marmot_ffi::{
    MkBytes, MkHostVtable, mk_bridge_call, mk_bridge_close, mk_bridge_drain, mk_bridge_free,
    mk_bridge_new, mk_bridge_open, mk_bytes_free,
};
use mk_bridge::MAX_REQUEST_BYTES;
use mk_bridge::protocol::*;
use support::*;

/// Null handles answer `not_open` from every entry point.
#[test]
fn null_handle_answers_not_open() {
    let mut out = MkBytes {
        ptr: std::ptr::null_mut(),
        len: 0,
    };
    unsafe {
        mk_bridge_open(std::ptr::null_mut(), b"{}".as_ptr(), 2, &mut out);
        assert_eq!(error_kind_of(&take_bytes(out)), "not_open");
        mk_bridge_call(std::ptr::null_mut(), b"{}".as_ptr(), 2, &mut out);
        assert_eq!(error_kind_of(&take_bytes(out)), "not_open");
        mk_bridge_drain(std::ptr::null_mut(), &mut out);
        assert_eq!(error_kind_of(&take_bytes(out)), "not_open");
    }
}

/// `len > MAX_REQUEST_BYTES` answers `too_large` — the pointer is never
/// read, so an invalid pointer with an absurd length stays safe.
#[test]
fn oversized_input_answers_too_large_without_reading() {
    let (_state, releases, ctx) = host_ctx();
    let vtable = vtable();
    let handle = unsafe { mk_bridge_new(&vtable, ctx) };
    assert!(!handle.is_null());
    let mut out = MkBytes {
        ptr: std::ptr::null_mut(),
        len: 0,
    };
    unsafe {
        // A wild pointer the validation must not dereference.
        let wild = 0x1usize as *const u8;
        mk_bridge_call(handle, wild, MAX_REQUEST_BYTES + 1, &mut out);
        assert_eq!(error_kind_of(&take_bytes(out)), "too_large");
        mk_bridge_call(handle, wild, usize::MAX, &mut out);
        assert_eq!(error_kind_of(&take_bytes(out)), "too_large");
        mk_bridge_open(handle, wild, MAX_REQUEST_BYTES + 1, &mut out);
        assert_eq!(error_kind_of(&take_bytes(out)), "too_large");
        mk_bridge_free(handle);
    }
    wait_for_release(&releases);
}

/// A null pointer with `len > 0` answers `invalid_request`.
#[test]
fn null_ptr_with_len_answers_invalid_request() {
    let (_state, releases, ctx) = host_ctx();
    let vtable = vtable();
    let handle = unsafe { mk_bridge_new(&vtable, ctx) };
    assert!(!handle.is_null());
    let mut out = MkBytes {
        ptr: std::ptr::null_mut(),
        len: 0,
    };
    unsafe {
        mk_bridge_call(handle, std::ptr::null(), 7, &mut out);
        assert_eq!(error_kind_of(&take_bytes(out)), "invalid_request");
        mk_bridge_open(handle, std::ptr::null(), 7, &mut out);
        assert_eq!(error_kind_of(&take_bytes(out)), "invalid_request");
        mk_bridge_free(handle);
    }
    wait_for_release(&releases);
}

/// A null pointer with `len == 0` is an empty request — valid at the ABI
/// layer, rejected by the bridge's own decoder.
#[test]
fn empty_request_reaches_the_bridge() {
    let (_state, releases, ctx) = host_ctx();
    let vtable = vtable();
    let handle = unsafe { mk_bridge_new(&vtable, ctx) };
    assert!(!handle.is_null());
    let mut out = MkBytes {
        ptr: std::ptr::null_mut(),
        len: 0,
    };
    unsafe {
        mk_bridge_call(handle, std::ptr::null(), 0, &mut out);
        let kind = error_kind_of(&take_bytes(out));
        assert_eq!(kind, "invalid_request", "empty call: {kind}");
        mk_bridge_free(handle);
    }
    wait_for_release(&releases);
}

/// `out` may be null: the call still runs and nothing is written.
#[test]
#[cfg_attr(miri, ignore = "miri cannot run secp256k1-sys C code (session open)")]
fn null_out_is_tolerated() {
    let (_state, releases, ctx) = host_ctx();
    let vtable = vtable();
    let handle = unsafe { mk_bridge_new(&vtable, ctx) };
    assert!(!handle.is_null());
    let config = fixed_config();
    let envelope = call_envelope(
        Time {
            unix_ms: 1_700_000_000_000,
            monotonic_ms: 1_000,
        },
        &Request::Groups(NoArgs {}),
    );
    unsafe {
        mk_bridge_open(handle, config.as_ptr(), config.len(), std::ptr::null_mut());
        mk_bridge_call(
            handle,
            envelope.as_ptr(),
            envelope.len(),
            std::ptr::null_mut(),
        );
        mk_bridge_drain(handle, std::ptr::null_mut());
        mk_bridge_free(handle);
    }
    wait_for_release(&releases);
}

/// `mk_bytes_free`/`mk_bridge_close`/`mk_bridge_free` tolerate null.
#[test]
fn null_free_and_close_are_noops() {
    unsafe {
        mk_bytes_free(MkBytes {
            ptr: std::ptr::null_mut(),
            len: 0,
        });
        mk_bridge_close(std::ptr::null_mut());
        mk_bridge_free(std::ptr::null_mut());
    }
}

/// A null vtable fails `mk_bridge_new` and never runs `release`.
#[test]
fn null_vtable_returns_null() {
    let (_state, releases, ctx) = host_ctx();
    let handle = unsafe { mk_bridge_new(std::ptr::null(), ctx) };
    assert!(handle.is_null(), "null vtable must fail");
    // `release` never ran; reclaim the context ourselves.
    assert_eq!(releases.load(std::sync::atomic::Ordering::SeqCst), 0);
    unsafe { drop(Box::from_raw(ctx.cast::<FfiHostCtx>())) };
}

/// A vtable without `load`/`commit` fails `open` as a host failure and
/// poisons the handle; `release` still runs exactly once.
#[test]
fn missing_callbacks_fail_open_and_poison() {
    let (_state, releases, ctx) = host_ctx();
    let vtable = MkHostVtable {
        load: None,
        commit: None,
        release: vtable().release,
    };
    let handle = unsafe { mk_bridge_new(&vtable, ctx) };
    assert!(!handle.is_null());
    let config = fixed_config();
    let mut out = MkBytes {
        ptr: std::ptr::null_mut(),
        len: 0,
    };
    unsafe {
        mk_bridge_open(handle, config.as_ptr(), config.len(), &mut out);
        assert_eq!(error_kind_of(&take_bytes(out)), "host_failed");
        let envelope = call_envelope(
            Time {
                unix_ms: 1_700_000_000_000,
                monotonic_ms: 1_000,
            },
            &Request::Groups(NoArgs {}),
        );
        mk_bridge_call(handle, envelope.as_ptr(), envelope.len(), &mut out);
        assert_eq!(error_kind_of(&take_bytes(out)), "poisoned");
        mk_bridge_free(handle);
    }
    wait_for_release(&releases);
}
