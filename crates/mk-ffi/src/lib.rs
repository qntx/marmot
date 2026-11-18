//! `mk-ffi` — the C ABI shell over `mk-bridge` for native iOS/Android hosts.
//!
//! The crate marshals nothing: every `mk_bridge_*` entry point moves bytes
//! between the caller and a [`mk_bridge::Bridge`] that lives on a dedicated
//! engine thread, so the `!Send` bridge never crosses a thread boundary and
//! all host callbacks run synchronously on that thread.
//!
//! One `mk_bridge_new` spawns one engine thread; `open`/`call`/`drain`
//! submit jobs over a channel and block the calling thread on a per-job
//! reply, so jobs run strictly FIFO even under concurrent native callers.
//! A panicking job poisons the handle: later calls answer an encoded
//! `poisoned` envelope, the bridge is dropped on the engine thread, and
//! `close`/`free` stay safe. `mk_bridge_close` blocks until the engine
//! thread ran `Bridge::close` and exited; `mk_bridge_free` never blocks —
//! it enqueues shutdown and detaches the thread.
//!
//! Engine futures register timers through Tokio, so the engine thread owns
//! a `current_thread` runtime with a time driver; see `README.md` for the
//! measured runtime requirements and the engine-thread stack size.
//!
//! The generated C header lives at `include/marmot_ffi.h` and documents
//! the ownership and threading contract; `tests/header.rs` keeps it in
//! sync with `cbindgen.toml`.
#![deny(unsafe_op_in_unsafe_fn)]

mod abi;
mod engine;

#[cfg(feature = "test-panic")]
pub use abi::mk_bridge_test_panic;
pub use abi::{
    MkBridge, MkBytes, MkHostVtable, mk_bridge_call, mk_bridge_close, mk_bridge_drain,
    mk_bridge_free, mk_bridge_new, mk_bridge_open, mk_bytes_free,
};

// Dev-dependencies serve only the integration tests; binding them under
// `cfg(test)` keeps `unused_crate_dependencies` honest for the lib's own
// test build without hiding real problems in normal builds.
#[cfg(test)]
use {cbindgen as _, hex as _, mk_core as _, serde_json as _};
