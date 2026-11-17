//! White Noise (`wn`/`wnd`) interop scenarios for `mk-core`.
//!
//! The crate carries no library code: every scenario lives in `tests/` and
//! drives `wn`/`wnd` built from the pinned MDK rev plus a real in-process
//! NIP-01 WebSocket relay, exercising the public `Session` API exactly the
//! way a host application would. See `CONTRIBUTING.md` for how to build the
//! binaries and run the scenarios.

#[cfg(test)]
use {
    axum as _, hex as _, mk_bridge as _, mk_core as _, nostr as _, nostr_sdk as _, serde_json as _,
    sha2 as _, tokio as _,
};
