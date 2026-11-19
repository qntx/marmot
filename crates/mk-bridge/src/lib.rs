//! `mk-bridge` is the byte-oriented host protocol between [`mk_core`] and a
//! host embedder (wasm/JavaScript today, any future platform tomorrow).
//!
//! A host owns storage, the clock and the account signer; the bridge owns a
//! [`mk_core::Session`]. Every entry point is bytes-in/bytes-out JSON and
//! never panics:
//!
//! - [`Bridge::open`] takes the open config (account, time, policy), loads
//!   durable rows through [`Host::load`] and answers `ready`,
//!   `needs_account_proof` or `error`.
//! - [`Bridge::call`] takes `{now, request}` envelopes. Async engine work may
//!   suspend mid-call on an [`mk_core::AccountSigner`] operation; the bridge
//!   answers `signer_pending` and resumes on `signer_reply`.
//! - [`Bridge::drain`] returns the queued session events.
//! - [`Bridge::close`] releases the session.
//!
//! Durability flows through two injected ports: [`mk_core::KvHost`] batches
//! queue inside the session and are flushed to [`Host::commit`] after every
//! poll and before every response; the host acknowledges them with
//! `ack_durable`, which is what releases gated outbound artifacts. A
//! [`HostError`] poisons the bridge: the session is over and must be
//! reopened from durable rows.
//!
//! Timing note for native embedders: engine futures register timers through
//! the surrounding async runtime, so calls that drive engine work must run
//! inside a runtime context with a time driver (the same requirement
//! `mk-core` documents). The bridge itself is runtime-free.
#![forbid(unsafe_code)]

mod bridge;
mod clock;
mod error;
mod host;
pub mod protocol;
mod rows;
mod signer;

pub use bridge::Bridge;
pub use error::{BridgeError, encode_error};
pub use host::{Host, HostError};
pub use protocol::{MAX_REQUEST_BYTES, PROTOCOL_VERSION};
pub use rows::{Row, RowsError, decode_rows, encode_rows};

#[cfg(test)]
use {tokio as _, tracing as _};
