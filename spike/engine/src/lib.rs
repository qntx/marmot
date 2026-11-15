//! M0 spike: MDK `cgka-engine` over a host-KV storage provider (native target).
//!
//! Proves the engine runs end-to-end on a non-SQLite `StorageProvider`:
//! a deterministic BTreeMap mirror with layered transaction/savepoint
//! overlays in front of a host KV (`KvHost`), exercised through
//! `transport-nostr-peeler` and an in-process fake relay in tests.

pub mod harness;
pub mod kv;
pub mod mls;
pub mod provider;
pub mod store;

pub use kv::{InMemoryHost, KvHost, KvOp};
pub use provider::{KvStorageProvider, SNAP_CAPTURES, SNAP_RESTORES};
