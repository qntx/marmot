//! Marmot kit core: a pure-Rust persistent key-value storage provider for the
//! MDK engine, and — in later milestones — the session API built on it.
//!
//! The crate's only product surface today is [`KvStorageProvider`]: a
//! `cgka_traits::storage::StorageProvider` whose durable state is a flat set of
//! versioned key-value rows held by a host through the [`kv`] port. It is a
//! semantic mirror of MDK's `storage-sqlite` backend (v0.16.0) over a minimal
//! transactional batch interface a host can implement on `IndexedDB`, MMKV, or
//! an existing embedded database.
//!
//! Layering: [`kv`] is the host boundary (atomic batches, one commit per
//! committed unit). `store` keeps the in-memory mirror plus transaction
//! overlays over it. `provider` implements every Marmot storage sub-trait on
//! top, `mls` implements the `OpenMLS` storage trait, and `snapshot` owns
//! group snapshot/checkpoint capture, restore, and the replay-state
//! fingerprint. `keys` is the single source of every row key and scan prefix
//! in the crate.
//!
//! Memory model: the mirror holds a copy of every durable row, so resident
//! size follows durable size. Pruning mirrors `storage-sqlite` exactly:
//! ingress dedup markers are bounded at 4,096 FIFO (see
//! `provider::INGRESS_DEDUP_MARKER_CAPACITY`); processed transport ids and
//! group-scoped `OpenMLS` rows are unbounded but are deleted with their group
//! (the `ON DELETE CASCADE` parity — upstream keeps them for the group's
//! lifetime too); every release/prune call upstream defines is implemented.
//! Row families `storage-sqlite` keeps unbounded (message ledger, queued
//! intents, snapshots and checkpoints, `OpenMLS` epoch material, welcomes,
//! account-device records) stay unbounded here for the same reason: their
//! lifecycle is owned by the engine, not the store. Transaction overlays and
//! the after-commit callback queue grow only with live transaction depth.
//!
//! Security: errors and diagnostics never contain account ids, group ids,
//! message ids, relay URLs, pubkeys, payloads, or key material. Secret-bearing
//! `OpenMLS` rows are moved through [`zeroize::Zeroizing`] buffers at the codec
//! boundary.

mod error;
mod keys;
pub mod kv;
mod mls;
mod provider;
mod ser;
mod snapshot;
mod store;
#[cfg(feature = "testing")]
pub mod testing;

pub use error::{MetaRow, OpenError};
pub use kv::{BatchCodecError, CommitSeq, KvBatch, KvHost, KvOp};
pub use mls::KvOpenMlsStorage;
pub use provider::{INGRESS_DEDUP_MARKER_CAPACITY, KvStorageProvider};

// Dev-dependencies serve only the integration tests; binding them under
// `cfg(test)` keeps `unused_crate_dependencies` honest for the lib's own
// test build without hiding real problems in normal builds.
#[cfg(test)]
use {
    base64 as _, cgka_engine as _, getrandom as _, hex as _, nostr as _, openmls_rust_crypto as _,
    proptest as _, storage_sqlite as _, tokio as _, transport_nostr_peeler as _,
};
