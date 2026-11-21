//! Marmot kit core: a pure-Rust, persistent account-device session over the
//! MDK engine, and the key-value storage backend beneath it.
//!
//! Two layers:
//!
//! * [`Session`] is the product API. It drives `cgka-engine` over
//!   [`KvStorageProvider`], freezes every transport artifact as a durable
//!   outbox obligation before it can leave the device, and exposes a
//!   plain-data surface ([`Event`], [`Outbound`], [`GroupSnapshot`]) with no
//!   third-party types. All host I/O is injected: durable bytes go through
//!   [`kv::KvHost`], signatures through [`AccountSigner`], time through
//!   [`Clock`], and the network is entirely the host's — the session only
//!   hands out exactly what to publish and absorbs the per-relay verdicts.
//! * [`KvStorageProvider`] is the `cgka_traits::storage::StorageProvider`
//!   under it: a semantic mirror of MDK's `storage-sqlite` backend
//!   (v0.18.0) over a minimal transactional batch interface a host can
//!   implement on `IndexedDB`, MMKV, or an existing embedded database.
//!
//! Module layering: [`kv`] is the host boundary (atomic batches, one commit
//! per committed unit). `store` keeps the in-memory mirror plus transaction
//! overlays over it. `provider` implements every Marmot storage sub-trait,
//! `mls` implements the `OpenMLS` storage trait, and `snapshot` owns group
//! snapshot/checkpoint capture, restore, and the replay-state fingerprint.
//! `keys` is the single source of every row key and scan prefix. `types`,
//! [`key_package`], `outbox`, `clock`, `signer` and `events` hold the
//! session layer's data types and injected ports; `session` is the
//! coordinator.
//!
//! Memory model: the mirror holds a copy of every durable row, so resident
//! size follows durable size. Pruning mirrors `storage-sqlite` exactly:
//! ingress dedup markers are bounded at 4,096 FIFO (see
//! `provider::INGRESS_DEDUP_MARKER_CAPACITY`); processed transport ids and
//! group-scoped `OpenMLS` rows are unbounded but are deleted with their
//! group (the `ON DELETE CASCADE` parity — upstream keeps them for the
//! group's lifetime too); every release/prune call upstream defines is
//! implemented. Row families `storage-sqlite` keeps unbounded (message
//! ledger, queued intents, snapshots and checkpoints, `OpenMLS` epoch
//! material, welcomes, account-device records) stay unbounded here for the
//! same reason: their lifecycle is owned by the engine, not the store.
//! Transaction overlays and the after-commit callback queue grow only with
//! live transaction depth.
//!
//! Security: errors and diagnostics never contain account ids, group ids,
//! message ids, relay URLs, pubkeys, payloads, or key material.
//! Secret-bearing `OpenMLS` rows are moved through
//! [`zeroize::Zeroizing`] buffers at the codec boundary.

pub mod agent_stream;
mod clock;
mod delivered;
mod error;
mod events;
pub mod key_package;
mod keys;
pub mod kv;
pub mod media;
mod mls;
mod outbox;
mod pacing;
mod provider;
mod semantics;
mod ser;
mod session;
mod signer;
mod snapshot;
mod store;
#[cfg(feature = "testing")]
pub mod testing;
mod types;

pub use clock::{Clock, Time};
pub use error::{MetaRow, OpenError};
pub use events::{
    Availability, Disposition, GroupChange, InputCategory, InputOutcome, InputReport, LocalState,
    ReceivedMessage, SessionEvent,
};
pub use kv::{BatchCodecError, CommitSeq, KvBatch, KvHost, KvOp};
pub use mls::KvOpenMlsStorage;
pub use outbox::{Outbound, OutboundPurpose, PublishResolution, RelayResult, RelayStatus};
pub use provider::{INGRESS_DEDUP_MARKER_CAPACITY, KvStorageProvider};
pub use semantics::{DropReason, MessageIntent, Semantics};
pub use session::OpenError as SessionOpenError;
pub use session::{
    AppMessage, AttachmentInput, ComponentUpdate, CreateGroup, DestinationPolicy, GroupAvatar,
    GroupSnapshot, GroupState, Invitee, MediaMessage, MemberSnapshot, Opened, PendingAttachment,
    PendingProof, ProofError, ProofRejected, SealedAttachment, SendOutcome, SentMessage, Session,
    SessionConfig, SessionError, TickReport,
};
pub use signer::{AccountSigner, SignerError, SignerFuture};
pub use types::{AccountId, Event, EventError, EventId, GroupId, OutboundId, UnsignedEvent};

// Dev-dependencies serve only the integration tests; binding them under
// `cfg(test)` keeps `unused_crate_dependencies` honest for the lib's own
// test build without hiding real problems in normal builds.
#[cfg(test)]
use {openmls_rust_crypto as _, proptest as _, storage_sqlite as _, tokio as _};
