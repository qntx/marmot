//! Test and harness support for the KV host boundary.
//!
//! [`RecordingHost`] is an in-memory [`crate::kv::KvHost`] that applies every
//! committed batch to a durable row map, recording each batch as it arrives.
//! It can replay what a real host would hold (`durable_rows`), drop future
//! commits (`fail_after`), or pretend an earlier commit was the last one the
//! host persisted (`cut_after`) — the fixtures reopen and migration tests,
//! the session durability gate, and the interop harness all need.

use std::collections::BTreeMap;
use std::sync::Mutex;

use crate::clock::{Clock, Time};
use crate::kv::{CommitSeq, KvBatch, KvHost, KvOp};
use crate::provider::KvStorageProvider;
use crate::signer::{AccountSigner, SignerError, SignerFuture};
use crate::types::{AccountId, Event, UnsignedEvent};

/// An in-memory recording [`KvHost`].
///
/// `commit` applies the batch to the durable map strictly in order. The host
/// never mutates rows outside `commit` and never calls back into the store.
///
/// Failure injection is by commit count: [`Self::fail_after`] drops every
/// commit after the `n`th (the store keeps running against a silently stale
/// host — the crash window before the durability gate exists), and
/// [`Self::durable_rows`] reports exactly what a reopen would load.
#[derive(Debug, Default)]
pub struct RecordingHost {
    inner: Mutex<Inner>,
}

#[derive(Debug, Default)]
struct Inner {
    rows: BTreeMap<Vec<u8>, Vec<u8>>,
    batches: Vec<KvBatch>,
    /// Commits after this many are dropped instead of applied.
    fail_after: Option<usize>,
}

impl RecordingHost {
    /// A host with nothing durable yet.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Drop every commit after the `n`th. `0` drops them all.
    pub fn fail_after(&self, commits: usize) {
        let Ok(mut inner) = self.inner.lock() else {
            return;
        };
        inner.fail_after = Some(commits);
    }

    /// Accept all commits again.
    pub fn resume(&self) {
        let Ok(mut inner) = self.inner.lock() else {
            return;
        };
        inner.fail_after = None;
    }

    /// The batches this host has been asked to apply, in order.
    #[must_use]
    pub fn batches(&self) -> Vec<KvBatch> {
        self.inner
            .lock()
            .map(|inner| inner.batches.clone())
            .unwrap_or_default()
    }

    /// The committed batch sequence numbers, in order — the monotonicity
    /// probe for `seq` semantics.
    #[must_use]
    pub fn seqs(&self) -> Vec<u64> {
        self.inner
            .lock()
            .map(|inner| inner.batches.iter().map(|batch| batch.seq.get()).collect())
            .unwrap_or_default()
    }

    /// Every durable row, as a reopen hands them to
    /// [`crate::KvStorageProvider::open`].
    #[must_use]
    pub fn durable_rows(&self) -> Vec<(Vec<u8>, Vec<u8>)> {
        self.inner
            .lock()
            .map(|inner| {
                inner
                    .rows
                    .iter()
                    .map(|(key, value)| (key.clone(), value.clone()))
                    .collect()
            })
            .unwrap_or_default()
    }

    /// The durable rows as they would be had only the first `n` batches
    /// applied — a reopen point mid-history. `n` past the end clamps.
    #[must_use]
    pub fn rows_after_batches(&self, n: usize) -> Vec<(Vec<u8>, Vec<u8>)> {
        self.inner
            .lock()
            .map(|inner| {
                let mut rows = BTreeMap::new();
                for batch in inner.batches.iter().take(n) {
                    apply(&mut rows, batch);
                }
                rows.iter()
                    .map(|(key, value)| (key.clone(), value.clone()))
                    .collect()
            })
            .unwrap_or_default()
    }

    /// The seq of the last applied commit, for the session's durability ack.
    #[must_use]
    pub fn last_seq(&self) -> Option<CommitSeq> {
        self.inner
            .lock()
            .ok()
            .and_then(|inner| inner.batches.last().map(|batch| batch.seq))
    }
}

impl KvHost for RecordingHost {
    fn commit(&self, batch: KvBatch) {
        let Ok(mut inner) = self.inner.lock() else {
            return;
        };
        if inner
            .fail_after
            .is_some_and(|limit| inner.batches.len() >= limit)
        {
            inner.batches.push(batch);
            return;
        }
        apply(&mut inner.rows, &batch);
        inner.batches.push(batch);
    }
}

fn apply(rows: &mut BTreeMap<Vec<u8>, Vec<u8>>, batch: &KvBatch) {
    for op in &batch.ops {
        match op {
            KvOp::Put { key, value } => {
                rows.insert(key.clone(), value.clone());
            }
            KvOp::Delete { key } => {
                rows.remove(key);
            }
        }
    }
}

impl KvStorageProvider {
    /// Distinct capability descriptions interned so far. `KNOWN`
    /// descriptions resolve to statics without interning, so a provider
    /// that only ever saw registered descriptions reports `0`.
    #[must_use]
    pub fn interned_description_count(&self) -> usize {
        self.interned_count()
    }
}

/// A manually-driven [`Clock`].
///
/// `advance`/`set` move both fields together; tests control convergence
/// deadlines and event `created_at` deterministically.
#[derive(Debug)]
pub struct ManualClock {
    inner: Mutex<Time>,
}

impl ManualClock {
    /// A clock at a fixed modern wall time (monotonic starts at 0).
    #[must_use]
    pub const fn new() -> Self {
        Self::at_unix_ms(1_700_000_000_000)
    }

    /// A clock at `unix_ms` wall time (monotonic starts at 0).
    #[must_use]
    pub const fn at_unix_ms(unix_ms: u64) -> Self {
        Self {
            inner: Mutex::new(Time {
                monotonic_ms: 0,
                unix_ms,
            }),
        }
    }

    /// Advance both scales by `ms`.
    pub fn advance(&self, ms: u64) {
        let Ok(mut inner) = self.inner.lock() else {
            return;
        };
        inner.monotonic_ms = inner.monotonic_ms.saturating_add(ms);
        inner.unix_ms = inner.unix_ms.saturating_add(ms);
    }

    /// Set both scales.
    pub fn set(&self, monotonic_ms: u64, unix_ms: u64) {
        let Ok(mut inner) = self.inner.lock() else {
            return;
        };
        *inner = Time {
            monotonic_ms,
            unix_ms,
        };
    }
}

impl Default for ManualClock {
    fn default() -> Self {
        Self::new()
    }
}

impl Clock for ManualClock {
    fn now(&self) -> Time {
        self.inner.lock().map_or(
            Time {
                monotonic_ms: 0,
                unix_ms: 0,
            },
            |inner| *inner,
        )
    }
}

/// A local [`AccountSigner`] over the pinned rust-nostr fork's `Keys`.
///
/// Real schnorr signatures and real NIP-44 crypto — only the key custody is
/// fake (a generated keypair held in memory).
#[derive(Debug)]
pub struct LocalSigner {
    keys: nostr::prelude::Keys,
}

impl LocalSigner {
    /// A signer over a freshly generated keypair.
    #[must_use]
    pub fn new() -> Self {
        Self {
            keys: nostr::prelude::Keys::generate(),
        }
    }

    /// A signer over a fixed secret key (32 bytes) for deterministic tests.
    #[must_use]
    pub fn from_secret(secret: [u8; 32]) -> Option<Self> {
        let secret = nostr::prelude::SecretKey::from_slice(&secret).ok()?;
        Some(Self {
            keys: nostr::prelude::Keys::new(secret),
        })
    }

    /// The signer's account (x-only public key).
    #[must_use]
    pub fn account(&self) -> AccountId {
        AccountId(self.keys.public_key().to_bytes())
    }
}

impl Default for LocalSigner {
    fn default() -> Self {
        Self::new()
    }
}

impl AccountSigner for LocalSigner {
    fn account(&self) -> AccountId {
        self.account()
    }

    fn sign_event(&self, unsigned: UnsignedEvent) -> SignerFuture<'_, Result<Event, SignerError>> {
        let result = unsigned
            .to_nostr_unsigned()
            .ok_or_else(|| SignerError::Failed("invalid unsigned event".to_owned()))
            .and_then(|unsigned| {
                nostr::prelude::SignEvent::sign_event(&self.keys, unsigned)
                    .map_err(|_| SignerError::Refused)
            })
            .map(|event| Event::from_nostr_event(&event));
        Box::pin(async move { result })
    }

    fn nip44_encrypt<'a>(
        &'a self,
        peer: &'a AccountId,
        plaintext: &'a str,
    ) -> SignerFuture<'a, Result<String, SignerError>> {
        let result = nostr::prelude::PublicKey::from_slice(&peer.0)
            .map_err(|_| SignerError::Failed("invalid peer key".to_owned()))
            .and_then(|peer| {
                nostr::prelude::Nip44::nip44_encrypt(&self.keys, &peer, plaintext)
                    .map_err(|_| SignerError::Refused)
            });
        Box::pin(async move { result })
    }

    fn nip44_decrypt<'a>(
        &'a self,
        peer: &'a AccountId,
        ciphertext: &'a str,
    ) -> SignerFuture<'a, Result<String, SignerError>> {
        let result = nostr::prelude::PublicKey::from_slice(&peer.0)
            .map_err(|_| SignerError::Failed("invalid peer key".to_owned()))
            .and_then(|peer| {
                nostr::prelude::Nip44::nip44_decrypt(&self.keys, &peer, ciphertext)
                    .map_err(|_| SignerError::Refused)
            });
        Box::pin(async move { result })
    }
}
