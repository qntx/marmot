//! Test and harness support for the KV host boundary.
//!
//! [`RecordingHost`] is an in-memory [`crate::kv::KvHost`] that applies every
//! committed batch to a durable row map, recording each batch as it arrives.
//! It can replay what a real host would hold (`durable_rows`), drop future
//! commits (`fail_after`), or pretend an earlier commit was the last one the
//! host persisted (`cut_after`) — the fixtures reopen and migration tests,
//! the session durability gate, and the interop harness all need.

use std::collections::{BTreeMap, HashMap};
use std::sync::Mutex;

use cgka_traits::message::MessageRecord;

use crate::clock::{Clock, Time};
use crate::keys::{self, Table};
use crate::kv::{CommitSeq, KvBatch, KvHost, KvOp};
use crate::provider::KvStorageProvider;
use crate::semantics::{self, Classified, Modifier, Resolution, Semantics, TargetFacts};
use crate::ser;
use crate::signer::{AccountSigner, SignerError, SignerFuture};
use crate::types::{AccountId, Event, EventId, UnsignedEvent};

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

    /// Run `f` inside a held savepoint — the session's signer-refusal
    /// span — then drop the guard uncommitted. A discarded span that held
    /// an `OpenMLS` write moves the generation's reset half, like every
    /// other rolled-back layer.
    ///
    /// # Errors
    /// `f`'s error, or [`cgka_traits::storage::StorageError`] on lock
    /// poisoning.
    pub fn dropped_held_savepoint<T, E, F>(&self, f: F) -> Result<T, E>
    where
        E: From<cgka_traits::storage::StorageError>,
        F: FnOnce() -> Result<T, E>,
    {
        let savepoint = self.store().begin_savepoint().map_err(E::from)?;
        let out = f();
        drop(savepoint);
        out
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

/// A fixed account-identity-proof request bound to an account.
///
/// Validates arbitrary signed events with the same checks
/// [`crate::PendingProof::provide_account_proof`] applies. Fuzz and replay
/// harnesses build one fixed request once, then exercise the decoder and
/// verifier against every input event.
#[derive(Debug)]
pub struct ProofRequest {
    account: AccountId,
    request: cgka_engine::account_identity_proof::AccountIdentityProofRequest,
}

impl ProofRequest {
    /// A deterministic current-profile request: fixed account, MLS
    /// signature key, ciphersuite and timestamp.
    #[must_use]
    pub fn fixed() -> Self {
        Self {
            account: AccountId([0x11; 32]),
            request: cgka_engine::account_identity_proof::AccountIdentityProofRequest::current(
                vec![0x11; 32],
                vec![0x22; 32],
                openmls::prelude::Ciphersuite::MLS_128_DHKEMX25519_AES128GCM_SHA256_Ed25519,
                openmls::prelude::SignatureScheme::ED25519,
                1_700_000_000,
            ),
        }
    }

    /// Run the proof pre-checks `provide_account_proof` runs before the
    /// engine rebuild; the returned signature is discarded by callers that
    /// only validate.
    ///
    /// # Errors
    /// [`crate::ProofError`] classifying the rejection.
    pub fn check(&self, signed: &Event) -> Result<[u8; 64], crate::ProofError> {
        crate::session::check_account_proof(self.account, &self.request, signed)
    }
}

/// `MlsGroup::load` calls made on the current thread so far, counted at
/// the one `OpenMLS` read only a load performs. Compare two readings taken
/// on the thread that drives the session.
#[must_use]
pub fn openmls_group_loads() -> usize {
    crate::mls::group_loads()
}

/// Require the stored index rows to match what the `Message` rows imply.
///
/// Rebuilds the expected `MessageState`/`MessageIndex` rows with the same
/// key functions: no missing keys, no extra keys, identical values,
/// `MessageState` values empty.
/// # Errors
/// A fixed-string drift report naming the table and both row counts.
pub fn check_message_index(rows: &[(Vec<u8>, Vec<u8>)]) -> Result<(), String> {
    let mut expected_state = BTreeMap::new();
    let mut expected_index = BTreeMap::new();
    let mut stored_state = BTreeMap::new();
    let mut stored_index = BTreeMap::new();
    for (key, value) in rows {
        match key.first().copied() {
            Some(t) if t == Table::Message as u8 => {
                let [gid, seq, mid]: [&[u8]; 3] = keys::segments(key)
                    .and_then(|segments| segments.try_into().ok())
                    .ok_or_else(|| "malformed message row key".to_owned())?;
                let record: MessageRecord =
                    ser::de_message(value).map_err(|_| "undecodable message row".to_owned())?;
                if record.id.as_slice() != mid || record.group_id.as_slice() != gid {
                    return Err("message row key drifted from its record".to_owned());
                }
                let seq = u64::from_be_bytes(
                    seq.try_into()
                        .map_err(|_| "malformed message row key".to_owned())?,
                );
                let (class, openmls_epoch) =
                    KvStorageProvider::openmls_content_columns(&record.payload);
                let state_key = keys::message_state_key(
                    gid,
                    record.state,
                    record.epoch.0,
                    seq,
                    mid,
                    class,
                    openmls_epoch,
                );
                expected_state.insert(state_key.clone(), Vec::new());
                expected_index.insert(keys::key(Table::MessageIndex, &[mid]), state_key);
            }
            Some(t) if t == Table::MessageState as u8 => {
                stored_state.insert(key.clone(), value.clone());
            }
            Some(t) if t == Table::MessageIndex as u8 => {
                stored_index.insert(key.clone(), value.clone());
            }
            _ => {}
        }
    }
    if stored_state != expected_state {
        return Err(format!(
            "MessageState index drift: expected {} rows, stored {}",
            expected_state.len(),
            stored_state.len()
        ));
    }
    if stored_index != expected_index {
        return Err(format!(
            "MessageIndex drift: expected {} rows, stored {}",
            expected_index.len(),
            stored_index.len()
        ));
    }
    Ok(())
}

/// Fuzz-facing mirror of the crate-private receive classifier: `mk-fuzz`
/// drives `semantics::classify`/`resolve` through these opaque shapes.
#[derive(Debug)]
pub enum FuzzClassified {
    /// `Semantics::Reaction`; the emoji survives verbatim for assertions.
    Reaction {
        /// The accepted emoji.
        emoji: String,
    },
    /// Any other delivered semantics.
    Deliver,
    /// Dropped with a reason (opaque to fuzzing).
    Drop,
    /// A held modifier, carried over for [`resolve_modifier`].
    Modifier(FuzzModifier),
}

/// A parsed kind-5/1009 modifier, fields flattened to public types.
#[derive(Clone, Debug)]
pub enum FuzzModifier {
    /// Kind 1009: replace `target`'s content.
    Edit {
        /// The target's inner event id.
        target: [u8; 32],
        /// The replacement body.
        content: String,
    },
    /// Kind 5: void `targets`.
    Delete {
        /// The deduplicated target ids, in tag order.
        targets: Vec<[u8; 32]>,
    },
}

/// `semantics::classify` for fuzzers; opaque shapes only.
#[must_use]
pub fn classify(
    kind: u64,
    tags: &[Vec<String>],
    content: &str,
    grant: Option<bool>,
    expires_at: Option<u64>,
    now_secs: u64,
) -> FuzzClassified {
    match semantics::classify(kind, tags, content, grant, expires_at, now_secs) {
        Classified::Deliver(Semantics::Reaction { emoji, .. }) => {
            FuzzClassified::Reaction { emoji }
        }
        Classified::Deliver(_) => FuzzClassified::Deliver,
        Classified::Drop(_) => FuzzClassified::Drop,
        Classified::Modifier(Modifier::Edit { target, content }) => {
            FuzzClassified::Modifier(FuzzModifier::Edit {
                target: target.0,
                content,
            })
        }
        Classified::Modifier(Modifier::Delete { targets }) => {
            FuzzClassified::Modifier(FuzzModifier::Delete {
                targets: targets.iter().map(|target| target.0).collect(),
            })
        }
    }
}

/// The outcome kind of [`resolve_modifier`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FuzzResolution {
    /// A target is still unknown; the modifier holds.
    Hold,
    /// Authorized; delivers.
    Deliver,
    /// Refused; drops.
    Drop,
}

/// `semantics::resolve` over a fuzz-provided index: `known` maps target id
/// to `(author, kind)`; ids absent from the map are unknown targets.
#[must_use]
pub fn resolve_modifier<S: std::hash::BuildHasher>(
    modifier: &FuzzModifier,
    sender: [u8; 32],
    known: &HashMap<[u8; 32], ([u8; 32], u64), S>,
    final_attempt: bool,
) -> FuzzResolution {
    let modifier = match modifier {
        FuzzModifier::Edit { target, content } => Modifier::Edit {
            target: EventId(*target),
            content: content.clone(),
        },
        FuzzModifier::Delete { targets } => Modifier::Delete {
            targets: targets.iter().map(|target| EventId(*target)).collect(),
        },
    };
    let outcome = semantics::resolve(
        &modifier,
        &AccountId(sender),
        |id| {
            known.get(&id.0).map(|(author, kind)| TargetFacts {
                author: AccountId(*author),
                kind: *kind,
            })
        },
        final_attempt,
    );
    match outcome {
        Resolution::Hold => FuzzResolution::Hold,
        Resolution::Deliver(_) => FuzzResolution::Deliver,
        Resolution::Drop(_) => FuzzResolution::Drop,
    }
}

/// Stored `KeyPackage` private-material bundles in `rows` — the set the
/// lifecycle's bundle-count invariant covers (`current` + `pending` +
/// `retained`).
#[must_use]
pub fn key_package_bundle_count(rows: &[(Vec<u8>, Vec<u8>)]) -> usize {
    rows.iter()
        .filter(|(key, _)| {
            key.first().copied() == Some(Table::OpenMlsValue as u8)
                && keys::segments(key).and_then(|segments| segments.first().copied())
                    == Some(crate::mls::KEY_PACKAGE_LABEL_BYTES)
        })
        .count()
}
