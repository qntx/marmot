//! The injected account signer and the adapters that bind it to MDK.
//!
//! The account's secret key never enters this crate: gift-wrap sealing of
//! Welcomes, NIP-44 decrypt on the peel path and every signed event go through
//! [`AccountSigner`]. Two narrow adapters project it onto MDK's traits:
//! [`SignerAdapter`] implements `transport_nostr_peeler::MarmotNostrSigner`
//! (its mandatory `nip04_*` surface answers with a typed refusal — NIP-59 gift
//! wrap only ever calls `nip44_*` and `sign_event`), and the internal proof
//! signers drive the engine's account-identity-proof handshake.

use core::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use crate::store::KvStore;

use cgka_engine::account_identity_proof::{
    AccountIdentityProofRequest, AccountIdentityProofSigner,
};
use thiserror::Error;
use transport_nostr_peeler::{MarmotNostrSigner, MarmotSignerError};

use crate::types::{AccountId, Event, UnsignedEvent};

/// Boxed future for signer calls — keeps [`AccountSigner`] object-safe and
/// executor-agnostic.
pub type SignerFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// A signer failure. Carries no payload, key material or peer identity.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum SignerError {
    /// The signer declined to complete the operation (e.g. user rejection or
    /// an unavailable key).
    #[error("signer refused")]
    Refused,
    /// The signer ran and the ciphertext does not decrypt or authenticate
    /// (MAC, padding, version or encoding). Permanent for that payload — a
    /// `nip44_decrypt` answer only — so the ingest commits as a rejection
    /// and the id is remembered; a re-delivery never reaches the signer.
    #[error("invalid payload")]
    InvalidPayload,
    /// A signer-side failure. The message is a fixed category supplied by the
    /// signer implementation; it must not embed payload or key material.
    #[error("signer failed: {0}")]
    Failed(String),
}

/// The account's external signer. `account()` must always report the same key.
pub trait AccountSigner: Send + Sync + fmt::Debug {
    /// The account this signer signs for.
    fn account(&self) -> AccountId;

    /// Sign `unsigned` and return the complete event. Implementations must
    /// set `id` to [`UnsignedEvent::id`] and a valid schnorr signature;
    /// [`CheckedSigner`] rejects every reply that does not.
    fn sign_event(&self, unsigned: UnsignedEvent) -> SignerFuture<'_, Result<Event, SignerError>>;

    /// NIP-44 seal `plaintext` for `peer`'s key.
    fn nip44_encrypt<'a>(
        &'a self,
        peer: &'a AccountId,
        plaintext: &'a str,
    ) -> SignerFuture<'a, Result<String, SignerError>>;

    /// NIP-44 open `ciphertext` from `peer`.
    fn nip44_decrypt<'a>(
        &'a self,
        peer: &'a AccountId,
        ciphertext: &'a str,
    ) -> SignerFuture<'a, Result<String, SignerError>>;
}

fn nostr_pubkey(account: &AccountId) -> Result<nostr::prelude::PublicKey, MarmotSignerError> {
    nostr::prelude::PublicKey::from_slice(&account.0)
        .map_err(|_| MarmotSignerError::new("invalid account public key"))
}

fn nostr_account(key: &nostr::prelude::PublicKey) -> Result<AccountId, MarmotSignerError> {
    AccountId::from_hex(&key.to_hex())
        .map_err(|_| MarmotSignerError::new("invalid account public key"))
}

/// Latch the retryable verdict of a `nip44_decrypt` error. An invalid
/// payload is not a signer failure — the signer ran and the wrap is
/// permanently undecryptable — so nothing is recorded for it and the
/// caller's savepoint commits the rejection.
fn record_verdict(latch: &Arc<SignerLatch>, error: &SignerError) {
    let verdict = match error {
        SignerError::Refused => Some(SignerVerdict::Refused),
        SignerError::Failed(_) => Some(SignerVerdict::Failed),
        SignerError::InvalidPayload => None,
    };
    if let Some(verdict) = verdict {
        latch.record(verdict);
    }
}

fn signer_failed(error: SignerError) -> MarmotSignerError {
    match error {
        SignerError::Refused => MarmotSignerError::new("signer refused"),
        SignerError::InvalidPayload => MarmotSignerError::new("invalid payload"),
        SignerError::Failed(message) => MarmotSignerError::new(message),
    }
}

const NIP04_UNSUPPORTED: &str = "nip04 unsupported";

/// The reply is not the requested event: a hostile or buggy signer can never
/// make the session publish a different event or an invalid signature.
const SIGNER_REPLY_MISMATCH: &str = "signer reply mismatch";

/// [`AccountSigner`] wrapper installed once by `Session::open` so EVERY
/// `sign_event` path — direct `sign_checked` calls, MDK seal/Welcome signing
/// through [`SignerAdapter`], and the bridge's `QueueSigner` — gets the same
/// reply validation: the signed event must come from the account, carry the
/// request's `pubkey`/`kind`/`content`/`tags`/`created_at` untouched, commit
/// to [`UnsignedEvent::id`] and hold a verifying schnorr signature. Anything
/// else is [`SignerError::Failed`]; the signer ran and answered wrong.
pub(crate) struct CheckedSigner {
    inner: Arc<dyn AccountSigner>,
}

impl CheckedSigner {
    pub(crate) const fn new(inner: Arc<dyn AccountSigner>) -> Self {
        Self { inner }
    }
}

impl fmt::Debug for CheckedSigner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CheckedSigner").finish_non_exhaustive()
    }
}

impl AccountSigner for CheckedSigner {
    fn account(&self) -> AccountId {
        self.inner.account()
    }

    fn sign_event(&self, unsigned: UnsignedEvent) -> SignerFuture<'_, Result<Event, SignerError>> {
        let inner = Arc::clone(&self.inner);
        Box::pin(async move {
            let id = unsigned.id();
            let signed = inner.sign_event(unsigned.clone()).await?;
            if signed.pubkey == inner.account()
                && signed.pubkey == unsigned.pubkey
                && signed.created_at == unsigned.created_at
                && signed.kind == unsigned.kind
                && signed.tags == unsigned.tags
                && signed.content == unsigned.content
                && signed.id == id
                && signed.verify().is_ok()
            {
                Ok(signed)
            } else {
                Err(SignerError::Failed(SIGNER_REPLY_MISMATCH.to_owned()))
            }
        })
    }

    fn nip44_encrypt<'a>(
        &'a self,
        peer: &'a AccountId,
        plaintext: &'a str,
    ) -> SignerFuture<'a, Result<String, SignerError>> {
        self.inner.nip44_encrypt(peer, plaintext)
    }

    fn nip44_decrypt<'a>(
        &'a self,
        peer: &'a AccountId,
        ciphertext: &'a str,
    ) -> SignerFuture<'a, Result<String, SignerError>> {
        self.inner.nip44_decrypt(peer, ciphertext)
    }
}

/// The first verdict an armed span observed: which way a `nip44_decrypt`
/// call ended while the latch was armed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SignerVerdict {
    /// [`SignerError::Refused`]
    Refused,
    /// [`SignerError::Failed`]
    Failed,
}

/// Shared latch between `Session` (arming it around a kind-1059 ingest
/// span) and [`SignerAdapter`] (recording `nip44_decrypt` failures into
/// it). Only `Refused`/`Failed` verdicts are latched —
/// [`SignerError::InvalidPayload`] is not a signer failure (the signer ran;
/// the payload is permanently bad) and does not reach the latch. On the
/// first recorded verdict it fences the store's writes, so nothing written
/// after a refused unwrap can reach durable state and the whole span can
/// be rolled back and reported as retryable instead of poisoned as a
/// permanent rejection.
pub(crate) struct SignerLatch {
    store: KvStore,
    state: Mutex<LatchState>,
}

#[derive(Default)]
struct LatchState {
    armed: bool,
    verdict: Option<SignerVerdict>,
}

impl SignerLatch {
    pub(crate) const fn new(store: KvStore) -> Self {
        Self {
            store,
            state: Mutex::new(LatchState {
                armed: false,
                verdict: None,
            }),
        }
    }

    fn state(&self) -> std::sync::MutexGuard<'_, LatchState> {
        match self.state.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        }
    }

    /// Open a span; clears any earlier verdict.
    pub(crate) fn arm(&self) {
        let mut state = self.state();
        state.armed = true;
        state.verdict = None;
    }

    /// Close the span; the recorded verdict stays readable until the next
    /// `arm`.
    pub(crate) fn disarm(&self) {
        self.state().armed = false;
    }

    /// The first verdict recorded this span, if any.
    pub(crate) fn verdict(&self) -> Option<SignerVerdict> {
        self.state().verdict
    }

    /// Record `verdict` while armed (first one wins) and fence the store so
    /// no write after the refused unwrap can land.
    fn record(&self, verdict: SignerVerdict) {
        {
            let mut state = self.state();
            if !state.armed {
                return;
            }
            state.verdict.get_or_insert(verdict);
        }
        self.store.fence_writes();
    }
}

impl fmt::Debug for SignerLatch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SignerLatch").finish_non_exhaustive()
    }
}

/// [`MarmotNostrSigner`] over [`AccountSigner`]. `nip04_*` returns a typed
/// refusal: NIP-59 Welcome wrap/peel exercises only `sign_event` and
/// `nip44_*`, so the refusal can never be hit by the production path (a test
/// counts calls to prove that).
pub(crate) struct SignerAdapter {
    inner: Arc<dyn AccountSigner>,
    latch: Arc<SignerLatch>,
}

impl SignerAdapter {
    pub(crate) fn new(inner: Arc<dyn AccountSigner>, latch: Arc<SignerLatch>) -> Self {
        Self { inner, latch }
    }
}

impl fmt::Debug for SignerAdapter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SignerAdapter").finish_non_exhaustive()
    }
}

impl MarmotNostrSigner for SignerAdapter {
    fn get_public_key(
        &self,
    ) -> SignerFuture<'_, Result<nostr::prelude::PublicKey, MarmotSignerError>> {
        let inner = Arc::clone(&self.inner);
        Box::pin(async move { nostr_pubkey(&inner.account()) })
    }

    fn sign_event(
        &self,
        unsigned: nostr::prelude::UnsignedEvent,
    ) -> SignerFuture<'_, Result<nostr::prelude::Event, MarmotSignerError>> {
        let inner = Arc::clone(&self.inner);
        Box::pin(async move {
            let unsigned = UnsignedEvent::from_nostr_unsigned(&unsigned);
            let signed = inner.sign_event(unsigned).await.map_err(signer_failed)?;
            signed
                .to_nostr_event()
                .ok_or_else(|| MarmotSignerError::new("signer returned a malformed event"))
        })
    }

    fn nip04_encrypt<'a>(
        &'a self,
        _public_key: &'a nostr::prelude::PublicKey,
        _content: &'a str,
    ) -> SignerFuture<'a, Result<String, MarmotSignerError>> {
        Box::pin(async move { Err(MarmotSignerError::new(NIP04_UNSUPPORTED)) })
    }

    fn nip04_decrypt<'a>(
        &'a self,
        _public_key: &'a nostr::prelude::PublicKey,
        _payload: &'a str,
    ) -> SignerFuture<'a, Result<String, MarmotSignerError>> {
        Box::pin(async move { Err(MarmotSignerError::new(NIP04_UNSUPPORTED)) })
    }

    fn nip44_encrypt<'a>(
        &'a self,
        public_key: &'a nostr::prelude::PublicKey,
        content: &'a str,
    ) -> SignerFuture<'a, Result<String, MarmotSignerError>> {
        let inner = Arc::clone(&self.inner);
        Box::pin(async move {
            let peer = nostr_account(public_key)?;
            inner
                .nip44_encrypt(&peer, content)
                .await
                .map_err(signer_failed)
        })
    }

    fn nip44_decrypt<'a>(
        &'a self,
        public_key: &'a nostr::prelude::PublicKey,
        payload: &'a str,
    ) -> SignerFuture<'a, Result<String, MarmotSignerError>> {
        let inner = Arc::clone(&self.inner);
        let latch = Arc::clone(&self.latch);
        Box::pin(async move {
            let peer = nostr_account(public_key)?;
            match inner.nip44_decrypt(&peer, payload).await {
                Ok(text) => Ok(text),
                Err(error) => {
                    record_verdict(&latch, &error);
                    Err(signer_failed(error))
                }
            }
        })
    }
}

/// Engine proof signer used at open: captures the request the engine asks to
/// sign, then fails, so `Session::open` can classify the build result without
/// matching on error strings.
pub(crate) struct DeferringProofSigner {
    captured: Mutex<Option<AccountIdentityProofRequest>>,
}

impl DeferringProofSigner {
    pub(crate) const fn new() -> Self {
        Self {
            captured: Mutex::new(None),
        }
    }

    /// The request the engine asked to sign, if the build reached proof
    /// production.
    pub(crate) fn captured(&self) -> Option<AccountIdentityProofRequest> {
        match self.captured.lock() {
            Ok(guard) => guard.clone(),
            Err(poisoned) => poisoned.into_inner().clone(),
        }
    }
}

impl fmt::Debug for DeferringProofSigner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DeferringProofSigner")
            .finish_non_exhaustive()
    }
}

impl AccountIdentityProofSigner for DeferringProofSigner {
    fn sign_account_identity_proof(
        &self,
        request: &AccountIdentityProofRequest,
    ) -> Result<[u8; 64], String> {
        match self.captured.lock() {
            Ok(mut guard) => *guard = Some(request.clone()),
            Err(poisoned) => *poisoned.into_inner() = Some(request.clone()),
        }
        Err("proof deferred".to_owned())
    }
}

/// Engine proof signer returning a fixed signature — used by
/// `provide_account_proof` to assemble the component from the host's signed
/// event without round-tripping the account signer.
pub(crate) struct FixedProofSigner {
    signature: [u8; 64],
}

impl FixedProofSigner {
    pub(crate) const fn new(signature: [u8; 64]) -> Self {
        Self { signature }
    }
}

impl fmt::Debug for FixedProofSigner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FixedProofSigner").finish_non_exhaustive()
    }
}

impl AccountIdentityProofSigner for FixedProofSigner {
    fn sign_account_identity_proof(
        &self,
        _request: &AccountIdentityProofRequest,
    ) -> Result<[u8; 64], String> {
        Ok(self.signature)
    }
}

/// Engine proof signer that must never be called — installed on rebuilds that
/// already carry a stored proof. If the engine does call it, the build fails
/// loudly rather than silently minting a bad proof.
pub(crate) struct RefusingProofSigner {
    calls: Mutex<usize>,
}

impl RefusingProofSigner {
    pub(crate) const fn new() -> Self {
        Self {
            calls: Mutex::new(0),
        }
    }

    /// How many times the engine asked for a signature.
    #[cfg(all(test, feature = "testing"))]
    pub(crate) fn calls(&self) -> usize {
        match self.calls.lock() {
            Ok(guard) => *guard,
            Err(poisoned) => *poisoned.into_inner(),
        }
    }
}

impl fmt::Debug for RefusingProofSigner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RefusingProofSigner")
            .finish_non_exhaustive()
    }
}

impl AccountIdentityProofSigner for RefusingProofSigner {
    fn sign_account_identity_proof(
        &self,
        _request: &AccountIdentityProofRequest,
    ) -> Result<[u8; 64], String> {
        match self.calls.lock() {
            Ok(mut guard) => *guard = guard.saturating_add(1),
            Err(poisoned) => {
                let mut guard = poisoned.into_inner();
                *guard = guard.saturating_add(1);
            }
        }
        Err("proof signer must not be called".to_owned())
    }
}

#[cfg(all(test, feature = "testing"))]
#[allow(
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::tests_outside_test_module,
    clippy::missing_assert_message,
    reason = "test code"
)]
mod checked_signer_tests {
    //! `CheckedSigner` reply validation: each tampered reply — every
    //! mismatch a hostile or buggy signer could produce — fails
    //! `SignerError::Failed`; the honest reply passes.

    use super::*;

    use crate::testing::LocalSigner;

    /// Answers `sign_event` with a prepared reply regardless of the request.
    struct FixedReplySigner {
        account: AccountId,
        reply: Event,
    }

    impl fmt::Debug for FixedReplySigner {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.debug_struct("FixedReplySigner").finish_non_exhaustive()
        }
    }

    impl AccountSigner for FixedReplySigner {
        fn account(&self) -> AccountId {
            self.account
        }

        fn sign_event(
            &self,
            _unsigned: UnsignedEvent,
        ) -> SignerFuture<'_, Result<Event, SignerError>> {
            let reply = self.reply.clone();
            Box::pin(async move { Ok(reply) })
        }

        fn nip44_encrypt<'a>(
            &'a self,
            _peer: &'a AccountId,
            _plaintext: &'a str,
        ) -> SignerFuture<'a, Result<String, SignerError>> {
            Box::pin(async { Err(SignerError::Failed("unused".to_owned())) })
        }

        fn nip44_decrypt<'a>(
            &'a self,
            _peer: &'a AccountId,
            _ciphertext: &'a str,
        ) -> SignerFuture<'a, Result<String, SignerError>> {
            Box::pin(async { Err(SignerError::Failed("unused".to_owned())) })
        }
    }

    fn request(account: AccountId) -> UnsignedEvent {
        UnsignedEvent {
            pubkey: account,
            created_at: 1_700_000_000,
            kind: 445,
            tags: vec![vec!["h".to_owned(), "cc".repeat(32)]],
            content: "hi".to_owned(),
        }
    }

    /// `request(account)` signed for real — the honest reply.
    async fn honest_reply(signer: &LocalSigner, account: AccountId) -> Event {
        signer
            .sign_event(request(account))
            .await
            .expect("honest sign")
    }

    /// The checked `sign_event` outcome when the signer answers `reply`.
    async fn outcome(reply: Event, account: AccountId) -> Result<Event, SignerError> {
        let signer = CheckedSigner::new(Arc::new(FixedReplySigner { account, reply }));
        signer.sign_event(request(account)).await
    }

    /// `reply` must be rejected as `Failed("signer reply mismatch")`.
    async fn expect_mismatch(reply: Event, account: AccountId) {
        match outcome(reply, account).await {
            Err(SignerError::Failed(message)) => {
                assert_eq!(message, SIGNER_REPLY_MISMATCH);
            }
            other => panic!("mismatched reply must fail: {other:?}"),
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn honest_reply_passes() {
        let honest = LocalSigner::new();
        let account = honest.account();
        let reply = honest_reply(&honest, account).await;
        let signed = outcome(reply, account).await.expect("checked");
        assert_eq!(signed.content, "hi");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn swapped_content_fails() {
        let honest = LocalSigner::new();
        let account = honest.account();
        let mut changed = request(account);
        changed.content = "swapped".to_owned();
        let reply = honest.sign_event(changed).await.expect("sign");
        expect_mismatch(reply, account).await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn swapped_tags_fail() {
        let honest = LocalSigner::new();
        let account = honest.account();
        let mut changed = request(account);
        changed.tags = vec![vec!["h".to_owned(), "dd".repeat(32)]];
        let reply = honest.sign_event(changed).await.expect("sign");
        expect_mismatch(reply, account).await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn swapped_kind_fails() {
        let honest = LocalSigner::new();
        let account = honest.account();
        let mut changed = request(account);
        changed.kind = 446;
        let reply = honest.sign_event(changed).await.expect("sign");
        expect_mismatch(reply, account).await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn swapped_created_at_fails() {
        let honest = LocalSigner::new();
        let account = honest.account();
        let mut changed = request(account);
        changed.created_at = 1_700_000_001;
        let reply = honest.sign_event(changed).await.expect("sign");
        expect_mismatch(reply, account).await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn wrong_pubkey_fails() {
        let honest = LocalSigner::new();
        let stranger = LocalSigner::new();
        let reply = stranger
            .sign_event(request(stranger.account()))
            .await
            .expect("sign");
        expect_mismatch(reply, honest.account()).await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn wrong_id_fails() {
        let honest = LocalSigner::new();
        let account = honest.account();
        let mut reply = honest_reply(&honest, account).await;
        reply.id = crate::types::EventId([9u8; 32]);
        expect_mismatch(reply, account).await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn bad_signature_fails() {
        let honest = LocalSigner::new();
        let account = honest.account();
        let mut reply = honest_reply(&honest, account).await;
        reply.sig[0] ^= 0xff;
        expect_mismatch(reply, account).await;
    }
}
