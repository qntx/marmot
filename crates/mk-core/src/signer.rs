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
    /// set `id` to [`UnsignedEvent::id`] and a valid schnorr signature; the
    /// session does not re-derive them.
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

fn signer_failed(error: SignerError) -> MarmotSignerError {
    match error {
        SignerError::Refused => MarmotSignerError::new("signer refused"),
        SignerError::Failed(message) => MarmotSignerError::new(message),
    }
}

const NIP04_UNSUPPORTED: &str = "nip04 unsupported";

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
/// it). On the first recorded verdict it fences the store's writes, so
/// nothing written after a refused unwrap can reach durable state and the
/// whole span can be rolled back and reported as retryable instead of
/// poisoned as a permanent rejection.
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
                    latch.record(match &error {
                        SignerError::Refused => SignerVerdict::Refused,
                        SignerError::Failed(_) => SignerVerdict::Failed,
                    });
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
