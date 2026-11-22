//! The account-signer queue: `AccountSigner` operations the engine awaits
//! are registered as numbered requests, surfaced through the wire protocol
//! and completed by `signer_reply`. The bridge never signs.

use std::collections::BTreeMap;
use std::future::poll_fn;
use std::sync::{Arc, Mutex};
use std::task::Poll;

use mk_core::{AccountId, AccountSigner, Event, SignerError, SignerFuture, UnsignedEvent};

use crate::host::lock;
use crate::protocol::{SignerOp, SignerRequest, SignerResult};

/// One pending batch of signer operations owned by a single suspended
/// session operation. `pending` holds every request the current operation
/// registered; `replies` holds the answers stored by `signer_reply`.
#[derive(Debug, Default)]
pub(crate) struct SignerState {
    next_id: u32,
    /// Registered requests, in registration order.
    pub(crate) pending: BTreeMap<u32, SignerOp>,
    /// Stored replies awaiting consumption by the suspended future.
    replies: BTreeMap<u32, SignerResult>,
}

impl SignerState {
    /// Register `op` and return its request id. `None` when the id space is
    /// exhausted (2^32 requests on one suspended operation).
    fn register(&mut self, op: SignerOp) -> Option<u32> {
        let id = self.next_id;
        self.next_id = self.next_id.checked_add(1)?;
        self.pending.insert(id, op);
        Some(id)
    }

    /// Store a reply the bridge validated for `id`.
    pub(crate) fn answer(&mut self, id: u32, result: SignerResult) {
        self.replies.insert(id, result);
    }

    /// Whether `id` names a registered request not yet answered.
    pub(crate) fn awaitable(&self, id: u32) -> bool {
        self.pending.contains_key(&id) && !self.replies.contains_key(&id)
    }

    /// Every registered request with no stored reply yet.
    pub(crate) fn unanswered(&self) -> Vec<SignerRequest> {
        self.pending
            .iter()
            .filter(|(id, _)| !self.replies.contains_key(id))
            .map(|(id, op)| SignerRequest {
                id: *id,
                op: op.clone(),
            })
            .collect()
    }

    /// The suspended operation finished; its request bookkeeping is done.
    pub(crate) fn clear(&mut self) {
        self.pending.clear();
        self.replies.clear();
    }
}

/// An [`AccountSigner`] that defers every operation to the host through
/// numbered [`SignerRequest`]s. `Send + Sync` per the trait contract; the
/// inert reply futures are polled by the suspended operation's poll, never
/// by an executor.
#[derive(Debug)]
pub(crate) struct QueueSigner {
    account: AccountId,
    state: Arc<Mutex<SignerState>>,
}

impl QueueSigner {
    pub(crate) const fn new(account: AccountId, state: Arc<Mutex<SignerState>>) -> Self {
        Self { account, state }
    }

    /// Future that resolves when a reply for `id` is stored.
    fn wait<T>(
        &self,
        op: SignerOp,
        map: impl Fn(SignerResult) -> Result<T, SignerError> + Send + 'static,
    ) -> SignerFuture<'static, Result<T, SignerError>> {
        let state = Arc::clone(&self.state);
        let Some(id) = lock(&state).register(op) else {
            return Box::pin(async {
                Err(SignerError::Failed("signer request id space".to_owned()))
            });
        };
        Box::pin(poll_fn(move |_| {
            let reply = lock(&state).replies.remove(&id);
            reply.map_or_else(|| Poll::Pending, |result| Poll::Ready(map(result)))
        }))
    }
}

/// A `sign_event` reply: a refusal survives as [`SignerError::Refused`];
/// a `failed`/`invalid_payload` reply or a mismatched result kind is a
/// generic failure (a sign/encrypt reply has no payload to be invalid).
fn sign_reply(result: SignerResult) -> Result<Event, SignerError> {
    match result {
        SignerResult::Signed { event } => Ok(event.into()),
        SignerResult::Refused => Err(SignerError::Refused),
        _ => Err(SignerError::Failed("host signer reply".to_owned())),
    }
}

/// A `nip44_*` reply: a refusal survives as [`SignerError::Refused`]; an
/// `invalid_payload` reply is the signer's permanent verdict on the
/// ciphertext; a `failed` reply or a mismatched result kind is a generic
/// failure.
fn text_reply(result: SignerResult) -> Result<String, SignerError> {
    match result {
        SignerResult::Text { text } => Ok(text),
        SignerResult::Refused => Err(SignerError::Refused),
        SignerResult::InvalidPayload => Err(SignerError::InvalidPayload),
        _ => Err(SignerError::Failed("host signer reply".to_owned())),
    }
}

impl AccountSigner for QueueSigner {
    fn account(&self) -> AccountId {
        self.account
    }

    fn sign_event(&self, unsigned: UnsignedEvent) -> SignerFuture<'_, Result<Event, SignerError>> {
        self.wait(
            SignerOp::SignEvent {
                unsigned: unsigned.into(),
            },
            sign_reply,
        )
    }

    fn nip44_encrypt<'a>(
        &'a self,
        peer: &'a AccountId,
        plaintext: &'a str,
    ) -> SignerFuture<'a, Result<String, SignerError>> {
        self.wait(
            SignerOp::Nip44Encrypt {
                peer: peer.0,
                plaintext: plaintext.to_owned(),
            },
            text_reply,
        )
    }

    fn nip44_decrypt<'a>(
        &'a self,
        peer: &'a AccountId,
        ciphertext: &'a str,
    ) -> SignerFuture<'a, Result<String, SignerError>> {
        self.wait(
            SignerOp::Nip44Decrypt {
                peer: peer.0,
                ciphertext: ciphertext.to_owned(),
            },
            text_reply,
        )
    }
}
