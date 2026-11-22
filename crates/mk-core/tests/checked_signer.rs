//! Signer-reply verification: a `sign_event` reply that does not answer the
//! request — valid signature, different commitment — must fail the op and
//! stage nothing. `CheckedSigner` wraps the account signer once at
//! `Session::open` so every signing path (direct `sign_checked`, MDK seal
//! signing, the bridge `QueueSigner`) is covered.
//!
//! Requires the `testing` feature for the test helpers.

#![allow(
    unused_crate_dependencies,
    reason = "every integration-test binary links the full dev-dependency set"
)]
#![allow(
    clippy::missing_docs_in_private_items,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::needless_pass_by_value,
    clippy::tests_outside_test_module,
    clippy::shadow_reuse,
    clippy::doc_markdown,
    reason = "test code"
)]

#[cfg(feature = "testing")]
mod support;

#[cfg(feature = "testing")]
mod checked_signer_tests {
    use super::support::*;

    use std::fmt;
    use std::sync::Arc;

    use mk_core::testing::LocalSigner;
    use mk_core::{AccountSigner, Event, SessionError, SignerError, SignerFuture, UnsignedEvent};

    /// A signer that honestly signs — but answers a kind-10050 `sign_event`
    /// with an event whose content is swapped. The reply is a validly signed
    /// event, just not the one that was requested; every other request (in
    /// particular the account-proof event) is answered honestly.
    struct SwapSigner {
        inner: LocalSigner,
    }

    impl fmt::Debug for SwapSigner {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.debug_struct("SwapSigner").finish_non_exhaustive()
        }
    }

    impl AccountSigner for SwapSigner {
        fn account(&self) -> mk_core::AccountId {
            self.inner.account()
        }

        fn sign_event(
            &self,
            unsigned: UnsignedEvent,
        ) -> SignerFuture<'_, Result<Event, SignerError>> {
            let unsigned = if unsigned.kind == 10_050 {
                UnsignedEvent {
                    content: "swapped".to_owned(),
                    ..unsigned
                }
            } else {
                unsigned
            };
            self.inner.sign_event(unsigned)
        }

        fn nip44_encrypt<'a>(
            &'a self,
            peer: &'a mk_core::AccountId,
            plaintext: &'a str,
        ) -> SignerFuture<'a, Result<String, SignerError>> {
            self.inner.nip44_encrypt(peer, plaintext)
        }

        fn nip44_decrypt<'a>(
            &'a self,
            peer: &'a mk_core::AccountId,
            ciphertext: &'a str,
        ) -> SignerFuture<'a, Result<String, SignerError>> {
            self.inner.nip44_decrypt(peer, ciphertext)
        }
    }

    /// A validly-signed reply for a different event fails the op as a signer
    /// error and nothing reaches the outbox.
    #[tokio::test(flavor = "current_thread")]
    async fn swapped_sign_event_reply_fails_and_stages_nothing() {
        let mut member = open_member_with(Arc::new(SwapSigner {
            inner: LocalSigner::new(),
        }))
        .await;
        let error = member
            .session
            .publish_inbox_relays(&["wss://r1.example".to_owned()])
            .await
            .expect_err("a mismatched signer reply must fail the op");
        assert!(
            matches!(error, SessionError::Signer(SignerError::Failed(_))),
            "the reply mismatch is a signer failure, not a refusal: {error:?}"
        );
        let staged = member.session.take_outbound().expect("take_outbound");
        assert!(
            staged.is_empty(),
            "nothing is staged for a rejected reply: {staged:?}"
        );
    }
}
