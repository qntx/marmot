//! Poison gift-wrap ingest: a validly signed kind-1059 whose NIP-44
//! payload cannot be decrypted. `SignerError::InvalidPayload` means the
//! signer ran and the payload is permanently bad, so the ingest commits
//! as `rejected: invalid_encoding`, the ingress dedup marker remembers
//! the id, and a re-delivery is `duplicate` without asking the signer
//! again — in-session and across a reopen. Contrast with a refused or
//! failed unwrap, which stays `unavailable` and retryable (flow.rs
//! `refused_welcome_unwrap_is_retryable`).
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
mod poison_wrap_tests {
    use super::support::*;
    use std::sync::Arc;

    use base64::Engine as _;
    use mk_core::testing::LocalSigner;
    use mk_core::{
        AccountSigner, Event, InputCategory, InputOutcome, Opened, Session, UnsignedEvent,
    };

    /// A gift wrap from a throwaway author whose `content` is a
    /// nip44-v2-shaped payload with a garbage MAC.
    async fn poison_wrap(recipient_hex: &str, created_at: u64) -> Event {
        let author = LocalSigner::new();
        let mut payload = vec![2u8];
        payload.extend_from_slice(&[0xab; 32 + 34 + 32]);
        let content = base64::engine::general_purpose::STANDARD.encode(payload);
        author
            .sign_event(UnsignedEvent {
                pubkey: author.account(),
                created_at,
                kind: 1059,
                tags: vec![vec!["p".to_owned(), recipient_hex.to_owned()]],
                content,
            })
            .await
            .expect("sign")
    }

    /// `reopen_member` over an arbitrary durable row set — e.g. a prefix
    /// of the committed batch history, for a crash that lost the tail.
    fn reopen_with_rows(member: &Member, rows: Vec<(Vec<u8>, Vec<u8>)>) -> Member {
        let opened = Session::open(session_config(
            Arc::clone(&member.signer),
            Arc::clone(&member.clock),
            Arc::clone(&member.host),
            rows,
        ))
        .expect("reopen");
        let session = match opened {
            Opened::Ready(session) => *session,
            Opened::NeedsAccountProof(_) => panic!("restart must not need a proof"),
        };
        Member {
            signer: Arc::clone(&member.signer),
            clock: Arc::clone(&member.clock),
            host: Arc::clone(&member.host),
            session,
        }
    }

    /// A gift wrap the signer reports undecryptable is a permanent
    /// rejection: `rejected: invalid_encoding`, committed so the dedup
    /// marker remembers the id. A re-delivery is `duplicate` and never
    /// reaches the signer — in-session and after a reopen.
    #[tokio::test(flavor = "current_thread")]
    async fn undecryptable_wrap_is_rejected_permanently() {
        let signer = Arc::new(GatedSigner::new());
        signer.set(DecryptGate::InvalidPayload);
        let mut member = open_member_with(Arc::<GatedSigner>::clone(&signer)).await;
        let wrap = poison_wrap(&member.account_hex(), 1_700_000_000).await;

        let report = member.session.ingest(&wrap).await.expect("ingest");
        assert_eq!(
            report.outcome,
            InputOutcome::Rejected {
                category: InputCategory::InvalidEncoding,
            },
            "an undecryptable wrap is a permanent rejection"
        );
        assert_eq!(signer.decrypt_calls(), 1, "the signer ran once");

        let again = member.session.ingest(&wrap).await.expect("re-ingest");
        assert_eq!(
            again.outcome,
            InputOutcome::Rejected {
                category: InputCategory::Duplicate,
            },
            "the rejected wrap is remembered"
        );
        assert_eq!(
            signer.decrypt_calls(),
            1,
            "a re-delivery never reaches the signer"
        );

        if let Some(seq) = member.session.awaiting_durability() {
            member.session.acknowledge_durable(seq);
        }
        let mut reopened = reopen_member(&member).await;
        let post = reopened
            .session
            .ingest(&wrap)
            .await
            .expect("post-reopen ingest");
        assert_eq!(
            post.outcome,
            InputOutcome::Rejected {
                category: InputCategory::Duplicate,
            },
            "the durable marker still rejects the wrap"
        );
        assert_eq!(signer.decrypt_calls(), 1, "no signer call after reopen");
    }

    /// Crash consistency: when the reject's batch never reached durable
    /// rows, the reopened session re-ingests the wrap and asks the signer
    /// again — landing on the same permanent rejection. Once the marker
    /// is durable, a reopen reports `duplicate` with no signer call.
    #[tokio::test(flavor = "current_thread")]
    async fn undecryptable_wrap_reject_is_idempotent_across_crash() {
        let signer = Arc::new(GatedSigner::new());
        signer.set(DecryptGate::InvalidPayload);
        let mut member = open_member_with(Arc::<GatedSigner>::clone(&signer)).await;
        let wrap = poison_wrap(&member.account_hex(), 1_700_000_000).await;

        let batches_before = member.host.batches().len();
        let report = member.session.ingest(&wrap).await.expect("ingest");
        assert_eq!(
            report.outcome,
            InputOutcome::Rejected {
                category: InputCategory::InvalidEncoding,
            },
            "an undecryptable wrap is a permanent rejection"
        );
        assert_eq!(signer.decrypt_calls(), 1);
        assert!(
            member.host.batches().len() > batches_before,
            "the rejection staged a batch"
        );

        // The ingest batch is lost before reaching durable rows: the
        // reopened session sees the wrap as fresh input, asks the signer
        // again and lands on the same permanent rejection.
        let lost = member.host.rows_after_batches(batches_before);
        let mut crashed = reopen_with_rows(&member, lost);
        let replay = crashed.session.ingest(&wrap).await.expect("re-ingest");
        assert_eq!(
            replay.outcome,
            InputOutcome::Rejected {
                category: InputCategory::InvalidEncoding,
            },
            "the re-ingest rejects identically"
        );
        assert_eq!(signer.decrypt_calls(), 2, "the signer is asked again");

        // With the marker durable, the next reopen reports `duplicate`
        // without a signer call.
        let mut reopened = reopen_member(&crashed).await;
        let last = reopened.session.ingest(&wrap).await.expect("re-ingest");
        assert_eq!(
            last.outcome,
            InputOutcome::Rejected {
                category: InputCategory::Duplicate,
            },
            "the durable marker rejects as duplicate"
        );
        assert_eq!(
            signer.decrypt_calls(),
            2,
            "no signer call once the marker is durable"
        );
    }
}
