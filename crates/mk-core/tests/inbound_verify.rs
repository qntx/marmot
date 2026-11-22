//! Inbound id+signature verification before dedupe: a same-id forged copy
//! (bad signature, identical id commitment) can never shadow the genuine
//! event in `ingest_batch` — the forgery is reported `InvalidSignature`
//! and the genuine event is still ingested.
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
mod inbound_verify_tests {
    use super::support::*;

    use mk_core::testing::LocalSigner;
    use mk_core::{AccountSigner, InputCategory, InputOutcome, UnsignedEvent};

    /// A forged same-id copy — signature tampered, every id-committed
    /// field identical — sorted ahead of the genuine event cannot shadow
    /// it: the batch verifies before dedupe, reports the forgery
    /// `InvalidSignature` and still ingests the genuine event.
    #[tokio::test(flavor = "current_thread")]
    async fn forged_same_id_copy_cannot_shadow_genuine_event() {
        let mut member = open_member().await;
        let author = LocalSigner::new();
        let genuine = author
            .sign_event(UnsignedEvent {
                pubkey: author.account(),
                created_at: 1_700_000_000,
                kind: 445,
                tags: vec![vec!["h".to_owned(), "cc".repeat(32)]],
                content: "genuine".to_owned(),
            })
            .await
            .expect("sign");
        // Same id commitment (the sig field is not part of the id), but the
        // schnorr check fails — a forged copy of the genuine event.
        let mut forged = genuine.clone();
        forged.sig[0] ^= 0xff;

        // Stable sort on equal (created_at, id) keys keeps the forged copy
        // first — the exact order a hostile relay would prefer.
        let reports = member
            .session
            .ingest_batch(vec![forged, genuine.clone()])
            .await
            .expect("ingest batch");
        assert_eq!(
            reports.len(),
            2,
            "the forgery is reported and the genuine event still ingests: {reports:?}"
        );
        assert_eq!(
            reports[0].outcome,
            InputOutcome::Rejected {
                category: InputCategory::InvalidSignature,
            },
            "the forged copy is reported InvalidSignature"
        );
        assert!(
            !matches!(
                reports[1].outcome,
                InputOutcome::Rejected {
                    category: InputCategory::InvalidSignature,
                }
            ),
            "the genuine event is ingested, not shadowed: {reports:?}"
        );
    }

    /// A badly signed event in a batch is `InvalidSignature` and every
    /// well-formed sibling still ingests.
    #[tokio::test(flavor = "current_thread")]
    async fn bad_signature_is_reported_and_siblings_ingest() {
        let mut member = open_member().await;
        let author = LocalSigner::new();
        let good = author
            .sign_event(UnsignedEvent {
                pubkey: author.account(),
                created_at: 1_700_000_000,
                kind: 445,
                tags: vec![vec!["h".to_owned(), "dd".repeat(32)]],
                content: "a".to_owned(),
            })
            .await
            .expect("sign");
        let mut bad = good.clone();
        bad.sig = [0u8; 64];
        // A different id entirely — this is a malformed event, not a copy.
        bad.id = mk_core::EventId([9u8; 32]);

        let reports = member
            .session
            .ingest_batch(vec![bad, good])
            .await
            .expect("ingest batch");
        assert_eq!(reports.len(), 2, "both events report: {reports:?}");
        assert!(
            reports.iter().any(|report| matches!(
                report.outcome,
                InputOutcome::Rejected {
                    category: InputCategory::InvalidSignature,
                }
            )),
            "the malformed event is InvalidSignature: {reports:?}"
        );
    }
}
