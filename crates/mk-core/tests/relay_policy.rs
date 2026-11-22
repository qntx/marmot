//! `relay_allowed` regression: `allow_loopback` is a loopback-only opt-in,
//! not a blanket IP-check bypass. Private, link-local and ULA relay literals
//! stay refused under the flag; loopback passes only under it.
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
    clippy::tests_outside_test_module,
    reason = "test code"
)]

#[cfg(feature = "testing")]
mod support;

#[cfg(feature = "testing")]
mod relay_policy_tests {
    use super::support::*;

    use std::sync::Arc;

    use mk_core::testing::{LocalSigner, ManualClock, RecordingHost};
    use mk_core::{AccountSigner, Opened, Session};

    /// Open a member whose destination policy has `allow_loopback` off.
    async fn open_member_no_loopback() -> Member {
        let signer: Arc<dyn AccountSigner> = Arc::new(LocalSigner::new());
        let clock = Arc::new(ManualClock::new());
        let host = Arc::new(RecordingHost::new());
        let mut config = session_config(
            Arc::clone(&signer),
            Arc::clone(&clock),
            Arc::clone(&host),
            Vec::new(),
        );
        config.destination_policy.allow_loopback = false;
        let opened = Session::open(config).expect("open");
        let session = match opened {
            Opened::NeedsAccountProof(pending) => {
                let signed = signer
                    .sign_event(pending.request().clone())
                    .await
                    .expect("sign");
                pending.provide_account_proof(&signed).expect("proof")
            }
            Opened::Ready(session) => *session,
        };
        Member {
            signer,
            clock,
            host,
            session,
        }
    }

    #[tokio::test]
    async fn allow_loopback_still_refuses_private_link_local_and_ula_relays() {
        let mut member = open_member().await;
        for relay in [
            "wss://10.0.0.1",
            "wss://169.254.169.254",
            "wss://192.168.1.1",
            "wss://[fd00::1]",
        ] {
            let refused = member
                .session
                .publish_inbox_relays(&[relay.to_owned()])
                .await
                .is_err();
            assert!(refused, "{relay} accepted under allow_loopback");
        }
    }

    #[tokio::test]
    async fn allow_loopback_admits_loopback_and_public_relays() {
        let mut member = open_member().await;
        for relay in [
            "ws://127.0.0.1:7777",
            "wss://[::1]",
            "wss://localhost",
            "wss://relay.example.com",
        ] {
            member
                .session
                .publish_inbox_relays(&[relay.to_owned()])
                .await
                .unwrap_or_else(|_| panic!("{relay} refused under allow_loopback"));
        }
    }

    #[tokio::test]
    async fn loopback_relays_are_refused_without_the_flag() {
        let mut member = open_member_no_loopback().await;
        for relay in ["wss://127.0.0.1", "wss://[::1]", "wss://localhost"] {
            let refused = member
                .session
                .publish_inbox_relays(&[relay.to_owned()])
                .await
                .is_err();
            assert!(refused, "{relay} accepted without allow_loopback");
        }
    }
}
