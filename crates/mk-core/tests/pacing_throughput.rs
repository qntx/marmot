//! Sustained send throughput on a real wall clock: one member sends chat
//! intents as fast as the session accepts them for 45 s, and the test reports
//! the pacing shape — the initial burst absorbed before the first
//! `SendRateExceeded`, the steady-state accepted rate, and refusals per wall
//! second. Ignored: run on demand, e.g.
//! `cargo test -p mk-core --all-features --release --test pacing_throughput -- --ignored --nocapture`.

#![allow(
    unused_crate_dependencies,
    clippy::missing_docs_in_private_items,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::needless_pass_by_value,
    clippy::tests_outside_test_module,
    clippy::shadow_reuse,
    clippy::doc_markdown,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    reason = "test code"
)]

use mk_core as _;
use tokio as _;

use base64 as _;
use cgka_engine as _;
use cgka_traits as _;
use chacha20poly1305 as _;
use getrandom as _;
use hex as _;
use hkdf as _;
use nostr as _;
use openmls as _;
use openmls_rust_crypto as _;
use openmls_traits as _;
use proptest as _;
use serde as _;
use serde_json as _;
use sha2 as _;
use storage_sqlite as _;
use thiserror as _;
use transport_nostr_peeler as _;
use url as _;
use zeroize as _;

#[cfg(feature = "testing")]
mod support;

#[cfg(feature = "testing")]
mod throughput {
    use super::support::*;
    use std::sync::Arc;
    use std::time::{Instant, SystemTime, UNIX_EPOCH};

    use mk_core::testing::{LocalSigner, RecordingHost};
    use mk_core::{
        AccountSigner, Clock, DestinationPolicy, MessageIntent, Opened, Session, SessionConfig,
        SessionError, Time,
    };

    const RELAY: &str = "ws://127.0.0.1:7777";
    const RUN_SECONDS: u64 = 45;

    /// A real clock: unix time from `SystemTime`, monotonic from `Instant`.
    #[derive(Debug)]
    struct WallClock {
        start: Instant,
    }

    impl WallClock {
        fn new() -> Self {
            Self {
                start: Instant::now(),
            }
        }
    }

    impl Clock for WallClock {
        fn now(&self) -> Time {
            Time {
                monotonic_ms: self.start.elapsed().as_millis() as u64,
                unix_ms: SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .expect("system clock before epoch")
                    .as_millis() as u64,
            }
        }
    }

    /// `open_member` with the wall clock instead of `ManualClock`.
    async fn open_wall_member() -> Member {
        let signer: Arc<dyn AccountSigner> = Arc::new(LocalSigner::new());
        let clock = Arc::new(WallClock::new());
        let host = Arc::new(RecordingHost::new());
        let kv_host: Arc<dyn mk_core::KvHost> = host.clone();
        let opened = Session::open(SessionConfig {
            signer: Arc::clone(&signer),
            host: kv_host,
            rows: Vec::new(),
            clock,
            destination_policy: DestinationPolicy {
                allow_loopback: true,
            },
            client_name: None,
        })
        .expect("open");
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
            // The handle's clock field is only advanced by helpers this test does not
            // use; the session reads the wall clock it was opened with.
            clock: Arc::new(mk_core::testing::ManualClock::new()),
            host,
            session,
        }
    }

    /// 45 s of maximal chat sends; report per-second accepted/refused.
    #[ignore = "45 s wall-clock throughput measurement; run on demand"]
    #[tokio::test(flavor = "current_thread")]
    async fn pacing_sustained_throughput() {
        let relay = FakeRelay::new();
        let mut a = open_wall_member().await;
        let mut b = open_member().await;
        publish_identity(&mut a, &relay, RELAY).await;
        publish_identity(&mut b, &relay, RELAY).await;
        let gid = group_with(&mut a, &relay, RELAY, &[&b], "throughput").await;
        join_via_welcome(&mut b, &relay, RELAY).await;
        sync_member(&mut a, &relay, RELAY, &gid).await;

        let run_start = Instant::now();
        let mut accepted_per_sec = vec![0_u64; RUN_SECONDS as usize + 2];
        let mut refused_per_sec = vec![0_u64; RUN_SECONDS as usize + 2];
        let mut first_refusal_ms: Option<u128> = None;
        let mut burst = 0_u64;
        let mut attempt = 0_u64;
        let mut drained_events = 0_usize;
        while run_start.elapsed().as_secs() < RUN_SECONDS {
            let second = run_start.elapsed().as_secs() as usize;
            attempt += 1;
            let sent = a
                .session
                .send_intent(
                    &gid,
                    MessageIntent::Chat {
                        content: format!("t{attempt}"),
                        reply_to: None,
                    },
                )
                .await;
            match sent {
                Ok(_) => {
                    accepted_per_sec[second] += 1;
                    if first_refusal_ms.is_none() {
                        burst += 1;
                    }
                    // Keep the gated outbound pipeline moving like a real host.
                    flush(&mut a, &relay).await;
                }
                Err(SessionError::SendRateExceeded) => {
                    refused_per_sec[second] += 1;
                    if first_refusal_ms.is_none() {
                        first_refusal_ms = Some(run_start.elapsed().as_millis());
                    }
                }
                Err(error) => panic!("unexpected send error: {error:?}"),
            }
            // Bound the event queue: drain every 64 attempts.
            if attempt % 64 == 0 {
                drained_events += a.session.drain_events().unwrap().len();
            }
        }
        drained_events += a.session.drain_events().unwrap().len();

        let accepted: u64 = accepted_per_sec.iter().sum();
        let refused: u64 = refused_per_sec.iter().sum();
        let elapsed = run_start.elapsed().as_secs_f64();
        eprintln!("pacing throughput: {attempt} attempts over {elapsed:.1}s");
        eprintln!("  accepted={accepted} refused={refused} drained_events={drained_events}");
        eprintln!(
            "  burst before first refusal={burst} first_refusal_at_ms={:?} \
             steady_state={:.1} accepted msgs/s",
            first_refusal_ms,
            accepted as f64 / elapsed,
        );
        for (second, (ok, no)) in accepted_per_sec
            .iter()
            .zip(refused_per_sec.iter())
            .enumerate()
        {
            if *ok > 0 || *no > 0 {
                eprintln!("  second {second}: accepted={ok} refused={no}");
            }
        }
    }
}
