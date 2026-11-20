//! Delivered-index durability property: after every random step, B's
//! durable `DeliveredMessage` rows equal exactly the set of messages
//! surfaced to B (plus B's own sent rows) minus swept-expired — no orphan
//! rows, no missing rows — and a forgotten group keeps neither a
//! `DeliveredMessage` nor a `SendPacing` row. Requires the `testing`
//! feature.

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
    clippy::excessive_nesting,
    clippy::cognitive_complexity,
    clippy::too_many_lines,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::shadow_unrelated,
    clippy::large_futures,
    variant_size_differences,
    reason = "test code"
)]

use mk_core as _;

use base64 as _;
use cgka_engine as _;
use cgka_traits as _;
use chacha20poly1305 as _;
use getrandom as _;
use hkdf as _;
use nostr as _;
use openmls as _;
use openmls_rust_crypto as _;
use openmls_traits as _;
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
mod delivered_tests {
    use super::support::*;
    use std::collections::BTreeSet;

    use mk_core::{AppMessage, EventId, GroupId, SendOutcome, SessionEvent};
    use proptest::prelude::*;
    use proptest::test_runner::{Config as ProptestConfig, TestRunner};

    const RELAY: &str = "ws://127.0.0.1:7777";
    const DELIVERED_TABLE: u8 = 40;
    const PACING_TABLE: u8 = 41;
    const RETENTION_SECS: u64 = 60;

    /// Inner ids of every `DeliveredMessage` row a member holds: each key
    /// is `[40][len][gid][len][inner]` — the last 32 bytes.
    fn delivered_inners(member: &Member) -> BTreeSet<[u8; 32]> {
        member
            .host
            .durable_rows()
            .iter()
            .filter(|(key, _)| key.first() == Some(&DELIVERED_TABLE))
            .map(|(key, _)| {
                <[u8; 32]>::try_from(&key[key.len() - 32..])
                    .expect("delivered key ends in inner id")
            })
            .collect()
    }

    /// A `SendPacing` row survives for the group.
    fn pacing_row_exists(member: &Member) -> bool {
        member
            .host
            .durable_rows()
            .iter()
            .any(|(key, _)| key.first() == Some(&PACING_TABLE))
    }

    /// Fold a drain into the model: received messages are indexed,
    /// expirations remove rows, received transport ids owe an ack.
    fn apply_events(
        events: &[SessionEvent],
        expected: &mut BTreeSet<[u8; 32]>,
        unacked: &mut Vec<EventId>,
    ) {
        for event in events {
            match event {
                SessionEvent::MessageReceived { message, .. } => {
                    expected.insert(
                        <[u8; 32]>::try_from(
                            hex::decode(&message.id).expect("inner hex").as_slice(),
                        )
                        .expect("32-byte inner id"),
                    );
                    unacked.push(message.transport_id);
                }
                SessionEvent::MessageExpired { message, .. } => {
                    expected.remove(&message.0);
                }
                _ => {}
            }
        }
    }

    /// The post-step invariant: durable rows equal the surfaced set
    /// exactly, and a forgotten group holds no pacing row either.
    fn assert_invariant(
        member: &Member,
        expected: &BTreeSet<[u8; 32]>,
        forgotten: bool,
        step: usize,
    ) {
        assert_eq!(
            delivered_inners(member),
            *expected,
            "step {step}: no orphan rows, no missing rows"
        );
        assert!(
            !(forgotten && pacing_row_exists(member)),
            "step {step}: a forgotten group keeps no SendPacing row"
        );
    }

    #[derive(Debug, Clone)]
    enum Op {
        /// `a` (true) or `b` (false) sends one chat and publishes it.
        Send(bool),
        /// B ingests the group's backlog and drains — no ack.
        Deliver,
        /// B acknowledges every delivered-but-unacked transport id.
        Ack,
        /// B reopens from its durable rows.
        Restart,
        /// B forgets the group.
        Forget,
        /// B's clock advances `u64` seconds, then `tick` runs the sweep.
        Advance(u64),
    }

    fn op_strategy() -> impl Strategy<Value = Op> {
        prop_oneof![
            4 => prop::bool::ANY.prop_map(Op::Send),
            4 => Just(Op::Deliver),
            2 => Just(Op::Ack),
            2 => Just(Op::Restart),
            1 => Just(Op::Forget),
            3 => (0u64..120).prop_map(Op::Advance),
        ]
    }

    /// Enable 60 s retention from `a`, then let `b` settle it.
    async fn enable_retention(a: &mut Member, b: &mut Member, relay: &FakeRelay, gid: &GroupId) {
        let outcome = a
            .session
            .set_retention(gid, RETENTION_SECS)
            .await
            .expect("set_retention");
        assert!(matches!(outcome, SendOutcome::Staged(_)), "{outcome:?}");
        flush(a, relay).await;
        sync_member(b, relay, RELAY, gid).await;
    }

    /// One ingest + drain round with no ack: the property covers delivered
    /// rows regardless of the host's ack discipline.
    async fn deliver_no_ack(
        member: &mut Member,
        relay: &FakeRelay,
        route: &str,
    ) -> Vec<SessionEvent> {
        member
            .session
            .ingest_batch(group_events(relay, RELAY, route))
            .await
            .expect("ingest_batch");
        member.session.tick().await.expect("tick");
        member.session.drain_events().unwrap()
    }

    async fn send_one(member: &mut Member, relay: &FakeRelay, gid: &GroupId, i: usize) -> EventId {
        member.clock.advance(1_000);
        let sent = member
            .session
            .send_message(
                gid,
                AppMessage {
                    kind: 9,
                    tags: Vec::new(),
                    content: format!("msg#{i}"),
                },
            )
            .await
            .expect("send_message");
        flush(member, relay).await;
        sent.message.id()
    }

    async fn run_case(ops: Vec<Op>) -> Result<(), TestCaseError> {
        let relay = FakeRelay::new();
        let mut a = open_member().await;
        let mut b = open_member().await;
        publish_identity(&mut a, &relay, RELAY).await;
        publish_identity(&mut b, &relay, RELAY).await;
        let gid = group_with(&mut a, &relay, RELAY, &[&b], "delivered-prop").await;
        join_via_welcome(&mut b, &relay, RELAY).await;
        sync_member(&mut a, &relay, RELAY, &gid).await;
        sync_member(&mut b, &relay, RELAY, &gid).await;
        enable_retention(&mut a, &mut b, &relay, &gid).await;
        b.session.drain_events().unwrap();
        let route_b = b.route_hex(&gid);

        let mut expected: BTreeSet<[u8; 32]> = BTreeSet::new();
        let mut unacked: Vec<EventId> = Vec::new();
        let mut forgotten = false;
        let mut counter = 0usize;

        for (step, op) in ops.iter().enumerate() {
            match op {
                Op::Send(by_a) => {
                    if *by_a {
                        send_one(&mut a, &relay, &gid, counter).await;
                    } else if !forgotten {
                        let inner = send_one(&mut b, &relay, &gid, counter).await;
                        expected.insert(inner.0);
                    }
                    counter += 1;
                }
                Op::Deliver => {
                    let events = deliver_no_ack(&mut b, &relay, &route_b).await;
                    apply_events(&events, &mut expected, &mut unacked);
                }
                Op::Ack => {
                    if !unacked.is_empty() {
                        b.session.acknowledge_messages(&unacked).expect("ack");
                        unacked.clear();
                    }
                }
                Op::Restart => {
                    b = reopen_member(&b).await;
                    let events = b.session.drain_events().unwrap();
                    apply_events(&events, &mut expected, &mut unacked);
                }
                Op::Forget => {
                    if !forgotten {
                        b.session.forget_group(&gid).expect("forget_group");
                        forgotten = true;
                        expected.clear();
                        unacked.clear();
                    }
                }
                Op::Advance(secs) => {
                    b.clock.advance(secs * 1_000);
                    b.session.tick().await.expect("tick");
                    let events = b.session.drain_events().unwrap();
                    apply_events(&events, &mut expected, &mut unacked);
                }
            }
            assert_invariant(&b, &expected, forgotten, step);
        }
        Ok(())
    }

    /// Random interleavings of send / deliver (no ack) / ack / restart /
    /// forget / clock-advance+tick must leave the durable
    /// `DeliveredMessage` set equal to exactly the messages surfaced to B
    /// (plus own-sent rows) minus swept-expired rows.
    #[test]
    fn delivered_rows_match_surfaced_messages() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let mut runner = TestRunner::new(ProptestConfig {
            cases: std::env::var("PROPTEST_CASES")
                .ok()
                .and_then(|value| value.parse().ok())
                .unwrap_or(32),
            ..ProptestConfig::default()
        });
        runner
            .run(&prop::collection::vec(op_strategy(), 4..40), |ops| {
                runtime.block_on(run_case(ops))
            })
            .expect("delivered-index invariant holds");
    }

    /// Row-size report for the `DeliveredMessage` table: one real row's
    /// key and value are measured through the session, then extrapolated
    /// to 10k and 50k delivered messages (rows are fixed-width).
    #[ignore = "reports table sizes; run on demand"]
    #[tokio::test(flavor = "current_thread")]
    async fn delivered_table_growth_report() {
        let (relay, mut a, mut b, gid) = {
            let relay = FakeRelay::new();
            let mut a = open_member().await;
            let mut b = open_member().await;
            publish_identity(&mut a, &relay, RELAY).await;
            publish_identity(&mut b, &relay, RELAY).await;
            let gid = group_with(&mut a, &relay, RELAY, &[&b], "growth").await;
            join_via_welcome(&mut b, &relay, RELAY).await;
            sync_member(&mut a, &relay, RELAY, &gid).await;
            sync_member(&mut b, &relay, RELAY, &gid).await;
            (relay, a, b, gid)
        };
        send_chat(&mut a, &relay, &gid, "growth probe").await;
        sync_member(&mut b, &relay, RELAY, &gid).await;

        let rows: Vec<(Vec<u8>, Vec<u8>)> = b
            .host
            .durable_rows()
            .into_iter()
            .filter(|(key, _)| key.first() == Some(&DELIVERED_TABLE))
            .collect();
        let (key, value) = rows.first().expect("a delivered row exists");
        eprintln!(
            "DeliveredMessage per-row: key={}B value={}B total={}B",
            key.len(),
            value.len(),
            key.len() + value.len()
        );
        for n in [10_000usize, 50_000] {
            let total = n * (key.len() + value.len());
            eprintln!(
                "DeliveredMessage table after {n} delivered messages: {total} bytes ({:.1} B/message)",
                total as f64 / n as f64
            );
        }
    }
}
