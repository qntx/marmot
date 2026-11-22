//! Fork reproduction for the S17 flake: two members of one group both
//! auto-commit the same `SelfRemove` proposal before either sees the
//! other's commit. With `ManualClock` the double-commit is deterministic —
//! each member's clock is advanced past the maximum jitter while the peer's
//! commit is withheld. The question the test answers: after every event is
//! redelivered in `(created_at, id)` order, do all members converge to one
//! branch and exchange messages without loss?

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
    clippy::excessive_nesting,
    clippy::too_many_lines,
    reason = "test code"
)]

use base64 as _;
use cgka_engine as _;
use cgka_traits as _;
use chacha20poly1305 as _;
use getrandom as _;
use hex as _;
use hkdf as _;
use mk_core as _;
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
use tokio as _;
use transport_nostr_peeler as _;
use url as _;
use zeroize as _;

#[cfg(feature = "testing")]
mod support;

#[cfg(feature = "testing")]
mod fork_tests {
    use super::support::*;
    use mk_core::{Event, GroupId, InputOutcome, SessionEvent};

    const RELAY: &str = "ws://127.0.0.1:7777";

    fn snapshot(member: &Member, gid: &GroupId) -> mk_core::GroupSnapshot {
        member
            .session
            .group(gid)
            .expect("group")
            .expect("group exists")
    }

    /// Ingest `events`, returning the per-input reports plus drained events.
    async fn ingest_reported(
        member: &mut Member,
        events: Vec<Event>,
    ) -> (Vec<mk_core::InputReport>, Vec<SessionEvent>) {
        let reports = member
            .session
            .ingest_batch(events)
            .await
            .expect("ingest_batch");
        let drained = member.session.drain_events().unwrap();
        let ids: Vec<_> = drained
            .iter()
            .filter_map(|event| match event {
                SessionEvent::MessageReceived { message, .. } => Some(message.transport_id),
                _ => None,
            })
            .collect();
        if !ids.is_empty() {
            member.session.acknowledge_messages(&ids).expect("ack");
        }
        (reports, drained)
    }

    /// Drive a member's SelfRemove auto-commit to a confirmed own commit:
    /// advance past the maximum jitter, tick, publish, confirm.
    async fn fire_auto_commit(member: &mut Member, relay: &FakeRelay) {
        member.clock.advance(5_000);
        member.session.tick().await.expect("tick");
        flush(member, relay).await;
        member.session.tick().await.expect("tick");
        flush(member, relay).await;
    }

    /// Core reproduction. Different accounts; the same-account S17 shape
    /// needs the account-devices sibling flow to seat a second device,
    /// which adds nothing to the convergence question.
    async fn competing_self_remove_commits() {
        let relay = FakeRelay::new();
        let mut b = open_member().await; // admin
        let mut a1 = open_member().await;
        let mut a2 = open_member().await;
        publish_identity(&mut b, &relay, RELAY).await;
        publish_identity(&mut a1, &relay, RELAY).await;
        publish_identity(&mut a2, &relay, RELAY).await;

        let gid: GroupId = group_with(&mut b, &relay, RELAY, &[&a1, &a2], "fork").await;
        join_via_welcome(&mut a1, &relay, RELAY).await;
        join_via_welcome(&mut a2, &relay, RELAY).await;
        sync_member(&mut b, &relay, RELAY, &gid).await;
        sync_member(&mut a1, &relay, RELAY, &gid).await;
        sync_member(&mut a2, &relay, RELAY, &gid).await;
        let epoch0 = snapshot(&b, &gid).epoch;
        eprintln!("[fork] settled epoch {epoch0}");

        // a2 leaves; only its SelfRemove proposal is on the relay.
        a2.session.leave(&gid).await.expect("leave");
        flush(&mut a2, &relay).await;
        let route = b.route_hex(&gid);
        let all = group_events(&relay, RELAY, &route);
        let proposal = all.last().expect("proposal").clone();
        eprintln!("[fork] proposal id {}", proposal.id);

        // Deliver ONLY the proposal to b and a1; each auto-commits with the
        // peer's commit withheld → deterministic competing same-epoch commits.
        let (rb, _) = ingest_reported(&mut b, vec![proposal.clone()]).await;
        let (ra1, _) = ingest_reported(&mut a1, vec![proposal]).await;
        eprintln!("[fork] b proposal outcome {:?}", rb[0].outcome);
        eprintln!("[fork] a1 proposal outcome {:?}", ra1[0].outcome);
        fire_auto_commit(&mut b, &relay).await;
        fire_auto_commit(&mut a1, &relay).await;
        eprintln!(
            "[fork] after own commits: b epoch {} a1 epoch {}",
            snapshot(&b, &gid).epoch,
            snapshot(&a1, &gid).epoch
        );

        // The relay now holds proposal + both commits. Redeliver EVERYTHING
        // in (created_at, id) order and settle; record what each member made
        // of the peer's commit on first delivery.
        let everything = group_events(&relay, RELAY, &route);
        eprintln!("[fork] relay holds {} group events", everything.len());
        let (b_reports, _) = ingest_reported(&mut b, everything.clone()).await;
        let (a1_reports, _) = ingest_reported(&mut a1, everything.clone()).await;
        for report in &a1_reports {
            if matches!(
                report.outcome,
                InputOutcome::Accepted
                    | InputOutcome::Deferred { .. }
                    | InputOutcome::Unavailable { .. }
            ) {
                eprintln!("[fork] a1 {} -> {:?}", report.event, report.outcome);
            }
        }
        // b sends on its own branch while a1 is still unsettled: the app
        // message reaches a1 mid-fork — the deferred-delivery case.
        send_chat(&mut b, &relay, &gid, "b-during-fork").await;

        let mut a1_settled_events: Vec<SessionEvent> = Vec::new();
        let a1_hex = a1.account_hex();
        for pass in 0..8 {
            // a2's group row can be fully forgotten once Removed.
            let mut live: Vec<&mut Member> = vec![&mut b, &mut a1];
            if a2.session.group(&gid).ok().flatten().is_some() {
                live.push(&mut a2);
            }
            for member in live {
                member.clock.advance(2_000);
                let backlog = group_events(&relay, RELAY, &member.route_hex(&gid));
                let (reports, drained) = ingest_reported(member, backlog).await;
                if member.account_hex() == a1_hex {
                    a1_settled_events.extend(drained);
                }
                for report in &reports {
                    if !matches!(report.outcome, InputOutcome::Accepted) {
                        eprintln!(
                            "[fork]   pass {pass} {} {} -> {:?}",
                            member.account_hex(),
                            report.event,
                            report.outcome
                        );
                    }
                }
                member.session.tick().await.expect("tick");
                flush(member, &relay).await;
            }
        }
        eprintln!(
            "[fork] settled: b {} a1 {} a2 {:?}",
            snapshot(&b, &gid).epoch,
            snapshot(&a1, &gid).epoch,
            a2.session.group(&gid).ok().flatten().map(|g| g.state)
        );

        // Both directions must cross the fork without loss.
        send_chat(&mut b, &relay, &gid, "b-after-fork").await;
        let a1_ev = sync_member(&mut a1, &relay, RELAY, &gid).await;
        send_chat(&mut a1, &relay, &gid, "a1-after-fork").await;
        let b_ev = sync_member(&mut b, &relay, RELAY, &gid).await;
        assert!(
            received(&a1_ev).contains(&"b-after-fork".to_owned()),
            "a1 must decode b's post-fork message (a1 recv: {:?})",
            received(&a1_ev)
        );
        let mut a1_all = a1_settled_events.clone();
        a1_all.extend(a1_ev.clone());
        assert!(
            received(&a1_all).contains(&"b-during-fork".to_owned()),
            "a1 must recover b's mid-fork message after settling (a1 recv: {:?})",
            received(&a1_ev)
        );
        assert!(
            received(&b_ev).contains(&"a1-after-fork".to_owned()),
            "b must decode a1's post-fork message (b recv: {:?})",
            received(&b_ev)
        );
        assert_eq!(snapshot(&b, &gid).epoch, snapshot(&a1, &gid).epoch);
        for report in &b_reports {
            eprintln!("[fork] b {} -> {:?}", report.event, report.outcome);
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn competing_self_remove_commits_converge() {
        competing_self_remove_commits().await;
    }

    /// The S17 ordering: a1 merges its own commit AND fully settles its
    /// convergence before b's competing commit is ever published. Does the
    /// late commit still enter convergence, or does it bounce off the peel
    /// (`TransportDeferred`) leaving a1 permanently diverged?
    #[tokio::test(flavor = "current_thread")]
    async fn settled_member_peels_late_competing_commit() {
        let relay = FakeRelay::new();
        let mut b = open_member().await;
        let mut a1 = open_member().await;
        let mut a2 = open_member().await;
        publish_identity(&mut b, &relay, RELAY).await;
        publish_identity(&mut a1, &relay, RELAY).await;
        publish_identity(&mut a2, &relay, RELAY).await;

        let gid: GroupId = group_with(&mut b, &relay, RELAY, &[&a1, &a2], "fork").await;
        join_via_welcome(&mut a1, &relay, RELAY).await;
        join_via_welcome(&mut a2, &relay, RELAY).await;
        sync_member(&mut b, &relay, RELAY, &gid).await;
        sync_member(&mut a1, &relay, RELAY, &gid).await;
        sync_member(&mut a2, &relay, RELAY, &gid).await;

        a2.session.leave(&gid).await.expect("leave");
        flush(&mut a2, &relay).await;
        let route = a1.route_hex(&gid);
        let proposal = group_events(&relay, RELAY, &route)
            .last()
            .expect("proposal")
            .clone();

        // Only a1 sees the proposal and commits; b's commit is withheld
        // entirely — b never even ingested the proposal yet.
        let _ = ingest_reported(&mut a1, vec![proposal.clone()]).await;
        fire_auto_commit(&mut a1, &relay).await;
        // a1 settles: several rounds of ingest+tick with only its own
        // commit on the relay, convergence cutoffs driven to completion.
        for _ in 0..8 {
            a1.clock.advance(5_000);
            let backlog = group_events(&relay, RELAY, &route);
            let _ = ingest_reported(&mut a1, backlog).await;
            a1.session.tick().await.expect("tick");
            flush(&mut a1, &relay).await;
        }
        eprintln!("[late] a1 settled at epoch {}", snapshot(&a1, &gid).epoch);

        // NOW b ingests the proposal, auto-commits and publishes — a1's
        // view is already settled on its own branch.
        let _ = ingest_reported(&mut b, vec![proposal]).await;
        fire_auto_commit(&mut b, &relay).await;
        eprintln!("[late] b committed; b epoch {}", snapshot(&b, &gid).epoch);

        // The moment of truth: a1's outcome for the late competing commit.
        let backlog = group_events(&relay, RELAY, &route);
        let (reports, _) = ingest_reported(&mut a1, backlog).await;
        for report in &reports {
            eprintln!("[late] a1 {} -> {:?}", report.event, report.outcome);
        }
        // Settle, then exchange both ways.
        for _ in 0..8 {
            for member in [&mut b, &mut a1] {
                member.clock.advance(5_000);
                let backlog = group_events(&relay, RELAY, &member.route_hex(&gid));
                let _ = ingest_reported(member, backlog).await;
                member.session.tick().await.expect("tick");
                flush(member, &relay).await;
            }
        }
        send_chat(&mut b, &relay, &gid, "b-late").await;
        let a1_ev = sync_member(&mut a1, &relay, RELAY, &gid).await;
        send_chat(&mut a1, &relay, &gid, "a1-late").await;
        let b_ev = sync_member(&mut b, &relay, RELAY, &gid).await;
        eprintln!(
            "[late] a1 recv {:?} | b recv {:?}",
            received(&a1_ev),
            received(&b_ev)
        );
        assert!(
            received(&a1_ev).contains(&"b-late".to_owned()),
            "a1 must decode b's post-fork message"
        );
        assert!(
            received(&b_ev).contains(&"a1-late".to_owned()),
            "b must decode a1's post-fork message"
        );
    }
}
