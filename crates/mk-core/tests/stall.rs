//! D2 stall-detector tests: the durable per-group counter, the one-shot
//! `GroupStallSuspected`/`GroupStalled` pair, epoch/message clearing, and
//! the `access` mapping's real-flow variants.

#![allow(
    clippy::missing_docs_in_private_items,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::needless_pass_by_value,
    clippy::tests_outside_test_module,
    clippy::shadow_reuse,
    clippy::shadow_unrelated,
    clippy::doc_markdown,
    clippy::excessive_nesting,
    clippy::too_many_lines,
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
mod stall_tests {
    use super::support::*;

    use mk_core::{
        Availability, Event, GroupAccess, GroupId, GroupState, InputOutcome, InputReport,
        SessionEvent,
    };

    const RELAY: &str = "ws://127.0.0.1:7777";

    /// Ingest and drain, returning both halves.
    async fn ingest(
        member: &mut Member,
        events: Vec<Event>,
    ) -> (Vec<InputReport>, Vec<SessionEvent>) {
        let reports = member
            .session
            .ingest_batch(events)
            .await
            .expect("ingest_batch");
        let drained = member.session.drain_events().expect("drain_events");
        (reports, drained)
    }

    /// True for `Unavailable{TransportDeferred}`.
    const fn deferred(report: &InputReport) -> bool {
        matches!(
            report.outcome,
            InputOutcome::Unavailable {
                category: Availability::TransportDeferred,
            }
        )
    }

    /// The `GroupStallSuspected`/`GroupStalled` drain counts.
    fn stall_counts(events: &[SessionEvent]) -> (usize, usize) {
        let suspected = events
            .iter()
            .filter(|event| matches!(event, SessionEvent::GroupStallSuspected { .. }))
            .count();
        let stalled = events
            .iter()
            .filter(|event| matches!(event, SessionEvent::GroupStalled { .. }))
            .count();
        (suspected, stalled)
    }

    fn snapshot(member: &Member, gid: &GroupId) -> mk_core::GroupSnapshot {
        member
            .session
            .group(gid)
            .expect("snapshot")
            .expect("the group")
    }

    fn access(member: &Member, gid: &GroupId) -> GroupAccess {
        snapshot(member, gid).access
    }

    /// A group where `b` has committed `c1` and sent `n` chats at the new
    /// epoch; `a` has seen none of it. Returns the withheld commit and
    /// the feed events minus it.
    async fn behind(
        b: &mut Member,
        a: &mut Member,
        relay: &FakeRelay,
        n: usize,
    ) -> (GroupId, Event, Vec<Event>) {
        publish_identity(b, relay, RELAY).await;
        publish_identity(a, relay, RELAY).await;
        let gid = group_with(b, relay, RELAY, &[a], "stall").await;
        join_via_welcome(a, relay, RELAY).await;
        sync_member(b, relay, RELAY, &gid).await;
        sync_member(a, relay, RELAY, &gid).await;

        // Evidence counts only past `PRE_WELCOME_CLOCK_SKEW_SECS` after
        // the copy's Welcome, so the withheld-epoch traffic must be
        // stamped outside that window.
        b.clock.advance(400_000);

        let route = b.route_hex(&gid);
        let seen: std::collections::BTreeSet<mk_core::EventId> = group_events(relay, RELAY, &route)
            .into_iter()
            .map(|event| event.id)
            .collect();
        b.session.self_update(&gid).await.expect("self_update");
        flush(b, relay).await;
        let feed = group_events(relay, RELAY, &route);
        let c1 = feed
            .iter()
            .find(|event| event.kind == 445 && !seen.contains(&event.id))
            .expect("the self-update commit")
            .clone();
        for i in 0..n {
            send_chat(b, relay, &gid, &format!("m{i}")).await;
        }
        let rest: Vec<Event> = group_events(relay, RELAY, &route)
            .into_iter()
            .filter(|event| event.id != c1.id && !seen.contains(&event.id))
            .collect();
        (gid, c1, rest)
    }

    /// Three distinct deferred events suspect once; extra and repeated
    /// deliveries emit nothing more.
    #[tokio::test(flavor = "current_thread")]
    async fn stall_detector_threshold_and_refetch() {
        let relay = FakeRelay::new();
        let mut b = open_member().await;
        let mut a = open_member().await;
        let (gid, _c1, msgs) = behind(&mut b, &mut a, &relay, 4).await;
        assert_eq!(access(&a, &gid), GroupAccess::Active);

        // Two distinct deferred ids are not enough.
        let (reports, drained) = ingest(&mut a, msgs[..2].to_vec()).await;
        assert!(reports.iter().all(deferred));
        assert_eq!(stall_counts(&drained), (0, 0));

        // Repeating the same two ids counts nothing new — a still-
        // deferred event defers again, and the counter dedups by id.
        let (reports, drained) = ingest(&mut a, msgs[..2].to_vec()).await;
        assert!(
            reports.iter().all(|report| deferred(report)
                || matches!(report.outcome, InputOutcome::Rejected { .. })),
            "redelivery defers or dedups: {reports:?}"
        );
        assert_eq!(stall_counts(&drained), (0, 0));

        // The third distinct id reaches the threshold exactly once.
        let (reports, drained) = ingest(&mut a, msgs[2..3].to_vec()).await;
        assert!(reports.iter().all(deferred));
        assert_eq!(stall_counts(&drained), (1, 0));
        assert_eq!(access(&a, &gid), GroupAccess::Active);

        // A fourth deferred event and a threshold redelivery emit nothing.
        let (reports, drained) = ingest(&mut a, msgs[3..4].to_vec()).await;
        assert!(reports.iter().all(deferred));
        assert_eq!(stall_counts(&drained), (0, 0));
        let (_reports, drained) = ingest(&mut a, msgs[..3].to_vec()).await;
        assert_eq!(stall_counts(&drained), (0, 0));

        // The refetch completes: one `GroupStalled`, `access` flips.
        a.session.history_fetched(&gid).expect("history_fetched");
        let drained = a.session.drain_events().expect("drain_events");
        assert_eq!(stall_counts(&drained), (0, 1));
        assert_eq!(access(&a, &gid), GroupAccess::Stalled);

        // `GroupStalled` is idempotent.
        a.session.history_fetched(&gid).expect("history_fetched");
        let drained = a.session.drain_events().expect("drain_events");
        assert_eq!(stall_counts(&drained), (0, 0));
        assert_eq!(access(&a, &gid), GroupAccess::Stalled);
    }

    /// The missing commit arriving late moves the epoch, clears the row,
    /// and lets three new deferred events suspect again at the new epoch.
    #[tokio::test(flavor = "current_thread")]
    async fn late_commit_clears_and_epoch_recounts() {
        let relay = FakeRelay::new();
        let mut b = open_member().await;
        let mut a = open_member().await;
        let (gid, c1, msgs) = behind(&mut b, &mut a, &relay, 3).await;
        let (reports, drained) = ingest(&mut a, msgs.clone()).await;
        assert!(reports.iter().all(deferred), "all deferred: {reports:?}");
        assert_eq!(stall_counts(&drained), (1, 0));

        // Epoch progress clears the row and the deferred traffic replays.
        let (_reports, drained) = ingest(&mut a, vec![c1]).await;
        assert_eq!(stall_counts(&drained), (0, 0));
        assert!(
            drained
                .iter()
                .any(|event| matches!(event, SessionEvent::EpochChanged { .. })),
            "the late commit moves the epoch"
        );
        assert_eq!(access(&a, &gid), GroupAccess::Active);

        // A history report after progress must not flag `stalled`.
        a.session.history_fetched(&gid).expect("history_fetched");
        let drained = a.session.drain_events().expect("drain_events");
        assert_eq!(stall_counts(&drained), (0, 0));
        assert_eq!(access(&a, &gid), GroupAccess::Active);

        // Three fresh deferred events at the new epoch suspect again.
        let route = b.route_hex(&gid);
        let seen: std::collections::BTreeSet<mk_core::EventId> =
            group_events(&relay, RELAY, &route)
                .into_iter()
                .map(|event| event.id)
                .collect();
        b.session.self_update(&gid).await.expect("self_update");
        flush(&mut b, &relay).await;
        let feed = group_events(&relay, RELAY, &route);
        let c2 = feed
            .iter()
            .find(|event| event.kind == 445 && !seen.contains(&event.id))
            .expect("the second self-update commit")
            .clone();
        for i in 0..3 {
            send_chat(&mut b, &relay, &gid, &format!("n{i}")).await;
        }
        let rest: Vec<Event> = group_events(&relay, RELAY, &route)
            .into_iter()
            .filter(|event| event.id != c2.id && !seen.contains(&event.id))
            .collect();
        let (reports, drained) = ingest(&mut a, rest).await;
        assert!(reports.iter().all(deferred));
        assert_eq!(stall_counts(&drained), (1, 0));
    }

    /// An accepted message at the counted epoch clears the counter.
    #[tokio::test(flavor = "current_thread")]
    async fn accepted_current_epoch_message_clears() {
        let relay = FakeRelay::new();
        let mut b = open_member().await;
        let mut a = open_member().await;
        publish_identity(&mut b, &relay, RELAY).await;
        publish_identity(&mut a, &relay, RELAY).await;
        let gid = group_with(&mut b, &relay, RELAY, &[&a], "stall-clear").await;
        join_via_welcome(&mut a, &relay, RELAY).await;
        sync_member(&mut b, &relay, RELAY, &gid).await;
        sync_member(&mut a, &relay, RELAY, &gid).await;

        // An epoch-1 message authored before the withheld commit, kept
        // back from A.
        send_chat(&mut b, &relay, &gid, "early").await;
        let route = b.route_hex(&gid);
        let seen: std::collections::BTreeSet<mk_core::EventId> =
            group_events(&relay, RELAY, &route)
                .into_iter()
                .map(|event| event.id)
                .collect();
        let early = group_events(&relay, RELAY, &route)
            .into_iter()
            .find(|event| event.kind == 445)
            .expect("the epoch-1 chat");
        let epoch0 = snapshot(&a, &gid).epoch;

        // See `behind`: the withheld-epoch traffic must postdate the
        // post-Welcome skew window to count.
        b.clock.advance(400_000);

        b.session.self_update(&gid).await.expect("self_update");
        flush(&mut b, &relay).await;
        let c1 = group_events(&relay, RELAY, &route)
            .into_iter()
            .find(|event| event.kind == 445 && !seen.contains(&event.id))
            .expect("the self-update commit");
        for i in 0..3 {
            send_chat(&mut b, &relay, &gid, &format!("m{i}")).await;
        }
        let deferred_msgs: Vec<Event> = group_events(&relay, RELAY, &route)
            .into_iter()
            .filter(|event| event.id != c1.id && event.id != early.id && !seen.contains(&event.id))
            .collect();
        let (_reports, drained) = ingest(&mut a, deferred_msgs).await;
        assert_eq!(stall_counts(&drained), (1, 0));

        // The epoch-1 message arrives: accepted at the counted epoch.
        let (_reports, drained) = ingest(&mut a, vec![early]).await;
        assert!(
            drained
                .iter()
                .any(|event| matches!(event, SessionEvent::MessageReceived { .. })),
            "the held-back message delivers"
        );
        assert_eq!(access(&a, &gid), GroupAccess::Active);

        // The counter restarted: one fresh deferred id does not suspect.
        let (reports, drained) = ingest(&mut a, Vec::new()).await;
        assert_eq!(reports, []);
        assert_eq!(stall_counts(&drained), (0, 0));
        // Refetch-reporting after the clear is a no-op.
        a.session.history_fetched(&gid).expect("history_fetched");
        let drained = a.session.drain_events().expect("drain_events");
        assert_eq!(stall_counts(&drained), (0, 0));
        assert_eq!(snapshot(&a, &gid).epoch, epoch0, "the copy never moved");
    }

    /// Pre-join history routes to the group but predates this copy; it
    /// must not count toward the threshold (P5).
    #[tokio::test(flavor = "current_thread")]
    async fn pre_join_history_never_counts() {
        let relay = FakeRelay::new();
        let mut b = open_member().await;
        let mut c = open_member().await;
        let mut a = open_member().await;
        publish_identity(&mut b, &relay, RELAY).await;
        publish_identity(&mut c, &relay, RELAY).await;
        publish_identity(&mut a, &relay, RELAY).await;
        let gid = group_with(&mut b, &relay, RELAY, &[&c], "pre-join").await;
        join_via_welcome(&mut c, &relay, RELAY).await;
        sync_member(&mut b, &relay, RELAY, &gid).await;
        sync_member(&mut c, &relay, RELAY, &gid).await;
        for i in 0..4 {
            send_chat(&mut b, &relay, &gid, &format!("pre{i}")).await;
        }
        // Snapshot the pre-join feed before the invite.
        let route = b.route_hex(&gid);
        let pre_join = group_events(&relay, RELAY, &route);

        // Invite A; it joins and then sees the pre-join backlog.
        let kp = key_package_of(&relay, RELAY, &a.account_hex());
        b.session
            .invite(
                &gid,
                vec![mk_core::Invitee {
                    key_package: kp,
                    inbox_relays: inbox_relays_of(&relay, RELAY, &a.account_hex()),
                }],
            )
            .await
            .expect("invite");
        flush(&mut b, &relay).await;
        join_via_welcome(&mut a, &relay, RELAY).await;
        sync_member(&mut a, &relay, RELAY, &gid).await;

        let (reports, drained) = ingest(&mut a, pre_join).await;
        assert!(
            reports.iter().all(|report| deferred(report)
                || matches!(report.outcome, InputOutcome::Rejected { .. })),
            "pre-join traffic is deferred or deduped, never counted"
        );
        assert_eq!(stall_counts(&drained), (0, 0));
        assert_eq!(access(&a, &gid), GroupAccess::Active);
    }

    /// A removed device that still shares the account with a live leaf is
    /// `RemovedDevice`; a fully removed account is `Removed` (P2).
    #[tokio::test(flavor = "current_thread")]
    async fn removed_device_and_removed_access() {
        use std::sync::Arc;

        let relay = FakeRelay::new();
        let signer: Arc<dyn mk_core::AccountSigner> =
            Arc::new(mk_core::testing::LocalSigner::from_secret([7u8; 32]).expect("signer"));
        let mut a1 = open_member_with(Arc::clone(&signer)).await;
        let mut a2 = open_member_with(signer).await;
        let mut b = open_member().await;
        publish_identity(&mut a1, &relay, RELAY).await;
        publish_identity(&mut b, &relay, RELAY).await;
        let account = a1.account_hex();
        let kp_of = |relay: &FakeRelay| -> Vec<Event> {
            relay.fetch(RELAY, {
                let wanted = account.clone();
                move |event| event.kind == 30_443 && event.pubkey.to_hex() == wanted
            })
        };
        let a1_kp = kp_of(&relay).into_iter().next().expect("a1 key package");
        publish_identity(&mut a2, &relay, RELAY).await;
        let a2_kp = kp_of(&relay)
            .into_iter()
            .find(|event| event.id != a1_kp.id)
            .expect("a2 key package");
        let invitee = |key_package: Event| mk_core::Invitee {
            key_package,
            inbox_relays: vec![RELAY.to_owned()],
        };

        // Both devices of the account join as separate leaves.
        let gid = b
            .session
            .create_group(mk_core::CreateGroup {
                name: "two-devices".to_owned(),
                description: String::new(),
                relays: vec![RELAY.to_owned()],
                invitees: vec![invitee(a1_kp), invitee(a2_kp)],
                admins: Vec::new(),
                components: Vec::new(),
            })
            .await
            .expect("create_group");
        flush(&mut b, &relay).await;
        for member in [&mut a1, &mut a2] {
            let welcomes = welcomes_for(&relay, RELAY, &account);
            ingest_backlog(member, &welcomes).await;
            member.session.tick().await.expect("tick");
            flush(member, &relay).await;
        }
        sync_member(&mut b, &relay, RELAY, &gid).await;
        sync_member(&mut a1, &relay, RELAY, &gid).await;
        sync_member(&mut a2, &relay, RELAY, &gid).await;

        // One device leaves: the removed copy still sees the account's
        // other leaf — `RemovedDevice`, state `Removed`.
        let route_a = a1.route_hex(&gid);
        let seen: std::collections::BTreeSet<mk_core::EventId> =
            group_events(&relay, RELAY, &route_a)
                .iter()
                .map(|event| event.id)
                .collect();
        a2.session.leave(&gid).await.expect("leave");
        flush(&mut a2, &relay).await;
        // B ingests the SelfRemove proposal and auto-commits past jitter.
        // a1's deliveries are ordered proposal-first: a commit ingested
        // before the proposal it references stays deferred past
        // redelivery (and event `created_at` is the signer's wall second,
        // so same-second batch order is by id — not something clock
        // advances can arrange). Ingesting the proposal alone first also
        // keeps a1's auto-committer from forking the epoch: the covering
        // commit lands before any jitter can fire.
        let proposal_ids: std::collections::BTreeSet<mk_core::EventId> =
            group_events(&relay, RELAY, &route_a)
                .iter()
                .filter(|event| !seen.contains(&event.id))
                .map(|event| event.id)
                .collect();
        b.clock.advance(60_000);
        let route = b.route_hex(&gid);
        ingest_backlog(&mut b, &group_events(&relay, RELAY, &route)).await;
        for _ in 0..2 {
            b.clock.advance(5_000);
            b.session.tick().await.expect("tick");
            flush(&mut b, &relay).await;
        }
        for _ in 0..6 {
            let feed = group_events(&relay, RELAY, &route_a);
            let (proposals, rest): (Vec<Event>, Vec<Event>) = feed
                .into_iter()
                .partition(|event| proposal_ids.contains(&event.id));
            if !proposals.is_empty() {
                ingest_backlog(&mut a1, &proposals).await;
            }
            ingest_backlog(&mut a1, &rest).await;
            a1.session.tick().await.expect("tick");
            a1.clock.advance(2_000);
            flush(&mut a1, &relay).await;
        }
        sync_member(&mut a2, &relay, RELAY, &gid).await;
        let snap = snapshot(&a2, &gid);
        assert_eq!(snap.state, GroupState::Removed);
        assert_eq!(snap.access, GroupAccess::RemovedDevice);
        assert_eq!(snapshot(&a1, &gid).access, GroupAccess::Active);

        // The admin then removes the whole account: a1 ends `Removed`.
        b.session
            .remove_members(&gid, std::slice::from_ref(&a1.session.account()))
            .await
            .expect("remove_members");
        flush(&mut b, &relay).await;
        // The removal may queue behind B's convergence; drive it to the
        // wire before the members sync.
        for _ in 0..2 {
            b.clock.advance(5_000);
            b.session.tick().await.expect("tick");
            flush(&mut b, &relay).await;
        }
        for _ in 0..6 {
            let feed = group_events(&relay, RELAY, &route_a);
            let (proposals, rest): (Vec<Event>, Vec<Event>) = feed
                .into_iter()
                .partition(|event| proposal_ids.contains(&event.id));
            if !proposals.is_empty() {
                ingest_backlog(&mut a1, &proposals).await;
            }
            ingest_backlog(&mut a1, &rest).await;
            a1.session.tick().await.expect("tick");
            a1.clock.advance(2_000);
            flush(&mut a1, &relay).await;
        }
        sync_member(&mut a2, &relay, RELAY, &gid).await;
        // a1's live copy processes the removal: no account leaf remains.
        let snap = snapshot(&a1, &gid);
        assert_eq!(snap.state, GroupState::Removed);
        assert_eq!(snap.access, GroupAccess::Removed);
        // a2's copy ended at its own leaf's removal and projects that
        // ending forever — it can no longer learn the account removal.
        let snap = snapshot(&a2, &gid);
        assert_eq!(snap.state, GroupState::Removed);
        assert_eq!(snap.access, GroupAccess::RemovedDevice);
    }
}
