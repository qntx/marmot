//! D4 `SelfUpdate` maintenance flows (m6-a step 4): the post-join
//! obligation's CatchUp → Quiet → Jitter → gate → PendingPublication →
//! Complete walk, the 24 h overdue path, quiet resets on peer state,
//! periodic rotation and the send-pacing boundary, all on `ManualClock`
//! with the durable `cgka_maintenance` rows as the observable.

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
    reason = "test code"
)]

use mk_core as _;
use tokio as _;

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
use proptest as _;
use serde as _;
use serde_json as _;
use sha2 as _;
use storage_sqlite as _;
use thiserror as _;
use transport_nostr_peeler as _;
use url as _;
use zeroize as _;

use hex as _;

#[cfg(feature = "testing")]
mod support;

#[cfg(feature = "testing")]
mod self_update_tests {

    use cgka_traits::maintenance::{
        MaintenanceObligation, MaintenancePhase, MaintenanceTrigger, PostJoinHistoryFetchOutcome,
    };
    use mk_core::testing::maintenance_rows;
    use mk_core::{Clock, GroupId, Outbound, OutboundPurpose, SessionError, SessionEvent};

    use super::support::*;

    const RELAY: &str = "ws://127.0.0.1:7777";

    /// Release the gated batch, take the releasable outbound.
    fn take(member: &mut Member) -> Vec<Outbound> {
        if let Some(seq) = member.session.awaiting_durability() {
            member.session.acknowledge_durable(seq);
        }
        member.session.take_outbound().expect("take_outbound")
    }

    /// Publish `item` to `relay` as accepted and report it back.
    async fn accept(member: &mut Member, relay: &FakeRelay, item: &Outbound) {
        let results = relay.publish(item);
        member
            .session
            .report_publish(&item.id, &results)
            .await
            .expect("report_publish");
    }

    /// The member's durable obligations for `group`.
    fn obligations(member: &Member, group: &GroupId) -> Vec<MaintenanceObligation> {
        let (_, obligations, _) = maintenance_rows(&member.host.durable_rows());
        obligations
            .into_iter()
            .filter(|obligation| obligation.group_id.as_slice() == group.0.as_slice())
            .collect()
    }

    /// The member's durable `GroupMaintenanceState` for `group`, if any.
    fn maintenance_of(
        member: &Member,
        group: &GroupId,
    ) -> Option<cgka_traits::maintenance::GroupMaintenanceState> {
        let (states, _, _) = maintenance_rows(&member.host.durable_rows());
        states
            .into_iter()
            .find(|state| state.group_id.as_slice() == group.0.as_slice())
    }

    /// Every `SelfUpdated` seen in a drain.
    fn self_updated(events: &[SessionEvent], group: &GroupId) -> bool {
        events
            .iter()
            .any(|event| matches!(event, SessionEvent::SelfUpdated { group: g } if g == group))
    }

    /// Set up: creator `b` makes a group, `a` joins via Welcome; `a`'s
    /// post-join obligation begins in `CatchUp`.
    async fn join_group() -> (FakeRelay, Member, Member, GroupId) {
        let relay = FakeRelay::new();
        let mut a = open_member().await;
        let mut b = open_member().await;
        publish_identity(&mut a, &relay, RELAY).await;
        publish_identity(&mut b, &relay, RELAY).await;
        let gid = group_with(&mut b, &relay, RELAY, &[&a], "d4").await;
        join_via_welcome(&mut a, &relay, RELAY).await;
        let obligations = obligations(&a, &gid);
        assert_eq!(obligations.len(), 1, "one post-join obligation");
        assert_eq!(
            obligations[0].trigger,
            MaintenanceTrigger::PostJoin,
            "trigger"
        );
        assert_eq!(obligations[0].phase, MaintenancePhase::CatchUp, "phase");
        (relay, a, b, gid)
    }

    /// Advance `member` to just past `obligation.not_before` and tick.
    async fn arrive_not_before(member: &mut Member, group: &GroupId) {
        let not_before = obligations(member, group)
            .into_iter()
            .find_map(|obligation| obligation.not_before)
            .expect("a persisted not_before");
        let now = member.clock.now().unix_ms / 1_000;
        assert!(not_before.0 > now, "not_before is in the future");
        member.clock.advance((not_before.0 - now + 1) * 1_000);
        member.session.tick().await.expect("tick past not_before");
    }

    #[tokio::test]
    async fn post_join_self_update_completes() {
        let (relay, mut a, mut b, gid) = Box::pin(join_group()).await;
        let now_ms = a.clock.now().unix_ms;

        // history_fetched → Quiet.
        a.session.history_fetched(&gid).expect("history_fetched");
        let obligation = &obligations(&a, &gid)[0];
        assert_eq!(obligation.phase, MaintenancePhase::Quiet);
        assert_eq!(obligation.quiet_since.unwrap().0, now_ms / 1_000);

        // +59 s: nothing.
        a.clock.advance(59_000);
        a.session.tick().await.expect("tick +59s");
        assert!(take(&mut a).is_empty(), "no outbound inside the window");
        assert_eq!(obligations(&a, &gid)[0].phase, MaintenancePhase::Quiet);

        // +60 s: Jitter with a persisted not_before.
        a.clock.advance(1_000);
        a.session.tick().await.expect("tick +60s");
        let now_obligation = &obligations(&a, &gid)[0];
        assert_eq!(
            now_obligation.phase,
            MaintenancePhase::Jitter,
            "jitter phase"
        );
        assert!(now_obligation.not_before.is_some(), "not_before persisted");

        // Past not_before: exactly one self-update Commit is staged.
        arrive_not_before(&mut a, &gid).await;
        let outbound = take(&mut a);
        let commits: Vec<_> = outbound
            .iter()
            .filter(|item| item.purpose == OutboundPurpose::Commit)
            .collect();
        assert_eq!(commits.len(), 1, "exactly one self-update commit");
        assert_eq!(
            obligations(&a, &gid)[0].phase,
            MaintenancePhase::PendingPublication
        );

        // Publish + ack → the leaf rotates → SelfUpdated + Complete.
        accept(&mut a, &relay, commits[0]).await;
        let events = a.session.drain_events().expect("drain");
        a.session.tick().await.expect("settle tick");
        let later = a.session.drain_events().expect("drain");
        assert!(
            self_updated(&events, &gid) || self_updated(&later, &gid),
            "SelfUpdated emitted once"
        );
        assert_eq!(obligations(&a, &gid)[0].phase, MaintenancePhase::Complete);

        // Group state: last_own_leaf_rotation_at set; next periodic ∈ [24,36] d.
        let state = maintenance_of(&a, &gid).expect("maintenance state");
        assert_eq!(
            state.last_own_leaf_rotation_at.unwrap().0,
            a.clock.now().unix_ms / 1_000
        );
        let next = state
            .next_periodic_rotation_at
            .expect("periodic rotation enrolled");
        let delta = next.0 - a.clock.now().unix_ms / 1_000;
        assert!(
            (24 * 86_400..=36 * 86_400).contains(&delta),
            "next periodic {delta}s out, expected 24–36 days"
        );
        b.session.tick().await.expect("b tick");
    }

    #[tokio::test]
    async fn eose_deadline_expiry_starts_quiet() {
        let (_relay, mut a, mut _b, gid) = Box::pin(join_group()).await;

        // No history_fetched: CatchUp holds for 300 s, recording
        // DeadlineExpired and entering Quiet.
        a.clock.advance(299_000);
        a.session.tick().await.expect("tick at 299s");
        assert_eq!(obligations(&a, &gid)[0].phase, MaintenancePhase::CatchUp);
        a.clock.advance(2_000);
        a.session.tick().await.expect("tick past 300s");
        let obligation = &obligations(&a, &gid)[0];
        assert_eq!(obligation.phase, MaintenancePhase::Quiet);
        let fetch = maintenance_of(&a, &gid)
            .and_then(|state| state.post_join_history)
            .expect("post-join fetch");
        assert_eq!(
            fetch.completion.expect("completion").outcome,
            PostJoinHistoryFetchOutcome::DeadlineExpired
        );

        // Quiet + jitter then the same self-update.
        a.clock.advance(60_000);
        a.session.tick().await.expect("tick past quiet");
        assert_eq!(obligations(&a, &gid)[0].phase, MaintenancePhase::Jitter);
        arrive_not_before(&mut a, &gid).await;
        let commits = take(&mut a)
            .into_iter()
            .filter(|item| item.purpose == OutboundPurpose::Commit)
            .count();
        assert_eq!(commits, 1, "one self-update after the deadline path");
    }

    #[tokio::test]
    async fn peer_commit_resets_quiet() {
        let (relay, mut a, mut b, gid) = Box::pin(join_group()).await;
        a.session.history_fetched(&gid).expect("history_fetched");
        a.clock.advance(30_000);

        // The inviter's own rotation lands during the quiet window.
        b.session.self_update(&gid).await.expect("b self_update");
        flush(&mut b, &relay).await;
        let route = a.route_hex(&gid);
        ingest_backlog(&mut a, &group_events(&relay, RELAY, &route)).await;

        // The window restarted: +59 s nothing, +60 s Jitter.
        a.clock.advance(59_000);
        a.session.tick().await.expect("tick +59s");
        assert_eq!(obligations(&a, &gid)[0].phase, MaintenancePhase::Quiet);
        assert!(
            take(&mut a)
                .iter()
                .all(|item| item.purpose != OutboundPurpose::Commit)
        );
        a.clock.advance(1_000);
        a.session.tick().await.expect("tick +60s");
        assert_eq!(obligations(&a, &gid)[0].phase, MaintenancePhase::Jitter);
    }

    #[tokio::test]
    async fn overdue_target_skips_waits() {
        let (_relay, mut a, mut _b, gid) = Box::pin(join_group()).await;

        // 24 h + 1 s without a fetch: the operational target skips every
        // wait and the self-update stages immediately through the gate.
        a.clock.advance(24 * 3_600_000 + 1_000);
        a.session.tick().await.expect("tick past target");
        let outbound = take(&mut a);
        let commits = outbound
            .iter()
            .filter(|item| item.purpose == OutboundPurpose::Commit)
            .count();
        assert_eq!(commits, 1, "overdue self-update staged at once");
        let obligation = &obligations(&a, &gid)[0];
        assert!(obligation.overdue);
        assert_eq!(obligation.phase, MaintenancePhase::PendingPublication);
    }

    #[tokio::test]
    async fn app_sends_not_blocked_by_obligation() {
        let (_relay, mut a, mut _b, gid) = Box::pin(join_group()).await;
        // Still in CatchUp — a send must work regardless.
        a.session
            .send_message(
                &gid,
                mk_core::AppMessage {
                    kind: 9,
                    tags: Vec::new(),
                    content: "during catchup".to_owned(),
                },
            )
            .await
            .expect("send during CatchUp");
        a.session.history_fetched(&gid).expect("history_fetched");
        a.session
            .send_message(
                &gid,
                mk_core::AppMessage {
                    kind: 9,
                    tags: Vec::new(),
                    content: "during quiet".to_owned(),
                },
            )
            .await
            .expect("send during Quiet");
        let outbound = take(&mut a);
        assert_eq!(outbound.len(), 2, "both application sends are owed");
    }

    #[tokio::test]
    async fn gate_holds_while_own_commit_in_flight() {
        let (_relay, mut a, mut _b, gid) = Box::pin(join_group()).await;
        a.session.history_fetched(&gid).expect("history_fetched");
        a.clock.advance(61_000);
        a.session.tick().await.expect("tick to jitter");
        arrive_not_before(&mut a, &gid).await;
        let commit = take(&mut a)
            .into_iter()
            .find(|item| item.purpose == OutboundPurpose::Commit)
            .expect("self-update commit staged");

        // The commit is still in flight: the next pass holds the gate —
        // no second self-update is authored.
        a.session.tick().await.expect("tick while in flight");
        assert!(
            take(&mut a).is_empty(),
            "no second commit while the first is unconfirmed"
        );
        assert_eq!(
            obligations(&a, &gid)[0].phase,
            MaintenancePhase::PendingPublication
        );
        drop(commit);
    }

    #[tokio::test]
    async fn history_fetched_errors_and_idempotent() {
        let (_relay, mut a, mut b, gid) = Box::pin(join_group()).await;
        let unknown = GroupId(vec![9u8; 16]);
        let error = a
            .session
            .history_fetched(&unknown)
            .expect_err("unknown group");
        assert!(matches!(error, SessionError::UnknownGroup), "got {error:?}");

        a.session.history_fetched(&gid).expect("first call");
        a.session.history_fetched(&gid).expect("idempotent second");

        // A created group owes no fetch: the call is a no-op.
        b.session
            .history_fetched(&gid)
            .expect("created group no-op");
        assert!(obligations(&b, &gid).is_empty(), "no obligation minted");
    }

    #[tokio::test]
    async fn created_group_gets_only_periodic() {
        let relay = FakeRelay::new();
        let mut b = open_member().await;
        publish_identity(&mut b, &relay, RELAY).await;
        let gid = group_with(&mut b, &relay, RELAY, &[], "solo").await;

        // Enrolment samples a periodic deadline in [24,36] d of enrolment;
        // no post-join obligation exists.
        b.session.tick().await.expect("tick");
        let state = maintenance_of(&b, &gid).expect("maintenance state");
        assert!(state.periodic_enrolled);
        let next = state.next_periodic_rotation_at.expect("periodic armed");
        let baseline = state
            .last_own_leaf_rotation_at
            .expect("creation counts as rotation");
        let delta = next.0 - baseline.0;
        assert!(
            (24 * 86_400..=36 * 86_400).contains(&delta),
            "periodic interval 24-36d"
        );
        assert!(obligations(&b, &gid).is_empty(), "peer has no obligation");

        // Reaching the deadline creates a Quiet periodic obligation — it
        // still runs its own quiet window plus the sampled jitter.
        b.clock
            .advance((next.0 - b.clock.now().unix_ms / 1_000 + 1) * 1_000);
        b.session.tick().await.expect("tick at periodic");
        let obligation = &obligations(&b, &gid)[0];
        assert_eq!(obligation.trigger, MaintenanceTrigger::Periodic);
        assert_eq!(obligation.phase, MaintenancePhase::Quiet);
        b.clock.advance(61_000);
        b.session.tick().await.expect("tick past quiet");
        assert_eq!(obligations(&b, &gid)[0].phase, MaintenancePhase::Jitter);
        arrive_not_before(&mut b, &gid).await;
        let commits = take(&mut b)
            .into_iter()
            .filter(|item| item.purpose == OutboundPurpose::Commit)
            .count();
        assert_eq!(commits, 1, "one periodic self-update commit");
        assert_eq!(
            obligations(&b, &gid)[0].phase,
            MaintenancePhase::PendingPublication
        );
    }

    #[tokio::test]
    async fn self_update_is_not_paced() {
        let (_relay, mut a, mut _b, gid) = Box::pin(join_group()).await;
        a.session.history_fetched(&gid).expect("history_fetched");
        a.clock.advance(61_000);
        a.session.tick().await.expect("tick to jitter");
        let not_before = obligations(&a, &gid)[0].not_before.unwrap();
        a.clock
            .advance((not_before.0 - a.clock.now().unix_ms / 1_000 + 1) * 1_000);

        // Fifty app sends in the obligation's due second: the self-update
        // commit still stages without consuming a pacing slot, and the
        // fifty-first send's created_at moves to the next second — paced
        // exactly as before.
        let second = a.clock.now().unix_ms / 1_000;
        let mut last_created = 0;
        for index in 0..50 {
            let sent = a
                .session
                .send_message(
                    &gid,
                    mk_core::AppMessage {
                        kind: 9,
                        tags: Vec::new(),
                        content: format!("m{index}"),
                    },
                )
                .await
                .expect("send");
            last_created = sent.message.created_at;
        }
        assert_eq!(last_created, second, "all fifty share the second");
        a.session.tick().await.expect("tick at due");
        let outbound = take(&mut a);
        let commits = outbound
            .iter()
            .filter(|item| item.purpose == OutboundPurpose::Commit)
            .count();
        assert_eq!(commits, 1, "the self-update was not paced");
        let fifty_first = a
            .session
            .send_message(
                &gid,
                mk_core::AppMessage {
                    kind: 9,
                    tags: Vec::new(),
                    content: "m51".to_owned(),
                },
            )
            .await
            .expect("fifty-first send");
        assert_eq!(
            fifty_first.message.created_at,
            second + 1,
            "the next app send is paced into the following second"
        );
    }

    #[tokio::test]
    async fn messages_converge_across_self_update_window() {
        // mdk#3584: the inviter keeps messaging before, during and after
        // the joiner's self-update; both sides converge.
        let (relay, mut a, mut b, gid) = Box::pin(join_group()).await;
        let route = a.route_hex(&gid);
        a.session.history_fetched(&gid).expect("history_fetched");

        // Before the window.
        send_chat(&mut b, &relay, &gid, "before-1").await;
        send_chat(&mut a, &relay, &gid, "a-before").await;
        ingest_backlog(&mut a, &group_events(&relay, RELAY, &route)).await;
        ingest_backlog(&mut b, &group_events(&relay, RELAY, &route)).await;
        send_chat(&mut b, &relay, &gid, "before-2").await;

        // Advance into the self-update.
        a.clock.advance(61_000);
        a.session.tick().await.expect("tick to jitter");
        arrive_not_before(&mut a, &gid).await;
        let outbound = take(&mut a);
        let commit = outbound
            .iter()
            .find(|item| item.purpose == OutboundPurpose::Commit)
            .cloned()
            .expect("self-update commit");

        // During the window: the commit is on the wire, unconfirmed.
        send_chat(&mut b, &relay, &gid, "during-1").await;
        accept(&mut a, &relay, &commit).await;
        send_chat(&mut b, &relay, &gid, "during-2").await;
        ingest_backlog(&mut a, &group_events(&relay, RELAY, &route)).await;

        // After: everything lands both ways and epochs agree.
        send_chat(&mut b, &relay, &gid, "after-1").await;
        send_chat(&mut a, &relay, &gid, "a-after").await;
        ingest_backlog(&mut a, &group_events(&relay, RELAY, &route)).await;
        ingest_backlog(&mut b, &group_events(&relay, RELAY, &route)).await;
        a.session.tick().await.expect("a settle");
        flush(&mut a, &relay).await;
        ingest_backlog(&mut b, &group_events(&relay, RELAY, &route)).await;
        b.session.tick().await.expect("b settle");
        flush(&mut b, &relay).await;
        ingest_backlog(&mut a, &group_events(&relay, RELAY, &route)).await;

        let epoch_a = a.session.group(&gid).expect("group").expect("exists").epoch;
        let epoch_b = b.session.group(&gid).expect("group").expect("exists").epoch;
        assert_eq!(epoch_a, epoch_b, "epochs converge");
    }
}
