//! Queued-intent regeneration across restarts and publish failures: a
//! send `Queued` behind a pending own commit regenerates on a later
//! convergence drain into a durable artifact, and that artifact must
//! resolve the durable intent exactly once — the same logical message is
//! never staged twice, never published twice. Requires the `testing`
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
    clippy::shadow_unrelated,
    clippy::large_futures,
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
mod queued_intent_tests {
    use super::support::*;

    use mk_core::{
        AppMessage, GroupId, Outbound, OutboundPurpose, PublishResolution, RelayResult,
        RelayStatus, SendOutcome,
    };

    const RELAY: &str = "ws://127.0.0.1:7777";

    /// A creates a group with B (B is a founding admin when `b_admin`),
    /// B joins, pins its post-join self-update jitter to 0 (the engine
    /// samples it from OS entropy at join; pinning makes the schedule
    /// deterministic), reports the history fetch, and two clock
    /// advances + ticks walk the obligation Quiet→Jitter→sent. Returns
    /// the staged, durable, unpublished self-update commit.
    async fn staged_self_update(b_admin: bool) -> (FakeRelay, Member, Member, GroupId, Outbound) {
        let relay = FakeRelay::new();
        let mut a = open_member().await;
        let mut b = open_member().await;
        publish_identity(&mut a, &relay, RELAY).await;
        publish_identity(&mut b, &relay, RELAY).await;
        let admins = if b_admin {
            vec![b.signer.account()]
        } else {
            Vec::new()
        };
        let gid = group_as(
            &mut a,
            &relay,
            RELAY,
            &[&b],
            "queued-intent",
            &admins,
            Vec::new(),
        )
        .await;
        join_via_welcome(&mut b, &relay, RELAY).await;
        b.session
            .set_maintenance_jitter(&gid, 0)
            .expect("pin jitter");
        b.session.history_fetched(&gid).expect("history_fetched");
        // Quiet (60 s) → Jitter; jitter 0 → due on the next pass.
        b.clock.advance(61_000);
        b.session.tick().await.expect("tick into Jitter");
        b.clock.advance(1_000);
        b.session.tick().await.expect("tick past not_before");
        if let Some(seq) = b.session.awaiting_durability() {
            b.session.acknowledge_durable(seq);
        }
        let staged = b.session.take_outbound().expect("take_outbound");
        assert_eq!(
            staged.len(),
            1,
            "exactly the self-update commit is staged: {staged:?}"
        );
        assert_eq!(
            staged[0].purpose,
            OutboundPurpose::Commit,
            "the staged artifact is the self-update commit"
        );
        let commit = staged.into_iter().next().expect("one staged item");
        (relay, a, b, gid, commit)
    }

    /// One chat that must queue behind B's pending self-update commit.
    async fn send_queued(b: &mut Member, gid: &GroupId, content: &str) {
        b.clock.advance(1_000);
        let sent = b
            .session
            .send_message(
                gid,
                AppMessage {
                    kind: 9,
                    tags: Vec::new(),
                    content: content.to_owned(),
                },
            )
            .await
            .expect("send_message");
        assert_eq!(
            sent.outcome,
            SendOutcome::Queued,
            "the send queues behind the pending commit"
        );
    }

    /// Publish + report the staged commit; it must confirm.
    async fn confirm_commit(b: &mut Member, relay: &FakeRelay, commit: &Outbound) {
        let resolution = b
            .session
            .report_publish(&commit.id, &relay.publish(commit))
            .await
            .expect("report commit");
        assert_eq!(
            resolution,
            PublishResolution::Confirmed,
            "the self-update commit confirms: {resolution:?}"
        );
    }

    /// Items carrying an application message.
    fn app_items(items: &[Outbound]) -> Vec<Outbound> {
        items
            .iter()
            .filter(|item| item.purpose == OutboundPurpose::ApplicationMessage)
            .cloned()
            .collect()
    }

    /// One tick, acknowledge the durability gate it staged, take the
    /// released outbound.
    async fn tick_take(b: &mut Member) -> Vec<Outbound> {
        b.session.tick().await.expect("tick");
        if let Some(seq) = b.session.awaiting_durability() {
            b.session.acknowledge_durable(seq);
        }
        b.session.take_outbound().expect("take_outbound")
    }

    /// Publish each item and report its verdicts; every report must
    /// resolve without error.
    async fn publish_report(b: &mut Member, relay: &FakeRelay, items: &[Outbound]) {
        for item in items {
            b.session
                .report_publish(&item.id, &relay.publish(item))
                .await
                .expect("report_publish");
        }
    }

    /// A syncs the group and must surface every `contents` message exactly
    /// once; the relay holds exactly `b_445s` kind-445 events under the
    /// group's route (commits plus one app event per logical message).
    async fn check_delivered_once(
        a: &mut Member,
        b: &Member,
        relay: &FakeRelay,
        gid: &GroupId,
        contents: &[&str],
        b_445s: usize,
    ) {
        let events = sync_member(a, relay, RELAY, gid).await;
        let got = received(&events);
        for content in contents {
            assert_eq!(
                got.iter().filter(|seen| seen.as_str() == *content).count(),
                1,
                "A receives {content:?} exactly once: {got:?}"
            );
        }
        // Kind-445 group events carry ephemeral sender keys, so they
        // cannot be attributed by pubkey: the group's `#h` route selects
        // them and `expected_445s` counts commits + one app event per
        // logical message.
        let authored = group_events(relay, RELAY, &b.route_hex(gid));
        assert_eq!(
            authored.len(),
            b_445s,
            "kind-445 events on the relay (commits + one app event per message)"
        );
    }

    /// The regenerated artifact is durable before the restart: reopen
    /// re-offers exactly that artifact, never a second regeneration.
    #[tokio::test(flavor = "current_thread")]
    async fn regenerated_intent_survives_restart_once() {
        let (relay, mut a, mut b, gid, commit) = staged_self_update(false).await;
        send_queued(&mut b, &gid, "one chat").await;
        confirm_commit(&mut b, &relay, &commit).await;

        // The drain regenerates the queued intent; the artifact, with the
        // intent named on its fanout, is durable before the host ever
        // publishes.
        let staged = tick_take(&mut b).await;
        let staged_apps = app_items(&staged);
        assert_eq!(
            staged_apps.len(),
            1,
            "the drain regenerates the queued intent once: {staged:?}"
        );

        let mut b = reopen_member(&b).await;
        let released = tick_take(&mut b).await;
        let apps = app_items(&released);
        assert_eq!(
            apps.len(),
            1,
            "restart re-offers only the durable artifact: {apps:?}"
        );
        assert_eq!(
            apps[0].event.id, staged_apps[0].event.id,
            "the same frozen artifact is re-offered"
        );
        publish_report(&mut b, &relay, &apps).await;

        let again = tick_take(&mut b).await;
        assert!(
            app_items(&again).is_empty(),
            "no second regeneration after resolve: {again:?}"
        );

        check_delivered_once(&mut a, &b, &relay, &gid, &["one chat"], 2).await;
    }

    /// The regeneration batch is lost before the host persists it: the
    /// reopened prefix still holds the queued intent, and exactly one
    /// artifact is ever regenerated and published.
    #[tokio::test(flavor = "current_thread")]
    async fn regenerated_intent_crash_before_durable() {
        let (relay, mut a, mut b, gid, commit) = staged_self_update(false).await;
        send_queued(&mut b, &gid, "lost-tail").await;
        confirm_commit(&mut b, &relay, &commit).await;

        // The drain's regeneration lands in batches the host never
        // persisted; the reopened prefix still owes the intent.
        let n = b.host.batches().len();
        b.session.tick().await.expect("tick");
        assert!(
            b.host.batches().len() > n,
            "the regeneration committed a batch"
        );
        let mut b = reopen_member_from_rows(&b, b.host.rows_after_batches(n)).await;

        let released = tick_take(&mut b).await;
        let apps = app_items(&released);
        assert_eq!(
            apps.len(),
            1,
            "the still-queued intent regenerates exactly once: {apps:?}"
        );
        publish_report(&mut b, &relay, &apps).await;

        let again = tick_take(&mut b).await;
        assert!(
            app_items(&again).is_empty(),
            "no second regeneration: {again:?}"
        );

        check_delivered_once(&mut a, &b, &relay, &gid, &["lost-tail"], 2).await;
    }

    /// Published but never reported: reopen re-offers the identical
    /// artifact — the in-flight intent stays the host's obligation.
    #[tokio::test(flavor = "current_thread")]
    async fn regenerated_intent_crash_after_publish_before_report() {
        let (relay, mut a, mut b, gid, commit) = staged_self_update(false).await;
        send_queued(&mut b, &gid, "published-not-reported").await;
        confirm_commit(&mut b, &relay, &commit).await;

        let staged = tick_take(&mut b).await;
        let apps = app_items(&staged);
        assert_eq!(apps.len(), 1, "one regenerated artifact: {staged:?}");
        let first = apps[0].clone();
        relay.publish(&first); // stored, but the report never reaches the session

        let mut b = reopen_member(&b).await;
        let released = tick_take(&mut b).await;
        let apps = app_items(&released);
        assert_eq!(
            apps.iter().map(|item| item.event.id).collect::<Vec<_>>(),
            vec![first.event.id],
            "reopen re-offers the identical artifact and nothing else: {apps:?}"
        );
        publish_report(&mut b, &relay, &apps).await;

        check_delivered_once(&mut a, &b, &relay, &gid, &["published-not-reported"], 2).await;
    }

    /// Two queued sends regenerate into two durable artifacts; a restart
    /// re-offers exactly those two, each intent resolving once.
    #[tokio::test(flavor = "current_thread")]
    async fn two_regenerated_intents_across_restart() {
        let (relay, mut a, mut b, gid, commit) = staged_self_update(false).await;
        send_queued(&mut b, &gid, "first").await;
        send_queued(&mut b, &gid, "second").await;
        confirm_commit(&mut b, &relay, &commit).await;

        let mut staged = Vec::new();
        for _ in 0..4 {
            staged.extend(app_items(&tick_take(&mut b).await));
            if staged.len() == 2 {
                break;
            }
        }
        assert_eq!(
            staged.len(),
            2,
            "both queued intents regenerate: {staged:?}"
        );

        let mut b = reopen_member(&b).await;
        let mut released = Vec::new();
        for _ in 0..4 {
            released.extend(app_items(&tick_take(&mut b).await));
        }
        assert_eq!(
            released.len(),
            2,
            "restart re-offers exactly the two durable artifacts: {released:?}"
        );
        let mut ids: Vec<_> = released.iter().map(|item| item.event.id).collect();
        ids.sort();
        ids.dedup();
        assert_eq!(ids.len(), 2, "distinct artifacts");
        publish_report(&mut b, &relay, &released).await;

        let again = tick_take(&mut b).await;
        assert!(
            app_items(&again).is_empty(),
            "no further regeneration: {again:?}"
        );

        check_delivered_once(&mut a, &b, &relay, &gid, &["first", "second"], 3).await;
    }

    /// An all-retryable report withholds the item under backoff while the
    /// durable fanout still names its intent — no distinct regeneration
    /// may appear; past the backoff the identical artifact is re-offered.
    #[tokio::test(flavor = "current_thread")]
    async fn retryable_failure_does_not_regenerate() {
        let (relay, mut a, mut b, gid, commit) = staged_self_update(false).await;
        send_queued(&mut b, &gid, "retryable").await;
        confirm_commit(&mut b, &relay, &commit).await;

        let staged = tick_take(&mut b).await;
        let apps = app_items(&staged);
        assert_eq!(apps.len(), 1, "one regenerated artifact: {staged:?}");
        let first = apps[0].clone();

        // Every relay unreachable: retryable, no acceptance — the intent
        // stays the host's obligation under backoff.
        let unavailable: Vec<RelayResult> = first
            .relays
            .iter()
            .map(|relay| RelayResult {
                relay: relay.clone(),
                status: RelayStatus::Unavailable,
            })
            .collect();
        b.session
            .report_publish(&first.id, &unavailable)
            .await
            .expect("report_publish");

        // Withheld by backoff — and no fresh artifact may appear.
        let withheld = tick_take(&mut b).await;
        let regenerated = app_items(&withheld);
        assert!(
            regenerated.is_empty(),
            "no regeneration while the fanout is outstanding: {regenerated:?}"
        );

        // Past the retry backoff the same artifact is re-offered.
        b.clock.advance(2_000);
        let reoffered = tick_take(&mut b).await;
        let apps = app_items(&reoffered);
        assert_eq!(
            apps.iter().map(|item| item.event.id).collect::<Vec<_>>(),
            vec![first.event.id],
            "backoff re-offers the identical artifact: {apps:?}"
        );
        publish_report(&mut b, &relay, &apps).await;

        check_delivered_once(&mut a, &b, &relay, &gid, &["retryable"], 2).await;
    }

    /// A group change queued behind the earlier app intent drains to a
    /// pending commit; a restart must not mint a second commit for the
    /// same change.
    #[tokio::test(flavor = "current_thread")]
    async fn drained_commit_intent_survives_restart() {
        let (relay, mut a, mut b, gid, commit) = staged_self_update(true).await;
        send_queued(&mut b, &gid, "behind").await;
        confirm_commit(&mut b, &relay, &commit).await;

        // The app intent still waits for the drain: the group change
        // queues behind it.
        let outcome = b
            .session
            .set_retention(&gid, 60)
            .await
            .expect("set_retention");
        assert_eq!(
            outcome,
            SendOutcome::Queued,
            "the group change queues behind the earlier intent: {outcome:?}"
        );

        // The drain regenerates the app artifact and the change's commit.
        let mut apps = Vec::new();
        let mut commits = Vec::new();
        for _ in 0..4 {
            for item in tick_take(&mut b).await {
                match item.purpose {
                    OutboundPurpose::ApplicationMessage => apps.push(item),
                    OutboundPurpose::Commit => commits.push(item),
                    _ => {}
                }
            }
            if !apps.is_empty() && !commits.is_empty() {
                break;
            }
        }
        assert_eq!(apps.len(), 1, "the queued send regenerated once: {apps:?}");
        assert_eq!(
            commits.len(),
            1,
            "the queued change produced one commit: {commits:?}"
        );

        let mut b = reopen_member(&b).await;
        let mut reapps = Vec::new();
        let mut recommits = Vec::new();
        for _ in 0..4 {
            for item in tick_take(&mut b).await {
                match item.purpose {
                    OutboundPurpose::ApplicationMessage => reapps.push(item),
                    OutboundPurpose::Commit => recommits.push(item),
                    _ => {}
                }
            }
        }
        assert!(
            reapps.len() <= 1,
            "the app artifact is re-offered at most once: {reapps:?}"
        );
        assert!(
            recommits.len() <= 1,
            "at most one commit carries the change across restart: {recommits:?}"
        );
        let both: Vec<Outbound> = reapps.into_iter().chain(recommits).collect();
        publish_report(&mut b, &relay, &both).await;

        let again = tick_take(&mut b).await;
        assert!(
            again
                .iter()
                .all(|item| item.purpose != OutboundPurpose::Commit),
            "no second commit for the same change: {again:?}"
        );

        check_delivered_once(&mut a, &b, &relay, &gid, &["behind"], 3).await;
    }
}
