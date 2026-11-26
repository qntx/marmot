//! Session flow tests driving the public `Session` API end to end against
//! the in-process `FakeRelay`, `RecordingHost`, `ManualClock` and
//! `LocalSigner` — no sockets, no real time. Requires the `testing`
//! feature for the test helpers.

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
mod flow_tests {
    use super::support::*;
    use std::sync::Arc;

    use mk_core::{
        AccountSigner, Availability, Clock, CreateGroup, GroupAccess, GroupId, InputCategory,
        InputOutcome, Invitee, Outbound, PublishResolution, RelayResult, RelayStatus, SendOutcome,
        SessionError, SessionEvent, UnsignedEvent,
    };

    const RELAY: &str = "ws://127.0.0.1:7777";
    const RELAY2: &str = "ws://127.0.0.1:7778";

    /// Create a group between `creator` and every `members` account whose
    /// `KeyPackage` + 10050 were already published, then deliver the Welcomes.
    async fn group_with(
        creator: &mut Member,
        relay: &FakeRelay,
        invitees: &[&Member],
        name: &str,
    ) -> GroupId {
        group_as(creator, relay, invitees, name, &[]).await
    }

    /// [`group_with`] plus extra founding admins (account ids).
    async fn group_as(
        creator: &mut Member,
        relay: &FakeRelay,
        invitees: &[&Member],
        name: &str,
        admins: &[mk_core::AccountId],
    ) -> GroupId {
        let invites = invitees
            .iter()
            .map(|member| Invitee {
                key_package: key_package_of(relay, RELAY, &member.account_hex()),
                inbox_relays: inbox_relays_of(relay, RELAY, &member.account_hex()),
            })
            .collect();
        let gid = creator
            .session
            .create_group(CreateGroup {
                name: name.to_owned(),
                description: String::new(),
                relays: vec![RELAY.to_owned()],
                invitees: invites,
                own_devices: Vec::new(),
                admins: admins.to_vec(),
                components: Vec::new(),
            })
            .await
            .expect("create_group");
        flush(creator, relay).await;
        gid
    }

    /// Let `member` pick up any pending Welcome and join.
    async fn join_via_welcome(member: &mut Member, relay: &FakeRelay) {
        let welcomes = welcomes_for(relay, RELAY, &member.account_hex());
        assert!(!welcomes.is_empty(), "a welcome must be waiting");
        ingest_backlog(member, &welcomes).await;
        member.session.tick().await.expect("tick");
        flush(member, relay).await;
    }

    fn snapshot(member: &Member, gid: &GroupId) -> mk_core::GroupSnapshot {
        member
            .session
            .group(gid)
            .expect("group")
            .expect("group exists")
    }

    /// Exact member account set of a snapshot.
    fn member_accounts(member: &Member, gid: &GroupId) -> Vec<mk_core::AccountId> {
        let mut accounts: Vec<_> = snapshot(member, gid)
            .members
            .iter()
            .map(|m| m.account)
            .collect();
        accounts.sort();
        accounts
    }

    /// Exact admin account set of a snapshot.
    fn admin_accounts(member: &Member, gid: &GroupId) -> Vec<mk_core::AccountId> {
        let mut accounts: Vec<_> = snapshot(member, gid)
            .live
            .map(|live| live.admins)
            .unwrap_or_default();
        accounts.sort();
        accounts
    }

    /// Transport ids of `MessageReceived` events.
    fn received_ids(events: &[SessionEvent]) -> Vec<mk_core::EventId> {
        events
            .iter()
            .filter_map(|event| match event {
                SessionEvent::MessageReceived { message, .. } => Some(message.transport_id),
                _ => None,
            })
            .collect()
    }

    /// Publish the staged outbound that carries `purpose`, returning its
    /// `report_publish` resolution.
    async fn publish_one(
        member: &mut Member,
        relay: &FakeRelay,
        purpose: mk_core::OutboundPurpose,
    ) -> PublishResolution {
        if let Some(seq) = member.session.awaiting_durability() {
            member.session.acknowledge_durable(seq);
        }
        let item = member
            .session
            .take_outbound()
            .expect("take_outbound")
            .into_iter()
            .find(|item| item.purpose == purpose)
            .expect("staged outbound of that purpose");
        member
            .session
            .report_publish(&item.id, &relay.publish(&item))
            .await
            .expect("report_publish")
    }

    /// Invite `new_member` into `gid`, deliver its Welcome and let it join.
    async fn add_member(
        admin: &mut Member,
        new_member: &mut Member,
        relay: &FakeRelay,
        gid: &GroupId,
    ) {
        admin
            .session
            .invite(
                gid,
                vec![Invitee {
                    key_package: key_package_of(relay, RELAY, &new_member.account_hex()),
                    inbox_relays: inbox_relays_of(relay, RELAY, &new_member.account_hex()),
                }],
            )
            .await
            .expect("invite");
        flush(admin, relay).await;
        join_via_welcome(new_member, relay).await;
    }

    /// Flow 1: KeyPackages + 10050 for A,B,C; A creates with B; B joins via
    /// Welcome; 500 messages; B reads the relay's newest-first backlog with a
    /// single `ingest_batch` per half — restarting between halves — and still
    /// receives every message exactly once; A adds C; all three exchange;
    /// A removes B; B observes removal and cannot read the next message.
    #[tokio::test(flavor = "current_thread")]
    async fn create_join_message_and_restart() {
        let relay = FakeRelay::new();
        let mut a = open_member().await;
        let mut b = open_member().await;
        let mut c = open_member().await;
        publish_identity(&mut a, &relay, RELAY).await;
        publish_identity(&mut b, &relay, RELAY).await;
        publish_identity(&mut c, &relay, RELAY).await;

        let gid = group_with(&mut a, &relay, &[&b], "m2-flow").await;
        join_via_welcome(&mut b, &relay).await;

        sync_member(&mut b, &relay, RELAY, &gid).await;
        sync_member(&mut a, &relay, RELAY, &gid).await;
        assert_eq!(snapshot(&a, &gid).members.len(), 2);
        assert_eq!(snapshot(&b, &gid).members.len(), 2);
        assert_eq!(snapshot(&a, &gid).epoch, snapshot(&b, &gid).epoch);

        // Wave 1: B reads A's 250 messages from the newest-first backlog
        // with a single `ingest_batch` (the batch sorts ascending), plus
        // at most one tick.
        for i in 0..250 {
            send_chat(&mut a, &relay, &gid, &format!("a1-{i:04}")).await;
        }
        let route = b.route_hex(&gid);
        let first = ingest_backlog(&mut b, &group_events(&relay, RELAY, &route)).await;
        b.session.tick().await.expect("tick");
        let extra = b.session.drain_events().unwrap();
        let extra_ids = received_ids(&extra);
        if !extra_ids.is_empty() {
            b.session.acknowledge_messages(&extra_ids).expect("ack");
        }
        let wave1 = received(&first)
            .into_iter()
            .chain(received(&extra))
            .filter(|m| m.starts_with("a1-"))
            .count();
        assert_eq!(
            wave1, 250,
            "one ingest_batch over the sorted batch delivers all 250"
        );

        // Wave 2: B sends 250, A sends 250 more; B restarts mid-ingest —
        // the first half of the sorted (ascending) backlog before the
        // crash, the rest after.
        for i in 0..250 {
            send_chat(&mut b, &relay, &gid, &format!("b-{i:04}")).await;
        }
        for i in 0..250 {
            send_chat(&mut a, &relay, &gid, &format!("a2-{i:04}")).await;
        }
        let mut sorted = group_events(&relay, RELAY, &route);
        sorted.sort_by_key(|event| event.created_at);
        let half = sorted.len() / 2;

        let mut delivered_ids = Vec::new();
        let mut seen = Vec::new();
        // Incarnation 1: one ingest_batch over the oldest half.
        let oldest = ingest_backlog(&mut b, &sorted[..half]).await;
        seen.extend(received(&oldest));
        delivered_ids.extend(received_ids(&oldest));
        // Crash mid-ingest: reopen from durable rows only.
        let mut b = reopen_member(&b).await;
        // Incarnation 2: one ingest_batch over the remaining half, then at
        // most one tick to settle whatever the restart deferred.
        let second = ingest_backlog(&mut b, &sorted[half..]).await;
        seen.extend(received(&second));
        delivered_ids.extend(received_ids(&second));
        b.session.tick().await.expect("tick");
        let tail_drained = b.session.drain_events().unwrap();
        let tail_ids = received_ids(&tail_drained);
        if !tail_ids.is_empty() {
            b.session.acknowledge_messages(&tail_ids).expect("ack");
        }
        seen.extend(received(&tail_drained));
        delivered_ids.extend(tail_ids);

        let a_got = seen.iter().filter(|m| m.starts_with("a2-")).count();
        assert_eq!(
            a_got, 250,
            "B receives all 250 of A's wave-2 messages across the restart"
        );
        assert!(
            seen.iter().all(|m| m.starts_with("a2-")),
            "only new A messages are delivered; B's own are echoes: {seen:?}"
        );
        let mut unique = delivered_ids.clone();
        unique.sort();
        unique.dedup();
        assert_eq!(
            unique.len(),
            delivered_ids.len(),
            "every message delivered exactly once across incarnations"
        );

        let a_route = a.route_hex(&gid);
        let a_events = ingest_backlog(&mut a, &group_events(&relay, RELAY, &a_route)).await;
        let b_seen = received(&a_events)
            .iter()
            .filter(|m| m.starts_with("b-"))
            .count();
        assert_eq!(b_seen, 250, "A receives all 250 of B's messages");

        // A adds C; all three exchange messages and read each other's.
        add_member(&mut a, &mut c, &relay, &gid).await;
        sync_member(&mut a, &relay, RELAY, &gid).await;
        sync_member(&mut b, &relay, RELAY, &gid).await;
        sync_member(&mut c, &relay, RELAY, &gid).await;
        assert_eq!(member_accounts(&a, &gid).len(), 3);
        assert_eq!(member_accounts(&b, &gid).len(), 3);
        assert_eq!(member_accounts(&c, &gid).len(), 3);

        send_chat(&mut a, &relay, &gid, "from-a").await;
        send_chat(&mut b, &relay, &gid, "from-b").await;
        send_chat(&mut c, &relay, &gid, "from-c").await;
        let drained_a = sync_member(&mut a, &relay, RELAY, &gid).await;
        let drained_b = sync_member(&mut b, &relay, RELAY, &gid).await;
        let drained_c = sync_member(&mut c, &relay, RELAY, &gid).await;
        // Each member reads the other two's messages (own messages are
        // echoes, not deliveries).
        for (who, drained, wants) in [
            ("a", &drained_a, ["from-b", "from-c"]),
            ("b", &drained_b, ["from-a", "from-c"]),
            ("c", &drained_c, ["from-a", "from-b"]),
        ] {
            let got = received(drained);
            for want in wants {
                assert!(
                    got.contains(&want.to_owned()),
                    "{who} reads {want} (got {got:?})"
                );
            }
        }

        // A removes B: B observes the removal and cannot read the next
        // message; A and C can.
        a.session
            .remove_members(&gid, &[b.signer.account()])
            .await
            .expect("remove B");
        flush(&mut a, &relay).await;
        a.session.tick().await.expect("tick");
        flush(&mut a, &relay).await;

        let b_drained = sync_member(&mut b, &relay, RELAY, &gid).await;
        let b_removed = b.session.group(&gid).expect("group").is_none_or(|snap| {
            matches!(
                snap.access,
                GroupAccess::RemovedDevice | GroupAccess::Removed
            )
        }) || b_drained
            .iter()
            .any(|event| matches!(event, SessionEvent::LocalCopyTerminated { .. }));
        assert!(b_removed, "B observes its own removal");
        sync_member(&mut a, &relay, RELAY, &gid).await;
        sync_member(&mut c, &relay, RELAY, &gid).await;
        assert_eq!(
            member_accounts(&a, &gid),
            {
                let mut v = vec![a.signer.account(), c.signer.account()];
                v.sort();
                v
            },
            "A's member set is exactly A and C"
        );
        assert_eq!(
            member_accounts(&c, &gid),
            {
                let mut v = vec![a.signer.account(), c.signer.account()];
                v.sort();
                v
            },
            "C's member set is exactly A and C"
        );

        send_chat(&mut a, &relay, &gid, "post-removal").await;
        let b_after = sync_member(&mut b, &relay, RELAY, &gid).await;
        assert!(
            !received(&b_after).contains(&"post-removal".to_owned()),
            "removed B cannot read the next message"
        );
        let c_route = c.route_hex(&gid);
        let c_after = sync_member(&mut c, &relay, RELAY, &gid).await;
        let c_all = ingest_backlog(&mut c, &group_events(&relay, RELAY, &c_route)).await;
        assert!(
            received(&c_after)
                .into_iter()
                .chain(received(&c_all))
                .any(|m| m == "post-removal"),
            "C reads the post-removal message"
        );
    }

    /// Leave paths: non-admin leave lands with the exact `SendOutcome` and
    /// the remaining member set; an admin step-down-and-leave lands both the
    /// step-down and the `SelfRemove`; the last admin cannot leave; and a
    /// self-update advances the epoch on every member.
    #[tokio::test(flavor = "current_thread")]
    async fn leave_and_self_update() {
        let relay = FakeRelay::new();
        let mut a = open_member().await;
        let mut b = open_member().await;
        publish_identity(&mut a, &relay, RELAY).await;
        publish_identity(&mut b, &relay, RELAY).await;
        let gid = group_with(&mut a, &relay, &[&b], "leave-flow").await;
        join_via_welcome(&mut b, &relay).await;
        sync_member(&mut a, &relay, RELAY, &gid).await;
        sync_member(&mut b, &relay, RELAY, &gid).await;

        let epoch_before = snapshot(&a, &gid).epoch;

        // Non-admin B leaves: the SelfRemove commit stages directly (no
        // step-down needed), and A's member set drops to exactly {A}.
        let outcome = b.session.leave(&gid).await.expect("leave");
        assert!(
            matches!(outcome, SendOutcome::Staged(_)),
            "non-admin leave stages its SelfRemove commit: {outcome:?}"
        );
        flush(&mut b, &relay).await;
        sync_member(&mut b, &relay, RELAY, &gid).await;
        sync_member(&mut a, &relay, RELAY, &gid).await;
        assert_eq!(
            member_accounts(&a, &gid),
            vec![a.signer.account()],
            "A's member set drops to just A"
        );

        // A's self-update advances the epoch on A; after delivery the same
        // advance is visible on B's replacement copy (rejoin) — but B left,
        // so check on a fresh pair for the all-members assertion below.
        a.session.self_update(&gid).await.expect("self update");
        flush(&mut a, &relay).await;
        assert!(
            snapshot(&a, &gid).epoch > epoch_before,
            "self-update advances the epoch on the committer"
        );

        // Admin step-down-and-leave: B is a founding admin alongside A.
        let mut a2 = open_member().await;
        let mut b2 = open_member().await;
        publish_identity(&mut a2, &relay, RELAY).await;
        publish_identity(&mut b2, &relay, RELAY).await;
        let gid2 = group_as(
            &mut a2,
            &relay,
            &[&b2],
            "admin-leave",
            &[b2.signer.account()],
        )
        .await;
        join_via_welcome(&mut b2, &relay).await;
        sync_member(&mut a2, &relay, RELAY, &gid2).await;
        sync_member(&mut b2, &relay, RELAY, &gid2).await;
        assert_eq!(
            admin_accounts(&a2, &gid2),
            {
                let mut v = vec![a2.signer.account(), b2.signer.account()];
                v.sort();
                v
            },
            "B is a founding admin"
        );

        let admin_outcome = b2.session.leave(&gid2).await.expect("admin leave");
        assert_eq!(
            admin_outcome,
            SendOutcome::LeaveRequested,
            "admin leave produces the durable leave request"
        );
        // The step-down then SelfRemove land as convergence permits —
        // cutoffs are time-based, so drive several timer rounds (no
        // ingest: B's local copy terminates mid-way).
        for _ in 0..6 {
            b2.session.tick().await.expect("tick");
            b2.clock.advance(2_000);
            flush(&mut b2, &relay).await;
        }
        for _ in 0..2 {
            sync_member(&mut a2, &relay, RELAY, &gid2).await;
        }
        assert_eq!(
            member_accounts(&a2, &gid2),
            vec![a2.signer.account()],
            "step-down and SelfRemove both land: member set is just A"
        );
        assert_eq!(
            admin_accounts(&a2, &gid2),
            vec![a2.signer.account()],
            "admin set is just A after the step-down"
        );

        // The last active admin cannot leave.
        let last_admin = a2.session.leave(&gid2).await;
        assert!(
            matches!(last_admin, Err(SessionError::AdminDepletion)),
            "last admin leave is AdminDepletion, got {last_admin:?}"
        );

        // Self-update advances the epoch on every member, not only the
        // committer — fresh pair keeps the assertions clean.
        let mut x = open_member().await;
        let mut y = open_member().await;
        publish_identity(&mut x, &relay, RELAY).await;
        publish_identity(&mut y, &relay, RELAY).await;
        let gid3 = group_with(&mut x, &relay, &[&y], "self-update-flow").await;
        join_via_welcome(&mut y, &relay).await;
        sync_member(&mut x, &relay, RELAY, &gid3).await;
        sync_member(&mut y, &relay, RELAY, &gid3).await;
        let before = snapshot(&x, &gid3).epoch;
        x.session.self_update(&gid3).await.expect("self update");
        flush(&mut x, &relay).await;
        sync_member(&mut y, &relay, RELAY, &gid3).await;
        assert_eq!(
            snapshot(&x, &gid3).epoch,
            before + 1,
            "committer epoch advanced exactly once"
        );
        assert_eq!(
            snapshot(&y, &gid3).epoch,
            snapshot(&x, &gid3).epoch,
            "peer epoch advances too"
        );
    }

    /// Concurrent commits converging: A updates the profile while B
    /// self-updates, both staged from the same parent epoch; after delivery
    /// and ticks all members agree on epoch, member set and group name, and
    /// the losing side's events show the fork was resolved.
    #[tokio::test(flavor = "current_thread")]
    async fn concurrent_commits_converge() {
        let relay = FakeRelay::new();
        let mut a = open_member().await;
        let mut b = open_member().await;
        publish_identity(&mut a, &relay, RELAY).await;
        publish_identity(&mut b, &relay, RELAY).await;
        let gid = group_with(&mut a, &relay, &[&b], "fork-flow").await;
        join_via_welcome(&mut b, &relay).await;
        sync_member(&mut a, &relay, RELAY, &gid).await;
        sync_member(&mut b, &relay, RELAY, &gid).await;
        let parent = snapshot(&a, &gid).epoch;
        assert_eq!(
            snapshot(&b, &gid).epoch,
            parent,
            "both start from the same epoch"
        );

        // Both commit on the same parent epoch before either sees the other.
        let b_outcome = b.session.self_update(&gid).await.expect("b self update");
        let a_outcome = a
            .session
            .update_group_profile(&gid, Some("renamed".to_owned()), None)
            .await
            .expect("a profile update");
        assert!(
            matches!(b_outcome, SendOutcome::Staged(_)),
            "B's commit staged from the shared parent epoch: {b_outcome:?}"
        );
        assert!(
            matches!(a_outcome, SendOutcome::Staged(_)),
            "A's commit staged from the shared parent epoch: {a_outcome:?}"
        );
        flush(&mut a, &relay).await;
        flush(&mut b, &relay).await;

        // Deliver everything and tick convergence until settled, keeping
        // the events each round drains.
        let mut a_events = Vec::new();
        let mut b_events = Vec::new();
        for _ in 0..8 {
            a_events.extend(sync_member(&mut a, &relay, RELAY, &gid).await);
            b_events.extend(sync_member(&mut b, &relay, RELAY, &gid).await);
            a.clock.advance(2_000);
            b.clock.advance(2_000);
        }
        a_events.extend(a.session.drain_events().unwrap());
        b_events.extend(b.session.drain_events().unwrap());
        let fork_resolved = a_events.iter().chain(b_events.iter()).any(|event| {
            matches!(
                event,
                SessionEvent::GroupStateInvalidated { .. }
                    | SessionEvent::GroupStateRevalidated { .. }
                    | SessionEvent::LocalCopyTerminated { .. }
                    | SessionEvent::QueuedIntentRefused { .. }
                    | SessionEvent::LocalCopyRestored { .. }
            )
        });
        assert!(
            fork_resolved,
            "the losing side's events show fork resolution (a={a_events:?} b={b_events:?})"
        );

        let sa = snapshot(&a, &gid);
        let sb = snapshot(&b, &gid);
        assert_eq!(sa.epoch, sb.epoch, "epochs converge");
        assert!(sa.epoch > parent, "the winner advanced the epoch");
        assert_eq!(sa.members, sb.members, "member sets converge");
        assert_eq!(sa.name, sb.name, "group name converges");
    }

    /// Publish failure + retry: every target terminally rejects a commit, the
    /// session reports `RolledBack` and the group returns to the prior
    /// epoch; the retried intent confirms and the peer converges to the
    /// retried epoch.
    #[tokio::test(flavor = "current_thread")]
    async fn publish_failure_rolls_back_and_retry_confirms() {
        let relay = FakeRelay::new();
        let mut a = open_member().await;
        let mut b = open_member().await;
        publish_identity(&mut a, &relay, RELAY).await;
        publish_identity(&mut b, &relay, RELAY).await;
        let gid = group_with(&mut a, &relay, &[&b], "retry-flow").await;
        join_via_welcome(&mut b, &relay).await;
        sync_member(&mut a, &relay, RELAY, &gid).await;
        sync_member(&mut b, &relay, RELAY, &gid).await;
        let epoch_before = snapshot(&a, &gid).epoch;

        a.session.self_update(&gid).await.expect("self update");
        if let Some(seq) = a.session.awaiting_durability() {
            a.session.acknowledge_durable(seq);
        }
        // Every target reports a terminal rejection.
        let mut resolutions = Vec::new();
        for item in a.session.take_outbound().expect("take_outbound") {
            let results: Vec<_> = item
                .relays
                .iter()
                .map(|relay_url| RelayResult {
                    relay: relay_url.clone(),
                    status: RelayStatus::Rejected,
                })
                .collect();
            resolutions.push(
                a.session
                    .report_publish(&item.id, &results)
                    .await
                    .expect("report"),
            );
        }
        assert!(
            resolutions.contains(&PublishResolution::RolledBack),
            "all-rejected round resolves RolledBack: {resolutions:?}"
        );
        a.session.tick().await.expect("tick");
        assert_eq!(
            snapshot(&a, &gid).epoch,
            epoch_before,
            "rejected commit rolls back to the prior epoch"
        );

        // Retry: the session re-derives the intent; this time it lands.
        a.session.self_update(&gid).await.expect("retry");
        let resolution = publish_one(&mut a, &relay, mk_core::OutboundPurpose::Commit).await;
        assert_eq!(
            resolution,
            PublishResolution::Confirmed,
            "the retried commit confirms"
        );
        flush(&mut a, &relay).await;
        sync_member(&mut b, &relay, RELAY, &gid).await;
        assert_eq!(
            snapshot(&a, &gid).epoch,
            snapshot(&b, &gid).epoch,
            "peer converges to the retried epoch"
        );
        assert_eq!(
            snapshot(&a, &gid).epoch,
            epoch_before + 1,
            "exactly one epoch applied"
        );
    }

    /// The held-attempt rule across a restart (mdk#2332): the same
    /// all-`Rejected` report that rolls a commit back inside one
    /// incarnation cannot after one. A `report_publish` on a fanout loaded
    /// from storage sees no attempt marks, so every result counts the
    /// interrupted attempt as possible exposure and the pending commit
    /// stays `Pending` — it never rolls back on a verdict for an attempt
    /// whose bytes may already have left. A later accepted report confirms
    /// that same commit, so the epoch still advances exactly once.
    #[tokio::test(flavor = "current_thread")]
    async fn rejected_after_restart_counts_as_exposed() {
        let relay = FakeRelay::new();
        let mut a = open_member().await;
        let mut b = open_member().await;
        publish_identity(&mut a, &relay, RELAY).await;
        publish_identity(&mut b, &relay, RELAY).await;
        let gid = group_with(&mut a, &relay, &[&b], "restart-reject").await;
        join_via_welcome(&mut b, &relay).await;
        sync_member(&mut a, &relay, RELAY, &gid).await;
        sync_member(&mut b, &relay, RELAY, &gid).await;
        let epoch_before = snapshot(&a, &gid).epoch;

        a.session.self_update(&gid).await.expect("self update");
        if let Some(seq) = a.session.awaiting_durability() {
            a.session.acknowledge_durable(seq);
        }
        // Hand the commit out, then crash before any verdict is recorded:
        // its attempts stay `Attempting` in the durable record.
        let staged: Vec<_> = a
            .session
            .take_outbound()
            .expect("take_outbound")
            .into_iter()
            .map(|item| item.id)
            .collect();
        assert!(!staged.is_empty(), "the commit staged an outbound");

        // Restart: only durable rows survive — the attempt marks do not.
        let mut a = reopen_member(&a).await;
        let reports: Vec<_> = a
            .session
            .take_outbound()
            .expect("take_outbound")
            .into_iter()
            .map(|item| {
                let results: Vec<_> = item
                    .relays
                    .iter()
                    .map(|relay_url| RelayResult {
                        relay: relay_url.clone(),
                        status: RelayStatus::Rejected,
                    })
                    .collect();
                (item.id, results)
            })
            .collect();
        assert_eq!(
            reports.iter().map(|(id, _)| *id).collect::<Vec<_>>(),
            staged,
            "the durable obligations are exactly the interrupted ones"
        );
        let mut resolutions = Vec::new();
        for (id, results) in &reports {
            resolutions.push(a.session.report_publish(id, results).await.expect("report"));
        }
        assert!(
            !resolutions.contains(&PublishResolution::RolledBack),
            "a verdict on an interrupted attempt never rolls back: {resolutions:?}"
        );
        assert!(
            resolutions.contains(&PublishResolution::Pending),
            "the rejected-but-possibly-exposed commit stays pending: {resolutions:?}"
        );

        // The next publish pass lands the same commit: a relay accept
        // confirms it without re-deriving the intent.
        let resolution = publish_one(&mut a, &relay, mk_core::OutboundPurpose::Commit).await;
        assert!(
            matches!(
                resolution,
                PublishResolution::Confirmed | PublishResolution::Complete
            ),
            "the surviving commit confirms on the retry pass: {resolution:?}"
        );
        assert_eq!(
            snapshot(&a, &gid).epoch,
            epoch_before + 1,
            "exactly one epoch applied"
        );
    }

    /// Duplicate delivery: re-ingesting events yields `Rejected { Duplicate }`
    /// both within one batch and across calls, and never re-delivers a
    /// message.
    #[tokio::test(flavor = "current_thread")]
    async fn duplicate_delivery_is_rejected() {
        let relay = FakeRelay::new();
        let mut a = open_member().await;
        let mut b = open_member().await;
        publish_identity(&mut a, &relay, RELAY).await;
        publish_identity(&mut b, &relay, RELAY).await;
        let gid = group_with(&mut a, &relay, &[&b], "dup-flow").await;
        join_via_welcome(&mut b, &relay).await;
        send_chat(&mut a, &relay, &gid, "hello-b").await;

        let backlog = group_events(&relay, RELAY, &b.route_hex(&gid));
        let first = ingest_backlog(&mut b, &backlog).await;
        assert!(received(&first).contains(&"hello-b".to_owned()));

        // Duplicates inside ONE batch.
        let mut twice = backlog.clone();
        twice.extend(backlog.iter().cloned());
        let reports = b.session.ingest_batch(twice).await.expect("ingest_batch");
        let dup = InputOutcome::Rejected {
            category: InputCategory::Duplicate,
        };
        assert!(
            reports.iter().any(|report| report.outcome == dup),
            "intra-batch duplicates are Rejected{{Duplicate}}: {reports:?}"
        );
        assert!(
            !received(&b.session.drain_events().unwrap()).contains(&"hello-b".to_owned()),
            "no duplicate delivery"
        );

        // Duplicates across separate calls.
        let cross_reports = b
            .session
            .ingest_batch(backlog.clone())
            .await
            .expect("ingest_batch");
        assert!(
            !cross_reports.is_empty() && cross_reports.iter().all(|report| report.outcome == dup),
            "cross-call re-ingest is uniformly Rejected{{Duplicate}}: {cross_reports:?}"
        );
        let leftover = received(&b.session.drain_events().unwrap());
        assert_eq!(leftover, Vec::<String>::new());
    }

    /// The durability gate on a fresh open: open itself emits host batches
    /// that are not host-acknowledged, `awaiting_durability` stays empty
    /// until something stages, and staged outbound is released only after an
    /// explicit `acknowledge_durable` covering its gate seq.
    #[tokio::test(flavor = "current_thread")]
    async fn fresh_open_gate_holds_until_ack() {
        let relay = FakeRelay::new();
        let mut a = open_member().await;
        assert!(
            !a.host.batches().is_empty(),
            "fresh open emits batches the host has not acked"
        );
        assert!(
            a.session.awaiting_durability().is_none(),
            "nothing is gated before anything stages"
        );

        let gid = a
            .session
            .create_group(CreateGroup {
                name: "gate".to_owned(),
                description: String::new(),
                relays: vec![RELAY.to_owned()],
                invitees: Vec::new(),
                own_devices: Vec::new(),
                admins: Vec::new(),
                components: Vec::new(),
            })
            .await
            .expect("create");
        a.session
            .send_message(
                &gid,
                mk_core::AppMessage {
                    kind: 9,
                    tags: Vec::new(),
                    content: "gated".to_owned(),
                },
            )
            .await
            .expect("send");

        // Staged but un-acked: awaiting reports a seq, take_outbound holds.
        let seq = a
            .session
            .awaiting_durability()
            .expect("staged outbound is gated");
        assert!(
            a.session.take_outbound().expect("take_outbound").is_empty(),
            "nothing staged later is released without an explicit ack"
        );
        a.session.acknowledge_durable(seq);
        let first = a.session.take_outbound().expect("take_outbound");
        assert!(!first.is_empty(), "ack releases the staged outbound");
        assert!(
            a.session.take_outbound().expect("take_outbound").is_empty(),
            "no repeat handoff before report"
        );
        for item in &first {
            a.session
                .report_publish(&item.id, &relay.publish(item))
                .await
                .expect("report");
        }
    }

    /// A queued app message regenerated behind a pending commit: crash
    /// between staging the regenerated artifact and reporting it; on reopen
    /// the fanout's `queued_intent_id` resolves the publish report to the
    /// durable intent — retired exactly once, never regenerated a second
    /// time.
    #[tokio::test(flavor = "current_thread")]
    async fn queued_intent_retires_after_restart() {
        let relay = FakeRelay::new();
        let mut a = open_member().await;
        publish_identity(&mut a, &relay, RELAY).await;
        let gid = a
            .session
            .create_group(CreateGroup {
                name: "queued".to_owned(),
                description: String::new(),
                relays: vec![RELAY.to_owned()],
                invitees: Vec::new(),
                own_devices: Vec::new(),
                admins: Vec::new(),
                components: Vec::new(),
            })
            .await
            .expect("create");
        flush(&mut a, &relay).await;

        // Pending own commit queues the next send.
        a.session.self_update(&gid).await.expect("self update");
        let outcome = a
            .session
            .send_message(
                &gid,
                mk_core::AppMessage {
                    kind: 9,
                    tags: Vec::new(),
                    content: "queued-behind-commit".to_owned(),
                },
            )
            .await
            .expect("send");
        assert_eq!(
            outcome.outcome,
            SendOutcome::Queued,
            "the send queues behind the pending commit"
        );

        // Confirm the commit; the convergence drain regenerates the queued
        // intent into a fresh artifact.
        let resolution = publish_one(&mut a, &relay, mk_core::OutboundPurpose::Commit).await;
        assert_eq!(resolution, PublishResolution::Confirmed);
        a.session.tick().await.expect("tick");
        if let Some(seq) = a.session.awaiting_durability() {
            a.session.acknowledge_durable(seq);
        }
        let staged = a.session.take_outbound().expect("take_outbound");
        assert_eq!(staged.len(), 1, "exactly the regenerated artifact");
        let staged_json: Vec<_> = staged.iter().map(|i| i.event.to_json()).collect();

        // Crash before reporting it.
        let mut a = reopen_member(&a).await;
        if let Some(seq) = a.session.awaiting_durability() {
            a.session.acknowledge_durable(seq);
        }
        let replayed = a.session.take_outbound().expect("take_outbound");
        assert_eq!(
            staged_json,
            replayed
                .iter()
                .map(|i| i.event.to_json())
                .collect::<Vec<_>>(),
            "reopen releases the identical frozen artifact"
        );
        let replay_resolution = a
            .session
            .report_publish(&replayed[0].id, &relay.publish(&replayed[0]))
            .await
            .expect("report");
        assert!(
            matches!(
                replay_resolution,
                PublishResolution::Confirmed | PublishResolution::Complete
            ),
            "the regenerated artifact resolves on acceptance: {replay_resolution:?}"
        );

        // The durable intent is retired: further drains produce no second
        // regeneration of the same logical send.
        a.session.tick().await.expect("tick");
        if let Some(seq) = a.session.awaiting_durability() {
            a.session.acknowledge_durable(seq);
        }
        assert!(
            a.session.take_outbound().expect("take_outbound").is_empty(),
            "no duplicate regeneration after restart+report"
        );
    }

    /// Restart with an outstanding obligation: the staged commit survives the
    /// crash, is re-released with the same bytes, resolves `Confirmed`
    /// exactly once, a second identical report is a no-op, and the peer
    /// applies the commit exactly once.
    #[tokio::test(flavor = "current_thread")]
    async fn restart_releases_same_bytes() {
        let relay = FakeRelay::new();
        let mut a = open_member().await;
        let mut b = open_member().await;
        publish_identity(&mut a, &relay, RELAY).await;
        publish_identity(&mut b, &relay, RELAY).await;
        let gid = group_with(&mut a, &relay, &[&b], "crash").await;
        join_via_welcome(&mut b, &relay).await;
        sync_member(&mut a, &relay, RELAY, &gid).await;
        sync_member(&mut b, &relay, RELAY, &gid).await;
        let epoch_before = snapshot(&b, &gid).epoch;

        a.session.self_update(&gid).await.expect("self update");
        let seq = a.session.awaiting_durability().expect("gated");
        a.session.acknowledge_durable(seq);
        let staged = a.session.take_outbound().expect("take_outbound");
        assert!(!staged.is_empty(), "commit staged");
        let staged_ids: Vec<_> = staged.iter().map(|item| item.id).collect();
        let staged_json: Vec<_> = staged.iter().map(|item| item.event.to_json()).collect();

        // Crash before report_publish: reopen from durable rows only.
        let mut a = reopen_member(&a).await;
        if let Some(gate) = a.session.awaiting_durability() {
            a.session.acknowledge_durable(gate);
        }
        let replayed = a.session.take_outbound().expect("take_outbound");
        assert_eq!(
            staged_json,
            replayed
                .iter()
                .map(|item| item.event.to_json())
                .collect::<Vec<_>>(),
            "the identical signed event is released again"
        );
        assert_eq!(
            staged_ids,
            replayed.iter().map(|item| item.id).collect::<Vec<_>>()
        );

        // Report once: the commit resolves exactly `Confirmed`, the epoch
        // advances exactly once.
        let commit = replayed
            .iter()
            .find(|item| item.purpose == mk_core::OutboundPurpose::Commit)
            .expect("commit artifact");
        let resolution = a
            .session
            .report_publish(&commit.id, &relay.publish(commit))
            .await
            .expect("report");
        assert_eq!(
            resolution,
            PublishResolution::Confirmed,
            "the surviving commit confirms exactly once"
        );
        assert_eq!(
            snapshot(&a, &gid).epoch,
            epoch_before + 1,
            "epoch advances exactly once"
        );

        // A second identical report is a no-op.
        let second = a
            .session
            .report_publish(&commit.id, &relay.publish(commit))
            .await;
        assert!(
            matches!(second, Err(SessionError::UnknownOutbound))
                || matches!(second, Ok(PublishResolution::Complete)),
            "second identical report is a no-op: {second:?}"
        );

        // The peer applies the commit exactly once, even ingested twice.
        flush(&mut a, &relay).await;
        sync_member(&mut b, &relay, RELAY, &gid).await;
        assert_eq!(
            snapshot(&b, &gid).epoch,
            epoch_before + 1,
            "peer applies the commit exactly once"
        );
        let route = b.route_hex(&gid);
        let dup_reports = b
            .session
            .ingest_batch(group_events(&relay, RELAY, &route))
            .await
            .expect("re-ingest");
        assert!(
            dup_reports.iter().all(|report| matches!(
                report.outcome,
                InputOutcome::Rejected {
                    category: InputCategory::Duplicate
                } | InputOutcome::Stale { .. }
                    | InputOutcome::Local(_)
            )),
            "re-ingested commit is not applied a second time: {dup_reports:?}"
        );
        assert_eq!(snapshot(&b, &gid).epoch, epoch_before + 1);
    }

    /// A five-member group: create with four invitees, all message, one
    /// removal, all converge.
    #[tokio::test(flavor = "current_thread")]
    async fn five_member_group() {
        let relay = FakeRelay::new();
        let mut a = open_member().await;
        let mut members: Vec<Member> = Vec::new();
        for _ in 0..4 {
            members.push(open_member().await);
        }
        publish_identity(&mut a, &relay, RELAY).await;
        for member in &mut members {
            publish_identity(member, &relay, RELAY).await;
        }
        let refs: Vec<&Member> = members.iter().collect();
        let gid = group_with(&mut a, &relay, &refs, "big-flow").await;
        for member in &mut members {
            join_via_welcome(member, &relay).await;
            sync_member(member, &relay, RELAY, &gid).await;
        }
        sync_member(&mut a, &relay, RELAY, &gid).await;
        assert_eq!(snapshot(&a, &gid).members.len(), 5);

        for (i, member) in members.iter_mut().enumerate() {
            send_chat(member, &relay, &gid, &format!("m{i}")).await;
        }
        send_chat(&mut a, &relay, &gid, "ma").await;
        let mut got: Vec<String> = Vec::new();
        for member in &mut members {
            let drained = sync_member(member, &relay, RELAY, &gid).await;
            got.extend(received(&drained));
        }
        let drained = sync_member(&mut a, &relay, RELAY, &gid).await;
        got.extend(received(&drained));
        let route = a.route_hex(&gid);
        let last = ingest_backlog(&mut a, &group_events(&relay, RELAY, &route)).await;
        got.extend(received(&last));
        for i in 0..4 {
            assert!(
                got.contains(&format!("m{i}")),
                "A sees member {i}'s message"
            );
        }

        // A removes member 3; everyone converges on 4 members.
        let target = members[3].signer.account();
        a.session
            .remove_members(&gid, &[target])
            .await
            .expect("remove");
        flush(&mut a, &relay).await;
        a.session.tick().await.expect("tick");
        flush(&mut a, &relay).await;
        for member in members.iter_mut().take(3) {
            sync_member(member, &relay, RELAY, &gid).await;
            assert_eq!(
                snapshot(member, &gid).members.len(),
                4,
                "member sees the removal"
            );
        }
    }

    /// `report_publish` resolves each reported fanout by reading that one
    /// row — never by listing the outbox (the large-group regression:
    /// publishing N create-time Welcomes must not scan N fanouts apiece).
    #[tokio::test(flavor = "current_thread")]
    async fn report_publish_reads_only_the_reported_fanout() {
        let relay = FakeRelay::new();
        let mut a = open_member().await;
        let mut invitees: Vec<Member> = Vec::new();
        for _ in 0..3 {
            invitees.push(open_member().await);
        }
        publish_identity(&mut a, &relay, RELAY).await;
        for member in &mut invitees {
            publish_identity(member, &relay, RELAY).await;
        }
        a.session
            .create_group(CreateGroup {
                name: "report-read".to_owned(),
                description: String::new(),
                relays: vec![RELAY.to_owned()],
                invitees: invitees
                    .iter()
                    .map(|member| Invitee {
                        key_package: key_package_of(&relay, RELAY, &member.account_hex()),
                        inbox_relays: inbox_relays_of(&relay, RELAY, &member.account_hex()),
                    })
                    .collect(),
                own_devices: Vec::new(),
                admins: Vec::new(),
                components: Vec::new(),
            })
            .await
            .expect("create_group");
        if let Some(seq) = a.session.awaiting_durability() {
            a.session.acknowledge_durable(seq);
        }
        let outbound = a.session.take_outbound().expect("take_outbound");
        assert_eq!(
            outbound
                .iter()
                .filter(|item| item.purpose == mk_core::OutboundPurpose::Welcome)
                .count(),
            3,
            "the create stages one welcome per invitee"
        );
        let scans = a.session.outbound_fanout_list_count();
        for item in &outbound {
            let resolution = a
                .session
                .report_publish(&item.id, &relay.publish(item))
                .await
                .expect("report_publish");
            assert_eq!(
                resolution,
                PublishResolution::Complete,
                "every reported item resolves terminal"
            );
        }
        assert_eq!(
            a.session.outbound_fanout_list_count(),
            scans,
            "report_publish must not list the fanout table"
        );
    }

    /// A refused or failed Welcome unwrap is retryable, never terminal:
    /// the ingest reports `unavailable` with the matching `signer_*`
    /// category, commits no durable batch, and re-ingesting the same gift
    /// wrap once the signer answers again joins the group in the same
    /// session.
    #[tokio::test(flavor = "current_thread")]
    async fn refused_welcome_unwrap_is_retryable() {
        for (mode, category) in [
            (DecryptGate::Refused, Availability::SignerRefused),
            (DecryptGate::Failed, Availability::SignerFailed),
        ] {
            let relay = FakeRelay::new();
            let mut a = open_member().await;
            publish_identity(&mut a, &relay, RELAY).await;
            let signer = Arc::new(GatedSigner::new());
            let mut b = open_member_with(Arc::<GatedSigner>::clone(&signer)).await;
            publish_identity(&mut b, &relay, RELAY).await;
            let gid = group_with(&mut a, &relay, &[&b], "retryable-unwrap").await;

            let welcomes = welcomes_for(&relay, RELAY, &b.account_hex());
            assert!(!welcomes.is_empty(), "a welcome must be waiting");
            let batches_before = b.host.batches().len();
            signer.set(mode);
            let reports = b
                .session
                .ingest_batch(welcomes.clone())
                .await
                .expect("refused ingest reports");
            assert!(
                reports
                    .iter()
                    .all(|report| report.outcome == InputOutcome::Unavailable { category }),
                "a refused/failed unwrap reports unavailable: {reports:?}"
            );
            assert_eq!(
                b.host.batches().len(),
                batches_before,
                "a refused ingest commits no durable batch"
            );
            assert!(
                matches!(b.session.group(&gid), Ok(None)),
                "no group before the unwrap succeeds"
            );

            signer.set(DecryptGate::Working);
            let retried = b.session.ingest_batch(welcomes).await.expect("retry");
            assert!(
                retried
                    .iter()
                    .all(|report| report.outcome == InputOutcome::Accepted),
                "the retried welcome is accepted: {retried:?}"
            );
            assert!(
                b.session.group(&gid).is_ok_and(|group| group.is_some()),
                "the retried ingest joined"
            );

            // The discarded savepoint left nothing cached: the retried
            // join lands on the inviter's epoch and decrypts fresh
            // traffic — an `MlsGroup` stale from inside the span could
            // neither match this epoch nor open this ciphertext. The
            // generation contract itself is proven by the cache model's
            // held-savepoint-drop step (tests/model.rs).
            assert_eq!(
                snapshot(&b, &gid).epoch,
                snapshot(&a, &gid).epoch,
                "the retried join lands on the current epoch"
            );
            send_chat(&mut a, &relay, &gid, "after-retry").await;
            let delivered = sync_member(&mut b, &relay, RELAY, &gid).await;
            assert!(
                received(&delivered).contains(&"after-retry".to_owned()),
                "the retried join decrypts new traffic: {delivered:?}"
            );
        }
    }

    /// The refused unwrap survives a restart too: nothing of the failed
    /// span reached durable rows, so the reopened session unwraps the same
    /// wrap and joins.
    #[tokio::test(flavor = "current_thread")]
    async fn refused_welcome_retry_survives_restart() {
        let relay = FakeRelay::new();
        let mut a = open_member().await;
        publish_identity(&mut a, &relay, RELAY).await;
        let signer = Arc::new(GatedSigner::new());
        let mut b = open_member_with(Arc::<GatedSigner>::clone(&signer)).await;
        publish_identity(&mut b, &relay, RELAY).await;
        let gid = group_with(&mut a, &relay, &[&b], "restart-unwrap").await;

        let welcomes = welcomes_for(&relay, RELAY, &b.account_hex());
        assert!(!welcomes.is_empty(), "a welcome must be waiting");
        signer.set(DecryptGate::Refused);
        let reports = b
            .session
            .ingest_batch(welcomes.clone())
            .await
            .expect("refused ingest reports");
        assert!(
            reports.iter().all(|report| report.outcome
                == InputOutcome::Unavailable {
                    category: Availability::SignerRefused,
                }),
            "refused: {reports:?}"
        );

        let mut b = reopen_member(&b).await;
        signer.set(DecryptGate::Working);
        let retried = b.session.ingest_batch(welcomes).await.expect("retry");
        assert!(
            retried
                .iter()
                .all(|report| report.outcome == InputOutcome::Accepted),
            "the retried welcome is accepted after restart: {retried:?}"
        );
        assert!(
            b.session.group(&gid).is_ok_and(|group| group.is_some()),
            "the retried ingest joined after restart"
        );
    }

    /// A wrap whose content decrypts but does not parse as a NIP-59 seal is
    /// a genuine rejection, not an availability verdict: it reports
    /// `rejected: invalid_encoding` and re-ingest is `duplicate`.
    #[tokio::test(flavor = "current_thread")]
    async fn malformed_welcome_stays_rejected() {
        let relay = FakeRelay::new();
        let mut b = open_member().await;
        publish_identity(&mut b, &relay, RELAY).await;

        // A gift wrap by a throwaway author whose NIP-44 content decrypts
        // to bytes that are not a seal.
        let author = mk_core::testing::LocalSigner::new();
        let ciphertext = author
            .nip44_encrypt(&b.signer.account(), "not a seal")
            .await
            .expect("encrypt");
        let unsigned = UnsignedEvent {
            pubkey: author.account(),
            created_at: b.clock.now().unix_ms / 1_000,
            kind: 1059,
            tags: vec![vec!["p".to_owned(), b.account_hex()]],
            content: ciphertext,
        };
        let malformed = author.sign_event(unsigned).await.expect("sign");

        let report = b.session.ingest(&malformed).await.expect("ingest");
        assert_eq!(
            report.outcome,
            InputOutcome::Rejected {
                category: InputCategory::InvalidEncoding,
            },
            "a malformed wrap is rejected"
        );
        let again = b.session.ingest(&malformed).await.expect("re-ingest");
        assert_eq!(
            again.outcome,
            InputOutcome::Rejected {
                category: InputCategory::Duplicate,
            },
            "the rejected wrap is remembered"
        );
    }

    /// Session retry pacing: an all-`unavailable` report arms a durable
    /// backoff deadline; the item is not re-offered before it, is offered
    /// exactly at it, and every later retryable report doubles the delay
    /// up to the 60-second cap.
    #[tokio::test(flavor = "current_thread")]
    async fn unavailable_publication_backoff_paces_reoffer() {
        let mut a = open_member().await;
        a.session
            .ensure_key_package(&[RELAY.to_owned()])
            .await
            .expect("key package");
        if let Some(seq) = a.session.awaiting_durability() {
            a.session.acknowledge_durable(seq);
        }
        let unavailable = |item: &Outbound| -> Vec<RelayResult> {
            item.relays
                .iter()
                .map(|relay| RelayResult {
                    relay: relay.clone(),
                    status: RelayStatus::Unavailable,
                })
                .collect()
        };
        let take_key_package = |member: &mut Member| -> Option<Outbound> {
            member
                .session
                .take_outbound()
                .expect("take_outbound")
                .into_iter()
                .find(|item| item.purpose == mk_core::OutboundPurpose::KeyPackage)
        };

        let item = take_key_package(&mut a).expect("first offer");
        a.session
            .report_publish(&item.id, &unavailable(&item))
            .await
            .expect("report");
        // Before the 1-second deadline: nothing.
        assert!(take_key_package(&mut a).is_none(), "not re-offered early");
        a.clock.advance(999);
        assert!(take_key_package(&mut a).is_none(), "still inside backoff");
        a.clock.advance(1);
        let retried = take_key_package(&mut a).expect("re-offered at the deadline");
        a.session
            .report_publish(&retried.id, &unavailable(&retried))
            .await
            .expect("report");

        // Backoff doubles per consecutive retryable report, capped at 60s.
        for expected in [2_000, 4_000, 8_000, 16_000, 32_000, 60_000, 60_000] {
            a.clock.advance(expected - 1);
            assert!(
                take_key_package(&mut a).is_none(),
                "inside the {expected}ms backoff"
            );
            a.clock.advance(1);
            let again = take_key_package(&mut a).expect("re-offered at deadline");
            a.session
                .report_publish(&again.id, &unavailable(&again))
                .await
                .expect("report");
        }
    }

    /// An accepted target resets the backoff: after the reset the
    /// still-outstanding item re-offers immediately.
    #[tokio::test(flavor = "current_thread")]
    async fn accepted_target_resets_retry_backoff() {
        let mut a = open_member().await;
        a.session
            .ensure_key_package(&[RELAY.to_owned(), RELAY2.to_owned()])
            .await
            .expect("key package");
        if let Some(seq) = a.session.awaiting_durability() {
            a.session.acknowledge_durable(seq);
        }
        let unavailable_all = |item: &Outbound| -> Vec<RelayResult> {
            item.relays
                .iter()
                .map(|relay| RelayResult {
                    relay: relay.clone(),
                    status: RelayStatus::Unavailable,
                })
                .collect()
        };
        let item = a
            .session
            .take_outbound()
            .expect("take")
            .into_iter()
            .find(|item| item.purpose == mk_core::OutboundPurpose::KeyPackage)
            .expect("key package offered");
        let resolution = a
            .session
            .report_publish(&item.id, &unavailable_all(&item))
            .await
            .expect("report");
        assert_eq!(resolution, PublishResolution::Pending);

        a.clock.advance(1_000);
        let retried = a
            .session
            .take_outbound()
            .expect("take")
            .into_iter()
            .find(|outbound| outbound.purpose == mk_core::OutboundPurpose::KeyPackage)
            .expect("re-offered");
        let results: Vec<RelayResult> = retried
            .relays
            .iter()
            .enumerate()
            .map(|(index, relay)| RelayResult {
                relay: relay.clone(),
                status: if index == 0 {
                    RelayStatus::Accepted
                } else {
                    RelayStatus::Unavailable
                },
            })
            .collect();
        let resolved = a
            .session
            .report_publish(&retried.id, &results)
            .await
            .expect("report");
        assert_eq!(resolved, PublishResolution::Pending);
        // Backoff reset: the surviving target re-offers with no wait.
        let again = a
            .session
            .take_outbound()
            .expect("take")
            .into_iter()
            .find(|outbound| outbound.purpose == mk_core::OutboundPurpose::KeyPackage)
            .expect("re-offered immediately after an accept");
        assert_eq!(again.relays.len(), 1, "only the retryable target remains");
    }

    /// Retry state is durable: after a restart the deadline still paces
    /// the item.
    #[tokio::test(flavor = "current_thread")]
    async fn retry_deadline_survives_restart() {
        let mut a = open_member().await;
        a.session
            .ensure_key_package(&[RELAY.to_owned()])
            .await
            .expect("key package");
        if let Some(seq) = a.session.awaiting_durability() {
            a.session.acknowledge_durable(seq);
        }
        let item = a
            .session
            .take_outbound()
            .expect("take")
            .into_iter()
            .find(|item| item.purpose == mk_core::OutboundPurpose::KeyPackage)
            .expect("offered");
        let results: Vec<RelayResult> = item
            .relays
            .iter()
            .map(|relay| RelayResult {
                relay: relay.clone(),
                status: RelayStatus::Unavailable,
            })
            .collect();
        a.session
            .report_publish(&item.id, &results)
            .await
            .expect("report");

        let mut a = reopen_member(&a).await;
        let offered = |member: &mut Member| -> bool {
            member
                .session
                .take_outbound()
                .expect("take")
                .iter()
                .any(|outbound| outbound.purpose == mk_core::OutboundPurpose::KeyPackage)
        };
        assert!(!offered(&mut a), "the deadline survives the restart");
        a.clock.advance(1_000);
        assert!(offered(&mut a), "offered once the deadline passes");
    }

    /// `tick().next_wake_ms` folds the earliest live retry deadline into
    /// the schedule.
    #[tokio::test(flavor = "current_thread")]
    async fn next_wake_tracks_earliest_retry_deadline() {
        let mut a = open_member().await;
        a.session
            .ensure_key_package(&[RELAY.to_owned()])
            .await
            .expect("key package");
        a.session
            .publish_inbox_relays(&[RELAY.to_owned()])
            .await
            .expect("inbox relays");
        if let Some(seq) = a.session.awaiting_durability() {
            a.session.acknowledge_durable(seq);
        }
        let items = a.session.take_outbound().expect("take");
        assert_eq!(items.len(), 2, "both publications offered");
        // First failure on the key package only: deadline now+1s.
        let key_package = items
            .iter()
            .find(|item| item.purpose == mk_core::OutboundPurpose::KeyPackage)
            .expect("key package");
        a.session
            .report_publish(
                &key_package.id,
                &[RelayResult {
                    relay: RELAY.to_owned(),
                    status: RelayStatus::Unavailable,
                }],
            )
            .await
            .expect("report");
        a.clock.advance(500);
        // A later failure on the inbox relays: deadline now+1s (=1.5s).
        let inbox = items
            .iter()
            .find(|item| item.purpose == mk_core::OutboundPurpose::InboxRelays)
            .expect("inbox relays");
        a.session
            .report_publish(
                &inbox.id,
                &[RelayResult {
                    relay: RELAY.to_owned(),
                    status: RelayStatus::Unavailable,
                }],
            )
            .await
            .expect("report");
        let report = a.session.tick().await.expect("tick");
        assert_eq!(
            report.next_wake_ms,
            Some(500),
            "the wake is the earliest live deadline"
        );
    }

    /// The full backoff shape over 600 simulated seconds against an
    /// always-unavailable relay: takes at t = 0,1,3,7,15,31,63,123,…,543 s
    /// — exactly 15 publish attempts.
    #[tokio::test(flavor = "current_thread")]
    async fn retry_backoff_soak_counts_attempts() {
        let mut a = open_member().await;
        a.session
            .ensure_key_package(&[RELAY.to_owned()])
            .await
            .expect("key package");
        if let Some(seq) = a.session.awaiting_durability() {
            a.session.acknowledge_durable(seq);
        }
        let start_ms = a.clock.now().unix_ms;
        let mut attempts = 0u32;
        loop {
            for item in a.session.take_outbound().expect("take") {
                assert_eq!(item.purpose, mk_core::OutboundPurpose::KeyPackage);
                let results: Vec<RelayResult> = item
                    .relays
                    .iter()
                    .map(|relay| RelayResult {
                        relay: relay.clone(),
                        status: RelayStatus::Unavailable,
                    })
                    .collect();
                let resolution = a
                    .session
                    .report_publish(&item.id, &results)
                    .await
                    .expect("report");
                assert_eq!(resolution, PublishResolution::Pending);
                attempts += 1;
            }
            let report = a.session.tick().await.expect("tick");
            let Some(wake) = report.next_wake_ms else {
                break;
            };
            if a.clock.now().unix_ms - start_ms + wake > 600_000 {
                break;
            }
            a.clock.advance(wake);
        }
        assert_eq!(attempts, 15, "600s of backoff yields 15 attempts");
    }

    /// `not_sent` records no exposure: an all-`not_sent` report holds the
    /// commit `Pending` and paces a re-offer, and a later all-`rejected`
    /// round still rolls the commit back, leaving the group usable for a
    /// new staged commit.
    #[tokio::test(flavor = "current_thread")]
    async fn not_sent_holds_commit_then_rejected_rolls_back() {
        let relay = FakeRelay::new();
        let mut a = open_member().await;
        let mut b = open_member().await;
        publish_identity(&mut a, &relay, RELAY).await;
        publish_identity(&mut b, &relay, RELAY).await;
        let gid = group_with(&mut a, &relay, &[&b], "not-sent-hold").await;
        join_via_welcome(&mut b, &relay).await;
        sync_member(&mut a, &relay, RELAY, &gid).await;
        sync_member(&mut b, &relay, RELAY, &gid).await;
        let epoch_before = snapshot(&a, &gid).epoch;

        a.session.self_update(&gid).await.expect("self update");
        if let Some(seq) = a.session.awaiting_durability() {
            a.session.acknowledge_durable(seq);
        }
        let item = a
            .session
            .take_outbound()
            .expect("take")
            .into_iter()
            .find(|item| item.purpose == mk_core::OutboundPurpose::Commit)
            .expect("commit staged");
        let verdicts = |outbound: &Outbound, status| -> Vec<RelayResult> {
            outbound
                .relays
                .iter()
                .map(|target| RelayResult {
                    relay: target.clone(),
                    status,
                })
                .collect()
        };
        let resolution = a
            .session
            .report_publish(&item.id, &verdicts(&item, RelayStatus::NotSent))
            .await
            .expect("report");
        assert_eq!(
            resolution,
            PublishResolution::Pending,
            "not_sent holds the commit for retry"
        );
        // Backoff applies to not_sent too: no immediate re-offer.
        assert!(
            !a.session
                .take_outbound()
                .expect("take")
                .iter()
                .any(|outbound| outbound.purpose == mk_core::OutboundPurpose::Commit),
            "the held commit is not re-offered inside backoff"
        );
        a.clock.advance(1_000);
        let retried = a
            .session
            .take_outbound()
            .expect("take")
            .into_iter()
            .find(|outbound| outbound.purpose == mk_core::OutboundPurpose::Commit)
            .expect("re-offered after the backoff");
        let resolved = a
            .session
            .report_publish(&retried.id, &verdicts(&retried, RelayStatus::Rejected))
            .await
            .expect("report");
        assert_eq!(
            resolved,
            PublishResolution::RolledBack,
            "definite refusals after not_sent still roll the commit back"
        );
        a.session.tick().await.expect("tick");
        assert_eq!(
            snapshot(&a, &gid).epoch,
            epoch_before,
            "rolled back to the prior epoch"
        );

        // The group stays usable: a fresh commit stages and confirms.
        a.session.self_update(&gid).await.expect("retry intent");
        let confirmed = publish_one(&mut a, &relay, mk_core::OutboundPurpose::Commit).await;
        assert_eq!(
            confirmed,
            PublishResolution::Confirmed,
            "a new commit on the rolled-back group confirms"
        );
        assert_eq!(snapshot(&a, &gid).epoch, epoch_before + 1);
    }

    /// Control: `unavailable` records possible exposure, so a later
    /// all-`rejected` round cannot roll the commit back.
    #[tokio::test(flavor = "current_thread")]
    async fn unavailable_then_rejected_stays_pending() {
        let relay = FakeRelay::new();
        let mut a = open_member().await;
        let mut b = open_member().await;
        publish_identity(&mut a, &relay, RELAY).await;
        publish_identity(&mut b, &relay, RELAY).await;
        let gid = group_with(&mut a, &relay, &[&b], "unavail-hold").await;
        join_via_welcome(&mut b, &relay).await;
        sync_member(&mut a, &relay, RELAY, &gid).await;
        let epoch_before = snapshot(&a, &gid).epoch;

        a.session.self_update(&gid).await.expect("self update");
        if let Some(seq) = a.session.awaiting_durability() {
            a.session.acknowledge_durable(seq);
        }
        let verdicts = |item: &Outbound, status| -> Vec<RelayResult> {
            item.relays
                .iter()
                .map(|target| RelayResult {
                    relay: target.clone(),
                    status,
                })
                .collect()
        };
        for status in [RelayStatus::Unavailable, RelayStatus::Rejected] {
            let item = a
                .session
                .take_outbound()
                .expect("take")
                .into_iter()
                .find(|item| item.purpose == mk_core::OutboundPurpose::Commit)
                .expect("commit offered");
            let resolution = a
                .session
                .report_publish(&item.id, &verdicts(&item, status))
                .await
                .expect("report");
            assert_eq!(
                resolution,
                PublishResolution::Pending,
                "possibly-exposed commit stays pending on {status:?}"
            );
            a.clock.advance(1_000);
        }
        a.session.tick().await.expect("tick");
        assert_eq!(
            snapshot(&a, &gid).epoch,
            epoch_before,
            "no rollback, no advance"
        );
    }

    /// One `unavailable` among `not_sent` results still records possible
    /// exposure: the later all-`rejected` round does not roll back.
    #[tokio::test(flavor = "current_thread")]
    async fn mixed_not_sent_unavailable_then_rejected_stays_pending() {
        let relay = FakeRelay::new();
        let mut a = open_member().await;
        let mut b = open_member().await;
        publish_identity(&mut a, &relay, RELAY).await;
        publish_identity(&mut b, &relay, RELAY).await;
        // Two routing relays so one report can mix verdicts.
        let gid = a
            .session
            .create_group(CreateGroup {
                name: "mixed-hold".to_owned(),
                description: String::new(),
                relays: vec![RELAY.to_owned(), RELAY2.to_owned()],
                invitees: vec![Invitee {
                    key_package: key_package_of(&relay, RELAY, &b.account_hex()),
                    inbox_relays: inbox_relays_of(&relay, RELAY, &b.account_hex()),
                }],
                own_devices: Vec::new(),
                admins: Vec::new(),
                components: Vec::new(),
            })
            .await
            .expect("create");
        flush(&mut a, &relay).await;
        join_via_welcome(&mut b, &relay).await;
        sync_member(&mut a, &relay, RELAY, &gid).await;

        a.session.self_update(&gid).await.expect("self update");
        if let Some(seq) = a.session.awaiting_durability() {
            a.session.acknowledge_durable(seq);
        }
        let item = a
            .session
            .take_outbound()
            .expect("take")
            .into_iter()
            .find(|item| item.purpose == mk_core::OutboundPurpose::Commit)
            .expect("commit staged");
        assert!(item.relays.len() > 1, "the commit fans out to both relays");
        let results: Vec<RelayResult> = item
            .relays
            .iter()
            .enumerate()
            .map(|(index, target)| RelayResult {
                relay: target.clone(),
                status: if index == 0 {
                    RelayStatus::NotSent
                } else {
                    RelayStatus::Unavailable
                },
            })
            .collect();
        let resolution = a
            .session
            .report_publish(&item.id, &results)
            .await
            .expect("report");
        assert_eq!(resolution, PublishResolution::Pending);

        a.clock.advance(1_000);
        let retried = a
            .session
            .take_outbound()
            .expect("take")
            .into_iter()
            .find(|outbound| outbound.purpose == mk_core::OutboundPurpose::Commit)
            .expect("re-offered");
        let refusals: Vec<RelayResult> = retried
            .relays
            .iter()
            .map(|target| RelayResult {
                relay: target.clone(),
                status: RelayStatus::Rejected,
            })
            .collect();
        let resolved = a
            .session
            .report_publish(&retried.id, &refusals)
            .await
            .expect("report");
        assert_eq!(
            resolved,
            PublishResolution::Pending,
            "one possibly-exposed target keeps the commit pending"
        );
    }

    /// `not_sent` then `accepted` confirms the commit exactly as a clean
    /// publish would.
    #[tokio::test(flavor = "current_thread")]
    async fn not_sent_then_accepted_confirms() {
        let relay = FakeRelay::new();
        let mut a = open_member().await;
        let mut b = open_member().await;
        publish_identity(&mut a, &relay, RELAY).await;
        publish_identity(&mut b, &relay, RELAY).await;
        let gid = group_with(&mut a, &relay, &[&b], "not-sent-ok").await;
        join_via_welcome(&mut b, &relay).await;
        sync_member(&mut a, &relay, RELAY, &gid).await;
        let epoch_before = snapshot(&a, &gid).epoch;

        a.session.self_update(&gid).await.expect("self update");
        if let Some(seq) = a.session.awaiting_durability() {
            a.session.acknowledge_durable(seq);
        }
        let verdicts = |item: &Outbound, status| -> Vec<RelayResult> {
            item.relays
                .iter()
                .map(|target| RelayResult {
                    relay: target.clone(),
                    status,
                })
                .collect()
        };
        let item = a
            .session
            .take_outbound()
            .expect("take")
            .into_iter()
            .find(|item| item.purpose == mk_core::OutboundPurpose::Commit)
            .expect("commit staged");
        let resolution = a
            .session
            .report_publish(&item.id, &verdicts(&item, RelayStatus::NotSent))
            .await
            .expect("report");
        assert_eq!(resolution, PublishResolution::Pending);
        a.clock.advance(1_000);
        let retried = a
            .session
            .take_outbound()
            .expect("take")
            .into_iter()
            .find(|outbound| outbound.purpose == mk_core::OutboundPurpose::Commit)
            .expect("re-offered");
        let resolved = a
            .session
            .report_publish(&retried.id, &relay.publish(&retried))
            .await
            .expect("report");
        assert_eq!(
            resolved,
            PublishResolution::Confirmed,
            "acceptance after not_sent confirms"
        );
        assert_eq!(snapshot(&a, &gid).epoch, epoch_before + 1);
    }

    /// An application message whose targets all report `not_sent` stays
    /// outstanding — never dropped — and is re-offered after the backoff.
    #[tokio::test(flavor = "current_thread")]
    async fn not_sent_message_stays_outstanding() {
        let relay = FakeRelay::new();
        let mut a = open_member().await;
        let mut b = open_member().await;
        publish_identity(&mut a, &relay, RELAY).await;
        publish_identity(&mut b, &relay, RELAY).await;
        let gid = group_with(&mut a, &relay, &[&b], "not-sent-msg").await;
        join_via_welcome(&mut b, &relay).await;
        sync_member(&mut a, &relay, RELAY, &gid).await;

        a.session
            .send_message(
                &gid,
                mk_core::AppMessage {
                    kind: 9,
                    tags: Vec::new(),
                    content: "held".to_owned(),
                },
            )
            .await
            .expect("send");
        if let Some(seq) = a.session.awaiting_durability() {
            a.session.acknowledge_durable(seq);
        }
        let item = a
            .session
            .take_outbound()
            .expect("take")
            .into_iter()
            .find(|item| item.purpose == mk_core::OutboundPurpose::ApplicationMessage)
            .expect("message staged");
        let results: Vec<RelayResult> = item
            .relays
            .iter()
            .map(|target| RelayResult {
                relay: target.clone(),
                status: RelayStatus::NotSent,
            })
            .collect();
        let resolution = a
            .session
            .report_publish(&item.id, &results)
            .await
            .expect("report");
        assert_eq!(
            resolution,
            PublishResolution::Pending,
            "a not_sent message stays outstanding"
        );
        a.clock.advance(1_000);
        let retried = a
            .session
            .take_outbound()
            .expect("take")
            .into_iter()
            .find(|outbound| outbound.purpose == mk_core::OutboundPurpose::ApplicationMessage)
            .expect("the message is re-offered, not dropped");
        let resolved = a
            .session
            .report_publish(&retried.id, &relay.publish(&retried))
            .await
            .expect("report");
        assert_eq!(resolved, PublishResolution::Complete);
    }

    fn app_message(content: &str) -> mk_core::AppMessage {
        mk_core::AppMessage {
            kind: 9,
            tags: Vec::new(),
            content: content.to_owned(),
        }
    }

    /// The engine's cached `MlsGroup` survives every step that writes no
    /// `OpenMLS` rows — the send, its publish, the sender's own-echo ingest,
    /// and a peer's application message — counted at the one `OpenMLS` read
    /// only `MlsGroup::load` performs.
    #[tokio::test(flavor = "current_thread")]
    async fn unchanged_openmls_rows_do_not_reload_the_group() {
        let relay = FakeRelay::new();
        let mut a = open_member().await;
        let mut b = open_member().await;
        publish_identity(&mut a, &relay, RELAY).await;
        publish_identity(&mut b, &relay, RELAY).await;
        let gid = group_with(&mut a, &relay, &[&b], "cache-flow").await;
        join_via_welcome(&mut b, &relay).await;
        sync_member(&mut a, &relay, RELAY, &gid).await;
        sync_member(&mut b, &relay, RELAY, &gid).await;

        // Warm up: one send → publish → own-echo ingest → peer delivery
        // leaves both engines holding their cached groups.
        send_chat(&mut a, &relay, &gid, "warm").await;
        let route = a.route_hex(&gid);
        ingest_backlog(&mut a, &group_events(&relay, RELAY, &route)).await;
        let delivered = ingest_backlog(&mut b, &group_events(&relay, RELAY, &route)).await;
        assert!(received(&delivered).contains(&"warm".to_owned()));

        let loads = mk_core::testing::openmls_group_loads();
        macro_rules! assert_no_load {
            ($step:literal) => {
                assert_eq!(
                    mk_core::testing::openmls_group_loads(),
                    loads,
                    "{} reloaded the group ({} -> {})",
                    $step,
                    loads,
                    mk_core::testing::openmls_group_loads()
                );
            };
        }

        // (1) A second send writes message/outbound rows only.
        a.session
            .send_message(&gid, app_message("second"))
            .await
            .expect("send_message");
        assert_no_load!("send_message");

        // (2) Publishing it resolves without touching `OpenMLS` state.
        let resolution =
            publish_one(&mut a, &relay, mk_core::OutboundPurpose::ApplicationMessage).await;
        assert_eq!(resolution, PublishResolution::Complete);
        assert_no_load!("publish/report");

        // (3) A ingests its own echo, then sends: still nothing to reload.
        ingest_backlog(&mut a, &group_events(&relay, RELAY, &route)).await;
        a.session
            .send_message(&gid, app_message("third"))
            .await
            .expect("send_message");
        assert_no_load!("own-echo ingest + send");

        // (4) B sends; A ingests B's application message, then sends.
        send_chat(&mut b, &relay, &gid, "from-b").await;
        let inbound = ingest_backlog(&mut a, &group_events(&relay, RELAY, &route)).await;
        assert!(received(&inbound).contains(&"from-b".to_owned()));
        a.session
            .send_message(&gid, app_message("fourth"))
            .await
            .expect("send_message");
        assert_no_load!("peer-message ingest + send");

        // B's first inbound batch ("second") and its second consecutive
        // inbound ("third"/"fourth") add no load either.
        let first = ingest_backlog(&mut b, &group_events(&relay, RELAY, &route)).await;
        assert!(received(&first).contains(&"second".to_owned()));
        flush(&mut a, &relay).await;
        let second = ingest_backlog(&mut b, &group_events(&relay, RELAY, &route)).await;
        let got = received(&second);
        assert!(
            got.contains(&"third".to_owned()) && got.contains(&"fourth".to_owned()),
            "B receives both of A's later sends: {got:?}"
        );
        assert_no_load!("B's consecutive inbound application messages");
    }

    /// A commit — here a profile rename — moves the generation and evicts
    /// the cached `MlsGroup`; the next send and the peer's decrypt across
    /// the commit boundary stay coherent.
    #[tokio::test(flavor = "current_thread")]
    async fn send_after_commit_stays_coherent() {
        let relay = FakeRelay::new();
        let mut a = open_member().await;
        let mut b = open_member().await;
        publish_identity(&mut a, &relay, RELAY).await;
        publish_identity(&mut b, &relay, RELAY).await;
        let gid = group_with(&mut a, &relay, &[&b], "commit-flow").await;
        join_via_welcome(&mut b, &relay).await;
        sync_member(&mut a, &relay, RELAY, &gid).await;
        sync_member(&mut b, &relay, RELAY, &gid).await;
        send_chat(&mut a, &relay, &gid, "pre").await;
        sync_member(&mut b, &relay, RELAY, &gid).await;

        let loads = mk_core::testing::openmls_group_loads();
        a.session
            .update_group_profile(&gid, Some("renamed".to_owned()), None)
            .await
            .expect("update_group_profile");
        let resolution = publish_one(&mut a, &relay, mk_core::OutboundPurpose::Commit).await;
        assert_eq!(resolution, PublishResolution::Confirmed);
        let commit_loads = mk_core::testing::openmls_group_loads() - loads;
        eprintln!("group loads across staged+confirmed commit: {commit_loads}");

        send_chat(&mut a, &relay, &gid, "post-rename").await;
        let b_route = b.route_hex(&gid);
        let delivered = ingest_backlog(&mut b, &group_events(&relay, RELAY, &b_route)).await;
        let got = received(&delivered);
        assert!(
            got.contains(&"post-rename".to_owned()),
            "B decrypts across the commit: {got:?}"
        );
        assert_eq!(snapshot(&b, &gid).name, "renamed");
        assert_eq!(
            snapshot(&a, &gid).epoch,
            snapshot(&b, &gid).epoch,
            "both sides land on the post-commit epoch"
        );
    }

    #[tokio::test]
    async fn founding_includes_own_devices() {
        let relay = FakeRelay::new();
        let signer: Arc<dyn AccountSigner> =
            Arc::new(mk_core::testing::LocalSigner::from_secret([9u8; 32]).expect("signer"));
        let mut a1 = open_member_with(Arc::clone(&signer)).await;
        let mut a2 = open_member_with(Arc::clone(&signer)).await;
        let mut a3 = open_member_with(signer).await;
        let mut b = open_member().await;
        publish_identity(&mut a1, &relay, RELAY).await;
        publish_identity(&mut a2, &relay, RELAY).await;
        publish_identity(&mut a3, &relay, RELAY).await;
        publish_identity(&mut b, &relay, RELAY).await;
        let account_hex = a1.account_hex();
        let slot_of = |member: &Member| {
            hex::encode(member.session.key_package_status().expect("status").slot)
        };
        let kp_of = |slot: &str| -> mk_core::Event {
            relay
                .fetch(RELAY, {
                    let wanted = account_hex.clone();
                    move |event| event.kind == 30_443 && event.pubkey.to_hex() == wanted
                })
                .into_iter()
                .find(|event| {
                    event
                        .tags
                        .iter()
                        .find(|tag| tag.first().map(String::as_str) == Some("d"))
                        .and_then(|tag| tag.get(1))
                        .map(String::as_str)
                        == Some(slot)
                })
                .expect("key package for slot")
        };
        let a1_kp = kp_of(&slot_of(&a1));
        let a2_kp = kp_of(&slot_of(&a2));
        let a3_kp = kp_of(&slot_of(&a3));
        let b_kp = key_package_of(&relay, RELAY, &b.account_hex());
        let invitee = |key_package: mk_core::Event| Invitee {
            key_package,
            inbox_relays: vec![RELAY.to_owned()],
        };
        let create = |invitees: Vec<Invitee>, own_devices: Vec<Invitee>| CreateGroup {
            name: "multi-device".to_owned(),
            description: String::new(),
            relays: vec![RELAY.to_owned()],
            invitees,
            own_devices,
            admins: Vec::new(),
            components: Vec::new(),
        };

        // A same-account KeyPackage in `invitees` is refused before
        // anything stages, and so is this device's own slot in
        // `own_devices`.
        let error = a1
            .session
            .create_group(create(
                vec![invitee(b_kp.clone()), invitee(a2_kp.clone())],
                Vec::new(),
            ))
            .await
            .unwrap_err();
        assert!(matches!(error, SessionError::InvalidInput(_)));
        let own_slot_error = a1
            .session
            .create_group(create(
                vec![invitee(b_kp.clone())],
                vec![invitee(a1_kp.clone())],
            ))
            .await
            .unwrap_err();
        assert!(matches!(own_slot_error, SessionError::InvalidInput(_)));
        assert!(
            a1.session.take_outbound().expect("outbound").is_empty(),
            "refused creates stage nothing"
        );

        // Founding with the sibling slot: a2 joins from its own Welcome
        // and the roster aggregates the account's two leaves.
        let gid = a1
            .session
            .create_group(create(vec![invitee(b_kp)], vec![invitee(a2_kp)]))
            .await
            .expect("create_group");
        flush(&mut a1, &relay).await;
        join_via_welcome(&mut a2, &relay).await;
        let account = a1.session.account();
        assert_eq!(
            snapshot(&a1, &gid)
                .members
                .iter()
                .find(|member| member.account == account)
                .map(|member| member.devices),
            Some(2)
        );

        // A third same-account device cannot be invited after founding;
        // the refusal stages nothing.
        let invite_error = a1
            .session
            .invite(&gid, vec![invitee(a3_kp)])
            .await
            .unwrap_err();
        assert!(matches!(invite_error, SessionError::InvalidInput(_)));
        assert!(
            a1.session.take_outbound().expect("outbound").is_empty(),
            "refused invite stages nothing"
        );
    }
}
