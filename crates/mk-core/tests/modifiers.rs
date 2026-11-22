//! Delivered-index session flow: kind-1009/5 modifiers authorizing against
//! the durable `DeliveredMessage` index, bounded in-memory holds, the
//! retention sweep, and the per-group send pacing allocator — all through
//! the public `Session` API on the in-process harness. Requires the
//! `testing` feature.

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
    clippy::too_many_lines,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
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
mod modifier_tests {
    use super::support::*;
    use std::collections::BTreeSet;

    use mk_core::media::EncryptedMediaPolicy;
    use mk_core::{
        AppMessage, AttachmentInput, Clock, DropReason, EventId, GroupId, MediaMessage,
        MessageIntent, ReceivedMessage, Semantics, SendOutcome, SentMessage, SessionError,
        SessionEvent,
    };

    const RELAY: &str = "ws://127.0.0.1:7777";

    /// Table byte of `PendingAppEvent` rows (`keys.rs`).
    const PENDING_APP_EVENT_TABLE: u8 = 6;
    /// Table byte of `MediaSecret` rows (`keys.rs`).
    const MEDIA_SECRET_TABLE: u8 = 36;
    /// Table byte of `DeliveredMessage` rows (`keys.rs`).
    const DELIVERED_TABLE: u8 = 40;
    /// Table byte of `SendPacing` rows (`keys.rs`).
    const PACING_TABLE: u8 = 41;

    /// The modifier hold window (`delivered.rs::HOLD_WINDOW_MS`).
    const HOLD_WINDOW_MS: u64 = 300_000;

    /// Two members with published identities in a joined, settled group:
    /// `a` is the creator (admin), `b` is not.
    async fn pair(name: &str) -> (FakeRelay, Member, Member, GroupId) {
        let relay = FakeRelay::new();
        let mut a = open_member().await;
        let mut b = open_member().await;
        publish_identity(&mut a, &relay, RELAY).await;
        publish_identity(&mut b, &relay, RELAY).await;
        let gid = group_with(&mut a, &relay, RELAY, &[&b], name).await;
        join_via_welcome(&mut b, &relay, RELAY).await;
        sync_member(&mut a, &relay, RELAY, &gid).await;
        sync_member(&mut b, &relay, RELAY, &gid).await;
        (relay, a, b, gid)
    }

    /// One ingest + tick + flush round over the group's backlog.
    async fn cycle(member: &mut Member, relay: &FakeRelay, route: &str) -> Vec<SessionEvent> {
        let mut all = ingest_backlog(member, &group_events(relay, RELAY, route)).await;
        member.session.tick().await.expect("tick");
        member.clock.advance(2_000);
        flush(member, relay).await;
        all.extend(member.session.drain_events().unwrap());
        all
    }

    /// A few `cycle` rounds — enough for commits, queued intents and
    /// deferred convergence to settle.
    async fn collect(member: &mut Member, relay: &FakeRelay, route: &str) -> Vec<SessionEvent> {
        let mut all = Vec::new();
        for _ in 0..6 {
            all.extend(cycle(member, relay, route).await);
        }
        all
    }

    /// Send a typed intent and publish it. Advances the clock like
    /// `send_chat` so `created_at` reflects send order.
    async fn send_typed(
        member: &mut Member,
        relay: &FakeRelay,
        group: &GroupId,
        intent: MessageIntent,
    ) -> SentMessage {
        member.clock.advance(1_000);
        let sent = member
            .session
            .send_intent(group, intent)
            .await
            .expect("send_intent");
        flush(member, relay).await;
        sent
    }

    /// Ingest `events` and drain without acknowledging — the hold/restart
    /// tests need the pending rows to stay.
    async fn ingest_no_ack(member: &mut Member, events: Vec<mk_core::Event>) -> Vec<SessionEvent> {
        member
            .session
            .ingest_batch(events)
            .await
            .expect("ingest_batch");
        member.session.drain_events().unwrap()
    }

    /// The drained `MessageReceived` whose content equals `want`.
    fn message_with<'a>(events: &'a [SessionEvent], want: &str) -> &'a ReceivedMessage {
        events
            .iter()
            .find_map(|event| match event {
                SessionEvent::MessageReceived { message, .. } if message.content == want => {
                    Some(message)
                }
                _ => None,
            })
            .unwrap_or_else(|| panic!("no MessageReceived carrying {want:?}"))
    }

    /// Every drained `AppEventDropped` reason.
    fn dropped_reasons(events: &[SessionEvent]) -> Vec<DropReason> {
        events
            .iter()
            .filter_map(|event| match event {
                SessionEvent::AppEventDropped { reason, .. } => Some(*reason),
                _ => None,
            })
            .collect()
    }

    /// The transport (kind-445) event id that wrapped the inner event
    /// `created` — the outer event carries the app's `created_at`.
    fn transport_id_for(relay: &FakeRelay, route: &str, created: u64) -> EventId {
        group_events(relay, RELAY, route)
            .into_iter()
            .find(|event| event.created_at == created)
            .expect("a group message at that created_at")
            .id
    }

    /// Every `PendingAppEvent` id: `[6][u32 len][mid]` — gone after a drop.
    fn pending_mids(member: &Member) -> BTreeSet<[u8; 32]> {
        member
            .host
            .durable_rows()
            .iter()
            .filter(|(key, _)| key.first() == Some(&PENDING_APP_EVENT_TABLE))
            .map(|(key, _)| {
                <[u8; 32]>::try_from(&key[key.len() - 32..]).expect("pending key ends in mid")
            })
            .collect()
    }

    /// A `PendingAppEvent` row exists for `transport_id`.
    fn pending_row_exists(member: &Member, transport_id: &EventId) -> bool {
        pending_mids(member).contains(&transport_id.0)
    }

    /// A `MediaSecret` row's last segment is the transport id.
    fn media_secret_row_exists(member: &Member, transport_id: &EventId) -> bool {
        member.host.durable_rows().iter().any(|(key, _)| {
            key.first() == Some(&MEDIA_SECRET_TABLE) && key.ends_with(&transport_id.0)
        })
    }

    /// Every inner event id the member's `DeliveredMessage` rows index:
    /// each key is `[40][len][gid][len][inner]` — the last 32 bytes.
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

    /// `EventId` from a received message's hex inner id.
    fn inner_of(message: &ReceivedMessage) -> EventId {
        EventId(
            <[u8; 32]>::try_from(hex::decode(&message.id).expect("inner hex")).expect("32 bytes"),
        )
    }

    /// The member's single `SendPacing` row exists (one group).
    fn pacing_row_exists(member: &Member) -> bool {
        member
            .host
            .durable_rows()
            .iter()
            .any(|(key, _)| key.first() == Some(&PACING_TABLE))
    }

    /// No `MessageReceived` carrying `kinds` and no `AppEventDropped` —
    /// "the modifier is still held".
    fn assert_still_held(events: &[SessionEvent], kinds: &[u64]) {
        assert!(
            !events.iter().any(|event| matches!(
                event,
                SessionEvent::MessageReceived { message, .. } if kinds.contains(&message.kind)
            )),
            "a held modifier must not surface: {events:?}"
        );
        assert!(
            dropped_reasons(events).is_empty(),
            "a held modifier must not drop: {:?}",
            dropped_reasons(events)
        );
    }

    /// Send `n` plain kind-9 messages on the member's current (frozen)
    /// clock, returning each `SentMessage` — no clock advance, so the
    /// pacing allocator spreads them across seconds itself.
    async fn send_burst(
        member: &mut Member,
        group: &GroupId,
        n: usize,
        prefix: &str,
    ) -> Vec<SentMessage> {
        let mut sent = Vec::with_capacity(n);
        for i in 0..n {
            sent.push(
                member
                    .session
                    .send_message(
                        group,
                        AppMessage {
                            kind: 9,
                            tags: Vec::new(),
                            content: format!("{prefix}{i:04}"),
                        },
                    )
                    .await
                    .expect("send_message"),
            );
        }
        sent
    }

    // --- authorized modifiers deliver ---

    /// An edit of an indexed chat and a non-admin kind-5 delete of an
    /// indexed chat both deliver with typed semantics. The sender's own
    /// message writes a `DeliveredMessage` row — the row a sibling device
    /// of the same account would authorize an edit against. (The harness
    /// cannot seat two devices of one account in a group — member ids are
    /// account ids — so the sibling half is pinned by own-row presence
    /// here and by `semantics::resolve` author == sender.)
    #[tokio::test(flavor = "current_thread")]
    async fn edit_and_delete_deliver_against_known_target() {
        let (relay, mut a, mut b, gid) = pair("modifiers").await;
        let route_a = a.route_hex(&gid);
        let route_b = b.route_hex(&gid);

        let target = send_typed(
            &mut a,
            &relay,
            &gid,
            MessageIntent::Chat {
                content: "edit me".to_owned(),
                reply_to: None,
            },
        )
        .await;
        let target_id = target.message.id();
        collect(&mut b, &relay, &route_b).await;

        // A's own send is indexed on A: keyed group/inner-id, the row a
        // same-account modifier would authorize against.
        assert!(
            delivered_inners(&a).contains(&target_id.0),
            "A's own sent message is indexed"
        );
        assert!(pacing_row_exists(&a), "A's send wrote the pacing row");

        // A edits its own chat: kind 1009, one e tag — authorized at B.
        let edit = send_typed(
            &mut a,
            &relay,
            &gid,
            MessageIntent::Edit {
                target: target_id,
                content: "edited".to_owned(),
            },
        )
        .await;
        assert_eq!(edit.message.kind, 1009);
        let b_events = collect(&mut b, &relay, &route_b).await;
        assert!(
            b_events.iter().any(|event| matches!(
                event,
                SessionEvent::MessageReceived { message, .. } if message.kind == 1009
                    && matches!(
                        &message.semantics,
                        Semantics::Edit { target, content }
                            if *target == target_id && content == "edited"
                    )
            )),
            "B sees the authorized edit typed: {b_events:?}"
        );

        // B (non-admin) deletes its own chat: kind 5 at the receiver —
        // honoured target, `Semantics::Deletion`.
        let own = send_typed(
            &mut b,
            &relay,
            &gid,
            MessageIntent::Chat {
                content: "delete mine".to_owned(),
                reply_to: None,
            },
        )
        .await;
        let own_id = own.message.id();
        collect(&mut a, &relay, &route_a).await;
        let delete = send_typed(
            &mut b,
            &relay,
            &gid,
            MessageIntent::Delete { target: own_id },
        )
        .await;
        assert_eq!(delete.message.kind, 5);
        let a_events = collect(&mut a, &relay, &route_a).await;
        assert!(
            a_events.iter().any(|event| matches!(
                event,
                SessionEvent::MessageReceived { message, .. } if message.kind == 5
                    && matches!(
                        &message.semantics,
                        Semantics::Deletion { targets } if targets == &[own_id]
                    )
            )),
            "A sees the authorized deletion typed: {a_events:?}"
        );
    }

    /// A kind-1009 edit by another account resolves `unauthorized` — the
    /// target is known and foreign — and releases its pending row.
    #[tokio::test(flavor = "current_thread")]
    async fn foreign_edit_drops_unauthorized() {
        let (relay, mut a, mut b, gid) = pair("modifier-drop").await;
        let route_a = a.route_hex(&gid);
        let route_b = b.route_hex(&gid);

        let target = send_typed(
            &mut a,
            &relay,
            &gid,
            MessageIntent::Chat {
                content: "not yours".to_owned(),
                reply_to: None,
            },
        )
        .await;
        collect(&mut b, &relay, &route_b).await;

        let edit = send_typed(
            &mut b,
            &relay,
            &gid,
            MessageIntent::Edit {
                target: target.message.id(),
                content: "spoofed".to_owned(),
            },
        )
        .await;
        assert_eq!(edit.message.kind, 1009);
        let before = pending_mids(&a);
        let a_events = collect(&mut a, &relay, &route_a).await;
        assert!(
            dropped_reasons(&a_events).contains(&DropReason::Unauthorized),
            "foreign edit drops unauthorized: {:?}",
            dropped_reasons(&a_events)
        );
        assert!(
            !a_events.iter().any(|event| matches!(
                event,
                SessionEvent::MessageReceived { message, .. } if message.kind == 1009
            )),
            "the spoofed edit is never delivered"
        );
        assert_eq!(
            pending_mids(&a),
            before,
            "the drop released the pending row"
        );
    }

    // --- holds ---

    /// A kind-1009 edit ingested before its target holds silently; the
    /// target's arrival releases it with `Semantics::Edit`.
    #[tokio::test(flavor = "current_thread")]
    async fn held_modifier_releases_when_target_arrives() {
        let (relay, mut a, mut b, gid) = pair("modifier-hold").await;
        let route_b = b.route_hex(&gid);

        let target = send_typed(
            &mut a,
            &relay,
            &gid,
            MessageIntent::Chat {
                content: "first".to_owned(),
                reply_to: None,
            },
        )
        .await;
        let edit = send_typed(
            &mut a,
            &relay,
            &gid,
            MessageIntent::Edit {
                target: target.message.id(),
                content: "edited".to_owned(),
            },
        )
        .await;
        let target_id = target.message.id();

        // The backlog is newest-first: [edit, chat, ...] — feed B the edit
        // alone first.
        let backlog = group_events(&relay, RELAY, &route_b);
        assert_eq!(backlog[0].created_at, edit.message.created_at);
        let (edit_event, rest) = (
            backlog[0].clone(),
            backlog[1..]
                .iter()
                .filter(|event| event.created_at == target.message.created_at)
                .cloned()
                .collect::<Vec<_>>(),
        );
        let held = ingest_no_ack(&mut b, vec![edit_event]).await;
        assert_still_held(&held, &[1009]);

        // The target lands: chat first, then the released edit.
        let released = ingest_no_ack(&mut b, rest).await;
        let msgs: Vec<&ReceivedMessage> = released
            .iter()
            .filter_map(|event| match event {
                SessionEvent::MessageReceived { message, .. } => Some(message),
                _ => None,
            })
            .collect();
        assert!(
            msgs.iter().any(|message| matches!(
                &message.semantics,
                Semantics::Edit { target, .. } if *target == target_id
            )),
            "the held edit released typed: {released:?}"
        );
        let positions: Vec<u64> = msgs.iter().map(|m| m.kind).collect();
        let chat_at = positions.iter().position(|k| *k == 9).expect("chat");
        let edit_at = positions.iter().position(|k| *k == 1009).expect("edit");
        assert!(chat_at < edit_at, "target surfaces before its edit");
    }

    /// The restart case: B ingests the edit (held), reopens from durable
    /// rows — the pending row re-presents the modifier and re-holds it —
    /// then the target arrives and releases it; after a final restart the
    /// acked pair is not re-presented.
    #[tokio::test(flavor = "current_thread")]
    async fn held_modifier_releases_across_restart() {
        let (relay, mut a, mut b, gid) = pair("modifier-restart").await;
        let route_b = b.route_hex(&gid);

        let target = send_typed(
            &mut a,
            &relay,
            &gid,
            MessageIntent::Chat {
                content: "restart target".to_owned(),
                reply_to: None,
            },
        )
        .await;
        let edit = send_typed(
            &mut a,
            &relay,
            &gid,
            MessageIntent::Edit {
                target: target.message.id(),
                content: "restart edited".to_owned(),
            },
        )
        .await;
        let target_id = target.message.id();

        let backlog = group_events(&relay, RELAY, &route_b);
        let edit_event = backlog[0].clone();
        let target_event = backlog[1].clone();
        assert_eq!(edit_event.created_at, edit.message.created_at);
        assert_eq!(target_event.created_at, target.message.created_at);

        // Held, restart, re-held — nothing surfaced across the boundary.
        let held = ingest_no_ack(&mut b, vec![edit_event]).await;
        assert_still_held(&held, &[1009]);
        let mut b = reopen_member(&b).await;
        let reheld = b.session.drain_events().unwrap();
        assert_still_held(&reheld, &[1009]);

        // The target arrives: chat surfaces, then the released edit.
        let released = ingest_no_ack(&mut b, vec![target_event]).await;
        let kinds: Vec<u64> = released
            .iter()
            .filter_map(|event| match event {
                SessionEvent::MessageReceived { message, .. } => Some(message.kind),
                _ => None,
            })
            .collect();
        assert_eq!(
            kinds,
            vec![9, 1009],
            "chat then released edit: {released:?}"
        );
        assert!(
            released.iter().any(|event| matches!(
                event,
                SessionEvent::MessageReceived { message, .. }
                    if matches!(&message.semantics, Semantics::Edit { target, .. } if *target == target_id)
            )),
            "the released edit carries typed semantics"
        );
        let ids: Vec<EventId> = released
            .iter()
            .filter_map(|event| match event {
                SessionEvent::MessageReceived { message, .. } => Some(message.transport_id),
                _ => None,
            })
            .collect();
        b.session.acknowledge_messages(&ids).expect("ack");

        // Acked pair: another restart re-presents nothing.
        let mut b = reopen_member(&b).await;
        let tail = b.session.drain_events().unwrap();
        assert!(
            !tail.iter().any(|event| matches!(
                event,
                SessionEvent::MessageReceived { .. } | SessionEvent::AppEventDropped { .. }
            )),
            "nothing re-presents after ack + restart: {tail:?}"
        );
    }

    /// The chain case: a held kind 5 voiding a held edit — B ingests the
    /// delete and the edit (both held across a restart), then the target
    /// chat arrives: it indexes, releases the edit, and the edit's index
    /// row releases the delete — a release-loop fixpoint.
    #[tokio::test(flavor = "current_thread")]
    async fn held_delete_voids_held_edit_across_restart() {
        let (relay, mut a, mut b, gid) = pair("modifier-chain").await;
        let route_b = b.route_hex(&gid);

        let target = send_typed(
            &mut a,
            &relay,
            &gid,
            MessageIntent::Chat {
                content: "chain target".to_owned(),
                reply_to: None,
            },
        )
        .await;
        let edit = send_typed(
            &mut a,
            &relay,
            &gid,
            MessageIntent::Edit {
                target: target.message.id(),
                content: "voided edit".to_owned(),
            },
        )
        .await;
        let edit_id = edit.message.id();
        // A kind-5 (Unreact shape) voiding the edit itself — kind 1009 is
        // an honoured delete target.
        let voiding = send_typed(
            &mut a,
            &relay,
            &gid,
            MessageIntent::Unreact {
                reactions: vec![edit_id],
            },
        )
        .await;
        assert_eq!(voiding.message.kind, 5);

        let backlog = group_events(&relay, RELAY, &route_b);
        // Newest-first: [kind5, edit, chat, ...].
        let (voiding_event, edit_event, target_event) =
            (backlog[0].clone(), backlog[1].clone(), backlog[2].clone());
        assert_eq!(voiding_event.created_at, voiding.message.created_at);
        assert_eq!(edit_event.created_at, edit.message.created_at);
        assert_eq!(target_event.created_at, target.message.created_at);

        // Both modifiers hold — the delete's target (the edit) and the
        // edit's target (the chat) are both unknown — across a restart.
        let held = ingest_no_ack(&mut b, vec![voiding_event, edit_event]).await;
        assert_still_held(&held, &[5, 1009]);
        let mut b = reopen_member(&b).await;
        let reheld = b.session.drain_events().unwrap();
        assert_still_held(&reheld, &[5, 1009]);

        // The chat indexes, releases the held edit, and the edit's index
        // row releases the held delete: chat, edit, deletion in one drain.
        let released = ingest_no_ack(&mut b, vec![target_event]).await;
        let kinds: Vec<u64> = released
            .iter()
            .filter_map(|event| match event {
                SessionEvent::MessageReceived { message, .. } => Some(message.kind),
                _ => None,
            })
            .collect();
        assert_eq!(
            kinds,
            vec![9, 1009, 5],
            "chat, released edit, released deletion: {released:?}"
        );
        assert!(
            released.iter().any(|event| matches!(
                event,
                SessionEvent::MessageReceived { message, .. } if message.kind == 5
                    && matches!(
                        &message.semantics,
                        Semantics::Deletion { targets } if targets == &[edit_id]
                    )
            )),
            "the chain releases the kind-5 as a Deletion voiding the edit"
        );
    }

    /// A held modifier whose window closes resolves `unverifiable` inside
    /// `tick`, releases its pending row, folds its deadline into
    /// `next_wake_ms`, and is not re-presented after restart.
    #[tokio::test(flavor = "current_thread")]
    async fn held_modifier_expires_via_tick() {
        let (relay, mut a, mut b, gid) = pair("modifier-expiry").await;
        let route_b = b.route_hex(&gid);

        let fake = EventId([0xee; 32]);
        send_typed(
            &mut a,
            &relay,
            &gid,
            MessageIntent::Edit {
                target: fake,
                content: "never resolves".to_owned(),
            },
        )
        .await;
        let before = pending_mids(&b);
        let backlog = group_events(&relay, RELAY, &route_b);
        let held = ingest_no_ack(&mut b, vec![backlog[0].clone()]).await;
        assert_still_held(&held, &[1009]);
        let new: Vec<[u8; 32]> = pending_mids(&b).difference(&before).copied().collect();
        assert_eq!(new.len(), 1, "the held modifier keeps one pending row");
        let transport_id = EventId(new[0]);

        // The hold's deadline folds into the wake.
        let report = b.session.tick().await.expect("tick");
        assert!(
            report
                .next_wake_ms
                .is_some_and(|wake| wake <= HOLD_WINDOW_MS),
            "the hold deadline folds into next_wake_ms: {:?}",
            report.next_wake_ms
        );

        // Past the window: the modifier resolves unverifiable, releases
        // its pending row, and does not come back.
        b.clock.advance(HOLD_WINDOW_MS + 1_000);
        b.session.tick().await.expect("tick");
        let expired = b.session.drain_events().unwrap();
        assert!(
            dropped_reasons(&expired).contains(&DropReason::Unverifiable),
            "an expired hold drops unverifiable: {:?}",
            dropped_reasons(&expired)
        );
        assert!(
            !pending_row_exists(&b, &transport_id),
            "the drop released the pending row"
        );
        let mut b = reopen_member(&b).await;
        let tail = b.session.drain_events().unwrap();
        assert!(
            !tail.iter().any(|event| matches!(
                event,
                SessionEvent::MessageReceived { message, .. } if message.kind == 1009
            )),
            "the expired modifier is not re-presented: {tail:?}"
        );
    }

    /// Beyond 128 held modifiers the queue evicts the oldest with a final
    /// resolution — all-unknown targets drop `unverifiable`; the surviving
    /// 128 re-hold across a restart without dropping.
    #[tokio::test(flavor = "current_thread")]
    async fn hold_queue_overflow_drops_oldest() {
        let (relay, mut a, mut b, gid) = pair("modifier-overflow").await;
        let route_a = a.route_hex(&gid);

        // 129 kind-5 modifiers, each targeting a distinct unknown id.
        for i in 0u8..129 {
            send_typed(
                &mut b,
                &relay,
                &gid,
                MessageIntent::Unreact {
                    reactions: vec![EventId([i.wrapping_add(1); 32])],
                },
            )
            .await;
        }
        let a_events = collect(&mut a, &relay, &route_a).await;
        assert_eq!(
            dropped_reasons(&a_events),
            vec![DropReason::Unverifiable],
            "exactly the oldest held modifier drops: {:?}",
            dropped_reasons(&a_events)
        );
        assert!(
            !a_events.iter().any(|event| matches!(
                event,
                SessionEvent::MessageReceived { message, .. } if message.kind == 5
            )),
            "no kind 5 delivers"
        );

        // The surviving 128 re-present from pending rows after restart and
        // re-hold — no further drops, no deliveries.
        let mut a = reopen_member(&a).await;
        let tail = a.session.drain_events().unwrap();
        assert!(
            dropped_reasons(&tail).is_empty(),
            "re-held modifiers do not drop on replay: {:?}",
            dropped_reasons(&tail)
        );
        assert_still_held(&tail, &[5]);
    }

    /// Held bytes are bounded too: five ~1 MiB edits exceed the 4 MiB
    /// group cap, so the fifth push resolves the oldest `unverifiable`
    /// while the four still-fitting modifiers stay held.
    #[tokio::test(flavor = "current_thread")]
    async fn hold_queue_byte_cap_evicts_oldest() {
        let (relay, mut a, mut b, gid) = pair("modifier-byte-cap").await;
        let route_a = a.route_hex(&gid);

        // Five kind-1009 edits of ~1 MB each (plus tag bytes), distinct
        // unknown targets: four fit under the 4 MiB cap, the fifth evicts.
        for i in 0u8..5 {
            send_typed(
                &mut b,
                &relay,
                &gid,
                MessageIntent::Edit {
                    target: EventId([i.wrapping_add(1); 32]),
                    content: "x".repeat(1_000_000),
                },
            )
            .await;
        }
        let a_events = collect(&mut a, &relay, &route_a).await;
        assert_eq!(
            dropped_reasons(&a_events),
            vec![DropReason::Unverifiable],
            "exactly the oldest held modifier drops: {:?}",
            dropped_reasons(&a_events)
        );
        assert!(
            !a_events.iter().any(|event| matches!(
                event,
                SessionEvent::MessageReceived { message, .. } if message.kind == 1009
            )),
            "no kind 1009 delivers"
        );
    }

    /// A message delivered under a branch that later loses convergence is
    /// invalidated: the drain's `MessageInvalidated` also removes its
    /// `DeliveredMessage` row. Both forked branches carry a self-update
    /// and a chat message; the committer tie-break (smaller member id)
    /// delivers M in the first settle, then the rival deepens its parked
    /// branch by one epoch and the second pass withdraws M.
    #[tokio::test(flavor = "current_thread")]
    async fn invalidation_removes_index_row() {
        let relay = FakeRelay::new();
        let mut a = open_member().await;
        let mut b = open_member().await;
        let mut c = open_member().await;
        publish_identity(&mut a, &relay, RELAY).await;
        publish_identity(&mut b, &relay, RELAY).await;
        publish_identity(&mut c, &relay, RELAY).await;
        let gid = group_with(&mut a, &relay, RELAY, &[&b, &c], "invalidation").await;
        join_via_welcome(&mut b, &relay, RELAY).await;
        join_via_welcome(&mut c, &relay, RELAY).await;
        sync_member(&mut a, &relay, RELAY, &gid).await;
        sync_member(&mut b, &relay, RELAY, &gid).await;
        sync_member(&mut c, &relay, RELAY, &gid).await;
        let route_b = b.route_hex(&gid);

        // `first` holds the smaller member id: the same-epoch committer
        // tie-break winner whose branch carries M. `second` deepens its
        // parked branch later and wins the second pass.
        let (first, second): (&mut Member, &mut Member) =
            if c.signer.account().0 < a.signer.account().0 {
                (&mut c, &mut a)
            } else {
                (&mut a, &mut c)
            };

        // Both members fork the shared epoch with a self-update and a chat
        // message; neither ingests the rival's commit.
        first.session.self_update(&gid).await.expect("self_update");
        flush(first, &relay).await;
        let sent = send_typed(
            first,
            &relay,
            &gid,
            MessageIntent::Chat {
                content: "doomed".to_owned(),
                reply_to: None,
            },
        )
        .await;
        let inner_id = sent.message.id();
        second.session.self_update(&gid).await.expect("self_update");
        flush(second, &relay).await;
        send_typed(
            second,
            &relay,
            &gid,
            MessageIntent::Chat {
                content: "revived".to_owned(),
                reply_to: None,
            },
        )
        .await;

        // First settle: the tie-break winner's branch applies — M is
        // delivered and indexed while the rival's app stays parked.
        let delivered = collect(&mut b, &relay, &route_b).await;
        let transport_id = message_with(&delivered, "doomed").transport_id;
        assert!(
            delivered_inners(&b).contains(&inner_id.0),
            "M is indexed at B"
        );
        assert!(
            !delivered.iter().any(|event| matches!(
                event,
                SessionEvent::MessageReceived { message, .. } if message.content == "revived"
            )),
            "the parked branch's app is not delivered"
        );

        // `second` deepens its parked branch by one epoch: the next pass
        // adopts it and withdraws the delivered M.
        second
            .session
            .self_update(&gid)
            .await
            .expect("deepen self_update");
        flush(second, &relay).await;
        let mut events = Vec::new();
        for _ in 0..16 {
            events.extend(cycle(&mut b, &relay, &route_b).await);
            if events.iter().any(|event| {
                matches!(
                    event,
                    SessionEvent::MessageInvalidated { message, .. } if *message == transport_id
                )
            }) {
                break;
            }
        }
        assert!(
            events.iter().any(|event| matches!(
                event,
                SessionEvent::MessageInvalidated { message, .. } if *message == transport_id
            )),
            "the delivered message is invalidated when its branch loses: {events:?}"
        );
        assert!(
            !delivered_inners(&b).contains(&inner_id.0),
            "the invalidation removed the index row"
        );
    }

    // --- retention sweep ---

    /// Enable 60 s retention: admin commit, published and applied at B.
    async fn enable_retention(
        a: &mut Member,
        b: &mut Member,
        relay: &FakeRelay,
        gid: &GroupId,
        seconds: u64,
    ) {
        let outcome = a
            .session
            .set_retention(gid, seconds)
            .await
            .expect("set_retention");
        assert!(matches!(outcome, SendOutcome::Staged(_)), "{outcome:?}");
        if let Some(seq) = a.session.awaiting_durability() {
            a.session.acknowledge_durable(seq);
        }
        let items = a.session.take_outbound().expect("take_outbound");
        for item in &items {
            let results = relay.publish(item);
            a.session
                .report_publish(&item.id, &results)
                .await
                .expect("report_publish");
        }
        flush(a, relay).await;
        let route_b = b.route_hex(gid);
        collect(b, relay, &route_b).await;
    }

    /// A delivered message past its retention expiry is swept by `tick`:
    /// one `MessageExpired`, the index row gone, a second tick silent; the
    /// earliest expiry is the wake.
    #[tokio::test(flavor = "current_thread")]
    async fn retention_sweep_expires_delivered_row() {
        let (relay, mut a, mut b, gid) = pair("retention-sweep").await;
        let route_b = b.route_hex(&gid);
        enable_retention(&mut a, &mut b, &relay, &gid, 60).await;

        let sent = send_typed(
            &mut a,
            &relay,
            &gid,
            MessageIntent::Chat {
                content: "expires".to_owned(),
                reply_to: None,
            },
        )
        .await;
        let events = collect(&mut b, &relay, &route_b).await;
        let msg = message_with(&events, "expires");
        let inner_id = inner_of(msg);
        let transport_id = msg.transport_id;
        let expiry = msg.expires_at.expect("expires_at");
        assert_eq!(expiry, sent.message.created_at + 60);
        assert!(delivered_inners(&b).contains(&inner_id.0));

        // The earliest expiry is the next wake.
        let report = b.session.tick().await.expect("tick");
        let now_ms = b.clock.now().unix_ms;
        assert_eq!(
            report.next_wake_ms,
            Some(expiry.saturating_mul(1_000).saturating_sub(now_ms)),
            "wake is the earliest expiry delay"
        );

        // Past expiry: one MessageExpired, row swept, second tick silent.
        b.clock
            .advance(expiry.saturating_mul(1_000).saturating_sub(now_ms) + 1);
        b.session.tick().await.expect("tick");
        let drained = b.session.drain_events().unwrap();
        assert!(
            drained.iter().any(|event| matches!(
                event,
                SessionEvent::MessageExpired { message, transport_id: tid, .. }
                    if *message == inner_id && *tid == transport_id
            )),
            "the expired message is reported: {drained:?}"
        );
        assert!(
            !delivered_inners(&b).contains(&inner_id.0),
            "the index row is swept"
        );
        b.session.tick().await.expect("tick");
        let again = b.session.drain_events().unwrap();
        assert!(
            !again
                .iter()
                .any(|event| matches!(event, SessionEvent::MessageExpired { .. })),
            "a swept row is not reported twice: {again:?}"
        );
    }

    /// The expiry heap is memory-only: a restart before the sweep rebuilds
    /// it from the table on the first `tick` and still sweeps.
    #[tokio::test(flavor = "current_thread")]
    async fn retention_sweep_runs_after_restart() {
        let (relay, mut a, mut b, gid) = pair("retention-restart").await;
        let route_b = b.route_hex(&gid);
        enable_retention(&mut a, &mut b, &relay, &gid, 60).await;

        let sent = send_typed(
            &mut a,
            &relay,
            &gid,
            MessageIntent::Chat {
                content: "expires later".to_owned(),
                reply_to: None,
            },
        )
        .await;
        let events = collect(&mut b, &relay, &route_b).await;
        let msg = message_with(&events, "expires later");
        let inner_id = inner_of(msg);
        let expiry = msg.expires_at.expect("expires_at");
        let _ = sent;

        // Restart before the sweep: the lazily rebuilt heap still finds
        // the row.
        let mut b = reopen_member(&b).await;
        let now_secs = b.clock.now().unix_ms / 1_000;
        if expiry >= now_secs {
            b.clock.advance((expiry + 1 - now_secs) * 1_000);
        }
        b.session.tick().await.expect("tick");
        let drained = b.session.drain_events().unwrap();
        assert!(
            drained.iter().any(|event| matches!(
                event,
                SessionEvent::MessageExpired { message, .. } if *message == inner_id
            )),
            "the sweep runs after restart: {drained:?}"
        );
        assert!(!delivered_inners(&b).contains(&inner_id.0));
    }

    /// This device's own sent media message sweeps too: the index row AND
    /// its own media-secret row go together, after which `open_attachment`
    /// reports the secret unavailable.
    #[tokio::test(flavor = "current_thread")]
    async fn retention_sweep_removes_own_media_secret() {
        let relay = FakeRelay::new();
        let mut a = open_member().await;
        let mut b = open_member().await;
        publish_identity(&mut a, &relay, RELAY).await;
        publish_identity(&mut b, &relay, RELAY).await;
        let policy = EncryptedMediaPolicy::blossom(&["https://blossom.example/".to_owned()]);
        let gid = group_as(
            &mut a,
            &relay,
            RELAY,
            &[&b],
            "media-sweep",
            &[],
            vec![policy.to_component().expect("component")],
        )
        .await;
        join_via_welcome(&mut b, &relay, RELAY).await;
        sync_member(&mut a, &relay, RELAY, &gid).await;
        sync_member(&mut b, &relay, RELAY, &gid).await;
        enable_retention(&mut a, &mut b, &relay, &gid, 60).await;

        let sealed = a
            .session
            .seal_attachment(
                &gid,
                AttachmentInput {
                    plaintext: b"media".to_vec(),
                    media_type: "image/png".to_owned(),
                    filename: "m.png".to_owned(),
                    dim: None,
                    thumbhash: None,
                },
            )
            .expect("seal");
        let locators = vec![mk_core::media::Locator {
            kind: "blossom-v1".to_owned(),
            value: format!(
                "https://blossom.example/{}",
                hex::encode(sealed.ciphertext_sha256)
            ),
        }];
        let sent = a
            .session
            .send_media(
                &gid,
                MediaMessage {
                    caption: "gone soon".to_owned(),
                    attachments: vec![(sealed.pending, locators)],
                    tags: Vec::new(),
                },
            )
            .await
            .expect("send_media");
        flush(&mut a, &relay).await;
        let inner_id = sent.message.id();
        let transport_id = transport_id_for(&relay, &a.route_hex(&gid), sent.message.created_at);
        assert!(delivered_inners(&a).contains(&inner_id.0));
        assert!(media_secret_row_exists(&a, &transport_id));

        // A's own row carries the expiry: advancing past it sweeps the row
        // and the secret together.
        let now_secs = a.clock.now().unix_ms / 1_000;
        let expiry = sent.message.created_at + 60;
        if expiry >= now_secs {
            a.clock.advance((expiry + 1 - now_secs) * 1_000);
        }
        a.session.tick().await.expect("tick");
        let drained = a.session.drain_events().unwrap();
        assert!(
            drained.iter().any(|event| matches!(
                event,
                SessionEvent::MessageExpired { message, transport_id: tid, .. }
                    if *message == inner_id && *tid == transport_id
            )),
            "own media sweeps: {drained:?}"
        );
        assert!(!delivered_inners(&a).contains(&inner_id.0));
        assert!(!media_secret_row_exists(&a, &transport_id));
        // The secret lookup precedes hash verification, so a minimal
        // reference is enough to see `MediaSecretUnavailable`.
        let reference = mk_core::media::MediaReference {
            locators: Vec::new(),
            ciphertext_sha256: sealed.ciphertext_sha256,
            plaintext_sha256: [0; 32],
            nonce: [0; 12],
            media_type: "image/png".to_owned(),
            filename: "m.png".to_owned(),
            dim: None,
            thumbhash: None,
        };
        assert!(matches!(
            a.session
                .open_attachment(&gid, &transport_id, &reference, &sealed.ciphertext),
            Err(SessionError::MediaSecretUnavailable)
        ));
    }

    /// The sweep is bounded at 256 rows per `tick`: 300 due rows take two
    /// ticks (256 then 44), reporting `next_wake_ms == Some(0)` in between.
    #[tokio::test(flavor = "current_thread")]
    async fn retention_sweep_bounded_at_256() {
        let (relay, mut a, mut b, gid) = pair("retention-bounded").await;
        let route_b = b.route_hex(&gid);
        enable_retention(&mut a, &mut b, &relay, &gid, 60).await;

        // 300 sends on a frozen clock: pacing spreads them over ~6
        // seconds, each carrying expires_at = created_at + 60.
        let burst = send_burst(&mut a, &gid, 300, "exp-").await;
        flush(&mut a, &relay).await;
        let max_expiry = burst
            .iter()
            .map(|sent| sent.message.created_at)
            .max()
            .expect("burst")
            + 60;
        let events = collect(&mut b, &relay, &route_b).await;
        assert_eq!(
            events
                .iter()
                .filter(|event| matches!(event, SessionEvent::MessageReceived { .. }))
                .count(),
            300,
            "all 300 delivered"
        );
        assert_eq!(delivered_inners(&b).len(), 300, "all 300 indexed");

        // Everything due: the first tick sweeps the 256 bound and reports
        // an immediate re-wake; the second sweeps the remaining 44.
        let now_secs = b.clock.now().unix_ms / 1_000;
        b.clock
            .advance(max_expiry.saturating_sub(now_secs).saturating_add(1) * 1_000);
        let report = b.session.tick().await.expect("tick");
        let first = b.session.drain_events().unwrap();
        let swept: usize = first
            .iter()
            .filter(|event| matches!(event, SessionEvent::MessageExpired { .. }))
            .count();
        assert_eq!(swept, 256, "first sweep is the bound: {swept}");
        assert_eq!(
            report.next_wake_ms,
            Some(0),
            "due rows remain — wake immediately"
        );
        let report2 = b.session.tick().await.expect("tick");
        let second = b.session.drain_events().unwrap();
        let swept2: usize = second
            .iter()
            .filter(|event| matches!(event, SessionEvent::MessageExpired { .. }))
            .count();
        assert_eq!(swept2, 44, "second sweep takes the remainder: {swept2}");
        assert!(delivered_inners(&b).is_empty(), "every expired row is gone");
        let _ = report2;
    }

    // --- pacing ---

    /// 50 sends share the clock's `created_at` second; the 51st spills
    /// into the next.
    #[tokio::test(flavor = "current_thread")]
    async fn pacing_caps_fifty_per_second_then_spills() {
        let (relay, mut a, _b, gid) = pair("pacing-cap").await;
        let second = a.clock.now().unix_ms / 1_000;
        for sent in send_burst(&mut a, &gid, 50, "cap-").await {
            assert_eq!(sent.message.created_at, second, "50 sends share the second");
        }
        let spilled = send_burst(&mut a, &gid, 1, "cap-").await;
        assert_eq!(spilled[0].message.created_at, second + 1, "the 51st spills");
        flush(&mut a, &relay).await;
    }

    /// A frozen clock reaches the 1550-send budget (50/second × 31
    /// seconds inside the 30 s skew); the 1551st refuses
    /// `SendRateExceeded`, staging nothing and writing no KV batch;
    /// advancing the clock one second recovers.
    #[tokio::test(flavor = "current_thread")]
    async fn pacing_refuses_past_thirty_seconds_of_skew() {
        let (relay, mut a, _b, gid) = pair("pacing-skew").await;
        send_burst(&mut a, &gid, 1_550, "burst-").await;
        flush(&mut a, &relay).await;

        let batches = a.host.batches().len();
        let refused = a
            .session
            .send_message(
                &gid,
                AppMessage {
                    kind: 9,
                    tags: Vec::new(),
                    content: "over".to_owned(),
                },
            )
            .await;
        assert!(
            matches!(refused, Err(SessionError::SendRateExceeded)),
            "the 1551st send refuses: {refused:?}"
        );
        assert_eq!(
            a.host.batches().len(),
            batches,
            "the refused send committed no KV batch"
        );
        assert!(
            a.session.take_outbound().expect("take_outbound").is_empty(),
            "the refused send staged no outbound"
        );

        // Advancing the clock one second frees a new paced second.
        a.clock.advance(1_000);
        a.session
            .send_message(
                &gid,
                AppMessage {
                    kind: 9,
                    tags: Vec::new(),
                    content: "recovered".to_owned(),
                },
            )
            .await
            .expect("sends again after the clock advances");
        flush(&mut a, &relay).await;
    }

    /// A restart mid-burst treats the persisted second as full: 20 sends
    /// share S, reopen, the next send lands on S+1.
    #[tokio::test(flavor = "current_thread")]
    async fn pacing_restart_skips_persisted_second() {
        let (relay, mut a, _b, gid) = pair("pacing-restart").await;
        let second = a.clock.now().unix_ms / 1_000;
        send_burst(&mut a, &gid, 20, "rs-").await;
        let mut a = reopen_member(&a).await;
        let next = send_burst(&mut a, &gid, 1, "rs-").await;
        assert_eq!(
            next[0].message.created_at,
            second + 1,
            "the persisted second is never reused"
        );
        flush(&mut a, &relay).await;
    }

    /// `created_at` never decreases: a clock stepping back keeps the
    /// allocator on its last second; a step back beyond the skew bound
    /// refuses until the clock catches up.
    #[tokio::test(flavor = "current_thread")]
    async fn pacing_created_at_stays_monotonic_on_clock_step_back() {
        let (relay, mut a, _b, gid) = pair("pacing-monotonic").await;
        // 60 sends on the frozen clock: 50 at S, 10 at S+1.
        let second = a.clock.now().unix_ms / 1_000;
        send_burst(&mut a, &gid, 60, "mono-").await;

        // Step back inside the skew: the last second is kept.
        let unix_now = a.clock.now().unix_ms;
        a.clock.set(0, unix_now - 1_000);
        let after = send_burst(&mut a, &gid, 1, "mono-").await;
        assert_eq!(
            after[0].message.created_at,
            second + 1,
            "a backward step never moves created_at back"
        );

        // Step back past the skew bound: sends refuse.
        a.clock.set(0, (second - 60) * 1_000);
        let refused = a
            .session
            .send_message(
                &gid,
                AppMessage {
                    kind: 9,
                    tags: Vec::new(),
                    content: "stuck".to_owned(),
                },
            )
            .await;
        assert!(
            matches!(refused, Err(SessionError::SendRateExceeded)),
            "a large step back refuses: {refused:?}"
        );

        // Clock catches up: sends resume on a non-decreasing second.
        a.clock.set(0, second * 1_000);
        let resumed = send_burst(&mut a, &gid, 1, "mono-").await;
        assert!(resumed[0].message.created_at > second);
        flush(&mut a, &relay).await;
    }

    /// The allocator is per-group: a burst that spills in one group leaves
    /// another group's `created_at` on the clock's own second.
    #[tokio::test(flavor = "current_thread")]
    async fn pacing_is_per_group() {
        let relay = FakeRelay::new();
        let mut a = open_member().await;
        let mut b = open_member().await;
        publish_identity(&mut a, &relay, RELAY).await;
        publish_identity(&mut b, &relay, RELAY).await;
        let gid_one = group_with(&mut a, &relay, RELAY, &[&b], "pacing-one").await;
        let gid_two = group_with(&mut a, &relay, RELAY, &[&b], "pacing-two").await;
        join_via_welcome(&mut b, &relay, RELAY).await;
        sync_member(&mut a, &relay, RELAY, &gid_one).await;
        sync_member(&mut b, &relay, RELAY, &gid_one).await;
        sync_member(&mut a, &relay, RELAY, &gid_two).await;
        sync_member(&mut b, &relay, RELAY, &gid_two).await;

        let second = a.clock.now().unix_ms / 1_000;
        send_burst(&mut a, &gid_one, 50, "one-").await;
        let spilled = send_burst(&mut a, &gid_one, 1, "one-").await;
        assert_eq!(
            spilled[0].message.created_at,
            second + 1,
            "group one's 51st send spills"
        );
        let other = send_burst(&mut a, &gid_two, 1, "two-").await;
        assert_eq!(
            other[0].message.created_at, second,
            "group two keeps its own allocator"
        );
        flush(&mut a, &relay).await;
    }
}
