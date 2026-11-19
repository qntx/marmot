//! M5a application-surface flow tests: typed send intents and wire shapes,
//! receive-side semantics and drop rules, restart redelivery, admin
//! operations, lifecycle, retention, deterministic system row ids and
//! `forget_group` cleanup — all through the public `Session` API on the
//! in-process harness. Requires the `testing` feature.

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
mod app_surface_tests {
    use super::support::*;
    use std::collections::BTreeMap;

    use mk_core::media::EncryptedMediaPolicy;
    use mk_core::{
        AccountId, AppMessage, AttachmentInput, Clock, ComponentUpdate, CreateGroup, DropReason,
        EventId, GroupAvatar, GroupChange, GroupId, GroupState, Invitee, MediaMessage,
        MessageIntent, OutboundPurpose, PublishResolution, ReceivedMessage, RelayResult,
        RelayStatus, Semantics, SendOutcome, SentMessage, SessionError, SessionEvent,
    };

    const RELAY: &str = "ws://127.0.0.1:7777";

    /// Table byte of `PendingAppEvent` rows (`keys.rs`).
    const PENDING_APP_EVENT_TABLE: u8 = 6;
    /// Table byte of `MediaSecret` rows (`keys.rs`).
    const MEDIA_SECRET_TABLE: u8 = 36;
    /// Table byte of the `ForgottenGroup` tombstone (`keys.rs`).
    const FORGOTTEN_GROUP_TABLE: u8 = 28;

    async fn group_as(
        creator: &mut Member,
        relay: &FakeRelay,
        invitees: &[&Member],
        name: &str,
        admins: &[AccountId],
        components: Vec<ComponentUpdate>,
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
                admins: admins.to_vec(),
                components,
            })
            .await
            .expect("create_group");
        flush(creator, relay).await;
        gid
    }

    async fn group_with(
        creator: &mut Member,
        relay: &FakeRelay,
        invitees: &[&Member],
        name: &str,
    ) -> GroupId {
        group_as(creator, relay, invitees, name, &[], Vec::new()).await
    }

    async fn join_via_welcome(member: &mut Member, relay: &FakeRelay) {
        let welcomes = welcomes_for(relay, RELAY, &member.account_hex());
        assert!(!welcomes.is_empty(), "a welcome must be waiting");
        ingest_backlog(member, &welcomes).await;
        member.session.tick().await.expect("tick");
        flush(member, relay).await;
    }

    /// Two members with published identities in a joined, settled group.
    async fn pair(name: &str) -> (FakeRelay, Member, Member, GroupId) {
        let relay = FakeRelay::new();
        let mut a = open_member().await;
        let mut b = open_member().await;
        publish_identity(&mut a, &relay, RELAY).await;
        publish_identity(&mut b, &relay, RELAY).await;
        let gid = group_with(&mut a, &relay, &[&b], name).await;
        join_via_welcome(&mut b, &relay).await;
        sync_member(&mut a, &relay, RELAY, &gid).await;
        sync_member(&mut b, &relay, RELAY, &gid).await;
        (relay, a, b, gid)
    }

    fn snapshot(member: &Member, gid: &GroupId) -> mk_core::GroupSnapshot {
        member
            .session
            .group(gid)
            .expect("group")
            .expect("group exists")
    }

    /// One ingest + tick + flush round over the group's backlog (the body of
    /// `sync_member`, with the route hex passed in so it also works for a
    /// group whose local copy already terminated).
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

    /// Every drained `GroupStateChanged` as `(group, epoch, actor, change, row_id)`.
    fn state_changes(
        events: &[SessionEvent],
    ) -> Vec<(&GroupId, u64, Option<AccountId>, &GroupChange, &str)> {
        events
            .iter()
            .filter_map(|event| match event {
                SessionEvent::GroupStateChanged {
                    group,
                    epoch,
                    actor,
                    change,
                    row_id,
                    ..
                } => Some((group, *epoch, *actor, change, row_id.as_str())),
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

    /// A `PendingAppEvent` row is `[6][u32 len][mid]` — gone after a drop.
    fn pending_row_exists(member: &Member, transport_id: &EventId) -> bool {
        member.host.durable_rows().iter().any(|(key, _)| {
            key.first() == Some(&PENDING_APP_EVENT_TABLE)
                && key.len() == 1 + 4 + 32
                && key[5..] == transport_id.0[..]
        })
    }

    /// A `MediaSecret` row is `[36][len][gid][len][mid]` — last segment the
    /// transport id.
    fn media_secret_row_exists(member: &Member, transport_id: &EventId) -> bool {
        member.host.durable_rows().iter().any(|(key, _)| {
            key.first() == Some(&MEDIA_SECRET_TABLE) && key.ends_with(&transport_id.0)
        })
    }

    /// The op staged nothing: no new committed KV batch, no gated outbound.
    fn assert_nothing_staged(member: &mut Member, batches_before: usize, what: &str) {
        assert_eq!(
            member.host.batches().len(),
            batches_before,
            "{what}: no new KV batch"
        );
        if let Some(seq) = member.session.awaiting_durability() {
            member.session.acknowledge_durable(seq);
        }
        assert!(
            member
                .session
                .take_outbound()
                .expect("take_outbound")
                .is_empty(),
            "{what}: no outbound staged"
        );
    }

    /// Publish the staged `purpose` outbound and return its resolution.
    async fn publish_one(
        member: &mut Member,
        relay: &FakeRelay,
        purpose: OutboundPurpose,
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

    /// Reconstruct the engine's `GroupStateChange` from the session-mapped
    /// `GroupChange` (`Renamed::old_name` inverts `unwrap_or_default`).
    fn engine_change(change: &GroupChange) -> cgka_traits::engine::GroupStateChange {
        use cgka_traits::engine::GroupStateChange as Change;
        use cgka_traits::types::MemberId;
        let member = |account: &AccountId| MemberId::new(account.0.to_vec());
        match change {
            GroupChange::MemberAdded { account } => Change::MemberAdded {
                member: member(account),
            },
            GroupChange::MemberRemoved { account } => Change::MemberRemoved {
                member: member(account),
            },
            GroupChange::MemberLeft { account } => Change::MemberLeft {
                member: member(account),
            },
            GroupChange::AdminAdded { account } => Change::AdminAdded {
                member: member(account),
            },
            GroupChange::AdminRemoved { account } => Change::AdminRemoved {
                member: member(account),
            },
            GroupChange::Renamed { old_name, new_name } => Change::GroupRenamed {
                name: new_name.clone(),
                previous_name: (!old_name.is_empty()).then(|| old_name.clone()),
            },
            GroupChange::AvatarChanged => Change::GroupAvatarChanged,
            GroupChange::RetentionChanged {
                old_seconds,
                new_seconds,
            } => Change::MessageRetentionChanged {
                old_seconds: *old_seconds,
                new_seconds: *new_seconds,
            },
            GroupChange::Disbanded => Change::GroupDisbanded,
            other => panic!("unmapped change: {other:?}"),
        }
    }

    /// `group_system_canonical_id` of an observed `GroupStateChanged`.
    fn canonical_row_id(
        gid: &GroupId,
        epoch: u64,
        actor: Option<AccountId>,
        change: &GroupChange,
    ) -> String {
        let actor_id = actor
            .as_ref()
            .map(|account| cgka_traits::types::MemberId::new(account.0.to_vec()));
        cgka_traits::app_event::group_system_canonical_id(
            &cgka_traits::types::GroupId::new(gid.0.clone()),
            epoch,
            actor_id.as_ref(),
            &engine_change(change),
        )
        .expect("canonical row id")
    }

    /// Chat/reply/reaction/unreact round trip: exact wire shapes, typed
    /// `Semantics`, `SentMessage.message.id()` equal to the inner id the
    /// receiver sees, and send-side validation labels.
    #[tokio::test(flavor = "current_thread")]
    async fn typed_intents_wire_shapes_and_semantics() {
        let (relay, mut a, mut b, gid) = pair("typed").await;
        let a_account = a.signer.account();
        let route = a.route_hex(&gid);

        // Chat: kind 9, no tags.
        let m1 = send_typed(
            &mut a,
            &relay,
            &gid,
            MessageIntent::Chat {
                content: "m1 hello".to_owned(),
                reply_to: None,
            },
        )
        .await;
        assert_eq!(m1.message.kind, 9);
        assert_eq!(m1.message.tags, Vec::<Vec<String>>::new());
        assert_eq!(m1.message.content, "m1 hello");
        assert_eq!(m1.message.pubkey, a_account);
        assert!(
            matches!(m1.outcome, SendOutcome::Staged(_)),
            "chat stages an outbound: {:?}",
            m1.outcome
        );
        let m1_id = m1.message.id();
        let m1_hex = m1_id.to_hex();

        let events = collect(&mut b, &relay, &route).await;
        let msg = message_with(&events, "m1 hello");
        assert_eq!(msg.kind, 9);
        assert_eq!(msg.id, m1_hex, "inner id is the wire id B receives");
        assert_eq!(msg.sender, a_account);
        assert!(
            matches!(msg.semantics, Semantics::Chat { reply_to: None }),
            "plain chat semantics: {:?}",
            msg.semantics
        );

        // Reply: kind 9 with the e/q pair carrying the same inner id.
        let r1 = send_typed(
            &mut b,
            &relay,
            &gid,
            MessageIntent::Chat {
                content: "r1 reply".to_owned(),
                reply_to: Some(m1_id),
            },
        )
        .await;
        assert_eq!(r1.message.kind, 9);
        assert_eq!(
            r1.message.tags,
            vec![
                vec!["e".to_owned(), m1_hex.clone()],
                vec!["q".to_owned(), m1_hex.clone()]
            ]
        );
        let events = collect(&mut a, &relay, &route).await;
        let msg = message_with(&events, "r1 reply");
        assert_eq!(msg.id, r1.message.id().to_hex());
        assert!(
            matches!(
                msg.semantics,
                Semantics::Chat {
                    reply_to: Some(target)
                } if target == m1_id
            ),
            "reply resolves to m1: {:?}",
            msg.semantics
        );

        // Reactions: kind 7, one e tag, emoji content — both directions.
        let react_a = send_typed(
            &mut a,
            &relay,
            &gid,
            MessageIntent::Reaction {
                target: m1_id,
                emoji: "👍".to_owned(),
            },
        )
        .await;
        assert_eq!(react_a.message.kind, 7);
        assert_eq!(
            react_a.message.tags,
            vec![vec!["e".to_owned(), m1_hex.clone()]]
        );
        assert_eq!(react_a.message.content, "👍");
        let react_b = send_typed(
            &mut b,
            &relay,
            &gid,
            MessageIntent::Reaction {
                target: m1_id,
                emoji: "🔥".to_owned(),
            },
        )
        .await;

        let a_events = collect(&mut a, &relay, &route).await;
        assert!(
            a_events.iter().any(|event| matches!(
                event,
                SessionEvent::MessageReceived { message, .. } if matches!(
                    &message.semantics,
                    Semantics::Reaction { target, emoji }
                        if *target == m1_id && emoji == "🔥"
                ) && message.sender == b.signer.account()
            )),
            "A sees B's reaction typed"
        );
        let b_events = collect(&mut b, &relay, &route).await;
        assert!(
            b_events.iter().any(|event| matches!(
                event,
                SessionEvent::MessageReceived { message, .. } if matches!(
                    &message.semantics,
                    Semantics::Reaction { target, emoji }
                        if *target == m1_id && emoji == "👍"
                ) && message.sender == a_account
            )),
            "B sees A's reaction typed"
        );

        // Unreact: kind 5, one e tag per own reaction id, empty content.
        let un = send_typed(
            &mut b,
            &relay,
            &gid,
            MessageIntent::Unreact {
                reactions: vec![react_b.message.id()],
            },
        )
        .await;
        assert_eq!(un.message.kind, 5);
        assert_eq!(
            un.message.tags,
            vec![vec!["e".to_owned(), react_b.message.id().to_hex()]]
        );
        assert_eq!(un.message.content, "");
        // Never surfaced (kind 5 is unverifiable without a per-message
        // author index): no delivery; it drops `unverifiable`.
        let a_events = collect(&mut a, &relay, &route).await;
        assert!(
            !a_events.iter().any(|event| matches!(
                event,
                SessionEvent::MessageReceived { message, .. } if message.kind == 5
            )),
            "kind 5 is never delivered"
        );
        assert!(
            dropped_reasons(&a_events).contains(&DropReason::Unverifiable),
            "unreact drops unverifiable: {:?}",
            dropped_reasons(&a_events)
        );

        // A unreacting B's reaction: mk-core has no durable
        // reaction-ownership index to refuse this at the send site (the
        // design's validation list has no ownership rule); on the wire it
        // is a plain kind 5, dropped `unverifiable` at every receiver —
        // an unauthorized unreact is never honoured.
        a.session
            .send_intent(
                &gid,
                MessageIntent::Unreact {
                    reactions: vec![react_b.message.id()],
                },
            )
            .await
            .expect("unreact of a foreign reaction still encodes");
        flush(&mut a, &relay).await;
        let b_events = collect(&mut b, &relay, &route).await;
        assert!(
            !b_events.iter().any(|event| matches!(
                event,
                SessionEvent::MessageReceived { message, .. } if message.kind == 5
            )),
            "the spoofed unreact is not delivered either"
        );

        // Validation: malformed reaction emoji is refused before staging.
        for emoji in ["", " 👍"] {
            let before = a.host.batches().len();
            let res = a
                .session
                .send_intent(
                    &gid,
                    MessageIntent::Reaction {
                        target: m1_id,
                        emoji: emoji.to_owned(),
                    },
                )
                .await;
            assert!(
                matches!(res, Err(SessionError::InvalidInput("emoji"))),
                "emoji {emoji:?}: {res:?}"
            );
            assert_eq!(a.host.batches().len(), before, "no staging on refusal");
        }
        // Empty unreact list / empty reply body are refused too.
        assert!(matches!(
            a.session
                .send_intent(
                    &gid,
                    MessageIntent::Unreact {
                        reactions: Vec::new()
                    }
                )
                .await,
            Err(SessionError::InvalidInput("reactions"))
        ));
        assert!(matches!(
            a.session
                .send_intent(
                    &gid,
                    MessageIntent::Chat {
                        content: "  ".to_owned(),
                        reply_to: Some(m1_id)
                    }
                )
                .await,
            Err(SessionError::InvalidInput("content"))
        ));
        assert!(matches!(
            a.session
                .send_intent(
                    &gid,
                    MessageIntent::Edit {
                        target: m1_id,
                        content: String::new()
                    }
                )
                .await,
            Err(SessionError::InvalidInput("content"))
        ));
    }

    /// A staged typed intent survives a sender restart and still publishes;
    /// a message ingested but not acknowledged redelivers after a receiver
    /// restart as the same typed event.
    #[tokio::test(flavor = "current_thread")]
    async fn restart_republishes_and_redelivers() {
        let (relay, mut a, mut b, gid) = pair("restart").await;
        let route = b.route_hex(&gid);

        // Sender restart between send_intent and publish.
        a.clock.advance(1_000);
        let sent = a
            .session
            .send_intent(
                &gid,
                MessageIntent::Chat {
                    content: "across restart".to_owned(),
                    reply_to: None,
                },
            )
            .await
            .expect("send_intent");
        let mut a = reopen_member(&a).await;
        flush(&mut a, &relay).await;
        let events = collect(&mut b, &relay, &route).await;
        let msg = message_with(&events, "across restart");
        assert_eq!(msg.id, sent.message.id().to_hex());
        assert!(
            matches!(msg.semantics, Semantics::Chat { reply_to: None }),
            "redelivered as the same typed event: {:?}",
            msg.semantics
        );

        // Receiver restart between ingest and ack: ingest only, reopen,
        // then drain — pending rows replay the same typed event.
        let m1_id = sent.message.id();
        let sent2 = send_typed(
            &mut a,
            &relay,
            &gid,
            MessageIntent::Chat {
                content: "after reopen".to_owned(),
                reply_to: Some(m1_id),
            },
        )
        .await;
        b.session
            .ingest_batch(group_events(&relay, RELAY, &route))
            .await
            .expect("ingest");
        let mut b = reopen_member(&b).await;
        let mut drained = b.session.drain_events().unwrap();
        if !drained.iter().any(|event| {
            matches!(
                event,
                SessionEvent::MessageReceived { message, .. } if message.content == "after reopen"
            )
        }) {
            b.session.tick().await.expect("tick");
            drained.extend(b.session.drain_events().unwrap());
        }
        let ids: Vec<_> = drained
            .iter()
            .filter_map(|event| match event {
                SessionEvent::MessageReceived { message, .. } => Some(message.transport_id),
                _ => None,
            })
            .collect();
        if !ids.is_empty() {
            b.session.acknowledge_messages(&ids).expect("ack");
        }
        let msg = message_with(&drained, "after reopen");
        assert_eq!(msg.id, sent2.message.id().to_hex());
        assert!(
            matches!(
                msg.semantics,
                Semantics::Chat {
                    reply_to: Some(target)
                } if target == m1_id
            ),
            "redelivered with the same typed semantics: {:?}",
            msg.semantics
        );
    }

    /// Delete vs remove: an admin `Delete` lands as kind-4891 `Removal`;
    /// a non-admin `Delete` is kind 5 and drops `unverifiable` with its
    /// pending row released; a non-admin `Remove` is refused
    /// `NotGroupAdmin` before staging.
    #[tokio::test(flavor = "current_thread")]
    async fn delete_remove_and_drop_hygiene() {
        let (relay, mut a, mut b, gid) = pair("moderation").await;
        let route_a = a.route_hex(&gid);
        let route_b = b.route_hex(&gid);

        let m1 = send_typed(
            &mut a,
            &relay,
            &gid,
            MessageIntent::Chat {
                content: "delete me".to_owned(),
                reply_to: None,
            },
        )
        .await;
        let m1_id = m1.message.id();
        let m1_hex = m1_id.to_hex();
        collect(&mut b, &relay, &route_b).await;

        // Admin delete -> kind 4891, exact removal payload; B sees Removal.
        let del = send_typed(
            &mut a,
            &relay,
            &gid,
            MessageIntent::Delete { target: m1_id },
        )
        .await;
        assert_eq!(del.message.kind, 4891);
        assert_eq!(del.message.tags, vec![vec!["e".to_owned(), m1_hex.clone()]]);
        assert_eq!(del.message.content, "{\"v\":1,\"action\":\"remove\"}");
        let b_events = collect(&mut b, &relay, &route_b).await;
        assert!(
            b_events.iter().any(|event| matches!(
                event,
                SessionEvent::MessageReceived { message, .. } if matches!(
                    &message.semantics,
                    Semantics::Removal { target } if *target == m1_id
                )
            )),
            "B sees the admin removal typed"
        );

        // Non-admin delete -> kind 5; A drops it `unverifiable` and
        // releases its pending row in the drain's storage unit.
        let del_b = send_typed(
            &mut b,
            &relay,
            &gid,
            MessageIntent::Delete { target: m1_id },
        )
        .await;
        assert_eq!(del_b.message.kind, 5);
        assert_eq!(del_b.message.tags, vec![vec!["e".to_owned(), m1_hex]]);
        let transport_id = transport_id_for(&relay, &route_b, del_b.message.created_at);
        let a_events = collect(&mut a, &relay, &route_a).await;
        assert!(
            dropped_reasons(&a_events).contains(&DropReason::Unverifiable),
            "non-admin delete drops unverifiable: {:?}",
            dropped_reasons(&a_events)
        );
        assert!(
            !a_events.iter().any(|event| matches!(
                event,
                SessionEvent::MessageReceived { message, .. } if message.kind == 5
            )),
            "kind 5 is never delivered"
        );
        assert!(
            !pending_row_exists(&a, &transport_id),
            "the drop released the pending row"
        );
        assert!(
            !media_secret_row_exists(&a, &transport_id),
            "the drop wrote no media secret"
        );
        let mut a = reopen_member(&a).await;
        let drained = a.session.drain_events().unwrap();
        assert!(
            !drained.iter().any(|event| matches!(
                event,
                SessionEvent::MessageReceived { message, .. } if message.kind == 5
            ) || matches!(event, SessionEvent::AppEventDropped { .. })),
            "a dropped message is not redelivered after reopen: {drained:?}"
        );

        // Non-admin Remove -> NotGroupAdmin before anything is staged.
        flush(&mut b, &relay).await;
        let before = b.host.batches().len();
        let res = b
            .session
            .send_intent(&gid, MessageIntent::Remove { target: m1_id })
            .await;
        assert!(
            matches!(res, Err(SessionError::NotGroupAdmin)),
            "non-admin Remove: {res:?}"
        );
        assert_nothing_staged(&mut b, before, "non-admin Remove");

        // A wire-level kind-4891 whose moderation grant is false cannot be
        // produced through the public send path: the engine refuses
        // `SendIntent::AppMessage` kind 4891 without admin authority
        // (send.rs `InvalidAppMessagePayload`), and `authority` is computed
        // by the *receiver's* engine from the source-epoch admin set — a
        // non-admin simply cannot emit it. The drop rule is pinned by the
        // `classify` unit tests (`grant None|false -> Unauthorized`).
    }

    /// Raw `send_message` refuses the reserved kinds and kind-9 `e`/`q`
    /// tags before anything is staged.
    #[tokio::test(flavor = "current_thread")]
    async fn raw_send_message_refuses_reserved_events() {
        let (relay, mut a, _b, gid) = pair("reserved").await;
        flush(&mut a, &relay).await;

        let target = "ab".repeat(32);
        for kind in [5u16, 7, 1009, 1210, 4891] {
            let before = a.host.batches().len();
            let res = a
                .session
                .send_message(
                    &gid,
                    AppMessage {
                        kind,
                        tags: Vec::new(),
                        content: String::new(),
                    },
                )
                .await;
            assert!(
                matches!(res, Err(SessionError::ReservedAppEvent)),
                "kind {kind}: {res:?}"
            );
            assert_nothing_staged(&mut a, before, "reserved kind");
        }
        for tags in [
            vec![vec!["e".to_owned(), target.clone()]],
            vec![vec!["q".to_owned(), target.clone()]],
        ] {
            let before = a.host.batches().len();
            let res = a
                .session
                .send_message(
                    &gid,
                    AppMessage {
                        kind: 9,
                        tags,
                        content: "x".to_owned(),
                    },
                )
                .await;
            assert!(
                matches!(res, Err(SessionError::ReservedAppEvent)),
                "kind 9 e/q: {res:?}"
            );
            assert_nothing_staged(&mut a, before, "kind 9 e/q");
        }
        // Plain kind 9 still sends (and now returns the inner event).
        let sent = a
            .session
            .send_message(
                &gid,
                AppMessage {
                    kind: 9,
                    tags: Vec::new(),
                    content: "raw ok".to_owned(),
                },
            )
            .await
            .expect("raw kind 9 sends");
        assert_eq!(sent.message.kind, 9);
        assert_eq!(sent.message.content, "raw ok");
        flush(&mut a, &relay).await;
    }

    /// Source-epoch retention: `expires_at = created_at + seconds` on the
    /// wire, `expired` drops at drain once the manual clock passes it,
    /// disabled at `0`, projected into the snapshot and the system row.
    #[tokio::test(flavor = "current_thread")]
    async fn retention_expiry_with_manual_clock() {
        let (relay, mut a, mut b, gid) = pair("retention").await;
        let route_b = b.route_hex(&gid);

        // Admin sets 60 s retention: staged -> published -> confirmed.
        let outcome = a
            .session
            .set_retention(&gid, 60)
            .await
            .expect("set_retention");
        assert!(matches!(outcome, SendOutcome::Staged(_)), "{outcome:?}");
        let resolution = publish_one(&mut a, &relay, OutboundPurpose::Commit).await;
        assert!(
            matches!(resolution, PublishResolution::Confirmed),
            "commit confirmed: {resolution:?}"
        );
        flush(&mut a, &relay).await;

        let b_events = collect(&mut b, &relay, &route_b).await;
        assert_eq!(snapshot(&a, &gid).retention_seconds, 60);
        assert_eq!(snapshot(&b, &gid).retention_seconds, 60);
        assert!(
            state_changes(&b_events)
                .iter()
                .any(|(.., change, _)| matches!(
                    change,
                    GroupChange::RetentionChanged {
                        new_seconds: 60,
                        ..
                    }
                )),
            "B sees the RetentionChanged system row"
        );

        // A message sent after the commit carries expires_at = +60.
        let sent = send_typed(
            &mut a,
            &relay,
            &gid,
            MessageIntent::Chat {
                content: "timed".to_owned(),
                reply_to: None,
            },
        )
        .await;
        let events = collect(&mut b, &relay, &route_b).await;
        let msg = message_with(&events, "timed");
        assert_eq!(msg.created_at, sent.message.created_at);
        assert_eq!(
            msg.expires_at,
            Some(sent.message.created_at + 60),
            "expires_at is source-epoch created_at + retention"
        );

        // A second message ingested only after B's clock passed expiry
        // drops `expired` and releases its pending row.
        let sent2 = send_typed(
            &mut a,
            &relay,
            &gid,
            MessageIntent::Chat {
                content: "stale".to_owned(),
                reply_to: None,
            },
        )
        .await;
        let transport_id = transport_id_for(&relay, &route_b, sent2.message.created_at);
        let expiry = sent2.message.created_at + 60;
        let now = b.clock.now().unix_ms / 1_000;
        if now <= expiry {
            b.clock.advance((expiry + 1 - now) * 1_000);
        }
        let b_events = collect(&mut b, &relay, &route_b).await;
        assert!(
            dropped_reasons(&b_events).contains(&DropReason::Expired),
            "expired message drops: {:?}",
            dropped_reasons(&b_events)
        );
        assert!(
            !b_events.iter().any(|event| matches!(
                event,
                SessionEvent::MessageReceived { message, .. } if message.content == "stale"
            )),
            "an expired message is not delivered"
        );
        assert!(
            !pending_row_exists(&b, &transport_id),
            "expired drop released the pending row"
        );
        assert!(
            !media_secret_row_exists(&b, &transport_id),
            "expired drop wrote no media secret"
        );
        let mut b = reopen_member(&b).await;
        let drained = b.session.drain_events().unwrap();
        assert!(
            !drained.iter().any(|event| matches!(
                event,
                SessionEvent::MessageReceived { message, .. } if message.content == "stale"
            ) || matches!(event, SessionEvent::AppEventDropped { .. })),
            "an expired drop is not redelivered: {drained:?}"
        );

        // Retention 0 disables expiry: no expires_at, snapshot reads 0.
        let outcome = a.session.set_retention(&gid, 0).await.expect("set 0");
        assert!(matches!(outcome, SendOutcome::Staged(_)), "{outcome:?}");
        flush(&mut a, &relay).await;
        let b_events = collect(&mut b, &relay, &route_b).await;
        assert_eq!(snapshot(&b, &gid).retention_seconds, 0);
        assert!(
            state_changes(&b_events)
                .iter()
                .any(|(.., change, _)| matches!(
                    change,
                    GroupChange::RetentionChanged { new_seconds: 0, .. }
                )),
            "B sees retention cleared"
        );
        send_typed(
            &mut a,
            &relay,
            &gid,
            MessageIntent::Chat {
                content: "untimed".to_owned(),
                reply_to: None,
            },
        )
        .await;
        let events = collect(&mut b, &relay, &route_b).await;
        assert_eq!(message_with(&events, "untimed").expires_at, None);
    }

    /// System rows: every member that applies the same commit sees the same
    /// `row_id`, equal to `group_system_canonical_id` of the exact engine
    /// change — across add/rename/avatar/retention/admin/remove ops.
    #[tokio::test(flavor = "current_thread")]
    async fn system_row_ids_are_canonical() {
        let relay = FakeRelay::new();
        let mut a = open_member().await;
        let mut b = open_member().await;
        let mut c = open_member().await;
        publish_identity(&mut a, &relay, RELAY).await;
        publish_identity(&mut b, &relay, RELAY).await;
        publish_identity(&mut c, &relay, RELAY).await;
        let gid = group_with(&mut a, &relay, &[&b, &c], "sys-rows").await;
        join_via_welcome(&mut b, &relay).await;
        join_via_welcome(&mut c, &relay).await;
        sync_member(&mut a, &relay, RELAY, &gid).await;
        sync_member(&mut b, &relay, RELAY, &gid).await;
        sync_member(&mut c, &relay, RELAY, &gid).await;
        let route = a.route_hex(&gid);

        // Run each admin op inline and collect B's and C's
        // `GroupStateChanged`s per step.
        let mut rows_b: Vec<(u64, Option<AccountId>, GroupChange, String)> = Vec::new();
        let mut rows_c: Vec<(u64, Option<AccountId>, GroupChange, String)> = Vec::new();
        let mut gather = |events_b: Vec<SessionEvent>, events_c: Vec<SessionEvent>| {
            for (_, epoch, actor, change, row_id) in state_changes(&events_b) {
                rows_b.push((epoch, actor, change.clone(), row_id.to_owned()));
            }
            for (_, epoch, actor, change, row_id) in state_changes(&events_c) {
                rows_c.push((epoch, actor, change.clone(), row_id.to_owned()));
            }
        };

        // Rename.
        a.session
            .update_group_profile(&gid, Some("sys-renamed".to_owned()), Some("d".to_owned()))
            .await
            .expect("rename");
        flush(&mut a, &relay).await;
        gather(
            collect(&mut b, &relay, &route).await,
            collect(&mut c, &relay, &route).await,
        );

        // Set avatar, then clear it.
        a.session
            .set_group_avatar(
                &gid,
                Some(GroupAvatar {
                    url: "https://example.com/a.png".to_owned(),
                    dim: Some("64x64".to_owned()),
                    thumbhash: None,
                }),
            )
            .await
            .expect("set avatar");
        flush(&mut a, &relay).await;
        gather(
            collect(&mut b, &relay, &route).await,
            collect(&mut c, &relay, &route).await,
        );
        a.session
            .set_group_avatar(&gid, None)
            .await
            .expect("clear avatar");
        flush(&mut a, &relay).await;
        gather(
            collect(&mut b, &relay, &route).await,
            collect(&mut c, &relay, &route).await,
        );

        // Retention.
        a.session
            .set_retention(&gid, 3600)
            .await
            .expect("set retention");
        flush(&mut a, &relay).await;
        gather(
            collect(&mut b, &relay, &route).await,
            collect(&mut c, &relay, &route).await,
        );

        // Promote then demote B.
        a.session
            .set_admin(&gid, b.signer.account(), true)
            .await
            .expect("promote B");
        flush(&mut a, &relay).await;
        gather(
            collect(&mut b, &relay, &route).await,
            collect(&mut c, &relay, &route).await,
        );
        a.session
            .set_admin(&gid, b.signer.account(), false)
            .await
            .expect("demote B");
        flush(&mut a, &relay).await;
        gather(
            collect(&mut b, &relay, &route).await,
            collect(&mut c, &relay, &route).await,
        );

        // Remove C — B still observes it.
        a.session
            .remove_members(&gid, &[c.signer.account()])
            .await
            .expect("remove C");
        flush(&mut a, &relay).await;
        a.session.tick().await.expect("tick");
        flush(&mut a, &relay).await;
        gather(collect(&mut b, &relay, &route).await, Vec::new());

        assert!(
            rows_b.len() >= 6,
            "B saw the op's state changes: {rows_b:?}"
        );
        // Every row B or C saw is the canonical id of its exact change.
        for (label, rows) in [("b", &rows_b), ("c", &rows_c)] {
            for (epoch, actor, change, row_id) in rows {
                assert_eq!(
                    *row_id,
                    canonical_row_id(&gid, *epoch, *actor, change),
                    "{label} row {row_id} for {change:?}"
                );
            }
        }
        // Same change, same row_id on B and C — key rows by epoch+kind.
        let map_b: BTreeMap<String, String> = rows_b
            .iter()
            .map(|(epoch, _, change, row_id)| {
                (format!("{epoch}-{}", row_id_suffix(change)), row_id.clone())
            })
            .collect();
        let map_c: BTreeMap<String, String> = rows_c
            .iter()
            .map(|(epoch, _, change, row_id)| {
                (format!("{epoch}-{}", row_id_suffix(change)), row_id.clone())
            })
            .collect();
        for (key, row_b) in &map_b {
            if let Some(row_c) = map_c.get(key) {
                assert_eq!(row_b, row_c, "same commit, same row_id on B and C");
            }
        }
        // And at least the shared ops were seen by both.
        assert!(
            map_c.len() >= 5,
            "C saw the shared ops before its removal: {map_c:?}"
        );
        // The expected change sequence actually arrived on B.
        for want in [
            "renamed",
            "avatar",
            "retention",
            "admin_added",
            "admin_removed",
            "member_removed",
        ] {
            assert!(
                rows_b
                    .iter()
                    .any(|(_, _, change, _)| row_id_suffix(change) == want),
                "B saw a {want} change"
            );
        }
    }

    /// Stable discriminant label for keying a `GroupChange`.
    const fn row_id_suffix(change: &GroupChange) -> &'static str {
        match change {
            GroupChange::MemberAdded { .. } => "member_added",
            GroupChange::MemberRemoved { .. } => "member_removed",
            GroupChange::MemberLeft { .. } => "member_left",
            GroupChange::AdminAdded { .. } => "admin_added",
            GroupChange::AdminRemoved { .. } => "admin_removed",
            GroupChange::Renamed { .. } => "renamed",
            GroupChange::AvatarChanged => "avatar",
            GroupChange::RetentionChanged { .. } => "retention",
            GroupChange::Disbanded => "disbanded",
            _ => "other",
        }
    }

    /// Admin gating: every admin-gated op refuses `NotGroupAdmin` for a
    /// non-admin with zero staging; admin path confirms and lands on the
    /// peer's snapshot; `NoChange`, `AdminDepletion`, `InvalidInput` and
    /// publish-rollback behave.
    #[tokio::test(flavor = "current_thread")]
    async fn admin_ops_gating_and_rollback() {
        let relay = FakeRelay::new();
        let mut a = open_member().await;
        let mut b = open_member().await;
        let mut c = open_member().await;
        publish_identity(&mut a, &relay, RELAY).await;
        publish_identity(&mut b, &relay, RELAY).await;
        publish_identity(&mut c, &relay, RELAY).await;
        let gid = group_with(&mut a, &relay, &[&b], "admin-ops").await;
        join_via_welcome(&mut b, &relay).await;
        sync_member(&mut a, &relay, RELAY, &gid).await;
        sync_member(&mut b, &relay, RELAY, &gid).await;
        let route_b = b.route_hex(&gid);

        // Every admin-gated op by non-admin B -> NotGroupAdmin, no batch,
        // no outbound.
        let invitee = Invitee {
            key_package: key_package_of(&relay, RELAY, &c.account_hex()),
            inbox_relays: inbox_relays_of(&relay, RELAY, &c.account_hex()),
        };
        let avatar = Some(GroupAvatar {
            url: "https://example.com/pic.png".to_owned(),
            dim: None,
            thumbhash: None,
        });
        flush(&mut b, &relay).await;
        let before = b.host.batches().len();
        assert!(matches!(
            b.session
                .update_group_profile(&gid, Some("nope".to_owned()), None)
                .await,
            Err(SessionError::NotGroupAdmin)
        ));
        assert!(matches!(
            b.session.set_group_avatar(&gid, avatar.clone()).await,
            Err(SessionError::NotGroupAdmin)
        ));
        assert!(matches!(
            b.session.set_group_avatar(&gid, None).await,
            Err(SessionError::NotGroupAdmin)
        ));
        assert!(matches!(
            b.session.set_retention(&gid, 60).await,
            Err(SessionError::NotGroupAdmin)
        ));
        assert!(matches!(
            b.session.set_admin(&gid, b.signer.account(), true).await,
            Err(SessionError::NotGroupAdmin)
        ));
        assert!(matches!(
            b.session.remove_members(&gid, &[a.signer.account()]).await,
            Err(SessionError::NotGroupAdmin)
        ));
        assert!(matches!(
            b.session.invite(&gid, vec![invitee]).await,
            Err(SessionError::NotGroupAdmin)
        ));
        assert!(matches!(
            b.session.disband(&gid).await,
            Err(SessionError::NotGroupAdmin)
        ));
        assert!(matches!(
            b.session.enable_disbanding(&gid).await,
            Err(SessionError::NotGroupAdmin)
        ));
        assert_nothing_staged(&mut b, before, "non-admin ops");

        // The admin path lands end to end: staged -> confirmed, and the
        // peer snapshot reflects normalized avatar + admin flag.
        let outcome = a
            .session
            .set_group_avatar(
                &gid,
                Some(GroupAvatar {
                    url: "https://EXAMPLE.com/a.png".to_owned(),
                    dim: Some("64x64".to_owned()),
                    thumbhash: None,
                }),
            )
            .await
            .expect("set avatar");
        assert!(matches!(outcome, SendOutcome::Staged(_)), "{outcome:?}");
        let resolution = publish_one(&mut a, &relay, OutboundPurpose::Commit).await;
        assert!(
            matches!(resolution, PublishResolution::Confirmed),
            "{resolution:?}"
        );
        flush(&mut a, &relay).await;

        let outcome = a
            .session
            .set_admin(&gid, b.signer.account(), true)
            .await
            .expect("promote B");
        assert!(matches!(outcome, SendOutcome::Staged(_)), "{outcome:?}");
        let resolution = publish_one(&mut a, &relay, OutboundPurpose::Commit).await;
        assert!(
            matches!(resolution, PublishResolution::Confirmed),
            "{resolution:?}"
        );
        flush(&mut a, &relay).await;

        collect(&mut b, &relay, &route_b).await;
        let snap = snapshot(&b, &gid);
        assert_eq!(
            snap.avatar.as_ref().map(|avatar| avatar.url.as_str()),
            Some("https://example.com/a.png"),
            "avatar URL is normalized (host lowercased)"
        );
        assert_eq!(
            snap.avatar
                .as_ref()
                .and_then(|avatar| avatar.dim.as_deref()),
            Some("64x64")
        );
        assert!(snap.own_account_admin, "B sees its own admin flag");
        assert!(
            snap.members
                .iter()
                .any(|member| member.account == b.signer.account() && member.admin),
            "B's member row carries admin"
        );

        // Idempotent re-promotion -> NoChange (before any staging of a
        // commit the engine returns the no-op).
        let outcome = a
            .session
            .set_admin(&gid, b.signer.account(), true)
            .await
            .expect("re-promote");
        assert_eq!(outcome, SendOutcome::NoChange, "{outcome:?}");

        // Promoting a non-member -> InvalidInput("admin").
        assert!(matches!(
            a.session.set_admin(&gid, c.signer.account(), true).await,
            Err(SessionError::InvalidInput("admin"))
        ));

        // Last-admin step-down -> AdminDepletion before staging.
        // First demote B back so A is alone again.
        a.session
            .set_admin(&gid, b.signer.account(), false)
            .await
            .expect("demote B");
        flush(&mut a, &relay).await;
        let before = a.host.batches().len();
        assert!(matches!(
            a.session.set_admin(&gid, a.signer.account(), false).await,
            Err(SessionError::AdminDepletion)
        ));
        assert_nothing_staged(&mut a, before, "last-admin step-down");

        // Refused avatar URLs -> InvalidInput("avatar").
        for url in [
            "http://example.com/a.png",
            "https://user:pw@example.com/a.png",
            "https://example.com/a.png#frag",
            &format!("https://example.com/{}", "a".repeat(2048)),
        ] {
            assert!(
                matches!(
                    a.session
                        .set_group_avatar(
                            &gid,
                            Some(GroupAvatar {
                                url: url.to_owned(),
                                dim: None,
                                thumbhash: None,
                            })
                        )
                        .await,
                    Err(SessionError::InvalidInput("avatar"))
                ),
                "avatar url {url:?}"
            );
        }

        // An all-relays-rejected publish rolls the staged change back:
        // the snapshot keeps its pre-commit value.
        let outcome = a.session.set_retention(&gid, 60).await.expect("stage");
        assert!(matches!(outcome, SendOutcome::Staged(_)), "{outcome:?}");
        if let Some(seq) = a.session.awaiting_durability() {
            a.session.acknowledge_durable(seq);
        }
        let item = a
            .session
            .take_outbound()
            .expect("take_outbound")
            .into_iter()
            .find(|item| item.purpose == OutboundPurpose::Commit)
            .expect("commit outbound");
        let results: Vec<RelayResult> = item
            .relays
            .iter()
            .map(|relay_url| RelayResult {
                relay: relay_url.clone(),
                status: RelayStatus::Rejected,
            })
            .collect();
        let resolution = a
            .session
            .report_publish(&item.id, &results)
            .await
            .expect("report");
        assert!(
            matches!(resolution, PublishResolution::RolledBack),
            "all-rejected rolls back: {resolution:?}"
        );
        assert_eq!(snapshot(&a, &gid).retention_seconds, 0);
    }

    /// Lifecycle: non-admin and admin leave paths, last-admin refusal,
    /// disband propagation, the `GroupDisbanding` forget guard, and a
    /// removed member keeping its received attachment.
    #[tokio::test(flavor = "current_thread")]
    async fn lifecycle_leave_disband_and_forget() {
        let relay = FakeRelay::new();
        let mut a = open_member().await;
        let mut b = open_member().await;
        publish_identity(&mut a, &relay, RELAY).await;
        publish_identity(&mut b, &relay, RELAY).await;

        // ---- non-admin leave ----
        let gid = group_with(&mut a, &relay, &[&b], "leave1").await;
        join_via_welcome(&mut b, &relay).await;
        sync_member(&mut a, &relay, RELAY, &gid).await;
        sync_member(&mut b, &relay, RELAY, &gid).await;
        let route_a = a.route_hex(&gid);
        let route_b = b.route_hex(&gid);

        let outcome = b.session.leave(&gid).await.expect("leave");
        assert!(matches!(outcome, SendOutcome::Staged(_)), "{outcome:?}");
        flush(&mut b, &relay).await;
        // The SelfRemove rides a proposal: B's copy terminates only once a
        // member's auto-commit removing B's leaf lands back at B.
        let a_events = collect(&mut a, &relay, &route_a).await;
        let b_events = collect(&mut b, &relay, &route_b).await;
        assert!(
            b_events.iter().any(|event| matches!(
                event,
                SessionEvent::LocalCopyTerminated {
                    voluntary: true,
                    ..
                }
            )),
            "leaver sees voluntary termination: {b_events:?}"
        );
        assert!(
            b.session
                .group(&gid)
                .expect("group")
                .is_none_or(|snap| snap.state == GroupState::Removed),
            "leaver's copy is removed"
        );
        assert!(
            b.session
                .send_message(
                    &gid,
                    AppMessage {
                        kind: 9,
                        tags: Vec::new(),
                        content: "after leave".to_owned(),
                    }
                )
                .await
                .is_err(),
            "a removed member cannot send"
        );
        assert!(
            a_events.iter().any(|event| matches!(
                event,
                SessionEvent::GroupStateChanged { change: GroupChange::MemberLeft { account }, .. }
                    if *account == b.signer.account()
            )),
            "A sees MemberLeft"
        );

        // ---- admin leave ----
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
            Vec::new(),
        )
        .await;
        join_via_welcome(&mut b2, &relay).await;
        sync_member(&mut a2, &relay, RELAY, &gid2).await;
        sync_member(&mut b2, &relay, RELAY, &gid2).await;
        let route_a2 = a2.route_hex(&gid2);
        let route_b2 = b2.route_hex(&gid2);

        let outcome = b2.session.leave(&gid2).await.expect("admin leave");
        assert_eq!(outcome, SendOutcome::LeaveRequested);
        // Step-down commit then SelfRemove proposal; A2's auto-commit is
        // what removes B2's leaf back at B2, so the two sides converge in
        // lockstep.
        let mut b2_events = Vec::new();
        let mut a2_events = Vec::new();
        for _ in 0..8 {
            b2_events.extend(cycle(&mut b2, &relay, &route_b2).await);
            a2_events.extend(cycle(&mut a2, &relay, &route_a2).await);
            if b2_events.iter().any(|event| {
                matches!(
                    event,
                    SessionEvent::LocalCopyTerminated {
                        voluntary: true,
                        ..
                    }
                )
            }) {
                break;
            }
        }
        assert!(
            b2_events.iter().any(|event| matches!(
                event,
                SessionEvent::LocalCopyTerminated {
                    voluntary: true,
                    ..
                }
            )),
            "admin leaver sees voluntary termination: {b2_events:?}"
        );
        a2_events.extend(collect(&mut a2, &relay, &route_a2).await);
        let changes = state_changes(&a2_events);
        let admin_removed = changes.iter().position(|(_, _, _, change, _)| {
            matches!(
                change,
                GroupChange::AdminRemoved { account } if *account == b2.signer.account()
            )
        });
        let member_left = changes.iter().position(|(_, _, _, change, _)| {
            matches!(
                change,
                GroupChange::MemberLeft { account } if *account == b2.signer.account()
            )
        });
        assert!(
            admin_removed.is_some() && member_left.is_some() && admin_removed < member_left,
            "A sees AdminRemoved then MemberLeft: {changes:?}"
        );

        // ---- last admin cannot leave ----
        assert!(matches!(
            a2.session.leave(&gid2).await,
            Err(SessionError::AdminDepletion)
        ));

        // ---- disband ----
        let mut a3 = open_member().await;
        let mut b3 = open_member().await;
        publish_identity(&mut a3, &relay, RELAY).await;
        publish_identity(&mut b3, &relay, RELAY).await;
        let gid3 = group_with(&mut a3, &relay, &[&b3], "disband").await;
        join_via_welcome(&mut b3, &relay).await;
        sync_member(&mut a3, &relay, RELAY, &gid3).await;
        sync_member(&mut b3, &relay, RELAY, &gid3).await;
        let route3 = a3.route_hex(&gid3);
        assert!(
            snapshot(&a3, &gid3).disband_enabled,
            "fresh groups require lifecycle"
        );

        let outcome = a3.session.disband(&gid3).await.expect("disband");
        assert_eq!(outcome, SendOutcome::DisbandRequested);

        // Forgetting while the disband is in progress refuses.
        assert!(matches!(
            a3.session.forget_group(&gid3),
            Err(SessionError::GroupDisbanding)
        ));

        // Drive both members until the terminal commit lands everywhere.
        let mut a3_events = Vec::new();
        let mut b3_events = Vec::new();
        for _ in 0..12 {
            a3_events.extend(cycle(&mut a3, &relay, &route3).await);
            b3_events.extend(cycle(&mut b3, &relay, &route3).await);
            let a_done = a3_events.iter().any(|event| {
                matches!(
                    event,
                    SessionEvent::GroupStateChanged {
                        change: GroupChange::Disbanded,
                        ..
                    }
                )
            }) && matches!(
                a3.session.group(&gid3),
                Ok(Some(snap)) if snap.state == GroupState::Disbanded
            );
            let b_done = b3_events.iter().any(|event| {
                matches!(
                    event,
                    SessionEvent::GroupStateChanged {
                        change: GroupChange::Disbanded,
                        ..
                    }
                )
            }) && matches!(
                b3.session.group(&gid3),
                Ok(Some(snap)) if snap.state == GroupState::Disbanded
            );
            if a_done && b_done {
                break;
            }
        }
        for (who, events, member) in [("a3", &a3_events, &a3), ("b3", &b3_events, &b3)] {
            assert!(
                events.iter().any(|event| {
                    matches!(
                        event,
                        SessionEvent::GroupStateChanged {
                            change: GroupChange::Disbanded,
                            ..
                        }
                    )
                }),
                "{who} sees the Disbanded system row"
            );
            assert_eq!(
                snapshot(member, &gid3).state,
                GroupState::Disbanded,
                "{who} snapshot is Disbanded"
            );
        }

        // A group without the lifecycle component cannot exist through the
        // public path: `create_group` requires `0x800c` whenever every
        // founding leaf advertises it — every mk-core leaf does — and MDK
        // refuses to un-require it (`lifecycle-v1 cannot be un-required`).
        // `SessionError::DisbandingNotEnabled` is therefore unreachable via
        // public APIs; `enable_disbanding` exists for pre-lifecycle groups
        // joined from foreign tooling.
    }

    /// `forget_group` on a live group removes every byte trace of it —
    /// group id and Nostr route id alike — except the `ForgottenGroup`
    /// tombstone, while a second group keeps working.
    #[tokio::test(flavor = "current_thread")]
    async fn forget_group_erases_rows_and_preserves_others() {
        let relay = FakeRelay::new();
        let mut a = open_member().await;
        let mut b = open_member().await;
        publish_identity(&mut a, &relay, RELAY).await;
        publish_identity(&mut b, &relay, RELAY).await;
        let gid1 = group_with(&mut a, &relay, &[&b], "forget-me").await;
        let gid2 = group_with(&mut a, &relay, &[&b], "keep-me").await;
        join_via_welcome(&mut b, &relay).await; // consumes both welcomes
        sync_member(&mut a, &relay, RELAY, &gid1).await;
        sync_member(&mut a, &relay, RELAY, &gid2).await;
        sync_member(&mut b, &relay, RELAY, &gid1).await;
        sync_member(&mut b, &relay, RELAY, &gid2).await;

        // Traffic in both groups so rows of every kind exist for gid1.
        send_typed(
            &mut a,
            &relay,
            &gid1,
            MessageIntent::Chat {
                content: "g1 traffic".to_owned(),
                reply_to: None,
            },
        )
        .await;
        send_typed(
            &mut a,
            &relay,
            &gid2,
            MessageIntent::Chat {
                content: "g2 traffic".to_owned(),
                reply_to: None,
            },
        )
        .await;
        let route1 = b.route_hex(&gid1);
        let route2 = b.route_hex(&gid2);
        collect(&mut b, &relay, &route1).await;
        collect(&mut b, &relay, &route2).await;

        let gid1_bytes = gid1.0.clone();
        let route1_bytes = snapshot(&a, &gid1).nostr_group_id;
        a.session.forget_group(&gid1).expect("forget_group");
        assert!(
            !matches!(a.session.group(&gid1), Ok(Some(_))),
            "forgotten group is gone from the snapshot surface"
        );

        // No remaining row's key or value carries the group id or the
        // Nostr route id — except the ForgottenGroup tombstone itself.
        let mut tombstone_rows = 0usize;
        for (key, value) in a.host.durable_rows() {
            if key.first() == Some(&FORGOTTEN_GROUP_TABLE) {
                tombstone_rows += 1;
                continue;
            }
            for (blob, what) in [(&key, "key"), (&value, "value")] {
                assert!(
                    !blob
                        .windows(gid1_bytes.len())
                        .any(|window| window == gid1_bytes.as_slice()),
                    "forgotten group id in {what}"
                );
                assert!(
                    !blob
                        .windows(route1_bytes.len())
                        .any(|window| window == &route1_bytes[..]),
                    "forgotten route id in {what}"
                );
            }
        }
        assert_eq!(tombstone_rows, 1, "the tombstone is the only trace");

        // The other group still works end to end.
        send_typed(
            &mut a,
            &relay,
            &gid2,
            MessageIntent::Chat {
                content: "g2 still works".to_owned(),
                reply_to: None,
            },
        )
        .await;
        let b_events = collect(&mut b, &relay, &route2).await;
        message_with(&b_events, "g2 still works");
    }

    /// A removed member keeps `state = Removed` and can still open an
    /// attachment it received while it was a member (the media-secret row
    /// outlives membership).
    #[tokio::test(flavor = "current_thread")]
    async fn removed_member_keeps_received_attachment() {
        let relay = FakeRelay::new();
        let mut a = open_member().await;
        let mut b = open_member().await;
        publish_identity(&mut a, &relay, RELAY).await;
        publish_identity(&mut b, &relay, RELAY).await;

        let policy = EncryptedMediaPolicy::blossom(&["https://blossom.example/".to_owned()]);
        let gid = group_as(
            &mut a,
            &relay,
            &[&b],
            "media-remove",
            &[],
            vec![policy.to_component().expect("component")],
        )
        .await;
        join_via_welcome(&mut b, &relay).await;
        sync_member(&mut a, &relay, RELAY, &gid).await;
        sync_member(&mut b, &relay, RELAY, &gid).await;
        let route_b = b.route_hex(&gid);

        // A sends an attachment; B opens it.
        let plaintext: Vec<u8> = (0u32..4_096).map(|n| (n % 251) as u8).collect();
        let sealed = a
            .session
            .seal_attachment(
                &gid,
                AttachmentInput {
                    plaintext: plaintext.clone(),
                    media_type: "image/png".to_owned(),
                    filename: "pic.png".to_owned(),
                    dim: None,
                    thumbhash: None,
                },
            )
            .expect("seal");
        let blob_url = format!(
            "https://blossom.example/{}",
            hex::encode(sealed.ciphertext_sha256)
        );
        let blobs = std::collections::HashMap::from([(
            sealed.ciphertext_sha256,
            sealed.ciphertext.clone(),
        )]);
        a.session
            .send_media(
                &gid,
                MediaMessage {
                    caption: "kept".to_owned(),
                    attachments: vec![(
                        sealed.pending,
                        vec![mk_core::media::Locator {
                            kind: "blossom-v1".to_owned(),
                            value: blob_url.clone(),
                        }],
                    )],
                    tags: Vec::new(),
                },
            )
            .await
            .expect("send_media");
        flush(&mut a, &relay).await;

        let events = collect(&mut b, &relay, &route_b).await;
        let (message, group) = events
            .iter()
            .find_map(|event| match event {
                SessionEvent::MessageReceived { group, message }
                    if !message.attachments.is_empty() =>
                {
                    Some((message, group))
                }
                _ => None,
            })
            .expect("media message");
        let reference = message.attachments[0].as_ref().expect("attachment parses");
        let transport_id = message.transport_id;
        let group = group.clone();
        let fetched = blobs.get(&reference.ciphertext_sha256).expect("uploaded");
        assert_eq!(
            b.session
                .open_attachment(&group, &transport_id, reference, fetched)
                .expect("open while member"),
            plaintext
        );

        // A removes B; B's copy terminates involuntarily.
        a.session
            .remove_members(&gid, &[b.signer.account()])
            .await
            .expect("remove B");
        flush(&mut a, &relay).await;
        a.session.tick().await.expect("tick");
        flush(&mut a, &relay).await;
        let _ = collect(&mut b, &relay, &route_b).await;
        assert_eq!(snapshot(&b, &gid).state, GroupState::Removed);

        // The received attachment still opens — the media-secret row is
        // retained with the (removed) local copy.
        assert_eq!(
            b.session
                .open_attachment(&group, &transport_id, reference, fetched)
                .expect("open after removal"),
            plaintext
        );
    }
}
