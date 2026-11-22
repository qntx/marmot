//! Byte-level bridge integration tests: every call goes through
//! `Bridge::open`/`call`/`drain` as JSON bytes; the shared driver in
//! `tests/support` plays the host (MemHost, fake relay, LocalSigner).

#![allow(
    clippy::cast_lossless,
    clippy::cast_possible_truncation,
    clippy::doc_markdown,
    clippy::excessive_nesting,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::manual_let_else,
    clippy::map_unwrap_or,
    clippy::future_not_send,
    clippy::missing_const_for_fn,
    clippy::missing_docs_in_private_items,
    clippy::missing_fields_in_debug,
    clippy::missing_panics_doc,
    clippy::needless_pass_by_value,
    clippy::panic,
    clippy::shadow_reuse,
    clippy::shadow_unrelated,
    clippy::significant_drop_tightening,
    clippy::tests_outside_test_module,
    clippy::too_many_lines,
    clippy::unused_async,
    clippy::unwrap_in_result,
    clippy::unwrap_used,
    clippy::wildcard_imports,
    unreachable_pub,
    dead_code,
    reason = "test code"
)]

mod support;

use dhat as _;
use hex as _;
use mk_bridge::protocol::*;
use mk_bridge::{Bridge, MAX_REQUEST_BYTES};
use mk_core::{self as _, AccountSigner};
use serde as _;
use serde_json::{Value, json};
use support::*;
use thiserror as _;
use tokio as _;
use tracing as _;

use std::collections::BTreeSet;

/// A media-policy component for `create_group`/`update_app_components`.
fn media_component() -> ComponentUpdate {
    let policy =
        mk_core::media::EncryptedMediaPolicy::blossom(&["https://blob.example/".to_owned()]);
    let update = policy.to_component().expect("component encodes");
    ComponentUpdate {
        component_id: update.component_id,
        data: update.data,
    }
}

fn attachment_input() -> AttachmentInput {
    AttachmentInput {
        plaintext: vec![0x70, 0x69, 0x63],
        media_type: "image/png".to_owned(),
        filename: "pic.png".to_owned(),
        dim: None,
        thumbhash: None,
    }
}

fn locator() -> Locator {
    Locator {
        kind: "blossom-v1".to_owned(),
        value: "https://blob.example/a".to_owned(),
    }
}

fn now(member: &Member) -> Time {
    Time {
        unix_ms: member.unix_ms,
        monotonic_ms: member.mono_ms,
    }
}

/// Every `Request` variant exercised at least once (success or typed
/// error), against a live session.
#[tokio::test(flavor = "current_thread")]
async fn every_request_variant() {
    let mut relay = FakeRelay::new();
    let mut a = Member::new();
    let mut b = Member::new();

    let mut covered = BTreeSet::new();
    macro_rules! mark {
        ($tag:expr, $response:expr $(,)?) => {{
            assert!(
                matches!($response, Response::Ok { .. } | Response::Error { .. }),
                "{}: {:?}",
                $tag,
                $response
            );
            covered.insert(($tag).to_owned());
        }};
    }

    // A opens with the two-phase proof so submit_proof is covered here.
    let config = a.config();
    let OpenResponse::NeedsAccountProof { unsigned } =
        serde_json::from_slice(&a.bridge.open(&config)).expect("open decodes")
    else {
        panic!("fresh open needs a proof")
    };
    let signed = a.signer.sign_event(unsigned.into()).await.expect("sign");
    mark!(
        "submit_proof",
        &a.call(Request::SubmitProof(SubmitProofRequest {
            event: signed.clone().into(),
        }))
        .await,
    );
    b.open().await;
    mark!(
        "ensure_key_package",
        &a.call(Request::EnsureKeyPackage(EnsureKeyPackageRequest {
            write_relays: vec![RELAY.to_owned()],
        }))
        .await,
    );
    mark!(
        "key_package_status",
        &a.call(Request::KeyPackageStatus(NoArgs {})).await,
    );
    mark!(
        "audit_key_package_slot",
        &a.call(Request::AuditKeyPackageSlot(AuditKeyPackageSlotRequest {
            events: Vec::new(),
        }))
        .await,
    );
    mark!(
        "publish_inbox_relays",
        &a.call(Request::PublishInboxRelays(PublishInboxRelaysRequest {
            relays: vec![RELAY.to_owned()],
        }))
        .await,
    );
    a.publish(&mut relay).await;
    b.publish_identity(&mut relay).await;

    // create_group with B as invitee, media component enabled.
    let response = a
        .call(Request::CreateGroup(CreateGroupRequest {
            name: "g".to_owned(),
            description: "d".to_owned(),
            relays: vec![RELAY.to_owned()],
            invitees: vec![Invitee {
                key_package: key_package_of(&relay, &b.account_hex()),
                inbox_relays: vec![RELAY.to_owned()],
            }],
            own_devices: Vec::new(),
            admins: Vec::new(),
            components: vec![media_component()],
        }))
        .await;
    mark!("create_group", &response);
    let OpResult::GroupCreated(created) = ok_result(response) else {
        panic!("create_group");
    };
    let group = created.group.id.clone();
    a.publish(&mut relay).await;

    // B joins via its gift-wrapped Welcome (nip44 through signer_pending).
    let welcomes = welcomes_for(&relay, &b.account_hex());
    assert_ne!(welcomes.len(), 0, "welcome published");
    mark!(
        "ingest",
        &b.call(Request::Ingest(IngestRequest {
            events: welcomes.clone()
        }))
        .await
    );
    b.sync(&mut relay, &group).await;
    a.sync(&mut relay, &group).await;

    // send_message, then a real publish to feed report_publish.
    a.advance(1_000);
    mark!(
        "send_message",
        &a.call(Request::SendMessage(SendMessageRequest {
            group: group.clone(),
            message: AppMessage {
                kind: 9,
                tags: Vec::new(),
                content: "hi".to_owned(),
            },
        }))
        .await,
    );
    mark!(
        "awaiting_durability",
        &a.call(Request::AwaitingDurability(NoArgs {})).await
    );
    mark!(
        "take_outbound",
        &a.call(Request::TakeOutbound(NoArgs {})).await
    );
    a.publish(&mut relay).await; // exercises ack_durable + report_publish accepted
    covered.insert("ack_durable".to_owned());
    covered.insert("report_publish".to_owned());

    // report_publish for an unknown id answers a typed error too.
    let response = a
        .call(Request::ReportPublish(ReportPublishRequest {
            id: [0x77; 32],
            results: vec![RelayResult {
                relay: RELAY.to_owned(),
                status: RelayStatus::Accepted,
            }],
        }))
        .await;
    assert_eq!(error_kind(&response), "unknown_outbound");

    // membership operations.
    let mut c = Member::new();
    c.open().await;
    c.publish_identity(&mut relay).await;
    mark!(
        "invite",
        &a.call(Request::Invite(InviteRequest {
            group: group.clone(),
            invitees: vec![Invitee {
                key_package: key_package_of(&relay, &c.account_hex()),
                inbox_relays: vec![RELAY.to_owned()],
            }],
        }))
        .await,
    );
    a.publish(&mut relay).await;
    let welcomes = welcomes_for(&relay, &c.account_hex());
    c.call(Request::Ingest(IngestRequest { events: welcomes }))
        .await;
    c.sync(&mut relay, &group).await;
    a.sync(&mut relay, &group).await;

    mark!(
        "update_group_profile",
        &a.call(Request::UpdateGroupProfile(UpdateGroupProfileRequest {
            group: group.clone(),
            name: Some("g2".to_owned()),
            description: None,
        }))
        .await,
    );
    mark!(
        "update_app_components",
        &a.call(Request::UpdateAppComponents(UpdateAppComponentsRequest {
            group: group.clone(),
            updates: vec![media_component()],
        }))
        .await,
    );
    mark!(
        "self_update",
        &a.call(Request::SelfUpdate(SelfUpdateRequest {
            group: group.clone(),
        }))
        .await,
    );
    mark!(
        "remove_members",
        &a.call(Request::RemoveMembers(RemoveMembersRequest {
            group: group.clone(),
            members: vec![c.signer.account().0],
        }))
        .await,
    );
    a.publish(&mut relay).await;

    // Media: seal, send, open through the bridge handle table.
    let sealed = a
        .call(Request::SealAttachment(SealAttachmentRequest {
            group: group.clone(),
            attachment: attachment_input(),
        }))
        .await;
    mark!("seal_attachment", &sealed);
    let OpResult::SealedAttachment(sealed) = ok_result(sealed) else {
        panic!("seal_attachment");
    };
    mark!(
        "send_media",
        &a.call(Request::SendMedia(SendMediaRequest {
            group: group.clone(),
            caption: "pic".to_owned(),
            attachments: vec![MediaAttachmentRef {
                handle: sealed.handle,
                locators: vec![locator()],
            }],
            tags: Vec::new(),
        }))
        .await,
    );
    a.publish(&mut relay).await;

    let spare = a
        .call(Request::SealAttachment(SealAttachmentRequest {
            group: group.clone(),
            attachment: attachment_input(),
        }))
        .await;
    let OpResult::SealedAttachment(spare) = ok_result(spare) else {
        panic!("seal for discard");
    };
    mark!(
        "discard_attachment",
        &a.call(Request::DiscardAttachment(DiscardAttachmentRequest {
            handle: spare.handle,
        }))
        .await,
    );

    // B opens the attachment A sent (blob store = ciphertext it sealed).
    let route = b.route_hex(&group).await;
    let mut events = Vec::new();
    for _ in 0..8 {
        let backlog = group_events(&relay, &route);
        events.extend(b.ingest_and_drain(backlog).await);
        let response = b.call(Request::Tick(NoArgs {})).await;
        assert!(matches!(response, Response::Ok { .. }), "tick");
        b.advance(2_000);
        b.publish(&mut relay).await;
    }
    let mut opened = false;
    for event in &events {
        let SessionEvent::MessageReceived { message, .. } = event else {
            continue;
        };
        for attachment in &message.attachments {
            let AttachmentResult::Ok { reference } = attachment else {
                continue;
            };
            let response = b
                .call(Request::OpenAttachment(OpenAttachmentRequest {
                    group: group.clone(),
                    message: message.transport_id,
                    reference: reference.clone(),
                    ciphertext: sealed.ciphertext.clone(),
                }))
                .await;
            mark!("open_attachment", &response);
            let OpResult::Attachment(data) = ok_result(response) else {
                panic!("open_attachment");
            };
            assert_eq!(data.plaintext, attachment_input().plaintext);
            opened = true;
        }
    }
    assert!(opened, "B opened A's attachment");

    mark!("tick", &a.call(Request::Tick(NoArgs {})).await);
    mark!(
        "acknowledge_messages",
        &a.call(Request::AcknowledgeMessages(AcknowledgeMessagesRequest {
            ids: vec![[0x11u8; 32]],
        }))
        .await,
    );
    mark!("groups", &a.call(Request::Groups(NoArgs {})).await);
    mark!(
        "group",
        &a.call(Request::Group(GroupRequest {
            group: group.clone(),
        }))
        .await,
    );
    mark!(
        "select_key_packages",
        &a.call(Request::SelectKeyPackages(SelectKeyPackagesRequest {
            account: b.signer.account().0,
            events: vec![key_package_of(&relay, &b.account_hex())],
            group: Some(group.clone()),
        }))
        .await,
    );
    mark!(
        "rejoin_offers",
        &a.call(Request::RejoinOffers(RejoinOffersRequest {
            group: group.clone(),
        }))
        .await,
    );
    mark!(
        "confirm_rejoin",
        &a.call(Request::ConfirmRejoin(ConfirmRejoinRequest {
            group: group.clone(),
            offer: [0xAA; 32],
            token: [0xBB; 32],
        }))
        .await,
    );
    mark!(
        "decline_rejoin",
        &a.call(Request::DeclineRejoin(DeclineRejoinRequest {
            offer: [0xAA; 32],
        }))
        .await,
    );
    mark!(
        "readd_member",
        &a.call(Request::ReaddMember(ReaddMemberRequest {
            group: group.clone(),
            account: b.signer.account().0,
            invitees: vec![Invitee {
                key_package: key_package_of(&relay, &a.account_hex()),
                inbox_relays: Vec::new(),
            }],
        }))
        .await,
    );
    // Protocol-v4 app surface requests against the live group.
    mark!(
        "send_intent",
        &a.call(Request::SendIntent(SendIntentRequest {
            group: group.clone(),
            intent: MessageIntent::Chat {
                content: "typed".to_owned(),
                reply_to: None,
            },
        }))
        .await,
    );
    mark!(
        "set_admin",
        &a.call(Request::SetAdmin(SetAdminRequest {
            group: group.clone(),
            account: b.signer.account().0,
            admin: true,
        }))
        .await,
    );
    mark!(
        "set_group_avatar",
        &a.call(Request::SetGroupAvatar(SetGroupAvatarRequest {
            group: group.clone(),
            avatar: Some(GroupAvatar {
                url: "https://example.com/a.png".to_owned(),
                dim: Some("64x64".to_owned()),
                thumbhash: None,
            }),
        }))
        .await,
    );
    mark!(
        "set_retention",
        &a.call(Request::SetRetention(SetRetentionRequest {
            group: group.clone(),
            seconds: 60,
        }))
        .await,
    );
    // `leave` on a second group member so it is a real send.
    mark!(
        "leave",
        &b.call(Request::Leave(LeaveRequest {
            group: group.clone()
        }))
        .await
    );
    // `history_fetched` after `leave` reports the group no longer live.
    mark!(
        "history_fetched",
        &b.call(Request::HistoryFetched(HistoryFetchedRequest {
            group: group.clone()
        }))
        .await,
    );
    // `sign_out` after `leave` reports the group already left.
    mark!(
        "sign_out",
        &b.call(Request::SignOut(SignOutRequest {
            write_relays: vec![RELAY.to_owned()]
        }))
        .await,
    );
    // `signer_reply` while Ready is a typed protocol error.
    mark!(
        "signer_reply",
        &a.call(Request::SignerReply(SignerReplyRequest {
            replies: Vec::new(),
        }))
        .await,
    );
    mark!(
        "enable_disbanding",
        &a.call(Request::EnableDisbanding(EnableDisbandingRequest {
            group: group.clone(),
        }))
        .await,
    );
    mark!(
        "disband",
        &a.call(Request::Disband(DisbandRequest {
            group: group.clone(),
        }))
        .await,
    );
    mark!(
        "forget_group",
        &a.call(Request::ForgetGroup(ForgetGroupRequest {
            group: group.clone(),
        }))
        .await,
    );

    let expected: BTreeSet<String> = REQUEST_TYPES.iter().map(|tag| (*tag).to_owned()).collect();
    assert_eq!(covered, expected, "every request variant exercised");
    // submit_proof was exercised during open: fresh-account needs_account_proof.
    let mut fresh = Member::new();
    let opened = fresh.open_bytes(
        &serde_json::to_vec(&Config {
            protocol_version: PROTOCOL_VERSION,
            account: fresh.signer.account().0,
            now: now(&fresh),
            allow_loopback: true,
            client_name: None,
        })
        .expect("config"),
    );
    assert!(
        matches!(opened, OpenResponse::NeedsAccountProof { .. }),
        "fresh account needs proof (submit_proof exercised by Member::open)"
    );
}

/// Full flow: A creates → invites B → B joins → 500 messages A→B →
/// restart both → add C → remove C → publish failure → retry accepted.
#[tokio::test(flavor = "current_thread")]
async fn full_flow() {
    let mut relay = FakeRelay::new();
    let mut a = Member::new();
    let mut b = Member::new();
    a.open().await;
    b.open().await;
    a.publish_identity(&mut relay).await;
    b.publish_identity(&mut relay).await;

    let response = a
        .call(Request::CreateGroup(CreateGroupRequest {
            name: "flow".to_owned(),
            description: String::new(),
            relays: vec![RELAY.to_owned()],
            invitees: vec![Invitee {
                key_package: key_package_of(&relay, &b.account_hex()),
                inbox_relays: vec![RELAY.to_owned()],
            }],
            own_devices: Vec::new(),
            admins: Vec::new(),
            components: Vec::new(),
        }))
        .await;
    let OpResult::GroupCreated(created) = ok_result(response) else {
        panic!("create_group");
    };
    let group = created.group.id.clone();
    a.publish(&mut relay).await;

    let welcomes = welcomes_for(&relay, &b.account_hex());
    b.call(Request::Ingest(IngestRequest { events: welcomes }))
        .await;
    b.sync(&mut relay, &group).await;
    a.sync(&mut relay, &group).await;

    for i in 0..500 {
        a.send_chat(&mut relay, &group, &format!("m{i}")).await;
    }
    let route = b.route_hex(&group).await;
    // One ingest delivers the whole backlog: `drain_events` is not
    // budgeted and the ingest already settles every deferred peel (the
    // deterministic `until_settled` form), so a single ingest + tick
    // suffices — verified: all 500 messages arrive in the first drain.
    let backlog = group_events(&relay, &route);
    let events = b.ingest_and_drain(backlog).await;
    let received = received_messages(&events);
    let response = b.call(Request::Tick(NoArgs {})).await;
    assert!(matches!(response, Response::Ok { .. }), "tick");
    b.advance(2_000);
    b.publish(&mut relay).await;
    assert_eq!(received.len(), 500, "B delivered every message");
    assert!(received.contains("m0") && received.contains("m499"));

    a.restart().await;
    b.restart().await;
    a.sync(&mut relay, &group).await;
    b.sync(&mut relay, &group).await;

    let mut c = Member::new();
    c.open().await;
    c.publish_identity(&mut relay).await;
    let response = a
        .call(Request::Invite(InviteRequest {
            group: group.clone(),
            invitees: vec![Invitee {
                key_package: key_package_of(&relay, &c.account_hex()),
                inbox_relays: vec![RELAY.to_owned()],
            }],
        }))
        .await;
    assert!(matches!(response, Response::Ok { .. }), "invite c");
    a.publish(&mut relay).await;
    let welcomes = welcomes_for(&relay, &c.account_hex());
    c.call(Request::Ingest(IngestRequest { events: welcomes }))
        .await;
    c.sync(&mut relay, &group).await;
    a.sync(&mut relay, &group).await;

    let response = a
        .call(Request::RemoveMembers(RemoveMembersRequest {
            group: group.clone(),
            members: vec![c.signer.account().0],
        }))
        .await;
    assert!(matches!(response, Response::Ok { .. }), "remove c");
    a.publish(&mut relay).await;
    c.sync(&mut relay, &group).await;
    a.sync(&mut relay, &group).await;

    // Publish failure: every relay rejects, then retry succeeds.
    a.advance(1_000);
    let response = a
        .call(Request::UpdateGroupProfile(UpdateGroupProfileRequest {
            group: group.clone(),
            name: Some("after".to_owned()),
            description: None,
        }))
        .await;
    assert!(matches!(response, Response::Ok { .. }), "profile update");
    relay.down = true;
    let failed = a.publish(&mut relay).await;
    assert!(
        !failed.is_empty()
            && failed
                .iter()
                .all(|(_, resolution)| matches!(resolution, PublishResolution::Pending)),
        "all-unavailable verdicts stay pending: {failed:?}"
    );
    relay.down = false;
    let response = a.call(Request::Tick(NoArgs {})).await;
    let Response::Ok { result } = response else {
        panic!("tick: {response:?}")
    };
    let OpResult::Ticked(TickedResult { next_wake_ms }) = *result else {
        panic!("tick result: {result:?}")
    };
    // The session paces the retry: the item re-offers only once its
    // backoff deadline passes.
    a.advance(next_wake_ms.unwrap_or(0).max(1_000));
    let retried = a.publish(&mut relay).await;
    assert!(
        retried
            .iter()
            .any(|(_, resolution)| !matches!(resolution, PublishResolution::Pending)),
        "retry reached a terminal resolution: {retried:?}"
    );
}

/// Two-phase identity proof: fresh account → needs_account_proof → wrong
/// account / wrong event → proof_rejected → correct → ready → reopen
/// ready directly.
#[tokio::test(flavor = "current_thread")]
async fn two_phase_proof() {
    let mut member = Member::new();
    let config = serde_json::to_vec(&Config {
        protocol_version: PROTOCOL_VERSION,
        account: member.signer.account().0,
        now: now(&member),
        allow_loopback: true,
        client_name: None,
    })
    .expect("config");
    let opened = member.open_bytes(&config);
    let OpenResponse::NeedsAccountProof { unsigned } = opened else {
        panic!("fresh account must need a proof: {opened:?}");
    };

    // Wrong account: a different signer signs a proof-shaped event under
    // its own key (same body, different pubkey).
    let stranger = mk_core::testing::LocalSigner::new();
    let mut foreign: mk_core::UnsignedEvent = unsigned.clone().into();
    foreign.pubkey = stranger.account();
    let wrong_account = stranger.sign_event(foreign).await.expect("sign");
    let response = member
        .call(Request::SubmitProof(SubmitProofRequest {
            event: wrong_account.into(),
        }))
        .await;
    assert_eq!(error_kind(&response), "proof_rejected");
    let Response::Error { error } = &response else {
        panic!()
    };
    assert!(
        matches!(&**error, mk_bridge::BridgeError::ProofRejected { reason } if reason == "wrong_account")
    );

    // Wrong event: the right account signs an unrelated event.
    let wrong_event = member
        .signer
        .sign_event(mk_core::UnsignedEvent {
            pubkey: member.signer.account(),
            created_at: 1_700_000_000,
            kind: 1,
            tags: Vec::new(),
            content: "not the proof".to_owned(),
        })
        .await
        .expect("sign");
    let response = member
        .call(Request::SubmitProof(SubmitProofRequest {
            event: wrong_event.into(),
        }))
        .await;
    assert_eq!(error_kind(&response), "proof_rejected");

    // Correct proof.
    let signed = member
        .signer
        .sign_event(unsigned.into())
        .await
        .expect("sign");
    let response = member
        .call(Request::SubmitProof(SubmitProofRequest {
            event: signed.into(),
        }))
        .await;
    let OpResult::Ready(_) = ok_result(response) else {
        panic!("proof accepted")
    };

    // Reopen: ready directly, no second proof.
    member.bridge.close();
    member.bridge = Bridge::new(member.host.clone());
    let opened = member.open().await;
    assert!(matches!(opened, OpenResponse::Ready { .. }));
}

/// The durability gate: staged outbound hidden until ack; lower/equal acks
/// are cumulative no-ops; future seqs are unknown; an ack while suspended
/// applies when the op completes.
#[tokio::test(flavor = "current_thread")]
async fn durability_gate() {
    let mut relay = FakeRelay::new();
    let mut a = Member::new();
    let mut b = Member::new();
    a.open().await;
    b.open().await;
    a.publish_identity(&mut relay).await;
    b.publish_identity(&mut relay).await;
    let response = a
        .call(Request::CreateGroup(CreateGroupRequest {
            name: "g".to_owned(),
            description: String::new(),
            relays: vec![RELAY.to_owned()],
            invitees: Vec::new(),
            own_devices: Vec::new(),
            admins: Vec::new(),
            components: Vec::new(),
        }))
        .await;
    let OpResult::GroupCreated(created) = ok_result(response) else {
        panic!()
    };
    let group = created.group.id.clone();
    a.publish(&mut relay).await;

    // Stage a send; the item must not be released before its batch is acked.
    a.advance(1_000);
    let response = a
        .call(Request::SendMessage(SendMessageRequest {
            group: group.clone(),
            message: AppMessage {
                kind: 9,
                tags: Vec::new(),
                content: "gated".to_owned(),
            },
        }))
        .await;
    assert!(matches!(response, Response::Ok { .. }));
    let Response::Ok { result } = a.call(Request::TakeOutbound(NoArgs {})).await else {
        panic!()
    };
    let OpResult::Outbound(OutboundResult { items }) = *result else {
        panic!()
    };
    assert!(items.is_empty(), "unacked outbound stays gated");

    // Future seq is refused.
    let last = a
        .host
        .state
        .lock()
        .unwrap()
        .history
        .last()
        .map(|b| b.seq.get())
        .unwrap_or(0);
    let response = a
        .call(Request::AckDurable(AckDurableRequest { seq: last + 1 }))
        .await;
    assert_eq!(error_kind(&response), "unknown_seq");

    // Ack up to the last committed batch: released.
    let response = a
        .call(Request::AckDurable(AckDurableRequest { seq: last }))
        .await;
    assert!(matches!(response, Response::Ok { .. }));
    let Response::Ok { result } = a.call(Request::TakeOutbound(NoArgs {})).await else {
        panic!()
    };
    let OpResult::Outbound(OutboundResult { items }) = *result else {
        panic!()
    };
    assert_eq!(items.len(), 1, "acked outbound released");

    // Repeating / lower acks are cumulative no-ops.
    for seq in [last, last.saturating_sub(1), 0] {
        let response = a.call(Request::AckDurable(AckDurableRequest { seq })).await;
        assert!(matches!(response, Response::Ok { .. }), "ack {seq} is done");
    }

    // An ack_durable received while suspended applies when the op
    // completes — it covers the batches committed before the suspension.
    a.advance(1_000);
    let response = a
        .call(Request::SendMessage(SendMessageRequest {
            group: group.clone(),
            message: AppMessage {
                kind: 9,
                tags: Vec::new(),
                content: "suspended-gate".to_owned(),
            },
        }))
        .await;
    assert!(matches!(response, Response::Ok { .. }), "send while ready");
    let gated_seq = a
        .host
        .state
        .lock()
        .unwrap()
        .history
        .last()
        .map(|b| b.seq.get())
        .unwrap_or(0);
    // Past the weekly refresh the slot wants a replacement: the ensure
    // suspends on the signer like the publish request it replaced.
    a.advance(8 * 86_400_000);
    let response = a.call_once(Request::EnsureKeyPackage(EnsureKeyPackageRequest {
        write_relays: vec![RELAY.to_owned()],
    }));
    let Response::SignerPending { requests } = &response else {
        panic!("key package signs: {response:?}");
    };
    assert_ne!(requests.len(), 0);
    let response = a
        .call(Request::AckDurable(AckDurableRequest { seq: gated_seq }))
        .await;
    assert!(matches!(response, Response::Ok { .. }), "deferred ack");
    let mut replies = Vec::new();
    for request in requests {
        replies.push(SignerReply {
            id: request.id,
            result: a.answer(&request.op).await,
        });
    }
    let response = a
        .call(Request::SignerReply(SignerReplyRequest { replies }))
        .await;
    assert!(matches!(response, Response::Ok { .. }), "op completes");
    // The deferred ack released the pre-suspension send; the op's own
    // batch was committed at completion and still waits on its own ack.
    let Response::Ok { result } = a.call(Request::TakeOutbound(NoArgs {})).await else {
        panic!()
    };
    let OpResult::Outbound(OutboundResult { items }) = *result else {
        panic!()
    };
    assert!(
        items
            .iter()
            .any(|item| item.purpose == OutboundPurpose::ApplicationMessage),
        "deferred ack released the staged send: {items:?}"
    );
    assert!(
        !items
            .iter()
            .any(|item| item.purpose == OutboundPurpose::KeyPackage),
        "the op's own batch still waits on its own ack"
    );
    let response = a.call(Request::AwaitingDurability(NoArgs {})).await;
    let Response::Ok { result } = response else {
        panic!()
    };
    let OpResult::AwaitingDurability(AwaitingDurabilityResult { seq: Some(seq) }) = *result else {
        panic!("key package batch still gated: {result:?}")
    };
    let response = a.call(Request::AckDurable(AckDurableRequest { seq })).await;
    assert!(matches!(response, Response::Ok { .. }));
    let Response::Ok { result } = a.call(Request::TakeOutbound(NoArgs {})).await else {
        panic!()
    };
    let OpResult::Outbound(OutboundResult { items }) = *result else {
        panic!()
    };
    assert!(
        items
            .iter()
            .any(|item| item.purpose == OutboundPurpose::KeyPackage),
        "acked key package released"
    );
}

/// A host `refused`/`failed` signer reply surfaces as a `signer` error
/// carrying the matching reason; the bridge settles back to Ready and the
/// refused operation releases nothing. On the welcome path the same refusal
/// fails the gift-wrap unwrap inside the engine: the ingest reports the
/// input `unavailable` with the matching `signer_*` category and stays
/// retryable — nothing of the failed span becomes durable or remembered,
/// so re-ingesting the same welcomes with a working signer joins.
#[tokio::test(flavor = "current_thread")]
async fn signer_refusal() {
    let mut relay = FakeRelay::new();

    for (result, reason) in [
        (SignerResult::Refused, "refused"),
        (SignerResult::Failed, "failed"),
    ] {
        let mut a = Member::new();
        a.open().await;
        let response = a.call_once(Request::EnsureKeyPackage(EnsureKeyPackageRequest {
            write_relays: vec![RELAY.to_owned()],
        }));
        let Response::SignerPending { requests } = response else {
            panic!("ensure_key_package suspends: {response:?}")
        };
        let replies = requests
            .iter()
            .map(|request| SignerReply {
                id: request.id,
                result: result.clone(),
            })
            .collect();
        let response = a.call_once(Request::SignerReply(SignerReplyRequest { replies }));
        assert_eq!(error_kind(&response), "signer");
        assert_eq!(error_reason(&response), reason);

        // Settled back to Ready; ack what the op committed and nothing of
        // it is released.
        let response = a.call(Request::Groups(NoArgs {})).await;
        assert!(
            matches!(response, Response::Ok { .. }),
            "ready: {response:?}"
        );
        let last = a
            .host
            .state
            .lock()
            .unwrap()
            .history
            .last()
            .map(|batch| batch.seq.get())
            .unwrap_or(0);
        let response = a
            .call(Request::AckDurable(AckDurableRequest { seq: last }))
            .await;
        assert!(matches!(response, Response::Ok { .. }), "ack: {response:?}");
        let Response::Ok { result: taken } = a.call(Request::TakeOutbound(NoArgs {})).await else {
            panic!()
        };
        let OpResult::Outbound(OutboundResult { items }) = *taken else {
            panic!()
        };
        assert!(items.is_empty(), "a {reason} reply releases no outbound");
    }

    // nip44: B's welcome ingest decrypts through the signer.
    let mut a = Member::new();
    a.open().await;
    a.publish_identity(&mut relay).await;
    for (refusal, category) in [
        (SignerResult::Refused, AvailabilityCategory::SignerRefused),
        (SignerResult::Failed, AvailabilityCategory::SignerFailed),
    ] {
        let mut b = Member::new();
        b.open().await;
        b.publish_identity(&mut relay).await;
        let response = a
            .call(Request::CreateGroup(CreateGroupRequest {
                name: "g".to_owned(),
                description: String::new(),
                relays: vec![RELAY.to_owned()],
                invitees: vec![Invitee {
                    key_package: key_package_of(&relay, &b.account_hex()),
                    inbox_relays: vec![RELAY.to_owned()],
                }],
                own_devices: Vec::new(),
                admins: Vec::new(),
                components: Vec::new(),
            }))
            .await;
        assert!(
            matches!(response, Response::Ok { .. }),
            "create: {response:?}"
        );
        a.publish(&mut relay).await;
        let welcomes = welcomes_for(&relay, &b.account_hex());
        assert_ne!(welcomes.len(), 0, "welcome published");
        let response = b.call_once(Request::Ingest(IngestRequest {
            events: welcomes.clone(),
        }));
        let Response::SignerPending { requests } = response else {
            panic!("welcome ingest suspends on nip44: {response:?}")
        };
        assert!(
            requests
                .iter()
                .any(|request| matches!(&request.op, SignerOp::Nip44Decrypt { .. })),
            "welcome unwrap asks nip44_decrypt: {requests:?}"
        );
        let replies = requests
            .iter()
            .map(|request| SignerReply {
                id: request.id,
                result: refusal.clone(),
            })
            .collect();
        let response = b.call_once(Request::SignerReply(SignerReplyRequest { replies }));
        let Response::Ok { result } = response else {
            panic!("refused welcome reports, not errors: {response:?}")
        };
        let OpResult::Ingested(IngestedResult { reports }) = *result else {
            panic!()
        };
        assert!(
            reports.iter().all(
                |report| matches!(report.outcome, InputOutcome::Unavailable {
                    category: reported,
                } if reported == category)
            ),
            "refused welcome stays retryable: {reports:?}"
        );

        // The bridge settles back to Ready and B never joined the group.
        let response = b.call(Request::Groups(NoArgs {})).await;
        let Response::Ok { result } = response else {
            panic!("b ready: {response:?}")
        };
        let OpResult::Groups(GroupsResult { groups }) = *result else {
            panic!()
        };
        assert!(groups.is_empty(), "a refused welcome leaves no group");

        // Nothing of the refused span became durable or remembered:
        // re-ingesting the same welcomes, answered by the working signer,
        // joins the group.
        let response = b
            .call(Request::Ingest(IngestRequest { events: welcomes }))
            .await;
        let Response::Ok { result } = response else {
            panic!("retry reports: {response:?}")
        };
        let OpResult::Ingested(IngestedResult { reports }) = *result else {
            panic!()
        };
        assert!(
            reports
                .iter()
                .all(|report| matches!(report.outcome, InputOutcome::Accepted)),
            "retried welcome accepted: {reports:?}"
        );
        let response = b.call(Request::Groups(NoArgs {})).await;
        let Response::Ok { result } = response else {
            panic!("b ready: {response:?}")
        };
        let OpResult::Groups(GroupsResult { groups }) = *result else {
            panic!()
        };
        assert_eq!(groups.len(), 1, "a retried welcome joins the group");
    }
}

/// A host `invalid_payload` reply to a pending `nip44_decrypt` is the
/// signer's verdict that the wrap itself is undecryptable: the ingest
/// reports `rejected: invalid_encoding` — a permanent rejection, not
/// `unavailable` — the dedup marker makes a re-delivery `duplicate`,
/// and the signer is never asked again.
#[tokio::test(flavor = "current_thread")]
async fn signer_invalid_payload() {
    let mut relay = FakeRelay::new();

    let mut a = Member::new();
    a.open().await;
    a.publish_identity(&mut relay).await;

    let mut b = Member::new();
    b.open().await;
    b.publish_identity(&mut relay).await;
    let response = a
        .call(Request::CreateGroup(CreateGroupRequest {
            name: "g".to_owned(),
            description: String::new(),
            relays: vec![RELAY.to_owned()],
            invitees: vec![Invitee {
                key_package: key_package_of(&relay, &b.account_hex()),
                inbox_relays: vec![RELAY.to_owned()],
            }],
            own_devices: Vec::new(),
            admins: Vec::new(),
            components: Vec::new(),
        }))
        .await;
    assert!(
        matches!(response, Response::Ok { .. }),
        "create: {response:?}"
    );
    a.publish(&mut relay).await;
    let welcomes = welcomes_for(&relay, &b.account_hex());
    assert_ne!(welcomes.len(), 0, "welcome published");

    let response = b.call_once(Request::Ingest(IngestRequest {
        events: welcomes.clone(),
    }));
    let Response::SignerPending { requests } = response else {
        panic!("welcome ingest suspends on nip44: {response:?}")
    };
    // `invalid_payload` only fits `nip44_decrypt`; other ops in the batch
    // get a real answer.
    let mut replies = Vec::with_capacity(requests.len());
    for request in &requests {
        let result = match &request.op {
            SignerOp::Nip44Decrypt { .. } => SignerResult::InvalidPayload,
            op => b.answer(op).await,
        };
        replies.push(SignerReply {
            id: request.id,
            result,
        });
    }
    let response = b.call_once(Request::SignerReply(SignerReplyRequest { replies }));
    let Response::Ok { result } = response else {
        panic!("invalid-payload welcome reports, not errors: {response:?}")
    };
    let OpResult::Ingested(IngestedResult { reports }) = *result else {
        panic!()
    };
    assert!(
        reports.iter().all(|report| matches!(
            report.outcome,
            InputOutcome::Rejected {
                category: InputCategory::InvalidEncoding,
            }
        )),
        "undecryptable welcome is a permanent rejection: {reports:?}"
    );

    // The rejection is remembered: a re-delivery is `duplicate` and the
    // ingest resolves without ever suspending on the signer.
    let response = b.call_once(Request::Ingest(IngestRequest { events: welcomes }));
    let Response::Ok { result } = response else {
        panic!("re-delivery must not suspend on the signer: {response:?}")
    };
    let OpResult::Ingested(IngestedResult { reports }) = *result else {
        panic!()
    };
    assert!(
        reports.iter().all(|report| matches!(
            report.outcome,
            InputOutcome::Rejected {
                category: InputCategory::Duplicate,
            }
        )),
        "re-delivered poison wrap is a duplicate: {reports:?}"
    );
}

/// Ingest order invariance: the same backlog delivered ascending,
/// descending and deterministically shuffled converges to identical
/// snapshots and message sets.
#[tokio::test(flavor = "current_thread")]
async fn ingest_order_invariance() {
    let mut relay = FakeRelay::new();
    let mut a = Member::new();
    let mut receivers: Vec<Member> = (0..3).map(|_| Member::new()).collect();
    a.open().await;
    a.publish_identity(&mut relay).await;
    for member in &mut receivers {
        member.open().await;
        member.publish_identity(&mut relay).await;
    }
    let invitees = receivers
        .iter()
        .map(|member| Invitee {
            key_package: key_package_of(&relay, &member.account_hex()),
            inbox_relays: vec![RELAY.to_owned()],
        })
        .collect();
    let response = a
        .call(Request::CreateGroup(CreateGroupRequest {
            name: "order".to_owned(),
            description: String::new(),
            relays: vec![RELAY.to_owned()],
            invitees,
            own_devices: Vec::new(),
            admins: Vec::new(),
            components: Vec::new(),
        }))
        .await;
    let OpResult::GroupCreated(created) = ok_result(response) else {
        panic!()
    };
    let group = created.group.id.clone();
    a.publish(&mut relay).await;

    for member in &mut receivers {
        let welcomes = welcomes_for(&relay, &member.account_hex());
        member
            .call(Request::Ingest(IngestRequest { events: welcomes }))
            .await;
        member.sync(&mut relay, &group).await;
    }
    a.sync(&mut relay, &group).await;

    for i in 0..20 {
        a.send_chat(&mut relay, &group, &format!("o{i}")).await;
    }
    let route = a.route_hex(&group).await;
    let backlog = group_events(&relay, &route);
    assert!(backlog.len() >= 20);

    let mut snapshots = Vec::new();
    let mut contents = Vec::new();
    for (index, member) in receivers.iter_mut().enumerate() {
        let mut ordered = backlog.clone();
        match index {
            0 => ordered.sort_by(|x, y| x.created_at.cmp(&y.created_at).then(x.id.cmp(&y.id))),
            1 => ordered.sort_by(|x, y| y.created_at.cmp(&x.created_at).then(y.id.cmp(&x.id))),
            _ => ordered.sort_by_key(|event| event.id),
        }
        let events = member.ingest_and_drain(ordered).await;
        contents.push(
            events
                .iter()
                .filter_map(|event| match event {
                    SessionEvent::MessageReceived { message, .. } => Some(message.content.clone()),
                    _ => None,
                })
                .collect::<BTreeSet<_>>(),
        );
        member.sync(&mut relay, &group).await;
        let response = member
            .call(Request::Group(GroupRequest {
                group: group.clone(),
            }))
            .await;
        let OpResult::Group(GroupResult {
            group: Some(snapshot),
        }) = ok_result(response)
        else {
            panic!("group snapshot")
        };
        snapshots.push(serde_json::to_value(*snapshot).expect("snapshot encodes"));
    }
    assert_eq!(snapshots[0], snapshots[1], "descending ingest converges");
    assert_eq!(snapshots[0], snapshots[2], "shuffled ingest converges");
    assert_eq!(contents[0], contents[1]);
    assert_eq!(contents[0], contents[2]);
    assert!(contents[0].len() >= 20, "every receiver saw the backlog");
}

/// Protocol-level errors: version probe, malformed inputs, unsupported
/// types, size cap, clock regression and every lifecycle refusal.
#[tokio::test(flavor = "current_thread")]
async fn protocol_errors() {
    let mut member = Member::new();

    // Version probe first: mismatch answers even with a malformed rest.
    let config = json!({"protocol_version": PROTOCOL_VERSION + 1, "account": 7, "now": "nope"});
    let opened = member.open_bytes(&serde_json::to_vec(&config).unwrap());
    assert_eq!(open_error_kind(&opened), "protocol_version_mismatch");

    // Malformed bodies.
    let opened = member.open_bytes(b"{not json");
    assert_eq!(open_error_kind(&opened), "invalid_request");
    let opened = member.open_bytes(&[0xff, 0xfe]);
    assert_eq!(open_error_kind(&opened), "invalid_request");

    let response = member.call_bytes(b"{not json");
    assert_eq!(error_kind(&response), "invalid_request");
    let response = member.call_bytes(&[0xff, 0xfe]);
    assert_eq!(error_kind(&response), "invalid_request");
    let response = member.call_bytes(&serde_json::to_vec(&json!({"now": "x"})).unwrap());
    assert_eq!(error_kind(&response), "invalid_request");

    // Calls before open.
    let response = member.call_bytes(
        &serde_json::to_vec(&json!({
            "now": {"unix_ms": "1", "monotonic_ms": "1"},
            "request": {"type": "groups"},
        }))
        .unwrap(),
    );
    assert_eq!(error_kind(&response), "not_open");

    // Unknown request type is `unsupported`, not a decode failure.
    let response = member.call_bytes(
        &serde_json::to_vec(&json!({
            "now": {"unix_ms": "1", "monotonic_ms": "1"},
            "request": {"type": "nope_totally_unknown"},
        }))
        .unwrap(),
    );
    assert_eq!(error_kind(&response), "unsupported");

    // Oversize input short-circuits before parsing.
    let response = member.call_bytes(&vec![b'{'; MAX_REQUEST_BYTES + 1]);
    assert_eq!(error_kind(&response), "too_large");

    // Awaiting proof refuses everything but submit_proof.
    let fresh_config = serde_json::to_vec(&Config {
        protocol_version: PROTOCOL_VERSION,
        account: member.signer.account().0,
        now: now(&member),
        allow_loopback: true,
        client_name: None,
    })
    .unwrap();
    let opened = member.open_bytes(&fresh_config);
    assert!(
        matches!(opened, OpenResponse::NeedsAccountProof { .. }),
        "fresh open needs proof: {opened:?}"
    );
    let response = member.call_once(Request::Groups(NoArgs {}));
    assert_eq!(error_kind(&response), "awaiting_proof");

    // Reopening a live bridge is refused.
    let response = member.open_bytes(&fresh_config);
    assert!(
        matches!(response, OpenResponse::Error { .. }),
        "second open: {response:?}"
    );

    // Wrong field types / unknown fields on an open bridge are decode
    // failures, not lifecycle errors.
    let mut busy = Member::new();
    busy.open().await;
    // Unknown fields are refused on argument-less requests too.
    let response = busy.call_bytes(&raw_call(now(&busy), json!({"type": "groups", "extra": 1})));
    assert_eq!(error_kind(&response), "invalid_request");
    let response = busy.call_bytes(&raw_call(
        now(&busy),
        json!({"type": "ack_durable", "seq": "0", "extra": 1}),
    ));
    assert_eq!(error_kind(&response), "invalid_request");
    let response = busy.call_bytes(&raw_call(
        now(&busy),
        json!({"type": "ack_durable", "seq": 7}),
    ));
    assert_eq!(error_kind(&response), "invalid_request");

    // `send_intent`: unknown fields inside each intent variant are refused.
    let group_hex = "aa".repeat(32);
    let intents = [
        json!({"type": "chat", "content": "x", "reply_to": null, "extra": 1}),
        json!({"type": "reaction", "target": group_hex, "emoji": "+1", "extra": 1}),
        json!({"type": "unreact", "reactions": [group_hex], "extra": 1}),
        json!({"type": "edit", "target": group_hex, "content": "x", "extra": 1}),
        json!({"type": "delete", "target": group_hex, "extra": 1}),
        json!({"type": "remove", "target": group_hex, "extra": 1}),
    ];
    for intent in intents {
        let response = busy.call_bytes(&raw_call(
            now(&busy),
            json!({"type": "send_intent", "group": group_hex, "intent": intent}),
        ));
        assert_eq!(
            error_kind(&response),
            "invalid_request",
            "intent with unknown field: {intent}"
        );
    }
    // `set_group_avatar`: unknown fields inside `avatar` are refused.
    let response = busy.call_bytes(&raw_call(
        now(&busy),
        json!({
            "type": "set_group_avatar",
            "group": group_hex,
            "avatar": {"url": "https://example.com/a.png", "extra": 1},
        }),
    ));
    assert_eq!(error_kind(&response), "invalid_request");

    // Busy: suspend on a signer op, then any other request answers busy.
    let response = busy.call_once(Request::EnsureKeyPackage(EnsureKeyPackageRequest {
        write_relays: vec![RELAY.to_owned()],
    }));
    assert!(
        matches!(response, Response::SignerPending { .. }),
        "ensure_key_package suspends"
    );
    let response = busy.call_once(Request::Groups(NoArgs {}));
    assert_eq!(error_kind(&response), "busy");

    // Clock regression on the suspended member: `now` older than the last
    // call's `now` is refused before dispatch.
    let response = busy.call_bytes(&raw_call(
        Time {
            unix_ms: busy.unix_ms.saturating_sub(1),
            monotonic_ms: busy.mono_ms.saturating_sub(1),
        },
        json!({"type": "signer_reply", "replies": []}),
    ));
    assert_eq!(error_kind(&response), "clock_regressed");

    // A suspended bridge closes directly.
    busy.bridge.close();
    let response = busy.call_once(Request::Groups(NoArgs {}));
    assert_eq!(error_kind(&response), "closed");

    // Poisoned: a host commit failure kills the bridge for good.
    let mut broken = Member::new();
    broken.open().await;
    broken.host.fail_after(broken.host.committed());
    let response = broken.call_once(Request::CreateGroup(CreateGroupRequest {
        name: "x".to_owned(),
        description: "x".to_owned(),
        relays: vec![RELAY.to_owned()],
        invitees: Vec::new(),
        own_devices: Vec::new(),
        admins: Vec::new(),
        components: Vec::new(),
    }));
    assert!(
        matches!(response, Response::Error { .. }),
        "commit failure produces an error: {response:?}"
    );
    let response = broken.call_once(Request::Groups(NoArgs {}));
    assert_eq!(error_kind(&response), "poisoned");
    let opened = broken.open_bytes(&fresh_config);
    assert_eq!(open_error_kind(&opened), "already_open");
}

/// One scripted step of the commit/ack crash scenario: `create` a group
/// inviting B, then `send` three messages, `publish`ing after each step.
enum Step {
    Create,
    Publish,
    Send(usize),
}

/// What one scenario replay recorded.
struct ScenarioRun {
    /// Whether each executed step finished without an error response
    /// (steps after the first failure never ran).
    step_ok: Vec<bool>,
    /// Outbound ids (hex) staged by each `send` that returned `Staged`.
    staged: Vec<Vec<String>>,
    /// The created group id, once `create` succeeded.
    group: Vec<u8>,
    /// `host.committed()` when the run stopped.
    total_commits: usize,
}

/// Replay the scenario, stopping at the first failed step (or, when
/// `stop_after_commits` is set, as soon as that commit index is
/// persisted). `accepted` collects every event id that reached a
/// terminal-accept resolution — inserting the same id twice means a
/// finished publish was duplicated.
async fn run_scenario(
    a: &mut Member,
    b_key_package: &Event,
    relay: &mut FakeRelay,
    accepted: &mut BTreeSet<[u8; 32]>,
    publish_steps: bool,
    stop_after_commits: Option<usize>,
) -> ScenarioRun {
    const STEPS: [Step; 8] = [
        Step::Create,
        Step::Publish,
        Step::Send(0),
        Step::Publish,
        Step::Send(1),
        Step::Publish,
        Step::Send(2),
        Step::Publish,
    ];
    let mut run = ScenarioRun {
        step_ok: Vec::new(),
        staged: Vec::new(),
        group: Vec::new(),
        total_commits: 0,
    };
    for step in &STEPS {
        let ok = match step {
            Step::Create => {
                let response = a
                    .call(Request::CreateGroup(CreateGroupRequest {
                        name: "crash".to_owned(),
                        description: String::new(),
                        relays: vec![RELAY.to_owned()],
                        invitees: vec![Invitee {
                            key_package: b_key_package.clone(),
                            inbox_relays: vec![RELAY.to_owned()],
                        }],
                        own_devices: Vec::new(),
                        admins: Vec::new(),
                        components: Vec::new(),
                    }))
                    .await;
                match response {
                    Response::Ok { result } => match *result {
                        OpResult::GroupCreated(created) => {
                            run.group.clone_from(&created.group.id);
                            true
                        }
                        _ => false,
                    },
                    _ => false,
                }
            }
            Step::Send(i) => {
                a.advance(1_000);
                let response = a
                    .call(Request::SendMessage(SendMessageRequest {
                        group: run.group.clone(),
                        message: AppMessage {
                            kind: 9,
                            tags: Vec::new(),
                            content: format!("m{i}"),
                        },
                    }))
                    .await;
                match response {
                    Response::Ok { result } => {
                        if let OpResult::MessageSent(MessageSentResult {
                            outcome: SendOutcome::Staged { ids },
                            ..
                        }) = *result
                        {
                            run.staged.push(ids);
                        }
                        true
                    }
                    _ => false,
                }
            }
            Step::Publish => {
                if publish_steps {
                    let (done, clean) = a.try_publish(relay).await;
                    record_accepts(&done, accepted);
                    clean
                } else {
                    true
                }
            }
        };
        run.step_ok.push(ok);
        run.total_commits = a.host.committed();
        if !ok || stop_after_commits.is_some_and(|n| run.total_commits > n) {
            break;
        }
    }
    run
}

/// Every terminally accepted event id must be new — a second accept for
/// the same event means a finished publish was handed out again.
fn record_accepts(done: &[(Outbound, PublishResolution)], accepted: &mut BTreeSet<[u8; 32]>) {
    for (item, resolution) in done {
        if matches!(
            resolution,
            PublishResolution::Confirmed | PublishResolution::Complete
        ) {
            assert!(
                accepted.insert(item.event.id),
                "event accepted twice: {}",
                hex::encode(item.event.id)
            );
        }
    }
}

/// Crash consistency, as a fault-injection matrix over durable commits.
///
/// a) Commit/ack: A creates a group inviting B, sends three messages,
///    publishing as it goes. For every commit index the scenario produces:
///    (drop) make that commit fail — the bridge poisons and the op dies —
///    then reopen and finish the publish loop; and (torn) persist the
///    batch but crash before its ack — replay ops without publishing so
///    nothing is ever acked, stop once the batch exists, then reopen and
///    publish. Either way the reopen must be `ready`, committed sends
///    must be released or already published, and no event may be
///    terminally accepted twice.
/// b) Mid-ingest: for every (sampled) commit index of a 50-event ingest,
///    fail the commit, reopen from the pre-ingest rows, re-ingest, and
///    converge to the uncrashed receiver's snapshot and message set.
#[tokio::test(flavor = "current_thread")]
async fn crash_consistency() {
    // ---- a) commit/ack matrix over create + three sends ----

    // A quiet baseline run counts the scenario's commit stream.
    let (setup_commits, total_commits) = {
        let mut relay = FakeRelay::new();
        let mut a = Member::new();
        let mut b = Member::new();
        a.open().await;
        b.open().await;
        a.publish_identity(&mut relay).await;
        b.publish_identity(&mut relay).await;
        let setup = a.host.committed();
        let key_package = key_package_of(&relay, &b.account_hex());
        let mut accepted = BTreeSet::new();
        let run = run_scenario(&mut a, &key_package, &mut relay, &mut accepted, true, None).await;
        assert_eq!(run.step_ok, [true; 8], "baseline scenario completes");
        (setup, run.total_commits)
    };
    assert!(
        total_commits > setup_commits,
        "scenario produced commits {setup_commits}..{total_commits}"
    );

    for cut in setup_commits..total_commits {
        // Drop variant: commit index `cut` fails; the op dies mid-flight.
        let mut relay = FakeRelay::new();
        let mut a = Member::new();
        let mut b = Member::new();
        a.open().await;
        b.open().await;
        a.publish_identity(&mut relay).await;
        b.publish_identity(&mut relay).await;
        let key_package = key_package_of(&relay, &b.account_hex());
        let mut accepted = BTreeSet::new();
        a.host.fail_after(cut);
        let run = run_scenario(&mut a, &key_package, &mut relay, &mut accepted, true, None).await;
        a.host.fail_after(usize::MAX);
        a.restart().await;
        // Regenerate queued intents, then finish every releasable item.
        let response = a.call(Request::Tick(NoArgs {})).await;
        assert!(matches!(response, Response::Ok { .. }), "drop {cut}: tick");
        let (done, clean) = a.try_publish(&mut relay).await;
        assert!(clean, "drop {cut}: post-reopen publish must finish");
        record_accepts(&done, &mut accepted);
        // A completed create is durable state: the group survives reopen.
        if run.step_ok.first() == Some(&true) {
            let response = a.call(Request::Groups(NoArgs {})).await;
            let OpResult::Groups(GroupsResult { groups }) = ok_result(response) else {
                panic!("drop {cut}: groups")
            };
            assert!(
                groups.iter().any(|snapshot| snapshot.id == run.group),
                "drop {cut}: committed create lost"
            );
        }
        // Every send whose call finished had its batch committed; each
        // staged outbound is either already published or must release now.
        let published: BTreeSet<[u8; 32]> = relay
            .publishes
            .iter()
            .map(|published| published.item.id)
            .collect();
        for ids in &run.staged {
            for id in ids {
                assert!(
                    published.contains(&hex_to_32(id)),
                    "drop {cut}: staged outbound {id} never published"
                );
            }
        }

        // Torn variant: batch `cut` persisted but never acked — ops run
        // without any publish (nothing is ever acked), stop once the
        // batch exists, then reopen with it included.
        let mut relay = FakeRelay::new();
        let mut a = Member::new();
        let mut b = Member::new();
        a.open().await;
        b.open().await;
        a.publish_identity(&mut relay).await;
        b.publish_identity(&mut relay).await;
        let key_package = key_package_of(&relay, &b.account_hex());
        let mut accepted = BTreeSet::new();
        let run = run_scenario(
            &mut a,
            &key_package,
            &mut relay,
            &mut accepted,
            false,
            Some(cut),
        )
        .await;
        a.restart().await;
        let response = a.call(Request::Tick(NoArgs {})).await;
        assert!(matches!(response, Response::Ok { .. }), "torn {cut}: tick");
        let (done, clean) = a.try_publish(&mut relay).await;
        assert!(clean, "torn {cut}: post-reopen publish must finish");
        record_accepts(&done, &mut accepted);
        let published: BTreeSet<[u8; 32]> = relay
            .publishes
            .iter()
            .map(|published| published.item.id)
            .collect();
        for ids in &run.staged {
            for id in ids {
                assert!(
                    published.contains(&hex_to_32(id)),
                    "torn {cut}: unacked staged outbound {id} never published"
                );
            }
        }
    }

    // ---- b) mid-ingest commit-failure matrix ----
    let mut relay = FakeRelay::new();
    let mut sender = Member::new();
    let mut clean = Member::new();
    let mut crashed = Member::new();
    for member in [&mut sender, &mut clean, &mut crashed] {
        member.open().await;
    }
    sender.publish_identity(&mut relay).await;
    clean.publish_identity(&mut relay).await;
    crashed.publish_identity(&mut relay).await;
    let invitees = [&clean, &crashed]
        .iter()
        .map(|member| Invitee {
            key_package: key_package_of(&relay, &member.account_hex()),
            inbox_relays: vec![RELAY.to_owned()],
        })
        .collect();
    let response = sender
        .call(Request::CreateGroup(CreateGroupRequest {
            name: "ingest".to_owned(),
            description: String::new(),
            relays: vec![RELAY.to_owned()],
            invitees,
            own_devices: Vec::new(),
            admins: Vec::new(),
            components: Vec::new(),
        }))
        .await;
    let OpResult::GroupCreated(created) = ok_result(response) else {
        panic!()
    };
    let group = created.group.id.clone();
    sender.publish(&mut relay).await;
    for member in [&mut clean, &mut crashed] {
        let welcomes = welcomes_for(&relay, &member.account_hex());
        member
            .call(Request::Ingest(IngestRequest { events: welcomes }))
            .await;
        member.sync(&mut relay, &group).await;
    }
    sender.sync(&mut relay, &group).await;
    for i in 0..50 {
        sender.advance(1_000);
        sender
            .call(Request::SendMessage(SendMessageRequest {
                group: group.clone(),
                message: AppMessage {
                    kind: 9,
                    tags: Vec::new(),
                    content: format!("s{i}"),
                },
            }))
            .await;
    }
    sender.publish(&mut relay).await;
    let route = sender.route_hex(&group).await;
    let backlog = group_events(&relay, &route);

    // Count the ingest's own commits on the clean receiver —
    // `acknowledge_messages` would inflate the index space the matrix
    // cuts through.
    let clean_commits_before = clean.host.committed();
    let response = clean
        .call(Request::Ingest(IngestRequest {
            events: backlog.clone(),
        }))
        .await;
    assert!(matches!(response, Response::Ok { .. }), "clean ingest");
    let ingest_commits = clean.host.committed() - clean_commits_before;
    let clean_events = clean.drain_and_ack().await;
    clean.sync(&mut relay, &group).await;
    let clean_snapshot = group_snapshot(&mut clean, &group).await;
    let clean_messages = received_messages(&clean_events);

    let baseline = crashed.host.committed();
    let stride = ingest_commits.div_ceil(40).max(1);
    let mut cuts: Vec<usize> = (0..ingest_commits).step_by(stride).collect();
    if ingest_commits > 0 && cuts.last() != Some(&(ingest_commits - 1)) {
        cuts.push(ingest_commits - 1);
    }
    for cut in cuts {
        // Roll durable state back to the pre-ingest snapshot, then fail
        // the `cut`-th commit of this incarnation's ingest. `truncate`
        // discards the tail permanently — a rolled-back store.
        crashed.host.truncate(baseline);
        crashed.bridge = Bridge::new(crashed.host.clone());
        crashed.acked = 0;
        let opened = crashed.open().await;
        assert!(matches!(opened, OpenResponse::Ready { .. }), "reopen ready");
        let position = crashed.host.committed();
        crashed.host.fail_after(position + cut);
        let response = crashed
            .call(Request::Ingest(IngestRequest {
                events: backlog.clone(),
            }))
            .await;
        assert!(
            matches!(response, Response::Error { .. }),
            "ingest cut {cut} must poison the bridge"
        );
        crashed.host.fail_after(usize::MAX);
        crashed.bridge = Bridge::new(crashed.host.clone());
        crashed.acked = 0;
        let opened = crashed.open().await;
        assert!(matches!(opened, OpenResponse::Ready { .. }), "reopen ready");
        let events = crashed.ingest_and_drain(backlog.clone()).await;
        crashed.sync(&mut relay, &group).await;
        assert_eq!(
            group_snapshot(&mut crashed, &group).await,
            clean_snapshot,
            "ingest cut {cut}: snapshot converges"
        );
        assert_eq!(
            received_messages(&events),
            clean_messages,
            "ingest cut {cut}: message set converges"
        );
    }
}

/// Every vector file entry runs through `open`/`call` without panicking —
/// the same seeds the fuzz targets start from.
#[tokio::test(flavor = "current_thread")]
async fn vectors_and_fuzz_seeds_never_panic() {
    let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../vectors/bridge");
    let mut member = Member::new();
    member.open().await;

    for entry in std::fs::read_dir(&dir).expect("vectors dir") {
        let path = entry.expect("entry").path();
        if path.extension().and_then(|ext| ext.to_str()) != Some("json") {
            continue;
        }
        let bytes = std::fs::read(&path).expect("vector bytes");
        // Feeding any bytes must return a decodable envelope, never panic.
        member.call_bytes(&bytes);
        member.open_bytes(&bytes);
        let _drained = member.bridge.drain();
    }

    // Each request vector as a real `call` envelope.
    let text = std::fs::read_to_string(dir.join("request.json")).expect("request.json");
    let map: serde_json::Map<String, Value> = serde_json::from_str(&text).expect("json");
    for (tag, request) in &map {
        let response = member.call_bytes(&call_envelope(now(&member), request.clone()));
        assert!(
            matches!(
                response,
                Response::Ok { .. } | Response::Error { .. } | Response::SignerPending { .. }
            ),
            "{tag} answered an envelope"
        );
    }

    // The recorded config opens a second bridge without panic.
    let mut second = Member::new();
    let config = std::fs::read(dir.join("config.json")).expect("config");
    second.open_bytes(&config);
}

/// Protocol v4 app surface, end to end over two bridge sessions:
/// `send_intent` typed receive, admin/avatar/retention ops,
/// `disband` to `Disbanded`, `forget_group`, and a non-admin attempt
/// answering `not_group_admin`.
#[tokio::test(flavor = "current_thread")]
async fn app_surface() {
    let mut relay = FakeRelay::new();
    let mut a = Member::new();
    let mut b = Member::new();
    a.open().await;
    b.open().await;
    a.publish_identity(&mut relay).await;
    b.publish_identity(&mut relay).await;

    let response = a
        .call(Request::CreateGroup(CreateGroupRequest {
            name: "v4".to_owned(),
            description: String::new(),
            relays: vec![RELAY.to_owned()],
            invitees: vec![Invitee {
                key_package: key_package_of(&relay, &b.account_hex()),
                inbox_relays: vec![RELAY.to_owned()],
            }],
            own_devices: Vec::new(),
            admins: Vec::new(),
            components: Vec::new(),
        }))
        .await;
    let OpResult::GroupCreated(created) = ok_result(response) else {
        panic!("create_group")
    };
    let group = created.group.id.clone();
    assert!(created.group.disband_enabled, "lifecycle required");
    assert_eq!(created.group.retention_seconds, 0);
    a.publish(&mut relay).await;
    let welcomes = welcomes_for(&relay, &b.account_hex());
    b.call(Request::Ingest(IngestRequest { events: welcomes }))
        .await;
    b.sync(&mut relay, &group).await;
    a.sync(&mut relay, &group).await;

    // `send_intent` chat: the result's `message.id` is the wire inner id
    // the receiver sees, no hashing on the host.
    a.advance(1_000);
    let response = a
        .call(Request::SendIntent(SendIntentRequest {
            group: group.clone(),
            intent: MessageIntent::Chat {
                content: "m1".to_owned(),
                reply_to: None,
            },
        }))
        .await;
    let OpResult::MessageSent(sent) = ok_result(response) else {
        panic!("send_intent chat")
    };
    assert!(matches!(sent.outcome, SendOutcome::Staged { .. }));
    assert_eq!(sent.message.kind, 9);
    let m1 = sent.message.id.clone();
    a.publish(&mut relay).await;

    let route = a.route_hex(&group).await;
    let events = b.ingest_and_drain(group_events(&relay, &route)).await;
    let received = events
        .iter()
        .find_map(|event| match event {
            SessionEvent::MessageReceived { message, .. } => Some(message),
            _ => None,
        })
        .expect("chat received");
    assert_eq!(received.id, m1, "wire inner id equals message_sent.id");
    assert_eq!(received.semantics, Semantics::Chat { reply_to: None });
    assert_eq!(received.expires_at, None, "no retention yet");

    // A reply intent arrives typed with the reply reference.
    a.advance(1_000);
    let m1_id = hex_to_32(&m1);
    let response = a
        .call(Request::SendIntent(SendIntentRequest {
            group: group.clone(),
            intent: MessageIntent::Chat {
                content: "r1".to_owned(),
                reply_to: Some(m1_id),
            },
        }))
        .await;
    let OpResult::MessageSent(reply_sent) = ok_result(response) else {
        panic!("send_intent reply")
    };
    let r1 = reply_sent.message.id.clone();
    a.publish(&mut relay).await;
    let events = b.ingest_and_drain(group_events(&relay, &route)).await;
    let reply = events
        .iter()
        .find_map(|event| match event {
            SessionEvent::MessageReceived { message, .. } if message.id == r1 => {
                Some(message.semantics.clone())
            }
            _ => None,
        })
        .expect("reply received");
    assert_eq!(
        reply,
        Semantics::Chat {
            reply_to: Some(m1_id)
        }
    );

    // B's reaction on m1 arrives typed at A.
    b.advance(1_000);
    let response = b
        .call(Request::SendIntent(SendIntentRequest {
            group: group.clone(),
            intent: MessageIntent::Reaction {
                target: m1_id,
                emoji: "👍".to_owned(),
            },
        }))
        .await;
    let OpResult::MessageSent(reaction_sent) = ok_result(response) else {
        panic!("send_intent reaction")
    };
    assert_eq!(reaction_sent.message.kind, 7);
    b.publish(&mut relay).await;
    let events = a.ingest_and_drain(group_events(&relay, &route)).await;
    let reaction = events
        .iter()
        .find_map(|event| match event {
            SessionEvent::MessageReceived { message, .. } if message.kind == 7 => {
                Some(message.semantics.clone())
            }
            _ => None,
        })
        .expect("reaction received");
    assert_eq!(
        reaction,
        Semantics::Reaction {
            target: m1_id,
            emoji: "👍".to_owned()
        }
    );

    // A non-admin operation is refused without staging.
    let response = b
        .call(Request::SetRetention(SetRetentionRequest {
            group: group.clone(),
            seconds: 60,
        }))
        .await;
    assert_eq!(error_kind(&response), "not_group_admin");

    // Admin ops: promote B, set the avatar, set retention — each lands on
    // B's snapshot after publish + sync.
    let response = a
        .call(Request::SetAdmin(SetAdminRequest {
            group: group.clone(),
            account: b.signer.account().0,
            admin: true,
        }))
        .await;
    let OpResult::SendOutcome(SendOutcomeResult {
        outcome: SendOutcome::Staged { .. },
    }) = ok_result(response)
    else {
        panic!("set_admin")
    };
    a.publish(&mut relay).await;
    b.sync(&mut relay, &group).await;
    a.sync(&mut relay, &group).await;

    let response = a
        .call(Request::SetGroupAvatar(SetGroupAvatarRequest {
            group: group.clone(),
            avatar: Some(GroupAvatar {
                url: "https://example.com/a.png".to_owned(),
                dim: Some("64x64".to_owned()),
                thumbhash: None,
            }),
        }))
        .await;
    assert!(
        matches!(ok_result(response), OpResult::SendOutcome(_)),
        "set_group_avatar"
    );
    a.publish(&mut relay).await;

    let response = a
        .call(Request::SetRetention(SetRetentionRequest {
            group: group.clone(),
            seconds: 60,
        }))
        .await;
    assert!(
        matches!(ok_result(response), OpResult::SendOutcome(_)),
        "set_retention"
    );
    a.publish(&mut relay).await;
    b.sync(&mut relay, &group).await;

    let response = b
        .call(Request::Group(GroupRequest {
            group: group.clone(),
        }))
        .await;
    let OpResult::Group(GroupResult {
        group: Some(snapshot),
    }) = ok_result(response)
    else {
        panic!("b snapshot")
    };
    assert!(snapshot.own_account_admin, "b is admin after promotion");
    let avatar = snapshot.avatar.expect("avatar set");
    assert_eq!(avatar.url, "https://example.com/a.png");
    assert_eq!(avatar.dim.as_deref(), Some("64x64"));
    assert_eq!(snapshot.retention_seconds, 60);
    assert!(snapshot.disband_enabled);
    assert!(!snapshot.disbanding);

    // A message under the policy arrives with `expires_at`.
    a.advance(1_000);
    let response = a
        .call(Request::SendIntent(SendIntentRequest {
            group: group.clone(),
            intent: MessageIntent::Chat {
                content: "expiring".to_owned(),
                reply_to: None,
            },
        }))
        .await;
    let OpResult::MessageSent(expiring) = ok_result(response) else {
        panic!("send_intent expiring")
    };
    a.publish(&mut relay).await;
    let events = b.ingest_and_drain(group_events(&relay, &route)).await;
    let message = events
        .iter()
        .find_map(|event| match event {
            SessionEvent::MessageReceived { message, .. } if message.id == expiring.message.id => {
                Some(message)
            }
            _ => None,
        })
        .expect("expiring message received");
    assert_eq!(message.expires_at, Some(message.created_at + 60));

    // `disband` stages the request; after publish + sync rounds every
    // member's snapshot reads `disbanded`.
    let response = a
        .call(Request::Disband(DisbandRequest {
            group: group.clone(),
        }))
        .await;
    let OpResult::SendOutcome(SendOutcomeResult { outcome }) = ok_result(response) else {
        panic!("disband")
    };
    assert_eq!(outcome, SendOutcome::DisbandRequested);
    a.publish(&mut relay).await;
    for _ in 0..8 {
        a.call(Request::Tick(NoArgs {})).await;
        a.advance(1_000);
        a.publish(&mut relay).await;
        b.sync(&mut relay, &group).await;
        a.sync(&mut relay, &group).await;
        let response = b
            .call(Request::Group(GroupRequest {
                group: group.clone(),
            }))
            .await;
        let OpResult::Group(GroupResult {
            group: Some(snapshot),
        }) = ok_result(response)
        else {
            panic!("b snapshot")
        };
        if matches!(snapshot.state, GroupState::Disbanded) {
            break;
        }
    }
    let response = b
        .call(Request::Group(GroupRequest {
            group: group.clone(),
        }))
        .await;
    let OpResult::Group(GroupResult {
        group: Some(snapshot),
    }) = ok_result(response)
    else {
        panic!("b snapshot")
    };
    assert!(
        matches!(snapshot.state, GroupState::Disbanded),
        "b sees the group disbanded: {:?}",
        snapshot.state
    );

    // `forget_group` on a live second group: `done`, then absent from
    // `groups`.
    let response = a
        .call(Request::CreateGroup(CreateGroupRequest {
            name: "temp".to_owned(),
            description: String::new(),
            relays: vec![RELAY.to_owned()],
            invitees: Vec::new(),
            own_devices: Vec::new(),
            admins: Vec::new(),
            components: Vec::new(),
        }))
        .await;
    let OpResult::GroupCreated(temp) = ok_result(response) else {
        panic!("create temp")
    };
    let response = a
        .call(Request::ForgetGroup(ForgetGroupRequest {
            group: temp.group.id.clone(),
        }))
        .await;
    assert!(
        matches!(ok_result(response), OpResult::Done),
        "forget_group answers done"
    );
    let response = a.call(Request::Groups(NoArgs {})).await;
    let OpResult::Groups(GroupsResult { groups }) = ok_result(response) else {
        panic!("groups")
    };
    assert!(
        !groups.iter().any(|snapshot| snapshot.id == temp.group.id),
        "forgotten group absent"
    );
}

/// The received-message contents of a drained event list, as a set.
fn received_messages(events: &[SessionEvent]) -> BTreeSet<String> {
    events
        .iter()
        .filter_map(|event| match event {
            SessionEvent::MessageReceived { message, .. } => Some(message.content.clone()),
            _ => None,
        })
        .collect()
}

async fn group_snapshot(member: &mut Member, group: &[u8]) -> Value {
    let response = member
        .call(Request::Group(GroupRequest {
            group: group.to_vec(),
        }))
        .await;
    let OpResult::Group(GroupResult { group }) = ok_result(response) else {
        panic!()
    };
    serde_json::to_value(group.expect("group")).expect("snapshot")
}

/// Protocol v4 modifier semantics: an edit ingested before its target is
/// held (nothing surfaces), then delivered as `Semantics::Edit` once the
/// target's index row exists.
#[tokio::test(flavor = "current_thread")]
async fn held_edit_releases_as_edit() {
    let mut relay = FakeRelay::new();
    let mut a = Member::new();
    let mut b = Member::new();
    a.open().await;
    b.open().await;
    a.publish_identity(&mut relay).await;
    b.publish_identity(&mut relay).await;

    let response = a
        .call(Request::CreateGroup(CreateGroupRequest {
            name: "mods".to_owned(),
            description: String::new(),
            relays: vec![RELAY.to_owned()],
            invitees: vec![Invitee {
                key_package: key_package_of(&relay, &b.account_hex()),
                inbox_relays: vec![RELAY.to_owned()],
            }],
            own_devices: Vec::new(),
            admins: Vec::new(),
            components: Vec::new(),
        }))
        .await;
    let OpResult::GroupCreated(created) = ok_result(response) else {
        panic!("create_group")
    };
    let group = created.group.id.clone();
    a.publish(&mut relay).await;
    b.call(Request::Ingest(IngestRequest {
        events: welcomes_for(&relay, &b.account_hex()),
    }))
    .await;
    b.sync(&mut relay, &group).await;
    a.sync(&mut relay, &group).await;
    let route = a.route_hex(&group).await;

    // The target: m1 is on the relay but B never ingests it yet.
    let mut backlog = group_events(&relay, &route);
    a.advance(1_000);
    let response = a
        .call(Request::SendIntent(SendIntentRequest {
            group: group.clone(),
            intent: MessageIntent::Chat {
                content: "m1".to_owned(),
                reply_to: None,
            },
        }))
        .await;
    let OpResult::MessageSent(sent) = ok_result(response) else {
        panic!("send m1")
    };
    let m1 = sent.message.id.clone();
    a.publish(&mut relay).await;
    let all = group_events(&relay, &route);
    let target_events = all[backlog.len()..].to_vec();
    backlog = all;
    assert_eq!(target_events.len(), 1, "one transport event for m1");

    // A's own edit of m1 (kind 1009): authorized for B once m1 is indexed.
    a.advance(1_000);
    let response = a
        .call(Request::SendIntent(SendIntentRequest {
            group: group.clone(),
            intent: MessageIntent::Edit {
                target: hex_to_32(&m1),
                content: "m1*".to_owned(),
            },
        }))
        .await;
    let OpResult::MessageSent(edit_sent) = ok_result(response) else {
        panic!("send edit")
    };
    a.publish(&mut relay).await;
    let all = group_events(&relay, &route);
    let edit_events = all[backlog.len()..].to_vec();
    let _ = backlog;
    assert_eq!(edit_events.len(), 1, "one transport event for the edit");

    // Ingesting the edit first surfaces nothing — the modifier is held.
    let events = b.ingest_and_drain(edit_events).await;
    assert!(
        events
            .iter()
            .all(|event| !matches!(event, SessionEvent::MessageReceived { .. })),
        "held edit surfaces no message: {events:?}"
    );

    // The target's arrival indexes it and releases the held edit as
    // `Semantics::Edit` carrying the inner target id.
    let events = b.ingest_and_drain(target_events).await;
    let mut saw_target = false;
    let mut edit = None;
    for event in &events {
        if let SessionEvent::MessageReceived { message, .. } = event {
            if message.id == m1 {
                saw_target = true;
            }
            if let Semantics::Edit { .. } = message.semantics {
                edit = Some(message.semantics.clone());
            }
        }
    }
    assert!(saw_target, "m1 delivered: {events:?}");
    assert_eq!(
        edit,
        Some(Semantics::Edit {
            target: hex_to_32(&m1),
            content: "m1*".to_owned(),
        }),
        "held edit released with edit semantics: {events:?}"
    );
    let _ = edit_sent;
}

/// Protocol v4 send pacing: 50 sends per `created_at` second per group
/// with 30 s of forward skew — 1,550 sends on a frozen clock stage, the
/// 1,551st answers `send_rate_exceeded` and stages no outbound / commits
/// no batch.
#[tokio::test(flavor = "current_thread")]
async fn send_pacing_rate_exceeded() {
    let mut relay = FakeRelay::new();
    let mut a = Member::new();
    a.open().await;
    a.publish_identity(&mut relay).await;
    let response = a
        .call(Request::CreateGroup(CreateGroupRequest {
            name: "paced".to_owned(),
            description: String::new(),
            relays: vec![RELAY.to_owned()],
            invitees: Vec::new(),
            own_devices: Vec::new(),
            admins: Vec::new(),
            components: Vec::new(),
        }))
        .await;
    let OpResult::GroupCreated(created) = ok_result(response) else {
        panic!("create_group")
    };
    let group = created.group.id.clone();
    a.publish(&mut relay).await;

    let message = |text: String| {
        Request::SendMessage(SendMessageRequest {
            group: group.clone(),
            message: AppMessage {
                kind: 9,
                tags: Vec::new(),
                content: text,
            },
        })
    };
    // Frozen clock: every send lands on one `created_at` second and the
    // allocator spills at the 50-per-second cap up to the 30 s skew bound.
    for i in 0..1_550usize {
        let response = a.call(message(format!("burst#{i}"))).await;
        assert!(matches!(response, Response::Ok { .. }), "send #{i}");
    }

    // Drain the staged outbound so the refused send's staging would be
    // visible; committed() is the KV-batch counter.
    let Response::Ok { result: _ } = a.call(Request::TakeOutbound(NoArgs {})).await else {
        panic!("take_outbound")
    };
    let committed = a.host.committed();
    let response = a.call(message("one too many".to_owned())).await;
    assert_eq!(error_kind(&response), "send_rate_exceeded");
    assert_eq!(a.host.committed(), committed, "refused send committed");

    let Response::Ok { result } = a.call(Request::TakeOutbound(NoArgs {})).await else {
        panic!("take_outbound")
    };
    let OpResult::Outbound(OutboundResult { items }) = *result else {
        panic!("take_outbound result")
    };
    assert!(items.is_empty(), "refused send staged nothing: {items:?}");
}

/// `Debug` on a live bridge surfaces the lifecycle label and counters only —
/// never attachment metadata parked in the pending table.
#[tokio::test(flavor = "current_thread")]
async fn bridge_debug_redacts_pending_data() {
    let mut a = Member::new();
    a.open().await;
    let response = a
        .call(Request::CreateGroup(CreateGroupRequest {
            name: "g".to_owned(),
            description: "d".to_owned(),
            relays: vec![RELAY.to_owned()],
            invitees: Vec::new(),
            own_devices: Vec::new(),
            admins: Vec::new(),
            components: vec![media_component()],
        }))
        .await;
    let OpResult::GroupCreated(created) = ok_result(response) else {
        panic!("create_group");
    };
    let sealed = a
        .call(Request::SealAttachment(SealAttachmentRequest {
            group: created.group.id,
            attachment: AttachmentInput {
                plaintext: vec![0x70, 0x69, 0x63],
                media_type: "image/png".to_owned(),
                filename: "debug-leak-probe.png".to_owned(),
                dim: None,
                thumbhash: None,
            },
        }))
        .await;
    assert!(matches!(sealed, Response::Ok { .. }), "seal: {sealed:?}");
    let debug = format!("{:?}", a.bridge);
    assert!(!debug.contains("debug-leak-probe"), "{debug}");
}

/// `select_key_packages` end to end: the host fetches the invitee's
/// kind-30443 publications and kind-5 deletion requests, the session
/// selects — under creation requirements with `group: null` and under a
/// real group's invite requirements.
#[tokio::test]
async fn select_key_packages_over_the_wire() {
    let mut relay = FakeRelay::new();
    let mut a = Member::new();
    a.open().await;
    let mut b = Member::new();
    b.open().await;
    b.publish_identity(&mut relay).await;

    let account = hex_to_32(&b.account_hex());
    let fetched = relay.fetch(|event| event.kind == 30_443 && event.pubkey == account);
    assert_eq!(fetched.len(), 1, "one publication on the relay");

    let response = a
        .call(Request::SelectKeyPackages(SelectKeyPackagesRequest {
            account,
            events: fetched.clone(),
            group: None,
        }))
        .await;
    let OpResult::KeyPackages(selected) = ok_result(response) else {
        panic!("key_packages")
    };
    assert_eq!(selected.events, fetched);

    // A group's invite requirements accept the same publication.
    let response = a
        .call(Request::CreateGroup(CreateGroupRequest {
            name: "sel".to_owned(),
            description: String::new(),
            relays: vec![RELAY.to_owned()],
            invitees: Vec::new(),
            own_devices: Vec::new(),
            admins: Vec::new(),
            components: Vec::new(),
        }))
        .await;
    let OpResult::GroupCreated(created) = ok_result(response) else {
        panic!("create_group")
    };
    let response = a
        .call(Request::SelectKeyPackages(SelectKeyPackagesRequest {
            account,
            events: fetched.clone(),
            group: Some(created.group.id.clone()),
        }))
        .await;
    let OpResult::KeyPackages(selected) = ok_result(response) else {
        panic!("key_packages")
    };
    assert_eq!(selected.events, fetched);

    // A kind-5 deletion by B covering the slot empties the selection.
    let slot = fetched[0]
        .tags
        .iter()
        .find(|tag| tag.first().map(String::as_str) == Some("d"))
        .and_then(|tag| tag.get(1))
        .expect("d tag")
        .clone();
    let deletion: Event = b
        .signer
        .sign_event(
            UnsignedEvent {
                pubkey: account,
                created_at: 1_700_000_000,
                kind: 5,
                tags: vec![vec![
                    "a".to_owned(),
                    format!("30443:{}:{slot}", b.account_hex()),
                ]],
                content: String::new(),
            }
            .into(),
        )
        .await
        .expect("sign deletion")
        .into();
    let mut events = fetched;
    events.push(deletion);
    let response = a
        .call(Request::SelectKeyPackages(SelectKeyPackagesRequest {
            account,
            events,
            group: None,
        }))
        .await;
    let OpResult::KeyPackages(selected) = ok_result(response) else {
        panic!("key_packages")
    };
    assert!(selected.events.is_empty(), "covered slot selects nothing");
}

/// `sign_out` returns the group lists, queues exactly one kind-5 outbound
/// and leaves the session answering `signed_out` to a later ensure.
#[tokio::test(flavor = "current_thread")]
async fn sign_out_withdraws_the_slot() {
    let mut relay = FakeRelay::new();
    let mut a = Member::new();
    a.open().await;
    a.publish_identity(&mut relay).await;

    let response = a
        .call(Request::SignOut(SignOutRequest {
            write_relays: vec![RELAY.to_owned()],
        }))
        .await;
    let OpResult::SignOut(sign_out) = ok_result(response) else {
        panic!("sign_out")
    };
    assert!(sign_out.left.is_empty() && sign_out.kept_admin_leaves.is_empty());
    a.ack_all().await;
    let Response::Ok { result } = a.call(Request::TakeOutbound(NoArgs {})).await else {
        panic!("take_outbound")
    };
    let OpResult::Outbound(OutboundResult { items }) = *result else {
        panic!("outbound")
    };
    assert_eq!(items.len(), 1, "only the kind-5 is queued: {items:?}");
    let deletion = &items[0];
    assert_eq!(deletion.purpose, OutboundPurpose::KeyPackageDeletion);
    assert_eq!(deletion.event.kind, 5);
    let a_tags = deletion
        .event
        .tags
        .iter()
        .filter(|tag| tag.first().map(String::as_str) == Some("a"))
        .count();
    assert_eq!(a_tags, 1, "one a tag names the slot");
    let k_tags: Vec<_> = deletion
        .event
        .tags
        .iter()
        .filter(|tag| tag.first().map(String::as_str) == Some("k"))
        .collect();
    assert_eq!(k_tags.len(), 1, "one k tag");
    assert_eq!(k_tags[0].get(1).map(String::as_str), Some("30443"));

    // A withdrawn slot refuses new publications.
    let response = a
        .call(Request::EnsureKeyPackage(EnsureKeyPackageRequest {
            write_relays: vec![RELAY.to_owned()],
        }))
        .await;
    assert_eq!(error_kind(&response), "signed_out");

    // A repeat sign-out stages no second kind-5.
    let response = a
        .call(Request::SignOut(SignOutRequest {
            write_relays: vec![RELAY.to_owned()],
        }))
        .await;
    let OpResult::SignOut(again) = ok_result(response) else {
        panic!("idempotent sign_out")
    };
    assert_eq!(again.left, Vec::<Vec<u8>>::new());
    let Response::Ok { result } = a.call(Request::TakeOutbound(NoArgs {})).await else {
        panic!("take_outbound")
    };
    let OpResult::Outbound(OutboundResult { items }) = *result else {
        panic!("outbound")
    };
    assert!(items.is_empty(), "nothing new is staged: {items:?}");
}

/// D2 stall over the wire: three deferred group events suspect once,
/// `history_fetched` flags the copy stalled and the snapshot reports
/// `access: "stalled"`.
#[tokio::test(flavor = "current_thread")]
async fn group_stall_over_the_wire() {
    let mut relay = FakeRelay::new();
    let mut a = Member::new();
    let mut b = Member::new();
    a.open().await;
    b.open().await;
    a.publish_identity(&mut relay).await;
    b.publish_identity(&mut relay).await;

    let response = b
        .call(Request::CreateGroup(CreateGroupRequest {
            name: "stall".to_owned(),
            description: String::new(),
            relays: vec![RELAY.to_owned()],
            invitees: vec![Invitee {
                key_package: key_package_of(&relay, &a.account_hex()),
                inbox_relays: vec![RELAY.to_owned()],
            }],
            own_devices: Vec::new(),
            admins: Vec::new(),
            components: Vec::new(),
        }))
        .await;
    let OpResult::GroupCreated(created) = ok_result(response) else {
        panic!("create_group")
    };
    let group = created.group.id.clone();
    b.publish(&mut relay).await;
    a.call(Request::Ingest(IngestRequest {
        events: welcomes_for(&relay, &a.account_hex()),
    }))
    .await;
    a.sync(&mut relay, &group).await;
    b.sync(&mut relay, &group).await;
    assert_eq!(
        group_snapshot(&mut a, &group).await["access"],
        "active",
        "a healthy copy is active"
    );

    // B self-updates (the commit is withheld from A) and sends three
    // chats at the new epoch. The traffic must postdate the copy's
    // post-Welcome skew window to count as stall evidence.
    b.advance(400_000);
    let route = b.route_hex(&group).await;
    let seen: BTreeSet<[u8; 32]> = group_events(&relay, &route)
        .into_iter()
        .map(|event| event.id)
        .collect();
    b.call(Request::SelfUpdate(SelfUpdateRequest {
        group: group.clone(),
    }))
    .await;
    b.publish(&mut relay).await;
    let c1 = group_events(&relay, &route)
        .into_iter()
        .find(|event| event.kind == 445 && !seen.contains(&event.id))
        .expect("the self-update commit");
    for i in 0..3 {
        b.send_chat(&mut relay, &group, &format!("m{i}")).await;
    }
    let deferred: Vec<Event> = group_events(&relay, &route)
        .into_iter()
        .filter(|event| event.id != c1.id && !seen.contains(&event.id))
        .collect();

    let events = a.ingest_and_drain(deferred).await;
    let suspected = events
        .iter()
        .filter(|event| matches!(event, SessionEvent::GroupStallSuspected { .. }))
        .count();
    assert_eq!(suspected, 1, "three deferred events suspect once");

    // The completed refetch leaves the counter full at the same epoch:
    // `group_stalled` fires and the snapshot reports it.
    a.call(Request::HistoryFetched(HistoryFetchedRequest {
        group: group.clone(),
    }))
    .await;
    let events = a.drained_events();
    let stalled = events
        .iter()
        .filter(|event| matches!(event, SessionEvent::GroupStalled { .. }))
        .count();
    assert_eq!(stalled, 1, "one stalled event");
    assert_eq!(
        group_snapshot(&mut a, &group).await["access"],
        "stalled",
        "the wire snapshot carries the flag"
    );
}
