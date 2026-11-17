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

use hex as _;
use mk_bridge::protocol::*;
use mk_bridge::{Bridge, MAX_REQUEST_BYTES};
use mk_core::{self as _, AccountSigner};
use serde as _;
use serde_json::{Value, json};
use support::*;
use thiserror as _;
use tokio as _;

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
        "publish_key_package",
        &a.call(Request::PublishKeyPackage(PublishKeyPackageRequest {
            relays: vec![RELAY.to_owned()],
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
        &a.call(Request::AwaitingDurability).await
    );
    mark!("take_outbound", &a.call(Request::TakeOutbound).await);
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
            members: vec![c.account_hex()],
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
        let response = b.call(Request::Tick).await;
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

    mark!("tick", &a.call(Request::Tick).await);
    mark!(
        "acknowledge_messages",
        &a.call(Request::AcknowledgeMessages(AcknowledgeMessagesRequest {
            ids: vec![hex::encode([0x11u8; 32])],
        }))
        .await,
    );
    mark!("groups", &a.call(Request::Groups).await);
    mark!(
        "group",
        &a.call(Request::Group(GroupRequest {
            group: group.clone(),
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
    // `signer_reply` while Ready is a typed protocol error.
    mark!(
        "signer_reply",
        &a.call(Request::SignerReply(SignerReplyRequest {
            replies: Vec::new(),
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
    let mut received = BTreeSet::new();
    for _ in 0..8 {
        let backlog = group_events(&relay, &route);
        let events = b.ingest_and_drain(backlog).await;
        received.extend(events.iter().filter_map(|event| match event {
            SessionEvent::MessageReceived { message, .. } => Some(message.content.clone()),
            _ => None,
        }));
        let response = b.call(Request::Tick).await;
        assert!(matches!(response, Response::Ok { .. }), "tick");
        b.advance(2_000);
        b.publish(&mut relay).await;
    }
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
            members: vec![c.account_hex()],
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
    let response = a.call(Request::Tick).await;
    assert!(matches!(response, Response::Ok { .. }), "tick");
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
    let Response::Ok { result } = a.call(Request::TakeOutbound).await else {
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
    let Response::Ok { result } = a.call(Request::TakeOutbound).await else {
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
    let response = a.call_once(Request::PublishKeyPackage(PublishKeyPackageRequest {
        relays: vec![RELAY.to_owned()],
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
    let Response::Ok { result } = a.call(Request::TakeOutbound).await else {
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
    let response = a.call(Request::AwaitingDurability).await;
    let Response::Ok { result } = response else {
        panic!()
    };
    let OpResult::AwaitingDurability(AwaitingDurabilityResult { seq: Some(seq) }) = *result else {
        panic!("key package batch still gated: {result:?}")
    };
    let response = a.call(Request::AckDurable(AckDurableRequest { seq })).await;
    assert!(matches!(response, Response::Ok { .. }));
    let Response::Ok { result } = a.call(Request::TakeOutbound).await else {
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
    let config = json!({"protocol_version": 2, "account": 7, "now": "nope"});
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
    let response = member.call_once(Request::Groups);
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

    // Busy: suspend on a signer op, then any other request answers busy.
    let response = busy.call_once(Request::PublishKeyPackage(PublishKeyPackageRequest {
        relays: vec![RELAY.to_owned()],
    }));
    assert!(
        matches!(response, Response::SignerPending { .. }),
        "publish_key_package suspends"
    );
    let response = busy.call_once(Request::Groups);
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
    let response = busy.call_once(Request::Groups);
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
        admins: Vec::new(),
        components: Vec::new(),
    }));
    assert!(
        matches!(response, Response::Error { .. }),
        "commit failure produces an error: {response:?}"
    );
    let response = broken.call_once(Request::Groups);
    assert_eq!(error_kind(&response), "poisoned");
    let opened = broken.open_bytes(&fresh_config);
    assert_eq!(open_error_kind(&opened), "already_open");
}

/// Crash consistency: unacked batches never release outbound; torn writes
/// reopen consistently; a mid-ingest commit failure converges after
/// re-ingest.
#[tokio::test(flavor = "current_thread")]
async fn crash_consistency() {
    let mut relay = FakeRelay::new();
    let mut a = Member::new();
    a.open().await;
    a.publish_identity(&mut relay).await;
    let response = a
        .call(Request::CreateGroup(CreateGroupRequest {
            name: "crash".to_owned(),
            description: String::new(),
            relays: vec![RELAY.to_owned()],
            invitees: Vec::new(),
            admins: Vec::new(),
            components: Vec::new(),
        }))
        .await;
    let OpResult::GroupCreated(created) = ok_result(response) else {
        panic!()
    };
    let group = created.group.id.clone();
    a.publish(&mut relay).await;
    let published_before = relay.publishes.len();
    let acked_commits = a.host.committed();

    // Stage a send, then "crash" before acking: drop the bridge mid-flight.
    a.advance(1_000);
    let response = a
        .call(Request::SendMessage(SendMessageRequest {
            group: group.clone(),
            message: AppMessage {
                kind: 9,
                tags: Vec::new(),
                content: "unacked".to_owned(),
            },
        }))
        .await;
    assert!(matches!(response, Response::Ok { .. }));
    a.bridge = Bridge::new(a.host.clone());
    let opened = a.open().await;
    assert!(matches!(opened, OpenResponse::Ready { .. }));
    // Its batch was committed before the crash, so the host persisted it:
    // a reopen loads it from durable rows and the gate releases it (no
    // in-flight gate record survives a restart).
    let Response::Ok { result } = a.call(Request::TakeOutbound).await else {
        panic!()
    };
    let OpResult::Outbound(OutboundResult { items }) = *result else {
        panic!()
    };
    assert_eq!(
        items.len(),
        1,
        "committed send is releasable after reopen: {items:?}"
    );

    // Reopen from acked-only rows: the unacked write never existed.
    a.bridge = Bridge::new(a.host.clone());
    a.host.load_prefix(Some(acked_commits));
    let opened = a.open().await;
    a.host.load_prefix(None);
    assert!(matches!(opened, OpenResponse::Ready { .. }));
    let Response::Ok { result } = a.call(Request::TakeOutbound).await else {
        panic!()
    };
    let OpResult::Outbound(OutboundResult { items }) = *result else {
        panic!()
    };
    assert!(
        items.is_empty(),
        "torn write: nothing published for lost batches"
    );
    assert_eq!(
        relay.publishes.len(),
        published_before,
        "nothing published before its gating batch was acked"
    );

    // Ack before publish, then crash: the item is released again and the
    // publish completes once accepted.
    let mut b = Member::new();
    b.open().await;
    b.publish_identity(&mut relay).await;
    let response = b
        .call(Request::PublishKeyPackage(PublishKeyPackageRequest {
            relays: vec![RELAY.to_owned()],
        }))
        .await;
    assert!(matches!(response, Response::Ok { .. }));
    b.ack_all().await;
    b.bridge = Bridge::new(b.host.clone());
    let opened = b.open().await;
    assert!(matches!(opened, OpenResponse::Ready { .. }));
    let resolutions = b.publish(&mut relay).await;
    assert!(
        resolutions
            .iter()
            .any(|(_, resolution)| !matches!(resolution, PublishResolution::Pending)),
        "re-released outbound resolves once accepted"
    );

    // Mid-ingest commit failure: poison, reopen, re-ingest, converge.
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

    clean.ingest_and_drain(backlog.clone()).await;
    clean.sync(&mut relay, &group).await;

    crashed.host.fail_after(crashed.host.committed() + 1);
    let response = crashed
        .call(Request::Ingest(IngestRequest {
            events: backlog.clone(),
        }))
        .await;
    assert!(
        matches!(response, Response::Error { .. }),
        "mid-ingest commit failure surfaces"
    );
    crashed.host.fail_after(usize::MAX);
    crashed.bridge = Bridge::new(crashed.host.clone());
    let opened = crashed.open().await;
    assert!(matches!(opened, OpenResponse::Ready { .. }));
    crashed.ingest_and_drain(backlog).await;
    crashed.sync(&mut relay, &group).await;

    let clean_snapshot = group_snapshot(&mut clean, &group).await;
    let crashed_snapshot = group_snapshot(&mut crashed, &group).await;
    assert_eq!(clean_snapshot, crashed_snapshot, "re-ingest converges");
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
