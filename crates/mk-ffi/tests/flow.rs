//! The full byte flow: two handles over `mk_bridge_*` only — create,
//! invite, join, 500 messages, close both, reopen from committed rows,
//! add and remove a third member, publish failure and retry.

#![allow(unsafe_code, reason = "the tests exercise the C entry points")]
#![allow(
    unused_crate_dependencies,
    clippy::borrow_as_ptr,
    clippy::cast_lossless,
    clippy::cast_possible_truncation,
    clippy::clone_on_ref_ptr,
    clippy::disallowed_methods,
    clippy::doc_markdown,
    clippy::excessive_nesting,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::manual_dangling_ptr,
    clippy::manual_let_else,
    clippy::map_unwrap_or,
    clippy::match_wildcard_for_single_variants,
    clippy::missing_const_for_fn,
    clippy::missing_docs_in_private_items,
    clippy::missing_fields_in_debug,
    clippy::missing_panics_doc,
    clippy::multiple_unsafe_ops_per_block,
    clippy::needless_pass_by_value,
    clippy::option_if_let_else,
    clippy::panic,
    clippy::shadow_reuse,
    clippy::shadow_unrelated,
    clippy::significant_drop_tightening,
    clippy::tests_outside_test_module,
    clippy::too_many_lines,
    clippy::undocumented_unsafe_blocks,
    clippy::unnecessary_wraps,
    clippy::unwrap_in_result,
    clippy::unwrap_used,
    unreachable_pub,
    dead_code,
    reason = "test code"
)]

mod support;

use mk_bridge::protocol::*;
use support::*;

/// End-to-end: every step goes through the C ABI byte envelopes.
#[test]
#[cfg_attr(miri, ignore = "miri cannot run secp256k1-sys C code (session open)")]
fn two_member_flow_reopen_rotate_and_retry() {
    let mut relay = FakeRelay::new();
    let mut alice = Member::new();
    alice.open();
    alice.publish_identity(&mut relay);
    let mut bob = Member::new();
    bob.open();
    bob.publish_identity(&mut relay);

    let group = create_and_join(&mut relay, &mut alice, &mut bob);

    // 500 messages alternating senders; sync every 50 so the receivers
    // ingest and acknowledge.
    for index in 0..500 {
        if index % 2 == 0 {
            alice.send_chat(&mut relay, &group, &format!("a-{index}"));
        } else {
            bob.send_chat(&mut relay, &group, &format!("b-{index}"));
        }
        if index % 50 == 49 {
            alice.sync(&mut relay, &group);
            bob.sync(&mut relay, &group);
        }
    }
    alice.sync(&mut relay, &group);
    bob.sync(&mut relay, &group);

    // Close both handles and reopen each from its committed rows.
    alice.close();
    bob.close();
    alice.restart();
    bob.restart();
    assert!(
        member_groups(&mut alice).contains(&group),
        "alice lost the group across reopen"
    );
    assert!(
        member_groups(&mut bob).contains(&group),
        "bob lost the group across reopen"
    );
    alice.sync(&mut relay, &group);
    bob.sync(&mut relay, &group);

    // Invite a third member, then remove her.
    let mut carol = Member::new();
    carol.open();
    carol.publish_identity(&mut relay);
    let key_package = relay.key_package_of(&carol.account_hex());
    let response = alice.call(Request::Invite(InviteRequest {
        group: group.clone(),
        invitees: vec![Invitee {
            key_package,
            inbox_relays: vec![RELAY.to_owned()],
        }],
    }));
    assert!(
        matches!(response, Response::Ok { .. }),
        "invite: {response:?}"
    );
    alice.publish(&mut relay);
    carol.ingest_and_drain(relay.welcomes_for(&carol.account_hex()));
    carol.sync(&mut relay, &group);
    alice.sync(&mut relay, &group);
    bob.sync(&mut relay, &group);
    assert!(
        member_groups(&mut carol).contains(&group),
        "carol never joined"
    );
    carol.send_chat(&mut relay, &group, "carol-hello");
    bob.sync(&mut relay, &group);

    let response = alice.call(Request::RemoveMembers(RemoveMembersRequest {
        group: group.clone(),
        members: vec![carol.signer.account().0],
    }));
    assert!(
        matches!(response, Response::Ok { .. }),
        "remove_members: {response:?}"
    );
    alice.publish(&mut relay);
    bob.sync(&mut relay, &group);
    carol.sync(&mut relay, &group);

    // Publish failure, then retry: the relay is unreachable once, the
    // report resolves `Pending` (retryable targets stay outstanding),
    // the next take_outbound re-hands the artifact and bob receives it.
    relay.unavailable = true;
    let response = alice.call(Request::SendMessage(SendMessageRequest {
        group: group.clone(),
        message: AppMessage {
            kind: 9,
            tags: Vec::new(),
            content: "retry-me".to_owned(),
        },
    }));
    assert!(
        matches!(response, Response::Ok { .. }),
        "send: {response:?}"
    );
    let attempted = alice.publish(&mut relay);
    assert!(
        attempted
            .iter()
            .all(|(_, resolution)| *resolution == PublishResolution::Pending),
        "unreachable relays must stay pending: {attempted:?}"
    );
    relay.unavailable = false;
    let retried = alice.publish(&mut relay);
    assert!(
        retried
            .iter()
            .any(|(_, resolution)| !matches!(resolution, PublishResolution::Pending)),
        "the retry republished nothing: {retried:?}"
    );
    let bob_route = bob.route_hex(&group);
    let events = bob.ingest_and_drain(relay.group_events(&bob_route));
    assert!(
        events.iter().any(|event| matches!(
            event,
            SessionEvent::MessageReceived { message, .. }
                if message.content == "retry-me"
        )),
        "bob never received the retried message"
    );

    alice.close();
    bob.close();
    carol.close();
    let releases = [alice.free(), bob.free(), carol.free()];
    for release in &releases {
        wait_for_release(release);
    }
}

/// A creates a group inviting B; B joins and both settle.
fn create_and_join(relay: &mut FakeRelay, alice: &mut Member, bob: &mut Member) -> Vec<u8> {
    let key_package = relay.key_package_of(&bob.account_hex());
    let response = alice.call(Request::CreateGroup(CreateGroupRequest {
        name: "flow".to_owned(),
        description: String::new(),
        relays: vec![RELAY.to_owned()],
        invitees: vec![Invitee {
            key_package,
            inbox_relays: vec![RELAY.to_owned()],
        }],
        admins: vec![],
        components: vec![],
    }));
    let Response::Ok { result } = response else {
        panic!("create_group: {response:?}");
    };
    let OpResult::GroupCreated(created) = *result else {
        panic!("create_group result: {result:?}");
    };
    let group = created.group.id.clone();
    alice.publish(relay);
    bob.ingest_and_drain(relay.welcomes_for(&bob.account_hex()));
    bob.sync(relay, &group);
    alice.sync(relay, &group);
    group
}

/// The `groups` listing of one member.
fn member_groups(member: &mut Member) -> Vec<Vec<u8>> {
    let response = member.call(Request::Groups(NoArgs {}));
    let Response::Ok { result } = response else {
        panic!("groups: {response:?}");
    };
    let OpResult::Groups(GroupsResult { groups }) = *result else {
        panic!("groups result: {result:?}");
    };
    groups.into_iter().map(|group| group.id).collect()
}
