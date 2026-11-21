//! Multi-device scenarios (D1-D4). A "device" is its own
//! `Engine<SqliteAccountStorage>`; devices of one account share the secp256k1
//! identity and differ in their MLS signature keys.

mod support;

use cgka_engine::ManualConvergenceClock;
use cgka_traits::app_components::ACCOUNT_DEVICES_COMPONENT_ID;
use cgka_traits::engine::{CgkaEngine, GroupEvent, SendIntent, SendResult};
use cgka_traits::ingest::IngestOutcome;
use cgka_traits::types::MemberId;
use support::*;

fn hexid(id: &MemberId) -> String {
    hex::encode(&id.as_slice()[..4])
}

/// D1: B invites both devices of account A in the founding Add.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn d1_inviter_adds_every_device_slot() {
    let mut a1 = device(b"alice", true);
    let mut a2 = device(b"alice", true);
    let mut bob = device(b"bob", true);

    let a1_kp = a1.engine.fresh_key_package().await.unwrap();
    let a2_kp = a2.engine.fresh_key_package().await.unwrap();
    let gid = found_group(&mut bob, vec![a1_kp, a2_kp], true, &mut [&mut a1, &mut a2]).await;

    let required = required(&bob.engine, &gid);
    println!("D1 required app components: {required:?}");
    assert!(required.contains(ACCOUNT_DEVICES_COMPONENT_ID));

    let members = bob.engine.members(&gid).unwrap();
    let mut accounts: Vec<Vec<u8>> = members.iter().map(|m| m.id.as_slice().to_vec()).collect();
    accounts.sort();
    accounts.dedup();
    println!(
        "D1 members: {} leaves over {} accounts",
        members.len(),
        accounts.len()
    );
    for m in &members {
        println!(
            "    leaf account={} credential={}",
            hexid(&m.id),
            hex::encode(&m.credential[..4])
        );
    }
    assert_eq!(members.len(), 3);
    assert_eq!(accounts.len(), 2);

    // Each sends one message; the other two receive it. The receiver-visible
    // sender is the account id; the leaf index distinguishes the device.
    async fn round_trip(
        name: &str,
        _gid: &cgka_traits::types::GroupId,
        expected: MemberId,
        msg: &cgka_traits::transport::TransportMessage,
        receivers: &mut [&mut Device],
    ) {
        for receiver in receivers.iter_mut() {
            let outcome = deliver(receiver, msg).await;
            println!("    {name} -> receiver ingest: {outcome:?}");
            assert!(matches!(outcome, IngestOutcome::Processed));
            let events = receiver.engine.drain_events();
            print_events("recv", &events);
            let received: Vec<_> = events
                .iter()
                .filter_map(|e| match e {
                    GroupEvent::MessageReceived {
                        sender,
                        sender_leaf_index,
                        ..
                    } => Some((sender.clone(), *sender_leaf_index)),
                    _ => None,
                })
                .collect();
            assert_eq!(received.len(), 1);
            println!(
                "    receiver sees sender={} leaf_index={:?} (expected account={})",
                hexid(&received[0].0),
                received[0].1,
                hexid(&expected)
            );
            assert_eq!(received[0].0, expected);
        }
    }

    let b_id = bob.engine.self_id();
    let a_id = a1.engine.self_id();

    let msg = send_app(&mut bob, &gid, "hello from B").await;
    round_trip("B", &gid, b_id, &msg, &mut [&mut a1, &mut a2]).await;

    let msg = send_app(&mut a1, &gid, "hello from A1").await;
    round_trip("A1", &gid, a_id.clone(), &msg, &mut [&mut bob, &mut a2]).await;

    // A sibling's message is a normal MessageReceived whose sender is the
    // shared account id, only the leaf index names the device.
    let msg = send_app(&mut a2, &gid, "hello from A2").await;
    round_trip("A2", &gid, a_id, &msg, &mut [&mut bob, &mut a1]).await;
}

/// D2: A1 founds a group whose founding Add covers B plus A2 — A2's Welcome
/// author is A1, a device of A2's own account (the founding exemption).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn d2_founding_exemption_own_device() {
    let mut a1 = device(b"alice", true);
    let mut a2 = device(b"alice", true);
    let mut bob = device(b"bob", true);

    let bob_kp = bob.engine.fresh_key_package().await.unwrap();
    let a2_kp = a2.engine.fresh_key_package().await.unwrap();
    let (gid, welcomes) = create(&mut a1, vec![bob_kp, a2_kp], vec![]).await.unwrap();
    assert_eq!(welcomes.len(), 2);
    assert!(requires_account_devices(&a1.engine, &gid));

    assert_joins(&mut bob, &welcomes[0], &gid, &a1).await;
    assert_joins(&mut a2, &welcomes[1], &gid, &a1).await;
    println!("D2 A2 joined via Welcome authored by its own account's device (A1)");
}

/// D3: a later device of account A joins a live {A1,B} chat three ways.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn d3_later_device_join_paths() {
    // (a)-(c): admin invite path.
    let mut bob = device(b"bob", true);
    let mut a1 = device(b"alice", true);
    let a1_kp = a1.engine.fresh_key_package().await.unwrap();
    let gid = found_group(&mut bob, vec![a1_kp], true, &mut [&mut a1]).await;
    assert!(requires_account_devices(&bob.engine, &gid));
    assert!(is_admin(&bob, &gid));
    assert!(!is_admin(&a1, &gid));
    println!(
        "D3 group requires 0x800d; B admin={} A1 admin={}",
        is_admin(&bob, &gid),
        is_admin(&a1, &gid)
    );

    // Three messages at epochs >= 1 before A3 exists.
    let mut early = Vec::new();
    early.push(send_app(&mut bob, &gid, "b1").await);
    early.push(send_app(&mut a1, &gid, "a1-1").await);
    early.push(send_app(&mut bob, &gid, "b2").await);
    for msg in &early {
        deliver(&mut a1, msg).await;
    }
    deliver(&mut bob, &early[1]).await;
    a1.engine.drain_events();
    bob.engine.drain_events();

    let mut a3 = device(b"alice", true);
    let a3_kp = a3.engine.fresh_key_package().await.unwrap();

    // (a) A1 (non-admin) Invite of A3's ordinary KeyPackage.
    let refused = send_intent(
        &mut a1,
        SendIntent::Invite {
            group_id: gid.clone(),
            key_packages: vec![a3_kp.clone()],
            initial_admins: vec![],
        },
    )
    .await;
    println!("D3(a) A1 Invite of A3 KP -> {refused:?}");
    assert!(refused.is_err());

    // (b) A1 AddOwnAccountDevices naming A3's KP with no device group behind it.
    let refused = send_intent(
        &mut a1,
        SendIntent::AddOwnAccountDevices {
            group_id: gid.clone(),
            siblings: vec![cgka_traits::engine::SiblingKeyPackage {
                slot: slot(3),
                key_package: a3_kp.clone(),
            }],
            forwarded: Default::default(),
        },
    )
    .await;
    println!("D3(b) A1 AddOwnAccountDevices without device group -> {refused:?}");
    assert!(refused.is_err());

    // (c) B (admin) invites A3's KP; A3 joins; the pre-join messages are stale.
    let sent = send_intent(
        &mut bob,
        SendIntent::Invite {
            group_id: gid.clone(),
            key_packages: vec![a3_kp],
            initial_admins: vec![],
        },
    )
    .await
    .unwrap();
    let SendResult::GroupEvolution {
        msg,
        welcomes,
        pending,
    } = sent
    else {
        panic!("expected GroupEvolution")
    };
    bob.engine.confirm_published(pending).await.unwrap();
    let commit = route_to_group(&msg, &gid);
    assert_accepted(&mut a1, &commit, &gid).await;
    assert_eq!(welcomes.len(), 1);
    assert_joins(&mut a3, &welcomes[0], &gid, &bob).await;

    for (i, msg) in early.iter().enumerate() {
        let outcome = deliver(&mut a3, msg).await;
        println!("D3(c) pre-join message {i} on A3 -> {outcome:?}");
    }
    let new_msg = send_app(&mut a1, &gid, "post-join").await;
    let outcome = deliver(&mut a3, &new_msg).await;
    println!("D3(c) post-join message on A3 -> {outcome:?}");
    assert!(matches!(outcome, IngestOutcome::Processed));

    // (d) STRETCH: the real device-group path — device group + link, private
    // KeyPackages, same-account Add + sibling Welcome (test-policy build).
    println!("D3(d) device-group path:");
    let (mut a1_tagged, mut rest, device_gid) = tagged_devices(b"alice", 1).await;
    let mut a3_tagged = rest.remove(0);
    println!(
        "    device group {device_gid:?} roster: {:?}",
        roster_of(&a1_tagged, &device_gid)
            .devices
            .keys()
            .collect::<Vec<_>>()
    );

    // A chat A1 is in with B, requiring 0x800d and routed (sibling-Welcome
    // delivery needs a routing component).
    let mut b_chat = chat_device(b"bob");
    let chat = found_routed_group(&mut b_chat, &mut [&mut a1_tagged]).await;
    assert!(requires_account_devices(&b_chat.engine, &chat));

    // A3 hands A1 a private KeyPackage; A1 adds A3 to the chat.
    let sibling = handed(&mut a3_tagged, slot(2), slot(1));
    let sent = send_intent(
        &mut a1_tagged,
        SendIntent::AddOwnAccountDevices {
            group_id: chat.clone(),
            siblings: vec![sibling],
            forwarded: Default::default(),
        },
    )
    .await;
    println!("    A1 AddOwnAccountDevices -> {sent:?}");
    let sent = sent.unwrap();
    let commit = confirm_evolution(&mut a1_tagged, sent, &chat)
        .await
        .expect("same-account add commits");
    assert_accepted(&mut b_chat, &commit, &chat).await;

    let owed = owed(&a1_tagged);
    println!("    owed sibling welcomes: {}", owed.len());
    assert_eq!(owed.len(), 1);
    assert_joins_from(&mut a3_tagged, &owed[0], &chat, &a1_tagged).await;

    let msg = send_app(&mut a3_tagged, &chat, "A3 via sibling welcome").await;
    let outcome = deliver(&mut b_chat, &msg).await;
    println!("    A3 message on B -> {outcome:?}");
    assert!(matches!(outcome, IngestOutcome::Processed));
    print_events("B", &b_chat.engine.drain_events());
}

/// D4: removing one device of a multi-device account, three ways.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn d4_device_removal() {
    // (a) A1 (non-admin) RemoveOwnAccountDevices naming A2's leaf, no device
    // group involved.
    let clock = ManualConvergenceClock::new(1_000, 10_000);
    let mut bob = clocked_device(b"bob", &clock);
    let mut a1 = device(b"alice", true);
    let mut a2 = device(b"alice", true);
    let a1_kp = a1.engine.fresh_key_package().await.unwrap();
    let a2_kp = a2.engine.fresh_key_package().await.unwrap();
    let gid = found_group(&mut bob, vec![a1_kp, a2_kp], true, &mut [&mut a1, &mut a2]).await;
    assert!(requires_account_devices(&bob.engine, &gid));

    let a2_sig = signature_key(&a2, &gid);
    println!("D4(a) removing A2 leaf sig={}", hex::encode(&a2_sig[..4]));
    let sent = send_intent(
        &mut a1,
        SendIntent::RemoveOwnAccountDevices {
            group_id: gid.clone(),
            leaf_signature_keys: vec![a2_sig],
        },
    )
    .await;
    println!("    A1 RemoveOwnAccountDevices -> {sent:?}");
    let commit = confirm_evolution(&mut a1, sent.unwrap(), &gid)
        .await
        .expect("remove commits");

    for (name, dev) in [("A2", &mut a2), ("B", &mut bob)] {
        let outcome = dev.engine.ingest(commit.clone()).await.unwrap();
        println!("    {name} ingest removal commit: {outcome:?}");
        settle(dev, &gid).await;
        print_events(name, &dev.engine.drain_events());
        let record = dev.engine.group_record(&gid).unwrap();
        println!(
            "    {name} group record: epoch={} members={} removed={} unrecoverable={}",
            record.epoch.0,
            record.members.len(),
            record.removed,
            record.unrecoverable
        );
        println!("    {name} epoch_state: {:?}", dev.engine.epoch_state(&gid));
    }
    let a2_payload = app_payload_for(&a2.engine, "a2 after removal");
    let send_after = send_intent(
        &mut a2,
        SendIntent::AppMessage {
            group_id: gid.clone(),
            payload: a2_payload,
            expected_epoch: None,
        },
    )
    .await;
    println!("    A2 send after removal -> {send_after:?}");
    let later = send_app(&mut bob, &gid, "post-removal").await;
    let outcome = deliver(&mut a2, &later).await;
    println!("    B post-removal message on A2 -> {outcome:?}");

    // (b) fresh group: B (admin) RemoveMembers of account A.
    let mut bob = device(b"bob", true);
    let mut a1 = device(b"alice", true);
    let mut a2 = device(b"alice", true);
    let a1_kp = a1.engine.fresh_key_package().await.unwrap();
    let a2_kp = a2.engine.fresh_key_package().await.unwrap();
    let gid = found_group(&mut bob, vec![a1_kp, a2_kp], true, &mut [&mut a1, &mut a2]).await;
    let a_account = a1.engine.self_id();
    // First: does naming A2's leaf credential as the member id even resolve?
    let a2_cred = signature_key(&a2, &gid);
    let probe = send_intent(
        &mut bob,
        SendIntent::RemoveMembers {
            group_id: gid.clone(),
            members: vec![MemberId::new(a2_cred)],
        },
    )
    .await;
    println!("D4(b) RemoveMembers named by A2 leaf credential -> {probe:?}");
    let sent = send_intent(
        &mut bob,
        SendIntent::RemoveMembers {
            group_id: gid.clone(),
            members: vec![a_account],
        },
    )
    .await;
    println!("    RemoveMembers(account A) -> {sent:?}");
    let commit = confirm_evolution(&mut bob, sent.unwrap(), &gid)
        .await
        .expect("remove commits");
    println!(
        "    B members after commit: {}",
        bob.engine.members(&gid).unwrap().len()
    );
    for (name, dev) in [("A1", &mut a1), ("A2", &mut a2)] {
        let outcome = dev.engine.ingest(commit.clone()).await.unwrap();
        println!("    {name} ingest removal: {outcome:?}");
        settle(dev, &gid).await;
        print_events(name, &dev.engine.drain_events());
        let record = dev.engine.group_record(&gid).unwrap();
        println!(
            "    {name} record: removed={} unrecoverable={} members={}",
            record.removed,
            record.unrecoverable,
            record.members.len()
        );
    }

    // (c) fresh group: A2 Leave (SelfRemove, non-admin); someone auto-commits.
    let clock = ManualConvergenceClock::new(1_000, 10_000);
    let mut bob = clocked_device(b"bob", &clock);
    let mut a1 = device(b"alice", true);
    let mut a2 = device(b"alice", true);
    let a1_kp = a1.engine.fresh_key_package().await.unwrap();
    let a2_kp = a2.engine.fresh_key_package().await.unwrap();
    let gid = found_group(&mut bob, vec![a1_kp, a2_kp], true, &mut [&mut a1, &mut a2]).await;

    let sent = send_intent(
        &mut a2,
        SendIntent::Leave {
            group_id: gid.clone(),
        },
    )
    .await;
    println!("D4(c) A2 Leave -> {sent:?}");
    let SendResult::Proposal { msg: proposal } = sent.unwrap() else {
        panic!("expected a SelfRemove proposal")
    };
    let proposal = route_to_group(&proposal, &gid);
    let outcome = bob.engine.ingest(proposal.clone()).await.unwrap();
    println!("    B ingest SelfRemove proposal: {outcome:?}");
    // A1 sees the proposal too, before the auto-commit lands.
    let outcome_a1 = a1.engine.ingest(proposal.clone()).await.unwrap();
    println!("    A1 ingest SelfRemove proposal: {outcome_a1:?}");
    print_events("A1-after-proposal", &a1.engine.drain_events());
    advance_selfremove_auto_commit(&mut bob.engine, &gid, &clock).await;
    let publications = bob.engine.drain_auto_publish();
    println!("    B drain_auto_publish: {} item(s)", publications.len());
    assert_eq!(publications.len(), 1, "B auto-commits A2's SelfRemove");
    let publication = publications.into_iter().next().unwrap();
    let commit = route_to_group(&publication.msg, &gid);
    bob.engine
        .confirm_published(publication.pending)
        .await
        .unwrap();
    for (name, dev) in [("A1", &mut a1), ("A2", &mut a2)] {
        let outcome = dev.engine.ingest(commit.clone()).await.unwrap();
        println!("    {name} ingest SelfRemove commit: {outcome:?}");
        settle(dev, &gid).await;
        print_events(name, &dev.engine.drain_events());
        let record = dev.engine.group_record(&gid).unwrap();
        println!(
            "    {name} record: epoch={} members={} removed={}",
            record.epoch.0,
            record.members.len(),
            record.removed
        );
    }
    println!(
        "    B members after leave: {}",
        bob.engine.members(&gid).unwrap().len()
    );
}
