//! Offline / crash-recovery scenarios (R1-R5). Bob is the joiner, Alice the
//! admin, one 1:1 group requiring 0x800d.

mod support;

use cgka_traits::engine::{CgkaEngine, SendIntent, SendResult};
use cgka_traits::ingest::IngestOutcome;
use cgka_traits::types::GroupId;
use storage_sqlite::{SqlCipherKey, SqliteAccountStorage};
use support::*;

async fn pair() -> (Device, Device, GroupId) {
    let mut alice = device(b"alice", true);
    let mut bob = device(b"bob", true);
    let kp = bob.engine.fresh_key_package().await.unwrap();
    let gid = found_group(&mut alice, vec![kp], true, &mut [&mut bob]).await;
    alice.engine.drain_events();
    bob.engine.drain_events();
    (alice, bob, gid)
}

/// One Alice round: a confirmed self_update commit then one app message.
async fn alice_round(
    alice: &mut Device,
    gid: &GroupId,
    i: usize,
) -> (
    cgka_traits::transport::TransportMessage,
    cgka_traits::transport::TransportMessage,
) {
    let commit = send_commit(
        alice,
        SendIntent::SelfUpdate {
            group_id: gid.clone(),
        },
        gid,
    )
    .await;
    let msg = send_app(alice, gid, &format!("msg-{i}")).await;
    (commit, msg)
}

/// R1: Bob offline; every commit+message delivered in send order. All accepted.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn r1_offline_catch_up_in_order() {
    for n in [3usize, 5, 6, 10] {
        let (mut alice, mut bob, gid) = pair().await;
        let mut rounds = Vec::new();
        for i in 0..n {
            rounds.push(alice_round(&mut alice, &gid, i).await);
        }
        let mut processed = 0usize;
        for (i, (commit, msg)) in rounds.iter().enumerate() {
            let oc = bob.engine.ingest(commit.clone()).await.unwrap();
            println!("R1 n={n} commit {i} -> {oc:?}");
            settle(&mut bob, &gid).await;
            let om = bob.engine.ingest(msg.clone()).await.unwrap();
            println!("R1 n={n} msg {i} -> {om:?}");
            if matches!(om, IngestOutcome::Processed) {
                processed += 1;
            }
        }
        let contents = received_contents(&bob.engine.drain_events());
        println!(
            "R1 n={n}: {processed}/{n} messages processed, {} MessageReceived events, bob epoch={}",
            contents.len(),
            bob.engine.epoch(&gid).unwrap().0
        );
        assert_eq!(processed, n);
        assert_eq!(contents.len(), n);
    }
}

/// R2: all commits first, convergence driven to a fixpoint, then all
/// messages. Prints the per-pass commit counts (is there a per-pass cap?)
/// and the real per-message outcomes / observed retention window.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn r2_offline_commits_first() {
    for n in [3usize, 5, 6, 10] {
        let (mut alice, mut bob, gid) = pair().await;
        let mut commits = Vec::new();
        let mut msgs = Vec::new();
        for i in 0..n {
            let (c, m) = alice_round(&mut alice, &gid, i).await;
            commits.push(c);
            msgs.push(m);
        }
        for (i, commit) in commits.iter().enumerate() {
            let oc = bob.engine.ingest(commit.clone()).await.unwrap();
            println!("R2 n={n} commit {i} -> {oc:?}");
        }
        // One settle used to leave mid-buffered commits unapplied (bob stuck
        // at a partial tip); drive convergence until the epoch stops moving.
        let applied = converge_to_fixpoint(&mut bob, &gid, 32);
        let tip = bob.engine.epoch(&gid).unwrap().0;
        println!("R2 n={n}: fixpoint after passes {applied:?} -> tip epoch {tip}");
        let mut retained = 0usize;
        let mut first_processed: Option<usize> = None;
        for (i, msg) in msgs.iter().enumerate() {
            let om = bob.engine.ingest(msg.clone()).await.unwrap();
            println!("R2 n={n} msg {i} (sent at epoch {}) -> {om:?}", i + 1);
            if matches!(om, IngestOutcome::Processed) {
                retained += 1;
                first_processed.get_or_insert(i + 1);
            }
        }
        let boundary = first_processed.map(|e| tip - e as u64);
        println!(
            "R2 n={n}: {retained}/{n} messages processed at tip {tip}; \
             first processed send-epoch={first_processed:?} (distance {boundary:?})"
        );
        assert!(
            matches!(
                bob.engine.ingest(msgs[n - 1].clone()).await,
                Ok(IngestOutcome::Processed) | Ok(IngestOutcome::Ignored { .. })
            ),
            "newest message must not error"
        );
    }
}

/// R3: one commit never arrives on time; later traffic buffers until it does.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn r3_missing_commit() {
    let (mut alice, mut bob, gid) = pair().await;
    let n = 4usize;
    let mut commits = Vec::new();
    let mut msgs = Vec::new();
    for i in 0..n {
        let (c, m) = alice_round(&mut alice, &gid, i).await;
        commits.push(c);
        msgs.push(m);
    }

    // Deliver commit 0 + msg 0 (fine), then everything except commit 1.
    println!(
        "R3 commit 0 -> {:?}",
        bob.engine.ingest(commits[0].clone()).await.unwrap()
    );
    settle(&mut bob, &gid).await;
    println!(
        "R3 msg 0 -> {:?}",
        bob.engine.ingest(msgs[0].clone()).await.unwrap()
    );
    println!(
        "R3 msg 1 (its commit missing) -> {:?}",
        bob.engine.ingest(msgs[1].clone()).await.unwrap()
    );
    for i in 2..n {
        println!(
            "R3 commit {i} (gap behind it) -> {:?}",
            bob.engine.ingest(commits[i].clone()).await.unwrap()
        );
        println!(
            "R3 msg {i} -> {:?}",
            bob.engine.ingest(msgs[i].clone()).await.unwrap()
        );
    }
    settle(&mut bob, &gid).await;
    println!(
        "R3 bob epoch={} epoch_state={:?} events:",
        bob.engine.epoch(&gid).unwrap().0,
        bob.engine.epoch_state(&gid)
    );
    print_events("bob", &bob.engine.drain_events());

    // The missing commit arrives late; convergence should replay the backlog.
    println!(
        "R3 late commit 1 -> {:?}",
        bob.engine.ingest(commits[1].clone()).await.unwrap()
    );
    settle(&mut bob, &gid).await;
    print_events("bob-after-late", &bob.engine.drain_events());
    println!(
        "R3 after late commit: bob epoch={} state={:?}",
        bob.engine.epoch(&gid).unwrap().0,
        bob.engine.epoch_state(&gid)
    );
    // Redeliver any messages still unaccounted for.
    for i in 1..n {
        let om = bob.engine.ingest(msgs[i].clone()).await.unwrap();
        println!("R3 redelivered msg {i} -> {om:?}");
    }
    let contents = received_contents(&bob.engine.drain_events());
    println!("R3 total decoded contents: {}", contents.len());
}

/// R4: Bob's database is restored from a snapshot taken before his own
/// confirmed self_update — his copy never staged the commit it later meets.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn r4_restored_stale_state_after_own_commit() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("bob.sqlite");
    let snap_dir = dir.path().join("snapshot");
    std::fs::create_dir_all(&snap_dir).unwrap();
    let key = SqlCipherKey::new("spike-bob-key").unwrap();

    let copy_db = |from: &std::path::Path, to: &std::path::Path| {
        for entry in std::fs::read_dir(from).unwrap() {
            let entry = entry.unwrap();
            let name = entry.file_name().to_string_lossy().into_owned();
            if name.starts_with("bob.sqlite") {
                std::fs::copy(entry.path(), to.join(&name)).unwrap();
            }
        }
    };

    let store = SqliteAccountStorage::open_encrypted(&db_path, &key).unwrap();
    let mut bob = device_on_storage(store.clone(), b"bob", true);
    let mut alice = device(b"alice", true);
    let kp = bob.engine.fresh_key_package().await.unwrap();
    let gid = found_group(&mut alice, vec![kp], true, &mut [&mut bob]).await;
    let e_before = bob.engine.epoch(&gid).unwrap().0;
    println!("R4 bob epoch before snapshot: {e_before}");

    // Snapshot Bob's database files.
    drop(bob.engine);
    store.close().unwrap();
    copy_db(dir.path(), &snap_dir);

    // Reopen from the live file; Bob self_updates and confirms the publish.
    let store2 = SqliteAccountStorage::open_encrypted(&db_path, &key).unwrap();
    let mut bob2 = device_on_storage(store2.clone(), b"bob", true);
    bob2.engine.hydrate_all_stored_groups().unwrap();
    let res = send_intent(
        &mut bob2,
        SendIntent::SelfUpdate {
            group_id: gid.clone(),
        },
    )
    .await
    .unwrap();
    let own_commit = confirm_evolution(&mut bob2, res, &gid)
        .await
        .expect("self-update commits");
    println!(
        "R4 bob self-update epoch: {}",
        bob2.engine.epoch(&gid).unwrap().0
    );

    // Alice processes it, then commits again and messages.
    assert_accepted(&mut alice, &own_commit, &gid).await;
    let a_commit = send_commit(
        &mut alice,
        SendIntent::SelfUpdate {
            group_id: gid.clone(),
        },
        &gid,
    )
    .await;
    let a_msg = send_app(&mut alice, &gid, "after-restore").await;

    // Restore the snapshot: the copy never staged Bob's own commit.
    drop(bob2.engine);
    store2.close().unwrap();
    copy_db(&snap_dir, dir.path());
    let store3 = SqliteAccountStorage::open_encrypted(&db_path, &key).unwrap();
    let mut bob3 = device_on_storage(store3.clone(), b"bob", true);
    bob3.engine.hydrate_all_stored_groups().unwrap();
    println!(
        "R4 restored bob epoch={} state={:?}",
        bob3.engine.epoch(&gid).unwrap().0,
        bob3.engine.epoch_state(&gid)
    );

    for (label, msg) in [
        ("own self-update commit", &own_commit),
        ("alice commit", &a_commit),
        ("alice message", &a_msg),
    ] {
        let outcome = bob3.engine.ingest(msg.clone()).await;
        println!("R4 restored ingest {label} -> {outcome:?}");
        match bob3
            .engine
            .converge_stored_openmls_messages_at(&gid, 1_000_000)
        {
            Ok(result) => println!("R4 convergence after {label}: {result:?}"),
            Err(e) => println!("R4 convergence after {label} error: {e:?}"),
        }
        print_events("restored", &bob3.engine.drain_events());
        let record = bob3.engine.group_record(&gid).unwrap();
        println!(
            "R4 restored record: epoch={} removed={} unrecoverable={} state={:?}",
            record.epoch.0,
            record.removed,
            record.unrecoverable,
            bob3.engine.epoch_state(&gid)
        );
    }
}

/// R5: after removal, a fresh KeyPackage Welcome for the same group id is a
/// rejoin offer needing explicit consent; an active copy holds it the same way.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn r5_rejoin_after_removed_and_second_leaf() {
    let (mut alice, mut bob, gid) = pair().await;
    let bob_id = bob.engine.self_id();

    // Alice removes Bob; Bob realizes the removal.
    let commit = send_commit(
        &mut alice,
        SendIntent::RemoveMembers {
            group_id: gid.clone(),
            members: vec![bob_id],
        },
        &gid,
    )
    .await;
    println!(
        "R5 bob removal ingest -> {:?}",
        bob.engine.ingest(commit).await.unwrap()
    );
    settle(&mut bob, &gid).await;
    print_events("bob-removed", &bob.engine.drain_events());
    let record = bob.engine.group_record(&gid).unwrap();
    println!(
        "R5 bob record after removal: removed={} unrecoverable={}",
        record.removed, record.unrecoverable
    );

    // Alice re-adds a fresh KeyPackage of Bob's to the SAME group id.
    let kp2 = bob.engine.fresh_key_package().await.unwrap();
    let sent = send_intent(
        &mut alice,
        SendIntent::Invite {
            group_id: gid.clone(),
            key_packages: vec![kp2],
            initial_admins: vec![],
        },
    )
    .await
    .unwrap();
    let SendResult::GroupEvolution {
        welcomes,
        pending,
        msg,
    } = sent
    else {
        panic!("expected GroupEvolution")
    };
    alice.engine.confirm_published(pending).await.unwrap();
    drop(msg);
    assert_eq!(welcomes.len(), 1);
    let outcome = bob.engine.ingest(welcomes[0].clone()).await.unwrap();
    println!("R5 welcome after removal -> {outcome:?}");
    let offers = bob.engine.pending_group_rejoins_for(Some(&gid)).unwrap();
    println!(
        "R5 pending_group_rejoins_for: {} offer(s): {offers:?}",
        offers.len()
    );
    for offer in &offers {
        let token = offer
            .rejoin
            .as_ref()
            .map(|r| r.local_state_token.clone())
            .unwrap_or_default();
        let confirmed = bob
            .engine
            .confirm_group_rejoin(&offer.message_id, &token)
            .await;
        println!("R5 confirm_group_rejoin -> {confirmed:?}");
    }
    print_events("bob-rejoin", &bob.engine.drain_events());
    println!(
        "R5 bob record post-rejoin: epoch={} removed={} members={}",
        bob.engine.group_record(&gid).unwrap().epoch.0,
        bob.engine.group_record(&gid).unwrap().removed,
        bob.engine.members(&gid).map(|m| m.len()).unwrap_or(0)
    );
    // Message round trip on the rejoined group.
    let msg = send_app(&mut alice, &gid, "welcome back").await;
    println!(
        "R5 alice msg on rejoined bob -> {:?}",
        deliver(&mut bob, &msg).await
    );
    let reply = send_app(&mut bob, &gid, "thanks").await;
    println!(
        "R5 bob msg on alice -> {:?}",
        deliver(&mut alice, &reply).await
    );

    // Second variant: a second DEVICE of Bob's account (bob2, fresh leaf key)
    // joins while bob's copy stays healthy — Invite dedups by leaf signature
    // key, so a fresh KP of bob's own device is refused; bob2's is not.
    let mut bob2 = device(b"bob", true);
    let kp3 = bob2.engine.fresh_key_package().await.unwrap();
    let sent = send_intent(
        &mut alice,
        SendIntent::Invite {
            group_id: gid.clone(),
            key_packages: vec![kp3],
            initial_admins: vec![],
        },
    )
    .await
    .unwrap();
    let SendResult::GroupEvolution {
        welcomes, pending, ..
    } = sent
    else {
        panic!("expected GroupEvolution")
    };
    alice.engine.confirm_published(pending).await.unwrap();
    // The same Welcome on the healthy first device: not addressed to its KP.
    let stray = bob.engine.ingest(welcomes[0].clone()).await;
    println!("R5 second-leaf Welcome on bob (healthy) -> {stray:?}");
    let outcome = bob2.engine.ingest(welcomes[0].clone()).await.unwrap();
    println!("R5 second-leaf Welcome on bob2 -> {outcome:?}");
    print_events("bob2", &bob2.engine.drain_events());
    let offers = bob2.engine.pending_group_rejoins_for(Some(&gid)).unwrap();
    println!("R5 bob2 offers: {} -> {:?}", offers.len(), offers);
    for offer in &offers {
        let token = offer
            .rejoin
            .as_ref()
            .map(|r| r.local_state_token.clone())
            .unwrap_or_default();
        let confirmed = bob2
            .engine
            .confirm_group_rejoin(&offer.message_id, &token)
            .await;
        println!("R5 healthy-copy confirm -> {confirmed:?}");
    }
    print_events("bob-healthy", &bob.engine.drain_events());
    println!(
        "R5 bob members after healthy-copy welcome: {:?}",
        bob.engine.members(&gid).map(|m| m.len())
    );
}

/// R3b: the same missing-commit gap as R3 but under the epoch-sealed
/// transport — a future-epoch message cannot even be peeled (DecryptFailed),
/// so the real Nostr contract is what gets recorded here.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn r3b_missing_commit_epoch_sealed() {
    let mut alice = sealed_device(b"alice", true);
    let mut bob = sealed_device(b"bob", true);
    let kp = bob.engine.fresh_key_package().await.unwrap();
    let gid = found_group(&mut alice, vec![kp], true, &mut [&mut bob]).await;
    alice.engine.drain_events();
    bob.engine.drain_events();

    let n = 4usize;
    let mut commits = Vec::new();
    let mut msgs = Vec::new();
    for i in 0..n {
        let (c, m) = alice_round(&mut alice, &gid, i).await;
        commits.push(c);
        msgs.push(m);
    }

    println!(
        "R3b commit 0 -> {:?}",
        bob.engine.ingest(commits[0].clone()).await
    );
    settle(&mut bob, &gid).await;
    println!(
        "R3b msg 0 -> {:?}",
        bob.engine.ingest(msgs[0].clone()).await
    );
    println!(
        "R3b msg 1 (its commit missing) -> {:?}",
        bob.engine.ingest(msgs[1].clone()).await
    );
    for i in 2..n {
        println!(
            "R3b commit {i} (gap behind it) -> {:?}",
            bob.engine.ingest(commits[i].clone()).await
        );
        println!(
            "R3b msg {i} (sealed under a future epoch) -> {:?}",
            bob.engine.ingest(msgs[i].clone()).await
        );
    }
    let applied = converge_to_fixpoint(&mut bob, &gid, 32);
    println!(
        "R3b fixpoint passes {applied:?}; bob epoch={} state={:?}",
        bob.engine.epoch(&gid).unwrap().0,
        bob.engine.epoch_state(&gid)
    );
    print_events("bob", &bob.engine.drain_events());

    // The missing commit arrives late. Does convergence recover the sealed
    // backlog, or must the transport redeliver it?
    println!(
        "R3b late commit 1 -> {:?}",
        bob.engine.ingest(commits[1].clone()).await
    );
    let applied = converge_to_fixpoint(&mut bob, &gid, 32);
    println!(
        "R3b after late commit: passes {applied:?}; epoch={} state={:?}",
        bob.engine.epoch(&gid).unwrap().0,
        bob.engine.epoch_state(&gid)
    );
    print_events("bob-after-late", &bob.engine.drain_events());

    // Whatever stayed undecryptable must be redelivered by the transport.
    for i in 1..n {
        println!(
            "R3b redelivered msg {i} -> {:?}",
            bob.engine.ingest(msgs[i].clone()).await
        );
    }
    settle(&mut bob, &gid).await;
    let contents = received_contents(&bob.engine.drain_events());
    println!("R3b total decoded contents: {} -> {contents:?}", contents.len());
}

/// R4b: the same restored-stale-state fork as R4 under the epoch-sealed
/// transport — peer traffic after the fork cannot be peeled at all.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn r4b_restored_state_epoch_sealed() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("bob.sqlite");
    let snap_dir = dir.path().join("snapshot");
    std::fs::create_dir_all(&snap_dir).unwrap();
    let key = SqlCipherKey::new("spike-bob-key").unwrap();

    let copy_db = |from: &std::path::Path, to: &std::path::Path| {
        for entry in std::fs::read_dir(from).unwrap() {
            let entry = entry.unwrap();
            let name = entry.file_name().to_string_lossy().into_owned();
            if name.starts_with("bob.sqlite") {
                std::fs::copy(entry.path(), to.join(&name)).unwrap();
            }
        }
    };
    let peeler = || Box::new(epoch_sealed_peeler::EpochSealedPeeler) as _;

    let store = SqliteAccountStorage::open_encrypted(&db_path, &key).unwrap();
    let mut bob = device_on_storage_with_peeler(store.clone(), b"bob", true, peeler());
    let mut alice = sealed_device(b"alice", true);
    let kp = bob.engine.fresh_key_package().await.unwrap();
    let gid = found_group(&mut alice, vec![kp], true, &mut [&mut bob]).await;
    println!("R4b bob epoch before snapshot: {}", bob.engine.epoch(&gid).unwrap().0);

    drop(bob.engine);
    store.close().unwrap();
    copy_db(dir.path(), &snap_dir);

    let store2 = SqliteAccountStorage::open_encrypted(&db_path, &key).unwrap();
    let mut bob2 = device_on_storage_with_peeler(store2.clone(), b"bob", true, peeler());
    bob2.engine.hydrate_all_stored_groups().unwrap();
    let res = send_intent(
        &mut bob2,
        SendIntent::SelfUpdate {
            group_id: gid.clone(),
        },
    )
    .await
    .unwrap();
    let own_commit = confirm_evolution(&mut bob2, res, &gid)
        .await
        .expect("self-update commits");
    println!(
        "R4b bob self-update epoch: {}",
        bob2.engine.epoch(&gid).unwrap().0
    );

    println!(
        "R4b alice ingest bob's own commit -> {:?}",
        alice.engine.ingest(own_commit.clone()).await
    );
    let applied = converge_to_fixpoint(&mut alice, &gid, 8);
    println!(
        "R4b alice converge passes {applied:?} -> epoch {}",
        alice.engine.epoch(&gid).unwrap().0
    );
    let a_commit = send_commit(
        &mut alice,
        SendIntent::SelfUpdate {
            group_id: gid.clone(),
        },
        &gid,
    )
    .await;
    let a_msg = send_app(&mut alice, &gid, "after-restore").await;

    drop(bob2.engine);
    store2.close().unwrap();
    copy_db(&snap_dir, dir.path());
    let store3 = SqliteAccountStorage::open_encrypted(&db_path, &key).unwrap();
    let mut bob3 = device_on_storage_with_peeler(store3.clone(), b"bob", true, peeler());
    bob3.engine.hydrate_all_stored_groups().unwrap();
    println!(
        "R4b restored bob epoch={} state={:?}",
        bob3.engine.epoch(&gid).unwrap().0,
        bob3.engine.epoch_state(&gid)
    );

    for (label, msg) in [
        ("own self-update commit", &own_commit),
        ("alice commit", &a_commit),
        ("alice message", &a_msg),
    ] {
        println!("R4b restored ingest {label} -> {:?}", bob3.engine.ingest(msg.clone()).await);
        match bob3
            .engine
            .converge_stored_openmls_messages_at(&gid, 1_000_000)
        {
            Ok(result) => println!("R4b convergence after {label}: {result:?}"),
            Err(e) => println!("R4b convergence after {label} error: {e:?}"),
        }
        print_events("restored", &bob3.engine.drain_events());
        let record = bob3.engine.group_record(&gid).unwrap();
        println!(
            "R4b restored record: epoch={} removed={} unrecoverable={} state={:?}",
            record.epoch.0,
            record.removed,
            record.unrecoverable,
            bob3.engine.epoch_state(&gid)
        );
    }
}

/// R5b: Bob is stale but believes his copy is active (he never received one
/// commit). Alice first tries to Invite a fresh KP of Bob's same device —
/// refused while the old leaf is in the tree — then removes Bob's account
/// and invites the fresh KP. Only the Welcome reaches stale Bob.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn r5b_rejoin_stale_active_copy() {
    let (mut alice, mut bob, gid) = pair().await;
    let bob_id = bob.engine.self_id();

    // Make Bob stale: Alice self-updates but the commit never reaches Bob.
    let missed = send_commit(
        &mut alice,
        SendIntent::SelfUpdate {
            group_id: gid.clone(),
        },
        &gid,
    )
    .await;
    drop(missed);
    println!(
        "R5b bob believes active at epoch {} (alice at {})",
        bob.engine.epoch(&gid).unwrap().0,
        alice.engine.epoch(&gid).unwrap().0
    );

    // (a) Invite a fresh KP of Bob's same device while his old leaf is still
    // in the tree — print the exact refusal.
    let kp_same = bob.engine.fresh_key_package().await.unwrap();
    let refused = send_intent(
        &mut alice,
        SendIntent::Invite {
            group_id: gid.clone(),
            key_packages: vec![kp_same],
            initial_admins: vec![],
        },
    )
    .await;
    println!("R5b invite fresh KP of same device -> {refused:?}");

    // (b) Remove Bob's account, then Invite the fresh KeyPackage.
    let removal = send_commit(
        &mut alice,
        SendIntent::RemoveMembers {
            group_id: gid.clone(),
            members: vec![bob_id],
        },
        &gid,
    )
    .await;
    drop(removal); // never delivered to stale Bob
    let kp_fresh = bob.engine.fresh_key_package().await.unwrap();
    let sent = send_intent(
        &mut alice,
        SendIntent::Invite {
            group_id: gid.clone(),
            key_packages: vec![kp_fresh],
            initial_admins: vec![],
        },
    )
    .await
    .unwrap();
    let SendResult::GroupEvolution {
        welcomes,
        pending,
        msg: _,
    } = sent
    else {
        panic!("expected GroupEvolution")
    };
    alice.engine.confirm_published(pending).await.unwrap();
    assert_eq!(welcomes.len(), 1);

    // Deliver ONLY the Welcome to the stale copy — it cannot process the
    // removal commit it never saw.
    let outcome = bob.engine.ingest(welcomes[0].clone()).await;
    println!("R5b welcome on stale active copy -> {outcome:?}");
    let offers = bob.engine.pending_group_rejoins_for(Some(&gid)).unwrap();
    println!("R5b rejoin offers: {} -> {offers:?}", offers.len());
    for offer in &offers {
        let token = offer
            .rejoin
            .as_ref()
            .map(|r| r.local_state_token.clone())
            .unwrap_or_default();
        let wrong = bob
            .engine
            .confirm_group_rejoin(&offer.message_id, &vec![0xAA; token.len().max(1)])
            .await;
        println!("R5b confirm_group_rejoin wrong token -> {wrong:?}");
        let confirmed = bob
            .engine
            .confirm_group_rejoin(&offer.message_id, &token)
            .await;
        println!("R5b confirm_group_rejoin -> {confirmed:?}");
    }
    print_events("bob-rejoin", &bob.engine.drain_events());
    let record = bob.engine.group_record(&gid).unwrap();
    println!(
        "R5b bob record post-rejoin: epoch={} removed={} unrecoverable={} members={}",
        record.epoch.0,
        record.removed,
        record.unrecoverable,
        bob.engine.members(&gid).map(|m| m.len()).unwrap_or(0)
    );

    // Round trip on the rejoined copy, both directions.
    let msg = send_app(&mut alice, &gid, "welcome back").await;
    println!("R5b alice->bob -> {:?}", deliver(&mut bob, &msg).await);
    let reply = send_app(&mut bob, &gid, "thanks").await;
    println!("R5b bob->alice -> {:?}", deliver(&mut alice, &reply).await);
}

/// R2b: R2's n=10 case but driven the way mk-core drives the engine
/// (`advance_convergence_inputs_until_settled` + `advance_convergence` +
/// the deferred-peel cutoff on the injected clock). Does the drive apply
/// the commits the plain `converge_stored_openmls_messages_at` fixpoint
/// left buffered at tip 7?
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn r2b_commits_first_driven() {
    let n = 10usize;
    let mut alice = device(b"alice", true);
    let clock = manual_convergence_clock();
    let mut bob = clocked_device(b"bob", &clock);
    let kp = bob.engine.fresh_key_package().await.unwrap();
    let gid = found_group(&mut alice, vec![kp], true, &mut [&mut bob]).await;
    let mut commits = Vec::new();
    let mut msgs = Vec::new();
    for i in 0..n {
        let (c, m) = alice_round(&mut alice, &gid, i).await;
        commits.push(c);
        msgs.push(m);
    }
    for (i, commit) in commits.iter().enumerate() {
        println!(
            "R2b commit {i} -> {:?}",
            bob.engine.ingest(commit.clone()).await
        );
    }
    println!("R2b drive pass 1:");
    drive_like_mk_core(&mut bob, &gid, &clock).await;
    let tip = bob.engine.epoch(&gid).unwrap().0;
    println!("R2b tip after first drive: {tip} (target {})", n as u64 + 1);
    if tip < n as u64 + 1 {
        // Still stuck: re-deliver the buffered commits and drive again —
        // does redelivery unblock what the stored backlog could not?
        for (i, commit) in commits.iter().enumerate() {
            println!(
                "R2b redelivered commit {i} -> {:?}",
                bob.engine.ingest(commit.clone()).await
            );
        }
        println!("R2b drive pass 2 (after redelivery):");
        drive_like_mk_core(&mut bob, &gid, &clock).await;
        println!(
            "R2b tip after redeliver+drive: {}",
            bob.engine.epoch(&gid).unwrap().0
        );
    }
    let mut retained = 0usize;
    for (i, msg) in msgs.iter().enumerate() {
        let om = bob.engine.ingest(msg.clone()).await.unwrap();
        println!("R2b msg {i} (sent after commit {i}) -> {om:?}");
        if matches!(om, IngestOutcome::Processed) {
            retained += 1;
        }
    }
    println!(
        "R2b: {retained}/{n} messages processed at tip {}",
        bob.engine.epoch(&gid).unwrap().0
    );
}

/// R2c: the real retention boundary — message M is sent at epoch E before
/// any commit, then k self-update commits follow. Deliver the commits,
/// drive to fixpoint, then deliver M. k ∈ {4,5,6,7}.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn r2c_window_boundary() {
    for k in [4usize, 5, 6, 7] {
        let mut alice = device(b"alice", true);
        let clock = manual_convergence_clock();
        let mut bob = clocked_device(b"bob", &clock);
        let kp = bob.engine.fresh_key_package().await.unwrap();
        let gid = found_group(&mut alice, vec![kp], true, &mut [&mut bob]).await;
        let e = alice.engine.epoch(&gid).unwrap().0;
        let m = send_app(&mut alice, &gid, &format!("early-msg-k{k}")).await;
        let mut commits = Vec::new();
        for _ in 0..k {
            commits.push(
                send_commit(
                    &mut alice,
                    SendIntent::SelfUpdate {
                        group_id: gid.clone(),
                    },
                    &gid,
                )
                .await,
            );
        }
        for (i, commit) in commits.iter().enumerate() {
            println!(
                "R2c k={k} commit {i} -> {:?}",
                bob.engine.ingest(commit.clone()).await
            );
        }
        drive_like_mk_core(&mut bob, &gid, &clock).await;
        let tip = bob.engine.epoch(&gid).unwrap().0;
        let om = bob.engine.ingest(m.clone()).await.unwrap();
        println!(
            "R2c k={k}: M sent at epoch {e}, tip {tip} (distance {}) -> {om:?}",
            tip - e
        );
    }
}

/// R3c: R3b's missing-commit gap under the epoch-sealed peeler, but driven
/// like mk-core after every delivery and after the late commit — does the
/// engine's deferred-peel machinery retry the TransportDeferred items on
/// its own once the epoch advances, or must the transport redeliver them?
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn r3c_missing_commit_epoch_sealed_driven() {
    let mut alice = sealed_device(b"alice", true);
    let clock = manual_convergence_clock();
    let mut bob = sealed_clocked_device(b"bob", &clock);
    let kp = bob.engine.fresh_key_package().await.unwrap();
    let gid = found_group(&mut alice, vec![kp], true, &mut [&mut bob]).await;
    alice.engine.drain_events();
    bob.engine.drain_events();

    let n = 4usize;
    let mut commits = Vec::new();
    let mut msgs = Vec::new();
    for i in 0..n {
        let (c, m) = alice_round(&mut alice, &gid, i).await;
        commits.push(c);
        msgs.push(m);
    }

    async fn ingest_and_drive(
        bob: &mut Device,
        gid: &GroupId,
        clock: &cgka_engine::ManualConvergenceClock,
        label: &str,
        msg: &cgka_traits::transport::TransportMessage,
    ) {
        println!("R3c {label} -> {:?}", bob.engine.ingest(msg.clone()).await);
        drive_like_mk_core(bob, gid, clock).await;
        println!(
            "R3c after {label}: epoch={} state={:?}",
            bob.engine.epoch(gid).unwrap().0,
            bob.engine.epoch_state(gid)
        );
        print_events("bob", &bob.engine.drain_events());
    }

    ingest_and_drive(&mut bob, &gid, &clock, "commit 0", &commits[0]).await;
    ingest_and_drive(&mut bob, &gid, &clock, "msg 0", &msgs[0]).await;
    ingest_and_drive(&mut bob, &gid, &clock, "msg 1 (its commit missing)", &msgs[1]).await;
    for i in 2..n {
        ingest_and_drive(&mut bob, &gid, &clock, &format!("commit {i}"), &commits[i]).await;
        ingest_and_drive(&mut bob, &gid, &clock, &format!("msg {i}"), &msgs[i]).await;
    }

    // The missing commit arrives late. Does the drive recover the deferred
    // backlog on its own?
    ingest_and_drive(&mut bob, &gid, &clock, "late commit 1", &commits[1]).await;
    let contents = received_contents(&bob.engine.drain_events());
    println!(
        "R3c decoded after late commit (no redelivery): {} -> {contents:?}; epoch={}",
        contents.len(),
        bob.engine.epoch(&gid).unwrap().0
    );

    // Then explicit transport redelivery of everything deferred — commits
    // first (msgs 2/3 are sealed under epochs only those commits install).
    for i in 1..n {
        ingest_and_drive(&mut bob, &gid, &clock, &format!("redelivered msg {i}"), &msgs[i]).await;
    }
    for i in 2..n {
        ingest_and_drive(&mut bob, &gid, &clock, &format!("redelivered commit {i}"), &commits[i]).await;
    }
    for i in 2..n {
        ingest_and_drive(&mut bob, &gid, &clock, &format!("redelivered msg {i} again"), &msgs[i]).await;
    }
    let contents = received_contents(&bob.engine.drain_events());
    println!(
        "R3c total decoded contents: {} -> {contents:?}",
        contents.len()
    );
}
