//! Post-join self_update scenarios (S1-S4).

mod support;

use cgka_engine::ConvergenceClock;
use cgka_traits::engine::{CgkaEngine, SendIntent};
use cgka_traits::ingest::IngestOutcome;
use cgka_traits::storage::MaintenanceStorage;
use cgka_traits::types::GroupId;
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

/// S1: Bob self_updates right after joining; Alice's old-epoch traffic still
/// decrypts on Bob, and Bob's post-commit message decrypts on Alice.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn s1_self_update_immediately_after_join_while_peer_sends() {
    let (mut alice, mut bob, gid) = pair().await;
    let join_epoch = bob.engine.epoch(&gid).unwrap().0;

    // Bob's commit, confirmed before Alice sees it.
    let res = send_intent(
        &mut bob,
        SendIntent::SelfUpdate {
            group_id: gid.clone(),
        },
    )
    .await
    .unwrap();
    let bob_commit = confirm_evolution(&mut bob, res, &gid)
        .await
        .expect("self-update commits");
    assert_eq!(bob.engine.epoch(&gid).unwrap().0, join_epoch + 1);

    // Alice, still at the pre-commit epoch, sends three messages.
    let mut msgs = Vec::new();
    for i in 0..3 {
        msgs.push(send_app(&mut alice, &gid, &format!("old-epoch-{i}")).await);
    }
    for (i, msg) in msgs.iter().enumerate() {
        let outcome = bob.engine.ingest(msg.clone()).await.unwrap();
        println!("S1 old-epoch msg {i} on bob (post-commit) -> {outcome:?}");
        assert!(matches!(outcome, IngestOutcome::Processed));
    }

    // Alice catches up on Bob's commit; Bob's reply decrypts.
    assert_accepted(&mut alice, &bob_commit, &gid).await;
    let reply = send_app(&mut bob, &gid, "post-commit reply").await;
    let outcome = alice.engine.ingest(reply).await.unwrap();
    println!("S1 bob reply on alice -> {outcome:?}");
    assert!(matches!(outcome, IngestOutcome::Processed));
    println!(
        "S1 final epochs: alice={} bob={}",
        alice.engine.epoch(&gid).unwrap().0,
        bob.engine.epoch(&gid).unwrap().0
    );
}

/// S1b: Bob's NEW-epoch message delivered to Alice BEFORE his commit.
/// Ordered app delivery depends on the transport: does the engine defer the
/// early message and replay it once the commit lands, or is it lost?
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn s1b_new_epoch_message_before_commit() {
    // Variant A: two separate ingest calls (message, inspect, then commit).
    {
        let (mut alice, mut bob, gid) = pair().await;
        let res = send_intent(
            &mut bob,
            SendIntent::SelfUpdate {
                group_id: gid.clone(),
            },
        )
        .await
        .unwrap();
        let bob_commit = confirm_evolution(&mut bob, res, &gid)
            .await
            .expect("self-update commits");
        let bob_msg = send_app(&mut bob, &gid, "new-epoch").await;

        println!(
            "S1b-A transport timestamps: commit={:?} message={:?} (MockPeeler stamps Timestamp(0); the engine does not set it — the transport owns created_at)",
            bob_commit.timestamp, bob_msg.timestamp
        );

        let early = alice.engine.ingest(bob_msg.clone()).await;
        println!("S1b-A message before commit -> {early:?}");
        let committed = alice.engine.ingest(bob_commit.clone()).await;
        println!("S1b-A commit after message -> {committed:?}");
        let result = alice
            .engine
            .converge_stored_openmls_messages_at(&gid, 1_000_000)
            .unwrap();
        println!("S1b-A convergence: {result:?}");
        print_events("alice-A", &alice.engine.drain_events());

        // Redeliver the message: accepted now, or was the first copy retained
        // and already replayed by the convergence pass?
        let late = alice.engine.ingest(bob_msg.clone()).await;
        println!("S1b-A message redelivered -> {late:?}");
        let contents = received_contents(&alice.engine.drain_events());
        println!("S1b-A decoded contents on alice: {contents:?}");
    }

    // Variant B: same ingest batch — the engine has no batch API; a "batch" is
    // consecutive ingests between convergence passes. Deliver both first, run
    // one convergence pass afterwards, then inspect.
    {
        let (mut alice, mut bob, gid) = pair().await;
        let res = send_intent(
            &mut bob,
            SendIntent::SelfUpdate {
                group_id: gid.clone(),
            },
        )
        .await
        .unwrap();
        let bob_commit = confirm_evolution(&mut bob, res, &gid)
            .await
            .expect("self-update commits");
        let bob_msg = send_app(&mut bob, &gid, "new-epoch").await;

        let early = alice.engine.ingest(bob_msg.clone()).await;
        let committed = alice.engine.ingest(bob_commit.clone()).await;
        println!("S1b-B batch: message -> {early:?} ; commit -> {committed:?}");
        let result = alice
            .engine
            .converge_stored_openmls_messages_at(&gid, 1_000_000)
            .unwrap();
        println!("S1b-B convergence: {result:?}");
        print_events("alice-B", &alice.engine.drain_events());
        let contents = received_contents(&alice.engine.drain_events());
        println!("S1b-B decoded contents on alice: {contents:?}");
    }
}

/// S2: both sides self_update from the same epoch; convergence picks one and
/// the loser's intent is re-issued by drain_auto_publish.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn s2_concurrent_commits_after_join() {
    let clock = manual_convergence_clock();
    let mut alice = clocked_device(b"alice", &clock);
    let mut bob = clocked_device(b"bob", &clock);
    let kp = bob.engine.fresh_key_package().await.unwrap();
    let gid = found_group(&mut alice, vec![kp], true, &mut [&mut bob]).await;
    alice.engine.drain_events();
    bob.engine.drain_events();
    let base = alice.engine.epoch(&gid).unwrap().0;
    println!("S2 fork base epoch {base}");

    let res_a = send_intent(
        &mut alice,
        SendIntent::SelfUpdate {
            group_id: gid.clone(),
        },
    )
    .await
    .unwrap();
    let commit_a = confirm_evolution(&mut alice, res_a, &gid)
        .await
        .expect("alice commits");
    let res_b = send_intent(
        &mut bob,
        SendIntent::SelfUpdate {
            group_id: gid.clone(),
        },
    )
    .await
    .unwrap();
    let commit_b = confirm_evolution(&mut bob, res_b, &gid)
        .await
        .expect("bob commits");

    // Crosswise delivery of the competing same-epoch commits.
    println!(
        "S2 alice ingest bob's commit -> {:?}",
        alice.engine.ingest(commit_b.clone()).await.unwrap()
    );
    println!(
        "S2 bob ingest alice's commit -> {:?}",
        bob.engine.ingest(commit_a.clone()).await.unwrap()
    );

    // Past the quiescence window, convergence resolves.
    clock.advance_ms(10_000);
    let cutoff = ConvergenceClock::now(&clock).monotonic_ms;
    let res_alice = alice
        .engine
        .converge_stored_openmls_messages_at(&gid, cutoff)
        .unwrap();
    let res_bob = bob
        .engine
        .converge_stored_openmls_messages_at(&gid, cutoff)
        .unwrap();
    println!(
        "S2 alice convergence: accepted={:?} dropped={:?} status={:?}",
        res_alice.accepted_commits,
        res_alice.dropped_messages.len(),
        res_alice.convergence_status
    );
    println!(
        "S2 bob convergence: accepted={:?} dropped={:?} status={:?}",
        res_bob.accepted_commits,
        res_bob.dropped_messages.len(),
        res_bob.convergence_status
    );
    print_events("alice", &alice.engine.drain_events());
    print_events("bob", &bob.engine.drain_events());

    // The loser re-issues its superseded intent; publish and relay it.
    let auto_a = alice.engine.drain_auto_publish();
    println!("S2 alice drain_auto_publish: {}", auto_a.len());
    for publication in auto_a {
        alice
            .engine
            .confirm_published(publication.pending)
            .await
            .unwrap();
        let commit = route_to_group(&publication.msg, &gid);
        bob.engine.ingest(commit).await.unwrap();
    }
    let auto_b = bob.engine.drain_auto_publish();
    println!("S2 bob drain_auto_publish: {}", auto_b.len());
    for publication in auto_b {
        bob.engine
            .confirm_published(publication.pending)
            .await
            .unwrap();
        let commit = route_to_group(&publication.msg, &gid);
        alice.engine.ingest(commit).await.unwrap();
    }
    let cutoff = ConvergenceClock::now(&clock).monotonic_ms + 1_000_000;
    alice
        .engine
        .converge_stored_openmls_messages_at(&gid, cutoff)
        .unwrap();
    bob.engine
        .converge_stored_openmls_messages_at(&gid, cutoff)
        .unwrap();
    print_events("alice-final", &alice.engine.drain_events());
    print_events("bob-final", &bob.engine.drain_events());
    println!(
        "S2 final epochs: alice={} bob={}",
        alice.engine.epoch(&gid).unwrap().0,
        bob.engine.epoch(&gid).unwrap().0
    );
    assert_eq!(
        alice.engine.epoch(&gid).unwrap(),
        bob.engine.epoch(&gid).unwrap(),
        "converged epochs"
    );

    // Message round trip after settling.
    let msg = send_app(&mut alice, &gid, "a->b after fork").await;
    println!("S2 a->b -> {:?}", deliver(&mut bob, &msg).await);
    let msg = send_app(&mut bob, &gid, "b->a after fork").await;
    println!("S2 b->a -> {:?}", deliver(&mut alice, &msg).await);
}

/// S3: a later self_update after substantial peer traffic just works.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn s3_delayed_self_update_after_peer_traffic() {
    let (mut alice, mut bob, gid) = pair().await;
    for i in 0..10 {
        let msg = send_app(&mut alice, &gid, &format!("traffic-{i}")).await;
        assert!(matches!(
            deliver(&mut bob, &msg).await,
            IngestOutcome::Processed
        ));
    }
    bob.engine.drain_events();

    let res = send_intent(
        &mut bob,
        SendIntent::SelfUpdate {
            group_id: gid.clone(),
        },
    )
    .await
    .unwrap();
    let commit = confirm_evolution(&mut bob, res, &gid)
        .await
        .expect("self-update commits");
    assert_accepted(&mut alice, &commit, &gid).await;

    let msg = send_app(&mut bob, &gid, "post-update").await;
    println!(
        "S3 bob post-update msg on alice -> {:?}",
        deliver(&mut alice, &msg).await
    );
    let msg = send_app(&mut alice, &gid, "reply").await;
    println!(
        "S3 alice reply on bob -> {:?}",
        deliver(&mut bob, &msg).await
    );
}

/// S4: the PostJoin maintenance obligation rows — does the ENGINE complete
/// them on a confirmed self_update, or does marmot-session's runtime own that?
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn s4_post_join_obligation_rows() {
    let (alice, mut bob, gid) = pair().await;

    let dump = |dev: &Device, label: &str| {
        let obligations = dev.storage.list_maintenance_obligations().unwrap();
        println!("S4 {label}: {} obligation row(s)", obligations.len());
        for o in &obligations {
            println!(
                "    trigger={:?} phase={:?} group_match={}",
                o.trigger,
                o.phase,
                o.group_id == gid
            );
        }
        obligations
    };

    let before = dump(&bob, "bob after join");
    let res = send_intent(
        &mut bob,
        SendIntent::SelfUpdate {
            group_id: gid.clone(),
        },
    )
    .await
    .unwrap();
    let commit = confirm_evolution(&mut bob, res, &gid)
        .await
        .expect("self-update commits");
    drop(commit);
    let after = dump(&bob, "bob after confirmed self_update");
    println!(
        "S4 PostJoin obligation: {} row(s) before -> {} after; phases after: {:?}",
        before
            .iter()
            .filter(|o| matches!(
                o.trigger,
                cgka_traits::maintenance::MaintenanceTrigger::PostJoin
            ))
            .count(),
        after
            .iter()
            .filter(|o| matches!(
                o.trigger,
                cgka_traits::maintenance::MaintenanceTrigger::PostJoin
            ))
            .count(),
        after.iter().map(|o| o.phase).collect::<Vec<_>>()
    );
    // Deliver it to Alice so the group stays coherent for any follow-up.
    let _ = alice;
}
