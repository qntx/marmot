//! KeyPackage lifecycle scenarios (K1-K5).

mod support;

use cgka_traits::engine::{CgkaEngine, SendIntent, SendResult};
use cgka_traits::storage::MaintenanceStorage;
use support::*;

/// K1: a fresh KeyPackage is last-resort, and lands in the owned inventory.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn k1_fresh_key_package_is_last_resort() {
    let mut bob = device(b"bob", true);
    let kp = bob.engine.fresh_key_package().await.unwrap();
    let meta = cgka_engine::key_package::key_package_metadata(&kp).unwrap();
    println!(
        "K1 is_last_resort={} lifetime=[{}, {}) delta={}s ref={}",
        meta.last_resort,
        meta.not_before,
        meta.not_after,
        meta.not_after.saturating_sub(meta.not_before),
        meta.key_package_ref_hex
    );
    assert!(meta.last_resort);
    assert!(meta.not_after > meta.not_before);

    let owned = bob.engine.durably_owned_key_packages().unwrap();
    println!("K1 durably_owned_key_packages: {}", owned.len());
    assert!(owned.iter().any(|k| k.bytes() == kp.bytes()));
}

/// K2: one published KeyPackage serves two group joins; the bundle stays.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn k2_last_resort_reuse() {
    let mut alice = device(b"alice", true);
    let mut carol = device(b"carol", true);
    let mut bob = device(b"bob", true);

    let kp = bob.engine.fresh_key_package().await.unwrap();
    println!(
        "K2 owned before joins: {}",
        bob.engine.durably_owned_key_packages().unwrap().len()
    );

    let g1 = found_group(&mut alice, vec![kp.clone()], true, &mut [&mut bob]).await;
    println!(
        "K2 joined G1 {g1:?}; owned after G1: {}",
        bob.engine.durably_owned_key_packages().unwrap().len()
    );

    let g2 = found_group(&mut carol, vec![kp], true, &mut [&mut bob]).await;
    println!(
        "K2 joined G2 {g2:?} with the SAME KeyPackage; owned after G2: {}",
        bob.engine.durably_owned_key_packages().unwrap().len()
    );
    assert_ne!(g1, g2);
}

/// K3: after Bob retires a KeyPackage (rotation), a Welcome built from it
/// fails while one built from the replacement joins.
///
/// marmot-session drives rotation via `stage_key_package_replacement` +
/// `promote_key_package_lifecycle`; at engine level the retire step is
/// `delete_key_package` plus the lifecycle row, which is what this simulates.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn k3_rotation_and_late_welcome() {
    let mut alice = device(b"alice", true);
    let mut carol = device(b"carol", true);
    let mut bob = device(b"bob", true);

    let old_kp = bob.engine.fresh_key_package().await.unwrap();
    // Alice already built a group Welcome from the old package (not yet sent).
    let (gid_old, welcomes_old) = create(&mut alice, vec![old_kp.clone()], vec![])
        .await
        .unwrap();
    assert_eq!(welcomes_old.len(), 1);

    // Rotation: replacement issued, old package retired (what
    // promote_key_package_lifecycle does to the retired bundle).
    let new_kp = bob.engine.fresh_key_package().await.unwrap();
    bob.engine.delete_key_package(&old_kp).await.unwrap();
    bob.storage
        .put_key_package_lifecycle(
            &cgka_traits::maintenance::KeyPackageLifecycleState::slot_only("k3-slot".to_string()),
        )
        .unwrap();
    println!(
        "K3 owned after retirement: {} (old deleted)",
        bob.engine.durably_owned_key_packages().unwrap().len()
    );

    // The late Welcome that names the retired package.
    let outcome = bob.engine.ingest(welcomes_old[0].clone()).await;
    println!("K3 welcome from retired KP -> {outcome:?}");
    print_events("bob", &bob.engine.drain_events());
    assert!(
        !matches!(outcome, Ok(cgka_traits::ingest::IngestOutcome::Processed)),
        "welcome via retired KP must not join: {outcome:?}"
    );

    // A Welcome built from the replacement joins.
    let (gid_new, welcomes_new) = create(&mut carol, vec![new_kp], vec![]).await.unwrap();
    assert_joins(&mut bob, &welcomes_new[0], &gid_new, &carol).await;
    println!("K3 welcome from replacement joined {gid_new:?} (was {gid_old:?})");
}

/// K4: a Welcome whose processing fails leaves the KeyPackage bundle.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn k4_failed_welcome_leaves_key_package() {
    // (a) A non-admin member's Welcome in a group that does not require
    // account-devices (adds are permissionless there).
    let mut alice = device(b"alice", false);
    let mut carol = device(b"carol", false);
    let mut bob = device(b"bob", false);
    let bob_kp = bob.engine.fresh_key_package().await.unwrap();

    let carol_kp = carol.engine.fresh_key_package().await.unwrap();
    let gid = found_group(&mut alice, vec![carol_kp], false, &mut [&mut carol]).await;
    assert!(!requires_account_devices(&alice.engine, &gid));
    // Carol is a member but not an admin; her Invite still produces a Welcome.
    let sent = send_intent(
        &mut carol,
        SendIntent::Invite {
            group_id: gid.clone(),
            key_packages: vec![bob_kp.clone()],
            initial_admins: vec![],
        },
    )
    .await;
    println!("K4(a) non-admin Invite send -> {sent:?}");
    if let Ok(SendResult::GroupEvolution {
        welcomes,
        pending,
        msg,
    }) = sent
    {
        carol.engine.confirm_published(pending).await.unwrap();
        drop(msg);
        let outcome = bob.engine.ingest(welcomes[0].clone()).await.unwrap();
        println!("K4(a) non-admin-authored Welcome -> {outcome:?}");
        print_events("bob", &bob.engine.drain_events());
        if !matches!(outcome, cgka_traits::ingest::IngestOutcome::Processed) {
            let owned = bob.engine.durably_owned_key_packages().unwrap();
            println!("K4(a) owned after failed join: {}", owned.len());
            assert!(owned.iter().any(|k| k.bytes() == bob_kp.bytes()));
        }
    } else {
        println!("K4(a) non-admin Invite refused at send (finding)");
    }

    // (b) A Welcome whose bytes are corrupted — delivery-level failure.
    let mut alice2 = device(b"alice", true);
    let mut bob2 = device(b"bob", true);
    let kp = bob2.engine.fresh_key_package().await.unwrap();
    let (gid2, welcomes2) = create(&mut alice2, vec![kp.clone()], vec![]).await.unwrap();
    let mut corrupt = welcomes2[0].clone();
    let n = corrupt.payload.len() / 2;
    corrupt.payload[n] ^= 0xff;
    // On a real transport the event id is the hash of the full event, so a
    // corrupted payload is a different delivery id, not a replay of this one.
    corrupt.id = hash_id(&corrupt.payload);
    let outcome = bob2.engine.ingest(corrupt).await;
    println!("K4(b) corrupted Welcome -> {outcome:?}");
    let owned = bob2.engine.durably_owned_key_packages().unwrap();
    println!("K4(b) owned after corrupt Welcome: {}", owned.len());
    assert!(owned.iter().any(|k| k.bytes() == kp.bytes()));

    // The intact Welcome with the same KeyPackage still works.
    assert_joins(&mut bob2, &welcomes2[0], &gid2, &alice2).await;
    println!("K4(b) valid Welcome with same KP joined");
}

/// K5: a deleted KeyPackage's Welcome fails.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn k5_delete_key_package() {
    let mut alice = device(b"alice", true);
    let mut bob = device(b"bob", true);

    let kp = bob.engine.fresh_key_package().await.unwrap();
    let (_gid, welcomes) = create(&mut alice, vec![kp.clone()], vec![]).await.unwrap();
    bob.engine.delete_key_package(&kp).await.unwrap();
    println!(
        "K5 owned after delete: {}",
        bob.engine.durably_owned_key_packages().unwrap().len()
    );

    let outcome = bob.engine.ingest(welcomes[0].clone()).await;
    println!("K5 welcome from deleted KP -> {outcome:?}");
    assert!(
        !matches!(outcome, Ok(cgka_traits::ingest::IngestOutcome::Processed)),
        "welcome via retired KP must not join: {outcome:?}"
    );
}
