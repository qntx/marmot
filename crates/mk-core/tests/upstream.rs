//! Engine-level reproductions of upstream MDK defects, kept on
//! `Engine<SqliteAccountStorage>` so they pin the defect independent of the
//! KV provider. Each test asserts the *current* upstream behaviour: a pin
//! bump that fixes the defect flips the assertion and is noticed here.

#![allow(
    unused_crate_dependencies,
    reason = "every integration-test binary links the full dev-dependency set"
)]
#![allow(
    clippy::unwrap_used,
    clippy::unwrap_in_result,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::cast_possible_truncation,
    clippy::missing_const_for_fn,
    clippy::shadow_unrelated,
    clippy::missing_docs_in_private_items,
    clippy::similar_names,
    clippy::assertions_on_result_states,
    clippy::items_after_statements,
    clippy::clone_on_ref_ptr,
    clippy::str_to_string,
    clippy::missing_assert_message,
    clippy::tests_outside_test_module,
    reason = "test code"
)]

mod harness;

use std::sync::Arc;

use cgka_engine::convergence_clock::ManualConvergenceClock;
use cgka_engine::{Engine, EngineBuilder};
use cgka_traits::engine::{CgkaEngine, CreateGroupRequest, SendIntent, SendResult};
use cgka_traits::engine_state::PendingStateRef;
use cgka_traits::error::EngineError;
use cgka_traits::group::ProtocolProfile;
use cgka_traits::storage::{MessageStorage, StorageError};
use cgka_traits::types::GroupId;
use harness::{
    HostAccountSigner, RELAY_URL, account_keys, routing_component, supported_components,
    supported_features,
};
use storage_sqlite::{SqlCipherKey, SqliteAccountStorage};
use transport_nostr_peeler::NostrMlsPeeler;

/// A file-backed engine member on `SqliteAccountStorage`, built exactly like
/// the KV `Member` so the only variable is the storage backend.
#[expect(dead_code, reason = "fields keep the handles alive for the engine")]
struct SqliteMember {
    signer: Arc<HostAccountSigner>,
    storage: SqliteAccountStorage,
    clock: Arc<ManualConvergenceClock>,
    engine: Engine<SqliteAccountStorage>,
}

fn open_sqlite_member(path: &std::path::Path, seed: &str) -> SqliteMember {
    let signer = HostAccountSigner::new(account_keys(seed));
    let storage = SqliteAccountStorage::open_encrypted(path, &SqlCipherKey::raw([0x42; 32]))
        .expect("sqlite opens");
    let peeler = NostrMlsPeeler::new().with_welcome_signer_arc(signer.clone());
    let clock = Arc::new(ManualConvergenceClock::new(0, 0));
    let mut engine = EngineBuilder::new(storage.clone())
        .identity(signer.public_key_bytes().to_vec())
        .account_identity_proof_signer(signer.clone())
        .protocol_profile(ProtocolProfile::Current)
        .supported_app_components(supported_components())
        .feature_registry(supported_features())
        .peeler(Box::new(peeler))
        .convergence_clock(clock.clone())
        .build()
        .expect("engine builds");
    engine
        .hydrate_stable_groups_from_storage()
        .expect("hydrate persisted groups");
    SqliteMember {
        signer,
        storage,
        clock,
        engine,
    }
}

/// Stage one deterministic group-evolution commit — a pure `AppDataUpdate`
/// carries no fresh key material, so a rebuild on the same parent state
/// produces a byte-identical inner MLS commit (the production trigger is a
/// `SelfRemove` auto-commit; `UpdateGroupData` shares the same deterministic
/// path). The transport wrapper is signed per publish, so the wrapped bytes
/// differ — the commit digest that keys the checkpoint does not.
async fn send_deterministic_commit(
    member: &mut SqliteMember,
    group_id: &GroupId,
) -> PendingStateRef {
    let result = member
        .engine
        .send(SendIntent::UpdateGroupData {
            group_id: group_id.clone(),
            name: Some("pinned".into()),
            description: None,
        })
        .await
        .expect("send");
    let SendResult::GroupEvolution {
        pending, welcomes, ..
    } = result
    else {
        panic!("expected GroupEvolution, got {result:?}");
    };
    assert!(welcomes.is_empty(), "a profile update stages no welcomes");
    pending
}

/// mdk defect: a re-confirm of an own commit collides on its own-commit
/// checkpoint.
///
/// `confirm_published` keys the checkpoint by commit digest
/// (`openmls-own-commit-v1-…`) and captures the full openmls state as the
/// blob (`publish.rs` → `create_group_state_checkpoint`). The capture embeds
/// `OpenMLS` `MessageSecrets.added_at`, stamped `SystemTime::now()` at every
/// merge (`past_secrets.rs`), so the blob is not a pure function of the
/// commit. When convergence rewinds canonical state to the pre-commit anchor
/// (checkpoint rows are convergence artifacts and survive the restore) and
/// the engine re-issues the byte-identical deterministic commit, the second
/// confirm reaches `create_checkpoint` with the same id and epoch but a
/// different blob — `storage-sqlite` `checkpoints.rs` answers
/// `StorageError::AlreadyExists`, escaping the whole confirm transaction
/// even though a relay already accepted the commit.
#[tokio::test(flavor = "current_thread")]
async fn reconfirm_of_same_own_commit_after_rewind_errors_already_exists() {
    let dir = std::env::temp_dir().join(format!("mk-upstream-{}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("temp dir");
    let path = dir.join("member.db");

    // Incarnation 1: found a group, anchor the pre-commit state, then stage,
    // publish and confirm one deterministic own commit.
    let mut member = open_sqlite_member(&path, "sqlite-member");
    let req = CreateGroupRequest {
        name: "repro".into(),
        description: String::new(),
        members: Vec::new(),
        required_features: Vec::new(),
        app_components: vec![routing_component(
            &member.engine.self_id(),
            &[RELAY_URL.to_string()],
        )],
        initial_admins: Vec::new(),
    };
    let (group_id, create_result) = member.engine.create_group(req).await.expect("create_group");
    let SendResult::FoundingGroupCreated { welcomes } = create_result else {
        panic!("expected FoundingGroupCreated, got {create_result:?}");
    };
    assert!(
        welcomes.is_empty(),
        "a single-member create stages no welcomes"
    );

    member
        .storage
        .create_group_snapshot(&group_id, "repro-anchor")
        .expect("anchor snapshot");
    let first_pending = send_deterministic_commit(&mut member, &group_id).await;
    member
        .engine
        .confirm_published(first_pending)
        .await
        .expect("first confirm writes the checkpoint");

    // Convergence rewinds canonical state to the pre-commit anchor; the
    // own-commit checkpoint row survives the restore.
    member
        .storage
        .rollback_group_state_to_snapshot(&group_id, "repro-anchor")
        .expect("anchor rollback");
    drop(member.engine);
    member.storage.close().expect("close sqlite");

    // Incarnation 2: reopen the same database. Durable state is the
    // pre-commit anchor; the one own-commit checkpoint row the first confirm
    // wrote is still present.
    let mut member = open_sqlite_member(&path, "sqlite-member");
    let checkpoints = member
        .storage
        .list_group_state_checkpoints(&group_id)
        .expect("list checkpoints");
    assert_eq!(
        checkpoints.len(),
        1,
        "the first confirm's checkpoint survives"
    );
    assert_eq!(checkpoints[0].resulting_epoch.0, 1);

    let second_pending = send_deterministic_commit(&mut member, &group_id).await;

    // A relay accepted the artifact; the merge lands, but the checkpoint
    // insert collides on the retained row — the byte-identical inner commit
    // resolves to the same checkpoint id at the same epoch, while the blob
    // differs only in the `OpenMLS` `added_at` timestamp. A different commit
    // would be a fresh insert and could not raise `AlreadyExists`.
    let error = member
        .engine
        .confirm_published(second_pending)
        .await
        .expect_err("re-confirm of the same own commit");
    assert!(
        matches!(error, EngineError::Storage(StorageError::AlreadyExists)),
        "expected AlreadyExists, got {error:?}"
    );
}
