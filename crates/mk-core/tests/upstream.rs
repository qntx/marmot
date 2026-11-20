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
    clippy::excessive_nesting,
    clippy::too_many_lines,
    reason = "test code"
)]

mod harness;

use std::sync::Arc;

use cgka_engine::convergence_clock::ManualConvergenceClock;
use cgka_engine::{Engine, EngineBuilder, FeatureRegistry};
use cgka_traits::agent_text_stream::{
    AGENT_TEXT_STREAM_QUIC_RECEIVE_CAPABILITY, AGENT_TEXT_STREAM_QUIC_RECEIVE_FEATURE,
    AgentTextStreamQuicPolicyV1,
};
use cgka_traits::app_components::{AGENT_TEXT_STREAM_QUIC_COMPONENT_ID, AppComponentData};
use cgka_traits::capabilities::{Capability, CapabilityRequirement, Feature, RequirementLevel};
use cgka_traits::engine::{
    CgkaEngine, CreateGroupRequest, KeyPackageSource, SendIntent, SendResult,
};
use cgka_traits::engine_state::PendingStateRef;
use cgka_traits::error::EngineError;
use cgka_traits::group::ProtocolProfile;
use cgka_traits::ingest::IngestOutcome;
use cgka_traits::message::MessageState;
use cgka_traits::storage::{MessageStorage, StorageError};
use cgka_traits::types::{GroupId, MessageId};
use harness::{
    HostAccountSigner, RELAY_URL, account_keys, routing_component, supported_components,
    supported_features,
};
use mk_core::agent_stream::{AgentStreamPolicy, StreamRoles};
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
    open_sqlite_member_with_features(path, seed, supported_features())
}

fn open_sqlite_member_with_features(
    path: &std::path::Path,
    seed: &str,
    registry: FeatureRegistry,
) -> SqliteMember {
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
        .feature_registry(registry)
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
/// checkpoint. This test reaches the state with a manual rollback; the
/// production paths are `refused_step_down_staged_again_collides_after_the_next_pass`
/// and `crashed_step_down_redriven_before_the_pass_collides` below.
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

/// Settle one member's convergence on `group_id`: advance the manual clock
/// past every pass deadline, advance stored inputs, run the full advance and
/// confirm every publish obligation it surfaces, like the session's `drive()`
/// minus relay delivery. Without the clock advance a collecting pass never
/// reaches its quiescence cutoff and retained input stays buffered forever.
async fn settle(member: &mut SqliteMember, group_id: &GroupId) {
    for _ in 0..10 {
        member.clock.advance_ms(60_000);
        let _ = member
            .engine
            .prepare_convergence_cutoff_delay_ms(group_id)
            .expect("arm convergence pass");
        member
            .engine
            .advance_convergence_inputs_until_settled(group_id, 0)
            .await
            .expect("settle inputs");
        let results = member
            .engine
            .advance_convergence(group_id)
            .await
            .expect("advance convergence");
        let mut progressed = !results.is_empty();
        for result in results {
            if let SendResult::GroupEvolution { pending, .. }
            | SendResult::GroupCreated { pending, .. } = result
            {
                member
                    .engine
                    .confirm_published(pending)
                    .await
                    .expect("confirm");
            }
        }
        for obligation in member.engine.drain_auto_publish() {
            progressed = true;
            member
                .engine
                .confirm_published(obligation.pending)
                .await
                .expect("confirm auto-publish");
        }
        let _ = member.engine.drain_auto_proposals();
        if !progressed {
            return;
        }
    }
}

/// mk-core's feature registry (`Session::feature_registry`): `SelfRemove`
/// required, the agent-text-stream receive role optional, send and fanout
/// never registered.
fn receive_only_features() -> FeatureRegistry {
    let mut registry = FeatureRegistry::new();
    registry.register(
        Feature("self-remove"),
        CapabilityRequirement {
            requires: Capability::Proposal(10),
            level: RequirementLevel::Required,
            description: "MIP-03 SelfRemove group departure",
        },
    );
    registry.register(
        AGENT_TEXT_STREAM_QUIC_RECEIVE_FEATURE.clone(),
        CapabilityRequirement {
            requires: AGENT_TEXT_STREAM_QUIC_RECEIVE_CAPABILITY,
            level: RequirementLevel::Optional,
            description: "receive QUIC-backed agent text stream previews",
        },
    );
    registry
}

/// spec `app-components/agent-text-stream-quic-v1.md` "Validation": a
/// component update adding a required member role is valid only when every
/// member of the resulting epoch advertises it. The pinned engine enforces
/// `required_member_roles` on the invite and join paths only
/// (`required_role_capabilities_from_group`), so an `AppDataUpdate` commit
/// that tightens the mask stages, confirms and applies on every member even
/// though none holds the send capability. Current upstream behaviour; a pin
/// bump that enforces the invariant flips this test. mk-core refuses the
/// same state at its own-commit gate instead.
#[tokio::test(flavor = "current_thread")]
async fn component_update_tightening_unheld_role_stages_and_applies() {
    let dir = std::env::temp_dir().join(format!("mk-upstream-roles-{}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("temp dir");
    let mut a =
        open_sqlite_member_with_features(&dir.join("a.db"), "upstream-a", receive_only_features());
    let mut b =
        open_sqlite_member_with_features(&dir.join("b.db"), "upstream-b", receive_only_features());

    // B's KeyPackage advertises receive only. A founds a group carrying the
    // user-to-agent `0x8006` policy (required: receive).
    let mut b_key_package = b.engine.fresh_key_package().await.expect("b key package");
    b_key_package.source = Some(KeyPackageSource {
        event_id: MessageId::new(vec![0x77; 32]),
    });
    let stream = AgentStreamPolicy::user_to_agent_default()
        .to_component()
        .expect("component");
    let req = CreateGroupRequest {
        name: "upstream-roles".into(),
        description: String::new(),
        members: vec![b_key_package],
        required_features: Vec::new(),
        app_components: vec![
            routing_component(&a.engine.self_id(), &[RELAY_URL.to_string()]),
            AppComponentData {
                component_id: stream.component_id,
                data: stream.data,
            },
        ],
        initial_admins: Vec::new(),
    };
    let (group_id, create_result) = a.engine.create_group(req).await.expect("create_group");
    let SendResult::FoundingGroupCreated { welcomes } = create_result else {
        panic!("expected FoundingGroupCreated, got {create_result:?}");
    };
    let welcome = welcomes.into_iter().next().expect("one welcome for B");
    let joined = b
        .engine
        .join_welcome(welcome)
        .await
        .expect("b joins via welcome");
    assert_eq!(joined, group_id);

    // The join leaves B publish-before-apply obligations of its own; settle
    // them or the group stays non-ingestible and A's commit only buffers.
    settle(&mut b, &group_id).await;

    // Tighten `required_member_roles` to receive|send. The spec's
    // resulting-epoch invariant forbids it — no member advertises send —
    // but upstream stages and applies the commit anyway.
    let tightened = AgentStreamPolicy {
        required_roles: StreamRoles::RECEIVE.union(StreamRoles::SEND),
        allowed_roles: StreamRoles::RECEIVE.union(StreamRoles::SEND),
        ..AgentStreamPolicy::user_to_agent_default()
    }
    .to_component()
    .expect("component");
    let result = a
        .engine
        .send(SendIntent::UpdateAppComponents {
            group_id: group_id.clone(),
            updates: vec![AppComponentData {
                component_id: tightened.component_id,
                data: tightened.data,
            }],
        })
        .await
        .expect("the update stages");
    let SendResult::GroupEvolution {
        msg,
        welcomes,
        pending,
    } = result
    else {
        panic!("expected GroupEvolution, got {result:?}");
    };
    assert!(welcomes.is_empty(), "a component update stages no welcomes");
    a.engine
        .confirm_published(pending)
        .await
        .expect("confirm_published");

    // A commit at the current epoch always enters distributed convergence
    // (`commit_should_enter_convergence`), so first delivery can only be
    // `Buffered` — `Processed` is reserved for accepted input once the pass
    // settles. Once the clock clears the pass's quiescence cutoff the pass
    // freezes, selects the commit and applies it: the invalid update lands
    // on the receiver's canonical state unchallenged.
    let outcome = b.engine.ingest(msg).await.expect("b ingests the commit");
    assert!(
        matches!(outcome, IngestOutcome::Buffered { .. }),
        "expected Buffered (retained for convergence), got {outcome:?}"
    );
    settle(&mut b, &group_id).await;
    let bytes = b
        .engine
        .app_component(&group_id, AGENT_TEXT_STREAM_QUIC_COMPONENT_ID)
        .expect("app_component")
        .expect("component present");
    let policy =
        AgentTextStreamQuicPolicyV1::decode_component_state(&bytes).expect("policy decodes");
    assert_eq!(policy.required_member_roles, 0x03);
}

fn temp_dir(tag: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("mk-upstream-{tag}-{}", std::process::id()));
    drop(std::fs::remove_dir_all(&dir));
    std::fs::create_dir_all(&dir).expect("temp dir");
    dir
}

/// A member as mk-core's session opens it: every stored group hydrated, so a
/// stranded pending commit is recovered at open.
fn open_hydrated(path: &std::path::Path, seed: &str) -> SqliteMember {
    let mut member = open_sqlite_member(path, seed);
    member
        .engine
        .hydrate_all_stored_groups()
        .expect("hydrate all stored groups");
    member
}

async fn found_alone(member: &mut SqliteMember) -> GroupId {
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
    member
        .engine
        .create_group(req)
        .await
        .expect("create_group")
        .0
}

/// A founds a group with B as a second admin; B joins and settles.
async fn two_admins(dir: &std::path::Path) -> (SqliteMember, SqliteMember, GroupId) {
    let mut a = open_hydrated(&dir.join("a.db"), "two-admins-a");
    let mut b = open_hydrated(&dir.join("b.db"), "two-admins-b");
    let mut b_key_package = b.engine.fresh_key_package().await.expect("b key package");
    b_key_package.source = Some(KeyPackageSource {
        event_id: MessageId::new(vec![0x55; 32]),
    });
    let req = CreateGroupRequest {
        name: "two-admins".into(),
        description: String::new(),
        members: vec![b_key_package],
        required_features: Vec::new(),
        app_components: vec![routing_component(
            &a.engine.self_id(),
            &[RELAY_URL.to_string()],
        )],
        initial_admins: vec![b.engine.self_id()],
    };
    let (group_id, created) = a.engine.create_group(req).await.expect("create_group");
    let SendResult::FoundingGroupCreated { welcomes } = created else {
        panic!("expected FoundingGroupCreated, got {created:?}");
    };
    let welcome = welcomes.into_iter().next().expect("one welcome for B");
    b.engine.join_welcome(welcome).await.expect("b joins");
    settle(&mut b, &group_id).await;
    (a, b, group_id)
}

/// B asks to leave; the leave re-drive stages its step-down.
async fn stage_step_down(b: &mut SqliteMember, group_id: &GroupId) -> cgka_traits::AutoPublish {
    let requested = b
        .engine
        .send(SendIntent::StepDownAndLeave {
            group_id: group_id.clone(),
        })
        .await
        .expect("leave");
    assert!(
        matches!(requested, SendResult::LeaveRequested { .. }),
        "{requested:?}"
    );
    b.engine
        .advance_convergence(group_id)
        .await
        .expect("advance");
    let mut staged = b.engine.drain_auto_publish();
    assert_eq!(staged.len(), 1, "the step-down");
    staged.remove(0)
}

/// One host convergence timer: arm the cutoff, wait it out, advance. Nothing
/// is confirmed; returns the own commits the advance staged and the number of
/// proposals it produced.
async fn wake(member: &mut SqliteMember, group_id: &GroupId) -> (Vec<PendingStateRef>, usize) {
    let _ = member
        .engine
        .prepare_convergence_cutoff_delay_ms(group_id)
        .expect("arm convergence pass");
    member.clock.advance_ms(60_000);
    member
        .engine
        .advance_convergence_inputs_until_settled(group_id, 0)
        .await
        .expect("settle inputs");
    let mut staged = Vec::new();
    for result in member
        .engine
        .advance_convergence(group_id)
        .await
        .expect("advance convergence")
    {
        if let SendResult::GroupEvolution { pending, .. } = result {
            staged.push(pending);
        }
    }
    staged.extend(
        member
            .engine
            .drain_auto_publish()
            .into_iter()
            .map(|auto| auto.pending),
    );
    let proposals = member.engine.drain_auto_proposals().len();
    (staged, proposals)
}

fn row_state(member: &SqliteMember, id: &MessageId) -> MessageState {
    member.storage.get_message(id).expect("message row").state
}

fn record_epoch(member: &SqliteMember, group_id: &GroupId) -> u64 {
    member
        .engine
        .group_record(group_id)
        .expect("group record")
        .epoch
        .0
}

fn is_admin(member: &SqliteMember, group_id: &GroupId) -> bool {
    let own = member.engine.self_id();
    member
        .engine
        .admin_pubkeys(group_id)
        .expect("admins")
        .iter()
        .any(|admin| admin.as_slice() == own.as_slice())
}

/// mdk defect: a rolled-back own commit is applied by a later convergence
/// pass. `publish_failed` rewinds the MLS state but leaves the commit's own
/// `Sent` message row, and `Sent` is a convergence graph input
/// (`OPENMLS_GRAPH_INPUT_STATES`, `PASS_OPENING_STATES`): the next pass takes
/// the row as a branch candidate and applies the commit no relay accepted.
#[tokio::test(flavor = "current_thread")]
async fn rolled_back_own_commit_is_applied_by_a_later_pass() {
    let dir = temp_dir("rolled-back");
    let mut member = open_hydrated(&dir.join("m.db"), "rolled-back");
    let group_id = found_alone(&mut member).await;
    let result = member
        .engine
        .send(SendIntent::UpdateGroupData {
            group_id: group_id.clone(),
            name: Some("pinned".into()),
            description: None,
        })
        .await
        .expect("send");
    let SendResult::GroupEvolution { msg, pending, .. } = result else {
        panic!("expected GroupEvolution, got {result:?}");
    };
    member
        .engine
        .publish_failed(pending)
        .await
        .expect("publish_failed");
    assert_eq!(record_epoch(&member, &group_id), 0);
    assert_eq!(row_state(&member, &msg.id), MessageState::Sent);

    let (staged, _) = wake(&mut member, &group_id).await;

    assert!(staged.is_empty(), "nothing was confirmed");
    assert_eq!(record_epoch(&member, &group_id), 1);
    assert_eq!(
        member.engine.group_record(&group_id).expect("record").name,
        "pinned"
    );
    assert_eq!(row_state(&member, &msg.id), MessageState::Processed);
}

/// The same defect through reopen: hydration clears a stranded pending
/// commit (`PendingCommitRecovered`, `engine.rs` "A staged commit that
/// survived process restart") but not its `Sent` row.
#[tokio::test(flavor = "current_thread")]
async fn own_commit_cleared_at_reopen_is_applied_by_a_later_pass() {
    let dir = temp_dir("cleared-at-reopen");
    let path = dir.join("m.db");
    let mut member = open_hydrated(&path, "cleared-at-reopen");
    let group_id = found_alone(&mut member).await;
    let (commit, _pending) = {
        let result = member
            .engine
            .send(SendIntent::UpdateGroupData {
                group_id: group_id.clone(),
                name: Some("pinned".into()),
                description: None,
            })
            .await
            .expect("send");
        let SendResult::GroupEvolution { msg, pending, .. } = result else {
            panic!("expected GroupEvolution, got {result:?}");
        };
        (msg.id, pending)
    };
    drop(member.engine);
    member.storage.close().expect("close sqlite");

    let mut member = open_hydrated(&path, "cleared-at-reopen");
    assert!(
        member.engine.drain_events().iter().any(|event| matches!(
            event,
            cgka_traits::GroupEvent::PendingCommitRecovered { .. }
        )),
        "the stranded pending commit is recovered at open"
    );
    assert_eq!(record_epoch(&member, &group_id), 0);
    assert_eq!(row_state(&member, &commit), MessageState::Sent);

    let (staged, _) = wake(&mut member, &group_id).await;

    assert!(staged.is_empty(), "nothing was confirmed");
    assert_eq!(record_epoch(&member, &group_id), 1);
    assert_eq!(
        member.engine.group_record(&group_id).expect("record").name,
        "pinned"
    );
    assert_eq!(row_state(&member, &commit), MessageState::Processed);
}

/// The production trigger of the own-commit checkpoint collision, with no
/// crash: the leave re-drive stages a refused step-down again (mdk#3014) and
/// the second, byte-identical step-down is confirmed. The next pass takes the
/// refused copy's `Sent` row as the branch (same commit digest), invalidates
/// the confirmed copy, rewinds the group to the parent epoch and re-issues
/// the step-down; that confirm meets the first confirm's checkpoint.
#[tokio::test(flavor = "current_thread")]
async fn refused_step_down_staged_again_collides_after_the_next_pass() {
    let dir = temp_dir("refused-step-down");
    let (_a, mut b, group_id) = Box::pin(two_admins(&dir)).await;
    let epoch = record_epoch(&b, &group_id);
    let refused = stage_step_down(&mut b, &group_id).await;
    b.engine
        .publish_failed(refused.pending)
        .await
        .expect("publish_failed");
    b.engine
        .advance_convergence(&group_id)
        .await
        .expect("advance");
    let accepted = b.engine.drain_auto_publish().remove(0);
    b.engine
        .confirm_published(accepted.pending)
        .await
        .expect("confirm the re-staged step-down");
    assert_eq!(record_epoch(&b, &group_id), epoch + 1);

    let (reissued, _) = wake(&mut b, &group_id).await;

    assert_eq!(row_state(&b, &refused.msg.id), MessageState::Processed);
    assert_eq!(
        row_state(&b, &accepted.msg.id),
        MessageState::EpochInvalidated
    );
    assert_eq!(record_epoch(&b, &group_id), epoch);
    assert_eq!(reissued.len(), 1, "the step-down is re-issued");
    let error = b
        .engine
        .confirm_published(reissued[0])
        .await
        .expect_err("the re-issued step-down collides");
    assert!(
        matches!(error, EngineError::Storage(StorageError::AlreadyExists)),
        "expected AlreadyExists, got {error:?}"
    );
}

/// The crash-matrix path (`tests/crash.rs` `restart_boundary_leaving`, cut
/// "durable 16"): the step-down's staging is durable, its fanout is not. At
/// reopen the pending commit is cleared and its `Sent` row kept. When the
/// leave re-drive runs before any convergence cutoff is armed, it stages and
/// confirms the byte-identical step-down; the next pass then collides exactly
/// like the refused case.
#[tokio::test(flavor = "current_thread")]
async fn crashed_step_down_redriven_before_the_pass_collides() {
    let dir = temp_dir("crashed-redriven");
    let (_a, mut b, group_id) = Box::pin(two_admins(&dir)).await;
    let epoch = record_epoch(&b, &group_id);
    let stranded = stage_step_down(&mut b, &group_id).await;
    drop(b.engine);
    b.storage.close().expect("close sqlite");

    let mut b = open_hydrated(&dir.join("b.db"), "two-admins-b");
    assert_eq!(record_epoch(&b, &group_id), epoch);
    assert_eq!(row_state(&b, &stranded.msg.id), MessageState::Sent);
    b.engine
        .advance_convergence(&group_id)
        .await
        .expect("advance");
    let redriven = b.engine.drain_auto_publish().remove(0);
    b.engine
        .confirm_published(redriven.pending)
        .await
        .expect("confirm the re-driven step-down");

    let (reissued, _) = wake(&mut b, &group_id).await;

    assert_eq!(row_state(&b, &stranded.msg.id), MessageState::Processed);
    assert_eq!(
        row_state(&b, &redriven.msg.id),
        MessageState::EpochInvalidated
    );
    assert_eq!(record_epoch(&b, &group_id), epoch);
    assert_eq!(reissued.len(), 1, "the step-down is re-issued");
    let error = b
        .engine
        .confirm_published(reissued[0])
        .await
        .expect_err("the re-issued step-down collides");
    assert!(
        matches!(error, EngineError::Storage(StorageError::AlreadyExists)),
        "expected AlreadyExists, got {error:?}"
    );
}

/// The other order after the same crash: a convergence cutoff armed before
/// the first advance opens a pass over the stranded `Sent` row, which holds
/// the re-drive back; when the pass fires it applies the never-published
/// step-down with no confirm, and the leave goes on to its `SelfRemove`.
/// No collision, but the member is past a commit no relay holds.
#[tokio::test(flavor = "current_thread")]
async fn crashed_step_down_applied_when_the_pass_runs_first() {
    let dir = temp_dir("crashed-pass-first");
    let (_a, mut b, group_id) = Box::pin(two_admins(&dir)).await;
    let epoch = record_epoch(&b, &group_id);
    let stranded = stage_step_down(&mut b, &group_id).await;
    drop(b.engine);
    b.storage.close().expect("close sqlite");

    let mut b = open_hydrated(&dir.join("b.db"), "two-admins-b");
    let (staged, proposals) = wake(&mut b, &group_id).await;

    assert!(staged.is_empty(), "nothing was re-staged or confirmed");
    assert_eq!(row_state(&b, &stranded.msg.id), MessageState::Processed);
    assert_eq!(record_epoch(&b, &group_id), epoch + 1);
    assert!(
        !is_admin(&b, &group_id),
        "the unpublished step-down applied"
    );
    assert_eq!(proposals, 1, "the SelfRemove follows");
}

/// The collision's blob difference, measured rather than read from code: on
/// the refused-step-down path, the first confirm's checkpoint and the one the
/// re-issued confirm captures (written after releasing the first) differ in
/// exactly one `OpenMLS` row, `MessageSecrets`, and only inside its
/// `added_at` timestamp (`OpenMLS` `past_secrets.rs` stamps `SystemTime::now()`
/// at merge). Every other row, the group record and the validated-tree
/// marker are byte-identical. On mk-core's own provider, whose checkpoint
/// rows the test can read.
#[tokio::test(flavor = "current_thread")]
async fn colliding_checkpoints_differ_only_in_added_at() {
    let mut a = harness::Member::fresh("a", "blob-diff-a");
    let mut b = harness::Member::fresh("b", "blob-diff-b");
    let mut b_key_package = b.engine.fresh_key_package().await.expect("b key package");
    b_key_package.source = Some(KeyPackageSource {
        event_id: MessageId::new(vec![0x55; 32]),
    });
    let req = CreateGroupRequest {
        name: "blob-diff".into(),
        description: String::new(),
        members: vec![b_key_package],
        required_features: Vec::new(),
        app_components: vec![routing_component(
            &a.engine.self_id(),
            &[RELAY_URL.to_string()],
        )],
        initial_admins: vec![b.engine.self_id()],
    };
    let (group_id, created) = a.engine.create_group(req).await.expect("create_group");
    let SendResult::FoundingGroupCreated { welcomes } = created else {
        panic!("expected FoundingGroupCreated, got {created:?}");
    };
    let welcome = welcomes.into_iter().next().expect("one welcome for B");
    b.engine.join_welcome(welcome).await.expect("b joins");
    for _ in 0..4 {
        b.clock.advance_ms(60_000);
        let _ = b
            .engine
            .prepare_convergence_cutoff_delay_ms(&group_id)
            .expect("arm convergence pass");
        b.engine
            .advance_convergence_inputs_until_settled(&group_id, 0)
            .await
            .expect("settle inputs");
        for result in b
            .engine
            .advance_convergence(&group_id)
            .await
            .expect("advance")
        {
            if let SendResult::GroupEvolution { pending, .. } = result {
                b.engine.confirm_published(pending).await.expect("confirm");
            }
        }
    }

    let requested = b
        .engine
        .send(SendIntent::StepDownAndLeave {
            group_id: group_id.clone(),
        })
        .await
        .expect("leave");
    assert!(matches!(requested, SendResult::LeaveRequested { .. }));
    b.engine
        .advance_convergence(&group_id)
        .await
        .expect("advance");
    let refused = b.engine.drain_auto_publish().remove(0);
    b.engine
        .publish_failed(refused.pending)
        .await
        .expect("publish_failed");
    b.engine
        .advance_convergence(&group_id)
        .await
        .expect("advance");
    let accepted = b.engine.drain_auto_publish().remove(0);
    b.engine
        .confirm_published(accepted.pending)
        .await
        .expect("confirm");
    let first = checkpoint_image(&b.host);

    let _ = b
        .engine
        .prepare_convergence_cutoff_delay_ms(&group_id)
        .expect("arm convergence pass");
    b.clock.advance_ms(60_000);
    b.engine
        .advance_convergence_inputs_until_settled(&group_id, 0)
        .await
        .expect("settle inputs");
    let _ = b
        .engine
        .advance_convergence(&group_id)
        .await
        .expect("advance");
    let reissued = b.engine.drain_auto_publish().remove(0);
    let error = b
        .engine
        .confirm_published(reissued.pending)
        .await
        .expect_err("collides");
    assert!(matches!(
        error,
        EngineError::Storage(StorageError::AlreadyExists)
    ));
    let checkpoints = b
        .provider
        .list_group_state_checkpoints(&group_id)
        .expect("list checkpoints");
    assert_eq!(checkpoints.len(), 1);
    b.provider
        .release_group_state_checkpoint(&group_id, &checkpoints[0].id)
        .expect("release checkpoint");
    b.engine
        .confirm_published(reissued.pending)
        .await
        .expect("confirm once the first checkpoint is gone");
    let second = checkpoint_image(&b.host);

    assert_ne!(first, second);
    assert_eq!(first["group"], second["group"], "group record");
    assert_eq!(
        first["validated_tree_marker"], second["validated_tree_marker"],
        "validated-tree marker"
    );
    let rows_a = first["openmls"].as_array().expect("openmls rows");
    let rows_b = second["openmls"].as_array().expect("openmls rows");
    assert_eq!(rows_a.len(), rows_b.len());
    let mut differing = Vec::new();
    for (row_a, row_b) in rows_a.iter().zip(rows_b) {
        assert_eq!(row_a[0], row_b[0], "same row keys in the same order");
        if row_a[1] != row_b[1] {
            differing.push((
                json_bytes(&row_a[0]),
                json_bytes(&row_a[1]),
                json_bytes(&row_b[1]),
            ));
        }
    }
    assert_eq!(differing.len(), 1, "exactly one OpenMLS row differs");
    let (key, value_a, value_b) = &differing[0];
    assert!(contains(key, b"MessageSecrets"), "the MessageSecrets row");
    // Compare the decoded rows, not their bytes: `added_at` is a serde
    // `SystemTime` whose `nanos_since_epoch` prints in 1 to 9 digits, so two
    // stamps straddling a power-of-ten nanosecond boundary differ in encoded
    // width (`system_time_stamp_width_tracks_its_nanos`).
    let (rest_a, stamps_a) = split_added_at(value_a);
    let (rest_b, stamps_b) = split_added_at(value_b);
    assert_eq!(rest_a, rest_b, "identical outside every added_at stamp");
    assert_eq!(stamps_a.len(), stamps_b.len());
    let changed = stamps_a
        .iter()
        .zip(&stamps_b)
        .filter(|(stamp_a, stamp_b)| stamp_a != stamp_b)
        .count();
    assert_eq!(changed, 1, "exactly one added_at stamp differs");
}

/// The two encoded widths a merge-time stamp can take a few milliseconds
/// apart: 99.99 ms and 100.01 ms past the same second encode one byte apart,
/// the width difference behind the old byte-length assertion's flakes.
#[test]
fn system_time_stamp_width_tracks_its_nanos() {
    use std::time::{Duration, SystemTime};
    let at = |nanos: u32| {
        serde_json::to_vec(&(SystemTime::UNIX_EPOCH + Duration::new(1_795_163_611, nanos)))
            .expect("stamp json")
    };
    assert_eq!(at(100_010_000).len(), at(99_990_000).len() + 1);
    assert_eq!(at(999_990_000).len(), at(9_990_000).len() + 2);
}

/// An `OpenMLS` row (`u32 BE`-length fields: presence byte, group key, entity
/// JSON) with every `added_at` value in its entity replaced by `null`, and the
/// replaced values in traversal order. The leading fields stay as raw bytes.
fn split_added_at(value: &[u8]) -> ((Vec<Vec<u8>>, serde_json::Value), Vec<serde_json::Value>) {
    fn walk(node: &mut serde_json::Value, stamps: &mut Vec<serde_json::Value>) {
        match node {
            serde_json::Value::Object(map) => {
                for (name, child) in map.iter_mut() {
                    if name == "added_at" {
                        stamps.push(child.take());
                    } else {
                        walk(child, stamps);
                    }
                }
            }
            serde_json::Value::Array(items) => {
                for item in items {
                    walk(item, stamps);
                }
            }
            _ => {}
        }
    }
    let mut fields = Vec::new();
    let mut rest = value;
    while !rest.is_empty() {
        let len = u32::from_be_bytes(rest[..4].try_into().expect("field length")) as usize;
        fields.push(rest[4..4 + len].to_vec());
        rest = &rest[4 + len..];
    }
    let entity = fields.pop().expect("entity field");
    let mut node: serde_json::Value = serde_json::from_slice(&entity).expect("entity json");
    let mut stamps = Vec::new();
    walk(&mut node, &mut stamps);
    ((fields, node), stamps)
}

/// The single checkpoint row on `host`, decoded to its `GroupStateImage`
/// JSON: row = `[u32 len][u64 epoch][u32 len][blob]`, blob = `[tag 1][json]`.
fn checkpoint_image(host: &harness::TestHost) -> serde_json::Value {
    let rows: Vec<_> = host
        .durable_rows()
        .into_iter()
        .filter(|(key, _)| key.first() == Some(&30))
        .collect();
    assert_eq!(rows.len(), 1, "one checkpoint row");
    let row = &rows[0].1;
    let len = u32::from_be_bytes(row[12..16].try_into().expect("blob length")) as usize;
    let blob = &row[16..16 + len];
    assert_eq!(blob[0], 1, "blob tag");
    serde_json::from_slice(&blob[1..]).expect("checkpoint image json")
}

fn json_bytes(value: &serde_json::Value) -> Vec<u8> {
    if let Some(text) = value.as_str() {
        use base64::Engine as _;
        return base64::engine::general_purpose::STANDARD
            .decode(text)
            .expect("base64 byte field");
    }
    value
        .as_array()
        .expect("byte array")
        .iter()
        .map(|n| u8::try_from(n.as_u64().expect("byte")).expect("byte"))
        .collect()
}

fn contains(haystack: &[u8], needle: &[u8]) -> bool {
    haystack
        .windows(needle.len())
        .any(|window| window == needle)
}
