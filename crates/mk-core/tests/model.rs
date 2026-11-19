//! Model test: random operation sequences applied to `KvStorageProvider` and
//! MDK's `SqliteAccountStorage` must produce identical observable results —
//! including nested transactions, group savepoints, snapshots, checkpoints,
//! every storage sub-trait family, the `OpenMLS` storage adapter, and full-state
//! dumps.

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
    clippy::many_single_char_names,
    clippy::missing_const_for_fn,
    clippy::use_self,
    clippy::default_trait_access,
    clippy::needless_for_each,
    clippy::manual_is_multiple_of,
    clippy::useless_let_if_seq,
    clippy::unneeded_struct_pattern,
    clippy::shadow_unrelated,
    clippy::missing_docs_in_private_items,
    clippy::too_many_lines,
    clippy::similar_names,
    clippy::cast_lossless,
    clippy::assertions_on_result_states,
    clippy::ignored_unit_patterns,
    clippy::items_after_statements,
    clippy::cast_possible_wrap,
    clippy::cast_sign_loss,
    clippy::redundant_clone,
    clippy::struct_excessive_bools,
    clippy::doc_markdown,
    clippy::missing_panics_doc,
    clippy::single_call_fn,
    clippy::implicit_hasher,
    clippy::redundant_type_annotations,
    clippy::field_reassign_with_default,
    clippy::tests_outside_test_module,
    clippy::clone_on_ref_ptr,
    clippy::str_to_string,
    clippy::missing_assert_message,
    reason = "test code"
)]
// `unused_crate_dependencies` counts per binary; the dev-dependency set is
// shared across all of them, so each file binds the crates it does not use.
use base64 as _;
use cgka_engine as _;
use chacha20poly1305 as _;
use getrandom as _;
use hex as _;
use hkdf as _;
use nostr as _;
use openmls_rust_crypto as _;
use tokio as _;
use transport_nostr_peeler as _;

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use cgka_traits::capabilities::{
    Capability, CapabilityRequirement, Feature, GroupCapabilities, RequirementLevel,
};
use cgka_traits::convergence_pass::{
    ConvergencePassMember, ConvergencePassMemberRole, ConvergencePassPhase, DurableConvergencePass,
};
use cgka_traits::engine::{GroupEvent, SendIntent};
use cgka_traits::group::{DisbandTombstone, Group, InvitationAdmission, Member, ProtocolProfile};
use cgka_traits::maintenance::{
    DurableGroupEvolution, DurableTransportFanout, GroupEvolutionPhase, GroupEvolutionSemantic,
    GroupMaintenanceState, KeyPackageLifecycleState, MaintenanceObligation, MaintenancePhase,
    MaintenanceTrigger, PeriodicMaintenancePolicy, TransportFanoutAttemptState,
    TransportFanoutTarget,
};
use cgka_traits::message::{MessageRecord, MessageState};
use cgka_traits::storage::{
    AccountDeviceHistoryKey, AccountDeviceSignerBinding, CommittedMembers, CommittedRoster,
    CommittedRosterWrite, DeferredPeelGeneration, DeviceGroupWelcomeExpectation, DisbandCandidate,
    DisbandRequest, DisbandRequestStatus, GroupStateCheckpointRef, LeaveRequest, OwnCommitBaseline,
    OwnCommitIntent, QueuedOutboundIntent, RosterRevision, StorageError, StorageProvider,
};
use cgka_traits::transport::{Timestamp, TransportEnvelope, TransportMessage, TransportSource};
use cgka_traits::transport_adapter::{
    OutboundFanout, TransportEndpoint, TransportPublishRequest, TransportPublishTarget,
};
use cgka_traits::types::{EpochId, GroupId, MemberId, MessageId};
use cgka_traits::welcome::PendingWelcome;
use mk_core::{KvBatch, KvHost, KvOp, KvStorageProvider};
use openmls::prelude::GroupId as MlsGroupId;
use openmls_traits::storage::{
    CURRENT_VERSION, Entity as MlsEntity, Key as MlsKey, StorageProvider as MlsStorage,
    traits as mls_traits,
};
use proptest::prelude::*;
use serde::{Deserialize, Serialize};
use storage_sqlite::SqliteAccountStorage;
use zeroize::Zeroizing;

#[derive(Debug, Default)]
struct TestHost {
    rows: Mutex<BTreeMap<Vec<u8>, Vec<u8>>>,
}

impl KvHost for TestHost {
    fn commit(&self, batch: KvBatch) {
        let mut rows = self.rows.lock().expect("host rows");
        for op in batch.ops {
            match op {
                KvOp::Put { key, value } => {
                    rows.insert(key, value);
                }
                KvOp::Delete { key } => {
                    rows.remove(&key);
                }
            }
        }
    }
}

const GROUPS: u8 = 3;
const MESSAGES: u8 = 12;
const SNAPSHOTS: u8 = 3;
const MLS_SLOTS: u8 = 10;
const MLS_GLOBAL: u8 = 4;
const FEATURES: [&str; 3] = ["feat-a", "feat-b", "feat-c"];

fn gid(n: u8) -> GroupId {
    GroupId::new(vec![n; 16])
}

fn mid(n: u8) -> MessageId {
    MessageId::new(vec![n; 32])
}

fn member(n: u8) -> MemberId {
    MemberId::new(vec![n; 32])
}

/// A small member set from a seed: empty for low seeds, duplicates folded
/// away by `CommittedMembers::from_ids`.
fn members(seed: u8) -> CommittedMembers {
    const POOL: [u8; 4] = [0, 1, 2, 7];
    CommittedMembers::from_ids((0..seed % 4).map(|i| member(POOL[usize::from(seed / 4 + i) % 4])))
}

fn mls_gid(n: u8) -> MlsGroupId {
    MlsGroupId::from_slice(gid(n).as_slice())
}

fn group(n: u8, epoch: u64) -> Group {
    Group {
        id: gid(n),
        name: format!("group-{n}-e{epoch}"),
        description: String::new(),
        epoch: EpochId(epoch),
        members: Vec::new(),
        required_capabilities: GroupCapabilities::default(),
        protocol_profile: ProtocolProfile::Current,
        removed: false,
        unrecoverable: false,
        disbanded: None,
        join_epoch: EpochId(epoch),
        local_copy_install_epoch: EpochId(epoch),
        self_removed_sibling_slots: Default::default(),
        local_copy_welcome_created_at: None,
        device_roster_departures: Vec::new(),
    }
}

fn record(n: u8, g: u8, state: MessageState, epoch: u64) -> MessageRecord {
    MessageRecord {
        id: mid(n),
        group_id: gid(g),
        epoch: EpochId(epoch),
        state,
        payload: vec![n; 4],
        deferred_peel: None,
    }
}

fn welcome(n: u8, g: u8) -> PendingWelcome {
    PendingWelcome {
        message_id: mid(n),
        group_id: gid(g),
        welcome_bytes: vec![g; 8],
        rejoin: None,
    }
}

fn intent(g: u8, n: u8) -> SendIntent {
    SendIntent::AppMessage {
        group_id: gid(g),
        payload: vec![n; 4],
        expected_epoch: None,
    }
}

fn queued_intent(n: u8, g: u8) -> QueuedOutboundIntent {
    QueuedOutboundIntent {
        id: mid(n),
        group_id: gid(g),
        intent: intent(g, n),
        created_at_ms: u64::from(n),
        reissue_attempts: 0,
    }
}

fn own_intent(n: u8, g: u8) -> OwnCommitIntent {
    OwnCommitIntent {
        commit_id: mid(n),
        group_id: gid(g),
        source_epoch: EpochId(u64::from(g)),
        intent: intent(g, n),
        baseline: OwnCommitBaseline::None,
        reinvite: None,
        reissue_attempts: 0,
        created_at_ms: u64::from(n),
    }
}

fn transport_message(n: u8, g: u8) -> TransportMessage {
    TransportMessage {
        id: mid(n),
        payload: vec![n; 4],
        timestamp: Timestamp(u64::from(n)),
        causal_deps: Vec::new(),
        source: TransportSource("model".to_owned()),
        envelope: TransportEnvelope::GroupMessage {
            transport_group_id: vec![g; 8],
        },
    }
}

fn fanout(n: u8, g: u8) -> OutboundFanout {
    OutboundFanout::stage(
        TransportPublishRequest {
            account_id: member(9),
            message: transport_message(n, g),
            target: TransportPublishTarget::Group {
                group_id: gid(g),
                transport_group_id: vec![g; 8],
                endpoints: vec![TransportEndpoint(format!("ep-{g}"))],
            },
            required_acks: 1,
        },
        None,
        None,
        u64::from(n),
    )
    .expect("valid fanout")
}

fn leave_request(g: u8) -> LeaveRequest {
    LeaveRequest {
        group_id: gid(g),
        requested_at_ms: u64::from(g),
        last_proposed_epoch: None,
        blocked: None,
    }
}

fn disband_request(g: u8) -> DisbandRequest {
    DisbandRequest {
        group_id: gid(g),
        requested_at_ms: u64::from(g),
        status: DisbandRequestStatus::Pending,
        last_prepared_epoch: None,
    }
}

fn disband_candidate(g: u8, n: u8) -> DisbandCandidate {
    DisbandCandidate {
        group_id: gid(g),
        source_epoch: EpochId(u64::from(g)),
        commit_id: mid(n),
        content_commit_id: mid(n),
        commit_digest: [n; 32],
        actor: member(n),
        local_was_committer_leaf: n % 2 == 0,
        former_members: Vec::new(),
    }
}

fn disband_tombstone(g: u8) -> DisbandTombstone {
    DisbandTombstone {
        epoch: EpochId(u64::from(g)),
        actor: member(g),
        origin_commit_id: None,
        commit_digest: [g; 32],
        local_was_committer_leaf: false,
        former_members: vec![Member {
            id: member(g),
            credential: vec![g; 4],
        }],
        announced: false,
    }
}

fn admission(g: u8) -> InvitationAdmission {
    InvitationAdmission {
        generation: [g; 32],
        created_locally: true,
        via_welcome: None,
    }
}

fn convergence_pass(g: u8) -> DurableConvergencePass {
    DurableConvergencePass {
        group_id: gid(g),
        generation: u64::from(g),
        phase: ConvergencePassPhase::Collecting,
        base_epoch: EpochId(u64::from(g)),
        clock_instance_id: u64::from(g),
        opened_monotonic_ms: 0,
        quiescence_deadline_monotonic_ms: 0,
        absolute_deadline_monotonic_ms: 0,
        opened_wall_ms: 0,
        quiescence_deadline_wall_ms: 0,
        absolute_deadline_wall_ms: 0,
        members: vec![ConvergencePassMember {
            message_id: mid(g),
            payload_digest: [g; 32],
            role: ConvergencePassMemberRole::CommitEdge,
            source_epoch: u64::from(g),
        }],
        frozen_at_wall_ms: None,
        cutoff_cause: None,
        fairness_slot_available: false,
        retry: None,
    }
}

fn deferred_gen(g: u8) -> DeferredPeelGeneration {
    DeferredPeelGeneration {
        group_id: gid(g),
        context_fingerprint: [g; 32],
    }
}

fn lifecycle(v: u8) -> KeyPackageLifecycleState {
    KeyPackageLifecycleState::slot_only(format!("slot-{v}"))
}

fn group_maintenance(g: u8) -> GroupMaintenanceState {
    GroupMaintenanceState {
        group_id: gid(g),
        enrolled_at: None,
        periodic_enrolled: false,
        last_own_leaf_rotation_at: None,
        next_periodic_rotation_at: None,
        leaf_refresh_due_at: None,
        post_join_history: None,
    }
}

fn obligation(n: u8, g: u8) -> MaintenanceObligation {
    MaintenanceObligation {
        id: mid(n),
        group_id: gid(g),
        trigger: MaintenanceTrigger::Manual,
        phase: MaintenancePhase::Complete,
        created_at: Timestamp(0),
        operational_target_at: None,
        overdue: false,
        eose_deadline_at: None,
        grace_until: None,
        quiet_since: None,
        own_leaf_baseline_hash: None,
        sampled_jitter_ms: 0,
        not_before: None,
        attempt_count: 0,
        semantic_rearm_count: 0,
        last_failure_code: None,
    }
}

fn evolution(n: u8, g: u8) -> DurableGroupEvolution {
    DurableGroupEvolution {
        id: mid(n),
        group_id: gid(g),
        source_epoch: EpochId(0),
        target_epoch: EpochId(1),
        phase: GroupEvolutionPhase::Preparing,
        semantic: GroupEvolutionSemantic::Invite,
        own_leaf_before_hash: None,
        removal_members: Vec::new(),
        signed_message_id: None,
        pending_ref: None,
    }
}

fn transport_fanout(n: u8, g: u8) -> DurableTransportFanout {
    DurableTransportFanout {
        id: mid(n),
        group_id: Some(gid(g)),
        evolution_id: None,
        exact_message: transport_message(n, g),
        target: TransportPublishTarget::Group {
            group_id: gid(g),
            transport_group_id: vec![g; 8],
            endpoints: Vec::new(),
        },
        targets: vec![TransportFanoutTarget {
            endpoint: TransportEndpoint(format!("ep-{g}")),
            state: TransportFanoutAttemptState::Unattempted,
            attempt_count: 0,
            last_attempt_at: None,
            failure_code: None,
        }],
        required_acks: 0,
        evolution_confirmed: false,
        possible_exposure: false,
        created_at: Timestamp(0),
        bounded_until: None,
    }
}

fn signer(n: u8) -> AccountDeviceSignerBinding {
    AccountDeviceSignerBinding {
        marmot_identity: member(n),
        mls_signature_public_key: vec![n; 8],
        account_identity_proof: None,
    }
}

fn history_key(n: u8) -> AccountDeviceHistoryKey {
    AccountDeviceHistoryKey {
        marmot_identity: member(n),
        ciphersuite: 1,
        public_key: vec![n; 8],
        private_key: Zeroizing::new(vec![n; 8]),
    }
}

fn expectation(g: u8) -> DeviceGroupWelcomeExpectation {
    DeviceGroupWelcomeExpectation {
        group_id: gid(g),
        sender_signature_key: vec![g; 8],
        key_package_ref: vec![g; 4],
        expires_at: u64::from(g),
    }
}

fn pending_event(n: u8, g: u8) -> GroupEvent {
    GroupEvent::MessageReceived {
        group_id: gid(g),
        message_id: mid(n),
        sender: member(7),
        sender_leaf_index: Some(0),
        epoch: EpochId(u64::from(g)),
        payload: vec![n; 4],
        retention: None,
        authority: None,
        encrypted_media_secret: None,
    }
}

fn capability_req(n: u8) -> CapabilityRequirement {
    const DESCRIPTIONS: [&str; 2] = ["desc-a", "desc-b"];
    CapabilityRequirement {
        requires: Capability::Proposal(u16::from(n)),
        level: if n % 2 == 0 {
            RequirementLevel::Required
        } else {
            RequirementLevel::Optional
        },
        description: DESCRIPTIONS[usize::from(n) % DESCRIPTIONS.len()],
    }
}

/// Serializable stand-in for every `OpenMLS` entity role in the model: one type
/// implements every entity marker so each typed storage method accepts it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct V(u8);

/// Serializable stand-in for every `OpenMLS` key role (refs, public keys, psk
/// ids, epoch keys).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct K(u8);

impl MlsEntity<CURRENT_VERSION> for V {}
impl mls_traits::TreeSync<CURRENT_VERSION> for V {}
impl mls_traits::GroupContext<CURRENT_VERSION> for V {}
impl mls_traits::GroupState<CURRENT_VERSION> for V {}
impl mls_traits::InterimTranscriptHash<CURRENT_VERSION> for V {}
impl mls_traits::ConfirmationTag<CURRENT_VERSION> for V {}
impl mls_traits::MessageSecrets<CURRENT_VERSION> for V {}
impl mls_traits::GroupEpochSecrets<CURRENT_VERSION> for V {}
impl mls_traits::MlsGroupJoinConfig<CURRENT_VERSION> for V {}
impl mls_traits::LeafNodeIndex<CURRENT_VERSION> for V {}
impl mls_traits::ResumptionPskStore<CURRENT_VERSION> for V {}
impl mls_traits::QueuedProposal<CURRENT_VERSION> for V {}
impl mls_traits::LeafNode<CURRENT_VERSION> for V {}
impl mls_traits::SignatureKeyPair<CURRENT_VERSION> for V {}
impl mls_traits::HpkeKeyPair<CURRENT_VERSION> for V {}
impl mls_traits::KeyPackage<CURRENT_VERSION> for V {}
impl mls_traits::PskBundle<CURRENT_VERSION> for V {}

impl MlsKey<CURRENT_VERSION> for K {}
impl MlsEntity<CURRENT_VERSION> for K {}
impl mls_traits::ProposalRef<CURRENT_VERSION> for K {}
impl mls_traits::SignaturePublicKey<CURRENT_VERSION> for K {}
impl mls_traits::EncryptionKey<CURRENT_VERSION> for K {}
impl mls_traits::HashReference<CURRENT_VERSION> for K {}
impl mls_traits::PskId<CURRENT_VERSION> for K {}
impl mls_traits::EpochKey<CURRENT_VERSION> for K {}

#[derive(Debug, Clone)]
enum Op {
    PutGroupWithRoster(u8, u64, u8),
    PutGroupKeepingRoster(u8, u64),
    PutCommittedRoster(u8, u8),
    CommittedRoster(u8),
    CommittedRosterRevision(u8),
    HasAtomicSavepoints,
    DeleteGroup(u8),
    GetGroup(u8),
    PutMessage(u8, u8, u8, u64),
    UpdateState(u8, u8),
    DeleteMessage(u8),
    GetMessage(u8),
    ReleaseMessage(u8, u8, u8, u64),
    PutWelcome(u8, u8),
    TakeWelcome(u8),
    PutRoute(u8, u8, u64),
    DeleteRoute(u8),
    DeleteRoutesBelow(u8, u64),
    PutQueuedIntent(u8, u8),
    DeleteQueuedIntent(u8),
    PutOwnIntent(u8, u8),
    DeleteOwnIntent(u8),
    PutFanout(u8, u8),
    DeleteFanout(u8),
    PutLeave(u8),
    ClearLeave(u8),
    PutDisbandReq(u8),
    ClearDisbandReq(u8),
    PutDisbandCand(u8, u8),
    ClearDisbandCand(u8),
    PutTombstone(u8),
    MarkTombstone(u8),
    PutAdmission(u8),
    ClearAdmission(u8),
    RegisterFeature(u8),
    PutPolicy(u8),
    PutMarker(u8),
    PutConvPass(u8),
    DeleteConvPass(u8),
    PutDefGen(u8),
    DeleteDefGen(u8),
    PutLifecycle(u8),
    PutGroupMaint(u8),
    DeleteGroupMaint(u8),
    PutObligation(u8, u8),
    DeleteObligation(u8),
    PutEvolution(u8, u8),
    DeleteEvolution(u8),
    PutTransportFanout(u8, u8),
    DeleteTransportFanout(u8),
    PutPeriodicPolicy(u8),
    PutProcessedId(u8, u8),
    PutDedup(u8),
    PutPendingEvent(u8, u8),
    DeletePendingEvent(u8),
    PutSigner(u8),
    PutHistoryKey(u8),
    PutExpectation(u8),
    DeleteExpectation(u8),
    ForgetGroup(u8),
    CompleteReset(u8),
    RollbackKeepLive(u8, u8),
    MlsWrite(u8, u8, u8),
    MlsDelete(u8, u8),
    MlsAppendLeaf(u8, u8),
    MlsDeleteLeaves(u8),
    MlsQueueProposal(u8, u8, u8),
    MlsRemoveProposal(u8, u8),
    MlsClearProposals(u8),
    MlsWriteGlobal(u8, u8, u8),
    MlsDeleteGlobal(u8, u8),
    MlsWriteEpochKeys(u8, u8),
    MlsDeleteEpochKeys(u8, u8),
    BeginCommit(Vec<Op>),
    BeginRollback(Vec<Op>),
    SavepointOk(u8, Vec<Op>),
    SavepointErr(u8, Vec<Op>),
    ReadSnapshot(Vec<Op>),
    SnapshotCreate(u8, u8),
    SnapshotRollback(u8, u8),
    SnapshotRelease(u8, u8),
    StateSnapshotCreate(u8, u8),
    StateSnapshotRollback(u8, u8),
    CheckpointCreate(u8, u8),
    CheckpointRestore(u8, u8),
    CheckpointRelease(u8, u8),
}

fn leaf_strategy() -> impl Strategy<Value = Op> {
    prop_oneof![
        2 => (0..GROUPS, 0..4u64, 0..16u8)
            .prop_map(|(g, e, m)| Op::PutGroupWithRoster(g, e, m)),
        1 => (0..GROUPS, 0..4u64).prop_map(|(g, e)| Op::PutGroupKeepingRoster(g, e)),
        2 => (0..GROUPS, 0..16u8).prop_map(|(g, m)| Op::PutCommittedRoster(g, m)),
        1 => (0..GROUPS).prop_map(Op::CommittedRoster),
        1 => (0..GROUPS).prop_map(Op::CommittedRosterRevision),
        1 => Just(Op::HasAtomicSavepoints),
        1 => (0..GROUPS).prop_map(Op::DeleteGroup),
        1 => (0..GROUPS).prop_map(Op::GetGroup),
        4 => (0..MESSAGES, 0..GROUPS, 0..7u8, 0..4u64).prop_map(|(m, g, s, e)| {
            Op::PutMessage(m, g, s, e)
        }),
        2 => (0..MESSAGES, 0..7u8).prop_map(|(m, s)| Op::UpdateState(m, s)),
        1 => (0..MESSAGES).prop_map(Op::DeleteMessage),
        1 => (0..MESSAGES, 0..GROUPS, 0..7u8, 0..4u64).prop_map(|(m, g, s, e)| {
            Op::ReleaseMessage(m, g, s, e)
        }),
        1 => (0..MESSAGES).prop_map(Op::GetMessage),
        2 => (0..MESSAGES, 0..GROUPS).prop_map(|(m, g)| Op::PutWelcome(m, g)),
        1 => (0..MESSAGES).prop_map(Op::TakeWelcome),
        2 => (0..MESSAGES, 0..GROUPS, 0..4u64).prop_map(|(t, g, e)| Op::PutRoute(t, g, e)),
        1 => (0..MESSAGES).prop_map(Op::DeleteRoute),
        1 => (0..GROUPS, 0..4u64).prop_map(|(g, e)| Op::DeleteRoutesBelow(g, e)),
        1 => (0..MESSAGES, 0..GROUPS).prop_map(|(m, g)| Op::PutQueuedIntent(m, g)),
        1 => (0..MESSAGES).prop_map(Op::DeleteQueuedIntent),
        1 => (0..MESSAGES, 0..GROUPS).prop_map(|(m, g)| Op::PutOwnIntent(m, g)),
        1 => (0..MESSAGES).prop_map(Op::DeleteOwnIntent),
        1 => (0..MESSAGES, 0..GROUPS).prop_map(|(m, g)| Op::PutFanout(m, g)),
        1 => (0..MESSAGES).prop_map(Op::DeleteFanout),
        1 => (0..GROUPS).prop_map(Op::PutLeave),
        1 => (0..GROUPS).prop_map(Op::ClearLeave),
        1 => (0..GROUPS).prop_map(Op::PutDisbandReq),
        1 => (0..GROUPS).prop_map(Op::ClearDisbandReq),
        1 => (0..GROUPS, 0..MESSAGES).prop_map(|(g, m)| Op::PutDisbandCand(g, m)),
        1 => (0..GROUPS).prop_map(Op::ClearDisbandCand),
        1 => (0..GROUPS).prop_map(Op::PutTombstone),
        1 => (0..GROUPS).prop_map(Op::MarkTombstone),
        1 => (0..GROUPS).prop_map(Op::PutAdmission),
        1 => (0..GROUPS).prop_map(Op::ClearAdmission),
        1 => (0..FEATURES.len() as u8).prop_map(Op::RegisterFeature),
        1 => (0..GROUPS).prop_map(Op::PutPolicy),
        1 => (0..GROUPS).prop_map(Op::PutMarker),
        1 => (0..GROUPS).prop_map(Op::PutConvPass),
        1 => (0..GROUPS).prop_map(Op::DeleteConvPass),
        1 => (0..GROUPS).prop_map(Op::PutDefGen),
        1 => (0..GROUPS).prop_map(Op::DeleteDefGen),
        1 => (0..2u8).prop_map(Op::PutLifecycle),
        1 => (0..GROUPS).prop_map(Op::PutGroupMaint),
        1 => (0..GROUPS).prop_map(Op::DeleteGroupMaint),
        1 => (0..MESSAGES, 0..GROUPS).prop_map(|(m, g)| Op::PutObligation(m, g)),
        1 => (0..MESSAGES).prop_map(Op::DeleteObligation),
        1 => (0..MESSAGES, 0..GROUPS).prop_map(|(m, g)| Op::PutEvolution(m, g)),
        1 => (0..MESSAGES).prop_map(Op::DeleteEvolution),
        1 => (0..MESSAGES, 0..GROUPS).prop_map(|(m, g)| Op::PutTransportFanout(m, g)),
        1 => (0..MESSAGES).prop_map(Op::DeleteTransportFanout),
        1 => (0..2u8).prop_map(Op::PutPeriodicPolicy),
        1 => (0..MESSAGES, 0..GROUPS).prop_map(|(m, g)| Op::PutProcessedId(m, g)),
        1 => (0..MESSAGES).prop_map(Op::PutDedup),
        1 => (0..MESSAGES, 0..GROUPS).prop_map(|(m, g)| Op::PutPendingEvent(m, g)),
        1 => (0..MESSAGES).prop_map(Op::DeletePendingEvent),
        1 => (0..MESSAGES).prop_map(Op::PutSigner),
        1 => (0..MESSAGES).prop_map(Op::PutHistoryKey),
        1 => (0..GROUPS).prop_map(Op::PutExpectation),
        1 => (0..GROUPS).prop_map(Op::DeleteExpectation),
        1 => (0..GROUPS).prop_map(Op::ForgetGroup),
        1 => (0..GROUPS).prop_map(Op::CompleteReset),
        1 => (0..GROUPS, 0..SNAPSHOTS).prop_map(|(g, s)| Op::RollbackKeepLive(g, s)),
        3 => (0..MLS_SLOTS, 0..GROUPS, 0..8u8).prop_map(|(s, g, v)| Op::MlsWrite(s, g, v)),
        1 => (0..MLS_SLOTS, 0..GROUPS).prop_map(|(s, g)| Op::MlsDelete(s, g)),
        1 => (0..GROUPS, 0..8u8).prop_map(|(g, v)| Op::MlsAppendLeaf(g, v)),
        1 => (0..GROUPS).prop_map(Op::MlsDeleteLeaves),
        1 => (0..GROUPS, 0..8u8, 0..8u8).prop_map(|(g, r, v)| Op::MlsQueueProposal(g, r, v)),
        1 => (0..GROUPS, 0..8u8).prop_map(|(g, r)| Op::MlsRemoveProposal(g, r)),
        1 => (0..GROUPS).prop_map(Op::MlsClearProposals),
        2 => (0..MLS_GLOBAL, 0..8u8, 0..8u8).prop_map(|(s, k, v)| Op::MlsWriteGlobal(s, k, v)),
        1 => (0..MLS_GLOBAL, 0..8u8).prop_map(|(s, k)| Op::MlsDeleteGlobal(s, k)),
        1 => (0..GROUPS, 0..8u8).prop_map(|(g, e)| Op::MlsWriteEpochKeys(g, e)),
        1 => (0..GROUPS, 0..8u8).prop_map(|(g, e)| Op::MlsDeleteEpochKeys(g, e)),
        2 => (0..GROUPS, 0..SNAPSHOTS).prop_map(|(g, s)| Op::SnapshotCreate(g, s)),
        1 => (0..GROUPS, 0..SNAPSHOTS).prop_map(|(g, s)| Op::SnapshotRollback(g, s)),
        1 => (0..GROUPS, 0..SNAPSHOTS).prop_map(|(g, s)| Op::SnapshotRelease(g, s)),
        1 => (0..GROUPS, 0..SNAPSHOTS).prop_map(|(g, s)| Op::StateSnapshotCreate(g, s)),
        1 => (0..GROUPS, 0..SNAPSHOTS).prop_map(|(g, s)| Op::StateSnapshotRollback(g, s)),
        1 => (0..GROUPS, 0..SNAPSHOTS).prop_map(|(g, s)| Op::CheckpointCreate(g, s)),
        1 => (0..GROUPS, 0..SNAPSHOTS).prop_map(|(g, s)| Op::CheckpointRestore(g, s)),
        1 => (0..GROUPS, 0..SNAPSHOTS).prop_map(|(g, s)| Op::CheckpointRelease(g, s)),
    ]
}

fn state_of(code: u8) -> MessageState {
    match code {
        0 => MessageState::Sent,
        1 => MessageState::Created,
        2 => MessageState::Processed,
        3 => MessageState::Failed,
        4 => MessageState::Retryable,
        5 => MessageState::PeelDeferred,
        _ => MessageState::EpochInvalidated,
    }
}

/// Ops runnable inside a unit (transaction, savepoint, read snapshot).
/// `ForgetGroup` is excluded at every depth: `forget_group_local` reaches
/// `delete_local_group_data_inner`, which opens a raw
/// `transaction_with_behavior(Immediate)` with no transaction-owner check —
/// `3rdparty/mdk/crates/storage-sqlite/src/cross/wipe.rs:84` — so sqlite
/// fails it with `Backend` inside any caller scope while the KV overlay runs
/// it. Every other op either joins the caller's transaction
/// (`connection.with_transaction`/`with_savepoint` passthrough) or guards on
/// `is_current_thread_transaction_owner` (snapshot capture/restore
/// `snapshots/capture.rs:100`, `checkpoints.rs:16,81,165`, `restore.rs:53`;
/// OpenMLS `value_store.rs:186,274,322,428,475`).
fn unit_op_strategy() -> impl Strategy<Value = Op> {
    leaf_strategy()
        .prop_filter(
            "nested BEGIN IMMEDIATE (storage-sqlite cross/wipe.rs:84)",
            |op| !matches!(op, Op::ForgetGroup(_)),
        )
        .prop_recursive(2, 16, 4, |inner| {
            prop_oneof![
                inner.clone().prop_map(|o| o),
                prop::collection::vec(inner.clone(), 1..6).prop_map(Op::BeginCommit),
                prop::collection::vec(inner.clone(), 1..6).prop_map(Op::BeginRollback),
                (0..GROUPS, prop::collection::vec(inner.clone(), 1..6))
                    .prop_map(|(g, ops)| Op::SavepointOk(g, ops)),
                (0..GROUPS, prop::collection::vec(inner.clone(), 1..6))
                    .prop_map(|(g, ops)| Op::SavepointErr(g, ops)),
                prop::collection::vec(inner, 1..4).prop_map(Op::ReadSnapshot),
            ]
        })
}

fn op_strategy() -> impl Strategy<Value = Op> {
    leaf_strategy().prop_recursive(2, 24, 8, |inner| {
        prop_oneof![
            inner.prop_map(|o| o),
            prop::collection::vec(unit_op_strategy(), 1..6).prop_map(Op::BeginCommit),
            prop::collection::vec(unit_op_strategy(), 1..6).prop_map(Op::BeginRollback),
            (0..GROUPS, prop::collection::vec(unit_op_strategy(), 1..6))
                .prop_map(|(g, ops)| Op::SavepointOk(g, ops)),
            (0..GROUPS, prop::collection::vec(unit_op_strategy(), 1..6))
                .prop_map(|(g, ops)| Op::SavepointErr(g, ops)),
            prop::collection::vec(unit_op_strategy(), 1..4).prop_map(Op::ReadSnapshot),
        ]
    })
}

/// One observable value, normalized so both backends compare identically.
#[derive(Debug, PartialEq)]
enum Outcome {
    Unit(Result<(), String>),
    /// `(inner op outcomes, unit result)` for transaction/savepoint/
    /// read-snapshot scopes — inner results are compared too, not just the
    /// unit result and the final dump.
    Nested(Vec<Outcome>, Result<(), String>),
    Group(Result<Group, String>),
    Message(Result<MessageRecord, String>),
    Welcome(Result<PendingWelcome, String>),
    Roster(Result<Option<CommittedRoster>, String>),
    RosterRevision(Result<Option<RosterRevision>, String>),
    RosterWrite(Result<CommittedRosterWrite, String>),
    Bool(Result<bool, String>),
}

fn kind(error: &StorageError) -> String {
    match error {
        StorageError::NotFound => "not-found".into(),
        StorageError::AlreadyExists => "already-exists".into(),
        StorageError::Serialization(_) => "serialization".into(),
        StorageError::Backend(_) => "backend".into(),
        other => format!("{other:?}").chars().take(24).collect(),
    }
}

fn maintenance<S: StorageProvider>(p: &S) -> &dyn cgka_traits::storage::MaintenanceStorage {
    p.maintenance_storage().expect("maintenance storage")
}

fn unit(result: Result<(), StorageError>) -> Outcome {
    Outcome::Unit(result.map_err(|e| kind(&e)))
}

/// `S::Mls` is only used through the `OpenMLS` storage trait; MLS errors are
/// backend-typed, so Err-ness is the parity point (both backends fail or
/// neither does).
fn mls_unit<M: MlsStorage<CURRENT_VERSION>>(
    mls: &M,
    f: impl FnOnce(&M) -> Result<(), M::Error>,
) -> Outcome {
    Outcome::Unit(f(mls).map_err(|_| "mls-err".to_owned()))
}

fn mls_write<M: MlsStorage<CURRENT_VERSION>>(mls: &M, slot: u8, g: u8, v: u8) -> Outcome {
    let gid = mls_gid(g);
    let value = V(v);
    mls_unit(mls, |m| match slot {
        0 => m.write_tree::<MlsGroupId, V>(&gid, &value),
        1 => m.write_context::<MlsGroupId, V>(&gid, &value),
        2 => m.write_group_state::<V, MlsGroupId>(&gid, &value),
        3 => m.write_interim_transcript_hash::<MlsGroupId, V>(&gid, &value),
        4 => m.write_confirmation_tag::<MlsGroupId, V>(&gid, &value),
        5 => m.write_message_secrets::<MlsGroupId, V>(&gid, &value),
        6 => m.write_group_epoch_secrets::<MlsGroupId, V>(&gid, &value),
        7 => m.write_mls_join_config::<MlsGroupId, V>(&gid, &value),
        8 => m.write_own_leaf_index::<MlsGroupId, V>(&gid, &value),
        _ => m.write_resumption_psk_store::<MlsGroupId, V>(&gid, &value),
    })
}

fn mls_delete<M: MlsStorage<CURRENT_VERSION>>(mls: &M, slot: u8, g: u8) -> Outcome {
    let gid = mls_gid(g);
    mls_unit(mls, |m| match slot {
        0 => m.delete_tree::<MlsGroupId>(&gid),
        1 => m.delete_context::<MlsGroupId>(&gid),
        2 => m.delete_group_state::<MlsGroupId>(&gid),
        3 => m.delete_interim_transcript_hash::<MlsGroupId>(&gid),
        4 => m.delete_confirmation_tag::<MlsGroupId>(&gid),
        5 => m.delete_message_secrets::<MlsGroupId>(&gid),
        6 => m.delete_group_epoch_secrets::<MlsGroupId>(&gid),
        7 => m.delete_group_config::<MlsGroupId>(&gid),
        8 => m.delete_own_leaf_index::<MlsGroupId>(&gid),
        _ => m.delete_all_resumption_psk_secrets::<MlsGroupId>(&gid),
    })
}

fn mls_write_global<M: MlsStorage<CURRENT_VERSION>>(mls: &M, slot: u8, k: u8, v: u8) -> Outcome {
    let key = K(k);
    let value = V(v);
    mls_unit(mls, |m| match slot {
        0 => m.write_signature_key_pair::<K, V>(&key, &value),
        1 => m.write_encryption_key_pair::<K, V>(&key, &value),
        2 => m.write_key_package::<K, V>(&key, &value),
        _ => m.write_psk::<K, V>(&key, &value),
    })
}

fn mls_delete_global<M: MlsStorage<CURRENT_VERSION>>(mls: &M, slot: u8, k: u8) -> Outcome {
    let key = K(k);
    mls_unit(mls, |m| match slot {
        0 => m.delete_signature_key_pair::<K>(&key),
        1 => m.delete_encryption_key_pair::<K>(&key),
        2 => m.delete_key_package::<K>(&key),
        _ => m.delete_psk::<K>(&key),
    })
}

fn apply<S: StorageProvider>(p: &S, op: &Op) -> Outcome
where
    S::Mls: MlsStorage<CURRENT_VERSION>,
{
    match op {
        Op::PutGroupWithRoster(g, e, m) => {
            unit(p.put_group_with_roster(&group(*g, *e), &members(*m)))
        }
        Op::PutGroupKeepingRoster(g, e) => unit(p.put_group_keeping_roster(&group(*g, *e))),
        Op::PutCommittedRoster(g, m) => Outcome::RosterWrite(
            p.put_committed_roster(&gid(*g), &members(*m))
                .map_err(|e| kind(&e)),
        ),
        Op::CommittedRoster(g) => {
            Outcome::Roster(p.committed_roster(&gid(*g)).map_err(|e| kind(&e)))
        }
        Op::CommittedRosterRevision(g) => {
            Outcome::RosterRevision(p.committed_roster_revision(&gid(*g)).map_err(|e| kind(&e)))
        }
        Op::HasAtomicSavepoints => Outcome::Bool(Ok(p.has_atomic_group_savepoints())),
        Op::DeleteGroup(g) => unit(p.delete_group(&gid(*g))),
        Op::GetGroup(g) => Outcome::Group(p.get_group(&gid(*g)).map_err(|e| kind(&e))),
        Op::PutMessage(m, g, s, e) => unit(p.put_message(&record(*m, *g, state_of(*s), *e))),
        Op::UpdateState(m, s) => unit(p.update_message_state(&mid(*m), state_of(*s))),
        Op::DeleteMessage(m) => unit(p.delete_message(&mid(*m))),
        Op::GetMessage(m) => Outcome::Message(p.get_message(&mid(*m)).map_err(|e| kind(&e))),
        Op::ReleaseMessage(m, g, s, e) => {
            unit(p.release_message_for_replay(&record(*m, *g, state_of(*s), *e)))
        }
        Op::PutWelcome(m, g) => unit(p.put_welcome(&welcome(*m, *g))),
        Op::TakeWelcome(m) => Outcome::Welcome(p.take_welcome(&mid(*m)).map_err(|e| kind(&e))),
        Op::PutRoute(t, g, e) => unit(p.put_transport_group_route(&[*t; 8], &gid(*g), EpochId(*e))),
        Op::DeleteRoute(t) => unit(p.delete_transport_group_route(&[*t; 8])),
        Op::DeleteRoutesBelow(g, e) => {
            unit(p.delete_transport_group_routes_below_epoch(&gid(*g), EpochId(*e)))
        }
        Op::PutQueuedIntent(m, g) => unit(p.put_queued_outbound_intent(&queued_intent(*m, *g))),
        Op::DeleteQueuedIntent(m) => unit(p.delete_queued_outbound_intent(&mid(*m))),
        Op::PutOwnIntent(m, g) => unit(p.put_own_commit_intent(&own_intent(*m, *g))),
        Op::DeleteOwnIntent(m) => unit(p.delete_own_commit_intent(&mid(*m))),
        Op::PutFanout(m, g) => unit(p.put_outbound_fanout(&fanout(*m, *g))),
        Op::DeleteFanout(m) => unit(p.delete_outbound_fanout(&mid(*m))),
        Op::PutLeave(g) => unit(p.put_leave_request(&leave_request(*g))),
        Op::ClearLeave(g) => unit(p.clear_leave_request(&gid(*g))),
        Op::PutDisbandReq(g) => unit(p.put_disband_request(&disband_request(*g))),
        Op::ClearDisbandReq(g) => unit(p.clear_disband_request(&gid(*g))),
        Op::PutDisbandCand(g, m) => unit(p.put_disband_candidate(&disband_candidate(*g, *m))),
        Op::ClearDisbandCand(g) => unit(p.clear_disband_candidates(&gid(*g))),
        Op::PutTombstone(g) => unit(p.put_disband_tombstone(&gid(*g), &disband_tombstone(*g))),
        Op::MarkTombstone(g) => unit(p.mark_disband_tombstone_announced(&gid(*g))),
        Op::PutAdmission(g) => unit(p.put_invitation_admission(&gid(*g), Some(&admission(*g)))),
        Op::ClearAdmission(g) => unit(p.put_invitation_admission(&gid(*g), None)),
        Op::RegisterFeature(f) => {
            unit(p.register_feature(Feature(FEATURES[usize::from(*f)]), capability_req(*f)))
        }
        Op::PutPolicy(g) => unit(p.put_convergence_policy(&gid(*g), &[*g; 4])),
        Op::PutMarker(g) => unit(p.put_validated_tree_marker(&gid(*g), &[*g; 4])),
        Op::PutConvPass(g) => unit(p.put_convergence_pass(&convergence_pass(*g))),
        Op::DeleteConvPass(g) => unit(p.delete_convergence_pass(&gid(*g))),
        Op::PutDefGen(g) => unit(p.put_deferred_peel_generation(&deferred_gen(*g))),
        Op::DeleteDefGen(g) => unit(p.delete_deferred_peel_generation(&gid(*g))),
        Op::PutLifecycle(v) => unit(maintenance(p).put_key_package_lifecycle(&lifecycle(*v))),
        Op::PutGroupMaint(g) => unit(maintenance(p).put_group_maintenance(&group_maintenance(*g))),
        Op::DeleteGroupMaint(g) => unit(maintenance(p).delete_group_maintenance(&gid(*g))),
        Op::PutObligation(m, g) => {
            unit(maintenance(p).put_maintenance_obligation(&obligation(*m, *g)))
        }
        Op::DeleteObligation(m) => unit(maintenance(p).delete_maintenance_obligation(&mid(*m))),
        Op::PutEvolution(m, g) => unit(maintenance(p).put_group_evolution(&evolution(*m, *g))),
        Op::DeleteEvolution(m) => unit(maintenance(p).delete_group_evolution(&mid(*m))),
        Op::PutTransportFanout(m, g) => {
            unit(maintenance(p).put_transport_fanout(&transport_fanout(*m, *g)))
        }
        Op::DeleteTransportFanout(m) => unit(maintenance(p).delete_transport_fanout(&mid(*m))),
        Op::PutPeriodicPolicy(v) => unit(maintenance(p).put_periodic_maintenance_policy(match v {
            0 => PeriodicMaintenancePolicy::EnabledForNewGroups,
            _ => PeriodicMaintenancePolicy::Disabled,
        })),
        Op::PutProcessedId(m, g) => unit(p.put_processed_transport_id(&gid(*g), &mid(*m))),
        Op::PutDedup(m) => unit(p.put_ingress_dedup_marker(&mid(*m))),
        Op::PutPendingEvent(m, g) => unit(p.put_pending_application_event(&pending_event(*m, *g))),
        Op::DeletePendingEvent(m) => unit(p.delete_pending_application_events(&[mid(*m)])),
        Op::PutSigner(m) => unit(p.put_account_device_signer(&signer(*m))),
        Op::PutHistoryKey(m) => Outcome::Unit(
            p.put_account_device_history_key_if_absent(&history_key(*m))
                .map(|_| ())
                .map_err(|e| kind(&e)),
        ),
        Op::PutExpectation(g) => unit(p.put_device_group_welcome_expectation(&expectation(*g))),
        Op::DeleteExpectation(g) => Outcome::Bool(
            p.delete_device_group_welcome_expectation(&gid(*g))
                .map_err(|e| kind(&e)),
        ),
        Op::ForgetGroup(g) => Outcome::Bool(
            p.forget_group_local(&gid(*g), Timestamp(0))
                .map_err(|e| kind(&e)),
        ),
        Op::CompleteReset(g) => unit(p.complete_group_local_reset(&gid(*g))),
        Op::RollbackKeepLive(g, name) => {
            unit(p.rollback_group_state_keeping_live_records(&gid(*g), &format!("snap-{name}")))
        }
        Op::MlsWrite(s, g, v) => mls_write(p.mls_storage(), *s, *g, *v),
        Op::MlsDelete(s, g) => mls_delete(p.mls_storage(), *s, *g),
        Op::MlsAppendLeaf(g, v) => {
            let gid = mls_gid(*g);
            mls_unit(p.mls_storage(), |m| {
                m.append_own_leaf_node::<MlsGroupId, V>(&gid, &V(*v))
            })
        }
        Op::MlsDeleteLeaves(g) => {
            let gid = mls_gid(*g);
            mls_unit(p.mls_storage(), |m| {
                m.delete_own_leaf_nodes::<MlsGroupId>(&gid)
            })
        }
        Op::MlsQueueProposal(g, r, v) => {
            let gid = mls_gid(*g);
            mls_unit(p.mls_storage(), |m| {
                m.queue_proposal::<MlsGroupId, K, V>(&gid, &K(*r), &V(*v))
            })
        }
        Op::MlsRemoveProposal(g, r) => {
            let gid = mls_gid(*g);
            mls_unit(p.mls_storage(), |m| {
                m.remove_proposal::<MlsGroupId, K>(&gid, &K(*r))
            })
        }
        Op::MlsClearProposals(g) => {
            let gid = mls_gid(*g);
            mls_unit(p.mls_storage(), |m| {
                m.clear_proposal_queue::<MlsGroupId, K>(&gid)
            })
        }
        Op::MlsWriteGlobal(s, k, v) => mls_write_global(p.mls_storage(), *s, *k, *v),
        Op::MlsDeleteGlobal(s, k) => mls_delete_global(p.mls_storage(), *s, *k),
        Op::MlsWriteEpochKeys(g, e) => {
            let gid = mls_gid(*g);
            mls_unit(p.mls_storage(), |m| {
                m.write_encryption_epoch_key_pairs::<MlsGroupId, K, V>(&gid, &K(*e), 0, &[V(*e)])
            })
        }
        Op::MlsDeleteEpochKeys(g, e) => {
            let gid = mls_gid(*g);
            mls_unit(p.mls_storage(), |m| {
                m.delete_encryption_epoch_key_pairs::<MlsGroupId, K>(&gid, &K(*e), 0)
            })
        }
        Op::BeginCommit(ops) => {
            let mut inner_outcomes = Vec::with_capacity(ops.len());
            let result: Result<(), StorageError> = p.with_transaction(|inner| {
                for op in ops {
                    inner_outcomes.push(apply(inner, op));
                }
                Ok(())
            });
            Outcome::Nested(inner_outcomes, result.map_err(|e| kind(&e)))
        }
        Op::BeginRollback(ops) => {
            let mut inner_outcomes = Vec::with_capacity(ops.len());
            let _discarded = p.with_transaction(|inner| -> Result<(), StorageError> {
                for op in ops {
                    inner_outcomes.push(apply(inner, op));
                }
                Err(StorageError::Backend("rollback".into()))
            });
            Outcome::Nested(inner_outcomes, Ok(()))
        }
        Op::SavepointOk(g, ops) => {
            let mut inner_outcomes = Vec::with_capacity(ops.len());
            let result: Result<(), StorageError> =
                p.with_group_savepoint(&gid(*g), "sp-model", |inner| {
                    for op in ops {
                        inner_outcomes.push(apply(inner, op));
                    }
                    Ok(())
                });
            Outcome::Nested(inner_outcomes, result.map_err(|e| kind(&e)))
        }
        Op::SavepointErr(g, ops) => {
            let mut inner_outcomes = Vec::with_capacity(ops.len());
            let _discarded =
                p.with_group_savepoint(&gid(*g), "sp-model", |inner| -> Result<(), StorageError> {
                    for op in ops {
                        inner_outcomes.push(apply(inner, op));
                    }
                    Err(StorageError::Backend("savepoint-err".into()))
                });
            Outcome::Nested(inner_outcomes, Ok(()))
        }
        Op::ReadSnapshot(ops) => {
            let mut inner_outcomes = Vec::with_capacity(ops.len());
            let result: Result<(), StorageError> = p.with_read_snapshot(|inner| {
                for op in ops {
                    inner_outcomes.push(apply(inner, op));
                }
                Ok(())
            });
            Outcome::Nested(inner_outcomes, result.map_err(|e| kind(&e)))
        }
        Op::SnapshotCreate(g, name) => {
            unit(p.create_group_snapshot(&gid(*g), &format!("snap-{name}")))
        }
        Op::SnapshotRollback(g, name) => {
            unit(p.rollback_group_to_snapshot(&gid(*g), &format!("snap-{name}")))
        }
        Op::SnapshotRelease(g, name) => {
            unit(p.release_group_snapshot(&gid(*g), &format!("snap-{name}")))
        }
        Op::StateSnapshotCreate(g, name) => {
            unit(p.create_group_state_snapshot(&gid(*g), &format!("ss-{name}")))
        }
        Op::StateSnapshotRollback(g, name) => {
            unit(p.rollback_group_state_to_snapshot(&gid(*g), &format!("ss-{name}")))
        }
        Op::CheckpointCreate(g, name) => unit(p.create_group_state_checkpoint(
            &gid(*g),
            &GroupStateCheckpointRef {
                id: format!("cp-{name}"),
                resulting_epoch: EpochId(u64::from(*name)),
            },
        )),
        Op::CheckpointRestore(g, name) => {
            unit(p.restore_group_state_checkpoint(&gid(*g), &format!("cp-{name}")))
        }
        Op::CheckpointRelease(g, name) => {
            unit(p.release_group_state_checkpoint(&gid(*g), &format!("cp-{name}")))
        }
    }
}

/// Everything observable, as one comparable structure. Records serialize
/// through `Debug` — both backends return the same `cgka_types` values, so a
/// content diff is a real divergence.
#[derive(Debug, PartialEq)]
struct Dump {
    groups: Result<Vec<GroupId>, String>,
    group_records: Result<Vec<Group>, String>,
    welcomes: Result<Vec<PendingWelcome>, String>,
    messages: Vec<(u8, Result<Vec<MessageRecord>, String>)>,
    snapshots: Vec<(u8, Result<Vec<String>, String>)>,
    checkpoints: Vec<(u8, Result<Vec<GroupStateCheckpointRef>, String>)>,
    canonical: BTreeMap<String, Result<String, String>>,
}

fn dump<S: StorageProvider>(p: &S) -> Dump
where
    S::Mls: MlsStorage<CURRENT_VERSION>,
{
    let mut canonical = BTreeMap::new();
    let mut put = |name: &str, value: Result<String, String>| {
        canonical.insert(name.to_owned(), value);
    };
    let dbg = |value: Result<Vec<String>, String>| value.map(|v| format!("{v:?}"));

    for g in 0..GROUPS {
        let id = gid(g);
        put(
            &format!("admission-{g}"),
            p.invitation_admission(&id)
                .map(|v| format!("{v:?}"))
                .map_err(|e| kind(&e)),
        );
        put(
            &format!("leave-{g}"),
            p.leave_request(&id)
                .map(|v| format!("{v:?}"))
                .map_err(|e| kind(&e)),
        );
        put(
            &format!("disband-req-{g}"),
            p.disband_request(&id)
                .map(|v| format!("{v:?}"))
                .map_err(|e| kind(&e)),
        );
        put(
            &format!("disband-cand-{g}"),
            p.list_disband_candidates(&id)
                .map(|v| format!("{v:?}"))
                .map_err(|e| kind(&e)),
        );
        put(
            &format!("tombstone-{g}"),
            p.disband_tombstone(&id)
                .map(|v| format!("{v:?}"))
                .map_err(|e| kind(&e)),
        );
        put(
            &format!("policy-{g}"),
            p.convergence_policy(&id)
                .map(|v| format!("{v:?}"))
                .map_err(|e| kind(&e)),
        );
        put(
            &format!("marker-{g}"),
            p.validated_tree_marker(&id)
                .map(|v| format!("{v:?}"))
                .map_err(|e| kind(&e)),
        );
        put(
            &format!("conv-pass-{g}"),
            p.convergence_pass(&id)
                .map(|v| format!("{v:?}"))
                .map_err(|e| kind(&e)),
        );
        put(
            &format!("defgen-{g}"),
            p.deferred_peel_generation(&id)
                .map(|v| format!("{v:?}"))
                .map_err(|e| kind(&e)),
        );
        put(
            &format!("group-maint-{g}"),
            maintenance(p)
                .group_maintenance(&id)
                .map(|v| format!("{v:?}"))
                .map_err(|e| kind(&e)),
        );
        put(
            &format!("queued-{g}"),
            p.list_queued_outbound_intents(&id)
                .map(|v| format!("{v:?}"))
                .map_err(|e| kind(&e)),
        );
        put(
            &format!("own-intents-{g}"),
            p.list_own_commit_intents(Some(&id))
                .map(|v| format!("{v:?}"))
                .map_err(|e| kind(&e)),
        );
        put(
            &format!("fanouts-group-{g}"),
            p.list_outbound_fanouts_for_group(&id)
                .map(|v| format!("{v:?}"))
                .map_err(|e| kind(&e)),
        );
        put(
            &format!("forgotten-{g}"),
            p.is_group_forgotten(&id)
                .map(|v| format!("{v:?}"))
                .map_err(|e| kind(&e)),
        );
        put(
            &format!("expectation-{g}"),
            p.device_group_welcome_expectation(&id)
                .map(|v| format!("{v:?}"))
                .map_err(|e| kind(&e)),
        );
        put(
            &format!("roster-{g}"),
            p.committed_roster(&id)
                .map(|v| format!("{v:?}"))
                .map_err(|e| kind(&e)),
        );
        let mls = mls_gid(g);
        let m = p.mls_storage();
        for (slot, name) in [
            (0u8, "tree"),
            (1, "context"),
            (2, "state"),
            (3, "interim"),
            (4, "confirmation"),
            (5, "msgsec"),
            (6, "epochsec"),
            (7, "joinconf"),
            (8, "leafidx"),
            (9, "resumption"),
        ] {
            let value: Result<Option<V>, _> = match slot {
                0 => m.tree::<MlsGroupId, V>(&mls),
                1 => m.group_context::<MlsGroupId, V>(&mls),
                2 => m.group_state::<V, MlsGroupId>(&mls),
                3 => m.interim_transcript_hash::<MlsGroupId, V>(&mls),
                4 => m.confirmation_tag::<MlsGroupId, V>(&mls),
                5 => m.message_secrets::<MlsGroupId, V>(&mls),
                6 => m.group_epoch_secrets::<MlsGroupId, V>(&mls),
                7 => m.mls_group_join_config::<MlsGroupId, V>(&mls),
                8 => m.own_leaf_index::<MlsGroupId, V>(&mls),
                _ => m.resumption_psk_store::<MlsGroupId, V>(&mls),
            };
            put(
                &format!("mls-{name}-{g}"),
                value
                    .map(|v| format!("{v:?}"))
                    .map_err(|_| "mls-err".into()),
            );
        }
        put(
            &format!("mls-leaves-{g}"),
            (m.own_leaf_nodes::<MlsGroupId, V>(&mls))
                .map(|v| format!("{v:?}"))
                .map_err(|_| "mls-err".into()),
        );
        put(
            &format!("mls-proposals-{g}"),
            (m.queued_proposals::<MlsGroupId, K, V>(&mls))
                .map(|v| format!("{v:?}"))
                .map_err(|_| "mls-err".into()),
        );
        for e in 0..8u8 {
            put(
                &format!("mls-epochkeys-{g}-{e}"),
                (m.encryption_epoch_key_pairs::<MlsGroupId, K, V>(&mls, &K(e), 0))
                    .map(|v| format!("{v:?}"))
                    .map_err(|_| "mls-err".into()),
            );
        }
    }

    for m in 0..MESSAGES {
        let id = mid(m);
        put(
            &format!("processed-{m}"),
            p.has_processed_transport_id(&id)
                .map(|v| format!("{v:?}"))
                .map_err(|e| kind(&e)),
        );
        put(
            &format!("dedup-{m}"),
            p.has_ingress_dedup_marker(&id)
                .map(|v| format!("{v:?}"))
                .map_err(|e| kind(&e)),
        );
        put(
            &format!("obligation-{m}"),
            maintenance(p)
                .maintenance_obligation(&id)
                .map(|v| format!("{v:?}"))
                .map_err(|e| kind(&e)),
        );
        put(
            &format!("evolution-{m}"),
            maintenance(p)
                .group_evolution(&id)
                .map(|v| format!("{v:?}"))
                .map_err(|e| kind(&e)),
        );
        put(
            &format!("tfanout-{m}"),
            maintenance(p)
                .transport_fanout(&id)
                .map(|v| format!("{v:?}"))
                .map_err(|e| kind(&e)),
        );
        put(
            &format!("signer-{m}"),
            p.account_device_signer(&member(m))
                .map(|v| format!("{v:?}"))
                .map_err(|e| kind(&e)),
        );
        put(
            &format!("history-{m}"),
            p.account_device_history_key(&member(m))
                .map(|v| format!("{v:?}"))
                .map_err(|e| kind(&e)),
        );
        for k in 0..8u8 {
            let key = K(k);
            let mm = p.mls_storage();
            for (slot, name) in [(0u8, "sig"), (1, "enc"), (2, "kpkg"), (3, "psk")] {
                let value: Result<Option<V>, _> = match slot {
                    0 => mm.signature_key_pair::<K, V>(&key),
                    1 => mm.encryption_key_pair::<V, K>(&key),
                    2 => mm.key_package::<K, V>(&key),
                    _ => mm.psk::<V, K>(&key),
                };
                put(
                    &format!("mls-{name}-{m}-{k}"),
                    value
                        .map(|v| format!("{v:?}"))
                        .map_err(|_| "mls-err".into()),
                );
            }
        }
    }

    for f in FEATURES {
        put(
            &format!("feature-{f}"),
            p.feature_requirement(&Feature(f))
                .map(|v| format!("{v:?}"))
                .map_err(|e| kind(&e)),
        );
    }

    put(
        "routes",
        p.list_transport_group_routes()
            .map(|v| format!("{v:?}"))
            .map_err(|e| kind(&e)),
    );
    put(
        "fanouts",
        p.list_outbound_fanouts()
            .map(|v| format!("{v:?}"))
            .map_err(|e| kind(&e)),
    );
    put(
        "own-intents-all",
        p.list_own_commit_intents(None)
            .map(|v| format!("{v:?}"))
            .map_err(|e| kind(&e)),
    );
    put(
        "tombstones",
        p.list_disband_tombstones()
            .map(|v| format!("{v:?}"))
            .map_err(|e| kind(&e)),
    );
    put(
        "conv-passes",
        p.list_convergence_passes()
            .map(|v| format!("{v:?}"))
            .map_err(|e| kind(&e)),
    );
    put(
        "lifecycle",
        maintenance(p)
            .key_package_lifecycle()
            .map(|v| format!("{v:?}"))
            .map_err(|e| kind(&e)),
    );
    put(
        "obligations",
        maintenance(p)
            .list_maintenance_obligations()
            .map(|v| format!("{v:?}"))
            .map_err(|e| kind(&e)),
    );
    put(
        "evolutions",
        maintenance(p)
            .list_group_evolutions()
            .map(|v| format!("{v:?}"))
            .map_err(|e| kind(&e)),
    );
    put(
        "tfanouts",
        maintenance(p)
            .list_transport_fanouts()
            .map(|v| format!("{v:?}"))
            .map_err(|e| kind(&e)),
    );
    put(
        "periodic",
        maintenance(p)
            .periodic_maintenance_policy()
            .map(|v| format!("{v:?}"))
            .map_err(|e| kind(&e)),
    );
    put(
        "pending",
        dbg(p
            .list_pending_application_events()
            .map(|events| events.iter().map(|e| format!("{e:?}")).collect())
            .map_err(|e| kind(&e))),
    );
    put(
        "authority",
        p.pending_application_authority_batch(None, 64)
            .map(|v| format!("{v:?}"))
            .map_err(|e| kind(&e)),
    );
    put(
        "deferred-meta",
        (0..GROUPS)
            .map(|g| p.list_deferred_message_metadata(&gid(g)))
            .collect::<Result<Vec<_>, _>>()
            .map(|v| format!("{v:?}"))
            .map_err(|e| kind(&e)),
    );
    put(
        "high-water",
        p.pending_ref_high_water()
            .map(|v| format!("{v:?}"))
            .map_err(|e| kind(&e)),
    );
    // The counter is backend-local; only presence is comparable.
    put(
        "mls-generation",
        Ok(format!("{:?}", p.mls_write_generation().is_some())),
    );
    put(
        "snap-prefixes",
        p.list_snapshots_with_prefixes(&["snap", "ss"])
            .map(|v| format!("{v:?}"))
            .map_err(|e| kind(&e)),
    );
    put(
        "expectations",
        p.device_group_welcome_expectations()
            .map(|v| format!("{v:?}"))
            .map_err(|e| kind(&e)),
    );
    put(
        "kp-bundles",
        p.stored_key_package_bundles()
            .map(|v| {
                format!(
                    "{:?}",
                    v.iter().map(|b| b.storage_key.clone()).collect::<Vec<_>>()
                )
            })
            .map_err(|e| kind(&e)),
    );

    Dump {
        groups: p.list_groups().map_err(|e| kind(&e)),
        group_records: p.list_group_records().map_err(|e| kind(&e)),
        welcomes: p.list_welcomes().map_err(|e| kind(&e)),
        messages: (0..GROUPS)
            .map(|g| {
                (
                    g,
                    p.list_messages(&gid(g), EpochId(0)).map_err(|e| kind(&e)),
                )
            })
            .collect(),
        snapshots: (0..GROUPS)
            .map(|g| (g, p.list_group_snapshots(&gid(g)).map_err(|e| kind(&e))))
            .collect(),
        checkpoints: (0..GROUPS)
            .map(|g| {
                (
                    g,
                    p.list_group_state_checkpoints(&gid(g))
                        .map_err(|e| kind(&e)),
                )
            })
            .collect(),
        canonical,
    }
}

proptest! {
    #![proptest_config(ProptestConfig {
        cases: std::env::var("PROPTEST_CASES")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(256),
        ..ProptestConfig::default()
    })]

    #[test]
    fn kv_matches_sqlite(ops in prop::collection::vec(op_strategy(), 8..64)) {
        let host = Arc::new(TestHost::default());
        let kv = KvStorageProvider::open(Vec::new(), host.clone()).expect("kv open");
        let sqlite = SqliteAccountStorage::in_memory_for_test().expect("sqlite open");

        for (i, op) in ops.iter().enumerate() {
            let kv_out = apply(&kv, op);
            let sql_out = apply(&sqlite, op);
            prop_assert_eq!(
                &kv_out, &sql_out,
                "op {} diverged: {:?}",
                i, op,
            );
        }

        let kv_dump = dump(&kv);
        let sql_dump = dump(&sqlite);
        for (key, kv_v) in &kv_dump.canonical {
            let sql_v = sql_dump.canonical.get(key);
            if Some(kv_v) != sql_v {
                eprintln!("DIFF {key}: kv={kv_v:?} sql={sql_v:?}");
            }
        }
        prop_assert_eq!(kv_dump, sql_dump, "final observable dump diverged");

        assert_message_index_held(&host);
    }
}

/// Every stored `Message` row must be exactly reflected by the two index
/// tables — checked on the durable rows at the end of each case.
fn assert_message_index_held(host: &TestHost) {
    let kv_rows: Vec<(Vec<u8>, Vec<u8>)> = host
        .rows
        .lock()
        .expect("host rows")
        .iter()
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect();
    assert_eq!(
        mk_core::testing::check_message_index(&kv_rows),
        Ok(()),
        "message index drifted from the stored message rows"
    );
}
