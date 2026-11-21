//! Shared engine fixtures, adapted from cgka-engine's own Tier-2 tests
//! (`tests/group_creation.rs`, `tests/ingest.rs`, `tests/account_devices.rs`
//! and `tests/account_devices/{device_roster,device_tag,tagged_siblings}.rs`,
//! `tests/invite_leave.rs`, `tests/support/mod.rs`) against MDK v0.18.0.
//!
//! - A `Device` is one `Engine<SqliteAccountStorage>` on its own database.
//!   Devices built from one `account` label share the secp256k1 account
//!   identity (and identity-proof signer) and differ only in their MLS
//!   signature keys, like two devices of one Nostr account.
//! - `MockPeeler` is the pass-through transport peeler: wrap copies
//!   `EncryptedPayload.ciphertext` into `TransportMessage.payload`, peel hands
//!   the bytes back. Tests deliver `TransportMessage`s explicitly, so ordering
//!   is fully controlled.
//! - Every group here is current-profile (`ProtocolProfile::Current`), so
//!   `create_group` returns `SendResult::FoundingGroupCreated` (canonical
//!   immediately, no `confirm_published`) and later commits are
//!   `SendResult::GroupEvolution { msg, pending, welcomes }` needing
//!   `confirm_published`.

#![allow(dead_code)]

pub mod epoch_sealed_peeler;
pub mod wnd;

use async_trait::async_trait;
use cgka_engine::account_identity_proof::{
    AccountIdentityProofRequest, AccountIdentityProofSigner,
};
use cgka_engine::canonicalization::CanonicalizationResult;
use cgka_engine::feature_registry::FeatureRegistry;
use cgka_engine::private_key_packages::SiblingWelcome;
use cgka_engine::provider::EngineOpenMlsProvider;
use cgka_engine::{
    ConvergenceClock, Engine, EngineBuilder, ManualConvergenceClock, OwedSiblingWelcome,
};
use cgka_traits::EngineError;
use cgka_traits::agent_text_stream::{
    AGENT_TEXT_STREAM_QUIC_COMPONENT_ID, AGENT_TEXT_STREAM_QUIC_RECEIVE_CAPABILITY,
    AGENT_TEXT_STREAM_QUIC_RECEIVE_FEATURE,
};
use cgka_traits::app_components::{
    ACCOUNT_DEVICES_COMPONENT_ID, AppComponentData, AppComponentSet, DEVICE_ROSTER_COMPONENT_ID,
    DeviceEntryV1, DeviceRosterOperationV1, DeviceRosterUpdateV1, DeviceRosterV1, DeviceSlotId,
    GROUP_ENCRYPTED_MEDIA_V2_COMPONENT_ID, NOSTR_ROUTING_COMPONENT_ID, NostrRoutingV1,
    decode_device_roster_v1, default_group_components, encode_device_roster_update_v1,
    encode_device_roster_v1, encode_nostr_routing_v1,
};
use cgka_traits::app_event::{MARMOT_APP_EVENT_KIND_CHAT, MarmotAppEvent};
use cgka_traits::capabilities::{Capability, CapabilityRequirement, Feature, RequirementLevel};
use cgka_traits::engine::{
    CgkaEngine, CreateGroupRequest, GroupEvent, KeyPackage, SendIntent, SendResult,
    SiblingKeyPackage,
};
use cgka_traits::error::PeelerError;
use cgka_traits::group::ProtocolProfile;
use cgka_traits::group_context::GroupContextSnapshot;
use cgka_traits::ingest::{IngestOutcome, PeeledContent, PeeledMessage};
use cgka_traits::maintenance::KeyPackageLifecycleState;
use cgka_traits::peeler::TransportPeeler;
use cgka_traits::storage::{DeviceGroupWelcomeExpectation, MaintenanceStorage, StorageProvider};
use cgka_traits::transport::{
    EncryptedPayload, Timestamp, TransportEnvelope, TransportMessage, TransportSource,
};
use cgka_traits::types::{GroupId, MemberId, MessageId};
use cgka_traits::welcome::SiblingWelcomeJoin;
use k256::schnorr::{SigningKey, signature::hazmat::PrehashSigner};
use openmls::group::MlsGroup;
use openmls_rust_crypto::RustCrypto;
use openmls_traits::OpenMlsProvider as _;
use sha2::{Digest, Sha256};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use storage_sqlite::SqliteAccountStorage;

// Identity

pub fn signing_key(seed: &[u8]) -> SigningKey {
    let mut counter = 0u64;
    loop {
        let mut material = [0u8; 32];
        let mut hasher = Sha256::new();
        hasher.update(b"cgka-engine-test-identity-v1");
        hasher.update(seed);
        hasher.update(counter.to_be_bytes());
        material.copy_from_slice(&hasher.finalize());
        if let Ok(sk) = SigningKey::from_slice(&material) {
            return sk;
        }
        counter += 1;
    }
}

/// Deterministic, spec-valid x-only secp256k1 account identity from a label.
pub fn pad32(name: &[u8]) -> Vec<u8> {
    signing_key(name).verifying_key().to_bytes().to_vec()
}

pub fn proof_signer(seed: &[u8]) -> Arc<dyn AccountIdentityProofSigner> {
    Arc::new(TestAccountIdentityProofSigner(signing_key(seed)))
}

struct TestAccountIdentityProofSigner(SigningKey);

impl AccountIdentityProofSigner for TestAccountIdentityProofSigner {
    fn sign_account_identity_proof(
        &self,
        request: &AccountIdentityProofRequest,
    ) -> Result<[u8; 64], String> {
        if self.0.verifying_key().to_bytes().as_slice() != request.account_identity.as_slice() {
            return Err("request account identity does not match test key".into());
        }
        let signature = self
            .0
            .sign_prehash(&request.proof_event_id()?)
            .map_err(|e| e.to_string())?;
        Ok(signature.to_bytes())
    }
}

/// The secp256k1 secret behind `pad32(name)`, when a test needs the raw key.
pub fn account_secret_key(name: &[u8]) -> [u8; 32] {
    signing_key(name).to_bytes().into()
}

// Transport peeler

pub fn hash_id(bytes: &[u8]) -> MessageId {
    MessageId::new(Sha256::digest(bytes).to_vec())
}

/// Pass-through peeler. Every Welcome gets its own id, as the Nostr adapter's
/// gift wraps do: one founding Welcome goes to several devices of one account.
#[derive(Default)]
pub struct MockPeeler(AtomicU64);

#[async_trait]
impl TransportPeeler for MockPeeler {
    async fn wrap_group_recovery(
        &self,
        _message: &cgka_traits::GroupRecoveryMessage,
        _recipient: &MemberId,
    ) -> Result<TransportMessage, PeelerError> {
        Err(PeelerError::Malformed(
            "pass-through fixture has no authenticated account channel".into(),
        ))
    }

    async fn peel_group_message(
        &self,
        msg: &cgka_traits::peeler::PreparedGroupMessage<'_>,
        _ctx: &GroupContextSnapshot,
    ) -> Result<PeeledMessage, PeelerError> {
        Ok(PeeledMessage {
            id: msg.message.id.clone(),
            group_id: None,
            sender: None,
            content: PeeledContent::MlsMessage {
                bytes: msg.message.payload.clone(),
            },
            origin: msg.message.clone(),
        })
    }

    async fn peel_welcome(&self, msg: &TransportMessage) -> Result<PeeledMessage, PeelerError> {
        Ok(PeeledMessage {
            id: msg.id.clone(),
            group_id: None,
            sender: None,
            content: PeeledContent::Welcome {
                created_at: None,
                bytes: msg.payload.clone(),
                key_package_event_id: None,
            },
            origin: msg.clone(),
        })
    }

    async fn wrap_group_message(
        &self,
        payload: &EncryptedPayload,
        _ctx: &GroupContextSnapshot,
    ) -> Result<TransportMessage, PeelerError> {
        Ok(TransportMessage {
            id: hash_id(&payload.ciphertext),
            payload: payload.ciphertext.clone(),
            timestamp: Timestamp(0),
            causal_deps: vec![],
            source: TransportSource("mock".into()),
            envelope: TransportEnvelope::GroupMessage {
                transport_group_id: vec![],
            },
        })
    }

    async fn wrap_welcome(
        &self,
        payload: &EncryptedPayload,
        recipient: &MemberId,
    ) -> Result<TransportMessage, PeelerError> {
        let n = self.0.fetch_add(1, Ordering::Relaxed);
        Ok(TransportMessage {
            id: hash_id(&[payload.ciphertext.as_slice(), &n.to_be_bytes()].concat()),
            payload: payload.ciphertext.clone(),
            timestamp: Timestamp(0),
            causal_deps: vec![],
            source: TransportSource("mock".into()),
            envelope: TransportEnvelope::Welcome {
                recipient: recipient.clone(),
            },
        })
    }
}

// Devices

pub const ACCOUNT_DEVICES: Feature = Feature("account-devices");
pub const SELF_REMOVE: Feature = Feature("self-remove");

pub fn registry() -> FeatureRegistry {
    let mut r = FeatureRegistry::new();
    r.register(
        SELF_REMOVE,
        CapabilityRequirement {
            requires: Capability::Proposal(10), // MIP-03 SelfRemove
            level: RequirementLevel::Required,
            description: "self-remove",
        },
    );
    r.register(
        ACCOUNT_DEVICES,
        CapabilityRequirement {
            requires: Capability::AppComponent(ACCOUNT_DEVICES_COMPONENT_ID),
            level: RequirementLevel::Optional,
            description: "account-devices",
        },
    );
    r
}

/// The supported-component set mk-core advertises
/// (`crates/mk-core/src/session.rs::supported_components`): the engine
/// defaults plus Nostr routing (0x8004), account-devices (0x800d), agent
/// text stream QUIC (0x8006, receive role) and encrypted media v2 (0x800b).
pub fn mkcore_components() -> Vec<u16> {
    let mut components = default_group_components();
    components.insert(NOSTR_ROUTING_COMPONENT_ID);
    components.insert(ACCOUNT_DEVICES_COMPONENT_ID);
    components.insert(AGENT_TEXT_STREAM_QUIC_COMPONENT_ID);
    components.insert(GROUP_ENCRYPTED_MEDIA_V2_COMPONENT_ID);
    components.into_iter().collect()
}

/// The feature registry mk-core advertises: this spike's registry plus the
/// agent text stream QUIC receive role as Optional (extension 0xf2d1).
pub fn mkcore_registry() -> FeatureRegistry {
    let mut r = registry();
    r.register(
        AGENT_TEXT_STREAM_QUIC_RECEIVE_FEATURE,
        CapabilityRequirement {
            requires: AGENT_TEXT_STREAM_QUIC_RECEIVE_CAPABILITY,
            level: RequirementLevel::Optional,
            description: "receive QUIC-backed agent text stream previews",
        },
    );
    r
}

pub struct Device {
    pub engine: Engine<SqliteAccountStorage>,
    pub storage: SqliteAccountStorage,
}

/// A current-profile device of `account`. Devices built from one label share
/// the account identity and differ in their MLS signature keys.
pub fn device(account: &[u8], advertises_account_devices: bool) -> Device {
    let extra: &[u16] = if advertises_account_devices {
        &[ACCOUNT_DEVICES_COMPONENT_ID]
    } else {
        &[]
    };
    device_advertising(account, extra, Box::<MockPeeler>::default())
}

/// A device of `account` that joins routed chats but no device group.
pub fn chat_device(account: &[u8]) -> Device {
    device_advertising(
        account,
        &[ACCOUNT_DEVICES_COMPONENT_ID, NOSTR_ROUTING_COMPONENT_ID],
        Box::<MockPeeler>::default(),
    )
}

/// A device of `account` that advertises the device roster beside
/// account-devices and Nostr routing (test-policy builds only).
pub fn roster_device(account: &[u8]) -> Device {
    roster_device_on(SqliteAccountStorage::in_memory_for_test().unwrap(), account)
}

pub fn roster_device_on(storage: SqliteAccountStorage, account: &[u8]) -> Device {
    let mut supported: Vec<_> = default_group_components().into_iter().collect();
    supported.extend([
        ACCOUNT_DEVICES_COMPONENT_ID,
        DEVICE_ROSTER_COMPONENT_ID,
        NOSTR_ROUTING_COMPONENT_ID,
    ]);
    let engine = EngineBuilder::new(storage.clone())
        .identity(pad32(account))
        .account_identity_proof_signer(proof_signer(account))
        .protocol_profile(ProtocolProfile::Current)
        .feature_registry(registry())
        .supported_app_components(supported)
        .peeler(Box::<MockPeeler>::default())
        .build()
        .unwrap();
    Device { engine, storage }
}

/// [`device`] with `peeler` in place of the [`MockPeeler`].
pub fn device_with_peeler(
    account: &[u8],
    advertises_account_devices: bool,
    peeler: Box<dyn TransportPeeler>,
) -> Device {
    let extra: &[u16] = if advertises_account_devices {
        &[ACCOUNT_DEVICES_COMPONENT_ID]
    } else {
        &[]
    };
    device_advertising(account, extra, peeler)
}

/// [`device`] advertising `extra` components beside the defaults.
pub fn device_advertising(
    account: &[u8],
    extra: &[u16],
    peeler: Box<dyn TransportPeeler>,
) -> Device {
    let storage = SqliteAccountStorage::in_memory_for_test().unwrap();
    device_on(storage, account, extra, peeler, None)
}

/// A device over `storage`, as a reopened session builds it. The caller
/// hydrates its groups.
pub fn device_on_storage(
    storage: SqliteAccountStorage,
    account: &[u8],
    advertises: bool,
) -> Device {
    device_on_storage_with_peeler(storage, account, advertises, Box::<MockPeeler>::default())
}

/// [`device_on_storage`] with `peeler` in place of the [`MockPeeler`].
pub fn device_on_storage_with_peeler(
    storage: SqliteAccountStorage,
    account: &[u8],
    advertises: bool,
    peeler: Box<dyn TransportPeeler>,
) -> Device {
    let extra: &[u16] = if advertises {
        &[ACCOUNT_DEVICES_COMPONENT_ID]
    } else {
        &[]
    };
    device_on(storage, account, extra, peeler, None)
}

/// A device on the epoch-faithful sealed peeler (production epoch visibility).
pub fn sealed_device(account: &[u8], advertises_account_devices: bool) -> Device {
    let extra: &[u16] = if advertises_account_devices {
        &[ACCOUNT_DEVICES_COMPONENT_ID]
    } else {
        &[]
    };
    device_advertising(
        account,
        extra,
        Box::new(epoch_sealed_peeler::EpochSealedPeeler),
    )
}

/// A sealed-peeler device on a manual convergence clock.
pub fn sealed_clocked_device(account: &[u8], clock: &ManualConvergenceClock) -> Device {
    device_on(
        SqliteAccountStorage::in_memory_for_test().unwrap(),
        account,
        &[ACCOUNT_DEVICES_COMPONENT_ID],
        Box::new(epoch_sealed_peeler::EpochSealedPeeler),
        Some(clock),
    )
}

/// A device on the real Nostr peeler (`transport-nostr-peeler`) whose
/// welcome signer is the account's Nostr key — the same `signing_key` that
/// backs `pad32`/`proof_signer`, so the account identity matches.
pub fn nostr_keys(account: &[u8]) -> nostr::key::Keys {
    nostr::key::Keys::new(
        nostr::key::SecretKey::from_slice(&account_secret_key(account)).unwrap(),
    )
}

pub fn nostr_peeler_device(account: &[u8], extra: &[u16]) -> Device {
    let peeler =
        transport_nostr_peeler::NostrMlsPeeler::new().with_welcome_signer(nostr_keys(account));
    device_advertising(account, extra, Box::new(peeler))
}

/// A device on the real Nostr peeler advertising the mk-core component set
/// and registry — what an m6 engine device looks like to `wn`.
pub fn mkcore_device(account: &[u8]) -> Device {
    let storage = SqliteAccountStorage::in_memory_for_test().unwrap();
    let peeler =
        transport_nostr_peeler::NostrMlsPeeler::new().with_welcome_signer(nostr_keys(account));
    let engine = EngineBuilder::new(storage.clone())
        .identity(pad32(account))
        .account_identity_proof_signer(proof_signer(account))
        .protocol_profile(ProtocolProfile::Current)
        .feature_registry(mkcore_registry())
        .supported_app_components(mkcore_components())
        .peeler(Box::new(peeler))
        .build()
        .unwrap();
    Device { engine, storage }
}

pub fn manual_convergence_clock() -> ManualConvergenceClock {
    let wall_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis()
        .try_into()
        .unwrap();
    ManualConvergenceClock::new(1_000, wall_ms)
}

/// A device of `account` on a manual convergence clock.
pub fn clocked_device(account: &[u8], clock: &ManualConvergenceClock) -> Device {
    device_on(
        SqliteAccountStorage::in_memory_for_test().unwrap(),
        account,
        &[ACCOUNT_DEVICES_COMPONENT_ID],
        Box::<MockPeeler>::default(),
        Some(clock),
    )
}

fn device_on(
    storage: SqliteAccountStorage,
    account: &[u8],
    extra: &[u16],
    peeler: Box<dyn TransportPeeler>,
    clock: Option<&ManualConvergenceClock>,
) -> Device {
    let mut supported: Vec<_> = default_group_components().into_iter().collect();
    supported.extend(extra.iter().copied());
    let mut builder = EngineBuilder::new(storage.clone())
        .identity(pad32(account))
        .account_identity_proof_signer(proof_signer(account))
        .protocol_profile(ProtocolProfile::Current)
        .feature_registry(registry())
        .supported_app_components(supported)
        .peeler(peeler);
    if let Some(clock) = clock {
        builder = builder.convergence_clock(Arc::new(clock.clone()));
    }
    let engine = builder.build().unwrap();
    Device { engine, storage }
}

// Group helpers

pub fn route_to_group(msg: &TransportMessage, gid: &GroupId) -> TransportMessage {
    TransportMessage {
        envelope: TransportEnvelope::GroupMessage {
            transport_group_id: gid.as_slice().to_vec(),
        },
        ..msg.clone()
    }
}

/// Publish one of `sender`'s commits and return it routed to the group.
pub async fn send_commit(
    sender: &mut Device,
    intent: SendIntent,
    gid: &GroupId,
) -> TransportMessage {
    let SendResult::GroupEvolution { msg, pending, .. } = sender.engine.send(intent).await.unwrap()
    else {
        panic!("expected GroupEvolution")
    };
    sender.engine.confirm_published(pending).await.unwrap();
    route_to_group(&msg, gid)
}

/// One `send` returning the full `SendResult` (for result-shape printing).
pub async fn send_intent(
    sender: &mut Device,
    intent: SendIntent,
) -> Result<SendResult, EngineError> {
    sender.engine.send(intent).await
}

/// If `result` is a `GroupEvolution`, confirm it and return the routed commit.
pub async fn confirm_evolution(
    sender: &mut Device,
    result: SendResult,
    gid: &GroupId,
) -> Option<TransportMessage> {
    match result {
        SendResult::GroupEvolution { msg, pending, .. } => {
            sender.engine.confirm_published(pending).await.unwrap();
            Some(route_to_group(&msg, gid))
        }
        _ => None,
    }
}

/// `creator` founds a group with `members` through the engine alone.
/// Current profile → `FoundingGroupCreated` (no confirm). Returns founding
/// Welcomes, one per KeyPackage in order.
pub async fn create(
    creator: &mut Device,
    members: Vec<KeyPackage>,
    initial_admins: Vec<MemberId>,
) -> Result<(GroupId, Vec<TransportMessage>), EngineError> {
    let (gid, created) = creator
        .engine
        .create_group(CreateGroupRequest {
            name: "spike".into(),
            description: String::new(),
            members,
            required_features: vec![],
            app_components: vec![],
            initial_admins,
        })
        .await?;
    let SendResult::FoundingGroupCreated { welcomes } = created else {
        panic!("current-profile founding group, got {created:?}")
    };
    Ok((gid, welcomes))
}

/// `alice` founds a group with `members` and each of `joiners` joins from the
/// Welcome at the same index. The group requires 0x800d when `require` is set.
pub async fn found_group(
    alice: &mut Device,
    members: Vec<KeyPackage>,
    require: bool,
    joiners: &mut [&mut Device],
) -> GroupId {
    found_group_with_admins(alice, members, require, vec![], joiners).await
}

pub async fn found_group_with_admins(
    alice: &mut Device,
    mut members: Vec<KeyPackage>,
    require: bool,
    initial_admins: Vec<MemberId>,
    joiners: &mut [&mut Device],
) -> GroupId {
    let mut outsider = None;
    if !require {
        let mut device = device(b"outsider", false);
        members.push(device.engine.fresh_key_package().await.unwrap());
        outsider = Some(device.engine.self_id());
    }
    let (gid, welcomes) = create(alice, members, initial_admins).await.unwrap();
    for (joiner, welcome) in joiners.iter_mut().zip(welcomes) {
        joiner.engine.join_welcome(welcome).await.unwrap();
    }
    if let Some(outsider) = outsider {
        let remove = send_commit(
            alice,
            SendIntent::RemoveMembers {
                group_id: gid.clone(),
                members: vec![outsider],
            },
            &gid,
        )
        .await;
        for joiner in joiners.iter_mut() {
            assert_accepted(joiner, &remove, &gid).await;
        }
    }
    assert_eq!(requires_account_devices(&alice.engine, &gid), require);
    gid
}

pub fn required(engine: &Engine<SqliteAccountStorage>, gid: &GroupId) -> AppComponentSet {
    engine
        .group_record(gid)
        .unwrap()
        .required_capabilities
        .app_components
}

pub fn requires_account_devices(engine: &Engine<SqliteAccountStorage>, gid: &GroupId) -> bool {
    required(engine, gid).contains(ACCOUNT_DEVICES_COMPONENT_ID)
}

pub fn with_account_devices(mut ids: AppComponentSet) -> AppComponentSet {
    ids.insert(ACCOUNT_DEVICES_COMPONENT_ID);
    ids
}

/// Activate 0x800d in an existing group through the admin's required-list
/// update, delivering the commit to `members`.
pub async fn activate(alice: &mut Device, gid: &GroupId, members: &mut [&mut Device]) {
    let ids = with_account_devices(required(&alice.engine, gid));
    let commit = send_commit(alice, update_required(gid, &ids), gid).await;
    for member in members.iter_mut() {
        assert_accepted(member, &commit, gid).await;
    }
    assert!(requires_account_devices(&alice.engine, gid));
}

pub fn update_required(gid: &GroupId, ids: &AppComponentSet) -> SendIntent {
    SendIntent::UpdateAppComponents {
        group_id: gid.clone(),
        updates: vec![AppComponentData {
            component_id: cgka_traits::app_components::APP_COMPONENTS_COMPONENT_ID,
            data: cgka_traits::app_components::encode_components_list(&ids.ids),
        }],
    }
}

// Delivery / convergence

pub async fn assert_accepted(
    receiver: &mut Device,
    commit: &TransportMessage,
    gid: &GroupId,
) -> CanonicalizationResult {
    assert_accepted_with_clock(receiver, commit, gid, None).await
}

/// Ingest `commit` then settle convergence at a far-future cutoff; asserts the
/// commit is the single accepted edge and the epoch advanced by one.
pub async fn assert_accepted_with_clock(
    receiver: &mut Device,
    commit: &TransportMessage,
    gid: &GroupId,
    clock: Option<&ManualConvergenceClock>,
) -> CanonicalizationResult {
    let before = receiver.engine.epoch(gid).unwrap().0;
    let outcome = receiver.engine.ingest(commit.clone()).await.unwrap();
    println!("    ingest commit outcome: {outcome:?}");
    let cutoff = if let Some(clock) = clock {
        clock.advance_ms(1_000_000);
        ConvergenceClock::now(clock).monotonic_ms
    } else {
        1_000_000
    };
    let result = receiver
        .engine
        .converge_stored_openmls_messages_at(gid, cutoff)
        .unwrap();
    assert_eq!(
        result.accepted_commits,
        vec![hex::encode(hash_id(&commit.payload).as_slice())],
        "dropped: {:?}",
        result.dropped_messages
    );
    assert_eq!(receiver.engine.epoch(gid).unwrap().0, before + 1);
    result
}

pub fn converge_buffered_commit(engine: &mut Engine<SqliteAccountStorage>, group_id: &GroupId) {
    let result = engine
        .converge_stored_openmls_messages_at(group_id, 1_000_000)
        .expect("buffered commit converges");
    println!("    convergence status: {:?}", result.convergence_status);
}

/// Drive the receiver's convergence to whatever outcome results; returns it
/// for printing. Never asserts — for scenarios whose point is the outcome.
pub async fn settle(receiver: &mut Device, gid: &GroupId) -> CanonicalizationResult {
    receiver
        .engine
        .converge_stored_openmls_messages_at(gid, 1_000_000)
        .unwrap()
}

/// Repeat `converge_stored_openmls_messages_at` until the epoch has not moved
/// for three consecutive passes (or `max_passes` is hit); returns the
/// accepted-commit count per pass, so the caller can see any per-pass cap
/// and whether a stalled pass later makes progress again.
pub fn converge_to_fixpoint(
    receiver: &mut Device,
    gid: &GroupId,
    max_passes: usize,
) -> Vec<usize> {
    let mut applied = Vec::new();
    let mut stagnant = 0usize;
    for _ in 0..max_passes {
        let before = receiver.engine.epoch(gid).unwrap().0;
        let result = receiver
            .engine
            .converge_stored_openmls_messages_at(gid, 1_000_000)
            .unwrap();
        applied.push(result.accepted_commits.len());
        if receiver.engine.epoch(gid).unwrap().0 == before {
            stagnant += 1;
            if stagnant >= 3 {
                break;
            }
        } else {
            stagnant = 0;
        }
    }
    applied
}

/// Drive one group the way mk-core's session drives the engine
/// (`crates/mk-core/src/session.rs::drive`): settle stored inputs via
/// `advance_convergence_inputs_until_settled`, then a full
/// `advance_convergence` that drains queued outbound work; when
/// `deferred_peel_cutoff_delay_ms` reports `Some(d)` the injected clock
/// advances by `d` and another advance runs. Loops until the epoch stops
/// moving and nothing is owed. Prints one line per iteration.
pub async fn drive_like_mk_core(
    device: &mut Device,
    gid: &GroupId,
    clock: &ManualConvergenceClock,
) {
    let mut stagnant = 0usize;
    for iteration in 0..32 {
        // Time passes between host ticks: the v1 settlement quiescence is
        // 1 s (`settlement_quiescence_ms`), so step the clock past it.
        clock.advance_ms(2_000);
        let epoch_before = device.engine.epoch(gid).map(|e| e.0).unwrap_or_default();
        let now_ms = ConvergenceClock::now(clock).monotonic_ms;
        let settled = device
            .engine
            .advance_convergence_inputs_until_settled(gid, now_ms)
            .await
            .unwrap();
        let results = device.engine.advance_convergence(gid).await.unwrap();
        for result in &results {
            println!("    drive advance -> SendResult: {result:?}");
            if let SendResult::GroupEvolution { pending, .. } = result {
                device
                    .engine
                    .confirm_published(pending.clone())
                    .await
                    .unwrap();
            }
        }
        let mut peel_drove = false;
        while let Some(d) = device.engine.deferred_peel_cutoff_delay_ms(gid).unwrap() {
            if d == 0 {
                break;
            }
            peel_drove = true;
            clock.advance_ms(d);
            let again = device.engine.advance_convergence(gid).await.unwrap();
            println!(
                "    drive deferred-peel cutoff +{d}ms -> {} SendResult(s)",
                again.len()
            );
        }
        let owes = device.engine.has_pending_convergence_inputs(gid).unwrap()
            || device.engine.has_queued_outbound_intents(gid).unwrap()
            || device.engine.leave_redrive_owed(gid).unwrap()
            || device
                .engine
                .scheduled_self_remove_auto_commit_delay_ms(gid)
                .unwrap()
                == Some(0)
            || device.engine.deferred_peel_cutoff_delay_ms(gid).unwrap() == Some(0);
        let epoch_after = device.engine.epoch(gid).map(|e| e.0).unwrap_or_default();
        println!(
            "    drive iteration {iteration} (clock={now_ms}ms): epoch \
             {epoch_before}->{epoch_after} inputs_settled={settled} \
             send_results={} peel_drove={peel_drove} owes={owes}",
            results.len()
        );
        if epoch_after == epoch_before && settled && results.is_empty() && !peel_drove && !owes {
            break;
        }
        if epoch_after == epoch_before && results.is_empty() && !peel_drove {
            stagnant += 1;
            if stagnant >= 5 {
                println!("    drive: 5 no-progress iterations, stopping");
                break;
            }
        } else {
            stagnant = 0;
        }
    }
}

/// Deliver `msg` to `receiver`, printing the ingest outcome. Returns it.
pub async fn deliver(receiver: &mut Device, msg: &TransportMessage) -> IngestOutcome {
    let outcome = receiver.engine.ingest(msg.clone()).await.unwrap();
    outcome
}

/// Drive the SelfRemove auto-committer on `engine` to its deadline.
pub async fn advance_selfremove_auto_commit(
    engine: &mut Engine<SqliteAccountStorage>,
    group_id: &GroupId,
    clock: &ManualConvergenceClock,
) {
    let due = engine
        .scheduled_self_remove_auto_commit_delay_ms(group_id)
        .unwrap()
        .expect("real scheduler deadline");
    clock.advance_ms(due);
    assert_eq!(
        engine
            .scheduled_self_remove_auto_commit_delay_ms(group_id)
            .unwrap(),
        Some(0)
    );
    let results = engine.advance_convergence(group_id).await.unwrap();
    println!("    advance_convergence results: {results:?}");
}

pub async fn join_outcome(joiner: &mut Device, welcome: &TransportMessage) -> IngestOutcome {
    joiner.engine.ingest(welcome.clone()).await.unwrap()
}

/// `joiner` joins `gid` through `welcome`, and names `author` as its author.
pub async fn assert_joins(
    joiner: &mut Device,
    welcome: &TransportMessage,
    gid: &GroupId,
    author: &Device,
) {
    assert_eq!(
        join_outcome(joiner, welcome).await,
        IngestOutcome::Processed
    );
    let welcomers: Vec<_> = joiner
        .engine
        .drain_events()
        .into_iter()
        .filter_map(|event| match event {
            GroupEvent::GroupJoined {
                group_id, welcomer, ..
            } if &group_id == gid => welcomer,
            _ => None,
        })
        .collect();
    assert_eq!(welcomers, vec![author.engine.self_id()]);
}

pub fn is_admin(device: &Device, gid: &GroupId) -> bool {
    let id = device.engine.self_id();
    device
        .engine
        .admin_pubkeys(gid)
        .unwrap()
        .iter()
        .any(|admin| admin.as_slice() == id.as_slice())
}

pub fn load_group(device: &Device, gid: &GroupId) -> MlsGroup {
    let crypto = RustCrypto::default();
    let provider =
        EngineOpenMlsProvider::<SqliteAccountStorage>::new(&crypto, device.storage.mls_storage());
    MlsGroup::load(
        provider.storage(),
        &openmls::group::GroupId::from_slice(gid.as_slice()),
    )
    .expect("load MLS group")
    .expect("device is a member")
}

/// The signature key of `device`'s leaf, which names it in a same-account
/// Remove / `RemoveOwnAccountDevices`.
pub fn signature_key(device: &Device, gid: &GroupId) -> Vec<u8> {
    load_group(device, gid)
        .own_leaf_node()
        .expect("a current member")
        .signature_key()
        .as_slice()
        .to_vec()
}

/// `device`'s `Member.id` (stable per-leaf signature pubkey) in `gid`.
pub fn member_id(device: &Device, gid: &GroupId) -> MemberId {
    let key = signature_key(device, gid);
    device
        .engine
        .members(gid)
        .unwrap()
        .into_iter()
        .find(|m| m.credential == key)
        .expect("device is a member")
        .id
}

// App messages

pub fn app_payload_for(engine: &Engine<SqliteAccountStorage>, content: &str) -> Vec<u8> {
    MarmotAppEvent::new(
        hex::encode(engine.self_id().as_slice()),
        1_700_000_000,
        MARMOT_APP_EVENT_KIND_CHAT,
        Vec::<Vec<String>>::new(),
        content,
    )
    .encode()
    .expect("app event encodes")
}

pub fn app_content(payload: &[u8]) -> Vec<u8> {
    MarmotAppEvent::decode(payload)
        .expect("app event decodes")
        .content
        .into_bytes()
}

/// `sender` sends one chat app message; returns the routed TransportMessage.
/// Pure app messages need no confirm.
pub async fn send_app(sender: &mut Device, gid: &GroupId, content: &str) -> TransportMessage {
    let sent = sender
        .engine
        .send(SendIntent::AppMessage {
            group_id: gid.clone(),
            payload: app_payload_for(&sender.engine, content),
            expected_epoch: None,
        })
        .await
        .expect("app send");
    match sent {
        SendResult::ApplicationMessage { msg, .. } => route_to_group(&msg, gid),
        other => panic!("expected ApplicationMessage, got {other:?}"),
    }
}

/// The `MessageReceived` chat contents `events` carries, decoded.
pub fn received_contents(events: &[GroupEvent]) -> Vec<Vec<u8>> {
    events
        .iter()
        .filter_map(|event| match event {
            GroupEvent::MessageReceived { payload, .. } => Some(app_content(payload)),
            _ => None,
        })
        .collect()
}

/// Print every event as one `Name { .. }` line.
pub fn print_events(label: &str, events: &[GroupEvent]) {
    for event in events {
        let name = match event {
            GroupEvent::LocalGroupCopyTerminated { voluntary, .. } => {
                format!("LocalGroupCopyTerminated(voluntary={voluntary:?})")
            }
            GroupEvent::LocalGroupCopyRestored { .. } => "LocalGroupCopyRestored".into(),
            GroupEvent::QueuedIntentRefused { reason, .. } => {
                format!("QueuedIntentRefused({reason})")
            }
            GroupEvent::GroupChangeNotDelivered { reason, .. } => {
                format!("GroupChangeNotDelivered({reason})")
            }
            GroupEvent::GroupMemberLeavesRemoved { leaves, .. } => {
                format!("GroupMemberLeavesRemoved(leaves={})", leaves.len())
            }
            GroupEvent::GroupCreated { .. } => "GroupCreated".into(),
            GroupEvent::GroupJoined { welcomer, .. } => {
                let w = welcomer
                    .as_ref()
                    .map(|w| hex::encode(w.as_slice()))
                    .unwrap_or_else(|| "none".into());
                format!("GroupJoined(welcomer={w})")
            }
            GroupEvent::TransportObjectResourceRefused { .. } => {
                "TransportObjectResourceRefused".into()
            }
            GroupEvent::MessageReceived { .. } => "MessageReceived".into(),
            GroupEvent::AppMessageInvalidated { .. } => "AppMessageInvalidated".into(),
            GroupEvent::GroupStateChanged { change, .. } => {
                format!("GroupStateChanged({change:?})")
            }
            GroupEvent::GroupHydrationQuarantined { reason, .. } => {
                format!("GroupHydrationQuarantined({reason:?})")
            }
            GroupEvent::EpochChanged { to, .. } => format!("EpochChanged({})", to.0),
            GroupEvent::CommitRolledBack { .. } => "CommitRolledBack".into(),
            GroupEvent::GroupStateInvalidated { .. } => "GroupStateInvalidated".into(),
            GroupEvent::GroupStateRevalidated { .. } => "GroupStateRevalidated".into(),
            GroupEvent::GroupUnrecoverable { .. } => "GroupUnrecoverable".into(),
            GroupEvent::PendingCommitRecovered {
                recovered_epoch, ..
            } => {
                format!("PendingCommitRecovered(epoch={})", recovered_epoch.0)
            }
            GroupEvent::GroupHydrationRecovered {
                recovered_epoch, ..
            } => {
                format!("GroupHydrationRecovered(epoch={})", recovered_epoch.0)
            }
        };
        println!("    {label}: {name}");
    }
}

// Device roster / device group (test-policy builds)

pub fn slot(n: u8) -> DeviceSlotId {
    [n; 32]
}

pub fn entry(label: &str) -> DeviceEntryV1 {
    DeviceEntryV1 {
        label: label.into(),
        client: "test-client".into(),
        ..DeviceEntryV1::default()
    }
}

/// A roster with one device per `(slot, label)`. Its leaf tag key is not zero.
pub fn roster(devices: &[(u8, &str)]) -> DeviceRosterV1 {
    DeviceRosterV1 {
        leaf_tag_key: [0x33; 32],
        devices: devices
            .iter()
            .map(|(n, label)| (slot(*n), entry(label)))
            .collect(),
        ..DeviceRosterV1::default()
    }
}

fn roster_component(roster: &DeviceRosterV1) -> AppComponentData {
    AppComponentData {
        component_id: DEVICE_ROSTER_COMPONENT_ID,
        data: encode_device_roster_v1(roster).unwrap(),
    }
}

pub fn update_bytes(operations: Vec<DeviceRosterOperationV1>) -> Vec<u8> {
    encode_device_roster_update_v1(&DeviceRosterUpdateV1::new(operations).unwrap()).unwrap()
}

pub fn roster_update(gid: &GroupId, operations: Vec<DeviceRosterOperationV1>) -> SendIntent {
    SendIntent::UpdateAppComponents {
        group_id: gid.clone(),
        updates: vec![AppComponentData {
            component_id: DEVICE_ROSTER_COMPONENT_ID,
            data: update_bytes(operations),
        }],
    }
}

fn roster_bytes_of(device: &Device, gid: &GroupId) -> Option<Vec<u8>> {
    load_group(device, gid)
        .extensions()
        .app_data_dictionary()?
        .dictionary()
        .get(&DEVICE_ROSTER_COMPONENT_ID)
        .map(<[u8]>::to_vec)
}

pub fn roster_of(device: &Device, gid: &GroupId) -> DeviceRosterV1 {
    decode_device_roster_v1(&roster_bytes_of(device, gid).expect("the group holds a roster"))
        .unwrap()
}

pub fn requires_roster(device: &Device, gid: &GroupId) -> bool {
    required(&device.engine, gid).contains(DEVICE_ROSTER_COMPONENT_ID)
}

/// `creator` founds a device group that `joiners` join, each by the Add that
/// sets its entry (D41). The joiners take the last slots of `initial`, in
/// order, and the founding roster lists the rest. The founding Add adds the
/// first joiner, and the creator links each later one.
pub async fn found_device_group(
    creator: &mut Device,
    initial: &DeviceRosterV1,
    joiners: &mut [&mut Device],
) -> GroupId {
    let (founding, joiner_slots) = founding_roster(initial, joiners.len());
    let Some((first, rest)) = joiners.split_first_mut() else {
        let (gid, created) = creator
            .engine
            .create_group(device_group_request(&founding, vec![]))
            .await
            .unwrap();
        assert!(
            matches!(created, SendResult::FoundingGroupCreated { .. }),
            "current-profile founding group"
        );
        return gid;
    };
    let key_package = first.engine.fresh_key_package().await.unwrap();
    let roster_update = update_bytes(vec![DeviceRosterOperationV1::SetDevice(
        joiner_slots[0],
        announced_entry(initial, &joiner_slots[0], &key_package),
    )]);
    let chosen = fresh_device_group_id();
    expect_link_welcome(first, &chosen, creator, &key_package);
    let (gid, created) = creator
        .engine
        .create_group_with_chosen_id_and_audit_context(
            device_group_request(&founding, vec![key_package]),
            chosen,
            Some(roster_update),
            NO_WELCOME_DEADLINE,
            None,
        )
        .await
        .unwrap();
    let SendResult::FoundingGroupCreated { welcomes } = created else {
        panic!("current-profile founding group")
    };
    assert_joins(first, &welcomes[0], &gid, creator).await;
    for index in 0..rest.len() {
        let (joined, waiting) = rest.split_at_mut(index);
        let joiner = &mut waiting[0];
        let joiner_slot = joiner_slots[index + 1];
        let key_package = joiner.engine.fresh_key_package().await.unwrap();
        let roster_update = update_bytes(vec![DeviceRosterOperationV1::SetDevice(
            joiner_slot,
            announced_entry(initial, &joiner_slot, &key_package),
        )]);
        expect_link_welcome(joiner, &gid, creator, &key_package);
        let SendResult::GroupEvolution {
            msg,
            welcomes,
            pending,
        } = creator
            .engine
            .send(SendIntent::LinkDevice {
                group_id: gid.clone(),
                key_package,
                roster_update,
                deliver_until: NO_WELCOME_DEADLINE,
            })
            .await
            .unwrap()
        else {
            panic!("a link is a group evolution")
        };
        creator.engine.confirm_published(pending).await.unwrap();
        let commit = route_to_group(&msg, &gid);
        assert_accepted(first, &commit, &gid).await;
        for member in joined.iter_mut() {
            assert_accepted(member, &commit, &gid).await;
        }
        assert_joins(joiner, &welcomes[0], &gid, creator).await;
    }
    gid
}

/// `initial` without its last `joiners` slots, which it returns.
fn founding_roster(
    initial: &DeviceRosterV1,
    joiners: usize,
) -> (DeviceRosterV1, Vec<DeviceSlotId>) {
    let slots: Vec<DeviceSlotId> = initial.devices.keys().copied().collect();
    let joiner_slots = slots[slots.len() - joiners..].to_vec();
    let mut founding = initial.clone();
    for joiner_slot in &joiner_slots {
        founding.devices.remove(joiner_slot);
    }
    (founding, joiner_slots)
}

/// `initial`'s entry for `slot`, announcing `key_package`.
pub fn announced_entry(
    initial: &DeviceRosterV1,
    slot: &DeviceSlotId,
    key_package: &KeyPackage,
) -> DeviceEntryV1 {
    DeviceEntryV1 {
        announced_key_package: Some(key_package_ref(key_package)),
        ..initial.devices[slot].clone()
    }
}

pub fn key_package_ref(key_package: &KeyPackage) -> Vec<u8> {
    hex::decode(
        cgka_engine::key_package::key_package_metadata(key_package)
            .unwrap()
            .key_package_ref_hex,
    )
    .unwrap()
}

/// `joiner` expects `author`'s Welcome into the device group `gid`, adding it
/// by `key_package`, as a confirmed device link makes it (D28, D30, D41).
pub fn expect_link_welcome(
    joiner: &mut Device,
    gid: &GroupId,
    author: &Device,
    key_package: &KeyPackage,
) {
    joiner
        .engine
        .expect_device_group_welcome(&DeviceGroupWelcomeExpectation {
            group_id: gid.clone(),
            sender_signature_key: author.engine.own_signature_key(),
            key_package_ref: key_package_ref(key_package),
            expires_at: u64::MAX,
        })
        .unwrap();
}

fn fresh_device_group_id() -> GroupId {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let mut id = vec![0xD6; 16];
    id[8..].copy_from_slice(&NEXT.fetch_add(1, Ordering::Relaxed).to_be_bytes());
    GroupId::new(id)
}

/// The founding request of a device group holding `initial` and adding
/// `key_packages`.
pub fn device_group_request(
    initial: &DeviceRosterV1,
    key_packages: Vec<KeyPackage>,
) -> CreateGroupRequest {
    CreateGroupRequest {
        name: String::new(),
        description: String::new(),
        members: key_packages,
        required_features: vec![],
        app_components: vec![roster_component(initial)],
        initial_admins: vec![],
    }
}

/// A device link Welcome's delivery deadline that no test reaches.
pub const NO_WELCOME_DEADLINE: Timestamp = Timestamp(u64::MAX);

// Device tags (mdk#2628 3.1d)

/// Gives `device` the KeyPackage publication slot `slot`, which makes the
/// roster entry there its own.
pub fn hold_slot(device: &Device, slot: DeviceSlotId) {
    device
        .storage
        .put_key_package_lifecycle(&KeyPackageLifecycleState::slot_only(hex::encode(slot)))
        .unwrap();
}

/// `device` sets its own `tag_signature_key` in its entry at `own_slot` in
/// the device group `gid`, and `others` apply the roster update.
pub async fn set_own_key(
    device: &mut Device,
    own_slot: DeviceSlotId,
    gid: &GroupId,
    others: &mut [&mut Device],
) {
    let mut own_entry = roster_of(device, gid).devices[&own_slot].clone();
    own_entry.tag_signature_key = Some(device.engine.own_signature_key());
    let update = roster_update(
        gid,
        vec![DeviceRosterOperationV1::SetDevice(own_slot, own_entry)],
    );
    let commit = send_commit(device, update, gid).await;
    for other in others.iter_mut() {
        assert_accepted(other, &commit, gid).await;
    }
}

/// The first device of `account` and `new` more, in the account's device
/// group, whose id it returns: the first at slot 1 and each of the others at
/// the slot after, every one tagged.
pub async fn tagged_devices(account: &[u8], new: usize) -> (Device, Vec<Device>, GroupId) {
    tag_devices((0..=new).map(|_| roster_device(account)).collect()).await
}

pub async fn tag_devices(mut devices: Vec<Device>) -> (Device, Vec<Device>, GroupId) {
    let new = devices.len() - 1;
    let labels = [(1, "Phone"), (2, "Laptop"), (3, "Tablet")];
    let (first, rest) = devices.split_first_mut().unwrap();
    let mut joiners: Vec<&mut Device> = rest.iter_mut().collect();
    let device_gid = found_device_group(first, &roster(&labels[..=new]), &mut joiners).await;
    for index in 0..devices.len() {
        let own = slot(u8::try_from(index + 1).unwrap());
        let (before, rest) = devices.split_at_mut(index);
        let (device, after) = rest.split_first_mut().unwrap();
        hold_slot(device, own);
        device
            .engine
            .set_device_tag_group(Some(device_gid.clone()))
            .unwrap();
        let mut others: Vec<&mut Device> = before.iter_mut().chain(after.iter_mut()).collect();
        set_own_key(device, own, &device_gid, &mut others).await;
    }
    let first = devices.remove(0);
    (first, devices, device_gid)
}

// Sibling KeyPackages / sibling Welcomes

pub const RELAY: &str = "wss://relay.example";
pub const NOW: u64 = 1_000;

/// A private KeyPackage that `sibling`, at `slot`, issues to the device at
/// `to`, as its hand-off gives it.
pub fn handed(sibling: &mut Device, slot: DeviceSlotId, to: DeviceSlotId) -> SiblingKeyPackage {
    let before = sibling.engine.issued_private_key_packages().unwrap();
    sibling
        .engine
        .issue_private_key_packages(&to, 1, None, NOW)
        .unwrap();
    let issued = sibling
        .engine
        .issued_private_key_packages()
        .unwrap()
        .into_iter()
        .find(|record| !before.contains(record))
        .unwrap();
    SiblingKeyPackage {
        slot,
        key_package: KeyPackage::new(issued.key_package)
            .with_protocol_profile(ProtocolProfile::Current),
    }
}

/// The sibling Welcomes `device` owes now.
pub fn owed(device: &Device) -> Vec<OwedSiblingWelcome> {
    device.engine.owed_sibling_welcomes(Timestamp(NOW)).unwrap()
}

/// `sibling` joins `gid` through `owed` and names `adder` as its author.
pub async fn assert_joins_from(
    sibling: &mut Device,
    owed: &OwedSiblingWelcome,
    gid: &GroupId,
    adder: &Device,
) {
    let welcome = SiblingWelcome::decode(&owed.content).unwrap();
    let joined = sibling
        .engine
        .join_sibling_welcome(&hash_id(&owed.content), &welcome, None)
        .await
        .unwrap();
    println!("    join_sibling_welcome: {joined:?}");
    assert_eq!(
        joined,
        SiblingWelcomeJoin::Settled(IngestOutcome::Processed)
    );
    let welcomers: Vec<_> = sibling
        .engine
        .drain_events()
        .into_iter()
        .filter_map(|event| match event {
            GroupEvent::GroupJoined {
                group_id, welcomer, ..
            } if &group_id == gid => welcomer,
            _ => None,
        })
        .collect();
    assert_eq!(welcomers, vec![adder.engine.self_id()]);
    assert_eq!(
        sibling.engine.epoch(gid).unwrap(),
        adder.engine.epoch(gid).unwrap()
    );
}

/// `founder` founds a chat routed through [`RELAY`] that each of `joiners`
/// joins from its Welcome. Every founder advertises 0x800d, so the chat
/// requires it.
pub async fn found_routed_group(founder: &mut Device, joiners: &mut [&mut Device]) -> GroupId {
    let mut key_packages = Vec::new();
    for joiner in joiners.iter_mut() {
        key_packages.push(joiner.engine.fresh_key_package().await.unwrap());
    }
    let routing = NostrRoutingV1::new([0x43; 32], vec![RELAY.to_string()]).unwrap();
    let (gid, created) = founder
        .engine
        .create_group(CreateGroupRequest {
            name: "routed".into(),
            description: String::new(),
            members: key_packages,
            required_features: vec![],
            app_components: vec![AppComponentData {
                component_id: NOSTR_ROUTING_COMPONENT_ID,
                data: encode_nostr_routing_v1(&routing).unwrap(),
            }],
            initial_admins: vec![],
        })
        .await
        .unwrap();
    let SendResult::FoundingGroupCreated { welcomes } = created else {
        panic!("current-profile founding group")
    };
    for (joiner, welcome) in joiners.iter_mut().zip(welcomes) {
        joiner.engine.join_welcome(welcome).await.unwrap();
    }
    assert!(requires_account_devices(&founder.engine, &gid));
    gid
}
