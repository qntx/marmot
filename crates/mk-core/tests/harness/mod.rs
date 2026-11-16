//! In-process harness for the M0 native flow test.
//!
//! The session layer an application would run above the engine, minimized to
//! what the spike scenario needs:
//!
//! - `HostAccountSigner` — the host-owned account key. The engine sees it only
//!   through `AccountIdentityProofSigner` (sync) and the peeler only through
//!   `MarmotNostrSigner` (async); both are trait objects, matching the
//!   "account key lives on the host" product boundary. This spike implements
//!   both over a local `nostr::Keys`.
//! - `FakeRelay` — an in-process relay: stores `NostrTransportEvent`s, filters
//!   by kind/`#h`/`#p`/author, returns backlog newest-first like a real relay.
//! - KeyPackage publication as kind `30443`, reimplemented from
//!   `transport-nostr-adapter::key_package::NostrKeyPackagePublication`
//!   (`to_event_at`) because that crate pulls relay-sdk machinery the spike
//!   does not need; tag values come from the engine's own
//!   `cgka_engine::key_package::key_package_metadata`.

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
    clippy::print_stderr,
    clippy::print_stdout,
    clippy::missing_assert_message,
    reason = "test code"
)]
#![allow(
    dead_code,
    reason = "shared test harness; not every test file uses every helper"
)]
use std::collections::BTreeMap;
use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use cgka_engine::account_identity_proof::{
    AccountIdentityProofRequest, AccountIdentityProofSigner,
};
use cgka_engine::convergence_clock::ManualConvergenceClock;
use cgka_engine::{Engine, EngineBuilder, FeatureRegistry};
use cgka_traits::agent_text_stream::{
    AGENT_TEXT_STREAM_QUIC_FANOUT_FEATURE, AGENT_TEXT_STREAM_QUIC_RECEIVE_FEATURE,
    AGENT_TEXT_STREAM_QUIC_SEND_FEATURE,
};
use cgka_traits::app_components::{
    AppComponentData, NOSTR_ROUTING_COMPONENT_ID, NostrRoutingV1,
    PRIVATE_USE_APP_COMPONENT_ID_START, default_group_components, encode_nostr_routing_v1,
};
use cgka_traits::app_event::{MARMOT_APP_EVENT_KIND_CHAT, MarmotAppEvent};
use cgka_traits::capabilities::{Capability, CapabilityRequirement, Feature, RequirementLevel};
use cgka_traits::engine::{
    CgkaEngine, CreateGroupRequest, GroupEvent, KeyPackage, KeyPackageSource, SendResult,
};
use cgka_traits::engine_state::PendingStateRef;
use cgka_traits::group::ProtocolProfile;
use cgka_traits::ingest::IngestOutcome;
use cgka_traits::storage::MessageStorage;
use cgka_traits::transport::TransportMessage;
use cgka_traits::types::{GroupId, MemberId, MessageId};
use nostr::prelude::*;
use transport_nostr_peeler::{
    KIND_NIP59_GIFT_WRAP, MarmotNostrSigner, MarmotSignerError, NostrMlsPeeler,
    NostrTransportEvent, SignerFuture,
};

use mk_core::{KvBatch, KvHost, KvOp, KvStorageProvider};

pub(crate) const KIND_MARMOT_KEY_PACKAGE: u64 = 30_443;
pub(crate) const RELAY_URL: &str = "ws://spike-relay.local:7777";

pub(crate) fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_secs()
}

/// Deterministic account key for a harness label.
pub(crate) fn account_keys(seed: &str) -> Keys {
    use sha2::{Digest, Sha256};
    let secret: [u8; 32] = Sha256::digest(format!("marmot-spike-account:{seed}").as_bytes()).into();
    Keys::new(SecretKey::from_slice(&secret).expect("seed key"))
}

/// Host-owned account key, reached only through the two signer trait objects
/// MDK takes: sync `AccountIdentityProofSigner` for MLS leaf identity proofs
/// and async `MarmotNostrSigner` for NIP-59 Welcome sealing.
pub(crate) struct HostAccountSigner {
    keys: Keys,
}

impl HostAccountSigner {
    pub(crate) fn new(keys: Keys) -> Arc<Self> {
        Arc::new(Self { keys })
    }

    pub(crate) fn keys(&self) -> &Keys {
        &self.keys
    }

    pub(crate) fn public_key_bytes(&self) -> [u8; 32] {
        self.keys.public_key().to_bytes()
    }
}

impl fmt::Debug for HostAccountSigner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HostAccountSigner")
            .field("public_key", &self.keys.public_key().to_hex())
            .finish()
    }
}

impl AccountIdentityProofSigner for HostAccountSigner {
    fn sign_account_identity_proof(
        &self,
        request: &AccountIdentityProofRequest,
    ) -> Result<[u8; 64], String> {
        if self.keys.public_key().to_bytes().as_slice() != request.account_identity.as_slice() {
            return Err("proof request names a different account".into());
        }
        let event = request
            .proof_event()
            .and_then(|event| event.finalize(&self.keys).map_err(|e| e.to_string()))?;
        request.signature_from_signed_event(event)
    }
}

impl MarmotNostrSigner for HostAccountSigner {
    fn get_public_key(&self) -> SignerFuture<'_, Result<PublicKey, MarmotSignerError>> {
        MarmotNostrSigner::get_public_key(&self.keys)
    }
    fn sign_event(
        &self,
        unsigned: UnsignedEvent,
    ) -> SignerFuture<'_, Result<Event, MarmotSignerError>> {
        MarmotNostrSigner::sign_event(&self.keys, unsigned)
    }
    fn nip04_encrypt<'a>(
        &'a self,
        public_key: &'a PublicKey,
        content: &'a str,
    ) -> SignerFuture<'a, Result<String, MarmotSignerError>> {
        MarmotNostrSigner::nip04_encrypt(&self.keys, public_key, content)
    }
    fn nip04_decrypt<'a>(
        &'a self,
        public_key: &'a PublicKey,
        payload: &'a str,
    ) -> SignerFuture<'a, Result<String, MarmotSignerError>> {
        MarmotNostrSigner::nip04_decrypt(&self.keys, public_key, payload)
    }
    fn nip44_encrypt<'a>(
        &'a self,
        public_key: &'a PublicKey,
        content: &'a str,
    ) -> SignerFuture<'a, Result<String, MarmotSignerError>> {
        MarmotNostrSigner::nip44_encrypt(&self.keys, public_key, content)
    }
    fn nip44_decrypt<'a>(
        &'a self,
        public_key: &'a PublicKey,
        payload: &'a str,
    ) -> SignerFuture<'a, Result<String, MarmotSignerError>> {
        MarmotNostrSigner::nip44_decrypt(&self.keys, public_key, payload)
    }
}

/// One account-device: its own host KV + provider + engine + peeler.
///
/// The engine runs a `ManualConvergenceClock` — the same public builder seam
/// production uses with `SystemConvergenceClock`. Deterministic time lets the
/// test drive the quiescence window and deferred-peel budgets exactly; a real
/// app waits out the same windows on wall time.
pub(crate) struct Member {
    pub(crate) name: String,
    pub(crate) signer: Arc<HostAccountSigner>,
    pub(crate) host: Arc<TestHost>,
    pub(crate) provider: KvStorageProvider,
    pub(crate) clock: Arc<ManualConvergenceClock>,
    pub(crate) engine: Engine<KvStorageProvider>,
}

impl Member {
    /// Open a member on a fresh host.
    pub(crate) fn fresh(name: &str, seed: &str) -> Self {
        Self::open(
            name,
            HostAccountSigner::new(account_keys(seed)),
            Arc::new(TestHost::new()),
        )
    }

    /// Open a member on an existing host (fresh or restored from a dump).
    pub(crate) fn open(name: &str, signer: Arc<HostAccountSigner>, host: Arc<TestHost>) -> Self {
        let provider =
            KvStorageProvider::open(host.durable_rows(), host.clone()).expect("provider opens");
        let peeler = NostrMlsPeeler::new().with_welcome_signer_arc(signer.clone());
        let clock = Arc::new(ManualConvergenceClock::new(0, 0));
        let mut engine = EngineBuilder::new(provider.clone())
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
        Self {
            name: name.to_string(),
            signer,
            host,
            provider,
            clock,
            engine,
        }
    }

    /// Simulate a process restart: keep only the host's durable rows.
    pub(crate) fn restart(
        rows: Vec<(Vec<u8>, Vec<u8>)>,
        name: &str,
        signer: Arc<HostAccountSigner>,
    ) -> Self {
        let host = Arc::new(TestHost::new());
        let provider = KvStorageProvider::open(rows, host.clone()).expect("provider opens");
        let peeler = NostrMlsPeeler::new().with_welcome_signer_arc(signer.clone());
        let clock = Arc::new(ManualConvergenceClock::new(0, 0));
        let mut engine = EngineBuilder::new(provider.clone())
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
        Self {
            name: name.to_string(),
            signer,
            host,
            provider,
            clock,
            engine,
        }
    }

    pub(crate) fn member_id(&self) -> MemberId {
        self.engine.self_id()
    }

    /// Collect drained `MessageReceived` payloads since the last drain.
    ///
    /// Drained app events are AT-LEAST-ONCE: they stay in durable
    /// `pending_application_events` until the app acknowledges them, and
    /// an engine rebuilt from the same store re-presents unacked events
    /// (Engine::build seeds `events_buf` from
    /// `list_pending_application_events`). The harness acknowledges after
    /// the test's "app store" (the `seen` set) has accepted each event —
    /// `delete_pending_application_events` is that ack.
    pub(crate) fn drain_received(&mut self) -> Vec<(GroupId, MemberId, Vec<u8>)> {
        let mut out = Vec::new();
        for event in self.drain_events() {
            if let GroupEvent::MessageReceived {
                group_id,
                sender,
                payload,
                ..
            } = event
            {
                out.push((group_id, sender, payload));
            }
        }
        out
    }

    /// All drained events, for steps that assert on non-message events.
    /// Acknowledges any `MessageReceived` payloads (see `drain_received`).
    pub(crate) fn drain_events(&mut self) -> Vec<GroupEvent> {
        let events = self.engine.drain_events();
        let ack: Vec<MessageId> = events
            .iter()
            .filter_map(|event| match event {
                GroupEvent::MessageReceived { message_id, .. } => Some(message_id.clone()),
                _ => None,
            })
            .collect();
        if !ack.is_empty() {
            self.provider
                .delete_pending_application_events(&ack)
                .expect("ack pending application events");
        }
        events
    }
}

/// Components the spike advertises: the engine defaults plus the
/// protocol-advertisable set White Noise groups require of members
/// (agent-text-stream, encrypted-media v2, account-devices). Only ids the
/// engine's `ADVERTISABLE_PROTOCOL_APP_COMPONENTS` gate accepts.
pub(crate) fn supported_components() -> Vec<u16> {
    use cgka_traits::app_components::{
        ACCOUNT_DEVICES_COMPONENT_ID, AGENT_TEXT_STREAM_QUIC_COMPONENT_ID,
        GROUP_ENCRYPTED_MEDIA_V2_COMPONENT_ID,
    };
    let mut components = default_group_components();
    components.insert(NOSTR_ROUTING_COMPONENT_ID);
    components.insert(AGENT_TEXT_STREAM_QUIC_COMPONENT_ID);
    components.insert(GROUP_ENCRYPTED_MEDIA_V2_COMPONENT_ID);
    components.insert(ACCOUNT_DEVICES_COMPONENT_ID);
    components.into_iter().collect()
}

/// Feature registry matching marmot-app's `app_feature_registry`: the
/// SelfRemove proposal and the three agent-text-stream QUIC extension
/// capabilities so KeyPackages carry them.
pub(crate) fn supported_features() -> FeatureRegistry {
    let mut registry = FeatureRegistry::new();
    registry.register(
        Feature("self-remove"),
        CapabilityRequirement {
            requires: Capability::Proposal(10),
            level: RequirementLevel::Required,
            description: "MIP-03 SelfRemove group departure",
        },
    );
    for (feature, capability, description) in [
        (
            AGENT_TEXT_STREAM_QUIC_RECEIVE_FEATURE.clone(),
            cgka_traits::agent_text_stream::AGENT_TEXT_STREAM_QUIC_RECEIVE_CAPABILITY,
            "receive QUIC-backed agent text stream previews",
        ),
        (
            AGENT_TEXT_STREAM_QUIC_SEND_FEATURE.clone(),
            cgka_traits::agent_text_stream::AGENT_TEXT_STREAM_QUIC_SEND_CAPABILITY,
            "send QUIC-backed agent text stream frames",
        ),
        (
            AGENT_TEXT_STREAM_QUIC_FANOUT_FEATURE.clone(),
            cgka_traits::agent_text_stream::AGENT_TEXT_STREAM_QUIC_FANOUT_CAPABILITY,
            "fan out QUIC-backed agent text stream frames",
        ),
    ] {
        registry.register(
            feature,
            CapabilityRequirement {
                requires: capability,
                level: RequirementLevel::Optional,
                description,
            },
        );
    }
    registry
}

/// App-component state for `create_group`: the group needs Nostr routing so
/// the engine can fill `WelcomeMetadata.relays` for the kind-444 rumor.
pub(crate) fn routing_component(creator: &MemberId, relays: &[String]) -> AppComponentData {
    // The nostr_group_id routes every kind-445 event of ONE group; it must be
    // unique per group or relays/daemons cannot tell two groups apart.
    let mut nostr_group_id = [0u8; 32];
    getrandom::fill(&mut nostr_group_id).expect("rng");
    let _ = creator;
    let routing = NostrRoutingV1::new(nostr_group_id, relays.to_vec()).expect("routing valid");
    AppComponentData {
        component_id: NOSTR_ROUTING_COMPONENT_ID,
        data: encode_nostr_routing_v1(&routing).expect("routing encodes"),
    }
}

/// In-process relay. Stores signed events, filters like a Nostr relay, and
/// returns backlog NEWEST-FIRST (created_at desc, id desc tie-break), the
/// order real relay implementations emit stored results.
#[derive(Default)]
pub(crate) struct FakeRelay {
    events: Mutex<Vec<NostrTransportEvent>>,
    fail_next: AtomicBool,
}

impl FakeRelay {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Make the next `publish_transport`/`publish_event` fail, once.
    pub(crate) fn fail_next_publish(&self) {
        self.fail_next.store(true, Ordering::SeqCst);
    }

    pub(crate) fn len(&self) -> usize {
        self.events.lock().unwrap().len()
    }

    /// Publish an engine-produced transport message (kind 445 or 1059).
    pub(crate) fn publish_transport(&self, msg: &TransportMessage) -> Result<MessageId, ()> {
        if self.fail_next.swap(false, Ordering::SeqCst) {
            return Err(());
        }
        let event = NostrTransportEvent::from_transport_message(msg).map_err(|_| ())?;
        if event.to_verified_nostr_event().is_err() {
            return Err(());
        }
        self.events.lock().unwrap().push(event);
        Ok(msg.id.clone())
    }

    /// Publish an already-signed transport event (e.g. a kind-30443 KP).
    pub(crate) fn publish_event(&self, event: NostrTransportEvent) -> Result<(), ()> {
        if self.fail_next.swap(false, Ordering::SeqCst) {
            return Err(());
        }
        if event.to_verified_nostr_event().is_err() {
            return Err(());
        }
        self.events.lock().unwrap().push(event);
        Ok(())
    }

    /// Backlog query: kind + optional `#h` + optional `#p`, newest-first.
    pub(crate) fn fetch_newest_first(
        &self,
        kind: u64,
        h_tag: Option<&str>,
        p_tag: Option<&str>,
    ) -> Vec<NostrTransportEvent> {
        let mut out = self.filtered(kind, h_tag, p_tag);
        out.sort_by(|a, b| (b.created_at, b.id.as_str()).cmp(&(a.created_at, a.id.as_str())));
        out
    }

    /// Same filter, ascending by (created_at, id) — the order the SDK should
    /// feed the engine for catch-up.
    pub(crate) fn fetch_ascending(
        &self,
        kind: u64,
        h_tag: Option<&str>,
        p_tag: Option<&str>,
    ) -> Vec<NostrTransportEvent> {
        let mut out = self.filtered(kind, h_tag, p_tag);
        out.sort_by(|a, b| (a.created_at, a.id.as_str()).cmp(&(b.created_at, b.id.as_str())));
        out
    }

    fn filtered(
        &self,
        kind: u64,
        h_tag: Option<&str>,
        p_tag: Option<&str>,
    ) -> Vec<NostrTransportEvent> {
        self.events
            .lock()
            .unwrap()
            .iter()
            .filter(|e| e.kind == kind)
            .filter(|e| h_tag.is_none_or(|h| e.tag_values("h").contains(&h)))
            .filter(|e| p_tag.is_none_or(|p| e.tag_values("p").contains(&p)))
            .cloned()
            .collect()
    }

    /// KeyPackage lookup by author, newest-first.
    pub(crate) fn fetch_key_packages(&self, author_hex: &str) -> Vec<NostrTransportEvent> {
        let mut out: Vec<_> = self
            .events
            .lock()
            .unwrap()
            .iter()
            .filter(|e| e.kind == KIND_MARMOT_KEY_PACKAGE && e.pubkey == author_hex)
            .cloned()
            .collect();
        out.sort_by(|a, b| (b.created_at, b.id.as_str()).cmp(&(a.created_at, a.id.as_str())));
        out
    }
}

/// Build, sign and return a kind-30443 KeyPackage event for the member,
/// reimplementing `NostrKeyPackagePublication::to_event_at` (MDK
/// `transport-nostr-adapter/src/key_package.rs`) against the peeler's DTO and
/// the engine's own `key_package_metadata` parser.
pub(crate) async fn publish_key_package(
    member: &mut Member,
    relay: &FakeRelay,
) -> NostrTransportEvent {
    let kp = member
        .engine
        .fresh_key_package()
        .await
        .expect("fresh_key_package");
    let metadata =
        cgka_engine::key_package::key_package_metadata(&kp).expect("key_package_metadata");
    assert_eq!(metadata.protocol_profile, ProtocolProfile::Current);
    assert_eq!(
        metadata.credential_identity_hex,
        member.signer.keys().public_key().to_hex()
    );

    let tags: Vec<Tag> = vec![
        Tag::custom("d", ["main".to_string()]),
        Tag::custom("mls_protocol_version", ["1.0".to_string()]),
        Tag::custom("i", [metadata.key_package_ref_hex.clone()]),
        Tag::custom(
            "mls_ciphersuite",
            [format!("0x{:04x}", metadata.ciphersuite)],
        ),
        Tag::custom(
            "mls_extensions",
            metadata
                .mls_extensions
                .iter()
                .map(|id| format!("0x{id:04x}"))
                .collect::<Vec<String>>(),
        ),
        Tag::custom(
            "mls_proposals",
            metadata
                .mls_proposals
                .iter()
                .map(|id| format!("0x{id:04x}"))
                .collect::<Vec<String>>(),
        ),
        Tag::custom(
            "app_components",
            metadata
                .app_components
                .iter()
                .filter(|id| **id >= PRIVATE_USE_APP_COMPONENT_ID_START)
                .map(|id| format!("0x{id:04x}"))
                .collect::<Vec<String>>(),
        ),
        Tag::custom("client", ["marmot-spike".to_string()]),
    ];

    let event = EventBuilder::new(
        Kind::Custom(KIND_MARMOT_KEY_PACKAGE as u16),
        BASE64_STANDARD.encode(kp.bytes()),
    )
    .tags(tags)
    .custom_created_at(Timestamp::from_secs(now_secs()))
    .finalize(member.signer.keys())
    .expect("sign key package event");

    let dto = NostrTransportEvent::from_nostr_event(&event).expect("dto");
    relay.publish_event(dto.clone()).expect("kp publish");
    dto
}

/// Resolve the newest published KeyPackage event of `author_hex` into the
/// engine invite input, preserving the source event id (the Welcome rumor's
/// required `e` tag).
pub(crate) fn key_package_from_event(event: &NostrTransportEvent) -> KeyPackage {
    assert_eq!(event.kind, KIND_MARMOT_KEY_PACKAGE);
    let bytes = BASE64_STANDARD
        .decode(event.content.as_bytes())
        .expect("kp content is base64");
    let event_id = MessageId::new(hex::decode(&event.id).expect("event id hex"));
    KeyPackage {
        bytes,
        source: Some(KeyPackageSource { event_id }),
        protocol_profile: ProtocolProfile::Current,
    }
}

/// Deliver one relay event to a member's engine.
pub(crate) async fn ingest_event(
    member: &mut Member,
    event: &NostrTransportEvent,
) -> IngestOutcome {
    let msg = event.to_transport_message().expect("event maps");
    member.engine.ingest(msg).await.expect("ingest")
}

/// Fetch every kind-1059 gift wrap addressed to a member and ingest it.
/// Returns per-event outcomes.
pub(crate) async fn ingest_welcomes(member: &mut Member, relay: &FakeRelay) -> Vec<IngestOutcome> {
    let p = member.signer.keys().public_key().to_hex();
    let events = relay.fetch_ascending(KIND_NIP59_GIFT_WRAP, None, Some(&p));
    let mut outcomes = Vec::new();
    for event in events {
        outcomes.push(ingest_event(member, &event).await);
    }
    outcomes
}

#[derive(Default, Debug)]
pub(crate) struct PublishReport {
    pub(crate) transport_messages: usize,
    pub(crate) welcomes: usize,
    pub(crate) pending: Option<PendingStateRef>,
    pub(crate) queued: usize,
    pub(crate) confirmed: bool,
}

/// Publish every artifact a `SendResult` carries to the relay, then resolve
/// the pending publish per the publish-before-apply contract. Returns counts.
pub(crate) async fn publish_send_result(
    member: &mut Member,
    result: SendResult,
    relay: &FakeRelay,
) -> PublishReport {
    let mut report = PublishReport::default();
    match result {
        SendResult::ApplicationMessage { msg, .. } => {
            relay.publish_transport(&msg).expect("publish app message");
            report.transport_messages += 1;
        }
        SendResult::Proposal { msg } => {
            relay.publish_transport(&msg).expect("publish proposal");
            report.transport_messages += 1;
        }
        SendResult::GroupEvolution {
            msg,
            welcomes,
            pending,
        } => {
            let mut published = true;
            if relay.publish_transport(&msg).is_err() {
                published = false;
            }
            for welcome in &welcomes {
                relay.publish_transport(welcome).expect("publish welcome");
                report.welcomes += 1;
            }
            report.transport_messages += 1;
            report.pending = Some(pending);
            if published {
                member
                    .engine
                    .confirm_published(pending)
                    .await
                    .expect("confirm_published");
                report.confirmed = true;
            } else {
                member
                    .engine
                    .publish_failed(pending)
                    .await
                    .expect("publish_failed");
            }
        }
        SendResult::GroupCreated { welcomes, pending } => {
            let mut published = true;
            for welcome in &welcomes {
                if relay.publish_transport(welcome).is_err() {
                    published = false;
                }
                report.welcomes += 1;
            }
            report.pending = Some(pending);
            if published {
                member
                    .engine
                    .confirm_published(pending)
                    .await
                    .expect("confirm_published");
                report.confirmed = true;
            } else {
                member
                    .engine
                    .publish_failed(pending)
                    .await
                    .expect("publish_failed");
            }
        }
        SendResult::FoundingGroupCreated { welcomes } => {
            for welcome in &welcomes {
                relay.publish_transport(welcome).expect("publish welcome");
                report.welcomes += 1;
            }
        }
        SendResult::Queued { .. } => {
            report.queued += 1;
        }
        SendResult::NoChange { .. } => {}
        other => panic!("unexpected send result in spike flow: {other:?}"),
    }
    report
}

/// Create a group with the Nostr routing component the Welcome metadata needs.
pub(crate) async fn create_group(
    member: &mut Member,
    relay: &FakeRelay,
    name: &str,
    invitee_kps: Vec<KeyPackage>,
) -> GroupId {
    let req = CreateGroupRequest {
        name: name.into(),
        description: String::new(),
        members: invitee_kps,
        required_features: Vec::new(),
        app_components: vec![routing_component(
            &member.member_id(),
            &[RELAY_URL.to_string()],
        )],
        initial_admins: Vec::new(),
    };
    let (group_id, result) = member.engine.create_group(req).await.expect("create_group");
    publish_send_result(member, result, relay).await;
    group_id
}

/// A Marmot kind-9 chat payload: the inner app event encoded for the engine.
pub(crate) fn chat_payload(sender_account_hex: &str, created_at: u64, content: &str) -> Vec<u8> {
    MarmotAppEvent::new(
        sender_account_hex,
        created_at,
        MARMOT_APP_EVENT_KIND_CHAT,
        Vec::new(),
        content,
    )
    .encode()
    .expect("app event encodes")
}

/// Drive the member's convergence machinery until the group produces no more
/// publishable work, no pending groups remain, and all stored input is
/// settled. Advances the manual clock so quiescence windows elapse
/// deterministically. Queued sends are regenerated and published here;
/// deferred-peel and buffered input are replayed to settlement.
pub(crate) async fn flush_outbound(member: &mut Member, gid: &GroupId, relay: &FakeRelay) -> usize {
    let mut published = 0;
    for _ in 0..16 {
        member.clock.advance_ms(60_000);
        let pending_groups = member.engine.drain_pending_convergence_groups();
        let settled = match member
            .engine
            .advance_convergence_inputs_until_settled(gid, 0)
            .await
        {
            Ok(settled) => settled,
            Err(error) => {
                eprintln!(
                    "flush_outbound({}): inputs settle failed: {error}",
                    member.name
                );
                return published;
            }
        };
        let results = match member.engine.advance_convergence(gid).await {
            Ok(results) => results,
            Err(error) => {
                eprintln!(
                    "flush_outbound({}): advance_convergence failed: {error}",
                    member.name
                );
                return published;
            }
        };
        if results.is_empty() && pending_groups.is_empty() && settled {
            break;
        }
        for result in results {
            published += publish_send_result(member, result, relay)
                .await
                .transport_messages;
        }
    }
    published
}

/// Minimal in-memory [`KvHost`]: applies every batch to a durable row map.
/// `durable_rows()` is what a reopen gets; `seqs()` proves batch ordering.
#[derive(Debug, Default)]
pub(crate) struct TestHost {
    rows: Mutex<BTreeMap<Vec<u8>, Vec<u8>>>,
    seqs: Mutex<Vec<u64>>,
}

impl TestHost {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn durable_rows(&self) -> Vec<(Vec<u8>, Vec<u8>)> {
        self.rows
            .lock()
            .expect("host rows")
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }

    pub(crate) fn seqs(&self) -> Vec<u64> {
        self.seqs.lock().expect("host seqs").clone()
    }
}

impl KvHost for TestHost {
    fn commit(&self, batch: KvBatch) {
        self.seqs.lock().expect("host seqs").push(batch.seq.get());
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
