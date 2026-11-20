//! The account-device session: the public API of this crate.
//!
//! [`Session`] owns a [`KvStorageProvider`], an `Engine` built on it and the
//! injected `AccountSigner`/`Clock`. Every durable byte still goes through
//! the host's [`crate::kv::KvHost`] port as before; every network byte
//! leaves only through the [`Session::take_outbound`] /
//! [`Session::report_publish`] gate.
//!
//! # The durability gate
//!
//! A host can crash between "session wrote rows" and "rows are durable".
//! The gate makes that window safe: every engine transport artifact is
//! frozen as an MDK `OutboundFanout` — and every account publication as a
//! session row — *before* [`Session::take_outbound`] can release it. The seq
//! of the storage batch that wrote the artifact becomes the item's gate
//! seq; [`Session::take_outbound`] releases only items whose gate seq is
//! covered by [`Session::acknowledge_durable`]. On reopen everything loaded
//! from durable rows is releasable immediately (the host already persisted
//! it); an `Attempting` target re-marked after restart records possible
//! exposure, so a commit that may have reached a relay before the crash is
//! retried with the same bytes and never rolled back.
//!
//! # Runtime requirement
//!
//! On native targets cgka-engine's send/ingest paths use `tokio::time`
//! (deferred-peel foreground budget), so session futures must be polled
//! inside a Tokio runtime with the time driver enabled (a current-thread
//! runtime is enough). On wasm32 the engine uses gloo-timers instead. This
//! crate itself never depends on tokio.

#![cfg_attr(
    target_family = "wasm",
    allow(
        clippy::future_not_send,
        reason = "wasm futures are single-threaded by construction"
    )
)]

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::sync::Arc;

use cgka_engine::account_identity_proof::{
    AccountIdentityProofRequest, AccountIdentityProofSigner, account_identity_proof_component,
};
use cgka_engine::{Engine, EngineBuilder, FeatureRegistry};

use cgka_traits::agent_text_stream::{
    AGENT_TEXT_STREAM_QUIC_COMPONENT_ID, AGENT_TEXT_STREAM_QUIC_RECEIVE_CAPABILITY,
    AGENT_TEXT_STREAM_QUIC_RECEIVE_FEATURE,
};
use cgka_traits::app_components::{
    ACCOUNT_DEVICES_COMPONENT_ID, APP_COMPONENTS_COMPONENT_ID, AppComponentData,
    GROUP_ADMIN_POLICY_COMPONENT_ID, GROUP_AVATAR_URL_COMPONENT_ID,
    GROUP_ENCRYPTED_MEDIA_EXPORTER_CACHE_KEY, GROUP_ENCRYPTED_MEDIA_V2_COMPONENT_ID,
    GROUP_LIFECYCLE_COMPONENT_ID, GROUP_MESSAGE_RETENTION_COMPONENT_ID, GroupAvatarUrlV1,
    GroupLifecycleV1, NOSTR_ROUTING_COMPONENT_ID, NostrRoutingV1, decode_components_list,
    decode_group_avatar_url_v1, decode_group_lifecycle_v1, decode_nostr_routing_v1,
    decode_quic_varint, default_group_components, encode_group_avatar_url_v1, encode_quic_varint,
    is_loopback_host, is_public_ip, validate_and_normalize_group_avatar_url,
    validate_nostr_relay_url,
};
use cgka_traits::app_event::{
    MARMOT_APP_EVENT_KIND_AGENT_STREAM_START, MARMOT_APP_EVENT_KIND_CHAT,
    MARMOT_APP_EVENT_KIND_DELETE, MARMOT_APP_EVENT_KIND_EDIT, MARMOT_APP_EVENT_KIND_GROUP_SYSTEM,
    MARMOT_APP_EVENT_KIND_REACTION, MARMOT_APP_EVENT_KIND_REMOVE, STREAM_START_TAG,
};
use cgka_traits::capabilities::{Capability, CapabilityRequirement, Feature, RequirementLevel};
use cgka_traits::engine::{
    CreateGroupRequest, EpochPin, KeyPackage, KeyPackageSource, SendIntent, SendResult,
};
use cgka_traits::engine_state::EpochState;
use cgka_traits::error::EngineError;
use cgka_traits::group::ProtocolProfile;
use cgka_traits::storage::{
    AccountDeviceSignerBinding, AccountDeviceSignerStorage, DisbandRequestStatus, MessageStorage,
    OutboundFanoutStorage,
};
use cgka_traits::transport_adapter::{
    FanoutPendingKind, FanoutTargetStatus, OutboundApplicationMessage, OutboundFanout,
    PublicationObservation, TransportEndpoint, TransportEndpointFailure,
    TransportEndpointFailureKind, TransportPublishRequest, TransportPublishTarget,
};
use cgka_traits::{
    CgkaEngine, GroupId as EngineGroupId, MemberId, MessageId, TransportEnvelope, TransportMessage,
};
use sha2::Digest as _;
use thiserror::Error;
use transport_nostr_peeler::NostrMlsPeeler;
use zeroize::Zeroizing;

use crate::agent_stream::{AgentStreamPolicy, StreamRoles};
use crate::clock::{Clock, EngineConvergenceClock, EngineMaintenanceRandom, EngineWallClock, Time};
use crate::delivered::{self, ExpiryHeap, HeldModifier};
use crate::events::{InputOutcome, InputReport, SessionEvent, input_outcome, session_event};
use crate::key_package::{self, KeyPackageInfo, is_valid_client_name};
use crate::kv::{CommitSeq, KvHost};
use crate::media::{
    self, EncryptedMediaPolicy, Locator, MediaError, MediaReference, validate_locator,
};
use crate::outbox::{
    Outbound, OutboundPurpose, PubTargetStatus, PublishResolution, RelayResult, RelayStatus,
    SessionPubPurpose, SessionPublication,
};
use crate::pacing::PacingMap;
use crate::provider::KvStorageProvider;
use crate::semantics::{self, MessageIntent};
use crate::signer::{
    AccountSigner, DeferringProofSigner, FixedProofSigner, RefusingProofSigner, SignerAdapter,
    SignerError, SignerLatch, SignerVerdict,
};
use crate::types::{AccountId, Event, EventId, GroupId, OutboundId, UnsignedEvent};
use cgka_traits::storage::StorageError;

/// Everything the session needs from the host.
pub struct SessionConfig {
    /// The account's external signer.
    pub signer: Arc<dyn AccountSigner>,
    /// The host's durable KV port.
    pub host: Arc<dyn KvHost>,
    /// Every durable row the host holds, loaded once.
    pub rows: Vec<(Vec<u8>, Vec<u8>)>,
    /// The host's clock.
    pub clock: Arc<dyn Clock>,
    /// Destination safety policy.
    pub destination_policy: DestinationPolicy,
    /// Optional NIP-89 `client` tag on `KeyPackage` publications
    /// (1..=64 bytes).
    pub client_name: Option<String>,
}

/// Local safety policy for relay URLs before anything is published.
///
/// Every URL passes MDK's relay-URL profile check
/// ([`validate_nostr_relay_url`]); additionally, literal hosts that are
/// loopback, private or link-local, and `localhost`, are refused unless
/// `allow_loopback` — a dev/test flag, default off. DNS resolution checks
/// (public A/AAAA records) are the host transport's job: a hostname that
/// resolves to a private address is not something this layer can see.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct DestinationPolicy {
    /// Permit loopback/private/link-local literal hosts and `localhost`
    /// (tests and local relays only).
    pub allow_loopback: bool,
}

/// What [`Session::open`] produced.
pub enum Opened {
    /// A usable session.
    Ready(Box<Session>),
    /// The engine needs the account's identity proof signed by the external
    /// signer before it can open.
    NeedsAccountProof(Box<PendingProof>),
}

/// Session open failures. Fixed kinds only — no ids or payload.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum OpenError {
    /// The host rows could not be opened by the store.
    #[error("storage open failed: {0}")]
    Storage(#[from] crate::OpenError),
    /// The engine build failed for a reason other than the deferred account
    /// proof.
    #[error("engine open failed: {kind}")]
    Engine {
        /// MDK's privacy-safe error kind.
        kind: &'static str,
    },
    /// The supplied configuration is invalid.
    #[error("invalid session config: {0}")]
    InvalidConfig(&'static str),
}

/// Why [`PendingProof::provide_account_proof`] rejected the signed event.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum ProofError {
    /// The event was signed by a different account than the session's.
    #[error("proof signed by the wrong account")]
    WrongAccount,
    /// The event id does not match the proof request — signed for another
    /// MLS key or timestamp.
    #[error("proof event does not match the request")]
    WrongEvent,
    /// The signature does not verify.
    #[error("proof signature invalid")]
    InvalidSignature,
    /// Persisting the proven binding failed.
    #[error("storage failed: {0}")]
    Storage(#[from] StorageError),
    /// Rebuilding the engine with the stored proof failed.
    #[error("engine open failed: {kind}")]
    Engine {
        /// MDK's privacy-safe error kind.
        kind: &'static str,
    },
}

/// A rejected proof attempt: the pending open is handed back so the host can
/// retry (or drop it to cancel).
#[derive(Debug)]
pub struct ProofRejected {
    /// The pending open, unchanged.
    pub pending: Box<PendingProof>,
    /// Why this attempt failed.
    pub error: ProofError,
}

/// The typed pre-checks `provide_account_proof` runs before MDK's own
/// verification: the signer must be this session's account, the event id
/// must equal the request's canonical id, and the signature must verify.
/// Returns the verified 64-byte signature on success.
pub(crate) fn check_account_proof(
    account: AccountId,
    request: &AccountIdentityProofRequest,
    signed: &Event,
) -> Result<[u8; 64], ProofError> {
    if signed.pubkey != account {
        return Err(ProofError::WrongAccount);
    }
    let expected_id = request
        .proof_event_id()
        .map_err(|_| ProofError::InvalidSignature)?;
    if signed.id.0 != expected_id {
        return Err(ProofError::WrongEvent);
    }
    let nostr_signed = signed
        .to_nostr_event()
        .ok_or(ProofError::InvalidSignature)?;
    request
        .signature_from_signed_event(nostr_signed)
        .map_err(|_| ProofError::InvalidSignature)
}

/// A session open paused at the account-identity-proof handshake. Holds the
/// provider and request so `provide_account_proof` can finish the open
/// without re-deriving the device's MLS signature key.
pub struct PendingProof {
    provider: KvStorageProvider,
    request: AccountIdentityProofRequest,
    unsigned: UnsignedEvent,
    signer: Arc<dyn AccountSigner>,
    clock: Arc<dyn Clock>,
    destination_policy: DestinationPolicy,
    client_name: Option<String>,
}

impl std::fmt::Debug for PendingProof {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PendingProof").finish_non_exhaustive()
    }
}

impl PendingProof {
    /// The canonical unsigned kind-450 proof event the account signer must
    /// sign. `sign_event` on the returned value produces the event to hand
    /// back.
    #[must_use]
    pub const fn request(&self) -> &UnsignedEvent {
        &self.unsigned
    }

    /// Finish the open with the signed proof event.
    ///
    /// Typed pre-checks run before MDK's own verification: the signer must
    /// be this session's account, and the event id must equal the request's
    /// canonical id (binding it to this device's MLS key and timestamp).
    /// Any error after the pre-checks is `InvalidSignature` — no string
    /// matching.
    ///
    /// # Errors
    /// [`ProofRejected`] carrying the pending open back for retry.
    pub fn provide_account_proof(self, signed: &Event) -> Result<Session, ProofRejected> {
        match check_account_proof(self.signer.account(), &self.request, signed) {
            Ok(signature) => self.finish(signature),
            Err(error) => Err(ProofRejected {
                pending: Box::new(self),
                error,
            }),
        }
    }

    fn finish(self, signature: [u8; 64]) -> Result<Session, ProofRejected> {
        let request = self.request.clone();
        let pair = openmls::prelude::Ciphersuite::try_from(request.ciphersuite)
            .ok()
            .zip(openmls::prelude::SignatureScheme::try_from(request.signature_scheme).ok());
        let Some((ciphersuite, scheme)) = pair else {
            return Err(ProofRejected {
                pending: Box::new(self),
                error: ProofError::InvalidSignature,
            });
        };
        let Ok(component) = account_identity_proof_component(
            &request.account_identity,
            &request.mls_signature_public_key,
            ciphersuite,
            scheme,
            request.created_at,
            &FixedProofSigner::new(signature),
        ) else {
            return Err(ProofRejected {
                pending: Box::new(self),
                error: ProofError::InvalidSignature,
            });
        };
        let binding = AccountDeviceSignerBinding {
            marmot_identity: MemberId::new(request.account_identity.clone()),
            mls_signature_public_key: request.mls_signature_public_key,
            account_identity_proof: Some(component),
        };
        let provider = self.provider.clone();
        let write = provider
            .store()
            .transact(|| provider.put_account_device_signer(&binding));
        if let Err(error) = write {
            return Err(ProofRejected {
                pending: Box::new(self),
                error: ProofError::Storage(error),
            });
        }
        let latch = Arc::new(SignerLatch::new(self.provider.store().clone()));
        let engine = match build_engine(
            &self.provider,
            &self.signer,
            &self.clock,
            Arc::new(RefusingProofSigner::new()),
            &latch,
        ) {
            Ok(engine) => engine,
            Err(error) => {
                return Err(ProofRejected {
                    pending: Box::new(self),
                    error: ProofError::Engine {
                        kind: error.privacy_safe_kind(),
                    },
                });
            }
        };
        let session = Session {
            provider: self.provider.clone(),
            engine,
            signer: Arc::clone(&self.signer),
            clock: Arc::clone(&self.clock),
            destination_policy: self.destination_policy,
            client_name: self.client_name.clone(),
            signer_latch: latch,
            acked: CommitSeq::from_raw(0),
            gate: BTreeMap::new(),
            handed: HashSet::new(),
            held: HashMap::new(),
            resolutions: Vec::new(),
            held_modifiers: HashMap::new(),
            tick_events: Vec::new(),
            expiry_heap: None,
            pacing: HashMap::new(),
        };
        open_session(session).map_err(|kind| ProofRejected {
            pending: Box::new(self),
            error: ProofError::Engine { kind },
        })
    }
}

/// Build an engine over `provider` with our adapters and registration set.
fn build_engine(
    provider: &KvStorageProvider,
    signer: &Arc<dyn AccountSigner>,
    clock: &Arc<dyn Clock>,
    proof_signer: Arc<dyn AccountIdentityProofSigner>,
    latch: &Arc<SignerLatch>,
) -> Result<Engine<KvStorageProvider>, EngineError> {
    let adapter = Arc::new(SignerAdapter::new(Arc::clone(signer), Arc::clone(latch)));
    let peeler = NostrMlsPeeler::new().with_welcome_signer_arc(adapter);
    EngineBuilder::new(provider.clone())
        .identity(signer.account().0.to_vec())
        .account_identity_proof_signer(proof_signer)
        .protocol_profile(ProtocolProfile::Current)
        .supported_app_components(supported_components())
        .feature_registry(feature_registry())
        .peeler(Box::new(peeler))
        .maintenance_sources(
            Arc::new(EngineWallClock::new(Arc::clone(clock))),
            Arc::new(EngineMaintenanceRandom),
        )
        .convergence_clock(Arc::new(EngineConvergenceClock::new(Arc::clone(clock))))
        .build()
}

/// The app-component set this session advertises: the engine's mandatory set
/// plus Nostr routing (`0x8004`), account-devices (`0x800d`), the agent text
/// stream QUIC policy (`0x8006`, receive role only — see
/// [`feature_registry`]) and encrypted media v2 (`0x800b`). Deliberately
/// absent: encrypted media v1 (`0x8008`) — superseded by v2 and never
/// advertised or emitted.
fn supported_components() -> Vec<u16> {
    let mut components = default_group_components();
    components.insert(NOSTR_ROUTING_COMPONENT_ID);
    components.insert(ACCOUNT_DEVICES_COMPONENT_ID);
    components.insert(AGENT_TEXT_STREAM_QUIC_COMPONENT_ID);
    components.insert(GROUP_ENCRYPTED_MEDIA_V2_COMPONENT_ID);
    components.into_iter().collect()
}

/// The feature registry this session advertises: proposal 10 (MIP-03
/// `SelfRemove`) as Required — our `leave` path uses it — and the agent
/// text stream QUIC *receive* role as Optional (`0xf2d1` in the leaf's
/// extension set; Optional so it never becomes a group requirement, matching
/// wn's own registration level). Send (`0xf2d2`) and fanout (`0xf2d4`) are
/// never registered: this crate implements receive only.
fn feature_registry() -> FeatureRegistry {
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
        AGENT_TEXT_STREAM_QUIC_RECEIVE_FEATURE,
        CapabilityRequirement {
            requires: AGENT_TEXT_STREAM_QUIC_RECEIVE_CAPABILITY,
            level: RequirementLevel::Optional,
            description: "receive QUIC-backed agent text stream previews",
        },
    );
    registry
}

/// Hydrate a freshly built engine. All stored groups hydrate so frozen
/// publish obligations reload at restart; obligations loaded from durable
/// rows need no gate entry because the host already persisted them. The
/// durable watermark stays at zero: batches the open itself emits are not
/// host-acknowledged yet.
fn open_session(mut session: Session) -> Result<Session, &'static str> {
    session
        .engine
        .hydrate_all_stored_groups()
        .map_err(|error| error.privacy_safe_kind())?;
    Ok(session)
}

/// Every failure the session's methods produce.
///
/// Engine errors are split into the typed variants a caller can act on
/// and [`SessionError::Engine`], which carries only MDK's privacy-safe
/// kind string (diagnostic, never control flow).
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum SessionError {
    /// An internal invariant broke; the string is a fixed site label.
    #[error("internal: {0}")]
    Internal(&'static str),
    /// Durable storage failed.
    #[error("storage failed: {0}")]
    Storage(#[from] StorageError),
    /// An engine error outside the actionable set. `kind` is
    /// [`EngineError::privacy_safe_kind`].
    #[error("engine failed: {kind}")]
    Engine {
        /// MDK's privacy-safe error kind.
        kind: &'static str,
    },
    /// The account signer failed.
    #[error("signer failed: {0}")]
    Signer(#[from] SignerError),
    /// The caller supplied an event that is not a valid signed NIP-01 event.
    #[error("invalid transport event")]
    InvalidTransportEvent,
    /// The engine refused a non-default ciphersuite.
    #[error("unsupported ciphersuite: got {got}, need {required}")]
    UnsupportedCiphersuite {
        /// Offered ciphersuite.
        got: u16,
        /// Required ciphersuite.
        required: u16,
    },
    /// A caller argument is invalid. The string is a fixed field label.
    #[error("invalid input: {0}")]
    InvalidInput(&'static str),
    /// The group is not known to this device (or not hydrated).
    #[error("unknown group")]
    UnknownGroup,
    /// The account is not a member of the group.
    #[error("not a member")]
    NotAMember,
    /// The operation needs group admin rights this account does not hold.
    #[error("not a group admin")]
    NotGroupAdmin,
    /// This account is the last active admin; it must hand off admin or
    /// disband rather than deplete the set.
    #[error("last active admin cannot leave")]
    AdminDepletion,
    /// A leave was already requested for this group.
    #[error("leave already requested")]
    LeaveAlreadyRequested,
    /// A member's advertised capabilities do not cover the group's required
    /// set.
    #[error("missing required capabilities")]
    MissingRequiredCapabilities,
    /// A send was refused because the group's per-second pacing is already
    /// as far ahead of the clock as the forward-skew bound allows. Typed
    /// and retryable: nothing was staged, no KV batch was written, no
    /// in-memory state changed — retry once the clock advances.
    #[error("send rate exceeded")]
    SendRateExceeded,
    /// The group does not require `marmot.group.lifecycle.v1`; call
    /// [`Session::enable_disbanding`] first.
    #[error("disbanding not enabled")]
    DisbandingNotEnabled,
    /// A disband request or disband candidate is in progress for the
    /// group, so it cannot be forgotten.
    #[error("group is disbanding")]
    GroupDisbanding,
    /// The group is busy: an own commit is in flight or convergence input
    /// is unresolved. Retry after [`Session::tick`].
    #[error("group unsettled")]
    GroupBusy,
    /// An own-commit confirm collided on the checkpoint a previous confirm
    /// of the byte-identical commit left behind: the checkpoint id derives
    /// from the commit digest and the epoch matches, but the captured blob
    /// embeds `OpenMLS` `added_at` (stamped at merge time), so a re-confirm
    /// after convergence replayed the commit is neither an exact retry nor
    /// a new checkpoint and upstream storage answers `AlreadyExists`.
    ///
    /// The artifact was relay-accepted, so the session never rolls the
    /// commit back locally: it keeps the fanout obligation outstanding and
    /// reports this error instead. Retrying the same report repeats the
    /// collision until convergence resolves the pending through another
    /// branch (upstream defect; `tests/upstream.rs`).
    #[error("own-commit confirm conflicted with a retained checkpoint")]
    ConfirmConflict,
    /// An [`OutboundId`] no live obligation matches.
    #[error("unknown outbound")]
    UnknownOutbound,
    /// A relay URL failed the profile or safety checks.
    #[error("invalid relay url")]
    InvalidRelayUrl,
    /// The app message touches an event surface this crate reserves: kind
    /// 1200 (agent stream start — mk-core holds no `send` role), an `imeta`
    /// tag (media must go through [`Session::send_media`] so the epoch is
    /// pinned), or a `stream-start` tag.
    #[error("reserved app event surface")]
    ReservedAppEvent,
    /// The group's `GroupContext` carries no `0x800b` encrypted-media-v2
    /// component — sealing or policy reads are refused.
    #[error("encrypted media not enabled for this group")]
    MediaNotEnabled,
    /// The pinned media epoch no longer matches the group's epoch: re-seal
    /// the attachment and resend.
    #[error("media epoch stale")]
    MediaEpochStale,
    /// The group's epoch is unsettled; retry the media send after
    /// [`Session::tick`].
    #[error("media epoch unsettled")]
    MediaEpochUnsettled,
    /// No media-secret row is stored for the message: the attachment cannot
    /// be opened.
    #[error("media secret unavailable")]
    MediaSecretUnavailable,
    /// The pure media layer refused (typed, no payload echo).
    #[error("media: {0}")]
    Media(#[from] MediaError),
}

fn session_engine_error(error: EngineError) -> SessionError {
    match error {
        EngineError::UnknownGroup(_) | EngineError::GroupNotHydrated(_) => {
            SessionError::UnknownGroup
        }
        EngineError::NotAMember { .. } => SessionError::NotAMember,
        EngineError::NotGroupAdmin { .. } => SessionError::NotGroupAdmin,
        EngineError::AdminDepletion { .. } | EngineError::AdminCannotSelfRemove { .. } => {
            SessionError::AdminDepletion
        }
        EngineError::LeaveAlreadyRequested { .. } => SessionError::LeaveAlreadyRequested,
        EngineError::MissingRequiredCapabilities { .. }
        | EngineError::DisbandingUnsupportedMembers { .. } => {
            SessionError::MissingRequiredCapabilities
        }
        EngineError::DisbandingNotEnabled { .. } => SessionError::DisbandingNotEnabled,
        EngineError::UnsupportedCiphersuite { got, required } => {
            SessionError::UnsupportedCiphersuite { got, required }
        }
        EngineError::InvalidCredentialIdentity(_)
        | EngineError::InvalidAppMessagePayload(_)
        | EngineError::InvalidAccountIdentityProof(_)
        | EngineError::InvalidKeyPackageLifetime { .. }
        | EngineError::InvalidKeyPackageCapabilities { .. }
        | EngineError::NoDataAppComponent { .. }
        | EngineError::InvalidWelcome
        | EngineError::MissingWelcomeKeyPackage
        | EngineError::WelcomeNotAddressedToThisDevice
        | EngineError::WelcomeAlreadyProcessed => SessionError::InvalidInput("record"),
        EngineError::QueuedOutboundAtCapacity { .. }
        | EngineError::LinkDeviceUnsettled { .. }
        | EngineError::DeviceEntryChanged { .. }
        | EngineError::AccountEntryChanged { .. }
        | EngineError::AccountEntryUnsettled { .. }
        | EngineError::GroupRecoveryUnsettled { .. }
        | EngineError::AppMessageEpochUnsettled { .. } => SessionError::GroupBusy,
        EngineError::Storage(inner) => SessionError::Storage(inner),
        // `PendingPublish`/`Merging` mean an own commit awaits its publish
        // resolution (`EpochState::is_resolving_local_publish`): a further
        // commit-producing op is transiently refused. Other transitions —
        // `Recovering`, `Disbanded is terminal`, `DeviceGroup` — keep the
        // engine kind.
        EngineError::InvalidTransition(transition)
            if matches!(transition.from, "PendingPublish" | "Merging") =>
        {
            SessionError::GroupBusy
        }
        _ => SessionError::Engine {
            kind: error.privacy_safe_kind(),
        },
    }
}

/// One invitee of a group creation or invite.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Invitee {
    /// The invitee's kind-30443 `KeyPackage` event.
    pub key_package: Event,
    /// The invitee's inbox relays (its kind-10050 set). Persisted so the
    /// Welcome can be routed now and on regeneration.
    pub inbox_relays: Vec<String>,
}

/// A group creation request.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreateGroup {
    /// Group name.
    pub name: String,
    /// Group description.
    pub description: String,
    /// The group's Nostr relays (canonical routing component).
    pub relays: Vec<String>,
    /// Founding invitees (the creator is always a member).
    pub invitees: Vec<Invitee>,
    /// Additional founding admins besides the creator.
    pub admins: Vec<AccountId>,
    /// Additional initial app components (e.g. an encrypted-media policy and
    /// an agent-stream policy for a White-Noise-profile group), validated by
    /// the engine and staged with the create commit.
    pub components: Vec<ComponentUpdate>,
}

/// An app-component update for [`Session::update_app_components`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ComponentUpdate {
    /// The app-component id.
    pub component_id: u16,
    /// The replacement component bytes.
    pub data: Vec<u8>,
}

/// An outbound application message.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AppMessage {
    /// The inner Marmot app-event kind.
    pub kind: u16,
    /// Inner tags.
    pub tags: Vec<Vec<String>>,
    /// Inner content.
    pub content: String,
}

/// What a send produced, plus the inner event the host renders for its
/// own message. `message.id()` is the wire inner id receivers see — NOT
/// the staged outbound id.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SentMessage {
    /// The send's outcome.
    pub outcome: SendOutcome,
    /// The host-rendered inner event (id, pubkey, `created_at`, kind, tags,
    /// content).
    pub message: UnsignedEvent,
}

/// What a send produced.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum SendOutcome {
    /// Artifacts were staged to the outbox and wait on the durability gate
    /// (carries their ids).
    Staged(Vec<OutboundId>),
    /// The intent was accepted but retained behind convergence or an
    /// earlier send; it regenerates on a later [`Session::tick`].
    Queued,
    /// The change was already canonical; nothing to publish.
    NoChange,
    /// The leave request is durable; the engine stages step-down and
    /// `SelfRemove` as convergence permits.
    LeaveRequested,
    /// The disband request is durable; the engine prepares the terminal
    /// Commit asynchronously after any staged publish and the convergence
    /// gate permit it.
    DisbandRequested,
}

/// One attachment to seal: plaintext plus the metadata the `imeta`
/// reference commits to.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AttachmentInput {
    /// The file bytes to encrypt.
    pub plaintext: Vec<u8>,
    /// The media type; canonicalized before sealing (`image/jpg` →
    /// `image/jpeg`, parameters stripped).
    pub media_type: String,
    /// The filename (1..=255 UTF-8 bytes, no NUL).
    pub filename: String,
    /// Optional `WxH` dimensions, verbatim.
    pub dim: Option<String>,
    /// Optional thumbhash, verbatim.
    pub thumbhash: Option<String>,
}

/// The result of [`Session::seal_attachment`]: the blob the host uploads
/// and the opaque handle [`Session::send_media`] needs to emit the
/// reference under the epoch it was sealed at.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct SealedAttachment {
    /// The encrypted blob (`plaintext.len() + 16` bytes).
    pub ciphertext: Vec<u8>,
    /// SHA-256 of `ciphertext` — the Blossom blob id the host uploads
    /// under and the locator's hash segment.
    pub ciphertext_sha256: [u8; 32],
    /// Opaque send-time handle: the reference fields minus locators plus
    /// the epoch pin the ciphertext's key material binds to. No secret.
    pub pending: PendingAttachment,
}

/// The send-time half of a sealed attachment: every `imeta` field except
/// locators, plus the [`EpochPin`] the media key was derived under. Opaque
/// to the host — construct it only via [`Session::seal_attachment`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PendingAttachment {
    /// SHA-256 of the ciphertext blob.
    pub(crate) ciphertext_sha256: [u8; 32],
    /// SHA-256 of the plaintext.
    pub(crate) plaintext_sha256: [u8; 32],
    /// The AEAD nonce sampled at seal time.
    pub(crate) nonce: [u8; 12],
    /// The canonical media type recorded at seal time.
    pub(crate) media_type: String,
    /// The filename recorded at seal time.
    pub(crate) filename: String,
    /// Optional dimensions recorded at seal time.
    pub(crate) dim: Option<String>,
    /// Optional thumbhash recorded at seal time.
    pub(crate) thumbhash: Option<String>,
    /// The epoch the media secret was exported at.
    pub(crate) pin: EpochPin,
}

/// A kind-9 chat message carrying sealed attachments.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MediaMessage {
    /// The message body (may be empty).
    pub caption: String,
    /// One `(pending, locators)` pair per sealed attachment, in emission
    /// order. At least one; every locator's kind must be in the group's
    /// `allowed_locator_kinds`.
    pub attachments: Vec<(PendingAttachment, Vec<Locator>)>,
    /// Extra tags for the inner event (e.g. NIP-30 emoji). `imeta` and
    /// `stream-start` are reserved and refused.
    pub tags: Vec<Vec<String>>,
}

/// What [`Session::tick`] reports.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TickReport {
    /// Milliseconds until the earliest scheduled convergence wake, if any
    /// group has one pending. `None` means nothing scheduled — tick again
    /// on new input only.
    pub next_wake_ms: Option<u64>,
}

/// A group's lifecycle bucket for UI projection.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum GroupState {
    /// Canonical and ingestible.
    Stable,
    /// An own commit awaits its publish resolution.
    PendingPublish,
    /// Convergence is selecting between forks.
    Recovering,
    /// Frozen pending a verified repair path.
    Unrecoverable,
    /// This device's copy ended (left, removed, or reset).
    Removed,
    /// Terminally disbanded.
    Disbanded,
}

/// One member in a [`GroupSnapshot`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MemberSnapshot {
    /// The member's account.
    pub account: AccountId,
    /// Whether the member is in the admin-policy set.
    pub admin: bool,
}

/// A UI-facing snapshot of one group.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GroupSnapshot {
    /// The MLS group id.
    pub id: GroupId,
    /// Profile name.
    pub name: String,
    /// Profile description.
    pub description: String,
    /// Current epoch.
    pub epoch: u64,
    /// Lifecycle bucket.
    pub state: GroupState,
    /// Members (account + admin flag).
    pub members: Vec<MemberSnapshot>,
    /// The group's Nostr routing relays.
    pub relays: Vec<String>,
    /// The 32-byte Nostr route id (`#h` filter value) for fetching this
    /// group's transport feed. Distinct from [`GroupId`].
    pub nostr_group_id: [u8; 32],
    /// Whether this account is an admin.
    pub own_account_admin: bool,
    /// The group's `0x800b` encrypted-media policy, when the component is
    /// present and decodes.
    pub encrypted_media: Option<EncryptedMediaPolicy>,
    /// The group's `0x8006` agent text stream policy, when the component is
    /// present and decodes.
    pub agent_text_stream: Option<AgentStreamPolicy>,
    /// The group's `0x8007` avatar, present only for a non-empty URL.
    pub avatar: Option<GroupAvatar>,
    /// The group's `0x8005` message-retention seconds; `0` means disabled.
    pub retention_seconds: u64,
    /// Whether the group requires `0x800c` and its lifecycle component is
    /// `Active`.
    pub disband_enabled: bool,
    /// Whether a disband is in progress
    /// (`engine.disbanding_in_progress`).
    pub disbanding: bool,
}

/// The group's `0x8007` avatar-url component, decoded.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GroupAvatar {
    /// The normalized avatar URL (https-only, no userinfo, no fragment).
    pub url: String,
    /// Optional `WxH` hint.
    pub dim: Option<String>,
    /// Optional thumbhash hint.
    pub thumbhash: Option<String>,
}

/// The account-device session.
///
/// Single-writer: methods take `&mut self`; the store's layered view has
/// exactly one writer, which is also the argument that makes engine
/// `with_read_snapshot` scopes consistent.
pub struct Session {
    pub(crate) provider: KvStorageProvider,
    engine: Engine<KvStorageProvider>,
    signer: Arc<dyn AccountSigner>,
    clock: Arc<dyn Clock>,
    destination_policy: DestinationPolicy,
    client_name: Option<String>,
    /// Cumulative durable watermark reported by the host.
    acked: CommitSeq,
    /// Outbound id -> seq of the batch that made it releasable.
    gate: BTreeMap<OutboundId, CommitSeq>,
    /// Items handed out this incarnation and not yet reported.
    handed: HashSet<OutboundId>,
    /// The in-memory fanout that marked each in-flight fanout's current
    /// attempts (mdk#2332). Only it can report those attempts as unexposed:
    /// a fanout value loaded from storage holds no attempt marks, so results
    /// recorded on it always count as possible exposure. Kept only while it
    /// equals the stored record (its `PartialEq` ignores the marks); dropped
    /// when the fanout leaves storage or the stored record moves ahead of it
    /// (for example after a restart, where every interrupted attempt counts
    /// as exposed — the intended semantics).
    held: HashMap<OutboundId, OutboundFanout>,
    /// Publish resolutions produced since the last `drain_events`.
    resolutions: Vec<SessionEvent>,
    /// The signer-refusal latch shared with the engine's `SignerAdapter`:
    /// armed around each kind-1059 ingest so a refused/failed unwrap can
    /// roll the span back instead of poisoning it as a permanent rejection.
    signer_latch: Arc<SignerLatch>,
    /// Held kind 1009/5 modifiers per group, arrival order. Memory-only:
    /// an unacked pending row re-presents the modifier after restart, so
    /// holds are crash-safe without a durable table. Keyed by group bytes.
    pub(crate) held_modifiers: HashMap<Vec<u8>, VecDeque<HeldModifier>>,
    /// Events `tick` produced (hold expiries, retention sweeps) for the
    /// next `drain_events`.
    pub(crate) tick_events: Vec<SessionEvent>,
    /// Lazily-built min-heap over index-row expiries — `None` until the
    /// first `tick` seeds it from `Table::DeliveredMessage`.
    pub(crate) expiry_heap: Option<ExpiryHeap>,
    /// Per-group send-pacing allocator state, keyed by group bytes.
    pub(crate) pacing: PacingMap,
}

impl std::fmt::Debug for Session {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Session").finish_non_exhaustive()
    }
}

impl std::fmt::Debug for SessionConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SessionConfig")
            .field("destination_policy", &self.destination_policy)
            .field("client_name", &self.client_name)
            .finish_non_exhaustive()
    }
}

impl std::fmt::Debug for Opened {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Ready(_) => f.write_str("Opened::Ready(..)"),
            Self::NeedsAccountProof(_) => f.write_str("Opened::NeedsAccountProof(..)"),
        }
    }
}

impl Session {
    /// Open (or create) the account's session over `config.rows`.
    ///
    /// If the engine needs an account-identity proof this device has not
    /// yet produced, returns [`Opened::NeedsAccountProof`]; hand the
    /// pending request to the signer and finish with
    /// [`PendingProof::provide_account_proof`].
    ///
    /// # Errors
    /// [`OpenError`] for storage, engine-build and configuration failures.
    pub fn open(config: SessionConfig) -> Result<Opened, OpenError> {
        if let Some(name) = &config.client_name
            && !is_valid_client_name(name)
        {
            return Err(OpenError::InvalidConfig("client name"));
        }
        let provider = KvStorageProvider::open(config.rows, config.host)?;
        let capture = Arc::new(DeferringProofSigner::new());
        let cloned = Arc::clone(&capture);
        let proof_signer: Arc<dyn AccountIdentityProofSigner> = cloned;
        let signer_latch = Arc::new(SignerLatch::new(provider.store().clone()));
        match build_engine(
            &provider,
            &config.signer,
            &config.clock,
            proof_signer,
            &signer_latch,
        ) {
            Err(error) => {
                let Some(request) = capture.captured() else {
                    return Err(open_error_of(&error));
                };
                let unsigned = proof_unsigned(&request)?;
                Ok(Opened::NeedsAccountProof(Box::new(PendingProof {
                    provider,
                    request,
                    unsigned,
                    signer: config.signer,
                    clock: config.clock,
                    destination_policy: config.destination_policy,
                    client_name: config.client_name,
                })))
            }
            Ok(engine) => open_session(Self {
                provider,
                engine,
                signer: config.signer,
                clock: config.clock,
                destination_policy: config.destination_policy,
                client_name: config.client_name,
                signer_latch,
                acked: CommitSeq::from_raw(0),
                gate: BTreeMap::new(),
                handed: HashSet::new(),
                held: HashMap::new(),
                resolutions: Vec::new(),
                held_modifiers: HashMap::new(),
                tick_events: Vec::new(),
                expiry_heap: None,
                pacing: HashMap::new(),
            })
            .map(|session| Opened::Ready(Box::new(session)))
            .map_err(|kind| OpenError::Engine { kind }),
        }
    }

    /// The session's account.
    #[must_use]
    pub fn account(&self) -> AccountId {
        self.signer.account()
    }

    /// The session clock's current reading.
    #[must_use]
    pub fn now(&self) -> Time {
        self.clock.now()
    }

    pub(crate) fn now_ms(&self) -> u64 {
        self.clock.now().unix_ms
    }

    // ---- durability gate + outbox ----

    /// Mark every batch with `seq` <= `seq` durable. Cumulative — a smaller
    /// seq than the current watermark is a no-op.
    pub fn acknowledge_durable(&mut self, seq: CommitSeq) {
        if seq > self.acked {
            self.acked = seq;
        }
    }

    /// The highest seq some releasable-pending outbound still waits on.
    #[must_use]
    pub fn awaiting_durability(&self) -> Option<CommitSeq> {
        self.gate
            .values()
            .filter(|seq| **seq > self.acked)
            .max()
            .copied()
    }

    /// Outbound items the host may publish now: durable under the gate and
    /// not already handed out unreported this incarnation. Each item's
    /// `relays` lists only its still-outstanding targets; after a
    /// [`Session::report_publish`] the remaining targets are released again
    /// by the next call.
    ///
    /// # Errors
    /// [`SessionError`] for storage/engine failures while listing or
    /// re-arming fanouts.
    pub fn take_outbound(&mut self) -> Result<Vec<Outbound>, SessionError> {
        let mut released = Vec::new();
        self.take_outbound_inner(&mut released)?;
        Ok(released)
    }

    fn take_outbound_inner(&mut self, released: &mut Vec<Outbound>) -> Result<(), SessionError> {
        let now_ms = self.now_ms();
        let retries: HashMap<OutboundId, crate::outbox::OutboundRetry> = self
            .provider
            .outbound_retries()
            .map_err(SessionError::Storage)?
            .into_iter()
            .collect();
        let fanouts = self
            .engine
            .outbound_fanouts()
            .map_err(session_engine_error)?;
        // Drop held values whose fanout left storage or was overwritten by
        // a path that did not carry this session's marks — the equality
        // check below would never select them again.
        let mut live: HashSet<OutboundId> = fanouts
            .iter()
            .filter_map(|fanout| outbound_id_of(fanout.message_id()).ok())
            .collect();
        self.held.retain(|id, _| live.contains(id));
        let mut stale_retries: Vec<OutboundId> = retries
            .keys()
            .filter(|id| !live.contains(*id))
            .copied()
            .collect();
        for stored in fanouts {
            let id = outbound_id_of(stored.message_id())?;
            if self.handed.contains(&id) || !self.releasable(&id) {
                continue;
            }
            // Backoff: a report that left the item outstanding on retryable
            // verdicts re-offers it only after `not_before_ms`.
            if retries
                .get(&id)
                .is_some_and(|retry| retry.not_before_ms > now_ms)
            {
                continue;
            }
            // Re-mark on the held value, not the fresh storage load: only
            // the value that marked an attempt can report it unexposed, and
            // the load holds no marks (mdk#2332). A diverged held value is
            // stale — the stored record then already counts those attempts
            // as exposed.
            let mut fanout = match self.held.get(&id) {
                Some(held) if *held == stored => held.clone(),
                _ => stored,
            };
            if self.rearm_retryable(&mut fanout)? {
                self.engine
                    .put_outbound_fanout(&fanout)
                    .map_err(session_engine_error)?;
                self.held.insert(id, fanout.clone());
            }
            let Some(outbound) = fanout_outbound(&fanout, id) else {
                continue;
            };
            self.handed.insert(id);
            released.push(outbound);
        }
        for publication in self
            .provider
            .session_publications()
            .map_err(SessionError::Storage)?
        {
            let paced = OutboundId::from_hex(&publication.id).is_ok_and(|id| {
                live.insert(id);
                retries
                    .get(&id)
                    .is_some_and(|retry| retry.not_before_ms > now_ms)
            });
            if paced {
                continue;
            }
            self.take_publication(publication, released)?;
        }
        // Cheap GC: a retry row whose item resolved elsewhere (or was never
        // the current record's) is dead weight — delete it only when there
        // is something to delete.
        stale_retries.retain(|id| !live.contains(id));
        self.delete_stale_retries(&stale_retries)
    }

    /// Drop retry rows whose item is no longer live, in one storage unit.
    /// Writes nothing when `stale` is empty.
    fn delete_stale_retries(&self, stale: &[OutboundId]) -> Result<(), SessionError> {
        if stale.is_empty() {
            return Ok(());
        }
        self.provider
            .store()
            .transact(|| -> Result<(), StorageError> {
                for id in stale {
                    self.provider.delete_outbound_retry(id)?;
                }
                Ok(())
            })
            .map_err(SessionError::Storage)
    }

    /// Every outbound id with a live record — engine fanouts plus session
    /// publications — for retry-deadline scheduling.
    fn live_outbound_ids(&self) -> Result<HashSet<OutboundId>, SessionError> {
        let mut live: HashSet<OutboundId> = self
            .engine
            .outbound_fanouts()
            .map_err(session_engine_error)?
            .iter()
            .filter_map(|fanout| outbound_id_of(fanout.message_id()).ok())
            .collect();
        for publication in self
            .provider
            .session_publications()
            .map_err(SessionError::Storage)?
        {
            if let Ok(id) = OutboundId::from_hex(&publication.id) {
                live.insert(id);
            }
        }
        Ok(live)
    }

    /// Milliseconds until the earliest retry deadline on a live, not-handed
    /// item — `None` when none is armed. A due item contributes nothing:
    /// the take that follows every tick picks it up, so reporting 0 would
    /// only spin.
    fn retry_wake(&self) -> Result<Option<u64>, SessionError> {
        let retries = self
            .provider
            .outbound_retries()
            .map_err(SessionError::Storage)?;
        if retries.is_empty() {
            return Ok(None);
        }
        let now_ms = self.now_ms();
        let live = self.live_outbound_ids()?;
        let mut wake = None;
        for (id, retry) in retries {
            if live.contains(&id) && !self.handed.contains(&id) && retry.not_before_ms > now_ms {
                let delay = retry.not_before_ms - now_ms;
                wake = Some(wake.map_or(delay, |armed: u64| armed.min(delay)));
            }
        }
        Ok(wake)
    }

    /// Re-mark retryable/possibly-exposed targets as Attempting so a relay
    /// cannot distinguish a restart retry from a first attempt — except that
    /// possible exposure stays recorded on the fanout itself.
    /// Returns whether the fanout changed.
    fn rearm_retryable(&self, fanout: &mut OutboundFanout) -> Result<bool, SessionError> {
        let mut remarked = false;
        for index in fanout.outstanding_target_indexes() {
            let retryable = matches!(
                fanout.target_status(index),
                Some(
                    FanoutTargetStatus::PossiblyExposed | FanoutTargetStatus::RetryableUnavailable
                )
            );
            if !retryable {
                continue;
            }
            fanout
                .mark_attempt_started_at(index, self.now_ms())
                .map_err(|_| SessionError::Internal("fanout mark"))?;
            remarked = true;
        }
        Ok(remarked)
    }

    /// Release one session-owned publication if the gate allows.
    fn take_publication(
        &mut self,
        publication: SessionPublication,
        released: &mut Vec<Outbound>,
    ) -> Result<(), SessionError> {
        let id = OutboundId::from_hex(&publication.id)
            .map_err(|_| SessionError::Internal("session publication id"))?;
        if self.handed.contains(&id) || !self.releasable(&id) {
            return Ok(());
        }
        let outstanding: Vec<usize> = publication
            .statuses
            .iter()
            .enumerate()
            .filter(|(_, status)| status.is_outstanding())
            .map(|(index, _)| index)
            .collect();
        if outstanding.is_empty() {
            return Ok(());
        }
        let mut publication = publication;
        for index in &outstanding {
            let Some(status) = publication.statuses.get_mut(*index) else {
                continue;
            };
            if matches!(
                *status,
                PubTargetStatus::PossiblyExposed | PubTargetStatus::RetryableUnavailable
            ) {
                *status = PubTargetStatus::Attempting;
            }
        }
        self.provider
            .put_session_publication(&publication)
            .map_err(SessionError::Storage)?;
        let event = Event::from_json(&publication.event_json)
            .map_err(|_| SessionError::Internal("session publication event"))?;
        let relays = outstanding
            .iter()
            .filter_map(|index| publication.relays.get(*index).cloned())
            .collect();
        self.handed.insert(id);
        released.push(Outbound {
            id,
            event,
            relays,
            purpose: match publication.purpose {
                SessionPubPurpose::KeyPackage => OutboundPurpose::KeyPackage,
                SessionPubPurpose::InboxRelays => OutboundPurpose::InboxRelays,
            },
        });
        Ok(())
    }

    /// Whether `id`'s gate seq is covered by the durable watermark. Items
    /// with no recorded gate seq were loaded from durable rows — the host
    /// already persisted them, so they are releasable.
    fn releasable(&self, id: &OutboundId) -> bool {
        self.gate.get(id).is_none_or(|seq| *seq <= self.acked)
    }

    /// Record the host's per-relay publish verdicts for one outbound item
    /// and resolve whatever the fanout state machine can now settle:
    /// first acceptance with pending MLS state confirms the commit (and
    /// stages its post-confirmation Welcomes); all-failed with no possible
    /// exposure rolls it back; fully terminal records are released.
    ///
    /// # Errors
    /// [`SessionError::UnknownOutbound`] when `id` is not a live
    /// obligation; [`SessionError::ConfirmConflict`] when an accepted
    /// own commit's confirm collides on a retained checkpoint (upstream
    /// defect — the obligation stays outstanding and is never rolled
    /// back); [`SessionError`] for storage/engine failures.
    pub async fn report_publish(
        &mut self,
        id: &OutboundId,
        results: &[RelayResult],
    ) -> Result<PublishResolution, SessionError> {
        self.handed.remove(id);
        if let Some(resolution) = self
            .report_session_publication(id, results)
            .map_err(SessionError::Storage)?
        {
            self.resolutions.push(SessionEvent::PublishResolved {
                outbound: *id,
                resolution,
            });
            return Ok(resolution);
        }
        let stored = self
            .provider
            .outbound_fanout(&MessageId::new(id.0.to_vec()))
            .map_err(SessionError::Storage)?
            .filter(|fanout| {
                fanout
                    .group_id()
                    .is_none_or(|group_id| self.engine.check_group_live(group_id).is_ok())
            })
            .ok_or(SessionError::UnknownOutbound)?;
        // Record results on the value that marked the attempts while it
        // still matches the stored record (`PartialEq` ignores the attempt
        // marks): only that value can report them unexposed (mdk#2332).
        let fanout = match self.held.get(id) {
            Some(held) if *held == stored => held.clone(),
            _ => stored,
        };
        let resolution = self.report_fanout(fanout, results).await?;
        self.resolutions.push(SessionEvent::PublishResolved {
            outbound: *id,
            resolution,
        });
        Ok(resolution)
    }

    fn report_session_publication(
        &self,
        id: &OutboundId,
        results: &[RelayResult],
    ) -> Result<Option<PublishResolution>, StorageError> {
        let Some(mut publication) = self.provider.session_publication(id)? else {
            return Ok(None);
        };
        for result in results {
            let Some(index) = publication
                .relays
                .iter()
                .position(|relay| relay == &result.relay)
            else {
                continue;
            };
            let Some(status) = publication.statuses.get_mut(index) else {
                continue;
            };
            *status = match result.status {
                RelayStatus::Accepted => PubTargetStatus::Accepted,
                RelayStatus::Rejected => PubTargetStatus::Failed,
                RelayStatus::Unavailable | RelayStatus::NotSent => {
                    PubTargetStatus::RetryableUnavailable
                }
                RelayStatus::Unknown => PubTargetStatus::PossiblyExposed,
            };
        }
        let complete = publication
            .statuses
            .iter()
            .all(|status| !status.is_outstanding());
        let accepted = results
            .iter()
            .any(|result| result.status == RelayStatus::Accepted);
        let retryable = results.iter().any(|result| retryable_status(result.status));
        let now_ms = self.now_ms();
        self.provider
            .store()
            .transact(|| -> Result<(), StorageError> {
                if complete {
                    self.provider.delete_session_publication(id)?;
                    self.provider.delete_outbound_retry(id)?;
                    return Ok(());
                }
                self.provider.put_session_publication(&publication)?;
                if accepted {
                    // One accepted target resets the backoff.
                    self.provider.delete_outbound_retry(id)?;
                } else if retryable {
                    self.provider.record_outbound_failure(id, now_ms)?;
                }
                Ok(())
            })?;
        if complete {
            return Ok(Some(PublishResolution::Complete));
        }
        Ok(Some(PublishResolution::Pending))
    }

    async fn report_fanout(
        &mut self,
        mut fanout: OutboundFanout,
        results: &[RelayResult],
    ) -> Result<PublishResolution, SessionError> {
        let endpoints = fanout.request().target.endpoints().to_vec();
        for result in results {
            let Some(index) = endpoints
                .iter()
                .position(|endpoint| endpoint.0 == result.relay)
            else {
                continue;
            };
            let Some(endpoint) = endpoints.get(index) else {
                return Err(SessionError::Internal("fanout endpoint"));
            };
            match result.status {
                RelayStatus::Accepted => {
                    fanout
                        .mark_target_accepted(index)
                        .map_err(|_| SessionError::Internal("fanout accept"))?;
                    fanout
                        .observe_first_acceptance(PublicationObservation {
                            at_ms: self.now_ms(),
                        })
                        .map_err(|_| SessionError::Internal("fanout observation"))?;
                }
                status => {
                    fanout
                        .record_target_failure(
                            index,
                            TransportEndpointFailure {
                                endpoint: endpoint.clone(),
                                reason: "reported".to_owned(),
                                kind: endpoint_failure_kind(status),
                                rejection_category: None,
                                size_class: None,
                                // `NotSent` proves no frame left: the
                                // target stays retryable without exposure.
                                // Every other relay-reported failure can
                                // never prove nothing left — `Unavailable`
                                // covers timeouts and silent drops, where
                                // a frame may have reached the relay.
                                pre_send: matches!(status, RelayStatus::NotSent),
                            },
                        )
                        .map_err(|_| SessionError::Internal("fanout failure"))?;
                }
            }
        }
        let id = outbound_id_of(fanout.message_id())?;
        let outcome = fanout.outcome();
        let mut resolution = PublishResolution::Pending;
        if outcome.mls_confirmation_required {
            resolution = self.confirm_fanout_mls(&mut fanout).await?;
        } else if outcome.fanout_complete
            && outcome.accepted_targets == 0
            && !fanout.possible_exposure()
            && let Some(pending) = fanout.pending_ref()
        {
            self.engine
                .publish_failed_fanout(pending, &mut fanout)
                .await
                .map_err(session_engine_error)?;
            resolution = PublishResolution::RolledBack;
        }
        if matches!(
            fanout.mls_state(),
            cgka_traits::transport_adapter::FanoutMlsState::Confirmed
        ) && !fanout.pending_post_confirmation_welcomes().is_empty()
        {
            let welcomes = fanout.pending_post_confirmation_welcomes().to_vec();
            for welcome in &welcomes {
                self.stage_welcome(welcome)?;
            }
            if fanout.mark_post_confirmation_welcomes_released() {
                self.engine
                    .put_outbound_fanout(&fanout)
                    .map_err(session_engine_error)?;
            }
        }
        // A regenerated standalone artifact's durable intent ends on first
        // acceptance or on all-terminal failure; ambiguous outcomes re-arm
        // it (marmot-session `resolve_regenerated_queued_intent`,
        // runtime.rs:4943). The association is the durable link row staged
        // with the fanout, so a restart between stage and report still
        // retires the intent exactly once.
        if let Some((group_id, intent_id)) = self
            .provider
            .queued_intent_link(fanout.message_id())
            .map_err(SessionError::Storage)?
        {
            self.provider
                .delete_queued_intent_link(fanout.message_id())
                .map_err(SessionError::Storage)?;
            let final_outcome = fanout.outcome();
            if final_outcome.accepted_targets > 0
                || (final_outcome.fanout_complete && !fanout.possible_exposure())
            {
                self.engine
                    .confirm_regenerated_queued_intent(&intent_id)
                    .map_err(session_engine_error)?;
            } else if !final_outcome.fanout_complete {
                self.engine
                    .retry_regenerated_queued_intent(&group_id, &intent_id);
            } else {
                self.engine
                    .confirm_regenerated_queued_intent(&intent_id)
                    .map_err(session_engine_error)?;
            }
        }
        if fanout.outcome().fanout_complete {
            self.provider
                .store()
                .transact(|| -> Result<(), SessionError> {
                    self.engine
                        .delete_outbound_fanout(fanout.message_id())
                        .map_err(session_engine_error)?;
                    self.provider
                        .delete_outbound_retry(&id)
                        .map_err(SessionError::Storage)?;
                    Ok(())
                })?;
            self.gate.remove(&id);
            self.held.remove(&id);
            if resolution == PublishResolution::Pending {
                resolution = PublishResolution::Complete;
            }
        } else {
            self.persist_retry_record(&fanout, &id, results)?;
            // The in-flight fanout stays outstanding: keep the held value —
            // which still marks every unreported current attempt — current
            // with what was persisted (mdk#2332).
            self.held.insert(id, fanout.clone());
        }
        Ok(resolution)
    }

    /// Persist the still-outstanding fanout plus this report's retry pacing
    /// in one storage unit: one accepted target resets the backoff; an
    /// all-retryable report records another failure.
    fn persist_retry_record(
        &self,
        fanout: &OutboundFanout,
        id: &OutboundId,
        results: &[RelayResult],
    ) -> Result<(), SessionError> {
        let accepted = results
            .iter()
            .any(|result| result.status == RelayStatus::Accepted);
        let retryable = results.iter().any(|result| retryable_status(result.status));
        let now_ms = self.now_ms();
        self.provider
            .store()
            .transact(|| -> Result<(), SessionError> {
                self.engine
                    .put_outbound_fanout(fanout)
                    .map_err(session_engine_error)?;
                if accepted {
                    // One accepted target resets the backoff.
                    self.provider
                        .delete_outbound_retry(id)
                        .map_err(SessionError::Storage)?;
                } else if retryable {
                    self.provider
                        .record_outbound_failure(id, now_ms)
                        .map_err(SessionError::Storage)?;
                }
                Ok(())
            })
    }

    /// Confirm the fanout's own-commit obligation with the engine once at
    /// least one relay accepted the artifact.
    async fn confirm_fanout_mls(
        &mut self,
        fanout: &mut OutboundFanout,
    ) -> Result<PublishResolution, SessionError> {
        let pending = fanout
            .pending_ref()
            .ok_or(SessionError::Internal("fanout pending"))?;
        match self.engine.confirm_published_fanout(pending, fanout).await {
            Ok(_) => Ok(PublishResolution::Confirmed),
            // A relay already accepted the artifact, so the commit must
            // never be rolled back locally. Persist the fanout with the
            // relay verdicts it already collected, keep the obligation
            // outstanding and surface the conflict — see
            // `SessionError::ConfirmConflict`.
            Err(EngineError::Storage(StorageError::AlreadyExists)) => {
                self.engine
                    .put_outbound_fanout(fanout)
                    .map_err(session_engine_error)?;
                if let Ok(id) = outbound_id_of(fanout.message_id()) {
                    self.held.insert(id, fanout.clone());
                }
                Err(SessionError::ConfirmConflict)
            }
            Err(error) => Err(session_engine_error(error)),
        }
    }

    // ---- account publications ----

    /// Build, sign and stage the account's kind-30443 `KeyPackage`
    /// publication. Returns the parsed info of the published package.
    ///
    /// # Errors
    /// [`SessionError`] for signer, engine or storage failures.
    pub async fn publish_key_package(
        &mut self,
        relays: &[String],
    ) -> Result<KeyPackageInfo, SessionError> {
        self.check_relays(relays)?;
        let key_package = self
            .engine
            .fresh_key_package()
            .await
            .map_err(session_engine_error)?;
        let slot = self
            .provider
            .key_package_slot()
            .map_err(SessionError::Storage)?;
        let unsigned = key_package::key_package_event(
            self.account(),
            &key_package.bytes,
            &hex::encode(slot),
            self.client_name.as_deref(),
            self.clock.now().unix_ms / 1_000,
        )
        .map_err(|_| SessionError::Internal("key package build"))?;
        let signed = self.sign_checked(unsigned).await?;
        let info = key_package::key_package_info(&signed)
            .map_err(|_| SessionError::Internal("key package verify"))?;
        self.stage_publication(&signed, relays, SessionPubPurpose::KeyPackage)?;
        Ok(info)
    }

    /// Build, sign and stage the account's kind-10050 inbox relay list.
    ///
    /// # Errors
    /// [`SessionError`] for signer, engine or storage failures.
    pub async fn publish_inbox_relays(
        &mut self,
        relays: &[String],
    ) -> Result<OutboundId, SessionError> {
        self.check_relays(relays)?;
        let unsigned = key_package::inbox_relays_event(
            self.account(),
            relays,
            self.clock.now().unix_ms / 1_000,
        );
        let signed = self.sign_checked(unsigned).await?;
        self.stage_publication(&signed, relays, SessionPubPurpose::InboxRelays)?;
        Ok(OutboundId(signed.id.0))
    }

    /// Sign `unsigned` through the account signer and sanity-check the
    /// result (right account, right id commitment).
    async fn sign_checked(&self, unsigned: UnsignedEvent) -> Result<Event, SessionError> {
        let signed = self
            .signer
            .sign_event(unsigned)
            .await
            .map_err(SessionError::Signer)?;
        if signed.pubkey != self.account() || signed.id != signed.compute_id() {
            return Err(SessionError::Signer(SignerError::Refused));
        }
        Ok(signed)
    }

    fn stage_publication(
        &mut self,
        event: &Event,
        relays: &[String],
        purpose: SessionPubPurpose,
    ) -> Result<(), SessionError> {
        let publication = SessionPublication {
            id: event.id.to_hex(),
            event_json: event.to_json(),
            relays: relays.to_vec(),
            statuses: vec![PubTargetStatus::Attempting; relays.len()],
            purpose,
        };
        self.provider
            .store()
            .transact(|| self.provider.put_session_publication(&publication))
            .map_err(SessionError::Storage)?;
        if let Some(seq) = self.provider.last_seq() {
            self.gate.insert(OutboundId(event.id.0), seq);
        }
        Ok(())
    }

    // ---- groups and messages ----

    /// Create a group: current profile, random `nostr_group_id`, Nostr
    /// routing component carrying `relays`, founding admins = creator +
    /// `req.admins`. Every invitee's inbox relays are persisted for Welcome
    /// routing.
    ///
    /// # Errors
    /// [`SessionError`] for invalid inputs or engine failures.
    pub async fn create_group(&mut self, req: CreateGroup) -> Result<GroupId, SessionError> {
        self.check_relays(&req.relays)?;
        check_agent_stream_roles(&req.components)?;
        let mut nostr_group_id = [0u8; 32];
        getrandom::fill(&mut nostr_group_id).map_err(|_| SessionError::Internal("rng"))?;
        let routing = NostrRoutingV1::new(nostr_group_id, req.relays.clone())
            .map_err(|_| SessionError::InvalidRelayUrl)?;
        let mut app_components = vec![AppComponentData {
            component_id: NOSTR_ROUTING_COMPONENT_ID,
            data: cgka_traits::app_components::encode_nostr_routing_v1(&routing)
                .map_err(|_| SessionError::Internal("routing encode"))?,
        }];
        app_components.extend(req.components.iter().map(|update| AppComponentData {
            component_id: update.component_id,
            data: update.data.clone(),
        }));
        let mut members = Vec::with_capacity(req.invitees.len());
        for invitee in &req.invitees {
            members.push(self.invitee_key_package(invitee)?);
        }
        let (group_id, result) = self
            .engine
            .create_group(CreateGroupRequest {
                name: req.name,
                description: req.description,
                members,
                required_features: Vec::new(),
                app_components,
                initial_admins: req
                    .admins
                    .iter()
                    .map(|account| MemberId::new(account.0.to_vec()))
                    .collect(),
            })
            .await
            .map_err(session_engine_error)?;
        let artifacts = self.drain_engine_artifacts();
        self.stage_send_results(std::iter::once(result).chain(artifacts))?;
        Ok(GroupId(group_id.as_slice().to_vec()))
    }

    /// Validate an invitee's `KeyPackage` event, persist its inbox relays and
    /// produce the engine `KeyPackage`.
    fn invitee_key_package(&self, invitee: &Invitee) -> Result<KeyPackage, SessionError> {
        let info = key_package::key_package_info(&invitee.key_package)
            .map_err(|_| SessionError::InvalidInput("key package"))?;
        self.check_relays(&invitee.inbox_relays)?;
        self.provider
            .put_inbox_relays(&info.account, &invitee.inbox_relays)
            .map_err(SessionError::Storage)?;
        Ok(KeyPackage {
            bytes: info.key_package,
            source: Some(KeyPackageSource {
                event_id: MessageId::new(info.event_id.0.to_vec()),
            }),
            protocol_profile: ProtocolProfile::Current,
        })
    }

    /// Invite members to an existing group.
    ///
    /// # Errors
    /// [`SessionError`] for invalid inputs or engine failures.
    pub async fn invite(
        &mut self,
        group: &GroupId,
        invitees: Vec<Invitee>,
    ) -> Result<SendOutcome, SessionError> {
        self.require_admin(group)?;
        let key_packages = invitees
            .iter()
            .map(|invitee| self.invitee_key_package(invitee))
            .collect::<Result<Vec<_>, _>>()?;
        let result = self
            .engine
            .send(SendIntent::Invite {
                group_id: engine_gid(group),
                key_packages,
                initial_admins: Vec::new(),
            })
            .await
            .map_err(session_engine_error)?;
        self.finish_send(result)
    }

    /// Remove member accounts from a group (admin Commit).
    ///
    /// # Errors
    /// [`SessionError`] for engine failures.
    pub async fn remove_members(
        &mut self,
        group: &GroupId,
        members: &[AccountId],
    ) -> Result<SendOutcome, SessionError> {
        self.require_admin(group)?;
        let result = self
            .engine
            .send(SendIntent::RemoveMembers {
                group_id: engine_gid(group),
                members: members
                    .iter()
                    .map(|account| MemberId::new(account.0.to_vec()))
                    .collect(),
            })
            .await
            .map_err(session_engine_error)?;
        self.finish_send(result)
    }

    /// Leave a group, stepping down as admin first when this account holds
    /// admin (`SendIntent::StepDownAndLeave`).
    ///
    /// # Errors
    /// [`SessionError::AdminDepletion`] for the last active admin;
    /// [`SessionError`] otherwise.
    pub async fn leave(&mut self, group: &GroupId) -> Result<SendOutcome, SessionError> {
        let result = self
            .engine
            .send(SendIntent::StepDownAndLeave {
                group_id: engine_gid(group),
            })
            .await
            .map_err(session_engine_error)?;
        self.finish_send(result)
    }

    /// Rotate this device's leaf key material.
    ///
    /// # Errors
    /// [`SessionError`] for engine failures.
    pub async fn self_update(&mut self, group: &GroupId) -> Result<SendOutcome, SessionError> {
        let result = self
            .engine
            .send(SendIntent::SelfUpdate {
                group_id: engine_gid(group),
            })
            .await
            .map_err(session_engine_error)?;
        self.finish_send(result)
    }

    /// Update the group's profile name/description.
    ///
    /// # Errors
    /// [`SessionError`] for engine failures.
    pub async fn update_group_profile(
        &mut self,
        group: &GroupId,
        name: Option<String>,
        description: Option<String>,
    ) -> Result<SendOutcome, SessionError> {
        self.require_admin(group)?;
        let result = self
            .engine
            .send(SendIntent::UpdateGroupData {
                group_id: engine_gid(group),
                name,
                description,
            })
            .await
            .map_err(session_engine_error)?;
        self.finish_send(result)
    }

    /// Replace app-component dictionary entries (routing, retention, …).
    /// Disbanding, device groups and recovery intents are out of scope
    /// (later milestones).
    ///
    /// # Errors
    /// [`SessionError`] for engine failures.
    pub async fn update_app_components(
        &mut self,
        group: &GroupId,
        updates: Vec<ComponentUpdate>,
    ) -> Result<SendOutcome, SessionError> {
        self.require_admin(group)?;
        check_agent_stream_roles(&updates)?;
        let result = self
            .engine
            .send(SendIntent::UpdateAppComponents {
                group_id: engine_gid(group),
                updates: updates
                    .iter()
                    .map(|update| AppComponentData {
                        component_id: update.component_id,
                        data: update.data.clone(),
                    })
                    .collect(),
            })
            .await
            .map_err(session_engine_error)?;
        self.finish_send(result)
    }

    /// Grant or revoke group admin for `account` (MIP-03 admin policy,
    /// component `0x8003`).
    ///
    /// # Errors
    /// [`SessionError::NotGroupAdmin`] when this account is not a current
    /// admin; [`SessionError::AdminDepletion`] when the change would empty
    /// the admin set; [`SessionError::InvalidInput`] (`"admin"`) when
    /// promoting an account that is not a member; [`SessionError`]
    /// otherwise. An unchanged set returns [`SendOutcome::NoChange`].
    pub async fn set_admin(
        &mut self,
        group: &GroupId,
        account: AccountId,
        admin: bool,
    ) -> Result<SendOutcome, SessionError> {
        let group_id = engine_gid(group);
        let mut admins = self.admin_set(&group_id)?;
        if !admins.contains(&self.account().0) {
            return Err(SessionError::NotGroupAdmin);
        }
        if admins.contains(&account.0) == admin {
            return Ok(SendOutcome::NoChange);
        }
        if admin {
            let member = self
                .engine
                .members(&group_id)
                .map_err(session_engine_error)?
                .iter()
                .any(|member| member.id.as_slice() == account.0);
            if !member {
                return Err(SessionError::InvalidInput("admin"));
            }
            admins.push(account.0);
        } else {
            admins.retain(|key| *key != account.0);
            if admins.is_empty() {
                return Err(SessionError::AdminDepletion);
            }
        }
        admins.sort_unstable();
        admins.dedup();
        let mut data = Vec::new();
        encode_quic_varint((admins.len() * 32) as u64, &mut data);
        for key in &admins {
            data.extend_from_slice(key);
        }
        self.update_app_components(
            group,
            vec![ComponentUpdate {
                component_id: GROUP_ADMIN_POLICY_COMPONENT_ID,
                data,
            }],
        )
        .await
    }

    /// Set or clear the group's avatar (component `0x8007`). `None`
    /// encodes the canonical cleared state.
    ///
    /// # Errors
    /// [`SessionError::InvalidInput`] (`"avatar"`) for a refused URL
    /// (`http://`, userinfo, fragment, over 2048 bytes);
    /// [`SessionError::NotGroupAdmin`] for a non-admin;
    /// [`SessionError`] otherwise.
    pub async fn set_group_avatar(
        &mut self,
        group: &GroupId,
        avatar: Option<GroupAvatar>,
    ) -> Result<SendOutcome, SessionError> {
        let component = match avatar {
            Some(avatar) => GroupAvatarUrlV1 {
                url: validate_and_normalize_group_avatar_url(&avatar.url)
                    .map_err(|_| SessionError::InvalidInput("avatar"))?,
                dim: avatar.dim.unwrap_or_default().into_bytes(),
                thumbhash: avatar.thumbhash.unwrap_or_default().into_bytes(),
            },
            None => GroupAvatarUrlV1 {
                url: String::new(),
                dim: Vec::new(),
                thumbhash: Vec::new(),
            },
        };
        let data = encode_group_avatar_url_v1(&component)
            .map_err(|_| SessionError::InvalidInput("avatar"))?;
        self.update_app_components(
            group,
            vec![ComponentUpdate {
                component_id: GROUP_AVATAR_URL_COMPONENT_ID,
                data,
            }],
        )
        .await
    }

    /// Set the group's message-retention seconds (component `0x8005`,
    /// 8-byte big-endian); `0` disables expiry.
    ///
    /// # Errors
    /// [`SessionError::NotGroupAdmin`] for a non-admin; [`SessionError`]
    /// otherwise.
    pub async fn set_retention(
        &mut self,
        group: &GroupId,
        seconds: u64,
    ) -> Result<SendOutcome, SessionError> {
        self.update_app_components(
            group,
            vec![ComponentUpdate {
                component_id: GROUP_MESSAGE_RETENTION_COMPONENT_ID,
                data: seconds.to_be_bytes().to_vec(),
            }],
        )
        .await
    }

    /// Request the group's terminal disband (`SendIntent::Disband`). The
    /// engine prepares the terminal Commit asynchronously once any staged
    /// publish lands and convergence permits.
    ///
    /// # Errors
    /// [`SessionError::NotGroupAdmin`] for a non-admin;
    /// [`SessionError::DisbandingNotEnabled`] when the group does not
    /// require `0x800c` (see [`Self::enable_disbanding`]);
    /// [`SessionError::MissingRequiredCapabilities`] when a member does
    /// not advertise the lifecycle component; [`SessionError`] otherwise.
    pub async fn disband(&mut self, group: &GroupId) -> Result<SendOutcome, SessionError> {
        self.require_admin(group)?;
        let result = self
            .engine
            .send(SendIntent::Disband {
                group_id: engine_gid(group),
            })
            .await
            .map_err(session_engine_error)?;
        self.finish_send(result)
    }

    /// Require `marmot.group.lifecycle.v1` on a group that predates it
    /// (`SendIntent::EnableDisbanding`): one admin Commit installing the
    /// component as `Active`.
    ///
    /// # Errors
    /// [`SessionError::NotGroupAdmin`] for a non-admin;
    /// [`SessionError::MissingRequiredCapabilities`] when a member does
    /// not advertise `0x800c`; [`SessionError`] otherwise.
    pub async fn enable_disbanding(
        &mut self,
        group: &GroupId,
    ) -> Result<SendOutcome, SessionError> {
        self.require_admin(group)?;
        let result = self
            .engine
            .send(SendIntent::EnableDisbanding {
                group_id: engine_gid(group),
            })
            .await
            .map_err(session_engine_error)?;
        self.finish_send(result)
    }

    /// Forget the group locally: `engine.forget_group_local` drops the
    /// engine's rows (leaving its `ForgottenGroup` tombstone so old
    /// Welcomes stay rejected), then one storage unit releases this
    /// session's own group rows — queued-intent links and retry rows
    /// whose outbound no longer exists.
    ///
    /// # Errors
    /// [`SessionError::GroupDisbanding`] while a disband is in progress;
    /// [`SessionError`] otherwise.
    pub fn forget_group(&mut self, group: &GroupId) -> Result<(), SessionError> {
        let group_id = engine_gid(group);
        if self
            .engine
            .disbanding_in_progress(&group_id)
            .map_err(session_engine_error)?
        {
            return Err(SessionError::GroupDisbanding);
        }
        self.engine
            .forget_group_local(&group_id)
            .map_err(session_engine_error)?;
        let live = self.live_outbound_ids()?;
        let stale: Vec<OutboundId> = self
            .provider
            .outbound_retries()
            .map_err(SessionError::Storage)?
            .into_iter()
            .map(|(id, _)| id)
            .filter(|id| !live.contains(id))
            .collect();
        let links = self
            .provider
            .queued_intent_links_for_group(&group_id)
            .map_err(SessionError::Storage)?;
        self.drop_group_memory(group);
        self.provider
            .store()
            .transact(|| -> Result<(), StorageError> {
                for message_id in &links {
                    self.provider.delete_queued_intent_link(message_id)?;
                }
                for id in &stale {
                    self.provider.delete_outbound_retry(id)?;
                }
                Ok(())
            })
            .map_err(SessionError::Storage)
    }

    /// Send an application message into a group.
    ///
    /// The typed surface is [`Self::send_intent`]; this raw path refuses
    /// the event kinds it owns (5, 7, 1009, 1210, 4891) and a kind 9
    /// carrying `e`/`q` tags with [`SessionError::ReservedAppEvent`], in
    /// addition to the existing kind-1200/`imeta`/`stream-start` refusals.
    ///
    /// # Errors
    /// [`SessionError`] for engine failures.
    pub async fn send_message(
        &mut self,
        group: &GroupId,
        message: AppMessage,
    ) -> Result<SentMessage, SessionError> {
        let kind = u64::from(message.kind);
        if kind == MARMOT_APP_EVENT_KIND_AGENT_STREAM_START
            || matches!(
                kind,
                MARMOT_APP_EVENT_KIND_DELETE
                    | MARMOT_APP_EVENT_KIND_REACTION
                    | MARMOT_APP_EVENT_KIND_EDIT
                    | MARMOT_APP_EVENT_KIND_GROUP_SYSTEM
                    | MARMOT_APP_EVENT_KIND_REMOVE
            )
            || (kind == MARMOT_APP_EVENT_KIND_CHAT
                && message
                    .tags
                    .iter()
                    .any(|tag| matches!(tag.first().map(String::as_str), Some("e" | "q"))))
            || message.tags.iter().any(|tag| {
                matches!(
                    tag.first().map(String::as_str),
                    Some(media::IMETA_TAG | STREAM_START_TAG)
                )
            })
        {
            return Err(SessionError::ReservedAppEvent);
        }
        self.send_app_event(group, kind, message.tags, message.content, None)
            .await
    }

    /// Send a typed application message ([`MessageIntent`]) into a group.
    ///
    /// # Errors
    /// [`SessionError::InvalidInput`] for a failed validation;
    /// [`SessionError::NotGroupAdmin`] for `Remove` by a non-admin;
    /// [`SessionError`] for engine failures.
    pub async fn send_intent(
        &mut self,
        group: &GroupId,
        intent: MessageIntent,
    ) -> Result<SentMessage, SessionError> {
        // Delete picks its kind from, and Remove refuses on, the current
        // admin set — read before anything is built.
        let sender_is_admin = match &intent {
            MessageIntent::Delete { .. } | MessageIntent::Remove { .. } => {
                let admins = self.admin_set(&engine_gid(group))?;
                let admin = admins.contains(&self.account().0);
                if matches!(&intent, MessageIntent::Remove { .. }) && !admin {
                    return Err(SessionError::NotGroupAdmin);
                }
                admin
            }
            _ => false,
        };
        let (kind, tags, content) =
            semantics::build_parts(&intent, sender_is_admin).map_err(SessionError::InvalidInput)?;
        self.send_app_event(group, kind, tags, content, None).await
    }

    /// Build the inner event, send it, and wrap the outcome with the
    /// host-rendered [`UnsignedEvent`]. The `created_at` second comes from
    /// the pacing allocator — allocated only after the callers' input
    /// validation, before the event is built — and a sent message records
    /// its own delivered-index row so a sibling device can authorize a
    /// modifier against it.
    async fn send_app_event(
        &mut self,
        group: &GroupId,
        kind: u64,
        tags: Vec<Vec<String>>,
        content: String,
        expected_epoch: Option<EpochPin>,
    ) -> Result<SentMessage, SessionError> {
        let created_at = self.allocate_send_second(&engine_gid(group))?;
        let app_event = cgka_traits::app_event::MarmotAppEvent::new(
            self.account().to_hex(),
            created_at,
            kind,
            tags,
            content,
        );
        let payload = app_event
            .encode()
            .map_err(|_| SessionError::Internal("app event encode"))?;
        let message = unsigned_of_app_event(&app_event)?;
        let result = self
            .engine
            .send(SendIntent::AppMessage {
                group_id: engine_gid(group),
                payload,
                expected_epoch,
            })
            .await
            .map_err(session_engine_error)?;
        let sent = if let SendResult::ApplicationMessage { msg, retention, .. } = &result {
            Some((msg.id.clone(), retention.expires_at))
        } else {
            None
        };
        let outcome = self.finish_send(result)?;
        if let Some((message_id, expires_at)) = sent {
            let transport_id: [u8; 32] = message_id
                .as_slice()
                .try_into()
                .map_err(|_| SessionError::Internal("transport id"))?;
            let store = self.provider.store().clone();
            store
                .transact(|| {
                    self.index_own_send(group, &message.id(), kind, transport_id, expires_at)
                })
                .map_err(SessionError::Storage)?;
        }
        Ok(SentMessage { outcome, message })
    }

    fn finish_send(&mut self, result: SendResult) -> Result<SendOutcome, SessionError> {
        let queued = matches!(result, SendResult::Queued { .. });
        let leave = matches!(result, SendResult::LeaveRequested { .. });
        let disband = matches!(result, SendResult::DisbandRequested { .. });
        let artifacts = self.drain_engine_artifacts();
        let staged = self.stage_send_results(std::iter::once(result).chain(artifacts))?;
        if queued {
            return Ok(SendOutcome::Queued);
        }
        if leave {
            return Ok(SendOutcome::LeaveRequested);
        }
        if disband {
            return Ok(SendOutcome::DisbandRequested);
        }
        if staged.is_empty() {
            return Ok(SendOutcome::NoChange);
        }
        Ok(SendOutcome::Staged(staged))
    }

    /// The group's decoded `0x800b` policy, or `None` when the component is
    /// absent. A present-but-corrupt component is an engine invariant
    /// violation, not a policy absence.
    fn media_policy(
        &self,
        group_id: &EngineGroupId,
    ) -> Result<Option<EncryptedMediaPolicy>, SessionError> {
        let Some(bytes) = self
            .engine
            .app_component(group_id, GROUP_ENCRYPTED_MEDIA_V2_COMPONENT_ID)
            .map_err(session_engine_error)?
        else {
            return Ok(None);
        };
        EncryptedMediaPolicy::from_component(&bytes)
            .map(Some)
            .map_err(|_| SessionError::Internal("media component"))
    }

    /// The group's decoded `0x8006` policy, or `None` when absent/corrupt.
    fn agent_stream_policy(
        &self,
        group_id: &EngineGroupId,
    ) -> Result<Option<AgentStreamPolicy>, SessionError> {
        self.engine
            .app_component(group_id, AGENT_TEXT_STREAM_QUIC_COMPONENT_ID)
            .map_err(session_engine_error)
            .map(|bytes| bytes.and_then(|bytes| AgentStreamPolicy::from_component(&bytes)))
    }

    /// The current epoch's 32-byte media secret:
    /// `MLS-Exporter("marmot","encrypted-media",32)`.
    fn media_secret(&self, group_id: &EngineGroupId) -> Result<Zeroizing<[u8; 32]>, SessionError> {
        let context = self
            .engine
            .group_context(group_id)
            .map_err(session_engine_error)?;
        let secret = context
            .exporter_secret(GROUP_ENCRYPTED_MEDIA_EXPORTER_CACHE_KEY, 32)
            .ok_or(SessionError::Internal("media exporter"))?;
        secret
            .as_slice()
            .try_into()
            .map(Zeroizing::new)
            .map_err(|_| SessionError::Internal("media exporter"))
    }

    /// Encrypt one attachment under the group's current-epoch media secret.
    /// The host uploads [`SealedAttachment::ciphertext`] itself — this crate
    /// holds no network dependency — then hands [`SealedAttachment::pending`]
    /// plus the upload's locators to [`Self::send_media`].
    ///
    /// # Errors
    /// [`SessionError::MediaNotEnabled`] when the group carries no `0x800b`
    /// component; [`SessionError::Media`] for an invalid media type,
    /// filename, an oversized plaintext, or a key-derivation failure.
    pub fn seal_attachment(
        &self,
        group: &GroupId,
        input: AttachmentInput,
    ) -> Result<SealedAttachment, SessionError> {
        let group_id = engine_gid(group);
        if self.media_policy(&group_id)?.is_none() {
            return Err(SessionError::MediaNotEnabled);
        }
        let media_type = media::canonical_media_type(&input.media_type)?;
        media::validate_filename(&input.filename)?;
        let plaintext_sha256 = sha256(&input.plaintext);
        let pin = self
            .engine
            .epoch_pin(&group_id)
            .map_err(session_engine_error)?;
        let secret = self.media_secret(&group_id)?;
        let key = media::derive_file_key(&secret, &plaintext_sha256, &media_type, &input.filename)?;
        let mut nonce = [0u8; 12];
        getrandom::fill(&mut nonce).map_err(|_| SessionError::Internal("rng"))?;
        let aad = media::media_aad(&plaintext_sha256, &media_type, &input.filename);
        let ciphertext = media::encrypt_file(&key, &nonce, &aad, &input.plaintext)?;
        let ciphertext_sha256 = sha256(&ciphertext);
        Ok(SealedAttachment {
            ciphertext,
            ciphertext_sha256,
            pending: PendingAttachment {
                ciphertext_sha256,
                plaintext_sha256,
                nonce,
                media_type,
                filename: input.filename,
                dim: input.dim,
                thumbhash: input.thumbhash,
                pin,
            },
        })
    }

    /// Emit one kind-9 message carrying every sealed attachment as an
    /// `imeta` tag, pinned to the epoch they were sealed under so receivers
    /// derive keys from the same epoch's media secret.
    ///
    /// On success the current epoch's media secret is also recorded under
    /// the sent message's own transport id, so this device can
    /// [`Self::open_attachment`] its own attachments — the engine only
    /// captures secrets on inbound messages. That row is written in its own
    /// storage unit after the send's staging: a crash between them costs
    /// only this device's ability to re-open its own attachment — the sender
    /// still holds the plaintext and the row can be re-derived while the
    /// epoch lives.
    ///
    /// # Errors
    /// [`SessionError::MediaNotEnabled`] without a `0x800b` component;
    /// [`SessionError::MediaEpochStale`] when the seal-time pin no longer
    /// matches (re-seal); [`SessionError::MediaEpochUnsettled`] while the
    /// group settles (retry after [`Self::tick`]);
    /// [`SessionError::InvalidInput`] for an empty attachment list, an
    /// attachment with no locators, or mixed pins; [`SessionError::Media`]
    /// for a structurally invalid locator or a locator kind the group
    /// policy does not allow; [`SessionError::ReservedAppEvent`] for
    /// `imeta`/`stream-start` in `message.tags`.
    pub async fn send_media(
        &mut self,
        group: &GroupId,
        message: MediaMessage,
    ) -> Result<SentMessage, SessionError> {
        let group_id = engine_gid(group);
        let policy = self
            .media_policy(&group_id)?
            .ok_or(SessionError::MediaNotEnabled)?;
        let Some((first, _)) = message.attachments.first() else {
            return Err(SessionError::InvalidInput("empty attachment list"));
        };
        let pin = first.pin;
        let mut tags = Vec::with_capacity(message.tags.len() + message.attachments.len());
        for tag in &message.tags {
            if matches!(
                tag.first().map(String::as_str),
                Some(media::IMETA_TAG | STREAM_START_TAG)
            ) {
                return Err(SessionError::ReservedAppEvent);
            }
            tags.push(tag.clone());
        }
        for (pending, locators) in &message.attachments {
            if pending.pin != pin {
                return Err(SessionError::InvalidInput("mixed attachment epochs"));
            }
            if locators.is_empty() {
                return Err(SessionError::InvalidInput("attachment without locators"));
            }
            for locator in locators {
                check_locator(&policy, locator)?;
            }
            tags.push(
                MediaReference {
                    locators: locators.clone(),
                    ciphertext_sha256: pending.ciphertext_sha256,
                    plaintext_sha256: pending.plaintext_sha256,
                    nonce: pending.nonce,
                    media_type: pending.media_type.clone(),
                    filename: pending.filename.clone(),
                    dim: pending.dim.clone(),
                    thumbhash: pending.thumbhash.clone(),
                }
                .to_imeta(),
            );
        }
        let created_at = self.allocate_send_second(&group_id)?;
        let app_event = cgka_traits::app_event::MarmotAppEvent::new(
            self.account().to_hex(),
            created_at,
            MARMOT_APP_EVENT_KIND_CHAT,
            tags,
            message.caption,
        );
        let payload = app_event
            .encode()
            .map_err(|_| SessionError::Internal("app event encode"))?;
        let unsigned = unsigned_of_app_event(&app_event)?;
        let result = match self
            .engine
            .send(SendIntent::AppMessage {
                group_id: group_id.clone(),
                payload,
                expected_epoch: Some(pin),
            })
            .await
        {
            Ok(result) => result,
            Err(EngineError::AppMessageEpochMismatch { .. }) => {
                return Err(SessionError::MediaEpochStale);
            }
            Err(EngineError::AppMessageEpochUnsettled { .. }) => {
                return Err(SessionError::MediaEpochUnsettled);
            }
            Err(error) => return Err(session_engine_error(error)),
        };
        let sent = if let SendResult::ApplicationMessage { msg, retention, .. } = &result {
            Some((msg.id.clone(), retention.expires_at))
        } else {
            None
        };
        let outcome = self.finish_send(result)?;
        if let Some((message_id, expires_at)) = sent {
            let secret = self.media_secret(&group_id)?;
            let transport_id: [u8; 32] = message_id
                .as_slice()
                .try_into()
                .map_err(|_| SessionError::Internal("transport id"))?;
            let store = self.provider.store().clone();
            store
                .transact(|| -> Result<(), StorageError> {
                    self.provider
                        .put_media_secret(&group_id, &message_id, &secret[..])?;
                    self.index_own_send(
                        group,
                        &unsigned.id(),
                        MARMOT_APP_EVENT_KIND_CHAT,
                        transport_id,
                        expires_at,
                    )
                })
                .map_err(SessionError::Storage)?;
        }
        Ok(SentMessage {
            outcome,
            message: unsigned,
        })
    }

    /// Decrypt one attachment of a delivered message: loads the
    /// `(group, transport)` media-secret row written when the message was
    /// drained, then verifies both hashes and the AEAD tag.
    ///
    /// # Errors
    /// [`SessionError::MediaSecretUnavailable`] when no secret row is
    /// stored (non-media message, already-invalidated, or a message whose
    /// attachments all failed parsing); [`SessionError::Media`] for hash
    /// mismatches or AEAD failure.
    pub fn open_attachment(
        &self,
        group: &GroupId,
        message: &EventId,
        reference: &MediaReference,
        ciphertext: &[u8],
    ) -> Result<Vec<u8>, SessionError> {
        let secret = self
            .provider
            .media_secret(&engine_gid(group), &MessageId::new(message.0.to_vec()))
            .map_err(SessionError::Storage)?
            .ok_or(SessionError::MediaSecretUnavailable)?;
        let secret: Zeroizing<[u8; 32]> = secret
            .as_slice()
            .try_into()
            .map(Zeroizing::new)
            .map_err(|_| SessionError::Internal("media secret row"))?;
        Ok(media::decrypt_file(&secret, reference, ciphertext)?)
    }

    /// Artifacts the engine produced outside the direct send result:
    /// auto-publish commits (`SelfRemove` auto-committer, replayed queued
    /// intents) and auto-proposals.
    fn drain_engine_artifacts(&mut self) -> Vec<SendResult> {
        let mut results = Vec::new();
        for auto in self.engine.drain_auto_publish() {
            results.push(SendResult::GroupEvolution {
                msg: auto.msg,
                welcomes: auto.welcomes,
                pending: auto.pending,
            });
        }
        for msg in self.engine.drain_auto_proposals() {
            results.push(SendResult::Proposal { msg });
        }
        results
    }

    /// Freeze every produced artifact as a fanout (one storage unit each),
    /// record its gate seq, and return the staged outbound ids in order.
    fn stage_send_results(
        &mut self,
        results: impl Iterator<Item = SendResult>,
    ) -> Result<Vec<OutboundId>, SessionError> {
        let mut staged = Vec::new();
        for result in results {
            match result {
                SendResult::NoChange { .. }
                | SendResult::DisbandRequested { .. }
                | SendResult::LeaveRequested { .. }
                | SendResult::Queued { .. } => {}
                SendResult::ApplicationMessage {
                    msg,
                    group_id,
                    app_event_id,
                    source_epoch,
                    retention,
                    authority,
                } => {
                    let mut fanout = self.stage_group_fanout(
                        &msg,
                        &group_id,
                        Some(OutboundApplicationMessage {
                            group_id: group_id.clone(),
                            app_event_id,
                            source_epoch,
                            retention,
                            authority,
                        }),
                    )?;
                    staged.push(self.commit_fanout(&mut fanout)?);
                }
                SendResult::Proposal { msg } => {
                    let group_id = self
                        .group_of_message(&msg)
                        .ok_or(SessionError::Internal("proposal group"))?;
                    let mut fanout = self.stage_group_fanout(&msg, &group_id, None)?;
                    staged.push(self.commit_fanout(&mut fanout)?);
                }
                SendResult::GroupEvolution {
                    msg,
                    welcomes,
                    pending,
                } => {
                    let group_id = self
                        .engine
                        .pending_group_id(pending)
                        .map_err(session_engine_error)?;
                    let kind = self
                        .engine
                        .pending_fanout_kind(pending)
                        .map_err(session_engine_error)?;
                    let origin = match kind {
                        FanoutPendingKind::CreateGroup => None,
                        _ => Some(
                            self.engine
                                .pending_origin_message_id(pending)
                                .map_err(session_engine_error)?,
                        ),
                    };
                    let mut fanout = OutboundFanout::stage_with_post_confirmation_welcomes(
                        TransportPublishRequest {
                            account_id: self.engine.self_id(),
                            message: msg.clone(),
                            target: self.group_target(&group_id)?,
                            required_acks: 1,
                        },
                        Some(pending),
                        Some(group_id),
                        self.now_ms(),
                        origin,
                        Some(kind),
                        welcomes,
                    )
                    .map_err(|_| SessionError::Internal("fanout stage"))?;
                    staged.push(self.commit_fanout(&mut fanout)?);
                }
                SendResult::GroupCreated { .. } => {
                    // Legacy-profile creation, unreachable under
                    // `ProtocolProfile::Current` (build() refuses the legacy
                    // profile). No pending lifecycle exists to resolve
                    // through a published origin commit; fail closed.
                    return Err(SessionError::Internal("legacy profile result"));
                }
                SendResult::FoundingGroupCreated { welcomes } => {
                    self.stage_welcomes(&welcomes, &mut staged)?;
                }
            }
        }
        Ok(staged)
    }

    /// Stage every Welcome in a result, appending their outbound ids.
    fn stage_welcomes(
        &mut self,
        welcomes: &[TransportMessage],
        staged: &mut Vec<OutboundId>,
    ) -> Result<(), SessionError> {
        for welcome in welcomes {
            staged.push(self.stage_welcome(welcome)?);
        }
        Ok(())
    }

    /// Group-scoped publish target from the live routing component — the
    /// relays in effect at staging time (for a commit, the routing of the
    /// epoch it was staged under).
    fn group_target(
        &self,
        group_id: &EngineGroupId,
    ) -> Result<TransportPublishTarget, SessionError> {
        let routing = self.routing_of(group_id)?;
        let endpoints = routing
            .relays
            .iter()
            .map(|relay| TransportEndpoint(relay.clone()))
            .collect();
        Ok(TransportPublishTarget::Group {
            group_id: group_id.clone(),
            transport_group_id: routing.nostr_group_id.to_vec(),
            endpoints,
        })
    }

    fn routing_of(&self, group_id: &EngineGroupId) -> Result<NostrRoutingV1, SessionError> {
        let bytes = self
            .engine
            .app_component(group_id, NOSTR_ROUTING_COMPONENT_ID)
            .map_err(session_engine_error)?
            .ok_or(SessionError::Internal("routing component missing"))?;
        decode_nostr_routing_v1(&bytes).map_err(|_| SessionError::Internal("routing decode"))
    }

    /// The live group whose routing id matches a group-scoped transport
    /// message (proposals carry no MLS group id in their envelope).
    fn group_of_message(&self, msg: &TransportMessage) -> Option<EngineGroupId> {
        let TransportEnvelope::GroupMessage { transport_group_id } = &msg.envelope else {
            return None;
        };
        for group_id in self.engine.live_group_ids().ok()? {
            let Ok(routing) = self.routing_of(&group_id) else {
                continue;
            };
            if routing.nostr_group_id.as_slice() == transport_group_id.as_slice() {
                return Some(group_id);
            }
        }
        None
    }

    /// Stage one fanout for a group-scoped artifact with no pending state.
    fn stage_group_fanout(
        &self,
        msg: &TransportMessage,
        group_id: &EngineGroupId,
        application: Option<OutboundApplicationMessage>,
    ) -> Result<OutboundFanout, SessionError> {
        let mut fanout = OutboundFanout::stage(
            TransportPublishRequest {
                account_id: self.engine.self_id(),
                message: msg.clone(),
                target: self.group_target(group_id)?,
                required_acks: 1,
            },
            None,
            Some(group_id.clone()),
            self.now_ms(),
        )
        .map_err(|_| SessionError::Internal("fanout stage"))?;
        if let Some(application) = application {
            fanout
                .set_application_message(application)
                .map_err(|_| SessionError::Internal("fanout app message"))?;
        }
        Ok(fanout)
    }

    /// Stage one Welcome as an inbox fanout routed by the recipient's
    /// stored inbox relays.
    fn stage_welcome(&mut self, msg: &TransportMessage) -> Result<OutboundId, SessionError> {
        let TransportEnvelope::Welcome { recipient } = &msg.envelope else {
            return Err(SessionError::Internal("welcome envelope"));
        };
        let recipient_bytes: [u8; 32] = recipient
            .as_slice()
            .try_into()
            .map_err(|_| SessionError::Internal("welcome recipient"))?;
        let relays = self
            .provider
            .inbox_relays(&AccountId(recipient_bytes))
            .map_err(SessionError::Storage)?
            .ok_or(SessionError::InvalidInput("inbox relays"))?;
        let mut fanout = OutboundFanout::stage(
            TransportPublishRequest {
                account_id: self.engine.self_id(),
                message: msg.clone(),
                target: TransportPublishTarget::Inbox {
                    recipient: recipient.clone(),
                    endpoints: relays
                        .iter()
                        .map(|relay| TransportEndpoint(relay.clone()))
                        .collect(),
                },
                required_acks: 1,
            },
            None,
            None,
            self.now_ms(),
        )
        .map_err(|_| SessionError::Internal("fanout stage"))?;
        self.commit_fanout(&mut fanout)
    }

    /// Mark every target attempt-started, persist the fanout in one storage
    /// unit (policy-refused endpoints become terminal failures on the
    /// record rather than publishable targets), and record the gate seq.
    fn commit_fanout(&mut self, fanout: &mut OutboundFanout) -> Result<OutboundId, SessionError> {
        let endpoints = fanout.request().target.endpoints().to_vec();
        for (index, endpoint) in endpoints.iter().enumerate() {
            fanout
                .mark_attempt_started_at(index, self.now_ms())
                .map_err(|_| SessionError::Internal("fanout mark"))?;
            if !self.relay_allowed(&endpoint.0) {
                fanout
                    .record_target_failure(
                        index,
                        TransportEndpointFailure {
                            endpoint: endpoint.clone(),
                            reason: "policy_refused".to_owned(),
                            kind: TransportEndpointFailureKind::TerminalRejected,
                            rejection_category: None,
                            size_class: None,
                            // The session refuses the relay itself: no
                            // frame ever leaves for it.
                            pre_send: true,
                        },
                    )
                    .map_err(|_| SessionError::Internal("fanout failure"))?;
            }
        }
        // A queued intent regenerated into this artifact is linked durably
        // in the same unit: the engine's `queued_intent_by_message`
        // association is in-memory only, so the report path reads the link
        // row instead of the map (restart keeps the obligation).
        let link = self
            .engine
            .regenerated_queued_intent_for_message(fanout.message_id());
        self.provider
            .store()
            .transact(|| -> Result<(), SessionError> {
                self.engine
                    .put_outbound_fanout(fanout)
                    .map_err(session_engine_error)?;
                if let Some((group_id, intent_id)) = &link {
                    self.provider
                        .put_queued_intent_link(fanout.message_id(), group_id, intent_id)
                        .map_err(SessionError::Storage)?;
                }
                Ok(())
            })?;
        let id = outbound_id_of(fanout.message_id())?;
        if let Some(seq) = self.provider.last_seq() {
            self.gate.insert(id, seq);
        }
        // Keep the value that marked these attempts: results recorded on it
        // can prove an attempt never left (mdk#2332); a later storage load
        // cannot.
        self.held.insert(id, fanout.clone());
        Ok(id)
    }

    // ---- inbound ----

    /// Admit one inbound transport event. Verifies the event (id + schnorr
    /// signature), converts it and hands it to the engine, then drives the
    /// convergence work it unblocked so a single ingest lands like a full
    /// publish/flush pass.
    ///
    /// # Errors
    /// [`SessionError`] for engine/storage failures. A malformed or badly
    /// signed event is reported as an [`InputOutcome`], not an error.
    pub async fn ingest(&mut self, event: &Event) -> Result<InputReport, SessionError> {
        let outcome = self.ingest_verified(event).await?;
        self.drive().await?;
        Ok(InputReport {
            event: event.id,
            outcome,
        })
    }

    /// Admit a batch of inbound events: sorted ascending by `(created_at,
    /// id)`, deduplicated by id within the batch, ingested one by one. The
    /// convergence drive runs once after the last event — batch ingest then
    /// tick reaches the same state as per-event drives.
    ///
    /// # Errors
    /// [`SessionError`] for engine/storage failures.
    pub async fn ingest_batch(
        &mut self,
        mut events: Vec<Event>,
    ) -> Result<Vec<InputReport>, SessionError> {
        events.sort_by_key(|a| (a.created_at, a.id));
        events.dedup_by_key(|event| event.id);
        let mut reports = Vec::with_capacity(events.len());
        for event in &events {
            let outcome = self.ingest_verified(event).await?;
            reports.push(InputReport {
                event: event.id,
                outcome,
            });
        }
        self.drive().await?;
        Ok(reports)
    }

    async fn ingest_verified(&mut self, event: &Event) -> Result<InputOutcome, SessionError> {
        if event.verify().is_err() {
            return Ok(InputOutcome::Rejected {
                category: crate::events::InputCategory::InvalidSignature,
            });
        }
        let nostr_event = event
            .to_nostr_event()
            .ok_or(SessionError::InvalidTransportEvent)?;
        let Ok(dto) = transport_nostr_peeler::NostrTransportEvent::from_nostr_event(&nostr_event)
        else {
            return Ok(InputOutcome::Rejected {
                category: crate::events::InputCategory::InvalidEncoding,
            });
        };
        let Ok(message) = dto.to_transport_message() else {
            return Ok(InputOutcome::Rejected {
                category: crate::events::InputCategory::InvalidEncoding,
            });
        };
        if event.kind == 1059 {
            // A refused or failed gift-wrap unwrap is retryable, never a
            // permanent rejection: the adapter's latch records the first
            // signer verdict and fences every write after it, so the ingest
            // ends in an engine error before the dedup epilogue can remember
            // the id — and the savepoint discards whatever it did write.
            self.signer_latch.arm();
            let savepoint = self
                .provider
                .store()
                .begin_savepoint()
                .map_err(SessionError::Storage)?;
            let result = self.engine.ingest(message).await;
            self.signer_latch.disarm();
            if let Some(verdict) = self.signer_latch.verdict() {
                drop(savepoint);
                return Ok(InputOutcome::Unavailable {
                    category: match verdict {
                        SignerVerdict::Refused => crate::events::Availability::SignerRefused,
                        SignerVerdict::Failed => crate::events::Availability::SignerFailed,
                    },
                });
            }
            // No verdict: commit the span even when the engine errored —
            // pre-latch behaviour kept those writes (a rejected input's
            // dedup marker is still the point of the ingest).
            savepoint.commit().map_err(SessionError::Storage)?;
            let outcome = result.map_err(session_engine_error)?;
            return Ok(input_outcome(&outcome));
        }
        let outcome = self
            .engine
            .ingest(message)
            .await
            .map_err(session_engine_error)?;
        Ok(input_outcome(&outcome))
    }

    /// Whether a live group owes convergence work beyond what the engine
    /// flagged or buffered as input. Mirrors the scheduling conditions of
    /// marmot-app's `convergence_schedule_state`
    /// (`crates/marmot-app/src/client/sync.rs:621`-`648` on the pinned rev):
    /// queued outbound intents, a pending disband request, an owed leave
    /// re-drive, a due scheduled `SelfRemove` auto-commit, or a due
    /// deferred-peel cutoff.
    fn group_owes_advance(&mut self, group_id: &EngineGroupId) -> Result<bool, SessionError> {
        if self
            .engine
            .has_pending_convergence_inputs(group_id)
            .map_err(session_engine_error)?
            || self
                .engine
                .has_queued_outbound_intents(group_id)
                .map_err(session_engine_error)?
            || self
                .engine
                .leave_redrive_owed(group_id)
                .map_err(session_engine_error)?
            || self
                .engine
                .disband_request(group_id)
                .map_err(session_engine_error)?
                .is_some_and(|request| request.status == DisbandRequestStatus::Pending)
            || self
                .engine
                .scheduled_self_remove_auto_commit_delay_ms(group_id)
                .map_err(session_engine_error)?
                == Some(0)
            || self
                .engine
                .deferred_peel_cutoff_delay_ms(group_id)
                .map_err(session_engine_error)?
                == Some(0)
        {
            return Ok(true);
        }
        Ok(false)
    }

    /// Drive pending convergence: every group the engine flagged plus every
    /// live group that owes an advance gets a convergence pass; every
    /// artifact produced is staged.
    ///
    /// The sequence mirrors marmot-session
    /// `AccountDeviceRuntime::advance_convergence`
    /// (`runtime.rs:3894`-`3924` on the pinned rev): stored inputs settle
    /// first — inbound convergence completes even while an outbound
    /// obligation is still outstanding — then the full advance drains the
    /// queued sends that can go out now; auto-publish and auto-proposal
    /// drains follow, exactly as `publish_session_effects` runs after the
    /// engine advance. The input settle uses the deterministic
    /// `until_settled` form so the injected session clock, not a wall-time
    /// budget, decides when a pass reports unfinished work.
    async fn drive(&mut self) -> Result<(), SessionError> {
        let mut groups = self.engine.drain_pending_convergence_groups();
        for group_id in self.engine.live_group_ids().map_err(session_engine_error)? {
            if !groups.contains(&group_id) && self.group_owes_advance(&group_id)? {
                groups.push(group_id);
            }
        }
        let now_ms = self.now_ms();
        for group_id in groups {
            self.engine
                .advance_convergence_inputs_until_settled(&group_id, now_ms)
                .await
                .map_err(session_engine_error)?;
            let results = self
                .engine
                .advance_convergence(&group_id)
                .await
                .map_err(session_engine_error)?;
            let artifacts = self.drain_engine_artifacts();
            self.stage_send_results(results.into_iter().chain(artifacts))?;
        }
        let artifacts = self.drain_engine_artifacts();
        self.stage_send_results(artifacts.into_iter())?;
        Ok(())
    }

    /// Run one maintenance/convergence pass and report when to wake again.
    ///
    /// Scheduled cutoffs are armed before the advance, matching
    /// marmot-session: the host calls `prepare_convergence_cutoff_delay_ms`
    /// to learn when each collecting pass selects, then `advance_convergence`
    /// on the wake (`runtime.rs:4194` forward + `advance_convergence`).
    /// `next_wake_ms` is the earliest armed delay.
    ///
    /// # Errors
    /// [`SessionError`] for engine/storage failures.
    pub async fn tick(&mut self) -> Result<TickReport, SessionError> {
        let mut next: Option<u64> = None;
        let fold = |delay: Option<u64>, wake: &mut Option<u64>| {
            if let Some(delay) = delay {
                *wake = Some(wake.map_or(delay, |current: u64| current.min(delay)));
            }
        };
        for group_id in self.engine.live_group_ids().map_err(session_engine_error)? {
            // The lifecycle timers join the wake computation exactly as
            // `convergence_schedule_state` chains them
            // (`marmot-app/src/client/sync.rs:641`-`647`).
            let convergence_delay = self
                .engine
                .prepare_convergence_cutoff_delay_ms(&group_id)
                .map_err(|_| SessionError::Internal("convergence cutoff"))?;
            fold(convergence_delay, &mut next);
            let lifecycle_delay = self
                .engine
                .scheduled_self_remove_auto_commit_delay_ms(&group_id)
                .map_err(session_engine_error)?;
            fold(lifecycle_delay, &mut next);
            let peel_delay = self
                .engine
                .deferred_peel_cutoff_delay_ms(&group_id)
                .map_err(session_engine_error)?;
            fold(peel_delay, &mut next);
        }
        // Fold each live, not-handed item's retry deadline into the wake.
        fold(self.retry_wake()?, &mut next);
        // Held modifiers past their window resolve finally and expired
        // index rows sweep (bounded), before `drive`: the produced events
        // surface on the next `drain_events` and the earliest hold/sweep
        // deadline folds into the wake.
        fold(self.process_expiry_and_holds(self.now_ms())?, &mut next);
        self.drive().await?;
        Ok(TickReport { next_wake_ms: next })
    }

    /// Drain the session-visible events produced since the last call,
    /// including publish resolutions.
    ///
    /// Media-secret custody: every `MessageReceived` carrying a captured
    /// epoch media secret and at least one valid attachment writes its
    /// `(group, transport)` row, and every `MessageInvalidated` deletes its
    /// row — all of one drain's writes in ONE storage unit (one `KvBatch`),
    /// committed before this call returns. Batch ordering then guarantees
    /// the secret row is durable before any later [`Self::acknowledge_messages`]
    /// batch can release the pending-application-event rows, so a crash
    /// between drain and ack replays both rather than leaving a message the
    /// host can never decrypt.
    ///
    /// The delivered index lands in the same unit: every surfaced
    /// `MessageReceived`/`AgentStreamStarted` writes its
    /// `DeliveredMessage` row; a kind 1009/5 modifier resolves against it —
    /// held when a target is still unknown, released when every target is
    /// indexed — and an invalidation removes the row its transport id
    /// maps to.
    ///
    /// # Errors
    /// [`SessionError`] for storage failures — the events stay drained but
    /// their rows were not persisted (the unit commits atomically or not at
    /// all).
    pub fn drain_events(&mut self) -> Result<Vec<SessionEvent>, SessionError> {
        let now_secs = self.clock.now().unix_ms / 1_000;
        let now_ms = self.now_ms();
        let mapped: Vec<crate::events::MappedEvent> = self
            .engine
            .drain_events()
            .into_iter()
            .filter_map(|event| session_event(event, now_secs))
            .collect();
        let dropped: Vec<MessageId> = mapped
            .iter()
            .filter_map(|item| item.drop_pending.clone())
            .collect();
        let store = self.provider.store().clone();
        let mut out = store
            .transact(|| -> Result<Vec<SessionEvent>, StorageError> {
                self.persist_media_secret_effects(&mapped)?;
                self.provider.delete_pending_application_events(&dropped)?;
                let mut out = Vec::new();
                for item in mapped {
                    self.process_mapped(item, now_ms, &mut out)?;
                }
                Ok(out)
            })
            .map_err(SessionError::Storage)?;
        out.extend(std::mem::take(&mut self.tick_events));
        out.extend(std::mem::take(&mut self.resolutions));
        Ok(out)
    }

    /// One mapped event's index/resolve/invalidation bookkeeping plus the
    /// event the app sees. Runs inside [`Self::drain_events`]' unit.
    fn process_mapped(
        &mut self,
        item: crate::events::MappedEvent,
        now_ms: u64,
        out: &mut Vec<SessionEvent>,
    ) -> Result<(), StorageError> {
        match (item.event, item.modifier) {
            (SessionEvent::MessageReceived { group, message }, Some(modifier)) => self
                .resolve_modifier(
                    &group,
                    HeldModifier {
                        message,
                        modifier,
                        deadline_ms: now_ms.saturating_add(delivered::HOLD_WINDOW_MS),
                    },
                    false,
                    out,
                ),
            (SessionEvent::MessageReceived { group, message }, None) => {
                self.index_received(&group, &message)?;
                out.push(SessionEvent::MessageReceived {
                    group: group.clone(),
                    message,
                });
                self.release_held(&group, out)
            }
            (SessionEvent::AgentStreamStarted { group, start }, _) => {
                self.index_anchor(&group, &start)?;
                out.push(SessionEvent::AgentStreamStarted {
                    group: group.clone(),
                    start,
                });
                self.release_held(&group, out)
            }
            (
                SessionEvent::MessageInvalidated {
                    group,
                    message,
                    epoch,
                },
                _,
            ) => {
                self.invalidate_transport(&group, &message)?;
                out.push(SessionEvent::MessageInvalidated {
                    group,
                    message,
                    epoch,
                });
                Ok(())
            }
            (event, _) => {
                out.push(event);
                Ok(())
            }
        }
    }

    /// Apply one drain's media-secret writes and deletes as the single
    /// storage unit [`Self::drain_events`] documents.
    fn persist_media_secret_effects(
        &self,
        mapped: &[crate::events::MappedEvent],
    ) -> Result<(), StorageError> {
        for item in mapped {
            if let Some((group_id, transport_id, secret)) = &item.secret_write {
                self.provider
                    .put_media_secret(group_id, transport_id, secret)?;
            }
            if let Some((group_id, transport_id)) = &item.secret_delete {
                self.provider.delete_media_secret(group_id, transport_id)?;
            }
        }
        Ok(())
    }

    /// Acknowledge delivered application messages (at-least-once): their
    /// durable pending-application-event rows are released, so a restart no
    /// longer re-presents them.
    ///
    /// # Errors
    /// [`SessionError`] for storage failures.
    pub fn acknowledge_messages(&self, ids: &[EventId]) -> Result<(), SessionError> {
        let message_ids: Vec<MessageId> =
            ids.iter().map(|id| MessageId::new(id.0.to_vec())).collect();
        self.provider
            .delete_pending_application_events(&message_ids)
            .map_err(SessionError::Storage)
    }

    // ---- snapshots ----

    /// Snapshots of every live group, for UI projection.
    ///
    /// # Errors
    /// [`SessionError`] for engine/storage failures.
    pub fn groups(&self) -> Result<Vec<GroupSnapshot>, SessionError> {
        let mut out = Vec::new();
        for group_id in self.engine.live_group_ids().map_err(session_engine_error)? {
            if let Some(snapshot) = self.group(&GroupId(group_id.as_slice().to_vec()))? {
                out.push(snapshot);
            }
        }
        Ok(out)
    }

    /// Snapshot of one group. `Ok(None)` when the group is not live.
    ///
    /// # Errors
    /// [`SessionError`] for engine/storage failures.
    pub fn group(&self, id: &GroupId) -> Result<Option<GroupSnapshot>, SessionError> {
        let gid = engine_gid(id);
        let record = match self.engine.group_record(&gid) {
            Ok(record) => record,
            Err(EngineError::UnknownGroup(_)) => return Ok(None),
            Err(error) => return Err(session_engine_error(error)),
        };
        // A disbanded copy keeps its durable record (name, epoch, the
        // former roster) but the engine has deleted the live MLS state, so
        // member/component reads below would fail — the tombstone answers
        // everything the snapshot can still say.
        if record.disbanded.is_some() {
            return Ok(Some(GroupSnapshot {
                id: id.clone(),
                name: record.name,
                description: record.description,
                epoch: record.epoch.0,
                state: GroupState::Disbanded,
                members: record
                    .members
                    .iter()
                    .filter_map(|member| {
                        let bytes: [u8; 32] = member.id.as_slice().try_into().ok()?;
                        Some(MemberSnapshot {
                            account: AccountId(bytes),
                            admin: false,
                        })
                    })
                    .collect(),
                relays: Vec::new(),
                nostr_group_id: [0; 32],
                own_account_admin: false,
                encrypted_media: None,
                agent_text_stream: None,
                avatar: None,
                retention_seconds: 0,
                disband_enabled: false,
                disbanding: false,
            }));
        }
        let members = self.engine.members(&gid).map_err(session_engine_error)?;
        let admins = self.admin_set(&gid)?;
        let state = match self.engine.epoch_state(&gid) {
            Some(EpochState::Stable { .. }) | None => GroupState::Stable,
            Some(EpochState::PendingPublish(_) | EpochState::Merging(_)) => {
                GroupState::PendingPublish
            }
            Some(EpochState::Recovering(_)) => GroupState::Recovering,
            Some(EpochState::Unrecoverable(_)) => GroupState::Unrecoverable,
            Some(EpochState::Disbanded(_)) => GroupState::Disbanded,
        };
        let state = if record.removed {
            GroupState::Removed
        } else if record.unrecoverable {
            GroupState::Unrecoverable
        } else {
            state
        };
        let routing = self.routing_of(&gid).ok();
        let relays = routing
            .as_ref()
            .map(|routing| routing.relays.clone())
            .unwrap_or_default();
        let nostr_group_id = routing.map_or([0; 32], |routing| routing.nostr_group_id);
        let epoch = record.epoch.0;
        let account = self.account();
        let avatar = self
            .engine
            .app_component(&gid, GROUP_AVATAR_URL_COMPONENT_ID)
            .map_err(session_engine_error)?
            .and_then(|bytes| decode_group_avatar_url_v1(&bytes).ok())
            .and_then(group_avatar);
        let retention_seconds = self
            .engine
            .app_component(&gid, GROUP_MESSAGE_RETENTION_COMPONENT_ID)
            .map_err(session_engine_error)?
            .and_then(|bytes| <[u8; 8]>::try_from(bytes.as_slice()).ok())
            .map_or(0, u64::from_be_bytes);
        let lifecycle_active = self
            .engine
            .app_component(&gid, GROUP_LIFECYCLE_COMPONENT_ID)
            .map_err(session_engine_error)?
            .is_some_and(|bytes| decode_group_lifecycle_v1(&bytes) == Ok(GroupLifecycleV1::Active));
        let disband_enabled = self
            .engine
            .app_component(&gid, APP_COMPONENTS_COMPONENT_ID)
            .map_err(session_engine_error)?
            .and_then(|bytes| decode_components_list(&bytes).ok())
            .is_some_and(|required| required.contains(&GROUP_LIFECYCLE_COMPONENT_ID))
            && lifecycle_active;
        let disbanding = self
            .engine
            .disbanding_in_progress(&gid)
            .map_err(session_engine_error)?;
        Ok(Some(GroupSnapshot {
            id: id.clone(),
            name: record.name,
            description: record.description,
            epoch,
            state,
            nostr_group_id,
            members: members
                .iter()
                .filter_map(|member| {
                    let bytes: [u8; 32] = member.id.as_slice().try_into().ok()?;
                    Some(MemberSnapshot {
                        account: AccountId(bytes),
                        admin: admins.contains(&bytes),
                    })
                })
                .collect(),
            relays,
            own_account_admin: admins.contains(&account.0),
            encrypted_media: self.media_policy(&gid)?,
            agent_text_stream: self.agent_stream_policy(&gid)?,
            avatar,
            retention_seconds,
            disband_enabled,
            disbanding,
        }))
    }

    /// `NotGroupAdmin` when this account is not a current admin — the
    /// session-side gate for admin ops so a refused call stages nothing
    /// (the engine may validate a commit only after staging it).
    fn require_admin(&self, group: &GroupId) -> Result<(), SessionError> {
        if !self
            .admin_set(&engine_gid(group))?
            .contains(&self.account().0)
        {
            return Err(SessionError::NotGroupAdmin);
        }
        Ok(())
    }

    /// The admin-policy component decoded: a QUIC-varint count followed by
    /// that many sorted 32-byte member keys (cgka-engine
    /// `app_components::decode_admin_policy`).
    fn admin_set(&self, group_id: &EngineGroupId) -> Result<Vec<[u8; 32]>, SessionError> {
        let Some(bytes) = self
            .engine
            .app_component(group_id, GROUP_ADMIN_POLICY_COMPONENT_ID)
            .map_err(session_engine_error)?
        else {
            return Ok(Vec::new());
        };
        let (len, prefix) =
            decode_quic_varint(&bytes).map_err(|_| SessionError::Internal("admin policy"))?;
        let len = usize::try_from(len).map_err(|_| SessionError::Internal("admin policy"))?;
        let body = bytes
            .get(prefix..)
            .ok_or(SessionError::Internal("admin policy"))?;
        if body.len() != len || len % 32 != 0 {
            return Err(SessionError::Internal("admin policy"));
        }
        body.as_chunks::<32>()
            .0
            .iter()
            .map(|chunk| {
                let mut key = [0u8; 32];
                key.copy_from_slice(chunk);
                Ok(key)
            })
            .collect()
    }

    // ---- helpers ----

    /// Relay validation: the spec URL profile plus the local safety policy.
    fn relay_allowed(&self, relay: &str) -> bool {
        if validate_nostr_relay_url(relay).is_err() {
            return false;
        }
        if self.destination_policy.allow_loopback {
            return true;
        }
        let Ok(url) = url::Url::parse(relay) else {
            return false;
        };
        match url.host() {
            Some(url::Host::Domain(domain)) => !is_loopback_host(url::Host::Domain(domain)),
            Some(url::Host::Ipv4(addr)) => is_public_ip(std::net::IpAddr::V4(addr)),
            Some(url::Host::Ipv6(addr)) => is_public_ip(std::net::IpAddr::V6(addr)),
            None => false,
        }
    }

    fn check_relays(&self, relays: &[String]) -> Result<(), SessionError> {
        if relays.is_empty() || !relays.iter().all(|relay| self.relay_allowed(relay)) {
            return Err(SessionError::InvalidRelayUrl);
        }
        Ok(())
    }

    /// Fanout listings this session's provider has served, across the
    /// engine's clone — the `report_publish` regression bound.
    #[cfg(feature = "testing")]
    #[must_use]
    pub fn outbound_fanout_list_count(&self) -> usize {
        self.provider.fanout_list_count()
    }
}

fn engine_gid(group: &GroupId) -> EngineGroupId {
    EngineGroupId::new(group.0.clone())
}

/// Refuse an `0x8006` component whose `required_member_roles` names a role
/// this device does not advertise. The own leaf is always a member of the
/// resulting epoch and carries only the receive role (`0xf2d1`), so a state
/// requiring send or fanout violates the spec's resulting-epoch invariant;
/// the pinned engine checks the mask only on invite and join, not on
/// `AppDataUpdate` commits. Undecodable bytes are left to the engine's own
/// validation.
fn check_agent_stream_roles(updates: &[ComponentUpdate]) -> Result<(), SessionError> {
    for update in updates {
        if update.component_id == AGENT_TEXT_STREAM_QUIC_COMPONENT_ID
            && let Some(policy) = AgentStreamPolicy::from_component(&update.data)
            && (policy.required_roles.contains(StreamRoles::SEND)
                || policy.required_roles.contains(StreamRoles::FANOUT))
        {
            return Err(SessionError::MissingRequiredCapabilities);
        }
    }
    Ok(())
}

/// One locator passes shape validation and the group's allowed-kinds list.
fn check_locator(policy: &EncryptedMediaPolicy, locator: &Locator) -> Result<(), SessionError> {
    validate_locator(locator)?;
    if !policy.allows_locator_kind(&locator.kind) {
        return Err(SessionError::Media(MediaError::LocatorNotAllowedByPolicy));
    }
    Ok(())
}

fn sha256(bytes: &[u8]) -> [u8; 32] {
    let digest = sha2::Sha256::digest(bytes);
    let mut out = [0u8; 32];
    out.copy_from_slice(&digest);
    out
}

fn outbound_id_of(message_id: &MessageId) -> Result<OutboundId, SessionError> {
    let bytes: [u8; 32] = message_id
        .as_slice()
        .try_into()
        .map_err(|_| SessionError::Internal("outbound id length"))?;
    Ok(OutboundId(bytes))
}

/// The host-rendered inner event a send produced. `UnsignedEvent::id()`
/// recomputes the NIP-01 id and must equal the engine-committed
/// `app_event.id`; a mismatch is an internal invariant failure.
fn unsigned_of_app_event(
    app_event: &cgka_traits::app_event::MarmotAppEvent,
) -> Result<UnsignedEvent, SessionError> {
    let pubkey = hex::decode(&app_event.pubkey)
        .ok()
        .and_then(|bytes| <[u8; 32]>::try_from(bytes.as_slice()).ok())
        .map(AccountId)
        .ok_or(SessionError::Internal("app event pubkey"))?;
    let unsigned = UnsignedEvent {
        pubkey,
        created_at: app_event.created_at,
        kind: u16::try_from(app_event.kind)
            .map_err(|_| SessionError::Internal("app event kind"))?,
        tags: app_event.tags.clone(),
        content: app_event.content.clone(),
    };
    if hex::encode(unsigned.id().0) != app_event.id {
        return Err(SessionError::Internal("app event id"));
    }
    Ok(unsigned)
}

/// The snapshot's avatar view of a decoded `0x8007` component: present
/// only for a non-empty URL, hints as UTF-8 strings.
fn group_avatar(decoded: GroupAvatarUrlV1) -> Option<GroupAvatar> {
    if decoded.url.is_empty() {
        return None;
    }
    let hint = |bytes: Vec<u8>| {
        String::from_utf8(bytes)
            .ok()
            .filter(|text| !text.is_empty())
    };
    Some(GroupAvatar {
        url: decoded.url,
        dim: hint(decoded.dim),
        thumbhash: hint(decoded.thumbhash),
    })
}

fn open_error_of(error: &EngineError) -> OpenError {
    OpenError::Engine {
        kind: error.privacy_safe_kind(),
    }
}

/// A `TransportMessage` back into the signed transport event (kind 445
/// group message or kind 1059 gift wrap).
fn transport_event(message: &TransportMessage) -> Option<Event> {
    let dto = transport_nostr_peeler::NostrTransportEvent::from_transport_message(message).ok()?;
    let nostr = dto.to_verified_nostr_event().ok()?;
    Some(Event::from_nostr_event(&nostr))
}

/// The canonical unsigned proof event a `PendingProof` hands to the signer.
fn proof_unsigned(request: &AccountIdentityProofRequest) -> Result<UnsignedEvent, OpenError> {
    let event = request.proof_event().map_err(|_| OpenError::Engine {
        kind: "invalid_account_identity_proof",
    })?;
    Ok(UnsignedEvent::from_nostr_unsigned(&event))
}

/// Convert one fanout into the host-facing [`Outbound`]. `None` when the
/// frozen event cannot be re-derived — a corrupt durable row the next
/// storage-touching call will surface.
fn fanout_outbound(fanout: &OutboundFanout, id: OutboundId) -> Option<Outbound> {
    let message = &fanout.request().message;
    let event = transport_event(message)?;
    let purpose = match &message.envelope {
        TransportEnvelope::Welcome { .. } => OutboundPurpose::Welcome,
        TransportEnvelope::GroupMessage { .. } => {
            if fanout.pending_ref().is_some() {
                OutboundPurpose::Commit
            } else if fanout.application_message().is_some() {
                OutboundPurpose::ApplicationMessage
            } else {
                OutboundPurpose::Proposal
            }
        }
    };
    let relays = fanout
        .outstanding_target_indexes()
        .into_iter()
        .filter_map(|index| {
            fanout
                .request()
                .target
                .endpoints()
                .get(index)
                .map(|endpoint| endpoint.0.clone())
        })
        .collect();
    Some(Outbound {
        id,
        event,
        relays,
        purpose,
    })
}

/// The [`RelayStatus`] → fanout failure-kind mapping. `Accepted` never
/// reaches here (it is matched above).
const fn endpoint_failure_kind(status: RelayStatus) -> TransportEndpointFailureKind {
    match status {
        RelayStatus::Rejected => TransportEndpointFailureKind::TerminalRejected,
        RelayStatus::Unavailable | RelayStatus::NotSent => {
            TransportEndpointFailureKind::RetryableUnavailable
        }
        RelayStatus::Unknown | RelayStatus::Accepted => {
            TransportEndpointFailureKind::PossiblyExposed
        }
    }
}

/// Whether a reported status leaves the item retryable — the statuses that
/// pace a re-offer through the session's backoff rows.
const fn retryable_status(status: RelayStatus) -> bool {
    matches!(
        status,
        RelayStatus::Unavailable | RelayStatus::NotSent | RelayStatus::Unknown
    )
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::shadow_unrelated,
    reason = "test code"
)]
mod proof_tests {
    //! Account-identity-proof flow tests. The deferred-build discriminator
    //! test is the one sanctioned pin seam: it asserts the exact
    //! `EngineError` variant MDK v0.17.0 returns when the proof signer
    //! defers, because our open path classifies `NeedsAccountProof` on the
    //! captured request alone (no string matching).

    #[cfg(feature = "testing")]
    use super::*;
    #[cfg(feature = "testing")]
    use crate::testing::{LocalSigner, ManualClock, RecordingHost};

    #[cfg(feature = "testing")]
    fn config(signer: Arc<dyn AccountSigner>, host: Arc<RecordingHost>) -> SessionConfig {
        SessionConfig {
            signer,
            host,
            rows: Vec::new(),
            clock: Arc::new(ManualClock::new()),
            destination_policy: DestinationPolicy {
                allow_loopback: true,
            },
            client_name: None,
        }
    }

    /// MDK v0.17.0 maps a deferring `AccountIdentityProofSigner` error
    /// through `EngineError::Other` at engine.rs `Identity::load_or_generate`.
    /// This test is the canary: if a pin bump ever changes that variant, the
    /// capture-based classification stays correct but this test must be
    /// updated to name the new variant.
    #[cfg(feature = "testing")]
    #[test]
    fn deferred_build_returns_other_with_captured_request() {
        let host = Arc::new(RecordingHost::new());
        let provider = KvStorageProvider::open(Vec::new(), host).expect("provider opens");
        let signer: Arc<dyn AccountSigner> = Arc::new(LocalSigner::new());
        let clock: Arc<dyn Clock> = Arc::new(ManualClock::new());
        let capture = Arc::new(DeferringProofSigner::new());
        let cloned = Arc::clone(&capture);
        let proof_signer: Arc<dyn AccountIdentityProofSigner> = cloned;
        let latch = Arc::new(SignerLatch::new(provider.store().clone()));
        let result = build_engine(&provider, &signer, &clock, proof_signer, &latch);
        assert!(
            matches!(result, Err(EngineError::Other(_))),
            "pinned deferred-build variant must be EngineError::Other"
        );
        assert!(capture.captured().is_some(), "proof request was captured");
    }

    /// After a stored binding exists, engine construction must not invoke
    /// the proof signer at all — a refusing signer over the durable rows
    /// still builds.
    #[cfg(feature = "testing")]
    #[tokio::test(flavor = "current_thread")]
    async fn stored_proof_skips_signer_on_rebuild() {
        let host = Arc::new(RecordingHost::new());
        let signer: Arc<dyn AccountSigner> = Arc::new(LocalSigner::new());
        let pending =
            match Session::open(config(Arc::clone(&signer), Arc::clone(&host))).expect("open") {
                Opened::NeedsAccountProof(pending) => pending,
                Opened::Ready(_) => panic!("first open must require a proof"),
            };
        let signed = signer
            .sign_event(pending.request().clone())
            .await
            .expect("signer signs");
        pending
            .provide_account_proof(&signed)
            .expect("proof accepted");

        let rows = host.durable_rows();
        let provider = KvStorageProvider::open(rows, host).expect("reopen");
        let clock: Arc<dyn Clock> = Arc::new(ManualClock::new());
        let refusing = Arc::new(RefusingProofSigner::new());
        let refusing_cloned = Arc::clone(&refusing);
        let refusing_dyn: Arc<dyn AccountIdentityProofSigner> = refusing_cloned;
        let latch = Arc::new(SignerLatch::new(provider.store().clone()));
        let engine = build_engine(&provider, &signer, &clock, refusing_dyn, &latch);
        assert!(
            engine.is_ok(),
            "stored binding must rebuild without signing"
        );
        assert_eq!(refusing.calls(), 0, "proof signer must not be invoked");
    }

    /// Wrong-account, wrong-event and bad-signature rejections return their
    /// typed errors and hand the pending open back for retry.
    #[cfg(feature = "testing")]
    #[tokio::test(flavor = "current_thread")]
    async fn proof_rejections_are_typed_and_retriable() {
        let host = Arc::new(RecordingHost::new());
        let signer: Arc<dyn AccountSigner> = Arc::new(LocalSigner::new());
        let pending =
            match Session::open(config(Arc::clone(&signer), Arc::clone(&host))).expect("open") {
                Opened::NeedsAccountProof(pending) => pending,
                Opened::Ready(_) => panic!("first open must require a proof"),
            };

        let other: Arc<dyn AccountSigner> = Arc::new(LocalSigner::new());
        let mut wrong_account_unsigned = pending.request().clone();
        wrong_account_unsigned.pubkey = other.account();
        let wrong_account = other
            .sign_event(wrong_account_unsigned)
            .await
            .expect("other signer signs");
        let rejected = pending
            .provide_account_proof(&wrong_account)
            .expect_err("wrong account must be rejected");
        assert!(matches!(rejected.error, ProofError::WrongAccount));
        let pending = *rejected.pending;

        let mut wrong_event_unsigned = pending.request().clone();
        wrong_event_unsigned.created_at = wrong_event_unsigned.created_at.saturating_add(1);
        let wrong_event = signer
            .sign_event(wrong_event_unsigned)
            .await
            .expect("signer signs");
        let rejected = pending
            .provide_account_proof(&wrong_event)
            .expect_err("wrong event must be rejected");
        assert!(matches!(rejected.error, ProofError::WrongEvent));
        let pending = *rejected.pending;

        let mut bad_sig = signer
            .sign_event(pending.request().clone())
            .await
            .expect("signer signs");
        bad_sig.sig = bad_sig.sig.map(|byte| byte.wrapping_add(1));
        let rejected = pending
            .provide_account_proof(&bad_sig)
            .expect_err("bad signature must be rejected");
        assert!(matches!(
            rejected.error,
            ProofError::WrongEvent | ProofError::InvalidSignature
        ));

        let signed = signer
            .sign_event(rejected.pending.request().clone())
            .await
            .expect("signer signs");
        rejected
            .pending
            .provide_account_proof(&signed)
            .expect("retry with the right proof succeeds");
    }
}
