//! Inbound classification and session-visible events.
//!
//! [`InputOutcome`] projects MDK's [`IngestOutcome`] onto the input
//! categories and dispositions of `spec/foundation/errors.md`. The mapping is
//! exhaustive — every `IngestOutcome`, `InputRejectionCategory`,
//! `ProposalRejectionCategory`, `StaleReason` (deprecated variants included)
//! and `LocalIngestState` variant has an explicit arm, so a pin bump that adds
//! a variant fails to compile rather than silently reclassifying input.
//!
//! Mapping table (one line per arm):
//!
//! | MDK variant | `InputOutcome` | why |
//! |---|---|---|
//! | `Processed` | `Accepted` | applied to the selected branch |
//! | `GroupRecovery` | `RecoveryOffered` | authenticated control input; recovery is a later milestone |
//! | `Buffered` | `Deferred { None }` | parked pending a local transition; no protocol category yet |
//! | `Ignored { category }` | `Rejected { mapped }` | pre-convergence routing/dedup rejection |
//! | `LocalState { state }` | `Local(mapped)` | canonical local condition, not a protocol verdict |
//! | `TransportDeferred` | `Unavailable { TransportDeferred }` | undecodable now, recoverable later |
//! | `ResourceRefused` | `Unavailable { ResourceRefused }` | local bound, never a duplicate |
//! | `Stale { reason }` | `Stale`/`Rejected`/`Local` (mapped) | epoch-history cause, or a deprecated arm's documented replacement |
//! | `Rejected { category }` | `Rejected { mapped }` | semantic admission failure |
//!
//! `StaleReason` maps to `Stale { StaleEpoch }` for every epoch-history
//! cause (`AlreadyAtEpoch`, `PreMembership`, `PredatesLocalCopy`,
//! `BeyondAnchor`, `BeyondRollbackHorizon`, `BeyondAppRetention`,
//! `LosingBranch`, `InvalidAgainstCanonicalState`); the deprecated variants
//! map to their documented replacement outcomes rather than `Stale`:
//! `AlreadySeen`→`Rejected { Duplicate }`,
//! `NotForThisClient`→`Rejected { WrongRecipient }`,
//! `UnknownGroup`→`Rejected { UnknownGroup }`,
//! `OwnEcho`→`Rejected { OwnEcho }`,
//! `SelfEvicted`→`Local(Removed)`, `Quarantined`→`Local(Quarantined)`.
//! `ProposalRejectionCategory` maps to `AuthorizationFailed`,
//! `UnsupportedRequiredFeature`, `InvalidEncoding`, `InvalidSignature` and
//! `AuthorizationFailed` (`InvalidSelfRemove` is an authorization rule).
//! `InputRejectionCategory::MissingWelcomeKeyPackage` maps to
//! `WrongRecipient` (a rumor naming a `KeyPackage` this device did not
//! publish is not addressed to this device — spec `transports/nostr.md`
//! "Welcome delivery"); `NotAddressedToThisDevice` maps to
//! `WrongRecipient`.

use cgka_traits::app_event::{
    AppMessageAuthority, AppMessageRetentionDecision, MARMOT_APP_EVENT_KIND_AGENT_STREAM_START,
    MARMOT_APP_EVENT_KIND_CHAT, MarmotAppEvent,
};
use cgka_traits::engine::GroupEvent;
use cgka_traits::ingest::{
    IngestOutcome, InputRejectionCategory, LocalIngestState, ProposalRejectionCategory, StaleReason,
};
use cgka_traits::{EncryptedMediaSecret, EpochId, GroupId as EngineGroupId, MemberId, MessageId};
use zeroize::Zeroizing;

use crate::agent_stream::{self, StreamFinal};
use crate::media::{self, AttachmentRejection, MediaReference};
use crate::outbox::PublishResolution;
use crate::semantics::{self, DropReason, Modifier, Semantics};
use crate::types::{AccountId, EventId, GroupId, OutboundId};

/// Spec `foundation/errors.md` input categories. `as_str` returns the exact
/// spec name.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum InputCategory {
    /// `duplicate`
    Duplicate,
    /// `own_echo`
    OwnEcho,
    /// `wrong_recipient`
    WrongRecipient,
    /// `unknown_group`
    UnknownGroup,
    /// `stale_epoch`
    StaleEpoch,
    /// `invalid_encoding`
    InvalidEncoding,
    /// `invalid_signature`
    InvalidSignature,
    /// `unsupported_required_feature`
    UnsupportedRequiredFeature,
    /// `authorization_failed`
    AuthorizationFailed,
    /// `missing_history`
    MissingHistory,
    /// `transport_deferred`
    TransportDeferred,
    /// `resource_refused`
    ResourceRefused,
    /// `transport_rejected`
    TransportRejected,
}

impl InputCategory {
    /// Every variant, in declaration order. A spec-parity test compares this
    /// against the recorded spec vocabulary.
    pub const ALL: [Self; 13] = [
        Self::Duplicate,
        Self::OwnEcho,
        Self::WrongRecipient,
        Self::UnknownGroup,
        Self::StaleEpoch,
        Self::InvalidEncoding,
        Self::InvalidSignature,
        Self::UnsupportedRequiredFeature,
        Self::AuthorizationFailed,
        Self::MissingHistory,
        Self::TransportDeferred,
        Self::ResourceRefused,
        Self::TransportRejected,
    ];

    /// The spec name (`snake_case`) of this category.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Duplicate => "duplicate",
            Self::OwnEcho => "own_echo",
            Self::WrongRecipient => "wrong_recipient",
            Self::UnknownGroup => "unknown_group",
            Self::StaleEpoch => "stale_epoch",
            Self::InvalidEncoding => "invalid_encoding",
            Self::InvalidSignature => "invalid_signature",
            Self::UnsupportedRequiredFeature => "unsupported_required_feature",
            Self::AuthorizationFailed => "authorization_failed",
            Self::MissingHistory => "missing_history",
            Self::TransportDeferred => "transport_deferred",
            Self::ResourceRefused => "resource_refused",
            Self::TransportRejected => "transport_rejected",
        }
    }
}

/// Spec `foundation/errors.md` convergence dispositions.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum Disposition {
    /// `accepted`
    Accepted,
    /// `deferred`
    Deferred,
    /// `stale`
    Stale,
    /// `invalidated`
    Invalidated,
}

impl Disposition {
    /// Every variant, in declaration order.
    pub const ALL: [Self; 4] = [
        Self::Accepted,
        Self::Deferred,
        Self::Stale,
        Self::Invalidated,
    ];

    /// The spec name of this disposition.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Accepted => "accepted",
            Self::Deferred => "deferred",
            Self::Stale => "stale",
            Self::Invalidated => "invalidated",
        }
    }
}

/// Why an input is not currently processable: the availability cause behind
/// `InputOutcome::Unavailable`.
///
/// `TransportDeferred`/`ResourceRefused` are the spec's own categories; the
/// `signer_*` causes are session-local and report under the spec's
/// `resource_refused` umbrella (spec `transports/nostr.md` resource-refused
/// rule).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum Availability {
    /// `transport_deferred` — undecodable now, recoverable later.
    TransportDeferred,
    /// `resource_refused` — a local bound refused it.
    ResourceRefused,
    /// `signer_refused` — the account signer declined the unwrap.
    SignerRefused,
    /// `signer_failed` — the account signer failed the unwrap.
    SignerFailed,
}

impl Availability {
    /// Every variant, in declaration order.
    pub const ALL: [Self; 4] = [
        Self::TransportDeferred,
        Self::ResourceRefused,
        Self::SignerRefused,
        Self::SignerFailed,
    ];

    /// The stable `snake_case` name of this cause.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::TransportDeferred => "transport_deferred",
            Self::ResourceRefused => "resource_refused",
            Self::SignerRefused => "signer_refused",
            Self::SignerFailed => "signer_failed",
        }
    }

    /// The spec category this cause reports under.
    const fn spec_category(self) -> InputCategory {
        match self {
            Self::TransportDeferred => InputCategory::TransportDeferred,
            Self::ResourceRefused | Self::SignerRefused | Self::SignerFailed => {
                InputCategory::ResourceRefused
            }
        }
    }
}

/// A canonical local ingest condition (MDK `LocalIngestState`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum LocalState {
    /// A replacement Welcome awaits explicit local consent.
    RejoinConfirmationRequired,
    /// Authenticated state recorded this device removed.
    Removed,
    /// The local copy is frozen pending repair.
    Quarantined,
}

/// The typed outcome of one admitted input.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum InputOutcome {
    /// Applied to the selected canonical branch.
    Accepted,
    /// Parked; reconsidered when its precondition arrives.
    Deferred {
        /// Why it is deferred, when a spec category applies.
        category: Option<InputCategory>,
    },
    /// Can no longer affect the group.
    Stale {
        /// Why it is stale.
        category: InputCategory,
    },
    /// Rejected before convergence with a protocol category.
    Rejected {
        /// Why it was rejected.
        category: InputCategory,
    },
    /// Not currently processable for an availability reason (transport
    /// decode/decrypt, a local resource bound, or a signer refusal); not a
    /// protocol verdict.
    Unavailable {
        /// Why it is unavailable.
        category: Availability,
    },
    /// A canonical local condition blocked processing.
    Local(LocalState),
    /// An authenticated recovery offer arrived; recovery is a later
    /// milestone, so nothing was applied.
    RecoveryOffered,
}

impl InputOutcome {
    /// The spec disposition this outcome carries, if any. Pre-convergence
    /// rejections and availability categories have none.
    #[must_use]
    pub const fn disposition(self) -> Option<Disposition> {
        match self {
            Self::Accepted => Some(Disposition::Accepted),
            Self::Deferred { .. } => Some(Disposition::Deferred),
            Self::Stale { .. } => Some(Disposition::Stale),
            Self::Rejected { .. }
            | Self::Unavailable { .. }
            | Self::Local(_)
            | Self::RecoveryOffered => None,
        }
    }

    /// The spec category this outcome carries, if any.
    #[must_use]
    pub const fn category(self) -> Option<InputCategory> {
        match self {
            Self::Accepted | Self::Local(_) | Self::RecoveryOffered => None,
            Self::Deferred { category } => category,
            Self::Stale { category } | Self::Rejected { category } => Some(category),
            Self::Unavailable { category } => Some(category.spec_category()),
        }
    }
}

/// Per-input report of [`crate::Session::ingest`]/[`crate::Session::ingest_batch`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct InputReport {
    /// The transport event that was processed.
    pub event: EventId,
    /// Its classification.
    pub outcome: InputOutcome,
}

const fn input_rejection_category(category: InputRejectionCategory) -> InputCategory {
    match category {
        InputRejectionCategory::Duplicate => InputCategory::Duplicate,
        InputRejectionCategory::OwnEcho => InputCategory::OwnEcho,
        InputRejectionCategory::WrongRecipient
        | InputRejectionCategory::NotAddressedToThisDevice
        | InputRejectionCategory::MissingWelcomeKeyPackage => InputCategory::WrongRecipient,
        InputRejectionCategory::UnknownGroup => InputCategory::UnknownGroup,
        InputRejectionCategory::InvalidEncoding => InputCategory::InvalidEncoding,
        InputRejectionCategory::InvalidSignature => InputCategory::InvalidSignature,
        InputRejectionCategory::UnsupportedRequiredFeature => {
            InputCategory::UnsupportedRequiredFeature
        }
        InputRejectionCategory::AuthorizationFailed => InputCategory::AuthorizationFailed,
    }
}

// The enum still carries the pre-Ignored variants: deprecated in v0.16.0
// but constructible in the pin, so the match stays exhaustive over them and
// each maps to the outcome its `#[deprecated]` note names rather than to
// `Stale`.
#[allow(
    deprecated,
    reason = "StaleReason keeps its pre-Ignored variants in the pinned API"
)]
const fn stale_outcome(reason: &StaleReason) -> InputOutcome {
    match reason {
        StaleReason::AlreadySeen => InputOutcome::Rejected {
            category: InputCategory::Duplicate,
        },
        StaleReason::NotForThisClient => InputOutcome::Rejected {
            category: InputCategory::WrongRecipient,
        },
        StaleReason::UnknownGroup => InputOutcome::Rejected {
            category: InputCategory::UnknownGroup,
        },
        StaleReason::OwnEcho => InputOutcome::Rejected {
            category: InputCategory::OwnEcho,
        },
        StaleReason::SelfEvicted => InputOutcome::Local(LocalState::Removed),
        StaleReason::Quarantined => InputOutcome::Local(LocalState::Quarantined),
        StaleReason::AlreadyAtEpoch { .. }
        | StaleReason::PreMembership
        | StaleReason::PredatesLocalCopy
        | StaleReason::BeyondAnchor
        | StaleReason::BeyondRollbackHorizon
        | StaleReason::BeyondAppRetention
        | StaleReason::LosingBranch
        | StaleReason::InvalidAgainstCanonicalState => InputOutcome::Stale {
            category: InputCategory::StaleEpoch,
        },
    }
}

const fn proposal_rejection_category(category: ProposalRejectionCategory) -> InputCategory {
    match category {
        ProposalRejectionCategory::AuthorizationFailed
        | ProposalRejectionCategory::InvalidSelfRemove => InputCategory::AuthorizationFailed,
        ProposalRejectionCategory::UnsupportedProposal => InputCategory::UnsupportedRequiredFeature,
        ProposalRejectionCategory::InvalidEncoding => InputCategory::InvalidEncoding,
        ProposalRejectionCategory::InvalidSignature => InputCategory::InvalidSignature,
    }
}

const fn local_state(state: LocalIngestState) -> LocalState {
    match state {
        LocalIngestState::RejoinConfirmationRequired => LocalState::RejoinConfirmationRequired,
        LocalIngestState::Removed => LocalState::Removed,
        LocalIngestState::Quarantined => LocalState::Quarantined,
    }
}

/// Map the engine's ingest outcome onto the spec vocabulary. Exhaustive by
/// construction.
pub(crate) const fn input_outcome(outcome: &IngestOutcome) -> InputOutcome {
    match outcome {
        IngestOutcome::Processed => InputOutcome::Accepted,
        IngestOutcome::GroupRecovery { .. } => InputOutcome::RecoveryOffered,
        IngestOutcome::Buffered { .. } => InputOutcome::Deferred { category: None },
        IngestOutcome::Ignored { category } => InputOutcome::Rejected {
            category: input_rejection_category(*category),
        },
        IngestOutcome::LocalState { state } => InputOutcome::Local(local_state(*state)),
        IngestOutcome::TransportDeferred { .. } => InputOutcome::Unavailable {
            category: Availability::TransportDeferred,
        },
        IngestOutcome::ResourceRefused { .. } => InputOutcome::Unavailable {
            category: Availability::ResourceRefused,
        },
        IngestOutcome::Stale { reason } => stale_outcome(reason),
        IngestOutcome::Rejected { category } => InputOutcome::Rejected {
            category: proposal_rejection_category(*category),
        },
    }
}

fn account_of(member: &MemberId) -> Option<AccountId> {
    let bytes: [u8; 32] = member.as_slice().try_into().ok()?;
    Some(AccountId(bytes))
}

fn event_id_of(message: &MessageId) -> Option<EventId> {
    let bytes: [u8; 32] = message.as_slice().try_into().ok()?;
    Some(EventId(bytes))
}

fn group_of(id: &EngineGroupId) -> GroupId {
    GroupId(id.as_slice().to_vec())
}

/// A decoded group application message (the inner `MarmotAppEvent` plus its
/// authenticated sender).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReceivedMessage {
    /// Canonical inner id (hex string, NIP-01 commitment).
    pub id: String,
    /// Sender account (MLS-authenticated).
    pub sender: AccountId,
    /// Inner `created_at`.
    pub created_at: u64,
    /// Inner kind.
    pub kind: u64,
    /// Inner tags.
    pub tags: Vec<Vec<String>>,
    /// Inner content.
    pub content: String,
    /// MLS epoch the message was sent under.
    pub epoch: u64,
    /// The transport event that carried it.
    pub transport_id: EventId,
    /// One entry per `imeta` tag, in tag order, for kind-9 messages (media
    /// messages are regular kind-9 chat app events); empty for every other
    /// kind. A rejected attachment never drops its siblings or the message.
    pub attachments: Vec<Result<MediaReference, AttachmentRejection>>,
    /// The agent-stream final anchor, when this kind-9 message carries all
    /// four `stream*` tags well-formed.
    pub stream_final: Option<StreamFinal>,
    /// The typed meaning of the inner event.
    pub semantics: Semantics,
    /// Source-epoch retention expiry (`engine.retention.expires_at`):
    /// `None` means no expiry or no recoverable policy.
    pub expires_at: Option<u64>,
}

/// The anchors of a delivered agent text stream start (kind 1200).
///
/// The host acks it with
/// [`Session::acknowledge_messages`](crate::Session::acknowledge_messages)
/// on `transport_id` like any message, may ignore it entirely (the receive
/// role never fetches QUIC candidates — `route`/`broker` tags are dropped
/// at the boundary), and withdraws any preview on [`SessionEvent::MessageInvalidated`]
/// for that transport id.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct AgentStreamAnchor {
    /// Canonical inner id of the kind-1200 event — the id later stream
    /// finals reference as `stream-start`.
    pub id: String,
    /// Sender account (MLS-authenticated).
    pub sender: AccountId,
    /// Inner `created_at`.
    pub created_at: u64,
    /// MLS epoch the start was sent under.
    pub epoch: u64,
    /// The transport event that carried it.
    pub transport_id: EventId,
    /// The 32-byte stream id.
    pub stream_id: [u8; 32],
    /// The prompt message this stream answers, when tagged.
    pub parent: Option<EventId>,
    /// Source-epoch retention expiry (`engine.retention.expires_at`):
    /// `None` means no expiry or no recoverable policy.
    pub expires_at: Option<u64>,
}

/// The state delta a `GroupStateChanged` event carries.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum GroupChange {
    /// A member account was added.
    MemberAdded {
        /// The account.
        account: AccountId,
    },
    /// A member account was removed.
    MemberRemoved {
        /// The account.
        account: AccountId,
    },
    /// A member account left via `SelfRemove`.
    MemberLeft {
        /// The account.
        account: AccountId,
    },
    /// A member gained admin.
    AdminAdded {
        /// The account.
        account: AccountId,
    },
    /// A member lost admin.
    AdminRemoved {
        /// The account.
        account: AccountId,
    },
    /// The group profile changed.
    Renamed {
        /// Previous name.
        old_name: String,
        /// New name.
        new_name: String,
    },
    /// The group avatar component changed.
    AvatarChanged,
    /// The message retention policy changed.
    RetentionChanged {
        /// Previous retention in seconds.
        old_seconds: u64,
        /// New retention in seconds.
        new_seconds: u64,
    },
    /// The group was disbanded.
    Disbanded,
}

/// Events the session surfaces to the host.
///
/// Exhaustively maps [`GroupEvent`]; variants belonging to features this
/// crate does not implement yet are matched explicitly and documented,
/// never silently dropped by a wildcard arm.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum SessionEvent {
    /// A group application message decrypted on the canonical branch.
    MessageReceived {
        /// The group.
        group: GroupId,
        /// The decoded message.
        message: ReceivedMessage,
    },
    /// An agent text stream start event (kind 1200) whose anchors passed the
    /// profile's parse. Emitted instead of [`Self::MessageReceived`]; a
    /// kind-1200 payload whose anchors fail is delivered as an ordinary
    /// message. Parsing is pure — no outbound, no I/O, and this crate holds
    /// no network dependency for the host's QUIC fetches either.
    AgentStreamStarted {
        /// The group.
        group: GroupId,
        /// The decoded anchors.
        start: AgentStreamAnchor,
    },
    /// A previously delivered application message decrypted only on a losing
    /// branch; the host must withdraw it.
    MessageInvalidated {
        /// The group.
        group: GroupId,
        /// Transport event id of the invalidated message.
        message: EventId,
        /// Source epoch.
        epoch: u64,
    },
    /// This device created a group.
    GroupCreated {
        /// The group.
        group: GroupId,
    },
    /// This device joined a group via Welcome.
    GroupJoined {
        /// The group.
        group: GroupId,
    },
    /// A post-join or periodic self-update obligation completed — the
    /// group's `SelfUpdated` rotation of this device's leaf is durable.
    /// Emitted exactly once per obligation, when its evolution confirms or
    /// the own-leaf baseline is observed rotated.
    SelfUpdated {
        /// The group.
        group: GroupId,
    },
    /// The group's canonical epoch advanced.
    EpochChanged {
        /// The group.
        group: GroupId,
        /// Previous epoch.
        from: u64,
        /// New epoch.
        to: u64,
    },
    /// An MLS-authenticated group-state change was applied.
    GroupStateChanged {
        /// The group.
        group: GroupId,
        /// Epoch reached.
        epoch: u64,
        /// The authenticated actor, when attributable.
        actor: Option<AccountId>,
        /// What changed.
        change: GroupChange,
        /// The commit that produced the change (for later invalidation).
        origin_commit: Option<EventId>,
        /// Canonical row id (`group_system_canonical_id`): identical on
        /// every member that applies the same commit and on re-delivery —
        /// hosts upsert system rows by it and withdraw rows whose
        /// `origin_commit` a `GroupStateInvalidated` names.
        row_id: String,
    },
    /// A commit's state notifications were withdrawn because it lost a fork.
    GroupStateInvalidated {
        /// The group.
        group: GroupId,
        /// The fork's source epoch.
        epoch: u64,
        /// The superseded commit's transport id.
        commit: EventId,
    },
    /// A withdrawn commit's state notifications are canonical again.
    GroupStateRevalidated {
        /// The group.
        group: GroupId,
        /// The fork's source epoch.
        epoch: u64,
        /// The re-adopted commit's transport id.
        commit: EventId,
    },
    /// This device's local group copy ended (leave, removal, disband, or
    /// reset).
    LocalCopyTerminated {
        /// The group.
        group: GroupId,
        /// Whether the departure was voluntary.
        voluntary: bool,
    },
    /// The device's participation was restored by branch selection.
    LocalCopyRestored {
        /// The group.
        group: GroupId,
    },
    /// A queued send was refused once the group settled.
    QueuedIntentRefused {
        /// The group.
        group: GroupId,
        /// The queued intent's id (as returned when it was accepted).
        intent: OutboundId,
        /// Stable name of the intent kind.
        intent_kind: String,
        /// Stable privacy-safe refusal name (MDK `privacy_safe_kind`).
        reason: String,
    },
    /// A persisted group failed hydration at open and is quarantined.
    HydrationQuarantined {
        /// The group.
        group: GroupId,
    },
    /// A quarantined group hydrated successfully on retry.
    HydrationRecovered {
        /// The group.
        group: GroupId,
        /// Epoch recovered at.
        epoch: u64,
    },
    /// A staged commit orphaned by a crash was cleared at open; the group is
    /// usable again and the host should resync it.
    PendingCommitRecovered {
        /// The group.
        group: GroupId,
        /// Epoch recovered at.
        epoch: u64,
    },
    /// The group needs a verified repair path; it stopped applying
    /// group-state changes.
    GroupUnrecoverable {
        /// The group.
        group: GroupId,
    },
    /// The stall detector counted `STALL_EVENTS` distinct deferred events
    /// at the group's current epoch — the host should refetch the group
    /// once (D2).
    GroupStallSuspected {
        /// The group.
        group: GroupId,
    },
    /// The group's refetch completed and the deferred-event count stayed
    /// full at the same epoch — the copy is flagged `stalled` until epoch
    /// progress or an accepted current-epoch message clears it (D2).
    GroupStalled {
        /// The group.
        group: GroupId,
    },
    /// A pending `KeyPackage` replacement was promoted after a relay
    /// acknowledgement (D3); the previous current is retained for its
    /// grace period.
    KeyPackageRotated,
    /// An outbound obligation reached a resolution checkpoint.
    PublishResolved {
        /// The outbound item.
        outbound: OutboundId,
        /// The verdict.
        resolution: PublishResolution,
    },
    /// A decrypted application event was dropped instead of delivered;
    /// the session acknowledged its pending row itself inside the drain.
    AppEventDropped {
        /// The group.
        group: GroupId,
        /// Why it was dropped.
        reason: DropReason,
    },
    /// A delivered message's retention expiry fired inside `tick`: the
    /// session deleted its index row and media secret; the host should
    /// retire its copy (the message may still be re-ingested, where
    /// `expired` drops it).
    MessageExpired {
        /// The group.
        group: GroupId,
        /// Inner event id of the expired message.
        message: EventId,
        /// Transport event id of the expired message.
        transport_id: EventId,
    },
}

/// The mapped session event plus the media-secret row effects
/// [`Session::drain_events`] applies alongside it.
pub(crate) struct MappedEvent {
    /// The session event to surface.
    pub event: SessionEvent,
    /// `(group, transport)` media-secret row [`Session::drain_events`]
    /// writes with the event — the captured source-epoch exporter secret of
    /// a media-carrying message.
    pub secret_write: Option<(EngineGroupId, MessageId, Zeroizing<Vec<u8>>)>,
    /// `(group, transport)` media-secret row to delete (the message's media
    /// secret is no longer needed once it is invalidated).
    pub secret_delete: Option<(EngineGroupId, MessageId)>,
    /// Pending application-event row the session acknowledges itself inside
    /// the drain's storage unit (the event dropped unverified, malformed,
    /// unauthorized, or expired — never a media secret written).
    pub drop_pending: Option<MessageId>,
    /// The kind 1009/5 modifier this `MessageReceived` carries, for the
    /// session to resolve against the delivered index. The mapped
    /// semantics is a placeholder; resolution delivers the authorized
    /// `Semantics` or drops the pending row.
    pub modifier: Option<Modifier>,
}

impl MappedEvent {
    /// An event with no media-secret side effects.
    const fn plain(event: SessionEvent) -> Self {
        Self {
            event,
            secret_write: None,
            secret_delete: None,
            drop_pending: None,
            modifier: None,
        }
    }
}

/// Map one engine event to a session event plus its media-secret row
/// effects. Returns `None` for variants that belong to device-internal
/// bookkeeping or to features this crate does not implement — matched
/// explicitly, never by wildcard:
///
/// - `GroupMemberLeavesRemoved`: device-destination bookkeeping for MDK's own
///   push-token cleanup; account-level departure already arrives via
///   `GroupStateChanged::MemberRemoved`/`MemberLeft`.
/// - `CommitRolledBack`: the commit-level counterpart of
///   `GroupStateInvalidated`, which is already surfaced with the same id.
/// - `TransportObjectResourceRefused`: internal history-recovery arming
///   signal (retained-object budget), not application state.
/// - `GroupHydrationQuarantined`/`GroupHydrationRecovered`/
///   `PendingCommitRecovered`/`GroupUnrecoverable`: surfaced.
/// - `AppMessageInvalidated` with an undecodable `decrypted_payload_ref`:
///   still surfaced as `MessageInvalidated` (the payload ref is diagnostic
///   only and carries no additional meaning for the host); its media-secret
///   row (if any) is retired with the event.
pub(crate) fn session_event(event: GroupEvent, now_secs: u64) -> Option<MappedEvent> {
    match event {
        GroupEvent::MessageReceived {
            group_id,
            message_id,
            sender,
            epoch,
            payload,
            retention,
            authority,
            encrypted_media_secret,
            ..
        } => message_received(
            group_id,
            message_id,
            &sender,
            epoch,
            &payload,
            retention,
            authority,
            encrypted_media_secret,
            now_secs,
        ),
        GroupEvent::AppMessageInvalidated {
            group_id,
            message_id,
            epoch,
            ..
        } => Some(MappedEvent {
            event: SessionEvent::MessageInvalidated {
                group: group_of(&group_id),
                message: event_id_of(&message_id)?,
                epoch: epoch.0,
            },
            secret_write: None,
            secret_delete: Some((group_id, message_id)),
            drop_pending: None,
            modifier: None,
        }),
        GroupEvent::GroupCreated { group_id } => {
            Some(MappedEvent::plain(SessionEvent::GroupCreated {
                group: group_of(&group_id),
            }))
        }
        GroupEvent::GroupJoined { group_id, .. } => {
            Some(MappedEvent::plain(SessionEvent::GroupJoined {
                group: group_of(&group_id),
            }))
        }
        GroupEvent::EpochChanged { group_id, from, to } => {
            Some(MappedEvent::plain(SessionEvent::EpochChanged {
                group: group_of(&group_id),
                from: from.0,
                to: to.0,
            }))
        }
        GroupEvent::GroupStateChanged {
            group_id,
            epoch,
            actor,
            change,
            origin_commit_id,
        } => {
            // The canonical row id binds the engine change verbatim —
            // including fields the mapped `GroupChange` drops (a rename's
            // `previous_name`) — so compute it before mapping. The id
            // function errors only for `GroupDisbanded` without an actor,
            // which the engine never emits; that malformed event is
            // dropped here.
            let row_id = cgka_traits::app_event::group_system_canonical_id(
                &group_id,
                epoch.0,
                actor.as_ref(),
                &change,
            )
            .ok()?;
            let change = group_change(change)?;
            Some(MappedEvent::plain(SessionEvent::GroupStateChanged {
                group: group_of(&group_id),
                epoch: epoch.0,
                actor: actor.as_ref().and_then(account_of),
                change,
                origin_commit: origin_commit_id.as_ref().and_then(event_id_of),
                row_id,
            }))
        }
        other => session_event_rest(other).map(MappedEvent::plain),
    }
}

/// Map a decrypted application message to its session event plus the
/// media-secret write it carries. A message the receive-side rules drop
/// (expired, unauthorized, unverifiable, malformed) maps to
/// [`SessionEvent::AppEventDropped`] and acknowledges its pending row
/// inside the drain — classification is stateless, so a restart between
/// ingest and ack re-delivers the same decision.
#[allow(
    clippy::too_many_arguments,
    reason = "every argument is a field of the engine's MessageReceived event"
)]
fn message_received(
    group_id: EngineGroupId,
    message_id: MessageId,
    sender: &MemberId,
    epoch: EpochId,
    payload: &[u8],
    retention: Option<AppMessageRetentionDecision>,
    authority: Option<AppMessageAuthority>,
    encrypted_media_secret: Option<EncryptedMediaSecret>,
    now_secs: u64,
) -> Option<MappedEvent> {
    let inner = MarmotAppEvent::decode(payload).ok()?;
    let group = group_of(&group_id);
    let transport_id = event_id_of(&message_id)?;
    let sender_account = account_of(sender)?;
    let expires_at = retention.and_then(|decision| decision.expires_at);
    let (semantics, modifier) = match semantics::classify(
        inner.kind,
        &inner.tags,
        &inner.content,
        authority.map(|grant| grant.moderation_grant),
        expires_at,
        now_secs,
    ) {
        semantics::Classified::Deliver(semantics) => (semantics, None),
        semantics::Classified::Modifier(modifier) => (Semantics::Other, Some(modifier)),
        semantics::Classified::Drop(reason) => {
            return Some(MappedEvent {
                event: SessionEvent::AppEventDropped { group, reason },
                secret_write: None,
                secret_delete: None,
                drop_pending: Some(message_id),
                modifier: None,
            });
        }
    };
    if inner.kind == MARMOT_APP_EVENT_KIND_AGENT_STREAM_START
        && let Ok(start) = agent_stream::parse_stream_start(&inner.tags)
    {
        return Some(MappedEvent::plain(SessionEvent::AgentStreamStarted {
            group,
            start: AgentStreamAnchor {
                id: inner.id,
                sender: sender_account,
                created_at: inner.created_at,
                epoch: epoch.0,
                transport_id,
                stream_id: start.stream_id,
                parent: start.parent.map(EventId),
                expires_at,
            },
        }));
    }
    let (attachments, stream_final) = if inner.kind == MARMOT_APP_EVENT_KIND_CHAT {
        (
            inner
                .tags
                .iter()
                .filter(|tag| tag.first().map(String::as_str) == Some(media::IMETA_TAG))
                .map(|tag| media::parse_imeta(tag))
                .collect(),
            agent_stream::parse_stream_final(&inner.tags),
        )
    } else {
        (Vec::new(), None)
    };
    // The captured exporter secret is worth persisting only when the
    // message actually commits to at least one readable attachment.
    let secret_write = encrypted_media_secret
        .filter(|_| attachments.iter().any(Result::is_ok))
        .map(|secret| {
            (
                group_id,
                message_id,
                Zeroizing::new(secret.as_bytes().to_vec()),
            )
        });
    Some(MappedEvent {
        event: SessionEvent::MessageReceived {
            group,
            message: ReceivedMessage {
                id: inner.id,
                sender: sender_account,
                created_at: inner.created_at,
                kind: inner.kind,
                tags: inner.tags,
                content: inner.content,
                epoch: epoch.0,
                transport_id,
                attachments,
                stream_final,
                semantics,
                expires_at,
            },
        },
        secret_write,
        secret_delete: None,
        drop_pending: None,
        modifier,
    })
}

fn group_change(change: cgka_traits::engine::GroupStateChange) -> Option<GroupChange> {
    use cgka_traits::engine::GroupStateChange as Change;
    Some(match change {
        Change::MemberAdded { member } => GroupChange::MemberAdded {
            account: account_of(&member)?,
        },
        Change::MemberRemoved { member } => GroupChange::MemberRemoved {
            account: account_of(&member)?,
        },
        Change::MemberLeft { member } => GroupChange::MemberLeft {
            account: account_of(&member)?,
        },
        Change::AdminAdded { member } => GroupChange::AdminAdded {
            account: account_of(&member)?,
        },
        Change::AdminRemoved { member } => GroupChange::AdminRemoved {
            account: account_of(&member)?,
        },
        Change::GroupRenamed {
            name,
            previous_name,
        } => GroupChange::Renamed {
            old_name: previous_name.unwrap_or_default(),
            new_name: name,
        },
        Change::GroupAvatarChanged => GroupChange::AvatarChanged,
        Change::MessageRetentionChanged {
            old_seconds,
            new_seconds,
        } => GroupChange::RetentionChanged {
            old_seconds,
            new_seconds,
        },
        Change::GroupDisbanded => GroupChange::Disbanded,
    })
}

fn session_event_rest(event: GroupEvent) -> Option<SessionEvent> {
    match event {
        GroupEvent::GroupStateInvalidated {
            group_id,
            epoch,
            invalidated_commit_id,
            ..
        } => Some(SessionEvent::GroupStateInvalidated {
            group: group_of(&group_id),
            epoch: epoch.0,
            commit: event_id_of(&invalidated_commit_id)?,
        }),
        GroupEvent::GroupStateRevalidated {
            group_id,
            epoch,
            revalidated_commit_id,
        } => Some(SessionEvent::GroupStateRevalidated {
            group: group_of(&group_id),
            epoch: epoch.0,
            commit: event_id_of(&revalidated_commit_id)?,
        }),
        GroupEvent::LocalGroupCopyTerminated {
            group_id,
            voluntary,
        } => Some(SessionEvent::LocalCopyTerminated {
            group: group_of(&group_id),
            voluntary,
        }),
        GroupEvent::LocalGroupCopyRestored { group_id } => Some(SessionEvent::LocalCopyRestored {
            group: group_of(&group_id),
        }),
        GroupEvent::QueuedIntentRefused {
            group_id,
            intent_id,
            intent_kind,
            reason,
            ..
        } => Some(SessionEvent::QueuedIntentRefused {
            group: group_of(&group_id),
            intent: OutboundId(event_id_of(&intent_id)?.0),
            intent_kind,
            reason,
        }),
        GroupEvent::GroupHydrationQuarantined { group_id, .. } => {
            Some(SessionEvent::HydrationQuarantined {
                group: group_of(&group_id),
            })
        }
        GroupEvent::GroupHydrationRecovered {
            group_id,
            recovered_epoch,
        } => Some(SessionEvent::HydrationRecovered {
            group: group_of(&group_id),
            epoch: recovered_epoch.0,
        }),
        GroupEvent::PendingCommitRecovered {
            group_id,
            recovered_epoch,
        } => Some(SessionEvent::PendingCommitRecovered {
            group: group_of(&group_id),
            epoch: recovered_epoch.0,
        }),
        GroupEvent::GroupUnrecoverable { group_id } => Some(SessionEvent::GroupUnrecoverable {
            group: group_of(&group_id),
        }),
        // Silenced by design: `GroupMemberLeavesRemoved` bookkeeping is
        // folded into the GroupStateChanged stream; `CommitRolledBack`
        // surfaces through publish resolution;
        // `TransportObjectResourceRefused` is a retained-object signal, not
        // application state; `GroupChangeNotDelivered` announces a host's
        // direct change our session never names, so the engine never emits
        // it for us. The first-half variants all exit through
        // `session_event` before this point.
        _ => None,
    }
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "test code"
)]
mod tests {
    //! Exhaustive outcome-mapping coverage: every `IngestOutcome` arm and
    //! every category/reason enum arm, so a silently reclassified input is
    //! impossible to miss.

    use cgka_traits::group_recovery::{
        AuthenticatedGroupRecovery, GroupRecoveryBody, GroupRecoveryMessage,
    };
    use cgka_traits::ingest::{DeferralLineage, InboundResourceLimit};
    use cgka_traits::{EpochId, MemberId};

    use super::*;

    fn gid() -> EngineGroupId {
        EngineGroupId::new(vec![7; 16])
    }

    fn recovery() -> AuthenticatedGroupRecovery {
        AuthenticatedGroupRecovery {
            sender: MemberId::new(vec![1; 32]),
            message: GroupRecoveryMessage {
                group_id: gid(),
                request_id: [2; 32],
                expires_at: 9,
                body: GroupRecoveryBody::Request {
                    device_key_hash: [3; 32],
                },
            },
        }
    }

    #[test]
    fn every_ingest_outcome_arm_maps() {
        let cases: &[(IngestOutcome, InputOutcome)] = &[
            (IngestOutcome::Processed, InputOutcome::Accepted),
            (
                IngestOutcome::GroupRecovery {
                    recovery: Box::new(recovery()),
                },
                InputOutcome::RecoveryOffered,
            ),
            (
                IngestOutcome::Buffered {
                    group_id: gid(),
                    epoch: EpochId(1),
                },
                InputOutcome::Deferred { category: None },
            ),
            (
                IngestOutcome::LocalState {
                    state: LocalIngestState::Removed,
                },
                InputOutcome::Local(LocalState::Removed),
            ),
            (
                IngestOutcome::TransportDeferred {
                    group_id: gid(),
                    lineage: DeferralLineage::Uncontested,
                },
                InputOutcome::Unavailable {
                    category: Availability::TransportDeferred,
                },
            ),
            (
                IngestOutcome::ResourceRefused {
                    group_id: gid(),
                    resource: InboundResourceLimit::TransportDeferredCapacity,
                },
                InputOutcome::Unavailable {
                    category: Availability::ResourceRefused,
                },
            ),
        ];
        for (outcome, expected) in cases {
            assert_eq!(input_outcome(outcome), *expected, "{outcome:?}");
        }
    }

    #[test]
    fn every_input_rejection_category_maps() {
        let cases: &[(InputRejectionCategory, InputCategory)] = &[
            (InputRejectionCategory::Duplicate, InputCategory::Duplicate),
            (InputRejectionCategory::OwnEcho, InputCategory::OwnEcho),
            (
                InputRejectionCategory::WrongRecipient,
                InputCategory::WrongRecipient,
            ),
            (
                InputRejectionCategory::NotAddressedToThisDevice,
                InputCategory::WrongRecipient,
            ),
            (
                InputRejectionCategory::UnknownGroup,
                InputCategory::UnknownGroup,
            ),
            (
                InputRejectionCategory::InvalidEncoding,
                InputCategory::InvalidEncoding,
            ),
            (
                InputRejectionCategory::InvalidSignature,
                InputCategory::InvalidSignature,
            ),
            (
                InputRejectionCategory::UnsupportedRequiredFeature,
                InputCategory::UnsupportedRequiredFeature,
            ),
            (
                InputRejectionCategory::AuthorizationFailed,
                InputCategory::AuthorizationFailed,
            ),
            (
                InputRejectionCategory::MissingWelcomeKeyPackage,
                InputCategory::WrongRecipient,
            ),
        ];
        for (category, expected) in cases {
            let outcome = input_outcome(&IngestOutcome::Ignored {
                category: *category,
            });
            assert_eq!(
                outcome,
                InputOutcome::Rejected {
                    category: *expected
                },
                "{category:?}"
            );
        }
    }

    #[test]
    fn every_proposal_rejection_category_maps() {
        let cases: &[(ProposalRejectionCategory, InputCategory)] = &[
            (
                ProposalRejectionCategory::AuthorizationFailed,
                InputCategory::AuthorizationFailed,
            ),
            (
                ProposalRejectionCategory::UnsupportedProposal,
                InputCategory::UnsupportedRequiredFeature,
            ),
            (
                ProposalRejectionCategory::InvalidEncoding,
                InputCategory::InvalidEncoding,
            ),
            (
                ProposalRejectionCategory::InvalidSignature,
                InputCategory::InvalidSignature,
            ),
            (
                ProposalRejectionCategory::InvalidSelfRemove,
                InputCategory::AuthorizationFailed,
            ),
        ];
        for (category, expected) in cases {
            let outcome = input_outcome(&IngestOutcome::Rejected {
                category: *category,
            });
            assert_eq!(
                outcome,
                InputOutcome::Rejected {
                    category: *expected
                },
                "{category:?}"
            );
        }
    }

    #[test]
    fn every_local_ingest_state_maps() {
        let cases: &[(LocalIngestState, LocalState)] = &[
            (
                LocalIngestState::RejoinConfirmationRequired,
                LocalState::RejoinConfirmationRequired,
            ),
            (LocalIngestState::Removed, LocalState::Removed),
            (LocalIngestState::Quarantined, LocalState::Quarantined),
        ];
        for (state, expected) in cases {
            let outcome = input_outcome(&IngestOutcome::LocalState { state: *state });
            assert_eq!(outcome, InputOutcome::Local(*expected), "{state:?}");
        }
    }

    /// `StaleReason` arms map to their documented replacements, not to
    /// `Stale`: deprecated dedup arms become `Rejected` with the matching
    /// routing category, `SelfEvicted`/`Quarantined` become `Local`, and
    /// only the genuine epoch-history causes stay `Stale { StaleEpoch }`.
    #[allow(
        deprecated,
        reason = "the pinned enum still carries the deprecated variants"
    )]
    #[test]
    fn every_stale_reason_maps() {
        let stale_epoch = InputOutcome::Stale {
            category: InputCategory::StaleEpoch,
        };
        let cases: &[(StaleReason, InputOutcome)] = &[
            (
                StaleReason::AlreadySeen,
                InputOutcome::Rejected {
                    category: InputCategory::Duplicate,
                },
            ),
            (
                StaleReason::NotForThisClient,
                InputOutcome::Rejected {
                    category: InputCategory::WrongRecipient,
                },
            ),
            (
                StaleReason::UnknownGroup,
                InputOutcome::Rejected {
                    category: InputCategory::UnknownGroup,
                },
            ),
            (
                StaleReason::OwnEcho,
                InputOutcome::Rejected {
                    category: InputCategory::OwnEcho,
                },
            ),
            (
                StaleReason::SelfEvicted,
                InputOutcome::Local(LocalState::Removed),
            ),
            (
                StaleReason::Quarantined,
                InputOutcome::Local(LocalState::Quarantined),
            ),
            (
                StaleReason::AlreadyAtEpoch {
                    current: EpochId(4),
                    msg_epoch: EpochId(3),
                },
                stale_epoch,
            ),
            (StaleReason::PreMembership, stale_epoch),
            (StaleReason::PredatesLocalCopy, stale_epoch),
            (StaleReason::BeyondAnchor, stale_epoch),
            (StaleReason::BeyondRollbackHorizon, stale_epoch),
            (StaleReason::BeyondAppRetention, stale_epoch),
            (StaleReason::LosingBranch, stale_epoch),
            (StaleReason::InvalidAgainstCanonicalState, stale_epoch),
        ];
        for (reason, expected) in cases {
            let outcome = input_outcome(&IngestOutcome::Stale {
                reason: reason.clone(),
            });
            assert_eq!(outcome, *expected, "{reason:?}");
        }
    }

    /// A kind-9/1200 `GroupEvent::MessageReceived` with the given inner tags.
    fn message_event(kind: u64, tags: Vec<Vec<String>>) -> GroupEvent {
        GroupEvent::MessageReceived {
            group_id: gid(),
            message_id: MessageId::new(vec![9; 32]),
            sender: MemberId::new(vec![1; 32]),
            sender_leaf_index: Some(0),
            epoch: EpochId(3),
            payload: MarmotAppEvent::new(
                "01".repeat(32),
                1_700_000_000,
                kind,
                tags,
                "body".to_owned(),
            )
            .encode()
            .expect("app event encode"),
            retention: None,
            authority: None,
            encrypted_media_secret: None,
        }
    }

    fn hex32(byte: u8) -> String {
        format!("{byte:02x}").repeat(32)
    }

    fn stream_tags() -> Vec<Vec<String>> {
        vec![
            vec!["stream".to_owned(), hex32(7)],
            vec!["stream-type".to_owned(), "text".to_owned()],
            vec!["final-kind".to_owned(), "9".to_owned()],
            vec!["parent".to_owned(), hex32(8)],
            vec!["route".to_owned(), "quic".to_owned()],
            vec!["broker".to_owned(), "quic://a".to_owned()],
            vec!["broker".to_owned(), "quic://b".to_owned()],
        ]
    }

    /// A valid kind-1200 stream start projects to `AgentStreamStarted` with
    /// the anchor fields and no stream candidate on the event.
    #[test]
    fn stream_start_maps_to_anchor_event() {
        let mapped = session_event(message_event(1200, stream_tags()), 0).expect("mapped");
        let SessionEvent::AgentStreamStarted { group, start } = mapped.event else {
            panic!("expected AgentStreamStarted");
        };
        assert_eq!(group.0, vec![7; 16]);
        assert_eq!(start.stream_id, [7; 32]);
        assert_eq!(start.parent, Some(EventId([8; 32])));
        assert_eq!(start.sender, AccountId([1; 32]));
        assert_eq!(start.created_at, 1_700_000_000);
        assert_eq!(start.epoch, 3);
        assert_eq!(start.transport_id, EventId([9; 32]));
        assert!(mapped.secret_write.is_none());
        assert!(mapped.secret_delete.is_none());
    }

    /// `parent` is case-insensitive (the spec does not constrain it);
    /// `stream` stays lowercase-only.
    #[test]
    fn stream_start_parent_hex_is_case_insensitive() {
        let mut parent_upper = stream_tags();
        *parent_upper
            .get_mut(3)
            .and_then(|tag| tag.get_mut(1))
            .expect("parent tag") = hex32(0xab).to_uppercase();
        let mapped = session_event(message_event(1200, parent_upper), 0).expect("mapped");
        let SessionEvent::AgentStreamStarted { start, .. } = mapped.event else {
            panic!("expected AgentStreamStarted");
        };
        assert_eq!(start.parent, Some(EventId([0xab; 32])));

        let mut stream_upper = stream_tags();
        *stream_upper
            .first_mut()
            .and_then(|tag| tag.get_mut(1))
            .expect("stream tag") = hex32(0xcd).to_uppercase();
        let mapped_upper = session_event(message_event(1200, stream_upper), 0).expect("mapped");
        assert!(
            matches!(mapped_upper.event, SessionEvent::MessageReceived { .. }),
            "uppercase stream id must stay a message"
        );
    }

    /// Malformed kind-1200 payloads stay ordinary `MessageReceived`.
    #[test]
    fn malformed_stream_start_stays_a_message() {
        for tags in [
            // bad stream hex (uppercase)
            vec![
                vec!["stream".to_owned(), "A".repeat(64)],
                vec!["stream-type".to_owned(), "text".to_owned()],
                vec!["final-kind".to_owned(), "9".to_owned()],
            ],
            // wrong stream-type
            vec![
                vec!["stream".to_owned(), hex32(7)],
                vec!["stream-type".to_owned(), "audio".to_owned()],
                vec!["final-kind".to_owned(), "9".to_owned()],
            ],
            // wrong final-kind
            vec![
                vec!["stream".to_owned(), hex32(7)],
                vec!["stream-type".to_owned(), "text".to_owned()],
                vec!["final-kind".to_owned(), "1".to_owned()],
            ],
            // duplicate stream tag
            vec![
                vec!["stream".to_owned(), hex32(7)],
                vec!["stream".to_owned(), hex32(7)],
                vec!["stream-type".to_owned(), "text".to_owned()],
                vec!["final-kind".to_owned(), "9".to_owned()],
            ],
        ] {
            let mapped = session_event(message_event(1200, tags), 0).expect("mapped");
            assert!(
                matches!(mapped.event, SessionEvent::MessageReceived { .. }),
                "malformed start must stay a message"
            );
        }
    }

    /// Kind-9 stream-final tags project to `stream_final`; one malformed tag
    /// degrades to `None` without dropping the message.
    #[test]
    fn stream_final_projection() {
        let good = message_event(
            9,
            vec![
                vec!["stream".to_owned(), hex32(7)],
                vec!["stream-start".to_owned(), hex32(8)],
                vec!["stream-hash".to_owned(), hex32(9)],
                vec!["stream-chunks".to_owned(), "42".to_owned()],
            ],
        );
        let SessionEvent::MessageReceived { message, .. } =
            session_event(good, 0).expect("mapped").event
        else {
            panic!("expected MessageReceived");
        };
        let final_ = message.stream_final.expect("stream_final");
        assert_eq!(final_.stream_id, [7; 32]);
        assert_eq!(final_.start_event_id, [8; 32]);
        assert_eq!(final_.transcript_hash, [9; 32]);
        assert_eq!(final_.chunk_count, 42);

        let bad = message_event(
            9,
            vec![
                vec!["stream".to_owned(), hex32(7)],
                vec!["stream-start".to_owned(), "not hex".to_owned()],
                vec!["stream-hash".to_owned(), hex32(9)],
                vec!["stream-chunks".to_owned(), "42".to_owned()],
            ],
        );
        let SessionEvent::MessageReceived {
            message: bad_message,
            ..
        } = session_event(bad, 0).expect("mapped").event
        else {
            panic!("expected MessageReceived");
        };
        assert_eq!(bad_message.content, "body");
        assert!(bad_message.stream_final.is_none());
    }

    /// A captured media secret is persisted only when the message commits to
    /// at least one parseable attachment.
    #[test]
    fn media_secret_write_gated_on_valid_attachment() {
        use cgka_traits::EncryptedMediaSecret;
        let with_secret = |tags: Vec<Vec<String>>| {
            let GroupEvent::MessageReceived {
                group_id,
                message_id,
                sender,
                sender_leaf_index,
                epoch,
                payload,
                retention,
                authority,
                ..
            } = message_event(9, tags)
            else {
                unreachable!()
            };
            GroupEvent::MessageReceived {
                group_id,
                message_id,
                sender,
                sender_leaf_index,
                epoch,
                payload,
                retention,
                authority,
                encrypted_media_secret: Some(EncryptedMediaSecret::new(Zeroizing::new(vec![
                    3;
                    32
                ]))),
            }
        };
        let valid = vec![vec![
            "imeta".to_owned(),
            "v encrypted-media-v2".to_owned(),
            format!("locator blossom-v1 https://blossom.example/{}", hex32(1)),
            format!("ciphertext_sha256 {}", hex32(1)),
            format!("plaintext_sha256 {}", hex32(2)),
            "m image/png".to_owned(),
            "filename pic.png".to_owned(),
            format!("nonce {}", "a".repeat(24)),
        ]];
        let mapped = session_event(with_secret(valid), 0).expect("mapped");
        assert!(mapped.secret_write.is_some());
        let malformed = vec![vec!["imeta".to_owned(), "v encrypted-media-v9".to_owned()]];
        let malformed_mapped = session_event(with_secret(malformed), 0).expect("mapped");
        assert!(malformed_mapped.secret_write.is_none());
    }

    /// One kind-9 carrying a valid and an invalid `imeta` still projects to
    /// `MessageReceived` with the body intact: attachments keep tag order —
    /// `Ok` for the parseable reference, `Err` with the exact rejection for
    /// the rest — and the captured secret is persisted for the valid one.
    #[test]
    fn mixed_imeta_tags_keep_message_and_order() {
        let tags = vec![
            vec![
                "imeta".to_owned(),
                "v encrypted-media-v2".to_owned(),
                format!("locator blossom-v1 https://blossom.example/{}", hex32(1)),
                format!("ciphertext_sha256 {}", hex32(1)),
                format!("plaintext_sha256 {}", hex32(2)),
                "m image/png".to_owned(),
                "filename pic.png".to_owned(),
                format!("nonce {}", "a".repeat(24)),
            ],
            vec!["imeta".to_owned(), "v encrypted-media-v9".to_owned()],
        ];
        let GroupEvent::MessageReceived {
            group_id,
            message_id,
            sender,
            sender_leaf_index,
            epoch,
            payload,
            retention,
            authority,
            ..
        } = message_event(9, tags)
        else {
            unreachable!()
        };
        let event = GroupEvent::MessageReceived {
            group_id,
            message_id,
            sender,
            sender_leaf_index,
            epoch,
            payload,
            retention,
            authority,
            encrypted_media_secret: Some(EncryptedMediaSecret::new(Zeroizing::new(vec![3; 32]))),
        };
        let mapped = session_event(event, 0).expect("mapped");
        let SessionEvent::MessageReceived { message, .. } = mapped.event else {
            panic!("expected MessageReceived");
        };
        assert_eq!(message.content, "body");
        assert_eq!(message.attachments.len(), 2);
        let reference = message
            .attachments
            .first()
            .expect("two attachments")
            .as_ref()
            .expect("first parses");
        assert_eq!(reference.ciphertext_sha256, [1; 32]);
        assert_eq!(
            message.attachments.get(1),
            Some(&Err(AttachmentRejection::UnsupportedFormat))
        );
        assert!(mapped.secret_write.is_some());
    }

    /// An invalidated app message withdraws the delivered row and asks the
    /// session to drop the media secret recorded under its transport id.
    #[test]
    fn invalidated_message_deletes_media_secret() {
        use cgka_traits::engine::AppMessageInvalidationReason;
        let group_id = gid();
        let message_id = MessageId::new(vec![9; 32]);
        let event = GroupEvent::AppMessageInvalidated {
            group_id: group_id.clone(),
            message_id: message_id.clone(),
            epoch: EpochId(3),
            reason: AppMessageInvalidationReason::LosingBranch,
            decrypted_payload_ref: None,
        };
        let mapped = session_event(event, 0).expect("mapped");
        assert!(matches!(
            mapped.event,
            SessionEvent::MessageInvalidated { .. }
        ));
        assert_eq!(mapped.secret_write, None);
        assert_eq!(mapped.secret_delete, Some((group_id, message_id)));
    }

    /// `group_system_canonical_id` errors only for a `GroupDisbanded`
    /// without an actor — the engine never emits it; if it ever did the
    /// event is dropped rather than surfaced with a fabricated row id.
    #[test]
    fn disbanded_without_actor_is_dropped() {
        let event = GroupEvent::GroupStateChanged {
            group_id: gid(),
            epoch: EpochId(3),
            actor: None,
            change: cgka_traits::engine::GroupStateChange::GroupDisbanded,
            origin_commit_id: None,
        };
        assert!(session_event(event, 0).is_none());
    }

    /// A well-formed change carries the canonical system-row id: equal to
    /// `group_system_canonical_id` of the exact group, epoch, actor and
    /// engine change (including fields the mapped `GroupChange` drops).
    #[test]
    fn state_change_row_id_is_canonical() {
        let actor = MemberId::new(vec![5; 32]);
        let engine_change = cgka_traits::engine::GroupStateChange::GroupRenamed {
            previous_name: Some("before".to_owned()),
            name: "after".to_owned(),
        };
        let event = GroupEvent::GroupStateChanged {
            group_id: gid(),
            epoch: EpochId(7),
            actor: Some(actor.clone()),
            change: engine_change.clone(),
            origin_commit_id: None,
        };
        let expected = cgka_traits::app_event::group_system_canonical_id(
            &gid(),
            7,
            Some(&actor),
            &engine_change,
        )
        .expect("canonical id");
        let mapped = session_event(event, 0).expect("mapped");
        let SessionEvent::GroupStateChanged { row_id, .. } = mapped.event else {
            panic!("expected GroupStateChanged");
        };
        assert_eq!(row_id, expected);
    }
}
