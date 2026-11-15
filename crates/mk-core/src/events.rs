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

use cgka_traits::engine::GroupEvent;
use cgka_traits::ingest::{
    IngestOutcome, InputRejectionCategory, LocalIngestState, ProposalRejectionCategory, StaleReason,
};
use cgka_traits::{GroupId as EngineGroupId, MemberId, MessageId};

use crate::outbox::PublishResolution;
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
    /// decode/decrypt or a local resource bound); not a protocol verdict.
    Unavailable {
        /// Why it is unavailable.
        category: InputCategory,
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
            Self::Stale { category }
            | Self::Rejected { category }
            | Self::Unavailable { category } => Some(category),
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
            category: InputCategory::TransportDeferred,
        },
        IngestOutcome::ResourceRefused { .. } => InputOutcome::Unavailable {
            category: InputCategory::ResourceRefused,
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
    /// An outbound obligation reached a resolution checkpoint.
    PublishResolved {
        /// The outbound item.
        outbound: OutboundId,
        /// The verdict.
        resolution: PublishResolution,
    },
}

/// Map one engine event to a session event. Returns `None` for variants that
/// belong to device-internal bookkeeping or to features this crate does not
/// implement — matched explicitly, never by wildcard:
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
///   only and carries no additional meaning for the host).
pub(crate) fn session_event(event: GroupEvent) -> Option<SessionEvent> {
    match event {
        GroupEvent::MessageReceived {
            group_id,
            message_id,
            sender,
            epoch,
            payload,
            ..
        } => {
            let inner = cgka_traits::app_event::MarmotAppEvent::decode(&payload).ok()?;
            Some(SessionEvent::MessageReceived {
                group: group_of(&group_id),
                message: ReceivedMessage {
                    id: inner.id,
                    sender: account_of(&sender)?,
                    created_at: inner.created_at,
                    kind: inner.kind,
                    tags: inner.tags,
                    content: inner.content,
                    epoch: epoch.0,
                    transport_id: event_id_of(&message_id)?,
                },
            })
        }
        GroupEvent::AppMessageInvalidated {
            group_id,
            message_id,
            epoch,
            ..
        } => Some(SessionEvent::MessageInvalidated {
            group: group_of(&group_id),
            message: event_id_of(&message_id)?,
            epoch: epoch.0,
        }),
        GroupEvent::GroupCreated { group_id } => Some(SessionEvent::GroupCreated {
            group: group_of(&group_id),
        }),
        GroupEvent::GroupJoined { group_id, .. } => Some(SessionEvent::GroupJoined {
            group: group_of(&group_id),
        }),
        GroupEvent::EpochChanged { group_id, from, to } => Some(SessionEvent::EpochChanged {
            group: group_of(&group_id),
            from: from.0,
            to: to.0,
        }),
        GroupEvent::GroupStateChanged {
            group_id,
            epoch,
            actor,
            change,
            origin_commit_id,
        } => {
            let change = group_change(change)?;
            Some(SessionEvent::GroupStateChanged {
                group: group_of(&group_id),
                epoch: epoch.0,
                actor: actor.as_ref().and_then(account_of),
                change,
                origin_commit: origin_commit_id.as_ref().and_then(event_id_of),
            })
        }
        other => session_event_rest(other),
    }
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
        // application state. The first-half variants all exit through
        // `session_event` before this point.
        _ => None,
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, reason = "test code")]
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
                    category: InputCategory::TransportDeferred,
                },
            ),
            (
                IngestOutcome::ResourceRefused {
                    group_id: gid(),
                    resource: InboundResourceLimit::TransportDeferredCapacity,
                },
                InputOutcome::Unavailable {
                    category: InputCategory::ResourceRefused,
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
}
