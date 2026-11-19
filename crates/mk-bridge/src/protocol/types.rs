//! Plain data wire types mirroring `mk-core` public values field-for-field,
//! plus the `From` conversions between them.
//!
//! `mk-core` enums are `#[non_exhaustive]`: a future variant has no honest
//! wire encoding, so output-side wire enums carry a final `unmapped`
//! variant that wildcard arms produce. Input-side enums mirror only what
//! `mk-core` accepts today — anything else fails strict decode.

use mk_core as core;
use serde::{Deserialize, Serialize};

use super::ser::{hex_bytes, hex_str, opt_hex32, u64_str};

/// One reading of the injected clock.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Time {
    /// Milliseconds since the Unix epoch (decimal string).
    #[serde(with = "u64_str")]
    pub unix_ms: u64,
    /// Milliseconds on a monotonic scale (decimal string).
    #[serde(with = "u64_str")]
    pub monotonic_ms: u64,
}

impl From<core::Time> for Time {
    fn from(time: core::Time) -> Self {
        Self {
            unix_ms: time.unix_ms,
            monotonic_ms: time.monotonic_ms,
        }
    }
}

impl From<Time> for core::Time {
    fn from(time: Time) -> Self {
        Self {
            unix_ms: time.unix_ms,
            monotonic_ms: time.monotonic_ms,
        }
    }
}

/// A signed NIP-01 event.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Event {
    /// Event id (SHA-256 of the NIP-01 serialization), 32-byte hex.
    #[serde(with = "hex_str")]
    pub id: [u8; 32],
    /// Signer's account key, 32-byte hex.
    #[serde(with = "hex_str")]
    pub pubkey: [u8; 32],
    /// Unix seconds (decimal string).
    #[serde(with = "u64_str")]
    pub created_at: u64,
    /// Nostr kind.
    pub kind: u16,
    /// Tag list: each entry is an array of strings.
    pub tags: Vec<Vec<String>>,
    /// Content string.
    pub content: String,
    /// BIP-340 schnorr signature over `id`, 64-byte hex.
    #[serde(with = "hex_str")]
    pub sig: [u8; 64],
}

impl From<Event> for core::Event {
    fn from(event: Event) -> Self {
        Self {
            id: core::EventId(event.id),
            pubkey: core::AccountId(event.pubkey),
            created_at: event.created_at,
            kind: event.kind,
            tags: event.tags,
            content: event.content,
            sig: event.sig,
        }
    }
}

impl From<core::Event> for Event {
    fn from(event: core::Event) -> Self {
        Self {
            id: event.id.0,
            pubkey: event.pubkey.0,
            created_at: event.created_at,
            kind: event.kind,
            tags: event.tags,
            content: event.content,
            sig: event.sig,
        }
    }
}

/// An unsigned NIP-01 event: everything except `id` and `sig`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UnsignedEvent {
    /// Signer's account key, 32-byte hex.
    #[serde(with = "hex_str")]
    pub pubkey: [u8; 32],
    /// Unix seconds (decimal string).
    #[serde(with = "u64_str")]
    pub created_at: u64,
    /// Nostr kind.
    pub kind: u16,
    /// Tag list.
    pub tags: Vec<Vec<String>>,
    /// Content string.
    pub content: String,
}

impl From<UnsignedEvent> for core::UnsignedEvent {
    fn from(event: UnsignedEvent) -> Self {
        Self {
            pubkey: core::AccountId(event.pubkey),
            created_at: event.created_at,
            kind: event.kind,
            tags: event.tags,
            content: event.content,
        }
    }
}

impl From<core::UnsignedEvent> for UnsignedEvent {
    fn from(event: core::UnsignedEvent) -> Self {
        Self {
            pubkey: event.pubkey.0,
            created_at: event.created_at,
            kind: event.kind,
            tags: event.tags,
            content: event.content,
        }
    }
}

/// One way to fetch an attachment: a `locator <kind> <value>` field.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Locator {
    /// The locator scheme.
    pub kind: String,
    /// The locator value (a URL).
    pub value: String,
}

impl From<Locator> for core::media::Locator {
    fn from(locator: Locator) -> Self {
        Self {
            kind: locator.kind,
            value: locator.value,
        }
    }
}

impl From<core::media::Locator> for Locator {
    fn from(locator: core::media::Locator) -> Self {
        Self {
            kind: locator.kind,
            value: locator.value,
        }
    }
}

/// A validated `encrypted-media-v2` attachment reference.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MediaReference {
    /// Locators in tag order.
    pub locators: Vec<Locator>,
    /// SHA-256 of the ciphertext blob, hex.
    #[serde(with = "hex_str")]
    pub ciphertext_sha256: [u8; 32],
    /// SHA-256 of the plaintext, hex.
    #[serde(with = "hex_str")]
    pub plaintext_sha256: [u8; 32],
    /// The 12-byte AEAD nonce, hex.
    #[serde(with = "hex_str")]
    pub nonce: [u8; 12],
    /// The canonical media type.
    pub media_type: String,
    /// The filename.
    pub filename: String,
    /// Optional `WxH` dimensions.
    pub dim: Option<String>,
    /// Optional thumbhash.
    pub thumbhash: Option<String>,
}

impl From<MediaReference> for core::media::MediaReference {
    fn from(reference: MediaReference) -> Self {
        Self {
            locators: reference
                .locators
                .into_iter()
                .map(core::media::Locator::from)
                .collect(),
            ciphertext_sha256: reference.ciphertext_sha256,
            plaintext_sha256: reference.plaintext_sha256,
            nonce: reference.nonce,
            media_type: reference.media_type,
            filename: reference.filename,
            dim: reference.dim,
            thumbhash: reference.thumbhash,
        }
    }
}

impl From<core::media::MediaReference> for MediaReference {
    fn from(reference: core::media::MediaReference) -> Self {
        Self {
            locators: reference.locators.into_iter().map(Locator::from).collect(),
            ciphertext_sha256: reference.ciphertext_sha256,
            plaintext_sha256: reference.plaintext_sha256,
            nonce: reference.nonce,
            media_type: reference.media_type,
            filename: reference.filename,
            dim: reference.dim,
            thumbhash: reference.thumbhash,
        }
    }
}

/// The stable category of one rejected `imeta` attachment.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AttachmentRejection {
    /// Not a decodable `imeta` tag.
    InvalidStructure,
    /// The `v` field names an unimplemented format.
    UnsupportedFormat,
    /// A required field is absent or empty.
    MissingField,
    /// A single-occurrence field appears more than once.
    DuplicateField,
    /// A present field has an invalid value.
    MalformedField,
    /// An `mk-core` rejection kind this wire version does not know.
    Unmapped,
}

impl From<core::media::AttachmentRejection> for AttachmentRejection {
    fn from(rejection: core::media::AttachmentRejection) -> Self {
        match rejection {
            core::media::AttachmentRejection::InvalidStructure => Self::InvalidStructure,
            core::media::AttachmentRejection::UnsupportedFormat => Self::UnsupportedFormat,
            core::media::AttachmentRejection::MissingField => Self::MissingField,
            core::media::AttachmentRejection::DuplicateField => Self::DuplicateField,
            core::media::AttachmentRejection::MalformedField => Self::MalformedField,
            // `AttachmentRejection` is `#[non_exhaustive]`.
            _ => Self::Unmapped,
        }
    }
}

/// One `imeta` attachment entry on a received message.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum AttachmentResult {
    /// A valid reference.
    Ok {
        /// The attachment reference.
        reference: MediaReference,
    },
    /// The `imeta` tag was rejected.
    Rejected {
        /// The stable rejection category.
        reason: AttachmentRejection,
    },
}

/// One default blob-store endpoint of the group's media policy.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BlobEndpoint {
    /// The locator kind this endpoint serves.
    pub locator_kind: String,
    /// The server's base URL.
    pub base_url: String,
}

impl From<core::media::BlobEndpoint> for BlobEndpoint {
    fn from(endpoint: core::media::BlobEndpoint) -> Self {
        Self {
            locator_kind: endpoint.locator_kind,
            base_url: endpoint.base_url,
        }
    }
}

/// The group's `encrypted-media-v2` policy (`0x800b` component), decoded.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EncryptedMediaPolicy {
    /// Locator kinds a sender may emit and receivers may fetch.
    pub allowed_locator_kinds: Vec<String>,
    /// Default blob endpoints, in policy order.
    pub endpoints: Vec<BlobEndpoint>,
}

impl From<core::media::EncryptedMediaPolicy> for EncryptedMediaPolicy {
    fn from(policy: core::media::EncryptedMediaPolicy) -> Self {
        Self {
            allowed_locator_kinds: policy.allowed_locator_kinds,
            endpoints: policy
                .endpoints
                .into_iter()
                .map(BlobEndpoint::from)
                .collect(),
        }
    }
}

/// The group's `0x8006` agent text stream policy. `StreamRoles` carries a
/// private mask in `mk-core`; the wire exposes the raw role bitmasks.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentStreamPolicy {
    /// Roles every member must advertise (bitmask).
    pub required_roles: u8,
    /// Roles members may hold (bitmask).
    pub allowed_roles: u8,
    /// Maximum plaintext bytes per stream frame.
    pub max_plaintext_frame_len: u32,
    /// Replay-protection time-to-live in seconds.
    pub replay_ttl_secs: u32,
}

impl From<core::agent_stream::AgentStreamPolicy> for AgentStreamPolicy {
    fn from(policy: core::agent_stream::AgentStreamPolicy) -> Self {
        // `StreamRoles`' mask is private in `mk-core`; the canonical `0x8006`
        // component encoding (`required u8, allowed u8` in the first two
        // bytes) exposes it without guessing bits.
        let (required_roles, allowed_roles) = match policy.to_component() {
            Ok(update) => match update.data.as_slice() {
                [required, allowed, ..] => (*required, *allowed),
                _ => (
                    roles_mask(policy.required_roles),
                    roles_mask(policy.allowed_roles),
                ),
            },
            Err(_) => (
                roles_mask(policy.required_roles),
                roles_mask(policy.allowed_roles),
            ),
        };
        Self {
            required_roles,
            allowed_roles,
            max_plaintext_frame_len: policy.max_plaintext_frame_len,
            replay_ttl_secs: policy.replay_ttl_secs,
        }
    }
}

/// Rebuild the mask from the three public role constants when the canonical
/// component encoding is unavailable.
fn roles_mask(roles: core::agent_stream::StreamRoles) -> u8 {
    u8::from(roles.contains(core::agent_stream::StreamRoles::RECEIVE))
        | u8::from(roles.contains(core::agent_stream::StreamRoles::SEND)) << 1
        | u8::from(roles.contains(core::agent_stream::StreamRoles::FANOUT)) << 2
}

/// One attachment to seal: plaintext plus `imeta` metadata.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AttachmentInput {
    /// The file bytes to encrypt, hex.
    #[serde(with = "hex_bytes")]
    pub plaintext: Vec<u8>,
    /// The media type; canonicalized before sealing.
    pub media_type: String,
    /// The filename (1..=255 UTF-8 bytes, no NUL).
    pub filename: String,
    /// Optional `WxH` dimensions, verbatim.
    pub dim: Option<String>,
    /// Optional thumbhash, verbatim.
    pub thumbhash: Option<String>,
}

impl From<AttachmentInput> for core::AttachmentInput {
    fn from(input: AttachmentInput) -> Self {
        Self {
            plaintext: input.plaintext,
            media_type: input.media_type,
            filename: input.filename,
            dim: input.dim,
            thumbhash: input.thumbhash,
        }
    }
}

/// One invitee of a group creation or invite.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Invitee {
    /// The invitee's kind-30443 `KeyPackage` event.
    pub key_package: Event,
    /// The invitee's inbox relays (its kind-10050 set).
    pub inbox_relays: Vec<String>,
}

impl From<Invitee> for core::Invitee {
    fn from(invitee: Invitee) -> Self {
        Self {
            key_package: invitee.key_package.into(),
            inbox_relays: invitee.inbox_relays,
        }
    }
}

/// An app-component update.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ComponentUpdate {
    /// The app-component id.
    pub component_id: u16,
    /// The replacement component bytes, hex.
    #[serde(with = "hex_bytes")]
    pub data: Vec<u8>,
}

impl From<ComponentUpdate> for core::ComponentUpdate {
    fn from(update: ComponentUpdate) -> Self {
        Self {
            component_id: update.component_id,
            data: update.data,
        }
    }
}

/// An outbound application message.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AppMessage {
    /// The inner Marmot app-event kind.
    pub kind: u16,
    /// Inner tags.
    pub tags: Vec<Vec<String>>,
    /// Inner content.
    pub content: String,
}

impl From<AppMessage> for core::AppMessage {
    fn from(message: AppMessage) -> Self {
        Self {
            kind: message.kind,
            tags: message.tags,
            content: message.content,
        }
    }
}

/// A sealed attachment referenced by `send_media`: its bridge handle plus
/// the locators the host's uploads produced.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MediaAttachmentRef {
    /// The handle `seal_attachment` returned.
    pub handle: u32,
    /// Where the ciphertext blob can be fetched.
    pub locators: Vec<Locator>,
}

/// What a send produced.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum SendOutcome {
    /// Artifacts were staged to the outbox and wait on the durability gate.
    Staged {
        /// The staged outbound ids, 32-byte hex each.
        ids: Vec<String>,
    },
    /// The intent was accepted but retained behind convergence or an
    /// earlier send; it regenerates on a later `tick`.
    Queued,
    /// The change was already canonical; nothing to publish.
    NoChange,
    /// The leave request is durable.
    LeaveRequested,
    /// An `mk-core` send outcome this wire version does not know.
    Unmapped,
}

impl From<core::SendOutcome> for SendOutcome {
    fn from(outcome: core::SendOutcome) -> Self {
        match outcome {
            core::SendOutcome::Staged(ids) => Self::Staged {
                ids: ids.iter().map(|id| hex::encode(id.0)).collect(),
            },
            core::SendOutcome::Queued => Self::Queued,
            core::SendOutcome::NoChange => Self::NoChange,
            core::SendOutcome::LeaveRequested => Self::LeaveRequested,
            // `SendOutcome` is `#[non_exhaustive]`.
            _ => Self::Unmapped,
        }
    }
}

/// One relay's publish verdict from the host transport.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RelayResult {
    /// The relay URL exactly as the outbound item listed it.
    pub relay: String,
    /// What the relay did with the event.
    pub status: RelayStatus,
}

/// Per-relay publish status the host reports.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RelayStatus {
    /// The relay accepted the event.
    Accepted,
    /// The relay definitively refused the event.
    Rejected,
    /// The relay could not be reached or did not answer.
    Unavailable,
    /// The event was not transmitted to this relay; no exposure recorded.
    NotSent,
    /// The outcome is ambiguous; the event may have been exposed.
    Unknown,
}

impl From<RelayStatus> for core::RelayStatus {
    fn from(status: RelayStatus) -> Self {
        match status {
            RelayStatus::Accepted => Self::Accepted,
            RelayStatus::Rejected => Self::Rejected,
            RelayStatus::Unavailable => Self::Unavailable,
            RelayStatus::NotSent => Self::NotSent,
            RelayStatus::Unknown => Self::Unknown,
        }
    }
}

/// Terminal or in-progress verdict of a `report_publish` call.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PublishResolution {
    /// Targets remain outstanding.
    Pending,
    /// A pending MLS commit was confirmed on first acceptance.
    Confirmed,
    /// A pending MLS commit was rolled back after every target failed with
    /// no possible exposure.
    RolledBack,
    /// All targets are terminal with no pending MLS work.
    Complete,
    /// An `mk-core` resolution this wire version does not know.
    Unmapped,
}

impl From<core::PublishResolution> for PublishResolution {
    fn from(resolution: core::PublishResolution) -> Self {
        match resolution {
            core::PublishResolution::Pending => Self::Pending,
            core::PublishResolution::Confirmed => Self::Confirmed,
            core::PublishResolution::RolledBack => Self::RolledBack,
            core::PublishResolution::Complete => Self::Complete,
            // `PublishResolution` is `#[non_exhaustive]`.
            _ => Self::Unmapped,
        }
    }
}

/// One outbound obligation handed to the host transport.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Outbound {
    /// Identity of the obligation (the frozen event's id), hex.
    #[serde(with = "hex_str")]
    pub id: [u8; 32],
    /// The exact signed event to publish.
    pub event: Event,
    /// Relay URLs to publish to, in target order.
    pub relays: Vec<String>,
    /// Why this artifact exists.
    pub purpose: OutboundPurpose,
}

impl From<core::Outbound> for Outbound {
    fn from(outbound: core::Outbound) -> Self {
        Self {
            id: outbound.id.0,
            event: outbound.event.into(),
            relays: outbound.relays,
            purpose: outbound.purpose.into(),
        }
    }
}

/// What kind of artifact an [`Outbound`] carries.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OutboundPurpose {
    /// An encrypted group application message (kind 445).
    ApplicationMessage,
    /// An MLS proposal broadcast (kind 445).
    Proposal,
    /// An MLS commit / group evolution (kind 445).
    Commit,
    /// A NIP-59 gift-wrapped Welcome (kind 1059).
    Welcome,
    /// The account's `KeyPackage` (kind 30443).
    KeyPackage,
    /// The account's inbox relay list (kind 10050).
    InboxRelays,
    /// An `mk-core` purpose this wire version does not know.
    Unmapped,
}

impl From<core::OutboundPurpose> for OutboundPurpose {
    fn from(purpose: core::OutboundPurpose) -> Self {
        match purpose {
            core::OutboundPurpose::ApplicationMessage => Self::ApplicationMessage,
            core::OutboundPurpose::Proposal => Self::Proposal,
            core::OutboundPurpose::Commit => Self::Commit,
            core::OutboundPurpose::Welcome => Self::Welcome,
            core::OutboundPurpose::KeyPackage => Self::KeyPackage,
            core::OutboundPurpose::InboxRelays => Self::InboxRelays,
            // `OutboundPurpose` is `#[non_exhaustive]`.
            _ => Self::Unmapped,
        }
    }
}

/// A group's lifecycle bucket for UI projection.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GroupState {
    /// Canonical and ingestible.
    Stable,
    /// An own commit awaits its publish resolution.
    PendingPublish,
    /// Convergence is selecting between forks.
    Recovering,
    /// Frozen pending a verified repair path.
    Unrecoverable,
    /// This device's copy ended.
    Removed,
    /// Terminally disbanded.
    Disbanded,
    /// An `mk-core` state this wire version does not know.
    Unmapped,
}

impl From<core::GroupState> for GroupState {
    fn from(state: core::GroupState) -> Self {
        match state {
            core::GroupState::Stable => Self::Stable,
            core::GroupState::PendingPublish => Self::PendingPublish,
            core::GroupState::Recovering => Self::Recovering,
            core::GroupState::Unrecoverable => Self::Unrecoverable,
            core::GroupState::Removed => Self::Removed,
            core::GroupState::Disbanded => Self::Disbanded,
            // `GroupState` is `#[non_exhaustive]`.
            _ => Self::Unmapped,
        }
    }
}

/// A convergence disposition (spec `foundation/errors.md` vocabulary).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Disposition {
    /// Accepted.
    Accepted,
    /// Deferred.
    Deferred,
    /// Stale.
    Stale,
    /// Invalidated.
    Invalidated,
    /// An `mk-core` disposition this wire version does not know.
    Unmapped,
}

/// A spec input category (spec `foundation/errors.md` vocabulary).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InputCategory {
    /// Duplicate input.
    Duplicate,
    /// Own echo.
    OwnEcho,
    /// Wrong recipient.
    WrongRecipient,
    /// Unknown group.
    UnknownGroup,
    /// Stale epoch.
    StaleEpoch,
    /// Invalid encoding.
    InvalidEncoding,
    /// Invalid signature.
    InvalidSignature,
    /// Unsupported required feature.
    UnsupportedRequiredFeature,
    /// Authorization failed.
    AuthorizationFailed,
    /// Missing history.
    MissingHistory,
    /// Transport deferred.
    TransportDeferred,
    /// Resource refused.
    ResourceRefused,
    /// Transport rejected.
    TransportRejected,
    /// An `mk-core` category this wire version does not know.
    Unmapped,
}

impl From<core::InputCategory> for InputCategory {
    fn from(category: core::InputCategory) -> Self {
        match category {
            core::InputCategory::Duplicate => Self::Duplicate,
            core::InputCategory::OwnEcho => Self::OwnEcho,
            core::InputCategory::WrongRecipient => Self::WrongRecipient,
            core::InputCategory::UnknownGroup => Self::UnknownGroup,
            core::InputCategory::StaleEpoch => Self::StaleEpoch,
            core::InputCategory::InvalidEncoding => Self::InvalidEncoding,
            core::InputCategory::InvalidSignature => Self::InvalidSignature,
            core::InputCategory::UnsupportedRequiredFeature => Self::UnsupportedRequiredFeature,
            core::InputCategory::AuthorizationFailed => Self::AuthorizationFailed,
            core::InputCategory::MissingHistory => Self::MissingHistory,
            core::InputCategory::TransportDeferred => Self::TransportDeferred,
            core::InputCategory::ResourceRefused => Self::ResourceRefused,
            core::InputCategory::TransportRejected => Self::TransportRejected,
            // `InputCategory` is `#[non_exhaustive]`.
            _ => Self::Unmapped,
        }
    }
}

/// The availability cause behind `InputOutcome::Unavailable`: the spec's
/// transport categories plus the session-local signer causes.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AvailabilityCategory {
    /// Undecodable now, recoverable later.
    TransportDeferred,
    /// A local bound refused it.
    ResourceRefused,
    /// The account signer declined the unwrap.
    SignerRefused,
    /// The account signer failed the unwrap.
    SignerFailed,
    /// An `mk-core` availability cause this wire version does not know.
    Unmapped,
}

impl From<core::Availability> for AvailabilityCategory {
    fn from(category: core::Availability) -> Self {
        match category {
            core::Availability::TransportDeferred => Self::TransportDeferred,
            core::Availability::ResourceRefused => Self::ResourceRefused,
            core::Availability::SignerRefused => Self::SignerRefused,
            core::Availability::SignerFailed => Self::SignerFailed,
            // `Availability` is `#[non_exhaustive]`.
            _ => Self::Unmapped,
        }
    }
}

/// A canonical local ingest condition.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LocalState {
    /// A replacement Welcome awaits explicit local consent.
    RejoinConfirmationRequired,
    /// Authenticated state recorded this device removed.
    Removed,
    /// The local copy is frozen pending repair.
    Quarantined,
    /// An `mk-core` local state this wire version does not know.
    Unmapped,
}

impl From<core::LocalState> for LocalState {
    fn from(state: core::LocalState) -> Self {
        match state {
            core::LocalState::RejoinConfirmationRequired => Self::RejoinConfirmationRequired,
            core::LocalState::Removed => Self::Removed,
            core::LocalState::Quarantined => Self::Quarantined,
            // `LocalState` is `#[non_exhaustive]`.
            _ => Self::Unmapped,
        }
    }
}

/// The typed outcome of one admitted input.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
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
    /// Not currently processable for an availability reason.
    Unavailable {
        /// Why it is unavailable.
        category: AvailabilityCategory,
    },
    /// A canonical local condition blocked processing.
    Local {
        /// The local condition.
        state: LocalState,
    },
    /// An authenticated recovery offer arrived.
    RecoveryOffered,
    /// An `mk-core` outcome this wire version does not know.
    Unmapped,
}

impl From<core::InputOutcome> for InputOutcome {
    fn from(outcome: core::InputOutcome) -> Self {
        match outcome {
            core::InputOutcome::Accepted => Self::Accepted,
            core::InputOutcome::Deferred { category } => Self::Deferred {
                category: category.map(InputCategory::from),
            },
            core::InputOutcome::Stale { category } => Self::Stale {
                category: category.into(),
            },
            core::InputOutcome::Rejected { category } => Self::Rejected {
                category: category.into(),
            },
            core::InputOutcome::Unavailable { category } => Self::Unavailable {
                category: category.into(),
            },
            core::InputOutcome::Local(state) => Self::Local {
                state: state.into(),
            },
            core::InputOutcome::RecoveryOffered => Self::RecoveryOffered,
            // `InputOutcome` is `#[non_exhaustive]`.
            _ => Self::Unmapped,
        }
    }
}

/// Per-input report of `ingest`/`ingest_batch`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InputReport {
    /// The transport event that was processed, hex.
    #[serde(with = "hex_str")]
    pub event: [u8; 32],
    /// Its classification.
    pub outcome: InputOutcome,
}

impl From<core::InputReport> for InputReport {
    fn from(report: core::InputReport) -> Self {
        Self {
            event: report.event.0,
            outcome: report.outcome.into(),
        }
    }
}

/// The four anchors of a delivered agent text stream final.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StreamFinal {
    /// The stream this finalizes, hex.
    #[serde(with = "hex_str")]
    pub stream_id: [u8; 32],
    /// The kind-1200 start event's inner id, hex.
    #[serde(with = "hex_str")]
    pub start_event_id: [u8; 32],
    /// The sender's transcript hash, hex.
    #[serde(with = "hex_str")]
    pub transcript_hash: [u8; 32],
    /// How many QUIC chunks the transcript spanned (decimal string).
    #[serde(with = "u64_str")]
    pub chunk_count: u64,
}

impl From<core::agent_stream::StreamFinal> for StreamFinal {
    fn from(final_: core::agent_stream::StreamFinal) -> Self {
        Self {
            stream_id: final_.stream_id,
            start_event_id: final_.start_event_id,
            transcript_hash: final_.transcript_hash,
            chunk_count: final_.chunk_count,
        }
    }
}

/// A decoded group application message.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReceivedMessage {
    /// Canonical inner id (hex string).
    pub id: String,
    /// Sender account, hex.
    #[serde(with = "hex_str")]
    pub sender: [u8; 32],
    /// Inner `created_at` (decimal string).
    #[serde(with = "u64_str")]
    pub created_at: u64,
    /// Inner kind (decimal string).
    #[serde(with = "u64_str")]
    pub kind: u64,
    /// Inner tags.
    pub tags: Vec<Vec<String>>,
    /// Inner content.
    pub content: String,
    /// MLS epoch the message was sent under (decimal string).
    #[serde(with = "u64_str")]
    pub epoch: u64,
    /// The transport event that carried it, hex.
    #[serde(with = "hex_str")]
    pub transport_id: [u8; 32],
    /// One entry per `imeta` tag, in tag order.
    pub attachments: Vec<AttachmentResult>,
    /// The agent-stream final anchor, when carried.
    pub stream_final: Option<StreamFinal>,
}

impl From<core::ReceivedMessage> for ReceivedMessage {
    fn from(message: core::ReceivedMessage) -> Self {
        Self {
            id: message.id,
            sender: message.sender.0,
            created_at: message.created_at,
            kind: message.kind,
            tags: message.tags,
            content: message.content,
            epoch: message.epoch,
            transport_id: message.transport_id.0,
            attachments: message
                .attachments
                .into_iter()
                .map(|result| match result {
                    Ok(reference) => AttachmentResult::Ok {
                        reference: reference.into(),
                    },
                    Err(reason) => AttachmentResult::Rejected {
                        reason: reason.into(),
                    },
                })
                .collect(),
            stream_final: message.stream_final.map(StreamFinal::from),
        }
    }
}

/// The anchors of a delivered agent text stream start (kind 1200).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentStreamAnchor {
    /// Canonical inner id of the kind-1200 event.
    pub id: String,
    /// Sender account, hex.
    #[serde(with = "hex_str")]
    pub sender: [u8; 32],
    /// Inner `created_at` (decimal string).
    #[serde(with = "u64_str")]
    pub created_at: u64,
    /// MLS epoch the start was sent under (decimal string).
    #[serde(with = "u64_str")]
    pub epoch: u64,
    /// The transport event that carried it, hex.
    #[serde(with = "hex_str")]
    pub transport_id: [u8; 32],
    /// The 32-byte stream id, hex.
    #[serde(with = "hex_str")]
    pub stream_id: [u8; 32],
    /// The prompt message this stream answers, when tagged, hex.
    #[serde(with = "opt_hex32")]
    pub parent: Option<[u8; 32]>,
}

impl AgentStreamAnchor {
    /// `mk-core`'s anchor type lives in a private module (reachable only as
    /// the `AgentStreamStarted` payload), so the conversion is field-based
    /// rather than a named `From` impl.
    fn from_fields(
        id: String,
        sender: core::AccountId,
        created_at: u64,
        epoch: u64,
        transport_id: core::EventId,
        stream_id: [u8; 32],
        parent: Option<core::EventId>,
    ) -> Self {
        Self {
            id,
            sender: sender.0,
            created_at,
            epoch,
            transport_id: transport_id.0,
            stream_id,
            parent: parent.map(|parent_id| parent_id.0),
        }
    }
}

/// The state delta a `GroupStateChanged` event carries.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum GroupChange {
    /// A member account was added.
    MemberAdded {
        /// The account, hex.
        #[serde(with = "hex_str")]
        account: [u8; 32],
    },
    /// A member account was removed.
    MemberRemoved {
        /// The account, hex.
        #[serde(with = "hex_str")]
        account: [u8; 32],
    },
    /// A member account left.
    MemberLeft {
        /// The account, hex.
        #[serde(with = "hex_str")]
        account: [u8; 32],
    },
    /// A member gained admin.
    AdminAdded {
        /// The account, hex.
        #[serde(with = "hex_str")]
        account: [u8; 32],
    },
    /// A member lost admin.
    AdminRemoved {
        /// The account, hex.
        #[serde(with = "hex_str")]
        account: [u8; 32],
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
        /// Previous retention in seconds (decimal string).
        #[serde(with = "u64_str")]
        old_seconds: u64,
        /// New retention in seconds (decimal string).
        #[serde(with = "u64_str")]
        new_seconds: u64,
    },
    /// The group was disbanded.
    Disbanded,
    /// An `mk-core` change this wire version does not know.
    Unmapped,
}

impl From<core::GroupChange> for GroupChange {
    fn from(change: core::GroupChange) -> Self {
        match change {
            core::GroupChange::MemberAdded { account } => Self::MemberAdded { account: account.0 },
            core::GroupChange::MemberRemoved { account } => {
                Self::MemberRemoved { account: account.0 }
            }
            core::GroupChange::MemberLeft { account } => Self::MemberLeft { account: account.0 },
            core::GroupChange::AdminAdded { account } => Self::AdminAdded { account: account.0 },
            core::GroupChange::AdminRemoved { account } => {
                Self::AdminRemoved { account: account.0 }
            }
            core::GroupChange::Renamed { old_name, new_name } => {
                Self::Renamed { old_name, new_name }
            }
            core::GroupChange::AvatarChanged => Self::AvatarChanged,
            core::GroupChange::RetentionChanged {
                old_seconds,
                new_seconds,
            } => Self::RetentionChanged {
                old_seconds,
                new_seconds,
            },
            core::GroupChange::Disbanded => Self::Disbanded,
            // `GroupChange` is `#[non_exhaustive]`.
            _ => Self::Unmapped,
        }
    }
}

/// Events the session surfaces to the host.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum SessionEvent {
    /// A group application message decrypted on the canonical branch.
    MessageReceived {
        /// The group, hex.
        #[serde(with = "hex_bytes")]
        group: Vec<u8>,
        /// The decoded message.
        message: ReceivedMessage,
    },
    /// An agent text stream start event (kind 1200).
    AgentStreamStarted {
        /// The group, hex.
        #[serde(with = "hex_bytes")]
        group: Vec<u8>,
        /// The decoded anchors.
        start: AgentStreamAnchor,
    },
    /// A previously delivered message decrypted only on a losing branch.
    MessageInvalidated {
        /// The group, hex.
        #[serde(with = "hex_bytes")]
        group: Vec<u8>,
        /// Transport event id of the invalidated message, hex.
        #[serde(with = "hex_str")]
        message: [u8; 32],
        /// Source epoch (decimal string).
        #[serde(with = "u64_str")]
        epoch: u64,
    },
    /// This device created a group.
    GroupCreated {
        /// The group, hex.
        #[serde(with = "hex_bytes")]
        group: Vec<u8>,
    },
    /// This device joined a group via Welcome.
    GroupJoined {
        /// The group, hex.
        #[serde(with = "hex_bytes")]
        group: Vec<u8>,
    },
    /// The group's canonical epoch advanced.
    EpochChanged {
        /// The group, hex.
        #[serde(with = "hex_bytes")]
        group: Vec<u8>,
        /// Previous epoch (decimal string).
        #[serde(with = "u64_str")]
        from: u64,
        /// New epoch (decimal string).
        #[serde(with = "u64_str")]
        to: u64,
    },
    /// An MLS-authenticated group-state change was applied.
    GroupStateChanged {
        /// The group, hex.
        #[serde(with = "hex_bytes")]
        group: Vec<u8>,
        /// Epoch reached (decimal string).
        #[serde(with = "u64_str")]
        epoch: u64,
        /// The authenticated actor, when attributable, hex.
        #[serde(with = "opt_hex32")]
        actor: Option<[u8; 32]>,
        /// What changed.
        change: GroupChange,
        /// The commit that produced the change, hex.
        #[serde(with = "opt_hex32")]
        origin_commit: Option<[u8; 32]>,
    },
    /// A commit's state notifications were withdrawn.
    GroupStateInvalidated {
        /// The group, hex.
        #[serde(with = "hex_bytes")]
        group: Vec<u8>,
        /// The fork's source epoch (decimal string).
        #[serde(with = "u64_str")]
        epoch: u64,
        /// The superseded commit's transport id, hex.
        #[serde(with = "hex_str")]
        commit: [u8; 32],
    },
    /// A withdrawn commit's state notifications are canonical again.
    GroupStateRevalidated {
        /// The group, hex.
        #[serde(with = "hex_bytes")]
        group: Vec<u8>,
        /// The fork's source epoch (decimal string).
        #[serde(with = "u64_str")]
        epoch: u64,
        /// The re-adopted commit's transport id, hex.
        #[serde(with = "hex_str")]
        commit: [u8; 32],
    },
    /// This device's local group copy ended.
    LocalCopyTerminated {
        /// The group, hex.
        #[serde(with = "hex_bytes")]
        group: Vec<u8>,
        /// Whether the departure was voluntary.
        voluntary: bool,
    },
    /// The device's participation was restored by branch selection.
    LocalCopyRestored {
        /// The group, hex.
        #[serde(with = "hex_bytes")]
        group: Vec<u8>,
    },
    /// A queued send was refused once the group settled.
    QueuedIntentRefused {
        /// The group, hex.
        #[serde(with = "hex_bytes")]
        group: Vec<u8>,
        /// The queued intent's id, hex.
        #[serde(with = "hex_str")]
        intent: [u8; 32],
        /// Stable name of the intent kind.
        intent_kind: String,
        /// Stable privacy-safe refusal name.
        reason: String,
    },
    /// A persisted group failed hydration at open and is quarantined.
    HydrationQuarantined {
        /// The group, hex.
        #[serde(with = "hex_bytes")]
        group: Vec<u8>,
    },
    /// A quarantined group hydrated successfully on retry.
    HydrationRecovered {
        /// The group, hex.
        #[serde(with = "hex_bytes")]
        group: Vec<u8>,
        /// Epoch recovered at (decimal string).
        #[serde(with = "u64_str")]
        epoch: u64,
    },
    /// A staged commit orphaned by a crash was cleared at open.
    PendingCommitRecovered {
        /// The group, hex.
        #[serde(with = "hex_bytes")]
        group: Vec<u8>,
        /// Epoch recovered at (decimal string).
        #[serde(with = "u64_str")]
        epoch: u64,
    },
    /// The group needs a verified repair path.
    GroupUnrecoverable {
        /// The group, hex.
        #[serde(with = "hex_bytes")]
        group: Vec<u8>,
    },
    /// An outbound obligation reached a resolution checkpoint.
    PublishResolved {
        /// The outbound item, hex.
        #[serde(with = "hex_str")]
        outbound: [u8; 32],
        /// The verdict.
        resolution: PublishResolution,
    },
    /// An `mk-core` session event this wire version does not know.
    Unmapped,
}

impl From<core::SessionEvent> for SessionEvent {
    fn from(event: core::SessionEvent) -> Self {
        match event {
            core::SessionEvent::MessageReceived { group, message } => Self::MessageReceived {
                group: group.0,
                message: message.into(),
            },
            core::SessionEvent::AgentStreamStarted { group, start } => Self::AgentStreamStarted {
                group: group.0,
                start: AgentStreamAnchor::from_fields(
                    start.id,
                    start.sender,
                    start.created_at,
                    start.epoch,
                    start.transport_id,
                    start.stream_id,
                    start.parent,
                ),
            },
            core::SessionEvent::MessageInvalidated {
                group,
                message,
                epoch,
            } => Self::MessageInvalidated {
                group: group.0,
                message: message.0,
                epoch,
            },
            core::SessionEvent::GroupCreated { group } => Self::GroupCreated { group: group.0 },
            core::SessionEvent::GroupJoined { group } => Self::GroupJoined { group: group.0 },
            core::SessionEvent::EpochChanged { group, from, to } => Self::EpochChanged {
                group: group.0,
                from,
                to,
            },
            core::SessionEvent::GroupStateChanged {
                group,
                epoch,
                actor,
                change,
                origin_commit,
            } => Self::GroupStateChanged {
                group: group.0,
                epoch,
                actor: actor.map(|account| account.0),
                change: change.into(),
                origin_commit: origin_commit.map(|id| id.0),
            },
            core::SessionEvent::GroupStateInvalidated {
                group,
                epoch,
                commit,
            } => Self::GroupStateInvalidated {
                group: group.0,
                epoch,
                commit: commit.0,
            },
            core::SessionEvent::GroupStateRevalidated {
                group,
                epoch,
                commit,
            } => Self::GroupStateRevalidated {
                group: group.0,
                epoch,
                commit: commit.0,
            },
            core::SessionEvent::LocalCopyTerminated { group, voluntary } => {
                Self::LocalCopyTerminated {
                    group: group.0,
                    voluntary,
                }
            }
            core::SessionEvent::LocalCopyRestored { group } => {
                Self::LocalCopyRestored { group: group.0 }
            }
            core::SessionEvent::QueuedIntentRefused {
                group,
                intent,
                intent_kind,
                reason,
            } => Self::QueuedIntentRefused {
                group: group.0,
                intent: intent.0,
                intent_kind,
                reason,
            },
            core::SessionEvent::HydrationQuarantined { group } => {
                Self::HydrationQuarantined { group: group.0 }
            }
            core::SessionEvent::HydrationRecovered { group, epoch } => Self::HydrationRecovered {
                group: group.0,
                epoch,
            },
            core::SessionEvent::PendingCommitRecovered { group, epoch } => {
                Self::PendingCommitRecovered {
                    group: group.0,
                    epoch,
                }
            }
            core::SessionEvent::GroupUnrecoverable { group } => {
                Self::GroupUnrecoverable { group: group.0 }
            }
            core::SessionEvent::PublishResolved {
                outbound,
                resolution,
            } => Self::PublishResolved {
                outbound: outbound.0,
                resolution: resolution.into(),
            },
            // `SessionEvent` is `#[non_exhaustive]`.
            _ => Self::Unmapped,
        }
    }
}

/// What `publish_key_package` returns.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct KeyPackageInfo {
    /// Raw `KeyPackage` bytes, hex.
    #[serde(with = "hex_bytes")]
    pub key_package: Vec<u8>,
    /// `KeyPackageRef` (hex) as asserted by the `i` tag.
    pub key_package_ref: String,
    /// The event id that published it, hex.
    #[serde(with = "hex_str")]
    pub event_id: [u8; 32],
    /// The publisher's account, hex.
    #[serde(with = "hex_str")]
    pub account: [u8; 32],
}

impl From<core::key_package::KeyPackageInfo> for KeyPackageInfo {
    fn from(info: core::key_package::KeyPackageInfo) -> Self {
        Self {
            key_package: info.key_package,
            key_package_ref: info.key_package_ref,
            event_id: info.event_id.0,
            account: info.account.0,
        }
    }
}

/// One member in a [`GroupSnapshot`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MemberSnapshot {
    /// The member's account, hex.
    #[serde(with = "hex_str")]
    pub account: [u8; 32],
    /// Whether the member is in the admin-policy set.
    pub admin: bool,
}

/// A UI-facing snapshot of one group.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GroupSnapshot {
    /// The MLS group id, hex.
    #[serde(with = "hex_bytes")]
    pub id: Vec<u8>,
    /// Profile name.
    pub name: String,
    /// Profile description.
    pub description: String,
    /// Current epoch (decimal string).
    #[serde(with = "u64_str")]
    pub epoch: u64,
    /// Lifecycle bucket.
    pub state: GroupState,
    /// Members (account + admin flag).
    pub members: Vec<MemberSnapshot>,
    /// The group's Nostr routing relays.
    pub relays: Vec<String>,
    /// The 32-byte Nostr route id (`#h` filter value), hex.
    #[serde(with = "hex_str")]
    pub nostr_group_id: [u8; 32],
    /// Whether this account is an admin.
    pub own_account_admin: bool,
    /// The group's `0x800b` encrypted-media policy, when present.
    pub encrypted_media: Option<EncryptedMediaPolicy>,
    /// The group's `0x8006` agent text stream policy, when present.
    pub agent_text_stream: Option<AgentStreamPolicy>,
}

impl From<core::GroupSnapshot> for GroupSnapshot {
    fn from(snapshot: core::GroupSnapshot) -> Self {
        Self {
            id: snapshot.id.0,
            name: snapshot.name,
            description: snapshot.description,
            epoch: snapshot.epoch,
            state: snapshot.state.into(),
            members: snapshot
                .members
                .into_iter()
                .map(|member| MemberSnapshot {
                    account: member.account.0,
                    admin: member.admin,
                })
                .collect(),
            relays: snapshot.relays,
            nostr_group_id: snapshot.nostr_group_id,
            own_account_admin: snapshot.own_account_admin,
            encrypted_media: snapshot.encrypted_media.map(EncryptedMediaPolicy::from),
            agent_text_stream: snapshot.agent_text_stream.map(AgentStreamPolicy::from),
        }
    }
}

/// One deferred account-signer operation surfaced to the host.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SignerRequest {
    /// Request id (monotonic per bridge).
    pub id: u32,
    /// The operation to run.
    pub op: SignerOp,
}

/// An account-signer operation the host must perform.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum SignerOp {
    /// Sign `unsigned` and return the complete event.
    SignEvent {
        /// The unsigned event to sign.
        unsigned: UnsignedEvent,
    },
    /// NIP-44 seal `plaintext` for `peer`'s key.
    Nip44Encrypt {
        /// The peer account, hex.
        #[serde(with = "hex_str")]
        peer: [u8; 32],
        /// The plaintext.
        plaintext: String,
    },
    /// NIP-44 open `ciphertext` from `peer`.
    Nip44Decrypt {
        /// The peer account, hex.
        #[serde(with = "hex_str")]
        peer: [u8; 32],
        /// The ciphertext.
        ciphertext: String,
    },
}

/// One host answer to a [`SignerRequest`].
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SignerReply {
    /// The request id being answered.
    pub id: u32,
    /// The operation's result.
    pub result: SignerResult,
}

/// The result the host reports for a signer operation.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum SignerResult {
    /// A `sign_event` answer.
    Signed {
        /// The completed signed event.
        event: Event,
    },
    /// A `nip44_*` answer.
    Text {
        /// The produced plaintext or ciphertext.
        text: String,
    },
    /// The signer declined the operation.
    Refused,
    /// The signer failed the operation.
    Failed,
}
