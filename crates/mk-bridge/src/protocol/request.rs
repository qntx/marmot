//! The `call` request vocabulary: one variant per `Session` surface, plus
//! `submit_proof` (two-phase open) and `signer_reply` (suspension resume).
//!
//! Every variant is a newtype wrapper over a `deny_unknown_fields`
//! argument struct — internally-tagged enums cannot reject unknown fields
//! themselves. Argument-less requests wrap the empty [`NoArgs`] struct.

use serde::{Deserialize, Serialize};

use super::ser::{hex_bytes, hex_str, hex_str_seq, u64_str};
use super::types::{
    AppMessage, AttachmentInput, ComponentUpdate, Event, GroupAvatar, Invitee, MediaAttachmentRef,
    MediaReference, MessageIntent, RelayResult, SignerReply,
};

/// The empty argument struct every argument-less request wraps.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NoArgs {}

/// Complete an `open` that answered `needs_account_proof`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SubmitProofRequest {
    /// The signed proof event.
    pub event: Event,
}

/// Acknowledge durable storage of committed batches.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AckDurableRequest {
    /// Highest batch sequence now durable (decimal string).
    #[serde(with = "u64_str")]
    pub seq: u64,
}

/// Report publish outcomes for one outbound item.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReportPublishRequest {
    /// The outbound id being reported, hex.
    #[serde(with = "hex_str")]
    pub id: [u8; 32],
    /// Per-relay verdicts.
    pub results: Vec<RelayResult>,
}

/// Build, sign and stage the account's kind-30443 `KeyPackage`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PublishKeyPackageRequest {
    /// The relays to publish to.
    pub relays: Vec<String>,
}

/// Build, sign and stage the account's kind-10050 inbox relay list.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PublishInboxRelaysRequest {
    /// The relays to advertise.
    pub relays: Vec<String>,
}

/// Create a group.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CreateGroupRequest {
    /// Group name.
    pub name: String,
    /// Group description.
    pub description: String,
    /// The group's Nostr relays.
    pub relays: Vec<String>,
    /// Founding invitees (the creator is always a member).
    pub invitees: Vec<Invitee>,
    /// Additional founding admins besides the creator, 32-byte hex each.
    #[serde(with = "hex_str_seq")]
    pub admins: Vec<[u8; 32]>,
    /// Additional initial app components.
    pub components: Vec<ComponentUpdate>,
}

/// Invite accounts to a group.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InviteRequest {
    /// The group, hex.
    #[serde(with = "hex_bytes")]
    pub group: Vec<u8>,
    /// The invitees.
    pub invitees: Vec<Invitee>,
}

/// Remove member accounts from a group.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RemoveMembersRequest {
    /// The group, hex.
    #[serde(with = "hex_bytes")]
    pub group: Vec<u8>,
    /// The member accounts to remove, 32-byte hex each.
    #[serde(with = "hex_str_seq")]
    pub members: Vec<[u8; 32]>,
}

/// Request to leave a group.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LeaveRequest {
    /// The group, hex.
    #[serde(with = "hex_bytes")]
    pub group: Vec<u8>,
}

/// Refresh this device's own membership (self-update commit).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SelfUpdateRequest {
    /// The group, hex.
    #[serde(with = "hex_bytes")]
    pub group: Vec<u8>,
}

/// Update the group profile.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UpdateGroupProfileRequest {
    /// The group, hex.
    #[serde(with = "hex_bytes")]
    pub group: Vec<u8>,
    /// New name, or leave unchanged with `null`.
    pub name: Option<String>,
    /// New description, or leave unchanged with `null`.
    pub description: Option<String>,
}

/// Apply app-component updates.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UpdateAppComponentsRequest {
    /// The group, hex.
    #[serde(with = "hex_bytes")]
    pub group: Vec<u8>,
    /// The component updates.
    pub updates: Vec<ComponentUpdate>,
}

/// Send an application message (kind 9 chat, kind 7 reactions).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SendMessageRequest {
    /// The group, hex.
    #[serde(with = "hex_bytes")]
    pub group: Vec<u8>,
    /// The inner app event.
    pub message: AppMessage,
}

/// Encrypt an attachment and pin it to the current media epoch.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SealAttachmentRequest {
    /// The group, hex.
    #[serde(with = "hex_bytes")]
    pub group: Vec<u8>,
    /// What to seal.
    pub attachment: AttachmentInput,
}

/// Send a kind-9 media message carrying sealed attachments.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SendMediaRequest {
    /// The group, hex.
    #[serde(with = "hex_bytes")]
    pub group: Vec<u8>,
    /// The message body.
    pub caption: String,
    /// One `(handle, locators)` pair per sealed attachment.
    pub attachments: Vec<MediaAttachmentRef>,
    /// Extra tags for the inner event.
    pub tags: Vec<Vec<String>>,
}

/// Drop one sealed attachment handle.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DiscardAttachmentRequest {
    /// The handle to drop.
    pub handle: u32,
}

/// Decrypt a media attachment reference.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OpenAttachmentRequest {
    /// The group, hex.
    #[serde(with = "hex_bytes")]
    pub group: Vec<u8>,
    /// The transport event id that carried the reference, hex.
    #[serde(with = "hex_str")]
    pub message: [u8; 32],
    /// The validated `imeta` reference.
    pub reference: MediaReference,
    /// The downloaded ciphertext blob, hex.
    #[serde(with = "hex_bytes")]
    pub ciphertext: Vec<u8>,
}

/// Admit inbound transport events.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IngestRequest {
    /// The events; sorted and deduplicated by the session.
    pub events: Vec<Event>,
}

/// Acknowledge delivered application messages (at-least-once).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AcknowledgeMessagesRequest {
    /// Transport event ids, 32-byte hex each.
    #[serde(with = "hex_str_seq")]
    pub ids: Vec<[u8; 32]>,
}

/// Snapshot one group.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GroupRequest {
    /// The group, hex.
    #[serde(with = "hex_bytes")]
    pub group: Vec<u8>,
}

/// Answer deferred signer operations and resume the suspended call.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SignerReplyRequest {
    /// The replies.
    pub replies: Vec<SignerReply>,
}

/// Send a typed application message (`MessageIntent`) into a group.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SendIntentRequest {
    /// The group, hex.
    #[serde(with = "hex_bytes")]
    pub group: Vec<u8>,
    /// The typed intent.
    pub intent: MessageIntent,
}

/// Grant or revoke a member's admin flag.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SetAdminRequest {
    /// The group, hex.
    #[serde(with = "hex_bytes")]
    pub group: Vec<u8>,
    /// The member account, hex.
    #[serde(with = "hex_str")]
    pub account: [u8; 32],
    /// Whether the account becomes an admin.
    pub admin: bool,
}

/// Set or clear the group's avatar component.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SetGroupAvatarRequest {
    /// The group, hex.
    #[serde(with = "hex_bytes")]
    pub group: Vec<u8>,
    /// The avatar to set, or `null` to clear.
    pub avatar: Option<GroupAvatar>,
}

/// Set the group's message retention.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SetRetentionRequest {
    /// The group, hex.
    #[serde(with = "hex_bytes")]
    pub group: Vec<u8>,
    /// Retention seconds; `0` disables (decimal string).
    #[serde(with = "u64_str")]
    pub seconds: u64,
}

/// Request the group's terminal disband.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DisbandRequest {
    /// The group, hex.
    #[serde(with = "hex_bytes")]
    pub group: Vec<u8>,
}

/// Require the lifecycle component on a group that predates it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EnableDisbandingRequest {
    /// The group, hex.
    #[serde(with = "hex_bytes")]
    pub group: Vec<u8>,
}

/// Forget the group locally.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ForgetGroupRequest {
    /// The group, hex.
    #[serde(with = "hex_bytes")]
    pub group: Vec<u8>,
}

/// A `call` request. The `"type"` tag must appear in
/// [`crate::protocol::REQUEST_TYPES`] before decode; anything else answers
/// `unsupported`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Request {
    /// Complete the account-identity proof.
    SubmitProof(SubmitProofRequest),
    /// Acknowledge durable batches.
    AckDurable(AckDurableRequest),
    /// Report the durability gate's outstanding watermark.
    AwaitingDurability(NoArgs),
    /// Release gated outbound items for publishing.
    TakeOutbound(NoArgs),
    /// Report publish verdicts for one outbound item.
    ReportPublish(ReportPublishRequest),
    /// Stage the account `KeyPackage`.
    PublishKeyPackage(PublishKeyPackageRequest),
    /// Stage the account inbox relay list.
    PublishInboxRelays(PublishInboxRelaysRequest),
    /// Create a group.
    CreateGroup(CreateGroupRequest),
    /// Invite accounts to a group.
    Invite(InviteRequest),
    /// Remove members from a group.
    RemoveMembers(RemoveMembersRequest),
    /// Request to leave a group.
    Leave(LeaveRequest),
    /// Refresh own membership.
    SelfUpdate(SelfUpdateRequest),
    /// Update the group profile.
    UpdateGroupProfile(UpdateGroupProfileRequest),
    /// Apply app-component updates.
    UpdateAppComponents(UpdateAppComponentsRequest),
    /// Send an application message.
    SendMessage(SendMessageRequest),
    /// Encrypt an attachment.
    SealAttachment(SealAttachmentRequest),
    /// Send a media message.
    SendMedia(SendMediaRequest),
    /// Drop a sealed attachment handle.
    DiscardAttachment(DiscardAttachmentRequest),
    /// Decrypt a media attachment.
    OpenAttachment(OpenAttachmentRequest),
    /// Admit inbound events.
    Ingest(IngestRequest),
    /// Run one maintenance/convergence pass.
    Tick(NoArgs),
    /// Acknowledge delivered messages.
    AcknowledgeMessages(AcknowledgeMessagesRequest),
    /// Snapshot every live group.
    Groups(NoArgs),
    /// Snapshot one group.
    Group(GroupRequest),
    /// Answer signer operations and resume.
    SignerReply(SignerReplyRequest),
    /// Send a typed application message.
    SendIntent(SendIntentRequest),
    /// Grant or revoke a member's admin flag.
    SetAdmin(SetAdminRequest),
    /// Set or clear the group's avatar.
    SetGroupAvatar(SetGroupAvatarRequest),
    /// Set the group's message retention.
    SetRetention(SetRetentionRequest),
    /// Request the group's terminal disband.
    Disband(DisbandRequest),
    /// Require the lifecycle component on the group.
    EnableDisbanding(EnableDisbandingRequest),
    /// Forget the group locally.
    ForgetGroup(ForgetGroupRequest),
}
