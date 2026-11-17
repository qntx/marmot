//! The `ok` result vocabulary: one variant per request outcome.

use serde::{Deserialize, Serialize};

use super::ser::{hex_bytes, hex_str, opt_u64_str};
use super::types::{GroupSnapshot, InputReport, Outbound, PublishResolution, SendOutcome};

/// `submit_proof` succeeded; the session is usable.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReadyResult {
    /// The session's account, hex.
    #[serde(with = "hex_str")]
    pub account: [u8; 32],
}

/// `awaiting_durability`: the gate's outstanding watermark.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AwaitingDurabilityResult {
    /// The lowest unacknowledged sequence, decimal string or `null`.
    #[serde(with = "opt_u64_str")]
    pub seq: Option<u64>,
}

/// `take_outbound`: gated items released for publishing.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OutboundResult {
    /// The released outbound items.
    pub items: Vec<Outbound>,
}

/// `report_publish` verdict.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PublishResolutionResult {
    /// The resolution the session computed.
    pub resolution: PublishResolution,
}

/// `publish_key_package` result.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct KeyPackageResult {
    /// Raw `KeyPackage` bytes, hex.
    #[serde(with = "hex_bytes")]
    pub key_package: Vec<u8>,
    /// `KeyPackageRef` (hex) as asserted by the `i` tag.
    pub key_package_ref: String,
    /// The published event's id, hex.
    #[serde(with = "hex_str")]
    pub event_id: [u8; 32],
    /// The publisher's account, hex.
    #[serde(with = "hex_str")]
    pub account: [u8; 32],
}

/// `publish_inbox_relays` result.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InboxRelaysResult {
    /// The staged outbound id, hex.
    #[serde(with = "hex_str")]
    pub outbound_id: [u8; 32],
}

/// `create_group` result.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GroupCreatedResult {
    /// The new group's snapshot.
    pub group: Box<GroupSnapshot>,
}

/// Any `send_*`/`invite`/membership result.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SendOutcomeResult {
    /// What the send produced.
    pub outcome: SendOutcome,
}

/// `seal_attachment` result: the blob to upload plus the send-time handle.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SealedAttachmentResult {
    /// The encrypted blob, hex.
    #[serde(with = "hex_bytes")]
    pub ciphertext: Vec<u8>,
    /// SHA-256 of `ciphertext`, hex.
    #[serde(with = "hex_str")]
    pub ciphertext_sha256: [u8; 32],
    /// The opaque handle `send_media` consumes.
    pub handle: u32,
}

/// `open_attachment` result.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AttachmentData {
    /// The decrypted plaintext, hex.
    #[serde(with = "hex_bytes")]
    pub plaintext: Vec<u8>,
}

/// `ingest` result.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IngestedResult {
    /// Per-input reports.
    pub reports: Vec<InputReport>,
}

/// `tick` result.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TickedResult {
    /// Milliseconds until the earliest scheduled wake, decimal string or
    /// `null`.
    #[serde(with = "opt_u64_str")]
    pub next_wake_ms: Option<u64>,
}

/// `groups` result.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GroupsResult {
    /// Snapshots of every live group.
    pub groups: Vec<GroupSnapshot>,
}

/// `group` result.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GroupResult {
    /// The group's snapshot, or `null` when not live.
    pub group: Option<Box<GroupSnapshot>>,
}

/// The typed result of a successful `call`, internally tagged by `"type"`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum OpResult {
    /// The session is usable (after `submit_proof`).
    Ready(ReadyResult),
    /// The request completed with no payload (`ack_durable`,
    /// `discard_attachment`, `acknowledge_messages`).
    Done,
    /// `awaiting_durability`.
    AwaitingDurability(AwaitingDurabilityResult),
    /// `take_outbound`.
    Outbound(OutboundResult),
    /// `report_publish`.
    PublishResolution(PublishResolutionResult),
    /// `publish_key_package`.
    KeyPackage(KeyPackageResult),
    /// `publish_inbox_relays`.
    InboxRelays(InboxRelaysResult),
    /// `create_group`.
    GroupCreated(GroupCreatedResult),
    /// A send/membership operation.
    SendOutcome(SendOutcomeResult),
    /// `seal_attachment`.
    SealedAttachment(SealedAttachmentResult),
    /// `open_attachment`.
    Attachment(AttachmentData),
    /// `ingest`.
    Ingested(IngestedResult),
    /// `tick`.
    Ticked(TickedResult),
    /// `groups`.
    Groups(GroupsResult),
    /// `group`.
    Group(GroupResult),
}
