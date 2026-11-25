//! The `ok` result vocabulary: one variant per request outcome.

use mk_core as core;
use serde::{Deserialize, Serialize};

use super::ser::{hex_bytes, hex_bytes_seq, hex_str, opt_u64_str};
use super::types::{
    Event, GroupSnapshot, InputReport, KeyPackageInfo, Outbound, PublishResolution, ReaddOutcome,
    RejoinOffer, SendOutcome, SentEvent,
};

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

/// `sign_out` result: which groups the device left and which admin
/// groups it stays in (the account's only admin leaf cannot `SelfRemove`
/// without orphaning the group — the host is told to decide).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SignOutResult {
    /// Groups the device left (`SelfRemove` committed or durable),
    /// hex each.
    #[serde(with = "hex_bytes_seq")]
    pub left: Vec<Vec<u8>>,
    /// Groups whose admin leaf stays, hex each.
    #[serde(with = "hex_bytes_seq")]
    pub kept_admin_leaves: Vec<Vec<u8>>,
}

/// `ensure_key_package`/`key_package_status` result: the account
/// `KeyPackage` slot's lifecycle standing.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct KeyPackageStatusResult {
    /// The stable replaceable-event slot (`d` tag), hex.
    #[serde(with = "hex_str")]
    pub slot: [u8; 32],
    /// The promoted, acknowledged publication, or `null`.
    pub current: Option<KeyPackageInfo>,
    /// Whether a replacement is staged or in flight.
    pub pending: bool,
    /// Retired private material still inside its grace period.
    pub retained: u32,
    /// Absolute Unix milliseconds of the earliest lifecycle deadline, or
    /// `null` when nothing is scheduled (decimal string).
    #[serde(with = "opt_u64_str")]
    pub next_due_ms: Option<u64>,
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

/// Any `send_message`/`send_media`/`send_intent` result: the outcome plus
/// the inner event the host renders for the sender's own copy.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MessageSentResult {
    /// What the send produced.
    pub outcome: SendOutcome,
    /// The inner app event, with its canonical id.
    pub message: SentEvent,
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
    /// Snapshots of every stored local copy — live and terminated
    /// (`removed`, `disbanded`) alike — in group-id byte order; a
    /// terminated copy stays listed until `forget_group`.
    pub groups: Vec<GroupSnapshot>,
}

/// `group` result.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GroupResult {
    /// The group's snapshot, or `null` when the device holds no copy of
    /// it (unknown, quarantined or forgotten).
    pub group: Option<Box<GroupSnapshot>>,
}

/// `select_key_packages` result.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct KeyPackagesResult {
    /// The selected kind-30443 publications, best first.
    pub events: Vec<Event>,
}

/// `rejoin_offers` result.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RejoinOffersResult {
    /// The stored offers awaiting consent.
    pub offers: Vec<RejoinOffer>,
}

/// `readd_member` result: which phase the re-add reached.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReaddResult {
    /// What the call staged.
    pub outcome: ReaddOutcome,
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
    /// `ensure_key_package`/`key_package_status`.
    KeyPackageStatus(KeyPackageStatusResult),
    /// `sign_out`.
    SignOut(SignOutResult),
    /// `publish_inbox_relays`.
    InboxRelays(InboxRelaysResult),
    /// `create_group`.
    GroupCreated(GroupCreatedResult),
    /// A send/membership operation.
    SendOutcome(SendOutcomeResult),
    /// `send_message`/`send_media`/`send_intent`.
    MessageSent(MessageSentResult),
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
    /// `select_key_packages`.
    KeyPackages(KeyPackagesResult),
    /// `rejoin_offers`.
    RejoinOffers(RejoinOffersResult),
    /// `readd_member`.
    Readd(ReaddResult),
}

impl From<core::SignOutReport> for SignOutResult {
    fn from(report: core::SignOutReport) -> Self {
        Self {
            left: report.left.into_iter().map(|id| id.0).collect(),
            kept_admin_leaves: report
                .kept_admin_leaves
                .into_iter()
                .map(|id| id.0)
                .collect(),
        }
    }
}

impl From<core::KeyPackageStatus> for KeyPackageStatusResult {
    fn from(status: core::KeyPackageStatus) -> Self {
        Self {
            slot: status.slot,
            current: status.current.map(KeyPackageInfo::from),
            pending: status.pending,
            retained: status.retained,
            next_due_ms: status.next_due_ms,
        }
    }
}
