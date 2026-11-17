//! The wire protocol: JSON messages between the host and the bridge.
//!
//! Encoding rules: UTF-8 JSON; bytes are lowercase hex strings; `u64`
//! values are decimal strings; `u8`/`u16`/`u32` are JSON numbers; no
//! floats; options are always emitted and decode to `null`. Enums carrying
//! data are internally tagged by `"type"` with `snake_case` variant names;
//! fieldless enums are plain `snake_case` strings. Serde error messages are
//! never surfaced — decode failures map to
//! [`BridgeError::InvalidRequest`] with a fixed reason label.

mod request;
mod result;
pub(crate) mod ser;
mod types;

pub use request::{
    AckDurableRequest, AcknowledgeMessagesRequest, CreateGroupRequest, DiscardAttachmentRequest,
    GroupRequest, IngestRequest, InviteRequest, LeaveRequest, NoArgs, OpenAttachmentRequest,
    PublishInboxRelaysRequest, PublishKeyPackageRequest, RemoveMembersRequest,
    ReportPublishRequest, Request, SealAttachmentRequest, SelfUpdateRequest, SendMediaRequest,
    SendMessageRequest, SignerReplyRequest, SubmitProofRequest, UpdateAppComponentsRequest,
    UpdateGroupProfileRequest,
};
pub use result::{
    AttachmentData, AwaitingDurabilityResult, GroupCreatedResult, GroupResult, GroupsResult,
    InboxRelaysResult, IngestedResult, KeyPackageResult, OpResult, OutboundResult,
    PublishResolutionResult, ReadyResult, SealedAttachmentResult, SendOutcomeResult, TickedResult,
};
pub use types::*;

use serde::{Deserialize, Serialize};

use crate::error::{BridgeError, invalid_reason};

/// Wire protocol version. Bumped on any breaking schema change.
pub const PROTOCOL_VERSION: u32 = 1;

/// Maximum accepted request/config byte length.
pub const MAX_REQUEST_BYTES: usize = 64 * 1024 * 1024;

/// Every `request.type` this build implements, in canonical order. Checked
/// before full decode: an unknown type answers [`BridgeError::Unsupported`]
/// even when the rest of the body is malformed.
pub const REQUEST_TYPES: &[&str] = &[
    "ack_durable",
    "acknowledge_messages",
    "awaiting_durability",
    "create_group",
    "discard_attachment",
    "group",
    "groups",
    "ingest",
    "invite",
    "leave",
    "open_attachment",
    "publish_inbox_relays",
    "publish_key_package",
    "remove_members",
    "report_publish",
    "seal_attachment",
    "self_update",
    "send_media",
    "send_message",
    "signer_reply",
    "submit_proof",
    "take_outbound",
    "tick",
    "update_app_components",
    "update_group_profile",
];

/// The `open` config: account binding, first clock reading and policy.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// Must equal [`PROTOCOL_VERSION`].
    pub protocol_version: u32,
    /// The account this session signs for, 32-byte hex.
    #[serde(with = "ser::hex_str")]
    pub account: [u8; 32],
    /// The host's clock reading at open.
    pub now: Time,
    /// Permit loopback/private relay endpoints (tests and local relays).
    pub allow_loopback: bool,
    /// Optional NIP-89 client tag on `KeyPackage` publications.
    pub client_name: Option<String>,
}

/// Lenient version probe decoded before the full config, so a mismatched
/// `protocol_version` answers `protocol_version_mismatch` regardless of the
/// rest of the config's shape.
#[derive(Debug, Deserialize)]
pub(crate) struct ConfigProbe {
    /// The version field, when present and numeric.
    pub(crate) protocol_version: u32,
}

/// A `call` envelope: the host's clock reading plus the request. The
/// request stays a raw [`serde_json::Value`] so its `type` can be checked
/// against [`REQUEST_TYPES`] before the strict decode.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CallEnvelope {
    /// The host's clock reading for this call.
    pub now: Time,
    /// The request object.
    pub request: serde_json::Value,
}

/// The `open` response envelope.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum OpenResponse {
    /// The session is usable.
    Ready {
        /// The session's account, 32-byte hex.
        #[serde(with = "ser::hex_str")]
        account: [u8; 32],
    },
    /// The account must sign the returned proof before the open completes.
    NeedsAccountProof {
        /// The unsigned proof the account signer must sign.
        unsigned: UnsignedEvent,
    },
    /// The open failed.
    Error {
        /// Why it failed.
        error: Box<BridgeError>,
    },
}

/// The `call` response envelope.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Response {
    /// The operation finished.
    Ok {
        /// Its typed result.
        result: Box<OpResult>,
    },
    /// The operation suspended on account-signer operations.
    SignerPending {
        /// Every unanswered signer request, in registration order.
        requests: Vec<SignerRequest>,
    },
    /// The call failed.
    Error {
        /// Why it failed.
        error: Box<BridgeError>,
    },
}

/// The `drain` response envelope.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum DrainResponse {
    /// Session events queued since the last drain.
    Events {
        /// The drained events.
        events: Vec<SessionEvent>,
    },
    /// The drain failed.
    Error {
        /// Why it failed.
        error: Box<BridgeError>,
    },
}

/// Encode a response envelope as JSON bytes. Serialization of the wire
/// types is infallible in practice; a failure still answers the fixed
/// internal-error envelope rather than panicking.
pub(crate) fn encode<T: Serialize>(value: &T) -> Vec<u8> {
    serde_json::to_vec(value).unwrap_or_else(|_| {
        br#"{"type":"error","error":{"kind":"internal","site":"encode"}}"#.to_vec()
    })
}

/// Decode JSON bytes into `T`, mapping every failure to
/// [`BridgeError::InvalidRequest`] with a fixed reason label.
pub(crate) fn decode<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> Result<T, BridgeError> {
    let text = std::str::from_utf8(bytes).map_err(|_| invalid(invalid_reason::UTF8))?;
    let value: serde_json::Value =
        serde_json::from_str(text).map_err(|_| invalid(invalid_reason::JSON))?;
    serde_json::from_value(value).map_err(|error| invalid(reason_label(&error)))
}

/// Build an `invalid_request` error.
pub(crate) fn invalid(reason: &str) -> BridgeError {
    BridgeError::InvalidRequest {
        reason: reason.to_owned(),
    }
}

/// Classify a serde decode error into the fixed reason vocabulary without
/// surfacing the message (which echoes input).
pub(crate) fn reason_label(error: &serde_json::Error) -> &'static str {
    let message = error.to_string();
    if message.starts_with(invalid_reason::HEX) {
        invalid_reason::HEX
    } else if message.starts_with(invalid_reason::NUMBER) {
        invalid_reason::NUMBER
    } else if message.contains("unknown variant")
        || message.starts_with("invalid value")
        || message.starts_with("invalid length")
    {
        invalid_reason::VALUE
    } else {
        invalid_reason::SHAPE
    }
}
