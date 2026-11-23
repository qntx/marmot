//! The typed wire error vocabulary.
//!
//! Every response envelope can carry a [`BridgeError`]. Error `Display`
//! output and serialized forms contain only fixed labels: no account ids,
//! group ids, message ids, relay URLs, public keys, payloads, key material
//! or serde messages ever appear here.

use std::fmt;

use mk_core::{
    OpenError as StoreOpenError, ProofError, SessionError, SessionOpenError, SignerError,
    media::MediaError,
};
use serde::{Deserialize, Serialize};

use crate::protocol::ser::u64_str;

/// The fixed decode-failure reason labels.
pub(crate) mod invalid_reason {
    /// The payload is not valid UTF-8.
    pub(crate) const UTF8: &str = "utf8";
    /// The payload is not JSON.
    pub(crate) const JSON: &str = "json";
    /// The JSON shape does not match the type (fields, types, structure).
    pub(crate) const SHAPE: &str = "shape";
    /// A hex string field is malformed.
    pub(crate) const HEX: &str = "hex";
    /// A number or decimal-string field is malformed.
    pub(crate) const NUMBER: &str = "number";
    /// A value is outside its domain (variant, enum, typed reply).
    pub(crate) const VALUE: &str = "value";
}

/// One error a bridge response can carry. `#[non_exhaustive]`: the wire
/// vocabulary may grow; unknown `kind` values fail the strict decode.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum BridgeError {
    /// The request bytes could not be decoded; `reason` is one of
    /// `"utf8"`, `"json"`, `"shape"`, `"hex"`, `"number"`, `"value"`.
    InvalidRequest {
        /// Fixed reason label.
        reason: String,
    },
    /// The request `type` is not in `REQUEST_TYPES`.
    Unsupported,
    /// The request exceeds [`crate::MAX_REQUEST_BYTES`].
    TooLarge {
        /// The size cap in bytes, decimal string.
        #[serde(with = "u64_str")]
        limit: u64,
    },
    /// The config's `protocol_version` is not this bridge's.
    ProtocolVersionMismatch {
        /// The version this build speaks.
        expected: u32,
        /// The version the config carried.
        got: u32,
    },
    /// `now.monotonic_ms` is lower than the previous reading.
    ClockRegressed,
    /// A call arrived before `open`.
    NotOpen,
    /// `open` on a bridge that already has a session or terminal state.
    AlreadyOpen,
    /// The bridge is waiting for `submit_proof`; nothing else is allowed.
    AwaitingProof,
    /// An operation is suspended on signer replies; only `signer_reply`
    /// and `ack_durable` are allowed.
    Busy,
    /// The bridge was closed.
    Closed,
    /// The bridge is poisoned (host failure or internal invariant break).
    Poisoned,
    /// [`crate::Host::load`] or [`crate::Host::commit`] returned
    /// [`crate::HostError`]; the session is over.
    HostFailed,
    /// [`crate::Host::load`] bytes failed the rows codec; `open` may be
    /// retried (the bridge stays fresh).
    HostRowsInvalid,
    /// `ack_durable` names a sequence beyond the last committed batch.
    UnknownSeq,
    /// `signer_reply` arrived while no operation was suspended.
    NoPendingSigner,
    /// A reply names a signer request id the bridge never asked for, or
    /// one already answered.
    UnknownSignerRequest,
    /// `send_media`/`discard_attachment` names a handle the table does
    /// not hold.
    UnknownAttachment,
    /// The attachment-handle table is full.
    AttachmentLimit,
    /// `open` failed. `reason` is `"storage"`, `"engine"` or
    /// `"invalid_config"`; `detail` is a fixed label.
    OpenFailed {
        /// Fixed reason label.
        reason: String,
        /// Fixed detail label.
        detail: String,
    },
    /// The account proof was rejected; the bridge stays awaiting proof.
    /// `reason` is `"wrong_account"`, `"wrong_event"`,
    /// `"invalid_signature"`, `"storage"` or `"engine"`.
    ProofRejected {
        /// Fixed reason label.
        reason: String,
    },
    /// An internal invariant broke; `site` is a fixed site label.
    Internal {
        /// Fixed site label.
        site: String,
    },
    /// Durable storage failed.
    Storage,
    /// An engine error outside the actionable set; `detail` is MDK's
    /// privacy-safe kind string.
    Engine {
        /// MDK's privacy-safe error kind.
        detail: String,
    },
    /// The account signer failed; `reason` is `"refused"` or `"failed"`.
    Signer {
        /// Fixed reason label.
        reason: String,
    },
    /// The caller supplied an event that is not a valid signed event.
    InvalidTransportEvent,
    /// The engine refused a non-default ciphersuite.
    UnsupportedCiphersuite {
        /// Offered ciphersuite.
        got: u16,
        /// Required ciphersuite.
        required: u16,
    },
    /// A caller argument is invalid; `field` is a fixed field label.
    InvalidInput {
        /// Fixed field label.
        field: String,
    },
    /// The group is not known to this device.
    UnknownGroup,
    /// The account is not a member of the group.
    NotAMember,
    /// The operation needs group admin rights.
    NotGroupAdmin,
    /// The last active admin cannot leave or be removed.
    AdminDepletion,
    /// A leave was already requested for this group.
    LeaveAlreadyRequested,
    /// A member's capabilities do not cover the group's required set.
    MissingRequiredCapabilities,
    /// The group is busy; retry after `tick`.
    GroupBusy,
    /// An own-commit confirm collided with a retained checkpoint.
    ConfirmConflict,
    /// An outbound id no live obligation matches.
    UnknownOutbound,
    /// The account's `KeyPackage` slot is withdrawn: this device signed
    /// out and never publishes `KeyPackage` artifacts again.
    SignedOut,
    /// A relay URL failed the profile or safety checks.
    InvalidRelayUrl,
    /// The app message touches a reserved event surface.
    ReservedAppEvent,
    /// The group does not require the `0x800c` lifecycle component.
    DisbandingNotEnabled,
    /// A disband request or disband candidate is in progress for the group.
    GroupDisbanding,
    /// The group's send pacing refused the send; retry after the clock
    /// advances.
    SendRateExceeded,
    /// A `confirm_rejoin` token no longer matches the stored offer —
    /// the offer rotated since it was read.
    RejoinTokenStale,
    /// The group carries no encrypted-media component.
    MediaNotEnabled,
    /// The pinned media epoch no longer matches the group's epoch.
    MediaEpochStale,
    /// The group's epoch is unsettled.
    MediaEpochUnsettled,
    /// No media-secret row is stored for the message.
    MediaSecretUnavailable,
    /// The media layer refused; `reason` is a `snake_case` `MediaError`
    /// label.
    Media {
        /// Fixed reason label.
        reason: String,
    },
}

impl fmt::Display for BridgeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidRequest { reason } => write!(f, "invalid request: {reason}"),
            Self::Unsupported => f.write_str("unsupported request type"),
            Self::TooLarge { limit } => write!(f, "request too large (limit {limit})"),
            Self::ProtocolVersionMismatch { expected, got } => {
                write!(
                    f,
                    "protocol version mismatch: expected {expected}, got {got}"
                )
            }
            Self::ClockRegressed => f.write_str("clock regressed"),
            Self::NotOpen => f.write_str("bridge not open"),
            Self::AlreadyOpen => f.write_str("bridge already open"),
            Self::AwaitingProof => f.write_str("awaiting account proof"),
            Self::Busy => f.write_str("operation suspended"),
            Self::Closed => f.write_str("bridge closed"),
            Self::Poisoned => f.write_str("bridge poisoned"),
            Self::HostFailed => f.write_str("host storage failed"),
            Self::HostRowsInvalid => f.write_str("host rows invalid"),
            Self::UnknownSeq => f.write_str("unknown commit sequence"),
            Self::NoPendingSigner => f.write_str("no pending signer request"),
            Self::UnknownSignerRequest => f.write_str("unknown signer request"),
            Self::UnknownAttachment => f.write_str("unknown attachment handle"),
            Self::AttachmentLimit => f.write_str("attachment handle limit"),
            Self::OpenFailed { reason, detail } => {
                write!(f, "open failed: {reason}: {detail}")
            }
            Self::ProofRejected { reason } => write!(f, "account proof rejected: {reason}"),
            Self::Internal { site } => write!(f, "internal: {site}"),
            Self::Storage => f.write_str("storage failed"),
            Self::Engine { detail } => write!(f, "engine failed: {detail}"),
            Self::Signer { reason } => write!(f, "signer failed: {reason}"),
            Self::InvalidTransportEvent => f.write_str("invalid transport event"),
            Self::UnsupportedCiphersuite { got, required } => {
                write!(f, "unsupported ciphersuite: got {got}, need {required}")
            }
            Self::InvalidInput { field } => write!(f, "invalid input: {field}"),
            Self::UnknownGroup => f.write_str("unknown group"),
            Self::NotAMember => f.write_str("not a member"),
            Self::NotGroupAdmin => f.write_str("not a group admin"),
            Self::AdminDepletion => f.write_str("last active admin cannot leave"),
            Self::LeaveAlreadyRequested => f.write_str("leave already requested"),
            Self::MissingRequiredCapabilities => f.write_str("missing required capabilities"),
            Self::GroupBusy => f.write_str("group unsettled"),
            Self::ConfirmConflict => f.write_str("own-commit confirm conflict"),
            Self::UnknownOutbound => f.write_str("unknown outbound"),
            Self::SignedOut => f.write_str("signed out"),
            Self::InvalidRelayUrl => f.write_str("invalid relay url"),
            Self::ReservedAppEvent => f.write_str("reserved app event surface"),
            Self::DisbandingNotEnabled => f.write_str("disbanding not enabled"),
            Self::GroupDisbanding => f.write_str("group disbanding in progress"),
            Self::SendRateExceeded => f.write_str("send rate exceeded"),
            Self::RejoinTokenStale => f.write_str("rejoin token stale"),
            Self::MediaNotEnabled => f.write_str("encrypted media not enabled"),
            Self::MediaEpochStale => f.write_str("media epoch stale"),
            Self::MediaEpochUnsettled => f.write_str("media epoch unsettled"),
            Self::MediaSecretUnavailable => f.write_str("media secret unavailable"),
            Self::Media { reason } => write!(f, "media: {reason}"),
        }
    }
}

impl std::error::Error for BridgeError {}

/// Serialize `{"type":"error","error":{...}}`. Public so the wasm shell can
/// emit the same envelope after a caught panic.
#[must_use]
pub fn encode_error(error: &BridgeError) -> Vec<u8> {
    let response = crate::protocol::Response::Error {
        error: Box::new(error.clone()),
    };
    crate::protocol::encode(&response)
}

/// Map a [`SessionError`] onto the flattened wire vocabulary.
pub(crate) fn session_error(error: &SessionError) -> BridgeError {
    match error {
        SessionError::Internal(site) => BridgeError::Internal {
            site: (*site).to_owned(),
        },
        SessionError::Storage(_) => BridgeError::Storage,
        SessionError::Engine { kind } => BridgeError::Engine {
            detail: (*kind).to_owned(),
        },
        SessionError::Signer(error) => signer_error(error),
        SessionError::InvalidTransportEvent => BridgeError::InvalidTransportEvent,
        SessionError::UnsupportedCiphersuite { got, required } => {
            BridgeError::UnsupportedCiphersuite {
                got: *got,
                required: *required,
            }
        }
        SessionError::InvalidInput(field) => BridgeError::InvalidInput {
            field: (*field).to_owned(),
        },
        SessionError::UnknownGroup => BridgeError::UnknownGroup,
        SessionError::NotAMember => BridgeError::NotAMember,
        SessionError::NotGroupAdmin => BridgeError::NotGroupAdmin,
        SessionError::AdminDepletion => BridgeError::AdminDepletion,
        SessionError::LeaveAlreadyRequested => BridgeError::LeaveAlreadyRequested,
        SessionError::MissingRequiredCapabilities => BridgeError::MissingRequiredCapabilities,
        SessionError::GroupBusy => BridgeError::GroupBusy,
        SessionError::ConfirmConflict => BridgeError::ConfirmConflict,
        SessionError::UnknownOutbound => BridgeError::UnknownOutbound,
        SessionError::SignedOut => BridgeError::SignedOut,
        SessionError::InvalidRelayUrl => BridgeError::InvalidRelayUrl,
        SessionError::ReservedAppEvent => BridgeError::ReservedAppEvent,
        SessionError::DisbandingNotEnabled => BridgeError::DisbandingNotEnabled,
        SessionError::GroupDisbanding => BridgeError::GroupDisbanding,
        SessionError::SendRateExceeded => BridgeError::SendRateExceeded,
        SessionError::RejoinTokenStale => BridgeError::RejoinTokenStale,
        SessionError::MediaNotEnabled => BridgeError::MediaNotEnabled,
        SessionError::MediaEpochStale => BridgeError::MediaEpochStale,
        SessionError::MediaEpochUnsettled => BridgeError::MediaEpochUnsettled,
        SessionError::MediaSecretUnavailable => BridgeError::MediaSecretUnavailable,
        SessionError::Media(error) => media_error(*error),
        // `SessionError` is `#[non_exhaustive]`; an unknown future variant
        // surfaces as a fixed site label rather than a guessed category.
        _ => BridgeError::Internal {
            site: "unmapped_session_error".to_owned(),
        },
    }
}

fn signer_error(error: &SignerError) -> BridgeError {
    match error {
        SignerError::Refused => BridgeError::Signer {
            reason: "refused".to_owned(),
        },
        // `SignerError` is `#[non_exhaustive]`; any present or future
        // failure kind that is not a refusal is a generic failure.
        SignerError::Failed(_) | _ => BridgeError::Signer {
            reason: "failed".to_owned(),
        },
    }
}

fn media_error(error: MediaError) -> BridgeError {
    let reason = match error {
        MediaError::InvalidMediaType => "invalid_media_type",
        MediaError::InvalidFilename => "invalid_filename",
        MediaError::CiphertextHashMismatch => "ciphertext_hash_mismatch",
        MediaError::DecryptionFailed => "decryption_failed",
        MediaError::PlaintextHashMismatch => "plaintext_hash_mismatch",
        MediaError::LocatorNotAllowedByPolicy => "locator_not_allowed_by_policy",
        MediaError::InvalidLocator => "invalid_locator",
        MediaError::InvalidPolicy => "invalid_policy",
        MediaError::PayloadTooLarge => "payload_too_large",
        MediaError::KeyDerivation => "key_derivation",
        // `MediaError` is `#[non_exhaustive]`; future kinds collapse to a
        // fixed label rather than echoing `Display`.
        _ => "unmapped_media_error",
    };
    BridgeError::Media {
        reason: reason.to_owned(),
    }
}

/// Map a [`SessionOpenError`] onto `open_failed { reason, detail }`.
pub(crate) fn open_error(error: &SessionOpenError) -> BridgeError {
    match error {
        SessionOpenError::Storage(inner) => BridgeError::OpenFailed {
            reason: "storage".to_owned(),
            detail: store_open_error_label(inner).to_owned(),
        },
        SessionOpenError::Engine { kind } => BridgeError::OpenFailed {
            reason: "engine".to_owned(),
            detail: (*kind).to_owned(),
        },
        SessionOpenError::InvalidConfig(label) => BridgeError::OpenFailed {
            reason: "invalid_config".to_owned(),
            detail: (*label).to_owned(),
        },
        // `SessionOpenError` is `#[non_exhaustive]`; unknown future kinds
        // collapse to a fixed label.
        _ => BridgeError::OpenFailed {
            reason: "internal".to_owned(),
            detail: "unmapped_open_error".to_owned(),
        },
    }
}

const fn store_open_error_label(error: &StoreOpenError) -> &'static str {
    match error {
        StoreOpenError::UnsupportedSchemaVersion { .. } => "unsupported_schema_version",
        StoreOpenError::MissingSchemaVersion => "missing_schema_version",
        StoreOpenError::MalformedMeta { .. } => "malformed_meta",
        StoreOpenError::LockPoisoned => "lock_poisoned",
        // `OpenError` is `#[non_exhaustive]`; future kinds collapse to a
        // fixed label.
        _ => "unmapped_store_open_error",
    }
}

/// Map a [`ProofError`] onto `proof_rejected { reason }`.
pub(crate) fn proof_error(error: &ProofError) -> BridgeError {
    let reason = match error {
        ProofError::WrongAccount => "wrong_account",
        ProofError::WrongEvent => "wrong_event",
        ProofError::InvalidSignature => "invalid_signature",
        ProofError::Storage(_) => "storage",
        ProofError::Engine { .. } => "engine",
        // `ProofError` is `#[non_exhaustive]`; future kinds collapse to a
        // fixed label.
        _ => "unmapped_proof_error",
    };
    BridgeError::ProofRejected {
        reason: reason.to_owned(),
    }
}
