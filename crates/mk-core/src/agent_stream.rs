//! Agent text stream QUIC anchors (`0x8006` / extension `0xf2d1`, receive
//! role): the stream policy component and the start/final tag parsing a
//! receive-only member performs.
//!
//! This module is pure: parsing never produces outbound messages or network
//! effects, and this crate has no network dependency at all. `route` and
//! `broker` tags are ignored entirely — the receive role may ignore QUIC
//! candidates (spec `features/agent-text-streams-quic.md`).

use cgka_traits::agent_text_stream::AgentTextStreamQuicPolicyV1;
use cgka_traits::app_components::AGENT_TEXT_STREAM_QUIC_COMPONENT_ID;
use thiserror::Error;

use crate::session::ComponentUpdate;

/// `stream` tag name on stream-start and stream-final events.
const STREAM_TAG: &str = "stream";
/// `stream-type` tag on the start event (must be `text`).
const STREAM_TYPE_TAG: &str = "stream-type";
/// `final-kind` tag on the start event (must be `9`).
const FINAL_KIND_TAG: &str = "final-kind";
/// `parent` tag on the start event (optional prompt message id).
const PARENT_TAG: &str = "parent";
/// `stream-start` tag on the final message (the start event's inner id).
const STREAM_START_TAG: &str = "stream-start";
/// `stream-hash` tag on the final message (transcript hash).
const STREAM_HASH_TAG: &str = "stream-hash";
/// `stream-chunks` tag on the final message (chunk count).
const STREAM_CHUNKS_TAG: &str = "stream-chunks";

/// The role mask of an agent text stream policy. `RECEIVE` is the only role
/// this crate implements; `SEND`/`FANOUT` exist so policies other members
/// wrote still decode.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct StreamRoles(u8);

impl StreamRoles {
    /// Delivered stream events are surfaced to the host.
    pub const RECEIVE: Self = Self(0x01);
    /// May author streams (mk-core never advertises it).
    pub const SEND: Self = Self(0x02);
    /// May broker stream chunks (mk-core never advertises it).
    pub const FANOUT: Self = Self(0x04);

    /// Union of two masks.
    #[must_use]
    pub const fn union(self, other: Self) -> Self {
        Self(self.0 | other.0)
    }

    /// Whether `other`'s roles are all in this mask.
    #[must_use]
    pub const fn contains(self, other: Self) -> bool {
        self.0 & other.0 == other.0
    }
}

/// The `0x8006` agent-text-stream policy, decoded. `padding_bucket_bytes` is
/// reserved in v1: accepted on decode, never surfaced, encoded as zero.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AgentStreamPolicy {
    /// Roles every member must advertise to stay in the group.
    pub required_roles: StreamRoles,
    /// Roles members may hold.
    pub allowed_roles: StreamRoles,
    /// Maximum plaintext bytes per stream frame.
    pub max_plaintext_frame_len: u32,
    /// Replay-protection time-to-live in seconds.
    pub replay_ttl_secs: u32,
}

/// Why a stream policy could not be produced.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Error)]
#[non_exhaustive]
#[error("invalid agent stream policy")]
pub struct AgentStreamPolicyError;

impl AgentStreamPolicy {
    /// The user-to-agent profile: members must hold `receive`; `receive` and
    /// `send` are allowed (mk-core satisfies it through `receive`).
    #[must_use]
    pub const fn user_to_agent_default() -> Self {
        Self {
            required_roles: StreamRoles::RECEIVE,
            allowed_roles: StreamRoles::RECEIVE.union(StreamRoles::SEND),
            max_plaintext_frame_len: 4096,
            replay_ttl_secs: 0,
        }
    }

    /// Decode `0x8006` component bytes (the engine's codec is the spec's).
    /// `None` when the bytes do not decode or fail the spec's validation.
    #[must_use]
    pub fn from_component(bytes: &[u8]) -> Option<Self> {
        let policy = AgentTextStreamQuicPolicyV1::decode_component_state(bytes).ok()?;
        Some(Self {
            required_roles: StreamRoles(policy.required_member_roles),
            allowed_roles: StreamRoles(policy.allowed_member_roles),
            max_plaintext_frame_len: policy.max_plaintext_frame_len,
            replay_ttl_secs: policy.replay_ttl_secs,
        })
    }

    /// Encode this policy as a `0x8006` [`ComponentUpdate`]; the engine's
    /// validation is the spec's.
    ///
    /// # Errors
    /// [`AgentStreamPolicyError`] when the roles/limits violate the
    /// component rules.
    pub fn to_component(self) -> Result<ComponentUpdate, AgentStreamPolicyError> {
        let policy = AgentTextStreamQuicPolicyV1 {
            required_member_roles: self.required_roles.0,
            allowed_member_roles: self.allowed_roles.0,
            max_plaintext_frame_len: self.max_plaintext_frame_len,
            replay_ttl_secs: self.replay_ttl_secs,
            padding_bucket_bytes: 0,
        };
        let data = policy
            .encode_component_state()
            .map_err(|_| AgentStreamPolicyError)?;
        Ok(ComponentUpdate {
            component_id: AGENT_TEXT_STREAM_QUIC_COMPONENT_ID,
            data,
        })
    }
}

/// Why a stream anchor's tags failed the profile.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Error)]
#[non_exhaustive]
pub enum StreamAnchorError {
    /// A required tag is missing or appears more than once.
    #[error("missing or repeated stream anchor tag")]
    MissingOrRepeatedTag,
    /// A `stream`/`stream-start`/`stream-hash`/`parent` value is not 64
    /// lowercase hex digits.
    #[error("malformed stream anchor hex field")]
    MalformedField,
    /// `stream-type` is not `text` or `final-kind` is not `9`.
    #[error("invalid stream anchor value")]
    InvalidValue,
}

/// The parsed anchors of a kind-1200 agent stream start event.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StreamStart {
    /// The 32-byte stream id.
    pub stream_id: [u8; 32],
    /// The prompt message this stream answers, when present.
    pub parent: Option<[u8; 32]>,
}

/// The parsed anchors of a kind-9 stream-final message.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StreamFinal {
    /// The stream this finalizes.
    pub stream_id: [u8; 32],
    /// The kind-1200 start event's inner id.
    pub start_event_id: [u8; 32],
    /// The sender's transcript hash.
    pub transcript_hash: [u8; 32],
    /// How many QUIC chunks the transcript spanned.
    pub chunk_count: u64,
}

/// Every value `name` carries, in tag order (`tag[0] == name` → `tag[1]`).
fn tag_values<'a>(tags: &'a [Vec<String>], name: &str) -> Vec<&'a str> {
    tags.iter()
        .filter(|tag| tag.first().is_some_and(|first| first == name))
        .filter_map(|tag| tag.get(1))
        .map(String::as_str)
        .collect()
}

/// Exactly one value for `name`, decoded as a 32-byte lowercase-hex id.
fn one_hex32(tags: &[Vec<String>], name: &str) -> Result<[u8; 32], StreamAnchorError> {
    let values = tag_values(tags, name);
    let [value] = values[..] else {
        return Err(StreamAnchorError::MissingOrRepeatedTag);
    };
    if value.len() != 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(StreamAnchorError::MalformedField);
    }
    let decoded = hex::decode(value).map_err(|_| StreamAnchorError::MalformedField)?;
    decoded
        .try_into()
        .map_err(|_| StreamAnchorError::MalformedField)
}

/// Exactly one value for `name`, decoded as a 32-byte hex id of any case.
/// Spec constrains the stream anchor ids to lowercase but not `parent`.
fn one_hex32_any_case(tags: &[Vec<String>], name: &str) -> Result<[u8; 32], StreamAnchorError> {
    let values = tag_values(tags, name);
    let [value] = values[..] else {
        return Err(StreamAnchorError::MissingOrRepeatedTag);
    };
    let decoded = hex::decode(value).map_err(|_| StreamAnchorError::MalformedField)?;
    decoded
        .try_into()
        .map_err(|_| StreamAnchorError::MalformedField)
}

/// Exactly one value for `name`, compared against `expected`.
fn one_literal(tags: &[Vec<String>], name: &str, expected: &str) -> Result<(), StreamAnchorError> {
    let values = tag_values(tags, name);
    let [value] = values[..] else {
        return Err(StreamAnchorError::MissingOrRepeatedTag);
    };
    if value != expected {
        return Err(StreamAnchorError::InvalidValue);
    }
    Ok(())
}

/// Parse the anchors of a kind-1200 agent stream start event.
///
/// Exactly one `stream` (64 lowercase hex), exactly one `stream-type` =
/// `text`, exactly one `final-kind` = `9`, at most one `parent` (64 hex,
/// case-insensitive — the spec does not constrain its case).
/// `route`/`broker` and every other tag are ignored.
///
/// # Errors
/// [`StreamAnchorError`] for any deviation; the caller treats a failed start
/// as an ordinary application message.
pub fn parse_stream_start(tags: &[Vec<String>]) -> Result<StreamStart, StreamAnchorError> {
    let stream_id = one_hex32(tags, STREAM_TAG)?;
    one_literal(tags, STREAM_TYPE_TAG, "text")?;
    one_literal(tags, FINAL_KIND_TAG, "9")?;
    let parents = tag_values(tags, PARENT_TAG);
    let parent = match parents[..] {
        [] => None,
        [_] => Some(one_hex32_any_case(tags, PARENT_TAG)?),
        [_, _, ..] => return Err(StreamAnchorError::MissingOrRepeatedTag),
    };
    Ok(StreamStart { stream_id, parent })
}

/// Parse the anchors of a kind-9 stream-final message.
///
/// All four of `stream`/`stream-start`/`stream-hash`/`stream-chunks` exactly
/// once and well-formed. `None` for anything else — the message still
/// delivers as an ordinary message.
#[must_use]
pub fn parse_stream_final(tags: &[Vec<String>]) -> Option<StreamFinal> {
    let stream_id = one_hex32(tags, STREAM_TAG).ok()?;
    let start_event_id = one_hex32(tags, STREAM_START_TAG).ok()?;
    let transcript_hash = one_hex32(tags, STREAM_HASH_TAG).ok()?;
    let values = tag_values(tags, STREAM_CHUNKS_TAG);
    let [value] = values[..] else { return None };
    if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    let chunk_count = value.parse::<u64>().ok()?;
    Some(StreamFinal {
        stream_id,
        start_event_id,
        transcript_hash,
        chunk_count,
    })
}
