//! Plain public value types: account/event/group/outbound ids and the strict
//! NIP-01 event codec the session exposes at its transport boundary.
//!
//! [`Event::from_json`] is the decoder we own (a fuzz target exercises it in a
//! later milestone). It rejects unknown and duplicate object fields, malformed
//! hex, wrong field lengths, non-string tags and a `kind` that does not fit
//! `u16`; [`Event::verify`] recomputes the NIP-01 id and checks the schnorr
//! signature through the pinned rust-nostr fork.

use core::fmt;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

/// Hex decoding that fails on any non-hex byte or wrong length.
fn hex_decode_exact<const N: usize>(hex: &str) -> Result<[u8; N], EventError> {
    if hex.len() != N * 2 {
        return Err(EventError::Malformed);
    }
    let mut out = [0u8; N];
    hex::decode_to_slice(hex, &mut out).map_err(|_| EventError::Malformed)?;
    Ok(out)
}

fn hex_lower(bytes: &[u8]) -> String {
    hex::encode(bytes)
}

/// The account's x-only secp256k1 public key (Nostr pubkey), 32 bytes.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct AccountId(pub [u8; 32]);

impl AccountId {
    /// Parse from 64 lowercase/uppercase hex characters.
    ///
    /// # Errors
    /// [`EventError::Malformed`] on wrong length or non-hex input.
    pub fn from_hex(hex: &str) -> Result<Self, EventError> {
        Ok(Self(hex_decode_exact::<32>(hex)?))
    }

    /// Lowercase hex form.
    #[must_use]
    pub fn to_hex(self) -> String {
        hex_lower(&self.0)
    }

    /// Raw key bytes.
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

impl fmt::Display for AccountId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.to_hex())
    }
}

impl TryFrom<String> for AccountId {
    type Error = EventError;
    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::from_hex(&value)
    }
}

impl From<AccountId> for String {
    fn from(value: AccountId) -> Self {
        value.to_hex()
    }
}

/// A Nostr event id: the SHA-256 of the NIP-01 serialization, 32 bytes.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct EventId(pub [u8; 32]);

impl EventId {
    /// Parse from 64 hex characters.
    ///
    /// # Errors
    /// [`EventError::Malformed`] on wrong length or non-hex input.
    pub fn from_hex(hex: &str) -> Result<Self, EventError> {
        Ok(Self(hex_decode_exact::<32>(hex)?))
    }

    /// Lowercase hex form.
    #[must_use]
    pub fn to_hex(self) -> String {
        hex_lower(&self.0)
    }

    /// Raw id bytes.
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

impl fmt::Display for EventId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.to_hex())
    }
}

impl TryFrom<String> for EventId {
    type Error = EventError;
    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::from_hex(&value)
    }
}

impl From<EventId> for String {
    fn from(value: EventId) -> Self {
        value.to_hex()
    }
}

/// Opaque MLS group id bytes — never the 32-byte Nostr routing id.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct GroupId(pub Vec<u8>);

impl GroupId {
    /// Raw group id bytes.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

impl fmt::Display for GroupId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&hex_lower(&self.0))
    }
}

/// Identity of one outbound obligation handed to the host transport. Equals
/// the published event's id.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct OutboundId(pub [u8; 32]);

impl OutboundId {
    /// Parse from 64 hex characters.
    ///
    /// # Errors
    /// [`EventError::Malformed`] on wrong length or non-hex input.
    pub fn from_hex(hex: &str) -> Result<Self, EventError> {
        Ok(Self(hex_decode_exact::<32>(hex)?))
    }

    /// Lowercase hex form.
    #[must_use]
    pub fn to_hex(self) -> String {
        hex_lower(&self.0)
    }
}

impl fmt::Display for OutboundId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.to_hex())
    }
}

impl TryFrom<String> for OutboundId {
    type Error = EventError;
    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::from_hex(&value)
    }
}

impl From<OutboundId> for String {
    fn from(value: OutboundId) -> Self {
        value.to_hex()
    }
}

/// Errors the owned NIP-01 codec and verifier produce. Carries no payload
/// bytes.
#[derive(Debug, Error, PartialEq, Eq)]
#[non_exhaustive]
pub enum EventError {
    /// JSON is not a well-formed NIP-01 event object (bad syntax, unknown or
    /// duplicate field, wrong type or length, non-hex string, `kind` outside
    /// `u16`, or a tags element that is not an array of strings).
    #[error("malformed nostr event")]
    Malformed,
    /// The event id does not equal the recomputed NIP-01 hash, or the schnorr
    /// signature does not verify against `pubkey`.
    #[error("invalid nostr event signature")]
    InvalidSignature,
}

/// A fully signed NIP-01 event as the session hands it to the host transport
/// and accepts it back.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Event {
    /// Event id (SHA-256 of the NIP-01 serialization).
    pub id: EventId,
    /// Signer's account key.
    pub pubkey: AccountId,
    /// Unix seconds.
    pub created_at: u64,
    /// Nostr kind.
    pub kind: u16,
    /// Tag list: each entry is an array of strings.
    pub tags: Vec<Vec<String>>,
    /// Content string.
    pub content: String,
    /// BIP-340 schnorr signature over `id`.
    pub sig: [u8; 64],
}

/// Mirror of [`Event`] for serde with strict field handling.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct EventJson {
    id: String,
    pubkey: String,
    created_at: u64,
    kind: u16,
    tags: Vec<Vec<String>>,
    content: String,
    sig: String,
}

impl Event {
    /// Strictly decode a NIP-01 event object.
    ///
    /// # Errors
    /// [`EventError::Malformed`] for any structural, field-name, hex or
    /// length violation. The signature is NOT checked; callers that admit the
    /// event to the engine must also run [`Event::verify`].
    pub fn from_json(json: &str) -> Result<Self, EventError> {
        let raw: EventJson = serde_json::from_str(json).map_err(|_| EventError::Malformed)?;
        Ok(Self {
            id: EventId(hex_decode_exact::<32>(&raw.id)?),
            pubkey: AccountId(hex_decode_exact::<32>(&raw.pubkey)?),
            created_at: raw.created_at,
            kind: raw.kind,
            tags: raw.tags,
            content: raw.content,
            sig: hex_decode_exact::<64>(&raw.sig)?,
        })
    }

    /// NIP-01 object JSON with canonical field order.
    #[must_use]
    pub fn to_json(&self) -> String {
        // Field order is fixed by construction; serde_json never fails on
        // string/integer data, so a manual layout keeps the output stable.
        let mut out = String::with_capacity(self.content.len() + 256);
        out.push_str("{\"id\":\"");
        out.push_str(&self.id.to_hex());
        out.push_str("\",\"pubkey\":\"");
        out.push_str(&self.pubkey.to_hex());
        out.push_str("\",\"created_at\":");
        out.push_str(&self.created_at.to_string());
        out.push_str(",\"kind\":");
        out.push_str(&self.kind.to_string());
        out.push_str(",\"tags\":");
        out.push_str(&tags_json(&self.tags));
        out.push_str(",\"content\":");
        out.push_str(&json_string(&self.content));
        out.push_str(",\"sig\":\"");
        out.push_str(&hex_lower(&self.sig));
        out.push_str("\"}");
        out
    }

    /// The NIP-01 id commitment: `[0, pubkey, created_at, kind, tags, content]`.
    #[must_use]
    pub fn compute_id(&self) -> EventId {
        unsigned_id(
            &self.pubkey,
            self.created_at,
            self.kind,
            &self.tags,
            &self.content,
        )
    }

    /// Convert to the pinned rust-nostr `nostr::prelude::Event` for peeler/signature
    /// interop. `None` only when a field cannot round-trip into the fork's
    /// types (unreachable for a structurally valid event).
    pub(crate) fn to_nostr_event(&self) -> Option<nostr::prelude::Event> {
        nostr::prelude::Event::from_json(self.to_json()).ok()
    }

    /// Build from a nostr event produced inside this crate or the peeler.
    pub(crate) fn from_nostr_event(event: &nostr::prelude::Event) -> Self {
        Self {
            id: EventId(event.id.to_bytes()),
            pubkey: AccountId(event.pubkey.to_bytes()),
            created_at: event.created_at.as_secs(),
            kind: event.kind.as_u16(),
            tags: event
                .tags
                .iter()
                .map(|tag| {
                    tag.as_slice()
                        .iter()
                        .map(ToString::to_string)
                        .collect::<Vec<String>>()
                })
                .collect(),
            content: event.content.clone(),
            sig: event.sig.to_bytes(),
        }
    }

    /// Recompute the id and verify the schnorr signature.
    ///
    /// # Errors
    /// [`EventError::InvalidSignature`] on either check; malformed nostr
    /// types map to the same error because the structural checks already ran.
    pub fn verify(&self) -> Result<(), EventError> {
        if self.compute_id() != self.id {
            return Err(EventError::InvalidSignature);
        }
        let event = self.to_nostr_event().ok_or(EventError::InvalidSignature)?;
        event.verify().map_err(|_| EventError::InvalidSignature)
    }
}

/// JSON-encode one tag list.
fn tags_json(tags: &[Vec<String>]) -> String {
    let mut out = String::from("[");
    for (i, tag) in tags.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        out.push('[');
        for (j, item) in tag.iter().enumerate() {
            if j > 0 {
                out.push(',');
            }
            out.push_str(&json_string(item));
        }
        out.push(']');
    }
    out.push(']');
    out
}

/// JSON-encode a string via serde (exact escaping rules).
fn json_string(value: &str) -> String {
    // serde_json string encoding is infallible; keep a defensive path so no
    // panicking unwrap exists in library code.
    serde_json::to_string(value).unwrap_or_else(|_| "\"\"".to_owned())
}

/// The unsigned half of a NIP-01 event: everything except `id` and `sig`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UnsignedEvent {
    /// Signer's account key.
    pub pubkey: AccountId,
    /// Unix seconds.
    pub created_at: u64,
    /// Nostr kind.
    pub kind: u16,
    /// Tag list.
    pub tags: Vec<Vec<String>>,
    /// Content string.
    pub content: String,
}

impl UnsignedEvent {
    /// The id a signature would commit to.
    #[must_use]
    pub fn id(&self) -> EventId {
        unsigned_id(
            &self.pubkey,
            self.created_at,
            self.kind,
            &self.tags,
            &self.content,
        )
    }

    /// JSON of the unsigned event (no `id`/`sig` keys).
    #[must_use]
    pub fn to_json(&self) -> String {
        let mut out = String::with_capacity(self.content.len() + 200);
        out.push_str("{\"pubkey\":\"");
        out.push_str(&self.pubkey.to_hex());
        out.push_str("\",\"created_at\":");
        out.push_str(&self.created_at.to_string());
        out.push_str(",\"kind\":");
        out.push_str(&self.kind.to_string());
        out.push_str(",\"tags\":");
        out.push_str(&tags_json(&self.tags));
        out.push_str(",\"content\":");
        out.push_str(&json_string(&self.content));
        out.push('}');
        out
    }

    /// The nostr fork's unsigned event, for signers that consume it.
    #[cfg(feature = "testing")]
    pub(crate) fn to_nostr_unsigned(&self) -> Option<nostr::prelude::UnsignedEvent> {
        let pubkey = nostr::prelude::PublicKey::from_slice(&self.pubkey.0).ok()?;
        let tags = self
            .tags
            .iter()
            .map(|tag| nostr::prelude::Tag::parse(tag.clone()))
            .collect::<Result<Vec<_>, _>>()
            .ok()?;
        Some(nostr::prelude::UnsignedEvent::new(
            pubkey,
            nostr::prelude::Timestamp::from_secs(self.created_at),
            nostr::prelude::Kind::from(self.kind),
            tags,
            &self.content,
        ))
    }

    /// Build from a nostr unsigned event handed to the signer adapter.
    pub(crate) fn from_nostr_unsigned(unsigned: &nostr::prelude::UnsignedEvent) -> Self {
        Self {
            pubkey: AccountId(unsigned.pubkey.to_bytes()),
            created_at: unsigned.created_at.as_secs(),
            kind: unsigned.kind.as_u16(),
            tags: unsigned
                .tags
                .iter()
                .map(|tag| {
                    tag.as_slice()
                        .iter()
                        .map(ToString::to_string)
                        .collect::<Vec<String>>()
                })
                .collect(),
            content: unsigned.content.clone(),
        }
    }
}

/// NIP-01 id: SHA-256 of the canonical serialization of
/// `[0, pubkey, created_at, kind, tags, content]`.
fn unsigned_id(
    pubkey: &AccountId,
    created_at: u64,
    kind: u16,
    tags: &[Vec<String>],
    content: &str,
) -> EventId {
    let mut serialization = String::with_capacity(content.len() + 128);
    serialization.push_str("[0,\"");
    serialization.push_str(&pubkey.to_hex());
    serialization.push_str("\",");
    serialization.push_str(&created_at.to_string());
    serialization.push(',');
    serialization.push_str(&kind.to_string());
    serialization.push(',');
    serialization.push_str(&tags_json(tags));
    serialization.push(',');
    serialization.push_str(&json_string(content));
    serialization.push(']');
    let digest = Sha256::digest(serialization.as_bytes());
    let mut id = [0u8; 32];
    id.copy_from_slice(&digest);
    EventId(id)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn signed_event_json() -> String {
        // A real NIP-01 event (kind 1 note) generated for this test.
        let unsigned = UnsignedEvent {
            pubkey: AccountId::from_hex(
                "82341f882b6eabcd2ba7f1ef90aad961cf074af15b9ef44a09f9d2a8fbfbe6a2",
            )
            .expect("hex"),
            created_at: 1_700_000_000,
            kind: 1,
            tags: vec![vec!["e".to_owned(), "aa".repeat(32)]],
            content: "hello".to_owned(),
        };
        let id = unsigned.id();
        // Not a real signature, but structurally valid hex; verifies the
        // codec, not the crypto (that path is exercised by flow tests).
        format!(
            "{{\"id\":\"{id}\",\"pubkey\":\"{}\",\"created_at\":1700000000,\"kind\":1,\
             \"tags\":[[\"e\",\"{}\"]],\"content\":\"hello\",\"sig\":\"{}\"}}",
            unsigned.pubkey,
            "aa".repeat(32),
            "11".repeat(64),
        )
    }

    #[test]
    fn event_json_round_trip() {
        let event = Event::from_json(&signed_event_json()).expect("decodes");
        let again = Event::from_json(&event.to_json()).expect("re-decodes");
        assert_eq!(event, again);
    }

    #[test]
    fn rejects_unknown_field() {
        let mut json = signed_event_json();
        json = json.replace("\"sig\":", "\"extra\":1,\"sig\":");
        assert_eq!(
            Event::from_json(&json),
            Err(EventError::Malformed),
            "unknown field must be rejected"
        );
    }

    #[test]
    fn rejects_duplicate_field() {
        let json = signed_event_json().replace("\"id\":", "\"kind\":1,\"id\":");
        assert_eq!(
            Event::from_json(&json),
            Err(EventError::Malformed),
            "duplicate field must be rejected"
        );
    }

    #[test]
    fn rejects_bad_hex_and_kind() {
        let json = signed_event_json().replace("\"kind\":1", "\"kind\":70000");
        assert_eq!(Event::from_json(&json), Err(EventError::Malformed));
        let bad = json.replace("\"id\":\"", "\"id\":\"zz");
        assert_eq!(Event::from_json(&bad), Err(EventError::Malformed));
    }

    #[test]
    fn rejects_truncated_sig() {
        let json = signed_event_json().replace(&"11".repeat(64), &"11".repeat(63));
        assert_eq!(Event::from_json(&json), Err(EventError::Malformed));
    }

    #[test]
    fn verify_fails_on_wrong_id() {
        let mut event = Event::from_json(&signed_event_json()).expect("decodes");
        event.created_at = event.created_at.saturating_add(1);
        assert_eq!(event.verify(), Err(EventError::InvalidSignature));
    }
}
