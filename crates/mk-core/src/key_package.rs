//! Account publications the engine does not own: kind 30443 `KeyPackages` and
//! the kind 10050 inbox relay list, plus [`key_package_info`], the parser the
//! invite path uses on peer `KeyPackage` events.
//!
//! The 30443 shape follows `marmot/transports/nostr.md` ("`KeyPackage`
//! publication"): `content` is base64 `MLSMessage` (the `mls_key_package`), the
//! `d` tag carries a stable random 32-byte lowercase-hex slot id generated
//! once per device (persisted in a session meta row), and the capability
//! tags are emitted exactly as the engine's `KeyPackage` metadata reports
//! them — single `mls_extensions`/`mls_proposals`/`app_components` tags with
//! `0x%04x` values, never duplicated.

use base64::Engine as _;
use base64::prelude::BASE64_STANDARD;
use thiserror::Error;

use crate::types::{Event, EventError, UnsignedEvent};

/// Nostr kind of a Marmot `KeyPackage` event.
pub const KIND_KEY_PACKAGE: u16 = 30443;

/// Nostr kind of the Marmot inbox relay list.
pub const KIND_INBOX_RELAYS: u16 = 10050;

/// Largest accepted `client` tag value, in bytes (NIP-89 client name).
pub const CLIENT_NAME_MAX_BYTES: usize = 64;

const D_TAG: &str = "d";
const IDENTITY_TAG: &str = "i";
const CLIENT_TAG: &str = "client";
const MLS_PROTOCOL_VERSION_TAG: &str = "mls_protocol_version";
const MLS_CIPHERSUITE_TAG: &str = "mls_ciphersuite";
const MLS_EXTENSIONS_TAG: &str = "mls_extensions";
const MLS_PROPOSALS_TAG: &str = "mls_proposals";
const APP_COMPONENTS_TAG: &str = "app_components";

/// `client` tag name the session emits when configured.
pub const CLIENT_TAG_NAME: &str = CLIENT_TAG;

/// Whether `name` is usable as a `KeyPackage` `client` tag value.
#[must_use]
pub fn is_valid_client_name(name: &str) -> bool {
    (1..=CLIENT_NAME_MAX_BYTES).contains(&name.len())
}

/// Errors [`key_package_info`] reports. Carries no payload or tag values.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum KeyPackageError {
    /// The event is not a well-formed kind-30443 publication (wrong kind,
    /// malformed signature, undecodable content or metadata).
    #[error("invalid key package event")]
    Invalid,
    /// A tag required exactly once is absent or duplicated.
    #[error("key package event has a missing or duplicated tag")]
    MalformedTags,
    /// The `i` tag does not match the `KeyPackageRef` of the decoded package.
    #[error("key package identity tag mismatch")]
    RefMismatch,
}

/// What [`key_package_info`] extracts from a kind-30443 event — the invite
/// input type.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct KeyPackageInfo {
    /// Raw `KeyPackage` bytes (the `MLSMessage` payload).
    pub key_package: Vec<u8>,
    /// `KeyPackageRef` (hex) as asserted by the `i` tag and verified against
    /// the decoded package.
    pub key_package_ref: String,
    /// The event id that published it (becomes the Welcome `e` tag on
    /// invite).
    pub event_id: crate::types::EventId,
    /// The publisher's account.
    pub account: crate::types::AccountId,
}

fn single_tag<'a>(event: &'a Event, name: &str) -> Result<Option<&'a str>, KeyPackageError> {
    let mut found = None;
    for tag in &event.tags {
        let Some((tag_name, value)) = tag.split_first() else {
            continue;
        };
        if tag_name != name {
            continue;
        }
        if found.is_some() || tag.len() != 2 {
            return Err(KeyPackageError::MalformedTags);
        }
        let Some(value) = value.first() else {
            return Err(KeyPackageError::MalformedTags);
        };
        found = Some(value.as_str());
    }
    Ok(found)
}

fn multi_tag_values<'a>(
    event: &'a Event,
    name: &str,
) -> Result<Option<&'a [String]>, KeyPackageError> {
    let mut found = None;
    for tag in &event.tags {
        let Some((tag_name, rest)) = tag.split_first() else {
            continue;
        };
        if tag_name != name {
            continue;
        }
        if found.is_some() || rest.is_empty() {
            return Err(KeyPackageError::MalformedTags);
        }
        found = Some(rest);
    }
    Ok(found)
}

/// Parse and validate a kind-30443 event (ours or a peer's).
///
/// Checks: kind and signature, exactly-once `d`/`mls_protocol_version`/`i`/
/// `mls_ciphersuite` tags, exactly-once non-empty
/// `mls_extensions`/`mls_proposals`/`app_components` value lists, base64
/// content decoding into a `KeyPackage` the engine can parse, and the `i` tag
/// matching that package's `KeyPackageRef`.
///
/// # Errors
/// [`KeyPackageError`] describing the violated invariant class.
pub fn key_package_info(event: &Event) -> Result<KeyPackageInfo, KeyPackageError> {
    if event.kind != KIND_KEY_PACKAGE {
        return Err(KeyPackageError::Invalid);
    }
    event.verify().map_err(|_| KeyPackageError::Invalid)?;
    if single_tag(event, D_TAG)?.is_none_or(str::is_empty)
        || single_tag(event, MLS_PROTOCOL_VERSION_TAG)? != Some("1.0")
        || single_tag(event, MLS_CIPHERSUITE_TAG)?.is_none_or(str::is_empty)
        || multi_tag_values(event, MLS_EXTENSIONS_TAG)?.is_none()
        || multi_tag_values(event, MLS_PROPOSALS_TAG)?.is_none()
        || multi_tag_values(event, APP_COMPONENTS_TAG)?.is_none()
    {
        return Err(KeyPackageError::MalformedTags);
    }
    let key_package = BASE64_STANDARD
        .decode(event.content.as_bytes())
        .map_err(|_| KeyPackageError::Invalid)?;
    let metadata =
        cgka_engine::key_package::key_package_metadata(&cgka_traits::engine::KeyPackage {
            bytes: key_package.clone(),
            source: None,
            protocol_profile: cgka_traits::group::ProtocolProfile::Current,
        })
        .map_err(|_| KeyPackageError::Invalid)?;
    let asserted_ref = single_tag(event, IDENTITY_TAG)?.ok_or(KeyPackageError::MalformedTags)?;
    if asserted_ref != metadata.key_package_ref_hex {
        return Err(KeyPackageError::RefMismatch);
    }
    Ok(KeyPackageInfo {
        key_package,
        key_package_ref: metadata.key_package_ref_hex,
        event_id: event.id,
        account: event.pubkey,
    })
}

/// The unsigned 30443 event body for `key_package` (before signing).
///
/// `created_at` comes from the session clock; `slot_id` is the stable
/// per-device `d` value; `client_name` must satisfy
/// [`is_valid_client_name`] (validated by the caller).
pub(crate) fn key_package_event(
    pubkey: crate::types::AccountId,
    key_package: &[u8],
    slot_id: &str,
    client_name: Option<&str>,
    created_at: u64,
) -> Result<UnsignedEvent, KeyPackageError> {
    let metadata =
        cgka_engine::key_package::key_package_metadata(&cgka_traits::engine::KeyPackage {
            bytes: key_package.to_vec(),
            source: None,
            protocol_profile: cgka_traits::group::ProtocolProfile::Current,
        })
        .map_err(|_| KeyPackageError::Invalid)?;
    // transports/nostr.md (and wn's `require_multi_value_key_package_tag_matches`):
    // the tag advertises only private-use component ids; 0x0001
    // (`app_components` itself) and other reserved ids stay out.
    let private_components = |ids: &[u16]| -> Vec<u16> {
        ids.iter()
            .copied()
            .filter(|id| *id >= cgka_traits::app_components::PRIVATE_USE_APP_COMPONENT_ID_START)
            .collect()
    };
    let hex_id_list =
        |ids: &[u16]| -> Vec<String> { ids.iter().map(|id| format!("0x{id:04x}")).collect() };
    let mut tags = vec![
        vec![D_TAG.to_owned(), slot_id.to_owned()],
        vec![MLS_PROTOCOL_VERSION_TAG.to_owned(), "1.0".to_owned()],
        vec![IDENTITY_TAG.to_owned(), metadata.key_package_ref_hex],
        vec![
            MLS_CIPHERSUITE_TAG.to_owned(),
            format!("0x{:04x}", metadata.ciphersuite),
        ],
        std::iter::once(MLS_EXTENSIONS_TAG.to_owned())
            .chain(hex_id_list(&metadata.mls_extensions))
            .collect(),
        std::iter::once(MLS_PROPOSALS_TAG.to_owned())
            .chain(hex_id_list(&metadata.mls_proposals))
            .collect(),
        std::iter::once(APP_COMPONENTS_TAG.to_owned())
            .chain(hex_id_list(&private_components(&metadata.app_components)))
            .collect(),
    ];
    if let Some(name) = client_name {
        tags.push(vec![CLIENT_TAG.to_owned(), name.to_owned()]);
    }
    Ok(UnsignedEvent {
        pubkey,
        created_at,
        kind: KIND_KEY_PACKAGE,
        tags,
        content: BASE64_STANDARD.encode(key_package),
    })
}

/// The unsigned 10050 inbox-relay-list event body: one `relay` tag per URL,
/// empty content.
pub(crate) fn inbox_relays_event(
    pubkey: crate::types::AccountId,
    relays: &[String],
    created_at: u64,
) -> UnsignedEvent {
    UnsignedEvent {
        pubkey,
        created_at,
        kind: KIND_INBOX_RELAYS,
        tags: relays
            .iter()
            .map(|relay| vec!["relay".to_owned(), relay.clone()])
            .collect(),
        content: String::new(),
    }
}

impl From<EventError> for KeyPackageError {
    fn from(_: EventError) -> Self {
        Self::Invalid
    }
}
