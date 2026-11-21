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

use std::collections::BTreeSet;

use base64::Engine as _;
use base64::prelude::BASE64_STANDARD;
use thiserror::Error;

use crate::types::{AccountId, Event, EventError, UnsignedEvent};

/// Nostr kind of a Marmot `KeyPackage` event.
pub const KIND_KEY_PACKAGE: u16 = 30443;

/// Nostr kind of the Marmot inbox relay list.
pub const KIND_INBOX_RELAYS: u16 = 10050;

/// Nostr kind of a deletion request (NIP-09).
pub const KIND_DELETION: u16 = 5;

/// The most key packages one account contributes to one Commit
/// (`foundation/key-packages.md`, "Selection and lifecycle").
pub const MAX_KEY_PACKAGES_PER_ACCOUNT: usize = 20;

/// A key package published no longer than this before selection is fresh.
pub const KEY_PACKAGE_FRESHNESS_SECS: u64 = 30 * 24 * 60 * 60;

/// Future-dating tolerance at selection: a publication newer than
/// `now + KEY_PACKAGE_MAX_FUTURE_SKEW_SECS` is skipped without claiming its
/// slot.
pub const KEY_PACKAGE_MAX_FUTURE_SKEW_SECS: u64 = 5 * 60;

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
    pub account: AccountId,
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
/// `mls_extensions`/`mls_proposals`/`app_components` value lists, non-empty
/// base64 content decoding into a `KeyPackage` the engine can parse, the
/// `mls_ciphersuite` tag equalling the decoded ciphersuite, each id-list tag
/// equalling the decoded advertisement set, the credential identity matching
/// the event author, and the `i` tag matching the package's `KeyPackageRef`.
///
/// # Errors
/// [`KeyPackageError`] describing the violated invariant class.
pub fn key_package_info(event: &Event) -> Result<KeyPackageInfo, KeyPackageError> {
    info_and_metadata(event).map(|(info, _)| info)
}

/// Whether the tag's value list holds each `0x%04x` of `expected_ids`
/// exactly once, in any order, and nothing else.
fn id_list_tag_matches(values: &[String], expected_ids: impl IntoIterator<Item = u16>) -> bool {
    let expected: BTreeSet<String> = expected_ids
        .into_iter()
        .map(|id| format!("0x{id:04x}"))
        .collect();
    values.len() == expected.len() && values.iter().all(|value| expected.contains(value))
}

/// [`key_package_info`] plus the decoded metadata, computed once.
fn info_and_metadata(
    event: &Event,
) -> Result<(KeyPackageInfo, cgka_engine::key_package::KeyPackageMetadata), KeyPackageError> {
    if event.kind != KIND_KEY_PACKAGE {
        return Err(KeyPackageError::Invalid);
    }
    event.verify().map_err(|_| KeyPackageError::Invalid)?;
    let asserted_ref = single_tag(event, IDENTITY_TAG)?;
    let ciphersuite = single_tag(event, MLS_CIPHERSUITE_TAG)?;
    let extensions = multi_tag_values(event, MLS_EXTENSIONS_TAG)?;
    let proposals = multi_tag_values(event, MLS_PROPOSALS_TAG)?;
    let app_components = multi_tag_values(event, APP_COMPONENTS_TAG)?;
    if single_tag(event, D_TAG)?.is_none_or(str::is_empty)
        || single_tag(event, MLS_PROTOCOL_VERSION_TAG)? != Some("1.0")
        || ciphersuite.is_none_or(str::is_empty)
        || asserted_ref.is_none()
        || extensions.is_none()
        || proposals.is_none()
        || app_components.is_none()
    {
        return Err(KeyPackageError::MalformedTags);
    }
    let key_package = BASE64_STANDARD
        .decode(event.content.as_bytes())
        .map_err(|_| KeyPackageError::Invalid)?;
    if key_package.is_empty() {
        return Err(KeyPackageError::Invalid);
    }
    let metadata =
        cgka_engine::key_package::key_package_metadata(&cgka_traits::engine::KeyPackage {
            bytes: key_package.clone(),
            source: None,
            protocol_profile: cgka_traits::group::ProtocolProfile::Current,
        })
        .map_err(|_| KeyPackageError::Invalid)?;
    // Every capability tag must declare exactly what the signed package
    // advertises (`transports/nostr.md`): a mismatch lets two readers
    // disagree on capabilities.
    if ciphersuite != Some(format!("0x{:04x}", metadata.ciphersuite).as_str())
        || !id_list_tag_matches(
            extensions.unwrap_or(&[]),
            metadata.mls_extensions.iter().copied(),
        )
        || !id_list_tag_matches(
            proposals.unwrap_or(&[]),
            metadata.mls_proposals.iter().copied(),
        )
        || !id_list_tag_matches(
            app_components.unwrap_or(&[]),
            metadata.app_components.iter().copied().filter(|id| {
                *id >= cgka_traits::app_components::PRIVATE_USE_APP_COMPONENT_ID_START
            }),
        )
    {
        return Err(KeyPackageError::MalformedTags);
    }
    if metadata.credential_identity_hex != event.pubkey.to_hex() {
        return Err(KeyPackageError::Invalid);
    }
    if asserted_ref != Some(metadata.key_package_ref_hex.as_str()) {
        return Err(KeyPackageError::RefMismatch);
    }
    Ok((
        KeyPackageInfo {
            key_package,
            key_package_ref: metadata.key_package_ref_hex.clone(),
            event_id: event.id,
            account: event.pubkey,
        },
        metadata,
    ))
}

/// The unsigned 30443 event body for `key_package` (before signing).
///
/// `created_at` comes from the session clock; `slot_id` is the stable
/// per-device `d` value; `client_name` must satisfy
/// [`is_valid_client_name`] (validated by the caller).
pub(crate) fn key_package_event(
    pubkey: AccountId,
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
    pubkey: AccountId,
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

/// A validated candidate and what ranks it.
struct RankedKeyPackage<'a> {
    event: &'a Event,
    last_resort: bool,
    signature_key: Vec<u8>,
    key_package_ref: Vec<u8>,
}

/// The value of the event's first `d` tag, `""` when absent.
fn slot_of(event: &Event) -> &str {
    event
        .tags
        .iter()
        .find(|tag| tag.first().is_some_and(|name| name == D_TAG))
        .and_then(|tag| tag.get(1))
        .map_or("", String::as_str)
}

/// Every first value of `name` tags on `event`.
fn tag_first_values<'a>(event: &'a Event, name: &str) -> impl Iterator<Item = &'a str> {
    event
        .tags
        .iter()
        .filter(move |tag| tag.first().is_some_and(|tag_name| tag_name == name))
        .filter_map(|tag| tag.get(1))
        .map(String::as_str)
}

/// Whether a kind-5 request of `account` covers `event`: by its id in an `e`
/// first-value, or — only for requests no older than the event — by the
/// slot's `kind:author:d` address in an `a` first-value.
fn package_is_deleted(account: &AccountId, event: &Event, deletions: &[&Event]) -> bool {
    let account_hex = account.to_hex();
    let event_id = event.id.to_hex();
    let slot = slot_of(event);
    deletions.iter().any(|deletion| {
        tag_first_values(deletion, "e").any(|reference| reference.eq_ignore_ascii_case(&event_id))
            || (event.created_at <= deletion.created_at
                && tag_first_values(deletion, "a").any(|reference| {
                    let mut parts = reference.splitn(3, ':');
                    parts.next().and_then(|kind| kind.parse::<u16>().ok()) == Some(KIND_KEY_PACKAGE)
                        && parts
                            .next()
                            .is_some_and(|author| author.eq_ignore_ascii_case(&account_hex))
                        && parts.next() == Some(slot)
                }))
    })
}

/// Select `account`'s key packages to add, best first.
///
/// The D1 selection of `foundation/key-packages.md` ("Selection and
/// lifecycle") over the account's fetched kind-30443 publications and kind-5
/// deletion requests. Mirrors MDK's `select_key_packages_from_records`;
/// signatures are checked here because callers fetch raw events, not
/// relay-verified records.
///
/// - Each `d` slot's current publication is its newest fresh event. It
///   supersedes the slot even when invalid or deleted: falling back could
///   invite a package whose private material has been retired.
/// - A candidate a deletion request covers, by slot address or event id, is
///   skipped.
/// - Malformed, expired and (with `requirements`) incompatible candidates
///   are rejected before ranking.
/// - Ranking: non-last-resort first, then newest, then the lower
///   `KeyPackageRef`. Of candidates sharing a leaf signature key only the
///   highest-ranked stays: a Commit cannot add two leaves with one key.
/// - Every fresh candidate is selected, at most
///   [`MAX_KEY_PACKAGES_PER_ACCOUNT`]; with none fresh, the single
///   highest-ranked one.
#[must_use]
pub fn select_key_packages(account: AccountId, events: &[Event], now_secs: u64) -> Vec<Event> {
    select_key_packages_with(account, events, now_secs, None)
}

/// [`select_key_packages`] with an engine admission prefilter (group invite
/// requirements); engine types stay out of the public API.
pub(crate) fn select_key_packages_with(
    account: AccountId,
    events: &[Event],
    now_secs: u64,
    requirements: Option<&cgka_engine::key_package::KeyPackageRequirements>,
) -> Vec<Event> {
    let deletions: Vec<&Event> = events
        .iter()
        .filter(|event| {
            event.kind == KIND_DELETION && event.pubkey == account && event.verify().is_ok()
        })
        .collect();
    let mut candidates: Vec<&Event> = events
        .iter()
        .filter(|event| {
            event.kind == KIND_KEY_PACKAGE && event.pubkey == account && event.verify().is_ok()
        })
        .collect();
    candidates.sort_by(|a, b| {
        b.created_at
            .cmp(&a.created_at)
            .then_with(|| a.id.cmp(&b.id))
    });
    let max_created_at = now_secs.saturating_add(KEY_PACKAGE_MAX_FUTURE_SKEW_SECS);
    let mut slots = BTreeSet::new();
    let mut ranked = Vec::new();
    for event in candidates {
        if event.created_at > max_created_at {
            continue;
        }
        let slot = slot_of(event);
        if !slot.is_empty() && !slots.insert(slot.to_owned()) {
            continue;
        }
        if package_is_deleted(&account, event, &deletions) {
            continue;
        }
        let Ok((info, metadata)) = info_and_metadata(event) else {
            continue;
        };
        if let Some(requirements) = requirements
            && requirements
                .validate(&cgka_traits::engine::KeyPackage {
                    bytes: info.key_package,
                    source: None,
                    protocol_profile: cgka_traits::group::ProtocolProfile::Current,
                })
                .is_err()
        {
            continue;
        }
        let Ok(key_package_ref) = hex::decode(&metadata.key_package_ref_hex) else {
            continue;
        };
        ranked.push(RankedKeyPackage {
            event,
            last_resort: metadata.last_resort,
            signature_key: metadata.leaf_signature_key,
            key_package_ref,
        });
    }
    ranked.sort_by(|a, b| {
        a.last_resort
            .cmp(&b.last_resort)
            .then_with(|| b.event.created_at.cmp(&a.event.created_at))
            .then_with(|| a.key_package_ref.cmp(&b.key_package_ref))
    });
    let fresh =
        |event: &Event| event.created_at.saturating_add(KEY_PACKAGE_FRESHNESS_SECS) >= now_secs;
    let has_fresh = ranked.iter().any(|candidate| fresh(candidate.event));
    let mut signature_keys = BTreeSet::new();
    ranked
        .into_iter()
        .filter(|candidate| !has_fresh || fresh(candidate.event))
        .filter(|candidate| signature_keys.insert(candidate.signature_key.clone()))
        .take(if has_fresh {
            MAX_KEY_PACKAGES_PER_ACCOUNT
        } else {
            1
        })
        .map(|candidate| candidate.event.clone())
        .collect()
}

impl From<EventError> for KeyPackageError {
    fn from(_: EventError) -> Self {
        Self::Invalid
    }
}
