//! Invitee `KeyPackage` selection (D1 of `foundation/key-packages.md`,
//! "Selection and lifecycle") over fetched kind-30443 + kind-5 events:
//! `mk_core::key_package::select_key_packages` and the requirements-aware
//! `Session::select_key_packages`. Candidates are real `KeyPackage`
//! publications produced by sessions; mutations are re-signed by the
//! account's `LocalSigner` so signatures verify wherever they should.

#![allow(
    unused_crate_dependencies,
    reason = "every integration-test binary links the full dev-dependency set"
)]
#![allow(
    clippy::unwrap_used,
    clippy::unwrap_in_result,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::missing_docs_in_private_items,
    clippy::tests_outside_test_module,
    clippy::shadow_unrelated,
    clippy::shadow_reuse,
    clippy::doc_markdown,
    clippy::missing_panics_doc,
    clippy::cast_lossless,
    clippy::cast_possible_truncation,
    clippy::str_to_string,
    clippy::needless_pass_by_value,
    clippy::items_after_statements,
    clippy::implicit_hasher,
    clippy::redundant_clone,
    clippy::too_many_lines,
    clippy::excessive_nesting,
    reason = "test code"
)]
mod support;

use std::sync::Arc;

use openmls_rust_crypto as _;
use proptest as _;
use storage_sqlite as _;

use mk_core::key_package::{
    KEY_PACKAGE_FRESHNESS_SECS, KEY_PACKAGE_MAX_FUTURE_SKEW_SECS, KIND_DELETION,
    MAX_KEY_PACKAGES_PER_ACCOUNT, select_key_packages,
};
use mk_core::testing::LocalSigner;
use mk_core::{AccountSigner, CreateGroup, Event, UnsignedEvent};
use support::*;

const RELAY: &str = "wss://r.example";
const DAY: u64 = 24 * 60 * 60;

fn ids(events: &[Event]) -> Vec<String> {
    events.iter().map(|event| event.id.to_hex()).collect()
}

fn slot(event: &Event) -> String {
    event
        .tags
        .iter()
        .find(|tag| tag.first().map(String::as_str) == Some("d"))
        .and_then(|tag| tag.get(1))
        .expect("d tag")
        .clone()
}

fn now(member: &Member) -> u64 {
    member.session.now().unix_ms / 1_000
}

/// Open a fresh device of `signer`'s account and stage+release its
/// `KeyPackage` publication.
async fn device_package(signer: &Arc<LocalSigner>) -> (Member, Event) {
    let mut member = open_member_with(Arc::<LocalSigner>::clone(signer)).await;
    member
        .session
        .publish_key_package(&[RELAY.to_owned()])
        .await
        .expect("publish_key_package");
    if let Some(seq) = member.session.awaiting_durability() {
        member.session.acknowledge_durable(seq);
    }
    let event = member
        .session
        .take_outbound()
        .expect("take_outbound")
        .into_iter()
        .next()
        .expect("key package outbound")
        .event;
    (member, event)
}

/// The same event signed again by `signer` after `edit` mutates the draft.
async fn resigned(
    signer: &LocalSigner,
    event: &Event,
    edit: impl FnOnce(&mut UnsignedEvent),
) -> Event {
    let mut draft = UnsignedEvent {
        pubkey: event.pubkey,
        created_at: event.created_at,
        kind: event.kind,
        tags: event.tags.clone(),
        content: event.content.clone(),
    };
    edit(&mut draft);
    signer.sign_event(draft).await.expect("re-sign")
}

/// A kind-5 deletion request by `signer`.
async fn deletion(signer: &LocalSigner, created_at: u64, tags: Vec<Vec<String>>) -> Event {
    signer
        .sign_event(UnsignedEvent {
            pubkey: signer.account(),
            created_at,
            kind: KIND_DELETION,
            tags,
            content: String::new(),
        })
        .await
        .expect("sign deletion")
}

fn address(kind: u16, author_hex: &str, slot: &str) -> String {
    format!("{kind}:{author_hex}:{slot}")
}

#[tokio::test]
async fn fresh_and_stale() {
    let signer = Arc::new(LocalSigner::new());
    let (member, fresh) = device_package(&signer).await;
    let stale = resigned(&signer, &fresh, |draft| {
        draft.created_at -= KEY_PACKAGE_FRESHNESS_SECS + DAY;
        draft
            .tags
            .retain(|tag| tag.first().map(String::as_str) != Some("d"));
        draft
            .tags
            .insert(0, vec!["d".to_owned(), "stale-slot".to_owned()]);
    })
    .await;
    let now = now(&member);
    let account = signer.account();

    // The fresh one wins; alone the stale one still selects (single best).
    assert_eq!(
        ids(&select_key_packages(
            account,
            &[fresh.clone(), stale.clone()],
            now
        )),
        ids(std::slice::from_ref(&fresh)),
    );
    assert_eq!(
        ids(&select_key_packages(
            account,
            std::slice::from_ref(&stale),
            now
        )),
        ids(std::slice::from_ref(&stale)),
    );
    // Two stales: only the best-ranked single one is kept.
    let older = resigned(&signer, &stale, |draft| {
        draft.created_at -= DAY;
        draft.tags[0] = vec!["d".to_owned(), "older-slot".to_owned()];
    })
    .await;
    assert_eq!(select_key_packages(account, &[stale, older], now).len(), 1,);
}

#[tokio::test]
async fn deletion_by_slot_address() {
    let signer = Arc::new(LocalSigner::new());
    let (member, package) = device_package(&signer).await;
    let now = now(&member);
    let account = signer.account();
    let account_hex = account.to_hex();
    let slot = slot(&package);

    // A deletion naming the slot's address covers it (equal or newer).
    let covers = deletion(
        &signer,
        package.created_at,
        vec![vec!["a".to_owned(), address(30_443, &account_hex, &slot)]],
    )
    .await;
    assert!(
        select_key_packages(account, &[package.clone(), covers], now).is_empty(),
        "a covering deletion selects nothing",
    );

    // A deletion older than the publication does not cover it.
    let older = deletion(
        &signer,
        package.created_at - 60,
        vec![vec!["a".to_owned(), address(30_443, &account_hex, &slot)]],
    )
    .await;
    assert_eq!(
        ids(&select_key_packages(
            account,
            &[package.clone(), older],
            now
        )),
        ids(std::slice::from_ref(&package)),
    );

    // Wrong kind, wrong author or wrong slot: no coverage.
    let other = LocalSigner::new();
    let mut misses = Vec::new();
    for value in [
        address(10_050, &account_hex, &slot),
        address(30_443, &other.account().to_hex(), &slot),
        address(30_443, &account_hex, "other-slot"),
    ] {
        misses.push(
            deletion(
                &signer,
                package.created_at,
                vec![vec!["a".to_owned(), value]],
            )
            .await,
        );
    }
    let mut events = misses;
    events.push(package.clone());
    assert_eq!(
        ids(&select_key_packages(account, &events, now)),
        ids(&[package]),
    );
}

#[tokio::test]
async fn deletion_by_event_id() {
    let signer = Arc::new(LocalSigner::new());
    let (member, package) = device_package(&signer).await;
    let now = now(&member);
    let account = signer.account();

    // `e` coverage ignores the relative ages.
    let covers = deletion(
        &signer,
        package.created_at - 60,
        vec![vec!["e".to_owned(), package.id.to_hex()]],
    )
    .await;
    assert!(
        select_key_packages(account, &[package.clone(), covers], now).is_empty(),
        "a covering deletion selects nothing",
    );

    // Uppercase hex matches too — both `e` ids and `a` authors.
    let upper = deletion(
        &signer,
        package.created_at,
        vec![
            vec!["e".to_owned(), package.id.to_hex().to_uppercase()],
            vec![
                "a".to_owned(),
                address(30_443, &account.to_hex().to_uppercase(), &slot(&package)),
            ],
        ],
    )
    .await;
    assert!(
        select_key_packages(account, &[package, upper], now).is_empty(),
        "uppercase hex coverage selects nothing",
    );
}

#[tokio::test]
async fn same_leaf_key_collapses() {
    let signer = Arc::new(LocalSigner::new());
    let (member, first) = device_package(&signer).await;
    // A second publication of the same device: same slot, same leaf
    // signature key, new `KeyPackageRef`.
    let mut member = member;
    member
        .session
        .publish_key_package(&[RELAY.to_owned()])
        .await
        .expect("publish_key_package");
    if let Some(seq) = member.session.awaiting_durability() {
        member.session.acknowledge_durable(seq);
    }
    let second = member
        .session
        .take_outbound()
        .expect("take_outbound")
        .into_iter()
        .next()
        .expect("key package outbound")
        .event;
    let other_slot = resigned(&signer, &second, |draft| {
        draft.tags[0] = vec!["d".to_owned(), "second-slot".to_owned()];
    })
    .await;
    let now = now(&member);
    let selected = select_key_packages(signer.account(), &[first, other_slot], now);
    assert_eq!(selected.len(), 1, "one leaf key, one selection");
}

#[tokio::test]
async fn fresh_cap() {
    let signer = Arc::new(LocalSigner::new());
    let mut member = None;
    let mut events = Vec::new();
    for _ in 0..=MAX_KEY_PACKAGES_PER_ACCOUNT {
        let (opened, package) = device_package(&signer).await;
        member = Some(opened);
        events.push(package);
    }
    let now = now(&member.expect("one member"));
    let selected = select_key_packages(signer.account(), &events, now);
    assert_eq!(selected.len(), MAX_KEY_PACKAGES_PER_ACCOUNT);
}

#[tokio::test]
async fn invalid_newer_masks_slot() {
    let signer = Arc::new(LocalSigner::new());
    let (member, package) = device_package(&signer).await;
    let now = now(&member);
    let account = signer.account();

    // A newer publication in the same slot that fails validation still
    // supersedes the slot: no fallback to the older valid one.
    let masked = resigned(&signer, &package, |draft| {
        draft.created_at += 60;
        draft.content = "not-base64!!".to_owned();
    })
    .await;
    assert!(
        select_key_packages(account, &[package.clone(), masked], now).is_empty(),
        "a masked slot selects nothing",
    );

    // A slot future-dated beyond the skew is ignored entirely: it neither
    // selects nor masks the older valid publication.
    let future = resigned(&signer, &package, |draft| {
        draft.created_at = now + KEY_PACKAGE_MAX_FUTURE_SKEW_SECS + 1;
        draft.content = "not-base64!!".to_owned();
    })
    .await;
    assert_eq!(
        ids(&select_key_packages(
            account,
            &[package.clone(), future],
            now
        )),
        ids(std::slice::from_ref(&package)),
    );
    // At exactly the skew it is considered (and masks the slot).
    let at_skew = resigned(&signer, &package, |draft| {
        draft.created_at = now + KEY_PACKAGE_MAX_FUTURE_SKEW_SECS;
        draft.content = "not-base64!!".to_owned();
    })
    .await;
    assert!(
        select_key_packages(account, &[package, at_skew], now).is_empty(),
        "at the skew bound the slot stays masked",
    );
}

#[tokio::test]
async fn rejected_candidates() {
    let signer = Arc::new(LocalSigner::new());
    let other = LocalSigner::new();
    let (member, package) = device_package(&signer).await;
    let now = now(&member);
    let account = signer.account();

    // Wrong author: another account's signature on the same body.
    let wrong_author = other
        .sign_event(UnsignedEvent {
            pubkey: other.account(),
            created_at: package.created_at,
            kind: package.kind,
            tags: package.tags.clone(),
            content: package.content.clone(),
        })
        .await
        .expect("sign");
    assert_eq!(
        ids(&select_key_packages(
            account,
            &[package.clone(), wrong_author],
            now
        )),
        ids(std::slice::from_ref(&package)),
    );

    // A wrong `i` tag rejects the event (its slot still claims).
    let bad_ref = resigned(&signer, &package, |draft| {
        for tag in &mut draft.tags {
            if tag.first().map(String::as_str) == Some("i") {
                tag[1] = "00".repeat(32);
            }
        }
    })
    .await;
    assert!(
        select_key_packages(account, &[bad_ref], now).is_empty(),
        "i tag not matching the KeyPackageRef"
    );

    // A capability tag that does not equal the decoded advertisement.
    let tag_mismatch = resigned(&signer, &package, |draft| {
        for tag in &mut draft.tags {
            if tag.first().map(String::as_str) == Some("app_components") {
                tag.push("0x9999".to_owned());
            }
        }
    })
    .await;
    assert!(
        select_key_packages(account, &[tag_mismatch], now).is_empty(),
        "a tag/metadata mismatch selects nothing",
    );

    // A wrong ciphersuite tag.
    let bad_ciphersuite = resigned(&signer, &package, |draft| {
        for tag in &mut draft.tags {
            if tag.first().map(String::as_str) == Some("mls_ciphersuite") {
                tag[1] = "0x0000".to_owned();
            }
        }
    })
    .await;
    assert!(
        select_key_packages(account, &[bad_ciphersuite], now).is_empty(),
        "a wrong ciphersuite tag selects nothing",
    );

    // Bad signature: filtered before slot accounting.
    let mut forged = package.clone();
    forged.sig[0] ^= 0xff;
    assert_eq!(
        ids(&select_key_packages(
            account,
            &[package.clone(), forged],
            now
        )),
        ids(std::slice::from_ref(&package)),
    );
}

#[tokio::test]
async fn duplicated_d_tag() {
    let signer = Arc::new(LocalSigner::new());
    let (member, package) = device_package(&signer).await;
    let now = now(&member);
    let account = signer.account();

    // Stricter than MDK (which reads the first `d`): the doubled tag is
    // malformed, the event is rejected — but its first `d` still claims the
    // slot, so a later valid publication of that slot loses too.
    let doubled = resigned(&signer, &package, |draft| {
        draft.created_at += 60;
        draft
            .tags
            .insert(1, vec!["d".to_owned(), "other".to_owned()]);
    })
    .await;
    assert!(
        select_key_packages(account, &[doubled, package.clone()], now).is_empty(),
        "doubled-d masks the first-d slot"
    );

    // A `d` past the first claims nothing: `doubled` names "other" first, so
    // the valid package's own slot stays open and selects.
    let doubled_other = resigned(&signer, &package, |draft| {
        draft.created_at += 60;
        draft
            .tags
            .insert(0, vec!["d".to_owned(), "other".to_owned()]);
    })
    .await;
    assert_eq!(
        ids(&select_key_packages(
            account,
            &[doubled_other, package.clone()],
            now,
        )),
        ids(&[package]),
    );
}

#[tokio::test]
async fn creation_requirements_pass() {
    let signer = Arc::new(LocalSigner::new());
    let (member, package) = device_package(&signer).await;
    // No group: the requirements a plain `create_group` would impose
    // (Nostr routing) — our own publication satisfies them.
    let selected = member
        .session
        .select_key_packages(signer.account(), std::slice::from_ref(&package), None)
        .expect("select_key_packages");
    assert_eq!(ids(&selected), ids(&[package]));
}

#[tokio::test]
async fn group_requirements() {
    let signer = Arc::new(LocalSigner::new());
    let (mut host_member, _) = device_package(&signer).await;
    let invitee_signer = Arc::new(LocalSigner::new());
    let (_invitee_member, invitee_package) = device_package(&invitee_signer).await;

    // A plain group's requirements accept a normal publication.
    let plain = host_member
        .session
        .create_group(CreateGroup {
            name: "plain".to_owned(),
            description: String::new(),
            relays: vec![RELAY.to_owned()],
            invitees: Vec::new(),
            admins: Vec::new(),
            components: Vec::new(),
        })
        .await
        .expect("create_group");
    let selected = host_member
        .session
        .select_key_packages(
            invitee_signer.account(),
            std::slice::from_ref(&invitee_package),
            Some(&plain),
        )
        .expect("select_key_packages");
    assert_eq!(ids(&selected), ids(&[invitee_package]));

    // A group this session does not hold errors rather than selecting
    // (the engine's record read surfaces a storage NotFound).
    let unknown = mk_core::GroupId(vec![0xaa; 8]);
    assert!(
        host_member
            .session
            .select_key_packages(signer.account(), &[], Some(&unknown))
            .is_err(),
    );
    // A requirements-negative needs a publication whose leaf lacks a
    // component the group requires: every group this session can create
    // only requires what its own leaf already advertises, and both sides
    // here share `supported_components()`.
}
