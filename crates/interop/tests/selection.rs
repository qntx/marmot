//! Differential `KeyPackage` selection: `mk_core::key_package::select_key_packages`
//! must return exactly the event ids, in the same order, that MDK
//! `nostr_directory::select_key_packages_from_records` (pinned v0.18.0)
//! returns for the same signed record sets (`Err` ≡ empty).
//!
//! The generator only emits inputs the two implementations agree on by
//! contract; documented deviations it never produces, each covered by a
//! `mk-core/tests/selection.rs` case:
//!
//! - a `d`/`i` tag repeated, or any single-valued tag (`d`, `i`,
//!   `mls_protocol_version`, `mls_ciphersuite`) carrying extra elements —
//!   MDK reads the first value (`tag_value`), mk-core rejects the event
//!   outright;
//! - a candidate whose signature does not verify — MDK trusts
//!   relay-verified records and never checks, mk-core verifies and ignores
//!   it (deletions are verified by both);
//! - group requirements — the differential always passes `requirements:
//!   None`; the requirements-aware path is session-internal and has no MDK
//!   record-level equivalent here.

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
    clippy::cast_sign_loss,
    clippy::str_to_string,
    clippy::needless_pass_by_value,
    clippy::items_after_statements,
    clippy::implicit_hasher,
    clippy::redundant_clone,
    clippy::too_many_lines,
    clippy::excessive_nesting,
    clippy::clone_on_ref_ptr,
    reason = "test code"
)]

use interop as _;
use mk_bridge as _;
use nostr as _;
use nostr_sdk as _;

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use mk_core::key_package::select_key_packages;
use mk_core::testing::{LocalSigner, ManualClock, RecordingHost};
use mk_core::{
    AccountSigner, Clock, DestinationPolicy, Event, KvHost, Opened, Session, SessionConfig,
    UnsignedEvent,
};
use nostr_directory::{DirectoryFreshness, select_key_packages_from_records};
use transport_nostr::DirectoryRelayEventRecord;
use transport_nostr_peeler::NostrTransportEvent;

/// Deterministic case budget.
const CASES: u64 = 2_000;
/// MDK's `DEFAULT_DIRECTORY_MAX_FUTURE_SKEW`: 5 minutes.
const MAX_FUTURE_SKEW: Duration = Duration::from_secs(300);
const DAY: u64 = 24 * 60 * 60;
const RELAY: &str = "wss://r.example";

/// xorshift64*: deterministic generation, no extra dependencies.
struct Rng(u64);

impl Rng {
    const fn seeded(seed: u64) -> Self {
        Self(seed | 1)
    }

    const fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    const fn below(&mut self, bound: u64) -> u64 {
        self.next() % bound
    }

    const fn chance(&mut self, percent: u64) -> bool {
        self.below(100) < percent
    }
}

/// The signed material one selection run draws on.
struct Material {
    /// The invitee account every generated event is (re-)signed by, unless
    /// a case deliberately picks `other` (wrong-author coverage both sides
    /// filter identically).
    signer: Arc<LocalSigner>,
    other: Arc<LocalSigner>,
    /// `signer`'s kind-30443 publications: several devices, one publishing
    /// twice so two entries share a leaf signature key.
    packages: Vec<Event>,
    /// One kind-30443 publication authored by `other`.
    foreign: Event,
}

/// Open one device session over `signer` (fresh host = fresh device,
/// distinct leaf signature key) and return its published kind-30443 event.
async fn device_package(signer: &Arc<LocalSigner>) -> (Session, Event) {
    let clock: Arc<dyn Clock> = Arc::new(ManualClock::new());
    let host: Arc<dyn KvHost> = Arc::new(RecordingHost::new());
    let signer_cfg: Arc<dyn AccountSigner> = signer.clone();
    let opened = Session::open(SessionConfig {
        signer: Arc::clone(&signer_cfg),
        host,
        rows: Vec::new(),
        clock,
        destination_policy: DestinationPolicy {
            allow_loopback: true,
        },
        client_name: None,
    })
    .expect("open");
    let mut session = match opened {
        Opened::NeedsAccountProof(pending) => {
            let signed = signer_cfg
                .sign_event(pending.request().clone())
                .await
                .expect("sign");
            pending.provide_account_proof(&signed).expect("proof")
        }
        Opened::Ready(session) => *session,
    };
    session
        .publish_key_package(&[RELAY.to_owned()])
        .await
        .expect("publish_key_package");
    if let Some(seq) = session.awaiting_durability() {
        session.acknowledge_durable(seq);
    }
    let event = session
        .take_outbound()
        .expect("take_outbound")
        .into_iter()
        .find(|outbound| outbound.event.kind == 30_443)
        .expect("key package outbound")
        .event;
    (session, event)
}

/// The value of `event`'s first `d` tag, `""` when absent — the slot both
/// sides derive identically.
fn slot_of(event: &Event) -> String {
    event
        .tags
        .iter()
        .find(|tag| tag.first().map(String::as_str) == Some("d"))
        .and_then(|tag| tag.get(1))
        .cloned()
        .unwrap_or_default()
}

/// Sign a draft with `signer`.
async fn sign(signer: &LocalSigner, draft: UnsignedEvent) -> Event {
    signer.sign_event(draft).await.expect("sign")
}

/// Replace or remove the single `name` tag, preserving the exactly-once
/// shape (`[name, value]`) so the result never hits the duplicated-tag
/// deviation.
fn set_single_tag(draft: &mut UnsignedEvent, name: &str, value: &str) {
    draft
        .tags
        .retain(|tag| tag.first().map(String::as_str) != Some(name));
    draft.tags.push(vec![name.to_owned(), value.to_owned()]);
}

/// One tag/content mutation both implementations reject identically.
fn mutate(draft: &mut UnsignedEvent, rng: &mut Rng) {
    match rng.below(11) {
        // Missing `i`.
        0 => draft
            .tags
            .retain(|tag| tag.first().map(String::as_str) != Some("i")),
        // Wrong `i` value.
        1 => set_single_tag(draft, "i", &"00".repeat(32)),
        // Wrong ciphersuite tag.
        2 => set_single_tag(draft, "mls_ciphersuite", "0x0000"),
        // A capability id the package does not advertise.
        3 => push_tag_value(draft, "app_components", "0x9999"),
        // A capability id missing from the tag.
        4 => pop_tag_value(draft, "mls_extensions"),
        // A repeated value inside an id-list tag.
        5 => duplicate_tag_value(draft, "mls_proposals"),
        // Garbage content: not base64, or valid base64 that is no package.
        6 => {
            draft.content = if rng.chance(50) {
                "not-base64!!".to_owned()
            } else {
                "AAAA".to_owned()
            };
        }
        // Missing or wrong protocol-version tag.
        7 => {
            if rng.chance(50) {
                draft
                    .tags
                    .retain(|tag| tag.first().map(String::as_str) != Some("mls_protocol_version"));
            } else {
                set_single_tag(draft, "mls_protocol_version", "2.0");
            }
        }
        // A repeated id-list tag name — both reject (`reject_duplicate` /
        // `single_tag`); never `d`/`i` where the semantics differ.
        8 => {
            let name = ["mls_ciphersuite", "app_components"][rng.below(2) as usize];
            if let Some(tag) = draft
                .tags
                .iter()
                .find(|tag| tag.first().map(String::as_str) == Some(name))
                .cloned()
            {
                draft.tags.push(tag);
            }
        }
        // A repeated value standing in for a missing one, same length —
        // both sides require set equality, not just membership.
        9 => substitute_tag_value(draft, "mls_proposals"),
        // Valid: no mutation.
        _ => {}
    }
}

/// Rewrite the last value of `name`'s tag to equal its first: same length,
/// a duplicate, one expected id missing — both sides reject.
fn substitute_tag_value(draft: &mut UnsignedEvent, name: &str) {
    if let Some(tag) = draft
        .tags
        .iter_mut()
        .find(|tag| tag.first().map(String::as_str) == Some(name))
        && tag.len() > 2
        && let Some(first) = tag.get(1).cloned()
        && let Some(last) = tag.last_mut()
    {
        *last = first;
    }
}

fn push_tag_value(draft: &mut UnsignedEvent, name: &str, value: &str) {
    if let Some(tag) = draft
        .tags
        .iter_mut()
        .find(|tag| tag.first().map(String::as_str) == Some(name))
    {
        tag.push(value.to_owned());
    }
}

fn pop_tag_value(draft: &mut UnsignedEvent, name: &str) {
    if let Some(tag) = draft
        .tags
        .iter_mut()
        .find(|tag| tag.first().map(String::as_str) == Some(name))
        && tag.len() > 2
    {
        tag.pop();
    }
}

fn duplicate_tag_value(draft: &mut UnsignedEvent, name: &str) {
    if let Some(tag) = draft
        .tags
        .iter_mut()
        .find(|tag| tag.first().map(String::as_str) == Some(name))
        && let Some(value) = tag.get(1).cloned()
    {
        tag.push(value);
    }
}

/// The `a`/`e` tag set of one kind-5 request; values mix covering and
/// non-covering spellings.
fn deletion_tags(
    rng: &mut Rng,
    account_hex: &str,
    other_hex: &str,
    candidates: &[Event],
) -> Vec<Vec<String>> {
    let mut tags = Vec::new();
    for _ in 0..=rng.below(2) {
        let Some(target) = candidates.get(rng.below(candidates.len() as u64) as usize) else {
            continue;
        };
        match rng.below(10) {
            // `e` coverage of the target id, exact or upper-case.
            0 | 1 => tags.push(vec![
                "e".to_owned(),
                if rng.chance(25) {
                    target.id.to_hex().to_uppercase()
                } else {
                    target.id.to_hex()
                },
            ]),
            // `e` naming nothing.
            2 => tags.push(vec!["e".to_owned(), "00".repeat(32)]),
            // `a` coverage of the target slot's address.
            3 | 4 => {
                let author = if rng.chance(25) {
                    account_hex.to_uppercase()
                } else {
                    account_hex.to_owned()
                };
                let mut tag = vec![
                    "a".to_owned(),
                    format!("30443:{author}:{}", slot_of(target)),
                ];
                // A relay-hint element keeps the first value authoritative.
                if rng.chance(30) {
                    tag.push(RELAY.to_owned());
                }
                tags.push(tag);
            }
            // `a` with a wrong kind, author or slot.
            5 => tags.push(vec![
                "a".to_owned(),
                format!("30444:{account_hex}:{}", slot_of(target)),
            ]),
            6 => tags.push(vec![
                "a".to_owned(),
                format!("30443:{other_hex}:{}", slot_of(target)),
            ]),
            7 => tags.push(vec![
                "a".to_owned(),
                format!("30443:{account_hex}:no-such-slot"),
            ]),
            // `a` malformed: missing fields, wrong field count.
            8 => tags.push(vec!["a".to_owned(), format!("30443:{account_hex}")]),
            // An `e` with a hint element — first value still covers.
            _ => tags.push(vec!["e".to_owned(), target.id.to_hex(), RELAY.to_owned()]),
        }
    }
    tags
}

/// One generated case's event set: mutated re-signed candidates plus
/// kind-5 requests and occasional unrelated events, shuffled.
async fn generate(rng: &mut Rng, material: &Material, now: u64) -> Vec<Event> {
    let mut events = Vec::new();
    let candidate_count = 1 + rng.below(8);
    for _ in 0..candidate_count {
        // A wrong-author variant: the account's event shape signed by the
        // other account, or that account's own publication — both sides
        // filter `pubkey == account` before anything else.
        let (base, signer) = if rng.chance(12) {
            let base = if rng.chance(50) {
                material.foreign.clone()
            } else {
                material.packages[rng.below(material.packages.len() as u64) as usize].clone()
            };
            (base, material.other.clone())
        } else {
            (
                material.packages[rng.below(material.packages.len() as u64) as usize].clone(),
                material.signer.clone(),
            )
        };
        let mut draft = UnsignedEvent {
            pubkey: signer.account(),
            created_at: base.created_at,
            kind: base.kind,
            tags: base.tags.clone(),
            content: base.content.clone(),
        };
        draft.created_at = now
            .saturating_sub(40 * DAY)
            .saturating_add(rng.below(40 * DAY + 600));
        match rng.below(10) {
            // Keep the device's own slot.
            0..=2 => {}
            // Pool slots collide often enough to exercise superseding.
            3..=7 => {
                let slot = format!("s{}", rng.below(8));
                set_single_tag(&mut draft, "d", &slot);
            }
            // Another device's slot or the empty slot.
            8 => {
                let other_slot = slot_of(
                    material
                        .packages
                        .get(rng.below(material.packages.len() as u64) as usize)
                        .expect("packages"),
                );
                set_single_tag(&mut draft, "d", &other_slot);
            }
            _ => set_single_tag(&mut draft, "d", ""),
        }
        if rng.chance(30) {
            mutate(&mut draft, rng);
        }
        events.push(sign(&signer, draft).await);
    }
    // An exact duplicate of a candidate: identical id and slot.
    if rng.chance(20)
        && let Some(event) = events.first().cloned()
    {
        events.push(event);
    }
    // An unrelated kind authored by the account — neither side reads it.
    if rng.chance(25) {
        events.push(
            sign(
                &material.signer,
                UnsignedEvent {
                    pubkey: material.signer.account(),
                    created_at: now.saturating_sub(rng.below(10 * DAY)),
                    kind: [10_050, 445, 6][rng.below(3) as usize],
                    tags: Vec::new(),
                    content: String::new(),
                },
            )
            .await,
        );
    }
    // Kind-5 deletion requests.
    let account_hex = material.signer.account().to_hex();
    let other_hex = material.other.account().to_hex();
    for _ in 0..rng.below(4) {
        let author = if rng.chance(85) {
            material.signer.clone()
        } else {
            material.other.clone()
        };
        let tags = deletion_tags(rng, &account_hex, &other_hex, &events);
        events.push(
            sign(
                &author,
                UnsignedEvent {
                    pubkey: author.account(),
                    created_at: now
                        .saturating_sub(40 * DAY)
                        .saturating_add(rng.below(40 * DAY + 600)),
                    kind: 5,
                    tags,
                    content: String::new(),
                },
            )
            .await,
        );
    }
    // Shuffle: selection order must not depend on input order.
    for i in (1..events.len()).rev() {
        events.swap(i, rng.below(i as u64 + 1) as usize);
    }
    events
}

/// The `DirectoryRelayEventRecord` a relay would hand MDK's directory.
fn record(event: &Event) -> DirectoryRelayEventRecord {
    DirectoryRelayEventRecord {
        endpoints: Vec::new(),
        event: NostrTransportEvent {
            id: event.id.to_hex(),
            pubkey: event.pubkey.to_hex(),
            created_at: event.created_at,
            kind: u64::from(event.kind),
            tags: event.tags.clone(),
            content: event.content.clone(),
            sig: Some(hex::encode(event.sig)),
        },
    }
}

#[tokio::test]
async fn selection_matches_mdk() {
    let signer = Arc::new(LocalSigner::from_secret([0x6d; 32]).expect("fixed secret"));
    let other = Arc::new(LocalSigner::from_secret([0x77; 32]).expect("fixed secret"));
    // Three devices of one account; device 0 publishes twice so two
    // publications share a leaf signature key.
    let mut packages = Vec::new();
    for _ in 0..3 {
        let (_session, package) = device_package(&signer).await;
        packages.push(package);
    }
    let (session0, package0) = device_package(&signer).await;
    let mut session0 = session0;
    packages.push(package0);
    session0
        .publish_key_package(&[RELAY.to_owned()])
        .await
        .expect("publish_key_package");
    if let Some(seq) = session0.awaiting_durability() {
        session0.acknowledge_durable(seq);
    }
    packages.push(
        session0
            .take_outbound()
            .expect("take_outbound")
            .into_iter()
            .find(|outbound| outbound.event.kind == 30_443)
            .expect("second key package")
            .event,
    );
    let (_foreign_session, foreign) = device_package(&other).await;
    let material = Material {
        signer: signer.clone(),
        other: other.clone(),
        packages,
        foreign,
    };
    let account = material.signer.account();
    let account_hex = account.to_hex();
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_secs();

    for case in 0..CASES {
        let mut rng = Rng::seeded(0x9e37_79b9_7f4a_7c15 ^ case.wrapping_mul(0x9e37_79b9));
        let events = generate(&mut rng, &material, now).await;
        let ours: Vec<String> = select_key_packages(account, &events, now)
            .iter()
            .map(|event| event.id.to_hex())
            .collect();
        let records: Vec<DirectoryRelayEventRecord> = events.iter().map(record).collect();
        let theirs: Vec<String> = select_key_packages_from_records(
            &account_hex,
            &records,
            DirectoryFreshness::at_for_test(now, MAX_FUTURE_SKEW),
            None,
        )
        .map(|selection| {
            selection
                .value
                .iter()
                .map(|fetched| fetched.key_package_event_id.clone())
                .collect()
        })
        .unwrap_or_default();
        assert_eq!(
            ours, theirs,
            "case {case}: mk-core and MDK selected different KeyPackages",
        );
    }
}
