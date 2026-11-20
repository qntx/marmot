//! End-to-end: a real `cgka_engine` over `KvStorageProvider`, two members, one
//! in-process fake relay — create, invite, one message, plus a mid-flow
//! restart from the host's durable rows.

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
    clippy::cast_possible_truncation,
    clippy::many_single_char_names,
    clippy::missing_const_for_fn,
    clippy::use_self,
    clippy::default_trait_access,
    clippy::needless_for_each,
    clippy::manual_is_multiple_of,
    clippy::useless_let_if_seq,
    clippy::unneeded_struct_pattern,
    clippy::shadow_unrelated,
    clippy::excessive_nesting,
    clippy::missing_docs_in_private_items,
    clippy::too_many_lines,
    clippy::similar_names,
    clippy::cast_lossless,
    clippy::assertions_on_result_states,
    clippy::ignored_unit_patterns,
    clippy::items_after_statements,
    clippy::cast_possible_wrap,
    clippy::cast_sign_loss,
    clippy::redundant_clone,
    clippy::struct_excessive_bools,
    clippy::doc_markdown,
    clippy::missing_panics_doc,
    clippy::single_call_fn,
    clippy::implicit_hasher,
    clippy::redundant_type_annotations,
    clippy::field_reassign_with_default,
    clippy::tests_outside_test_module,
    clippy::clone_on_ref_ptr,
    clippy::str_to_string,
    clippy::missing_assert_message,
    reason = "test code"
)]
mod harness;

// `unused_crate_dependencies` counts per binary; the dev-dependency set is
// shared across all of them, so each file binds the crates it does not use.
use openmls_rust_crypto as _;
use proptest as _;
use storage_sqlite as _;

use cgka_traits::engine::{CgkaEngine, SendIntent, SendResult};
use harness::*;
use transport_nostr_peeler::KIND_MARMOT_GROUP_MESSAGE;

/// Create a group with one invitee, deliver the Welcome, send one chat
/// message, and assert the invitee decodes it.
#[tokio::test(flavor = "current_thread")]
async fn engine_create_invite_one_message() {
    let relay = FakeRelay::new();
    let mut alice = Member::fresh("alice", "alice");
    let mut bob = Member::fresh("bob", "bob");

    publish_key_package(&mut bob, &relay).await;
    let bob_kp = key_package_from_event(
        relay
            .fetch_key_packages(&bob.signer.keys().public_key().to_hex())
            .first()
            .expect("bob published a key package"),
    );

    let gid = create_group(&mut alice, &relay, "kv-e2e", vec![bob_kp]).await;
    for outcome in ingest_welcomes(&mut bob, &relay).await {
        assert!(!matches!(
            outcome,
            cgka_traits::ingest::IngestOutcome::Rejected { .. }
        ));
    }
    flush_outbound(&mut bob, &gid, &relay).await;

    let result = alice
        .engine
        .send(SendIntent::AppMessage {
            group_id: gid.clone(),
            payload: chat_payload(
                &alice.signer.keys().public_key().to_hex(),
                now_secs(),
                "hello bob",
            ),
            expected_epoch: None,
        })
        .await
        .expect("send");
    publish_send_result(&mut alice, result, &relay).await;

    for event in relay.fetch_ascending(KIND_MARMOT_GROUP_MESSAGE, None, None) {
        ingest_event(&mut bob, &event).await;
    }
    flush_outbound(&mut bob, &gid, &relay).await;

    let received = bob.drain_received();
    assert!(
        received.iter().any(|(_, _, payload)| {
            cgka_traits::app_event::MarmotAppEvent::decode(payload)
                .is_ok_and(|app| app.content == "hello bob")
        }),
        "bob decodes alice's chat message"
    );
}

/// Same flow, but bob's storage is dropped after the Welcome lands and
/// reopened from the host's durable rows before the message arrives.
#[tokio::test(flavor = "current_thread")]
async fn engine_restart_midflow_from_host_rows() {
    let relay = FakeRelay::new();
    let mut alice = Member::fresh("alice", "alice");
    let mut bob = Member::fresh("bob", "bob");

    publish_key_package(&mut bob, &relay).await;
    let bob_kp = key_package_from_event(
        relay
            .fetch_key_packages(&bob.signer.keys().public_key().to_hex())
            .first()
            .expect("bob published a key package"),
    );

    let gid = create_group(&mut alice, &relay, "kv-restart", vec![bob_kp]).await;
    ingest_welcomes(&mut bob, &relay).await;
    flush_outbound(&mut bob, &gid, &relay).await;

    // Restart: every row the host holds is all that survives.
    let rows = bob.host.durable_rows();
    let signer = bob.signer.clone();
    let mut bob = Member::restart(rows, "bob", signer);

    let result = alice
        .engine
        .send(SendIntent::AppMessage {
            group_id: gid.clone(),
            payload: chat_payload(
                &alice.signer.keys().public_key().to_hex(),
                now_secs(),
                "after restart",
            ),
            expected_epoch: None,
        })
        .await
        .expect("send");
    publish_send_result(&mut alice, result, &relay).await;

    for event in relay.fetch_ascending(KIND_MARMOT_GROUP_MESSAGE, None, None) {
        ingest_event(&mut bob, &event).await;
    }
    flush_outbound(&mut bob, &gid, &relay).await;

    let received = bob.drain_received();
    assert!(
        received.iter().any(|(_, _, payload)| {
            cgka_traits::app_event::MarmotAppEvent::decode(payload)
                .is_ok_and(|app| app.content == "after restart")
        }),
        "bob decodes the message after reopening from durable rows"
    );
}

/// Reopening from durable rows resolves every registered capability
/// description through `KNOWN_DESCRIPTIONS`: `cgka-engine` registers none
/// itself and this crate registers only SelfRemove, so a reopened provider
/// interns nothing.
///
/// `register_feature`/`feature_requirement` are pure storage calls — the
/// pinned engine and marmot-app never invoke them — so the capability rows
/// are seeded directly, alongside a real engine run.
#[cfg(feature = "testing")]
#[test]
fn reopened_provider_interns_no_descriptions() {
    use cgka_traits::capabilities::{Capability, CapabilityRequirement, Feature, RequirementLevel};
    use cgka_traits::storage::CapabilityStorage;
    use mk_core::KvStorageProvider;

    let member = Member::fresh("desc-member", "desc-seed");
    member
        .provider
        .register_feature(
            Feature("self-remove"),
            CapabilityRequirement {
                requires: Capability::Proposal(10),
                level: RequirementLevel::Required,
                description: "MIP-03 SelfRemove group departure",
            },
        )
        .expect("feature registers");

    let reopened = KvStorageProvider::open(member.host.durable_rows(), member.host.clone())
        .expect("provider reopens from durable rows");
    let requirement = reopened
        .feature_requirement(&Feature("self-remove"))
        .expect("requirement reads")
        .expect("requirement is present");
    assert_eq!(
        requirement.description, "MIP-03 SelfRemove group departure",
        "description round-trips"
    );
    assert_eq!(
        reopened.interned_description_count(),
        0,
        "known descriptions resolve to statics without interning"
    );
}

/// Pins the upstream same-second limitation the session's send pacing
/// sidesteps — the Rust home of the retired wasm `same-second-burst`
/// cases, which needed a sender that is not pace-limited. One member
/// sends 250 app messages sharing a single `created_at` second. Ingested
/// in `(created_at, id)` order — the order `Session::ingest_batch`
/// produces — the shuffled ratchet generations push a tail past the
/// 100-generation out-of-order tolerance and it lands `stale`; ingested in
/// publish order all 250 deliver.
#[tokio::test(flavor = "current_thread")]
async fn same_second_burst_sorted_loses_tail_publish_order_delivers() {
    const MESSAGES: usize = 250;

    // A fresh pair per case: a message stamped stale never re-delivers.
    for sorted in [true, false] {
        let relay = FakeRelay::new();
        let mut alice = Member::fresh("alice", "alice");
        let mut bob = Member::fresh("bob", "bob");
        publish_key_package(&mut bob, &relay).await;
        let bob_kp = key_package_from_event(
            relay
                .fetch_key_packages(&bob.signer.keys().public_key().to_hex())
                .first()
                .expect("bob published a key package"),
        );
        let gid = create_group(&mut alice, &relay, "burst", vec![bob_kp]).await;
        ingest_welcomes(&mut bob, &relay).await;
        flush_outbound(&mut bob, &gid, &relay).await;
        bob.drain_events();

        // Every inner event carries the same `created_at`, and the peeler
        // binds the outer kind-445 `created_at` to it, so the whole burst
        // shares one second on the wire.
        let second = now_secs();
        let baseline = relay
            .fetch_ascending(KIND_MARMOT_GROUP_MESSAGE, None, None)
            .len();
        let mut publish_order = Vec::with_capacity(MESSAGES);
        for i in 0..MESSAGES {
            let result = alice
                .engine
                .send(SendIntent::AppMessage {
                    group_id: gid.clone(),
                    payload: chat_payload(
                        &alice.signer.keys().public_key().to_hex(),
                        second,
                        &format!("burst#{i}"),
                    ),
                    expected_epoch: None,
                })
                .await
                .expect("send");
            if let SendResult::ApplicationMessage { msg, .. } = &result {
                publish_order.push(msg.id.clone());
            }
            publish_send_result(&mut alice, result, &relay).await;
        }
        assert_eq!(publish_order.len(), MESSAGES);

        let all = relay.fetch_ascending(KIND_MARMOT_GROUP_MESSAGE, None, None);
        let events = &all[baseline..];
        assert_eq!(events.len(), MESSAGES, "one event per send");
        assert!(
            events.iter().all(|event| event.created_at == second),
            "the burst shares one outer second"
        );

        // `fetch_ascending` already orders by `(created_at, id)` — the
        // sorted ingest case. For publish order, re-sort the same events
        // by send sequence.
        let mut events: Vec<_> = events.to_vec();
        if !sorted {
            let rank: std::collections::BTreeMap<_, usize> = publish_order
                .iter()
                .enumerate()
                .map(|(i, id)| (hex::encode(id.as_slice()), i))
                .collect();
            events.sort_by_key(|event| rank[&event.id]);
        }

        let mut stale = 0usize;
        for event in &events {
            let outcome = ingest_event(&mut bob, event).await;
            if matches!(outcome, cgka_traits::ingest::IngestOutcome::Stale { .. }) {
                stale += 1;
            }
        }
        let delivered = bob.drain_received().len();

        if sorted {
            assert!(stale > 0, "a stale tail is lost beyond the tolerance");
            assert_eq!(
                delivered + stale,
                MESSAGES,
                "every message either delivered or reported stale (stale={stale})"
            );
        } else {
            assert_eq!(stale, 0, "publish order stays inside the tolerance");
            assert_eq!(delivered, MESSAGES, "all 250 deliver in publish order");
        }
    }
}
