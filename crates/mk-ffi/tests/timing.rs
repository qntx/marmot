//! Establish what `mk-bridge`'s poll-with-noop-waker model needs from a
//! native Tokio runtime — the evidence behind the engine thread's
//! runtime setup (`crates/mk-ffi/README.md` records the findings).
//!
//! The only Tokio use reachable through the bridge is `cgka-engine`'s
//! `deadline::timeout` on the deferred-peel reingest path, so the probe
//! defers a row, changes the peel context, and lets a send's admission
//! sweep re-ingest it under the foreground budget.

#![allow(unsafe_code, reason = "the tests also exercise the C entry points")]
#![allow(
    unused_crate_dependencies,
    clippy::borrow_as_ptr,
    clippy::cast_lossless,
    clippy::cast_possible_truncation,
    clippy::clone_on_ref_ptr,
    clippy::disallowed_methods,
    clippy::doc_markdown,
    clippy::excessive_nesting,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::manual_dangling_ptr,
    clippy::manual_let_else,
    clippy::map_unwrap_or,
    clippy::match_wildcard_for_single_variants,
    clippy::missing_const_for_fn,
    clippy::missing_docs_in_private_items,
    clippy::missing_fields_in_debug,
    clippy::missing_panics_doc,
    clippy::multiple_unsafe_ops_per_block,
    clippy::needless_pass_by_value,
    clippy::option_if_let_else,
    clippy::panic,
    clippy::shadow_reuse,
    clippy::shadow_unrelated,
    clippy::significant_drop_tightening,
    clippy::tests_outside_test_module,
    clippy::too_many_lines,
    clippy::undocumented_unsafe_blocks,
    clippy::unnecessary_wraps,
    clippy::unwrap_in_result,
    clippy::unwrap_used,
    unreachable_pub,
    dead_code,
    reason = "test code"
)]

mod support;

use std::task::{Context, Poll, Waker};
use std::time::Duration;

use mk_bridge::protocol::*;
use mk_core::AccountSigner;
use support::*;

/// A two-member setup that lands a `PeelDeferred` row on B — a
/// signed-but-undecryptable kind-445 for the group's route — then makes B
/// send: the send-admission convergence sweep re-ingests the row under
/// the foreground budget, which is the one `tokio::time::timeout` site
/// reachable through the bridge.
fn deferred_peel_flow() {
    let mut relay = FakeRelay::new();
    let mut a = DirectMember::new();
    a.open();
    a.publish_identity(&mut relay);
    let mut b = DirectMember::new();
    b.open();
    b.publish_identity(&mut relay);

    let key_package = relay.key_package_of(&b.account_hex());
    let response = a.call(Request::CreateGroup(CreateGroupRequest {
        name: "fork".to_owned(),
        description: String::new(),
        relays: vec![RELAY.to_owned()],
        invitees: vec![Invitee {
            key_package,
            inbox_relays: vec![RELAY.to_owned()],
        }],
        admins: vec![],
        components: vec![],
    }));
    let Response::Ok { result } = response else {
        panic!("create_group: {response:?}");
    };
    let OpResult::GroupCreated(created) = *result else {
        panic!("create_group result: {result:?}");
    };
    let group = created.group.id.clone();
    a.publish(&mut relay);
    b.ingest_and_drain(relay.welcomes_for(&b.account_hex()));
    b.sync(&mut relay, &group);
    a.sync(&mut relay, &group);

    // A kind-445 addressed to the group's route whose content is base64
    // but not a valid group envelope: peel fails, the raw transport is
    // retained as `PeelDeferred` and attempted under the current
    // peel-context fingerprint by the ingest's own drive.
    let route_hex = b.route_hex(&group);
    let forged = block_on_now(
        b.signer.sign_event(
            UnsignedEvent {
                pubkey: b.signer.account().0,
                created_at: 1_700_100_000,
                kind: 445,
                tags: vec![vec!["h".to_owned(), route_hex]],
                // base64(32 zero bytes): valid base64, undecryptable content.
                content: "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=".to_owned(),
            }
            .into(),
        ),
    )
    .expect("forge 445");
    let response = b.call(Request::Ingest(IngestRequest {
        events: vec![forged.into()],
    }));
    let Response::Ok { result } = response else {
        panic!("forge ingest: {response:?}");
    };
    let OpResult::Ingested(ingested) = *result else {
        panic!("forge ingest result: {result:?}");
    };
    assert!(
        ingested.reports.iter().any(|report| matches!(
            report.outcome,
            InputOutcome::Unavailable {
                category: InputCategory::TransportDeferred,
            }
        )),
        "the forged event must defer: {ingested:?}"
    );

    // An own leaf update stages a commit; confirming its publish merges it
    // and the epoch advance changes the complete peel-context fingerprint.
    // `report_publish` schedules convergence but does not sweep, so the
    // deferred row is still untested under the new fingerprint.
    let response = b.call(Request::SelfUpdate(SelfUpdateRequest {
        group: group.clone(),
    }));
    assert!(
        matches!(response, Response::Ok { .. }),
        "self_update: {response:?}"
    );
    b.publish(&mut relay);

    // The next foreground send's admission sweep finds the row unattempted
    // under the new fingerprint and re-ingests it inside
    // `deadline::timeout` — the one `tokio::time::Sleep` the bridge's
    // poll-with-noop-waker model can poll.
    b.send_chat(&mut relay, &group, "after-deferred");
}

/// (1) A plain std thread with no runtime in context: the bridge's
/// futures must reach `tokio::time::timeout` on the deferred-peel path
/// and fail there.
#[test]
#[cfg_attr(miri, ignore = "miri cannot run secp256k1-sys C code (session open)")]
fn bare_thread_panics_inside_engine_work() {
    let outcome = std::thread::Builder::new()
        .stack_size(16 * 1024 * 1024)
        .spawn(|| panic_message(deferred_peel_flow))
        .expect("spawn")
        .join()
        .expect("join");
    let message = outcome.expect("the deferred-peel flow must panic without a runtime");
    assert!(
        message.contains("there is no reactor running"),
        "unexpected bare-thread panic: {message}"
    );
}

/// (2) A `current_thread` runtime entered but built without
/// `enable_time`: context exists, the time driver does not.
#[test]
#[cfg_attr(miri, ignore = "miri cannot run secp256k1-sys C code (session open)")]
fn runtime_without_time_panics() {
    let outcome = std::thread::Builder::new()
        .stack_size(16 * 1024 * 1024)
        .spawn(|| {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .build()
                .expect("runtime");
            let _guard = runtime.enter();
            panic_message(deferred_peel_flow)
        })
        .expect("spawn")
        .join()
        .expect("join");
    let message = outcome.expect("the deferred-peel flow must panic without a time driver");
    assert!(
        message.contains("timers are disabled"),
        "unexpected no-time-driver panic: {message}"
    );
}

/// (3) `new_current_thread().enable_time()` entered for the thread's
/// life but never driven: every operation still resolves — the noop
/// waker model never needs the reactor to run.
#[test]
#[cfg_attr(miri, ignore = "miri cannot run secp256k1-sys C code (session open)")]
fn entered_undriven_runtime_passes() {
    std::thread::Builder::new()
        .stack_size(16 * 1024 * 1024)
        .spawn(|| {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_time()
                .build()
                .expect("runtime");
            let _guard = runtime.enter();
            deferred_peel_flow();
        })
        .expect("spawn")
        .join()
        .expect("join");
}

/// (4) Under the undriven model a `tokio::time::timeout` deadline that
/// elapses in real time never fires on re-poll — the timer wheel only
/// advances when the driver is turned. Turning it once before the next
/// poll (one `block_on` on a yield) releases the elapsed deadline.
#[test]
fn undriven_deadline_only_fires_after_a_driver_turn() {
    std::thread::Builder::new()
        .spawn(|| {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_time()
                .build()
                .expect("runtime");
            let _guard = runtime.enter();
            let mut sleep = std::pin::pin!(tokio::time::sleep(Duration::from_millis(20)));
            let waker = Waker::noop();
            let mut context = Context::from_waker(waker);
            std::thread::sleep(Duration::from_millis(40));
            assert!(
                matches!(sleep.as_mut().poll(&mut context), Poll::Pending),
                "elapsed deadline fired on a driver that never turned"
            );
            runtime.block_on(tokio::task::yield_now());
            assert!(
                matches!(sleep.as_mut().poll(&mut context), Poll::Ready(())),
                "one driver turn did not release the elapsed deadline"
            );
        })
        .expect("spawn")
        .join()
        .expect("join");
}
