//! Send-time regression guard: `Session::send_message` in one group must
//! stay bounded as group history grows (the store-scaling design promise).

#![allow(
    clippy::cast_lossless,
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss,
    clippy::doc_markdown,
    clippy::excessive_nesting,
    clippy::future_not_send,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::manual_let_else,
    clippy::missing_docs_in_private_items,
    clippy::missing_panics_doc,
    clippy::panic,
    clippy::shadow_reuse,
    clippy::shadow_unrelated,
    clippy::tests_outside_test_module,
    clippy::unwrap_in_result,
    clippy::unwrap_used,
    clippy::wildcard_imports,
    reason = "test code"
)]

#[path = "support/mod.rs"]
mod support;

use std::time::Instant;

use serde as _;
use thiserror as _;

use mk_bridge::protocol::{
    AppMessage, CreateGroupRequest, IngestRequest, Invitee, OpResult, Request, Response,
    SendMessageRequest,
};
use support::*;

/// Total history built before the late window ends.
const HISTORY: usize = 2_000;
/// Window start (messages sent before the window) for the early probe.
const H_EARLY: usize = 10;
/// Window start for the late probe — its last send lands on `HISTORY`.
const H_LATE: usize = 1_979;
/// Sends timed per window.
const WINDOW: usize = 21;
/// Allowed `median_late / median_early` ratio.
const RATIO: f64 = 3.0;

fn median(samples: &[f64]) -> f64 {
    let mut sorted = samples.to_vec();
    sorted.sort_by(f64::total_cmp);
    sorted[sorted.len() / 2]
}

/// One timed `SendMessage` bridge call (incl. its signer round-trips);
/// the publish half of the send runs untimed afterwards.
async fn timed_send(
    a: &mut Member<CompactHost>,
    relay: &mut FakeRelay,
    group: &[u8],
    i: usize,
) -> f64 {
    a.advance(1_000);
    let start = Instant::now();
    let response = a
        .call(Request::SendMessage(SendMessageRequest {
            group: group.to_vec(),
            message: AppMessage {
                kind: 9,
                tags: Vec::new(),
                content: format!("m{i}"),
            },
        }))
        .await;
    let ms = start.elapsed().as_secs_f64() * 1_000.0;
    assert!(matches!(response, Response::Ok { .. }), "send {i}");
    a.publish(relay).await;
    ms
}

#[tokio::test(flavor = "current_thread")]
async fn send_time_is_bounded_against_history() {
    let mut relay = FakeRelay::new();
    let mut a = Member::with_host(CompactHost::new());
    let mut b = Member::with_host(CompactHost::new());
    a.indexed_publish = true;
    a.open().await;
    b.open().await;
    a.publish_identity(&mut relay).await;
    b.publish_identity(&mut relay).await;
    let response = a
        .call(Request::CreateGroup(CreateGroupRequest {
            name: "scaling".to_owned(),
            description: String::new(),
            relays: vec![RELAY.to_owned()],
            invitees: vec![Invitee {
                key_package: key_package_of(&relay, &b.account_hex()),
                inbox_relays: vec![RELAY.to_owned()],
            }],
            admins: Vec::new(),
            components: Vec::new(),
        }))
        .await;
    let Response::Ok { result } = response else {
        panic!("create_group");
    };
    let OpResult::GroupCreated(created) = *result else {
        panic!("create_group");
    };
    let group = created.group.id.clone();
    a.publish(&mut relay).await;
    let welcomes = welcomes_for(&relay, &b.account_hex());
    b.call(Request::Ingest(IngestRequest { events: welcomes }))
        .await;
    b.sync(&mut relay, &group).await;
    a.sync(&mut relay, &group).await;

    let mut times = Vec::with_capacity(HISTORY);
    for i in 0..HISTORY {
        times.push(timed_send(&mut a, &mut relay, &group, i).await);
    }

    let early = median(&times[H_EARLY..H_EARLY + WINDOW]);
    let late = median(&times[H_LATE..H_LATE + WINDOW]);
    eprintln!("send medians: h=10 {early:.3} ms, h=2000 {late:.3} ms");
    assert!(
        late <= RATIO * early,
        "send median at history {HISTORY} is {late:.3} ms, more than {RATIO}x \
         the {early:.3} ms median at history {H_EARLY}"
    );
}
