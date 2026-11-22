//! Live-heap attribution after a long history, under the dhat heap
//! profiler. A and B open, A creates a group inviting B, B joins; then
//!
//! - `HEAP_SIDE=receive` (default): A sends `HEAP_MESSAGES` messages
//!   unprofiled; B ingests them in calls of `HEAP_CHUNK` events, acking
//!   durability after each call as a host does, and drains once at the end
//!   (the `scaling` bench's ingest shape);
//! - `HEAP_SIDE=send`: A sends and publishes `HEAP_MESSAGES` messages, then
//!   drains its session events once.
//!
//! The profiler runs from just before the first profiled call until after
//! the final drain, and is stopped while both members are still alive, so
//! the dhat file's t-end view is exactly the heap that the profiled side
//! retained, by allocation call site. Blocks allocated before the profiler
//! started are not tracked. The store mirror's share is measured by
//! reopening a provider over the profiled member's durable rows and taking
//! the live-byte delta. Emits one JSON line prefixed `heap_profile: ` and
//! writes the dhat file to `HEAP_OUT` (default `dhat-heap.json`).

#![allow(
    clippy::cast_possible_truncation,
    clippy::doc_markdown,
    clippy::expect_used,
    clippy::future_not_send,
    clippy::missing_panics_doc,
    clippy::panic,
    clippy::print_stdout,
    clippy::unwrap_used,
    clippy::wildcard_imports,
    unused_crate_dependencies,
    reason = "bench harness: asserts fail loudly and stdout is the report"
)]

#[path = "../tests/support/mod.rs"]
mod support;

use std::sync::Arc;
use std::time::Instant;

use mk_bridge::protocol::{
    CreateGroupRequest, IngestRequest, Invitee, OpResult, Request, Response, SessionEvent,
};
use mk_core::testing::RecordingHost;
use serde_json::json;
use support::*;

#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name).map_or(default, |value| value.parse().expect(name))
}

fn live_bytes() -> usize {
    dhat::HeapStats::get().curr_bytes
}

/// A and B open and publish identities, A creates the group inviting B,
/// B joins from its Welcome, both sync.
async fn setup_pair() -> (FakeRelay, Member<CompactHost>, Member<CompactHost>, Vec<u8>) {
    let mut relay = FakeRelay::new();
    let mut a = Member::with_host(CompactHost::new());
    let mut b = Member::with_host(CompactHost::new());
    a.indexed_publish = true;
    b.indexed_publish = true;
    a.open().await;
    b.open().await;
    a.publish_identity(&mut relay).await;
    b.publish_identity(&mut relay).await;
    let response = a
        .call(Request::CreateGroup(CreateGroupRequest {
            name: "heap".to_owned(),
            description: String::new(),
            relays: vec![RELAY.to_owned()],
            invitees: vec![Invitee {
                key_package: key_package_of(&relay, &b.account_hex()),
                inbox_relays: vec![RELAY.to_owned()],
            }],
            own_devices: Vec::new(),
            admins: Vec::new(),
            components: Vec::new(),
        }))
        .await;
    let OpResult::GroupCreated(created) = ok_result(response) else {
        panic!("create_group");
    };
    let group = created.group.id.clone();
    a.publish(&mut relay).await;
    let welcomes = welcomes_for(&relay, &b.account_hex());
    b.call(Request::Ingest(IngestRequest { events: welcomes }))
        .await;
    b.sync(&mut relay, &group).await;
    a.sync(&mut relay, &group).await;
    (relay, a, b, group)
}

/// Live bytes a fresh provider over `host`'s durable rows holds — row
/// buffers and map nodes: the store mirror's size for that row set.
fn mirror_bytes(host: &CompactHost) -> usize {
    let before = live_bytes();
    let rows = host.rows();
    let provider = mk_core::KvStorageProvider::open(rows, Arc::new(RecordingHost::new()))
        .expect("mirror provider opens");
    let mirror = live_bytes().saturating_sub(before);
    drop(provider);
    mirror
}

async fn run() {
    let messages = env_usize("HEAP_MESSAGES", 10_000);
    let chunk = env_usize("HEAP_CHUNK", 500);
    let side = std::env::var("HEAP_SIDE").unwrap_or_else(|_| "receive".to_owned());
    let out = std::env::var("HEAP_OUT").unwrap_or_else(|_| "dhat-heap.json".to_owned());

    let (mut relay, mut a, mut b, group) = setup_pair().await;
    let profiler = match side.as_str() {
        "receive" => {
            for i in 0..messages {
                a.send_chat(&mut relay, &group, &format!("m{i}")).await;
            }
            let route = b.route_hex(&group).await;
            let backlog = group_events(&relay, &route);
            assert_eq!(backlog.len(), messages, "one group event per message");
            let profiler = dhat::Profiler::builder()
                .file_name(&out)
                .trim_backtraces(Some(48))
                .build();
            let start = Instant::now();
            for events in backlog.chunks(chunk) {
                let response = b
                    .call(Request::Ingest(IngestRequest {
                        events: events.to_vec(),
                    }))
                    .await;
                assert!(matches!(response, Response::Ok { .. }), "ingest");
                b.ack_all().await;
            }
            let peak_before_drain = dhat::HeapStats::get().max_bytes;
            let drained = b.drain_and_ack().await;
            let received = drained
                .iter()
                .filter(|event| matches!(event, SessionEvent::MessageReceived { .. }))
                .count();
            assert_eq!(received, messages, "B receives every message");
            drop(drained);
            drop(backlog);
            println!(
                "heap_profile: {}",
                json!({
                    "phase": "ingested",
                    "ms": start.elapsed().as_secs_f64() * 1_000.0,
                    "peak_before_drain_bytes": peak_before_drain,
                })
            );
            profiler
        }
        "send" => {
            let profiler = dhat::Profiler::builder()
                .file_name(&out)
                .trim_backtraces(Some(48))
                .build();
            for i in 0..messages {
                a.send_chat(&mut relay, &group, &format!("m{i}")).await;
            }
            drop(a.drain_and_ack().await);
            profiler
        }
        other => panic!("unknown HEAP_SIDE {other}"),
    };
    let stats = dhat::HeapStats::get();
    let profiled = if side == "send" { &a.host } else { &b.host };
    let mirror = mirror_bytes(profiled);
    drop(profiler);
    println!(
        "heap_profile: {}",
        json!({
            "phase": "end",
            "side": side,
            "messages": messages,
            "chunk": chunk,
            "live_bytes": stats.curr_bytes,
            "live_blocks": stats.curr_blocks,
            "peak_bytes": stats.max_bytes,
            "total_bytes": stats.total_bytes,
            "store_mirror_bytes": mirror,
            "dhat_file": out,
        })
    );
    drop((a, b, relay));
}

fn main() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");
    runtime.block_on(run());
}
