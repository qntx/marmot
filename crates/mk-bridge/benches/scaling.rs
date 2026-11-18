//! Store-scaling measurements over the bridge byte protocol: send cost vs
//! group history, bulk ingest cost vs backlog, and the `full_flow` shape as
//! a fixed reference point. Emits one JSON line per scenario on stdout,
//! prefixed `scaling: `.
//!
//! Knobs (`SCALING_*` env): `SCALING_SCENARIO=send|ingest|single500|all`
//! (default `all`), `SCALING_MAX_HISTORY` sends in the send scenario
//! (default 10000), `SCALING_INGEST` backlog size for ingest (default
//! 10000), `SCALING_REPEAT` single500 repetitions (default 5).

#![allow(
    clippy::cast_lossless,
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss,
    clippy::cast_sign_loss,
    clippy::doc_markdown,
    clippy::excessive_nesting,
    clippy::expect_used,
    clippy::future_not_send,
    clippy::indexing_slicing,
    clippy::manual_let_else,
    clippy::missing_fields_in_debug,
    clippy::missing_panics_doc,
    clippy::needless_pass_by_value,
    clippy::panic,
    clippy::print_stdout,
    clippy::shadow_reuse,
    clippy::shadow_unrelated,
    clippy::too_many_lines,
    clippy::unwrap_used,
    clippy::wildcard_imports,
    unused_crate_dependencies,
    reason = "bench harness: asserts fail loudly, stdout is the report"
)]

#[path = "../tests/support/mod.rs"]
mod support;

use std::collections::BTreeMap;
use std::time::Instant;

use mk_bridge::protocol::{
    AppMessage, CreateGroupRequest, IngestRequest, Invitee, OpResult, Request, Response,
    SendMessageRequest,
};
use mk_bridge::{Host, PROTOCOL_VERSION};
use serde_json::json;
use support::*;

/// Send-window starts (`h` = messages already sent before the window).
const WINDOWS: [usize; 7] = [10, 100, 500, 1_000, 2_000, 5_000, 10_000];
/// Window width in sends.
const WINDOW_SENDS: usize = 21;
/// Events per `ingest` call in the chunked-ingest scenario.
const INGEST_CHUNK: usize = 500;

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|text| text.parse().ok())
        .unwrap_or(default)
}

/// `(median, p10, p90, min, max)` of `samples` (ms).
fn stats(samples: &[f64]) -> serde_json::Value {
    let mut sorted = samples.to_vec();
    sorted.sort_by(f64::total_cmp);
    let pick = |q: f64| sorted[((sorted.len() - 1) as f64 * q).round() as usize];
    json!({
        "n": sorted.len(),
        "median_ms": pick(0.5),
        "p10_ms": pick(0.1),
        "p90_ms": pick(0.9),
        "min_ms": sorted[0],
        "max_ms": sorted[sorted.len() - 1],
    })
}

/// Process RSS in KiB via `ps` (`-o rss=`), or `None` when unavailable.
fn rss_kib() -> Option<u64> {
    let output = std::process::Command::new("ps")
        .args(["-o", "rss=", "-p", &std::process::id().to_string()])
        .output()
        .ok()?;
    String::from_utf8(output.stdout).ok()?.trim().parse().ok()
}

/// Row-set accounting: `(row_count, key+value bytes)` overall and per
/// `key[0]` table tag.
fn row_accounting(host: &CompactHost) -> (usize, u64, BTreeMap<u8, (usize, u64)>) {
    let mut total = 0u64;
    let mut tables: BTreeMap<u8, (usize, u64)> = BTreeMap::new();
    let rows = host.rows();
    for (key, value) in &rows {
        let bytes = (key.len() + value.len()) as u64;
        total += bytes;
        if let Some(&tag) = key.first() {
            let entry = tables.entry(tag).or_default();
            entry.0 += 1;
            entry.1 += bytes;
        }
    }
    (rows.len(), total, tables)
}

/// The `full_flow` setup: A and B open, publish identities, A creates the
/// group inviting B, B joins via the Welcome, both sync.
async fn setup_pair<H>(host_a: H, host_b: H) -> (FakeRelay, Member<H>, Member<H>, Vec<u8>)
where
    H: Host + DurableKeys + Clone,
{
    let mut relay = FakeRelay::new();
    let mut a = Member::with_host(host_a);
    let mut b = Member::with_host(host_b);
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

/// One timed `send_message` (the bridge call, incl. signer round-trips);
/// the publish half of the send runs untimed afterwards.
async fn timed_send<H>(a: &mut Member<H>, relay: &mut FakeRelay, group: &[u8], i: usize) -> f64
where
    H: Host + DurableKeys + Clone,
{
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

/// The send-cost-vs-history scenario on a live `(A, B)` pair: `max`
/// consecutive sends, windowed stats at every [`WINDOWS`] start that fits.
async fn send_on(
    relay: &mut FakeRelay,
    a: &mut Member<CompactHost>,
    group: &[u8],
    max: usize,
) -> Vec<f64> {
    let mut times = Vec::with_capacity(max);
    for i in 0..max {
        times.push(timed_send(a, relay, group, i).await);
    }
    let windows: Vec<serde_json::Value> = WINDOWS
        .into_iter()
        .filter(|h| h + WINDOW_SENDS <= max)
        .map(|h| {
            json!({
                "h": h,
                "stats": stats(&times[h..h + WINDOW_SENDS]),
            })
        })
        .collect();
    println!(
        "scaling: {}",
        json!({"scenario": "send", "max": max, "windows": windows})
    );
    times
}

/// The chunked bulk-ingest scenario on a live `(A, B)` pair: B ingests
/// `backlog` in `ingest` calls of at most [`INGEST_CHUNK`] events, then
/// drains and acknowledges once.
async fn ingest_on(b: &mut Member<CompactHost>, backlog: Vec<mk_bridge::protocol::Event>) {
    let calls = backlog.len().div_ceil(INGEST_CHUNK);
    let rss_before = rss_kib();
    let wall = Instant::now();
    let mut per_call = Vec::with_capacity(calls);
    for chunk in backlog.chunks(INGEST_CHUNK) {
        let start = Instant::now();
        let response = b
            .call(Request::Ingest(IngestRequest {
                events: chunk.to_vec(),
            }))
            .await;
        per_call.push(start.elapsed().as_secs_f64() * 1_000.0);
        assert!(matches!(response, Response::Ok { .. }), "ingest");
    }
    let drained = b.drain_and_ack().await;
    let total_ms = wall.elapsed().as_secs_f64() * 1_000.0;
    let rss_after = rss_kib();
    let (row_count, total_bytes, tables) = row_accounting(&b.host);
    let table_stats: Vec<serde_json::Value> = tables
        .iter()
        .map(|(tag, (count, bytes))| json!({"table": tag, "rows": count, "bytes": bytes}))
        .collect();
    let received = received_messages(&drained).len();
    println!(
        "scaling: {}",
        json!({
            "scenario": "ingest",
            "events": backlog.len(),
            "calls": calls,
            "received": received,
            "total_ms": total_ms,
            "per_call_ms": stats(&per_call),
            "b_rows": row_count,
            "b_bytes": total_bytes,
            "b_tables": table_stats,
            "rss_before_kib": rss_before,
            "rss_after_kib": rss_after,
        })
    );
}

/// `received` message contents out of a drained event list.
fn received_messages(events: &[mk_bridge::protocol::SessionEvent]) -> Vec<String> {
    use mk_bridge::protocol::SessionEvent;
    events
        .iter()
        .filter_map(|event| match event {
            SessionEvent::MessageReceived { message, .. } => Some(message.content.clone()),
            _ => None,
        })
        .collect()
}

/// `full_flow`'s exact shape: fresh members, A sends 500, B ingests the
/// whole backlog in one call and drains. Returns B's ingest+drain ms.
async fn single500_once() -> f64 {
    let (mut relay, mut a, mut b, group) = setup_pair(MemHost::new(), MemHost::new()).await;
    for i in 0..500 {
        a.advance(1_000);
        let response = a
            .call(Request::SendMessage(SendMessageRequest {
                group: group.clone(),
                message: AppMessage {
                    kind: 9,
                    tags: Vec::new(),
                    content: format!("m{i}"),
                },
            }))
            .await;
        assert!(matches!(response, Response::Ok { .. }), "send {i}");
        a.publish(&mut relay).await;
    }
    let route = b.route_hex(&group).await;
    let backlog = group_events(&relay, &route);
    let start = Instant::now();
    let drained = b.ingest_and_drain(backlog).await;
    let ms = start.elapsed().as_secs_f64() * 1_000.0;
    assert_eq!(received_messages(&drained).len(), 500, "B got all 500");
    ms
}

async fn run() {
    let scenario = std::env::var("SCALING_SCENARIO").unwrap_or_else(|_| "all".to_owned());
    let max = env_usize("SCALING_MAX_HISTORY", 10_000);
    let ingest_n = env_usize("SCALING_INGEST", 10_000);
    let repeat = env_usize("SCALING_REPEAT", 5);
    let _ = PROTOCOL_VERSION;

    match scenario.as_str() {
        "send" => {
            let (mut relay, mut a, _b, group) =
                setup_pair(CompactHost::new(), CompactHost::new()).await;
            send_on(&mut relay, &mut a, &group, max).await;
        }
        "ingest" => {
            let (mut relay, mut a, mut b, group) =
                setup_pair(CompactHost::new(), CompactHost::new()).await;
            for i in 0..ingest_n {
                a.advance(1_000);
                a.send_chat(&mut relay, &group, &format!("m{i}")).await;
            }
            let route = b.route_hex(&group).await;
            let backlog = group_events(&relay, &route);
            ingest_on(&mut b, backlog).await;
        }
        "single500" => {
            let mut runs = Vec::with_capacity(repeat);
            for _ in 0..repeat {
                runs.push(single500_once().await);
            }
            println!(
                "scaling: {}",
                json!({
                    "scenario": "single500",
                    "runs_ms": runs,
                    "median_ms": stats(&runs)["median_ms"],
                })
            );
        }
        "all" => {
            let (mut relay, mut a, mut b, group) =
                setup_pair(CompactHost::new(), CompactHost::new()).await;
            send_on(&mut relay, &mut a, &group, max).await;
            let route = b.route_hex(&group).await;
            let backlog = group_events(&relay, &route);
            ingest_on(&mut b, backlog).await;
            let mut runs = Vec::with_capacity(repeat);
            for _ in 0..repeat {
                runs.push(single500_once().await);
            }
            println!(
                "scaling: {}",
                json!({
                    "scenario": "single500",
                    "runs_ms": runs,
                    "median_ms": stats(&runs)["median_ms"],
                })
            );
        }
        other => panic!("unknown SCALING_SCENARIO {other}"),
    }
}

fn main() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");
    runtime.block_on(run());
}
