//! Large-group reproduction over the bridge byte protocol: one creator and
//! `N - 1` invitees, every member a real bridge session over its own host,
//! all publishing through one in-memory relay. Per `N` it records the
//! create time, each send's outcome and time, the first `queued` send, and —
//! once a send queues — whether continued tick + publish rounds ever
//! release it, with per-tick time and `next_wake_ms` (one idle tick when
//! nothing queued). Emits one JSON line
//! per `N` on stdout, prefixed `large_group: `.
//!
//! A test-only `tracing` subscriber counts the engine's
//! `queue_outbound_intent` `queue_reason` field, so every queued send
//! carries the engine's own classification.
//!
//! Knobs (`LG_*` env): `LG_N` comma-separated member counts (default
//! `50,100,130,200`), `LG_SENDS` consecutive sends (default 20),
//! `LG_ROUNDS` tick/publish rounds once a send queues (default 40),
//! `LG_ROUND_MS` simulated ms per round (default 15000), `LG_JOIN=0` skips
//! the invitees' Welcome joins, `LG_ECHO=0` stops the creator ingesting
//! its own group events back from the relay, `LG_RECEIVE` group events
//! one joined invitee ingests in a single call after the sends (default
//! 500, `0` skips).
//!
//! `MlsGroup::load` calls on the creator are counted per phase of the send
//! loop (the send call, its publish, its relay echo) through
//! `mk_core::testing::openmls_group_loads`; the bench runs on one thread.

#![allow(
    clippy::cast_lossless,
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss,
    clippy::cast_sign_loss,
    clippy::cognitive_complexity,
    clippy::doc_markdown,
    clippy::excessive_nesting,
    clippy::expect_used,
    clippy::future_not_send,
    clippy::indexing_slicing,
    clippy::manual_let_else,
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
    reason = "bench harness: asserts fail loudly and stdout is the report"
)]

#[path = "../tests/support/mod.rs"]
mod support;

use std::time::Instant;

use mk_bridge::protocol::{
    AppMessage, CreateGroupRequest, IngestRequest, Invitee, MessageSentResult, NoArgs, OpResult,
    Request, SendMessageRequest, SendOutcome, SessionEvent, TickedResult,
};
use mk_core::testing::openmls_group_loads;
use serde_json::{Value, json};
use support::*;

/// Counts the engine's queue reasons (test-only instrumentation).
mod reasons {
    use std::collections::BTreeMap;
    use std::fmt;
    use std::sync::Mutex;

    use tracing::field::{Field, Visit};
    use tracing::span::{Attributes, Id, Record};
    use tracing::{Event, Metadata, Subscriber};

    static COUNTS: Mutex<BTreeMap<String, u64>> = Mutex::new(BTreeMap::new());
    /// `retry_deferred_peels` `metadata_loaded` phases: `(duration_ms, backlog)`.
    static METADATA: Mutex<Vec<(u64, u64)>> = Mutex::new(Vec::new());

    pub(super) struct Counter;

    #[derive(Default)]
    struct Fields {
        reason: Option<String>,
        phase: Option<String>,
        duration_ms: Option<u64>,
        backlog: Option<u64>,
    }

    impl Visit for Fields {
        fn record_str(&mut self, field: &Field, value: &str) {
            match field.name() {
                "queue_reason" => self.reason = Some(value.to_owned()),
                "phase" => self.phase = Some(value.to_owned()),
                _ => {}
            }
        }

        fn record_u64(&mut self, field: &Field, value: u64) {
            match field.name() {
                "duration_ms" => self.duration_ms = Some(value),
                "backlog" => self.backlog = Some(value),
                _ => {}
            }
        }

        fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
            if field.name() == "queue_reason" {
                self.reason = Some(format!("{value:?}"));
            }
        }
    }

    impl Subscriber for Counter {
        fn enabled(&self, metadata: &Metadata<'_>) -> bool {
            metadata.target() == "cgka_engine::message_processor"
        }

        fn new_span(&self, _: &Attributes<'_>) -> Id {
            Id::from_u64(1)
        }

        fn record(&self, _: &Id, _: &Record<'_>) {}

        fn record_follows_from(&self, _: &Id, _: &Id) {}

        fn event(&self, event: &Event<'_>) {
            let mut fields = Fields::default();
            event.record(&mut fields);
            if let Some(reason) = fields.reason {
                *COUNTS.lock().unwrap().entry(reason).or_default() += 1;
            }
            if fields.phase.as_deref() == Some("metadata_loaded") {
                METADATA.lock().unwrap().push((
                    fields.duration_ms.unwrap_or_default(),
                    fields.backlog.unwrap_or_default(),
                ));
            }
        }

        fn enter(&self, _: &Id) {}

        fn exit(&self, _: &Id) {}
    }

    /// Drain the counts since the previous call.
    pub(super) fn take() -> BTreeMap<String, u64> {
        std::mem::take(&mut *COUNTS.lock().unwrap())
    }

    /// Drain the `metadata_loaded` phases since the previous call.
    pub(super) fn take_metadata() -> Vec<(u64, u64)> {
        std::mem::take(&mut *METADATA.lock().unwrap())
    }
}

/// Row count, total and largest value bytes per table tag (`keys.rs`
/// first key byte) in `host` — which rows a full-table decode touches.
fn table_stats(host: &CompactHost) -> Value {
    let mut stats: std::collections::BTreeMap<u8, (u64, u64, u64)> =
        std::collections::BTreeMap::new();
    for (key, value) in host.rows() {
        let Some(tag) = key.first() else { continue };
        let entry = stats.entry(*tag).or_default();
        entry.0 += 1;
        entry.1 += value.len() as u64;
        entry.2 = entry.2.max(value.len() as u64);
    }
    stats
        .into_iter()
        .filter(|(_, (_, bytes, _))| *bytes >= 64 * 1024)
        .map(|(tag, (rows, bytes, max))| {
            (
                tag.to_string(),
                json!({ "rows": rows, "bytes": bytes, "max": max }),
            )
        })
        .collect::<serde_json::Map<_, _>>()
        .into()
}

/// `(max duration_ms, max backlog, count)` over captured metadata phases.
fn metadata_summary(phases: &[(u64, u64)]) -> Value {
    json!({
        "phases": phases.len(),
        "max_duration_ms": phases.iter().map(|p| p.0).max(),
        "max_backlog": phases.iter().map(|p| p.1).max(),
    })
}

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name).map_or(default, |value| value.parse().expect(name))
}

fn env_flag(name: &str) -> bool {
    std::env::var(name).map_or(true, |value| value != "0")
}

fn ms_since(start: Instant) -> f64 {
    start.elapsed().as_secs_f64() * 1_000.0
}

fn median(samples: &[f64]) -> Option<f64> {
    let mut sorted = samples.to_vec();
    sorted.sort_by(f64::total_cmp);
    sorted.get(sorted.len() / 2).copied()
}

/// The creator's kind-445 events on the relay.
fn creator_group_events(relay: &FakeRelay, route: &str) -> usize {
    group_events(relay, route).len()
}

/// Ingest group events the creator has not seen yet (its relay echo).
async fn echo(creator: &mut Member<CompactHost>, relay: &FakeRelay, route: &str, seen: &mut usize) {
    let events = group_events(relay, route);
    if events.len() > *seen {
        let fresh = events[*seen..].to_vec();
        *seen = events.len();
        creator
            .call(Request::Ingest(IngestRequest { events: fresh }))
            .await;
        creator.drain_and_ack().await;
    }
}

async fn tick(creator: &mut Member<CompactHost>) -> (f64, Option<u64>) {
    let start = Instant::now();
    let response = creator.call(Request::Tick(NoArgs {})).await;
    let ms = ms_since(start);
    let OpResult::Ticked(TickedResult { next_wake_ms }) = ok_result(response) else {
        panic!("tick result");
    };
    (ms, next_wake_ms)
}

async fn run_n(n: usize) -> Value {
    let sends = env_usize("LG_SENDS", 20);
    let rounds = env_usize("LG_ROUNDS", 40);
    let round_ms = env_usize("LG_ROUND_MS", 15_000) as u64;
    let join = env_flag("LG_JOIN");
    let echo_on = env_flag("LG_ECHO");
    let receive = env_usize("LG_RECEIVE", 500);

    let mut relay = FakeRelay::new();
    let setup = Instant::now();
    let mut members: Vec<Member<CompactHost>> = Vec::with_capacity(n);
    for _ in 0..n {
        let mut member = Member::with_host(CompactHost::new());
        member.indexed_publish = true;
        member.max_signer_rounds = usize::MAX;
        member.open().await;
        member.publish_identity(&mut relay).await;
        members.push(member);
    }
    let setup_ms = ms_since(setup);
    let mut creator = members.remove(0);
    let invitees: Vec<Invitee> = members
        .iter()
        .map(|member| Invitee {
            key_package: key_package_of(&relay, &member.account_hex()),
            inbox_relays: vec![RELAY.to_owned()],
        })
        .collect();

    reasons::take();
    let rounds_before = creator.signer_rounds;
    let start = Instant::now();
    let response = creator
        .call(Request::CreateGroup(CreateGroupRequest {
            name: "large".to_owned(),
            description: String::new(),
            relays: vec![RELAY.to_owned()],
            invitees,
            admins: Vec::new(),
            components: Vec::new(),
        }))
        .await;
    let create_ms = ms_since(start);
    let create_signer_rounds = creator.signer_rounds - rounds_before;
    let OpResult::GroupCreated(created) = ok_result(response) else {
        panic!("create_group result");
    };
    let group = created.group.id.clone();
    let start = Instant::now();
    let create_publishes = creator.publish(&mut relay).await.len();
    let create_publish_ms = ms_since(start);
    let route = creator.route_hex(&group).await;

    let mut join_ms = Vec::new();
    if join {
        for member in &mut members {
            let welcomes = welcomes_for(&relay, &member.account_hex());
            let start = Instant::now();
            member
                .call(Request::Ingest(IngestRequest { events: welcomes }))
                .await;
            member.drain_and_ack().await;
            join_ms.push(ms_since(start));
        }
    }
    let mut seen = 0usize;
    if echo_on {
        echo(&mut creator, &relay, &route, &mut seen).await;
    }
    let create_reasons = reasons::take();
    reasons::take_metadata();
    let rows_after_create = table_stats(&creator.host);
    let events_before_sends = creator_group_events(&relay, &route);

    let mut loads = [0usize; 3];
    let mut send_ms = Vec::with_capacity(sends);
    let mut outcomes = Vec::with_capacity(sends);
    let mut first_queued = None;
    let mut queued = 0usize;
    for i in 0..sends {
        creator.advance(1_000);
        let loads_at = openmls_group_loads();
        let start = Instant::now();
        let response = creator
            .call(Request::SendMessage(SendMessageRequest {
                group: group.clone(),
                message: AppMessage {
                    kind: 9,
                    tags: Vec::new(),
                    content: format!("m{i}"),
                },
            }))
            .await;
        send_ms.push(ms_since(start));
        loads[0] += openmls_group_loads() - loads_at;
        let OpResult::MessageSent(MessageSentResult { outcome, .. }) = ok_result(response) else {
            panic!("send result");
        };
        let label = match outcome {
            SendOutcome::Staged { .. } => "staged",
            SendOutcome::Queued => {
                queued += 1;
                first_queued.get_or_insert(i);
                "queued"
            }
            SendOutcome::NoChange => "no_change",
            SendOutcome::LeaveRequested => "leave_requested",
            SendOutcome::DisbandRequested => "disband_requested",
            SendOutcome::Unmapped => "unmapped",
        };
        outcomes.push(label);
        let loads_at = openmls_group_loads();
        creator.publish(&mut relay).await;
        loads[1] += openmls_group_loads() - loads_at;
        if echo_on {
            let loads_at = openmls_group_loads();
            echo(&mut creator, &relay, &route, &mut seen).await;
            loads[2] += openmls_group_loads() - loads_at;
        }
    }
    let receipt = if join && receive > 0 && !members.is_empty() {
        let backlog: Vec<_> = group_events(&relay, &route)
            .into_iter()
            .skip(events_before_sends)
            .take(receive)
            .collect();
        let events = backlog.len();
        let receiver = &mut members[0];
        let loads_at = openmls_group_loads();
        let start = Instant::now();
        let drained = receiver.ingest_and_drain(backlog).await;
        let receive_ms = ms_since(start);
        let received = drained
            .iter()
            .filter(|event| matches!(event, SessionEvent::MessageReceived { .. }))
            .count();
        assert_eq!(received, events, "the receiver gets every message");
        json!({
            "events": events,
            "receive_ms": receive_ms,
            "group_loads": openmls_group_loads() - loads_at,
        })
    } else {
        Value::Null
    };
    let send_reasons = reasons::take();
    let send_metadata = metadata_summary(&reasons::take_metadata());
    let rows_after_sends = table_stats(&creator.host);
    let before_release = creator_group_events(&relay, &route);

    let mut ticks = Vec::new();
    let mut released_after = None;
    let target = before_release + queued;
    let rounds = if queued > 0 { rounds } else { 1 };
    for round in 0..rounds {
        let (ms, next_wake_ms) = tick(&mut creator).await;
        creator.advance(round_ms);
        creator.publish(&mut relay).await;
        if echo_on {
            echo(&mut creator, &relay, &route, &mut seen).await;
        }
        let published = creator_group_events(&relay, &route);
        ticks.push(json!({
            "round": round,
            "tick_ms": ms,
            "next_wake_ms": next_wake_ms,
            "group_events": published,
        }));
        if queued > 0 && published >= target {
            released_after = Some(round + 1);
            break;
        }
    }
    let release_reasons = reasons::take();
    let release_metadata = metadata_summary(&reasons::take_metadata());
    let tick_ms: Vec<f64> = ticks
        .iter()
        .filter_map(|tick| tick["tick_ms"].as_f64())
        .collect();

    json!({
        "n": n,
        "join": join,
        "echo": echo_on,
        "setup_ms": setup_ms,
        "create_ms": create_ms,
        "create_signer_rounds": create_signer_rounds,
        "create_publish_ms": create_publish_ms,
        "create_publishes": create_publishes,
        "join_median_ms": median(&join_ms),
        "join_total_ms": join_ms.iter().sum::<f64>(),
        "create_queue_reasons": create_reasons,
        "sends": sends,
        "outcomes": outcomes,
        "first_queued": first_queued,
        "queued": queued,
        "send_queue_reasons": send_reasons,
        "send_metadata_loaded": send_metadata,
        "rows_after_create": rows_after_create,
        "rows_after_sends": rows_after_sends,
        "send_ms": send_ms,
        "send_median_ms": median(&send_ms),
        "send_first_ms": send_ms.first(),
        "send_group_loads": loads[0],
        "publish_group_loads": loads[1],
        "echo_group_loads": loads[2],
        "group_loads_per_send": loads[0] as f64 / sends.max(1) as f64,
        "receive": receipt,
        "released_after_rounds": released_after,
        "release_rounds_run": ticks.len(),
        "tick_median_ms": median(&tick_ms),
        "release_queue_reasons": release_reasons,
        "release_metadata_loaded": release_metadata,
        "ticks": ticks,
    })
}

async fn run() {
    let counts = std::env::var("LG_N").unwrap_or_else(|_| "50,100,130,200".to_owned());
    for n in counts.split(',') {
        let n: usize = n.trim().parse().expect("LG_N");
        let report = run_n(n).await;
        println!("large_group: {report}");
    }
}

fn main() {
    tracing::subscriber::set_global_default(reasons::Counter).expect("subscriber");
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");
    runtime.block_on(run());
}
