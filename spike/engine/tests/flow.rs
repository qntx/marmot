//! M0 native flow test: MDK `cgka-engine` + `transport-nostr-peeler` over the
//! KV storage provider, three members, in-process fake relay.
//!
//! Run: `cargo test --release -- --nocapture` (the summary table is printed).

use std::collections::BTreeSet;

use cgka_traits::app_event::MarmotAppEvent;
use cgka_traits::engine::{CgkaEngine, SendIntent};
use cgka_traits::ingest::{IngestOutcome, InputRejectionCategory};
use cgka_traits::types::GroupId;
use marmot_spike::harness::*;
use transport_nostr_peeler::{KIND_MARMOT_GROUP_MESSAGE, NostrTransportEvent};

const MSG_PER_SENDER: usize = 250;

#[derive(Default)]
struct IngestCounts {
    processed: usize,
    buffered: usize,
    deferred: usize,
    ignored_own_echo: usize,
    ignored_other: usize,
    local_state: usize,
    stale: usize,
    rejected: usize,
    other: usize,
}

impl IngestCounts {
    fn add(&mut self, outcome: &IngestOutcome) {
        match outcome {
            IngestOutcome::Processed => self.processed += 1,
            IngestOutcome::Buffered { .. } => self.buffered += 1,
            IngestOutcome::TransportDeferred { .. } => self.deferred += 1,
            IngestOutcome::Ignored {
                category: InputRejectionCategory::OwnEcho,
            } => self.ignored_own_echo += 1,
            IngestOutcome::Ignored { .. } => self.ignored_other += 1,
            IngestOutcome::LocalState { .. } => self.local_state += 1,
            IngestOutcome::Stale { .. } => self.stale += 1,
            IngestOutcome::Rejected { .. } => self.rejected += 1,
            _ => self.other += 1,
        }
    }

    fn line(&self) -> String {
        format!(
            "processed={} buffered={} deferred={} own_echo={} ignored={} local={} stale={} rejected={} other={}",
            self.processed,
            self.buffered,
            self.deferred,
            self.ignored_own_echo,
            self.ignored_other,
            self.local_state,
            self.stale,
            self.rejected,
            self.other
        )
    }
}

/// Ingest a fetched backlog into `reader`, then drive convergence so deferred
/// or buffered input can resolve. Returns (per-ingest counts, delivered
/// decoded contents, duplicates re-delivered).
async fn read_backlog(
    reader: &mut Member,
    relay_events: Vec<NostrTransportEvent>,
    relay: &FakeRelay,
    gid: &GroupId,
) -> (IngestCounts, BTreeSet<String>, usize) {
    let mut counts = IngestCounts::default();
    for event in &relay_events {
        let outcome = ingest_event(reader, event).await;
        counts.add(&outcome);
    }
    // Give deferred/buffered work its recovery passes.
    flush_outbound(reader, gid, relay).await;
    let mut seen = BTreeSet::new();
    let mut dups = 0usize;
    for (_, _, payload) in reader.drain_received() {
        let text = decode_chat(&payload);
        if !seen.insert(text) {
            dups += 1;
        }
    }
    (counts, seen, dups)
}

fn decode_chat(payload: &[u8]) -> String {
    MarmotAppEvent::decode(payload)
        .map(|e| e.content)
        .unwrap_or_else(|_| format!("<raw:{}>", payload.len()))
}

async fn send_chat(member: &mut Member, relay: &FakeRelay, gid: &GroupId, tag: &str, seq: u64) {
    let payload = chat_payload(member, seq, &format!("{tag}-{seq:04}"));
    let result = member
        .engine
        .send(SendIntent::AppMessage {
            group_id: gid.clone(),
            payload,
            expected_epoch: None,
        })
        .await
        .expect("send");
    publish_send_result(member, result, relay).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn m0_native_flow() {
    let relay = FakeRelay::new();
    let mut table: Vec<String> = Vec::new();
    let row = |step: &str, detail: String| {
        let line = format!("| {step:<58} | {detail} |");
        println!("{line}");
        line
    };

    // ── Step 1: accounts, KeyPackage publication (kind 30443) ───────────
    let mut a = Member::fresh("A", "alice");
    let mut b = Member::fresh("B", "bob");
    let mut c = Member::fresh("C", "carol");

    let a_kp_event = publish_key_package(&mut a, &relay).await;
    let b_kp_event = publish_key_package(&mut b, &relay).await;
    let _c_kp_event = publish_key_package(&mut c, &relay).await;
    table.push(row(
        "1. key packages published (kind 30443)",
        "A B C".into(),
    ));
    assert_eq!(a_kp_event.kind, KIND_MARMOT_KEY_PACKAGE);
    assert_eq!(
        relay
            .fetch_key_packages(&b.signer.keys().public_key().to_hex())
            .len(),
        1
    );

    // ── Step 2: A creates group, invites B (Welcome = kind 444 rumor in a
    // NIP-59 gift wrap kind 1059) ───────────────────────────────────────
    let b_kp = key_package_from_event(&b_kp_event);
    let gid = create_group(&mut a, &relay, "m0-spike", vec![b_kp]).await;

    let b_welcome_outcomes = ingest_welcomes(&mut b, &relay).await;
    assert_eq!(b_welcome_outcomes.len(), 1, "B got exactly one welcome");
    assert!(matches!(b_welcome_outcomes[0], IngestOutcome::Processed));

    let a_members = a.engine.members(&gid).expect("a members");
    let b_members = b.engine.members(&gid).expect("b members");
    assert_eq!(a_members.len(), 2, "A sees both members");
    assert_eq!(b_members.len(), 2, "B sees both members");
    assert_eq!(
        a.engine.epoch(&gid).expect("a epoch"),
        b.engine.epoch(&gid).expect("b epoch"),
        "same epoch"
    );
    assert_eq!(
        a_members
            .iter()
            .map(|m| m.id.as_slice().to_vec())
            .collect::<BTreeSet<_>>(),
        b_members
            .iter()
            .map(|m| m.id.as_slice().to_vec())
            .collect::<BTreeSet<_>>(),
        "same membership"
    );
    b.drain_events();
    a.drain_events();
    table.push(row(
        "2. group created, B joined via welcome",
        format!("epoch={} members=2", a.engine.epoch(&gid).unwrap()),
    ));

    // ── Step 3: 500 application messages, publish first, read backlog ───
    let base = 1_700_000_000u64;
    for i in 0..MSG_PER_SENDER {
        send_chat(&mut a, &relay, &gid, "a", base + i as u64).await;
    }
    for i in 0..MSG_PER_SENDER {
        send_chat(&mut b, &relay, &gid, "b", base + i as u64).await;
    }
    let group_events = relay.fetch_ascending(KIND_MARMOT_GROUP_MESSAGE, None, None);
    assert_eq!(
        group_events.len(),
        MSG_PER_SENDER * 2,
        "relay holds all 500 group events"
    );
    table.push(row("3. published 500 kind-445 messages", "500".into()));

    // B's restart dump BEFORE reading the backlog: this is also step 4's
    // persisted-state rebuild.
    let b_dump = b.host.dump();
    let b_signer = b.signer.clone();
    drop(b);

    // Reader 1: restarted B ingests newest-first (what a naive REQ would
    // feed the engine).
    let mut b_newest = Member::restart(&b_dump, "B", b_signer.clone());
    let newest_events = relay.fetch_newest_first(KIND_MARMOT_GROUP_MESSAGE, None, None);
    let (newest_counts, newest_delivered, newest_dups) =
        read_backlog(&mut b_newest, newest_events, &relay, &gid).await;
    table.push(row(
        "3a. B-after-restart reads backlog NEWEST-FIRST",
        format!(
            "{} delivered={} dups={}",
            newest_counts.line(),
            newest_delivered.len(),
            newest_dups
        ),
    ));

    // Reader 2: same restart dump, ascending (created_at, id) — the SDK path.
    let mut b_ascending = Member::restart(&b_dump, "B", b_signer.clone());
    let (asc_counts, asc_delivered, asc_dups) =
        read_backlog(&mut b_ascending, group_events.clone(), &relay, &gid).await;
    table.push(row(
        "3b. B-after-restart reads backlog ASCENDING",
        format!(
            "{} delivered={} dups={}",
            asc_counts.line(),
            asc_delivered.len(),
            asc_dups
        ),
    ));

    // B must receive all 250 of A's messages; B's own 250 are own-echoes.
    let expected_a: BTreeSet<String> = (0..MSG_PER_SENDER)
        .map(|i| format!("a-{:04}", base + i as u64))
        .collect();
    assert_eq!(
        asc_delivered, expected_a,
        "ascending read delivers every A message exactly once"
    );
    assert_eq!(asc_dups, 0, "no re-delivery on ascending path");
    if newest_delivered != expected_a {
        println!(
            "!! FINDING: newest-first backlog lost {} of A's messages",
            expected_a.len() - newest_delivered.len()
        );
    }

    // The ascending-path reader becomes B for the rest of the scenario.
    let mut b = b_ascending;
    table.push(row(
        "4. restart from persisted KV mid-way",
        format!(
            "batches={} post-restart msgs readable",
            b.host.batch_count()
        ),
    ));

    // A reads B's 250 too (its own 250 are echoes).
    let mut a_counts = IngestCounts::default();
    for event in &group_events {
        a_counts.add(&ingest_event(&mut a, event).await);
    }
    let mut a_received = BTreeSet::new();
    for (_, _, payload) in a.drain_received() {
        a_received.insert(decode_chat(&payload));
    }
    let expected_b: BTreeSet<String> = (0..MSG_PER_SENDER)
        .map(|i| format!("b-{:04}", base + i as u64))
        .collect();
    assert_eq!(a_received, expected_b, "A reads all of B's messages");
    table.push(row(
        "4b. A reads the same backlog ascending",
        format!("{} delivered={}", a_counts.line(), a_received.len()),
    ));

    // ── Step 5: A adds C; three-way messages ────────────────────────────
    let c_kp = key_package_from_event(
        &relay.fetch_key_packages(&c.signer.keys().public_key().to_hex())[0],
    );
    let invite = a
        .engine
        .send(SendIntent::Invite {
            group_id: gid.clone(),
            key_packages: vec![c_kp],
            initial_admins: Vec::new(),
        })
        .await
        .expect("invite C");
    publish_send_result(&mut a, invite, &relay).await;

    let c_outcomes = ingest_welcomes(&mut c, &relay).await;
    assert!(
        c_outcomes
            .iter()
            .any(|o| matches!(o, IngestOutcome::Processed)),
        "C joins via welcome: {c_outcomes:?}"
    );
    c.drain_events();

    // B ingests the add commit so its epoch matches. Under distributed
    // convergence the commit arrives `Buffered`; it applies when B's
    // convergence drain runs below.
    let h_hex = group_events[0].tag_value("h").expect("h tag").to_string();
    for event in relay.fetch_ascending(KIND_MARMOT_GROUP_MESSAGE, Some(&h_hex), None) {
        ingest_event(&mut b, &event).await;
    }
    b.drain_received();
    a.drain_received();

    for (m, tag, seq) in [
        (&mut a, "post-c-a", 1u64),
        (&mut b, "post-c-b", 2),
        (&mut c, "post-c-c", 3),
    ] {
        send_chat(m, &relay, &gid, tag, base + seq).await;
    }
    // Sends that return `Queued` regenerate out of the convergence drain —
    // flush every sender before readers consume the backlog.
    for m in [&mut a, &mut b, &mut c] {
        flush_outbound(m, &gid, &relay).await;
    }
    // Each reader expects the OTHER two members' messages: an engine never
    // re-delivers a sender's own echo (OwnEcho), that echo is an app concern.
    // C's 500 pre-join events stay deferred forever — it can never read them;
    // that is a count we report, not a delivery failure.
    let mut three_way_counts = Vec::new();
    let mut three_way: Vec<BTreeSet<String>> = Vec::new();
    let h_events = relay.fetch_ascending(KIND_MARMOT_GROUP_MESSAGE, Some(&h_hex), None);
    for m in [&mut a, &mut b, &mut c] {
        let (counts, delivered, _) = read_backlog(m, h_events.clone(), &relay, &gid).await;
        three_way_counts.push(counts);
        three_way.push(delivered);
    }
    let expected_3way: Vec<BTreeSet<String>> = vec![
        [
            format!("post-c-b-{:04}", base + 2),
            format!("post-c-c-{:04}", base + 3),
        ]
        .into_iter()
        .collect(),
        [
            format!("post-c-a-{:04}", base + 1),
            format!("post-c-c-{:04}", base + 3),
        ]
        .into_iter()
        .collect(),
        [
            format!("post-c-a-{:04}", base + 1),
            format!("post-c-b-{:04}", base + 2),
        ]
        .into_iter()
        .collect(),
    ];
    for (idx, set) in three_way.iter().enumerate() {
        let missing: Vec<_> = expected_3way[idx].difference(set).collect();
        println!(
            "three-way member {idx}: {} missing={missing:?}",
            three_way_counts[idx].line()
        );
        assert!(
            expected_3way[idx].is_subset(set),
            "member {idx} sees all three post-add messages: {missing:?}"
        );
    }
    assert_eq!(
        c.engine.members(&gid).unwrap().len(),
        3,
        "C sees all three members"
    );
    table.push(row(
        "5. A added C; three-way messages readable",
        format!("epoch={} members=3", c.engine.epoch(&gid).unwrap()),
    ));

    // ── Step 6: A removes B; post-removal message unreadable by B ───────
    let b_id = b.member_id();
    let remove = a
        .engine
        .send(SendIntent::RemoveMembers {
            group_id: gid.clone(),
            members: vec![b_id.clone()],
        })
        .await
        .expect("remove B");
    publish_send_result(&mut a, remove, &relay).await;
    // Like every commit-producing send under quiescence, RemoveMembers can
    // come back Queued; the drain regenerates and publishes it.
    flush_outbound(&mut a, &gid, &relay).await;

    for m in [&mut a, &mut b, &mut c] {
        for event in relay.fetch_ascending(KIND_MARMOT_GROUP_MESSAGE, Some(&h_hex), None) {
            ingest_event(m, &event).await;
        }
        // Inbound commits settle through convergence, not at ingest.
        flush_outbound(m, &gid, &relay).await;
    }
    let b_events = b.drain_events();
    assert_eq!(
        a.engine.members(&gid).unwrap().len(),
        2,
        "A sees 2 members after removal"
    );
    assert_eq!(c.engine.members(&gid).unwrap().len(), 2);
    let b_removed = !b_events.is_empty()
        || b.engine.epoch(&gid).is_err()
        || b.engine.members(&gid).map(|m| m.len()).unwrap_or(0) == 0;
    assert!(b_removed, "B observes its removal ({b_events:?})");

    let relay_len_before = relay.len();
    send_chat(&mut a, &relay, &gid, "after-removal", base + 9).await;
    flush_outbound(&mut a, &gid, &relay).await;
    let post_removal = relay.fetch_ascending(KIND_MARMOT_GROUP_MESSAGE, Some(&h_hex), None);
    assert!(
        relay.len() > relay_len_before,
        "A's post-removal message reached the relay"
    );
    // B is removed: none of the post-removal events may deliver the payload.
    let mut b_post = IngestCounts::default();
    for event in &post_removal {
        b_post.add(&ingest_event(&mut b, event).await);
    }
    flush_outbound(&mut b, &gid, &relay).await;
    for event in &post_removal {
        ingest_event(&mut c, event).await;
    }
    flush_outbound(&mut c, &gid, &relay).await;
    let b_last: Vec<String> = b
        .drain_received()
        .into_iter()
        .map(|(_, _, p)| decode_chat(&p))
        .collect();
    let c_last: Vec<String> = c
        .drain_received()
        .into_iter()
        .map(|(_, _, p)| decode_chat(&p))
        .collect();
    let expected_text = format!("after-removal-{:04}", base + 9);
    assert!(
        !b_last.contains(&expected_text),
        "post-removal message must not be readable by B: {}",
        b_post.line()
    );
    // A authored it; the engine never re-delivers an own echo, so only C's
    // delivery proves readability for remaining members.
    assert!(
        c_last.contains(&expected_text),
        "C reads post-removal message"
    );
    table.push(row(
        "6. A removed B; post-removal msg unreadable by B",
        format!("b_ingest={} members=2", b_post.line()),
    ));

    // ── Step 7: publish failure → publish_failed → retry ────────────────
    relay.fail_next_publish();
    let update = a
        .engine
        .send(SendIntent::SelfUpdate {
            group_id: gid.clone(),
        })
        .await
        .expect("self-update send");
    let failed_report = publish_send_result(&mut a, update, &relay).await;
    assert!(
        failed_report.pending.is_some() && !failed_report.confirmed,
        "commit staged and publish_failed ran"
    );
    // Retry the same intent: the engine re-stages a fresh commit.
    let retry = a
        .engine
        .send(SendIntent::SelfUpdate {
            group_id: gid.clone(),
        })
        .await
        .expect("self-update retry");
    let retry_report = publish_send_result(&mut a, retry, &relay).await;
    assert!(retry_report.confirmed, "retry confirmed");
    let epoch_after = a.engine.epoch(&gid).expect("epoch after retry");
    for event in relay.fetch_ascending(KIND_MARMOT_GROUP_MESSAGE, Some(&h_hex), None) {
        ingest_event(&mut c, &event).await;
    }
    flush_outbound(&mut c, &gid, &relay).await;
    assert_eq!(
        c.engine.epoch(&gid).expect("c epoch"),
        epoch_after,
        "C converged to the retried epoch"
    );
    table.push(row(
        "7. publish_failed + retry keeps group consistent",
        format!("epoch={epoch_after}"),
    ));

    // ── Summary ─────────────────────────────────────────────────────────
    let relay_size = relay.len();
    println!();
    println!("=== M0 native flow summary ===");
    for line in &table {
        println!("{line}");
    }
    println!("relay events stored: {relay_size}");
    println!("newest-first vs ascending finding: see 3a/3b rows above");
}
