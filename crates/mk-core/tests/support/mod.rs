//! Shared in-process harness for the `mk-core` session flow tests: a
//! `FakeRelay`, a `Member` wrapping the public `Session` API, and publish /
//! ingest helpers that mimic what a host app would do.

#![allow(
    clippy::missing_docs_in_private_items,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::missing_panics_doc,
    clippy::needless_pass_by_value,
    clippy::tests_outside_test_module,
    clippy::shadow_reuse,
    clippy::doc_markdown,
    clippy::significant_drop_tightening,
    unreachable_pub,
    clippy::unused_async,
    dead_code,
    reason = "test harness code; each consumer uses a subset"
)]

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use mk_core::testing::{LocalSigner, ManualClock, RecordingHost};
use mk_core::{
    AccountSigner, DestinationPolicy, Event, GroupId, Opened, Outbound, RelayResult, RelayStatus,
    Session, SessionConfig, SessionEvent,
};

/// An in-process relay set: published events per URL, tag/kind filtered
/// fetches.
#[derive(Debug, Default)]
pub struct FakeRelay {
    events: Mutex<BTreeMap<String, Vec<Event>>>,
}

impl FakeRelay {
    pub fn new() -> Self {
        Self::default()
    }

    /// Store `event` under every URL in `relays` it accepts; returns the
    /// per-relay results to report back to the session.
    pub fn publish(&self, outbound: &Outbound) -> Vec<RelayResult> {
        let mut results = Vec::new();
        let mut events = self.events.lock().unwrap();
        for relay in &outbound.relays {
            let stored = events.entry(relay.clone()).or_default();
            if !stored.iter().any(|event| event.id == outbound.event.id) {
                stored.push(outbound.event.clone());
            }
            results.push(RelayResult {
                relay: relay.clone(),
                status: RelayStatus::Accepted,
            });
        }
        results
    }

    /// Newest-first events matching every predicate across all URLs.
    pub fn fetch<F>(&self, relay: &str, matches: F) -> Vec<Event>
    where
        F: Fn(&Event) -> bool,
    {
        let events = self.events.lock().unwrap();
        let mut found: Vec<Event> = events
            .get(relay)
            .into_iter()
            .flatten()
            .filter(|event| matches(event))
            .cloned()
            .collect();
        found.sort_by(|a, b| b.created_at.cmp(&a.created_at).then(b.id.cmp(&a.id)));
        found
    }
}

fn tag<'a>(event: &'a Event, name: &str) -> Option<&'a str> {
    event
        .tags
        .iter()
        .find(|tag| tag.first().map(String::as_str) == Some(name))
        .and_then(|tag| tag.get(1))
        .map(String::as_str)
}

/// The group's `#h` route id as hex, read through a member's snapshot.
pub fn route_hex(member: &Member, group: &GroupId) -> String {
    hex::encode(
        member
            .session
            .group(group)
            .expect("group")
            .expect("group exists")
            .nostr_group_id,
    )
}

/// Group message events (kind 445) carrying `#h` = the nostr route hex.
pub fn group_events(relay: &FakeRelay, url: &str, wanted: &str) -> Vec<Event> {
    let wanted = wanted.to_owned();
    relay.fetch(url, move |event| {
        event.kind == 445 && tag(event, "h") == Some(wanted.as_str())
    })
}

/// Gift-wrapped Welcome events (kind 1059) addressed to `account`.
pub fn welcomes_for(relay: &FakeRelay, url: &str, account_hex: &str) -> Vec<Event> {
    relay.fetch(url, move |event| {
        event.kind == 1059 && tag(event, "p") == Some(account_hex)
    })
}

/// The newest kind-`30443` `KeyPackage` event authored by `account`, if
/// any was published.
pub fn maybe_key_package_of(relay: &FakeRelay, url: &str, account_hex: &str) -> Option<Event> {
    relay
        .fetch(url, move |event| {
            event.kind == 30_443 && event.pubkey.to_hex() == account_hex
        })
        .into_iter()
        .next()
}

/// The newest kind-`30443` `KeyPackage` event authored by `account`.
pub fn key_package_of(relay: &FakeRelay, url: &str, account_hex: &str) -> Event {
    maybe_key_package_of(relay, url, account_hex).expect("account published a KeyPackage")
}

/// The relay URLs an account's newest kind-10050 lists.
pub fn inbox_relays_of(relay: &FakeRelay, url: &str, account_hex: &str) -> Vec<String> {
    relay
        .fetch(url, move |event| {
            event.kind == 10_050 && event.pubkey.to_hex() == account_hex
        })
        .into_iter()
        .next()
        .map(|event| {
            event
                .tags
                .iter()
                .filter(|tag| tag.first().map(String::as_str) == Some("relay"))
                .filter_map(|tag| tag.get(1).cloned())
                .collect()
        })
        .unwrap_or_default()
}

/// One account-device session under test.
pub struct Member {
    pub signer: Arc<LocalSigner>,
    pub clock: Arc<ManualClock>,
    pub host: Arc<RecordingHost>,
    pub session: Session,
}

impl Member {
    pub fn account_hex(&self) -> String {
        self.signer.account().to_hex()
    }

    /// The group's `#h` route id as hex, for `group_events`.
    pub fn route_hex(&self, group: &GroupId) -> String {
        route_hex(self, group)
    }
}

/// The shared `SessionConfig` used by [`open_member`] / [`reopen_member`].
pub fn session_config(
    signer: Arc<LocalSigner>,
    clock: Arc<ManualClock>,
    host: Arc<RecordingHost>,
    rows: Vec<(Vec<u8>, Vec<u8>)>,
) -> SessionConfig {
    SessionConfig {
        signer,
        host,
        rows,
        clock,
        destination_policy: DestinationPolicy {
            allow_loopback: true,
        },
        client_name: None,
    }
}

/// Open a fresh member, completing the account-proof handshake.
pub async fn open_member() -> Member {
    let signer = Arc::new(LocalSigner::new());
    let clock = Arc::new(ManualClock::new());
    let host = Arc::new(RecordingHost::new());
    let opened = Session::open(session_config(
        Arc::clone(&signer),
        Arc::clone(&clock),
        Arc::clone(&host),
        Vec::new(),
    ))
    .expect("open");
    let session = match opened {
        Opened::NeedsAccountProof(pending) => {
            let signed = signer
                .sign_event(pending.request().clone())
                .await
                .expect("sign");
            pending.provide_account_proof(&signed).expect("proof")
        }
        Opened::Ready(session) => *session,
    };
    Member {
        signer,
        clock,
        host,
        session,
    }
}

/// Reopen the member from its durable rows (a restart).
pub async fn reopen_member(member: &Member) -> Member {
    let rows = member.host.durable_rows();
    mk_core::testing::check_message_index(&rows).expect("message index consistent before reopen");
    let opened = Session::open(session_config(
        Arc::clone(&member.signer),
        Arc::clone(&member.clock),
        Arc::clone(&member.host),
        rows,
    ))
    .expect("reopen");
    let session = match opened {
        Opened::Ready(session) => *session,
        Opened::NeedsAccountProof(_) => panic!("restart must not need a proof"),
    };
    Member {
        signer: Arc::clone(&member.signer),
        clock: Arc::clone(&member.clock),
        host: Arc::clone(&member.host),
        session,
    }
}

/// Drain the session's gated outbound: acknowledge the highest pending seq,
/// take the outbound, publish to `relay`, and report the results. Returns
/// the resolutions keyed by outbound id. Loops until nothing remains so
/// regenerated intents also publish.
pub async fn flush(member: &mut Member, relay: &FakeRelay) {
    for _ in 0..64 {
        if let Some(seq) = member.session.awaiting_durability() {
            member.session.acknowledge_durable(seq);
        }
        let outbound = member.session.take_outbound().expect("take_outbound");
        if outbound.is_empty() {
            break;
        }
        for item in outbound {
            let results = relay.publish(&item);
            member
                .session
                .report_publish(&item.id, &results)
                .await
                .expect("report_publish");
        }
        if let Some(seq) = member.session.awaiting_durability() {
            member.session.acknowledge_durable(seq);
        }
    }
}

/// Publish a member's `KeyPackage` + inbox relay list to `url`.
pub async fn publish_identity(member: &mut Member, relay: &FakeRelay, url: &str) {
    member
        .session
        .publish_key_package(&[url.to_owned()])
        .await
        .expect("key package");
    flush(member, relay).await;
    member
        .session
        .publish_inbox_relays(&[url.to_owned()])
        .await
        .expect("inbox relays");
    flush(member, relay).await;
}

/// Ingest `events` in relay order (newest-first input is fine; the session
/// sorts a batch), ack every delivered message (pending-app-event retention
/// is bounded, like a host clearing its UI buffer), and return the drained
/// `SessionEvent`s.
pub async fn ingest_backlog(member: &mut Member, events: &[Event]) -> Vec<SessionEvent> {
    member
        .session
        .ingest_batch(events.to_vec())
        .await
        .expect("ingest_batch");
    let drained = member.session.drain_events().unwrap();
    let ids: Vec<_> = drained
        .iter()
        .filter_map(|event| match event {
            SessionEvent::MessageReceived { message, .. } => Some(message.transport_id),
            _ => None,
        })
        .collect();
    if !ids.is_empty() {
        member
            .session
            .acknowledge_messages(&ids)
            .expect("acknowledge_messages");
    }
    drained
}

/// Application messages a member has delivered but not yet acked.
pub fn received(events: &[SessionEvent]) -> Vec<String> {
    events
        .iter()
        .filter_map(|event| match event {
            SessionEvent::MessageReceived { message, .. } => Some(message.content.clone()),
            _ => None,
        })
        .collect()
}

/// Fetch + ingest the group's newest-first backlog, then drive convergence
/// over several rounds so deferred input and queued intents settle.
pub async fn sync_member(
    member: &mut Member,
    relay: &FakeRelay,
    url: &str,
    group: &GroupId,
) -> Vec<SessionEvent> {
    let mut all = Vec::new();
    for _ in 0..6 {
        let backlog = group_events(relay, url, &member.route_hex(group));
        let _reports = member
            .session
            .ingest_batch(backlog)
            .await
            .expect("ingest_batch");
        let drained = member.session.drain_events().unwrap();
        let ids: Vec<_> = drained
            .iter()
            .filter_map(|event| match event {
                SessionEvent::MessageReceived { message, .. } => Some(message.transport_id),
                _ => None,
            })
            .collect();
        if !ids.is_empty() {
            member.session.acknowledge_messages(&ids).expect("ack");
        }
        all.extend(drained);
        member.session.tick().await.expect("tick");
        // Convergence cutoffs are time-based; advance the injected clock
        // like a real host's timer would.
        member.clock.advance(2_000);
        flush(member, relay).await;
    }
    all
}

/// Send one chat message and publish it. Advances the member's clock so
/// `created_at` reflects send order (relays serve backlogs chronologically).
pub async fn send_chat(member: &mut Member, relay: &FakeRelay, group: &GroupId, text: &str) {
    member.clock.advance(1_000);
    member
        .session
        .send_message(
            group,
            mk_core::AppMessage {
                kind: 9,
                tags: Vec::new(),
                content: text.to_owned(),
            },
        )
        .await
        .expect("send_message");
    flush(member, relay).await;
}
