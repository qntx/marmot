//! Shared native driver for the `mk-bridge` byte-level tests: an
//! in-memory [`Host`], a fake relay set, a clock and a `LocalSigner` that
//! answers `signer_pending` envelopes — everything a wasm/JavaScript host
//! would implement, driven through raw bytes only.
//!
//! Every decoded error response is scanned for a 64-hex-character run in
//! both its JSON and its `Display` form (the security scan that covers
//! every error this suite produces).

#![allow(
    clippy::cast_lossless,
    clippy::cast_possible_truncation,
    clippy::doc_markdown,
    clippy::excessive_nesting,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::manual_let_else,
    clippy::map_unwrap_or,
    clippy::future_not_send,
    clippy::missing_const_for_fn,
    clippy::missing_docs_in_private_items,
    clippy::missing_fields_in_debug,
    clippy::missing_panics_doc,
    clippy::needless_pass_by_value,
    clippy::panic,
    clippy::shadow_reuse,
    clippy::shadow_unrelated,
    clippy::significant_drop_tightening,
    clippy::tests_outside_test_module,
    clippy::too_many_lines,
    clippy::unused_async,
    clippy::unwrap_in_result,
    clippy::unwrap_used,
    unreachable_pub,
    dead_code,
    reason = "test code"
)]

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use mk_bridge::protocol::*;
use mk_bridge::{Bridge, BridgeError, Host, HostError, Row, encode_rows};
use mk_core::testing::LocalSigner;
use mk_core::{AccountSigner, KvBatch, KvOp};
use serde_json::{Value, json};

pub const RELAY: &str = "wss://relay.example";

/// Whether `text` contains a run of 64+ hex characters — the forbidden
/// leak signature for ids and key material in error surfaces.
pub fn has_hex_run(text: &str) -> bool {
    let mut run = 0usize;
    for ch in text.chars() {
        run = if ch.is_ascii_hexdigit() { run + 1 } else { 0 };
        if run >= 64 {
            return true;
        }
    }
    false
}

/// Scan an error value's JSON and `Display` for the forbidden hex run.
pub fn assert_error_is_clean(error: &BridgeError) {
    let json = serde_json::to_string(error).expect("error encodes");
    assert!(!has_hex_run(&json), "error JSON leaked hex: {json}");
    let display = error.to_string();
    assert!(
        !has_hex_run(&display),
        "error Display leaked hex: {display}"
    );
}

/// Scan every response the driver decodes; errors are leak-checked.
fn scan_response(response: &Response) {
    if let Response::Error { error } = response {
        assert_error_is_clean(error);
    }
}

fn scan_open_response(response: &OpenResponse) {
    if let OpenResponse::Error { error } = response {
        assert_error_is_clean(error);
    }
}

fn scan_drain_response(response: &DrainResponse) {
    if let DrainResponse::Error { error } = response {
        assert_error_is_clean(error);
    }
}

fn apply(rows: &mut BTreeMap<Vec<u8>, Vec<u8>>, batch: &KvBatch) {
    for op in &batch.ops {
        match op {
            KvOp::Put { key, value } => {
                rows.insert(key.clone(), value.clone());
            }
            KvOp::Delete { key } => {
                rows.remove(key);
            }
        }
    }
}

#[derive(Default)]
pub struct HostState {
    /// Every accepted batch, in commit order.
    pub history: Vec<KvBatch>,
    /// When set, commits fail once `history.len() >= n`.
    pub fail_after: Option<usize>,
    /// `load` fails.
    pub fail_load: bool,
    /// The encoded rows the most recent `load` served — this
    /// incarnation's durable baseline.
    pub last_served: Vec<u8>,
}

/// The durable-row view a host must expose for the publish loop's
/// durability invariant: the row keys a crash could not lose.
pub trait DurableKeys {
    /// Every `Put` key durable at `watermark`.
    fn durable_keys(&self, watermark: u64) -> Vec<Vec<u8>>;
}

/// An in-memory host: records batches in commit order and rebuilds the
/// durable row set from them on `load`. A commit failure poisons the
/// bridge, matching an unrecoverable storage error.
#[derive(Clone, Default)]
pub struct MemHost {
    pub state: Arc<Mutex<HostState>>,
}

impl MemHost {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn committed(&self) -> usize {
        self.state.lock().unwrap().history.len()
    }

    /// Fail every commit once `n` batches have been accepted.
    pub fn fail_after(&self, n: usize) {
        self.state.lock().unwrap().fail_after = Some(n);
    }

    /// Fail `load` once.
    pub fn fail_load(&self, fail: bool) {
        self.state.lock().unwrap().fail_load = fail;
    }

    /// Drop every committed batch past `n` — a store that rolled back to a
    /// snapshot. Unlike `load_prefix`, the tail is gone for good: later
    /// commits append at `n`.
    pub fn truncate(&self, n: usize) {
        self.state.lock().unwrap().history.truncate(n);
    }
}

impl DurableKeys for MemHost {
    /// Every `Put` key of committed batches up to `watermark`, plus the
    /// row set this incarnation loaded at `open`.
    fn durable_keys(&self, watermark: u64) -> Vec<Vec<u8>> {
        let state = self.state.lock().unwrap();
        let mut keys: Vec<Vec<u8>> = state
            .history
            .iter()
            .filter(|batch| batch.seq.get() <= watermark)
            .flat_map(|batch| batch.ops.iter())
            .filter_map(|op| match op {
                KvOp::Put { key, .. } => Some(key.clone()),
                KvOp::Delete { .. } => None,
            })
            .collect();
        if let Ok(rows) = mk_bridge::decode_rows(&state.last_served) {
            keys.extend(rows.into_iter().map(|(key, _)| key));
        }
        keys
    }
}

impl Host for MemHost {
    fn load(&mut self) -> Result<Vec<u8>, HostError> {
        let mut state = self.state.lock().unwrap();
        if state.fail_load {
            return Err(HostError::new());
        }
        let mut rows = BTreeMap::new();
        for batch in &state.history {
            apply(&mut rows, batch);
        }
        let rows: Vec<Row> = rows.into_iter().collect();
        let encoded = encode_rows(&rows);
        state.last_served.clone_from(&encoded);
        Ok(encoded)
    }

    fn commit(&mut self, batch: &[u8]) -> Result<(), HostError> {
        let mut state = self.state.lock().unwrap();
        if state
            .fail_after
            .is_some_and(|limit| state.history.len() >= limit)
        {
            return Err(HostError::new());
        }
        let batch = KvBatch::decode(batch).map_err(|_| HostError::new())?;
        state.history.push(batch);
        Ok(())
    }
}

/// An in-memory host that keeps only the compacted current row set — no
/// batch history — so process memory reflects the durable store, not the
/// driver's journal.
#[derive(Clone, Default)]
pub struct CompactHost {
    rows: Arc<Mutex<BTreeMap<Vec<u8>, Vec<u8>>>>,
}

impl CompactHost {
    pub fn new() -> Self {
        Self::default()
    }

    /// A snapshot of the current row set.
    pub fn rows(&self) -> Vec<Row> {
        self.rows
            .lock()
            .unwrap()
            .iter()
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect()
    }
}

impl DurableKeys for CompactHost {
    /// Every row is committed by construction; the watermark is advisory.
    fn durable_keys(&self, _watermark: u64) -> Vec<Vec<u8>> {
        self.rows.lock().unwrap().keys().cloned().collect()
    }
}

impl Host for CompactHost {
    fn load(&mut self) -> Result<Vec<u8>, HostError> {
        let rows: Vec<Row> = self
            .rows
            .lock()
            .unwrap()
            .iter()
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect();
        Ok(encode_rows(&rows))
    }

    fn commit(&mut self, batch: &[u8]) -> Result<(), HostError> {
        let batch = KvBatch::decode(batch).map_err(|_| HostError::new())?;
        apply(&mut self.rows.lock().unwrap(), &batch);
        Ok(())
    }
}

/// What one publish attempt looked like to the fake relay.
#[derive(Clone, Debug)]
pub struct Published {
    /// The outbound item.
    pub item: Outbound,
    /// The bridge's committed watermark at publish time.
    pub watermark: u64,
}

/// An in-process relay set: published events per URL, verdict injection.
#[derive(Default)]
pub struct FakeRelay {
    /// Everything ever handed to `publish`, in order.
    pub publishes: Vec<Published>,
    /// Report every target `rejected`.
    pub reject_all: bool,
    /// Report every target `unavailable` (retryable, never terminal).
    pub down: bool,
}

impl FakeRelay {
    pub fn new() -> Self {
        Self::default()
    }

    /// Store the event under every URL it accepts; returns the per-relay
    /// verdicts to report.
    pub fn publish(&mut self, item: &Outbound, watermark: u64) -> Vec<RelayResult> {
        let results = item
            .relays
            .iter()
            .map(|relay| RelayResult {
                relay: relay.clone(),
                status: if self.reject_all {
                    RelayStatus::Rejected
                } else if self.down {
                    RelayStatus::Unavailable
                } else {
                    RelayStatus::Accepted
                },
            })
            .collect();
        self.publishes.push(Published {
            item: item.clone(),
            watermark,
        });
        results
    }

    /// Published events matching `matches`, in publish order.
    pub fn fetch(&self, matches: impl Fn(&Event) -> bool) -> Vec<Event> {
        self.publishes
            .iter()
            .map(|published| &published.item.event)
            .filter(|event| matches(event))
            .cloned()
            .collect()
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

/// Gift-wrapped Welcome events (kind 1059) addressed to `account_hex`.
pub fn welcomes_for(relay: &FakeRelay, account_hex: &str) -> Vec<Event> {
    relay.fetch(|event| event.kind == 1059 && tag(event, "p") == Some(account_hex))
}

/// Group message events (kind 445) carrying `#h` = `route_hex`.
pub fn group_events(relay: &FakeRelay, route_hex: &str) -> Vec<Event> {
    relay.fetch(|event| event.kind == 445 && tag(event, "h") == Some(route_hex))
}

/// The durability invariant: a published event's id must already sit
/// inside a durable row key — a committed batch at or below the acked
/// watermark, or the rows this incarnation loaded at open (a batch
/// committed before a restart is durable even though its in-session ack
/// died). Keys are host-defined bytes carrying the id raw (the
/// `SessionOutbox` key segment is the event id), so a raw window match
/// suffices; the hex form is accepted too.
pub fn assert_durable(durable: &[Vec<u8>], item: &Outbound) {
    assert!(
        durable.iter().any(|key| contains_id(key, &item.event.id)),
        "published event escaped its gating batch: {}",
        hex::encode(item.event.id)
    );
}

/// Whether `buf` contains `id` as ASCII hex or raw bytes.
pub fn contains_id(buf: &[u8], id: &[u8; 32]) -> bool {
    let event_hex = hex::encode(id);
    buf.windows(event_hex.len())
        .any(|w| w == event_hex.as_bytes())
        || buf.windows(32).any(|w| w == id)
}

/// The newest kind-30443 `KeyPackage` event authored by `account_hex`.
pub fn key_package_of(relay: &FakeRelay, account_hex: &str) -> Event {
    relay
        .fetch(|event| event.kind == 30_443 && event.pubkey == hex_to_32(account_hex))
        .into_iter()
        .next_back()
        .expect("account published a KeyPackage")
}

pub fn hex_to_32(text: &str) -> [u8; 32] {
    let decoded = hex::decode(text).expect("hex id");
    decoded.try_into().expect("32-byte id")
}

/// One account-device bridge under test.
pub struct Member<H: Host = MemHost> {
    pub bridge: Bridge<H>,
    pub host: H,
    pub signer: LocalSigner,
    pub unix_ms: u64,
    pub mono_ms: u64,
    /// The cumulative acked watermark this incarnation has acked — the
    /// highest seq ever passed to `ack_durable`. `awaiting_durability`
    /// reports only still-gated seqs, so a round with nothing new pending
    /// returns `None` even though earlier rounds acked higher.
    pub acked: u64,
}

impl Member<MemHost> {
    /// A fresh bridge over empty storage.
    pub fn new() -> Self {
        Self::with_host(MemHost::new())
    }
}

impl<H> Member<H>
where
    H: Host + DurableKeys + Clone,
{
    /// A fresh bridge over `host`.
    pub fn with_host(host: H) -> Self {
        Self {
            bridge: Bridge::new(host.clone()),
            host,
            signer: LocalSigner::new(),
            unix_ms: 1_700_000_000_000,
            mono_ms: 1_000,
            acked: 0,
        }
    }

    /// The account's hex pubkey.
    pub fn account_hex(&self) -> String {
        hex::encode(self.signer.account().0)
    }

    /// Serialized open config for this member.
    pub fn config(&self) -> Vec<u8> {
        serde_json::to_vec(&Config {
            protocol_version: PROTOCOL_VERSION,
            account: self.signer.account().0,
            now: self.time(),
            allow_loopback: true,
            client_name: None,
        })
        .expect("config encodes")
    }

    fn time(&self) -> Time {
        Time {
            unix_ms: self.unix_ms,
            monotonic_ms: self.mono_ms,
        }
    }

    /// Open, completing the account proof through the signer when asked.
    pub async fn open(&mut self) -> OpenResponse {
        let config = self.config();
        let bytes = self.bridge.open(&config);
        let response: OpenResponse = serde_json::from_slice(&bytes).expect("open decodes");
        scan_open_response(&response);
        match response {
            OpenResponse::NeedsAccountProof { unsigned } => {
                let signed = self
                    .signer
                    .sign_event(unsigned.into())
                    .await
                    .expect("sign proof");
                let response = self
                    .call(Request::SubmitProof(SubmitProofRequest {
                        event: signed.into(),
                    }))
                    .await;
                match response {
                    Response::Ok { .. } => OpenResponse::Ready {
                        account: self.signer.account().0,
                    },
                    other => panic!("submit_proof failed: {other:?}"),
                }
            }
            ready @ OpenResponse::Ready { .. } => ready,
            error => panic!("open failed: {error:?}"),
        }
    }

    /// `open` with arbitrary bytes; decodes the envelope.
    pub fn open_bytes(&mut self, config: &[u8]) -> OpenResponse {
        let bytes = self.bridge.open(config);
        let response: OpenResponse = serde_json::from_slice(&bytes).expect("open decodes");
        scan_open_response(&response);
        response
    }

    /// One `call` with the current clock, decoded. Does NOT answer
    /// `signer_pending` — callers wanting the full client loop use
    /// [`Self::drive`].
    pub fn call_once(&mut self, request: Request) -> Response {
        let envelope = serde_json::to_vec(&CallEnvelope {
            now: self.time(),
            request: serde_json::to_value(&request).expect("request encodes"),
        })
        .expect("envelope encodes");
        self.call_bytes(&envelope)
    }

    /// `call` with raw bytes; decodes and leak-scans the response.
    pub fn call_bytes(&mut self, request: &[u8]) -> Response {
        let bytes = self.bridge.call(request);
        let response: Response = serde_json::from_slice(&bytes).expect("response decodes");
        scan_response(&response);
        response
    }

    /// The host client loop: answer every `signer_pending` with
    /// `LocalSigner` until the call resolves.
    pub async fn call(&mut self, request: Request) -> Response {
        let mut response = self.call_once(request);
        for _ in 0..32 {
            let Response::SignerPending { requests } = &response else {
                return response;
            };
            let mut replies = Vec::with_capacity(requests.len());
            for request in requests {
                replies.push(SignerReply {
                    id: request.id,
                    result: self.answer(&request.op).await,
                });
            }
            response = self.call_once(Request::SignerReply(SignerReplyRequest { replies }));
        }
        panic!("signer loop did not settle")
    }

    pub async fn answer(&self, op: &SignerOp) -> SignerResult {
        match op {
            SignerOp::SignEvent { unsigned } => self
                .signer
                .sign_event(unsigned.clone().into())
                .await
                .map(|event| SignerResult::Signed {
                    event: event.into(),
                })
                .unwrap_or(SignerResult::Failed),
            SignerOp::Nip44Encrypt { peer, plaintext } => self
                .signer
                .nip44_encrypt(&mk_core::AccountId(*peer), plaintext)
                .await
                .map(|text| SignerResult::Text { text })
                .unwrap_or(SignerResult::Failed),
            SignerOp::Nip44Decrypt { peer, ciphertext } => self
                .signer
                .nip44_decrypt(&mk_core::AccountId(*peer), ciphertext)
                .await
                .map(|text| SignerResult::Text { text })
                .unwrap_or(SignerResult::Failed),
        }
    }

    /// Drain the queued session events.
    pub fn drain(&mut self) -> DrainResponse {
        let bytes = self.bridge.drain();
        let response: DrainResponse = serde_json::from_slice(&bytes).expect("drain decodes");
        scan_drain_response(&response);
        response
    }

    /// The events a drain returned (empty on an error envelope).
    pub fn drained_events(&mut self) -> Vec<SessionEvent> {
        match self.drain() {
            DrainResponse::Events { events } => events,
            DrainResponse::Error { error } => panic!("drain failed: {error:?}"),
        }
    }

    /// Advance the injected clock (never regresses).
    pub fn advance(&mut self, ms: u64) {
        self.unix_ms += ms;
        self.mono_ms += ms;
    }

    /// Close and reopen against the same durable host; the session must
    /// come back `ready` — the proof is persisted.
    pub async fn restart(&mut self) {
        self.bridge.close();
        self.bridge = Bridge::new(self.host.clone());
        self.acked = 0;
        let response = self.open().await;
        assert!(
            matches!(response, OpenResponse::Ready { .. }),
            "restart must be ready"
        );
    }

    /// The highest committed seq the session reports behind the gate,
    /// then ack it (the driver's durability contract).
    pub async fn ack_all(&mut self) -> u64 {
        let response = self.call(Request::AwaitingDurability(NoArgs {})).await;
        let Response::Ok { result } = &response else {
            panic!("awaiting_durability failed: {response:?}");
        };
        let OpResult::AwaitingDurability(AwaitingDurabilityResult { seq }) = **result else {
            panic!("awaiting_durability result: {result:?}");
        };
        if let Some(seq) = seq {
            let response = self
                .call(Request::AckDurable(AckDurableRequest { seq }))
                .await;
            assert!(matches!(response, Response::Ok { .. }), "ack {seq}");
            self.acked = self.acked.max(seq);
        }
        self.acked
    }

    /// Fault-tolerant publish loop for fault-injection tests: identical to
    /// [`Self::publish`], including the durability invariant on every item,
    /// but stops and reports `clean = false` on the first error response
    /// (a poisoned bridge) instead of panicking. Returns the
    /// `(item, resolution)` pairs completed before the failure.
    pub async fn try_publish(
        &mut self,
        relay: &mut FakeRelay,
    ) -> (Vec<(Outbound, PublishResolution)>, bool) {
        let mut done = Vec::new();
        for _ in 0..64 {
            // Ack everything outstanding, tolerating a dead bridge.
            let response = self.call(Request::AwaitingDurability(NoArgs {})).await;
            let Response::Ok { result } = response else {
                return (done, false);
            };
            let OpResult::AwaitingDurability(AwaitingDurabilityResult { seq }) = *result else {
                return (done, false);
            };
            if let Some(seq) = seq {
                let response = self
                    .call(Request::AckDurable(AckDurableRequest { seq }))
                    .await;
                if !matches!(response, Response::Ok { .. }) {
                    return (done, false);
                }
                self.acked = self.acked.max(seq);
            }
            let response = self.call(Request::TakeOutbound(NoArgs {})).await;
            let Response::Ok { result } = response else {
                return (done, false);
            };
            let OpResult::Outbound(OutboundResult { items }) = *result else {
                return (done, false);
            };
            if items.is_empty() {
                break;
            }
            let durable = self.host.durable_keys(self.acked);
            for item in items {
                assert_durable(&durable, &item);
                let results = relay.publish(&item, self.acked);
                let id = item.id;
                let response = self
                    .call(Request::ReportPublish(ReportPublishRequest { id, results }))
                    .await;
                let Response::Ok { result } = response else {
                    return (done, false);
                };
                let OpResult::PublishResolution(PublishResolutionResult { resolution }) = *result
                else {
                    return (done, false);
                };
                done.push((item, resolution));
            }
        }
        (done, true)
    }

    /// Release gated outbound, publish every item to `relay`, and report
    /// verdicts back — the host's delivery loop. Returns resolutions per
    /// outbound id.
    pub async fn publish(&mut self, relay: &mut FakeRelay) -> Vec<(String, PublishResolution)> {
        let (done, clean) = self.try_publish(relay).await;
        assert!(clean, "publish loop hit an error response");
        done.into_iter()
            .map(|(item, resolution)| (hex::encode(item.id), resolution))
            .collect()
    }

    /// Publish the account's `KeyPackage` and inbox relay list.
    pub async fn publish_identity(&mut self, relay: &mut FakeRelay) {
        let response = self
            .call(Request::PublishKeyPackage(PublishKeyPackageRequest {
                relays: vec![RELAY.to_owned()],
            }))
            .await;
        assert!(matches!(response, Response::Ok { .. }), "key_package");
        self.publish(relay).await;
        let response = self
            .call(Request::PublishInboxRelays(PublishInboxRelaysRequest {
                relays: vec![RELAY.to_owned()],
            }))
            .await;
        assert!(matches!(response, Response::Ok { .. }), "inbox_relays");
        self.publish(relay).await;
    }

    /// Ingest `events`, then drain and acknowledge message deliveries.
    pub async fn ingest_and_drain(&mut self, events: Vec<Event>) -> Vec<SessionEvent> {
        let response = self.call(Request::Ingest(IngestRequest { events })).await;
        assert!(matches!(response, Response::Ok { .. }), "ingest");
        self.drain_and_ack().await
    }

    /// Drain pending session events and acknowledge every message
    /// delivery — the host's receive contract.
    pub async fn drain_and_ack(&mut self) -> Vec<SessionEvent> {
        let drained = self.drained_events();
        let ids: Vec<[u8; 32]> = drained
            .iter()
            .filter_map(|event| match event {
                SessionEvent::MessageReceived { message, .. } => Some(message.transport_id),
                _ => None,
            })
            .collect();
        if !ids.is_empty() {
            let response = self
                .call(Request::AcknowledgeMessages(AcknowledgeMessagesRequest {
                    ids,
                }))
                .await;
            assert!(matches!(response, Response::Ok { .. }), "ack messages");
        }
        drained
    }

    /// The group's `#h` route id as hex, read via the `group` request.
    pub async fn route_hex(&mut self, group: &[u8]) -> String {
        let response = self
            .call(Request::Group(GroupRequest {
                group: group.to_vec(),
            }))
            .await;
        let Response::Ok { result } = response else {
            panic!("group failed");
        };
        let OpResult::Group(GroupResult {
            group: Some(snapshot),
        }) = *result
        else {
            panic!("group result: {result:?}");
        };
        hex::encode(snapshot.nostr_group_id)
    }

    /// Sync the group: ingest the relay backlog for `route_hex`, tick, and
    /// publish whatever the session emits, until nothing new appears.
    pub async fn sync(&mut self, relay: &mut FakeRelay, group: &[u8]) {
        let route = self.route_hex(group).await;
        for _ in 0..8 {
            let backlog = group_events(relay, &route);
            self.ingest_and_drain(backlog).await;
            let response = self.call(Request::Tick(NoArgs {})).await;
            assert!(matches!(response, Response::Ok { .. }), "tick");
            self.advance(2_000);
            self.publish(relay).await;
        }
    }

    /// Send one chat message and publish it.
    pub async fn send_chat(&mut self, relay: &mut FakeRelay, group: &[u8], text: &str) {
        self.advance(1_000);
        let response = self
            .call(Request::SendMessage(SendMessageRequest {
                group: group.to_vec(),
                message: AppMessage {
                    kind: 9,
                    tags: Vec::new(),
                    content: text.to_owned(),
                },
            }))
            .await;
        assert!(matches!(response, Response::Ok { .. }), "send {text}");
        self.publish(relay).await;
    }
}

/// The `ok` result payload of a response, unwrapped.
pub fn ok_result(response: Response) -> OpResult {
    match response {
        Response::Ok { result } => *result,
        other => panic!("expected ok: {other:?}"),
    }
}

/// The error kind of an error response, unwrapped.
pub fn error_kind(response: &Response) -> String {
    match response {
        Response::Error { error } => serde_json::to_value(error)
            .expect("error encodes")
            .get("kind")
            .and_then(Value::as_str)
            .expect("error has kind")
            .to_owned(),
        other => panic!("expected error: {other:?}"),
    }
}

/// The fixed reason label of an error response, unwrapped.
pub fn error_reason(response: &Response) -> String {
    match response {
        Response::Error { error } => serde_json::to_value(error)
            .expect("error encodes")
            .get("reason")
            .and_then(Value::as_str)
            .expect("error has reason")
            .to_owned(),
        other => panic!("expected error: {other:?}"),
    }
}

/// The error kind of an open error response, unwrapped.
pub fn open_error_kind(response: &OpenResponse) -> String {
    match response {
        OpenResponse::Error { error } => serde_json::to_value(error)
            .expect("error encodes")
            .get("kind")
            .and_then(Value::as_str)
            .expect("error has kind")
            .to_owned(),
        other => panic!("expected error: {other:?}"),
    }
}

/// A raw `call` envelope value for hand-built protocol tests.
pub fn call_envelope(now: Time, request: Value) -> Vec<u8> {
    serde_json::to_vec(&CallEnvelope { now, request }).expect("envelope encodes")
}

/// Build a call envelope whose `request` is an arbitrary JSON value.
pub fn raw_call(now: Time, request: Value) -> Vec<u8> {
    serde_json::to_vec(&json!({"now": now, "request": request})).expect("envelope")
}
