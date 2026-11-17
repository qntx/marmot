//! White Noise (`wn`/`wnd`) ↔ `mk-core` interop driving the marmot side
//! only through `mk-bridge` bytes — the same protocol surface wasm and C
//! hosts use. Open/call/drain are byte envelopes; signer ops are answered
//! with `LocalSigner` over `signer_reply`; publishing goes through
//! `take_outbound`/`report_publish` to the loopback relay; inbound events
//! arrive via `ingest`.

#![allow(
    clippy::missing_docs_in_private_items,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::missing_panics_doc,
    clippy::needless_pass_by_value,
    clippy::doc_markdown,
    clippy::excessive_nesting,
    clippy::too_many_lines,
    clippy::cast_possible_truncation,
    clippy::str_to_string,
    clippy::shadow_unrelated,
    clippy::shadow_reuse,
    clippy::tests_outside_test_module,
    clippy::items_after_statements,
    clippy::clone_on_ref_ptr,
    clippy::significant_drop_tightening,
    clippy::future_not_send,
    unreachable_pub,
    reason = "scenario tests: asserts are the assertions, unwraps fail loudly"
)]

use interop as _;

mod support;

use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use mk_bridge::protocol::{
    self, AckDurableRequest, AcknowledgeMessagesRequest, AppMessage, DrainResponse, GroupRequest,
    IngestRequest, NoArgs, OpResult, OpenResponse, PublishInboxRelaysRequest,
    PublishKeyPackageRequest, RelayStatus, ReportPublishRequest, Request, Response,
    SendMessageRequest, SessionEvent, SignerOp, SignerReply, SignerReplyRequest, SignerResult,
    SubmitProofRequest,
};
use mk_bridge::{Bridge, Host, HostError, PROTOCOL_VERSION, encode_rows};
use mk_core::testing::LocalSigner;
use mk_core::{AccountId, AccountSigner, Clock, KvBatch, KvOp};
use nostr::prelude::{Event as NostrEvent, PublicKey, RelayUrl, ToBech32};
use support::*;
use tokio::time::sleep;

/// Marmot inner app-event kind for chat.
const CHAT_KIND: u16 = 9;

/// Synchronous in-memory host. `commit` applies the batch and records its
/// seq; the member acknowledges committed seqs so the durability gate
/// releases outbound work immediately.
#[derive(Clone, Default)]
struct MemHost {
    state: Arc<Mutex<MemState>>,
}

#[derive(Default)]
struct MemState {
    rows: BTreeMap<Vec<u8>, Vec<u8>>,
    committed: Vec<u64>,
}

impl Host for MemHost {
    fn load(&mut self) -> Result<Vec<u8>, HostError> {
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        Ok(encode_rows(
            &state
                .rows
                .iter()
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect::<Vec<_>>(),
        ))
    }

    fn commit(&mut self, batch: &[u8]) -> Result<(), HostError> {
        let batch = KvBatch::decode(batch).map_err(|_| HostError::new())?;
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        for op in batch.ops {
            match op {
                KvOp::Put { key, value } => {
                    state.rows.insert(key, value);
                }
                KvOp::Delete { key } => {
                    state.rows.remove(&key);
                }
            }
        }
        state.committed.push(batch.seq.get());
        Ok(())
    }
}

impl MemHost {
    /// The highest committed seq, or none before the first commit.
    fn max_committed(&self) -> Option<u64> {
        self.state
            .lock()
            .expect("mem host poisoned")
            .committed
            .last()
            .copied()
    }
}

/// A member driven exclusively through `Bridge` byte calls.
struct BridgeMember {
    name: &'static str,
    signer: Arc<LocalSigner>,
    clock: WallClock,
    host: MemHost,
    bridge: Bridge<MemHost>,
    routes_seen: BTreeSet<String>,
    account: [u8; 32],
}

impl BridgeMember {
    /// Open a fresh bridge session, answering the account proof if asked.
    async fn open(name: &'static str, signer: Arc<LocalSigner>) -> Self {
        let host = MemHost::default();
        let bridge = Bridge::new(host.clone());
        let account = signer.account().0;
        let clock = WallClock::new();
        let config = protocol::Config {
            protocol_version: PROTOCOL_VERSION,
            account,
            now: clock.now().into(),
            allow_loopback: true,
            client_name: None,
        };
        let mut member = Self {
            name,
            signer,
            clock,
            host,
            bridge,
            routes_seen: BTreeSet::new(),
            account,
        };
        match decode_open(&member.bridge.open(&encode_config(&config))) {
            OpenResponse::Ready { .. } => {}
            OpenResponse::NeedsAccountProof { unsigned } => {
                let signer = member.signer.clone();
                let event = Self::sign(&signer, unsigned.into()).await;
                match member
                    .call(Request::SubmitProof(SubmitProofRequest { event }))
                    .await
                {
                    OpResult::Ready(_) => {}
                    other => panic!("{name}: submit_proof: {other:?}"),
                }
            }
            OpenResponse::Error { error } => panic!("{name}: open: {error:?}"),
        }
        member.ack_committed();
        member
    }

    /// One `call` envelope round-trip, no signer handling.
    fn raw(&mut self, request: &Request) -> Response {
        let envelope = serde_json::json!({
            "now": protocol::Time::from(self.clock.now()),
            "request": serde_json::to_value(request).expect("request encodes"),
        });
        decode_response(
            &self
                .bridge
                .call(&serde_json::to_vec(&envelope).expect("envelope encodes")),
        )
    }

    /// `call` plus `signer_reply` retries until the op resolves.
    async fn call(&mut self, mut request: Request) -> OpResult {
        for _ in 0..32 {
            match self.raw(&request) {
                Response::Ok { result } => return *result,
                Response::SignerPending { requests } => {
                    let signer = self.signer.clone();
                    let mut replies = Vec::with_capacity(requests.len());
                    for pending in requests {
                        let result = match pending.op {
                            SignerOp::SignEvent { unsigned } => SignerResult::Signed {
                                event: Self::sign(&signer, unsigned.into()).await,
                            },
                            SignerOp::Nip44Encrypt { peer, plaintext } => {
                                let peer = AccountId(peer);
                                signer
                                    .nip44_encrypt(&peer, &plaintext)
                                    .await
                                    .map_or(SignerResult::Failed, |text| SignerResult::Text {
                                        text,
                                    })
                            }
                            SignerOp::Nip44Decrypt { peer, ciphertext } => {
                                let peer = AccountId(peer);
                                signer
                                    .nip44_decrypt(&peer, &ciphertext)
                                    .await
                                    .map_or(SignerResult::Failed, |text| SignerResult::Text {
                                        text,
                                    })
                            }
                        };
                        replies.push(SignerReply {
                            id: pending.id,
                            result,
                        });
                    }
                    request = Request::SignerReply(SignerReplyRequest { replies });
                }
                Response::Error { error } => panic!("{}: {error:?}", self.name),
            }
        }
        panic!("{}: signer loop did not converge", self.name);
    }

    async fn sign(signer: &LocalSigner, unsigned: mk_core::UnsignedEvent) -> protocol::Event {
        signer
            .sign_event(unsigned)
            .await
            .expect("sign_event")
            .into()
    }

    /// Acknowledge every committed batch so the durability gate releases
    /// staged outbound work.
    fn ack_committed(&mut self) {
        let Some(max) = self.host.max_committed() else {
            return;
        };
        let OpResult::AwaitingDurability(awaiting) =
            self.call_blocking(Request::AwaitingDurability(NoArgs {}))
        else {
            panic!("{}: awaiting_durability: unexpected", self.name);
        };
        if awaiting.seq.is_some() {
            match self.raw(&Request::AckDurable(AckDurableRequest { seq: max })) {
                Response::Ok { .. } => {}
                other => panic!("{}: ack_durable {max}: {other:?}", self.name),
            }
        }
    }

    /// `call` for requests that never suspend on the signer.
    fn call_blocking(&mut self, request: Request) -> OpResult {
        match self.raw(&request) {
            Response::Ok { result } => *result,
            other => panic!("{}: {other:?}", self.name),
        }
    }

    fn drain_events(&mut self) -> Vec<SessionEvent> {
        match decode_drain(&self.bridge.drain()) {
            DrainResponse::Events { events } => events,
            DrainResponse::Error { error } => panic!("{}: drain: {error:?}", self.name),
        }
    }

    /// Ack durable batches, publish everything `take_outbound` releases and
    /// report verdicts, until the outbox is empty.
    async fn flush(&mut self, rig: &Rig) {
        for _ in 0..64 {
            self.ack_committed();
            let OpResult::Outbound(outbound) = self.call(Request::TakeOutbound(NoArgs {})).await
            else {
                panic!("{}: take_outbound: unexpected", self.name);
            };
            if outbound.items.is_empty() {
                return;
            }
            for item in outbound.items {
                let event: mk_core::Event = item.event.clone().into();
                let nostr_event = NostrEvent::from_json(event.to_json()).expect("outbound json");
                let urls: Vec<RelayUrl> = item
                    .relays
                    .iter()
                    .map(|relay| RelayUrl::parse(relay).expect("relay url"))
                    .collect();
                let output = rig
                    .client
                    .send_event(&nostr_event)
                    .to(urls.iter().cloned())
                    .await
                    .expect("send_event");
                let results = item
                    .relays
                    .iter()
                    .map(|relay| {
                        let url = RelayUrl::parse(relay).expect("relay url");
                        let status = if output.success.contains_key(&url) {
                            RelayStatus::Accepted
                        } else if output.failed.contains_key(&url) {
                            RelayStatus::Rejected
                        } else {
                            RelayStatus::Unavailable
                        };
                        protocol::RelayResult {
                            relay: relay.clone(),
                            status,
                        }
                    })
                    .collect();
                match self
                    .call(Request::ReportPublish(ReportPublishRequest {
                        id: item.id,
                        results,
                    }))
                    .await
                {
                    OpResult::PublishResolution(_) => {}
                    other => panic!("{}: report_publish: {other:?}", self.name),
                }
            }
        }
        panic!("{}: outbox never drained", self.name);
    }

    /// Stage the KeyPackage and inbox relay list, then publish both.
    async fn publish_identity(&mut self, rig: &Rig) {
        match self
            .call(Request::PublishKeyPackage(PublishKeyPackageRequest {
                relays: vec![rig.url.clone()],
            }))
            .await
        {
            OpResult::KeyPackage(_) => {}
            other => panic!("{}: publish_key_package: {other:?}", self.name),
        }
        match self
            .call(Request::PublishInboxRelays(PublishInboxRelaysRequest {
                relays: vec![rig.url.clone()],
            }))
            .await
        {
            OpResult::InboxRelays(_) => {}
            other => panic!("{}: publish_inbox_relays: {other:?}", self.name),
        }
        self.flush(rig).await;
    }

    /// Ingest gift-wrapped welcomes, `tick`, drain and publish anything the
    /// ingest staged. Returns the drained session events.
    async fn sync_welcomes(&mut self, rig: &Rig) -> Vec<SessionEvent> {
        let fetched = fetch_welcomes(rig, &self.account_hex()).await;
        self.ingest_wire(rig, fetched).await
    }

    /// Fetch the backlog of every known group route, `ingest`, `tick`,
    /// drain, acknowledge delivered messages and publish anything staged.
    /// Returns the drained session events.
    async fn sync(&mut self, rig: &Rig) -> Vec<SessionEvent> {
        self.refresh_routes();
        let routes: Vec<String> = self.routes_seen.iter().cloned().collect();
        let mut fetched = Vec::new();
        for route in routes {
            fetched.extend(fetch_group(rig, &route).await);
        }
        self.ingest_wire(rig, fetched).await
    }

    /// `ingest` + `tick` + `drain` + message acks + `flush`.
    async fn ingest_wire(&mut self, rig: &Rig, fetched: Vec<mk_core::Event>) -> Vec<SessionEvent> {
        if !fetched.is_empty() {
            let events = fetched.into_iter().map(protocol::Event::from).collect();
            match self.call(Request::Ingest(IngestRequest { events })).await {
                OpResult::Ingested(_) => {}
                other => panic!("{}: ingest: {other:?}", self.name),
            }
        }
        self.call_blocking(Request::Tick(NoArgs {}));
        let drained = self.drain_events();
        let ids: Vec<[u8; 32]> = drained
            .iter()
            .filter_map(|event| match event {
                SessionEvent::MessageReceived { message, .. } => Some(message.transport_id),
                _ => None,
            })
            .collect();
        if !ids.is_empty() {
            self.call_blocking(Request::AcknowledgeMessages(AcknowledgeMessagesRequest {
                ids,
            }));
        }
        self.flush(rig).await;
        self.refresh_routes();
        drained
    }

    /// Union every live group's `#h` route into `routes_seen` so routes stay
    /// fetchable after a removal ends membership.
    fn refresh_routes(&mut self) {
        if let OpResult::Groups(result) = self.call_blocking(Request::Groups(NoArgs {})) {
            for group in result.groups {
                self.routes_seen.insert(hex::encode(group.nostr_group_id));
            }
        }
    }

    fn snapshot(&mut self, gid: &[u8]) -> Option<Box<protocol::GroupSnapshot>> {
        match self.call_blocking(Request::Group(GroupRequest {
            group: gid.to_vec(),
        })) {
            OpResult::Group(result) => result.group,
            _ => None,
        }
    }

    fn roster(&mut self, gid: &[u8]) -> Vec<String> {
        self.snapshot(gid).map_or_else(Vec::new, |snapshot| {
            snapshot
                .members
                .iter()
                .map(|member| hex::encode(member.account))
                .collect()
        })
    }

    async fn send(&mut self, rig: &Rig, gid: &[u8], content: &str) {
        match self
            .call(Request::SendMessage(SendMessageRequest {
                group: gid.to_vec(),
                message: AppMessage {
                    kind: CHAT_KIND,
                    tags: Vec::new(),
                    content: content.to_owned(),
                },
            }))
            .await
        {
            OpResult::SendOutcome(_) => {}
            other => panic!("{}: send_message: {other:?}", self.name),
        }
        self.flush(rig).await;
    }

    fn account_hex(&self) -> String {
        hex::encode(self.account)
    }

    fn npub(&self) -> String {
        PublicKey::from_slice(&self.account)
            .expect("account pubkey")
            .to_bech32()
            .expect("npub")
    }
}

fn encode_config(value: &protocol::Config) -> Vec<u8> {
    serde_json::to_vec(value).expect("config encodes")
}

fn decode_open(bytes: &[u8]) -> OpenResponse {
    serde_json::from_slice(bytes).expect("open response decodes")
}

fn decode_response(bytes: &[u8]) -> Response {
    serde_json::from_slice(bytes).expect("response decodes")
}

fn decode_drain(bytes: &[u8]) -> DrainResponse {
    serde_json::from_slice(bytes).expect("drain decodes")
}

/// `until_member`, but for `BridgeMember`. Not `Send`: `Bridge` holds
/// suspended-op futures that are single-threaded.
async fn until_bridge<T, F>(member: &mut BridgeMember, mut f: F, what: &str, ms: u64) -> T
where
    F: for<'m> FnMut(&'m mut BridgeMember) -> Pin<Box<dyn Future<Output = Option<T>> + 'm>>,
{
    let start = Instant::now();
    loop {
        if let Some(value) = f(&mut *member).await {
            return value;
        }
        assert!(
            start.elapsed() < Duration::from_millis(ms),
            "timeout waiting for {what}"
        );
        sleep(Duration::from_millis(800)).await;
    }
}

/// `wn` CLI helper (same shape as `wn_group_id` in `wn.rs`).
fn wn_group_id(created: &serde_json::Value) -> String {
    created
        .pointer("/result/group_id")
        .and_then(serde_json::Value::as_str)
        .or_else(|| {
            created
                .pointer("/result/id")
                .and_then(serde_json::Value::as_str)
        })
        .expect("wn group id")
        .to_owned()
}

/// `WN → us`: wn creates a group and invites us; we join, exchange a
/// message each way, and observe our removal — every local action driven
/// through `mk-bridge` request/response bytes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs wn/wnd from the pinned MDK rev; see CONTRIBUTING"]
async fn scenario7_bridge_wn_invites_us() {
    let start = Instant::now();
    let rig = Arc::new(rig("s7").await);
    let mut a = BridgeMember::open("A", Arc::new(LocalSigner::new())).await;
    a.publish_identity(&rig).await;
    let a_npub = a.npub();
    let a_hex = a.account_hex();

    let created = wn(
        &rig.home,
        &rig.socket,
        Some(&rig.wn_npub),
        &["groups", "create", "interop-7", &a_npub],
    );
    let wn_gid = wn_group_id(&created);
    let gid = hex::decode(&wn_gid).expect("wn group id hex");

    // The Welcome arrives gift-wrapped at our inbox relay; joining under
    // wn's group id proves both sides share the same MLS group id.
    until_bridge(
        &mut a,
        |a| {
            let rig = Arc::clone(&rig);
            let gid = gid.clone();
            Box::pin(async move {
                let events = a.sync_welcomes(&rig).await;
                events
                    .iter()
                    .any(|event| {
                        matches!(event, SessionEvent::GroupJoined { group } if *group == gid)
                    })
                    .then_some(())
            })
        },
        "welcome join",
        90_000,
    )
    .await;

    until_bridge(
        &mut a,
        |a| {
            let rig = Arc::clone(&rig);
            let gid = gid.clone();
            let wn_pub = rig.wn_pub.clone();
            Box::pin(async move {
                let _ = a.sync(&rig).await;
                a.roster(&gid).contains(&wn_pub).then_some(())
            })
        },
        "mk roster under wn group id",
        90_000,
    )
    .await;
    let roster = a.roster(&gid);
    assert_eq!(roster.len(), 2, "roster {roster:?}");
    assert!(roster.contains(&rig.wn_pub), "roster contains wn");
    assert!(roster.contains(&a_hex), "roster contains self");
    sleep(Duration::from_millis(WND_SETTLE_MS)).await;

    // wn says hello; the bridge member decodes it.
    wn_send(&rig, &wn_gid, "hello-from-wn");
    until_bridge(
        &mut a,
        |a| {
            let rig = Arc::clone(&rig);
            Box::pin(async move {
                let events = a.sync(&rig).await;
                events
                    .iter()
                    .any(|event| {
                        matches!(event, SessionEvent::MessageReceived { message, .. }
                            if message.content == "hello-from-wn")
                    })
                    .then_some(())
            })
        },
        "mk decodes wn message",
        90_000,
    )
    .await;

    // We reply; wn lists it.
    a.send(&rig, &gid, "hello-from-bridge").await;
    until(
        || {
            let rig = Arc::clone(&rig);
            let wn_gid = wn_gid.clone();
            async move {
                wn_messages(&rig, &wn_gid, 600)
                    .iter()
                    .any(|message| message.to_string().contains("hello-from-bridge"))
                    .then_some(())
            }
        },
        "wn lists mk message",
        120_000,
        2_000,
    )
    .await;

    // wn, the group founder, removes us; the bridge surfaces the removal as
    // local_copy_terminated and/or the snapshot's `removed` state.
    wn(
        &rig.home,
        &rig.socket,
        Some(&rig.wn_npub),
        &["groups", "remove-members", &wn_gid, &a_npub],
    );
    until_bridge(
        &mut a,
        |a| {
            let rig = Arc::clone(&rig);
            let gid = gid.clone();
            let a_hex = a_hex.clone();
            Box::pin(async move {
                let events = a.sync(&rig).await;
                let terminated = events
                    .iter()
                    .any(|event| matches!(event, SessionEvent::LocalCopyTerminated { .. }));
                let gone = a.snapshot(&gid).is_none_or(|snapshot| {
                    matches!(snapshot.state, protocol::GroupState::Removed)
                        || !snapshot
                            .members
                            .iter()
                            .any(|member| hex::encode(member.account) == a_hex)
                });
                (terminated || gone).then_some(())
            })
        },
        "mk observes removal",
        120_000,
    )
    .await;

    rig.relay.shutdown();
    eprintln!("SCENARIO-7 PASS {:.1}s", start.elapsed().as_secs_f64());
}
