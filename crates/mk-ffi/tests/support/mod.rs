//! Shared driver for the `mk-ffi` byte-level tests: an in-memory host
//! behind the real `MkHostVtable` callbacks, a `LocalSigner` that answers
//! `signer_pending` envelopes, and a `Member` whose every operation goes
//! through the `mk_bridge_*` C entry points — exactly what a native host
//! does. `DirectMember` drives `mk_bridge::Bridge` in-process for the
//! runtime-requirement tests; both implement [`SessionDriver`].
#![allow(
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

use std::collections::BTreeMap;
use std::ffi::c_void;
use std::future::Future;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::pin::pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

use marmot_ffi::{
    MkBridge, MkBytes, MkHostVtable, mk_bridge_call, mk_bridge_close, mk_bridge_drain,
    mk_bridge_free, mk_bridge_new, mk_bridge_open, mk_bytes_free,
};
use mk_bridge::protocol::*;
use mk_bridge::{Bridge, Host, HostError, Row, encode_rows};
use mk_core::testing::LocalSigner;
use mk_core::{AccountSigner, KvBatch, KvOp};
use serde_json::Value;

pub const RELAY: &str = "wss://relay.example";

/// Drive a `LocalSigner` future: it resolves on the first poll.
pub fn block_on_now<F: Future>(future: F) -> F::Output {
    let mut future = pin!(future);
    let waker = Waker::noop();
    let mut context = Context::from_waker(waker);
    match future.as_mut().poll(&mut context) {
        Poll::Ready(value) => value,
        Poll::Pending => panic!("LocalSigner future pending"),
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

/// The shared host bookkeeping behind both `Host` impls.
#[derive(Default)]
pub struct HostState {
    /// Every accepted batch, in commit order.
    pub history: Vec<KvBatch>,
    /// When set, commits fail once `history.len() >= n`.
    pub fail_after: Option<usize>,
    /// `load` fails.
    pub fail_load: bool,
    /// Sleep inside `commit` — the lifecycle tests queue a long job.
    pub commit_delay_ms: u64,
    /// The encoded rows the most recent `load` served.
    pub last_served: Vec<u8>,
    /// Thread names observed inside `commit`, in order.
    pub commit_threads: Vec<String>,
    /// Thread ids observed inside `commit`, in order.
    pub commit_thread_ids: Vec<std::thread::ThreadId>,
}

impl HostState {
    /// The rows blob `load` would serve now.
    fn rows_blob(&self) -> Vec<u8> {
        let mut rows = BTreeMap::new();
        for batch in &self.history {
            apply(&mut rows, batch);
        }
        let rows: Vec<Row> = rows.into_iter().collect();
        encode_rows(&rows)
    }
}

/// The `*mut c_void` the FFI callbacks unpack. `load` stores its scratch
/// buffer here — the contract only needs it valid until `mk_bridge_open`
/// returns, and the callback runs inside that call.
pub struct FfiHostCtx {
    pub state: Arc<Mutex<HostState>>,
    pub releases: Arc<AtomicUsize>,
    load_buf: Vec<u8>,
}

/// Allocate a host context; the engine thread's `release` frees it and
/// bumps `releases`.
pub fn host_ctx() -> (Arc<Mutex<HostState>>, Arc<AtomicUsize>, *mut c_void) {
    let state = Arc::new(Mutex::new(HostState::default()));
    let releases = Arc::new(AtomicUsize::new(0));
    let ctx = Box::new(FfiHostCtx {
        state: Arc::clone(&state),
        releases: Arc::clone(&releases),
        load_buf: Vec::new(),
    });
    (state, releases, Box::into_raw(ctx).cast::<c_void>())
}

unsafe extern "C" fn ctx_load(ctx: *mut c_void, ptr: *mut *const u8, len: *mut usize) -> i32 {
    let ctx = unsafe { &mut *(ctx.cast::<FfiHostCtx>()) };
    {
        let mut state = ctx.state.lock().unwrap();
        if state.fail_load {
            return 1;
        }
        ctx.load_buf = state.rows_blob();
        state.last_served.clone_from(&ctx.load_buf);
    }
    unsafe {
        *ptr = ctx.load_buf.as_ptr();
        *len = ctx.load_buf.len();
    }
    0
}

unsafe extern "C" fn ctx_commit(ctx: *mut c_void, ptr: *const u8, len: usize) -> i32 {
    let ctx = unsafe { &mut *(ctx.cast::<FfiHostCtx>()) };
    let bytes = unsafe { std::slice::from_raw_parts(ptr, len) };
    let Ok(batch) = KvBatch::decode(bytes) else {
        return 1;
    };
    let delay = ctx.state.lock().unwrap().commit_delay_ms;
    if delay > 0 {
        std::thread::sleep(std::time::Duration::from_millis(delay));
    }
    let mut state = ctx.state.lock().unwrap();
    if state
        .fail_after
        .is_some_and(|limit| state.history.len() >= limit)
    {
        return 1;
    }
    if let Some(name) = std::thread::current().name() {
        state.commit_threads.push(name.to_owned());
    }
    state.commit_thread_ids.push(std::thread::current().id());
    state.history.push(batch);
    0
}

unsafe extern "C" fn ctx_release(ctx: *mut c_void) {
    let ctx = unsafe { Box::from_raw(ctx.cast::<FfiHostCtx>()) };
    ctx.releases.fetch_add(1, Ordering::SeqCst);
}

/// The vtable `mk_bridge_new` copies: full `load`/`commit`/`release`.
pub fn vtable() -> MkHostVtable {
    MkHostVtable {
        load: Some(ctx_load),
        commit: Some(ctx_commit),
        release: Some(ctx_release),
    }
}

/// A vtable with no `load`/`commit`/`release` at all — every bridge
/// interaction is a host failure.
pub fn empty_vtable() -> MkHostVtable {
    MkHostVtable {
        load: None,
        commit: None,
        release: None,
    }
}

/// Call `mk_bridge_call`; return the owned response bytes.
pub fn ffi_call(handle: *mut MkBridge, bytes: &[u8]) -> Vec<u8> {
    let mut out = MkBytes {
        ptr: std::ptr::null_mut(),
        len: 0,
    };
    unsafe { mk_bridge_call(handle, bytes.as_ptr(), bytes.len(), &mut out) };
    take_bytes(out)
}

/// Call `mk_bridge_open`; return the owned response bytes.
pub fn ffi_open(handle: *mut MkBridge, bytes: &[u8]) -> Vec<u8> {
    let mut out = MkBytes {
        ptr: std::ptr::null_mut(),
        len: 0,
    };
    unsafe { mk_bridge_open(handle, bytes.as_ptr(), bytes.len(), &mut out) };
    take_bytes(out)
}

/// Call `mk_bridge_drain`; return the owned response bytes.
pub fn ffi_drain(handle: *mut MkBridge) -> Vec<u8> {
    let mut out = MkBytes {
        ptr: std::ptr::null_mut(),
        len: 0,
    };
    unsafe { mk_bridge_drain(handle, &mut out) };
    take_bytes(out)
}

/// Copy an `MkBytes` out and free it.
pub fn take_bytes(out: MkBytes) -> Vec<u8> {
    if out.ptr.is_null() && out.len == 0 {
        return Vec::new();
    }
    let bytes = unsafe { std::slice::from_raw_parts(out.ptr, out.len) }.to_vec();
    unsafe { mk_bytes_free(out) };
    bytes
}

/// A fixed `Config` envelope — no `LocalSigner`, so tests that never
/// sign stay free of secp256k1 and run under Miri.
pub fn fixed_config() -> Vec<u8> {
    serde_json::to_vec(&Config {
        protocol_version: PROTOCOL_VERSION,
        account: [0xAA; 32],
        now: Time {
            unix_ms: 1_700_000_000_000,
            monotonic_ms: 1_000,
        },
        allow_loopback: true,
        client_name: None,
    })
    .expect("config encodes")
}

/// The encoded `call` envelope for `request` at `time`.
pub fn call_envelope(time: Time, request: &Request) -> Vec<u8> {
    serde_json::to_vec(&CallEnvelope {
        now: time,
        request: serde_json::to_value(request).expect("request encodes"),
    })
    .expect("envelope encodes")
}

/// `ffi_call` plus the signer pump, as a free function: threads that
/// share one handle each bring their own `LocalSigner` over the
/// session's secret and drive the loop without `Member` state.
pub fn ffi_pumped_call(
    handle: *mut MkBridge,
    signer: &LocalSigner,
    time: Time,
    request: &Request,
) -> Response {
    let mut outcome = response(&ffi_call(handle, &call_envelope(time, request)));
    for _ in 0..32 {
        let Response::SignerPending { requests } = &outcome else {
            return outcome;
        };
        let mut replies = Vec::with_capacity(requests.len());
        for request in requests {
            replies.push(SignerReply {
                id: request.id,
                result: answer(signer, &request.op),
            });
        }
        outcome = response(&ffi_call(
            handle,
            &call_envelope(time, &Request::SignerReply(SignerReplyRequest { replies })),
        ));
    }
    panic!("signer loop did not settle")
}

/// The error `kind` label of any error envelope (open/call/drain share
/// the `{"type":"error"}` shape).
pub fn error_kind_of(bytes: &[u8]) -> String {
    let value: Value = serde_json::from_slice(bytes).expect("envelope decodes");
    assert_eq!(value["type"], "error", "expected error envelope: {value}");
    value["error"]["kind"]
        .as_str()
        .expect("error kind")
        .to_owned()
}

/// Decode a call response; panics on malformed bytes.
pub fn response(bytes: &[u8]) -> Response {
    serde_json::from_slice(bytes).expect("response decodes")
}

/// The host-side session driver surface shared by `Member` (C ABI) and
/// `DirectMember` (in-process `Bridge`). The default methods implement
/// the whole client loop: signer pumping, durability acks, the relay
/// publish round and drain/ack.
pub trait SessionDriver {
    /// The signer's account (x-only pubkey).
    fn signer(&self) -> &LocalSigner;
    /// The injected clock, for envelopes.
    fn time(&self) -> Time;
    /// Advance the injected clock (never regresses).
    fn advance(&mut self, ms: u64);
    /// One `call` envelope; no signer pumping.
    fn call_once(&mut self, request: Request) -> Response;
    /// The encoded open config.
    fn config(&self) -> Vec<u8>;
    /// The `open` envelope's raw response.
    fn open_once(&mut self, config: &[u8]) -> OpenResponse;
    /// Raw `drain`.
    fn drain_once(&mut self) -> DrainResponse;
    /// The cumulative ack watermark of this incarnation.
    fn acked(&mut self) -> &mut u64;

    /// The account's hex pubkey.
    fn account_hex(&self) -> String {
        hex::encode(self.signer().account().0)
    }

    /// `open`, completing the account proof through the signer when
    /// asked.
    fn open(&mut self) {
        let config = self.config();
        let open = self.open_once(&config);
        match open {
            OpenResponse::NeedsAccountProof { unsigned } => {
                let signed =
                    block_on_now(self.signer().sign_event(unsigned.into())).expect("sign proof");
                let response = self.call(Request::SubmitProof(SubmitProofRequest {
                    event: signed.into(),
                }));
                assert!(
                    matches!(response, Response::Ok { .. }),
                    "submit_proof: {response:?}"
                );
            }
            OpenResponse::Ready { .. } => {}
            error => panic!("open failed: {error:?}"),
        }
    }

    /// The host client loop: answer every `signer_pending` until the
    /// call resolves.
    fn call(&mut self, request: Request) -> Response {
        let mut response = self.call_once(request);
        for _ in 0..32 {
            let Response::SignerPending { requests } = &response else {
                return response;
            };
            let signer = self.signer();
            let mut replies = Vec::with_capacity(requests.len());
            for request in requests {
                replies.push(SignerReply {
                    id: request.id,
                    result: answer(signer, &request.op),
                });
            }
            response = self.call_once(Request::SignerReply(SignerReplyRequest { replies }));
        }
        panic!("signer loop did not settle")
    }

    /// The events a drain returned (empty on an error envelope).
    fn drained_events(&mut self) -> Vec<SessionEvent> {
        match self.drain_once() {
            DrainResponse::Events { events } => events,
            DrainResponse::Error { error } => panic!("drain failed: {error:?}"),
        }
    }

    /// The highest committed seq the session reports behind the gate,
    /// then ack it.
    fn ack_all(&mut self) -> u64 {
        let response = self.call(Request::AwaitingDurability(NoArgs {}));
        let Response::Ok { result } = &response else {
            panic!("awaiting_durability failed: {response:?}");
        };
        let OpResult::AwaitingDurability(AwaitingDurabilityResult { seq }) = **result else {
            panic!("awaiting_durability result: {result:?}");
        };
        if let Some(seq) = seq {
            let response = self.call(Request::AckDurable(AckDurableRequest { seq }));
            assert!(matches!(response, Response::Ok { .. }), "ack {seq}");
            *self.acked() = (*self.acked()).max(seq);
        }
        *self.acked()
    }

    /// Release gated outbound, publish every item to `relay`, report
    /// verdicts back.
    fn publish(&mut self, relay: &mut FakeRelay) -> Vec<(String, PublishResolution)> {
        let mut done = Vec::new();
        for _ in 0..64 {
            self.ack_all();
            let response = self.call(Request::TakeOutbound(NoArgs {}));
            let Response::Ok { result } = response else {
                panic!("take_outbound failed: {response:?}");
            };
            let OpResult::Outbound(OutboundResult { items }) = *result else {
                panic!("take_outbound result: {result:?}");
            };
            if items.is_empty() {
                break;
            }
            for item in items {
                let results = relay.publish(&item);
                let id = item.id;
                let response =
                    self.call(Request::ReportPublish(ReportPublishRequest { id, results }));
                let Response::Ok { result } = response else {
                    panic!("report_publish failed: {response:?}");
                };
                let OpResult::PublishResolution(PublishResolutionResult { resolution }) = *result
                else {
                    panic!("report_publish result: {result:?}");
                };
                done.push((hex::encode(id), resolution));
            }
        }
        done
    }

    /// Publish the account's `KeyPackage` and inbox relay list.
    fn publish_identity(&mut self, relay: &mut FakeRelay) {
        let response = self.call(Request::EnsureKeyPackage(EnsureKeyPackageRequest {
            write_relays: vec![RELAY.to_owned()],
        }));
        assert!(matches!(response, Response::Ok { .. }), "key_package");
        self.publish(relay);
        let response = self.call(Request::PublishInboxRelays(PublishInboxRelaysRequest {
            relays: vec![RELAY.to_owned()],
        }));
        assert!(matches!(response, Response::Ok { .. }), "inbox_relays");
        self.publish(relay);
    }

    /// Ingest `events`, then drain and acknowledge message deliveries.
    fn ingest_and_drain(&mut self, events: Vec<Event>) -> Vec<SessionEvent> {
        let response = self.call(Request::Ingest(IngestRequest { events }));
        assert!(matches!(response, Response::Ok { .. }), "ingest");
        self.drain_and_ack()
    }

    /// Drain pending session events and acknowledge every delivery.
    fn drain_and_ack(&mut self) -> Vec<SessionEvent> {
        let drained = self.drained_events();
        let ids: Vec<[u8; 32]> = drained
            .iter()
            .filter_map(|event| match event {
                SessionEvent::MessageReceived { message, .. } => Some(message.transport_id),
                _ => None,
            })
            .collect();
        if !ids.is_empty() {
            let response = self.call(Request::AcknowledgeMessages(AcknowledgeMessagesRequest {
                ids,
            }));
            assert!(matches!(response, Response::Ok { .. }), "ack messages");
        }
        drained
    }

    /// The group's `#h` route id as hex.
    fn route_hex(&mut self, group: &[u8]) -> String {
        let response = self.call(Request::Group(GroupRequest {
            group: group.to_vec(),
        }));
        let Response::Ok { result } = response else {
            panic!("group failed: {response:?}");
        };
        let OpResult::Group(GroupResult {
            group: Some(snapshot),
        }) = *result
        else {
            panic!("group result: {result:?}");
        };
        hex::encode(snapshot.nostr_group_id)
    }

    /// Sync the group: ingest the relay backlog, tick, publish, until
    /// nothing new appears.
    fn sync(&mut self, relay: &mut FakeRelay, group: &[u8]) {
        let route = self.route_hex(group);
        for _ in 0..8 {
            let backlog = relay.group_events(&route);
            self.ingest_and_drain(backlog);
            let response = self.call(Request::Tick(NoArgs {}));
            assert!(matches!(response, Response::Ok { .. }), "tick");
            self.advance(2_000);
            self.publish(relay);
        }
    }

    /// Send one chat message and publish it.
    fn send_chat(&mut self, relay: &mut FakeRelay, group: &[u8], text: &str) {
        self.advance(1_000);
        let response = self.call(Request::SendMessage(SendMessageRequest {
            group: group.to_vec(),
            message: AppMessage {
                kind: 9,
                tags: Vec::new(),
                content: text.to_owned(),
            },
        }));
        assert!(matches!(response, Response::Ok { .. }), "send {text}");
        self.publish(relay);
    }
}

/// One account-device bridge driven purely through the C ABI.
pub struct Member {
    pub handle: *mut MkBridge,
    pub state: Arc<Mutex<HostState>>,
    pub releases: Arc<AtomicUsize>,
    pub signer: LocalSigner,
    pub unix_ms: u64,
    pub mono_ms: u64,
    /// The cumulative acked watermark this incarnation has acked.
    pub acked: u64,
}

impl Member {
    /// A fresh handle over empty storage; `mk_bridge_new` must succeed.
    pub fn new() -> Self {
        Self::with_signer(LocalSigner::new())
    }

    /// A fresh handle whose signer derives from `secret` — deterministic
    /// so threads can rebuild the same signer for their own pump loops.
    pub fn with_secret(secret: [u8; 32]) -> Self {
        Self::with_signer(LocalSigner::from_secret(secret).expect("valid secret"))
    }

    /// A fresh handle over `signer`.
    pub fn with_signer(signer: LocalSigner) -> Self {
        let (state, releases, ctx) = host_ctx();
        let vtable = vtable();
        let handle = unsafe { mk_bridge_new(&vtable, ctx) };
        assert!(!handle.is_null(), "mk_bridge_new failed");
        Self {
            handle,
            state,
            releases,
            signer,
            unix_ms: 1_700_000_000_000,
            mono_ms: 1_000,
            acked: 0,
        }
    }

    /// `mk_bridge_close` the handle.
    pub fn close(&mut self) {
        unsafe { mk_bridge_close(self.handle) };
    }

    /// Free the handle without closing. Consumes the member; returns
    /// the `release` counter so the caller can watch the detached
    /// engine thread run it.
    pub fn free(self) -> Arc<AtomicUsize> {
        unsafe { mk_bridge_free(self.handle) };
        self.releases
    }

    /// Close, then open a fresh handle over the same durable host — the
    /// session must come back `ready`.
    pub fn restart(&mut self) {
        self.close();
        let (state, releases, ctx) = host_ctx();
        state
            .lock()
            .unwrap()
            .history
            .clone_from(&self.state.lock().unwrap().history);
        let vtable = vtable();
        self.handle = unsafe { mk_bridge_new(&vtable, ctx) };
        assert!(!self.handle.is_null(), "mk_bridge_new failed");
        self.state = state;
        self.releases = releases;
        self.acked = 0;
        self.open();
    }
}

impl SessionDriver for Member {
    fn signer(&self) -> &LocalSigner {
        &self.signer
    }

    fn time(&self) -> Time {
        Time {
            unix_ms: self.unix_ms,
            monotonic_ms: self.mono_ms,
        }
    }

    fn advance(&mut self, ms: u64) {
        self.unix_ms += ms;
        self.mono_ms += ms;
    }

    fn call_once(&mut self, request: Request) -> Response {
        let envelope = serde_json::to_vec(&CallEnvelope {
            now: self.time(),
            request: serde_json::to_value(&request).expect("request encodes"),
        })
        .expect("envelope encodes");
        response(&ffi_call(self.handle, &envelope))
    }

    fn config(&self) -> Vec<u8> {
        serde_json::to_vec(&Config {
            protocol_version: PROTOCOL_VERSION,
            account: self.signer.account().0,
            now: self.time(),
            allow_loopback: true,
            client_name: None,
        })
        .expect("config encodes")
    }

    fn open_once(&mut self, config: &[u8]) -> OpenResponse {
        let bytes = ffi_open(self.handle, config);
        serde_json::from_slice(&bytes).expect("open decodes")
    }

    fn drain_once(&mut self) -> DrainResponse {
        serde_json::from_slice(&ffi_drain(self.handle)).expect("drain decodes")
    }

    fn acked(&mut self) -> &mut u64 {
        &mut self.acked
    }
}

/// An in-process relay set: published events per URL, verdict injection.
#[derive(Default)]
pub struct FakeRelay {
    /// Everything ever handed to `publish`, in order.
    pub publishes: Vec<Outbound>,
    /// Report every target `rejected`.
    pub unavailable: bool,
}

impl FakeRelay {
    pub fn new() -> Self {
        Self::default()
    }

    /// Store the event under every URL it accepts; returns the verdicts
    /// to report back.
    pub fn publish(&mut self, item: &Outbound) -> Vec<RelayResult> {
        let results = item
            .relays
            .iter()
            .map(|relay| RelayResult {
                relay: relay.clone(),
                status: if self.unavailable {
                    RelayStatus::Unavailable
                } else {
                    RelayStatus::Accepted
                },
            })
            .collect();
        self.publishes.push(item.clone());
        results
    }

    /// Published events matching `matches`, in publish order.
    pub fn fetch(&self, matches: impl Fn(&Event) -> bool) -> Vec<Event> {
        self.publishes
            .iter()
            .map(|published| &published.event)
            .filter(|event| matches(event))
            .cloned()
            .collect()
    }

    /// Group message events (kind 445) carrying `#h` = `route_hex`.
    pub fn group_events(&self, route_hex: &str) -> Vec<Event> {
        self.fetch(|event| event.kind == 445 && tag(event, "h") == Some(route_hex))
    }

    /// Gift-wrapped Welcome events (kind 1059) addressed to `account_hex`.
    pub fn welcomes_for(&self, account_hex: &str) -> Vec<Event> {
        self.fetch(|event| event.kind == 1059 && tag(event, "p") == Some(account_hex))
    }

    /// The newest kind-30443 `KeyPackage` event authored by `account_hex`.
    pub fn key_package_of(&self, account_hex: &str) -> Event {
        self.fetch(|event| event.kind == 30_443 && event.pubkey == hex_to_32(account_hex))
            .into_iter()
            .next_back()
            .expect("account published a KeyPackage")
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

pub fn hex_to_32(text: &str) -> [u8; 32] {
    let decoded = hex::decode(text).expect("hex id");
    decoded.try_into().expect("32-byte id")
}

/// An in-memory `Host` for driving `Bridge` directly — no FFI.
#[derive(Clone, Default)]
pub struct MemHost {
    pub state: Arc<Mutex<HostState>>,
}

impl MemHost {
    pub fn new() -> Self {
        Self::default()
    }
}

impl Host for MemHost {
    fn load(&mut self) -> Result<Vec<u8>, HostError> {
        let mut state = self.state.lock().unwrap();
        if state.fail_load {
            return Err(HostError::new());
        }
        let blob = state.rows_blob();
        state.last_served.clone_from(&blob);
        Ok(blob)
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

/// A `Member`-shaped driver over an in-process `Bridge<MemHost>` — the
/// timing tests use it to establish what the poll model needs from a
/// Tokio runtime on native.
pub struct DirectMember {
    pub bridge: Bridge<MemHost>,
    pub host: MemHost,
    pub signer: LocalSigner,
    pub unix_ms: u64,
    pub mono_ms: u64,
    pub acked: u64,
}

impl DirectMember {
    pub fn new() -> Self {
        let host = MemHost::new();
        Self {
            bridge: Bridge::new(host.clone()),
            host,
            signer: LocalSigner::new(),
            unix_ms: 1_700_000_000_000,
            mono_ms: 1_000,
            acked: 0,
        }
    }
}

impl SessionDriver for DirectMember {
    fn signer(&self) -> &LocalSigner {
        &self.signer
    }

    fn time(&self) -> Time {
        Time {
            unix_ms: self.unix_ms,
            monotonic_ms: self.mono_ms,
        }
    }

    fn advance(&mut self, ms: u64) {
        self.unix_ms += ms;
        self.mono_ms += ms;
    }

    fn call_once(&mut self, request: Request) -> Response {
        let envelope = serde_json::to_vec(&CallEnvelope {
            now: self.time(),
            request: serde_json::to_value(&request).expect("request encodes"),
        })
        .expect("envelope encodes");
        serde_json::from_slice(&self.bridge.call(&envelope)).expect("response decodes")
    }

    fn config(&self) -> Vec<u8> {
        serde_json::to_vec(&Config {
            protocol_version: PROTOCOL_VERSION,
            account: self.signer.account().0,
            now: self.time(),
            allow_loopback: true,
            client_name: None,
        })
        .expect("config encodes")
    }

    fn open_once(&mut self, config: &[u8]) -> OpenResponse {
        let open: OpenResponse =
            serde_json::from_slice(&self.bridge.open(config)).expect("open decodes");
        open
    }

    fn drain_once(&mut self) -> DrainResponse {
        serde_json::from_slice(&self.bridge.drain()).expect("drain decodes")
    }

    fn acked(&mut self) -> &mut u64 {
        &mut self.acked
    }
}

/// Answer one signer request with `signer`.
fn answer(signer: &LocalSigner, op: &SignerOp) -> SignerResult {
    match op {
        SignerOp::SignEvent { unsigned } => {
            block_on_now(signer.sign_event(unsigned.clone().into()))
                .map(|event| SignerResult::Signed {
                    event: event.into(),
                })
                .unwrap_or(SignerResult::Failed)
        }
        SignerOp::Nip44Encrypt { peer, plaintext } => {
            block_on_now(signer.nip44_encrypt(&mk_core::AccountId(*peer), plaintext))
                .map(|text| SignerResult::Text { text })
                .unwrap_or(SignerResult::Failed)
        }
        SignerOp::Nip44Decrypt { peer, ciphertext } => {
            block_on_now(signer.nip44_decrypt(&mk_core::AccountId(*peer), ciphertext))
                .map(|text| SignerResult::Text { text })
                .unwrap_or(SignerResult::Failed)
        }
    }
}

/// Wait until the host `release` ran, up to a generous bound — used
/// after `mk_bridge_free`, whose detached engine thread releases
/// asynchronously.
pub fn wait_for_release(releases: &AtomicUsize) {
    for _ in 0..500 {
        if releases.load(Ordering::SeqCst) >= 1 {
            return;
        }
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
    panic!("release did not run");
}

/// Catch a panic and extract its message.
pub fn panic_message(body: impl FnOnce()) -> Option<String> {
    let outcome = catch_unwind(AssertUnwindSafe(body));
    let payload = outcome.err()?;
    if let Some(message) = payload.downcast_ref::<&str>() {
        Some((*message).to_owned())
    } else {
        payload.downcast_ref::<String>().cloned()
    }
}
