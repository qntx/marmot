//! Shared harness for the White Noise interop scenarios: an in-process
//! NIP-01 `LocalRelay`, a `nostr-sdk` client, an mk-core `Member`, and the
//! `wn`/`wnd` process control the scenario tests drive. Ports the roles
//! `spike/js/run-interop.ts` gave `serveFakeRelay`, `WsRelay` and
//! `LocalMember` to the public `Session` API.

#![allow(
    clippy::missing_docs_in_private_items,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::missing_panics_doc,
    clippy::missing_errors_doc,
    clippy::needless_pass_by_value,
    clippy::tests_outside_test_module,
    clippy::shadow_reuse,
    clippy::shadow_unrelated,
    clippy::doc_markdown,
    clippy::significant_drop_tightening,
    clippy::unused_async,
    clippy::too_many_lines,
    clippy::excessive_nesting,
    clippy::cognitive_complexity,
    clippy::format_push_string,
    clippy::str_to_string,
    clippy::cast_possible_truncation,
    clippy::cast_lossless,
    clippy::clone_on_ref_ptr,
    unreachable_pub,
    dead_code,
    reason = "test harness code; each scenario uses a subset"
)]

use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use mk_bridge as _;
use mk_core::testing::{LocalSigner, RecordingHost};
use mk_core::{
    AccountId, AccountSigner, Clock, DestinationPolicy, Event, GroupId, KvHost, Opened, Outbound,
    RelayResult, RelayStatus, Session, SessionConfig, SessionEvent, Time,
};
use nostr::prelude::{
    Event as NostrEvent, Filter, FromBech32, Kind, PublicKey, RelayUrl, SingleLetterTag, ToBech32,
};
use nostr_sdk::client::Client;
use nostr_sdk::local_relay::{LocalRelay, RateLimit};
use serde_json::Value;

pub const KIND_GROUP_MESSAGE: u16 = 445;
pub const KIND_GIFT_WRAP: u16 = 1059;
pub const KIND_KEY_PACKAGE: u16 = 30_443;
pub const KIND_INBOX_RELAYS: u16 = 10_050;

/// Documented wnd settle from `spike/INTEROP.md`: after the roster is
/// visible the daemon finishes post-join fanout/subscription install with
/// no observable completion signal; 3 s was sufficient in every spike run.
pub const WND_SETTLE_MS: u64 = 3_000;

/// Absolute path of the `wn` CLI under test (`WN_BIN` env).
pub fn wn_bin() -> PathBuf {
    PathBuf::from(std::env::var("WN_BIN").expect("WN_BIN must point at the wn binary"))
}

/// Absolute path of the `wnd` daemon under test (`WND_BIN` env).
pub fn wnd_bin() -> PathBuf {
    PathBuf::from(std::env::var("WND_BIN").expect("WND_BIN must point at the wnd binary"))
}

/// The MDK tag pinned in `upstream.json` (`mdk.tag`, e.g. `v0.17.0`).
pub fn pinned_mdk_tag() -> String {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../upstream.json")
        .canonicalize()
        .expect("upstream.json path");
    let parsed: Value =
        serde_json::from_slice(&std::fs::read(path).expect("read upstream.json")).expect("json");
    parsed
        .pointer("/mdk/tag")
        .and_then(Value::as_str)
        .expect("mdk.tag")
        .to_owned()
}

/// Hard-fail unless `wn --version` reports the pinned MDK tag's version.
pub fn assert_wn_version() {
    let out = Command::new(wn_bin())
        .arg("--version")
        .output()
        .expect("spawn wn --version");
    assert!(out.status.success(), "wn --version failed");
    let text = String::from_utf8_lossy(&out.stdout);
    let version = text
        .split_whitespace()
        .nth(1)
        .expect("wn --version prints `wn <version>`");
    assert_eq!(
        format!("v{version}"),
        pinned_mdk_tag(),
        "wn build must match the pinned MDK tag"
    );
}

/// `wn` CLI invocation returning parsed `--json` output; fails the test on
/// a non-zero exit or non-JSON output.
pub fn wn(home: &Path, socket: &Path, account: Option<&str>, args: &[&str]) -> Value {
    let (ok, parsed, raw) = wn_run(home, socket, account, args);
    assert!(ok, "wn {args:?} refused: {raw}");
    parsed
}

/// `wn` CLI invocation that tolerates refusals: returns the parsed payload
/// and whether it reported `ok: true` (a refusal is expected evidence in
/// some scenarios).
pub fn wn_try(home: &Path, socket: &Path, account: Option<&str>, args: &[&str]) -> (bool, Value) {
    let (ok, parsed, _) = wn_run(home, socket, account, args);
    (ok, parsed)
}

fn wn_run(
    home: &Path,
    socket: &Path,
    account: Option<&str>,
    args: &[&str],
) -> (bool, Value, String) {
    let mut cmd = Command::new(wn_bin());
    cmd.arg("--json")
        .arg("--home")
        .arg(home)
        .arg("--socket")
        .arg(socket)
        .env("WN_HOME", home)
        .env("WN_SECRET_STORE", "file")
        .env("WN_ALLOW_LOOPBACK_RELAYS", "1")
        .env("WN_ALLOW_LOOPBACK_BLOB_ENDPOINTS", "1");
    if let Some(account) = account {
        cmd.arg("--account").arg(account);
    }
    cmd.args(args);
    let out = cmd.output().expect("spawn wn");
    let stdout = String::from_utf8_lossy(&out.stdout).into_owned();
    let stderr = String::from_utf8_lossy(&out.stderr).into_owned();
    let text = if stdout.trim().is_empty() {
        stderr
    } else {
        stdout
    };
    match serde_json::from_str::<Value>(&text) {
        Ok(parsed) => (
            parsed.get("ok").and_then(Value::as_bool).unwrap_or(false),
            parsed,
            text,
        ),
        Err(_) => (false, Value::Null, text),
    }
}

/// An `wnd` process owned by the test: killed and reaped on drop, also on
/// panic unwind. Its `WN_HOME` is removed; its logs stay under
/// `target/wn-logs/`.
#[derive(Debug)]
pub struct Wnd {
    child: Option<Child>,
    home: PathBuf,
}

impl Drop for Wnd {
    fn drop(&mut self) {
        if let Some(mut child) = self.child.take() {
            drop(child.kill());
            drop(child.wait());
        }
        drop(std::fs::remove_dir_all(&self.home));
    }
}

/// Spawn `wnd` against the relay and wait until its socket answers `wn
/// whoami` (proves the socket is live, not just created).
pub async fn spawn_wnd(home: &Path, socket: &Path, logs: &Path, relay_url: &str) -> Wnd {
    std::fs::create_dir_all(home).expect("wn home");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        // wnd refuses a socket directory that is not private.
        std::fs::set_permissions(home, std::fs::Permissions::from_mode(0o700))
            .expect("wn home 0700");
    }
    std::fs::create_dir_all(logs).expect("wn logs dir");
    let out = std::fs::File::create(logs.join("wnd.out.log")).expect("wnd out log");
    let err = std::fs::File::create(logs.join("wnd.err.log")).expect("wnd err log");
    let child = Command::new(wnd_bin())
        .arg("--home")
        .arg(home)
        .arg("--socket")
        .arg(socket)
        .arg("--logs-dir")
        .arg(logs)
        .arg("--discovery-relays")
        .arg(relay_url)
        .arg("--default-account-relays")
        .arg(relay_url)
        .env("WN_HOME", home)
        .env("WN_SECRET_STORE", "file")
        .env("WN_ALLOW_LOOPBACK_RELAYS", "1")
        .env("WN_ALLOW_LOOPBACK_BLOB_ENDPOINTS", "1")
        .stdout(Stdio::from(out))
        .stderr(Stdio::from(err))
        .spawn()
        .expect("spawn wnd");
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        assert!(
            Instant::now() < deadline,
            "wnd socket never appeared at {}",
            socket.display()
        );
        if socket.exists() {
            let probe = Command::new(wn_bin())
                .arg("--json")
                .arg("--home")
                .arg(home)
                .arg("--socket")
                .arg(socket)
                .arg("whoami")
                .env("WN_HOME", home)
                .env("WN_SECRET_STORE", "file")
                .env("WN_ALLOW_LOOPBACK_RELAYS", "1")
                .env("WN_ALLOW_LOOPBACK_BLOB_ENDPOINTS", "1")
                .output();
            if probe.is_ok_and(|out| out.status.success()) {
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    Wnd {
        child: Some(child),
        home: home.to_path_buf(),
    }
}

/// One full interop rig: in-process relay, nostr client, wnd daemon, and
/// the wn account (created on first use, KeyPackage published to the
/// relay).
#[derive(Debug)]
pub struct Rig {
    pub relay: LocalRelay,
    pub client: Client,
    /// The relay URL as sessions and wn see it (no trailing slash).
    pub url: String,
    pub home: PathBuf,
    pub socket: PathBuf,
    pub logs: PathBuf,
    pub wn_npub: String,
    pub wn_pub: String,
    pub wnd: Wnd,
}

/// Start relay + wnd and ensure a wn identity with a published KeyPackage.
pub async fn rig(test_name: &str) -> Rig {
    assert_wn_version();
    // A real relay's defaults (60 notes/min write cap, 500/filter read cap)
    // would silently reject the bulk-traffic scenarios; lift both while
    // keeping real NIP-01 storage/ordering semantics.
    let relay = LocalRelay::builder()
        .rate_limit(RateLimit {
            max_reqs: 500,
            notes_per_minute: 1_000_000,
        })
        .max_filter_limit(10_000)
        .build();
    relay.run().await.expect("local relay run");
    let url = relay
        .url()
        .await
        .to_string()
        .trim_end_matches('/')
        .to_owned();

    let client = Client::builder().build();
    client
        .add_relay(RelayUrl::parse(&url).expect("relay url"))
        .and_connect()
        .await
        .expect("client connect");

    let base = Path::new(env!("CARGO_MANIFEST_DIR")).join("target");
    let stamp = format!("{}-{}", test_name, std::process::id());
    // Keep the socket path short: macOS rejects unix paths near 104 chars.
    let home = std::env::temp_dir().join(format!("mi-{stamp}"));
    drop(std::fs::remove_dir_all(&home));
    let socket = home.join("w.sock");
    let logs = base.join("wn-logs").join(&stamp);
    let wnd = spawn_wnd(&home, &socket, &logs, &url).await;

    let who = wn(&home, &socket, None, &["whoami"]);
    let wn_npub = who
        .pointer("/result/accounts/0/npub")
        .and_then(Value::as_str)
        .map_or_else(
            || {
                let created = wn(&home, &socket, None, &["create-identity"]);
                created
                    .pointer("/result/npub")
                    .and_then(Value::as_str)
                    .expect("create-identity npub")
                    .to_owned()
            },
            str::to_owned,
        );
    wn(&home, &socket, Some(&wn_npub), &["keys", "publish"]);
    let wn_pub = PublicKey::from_bech32(&wn_npub).expect("wn npub").to_hex();
    Rig {
        relay,
        client,
        url,
        home,
        socket,
        logs,
        wn_npub,
        wn_pub,
        wnd,
    }
}

/// A wall clock for sessions that must run on real time (convergence
/// cutoffs, event `created_at`).
#[derive(Debug)]
pub struct WallClock {
    epoch: Instant,
}

impl WallClock {
    pub fn new() -> Self {
        Self {
            epoch: Instant::now(),
        }
    }
}

impl Clock for WallClock {
    fn now(&self) -> Time {
        let unix_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| d.as_millis() as u64);
        Time {
            monotonic_ms: self.epoch.elapsed().as_millis() as u64,
            unix_ms,
        }
    }
}

/// One mk-core account-device session on the shared relay.
#[derive(Debug)]
pub struct Member {
    pub signer: Arc<LocalSigner>,
    pub clock: Arc<WallClock>,
    pub host: Arc<RecordingHost>,
    pub session: Session,
}

impl Member {
    pub fn account_hex(&self) -> String {
        self.signer.account().to_hex()
    }

    pub fn npub(&self) -> String {
        PublicKey::from_slice(self.signer.account().as_bytes())
            .expect("account pubkey")
            .to_bech32()
            .expect("npub")
    }

    pub fn account(&self) -> AccountId {
        self.signer.account()
    }

    /// The group's `#h` route id as hex, read through the member's snapshot.
    pub fn route_hex(&self, group: &GroupId) -> String {
        hex::encode(
            self.session
                .group(group)
                .expect("group")
                .expect("group exists")
                .nostr_group_id,
        )
    }

    /// Member accounts in the group's roster.
    pub fn roster(&self, group: &GroupId) -> Vec<String> {
        self.session
            .group(group)
            .expect("group")
            .map(|snapshot| {
                snapshot
                    .members
                    .iter()
                    .map(|member| member.account.to_hex())
                    .collect()
            })
            .unwrap_or_default()
    }
}

/// Open a fresh member, completing the account-proof handshake.
pub async fn open_member() -> Member {
    let signer = Arc::new(LocalSigner::new());
    let clock = Arc::new(WallClock::new());
    let host = Arc::new(RecordingHost::new());
    let signer_cfg: Arc<dyn AccountSigner> = signer.clone();
    let host_cfg: Arc<dyn KvHost> = host.clone();
    let clock_cfg: Arc<dyn Clock> = clock.clone();
    let opened = Session::open(SessionConfig {
        signer: signer_cfg,
        host: host_cfg,
        rows: Vec::new(),
        clock: clock_cfg,
        destination_policy: DestinationPolicy {
            allow_loopback: true,
        },
        client_name: None,
    })
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

/// Publish one outbound artifact to its listed relays and build the
/// per-relay verdict to report back.
pub async fn publish(rig: &Rig, item: &Outbound) -> Vec<RelayResult> {
    let event = NostrEvent::from_json(item.event.to_json()).expect("outbound json");
    let urls: Vec<RelayUrl> = item
        .relays
        .iter()
        .map(|relay| RelayUrl::parse(relay).expect("relay url"))
        .collect();
    let output = rig
        .client
        .send_event(&event)
        .to(urls.iter().cloned())
        .await
        .expect("send_event");
    item.relays
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
            RelayResult {
                relay: relay.clone(),
                status,
            }
        })
        .collect()
}

/// Drain the session's gated outbound: acknowledge durability, take the
/// outbound, publish each artifact to the real relay, report the per-relay
/// OK results. Loops until nothing remains so regenerated intents also
/// publish.
pub async fn flush(member: &mut Member, rig: &Rig) {
    for _ in 0..64 {
        if let Some(seq) = member.session.awaiting_durability() {
            member.session.acknowledge_durable(seq);
        }
        let outbound = member.session.take_outbound().expect("take_outbound");
        if outbound.is_empty() {
            break;
        }
        for item in outbound {
            let results = publish(rig, &item).await;
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

/// Publish the member's kind-30443 `KeyPackage` and kind-10050 inbox relay
/// list (wn requires both before it will invite an account).
pub async fn publish_identity(member: &mut Member, rig: &Rig) {
    member
        .session
        .publish_key_package(std::slice::from_ref(&rig.url))
        .await
        .expect("key package");
    flush(member, rig).await;
    member
        .session
        .publish_inbox_relays(std::slice::from_ref(&rig.url))
        .await
        .expect("inbox relays");
    flush(member, rig).await;
}

/// Fetch events for `filter` over the live WebSocket connection; the relay
/// serves them newest-first, like a real relay.
pub async fn fetch(rig: &Rig, filter: Filter) -> Vec<Event> {
    rig.client
        .fetch_events(filter)
        .timeout(Duration::from_secs(10))
        .await
        .expect("fetch_events")
        .into_iter()
        .map(|event| Event::from_json(&event.as_json()).expect("relay event"))
        .collect()
}

/// Kind-445 filter for a `#h` route id.
pub fn group_filter(route_hex: &str) -> Filter {
    Filter::new()
        .kind(Kind::Custom(KIND_GROUP_MESSAGE))
        .custom_tag(SingleLetterTag::LOWERCASE_H, route_hex)
}

/// Kind-1059 filter for gift wraps addressed to `account_hex` (`#p`).
pub fn welcome_filter(account_hex: &str) -> Filter {
    Filter::new()
        .kind(Kind::Custom(KIND_GIFT_WRAP))
        .custom_tag(SingleLetterTag::LOWERCASE_P, account_hex)
}

/// Kind-30443 filter for `account_hex`'s `KeyPackage` events.
pub fn key_package_filter(account_hex: &str) -> Filter {
    Filter::new()
        .kind(Kind::Custom(KIND_KEY_PACKAGE))
        .author(PublicKey::from_hex(account_hex).expect("hex pubkey"))
}

/// Kind-445 group feed for a `#h` route id, newest-first.
pub async fn fetch_group(rig: &Rig, route_hex: &str) -> Vec<Event> {
    fetch(rig, group_filter(route_hex)).await
}

/// Kind-1059 gift wraps addressed to `account_hex`, newest-first.
pub async fn fetch_welcomes(rig: &Rig, account_hex: &str) -> Vec<Event> {
    fetch(rig, welcome_filter(account_hex)).await
}

/// The newest kind-30443 `KeyPackage` published by `account_hex`, if any.
pub async fn fetch_key_package(rig: &Rig, account_hex: &str) -> Option<Event> {
    fetch(rig, key_package_filter(account_hex))
        .await
        .into_iter()
        .next()
}

/// One catch-up pass: fetch the group's backlog, `ingest_batch` it (the
/// session sorts the relay's newest-first order), acknowledge delivered
/// messages, drive one convergence tick, and publish anything produced.
/// Returns the drained session events.
pub async fn sync(member: &mut Member, rig: &Rig, group: &GroupId) -> Vec<SessionEvent> {
    let backlog = fetch_group(rig, &member.route_hex(group)).await;
    let events = ingest(member, backlog).await;
    member.session.tick().await.expect("tick");
    flush(member, rig).await;
    events
}

/// `ingest_batch` + ack of every delivered message; returns drained events.
pub async fn ingest(member: &mut Member, events: Vec<Event>) -> Vec<SessionEvent> {
    member
        .session
        .ingest_batch(events)
        .await
        .expect("ingest_batch");
    let drained = member.session.drain_events().unwrap();
    let ids: Vec<_> = drained
        .iter()
        .filter_map(|event| match event {
            SessionEvent::MessageReceived { message, .. } => Some(message.transport_id),
            SessionEvent::AgentStreamStarted { start, .. } => Some(start.transport_id),
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

/// Application message contents a member has delivered (and acked).
pub fn received(events: &[SessionEvent]) -> Vec<String> {
    events
        .iter()
        .filter_map(|event| match event {
            SessionEvent::MessageReceived { message, .. } => Some(message.content.clone()),
            _ => None,
        })
        .collect()
}

/// Poll `f` every `step` until it yields, failing after `ms`.
pub async fn until<F, Fut, T>(mut f: F, what: &str, ms: u64, step: u64) -> T
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Option<T>>,
{
    let start = Instant::now();
    loop {
        if let Some(value) = f().await {
            return value;
        }
        assert!(
            start.elapsed() < Duration::from_millis(ms),
            "timeout waiting for {what}"
        );
        tokio::time::sleep(Duration::from_millis(step)).await;
    }
}

/// Poll `f` like [`until`]; `f` yields `(done, observation)` and a timeout
/// reports the last observation.
pub async fn until_observed<F, Fut, O: std::fmt::Debug>(
    mut f: F,
    what: &str,
    ms: u64,
    step: u64,
) -> O
where
    F: FnMut() -> Fut,
    Fut: Future<Output = (bool, O)>,
{
    let start = Instant::now();
    loop {
        let (done, observation) = f().await;
        if done {
            return observation;
        }
        assert!(
            start.elapsed() < Duration::from_millis(ms),
            "timeout waiting for {what} (last: {observation:?})"
        );
        tokio::time::sleep(Duration::from_millis(step)).await;
    }
}

/// Poll `f` until it yields; `f` gets a fresh `&mut Member` every pass and
/// decides what a pass does (fetch, ingest, `sync`, snapshot reads).
pub async fn until_member<T, F>(member: &mut Member, mut f: F, what: &str, ms: u64) -> T
where
    F: for<'m> FnMut(&'m mut Member) -> Pin<Box<dyn Future<Output = Option<T>> + Send + 'm>>,
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
        tokio::time::sleep(Duration::from_millis(800)).await;
    }
}

/// `wn groups members` as a list of JSON values (empty on refusal).
pub fn wn_members(rig: &Rig, group: &str) -> Vec<Value> {
    let (ok, parsed) = wn_try(
        &rig.home,
        &rig.socket,
        Some(&rig.wn_npub),
        &["groups", "members", group],
    );
    if !ok {
        return Vec::new();
    }
    let members = parsed
        .pointer("/result/members")
        .and_then(Value::as_array)
        .or_else(|| parsed.pointer("/result").and_then(Value::as_array));
    members.cloned().unwrap_or_default()
}

/// `wn messages list` items for a group (empty on refusal).
pub fn wn_messages(rig: &Rig, group: &str, limit: usize) -> Vec<Value> {
    let limit_str = limit.to_string();
    let (ok, parsed) = wn_try(
        &rig.home,
        &rig.socket,
        Some(&rig.wn_npub),
        &["messages", "list", group, "--limit", &limit_str],
    );
    if !ok {
        return Vec::new();
    }
    let messages = parsed
        .pointer("/result/messages")
        .and_then(Value::as_array)
        .or_else(|| parsed.pointer("/result/items").and_then(Value::as_array))
        .or_else(|| parsed.pointer("/result").and_then(Value::as_array));
    messages.cloned().unwrap_or_default()
}

/// `wn messages send`.
pub fn wn_send(rig: &Rig, group: &str, text: &str) {
    wn(
        &rig.home,
        &rig.socket,
        Some(&rig.wn_npub),
        &["messages", "send", group, text],
    );
}

/// Hex pubkey of an npub.
pub fn npub_to_hex(npub: &str) -> String {
    PublicKey::from_bech32(npub).expect("npub").to_hex()
}

/// Blob store shared by the axum routes and the test-side `put`/`get`.
#[derive(Clone, Debug)]
struct BlossomState {
    /// `http://127.0.0.1:<port>` (no trailing slash).
    url: String,
    blobs: Arc<std::sync::Mutex<std::collections::HashMap<String, Vec<u8>>>>,
}

/// A minimal Blossom (BUD-01/BUD-02) dev server on axum over the test's
/// tokio runtime: `PUT /upload` requires an `Authorization: Nostr …` header
/// and answers the descriptor `wn` parses (`{url, sha256, size, type,
/// uploaded}`); `GET /<sha256>` serves the stored blob and `HEAD /<sha256>`
/// answers headers only (axum serves both on the GET route). The test
/// host's mk-core side uses `put`/`get` directly — no HTTP for our half.
#[derive(Debug)]
pub struct BlossomServer {
    /// `http://127.0.0.1:<port>` (no trailing slash).
    pub url: String,
    state: BlossomState,
    task: tokio::task::JoinHandle<()>,
}

impl BlossomServer {
    /// Bind loopback, spawn the axum server on the current tokio runtime.
    pub fn new() -> Self {
        let std_listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind blossom");
        std_listener.set_nonblocking(true).expect("nonblocking");
        let address = std_listener.local_addr().expect("addr");
        let listener = tokio::net::TcpListener::from_std(std_listener).expect("tokio listener");
        let state = BlossomState {
            url: format!("http://{address}"),
            blobs: Arc::new(std::sync::Mutex::new(std::collections::HashMap::new())),
        };
        let app = axum::Router::new()
            .route("/upload", axum::routing::put(upload))
            .route("/:sha256", axum::routing::get(download))
            .with_state(state.clone());
        let task = tokio::spawn(async move {
            axum::serve(listener, app).await.expect("blossom serve");
        });
        Self {
            url: state.url.clone(),
            state,
            task,
        }
    }

    /// Store `bytes` under `hash_hex` (the test's "upload").
    pub fn put(&self, hash_hex: &str, bytes: &[u8]) {
        self.state
            .blobs
            .lock()
            .expect("blobs")
            .insert(hash_hex.to_owned(), bytes.to_vec());
    }

    /// Fetch the blob stored under `hash_hex` (the test's "download").
    pub fn get(&self, hash_hex: &str) -> Option<Vec<u8>> {
        self.state
            .blobs
            .lock()
            .expect("blobs")
            .get(hash_hex)
            .cloned()
    }
}

impl Drop for BlossomServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// `PUT /upload`: requires `Authorization: Nostr …`; stores the body under
/// its sha256 and answers the BUD-02 blob descriptor `wn` parses.
async fn upload(
    axum::extract::State(state): axum::extract::State<BlossomState>,
    headers: axum::http::HeaderMap,
    body: axum::body::Bytes,
) -> (axum::http::StatusCode, axum::Json<Value>) {
    let authed = headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.to_ascii_lowercase().contains("nostr"));
    if !authed {
        return (
            axum::http::StatusCode::UNAUTHORIZED,
            axum::Json(serde_json::json!({"reason": "missing Nostr authorization"})),
        );
    }
    let hash = sha256_hex(&body);
    state
        .blobs
        .lock()
        .expect("blobs")
        .insert(hash.clone(), body.to_vec());
    (
        axum::http::StatusCode::OK,
        axum::Json(serde_json::json!({
            "url": format!("{}/{hash}", state.url),
            "sha256": hash,
            "size": body.len(),
            "type": "application/octet-stream",
            "uploaded": SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_or(0, |d| d.as_secs()),
        })),
    )
}

/// `GET /<sha256>`: the stored blob with an explicit `Content-Length`, or
/// 404. axum answers `HEAD` on the same route with the headers kept and the
/// body stripped.
async fn download(
    axum::extract::State(state): axum::extract::State<BlossomState>,
    axum::extract::Path(sha256): axum::extract::Path<String>,
) -> axum::response::Response {
    let found = state.blobs.lock().expect("blobs").get(&sha256).cloned();
    found.map_or_else(
        || {
            axum::response::IntoResponse::into_response((
                axum::http::StatusCode::NOT_FOUND,
                "not found",
            ))
        },
        |bytes| {
            axum::response::Response::builder()
                .status(axum::http::StatusCode::OK)
                .header(axum::http::header::CONTENT_TYPE, "application/octet-stream")
                .header(axum::http::header::CONTENT_LENGTH, bytes.len())
                .body(axum::body::Body::from(bytes))
                .expect("response")
        },
    )
}

/// SHA-256 of `data` as lowercase hex.
pub fn sha256_hex(data: &[u8]) -> String {
    use sha2::Digest as _;
    hex::encode(sha2::Sha256::digest(data))
}
