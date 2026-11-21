//! wn/wnd v0.18.0 interop harness, ported from `crates/interop/tests/support`
//! (the parts that do not depend on mk-core): an in-process NIP-01
//! `LocalRelay` behind a TCP front that serves the NIP-11 document, per-device
//! `wnd` daemons over `WN_HOME`, `wn --json` invocation, and evidence JSONL
//! recording into `spike/m6/evidence/wnd/`.

#![allow(dead_code)]

use nostr::types::RelayUrl;
use nostr_sdk::client::Client;
use nostr_sdk::local_relay::{LocalRelay, RateLimit};
use serde_json::{Value, json};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// Documented wnd settle: after the roster is visible the daemon finishes
/// post-join fanout/subscription install with no completion signal.
pub const WND_SETTLE_MS: u64 = 3_000;

pub fn wn_bin() -> PathBuf {
    PathBuf::from(std::env::var("WN_BIN").expect("WN_BIN must point at the wn binary"))
}

pub fn wnd_bin() -> PathBuf {
    PathBuf::from(std::env::var("WND_BIN").expect("WND_BIN must point at the wnd binary"))
}

fn evidence_dir() -> PathBuf {
    let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("evidence/wnd");
    std::fs::create_dir_all(&dir).expect("evidence dir");
    dir
}

/// Scenarios whose evidence file was already truncated this run. The first
/// `record` per scenario truncates so each file holds only the final run.
static BEGUN: std::sync::OnceLock<std::sync::Mutex<std::collections::HashSet<String>>> =
    std::sync::OnceLock::new();

/// Append one evidence line to `evidence/wnd/<scenario>.jsonl` and print it.
/// The first line of a scenario truncates the file.
pub fn record(scenario: &str, step: &str, payload: Value) {
    let line = json!({
        "step": step,
        "at_unix_ms": std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0),
        "data": payload,
    });
    let text = serde_json::to_string(&line).unwrap();
    println!("    [{scenario}] {step}: {text}");
    let path = evidence_dir().join(format!("{scenario}.jsonl"));
    let fresh = BEGUN
        .get_or_init(|| std::sync::Mutex::new(std::collections::HashSet::new()))
        .lock()
        .unwrap()
        .insert(scenario.to_string());
    use std::io::Write as _;
    let mut file = if fresh {
        std::fs::File::create(path).expect("evidence file")
    } else {
        std::fs::OpenOptions::new()
            .append(true)
            .open(path)
            .expect("evidence file")
    };
    writeln!(file, "{text}").unwrap();
}

/// `wn --json` call; asserts ok and returns the parsed payload. Records raw
/// JSON into the scenario's evidence file.
pub fn wn(
    scenario: &str,
    home: &Path,
    socket: &Path,
    account: Option<&str>,
    args: &[&str],
) -> Value {
    let (ok, parsed, raw) = wn_run(home, Some(socket), account, args, None);
    record(
        scenario,
        &format!("wn {}", args.join(" ")),
        json!({"ok": ok, "raw": raw}),
    );
    assert!(ok, "wn {args:?} refused: {raw}");
    parsed
}

/// Tolerant variant for calls whose refusal is itself evidence.
pub fn wn_try(
    scenario: &str,
    home: &Path,
    socket: &Path,
    account: Option<&str>,
    args: &[&str],
) -> (bool, Value) {
    let (ok, parsed, raw) = wn_run(home, Some(socket), account, args, None);
    record(
        scenario,
        &format!("wn {}", args.join(" ")),
        json!({"ok": ok, "raw": raw}),
    );
    (ok, parsed)
}

/// `wn login --nsec-stdin`: the nsec goes through stdin, never argv.
pub fn wn_login_nsec(scenario: &str, home: &Path, socket: &Path, nsec: &str) -> Value {
    let (ok, parsed, raw) = wn_run(home, Some(socket), None, &["login", "--nsec-stdin"], Some(nsec));
    record(
        scenario,
        "wn login --nsec-stdin",
        json!({"ok": ok, "raw": raw}),
    );
    assert!(ok, "wn login refused: {raw}");
    parsed
}

/// Commands wn forbids through the wnd socket (e.g. `logout`: it removes a
/// local account) run directly against `WN_HOME` instead.
pub fn wn_direct_try(
    scenario: &str,
    home: &Path,
    account: Option<&str>,
    args: &[&str],
) -> (bool, Value) {
    let (ok, parsed, raw) = wn_run(home, None, account, args, None);
    record(
        scenario,
        &format!("wn-direct {}", args.join(" ")),
        json!({"ok": ok, "raw": raw}),
    );
    (ok, parsed)
}

fn wn_run(
    home: &Path,
    socket: Option<&Path>,
    account: Option<&str>,
    args: &[&str],
    stdin_text: Option<&str>,
) -> (bool, Value, String) {
    let mut cmd = Command::new(wn_bin());
    cmd.arg("--json")
        .arg("--home")
        .arg(home)
        .env("WN_HOME", home)
        .env("WN_SECRET_STORE", "file")
        .env("WN_ALLOW_LOOPBACK_RELAYS", "1")
        .env("WN_ALLOW_LOOPBACK_BLOB_ENDPOINTS", "1");
    if let Some(socket) = socket {
        cmd.arg("--socket").arg(socket);
    }
    if let Some(account) = account {
        cmd.arg("--account").arg(account);
    }
    cmd.args(args);
    let out = if let Some(text) = stdin_text {
        use std::io::Write as _;
        let mut child = cmd
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("spawn wn");
        child
            .stdin
            .take()
            .unwrap()
            .write_all(format!("{text}\n").as_bytes())
            .expect("write nsec");
        child.wait_with_output().expect("wn output")
    } else {
        cmd.output().expect("spawn wn")
    };
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
/// panic unwind. Its `WN_HOME` is removed; its logs stay under `target/wn-logs/`.
#[derive(Debug)]
pub struct Wnd {
    child: Option<Child>,
    home: PathBuf,
}

impl Wnd {
    /// Kill the daemon but keep its WN_HOME — for a restart that must
    /// preserve the account state living there. Clears `home` so the
    /// `Drop` impl does not remove it when this guard is replaced.
    pub fn shutdown_keep_home(&mut self) {
        if let Some(mut child) = self.child.take() {
            drop(child.kill());
            drop(child.wait());
        }
        self.home = PathBuf::new();
    }
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

/// Spawn `wnd` against the relay and wait until its socket answers `wn whoami`.
pub async fn spawn_wnd(home: &Path, socket: &Path, logs: &Path, relay_url: &str) -> Wnd {
    std::fs::create_dir_all(home).expect("wn home");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
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

/// A TCP front that answers NIP-11 relay-information probes itself and
/// splices every other connection to the `LocalRelay` byte-for-byte.
/// nostr-sdk's `LocalRelay` serves no relay document; wnd's post-join
/// history sweep reads it.
#[derive(Debug)]
pub struct Nip11Front {
    pub url: String,
    task: tokio::task::JoinHandle<()>,
}

impl Nip11Front {
    async fn bind(backend: String, document: String) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind nip11 front");
        let address = listener.local_addr().expect("front addr");
        let task = tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                tokio::spawn(front_connection(stream, backend.clone(), document.clone()));
            }
        });
        Self {
            url: format!("ws://{address}"),
            task,
        }
    }
}

impl Drop for Nip11Front {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn front_connection(mut stream: tokio::net::TcpStream, backend: String, document: String) {
    let Some(head) = read_head(&mut stream).await else {
        return;
    };
    if is_nip11_probe(&head) {
        let response = format!(
            "HTTP/1.1 200 OK\r\ncontent-type: application/nostr+json\r\naccess-control-allow-origin: *\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{document}",
            document.len()
        );
        drop(stream.write_all(response.as_bytes()).await);
        return;
    }
    let Ok(mut upstream) = tokio::net::TcpStream::connect(&backend).await else {
        return;
    };
    if upstream.write_all(&head).await.is_err() {
        return;
    }
    drop(tokio::io::copy_bidirectional(&mut stream, &mut upstream).await);
}

async fn read_head(stream: &mut tokio::net::TcpStream) -> Option<Vec<u8>> {
    let mut head = Vec::new();
    let mut chunk = [0_u8; 1024];
    while !head.windows(4).any(|tail| tail == b"\r\n\r\n") {
        if head.len() >= 16 * 1024 {
            return None;
        }
        let n = stream.read(&mut chunk).await.ok()?;
        if n == 0 {
            return None;
        }
        head.extend_from_slice(&chunk[..n]);
    }
    Some(head)
}

fn is_nip11_probe(head: &[u8]) -> bool {
    let text = String::from_utf8_lossy(head);
    let mut accept = false;
    let mut upgrade = false;
    for line in text
        .split("\r\n")
        .skip(1)
        .take_while(|line| !line.is_empty())
    {
        if let Some((name, value)) = line.split_once(':') {
            let name = name.trim();
            if name.eq_ignore_ascii_case("accept") && value.contains("application/nostr+json") {
                accept = true;
            }
            if name.eq_ignore_ascii_case("upgrade") {
                upgrade = true;
            }
        }
    }
    accept && !upgrade
}

/// One shared relay + NIP-11 front + nostr client, as every scenario needs it.
pub struct Relay {
    pub relay: LocalRelay,
    pub client: Client,
    pub url: String,
    front: Nip11Front,
}

pub async fn start_relay() -> Relay {
    const MAX_FILTER_LIMIT: usize = 10_000;
    let relay = LocalRelay::builder()
        .rate_limit(RateLimit {
            max_reqs: 500,
            notes_per_minute: 1_000_000,
        })
        .max_filter_limit(MAX_FILTER_LIMIT)
        .build();
    relay.run().await.expect("local relay run");
    let document = serde_json::json!({
        "name": "m6-spike",
        "supported_nips": [1, 11],
        "limitation": {"max_limit": MAX_FILTER_LIMIT},
    });
    let backend = relay
        .url()
        .await
        .as_str()
        .strip_prefix("ws://")
        .expect("local relay ws url")
        .trim_end_matches('/')
        .to_owned();
    let front = Nip11Front::bind(backend, document.to_string()).await;
    let client = Client::builder().build();
    client
        .add_relay(RelayUrl::parse(&front.url).expect("relay url"))
        .and_connect()
        .await
        .expect("client connect");
    Relay {
        relay,
        client,
        url: front.url.clone(),
        front,
    }
}

/// One wn device: its own `wnd` daemon and `WN_HOME` on the shared relay.
pub struct WnDevice {
    pub home: PathBuf,
    pub socket: PathBuf,
    pub logs: PathBuf,
    pub wnd: Wnd,
}

pub async fn spawn_device(tag: &str, relay_url: &str) -> WnDevice {
    let stamp = format!("{tag}-{}", std::process::id());
    let home = std::env::temp_dir().join(format!("m6-{stamp}"));
    drop(std::fs::remove_dir_all(&home));
    let socket = home.join("w.sock");
    let logs = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("target/wn-logs")
        .join(&stamp);
    let wnd = spawn_wnd(&home, &socket, &logs, relay_url).await;
    WnDevice {
        home,
        socket,
        logs,
        wnd,
    }
}

/// `wn create-identity` on the device; returns the npub.
pub fn create_identity(scenario: &str, dev: &WnDevice) -> String {
    let created = wn(scenario, &dev.home, &dev.socket, None, &["create-identity"]);
    created
        .pointer("/result/npub")
        .and_then(Value::as_str)
        .expect("create-identity npub")
        .to_owned()
}

/// `wn whoami` npub of the first account, or None.
pub fn whoami_npub(scenario: &str, dev: &WnDevice) -> Option<String> {
    let who = wn(scenario, &dev.home, &dev.socket, None, &["whoami"]);
    who.pointer("/result/accounts/0/npub")
        .and_then(Value::as_str)
        .map(str::to_owned)
}

/// Poll `f` until it returns Some or `timeout` passes; evidence-friendly.
pub async fn wait_for<T, F>(label: &str, timeout: Duration, mut f: F) -> Option<T>
where
    F: FnMut() -> Option<T>,
{
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(value) = f() {
            return Some(value);
        }
        if Instant::now() >= deadline {
            println!("    wait_for {label}: timed out");
            return None;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

pub async fn settle() {
    tokio::time::sleep(Duration::from_millis(WND_SETTLE_MS)).await;
}
