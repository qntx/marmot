//! Generate the seed corpus from real session artifacts.
//!
//! `fuzz/corpus/` is gitignored — regenerate seeds whenever the row formats
//! change; CI fuzzes from an empty corpus. Run:
//! `cargo run --features corpus-gen --bin gen_corpus`.
//! The session flow mirrors `crates/mk-core/tests/support/mod.rs`:
//! open (account-proof handshake), publish a KeyPackage, then serialize the
//! first durable batch, the durable rows, the signed proof event and the
//! KeyPackage event into `corpus/<target>/`.

use std::path::PathBuf;
use std::sync::Arc;

use mk_core::testing::{LocalSigner, ManualClock, RecordingHost};
use mk_core::{
    AccountSigner, AppMessage, CreateGroup, DestinationPolicy, Opened, Session, SessionConfig,
};

fn config(
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

fn write(corpus: &std::path::Path, target: &str, name: &str, bytes: &[u8]) {
    let dir = corpus.join(target);
    std::fs::create_dir_all(&dir).unwrap_or_else(|err| panic!("mkdir {dir:?}: {err}"));
    let path = dir.join(name);
    std::fs::write(&path, bytes).unwrap_or_else(|err| panic!("write {path:?}: {err}"));
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let corpus = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("corpus");
    let signer = Arc::new(LocalSigner::new());
    let clock = Arc::new(ManualClock::new());
    let host = Arc::new(RecordingHost::new());

    let opened = Session::open(config(
        Arc::clone(&signer),
        Arc::clone(&clock),
        Arc::clone(&host),
        Vec::new(),
    ))
    .expect("open");
    let mut session = match opened {
        Opened::NeedsAccountProof(pending) => {
            let request = pending.request().clone();
            let signed = signer.sign_event(request).await.expect("sign proof");
            let json = signed.to_json();
            let session = pending.provide_account_proof(&signed).expect("proof");
            write(
                &corpus,
                "account_proof",
                "signed-proof.json",
                json.as_bytes(),
            );
            session
        }
        Opened::Ready(session) => *session,
    };

    session
        .publish_key_package(&["ws://localhost:7777".to_string()])
        .await
        .expect("key package");
    if let Some(seq) = session.awaiting_durability() {
        session.acknowledge_durable(seq);
    }
    let outbound = session.take_outbound().expect("take_outbound");
    let key_package = outbound.first().expect("key package outbound");
    let kp_json = key_package.event.to_json();
    write(
        &corpus,
        "key_package_event",
        "key-package.json",
        kp_json.as_bytes(),
    );
    write(
        &corpus,
        "event_json",
        "key-package.json",
        kp_json.as_bytes(),
    );

    // key_package_selection: the publication plus a kind-5 request covering
    // it (slot `a` and id `e`), framed as the target decodes — unsigned
    // halves only; the target re-signs with its own fixed account.
    let unsigned = |event: &mk_core::Event| {
        serde_json::json!({
            "pubkey": event.pubkey.to_hex(),
            "created_at": event.created_at.to_string(),
            "kind": event.kind,
            "tags": event.tags,
            "content": event.content,
        })
    };
    let slot = key_package
        .event
        .tags
        .iter()
        .find(|tag| tag.first().map(String::as_str) == Some("d"))
        .and_then(|tag| tag.get(1))
        .cloned()
        .unwrap_or_default();
    let deletion = mk_core::UnsignedEvent {
        pubkey: signer.account(),
        created_at: key_package.event.created_at,
        kind: 5,
        tags: vec![
            vec![
                "a".to_owned(),
                format!("30443:{}:{slot}", signer.account().to_hex()),
            ],
            vec!["e".to_owned(), key_package.event.id.to_hex()],
        ],
        content: String::new(),
    };
    let deletion = signer
        .sign_event(deletion)
        .await
        .expect("sign deletion seed");
    let seed = serde_json::json!({
        "now": key_package.event.created_at,
        "events": [unsigned(&key_package.event), unsigned(&deletion)],
    });
    write(
        &corpus,
        "key_package_selection",
        "selection-0",
        serde_json::to_string(&seed)
            .expect("seed encodes")
            .as_bytes(),
    );

    // A real durable batch as emitted by the host boundary.
    let batch = host
        .batches()
        .first()
        .expect("at least one durable batch")
        .encode();
    write(&corpus, "kv_batch", "batch-0", &batch);

    // The durable row set after the identity publications, laid out in the
    // fuzz derivation format: 64-byte id prefix, then `[klen u16][key][vlen u16][val]`.
    let rows = host.durable_rows();
    let mut kv_rows = vec![0u8; 64];
    for (key, value) in &rows {
        assert!(key.len() <= u16::MAX as usize, "seed key too long");
        assert!(value.len() <= u16::MAX as usize, "seed value too long");
        kv_rows.extend_from_slice(&(key.len() as u16).to_le_bytes());
        kv_rows.extend_from_slice(key);
        kv_rows.extend_from_slice(&(value.len() as u16).to_le_bytes());
        kv_rows.extend_from_slice(value);
    }
    write(&corpus, "kv_rows", "rows-0", &kv_rows);

    // imeta_tag: a real `imeta` tag plus a mutated one.
    write(
        &corpus,
        "imeta_tag",
        "imeta-valid",
        b"v encrypted-media-v2\0locator blossom-v1 https://blossom.example/aa\0ciphertext_sha256 0000000000000000000000000000000000000000000000000000000000000001\0plaintext_sha256 0000000000000000000000000000000000000000000000000000000000000002\0nonce aaaaaaaaaaaaaaaaaaaaaaaa\0m image/png\0filename pic.png",
    );

    // stream_anchor: a valid stream-start tag set, newline/tab format.
    write(
        &corpus,
        "stream_anchor",
        "stream-start",
        format!(
            "stream\t{}\nstream-type\ttext\nfinal-kind\t9\nparent\t{}\nroute\tquic\nbroker\tquic://b1\nbroker\tquic://b2",
            "07".repeat(32),
            "08".repeat(32),
        )
        .as_bytes(),
    );
    // stream_anchor: a real encoded 0x8006 policy (from_component path).
    if let Ok(component) =
        mk_core::agent_stream::AgentStreamPolicy::user_to_agent_default().to_component()
    {
        write(&corpus, "stream_anchor", "policy-0x8006", &component.data);
    }

    // bridge_request + ffi_call: every `Request` variant wrapped in a
    // call envelope, plus the canonical `call` vector. `ffi_call` takes
    // the same bytes — they feed `mk_bridge_call` verbatim.
    let vectors = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("vectors")
        .join("bridge");
    let requests: serde_json::Map<String, serde_json::Value> = serde_json::from_str(
        &std::fs::read_to_string(vectors.join("request.json")).expect("request vector"),
    )
    .expect("request vector parses");
    for (variant, request) in &requests {
        let envelope = serde_json::json!({
            "now": {"unix_ms": "1700000000000", "monotonic_ms": "0"},
            "request": request,
        });
        let bytes = serde_json::to_string(&envelope)
            .expect("envelope encodes")
            .into_bytes();
        write(
            &corpus,
            "bridge_request",
            &format!("request-{variant}"),
            &bytes,
        );
        write(&corpus, "ffi_call", &format!("request-{variant}"), &bytes);
    }
    for target in ["bridge_request", "ffi_call"] {
        write(
            &corpus,
            target,
            "call",
            &std::fs::read(vectors.join("call.json")).expect("call vector"),
        );
    }

    // bridge_config: the canonical config, plus a real encoded durable-row
    // set as `Host::load` input.
    write(
        &corpus,
        "bridge_config",
        "config",
        &std::fs::read(vectors.join("config.json")).expect("config vector"),
    );
    write(
        &corpus,
        "bridge_config",
        "rows-0",
        &mk_bridge::encode_rows(&rows),
    );

    // session_rows: a group, one sent message, and an outbound item still
    // staged (no take_outbound since the send) — the durable frontier after
    // every commit is acknowledged.
    let group = session
        .create_group(CreateGroup {
            name: "fuzz seed".to_owned(),
            description: "d".to_owned(),
            relays: vec!["ws://localhost:7777".to_owned()],
            invitees: Vec::new(),
            admins: Vec::new(),
            components: Vec::new(),
        })
        .await
        .expect("create group");
    session
        .send_message(
            &group,
            AppMessage {
                kind: 9,
                tags: Vec::new(),
                content: "seed message".to_owned(),
            },
        )
        .await
        .expect("send seed");
    while let Some(seq) = session.awaiting_durability() {
        session.acknowledge_durable(seq);
    }
    write(
        &corpus,
        "session_rows",
        "rows-0",
        &mk_bridge::encode_rows(&host.durable_rows()),
    );
}
