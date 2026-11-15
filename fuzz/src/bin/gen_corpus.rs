//! Generate the committed seed corpus from real session artifacts.
//!
//! Run once when the corpus needs refreshing:
//! `cargo run --features corpus-gen --bin gen_corpus`.
//! The session flow mirrors `crates/mk-core/tests/support/mod.rs`:
//! open (account-proof handshake), publish a KeyPackage, then serialize the
//! first durable batch, the durable rows, the signed proof event and the
//! KeyPackage event into `corpus/<target>/`.

use std::path::PathBuf;
use std::sync::Arc;

use mk_core::testing::{LocalSigner, ManualClock, RecordingHost};
use mk_core::{AccountSigner, Opened, RelayPolicy, Session, SessionConfig};

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
        relay_policy: RelayPolicy {
            allow_loopback: true,
        },
        client_name: None,
    }
}

fn write(corpus: &std::path::Path, target: &str, name: &str, bytes: &[u8]) {
    let path = corpus.join(target).join(name);
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
}
