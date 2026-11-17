#![no_main]

use std::sync::OnceLock;

use libfuzzer_sys::fuzz_target;
use mk_bridge::protocol::{
    CallEnvelope, Config, OpenResponse, PROTOCOL_VERSION, Request, Response, SignerOp, SignerReply,
    SignerReplyRequest, SignerResult, SubmitProofRequest, Time,
};
use mk_bridge::{Bridge, Host, HostError};
use mk_core::testing::LocalSigner;
use mk_core::{AccountId, AccountSigner};
use tokio::runtime::Runtime;

struct MemHost;

impl Host for MemHost {
    fn load(&mut self) -> Result<Vec<u8>, HostError> {
        Ok(mk_bridge::encode_rows(&[]))
    }

    fn commit(&mut self, _batch: &[u8]) -> Result<(), HostError> {
        Ok(())
    }
}

fn runtime() -> &'static Runtime {
    static RUNTIME: OnceLock<Runtime> = OnceLock::new();
    RUNTIME.get_or_init(|| {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime")
    })
}

async fn answer(signer: &LocalSigner, op: &SignerOp) -> SignerResult {
    match op {
        SignerOp::SignEvent { unsigned } => signer
            .sign_event(unsigned.clone().into())
            .await
            .map(|event| SignerResult::Signed {
                event: event.into(),
            })
            .unwrap_or(SignerResult::Refused),
        SignerOp::Nip44Encrypt { peer, plaintext } => signer
            .nip44_encrypt(&AccountId(*peer), plaintext)
            .await
            .map(|text| SignerResult::Text { text })
            .unwrap_or(SignerResult::Refused),
        SignerOp::Nip44Decrypt { peer, ciphertext } => signer
            .nip44_decrypt(&AccountId(*peer), ciphertext)
            .await
            .map(|text| SignerResult::Text { text })
            .unwrap_or(SignerResult::Refused),
    }
}

fn call_bytes(now: u64, request: Request) -> Vec<u8> {
    serde_json::to_vec(&CallEnvelope {
        now: Time {
            unix_ms: 1_700_000_000_000 + now,
            monotonic_ms: now,
        },
        request: serde_json::to_value(&request).expect("request encodes"),
    })
    .expect("envelope encodes")
}

/// A freshly opened, proof-authenticated bridge on an empty in-memory host.
async fn open_bridge() -> Bridge<MemHost> {
    let signer = LocalSigner::from_secret([0x42; 32]).expect("fixed secret");
    let mut bridge = Bridge::new(MemHost);
    let config = serde_json::to_vec(&Config {
        protocol_version: PROTOCOL_VERSION,
        account: signer.account().0,
        now: Time {
            unix_ms: 1_700_000_000_000,
            monotonic_ms: 0,
        },
        allow_loopback: true,
        client_name: None,
    })
    .expect("config encodes");
    let OpenResponse::NeedsAccountProof { unsigned } =
        serde_json::from_slice(&bridge.open(&config)).expect("open decodes")
    else {
        return bridge;
    };
    let signed = signer
        .sign_event(unsigned.into())
        .await
        .expect("sign proof");
    let mut now = 1u64;
    let mut response: Response = serde_json::from_slice(&bridge.call(&call_bytes(
        now,
        Request::SubmitProof(SubmitProofRequest {
            event: signed.into(),
        }),
    )))
    .expect("response decodes");
    for _ in 0..8 {
        let Response::SignerPending { requests } = &response else {
            break;
        };
        let mut replies = Vec::with_capacity(requests.len());
        for request in requests {
            replies.push(SignerReply {
                id: request.id,
                result: answer(&signer, &request.op).await,
            });
        }
        now += 1;
        response = serde_json::from_slice(&bridge.call(&call_bytes(
            now,
            Request::SignerReply(SignerReplyRequest { replies }),
        )))
        .expect("response decodes");
    }
    bridge
}

fuzz_target!(|data: &[u8]| {
    runtime().block_on(async move {
        let mut bridge = open_bridge().await;
        let response = bridge.call(data);
        let decoded: Response =
            serde_json::from_slice(&response).expect("call response must decode");
        let _ = decoded;
    });
});
