//! Fuzz the C ABI: arbitrary bytes through `mk_bridge_call` on a real
//! handle (dedicated engine thread, in-memory host callbacks), with
//! `mk_bridge_free` + `mk_bridge_new` cycles every N inputs. Every
//! response must decode as a `Response` envelope.

#![no_main]

use std::ffi::c_void;
use std::ptr;
use std::sync::{Mutex, OnceLock};

use libfuzzer_sys::fuzz_target;
use marmot_ffi::{
    MkBridge, MkBytes, MkHostVtable, mk_bridge_call, mk_bridge_free, mk_bridge_new, mk_bridge_open,
    mk_bytes_free,
};
use mk_bridge::protocol::{
    CallEnvelope, Config, OpenResponse, PROTOCOL_VERSION, Request, Response, SignerOp, SignerReply,
    SignerReplyRequest, SignerResult, SubmitProofRequest, Time,
};
use mk_core::testing::LocalSigner;
use mk_core::{AccountId, AccountSigner};
use tokio::runtime::Runtime;

/// The host `load` serves one static empty-rows blob; it stays valid
/// for the whole process, satisfying the vtable's buffer contract.
unsafe extern "C" fn host_load(
    _ctx: *mut c_void,
    ptr: *mut *const u8,
    len: *mut usize,
) -> i32 {
    static ROWS: OnceLock<Vec<u8>> = OnceLock::new();
    let rows = ROWS.get_or_init(|| mk_bridge::encode_rows(&[]));
    // SAFETY: `ptr`/`len` are caller-provided out-pointers per the
    // `MkHostVtable` contract; `rows` is a 'static buffer.
    unsafe {
        ptr::write(ptr, rows.as_ptr());
        ptr::write(len, rows.len());
    }
    0
}

/// Commits are notifications; the in-memory host accepts and drops them.
unsafe extern "C" fn host_commit(_ctx: *mut c_void, _ptr: *const u8, _len: usize) -> i32 {
    0
}

static VTABLE: MkHostVtable = MkHostVtable {
    load: Some(host_load),
    commit: Some(host_commit),
    release: None,
};

fn runtime() -> &'static Runtime {
    static RUNTIME: OnceLock<Runtime> = OnceLock::new();
    RUNTIME.get_or_init(|| {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime")
    })
}

fn signer() -> &'static LocalSigner {
    static SIGNER: OnceLock<LocalSigner> = OnceLock::new();
    SIGNER.get_or_init(|| LocalSigner::from_secret([0x42; 32]).expect("fixed secret"))
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

/// Copy out the `MkBytes` a call wrote, then release the buffer.
fn take(out: MkBytes) -> Vec<u8> {
    if out.ptr.is_null() {
        return Vec::new();
    }
    // SAFETY: `out` was produced by `mk_bridge_*`; [ptr, ptr+len) is a
    // readable Rust-owned buffer until `mk_bytes_free` releases it.
    let bytes = unsafe { std::slice::from_raw_parts(out.ptr, out.len) }.to_vec();
    // SAFETY: `out` came from this library and is freed exactly once.
    unsafe { mk_bytes_free(out) };
    bytes
}

/// `mk_bridge_call` wrapper returning the decoded response.
fn call(handle: *mut MkBridge, request: &[u8]) -> Response {
    let mut out = MkBytes {
        ptr: ptr::null_mut(),
        len: 0,
    };
    // SAFETY: `handle` is a live handle owned by the guarded state;
    // `request` is readable for the call's duration; `out` names valid
    // `MkBytes` storage.
    unsafe { mk_bridge_call(handle, request.as_ptr(), request.len(), &mut out) };
    serde_json::from_slice(&take(out)).expect("call response must decode")
}

fn call_envelope(now: u64, request: Request) -> Vec<u8> {
    serde_json::to_vec(&CallEnvelope {
        now: Time {
            unix_ms: 1_700_000_000_000 + now,
            monotonic_ms: now,
        },
        request: serde_json::to_value(&request).expect("request encodes"),
    })
    .expect("envelope encodes")
}

/// A fresh handle opened to `Ready`: config open, proof signed, signer
/// requests pumped through `mk_bridge_call`.
async fn new_handle() -> *mut MkBridge {
    let signer = signer();
    // SAFETY: `VTABLE` is 'static; NULL `ctx` is fine — the in-memory
    // callbacks never read it.
    let handle = unsafe { mk_bridge_new(&VTABLE, ptr::null_mut()) };
    assert!(!handle.is_null(), "mk_bridge_new failed");
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
    let mut out = MkBytes {
        ptr: ptr::null_mut(),
        len: 0,
    };
    // SAFETY: `handle` is live; `config` is readable for the call's
    // duration; `out` names valid `MkBytes` storage.
    unsafe { mk_bridge_open(handle, config.as_ptr(), config.len(), &mut out) };
    let reply = take(out);
    let open: OpenResponse = serde_json::from_slice(&reply).expect("open envelope decodes");
    let mut now = 1u64;
    if let OpenResponse::NeedsAccountProof { unsigned } = open {
        let signed = signer
            .sign_event(unsigned.into())
            .await
            .expect("sign proof");
        let mut response = call(
            handle,
            &call_envelope(
                now,
                Request::SubmitProof(SubmitProofRequest {
                    event: signed.into(),
                }),
            ),
        );
        for _ in 0..8 {
            let Response::SignerPending { requests } = &response else {
                break;
            };
            let mut replies = Vec::with_capacity(requests.len());
            for request in requests {
                replies.push(SignerReply {
                    id: request.id,
                    result: answer(signer, &request.op).await,
                });
            }
            now += 1;
            response = call(
                handle,
                &call_envelope(now, Request::SignerReply(SignerReplyRequest { replies })),
            );
        }
    }
    handle
}

/// The one live handle plus the per-process call counter. Recreated in
/// place every `RECYCLE_EVERY` calls so `free`/`new` get fuzzed too.
struct State {
    handle: *mut MkBridge,
    calls: u64,
}

// SAFETY: the raw handle is touched only while the state's own `Mutex`
// is held; libFuzzer drives `fuzz_target` sequentially.
unsafe impl Send for State {}

const RECYCLE_EVERY: u64 = 256;

fuzz_target!(|data: &[u8]| {
    static STATE: OnceLock<Mutex<State>> = OnceLock::new();
    runtime().block_on(async move {
        let state = STATE.get_or_init(|| {
            Mutex::new(State {
                handle: ptr::null_mut(),
                calls: 0,
            })
        });
        let mut state = state.lock().unwrap_or_else(|error| error.into_inner());
        if state.calls % RECYCLE_EVERY == 0 {
            if !state.handle.is_null() {
                // SAFETY: `handle` was produced by `mk_bridge_new` and is
                // freed exactly once, here.
                unsafe { mk_bridge_free(state.handle) };
            }
            state.handle = new_handle().await;
        }
        state.calls += 1;
        let _ = call(state.handle, data);
    });
});
