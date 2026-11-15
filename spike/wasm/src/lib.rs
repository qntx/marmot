//! wasm32 bindings for the M0 spike: MDK `cgka-engine` + the KV storage
//! provider behind a JS host boundary (wasm-bindgen).
//!
//! The account key NEVER enters wasm. The JS side supplies a signer object:
//!
//! ```text
//! signer = {
//!   publicKey(): string                          // hex, sync
//!   signAccountIdentityProof(bytes): Uint8Array  // sync; 64-byte schnorr sig
//!                                              // over the serialized proof
//!                                              // UnsignedEvent
//!   signEvent(unsignedJson): Promise<string>     // async; signed event JSON
//!   nip44Encrypt(pubkeyHex, plaintext): Promise<string>
//!   nip44Decrypt(pubkeyHex, ciphertext): Promise<string>
//! }
//! ```
//!
//! Storage is `Client.open({ entries, onCommit, signer, nowMs? })`:
//! `entries` = `[[hexKey, hexValue], ...]` persisted state; `onCommit(batch)`
//! receives each atomic batch as `[{ op, key, value }]` (hex strings) — the
//! host persists it and must guarantee durability before publishing whatever
//! the batch staged (see spike/STORAGE.md).
//!
//! wasm-bindgen is the binding mechanism (not a C ABI): the engine and peeler
//! on wasm32 already require wasm-bindgen glue (`getrandom 0.2/js`,
//! `gloo-timers`, `openmls/js`, `web-time`), and the engine API is async — a C
//! ABI would need our own executor plus callback plumbing for every await.

use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, Mutex};

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as B64;
use cgka_engine::account_identity_proof::{
    AccountIdentityProofRequest, AccountIdentityProofSigner,
};
use cgka_engine::convergence_clock::ManualConvergenceClock;
use cgka_engine::{Engine, EngineBuilder};
use cgka_traits::app_event::MarmotAppEvent;
use cgka_traits::engine::{
    CgkaEngine, CreateGroupRequest, GroupEvent, SendIntent, SendResult,
};
use cgka_traits::engine_state::PendingStateRef;
use cgka_traits::group::ProtocolProfile;
use cgka_traits::ingest::{IngestOutcome, InputRejectionCategory};
use cgka_traits::maintenance::WallClock;
use cgka_traits::storage::MessageStorage;
use cgka_traits::transport::TransportMessage;
use cgka_traits::types::{GroupId, MessageId};
use js_sys::{Array, Function, Object, Promise, Reflect, Uint8Array};
use marmot_spike::harness::{
    KIND_MARMOT_KEY_PACKAGE, chat_payload, key_package_from_event, routing_component,
    supported_components, supported_features,
};
use marmot_spike::{KvHost, KvOp, KvStorageProvider};
use nostr::prelude::{Event, PublicKey, Signature, UnsignedEvent};
use serde::Serialize;
use serde_json::{Value, json};
use transport_nostr_peeler::{
    MarmotNostrSigner, MarmotSignerError, NostrMlsPeeler, NostrTransportEvent, SignerFuture,
};
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::JsFuture;

// ---------------------------------------------------------------------------
// JS object registry
//
// js_sys::Object is !Send/!Sync while the engine's signer/host traits require
// Send + Sync. wasm32-unknown-unknown is single-threaded, so JS objects live
// in a thread_local registry and trait objects carry only the u32 handle —
// no unsafe impls anywhere.
// ---------------------------------------------------------------------------

thread_local! {
    static JS_OBJECTS: RefCell<HashMap<u32, Object>> = RefCell::new(HashMap::new());
    static NEXT_JS_ID: Cell<u32> = const { Cell::new(1) };
}

fn register_object(obj: Object) -> u32 {
    let id = NEXT_JS_ID.with(|n| {
        let id = n.get();
        n.set(id + 1);
        id
    });
    JS_OBJECTS.with(|m| m.borrow_mut().insert(id, obj));
    id
}

fn lookup_object(id: u32) -> Result<Object, String> {
    JS_OBJECTS
        .with(|m| m.borrow().get(&id).cloned())
        .ok_or_else(|| format!("js object {id} is gone"))
}

fn js_method(id: u32, name: &str) -> Result<(Object, Function), String> {
    let obj = lookup_object(id)?;
    let method = Reflect::get(&obj, &JsValue::from_str(name))
        .and_then(JsValue::dyn_into::<Function>)
        .map_err(|_| format!("js object {id}: {name} is not a function"))?;
    Ok((obj, method))
}

// ---------------------------------------------------------------------------
// JS-backed KvHost. `commit` is synchronous — the engine cannot await — so an
// async host (IndexedDB) must enqueue the batch and flush it to durability
// before publishing whatever it staged (spike/STORAGE.md).
// ---------------------------------------------------------------------------

struct JsKvHost {
    callback_id: u32,
    initial: Mutex<Vec<(Vec<u8>, Vec<u8>)>>,
}

impl KvHost for JsKvHost {
    fn load_all(&self) -> Vec<(Vec<u8>, Vec<u8>)> {
        std::mem::take(&mut *self.initial.lock().unwrap())
    }

    fn commit(&self, batch: Vec<KvOp>) {
        let ops = Array::new();
        for op in batch {
            let o = Object::new();
            match op {
                KvOp::Put(k, v) => {
                    set_str(&o, "op", "put");
                    set_str(&o, "key", &hex::encode(k));
                    set_str(&o, "value", &hex::encode(v));
                }
                KvOp::Delete(k) => {
                    set_str(&o, "op", "delete");
                    set_str(&o, "key", &hex::encode(k));
                }
            }
            ops.push(&o);
        }
        // The registered object IS the onCommit function.
        match lookup_object(self.callback_id) {
            Ok(cb) => {
                let f: &Function = cb.unchecked_ref();
                if let Err(e) = f.call1(&JsValue::NULL, &ops) {
                    log(&format!("onCommit threw: {e:?}"));
                }
            }
            Err(e) => log(&e),
        }
    }
}

fn set_str(obj: &Object, key: &str, value: &str) {
    let _ = Reflect::set(obj, &JsValue::from_str(key), &JsValue::from_str(value));
}

#[wasm_bindgen]
extern "C" {
    #[wasm_bindgen(js_namespace = console)]
    fn error(s: &str);
}

fn log(msg: &str) {
    error(msg);
}

// ---------------------------------------------------------------------------
// JS-backed signers. `signAccountIdentityProof` is SYNC because the engine's
// AccountIdentityProofSigner hook is synchronous — a browser remote signer
// (NIP-07/46) cannot serve it; the JS key must sign locally (spike finding).
// ---------------------------------------------------------------------------

struct JsAccountSigner {
    id: u32,
    pubkey_hex: String,
    pubkey_bytes: [u8; 32],
}

impl fmt::Debug for JsAccountSigner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("JsAccountSigner")
            .field("public_key", &self.pubkey_hex)
            .finish()
    }
}

impl AccountIdentityProofSigner for JsAccountSigner {
    fn sign_account_identity_proof(
        &self,
        request: &AccountIdentityProofRequest,
    ) -> Result<[u8; 64], String> {
        if self.pubkey_bytes.as_slice() != request.account_identity.as_slice() {
            return Err("proof request names a different account".into());
        }
        let unsigned = request.proof_event()?;
        let json = serde_json::to_vec(&unsigned).map_err(|e| format!("proof json: {e}"))?;
        let (obj, f) = js_method(self.id, "signAccountIdentityProof")?;
        let out = f
            .call1(&obj, &Uint8Array::from(json.as_slice()))
            .map_err(|e| format!("signAccountIdentityProof threw: {e:?}"))?;
        let sig_bytes = Uint8Array::new(&out).to_vec();
        let sig =
            Signature::from_slice(&sig_bytes).map_err(|e| format!("bad proof sig: {e}"))?;
        let event = unsigned
            .add_signature(sig)
            .map_err(|e| format!("proof event verify failed: {e}"))?;
        request.signature_from_signed_event(event)
    }
}

/// SignerFuture requires `Send`; `JsFuture` is `!Send`. `SendWrapper` marks
/// it Send and panics if polled on another thread — impossible on
/// single-threaded wasm32, which is exactly why this is safe.
fn wrapped<F: std::future::Future + 'static>(fut: F) -> std::pin::Pin<Box<dyn std::future::Future<Output = F::Output> + Send>> {
    Box::pin(send_wrapper::SendWrapper::new(fut))
}

impl MarmotNostrSigner for JsAccountSigner {
    fn get_public_key(&self) -> SignerFuture<'_, Result<PublicKey, MarmotSignerError>> {
        let result = PublicKey::from_hex(&self.pubkey_hex)
            .map_err(|e| MarmotSignerError::from(format!("bad public key: {e}")));
        wrapped(async move { result })
    }

    fn sign_event(
        &self,
        unsigned: UnsignedEvent,
    ) -> SignerFuture<'_, Result<Event, MarmotSignerError>> {
        let id = self.id;
        wrapped(async move {
            let json = serde_json::to_string(&unsigned)
                .map_err(|e| MarmotSignerError::from(format!("unsigned json: {e}")))?;
            let (obj, f) = js_method(id, "signEvent").map_err(MarmotSignerError::from)?;
            let promise = f
                .call1(&obj, &JsValue::from_str(&json))
                .map_err(|e| MarmotSignerError::from(format!("signEvent threw: {e:?}")))?;
            let out = JsFuture::from(Promise::from(promise))
                .await
                .map_err(|e| MarmotSignerError::from(format!("signEvent rejected: {e:?}")))?;
            let json = out
                .as_string()
                .ok_or_else(|| MarmotSignerError::from("signEvent returned non-string"))?;
            serde_json::from_str::<Event>(&json)
                .map_err(|e| MarmotSignerError::from(format!("signed event json: {e}")))
        })
    }

    fn nip04_encrypt<'a>(
        &'a self,
        _public_key: &'a PublicKey,
        _content: &'a str,
    ) -> SignerFuture<'a, Result<String, MarmotSignerError>> {
        // Stub 1 of 2: NIP-04 is unreachable in this flow — NIP-59 wrapping
        // uses NIP-44 only. Distinct error, counted in the report.
        wrapped(async move {
            Err(MarmotSignerError::from(
                "nip04 encrypt unsupported in spike",
            ))
        })
    }

    fn nip04_decrypt<'a>(
        &'a self,
        _public_key: &'a PublicKey,
        _payload: &'a str,
    ) -> SignerFuture<'a, Result<String, MarmotSignerError>> {
        // Stub 2 of 2: see nip04_encrypt.
        wrapped(async move {
            Err(MarmotSignerError::from(
                "nip04 decrypt unsupported in spike",
            ))
        })
    }

    fn nip44_encrypt<'a>(
        &'a self,
        public_key: &'a PublicKey,
        content: &'a str,
    ) -> SignerFuture<'a, Result<String, MarmotSignerError>> {
        let id = self.id;
        let pk = public_key.to_hex();
        let content = content.to_string();
        wrapped(async move {
            let (obj, f) = js_method(id, "nip44Encrypt").map_err(MarmotSignerError::from)?;
            let promise = f
                .call2(&obj, &JsValue::from_str(&pk), &JsValue::from_str(&content))
                .map_err(|e| MarmotSignerError::from(format!("nip44Encrypt threw: {e:?}")))?;
            let out = JsFuture::from(Promise::from(promise))
                .await
                .map_err(|e| {
                    MarmotSignerError::from(format!("nip44Encrypt rejected: {e:?}"))
                })?;
            out.as_string()
                .ok_or_else(|| MarmotSignerError::from("nip44Encrypt returned non-string"))
        })
    }

    fn nip44_decrypt<'a>(
        &'a self,
        public_key: &'a PublicKey,
        payload: &'a str,
    ) -> SignerFuture<'a, Result<String, MarmotSignerError>> {
        let id = self.id;
        let pk = public_key.to_hex();
        let payload = payload.to_string();
        wrapped(async move {
            let (obj, f) = js_method(id, "nip44Decrypt").map_err(MarmotSignerError::from)?;
            let promise = f
                .call2(&obj, &JsValue::from_str(&pk), &JsValue::from_str(&payload))
                .map_err(|e| MarmotSignerError::from(format!("nip44Decrypt threw: {e:?}")))?;
            let out = JsFuture::from(Promise::from(promise))
                .await
                .map_err(|e| {
                    MarmotSignerError::from(format!("nip44Decrypt rejected: {e:?}"))
                })?;
            out.as_string()
                .ok_or_else(|| MarmotSignerError::from("nip44Decrypt returned non-string"))
        })
    }
}

// ---------------------------------------------------------------------------
// Wall clock: injected `nowMs` when given (deterministic), else Date.now().
// The engine default goes through web-time → Date.now() on wasm anyway; the
// injection keeps the seam for reproducible runs.
// ---------------------------------------------------------------------------

struct JsWallClock {
    fixed_ms: Option<u64>,
}

/// Maintenance jitter source — scheduling only, never cryptographic. A small
/// deterministic xorshift keeps runs reproducible when `nowMs` is injected.
struct JsMaintenanceRandom {
    state: Mutex<u64>,
}

impl cgka_traits::maintenance::MaintenanceRandom for JsMaintenanceRandom {
    fn next_u64(&self) -> u64 {
        let mut s = self.state.lock().unwrap();
        let mut x = *s;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        *s = x;
        x
    }
}

impl WallClock for JsWallClock {
    fn now(&self) -> cgka_traits::Timestamp {
        cgka_traits::Timestamp(self.now_ms() / 1_000)
    }

    fn now_ms(&self) -> u64 {
        self.fixed_ms.unwrap_or_else(|| js_sys::Date::now() as u64)
    }
}

// ---------------------------------------------------------------------------
// universal-time provider
//
// The erskingardner `nostr` fork calls `universal_time::{Instant,SystemTime}::
// now()`, which has NO default backend on wasm32-unknown-unknown — the symbol
// is an extern that only `define_time_provider!` in the final crate supplies.
// Without this the wasm link fails with "a time provider is required".
// Backed by web-time: Date.now() for wall time, performance.now() for ticks.
// ---------------------------------------------------------------------------

struct JsTimeProvider;

static MONO_START: std::sync::OnceLock<web_time::Instant> = std::sync::OnceLock::new();

impl universal_time::WallClock for JsTimeProvider {
    fn system_time(&self) -> universal_time::SystemTime {
        let d = web_time::SystemTime::now()
            .duration_since(web_time::UNIX_EPOCH)
            .unwrap_or_default();
        universal_time::SystemTime::from_unix_duration(d)
    }
}

impl universal_time::MonotonicClock for JsTimeProvider {
    fn instant(&self) -> universal_time::Instant {
        universal_time::Instant::from_ticks(
            MONO_START.get_or_init(web_time::Instant::now).elapsed(),
        )
    }
}

universal_time::define_time_provider!(JsTimeProvider);

// ---------------------------------------------------------------------------
// Client
// ---------------------------------------------------------------------------

fn err(e: impl fmt::Debug) -> JsError {
    JsError::new(&format!("{e:?}"))
}

// json_compatible: serde maps must become plain JS objects — the default
// serializer emits JS `Map` instances, which JSON.stringify erases to `{}`.
fn to_js<T: Serialize + ?Sized>(v: &T) -> Result<JsValue, JsError> {
    v.serialize(&serde_wasm_bindgen::Serializer::json_compatible())
        .map_err(err)
}

fn parse_gid(s: &str) -> Result<GroupId, JsError> {
    Ok(GroupId::new(hex::decode(s).map_err(err)?))
}

fn transport_events(
    msgs: impl Iterator<Item = TransportMessage>,
) -> Result<Vec<NostrTransportEvent>, JsError> {
    msgs.map(|m| NostrTransportEvent::from_transport_message(&m).map_err(err))
        .collect()
}

/// SendResult decomposed for JS: transport events to publish plus the pending
/// handle the host resolves with confirmPublished/publishFailed.
fn send_result_json(result: SendResult) -> Result<Value, JsError> {
    let mut obj = serde_json::Map::new();
    let mut events: Vec<NostrTransportEvent> = Vec::new();
    match result {
        SendResult::ApplicationMessage { msg, .. } => {
            obj.insert("type".into(), json!("applicationMessage"));
            events.extend(transport_events([msg].into_iter())?);
        }
        SendResult::Proposal { msg } => {
            obj.insert("type".into(), json!("proposal"));
            events.extend(transport_events([msg].into_iter())?);
        }
        SendResult::GroupEvolution {
            msg,
            welcomes,
            pending,
        } => {
            obj.insert("type".into(), json!("groupEvolution"));
            obj.insert(
                "pending".into(),
                serde_json::to_value(pending).map_err(err)?,
            );
            events.extend(transport_events([msg].into_iter().chain(welcomes))?);
        }
        SendResult::GroupCreated { welcomes, pending } => {
            obj.insert("type".into(), json!("groupCreated"));
            obj.insert(
                "pending".into(),
                serde_json::to_value(pending).map_err(err)?,
            );
            events.extend(transport_events(welcomes.into_iter())?);
        }
        SendResult::FoundingGroupCreated { welcomes } => {
            obj.insert("type".into(), json!("foundingGroupCreated"));
            events.extend(transport_events(welcomes.into_iter())?);
        }
        SendResult::Queued { .. } => {
            obj.insert("type".into(), json!("queued"));
        }
        SendResult::NoChange { .. } => {
            obj.insert("type".into(), json!("noChange"));
        }
        other => {
            obj.insert("type".into(), json!("other"));
            obj.insert("debug".into(), json!(format!("{other:?}")));
        }
    }
    obj.insert(
        "events".into(),
        serde_json::to_value(events).map_err(err)?,
    );
    Ok(Value::Object(obj))
}

fn ingest_outcome_json(outcome: &IngestOutcome) -> Value {
    let (kind, category) = match outcome {
        IngestOutcome::Processed => ("processed", None),
        IngestOutcome::Buffered { .. } => ("buffered", None),
        IngestOutcome::TransportDeferred { .. } => ("deferred", None),
        IngestOutcome::Ignored {
            category: InputRejectionCategory::OwnEcho,
        } => ("ignored", Some("ownEcho".to_string())),
        IngestOutcome::Ignored { category } => ("ignored", Some(format!("{category:?}"))),
        IngestOutcome::LocalState { .. } => ("localState", None),
        IngestOutcome::Stale { .. } => ("stale", None),
        IngestOutcome::Rejected { .. } => ("rejected", None),
        _ => ("other", None),
    };
    match category {
        Some(c) => json!({"outcome": kind, "category": c}),
        None => json!({"outcome": kind}),
    }
}

fn group_event_json(event: &GroupEvent) -> Value {
    match event {
        GroupEvent::MessageReceived {
            group_id,
            message_id,
            sender,
            epoch,
            payload,
            ..
        } => {
            let decoded = MarmotAppEvent::decode(payload).ok().map(|e| {
                json!({
                    "author": e.pubkey,
                    "createdAt": e.created_at,
                    "kind": e.kind,
                    "content": e.content,
                })
            });
            json!({
                "type": "messageReceived",
                "groupId": hex::encode(group_id.as_slice()),
                "messageId": hex::encode(message_id.as_slice()),
                "sender": hex::encode(sender.as_slice()),
                "epoch": epoch.0,
                "payload": B64.encode(payload),
                "decoded": decoded,
            })
        }
        other => {
            let name = format!("{other:?}");
            let variant = name.split(['(', ' ']).next().unwrap_or("unknown");
            json!({"type": "other", "variant": variant})
        }
    }
}

/// wasm-bindgen exported client: one Marmot account-device.
#[wasm_bindgen]
pub struct Client {
    engine: Engine<KvStorageProvider>,
    provider: KvStorageProvider,
    clock: Arc<ManualConvergenceClock>,
    identity_hex: String,
}

#[wasm_bindgen]
impl Client {
    /// `Client.open({ entries, onCommit, signer, nowMs? })`.
    ///
    /// - `entries`: `[[hexKey, hexValue], ...]` — previously persisted state
    ///   (empty array for a fresh device)
    /// - `onCommit(batch)`: host persistence hook, `[{op,key,value}]` hex
    /// - `signer`: the JS signer object documented in the module docs
    /// - `nowMs`: optional fixed wall clock (ms since epoch)
    #[wasm_bindgen(js_name = open)]
    pub fn open(options: JsValue) -> Result<Client, JsError> {
        console_error_panic_hook::set_once();

        let opts: Object = options.dyn_into().map_err(err)?;
        let entries_v = Reflect::get(&opts, &JsValue::from_str("entries")).map_err(err)?;
        let on_commit = Reflect::get(&opts, &JsValue::from_str("onCommit"))
            .map_err(err)?
            .dyn_into::<Object>()
            .map_err(err)?;
        let signer = Reflect::get(&opts, &JsValue::from_str("signer"))
            .map_err(err)?
            .dyn_into::<Object>()
            .map_err(err)?;
        let now_ms = Reflect::get(&opts, &JsValue::from_str("nowMs"))
            .ok()
            .and_then(|v| v.as_f64())
            .map(|v| v as u64);
        // bareCapabilities: publish only the engine-default component set —
        // used to prove which capabilities wn actually requires of members.
        let bare = Reflect::get(&opts, &JsValue::from_str("bareCapabilities"))
            .ok()
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        // omitComponents: drop these ids from the advertised set — used to
        // build a key package missing exactly one required component.
        let omit: Vec<u16> = Reflect::get(&opts, &JsValue::from_str("omitComponents"))
            .ok()
            .and_then(|v| serde_wasm_bindgen::from_value(v).ok())
            .unwrap_or_default();

        let entries_hex: Vec<(String, String)> =
            serde_wasm_bindgen::from_value(entries_v).map_err(err)?;
        let mut entries = Vec::with_capacity(entries_hex.len());
        for (k, v) in entries_hex {
            entries.push((hex::decode(k).map_err(err)?, hex::decode(v).map_err(err)?));
        }

        let signer_id = register_object(signer);
        let pubkey_hex = {
            let (obj, f) = js_method(signer_id, "publicKey").map_err(err)?;
            f.call0(&obj)
                .map_err(|e| err(format!("signer.publicKey threw: {e:?}")))?
                .as_string()
                .ok_or_else(|| err("signer.publicKey returned non-string"))?
        };
        let pubkey_bytes: [u8; 32] = hex::decode(&pubkey_hex)
            .map_err(err)?
            .try_into()
            .map_err(|_| err("signer.publicKey is not 32 bytes"))?;

        let host = JsKvHost {
            callback_id: register_object(on_commit),
            initial: Mutex::new(entries),
        };
        let provider = KvStorageProvider::open(Arc::new(host));
        let signer = Arc::new(JsAccountSigner {
            id: signer_id,
            pubkey_hex: pubkey_hex.clone(),
            pubkey_bytes,
        });
        let peeler = NostrMlsPeeler::new().with_welcome_signer_arc(signer.clone());
        let clock = Arc::new(ManualConvergenceClock::new(0, 0));
        let mut builder = EngineBuilder::new(provider.clone())
            .identity(pubkey_bytes.to_vec())
            .account_identity_proof_signer(signer)
            .protocol_profile(ProtocolProfile::Current);
        if !bare {
            let advertised: Vec<u16> = supported_components()
                .into_iter()
                .filter(|id| !omit.contains(id))
                .collect();
            builder = builder
                .supported_app_components(advertised)
                .feature_registry(supported_features());
        }
        let mut engine = builder
            .peeler(Box::new(peeler))
            .convergence_clock(clock.clone())
            .maintenance_sources(
                Arc::new(JsWallClock { fixed_ms: now_ms }),
                Arc::new(JsMaintenanceRandom {
                    state: Mutex::new(0x9e37_79b9_7f4a_7c15),
                }),
            )
            .build()
            .map_err(err)?;
        engine
            .hydrate_stable_groups_from_storage()
            .map_err(err)?;

        Ok(Client {
            engine,
            provider,
            clock,
            identity_hex: pubkey_hex,
        })
    }

    /// Build a fresh KeyPackage and return the unsigned kind-30443 event
    /// fields: `{ kind, created_at, content, tags, pubkey }`. JS finalizes
    /// (signs) it and publishes to the relay.
    #[wasm_bindgen(js_name = publishKeyPackage)]
    pub async fn publish_key_package(&mut self) -> Result<JsValue, JsError> {
        let kp = self.engine.fresh_key_package().await.map_err(err)?;
        let metadata =
            cgka_engine::key_package::key_package_metadata(&kp).map_err(err)?;
        let tags: Vec<Vec<String>> = vec![
            vec!["d".into(), "main".into()],
            vec!["mls_protocol_version".into(), "1.0".into()],
            vec!["i".into(), metadata.key_package_ref_hex.clone()],
            vec![
                "mls_ciphersuite".into(),
                format!("0x{:04x}", metadata.ciphersuite),
            ],
            [
                vec!["mls_extensions".into()],
                metadata
                    .mls_extensions
                    .iter()
                    .map(|id| format!("0x{id:04x}"))
                    .collect(),
            ]
            .concat(),
            [
                vec!["mls_proposals".into()],
                metadata
                    .mls_proposals
                    .iter()
                    .map(|id| format!("0x{id:04x}"))
                    .collect(),
            ]
            .concat(),
            [
                vec!["app_components".into()],
                metadata
                    .app_components
                    .iter()
                    .filter(|id| {
                        **id >= cgka_traits::app_components::PRIVATE_USE_APP_COMPONENT_ID_START
                    })
                    .map(|id| format!("0x{id:04x}"))
                    .collect(),
            ]
            .concat(),
            vec!["client".into(), "marmot-spike-wasm".into()],
        ];
        to_js(&json!({
            "kind": KIND_MARMOT_KEY_PACKAGE,
            "created_at": js_sys::Date::now() as u64 / 1_000,
            "content": B64.encode(kp.bytes()),
            "tags": tags,
            "pubkey": self.identity_hex,
        }))
    }

    /// `{ name, relays, keyPackageEvents }` → `{ groupId, ...sendResult }`.
    #[wasm_bindgen(js_name = createGroup)]
    pub async fn create_group(&mut self, options: JsValue) -> Result<JsValue, JsError> {
        #[derive(serde::Deserialize)]
        struct CreateOpts {
            name: String,
            #[serde(default)]
            relays: Vec<String>,
            #[serde(default, rename = "keyPackageEvents")]
            key_package_events: Vec<NostrTransportEvent>,
        }
        let opts: CreateOpts = serde_wasm_bindgen::from_value(options).map_err(err)?;
        let invitees: Vec<cgka_traits::engine::KeyPackage> = opts
            .key_package_events
            .iter()
            .map(key_package_from_event)
            .collect();
        let req = CreateGroupRequest {
            name: opts.name,
            description: String::new(),
            members: invitees,
            required_features: Vec::new(),
            app_components: vec![routing_component(&self.engine.self_id(), &opts.relays)],
            initial_admins: Vec::new(),
        };
        let (group_id, result) = self.engine.create_group(req).await.map_err(err)?;
        let mut out = send_result_json(result)?;
        out["groupId"] = json!(hex::encode(group_id.as_slice()));
        to_js(&out)
    }

    /// `{ groupId, intent }` → sendResult. Intents:
    /// `{type:"appMessage",content}` | `{type:"invite",keyPackageEvents}` |
    /// `{type:"removeMembers",memberIds}` | `{type:"selfUpdate"}`.
    #[wasm_bindgen(js_name = send)]
    pub async fn send(&mut self, options: JsValue) -> Result<JsValue, JsError> {
        #[derive(serde::Deserialize)]
        struct SendOpts {
            #[serde(rename = "groupId")]
            group_id: String,
            intent: Value,
        }
        let opts: SendOpts = serde_wasm_bindgen::from_value(options).map_err(err)?;
        let group_id = parse_gid(&opts.group_id)?;
        let intent = match opts.intent["type"].as_str().unwrap_or("") {
            "appMessage" => {
                let content = opts.intent["content"].as_str().unwrap_or_default();
                let created_at = opts.intent["createdAt"]
                    .as_u64()
                    .unwrap_or_else(|| js_sys::Date::now() as u64 / 1_000);
                SendIntent::AppMessage {
                    group_id,
                    payload: chat_payload(&self.identity_hex, created_at, content),
                    expected_epoch: None,
                }
            }
            "invite" => {
                let events: Vec<NostrTransportEvent> = serde_json::from_value(
                    opts.intent
                        .get("keyPackageEvents")
                        .cloned()
                        .unwrap_or(Value::Array(vec![])),
                )
                .map_err(err)?;
                SendIntent::Invite {
                    group_id,
                    key_packages: events.iter().map(key_package_from_event).collect(),
                    initial_admins: Vec::new(),
                }
            }
            "removeMembers" => {
                let ids: Vec<String> = serde_json::from_value(
                    opts.intent
                        .get("memberIds")
                        .cloned()
                        .unwrap_or(Value::Array(vec![])),
                )
                .map_err(err)?;
                SendIntent::RemoveMembers {
                    group_id,
                    members: ids
                        .into_iter()
                        .map(|h| hex::decode(h).map(cgka_traits::types::MemberId::new))
                        .collect::<Result<_, _>>()
                        .map_err(err)?,
                }
            }
            "selfUpdate" => SendIntent::SelfUpdate { group_id },
            other => return Err(err(format!("unknown intent type {other}"))),
        };
        let result = self.engine.send(intent).await.map_err(err)?;
        to_js(&send_result_json(result)?)
    }

    /// Feed one relay event (kind 445 group message or kind 1059 gift wrap)
    /// into the engine. Returns `{ outcome, category? }`.
    #[wasm_bindgen(js_name = ingest)]
    pub async fn ingest(&mut self, event: JsValue) -> Result<JsValue, JsError> {
        let ev: NostrTransportEvent = serde_wasm_bindgen::from_value(event).map_err(err)?;
        let msg = ev.to_transport_message().map_err(err)?;
        let outcome = self.engine.ingest(msg).await.map_err(err)?;
        to_js(&ingest_outcome_json(&outcome))
    }

    /// One convergence pass: advances the manual clock, drains pending
    /// groups, settles stored inputs, then advances convergence. Returns
    /// `{ settled, pendingGroups, results: [sendResult...] }`; JS publishes
    /// each result's events and resolves pendings, then calls again until
    /// `settled && pendingGroups == 0 && results empty` (port of
    /// harness::flush_outbound).
    #[wasm_bindgen(js_name = advanceConvergence)]
    pub async fn advance_convergence(&mut self, group_id: String) -> Result<JsValue, JsError> {
        let gid = parse_gid(&group_id)?;
        self.clock.advance_ms(60_000);
        let pending_groups = self.engine.drain_pending_convergence_groups().len();
        let settled = self
            .engine
            .advance_convergence_inputs_until_settled(&gid, 0)
            .await
            .map_err(err)?;
        let results = self.engine.advance_convergence(&gid).await.map_err(err)?;
        let results_json: Vec<Value> = results
            .into_iter()
            .map(send_result_json)
            .collect::<Result<_, _>>()?;
        to_js(&json!({
            "settled": settled,
            "pendingGroups": pending_groups,
            "results": results_json,
        }))
    }

    /// Mark a staged publish as delivered (pending handle from a sendResult).
    #[wasm_bindgen(js_name = confirmPublished)]
    pub async fn confirm_published(&mut self, pending: JsValue) -> Result<JsValue, JsError> {
        let p: PendingStateRef = serde_wasm_bindgen::from_value(pending).map_err(err)?;
        let event = self.engine.confirm_published(p).await.map_err(err)?;
        to_js(&group_event_json(&event))
    }

    /// Mark a staged publish as failed.
    #[wasm_bindgen(js_name = publishFailed)]
    pub async fn publish_failed(&mut self, pending: JsValue) -> Result<(), JsError> {
        let p: PendingStateRef = serde_wasm_bindgen::from_value(pending).map_err(err)?;
        self.engine.publish_failed(p).await.map_err(err)
    }

    /// Drain pending GroupEvents and acknowledge MessageReceived payloads —
    /// the durable at-least-once app-event contract: unacked events re-present
    /// after restart (see spike/STORAGE.md).
    #[wasm_bindgen(js_name = drainEvents)]
    pub fn drain_events(&mut self) -> Result<JsValue, JsError> {
        let events = self.engine.drain_events();
        let ack: Vec<MessageId> = events
            .iter()
            .filter_map(|e| match e {
                GroupEvent::MessageReceived { message_id, .. } => Some(message_id.clone()),
                _ => None,
            })
            .collect();
        if !ack.is_empty() {
            self.provider
                .delete_pending_application_events(&ack)
                .map_err(err)?;
        }
        to_js(&events.iter().map(group_event_json).collect::<Vec<_>>())
    }

    #[wasm_bindgen(js_name = members)]
    pub fn members(&self, group_id: String) -> Result<JsValue, JsError> {
        let members = self.engine.members(&parse_gid(&group_id)?).map_err(err)?;
        let ids: Vec<String> = members
            .iter()
            .map(|m| hex::encode(m.id.as_slice()))
            .collect();
        to_js(&ids)
    }

    #[wasm_bindgen(js_name = epoch)]
    pub fn epoch(&self, group_id: String) -> Result<f64, JsError> {
        let e = self.engine.epoch(&parse_gid(&group_id)?).map_err(err)?;
        Ok(e.0 as f64)
    }

    #[wasm_bindgen(js_name = memberId)]
    pub fn member_id(&self) -> String {
        hex::encode(self.engine.self_id().as_slice())
    }

    #[wasm_bindgen(js_name = wasmMemoryBytes)]
    pub fn wasm_memory_bytes(&self) -> f64 {
        wasm_memory_bytes()
    }
}

/// Linear memory size in bytes (pages × 64 KiB).
#[wasm_bindgen(js_name = wasmMemoryBytes)]
pub fn wasm_memory_bytes() -> f64 {
    (core::arch::wasm32::memory_size::<0>() * 65_536) as f64
}
