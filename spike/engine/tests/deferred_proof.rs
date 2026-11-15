//! Deferred account-identity-proof signing: a two-phase engine open serves an
//! async/remote account signer without an MDK change.
//!
//! Phase 1 opens the engine with a signer that records the
//! `AccountIdentityProofRequest` and fails. `Identity::load_or_generate`
//! persists the `AccountDeviceSignerBinding` (MLS key bound, no proof) before
//! signing, so the request's `proof_event` can be signed out of band — the
//! shape a NIP-07/NIP-46 round trip would take — and the finished 104-byte
//! current-profile component written back with `put_account_device_signer`.
//! Phase 2 reopens on the same KV: the retained proof validates and the signer
//! is never called again — not on open, KeyPackage generation, self-update, or
//! restart.
//!
//! Run: `cargo test --release --test deferred_proof -- --nocapture`

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use cgka_engine::account_identity_proof::{
    AccountIdentityProofRequest, AccountIdentityProofSigner, account_identity_proof_component,
};
use cgka_engine::convergence_clock::ManualConvergenceClock;
use cgka_engine::{Engine, EngineBuilder};
use cgka_traits::app_event::MarmotAppEvent;
use cgka_traits::engine::{CgkaEngine, KeyPackage, SendIntent, SendResult};
use cgka_traits::error::EngineError;
use cgka_traits::group::ProtocolProfile;
use cgka_traits::ingest::IngestOutcome;
use cgka_traits::storage::AccountDeviceSignerStorage;
use cgka_traits::types::{GroupId, MemberId};
use marmot_spike::harness::*;
use marmot_spike::{InMemoryHost, KvStorageProvider};
use nostr::prelude::*;
use openmls::prelude::SignatureScheme;
use openmls_traits::types::Ciphersuite;
use transport_nostr_peeler::{KIND_MARMOT_GROUP_MESSAGE, NostrMlsPeeler};

const SUITE: Ciphersuite = Ciphersuite::MLS_128_DHKEMX25519_AES128GCM_SHA256_Ed25519;

/// Counting signer that records every request and always fails — stands in
/// for "the account key is not reachable synchronously". Any call after the
/// proof is deferred is itself the failure this test counts.
#[derive(Default)]
struct DeferredSigner {
    calls: AtomicUsize,
    requests: Mutex<Vec<AccountIdentityProofRequest>>,
}

impl DeferredSigner {
    fn calls(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }
}

impl AccountIdentityProofSigner for DeferredSigner {
    fn sign_account_identity_proof(
        &self,
        request: &AccountIdentityProofRequest,
    ) -> Result<[u8; 64], String> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.requests.lock().unwrap().push(request.clone());
        Err("deferred".into())
    }
}

/// Returns exactly the signature captured out of band.
struct FixedSigner([u8; 64]);

impl AccountIdentityProofSigner for FixedSigner {
    fn sign_account_identity_proof(
        &self,
        _request: &AccountIdentityProofRequest,
    ) -> Result<[u8; 64], String> {
        Ok(self.0)
    }
}

/// `Member::open` with a caller-chosen `AccountIdentityProofSigner`; the
/// peeler's async welcome signer stays the host account key, which a remote
/// signer can serve because `MarmotNostrSigner` is already async.
fn build_engine(
    provider: &KvStorageProvider,
    identity_bytes: Vec<u8>,
    proof_signer: Arc<dyn AccountIdentityProofSigner>,
    welcome_signer: Arc<HostAccountSigner>,
    clock: Arc<ManualConvergenceClock>,
) -> Result<Engine<KvStorageProvider>, EngineError> {
    let peeler = NostrMlsPeeler::new().with_welcome_signer_arc(welcome_signer);
    EngineBuilder::new(provider.clone())
        .identity(identity_bytes)
        .account_identity_proof_signer(proof_signer)
        .protocol_profile(ProtocolProfile::Current)
        .supported_app_components(supported_components())
        .feature_registry(supported_features())
        .peeler(Box::new(peeler))
        .convergence_clock(clock)
        .build()
}

fn decode_chat(payload: &[u8]) -> String {
    MarmotAppEvent::decode(payload)
        .map(|e| e.content)
        .unwrap_or_else(|_| format!("<raw:{}>", payload.len()))
}

async fn send_chat(member: &mut Member, relay: &FakeRelay, gid: &GroupId, text: &str) {
    let payload = chat_payload(&member.signer.keys().public_key().to_hex(), 0, text);
    let result = member
        .engine
        .send(SendIntent::AppMessage {
            group_id: gid.clone(),
            payload,
            expected_epoch: None,
        })
        .await
        .expect("send");
    assert!(matches!(result, SendResult::ApplicationMessage { .. }));
    publish_send_result(member, result, relay).await;
}

fn leaf_signature_key_of(kp_event: &transport_nostr_peeler::NostrTransportEvent) -> Vec<u8> {
    let kp = KeyPackage {
        bytes: BASE64_STANDARD
            .decode(kp_event.content.as_bytes())
            .expect("kp base64"),
        source: None,
        protocol_profile: ProtocolProfile::Current,
    };
    cgka_engine::key_package::key_package_metadata(&kp)
        .expect("kp metadata")
        .leaf_signature_key
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn deferred_proof_two_phase_open() {
    let relay = FakeRelay::new();
    let row = |step: &str, detail: String| {
        println!("| {step:<58} | {detail} |");
    };

    let host_signer = HostAccountSigner::new(account_keys("alice"));
    let identity_bytes = host_signer.public_key_bytes().to_vec();
    let member_id = MemberId::new(identity_bytes.clone());
    let host = Arc::new(InMemoryHost::new());
    let provider = KvStorageProvider::open(host.clone());
    let clock = Arc::new(ManualConvergenceClock::new(0, 0));
    let deferred = Arc::new(DeferredSigner::default());

    // Phase 1: build fails, the key binding lands in storage unsigned.
    let err = match build_engine(
        &provider,
        identity_bytes.clone(),
        deferred.clone(),
        host_signer.clone(),
        clock.clone(),
    ) {
        Ok(_) => panic!("phase-1 build must fail while the proof signer defers"),
        Err(err) => err,
    };
    let binding = provider
        .account_device_signer(&member_id)
        .expect("binding read")
        .expect("phase 1 must persist the key binding");
    assert!(
        binding.account_identity_proof.is_none(),
        "phase-1 binding carries no proof yet"
    );
    assert!(!binding.mls_signature_public_key.is_empty());
    let request = {
        let requests = deferred.requests.lock().unwrap();
        assert_eq!(requests.len(), 1, "exactly one proof request");
        requests[0].clone()
    };
    assert_eq!(request.account_identity, identity_bytes);
    assert_eq!(
        request.mls_signature_public_key, binding.mls_signature_public_key,
        "the request binds the persisted MLS key"
    );
    assert_eq!(request.ciphersuite, u16::from(SUITE));
    assert_eq!(request.protocol_profile, ProtocolProfile::Current);
    row(
        "1. build() failed; binding persisted without proof",
        format!("signer_calls={} err={err}", deferred.calls()),
    );

    // Out of band: the account key (held here; remote in production) signs
    // the same event shape a retry would build, at a fresh timestamp.
    let created_at = now_secs();
    let oob_request = AccountIdentityProofRequest::current(
        identity_bytes.clone(),
        binding.mls_signature_public_key.clone(),
        SUITE,
        SignatureScheme::ED25519,
        created_at,
    );
    let signed = oob_request
        .proof_event()
        .expect("proof event")
        .finalize(host_signer.keys())
        .expect("account key signs the proof event");
    let signature = oob_request
        .signature_from_signed_event(signed)
        .expect("signature extracts");
    let proof = account_identity_proof_component(
        &identity_bytes,
        &binding.mls_signature_public_key,
        SUITE,
        SignatureScheme::ED25519,
        created_at,
        &FixedSigner(signature),
    )
    .expect("proof component assembles");
    assert_eq!(proof.len(), 104, "current proof component is 104 bytes");
    let mut bound = binding.clone();
    bound.account_identity_proof = Some(proof);
    provider
        .put_account_device_signer(&bound)
        .expect("proof writes back");

    // Phase 2: the same signer would still fail if asked — it is not asked.
    let calls_after_phase1 = deferred.calls();
    let mut engine = build_engine(
        &provider,
        identity_bytes.clone(),
        deferred.clone(),
        host_signer.clone(),
        clock.clone(),
    )
    .expect("phase-2 build succeeds on the retained proof");
    engine
        .hydrate_stable_groups_from_storage()
        .expect("hydrate");
    assert_eq!(engine.self_id(), member_id);
    assert_eq!(
        deferred.calls(),
        calls_after_phase1,
        "retained proof reused; signer untouched"
    );
    row(
        "2+3. build() succeeded on the injected proof",
        format!("signer_calls={}", deferred.calls()),
    );

    let mut a = Member {
        name: "A".to_string(),
        signer: host_signer.clone(),
        host,
        provider,
        clock,
        engine,
    };
    let mut b = Member::fresh("B", "bob");

    // Step 4: a full two-member flow proves peers accept the proof — and
    // none of it asks the account key again.
    let a_kp_event = publish_key_package(&mut a, &relay).await;
    assert_eq!(
        leaf_signature_key_of(&a_kp_event),
        binding.mls_signature_public_key,
        "the KeyPackage leaf signs with the phase-1 MLS key"
    );

    let gid = create_group(
        &mut b,
        &relay,
        "deferred-proof",
        vec![key_package_from_event(&a_kp_event)],
    )
    .await;
    let welcomes = ingest_welcomes(&mut a, &relay).await;
    assert!(
        welcomes
            .iter()
            .any(|o| matches!(o, IngestOutcome::Processed)),
        "A joins via Welcome: {welcomes:?}"
    );
    assert_eq!(b.engine.members(&gid).unwrap().len(), 2);
    assert_eq!(a.engine.members(&gid).unwrap().len(), 2);

    // One message each way.
    send_chat(&mut a, &relay, &gid, "a-0001").await;
    send_chat(&mut b, &relay, &gid, "b-0002").await;
    for event in relay.fetch_ascending(KIND_MARMOT_GROUP_MESSAGE, None, None) {
        ingest_event(&mut a, &event).await;
        ingest_event(&mut b, &event).await;
    }
    let a_got: Vec<String> = a
        .drain_received()
        .into_iter()
        .map(|(_, _, p)| decode_chat(&p))
        .collect();
    let b_got: Vec<String> = b
        .drain_received()
        .into_iter()
        .map(|(_, _, p)| decode_chat(&p))
        .collect();
    assert!(
        b_got.iter().any(|m| m == "a-0001"),
        "B reads A's message: {b_got:?}"
    );
    assert!(
        a_got.iter().any(|m| m == "b-0002"),
        "A reads B's message: {a_got:?}"
    );

    // Self-update builds a new own LeafNode — it reuses the stored proof.
    let update = a
        .engine
        .send(SendIntent::SelfUpdate {
            group_id: gid.clone(),
        })
        .await
        .expect("self-update");
    publish_send_result(&mut a, update, &relay).await;
    for event in relay.fetch_ascending(KIND_MARMOT_GROUP_MESSAGE, None, None) {
        ingest_event(&mut b, &event).await;
    }
    flush_outbound(&mut b, &gid, &relay).await;
    b.drain_received();
    assert_eq!(
        a.engine.epoch(&gid).unwrap(),
        b.engine.epoch(&gid).unwrap(),
        "B applied A's self-update commit — its leaf proof validated"
    );

    // A fresh KeyPackage still signs with the phase-1 key, proof intact.
    let a_kp2 = publish_key_package(&mut a, &relay).await;
    assert_eq!(
        leaf_signature_key_of(&a_kp2),
        binding.mls_signature_public_key
    );
    assert_eq!(
        deferred.calls(),
        calls_after_phase1,
        "KP + join + messages + self-update + KP: no signer calls"
    );
    row(
        "4. KP, welcome join, messages, self-update, KP",
        format!("signer_calls={}", deferred.calls()),
    );

    // Step 5: reopen from the persisted KV — the retained proof survives.
    // Groups hydrate lazily on first use, so drive the backlog before
    // reading the epoch, then prove the group is usable with one send.
    let dump = a.host.dump();
    let calls_before_restart = deferred.calls();
    drop(a);
    let host2 = Arc::new(InMemoryHost::load(&dump));
    let provider2 = KvStorageProvider::open(host2.clone());
    let clock2 = Arc::new(ManualConvergenceClock::new(0, 0));
    let mut engine2 = build_engine(
        &provider2,
        identity_bytes,
        deferred.clone(),
        host_signer.clone(),
        clock2.clone(),
    )
    .expect("restart builds");
    engine2
        .hydrate_stable_groups_from_storage()
        .expect("hydrate after restart");
    assert_eq!(engine2.self_id(), member_id);
    let mut a2 = Member {
        name: "A2".to_string(),
        signer: host_signer,
        host: host2,
        provider: provider2,
        clock: clock2,
        engine: engine2,
    };
    for event in relay.fetch_ascending(KIND_MARMOT_GROUP_MESSAGE, None, None) {
        ingest_event(&mut a2, &event).await;
    }
    flush_outbound(&mut a2, &gid, &relay).await;
    a2.drain_received();
    assert_eq!(
        a2.engine.epoch(&gid).expect("group survives restart"),
        b.engine.epoch(&gid).unwrap()
    );
    send_chat(&mut a2, &relay, &gid, "a-after-restart").await;
    for event in relay.fetch_ascending(KIND_MARMOT_GROUP_MESSAGE, None, None) {
        ingest_event(&mut b, &event).await;
    }
    let b_got_restart: Vec<String> = b
        .drain_received()
        .into_iter()
        .map(|(_, _, p)| decode_chat(&p))
        .collect();
    assert!(
        b_got_restart.iter().any(|m| m == "a-after-restart"),
        "B reads A's post-restart message: {b_got_restart:?}"
    );
    assert_eq!(
        deferred.calls(),
        calls_before_restart,
        "restart needs no signer call"
    );
    row(
        "5. restart reopened; post-restart send read by B",
        format!("signer_calls={}", deferred.calls()),
    );

    println!(
        "total sign_account_identity_proof calls: {}",
        deferred.calls()
    );
}
