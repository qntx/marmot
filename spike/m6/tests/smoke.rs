//! Minimal MDK v0.18.0 cgka-engine round trip on in-memory SQLite storage.
//!
//! Two `Engine<SqliteAccountStorage>` instances share a pass-through
//! `MockPeeler` shape (lifted from cgka-engine's own Tier-2 tests) that
//! reflects `EncryptedPayload` bytes into a `TransportMessage` without any
//! transport crypto, so the engines exercise real OpenMLS create/commit,
//! Welcome, and application-message paths.

use async_trait::async_trait;
use cgka_engine::account_identity_proof::{
    AccountIdentityProofRequest, AccountIdentityProofSigner,
};
use cgka_engine::feature_registry::FeatureRegistry;
use cgka_engine::{Engine, EngineBuilder};
use cgka_traits::app_event::{MARMOT_APP_EVENT_KIND_CHAT, MarmotAppEvent};
use cgka_traits::capabilities::{Capability, CapabilityRequirement, Feature, RequirementLevel};
use cgka_traits::engine::{CgkaEngine, CreateGroupRequest, GroupEvent, SendIntent, SendResult};
use cgka_traits::error::PeelerError;
use cgka_traits::group_context::GroupContextSnapshot;
use cgka_traits::ingest::{IngestOutcome, PeeledContent, PeeledMessage};
use cgka_traits::peeler::{PreparedGroupMessage, TransportPeeler};
use cgka_traits::transport::{
    EncryptedPayload, Timestamp, TransportEnvelope, TransportMessage, TransportSource,
};
use cgka_traits::types::{MemberId, MessageId};
use k256::schnorr::{SigningKey, signature::hazmat::PrehashSigner};
use sha2::{Digest, Sha256};
use std::sync::Arc;
use storage_sqlite::SqliteAccountStorage;

fn signing_key(seed: &[u8]) -> SigningKey {
    let mut counter = 0u64;
    loop {
        let mut material = [0u8; 32];
        let mut hasher = Sha256::new();
        hasher.update(b"cgka-engine-test-identity-v1");
        hasher.update(seed);
        hasher.update(counter.to_be_bytes());
        material.copy_from_slice(&hasher.finalize());
        if let Ok(sk) = SigningKey::from_slice(&material) {
            return sk;
        }
        counter += 1;
    }
}

/// Deterministic, spec-valid x-only secp256k1 identity derived from a label.
fn valid_identity(seed: &[u8]) -> Vec<u8> {
    signing_key(seed).verifying_key().to_bytes().to_vec()
}

fn proof_signer(seed: &[u8]) -> Arc<dyn AccountIdentityProofSigner> {
    Arc::new(TestAccountIdentityProofSigner(signing_key(seed)))
}

struct TestAccountIdentityProofSigner(SigningKey);

impl AccountIdentityProofSigner for TestAccountIdentityProofSigner {
    fn sign_account_identity_proof(
        &self,
        request: &AccountIdentityProofRequest,
    ) -> Result<[u8; 64], String> {
        if self.0.verifying_key().to_bytes().as_slice() != request.account_identity.as_slice() {
            return Err("request account identity does not match test key".into());
        }
        let signature = self
            .0
            .sign_prehash(&request.proof_event_id()?)
            .map_err(|e| e.to_string())?;
        Ok(signature.to_bytes())
    }
}

/// Pass-through transport peeler: raw MLS bytes cross the boundary untouched.
struct MockPeeler;

fn hash_id(bytes: &[u8]) -> MessageId {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    let mut h = DefaultHasher::new();
    bytes.hash(&mut h);
    MessageId::new(h.finish().to_be_bytes().to_vec())
}

#[async_trait]
impl TransportPeeler for MockPeeler {
    async fn wrap_group_recovery(
        &self,
        _message: &cgka_traits::GroupRecoveryMessage,
        _recipient: &MemberId,
    ) -> Result<TransportMessage, PeelerError> {
        Err(PeelerError::Malformed(
            "pass-through fixture has no authenticated account channel".into(),
        ))
    }

    async fn peel_group_message(
        &self,
        msg: &PreparedGroupMessage<'_>,
        _ctx: &GroupContextSnapshot,
    ) -> Result<PeeledMessage, PeelerError> {
        Ok(PeeledMessage {
            id: msg.message.id.clone(),
            group_id: None,
            sender: None,
            content: PeeledContent::MlsMessage {
                bytes: msg.message.payload.clone(),
            },
            origin: msg.message.clone(),
        })
    }

    async fn peel_welcome(&self, msg: &TransportMessage) -> Result<PeeledMessage, PeelerError> {
        Ok(PeeledMessage {
            id: msg.id.clone(),
            group_id: None,
            sender: None,
            content: PeeledContent::Welcome {
                created_at: None,
                bytes: msg.payload.clone(),
                key_package_event_id: None,
            },
            origin: msg.clone(),
        })
    }

    async fn wrap_group_message(
        &self,
        payload: &EncryptedPayload,
        ctx: &GroupContextSnapshot,
    ) -> Result<TransportMessage, PeelerError> {
        Ok(TransportMessage {
            id: hash_id(&payload.ciphertext),
            payload: payload.ciphertext.clone(),
            timestamp: Timestamp(0),
            causal_deps: vec![],
            source: TransportSource("mock".into()),
            envelope: TransportEnvelope::GroupMessage {
                transport_group_id: ctx.transport_group_id().unwrap_or_default().to_vec(),
            },
        })
    }

    async fn wrap_welcome(
        &self,
        payload: &EncryptedPayload,
        recipient: &MemberId,
    ) -> Result<TransportMessage, PeelerError> {
        let mut id_material = payload.ciphertext.clone();
        id_material.extend_from_slice(recipient.as_slice());
        Ok(TransportMessage {
            id: hash_id(&id_material),
            payload: payload.ciphertext.clone(),
            timestamp: Timestamp(0),
            causal_deps: vec![],
            source: TransportSource("mock".into()),
            envelope: TransportEnvelope::Welcome {
                recipient: recipient.clone(),
            },
        })
    }
}

fn selfremove_registry() -> FeatureRegistry {
    let mut r = FeatureRegistry::new();
    r.register(
        Feature("self-remove"),
        CapabilityRequirement {
            requires: Capability::Proposal(10), // MIP-03 SelfRemove
            level: RequirementLevel::Required,
            description: "MIP-03",
        },
    );
    r
}

fn build_client(identity: &[u8]) -> Engine<SqliteAccountStorage> {
    EngineBuilder::new(SqliteAccountStorage::in_memory_for_test().expect("in-memory storage"))
        .identity(valid_identity(identity))
        .account_identity_proof_signer(proof_signer(identity))
        .feature_registry(selfremove_registry())
        .peeler(Box::new(MockPeeler))
        .build()
        .expect("build engine")
}

fn app_payload_for(engine: &Engine<SqliteAccountStorage>, content: &str) -> Vec<u8> {
    MarmotAppEvent::new(
        hex::encode(engine.self_id().as_slice()),
        1_700_000_000,
        MARMOT_APP_EVENT_KIND_CHAT,
        vec![],
        content,
    )
    .encode()
    .expect("app event encodes")
}

fn app_content(payload: &[u8]) -> Vec<u8> {
    MarmotAppEvent::decode(payload)
        .expect("app event decodes")
        .content
        .into_bytes()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn alice_creates_group_bob_joins_and_receives_message() {
    let mut alice = build_client(b"alice");
    let mut bob = build_client(b"bob");

    // Bob mints a KeyPackage; Alice founds a group that adds it.
    let bob_kp = bob.fresh_key_package().await.expect("bob key package");
    let (group_id, created) = alice
        .create_group(CreateGroupRequest {
            name: "spike".into(),
            description: "m6 smoke".into(),
            members: vec![bob_kp],
            required_features: vec![Feature("self-remove")],
            app_components: vec![],
            initial_admins: vec![],
        })
        .await
        .expect("create group");

    // Current profile: the group is canonical locally already; the result
    // carries only the independently deliverable Welcomes (one per invitee).
    // Publish-before-apply (`SendResult::GroupCreated { pending }`) exists
    // only on the explicit legacy compatibility profile.
    let welcome = match created {
        SendResult::FoundingGroupCreated { mut welcomes } => {
            assert_eq!(welcomes.len(), 1, "one welcome per invitee");
            welcomes.remove(0)
        }
        other => panic!("expected FoundingGroupCreated, got {other:?}"),
    };

    // Bob joins by ingesting the Welcome transport message.
    let joined = bob.ingest(welcome).await.expect("bob ingests welcome");
    assert!(matches!(joined, IngestOutcome::Processed));
    assert_eq!(bob.members(&group_id).expect("bob members").len(), 2);

    // Alice sends one application message; Bob ingests the transport copy.
    let sent = alice
        .send(SendIntent::AppMessage {
            group_id: group_id.clone(),
            payload: app_payload_for(&alice, "hello bob"),
            expected_epoch: None,
        })
        .await
        .expect("alice sends");
    let msg = match sent {
        SendResult::ApplicationMessage { msg, .. } => TransportMessage {
            envelope: TransportEnvelope::GroupMessage {
                transport_group_id: group_id.as_slice().to_vec(),
            },
            ..msg
        },
        other => panic!("expected ApplicationMessage, got {other:?}"),
    };

    let outcome = bob.ingest(msg).await.expect("bob ingests message");
    assert!(matches!(outcome, IngestOutcome::Processed));

    let events = bob.drain_events();
    assert!(
        events.iter().any(|event| matches!(
            event,
            GroupEvent::MessageReceived { payload, .. } if app_content(payload) == b"hello bob"
        )),
        "expected bob to receive the chat payload, got {events:?}"
    );
}
