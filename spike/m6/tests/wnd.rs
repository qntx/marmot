//! Part B: real `wn`/`wnd` v0.18.0 interop against an in-process LocalRelay.
//! Every test is `#[ignore]`d; run with
//! `WN_BIN=<wn> WND_BIN=<wnd> cargo test --release --test wnd -- --ignored --nocapture --test-threads=1`.
//! Raw `wn --json` output is recorded under `spike/m6/evidence/wnd/<id>.jsonl`.

mod support;

use cgka_engine::account_identity_proof::{
    AccountIdentityProofRequest, AccountIdentityProofSigner,
};
use cgka_engine::{Engine, EngineBuilder};
use cgka_traits::engine::{CgkaEngine, SendIntent, SendResult};
use cgka_traits::transport::TransportMessage;
use cgka_traits::transport_adapter::TransportEndpoint;
use cgka_traits::types::MemberId;
use nostr::prelude::*;
use nostr::types::time::Timestamp as NostrTimestamp;
use serde_json::{Value, json};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use storage_sqlite::SqliteAccountStorage;
use support::wnd::*;
use transport_nostr_peeler::NostrTransportEvent;

fn result<'a>(v: &'a Value, path: &str) -> Option<&'a Value> {
    v.pointer(&format!("/result{path}"))
}

fn group_hex(v: &Value) -> String {
    result(v, "/group_id")
        .or_else(|| result(v, "/group/id"))
        .or_else(|| result(v, "/id"))
        .and_then(Value::as_str)
        .expect("group id in json")
        .to_owned()
}

/// `wn groups create` with retries: the inviter resolves the invitee's
/// kind-30443 KeyPackage events from the relay, which can lag `keys publish`
/// by a few seconds (`member_outcomes[].status = "missing_key_package"`).
async fn create_group_invite(
    scenario: &str,
    inviter: &WnDevice,
    inviter_npub: &str,
    name: &str,
    invitee_npub: &str,
) -> Value {
    let mut last = Value::Null;
    for _ in 0..8 {
        let created = wn(
            scenario,
            &inviter.home,
            &inviter.socket,
            Some(inviter_npub),
            &["groups", "create", name, invitee_npub],
        );
        let missing = result(&created, "/group_id").is_none_or(Value::is_null)
            || created.to_string().contains("missing_key_package");
        last = created;
        if !missing {
            return last;
        }
        settle().await;
    }
    last
}

/// List `groups members` leaf/device counts per account on `dev`.
fn members_device_counts(scenario: &str, dev: &WnDevice, account: &str, gid: &str) -> Vec<i64> {
    let members = wn(
        scenario,
        &dev.home,
        &dev.socket,
        Some(account),
        &["groups", "members", gid],
    );
    result(&members, "/members")
        .and_then(Value::as_array)
        .map(|rows| {
            rows.iter()
                .map(|row| {
                    row.get("device_count")
                        .and_then(Value::as_i64)
                        .unwrap_or(-1)
                })
                .collect()
        })
        .unwrap_or_default()
}

/// W1: B invites account A after BOTH wn devices published KeyPackages.
#[ignore = "needs WN_BIN/WND_BIN"]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn w1_two_wn_devices_of_one_account() {
    const S: &str = "w1";
    let relay = start_relay().await;
    let a1 = spawn_device("a1", &relay.url).await;
    let a2 = spawn_device("a2", &relay.url).await;
    let b = spawn_device("b", &relay.url).await;

    // One account key signs in on both wn homes (each home is a device).
    let a_keys = Keys::generate();
    let a_nsec = a_keys.secret_key().to_bech32().unwrap();
    let a_npub = a_keys.public_key().to_bech32().unwrap();
    wn_login_nsec(S, &a1.home, &a1.socket, &a_nsec);
    wn_login_nsec(S, &a2.home, &a2.socket, &a_nsec);
    let b_npub = create_identity(S, &b);
    record(S, "accounts", json!({"a_npub": a_npub, "b_npub": b_npub}));

    // Both A devices publish KeyPackages before B invites.
    wn(S, &a1.home, &a1.socket, Some(&a_npub), &["keys", "publish"]);
    wn(S, &a2.home, &a2.socket, Some(&a_npub), &["keys", "publish"]);
    settle().await;

    let created = create_group_invite(S, &b, &b_npub, "w1", &a_npub).await;
    let gid = group_hex(&created);
    record(
        S,
        "b-member-outcomes",
        result(&created, "/member_outcomes")
            .cloned()
            .unwrap_or(Value::Null),
    );
    settle().await;

    // Did both A devices receive invites?
    for (name, dev) in [("a1", &a1), ("a2", &a2)] {
        let invites = wn(
            S,
            &dev.home,
            &dev.socket,
            Some(&a_npub),
            &["groups", "invites"],
        );
        record(
            S,
            &format!("{name}-invites"),
            result(&invites, "").cloned().unwrap_or(Value::Null),
        );
        let groups = wn(
            S,
            &dev.home,
            &dev.socket,
            Some(&a_npub),
            &["groups", "list"],
        );
        record(
            S,
            &format!("{name}-groups"),
            result(&groups, "").cloned().unwrap_or(Value::Null),
        );
        let (ok, accepted) = wn_try(
            S,
            &dev.home,
            &dev.socket,
            Some(&a_npub),
            &["groups", "accept", &gid],
        );
        println!("    {name} accept ok={ok}: {accepted}");
    }
    settle().await;
    settle().await;

    let counts = members_device_counts(S, &b, &b_npub, &gid);
    record(S, "b-members-device-counts", json!(counts));
    println!("W1 device counts on B: {counts:?}");

    // Message round trip: B -> both A devices, A2 -> B.
    wn(
        S,
        &b.home,
        &b.socket,
        Some(&b_npub),
        &["messages", "send", &gid, "hi from B"],
    );
    settle().await;
    for (name, dev) in [("a1", &a1), ("a2", &a2)] {
        let msgs = wn(
            S,
            &dev.home,
            &dev.socket,
            Some(&a_npub),
            &["messages", "list", &gid, "--limit", "5"],
        );
        record(
            S,
            &format!("{name}-messages"),
            result(&msgs, "").cloned().unwrap_or(Value::Null),
        );
    }
    wn(
        S,
        &a2.home,
        &a2.socket,
        Some(&a_npub),
        &["messages", "send", &gid, "hi from A2"],
    );
    settle().await;
    let msgs = wn(
        S,
        &b.home,
        &b.socket,
        Some(&b_npub),
        &["messages", "list", &gid, "--limit", "5"],
    );
    record(
        S,
        "b-messages",
        result(&msgs, "").cloned().unwrap_or(Value::Null),
    );
}

/// W2: a device linked later joins an existing chat through the device group.
#[ignore = "needs WN_BIN/WND_BIN"]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn w2_device_link_into_existing_chat() {
    const S: &str = "w2";
    let relay = start_relay().await;
    let a1 = spawn_device("a1", &relay.url).await;
    let a2 = spawn_device("a2", &relay.url).await;
    let b = spawn_device("b", &relay.url).await;

    let a_keys = Keys::generate();
    let a_nsec = a_keys.secret_key().to_bech32().unwrap();
    let a_npub = a_keys.public_key().to_bech32().unwrap();
    wn_login_nsec(S, &a1.home, &a1.socket, &a_nsec);
    let b_npub = create_identity(S, &b);

    // Existing chat: B + A1.
    let created = create_group_invite(S, &b, &b_npub, "w2", &a_npub).await;
    let gid = group_hex(&created);
    settle().await;
    wn_try(
        S,
        &a1.home,
        &a1.socket,
        Some(&a_npub),
        &["groups", "accept", &gid],
    );
    settle().await;
    wn(
        S,
        &b.home,
        &b.socket,
        Some(&b_npub),
        &["messages", "send", &gid, "pre-link"],
    );

    // A2 signs in to the same account on its own home, then links.
    wn_login_nsec(S, &a2.home, &a2.socket, &a_nsec);
    let (ok, started) = wn_try(
        S,
        &a2.home,
        &a2.socket,
        Some(&a_npub),
        &["devices", "link", "start"],
    );
    println!("    link start ok={ok}: {started}");
    if !ok {
        record(S, "link-start-refused", started);
        return; // pairing unsupported in this build — the refusal is the finding
    }
    let mut confirmed = false;
    let mut approved = false;
    for _ in 0..20 {
        let status = wn(
            S,
            &a2.home,
            &a2.socket,
            Some(&a_npub),
            &["devices", "link", "status"],
        );
        let text = status.to_string();
        let new_device_done = result(&status, "/new_device")
            .map(Value::is_null)
            .unwrap_or(true);
        record(S, "a2-link-status", status);
        let other = wn(
            S,
            &a1.home,
            &a1.socket,
            Some(&a_npub),
            &["devices", "link", "status"],
        );
        record(S, "a1-link-status", other.clone());
        // The new device confirms the emoji code once it reaches `comparing`.
        if !confirmed && text.contains("\"comparing\"") {
            let (ok, refused) = wn_try(
                S,
                &a2.home,
                &a2.socket,
                Some(&a_npub),
                &["devices", "link", "confirm-match"],
            );
            println!("    a2 confirm-match ok={ok}: {refused}");
            confirmed = ok;
        }
        // Approve on the existing device once a prompt id exists.
        if !approved {
            if let Some(prompt) = result(&other, "/prompts/0/prompt_id")
                .and_then(Value::as_str)
                .map(str::to_owned)
            {
                let (ok, approved_out) = wn_try(
                    S,
                    &a1.home,
                    &a1.socket,
                    Some(&a_npub),
                    &["devices", "link", "approve", &prompt, "--label", "Spike"],
                );
                println!("    a1 approve {prompt} ok={ok}: {approved_out}");
                approved = ok;
            }
        }
        if confirmed && approved && (text.contains("\"linked\"") || new_device_done) {
            break;
        }
        settle().await;
    }
    println!("    link handshake: confirmed={confirmed} approved={approved}");
    for dev in [&a1, &a2] {
        let list = wn(
            S,
            &dev.home,
            &dev.socket,
            Some(&a_npub),
            &["devices", "list"],
        );
        record(S, "devices-list", list);
    }

    // Observe the sibling Add in the existing chat.
    for _ in 0..10 {
        let counts = members_device_counts(S, &b, &b_npub, &gid);
        println!("    B device counts: {counts:?}");
        if counts.iter().any(|c| *c >= 2) {
            break;
        }
        settle().await;
    }
    record(
        S,
        "b-members-final",
        json!(members_device_counts(S, &b, &b_npub, &gid)),
    );
    // A1 commits the same-account Add; the sibling Welcome still has to reach
    // A2 and be applied before the chat shows up locally — poll for it.
    let mut a2_joined = false;
    for _ in 0..15 {
        let groups_a2 = wn(
            S,
            &a2.home,
            &a2.socket,
            Some(&a_npub),
            &["groups", "list"],
        );
        record(S, "a2-groups", groups_a2.clone());
        if groups_a2.to_string().contains(&gid) {
            a2_joined = true;
            break;
        }
        settle().await;
    }
    println!("    A2 sees chat: {a2_joined}");

    // Round trip through the linked device.
    let (ok, _) = wn_try(
        S,
        &a2.home,
        &a2.socket,
        Some(&a_npub),
        &["messages", "send", &gid, "from A2 linked"],
    );
    println!("    A2 send ok={ok}");
    settle().await;
    let msgs = wn(
        S,
        &b.home,
        &b.socket,
        Some(&b_npub),
        &["messages", "list", &gid, "--limit", "10"],
    );
    record(S, "b-messages", msgs);
}

/// W3: signing a device out removes its leaves from shared chats.
#[ignore = "needs WN_BIN/WND_BIN"]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn w3_sign_out_removes_device_leaf() {
    const S: &str = "w3";
    let relay = start_relay().await;
    let a1 = spawn_device("a1", &relay.url).await;
    let a2 = spawn_device("a2", &relay.url).await;
    let b = spawn_device("b", &relay.url).await;

    let a_keys = Keys::generate();
    let a_nsec = a_keys.secret_key().to_bech32().unwrap();
    let a_npub = a_keys.public_key().to_bech32().unwrap();
    wn_login_nsec(S, &a1.home, &a1.socket, &a_nsec);
    wn_login_nsec(S, &a2.home, &a2.socket, &a_nsec);
    let b_npub = create_identity(S, &b);
    wn(S, &a1.home, &a1.socket, Some(&a_npub), &["keys", "publish"]);
    wn(S, &a2.home, &a2.socket, Some(&a_npub), &["keys", "publish"]);
    settle().await;

    let created = create_group_invite(S, &b, &b_npub, "w3", &a_npub).await;
    let gid = group_hex(&created);
    settle().await;
    for dev in [&a1, &a2] {
        wn_try(
            S,
            &dev.home,
            &dev.socket,
            Some(&a_npub),
            &["groups", "accept", &gid],
        );
    }
    settle().await;
    record(
        S,
        "counts-before-logout",
        json!(members_device_counts(S, &b, &b_npub, &gid)),
    );

    // A2 signs out. `logout` is refused through the wnd socket (it removes a
    // local account); wn tells us to run it directly against WN_HOME.
    let (ok, out) = wn_try(S, &a2.home, &a2.socket, Some(&a_npub), &["logout", &a_npub]);
    println!("    A2 logout via socket ok={ok}: {out}");
    let logged_out = if ok {
        true
    } else {
        let (ok2, out2) = wn_direct_try(S, &a2.home, Some(&a_npub), &["logout", &a_npub]);
        println!("    A2 logout direct ok={ok2}: {out2}");
        ok2
    };
    println!("    A2 logged out: {logged_out}");
    record(S, "a2-logged-out", json!(logged_out));
    // Observe for up to 180 s: poll B's per-member device counts every 10 s
    // and record what A1's device views show along the way.
    let mut a1_sampled = false;
    for tick in 0..18 {
        let counts = members_device_counts(S, &b, &b_npub, &gid);
        println!("    B device counts after logout (tick {tick}): {counts:?}");
        if counts.iter().all(|c| *c <= 1) && !counts.is_empty() {
            break;
        }
        if !a1_sampled && tick == 8 {
            a1_sampled = true;
            let list = wn_try(S, &a1.home, &a1.socket, Some(&a_npub), &["devices", "list"]);
            record(S, "a1-devices-list-mid", json!(list.1));
            let status = wn_try(
                S,
                &a1.home,
                &a1.socket,
                Some(&a_npub),
                &["devices", "link", "status"],
            );
            record(S, "a1-link-status-mid", json!(status.1));
        }
        tokio::time::sleep(Duration::from_secs(10)).await;
    }
    let list = wn_try(S, &a1.home, &a1.socket, Some(&a_npub), &["devices", "list"]);
    record(S, "a1-devices-list-final", json!(list.1));
    let status = wn_try(
        S,
        &a1.home,
        &a1.socket,
        Some(&a_npub),
        &["devices", "link", "status"],
    );
    record(S, "a1-link-status-final", json!(status.1));
    record(
        S,
        "counts-after-logout",
        json!(members_device_counts(S, &b, &b_npub, &gid)),
    );
}

/// W4: an MDK-engine device "A3" in an unpublished slot — does wn invite it?
#[ignore = "needs WN_BIN/WND_BIN"]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn w4_foreign_slot_engine_device() {
    const S: &str = "w4";
    let relay = start_relay().await;
    let a1 = spawn_device("a1", &relay.url).await;
    let b = spawn_device("b", &relay.url).await;

    let a_keys = Keys::generate();
    let a_nsec = a_keys.secret_key().to_bech32().unwrap();
    let a_npub = a_keys.public_key().to_bech32().unwrap();
    let mut a1 = a1;
    wn_login_nsec(S, &a1.home, &a1.socket, &a_nsec);
    let b_npub = create_identity(S, &b);
    wn(S, &a1.home, &a1.socket, Some(&a_npub), &["keys", "publish"]);
    settle().await;

    // A3: an engine device of the same account, on the real Nostr peeler so
    // it can unwrap gift-wrapped Welcomes and decrypt kind-445 envelopes.
    struct NostrProof(nostr::key::SecretKey);
    impl AccountIdentityProofSigner for NostrProof {
        fn sign_account_identity_proof(
            &self,
            request: &AccountIdentityProofRequest,
        ) -> Result<[u8; 64], String> {
            let sk = k256::schnorr::SigningKey::from_slice(self.0.as_secret_bytes())
                .map_err(|e| e.to_string())?;
            if sk.verifying_key().to_bytes().as_slice() != request.account_identity.as_slice() {
                return Err("account identity mismatch".into());
            }
            use k256::schnorr::signature::hazmat::PrehashSigner;
            sk.sign_prehash(&request.proof_event_id()?)
                .map(|s| s.to_bytes())
                .map_err(|e| e.to_string())
        }
    }
    // A3 is built exactly like mk-core builds its engine: same feature
    // registry and supported-component set, so wn sees a compatible KP.
    let storage = SqliteAccountStorage::in_memory_for_test().unwrap();
    let peeler = transport_nostr_peeler::NostrMlsPeeler::new().with_welcome_signer(a_keys.clone());
    let mut a3: Engine<SqliteAccountStorage> = EngineBuilder::new(storage)
        .identity(a_keys.public_key().to_bytes().to_vec())
        .account_identity_proof_signer(Arc::new(NostrProof(a_keys.secret_key().clone())))
        .protocol_profile(cgka_traits::group::ProtocolProfile::Current)
        .feature_registry(support::mkcore_registry())
        .supported_app_components(support::mkcore_components())
        .peeler(Box::new(peeler))
        .build()
        .unwrap();

    // Publish A3's KeyPackage to a fresh random `d` slot (32 bytes as 64
    // lowercase hex, like a real device slot), plus relay lists.
    let kp = a3.fresh_key_package().await.unwrap();
    let meta = cgka_engine::key_package::key_package_metadata(&kp).unwrap();
    let slot_id = hex::encode(Keys::generate().secret_key().to_secret_bytes());
    let publication = transport_nostr_adapter::NostrKeyPackagePublication {
        client_name: Some("m6-spike".into()),
        account_id: MemberId::new(a_keys.public_key().to_bytes().to_vec()),
        key_package: kp,
        key_package_slot_id: slot_id.clone(),
        key_package_ref: meta.key_package_ref_hex.clone(),
        mls_ciphersuite: format!("0x{:04x}", meta.ciphersuite),
        mls_extensions: meta
            .mls_extensions
            .iter()
            .map(|id| format!("0x{id:04x}"))
            .collect(),
        mls_proposals: meta
            .mls_proposals
            .iter()
            .map(|id| format!("0x{id:04x}"))
            .collect(),
        app_components: meta
            .app_components
            .iter()
            .filter(|id| **id >= cgka_traits::app_components::PRIVATE_USE_APP_COMPONENT_ID_START)
            .map(|id| format!("0x{id:04x}"))
            .collect(),
        publish_endpoints: vec![TransportEndpoint(relay.url.clone())],
    };
    let dto = publication.to_event().expect("kp event dto");
    record(S, "a3-kp-event", json!({"slot": slot_id, "tags": dto.tags}));
    let unsigned = EventBuilder::new(Kind::Custom(dto.kind as u16), dto.content.clone())
        .tags(dto.tags.iter().filter_map(|t| Tag::parse(t.clone()).ok()))
        .custom_created_at(NostrTimestamp::from_secs(dto.created_at));
    let event = unsigned.finalize(&a_keys).expect("sign kp event");
    let kp_event_id = event.id.to_hex();
    relay.client.send_event(&event).await.expect("publish kp");
    // Inbox relay list (10050) + NIP-65 relay list (10002) for the account.
    for (kind, tag) in [(10_050u16, "relay"), (10_002u16, "r")] {
        let ev = EventBuilder::new(Kind::Custom(kind), "")
            .tag(Tag::parse(vec![tag.into(), relay.url.clone()]).unwrap())
            .finalize(&a_keys)
            .expect("sign relay list");
        relay
            .client
            .send_event(&ev)
            .await
            .expect("publish relay list");
    }
    settle().await;

    // (a) What do A1's device views show about the foreign slot? Sign-ins
    // are checked at client start, so poll link status for 180 s; if the
    // slot never appears, restart A1's wnd once and poll again.
    let list = wn(S, &a1.home, &a1.socket, Some(&a_npub), &["devices", "list"]);
    record(S, "a1-devices-list", list);
    let mut sign_in_seen = false;
    let mut restarted = false;
    for tick in 0..24 {
        let status = wn(
            S,
            &a1.home,
            &a1.socket,
            Some(&a_npub),
            &["devices", "link", "status"],
        );
        let text = serde_json::to_string(&status).unwrap_or_default();
        println!("    a1 link status (tick {tick}): {text}");
        let prompts_nonempty = result(&status, "/prompts")
            .and_then(Value::as_array)
            .is_some_and(|p| !p.is_empty());
        if text.contains(&slot_id) || prompts_nonempty {
            sign_in_seen = true;
            record(S, "a1-link-status-signin", status);
            break;
        }
        if tick == 17 {
            record(S, "a1-link-status-before-restart", status);
            if !restarted {
                restarted = true;
                println!("    no sign-in after 180 s; restarting A1's wnd once");
                a1.wnd.shutdown_keep_home();
                a1.wnd = spawn_wnd(&a1.home, &a1.socket, &a1.logs, &relay.url).await;
            }
        } else {
            tokio::time::sleep(Duration::from_secs(10)).await;
        }
    }
    record(
        S,
        "a3-seen-as-signin",
        json!({"seen": sign_in_seen, "restarted": restarted, "slot": slot_id}),
    );

    // (b) B invites account A — does it select A3's KeyPackage too?
    let created = create_group_invite(S, &b, &b_npub, "w4", &a_npub).await;
    let gid = group_hex(&created);
    record(
        S,
        "b-member-outcomes",
        result(&created, "/member_outcomes")
            .cloned()
            .unwrap_or(Value::Null),
    );
    record(
        S,
        "b-selected-device-count",
        result(&created, "/selected_device_count")
            .or_else(|| result(&created, "/member_outcomes/0/selected_device_count"))
            .cloned()
            .unwrap_or(Value::Null),
    );
    settle().await;
    settle().await;

    // Fetch gift wraps addressed to account A; try each against A3's engine.
    let filter = Filter::new()
        .kind(Kind::Custom(1059))
        .custom_tag(SingleLetterTag::LOWERCASE_P, a_keys.public_key().to_hex());
    let wraps = relay
        .client
        .fetch_events(filter)
        .await
        .map(|events| events.into_iter().collect::<Vec<_>>())
        .unwrap_or_default();
    println!("W4 gift wraps for A: {}", wraps.len());
    let mut joined = false;
    for wrap in &wraps {
        let Ok(dto) = NostrTransportEvent::from_nostr_event(wrap) else {
            continue;
        };
        let Ok(msg) = dto.to_transport_message() else {
            continue;
        };
        match a3.ingest(msg).await {
            Ok(outcome) => {
                println!("    wrap -> {outcome:?}");
                if matches!(outcome, cgka_traits::ingest::IngestOutcome::Processed) {
                    joined = true;
                }
            }
            Err(e) => println!("    wrap not for A3: {e:?}"),
        }
    }
    let events = a3.drain_events();
    support::print_events("a3", &events);
    println!("W4 A3 joined: {joined}");
    record(
        S,
        "a3-joined",
        json!({"joined": joined, "kp_event_id": kp_event_id}),
    );

    if joined {
        // A3 -> B: engine app message through the nostr envelope.
        let sent = a3
            .send(SendIntent::AppMessage {
                group_id: cgka_traits::types::GroupId::new(hex::decode(&gid).unwrap()),
                payload: support::app_payload_for(&a3, "hello from A3 engine"),
                expected_epoch: None,
            })
            .await
            .unwrap();
        let SendResult::ApplicationMessage { msg, .. } = sent else {
            panic!("expected ApplicationMessage")
        };
        let dto = NostrTransportEvent::from_transport_message(&msg).unwrap();
        let event = dto.to_verified_nostr_event().unwrap();
        record(S, "a3-outbound-445", json!({"h": dto.tag_value("h")}));
        relay.client.send_event(&event).await.expect("publish 445");
        settle().await;
        let msgs = wn(
            S,
            &b.home,
            &b.socket,
            Some(&b_npub),
            &["messages", "list", &gid, "--limit", "5"],
        );
        record(S, "b-messages", msgs);
    }
}

/// W5: wn rotates the KeyPackage after a Welcome consumed it.
#[ignore = "needs WN_BIN/WND_BIN"]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn w5_keypackage_rotation_by_wn() {
    const S: &str = "w5";
    let relay = start_relay().await;
    let a1 = spawn_device("a1", &relay.url).await;
    let b = spawn_device("b", &relay.url).await;

    let a_npub = create_identity(S, &a1);
    let b_npub = create_identity(S, &b);
    wn(S, &a1.home, &a1.socket, Some(&a_npub), &["keys", "publish"]);
    settle().await;

    let before = wn(S, &a1.home, &a1.socket, Some(&a_npub), &["keys", "list"]);
    let status_before = wn(
        S,
        &a1.home,
        &a1.socket,
        Some(&a_npub),
        &["keys", "maintenance-status"],
    );
    record(
        S,
        "keys-before",
        json!({"list": before, "status": status_before}),
    );

    let created = create_group_invite(S, &b, &b_npub, "w5", &a_npub).await;
    let gid = group_hex(&created);
    settle().await;
    wn_try(
        S,
        &a1.home,
        &a1.socket,
        Some(&a_npub),
        &["groups", "accept", &gid],
    );
    settle().await;

    // Poll `keys maintenance-status` every 10 s for up to 180 s; a consumed
    // KeyPackage should schedule a replacement. If none appears, run
    // account/group maintenance once and keep polling.
    let replacement_seen = |status: &Value| -> bool {
        serde_json::to_string(status).is_ok_and(|t| {
            !t.contains("\"pending_replacement\":null") && t.contains("pending_replacement")
        })
    };
    let mut seen = false;
    let mut maintenance_ran = false;
    for tick in 0..30 {
        let status = wn(
            S,
            &a1.home,
            &a1.socket,
            Some(&a_npub),
            &["keys", "maintenance-status"],
        );
        let keys = wn(S, &a1.home, &a1.socket, Some(&a_npub), &["keys", "list"]);
        println!("    a1 keys maintenance-status (tick {tick}): {status}");
        if replacement_seen(&status) {
            seen = true;
            record(
                S,
                "replacement-observed",
                json!({"tick": tick, "status": status, "list": keys}),
            );
            break;
        }
        if tick == 17 {
            record(
                S,
                "status-after-180s",
                json!({"status": status, "list": keys}),
            );
            if !maintenance_ran {
                maintenance_ran = true;
                let ran = wn_try(
                    S,
                    &a1.home,
                    &a1.socket,
                    Some(&a_npub),
                    &["groups", "run-maintenance"],
                );
                record(
                    S,
                    "groups-run-maintenance",
                    json!({"ok": ran.0, "out": ran.1}),
                );
                settle().await;
            }
        } else {
            tokio::time::sleep(Duration::from_secs(10)).await;
        }
    }
    let after = wn(S, &a1.home, &a1.socket, Some(&a_npub), &["keys", "list"]);
    let status_after = wn(
        S,
        &a1.home,
        &a1.socket,
        Some(&a_npub),
        &["keys", "maintenance-status"],
    );
    record(
        S,
        "keys-after",
        json!({"list": after, "status": status_after, "replacement_seen": seen, "maintenance_ran": maintenance_ran}),
    );
    println!("W5 replacement_seen={seen} maintenance_ran={maintenance_ran}");
}

/// W6: how long does wn's post-join self-update take, and through which phases?
#[ignore = "needs WN_BIN/WND_BIN"]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn w6_post_join_self_update_timing() {
    const S: &str = "w6";
    let relay = start_relay().await;
    let a1 = spawn_device("a1", &relay.url).await;
    let b = spawn_device("b", &relay.url).await;

    let a_npub = create_identity(S, &a1);
    let b_npub = create_identity(S, &b);
    let created = create_group_invite(S, &b, &b_npub, "w6", &a_npub).await;
    let gid = group_hex(&created);
    settle().await;
    wn_try(
        S,
        &a1.home,
        &a1.socket,
        Some(&a_npub),
        &["groups", "accept", &gid],
    );

    let started = std::time::Instant::now();
    for _ in 0..40 {
        let status = wn(
            S,
            &a1.home,
            &a1.socket,
            Some(&a_npub),
            &["groups", "maintenance-status", &gid],
        );
        record(
            S,
            "maintenance-status",
            json!({
                "elapsed_s": started.elapsed().as_secs(),
                "status": result(&status, "").cloned().unwrap_or(Value::Null),
            }),
        );
        let text = status.to_string();
        if text.contains("\"phase\":\"complete\"") {
            println!(
                "W6 post-join self-update done after {}s",
                started.elapsed().as_secs()
            );
            break;
        }
        tokio::time::sleep(Duration::from_secs(5)).await;
    }
    println!("W6 total elapsed: {}s", started.elapsed().as_secs());
}

#[allow(dead_code)]
fn _unused(_p: &Path, _m: &TransportMessage) {}
