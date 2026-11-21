//! G1: non-Welcome gift wraps. A non-device-group client receives a
//! kind-1059 wrap whose rumor is a kind-452 device-pairing request or a
//! kind-1159 group-recovery rumor — print how the engine classifies each.

mod support;

use cgka_traits::app_components::ACCOUNT_DEVICES_COMPONENT_ID;
use cgka_traits::engine::CgkaEngine;
use cgka_traits::types::GroupId;
use cgka_traits::{GroupRecoveryBody, GroupRecoveryMessage};
use nostr::prelude::*;
use nostr::types::time::Timestamp as NostrTimestamp;
use std::sync::Arc;
use support::*;
use transport_nostr_peeler::{NostrTransportEvent, gift_wrap_rumor};

async fn wrap_to_self(a_keys: &Keys, rumor: UnsignedEvent) -> Event {
    gift_wrap_rumor(
        Arc::new(a_keys.clone()),
        a_keys.public_key(),
        rumor,
    )
    .await
    .expect("gift wrap")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn g1_non_welcome_gift_wraps() {
    let a_keys = nostr_keys(b"alice");
    let mut a = nostr_peeler_device(b"alice", &[ACCOUNT_DEVICES_COMPONENT_ID]);

    // (a) kind-452 device-pairing rumor, arbitrary content.
    let rumor_452 = EventBuilder::new(Kind::Custom(452), "m6-spike pairing probe")
        .finalize_unsigned(a_keys.public_key());
    let wrap = wrap_to_self(&a_keys, rumor_452).await;
    let dto = NostrTransportEvent::from_nostr_event(&wrap).unwrap();
    let msg = dto.to_transport_message().unwrap();
    let outcome = a.engine.ingest(msg).await;
    println!("G1(a) kind-452 pairing rumor ingest -> {outcome:?}");

    // (b) kind-1159 group-recovery rumor, mdk_group_recovery version tag.
    let recovery = GroupRecoveryMessage {
        group_id: GroupId::new(vec![0x42; 16]),
        request_id: [9; 32],
        expires_at: NostrTimestamp::now().as_secs() + 86_400,
        body: GroupRecoveryBody::Request {
            device_key_hash: [7; 32],
        },
    };
    let content = String::from_utf8(recovery.encode().unwrap()).unwrap();
    let rumor_1159 = EventBuilder::new(Kind::Custom(1159), content)
        .tags([
            Tag::parse(["mdk_group_recovery".to_string(), "1".to_string()]).unwrap(),
            Tag::public_key(a_keys.public_key()),
        ])
        .finalize_unsigned(a_keys.public_key());
    let wrap = wrap_to_self(&a_keys, rumor_1159).await;
    let dto = NostrTransportEvent::from_nostr_event(&wrap).unwrap();
    let msg = dto.to_transport_message().unwrap();
    let outcome = a.engine.ingest(msg).await;
    println!("G1(b) kind-1159 recovery rumor ingest -> {outcome:?}");

    print_events("a", &a.engine.drain_events());
}
