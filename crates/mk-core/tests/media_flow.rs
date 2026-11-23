//! Encrypted-media session flow: seal/upload/send/open through the public
//! `Session` API over the in-process harness — epoch pinning, own-message
//! secret custody, durable reopen, typed refusals.

#![allow(
    clippy::missing_docs_in_private_items,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::needless_pass_by_value,
    clippy::tests_outside_test_module,
    clippy::shadow_reuse,
    clippy::doc_markdown,
    clippy::excessive_nesting,
    clippy::too_many_lines,
    reason = "test code"
)]

use mk_core as _;
use tokio as _;

use base64 as _;
use cgka_engine as _;
use cgka_traits as _;
use chacha20poly1305 as _;
use getrandom as _;
use hex as _;
use hkdf as _;
use nostr as _;
use openmls as _;
use openmls_rust_crypto as _;
use openmls_traits as _;
use proptest as _;
use serde as _;
use serde_json as _;
use sha2 as _;
use storage_sqlite as _;
use thiserror as _;
use transport_nostr_peeler as _;
use url as _;
use zeroize as _;

#[cfg(feature = "testing")]
mod support;

#[cfg(feature = "testing")]
mod media_flow_tests {
    use super::support::*;
    use mk_core::media::{EncryptedMediaPolicy, Locator, MediaError};
    use mk_core::{
        AppMessage, AttachmentInput, CreateGroup, GroupId, Invitee, MediaMessage, SessionError,
        SessionEvent,
    };

    const RELAY: &str = "ws://127.0.0.1:7777";

    /// Create a group carrying `components`, then deliver the Welcomes.
    async fn group_with(
        creator: &mut Member,
        relay: &FakeRelay,
        invitees: &[&Member],
        components: Vec<mk_core::ComponentUpdate>,
    ) -> GroupId {
        let invites = invitees
            .iter()
            .map(|member| Invitee {
                key_package: key_package_of(relay, RELAY, &member.account_hex()),
                inbox_relays: inbox_relays_of(relay, RELAY, &member.account_hex()),
            })
            .collect();
        let gid = creator
            .session
            .create_group(CreateGroup {
                name: "media".to_owned(),
                description: String::new(),
                relays: vec![RELAY.to_owned()],
                invitees: invites,
                own_devices: Vec::new(),
                admins: Vec::new(),
                components,
            })
            .await
            .expect("create_group");
        flush(creator, relay).await;
        gid
    }

    /// Let `member` pick up any pending Welcome and join.
    async fn join_via_welcome(member: &mut Member, relay: &FakeRelay) {
        let welcomes = welcomes_for(relay, RELAY, &member.account_hex());
        assert!(!welcomes.is_empty(), "a welcome must be waiting");
        ingest_backlog(member, &welcomes).await;
        member.session.tick().await.expect("tick");
        flush(member, relay).await;
    }

    fn input(plaintext: &[u8]) -> AttachmentInput {
        AttachmentInput {
            plaintext: plaintext.to_vec(),
            media_type: "image/png".to_owned(),
            filename: "pic.png".to_owned(),
            dim: None,
            thumbhash: None,
        }
    }

    fn locator(url: &str) -> Locator {
        Locator {
            kind: "blossom-v1".to_owned(),
            value: url.to_owned(),
        }
    }

    /// The full A→B media round trip, including durable reopen and
    /// tamper/open failure. The test host plays the blob store: an
    /// in-memory map keyed by ciphertext hash, exactly what a real
    /// Blossom upload/fetch commits to.
    #[tokio::test]
    async fn media_round_trip_and_tamper() {
        let relay = FakeRelay::new();
        let mut a = open_member().await;
        let mut b = open_member().await;
        publish_identity(&mut a, &relay, RELAY).await;
        publish_identity(&mut b, &relay, RELAY).await;

        let policy = EncryptedMediaPolicy::blossom(&["https://blossom.example/".to_owned()]);
        let gid = group_with(
            &mut a,
            &relay,
            &[&b],
            vec![policy.to_component().expect("component")],
        )
        .await;
        join_via_welcome(&mut b, &relay).await;
        for member in [&a, &b] {
            let snapshot = member.session.group(&gid).expect("group").expect("exists");
            assert_eq!(
                snapshot.encrypted_media.as_ref().map(|p| &p.endpoints),
                Some(&policy.endpoints),
                "policy visible on both ends"
            );
        }

        // A seals; the host "uploads" the blob; A sends the reference.
        let plaintext: Vec<u8> = (0u32..50_000).map(|n| (n % 251) as u8).collect();
        let sealed = a
            .session
            .seal_attachment(&gid, input(&plaintext))
            .expect("seal");
        let blob_url = format!(
            "https://blossom.example/{}",
            hex::encode(sealed.ciphertext_sha256)
        );
        let mut blobs = std::collections::HashMap::new();
        blobs.insert(sealed.ciphertext_sha256, sealed.ciphertext.clone());
        a.session
            .send_media(
                &gid,
                MediaMessage {
                    caption: "a pic".to_owned(),
                    attachments: vec![(sealed.pending, vec![locator(&blob_url)])],
                    tags: Vec::new(),
                },
            )
            .await
            .expect("send_media");
        flush(&mut a, &relay).await;

        // B ingests: one kind-9 with one valid attachment.
        let events = sync_member(&mut b, &relay, RELAY, &gid).await;
        let (message, group) = events
            .iter()
            .find_map(|event| match event {
                SessionEvent::MessageReceived { group, message }
                    if !message.attachments.is_empty() =>
                {
                    Some((message, group))
                }
                _ => None,
            })
            .expect("media message");
        assert_eq!(message.kind, 9);
        assert_eq!(message.content, "a pic");
        assert_eq!(message.attachments.len(), 1);
        let reference = message.attachments[0].as_ref().expect("attachment parses");
        assert_eq!(reference.locators[0].value, blob_url);
        let transport_id = message.transport_id;
        let group = group.clone();

        // B fetches the blob by its locator hash and opens it.
        let fetched = blobs.get(&reference.ciphertext_sha256).expect("uploaded");
        assert_eq!(
            b.session
                .open_attachment(&group, &transport_id, reference, fetched)
                .expect("open"),
            plaintext
        );

        // A can open its own attachment: the sender-side secret row is
        // keyed by A's transport id — the kind-445 event it published.
        let own_transport_id = group_events(&relay, RELAY, &a.route_hex(&gid))[0].id;
        assert_eq!(
            a.session
                .open_attachment(&group, &own_transport_id, reference, fetched)
                .expect("own open"),
            plaintext
        );

        // Reopen B from durable rows — the secret row survives; still opens.
        let b2 = reopen_member(&b).await;
        assert_eq!(
            b2.session
                .open_attachment(&group, &transport_id, reference, fetched)
                .expect("reopen open"),
            plaintext
        );

        // Tampered ciphertext → typed ciphertext-hash error.
        let mut tampered = fetched.clone();
        tampered[0] ^= 0x01;
        assert!(matches!(
            b2.session
                .open_attachment(&group, &transport_id, reference, &tampered),
            Err(SessionError::Media(MediaError::CiphertextHashMismatch))
        ));
    }

    /// The sealed attachment's epoch pin gates the send: move the epoch,
    /// then the stale pin refuses with `MediaEpochStale` and stages nothing.
    #[tokio::test]
    async fn stale_pin_refuses() {
        let relay = FakeRelay::new();
        let mut a = open_member().await;
        publish_identity(&mut a, &relay, RELAY).await;
        let policy = EncryptedMediaPolicy::blossom(&["https://blossom.example/".to_owned()]);
        let gid = group_with(
            &mut a,
            &relay,
            &[],
            vec![policy.to_component().expect("component")],
        )
        .await;

        let sealed = a
            .session
            .seal_attachment(&gid, input(b"data"))
            .expect("seal");
        // Move the epoch with a self-update and confirm it.
        a.session.self_update(&gid).await.expect("self_update");
        flush(&mut a, &relay).await;
        a.session.tick().await.expect("tick");
        flush(&mut a, &relay).await;

        let err = a
            .session
            .send_media(
                &gid,
                MediaMessage {
                    caption: String::new(),
                    attachments: vec![(sealed.pending, vec![locator("https://blossom.example/x")])],
                    tags: Vec::new(),
                },
            )
            .await
            .expect_err("stale pin");
        assert!(matches!(err, SessionError::MediaEpochStale));
        assert_eq!(a.session.take_outbound().expect("outbound").len(), 0);
    }

    /// Typed refusals: no `0x800b` → `MediaNotEnabled`; locator kind outside
    /// the group policy → `Media(LocatorNotAllowedByPolicy)`; reserved
    /// surfaces in `send_message` → `ReservedAppEvent`.
    #[tokio::test]
    async fn typed_refusals() {
        let relay = FakeRelay::new();
        let mut a = open_member().await;
        publish_identity(&mut a, &relay, RELAY).await;
        // Plain group: no media component.
        let plain = group_with(&mut a, &relay, &[], Vec::new()).await;
        assert!(matches!(
            a.session.seal_attachment(&plain, input(b"x")),
            Err(SessionError::MediaNotEnabled)
        ));

        let policy = EncryptedMediaPolicy::blossom(&["https://blossom.example/".to_owned()]);
        let gid = group_with(
            &mut a,
            &relay,
            &[],
            vec![policy.to_component().expect("component")],
        )
        .await;
        let sealed = a.session.seal_attachment(&gid, input(b"x")).expect("seal");
        let out_of_policy = Locator {
            kind: "ipfs-v0".to_owned(),
            value: "https://gateway.example/ipfs/x".to_owned(),
        };
        assert!(matches!(
            a.session
                .send_media(
                    &gid,
                    MediaMessage {
                        caption: String::new(),
                        attachments: vec![(sealed.pending, vec![out_of_policy])],
                        tags: Vec::new(),
                    },
                )
                .await,
            Err(SessionError::Media(MediaError::LocatorNotAllowedByPolicy))
        ));

        // Input errors: no attachments at all, and an attachment with no
        // locators — both `InvalidInput`, never `InvalidLocator`.
        let unlocated = a.session.seal_attachment(&gid, input(b"x")).expect("seal");
        for attachments in [Vec::new(), vec![(unlocated.pending, Vec::new())]] {
            assert!(matches!(
                a.session
                    .send_media(
                        &gid,
                        MediaMessage {
                            caption: String::new(),
                            attachments,
                            tags: Vec::new(),
                        },
                    )
                    .await,
                Err(SessionError::InvalidInput(_))
            ));
        }

        // Reserved surfaces: kind 1200, an `imeta` tag, a `stream-start` tag.
        for message in [
            AppMessage {
                kind: 1200,
                tags: Vec::new(),
                content: "x".to_owned(),
            },
            AppMessage {
                kind: 9,
                tags: vec![vec!["imeta".to_owned(), "v encrypted-media-v2".to_owned()]],
                content: "x".to_owned(),
            },
            AppMessage {
                kind: 9,
                tags: vec![vec!["stream-start".to_owned(), "ab".to_owned()]],
                content: "x".to_owned(),
            },
        ] {
            assert!(matches!(
                a.session.send_message(&gid, message).await,
                Err(SessionError::ReservedAppEvent)
            ));
        }
    }
}
