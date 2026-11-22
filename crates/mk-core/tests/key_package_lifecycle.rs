//! D3 `KeyPackage` lifecycle flows (m6-a step 2): the slot's durable
//! record drives staging, signing, publication, promotion, refresh and
//! retention through the public `Session` API.

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
    reason = "test code"
)]

use mk_core as _;
use tokio as _;

use base64 as _;
use cgka_engine as _;
use cgka_traits as _;
use chacha20poly1305 as _;
use getrandom as _;
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

use hex as _;

#[cfg(feature = "testing")]
mod support;

#[cfg(feature = "testing")]
mod lifecycle_tests {

    use std::sync::Arc;

    use mk_core::{
        Availability, InputOutcome, Outbound, OutboundPurpose, SessionEvent,
        testing::key_package_bundle_count,
    };

    use super::support::*;

    const RELAY: &str = "ws://127.0.0.1:7777";
    const RELAY2: &str = "ws://127.0.0.1:7778";

    /// The `d` tag value of a KeyPackage event.
    fn d_tag(event: &mk_core::Event) -> String {
        event
            .tags
            .iter()
            .find(|tag| tag.first().map(String::as_str) == Some("d"))
            .and_then(|tag| tag.get(1).cloned())
            .expect("key package carries a d tag")
    }

    /// Release the gated batch, take the releasable outbound.
    fn take(member: &mut Member) -> Vec<Outbound> {
        if let Some(seq) = member.session.awaiting_durability() {
            member.session.acknowledge_durable(seq);
        }
        member.session.take_outbound().expect("take_outbound")
    }

    /// Publish `item` to `relay` as accepted and report it back.
    async fn accept(member: &mut Member, relay: &FakeRelay, item: &Outbound) {
        let results = relay.publish(item);
        member
            .session
            .report_publish(&item.id, &results)
            .await
            .expect("report_publish");
    }

    /// Ensure + release + accept the staged publication: the slot ends
    /// promoted with `current` set. Returns the published outbound.
    async fn publish_promoted(member: &mut Member, relay: &FakeRelay) -> Outbound {
        member
            .session
            .ensure_key_package(&[RELAY.to_owned()])
            .await
            .expect("ensure_key_package");
        let outbound = take(member);
        assert_eq!(outbound.len(), 1, "one staged publication");
        let item = outbound[0].clone();
        accept(member, relay, &item).await;
        item
    }

    /// The bundle-count invariant: durable private material is exactly
    /// `current + pending + retained`.
    fn assert_bundle_invariant(member: &Member) {
        let status = member.session.key_package_status().expect("status");
        let expected = usize::from(status.current.is_some())
            + usize::from(status.pending)
            + status.retained as usize;
        assert_eq!(
            key_package_bundle_count(&member.host.durable_rows()),
            expected,
            "bundles == current + pending + retained"
        );
    }

    /// First `ensure_key_package` stages, signs and queues exactly one
    /// outbound; a repeat call against the same write set is a no-op.
    #[tokio::test(flavor = "current_thread")]
    async fn ensure_stages_one_and_repeats_no_op() {
        let mut a = open_member().await;

        let status = a
            .session
            .ensure_key_package(&[RELAY.to_owned()])
            .await
            .expect("ensure_key_package");
        assert!(status.pending, "a replacement is in flight");
        assert!(status.current.is_none(), "nothing promoted yet");
        assert_ne!(status.slot, [0u8; 32], "a stable slot exists");

        let outbound = take(&mut a);
        assert_eq!(outbound.len(), 1, "exactly one outbound");
        let item = &outbound[0];
        assert_eq!(item.purpose, OutboundPurpose::KeyPackage);
        assert_eq!(item.relays, vec![RELAY.to_owned()]);
        assert_eq!(item.event.kind, 30_443);
        assert_eq!(d_tag(&item.event), hex::encode(status.slot));

        // Repeating against the same write set stages nothing new.
        a.session
            .ensure_key_package(&[RELAY.to_owned()])
            .await
            .expect("repeat ensure");
        assert!(take(&mut a).is_empty(), "repeat ensure is a no-op");
        assert_bundle_invariant(&a);
    }

    /// A relay acknowledgement promotes the pending publication and emits
    /// `KeyPackageRotated`.
    #[tokio::test(flavor = "current_thread")]
    async fn acknowledgement_promotes_and_rotates() {
        let relay = FakeRelay::new();
        let mut a = open_member().await;
        let item = publish_promoted(&mut a, &relay).await;

        let status = a.session.key_package_status().expect("status");
        let current = status.current.expect("promoted to current");
        assert_eq!(current.event_id, item.event.id);
        assert!(!status.pending);
        let events = a.session.drain_events().expect("drain");
        assert!(
            events
                .iter()
                .any(|event| matches!(event, SessionEvent::KeyPackageRotated)),
            "promotion emitted KeyPackageRotated: {events:?}"
        );
        assert_bundle_invariant(&a);
    }

    /// A Welcome consuming the current package makes the next `tick` stage
    /// a replacement in the same `d` slot with a newer `created_at`.
    #[tokio::test(flavor = "current_thread")]
    async fn consumption_replaces_in_same_slot() {
        let relay = FakeRelay::new();
        let mut a = open_member().await;
        publish_identity(&mut a, &relay, RELAY).await;
        let first = key_package_of(&relay, RELAY, &a.account_hex());

        // B invites A: the Welcome consumes A's current KeyPackage.
        let mut b = open_member().await;
        publish_identity(&mut b, &relay, RELAY).await;
        group_with(&mut b, &relay, RELAY, &[&a], "consume").await;
        let welcomes = welcomes_for(&relay, RELAY, &a.account_hex());
        ingest_backlog(&mut a, &welcomes).await;

        // The lifecycle now wants a replacement; `tick` stages, signs and
        // queues it against the remembered write set — before the host
        // publishes or reports it.
        a.session.tick().await.expect("tick");
        let before = take(&mut a);
        let replacement = before
            .iter()
            .find(|item| item.purpose == OutboundPurpose::KeyPackage)
            .cloned()
            .expect("a replacement KeyPackage outbound");
        assert_eq!(d_tag(&replacement.event), d_tag(&first));
        assert!(
            replacement.event.created_at > first.created_at,
            "the replacement postdates the consumed publication"
        );
        let status = a.session.key_package_status().expect("status");
        assert!(status.pending, "replacement in flight");
        assert_bundle_invariant(&a);
    }

    /// A promoted replacement retains the old private material for its
    /// 24-hour grace, then the sweep deletes it.
    #[tokio::test(flavor = "current_thread")]
    async fn retained_material_lapses_after_grace() {
        let relay = FakeRelay::new();
        let mut a = open_member().await;
        publish_identity(&mut a, &relay, RELAY).await;
        let first = key_package_of(&relay, RELAY, &a.account_hex());

        // Consume the current package through a real Welcome so the
        // replacement has a reason to exist.
        let mut b = open_member().await;
        publish_identity(&mut b, &relay, RELAY).await;
        group_with(&mut b, &relay, RELAY, &[&a], "grace").await;
        let welcomes = welcomes_for(&relay, RELAY, &a.account_hex());
        ingest_backlog(&mut a, &welcomes).await;
        a.session.tick().await.expect("tick");

        let outbound = take(&mut a);
        let replacement = outbound
            .iter()
            .find(|item| item.purpose == OutboundPurpose::KeyPackage)
            .cloned()
            .expect("a replacement outbound");
        accept(&mut a, &relay, &replacement).await;

        let status = a.session.key_package_status().expect("status");
        assert_eq!(status.retained, 1, "the old bundle is retained");
        assert_eq!(
            status.current.expect("current").event_id,
            replacement.event.id
        );
        assert_eq!(d_tag(&first), d_tag(&replacement.event));
        assert_bundle_invariant(&a);

        // Inside the grace nothing is deleted.
        a.clock.advance(23 * 3_600_000);
        a.session.tick().await.expect("tick");
        assert_eq!(
            a.session.key_package_status().expect("status").retained,
            1,
            "retention holds inside the grace"
        );
        assert_bundle_invariant(&a);

        // Past it the sweep deletes the bundle.
        a.clock.advance(2 * 3_600_000);
        a.session.tick().await.expect("tick past grace");
        assert_eq!(
            a.session.key_package_status().expect("status").retained,
            0,
            "the retained bundle lapsed"
        );
        assert_bundle_invariant(&a);
    }

    /// Write-set growth re-sends the same signed event id to only the new
    /// relays, on a `KeyPackageRepublish` outbound.
    #[tokio::test(flavor = "current_thread")]
    async fn relay_growth_republishes_same_event() {
        let relay = FakeRelay::new();
        let mut a = open_member().await;
        let first = publish_promoted(&mut a, &relay).await;
        a.session.drain_events().expect("drain");

        let status = a
            .session
            .ensure_key_package(&[RELAY.to_owned(), RELAY2.to_owned()])
            .await
            .expect("ensure_key_package growth");

        let outbound = take(&mut a);
        assert_eq!(outbound.len(), 1, "one republish outbound");
        let republish = &outbound[0];
        assert_eq!(republish.event.id, first.event.id, "the same signed event");
        assert_eq!(
            republish.relays,
            vec![RELAY2.to_owned()],
            "only the new relay"
        );
        assert_eq!(republish.purpose, OutboundPurpose::KeyPackageRepublish);
        assert!(
            !status.pending,
            "a republish is not a replacement: {status:?}"
        );
        assert_bundle_invariant(&a);

        // Repeating the grown set re-sends nothing.
        a.session
            .ensure_key_package(&[RELAY.to_owned(), RELAY2.to_owned()])
            .await
            .expect("ensure_key_package repeat");
        assert!(take(&mut a).is_empty(), "grown set is satisfied");
    }

    /// A Welcome whose unwrap the signer refuses is `unavailable` and leaves
    /// the lifecycle record untouched.
    #[tokio::test(flavor = "current_thread")]
    async fn failed_welcome_leaves_lifecycle_untouched() {
        let relay = FakeRelay::new();
        let signer = Arc::new(GatedSigner::new());
        let mut a = open_member_with(Arc::<GatedSigner>::clone(&signer)).await;
        publish_identity(&mut a, &relay, RELAY).await;
        let first = key_package_of(&relay, RELAY, &a.account_hex());

        let mut b = open_member().await;
        publish_identity(&mut b, &relay, RELAY).await;
        group_with(&mut b, &relay, RELAY, &[&a], "failed-join").await;

        let before = a.session.key_package_status().expect("status");
        let welcomes = welcomes_for(&relay, RELAY, &a.account_hex());
        assert!(!welcomes.is_empty(), "a welcome must be waiting");
        signer.set(DecryptGate::Refused);
        let reports = a
            .session
            .ingest_batch(welcomes)
            .await
            .expect("refused ingest reports");
        assert!(
            reports.iter().all(|report| {
                report.outcome
                    == InputOutcome::Unavailable {
                        category: Availability::SignerRefused,
                    }
            }),
            "a refused unwrap reports unavailable: {reports:?}"
        );

        let after = a.session.key_package_status().expect("status");
        assert_eq!(after, before, "a failed welcome mutates nothing");
        assert_eq!(
            after.current.expect("current").event_id,
            first.id,
            "the publication still stands"
        );
        assert_bundle_invariant(&a);
    }
}
