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

    use std::sync::{Arc, Mutex};

    use mk_core::{
        AccountId, AccountSigner, Availability, Clock, Event, GroupState, InputOutcome, Outbound,
        OutboundPurpose, SessionError, SessionEvent, SignerError, SignerFuture, UnsignedEvent,
        testing::{LocalSigner, key_package_bundle_count},
    };

    use super::support::*;

    const RELAY: &str = "ws://127.0.0.1:7777";
    const RELAY2: &str = "ws://127.0.0.1:7778";

    /// A `LocalSigner` whose `sign_event` can be switched to refuse —
    /// for the tick-driven `KeyPackage` sign retry.
    #[derive(Debug)]
    struct SignGateSigner {
        inner: LocalSigner,
        refused: Mutex<bool>,
    }

    impl SignGateSigner {
        fn new() -> Self {
            Self {
                inner: LocalSigner::new(),
                refused: Mutex::new(false),
            }
        }

        fn set_refused(&self) {
            *self.refused.lock().unwrap() = true;
        }
    }

    impl AccountSigner for SignGateSigner {
        fn account(&self) -> AccountId {
            self.inner.account()
        }

        fn sign_event(
            &self,
            unsigned: UnsignedEvent,
        ) -> SignerFuture<'_, Result<Event, SignerError>> {
            if *self.refused.lock().unwrap() {
                return Box::pin(async { Err(SignerError::Refused) });
            }
            self.inner.sign_event(unsigned)
        }

        fn nip44_encrypt<'a>(
            &'a self,
            peer: &'a AccountId,
            plaintext: &'a str,
        ) -> SignerFuture<'a, Result<String, SignerError>> {
            self.inner.nip44_encrypt(peer, plaintext)
        }

        fn nip44_decrypt<'a>(
            &'a self,
            peer: &'a AccountId,
            ciphertext: &'a str,
        ) -> SignerFuture<'a, Result<String, SignerError>> {
            self.inner.nip44_decrypt(peer, ciphertext)
        }
    }

    /// The `d` tag value of a KeyPackage event.
    fn d_tag(event: &Event) -> String {
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

    /// A fresh session with nothing published wakes for nothing: a due
    /// replacement that cannot be acted on (no write set known) must
    /// not arm an immediate `tick` retry.
    #[tokio::test(flavor = "current_thread")]
    async fn fresh_session_tick_wakes_for_nothing() {
        let mut a = open_member().await;
        let first = a.session.tick().await.expect("tick");
        assert!(
            first.next_wake_ms.is_none(),
            "no lifecycle work yet, no wake: {first:?}"
        );
        let second = a.session.tick().await.expect("tick");
        assert!(
            second.next_wake_ms.is_none(),
            "a second pass stages nothing either: {second:?}"
        );
        assert!(
            take(&mut a).is_empty(),
            "no write set, no replacement staged"
        );
    }

    /// A signer refusal on the tick-driven replacement leaves the
    /// pending unsigned and arms the 5-minute in-memory retry — the
    /// wake is the retry delay, never an immediate spin.
    #[tokio::test(flavor = "current_thread")]
    async fn tick_signer_refusal_arms_five_minute_retry() {
        let relay = FakeRelay::new();
        let signer = Arc::new(SignGateSigner::new());
        let mut a = open_member_with(Arc::<SignGateSigner>::clone(&signer)).await;
        publish_identity(&mut a, &relay, RELAY).await;

        // Past the weekly refresh the slot wants a replacement.
        a.clock.advance(8 * 86_400_000);
        signer.set_refused();
        let report = a.session.tick().await.expect("tick");
        assert_eq!(
            report.next_wake_ms,
            Some(5 * 60 * 1_000),
            "the wake is the sign retry, not an immediate re-tick"
        );
        let status = a.session.key_package_status().expect("status");
        assert!(status.pending, "the unsigned replacement stays staged");
        assert_eq!(
            status.next_due_ms,
            Some(a.clock.now().unix_ms + 5 * 60 * 1_000),
            "status reports the same retry deadline"
        );
        assert_bundle_invariant(&a);
    }

    /// A current inside its last day while a replacement is already in
    /// flight leaves a strictly future wake — the lapsed near-expiry
    /// deadline is not re-armed every tick.
    #[tokio::test(flavor = "current_thread")]
    async fn pending_in_flight_never_wakes_immediately() {
        let relay = FakeRelay::new();
        let mut a = open_member().await;
        publish_identity(&mut a, &relay, RELAY).await;

        // Stage a replacement through the refresh path and leave it
        // unacknowledged: pending in flight.
        a.clock.advance(8 * 86_400_000);
        a.session.tick().await.expect("tick");
        assert!(
            take(&mut a)
                .iter()
                .any(|item| item.purpose == OutboundPurpose::KeyPackage),
            "the refresh staged a replacement"
        );
        assert!(a.session.key_package_status().expect("status").pending);

        // Now inside the current's last day (30-day lifetime): the
        // near-expiry deadline has lapsed but the pending stands in.
        a.clock.advance(21 * 86_400_000 + 12 * 3_600_000);
        let report = a.session.tick().await.expect("tick");
        assert!(
            matches!(report.next_wake_ms, Some(wake) if wake > 0),
            "the wake stays future-dated: {report:?}"
        );
        let status = a.session.key_package_status().expect("status");
        assert!(status.pending);
        assert!(status.current.is_some(), "the current still stands");
        assert_bundle_invariant(&a);
    }

    /// Sign-out withdraws the slot atomically: one well-formed kind-5 to
    /// the union write set, every bundle gone, every outstanding
    /// `KeyPackage`/`KeyPackageRepublish` row dropped, `ensure` answers
    /// `SignedOut` and ticks stage nothing.
    #[tokio::test(flavor = "current_thread")]
    async fn sign_out_withdraws_slot_and_deletes_bundles() {
        let relay = FakeRelay::new();
        let mut a = open_member().await;
        publish_identity(&mut a, &relay, RELAY).await;

        // Consume the current package so the replacement leaves a retained
        // bundle still inside its grace — the union retirement must catch
        // it too, not just the engine's judged ownership list.
        let mut b = open_member().await;
        publish_identity(&mut b, &relay, RELAY).await;
        let group = group_with(&mut b, &relay, RELAY, &[&a], "slot").await;
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
        assert_eq!(
            a.session.key_package_status().expect("status").retained,
            1,
            "the consumed bundle is retained in grace"
        );
        let current = replacement.event.clone();

        // Grow the write set and leave the republish outstanding.
        a.session
            .ensure_key_package(&[RELAY.to_owned(), RELAY2.to_owned()])
            .await
            .expect("ensure");
        let republish = take(&mut a)
            .into_iter()
            .find(|item| item.purpose == OutboundPurpose::KeyPackageRepublish)
            .expect("republish outbound");

        let report = a
            .session
            .sign_out(&[RELAY.to_owned()])
            .await
            .expect("sign_out");
        assert_eq!(report.left, [group], "the member group stages a leave");
        assert_eq!(report.kept_admin_leaves, []);

        let outbounds = take(&mut a);
        let deletion = outbounds
            .iter()
            .find(|item| item.purpose == OutboundPurpose::KeyPackageDeletion)
            .expect("the kind-5 deletion");
        assert_eq!(
            outbounds.len(),
            2,
            "the kind-5 and the leave commit are owed"
        );
        assert_eq!(deletion.purpose, OutboundPurpose::KeyPackageDeletion);
        assert_eq!(deletion.event.kind, 5);
        assert_eq!(deletion.event.pubkey, a.signer.account());
        assert_eq!(deletion.event.content, "");
        let a_tags: Vec<_> = deletion
            .event
            .tags
            .iter()
            .filter(|tag| tag.first().map(String::as_str) == Some("a"))
            .collect();
        assert_eq!(a_tags.len(), 1, "exactly one a tag");
        assert_eq!(
            a_tags[0].get(1).map(String::as_str),
            Some(format!("30443:{}:{}", a.account_hex(), d_tag(&current)).as_str()),
            "the a tag names the slot"
        );
        let k_tags: Vec<_> = deletion
            .event
            .tags
            .iter()
            .filter(|tag| tag.first().map(String::as_str) == Some("k"))
            .collect();
        assert_eq!(k_tags.len(), 1, "exactly one k tag");
        assert_eq!(k_tags[0].get(1).map(String::as_str), Some("30443"));
        assert!(
            deletion.event.created_at >= current.created_at,
            "the request covers every publication in the slot"
        );
        let mut expected: Vec<String> = republish.relays.clone();
        expected.push(RELAY.to_owned());
        expected.sort();
        let mut got = deletion.relays.clone();
        got.sort();
        assert_eq!(got, expected, "the union write set carries the kind-5");

        // Every bundle is gone and the status reads empty.
        assert_eq!(
            key_package_bundle_count(&a.host.durable_rows()),
            0,
            "no KeyPackage bundles remain"
        );
        let status = a.session.key_package_status().expect("status");
        assert!(status.current.is_none() && !status.pending && status.retained == 0);
        assert!(status.next_due_ms.is_none());
        let error = a
            .session
            .ensure_key_package(&[RELAY.to_owned()])
            .await
            .expect_err("ensure after withdrawal");
        assert!(matches!(error, SessionError::SignedOut));

        // A repeat call skips the withdrawal: no second kind-5.
        a.session
            .sign_out(&[RELAY.to_owned()])
            .await
            .expect("idempotent sign_out");
        a.session.tick().await.expect("tick");
        assert!(
            take(&mut a).is_empty(),
            "nothing new is staged after withdrawal"
        );
    }

    /// A non-admin member signs out: the group gets a `SelfRemove` the
    /// admin commits; the leaver ends `Removed` and the admin's roster no
    /// longer lists the account.
    #[tokio::test(flavor = "current_thread")]
    async fn sign_out_sends_self_remove_to_member_group() {
        let relay = FakeRelay::new();
        let mut b = open_member().await;
        publish_identity(&mut b, &relay, RELAY).await;
        let mut a = open_member().await;
        publish_identity(&mut a, &relay, RELAY).await;
        let gid = group_with(&mut b, &relay, RELAY, &[&a], "sign-out").await;
        join_via_welcome(&mut a, &relay, RELAY).await;
        assert!(
            !a.session
                .group(&gid)
                .expect("group")
                .expect("snapshot")
                .own_account_admin,
            "a is a plain member"
        );

        let report = a
            .session
            .sign_out(&[RELAY.to_owned()])
            .await
            .expect("sign_out");
        assert_eq!(report.left, vec![gid.clone()]);
        assert_eq!(report.kept_admin_leaves, []);
        flush(&mut a, &relay).await;

        // The admin applies and commits the SelfRemove proposal.
        sync_member(&mut b, &relay, RELAY, &gid).await;
        sync_member(&mut b, &relay, RELAY, &gid).await;
        let b_snapshot = b.session.group(&gid).expect("group").expect("snapshot");
        assert!(
            !b_snapshot
                .members
                .iter()
                .any(|member| member.account == a.signer.account()),
            "the account is gone from the admin's roster"
        );

        // The leaver follows the commit to `Removed`.
        let backlog = group_events(&relay, RELAY, &a.route_hex(&gid));
        ingest_backlog(&mut a, &backlog).await;
        a.session.tick().await.expect("tick");
        flush(&mut a, &relay).await;
        let a_snapshot = a.session.group(&gid).expect("group").expect("snapshot");
        assert_eq!(a_snapshot.state, GroupState::Removed);
    }

    /// An admin leaf cannot `SelfRemove`: the group is reported in
    /// `kept_admin_leaves` and nothing is sent there.
    #[tokio::test(flavor = "current_thread")]
    async fn sign_out_keeps_admin_leaf() {
        let relay = FakeRelay::new();
        let mut a = open_member().await;
        publish_identity(&mut a, &relay, RELAY).await;
        let mut b = open_member().await;
        publish_identity(&mut b, &relay, RELAY).await;
        let gid = group_with(&mut a, &relay, RELAY, &[&b], "admin-kept").await;
        join_via_welcome(&mut b, &relay, RELAY).await;

        let report = a
            .session
            .sign_out(&[RELAY.to_owned()])
            .await
            .expect("sign_out");
        assert_eq!(report.kept_admin_leaves, vec![gid.clone()]);
        assert_eq!(report.left, []);
        let outbounds = take(&mut a);
        assert!(
            outbounds
                .iter()
                .all(|item| item.purpose == OutboundPurpose::KeyPackageDeletion),
            "nothing but the kind-5 is owed: {outbounds:?}"
        );

        // The member still sees the admin's account after a settle.
        sync_member(&mut b, &relay, RELAY, &gid).await;
        let b_snapshot = b.session.group(&gid).expect("group").expect("snapshot");
        assert!(
            b_snapshot
                .members
                .iter()
                .any(|member| member.account == a.signer.account()),
            "the admin leaf stays"
        );
    }

    /// A Welcome naming a deleted `KeyPackage` is ignored; the slot's
    /// kind-5 also removes the publication from selection.
    #[tokio::test(flavor = "current_thread")]
    async fn deleted_package_joins_nothing_and_selects_nothing() {
        let relay = FakeRelay::new();
        let mut a = open_member().await;
        publish_identity(&mut a, &relay, RELAY).await;
        let published = key_package_of(&relay, RELAY, &a.account_hex());

        let mut b = open_member().await;
        publish_identity(&mut b, &relay, RELAY).await;
        let _gid = group_with(&mut b, &relay, RELAY, &[&a], "deleted-kp").await;

        a.session
            .sign_out(&[RELAY.to_owned()])
            .await
            .expect("sign_out");
        let deletion = take(&mut a)
            .into_iter()
            .find(|item| item.purpose == OutboundPurpose::KeyPackageDeletion)
            .expect("kind-5");

        // The Welcome names a bundle the device no longer holds.
        let welcomes = welcomes_for(&relay, RELAY, &a.account_hex());
        let events = ingest_backlog(&mut a, &welcomes).await;
        assert!(
            !events
                .iter()
                .any(|event| matches!(event, SessionEvent::GroupJoined { .. })),
            "no join from a deleted KeyPackage"
        );
        assert_eq!(a.session.groups().expect("groups"), []);

        // Selection drops the slot's publications covered by its kind-5.
        let selected = a
            .session
            .select_key_packages(
                a.signer.account(),
                &[published.clone(), deletion.event.clone()],
                None,
            )
            .expect("select");
        assert!(selected.is_empty(), "the slot is deleted");
    }

    /// A kind-30443 event signed by `signer` — a publication of this
    /// account's slot the device did not make.
    async fn slot_event(
        signer: &Arc<dyn AccountSigner>,
        created_at: u64,
        slot: &str,
        kind: u16,
    ) -> Event {
        signer
            .sign_event(UnsignedEvent {
                pubkey: signer.account(),
                created_at,
                kind,
                tags: vec![vec!["d".to_owned(), slot.to_owned()]],
                content: String::new(),
            })
            .await
            .expect("sign")
    }

    /// The slot audit restages an outranking replacement only for
    /// foreign publications inside the skew window.
    #[tokio::test(flavor = "current_thread")]
    async fn audit_outranks_only_foreign_in_window() {
        let relay = FakeRelay::new();
        let mut a = open_member().await;
        let own = publish_promoted(&mut a, &relay).await;
        let slot = hex::encode(a.session.key_package_status().expect("status").slot);
        let now = a.clock.now().unix_ms / 1_000;
        let signer = Arc::clone(&a.signer);

        // A foreign publication inside the window is outranked: the
        // staged replacement's `created_at` is strictly newer.
        let foreign = slot_event(&signer, now + 10, &slot, 30_443).await;
        a.session
            .audit_key_package_slot(&[own.event.clone(), foreign.clone()])
            .await
            .expect("audit");
        let outbound = take(&mut a);
        assert_eq!(outbound.len(), 1, "the audit stages one replacement");
        assert!(
            outbound[0].event.created_at > foreign.created_at,
            "replacement postdates the foreign publication"
        );
        assert_eq!(d_tag(&outbound[0].event), slot);
        let current = outbound[0].event.clone();
        accept(&mut a, &relay, &outbound[0]).await;

        // Own current publication alone is a no-op. (`own` is the
        // superseded publication now — foreign by id — so it stays out.)
        a.session
            .audit_key_package_slot(&[current])
            .await
            .expect("audit own");
        assert!(take(&mut a).is_empty(), "own publication is ignored");

        // Wrong `d`, wrong author, bad signature, wrong kind: all ignored.
        let other_signer: Arc<dyn AccountSigner> = Arc::new(LocalSigner::new());
        let wrong_d = slot_event(&signer, now + 20, &"ff".repeat(32), 30_443).await;
        let wrong_author = slot_event(&other_signer, now + 20, &slot, 30_443).await;
        let mut bad_sig = slot_event(&signer, now + 20, &slot, 30_443).await;
        bad_sig.sig[0] ^= 0xff;
        let wrong_kind = slot_event(&signer, now + 20, &slot, 1).await;
        a.session
            .audit_key_package_slot(&[wrong_d, wrong_author, bad_sig, wrong_kind])
            .await
            .expect("audit ignored");
        assert!(take(&mut a).is_empty(), "non-slot events are ignored");

        // Beyond the skew window the foreign publication is ignored too.
        let far = slot_event(&signer, now + 301, &slot, 30_443).await;
        a.session
            .audit_key_package_slot(&[far])
            .await
            .expect("audit far");
        assert!(take(&mut a).is_empty(), "beyond-skew is ignored");

        // A pending replacement already newer needs no second stage —
        // a fresh member whose ensure-staged pending postdates the find.
        let mut b = open_member().await;
        b.session
            .ensure_key_package(&[RELAY.to_owned()])
            .await
            .expect("ensure b");
        let _ = take(&mut b);
        assert!(b.session.key_package_status().expect("status").pending);
        let b_slot = hex::encode(b.session.key_package_status().expect("status").slot);
        let older = slot_event(&Arc::clone(&b.signer), now - 100, &b_slot, 30_443).await;
        b.session
            .audit_key_package_slot(&[older])
            .await
            .expect("audit pending");
        assert!(
            take(&mut b).is_empty(),
            "a newer pending already outranks the finding"
        );
        assert_bundle_invariant(&b);
    }

    /// The audit refuses after the slot is withdrawn.
    #[tokio::test(flavor = "current_thread")]
    async fn audit_signed_out_slot_is_signed_out() {
        let relay = FakeRelay::new();
        let mut a = open_member().await;
        publish_promoted(&mut a, &relay).await;
        a.session
            .sign_out(&[RELAY.to_owned()])
            .await
            .expect("sign_out");
        let error = a
            .session
            .audit_key_package_slot(&[])
            .await
            .expect_err("audit after withdrawal");
        assert!(matches!(error, SessionError::SignedOut));
    }
}
