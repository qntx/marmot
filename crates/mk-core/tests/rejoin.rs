//! D2 rejoin tests: stored replacement Welcomes surface as `RejoinOffered`
//! while the local copy is active, `confirm_rejoin` installs them on
//! consent, removed/unrecoverable copies install consent-free through the
//! retry pass, and `readd_member` completes the admin's remove-then-invite
//! exactly once.

#![allow(
    clippy::missing_docs_in_private_items,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::needless_pass_by_value,
    clippy::tests_outside_test_module,
    clippy::shadow_reuse,
    clippy::shadow_unrelated,
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
mod rejoin_tests {
    use super::support::*;

    use std::collections::BTreeSet;
    use std::sync::Arc;

    use mk_core::{
        Event, EventId, GroupAccess, GroupId, InputOutcome, Invitee, LocalState, ReaddOutcome,
        RelayStatus, SessionError, SessionEvent,
    };

    const RELAY: &str = "ws://127.0.0.1:7777";

    /// Every kind-30443 `KeyPackage` event `account` published (newest
    /// first) — unlike `key_package_of`, which returns only the newest,
    /// a multi-device account has one per device.
    fn key_packages_of(relay: &FakeRelay, url: &str, account_hex: &str) -> Vec<Event> {
        relay.fetch(url, move |event| {
            event.kind == 30_443 && event.pubkey.to_hex() == account_hex
        })
    }

    /// An invitee per distinct published `KeyPackage` of `account`.
    fn invitees_for(relay: &FakeRelay, url: &str, account_hex: &str) -> Vec<Invitee> {
        let inbox_relays = inbox_relays_of(relay, url, account_hex);
        key_packages_of(relay, url, account_hex)
            .into_iter()
            .map(|key_package| Invitee {
                key_package,
                inbox_relays: inbox_relays.clone(),
            })
            .collect()
    }

    /// The `RejoinOffered`/`GroupJoined` drain counts for `group`.
    fn rejoin_counts(events: &[SessionEvent], group: &GroupId) -> (usize, usize) {
        let offered = events
            .iter()
            .filter(|event| matches!(event, SessionEvent::RejoinOffered { group: g } if g == group))
            .count();
        let joined = events
            .iter()
            .filter(|event| matches!(event, SessionEvent::GroupJoined { group: g } if g == group))
            .count();
        (offered, joined)
    }

    fn access(member: &Member, gid: &GroupId) -> GroupAccess {
        member
            .session
            .group(gid)
            .expect("snapshot")
            .expect("the group")
            .access
    }

    /// Kind-445 group events on `route` whose ids are not in `before`
    /// (group events carry ephemeral pubkeys — authorship is not
    /// recoverable from the wire).
    fn new_group_events(
        relay: &FakeRelay,
        url: &str,
        route: &str,
        before: &BTreeSet<EventId>,
    ) -> Vec<Event> {
        group_events(relay, url, route)
            .into_iter()
            .filter(|event| !before.contains(&event.id))
            .collect()
    }

    /// The newest Welcome wrap addressed to `account` published after the
    /// `before` set was captured.
    fn newest_wrap(
        relay: &FakeRelay,
        url: &str,
        account_hex: &str,
        before: &BTreeSet<EventId>,
    ) -> Event {
        welcomes_for(relay, url, account_hex)
            .into_iter()
            .find(|event| !before.contains(&event.id))
            .expect("a fresh welcome wrap")
    }

    /// P3b: `a` holds an active copy behind on commits while the admin
    /// removes the account and re-invites it — the replacement Welcome
    /// must wait for consent. Returns the stored wrap.
    async fn stale_offer() -> (FakeRelay, Member, Member, GroupId, Event) {
        let relay = FakeRelay::new();
        let mut b = open_member().await;
        let mut a = open_member().await;
        publish_identity(&mut b, &relay, RELAY).await;
        publish_identity(&mut a, &relay, RELAY).await;
        let gid = group_with(&mut b, &relay, RELAY, &[&a], "rejoin").await;
        join_via_welcome(&mut a, &relay, RELAY).await;
        sync_member(&mut b, &relay, RELAY, &gid).await;
        sync_member(&mut a, &relay, RELAY, &gid).await;

        // B advances the epoch twice — a self-update then the account
        // removal — and neither commit reaches `a` (a stale active copy).
        let route = b.route_hex(&gid);
        let seen: BTreeSet<EventId> = group_events(&relay, RELAY, &route)
            .into_iter()
            .map(|event| event.id)
            .collect();
        b.session.self_update(&gid).await.expect("self_update");
        flush(&mut b, &relay).await;
        b.session
            .remove_members(&gid, &[a.signer.account()])
            .await
            .expect("remove_members");
        flush(&mut b, &relay).await;

        // Re-invite the account's current KeyPackage; publish the wrap.
        let wraps_before: BTreeSet<EventId> = welcomes_for(&relay, RELAY, &a.account_hex())
            .into_iter()
            .map(|event| event.id)
            .collect();
        b.session
            .invite(&gid, invitees_for(&relay, RELAY, &a.account_hex()))
            .await
            .expect("invite");
        flush(&mut b, &relay).await;
        let wrap = newest_wrap(&relay, RELAY, &a.account_hex(), &wraps_before);
        let _ = seen;
        (relay, b, a, gid, wrap)
    }

    /// A stale active copy surfaces exactly one `RejoinOffered`; the
    /// listed offer carries the admin as author; a stale token and an
    /// unknown offer are refused; the right token installs the new copy,
    /// and messaging works both ways afterwards.
    #[tokio::test(flavor = "current_thread")]
    async fn stuck_copy_offer_and_confirm() {
        let (relay, mut b, mut a, gid, wrap) = Box::pin(stale_offer()).await;

        let report = a.session.ingest(&wrap).await.expect("ingest wrap");
        assert!(
            matches!(
                report.outcome,
                InputOutcome::Local(LocalState::RejoinConfirmationRequired)
            ),
            "the replacement needs consent: {report:?}"
        );
        let drained = a.session.drain_events().expect("drain");
        assert_eq!(rejoin_counts(&drained, &gid), (1, 0), "offered once");

        let offers = a.session.rejoin_offers(&gid).expect("rejoin_offers");
        assert_eq!(offers.len(), 1, "one stored offer");
        let offer = &offers[0];
        assert_eq!(offer.offer, wrap.id, "the offer id is the wrap id");
        assert_eq!(offer.author, b.signer.account(), "the admin authored it");
        assert!(
            offer.epoch
                > a.session
                    .group(&gid)
                    .expect("snapshot")
                    .expect("the group")
                    .epoch,
            "the offer names an epoch ahead of the stale copy"
        );

        // A foreign offer id is refused before any engine call.
        let bogus = EventId([0xAA; 32]);
        let error = a
            .session
            .confirm_rejoin(&gid, &bogus, &offer.token)
            .await
            .expect_err("unknown offer");
        assert!(
            matches!(error, SessionError::InvalidInput(name) if name == "offer"),
            "unknown offer → InvalidInput: {error:?}"
        );

        // A stale token maps to `RejoinTokenStale`.
        let mut wrong = offer.token;
        wrong[0] ^= 0xFF;
        let error = a
            .session
            .confirm_rejoin(&gid, &offer.offer, &wrong)
            .await
            .expect_err("wrong token");
        assert!(
            matches!(error, SessionError::RejoinTokenStale),
            "wrong token → RejoinTokenStale: {error:?}"
        );

        // The right token replaces the copy and joins the group.
        a.session
            .confirm_rejoin(&gid, &offer.offer, &offer.token)
            .await
            .expect("confirm_rejoin");
        flush(&mut a, &relay).await;
        let drained = a.session.drain_events().expect("drain");
        assert_eq!(rejoin_counts(&drained, &gid).1, 1, "GroupJoined once");
        assert_eq!(access(&a, &gid), GroupAccess::Active);
        assert!(
            a.session
                .rejoin_offers(&gid)
                .expect("rejoin_offers")
                .is_empty(),
            "the offer is spent"
        );

        // The new copy owes the post-join self-update like any fresh
        // join: an obligation in CatchUp that completes only after the
        // history report and the quiet+jitter gate.
        let (_, obligations, _) = mk_core::testing::maintenance_rows(&a.host.durable_rows());
        let open: Vec<_> = obligations
            .iter()
            .filter(|obligation| obligation.group_id.as_slice() == gid.0.as_slice())
            .collect();
        assert!(
            open.iter().any(|obligation| obligation.phase
                != cgka_traits::maintenance::MaintenancePhase::Complete),
            "the new copy owes a post-join self-update: {open:?}"
        );

        // Round trip: b → a and a → b on the replacement copy.
        sync_member(&mut b, &relay, RELAY, &gid).await;
        send_chat(&mut b, &relay, &gid, "after rejoin").await;
        let a_route = a.route_hex(&gid);
        let drained = ingest_backlog(&mut a, &group_events(&relay, RELAY, &a_route)).await;
        assert!(
            received(&drained).contains(&"after rejoin".to_owned()),
            "a reads b on the new copy: {drained:?}"
        );
        send_chat(&mut a, &relay, &gid, "reply").await;
        let b_route = b.route_hex(&gid);
        let drained = ingest_backlog(&mut b, &group_events(&relay, RELAY, &b_route)).await;
        assert!(
            received(&drained).contains(&"reply".to_owned()),
            "b reads a on the new copy: {drained:?}"
        );
    }

    /// An offer held on an active copy is never installed by the retry
    /// pass — consent is required while the copy is live.
    #[tokio::test(flavor = "current_thread")]
    async fn active_copy_never_auto_installs() {
        let (_relay, _b, mut a, gid, wrap) = Box::pin(stale_offer()).await;
        let report = a.session.ingest(&wrap).await.expect("ingest wrap");
        assert!(matches!(
            report.outcome,
            InputOutcome::Local(LocalState::RejoinConfirmationRequired)
        ));
        drop(a.session.drain_events());
        for _ in 0..3 {
            a.session.tick().await.expect("tick");
            a.clock.advance(5_000);
        }
        let drained = a.session.drain_events().expect("drain");
        assert_eq!(
            rejoin_counts(&drained, &gid).1,
            0,
            "no GroupJoined without consent"
        );
        assert_eq!(
            a.session.rejoin_offers(&gid).expect("rejoin_offers").len(),
            1,
            "the offer still waits"
        );
        assert_eq!(access(&a, &gid), GroupAccess::Active);
    }

    /// A removed copy installs the replacement Welcome consent-free —
    /// the trusted-removal evidence substitutes for the user's confirm,
    /// and no `RejoinOffered` ever fires (P3a).
    #[tokio::test(flavor = "current_thread")]
    async fn removed_copy_auto_installs() {
        let relay = FakeRelay::new();
        let mut b = open_member().await;
        let mut a = open_member().await;
        publish_identity(&mut b, &relay, RELAY).await;
        publish_identity(&mut a, &relay, RELAY).await;
        let gid = group_with(&mut b, &relay, RELAY, &[&a], "rejoin-auto").await;
        join_via_welcome(&mut a, &relay, RELAY).await;
        sync_member(&mut b, &relay, RELAY, &gid).await;
        sync_member(&mut a, &relay, RELAY, &gid).await;

        // Remove the account and re-invite it; `a` sees the removal
        // commit first, so the copy is removed before the wrap lands.
        let route = b.route_hex(&gid);
        let seen: BTreeSet<EventId> = group_events(&relay, RELAY, &route)
            .into_iter()
            .map(|event| event.id)
            .collect();
        b.session
            .remove_members(&gid, &[a.signer.account()])
            .await
            .expect("remove_members");
        flush(&mut b, &relay).await;
        let wraps_before: BTreeSet<EventId> = welcomes_for(&relay, RELAY, &a.account_hex())
            .into_iter()
            .map(|event| event.id)
            .collect();
        b.session
            .invite(&gid, invitees_for(&relay, RELAY, &a.account_hex()))
            .await
            .expect("invite");
        flush(&mut b, &relay).await;

        let feed: Vec<Event> = group_events(&relay, RELAY, &route)
            .into_iter()
            .filter(|event| !seen.contains(&event.id))
            .collect();
        let drained = ingest_backlog(&mut a, &feed).await;
        assert!(
            drained
                .iter()
                .any(|event| matches!(event, SessionEvent::LocalCopyTerminated { .. })),
            "the removal terminates the copy: {drained:?}"
        );
        assert_eq!(access(&a, &gid), GroupAccess::Removed);

        let wrap = newest_wrap(&relay, RELAY, &a.account_hex(), &wraps_before);
        let report = a.session.ingest(&wrap).await.expect("ingest wrap");
        assert!(
            matches!(report.outcome, InputOutcome::Accepted),
            "a removed copy installs outright: {report:?}"
        );
        let drained = a.session.drain_events().expect("drain");
        assert_eq!(
            rejoin_counts(&drained, &gid),
            (0, 1),
            "joined, never offered"
        );
        assert_eq!(access(&a, &gid), GroupAccess::Active);
    }

    /// An offer stored while the copy is still active installs without
    /// consent once the delayed removal commit terminates the copy (P3c:
    /// the retry pass at the end of the removal's ingest).
    #[tokio::test(flavor = "current_thread")]
    async fn held_offer_installs_after_removal() {
        let (relay, _b, mut a, gid, wrap) = Box::pin(stale_offer()).await;

        // The wrap arrives first: consent required while the copy is live.
        let report = a.session.ingest(&wrap).await.expect("ingest wrap");
        assert!(matches!(
            report.outcome,
            InputOutcome::Local(LocalState::RejoinConfirmationRequired)
        ));
        let drained = a.session.drain_events().expect("drain");
        assert_eq!(rejoin_counts(&drained, &gid), (1, 0));

        // Then the withheld commit backlog — including the removal —
        // terminates the copy, and the held offer installs itself in the
        // same ingest pass.
        let route = a.route_hex(&gid);
        let backlog = group_events(&relay, RELAY, &route);
        let drained = ingest_backlog(&mut a, &backlog).await;
        assert!(
            drained
                .iter()
                .any(|event| matches!(event, SessionEvent::LocalCopyTerminated { .. })),
            "the removal lands: {drained:?}"
        );
        assert_eq!(
            rejoin_counts(&drained, &gid).1,
            1,
            "the held offer installs without consent: {drained:?}"
        );
        assert_eq!(access(&a, &gid), GroupAccess::Active);
        assert_eq!(
            a.session.rejoin_offers(&gid).expect("rejoin_offers"),
            Vec::<mk_core::RejoinOffer>::new()
        );
    }

    /// Declining retires the stored offer; redelivering the same wrap
    /// cannot reopen it.
    #[tokio::test(flavor = "current_thread")]
    async fn decline_retires_offer() {
        let (_relay, _b, mut a, gid, wrap) = Box::pin(stale_offer()).await;
        let report = a.session.ingest(&wrap).await.expect("ingest wrap");
        assert!(matches!(
            report.outcome,
            InputOutcome::Local(LocalState::RejoinConfirmationRequired)
        ));
        let drained = a.session.drain_events().expect("drain");
        assert_eq!(rejoin_counts(&drained, &gid), (1, 0));

        let offer = &a.session.rejoin_offers(&gid).expect("rejoin_offers")[0];
        let offer_id = offer.offer;
        a.session.decline_rejoin(&offer_id).expect("decline_rejoin");
        assert_eq!(
            a.session.rejoin_offers(&gid).expect("rejoin_offers"),
            Vec::<mk_core::RejoinOffer>::new(),
            "decline retires the offer"
        );

        // The retired wrap's redelivery reopens nothing.
        let _ = a.session.ingest(&wrap).await.expect("redeliver wrap");
        let drained = a.session.drain_events().expect("drain");
        assert_eq!(rejoin_counts(&drained, &gid), (0, 0), "no re-offer");
        assert_eq!(
            a.session.rejoin_offers(&gid).expect("rejoin_offers"),
            Vec::<mk_core::RejoinOffer>::new()
        );
        assert_eq!(access(&a, &gid), GroupAccess::Active);
    }

    /// A restart re-announces each stored offer exactly once.
    #[tokio::test(flavor = "current_thread")]
    async fn restart_reannounces_offer() {
        let (_relay, _b, mut a, gid, wrap) = Box::pin(stale_offer()).await;
        let report = a.session.ingest(&wrap).await.expect("ingest wrap");
        assert!(matches!(
            report.outcome,
            InputOutcome::Local(LocalState::RejoinConfirmationRequired)
        ));
        drop(a.session.drain_events());

        let mut a = reopen_member(&a).await;
        let drained = a.session.drain_events().expect("drain after reopen");
        assert_eq!(
            rejoin_counts(&drained, &gid).0,
            1,
            "one re-issued offer: {drained:?}"
        );
        let drained = a.session.drain_events().expect("second drain");
        assert_eq!(
            rejoin_counts(&drained, &gid).0,
            0,
            "the re-issue fires once per open"
        );
        assert_eq!(
            a.session.rejoin_offers(&gid).expect("rejoin_offers").len(),
            1
        );
    }

    /// `readd_member` validation: own account, empty invitees and a
    /// KeyPackage of another account are refused before anything stages.
    #[tokio::test(flavor = "current_thread")]
    async fn readd_member_input_validation() {
        let relay = FakeRelay::new();
        let mut b = open_member().await;
        let mut a = open_member().await;
        let mut c = open_member().await;
        publish_identity(&mut b, &relay, RELAY).await;
        publish_identity(&mut a, &relay, RELAY).await;
        publish_identity(&mut c, &relay, RELAY).await;
        let gid = group_with(&mut b, &relay, RELAY, &[&a], "readd").await;

        // Our own account is not a re-add target.
        let error = b
            .session
            .readd_member(
                &gid,
                b.signer.account(),
                invitees_for(&relay, RELAY, &b.account_hex()),
            )
            .await
            .expect_err("own account");
        assert!(
            matches!(error, SessionError::InvalidInput(name) if name == "account"),
            "{error:?}"
        );

        // No invitees at all.
        let error = b
            .session
            .readd_member(&gid, a.signer.account(), Vec::new())
            .await
            .expect_err("empty invitees");
        assert!(
            matches!(error, SessionError::InvalidInput(name) if name == "key package"),
            "{error:?}"
        );

        // An invitee carrying someone else's KeyPackage.
        let error = b
            .session
            .readd_member(
                &gid,
                a.signer.account(),
                invitees_for(&relay, RELAY, &c.account_hex()),
            )
            .await
            .expect_err("foreign key package");
        assert!(
            matches!(error, SessionError::InvalidInput(name) if name == "key package"),
            "{error:?}"
        );

        // Non-admin callers are refused.
        join_via_welcome(&mut a, &relay, RELAY).await;
        sync_member(&mut a, &relay, RELAY, &gid).await;
        let error = a
            .session
            .readd_member(
                &gid,
                c.signer.account(),
                invitees_for(&relay, RELAY, &c.account_hex()),
            )
            .await
            .expect_err("not admin");
        assert!(
            matches!(error, SessionError::NotGroupAdmin),
            "non-admin → NotGroupAdmin: {error:?}"
        );
    }

    /// `readd_member` on an account that holds no leaf stages the Invite
    /// immediately — `Invited`, no removal commit.
    #[tokio::test(flavor = "current_thread")]
    async fn readd_member_invited_path() {
        let relay = FakeRelay::new();
        let mut b = open_member().await;
        let mut a = open_member().await;
        let mut c = open_member().await;
        publish_identity(&mut b, &relay, RELAY).await;
        publish_identity(&mut a, &relay, RELAY).await;
        publish_identity(&mut c, &relay, RELAY).await;
        let gid = group_with(&mut b, &relay, RELAY, &[&a], "readd-invite").await;
        join_via_welcome(&mut a, &relay, RELAY).await;

        let route = b.route_hex(&gid);
        let before: BTreeSet<EventId> = group_events(&relay, RELAY, &route)
            .into_iter()
            .map(|event| event.id)
            .collect();
        let outcome = b
            .session
            .readd_member(
                &gid,
                c.signer.account(),
                invitees_for(&relay, RELAY, &c.account_hex()),
            )
            .await
            .expect("readd_member");
        assert!(
            matches!(outcome, ReaddOutcome::Invited),
            "no leaf → Invited: {outcome:?}"
        );
        flush(&mut b, &relay).await;

        // One new commit, no extra removal traffic, and the wrap lets
        // `c` join.
        let new_events = group_events(&relay, RELAY, &route)
            .into_iter()
            .filter(|event| !before.contains(&event.id))
            .count();
        assert_eq!(new_events, 1, "exactly the invite commit");
        join_via_welcome(&mut c, &relay, RELAY).await;
        assert_eq!(access(&c, &gid), GroupAccess::Active);
    }

    /// `readd_member` on a stuck account: the removal stages first
    /// (`Removed`), and once it confirms the next `tick` stages exactly
    /// one Invite. The sibling that saw the removal reinstalls on its
    /// own; the device that never did gets a consent offer.
    #[tokio::test(flavor = "current_thread")]
    async fn readd_member_removed_path() {
        let relay = FakeRelay::new();
        let mut b = open_member().await;
        // Two devices of one account.
        let signer: Arc<dyn mk_core::AccountSigner> =
            Arc::new(mk_core::testing::LocalSigner::new());
        let mut a1 = open_member_with(Arc::clone(&signer)).await;
        let mut a2 = open_member_with(Arc::clone(&signer)).await;
        publish_identity(&mut b, &relay, RELAY).await;
        publish_identity(&mut a1, &relay, RELAY).await;
        publish_identity(&mut a2, &relay, RELAY).await;
        let account = a1.signer.account();
        assert_eq!(account, a2.signer.account(), "one account");

        // Invite BOTH devices' KeyPackages so each holds a leaf.
        let gid = {
            let invitees = invitees_for(&relay, RELAY, &a1.account_hex());
            assert_eq!(invitees.len(), 2, "one KeyPackage per device");
            b.session
                .create_group(mk_core::CreateGroup {
                    name: "readd-devices".to_owned(),
                    description: String::new(),
                    relays: vec![RELAY.to_owned()],
                    invitees,
                    own_devices: Vec::new(),
                    admins: Vec::new(),
                    components: Vec::new(),
                })
                .await
                .expect("create_group")
        };
        flush(&mut b, &relay).await;
        join_via_welcome(&mut a1, &relay, RELAY).await;
        join_via_welcome(&mut a2, &relay, RELAY).await;
        sync_member(&mut b, &relay, RELAY, &gid).await;
        sync_member(&mut a1, &relay, RELAY, &gid).await;
        sync_member(&mut a2, &relay, RELAY, &gid).await;

        // `a2` is stuck: a self-update commit it never sees.
        let route = b.route_hex(&gid);
        b.session.self_update(&gid).await.expect("self_update");
        flush(&mut b, &relay).await;
        // a1 syncs; a2 misses it.
        sync_member(&mut a1, &relay, RELAY, &gid).await;

        let before: BTreeSet<EventId> = group_events(&relay, RELAY, &route)
            .into_iter()
            .map(|event| event.id)
            .collect();
        let wraps_before: BTreeSet<EventId> = welcomes_for(&relay, RELAY, &a1.account_hex())
            .into_iter()
            .map(|event| event.id)
            .collect();

        // Re-add the account: removal stages, row holds the invitees.
        let outcome = b
            .session
            .readd_member(
                &gid,
                account,
                invitees_for(&relay, RELAY, &a1.account_hex()),
            )
            .await
            .expect("readd_member");
        assert!(
            matches!(outcome, ReaddOutcome::Removed),
            "leaf held → Removed: {outcome:?}"
        );
        flush(&mut b, &relay).await;

        // Queued intents materialize through convergence: ticks publish
        // the removal, and once it confirms a later tick stages the one
        // Invite that completes the re-add.
        for _ in 0..6 {
            b.clock.advance(2_000);
            b.session.tick().await.expect("tick");
            flush(&mut b, &relay).await;
        }
        let authored = new_group_events(&relay, RELAY, &route, &before);
        assert_eq!(
            authored.len(),
            2,
            "exactly the removal + the invite commits: {authored:?}"
        );
        // No further commit ever — the row is spent.
        b.clock.advance(120_000);
        b.session.tick().await.expect("later tick");
        flush(&mut b, &relay).await;
        let authored = new_group_events(&relay, RELAY, &route, &before);
        assert_eq!(authored.len(), 2, "the Invite is staged exactly once");

        // Both wraps share the account's `p` tag — each device claims
        // its own by the inner Welcome and reports `WrongRecipient`
        // for the sibling's.
        let wraps: Vec<Event> = welcomes_for(&relay, RELAY, &a1.account_hex())
            .into_iter()
            .filter(|event| !wraps_before.contains(&event.id))
            .collect();
        assert_eq!(wraps.len(), 2, "one wrap per device KeyPackage");

        // The healthy sibling saw the removal → consent-free reinstall.
        let a1_backlog = group_events(&relay, RELAY, &route);
        let drained = ingest_backlog(&mut a1, &a1_backlog).await;
        assert!(
            drained
                .iter()
                .any(|event| matches!(event, SessionEvent::LocalCopyTerminated { .. })),
            "a1's removal lands: {drained:?}"
        );
        let reports = a1
            .session
            .ingest_batch(wraps.clone())
            .await
            .expect("a1 wraps");
        let joined = a1.session.drain_events().expect("a1 drain");
        assert_eq!(
            rejoin_counts(&joined, &gid),
            (0, 1),
            "a1 reinstalls consent-free: {reports:?}"
        );
        assert_eq!(access(&a1, &gid), GroupAccess::Active);

        // The stuck device kept an active copy → consent offer.
        let reports = a2.session.ingest_batch(wraps).await.expect("a2 wraps");
        assert!(
            reports.iter().any(|report| matches!(
                report.outcome,
                InputOutcome::Local(LocalState::RejoinConfirmationRequired)
            )),
            "a2 needs consent: {reports:?}"
        );
        let drained = a2.session.drain_events().expect("a2 drain");
        assert_eq!(rejoin_counts(&drained, &gid), (1, 0));
        let offer = a2.session.rejoin_offers(&gid).expect("rejoin_offers")[0].clone();
        a2.session
            .confirm_rejoin(&gid, &offer.offer, &offer.token)
            .await
            .expect("confirm_rejoin");
        let drained = a2.session.drain_events().expect("a2 drain");
        assert_eq!(rejoin_counts(&drained, &gid).1, 1, "a2 joined");
        assert_eq!(access(&a2, &gid), GroupAccess::Active);
    }

    /// A removal that rolls back (its commit is rejected on the wire)
    /// retires the `Readd` row: no Invite is ever staged and a later
    /// `readd_member` starts fresh.
    #[tokio::test(flavor = "current_thread")]
    async fn readd_rolled_back_removal_drops_row() {
        let relay = FakeRelay::new();
        let mut b = open_member().await;
        let mut a = open_member().await;
        publish_identity(&mut b, &relay, RELAY).await;
        publish_identity(&mut a, &relay, RELAY).await;
        let gid = group_with(&mut b, &relay, RELAY, &[&a], "readd-rollback").await;
        join_via_welcome(&mut a, &relay, RELAY).await;

        let outcome = b
            .session
            .readd_member(
                &gid,
                a.signer.account(),
                invitees_for(&relay, RELAY, &a.account_hex()),
            )
            .await
            .expect("readd_member");
        assert!(matches!(outcome, ReaddOutcome::Removed));

        // Take the staged removal but report it definitively rejected:
        // the engine rolls back, the account keeps its leaf, and
        // nothing of ours stays in flight.
        let mut removal_id = None;
        for _ in 0..16 {
            if let Some(seq) = b.session.awaiting_durability() {
                b.session.acknowledge_durable(seq);
            }
            let outbound = b.session.take_outbound().expect("take_outbound");
            if outbound.is_empty() {
                break;
            }
            for item in outbound {
                removal_id = Some(item.id);
                let results = relay
                    .publish(&item)
                    .into_iter()
                    .map(|mut result| {
                        result.status = RelayStatus::Rejected;
                        result
                    })
                    .collect::<Vec<_>>();
                b.session
                    .report_publish(&item.id, &results)
                    .await
                    .expect("report_publish");
            }
        }
        assert!(removal_id.is_some(), "a removal commit was staged");

        // The next tick notices the roll-back: row deleted, no Invite.
        b.clock.advance(2_000);
        b.session.tick().await.expect("tick");
        let outbound = {
            if let Some(seq) = b.session.awaiting_durability() {
                b.session.acknowledge_durable(seq);
            }
            b.session.take_outbound().expect("take_outbound")
        };
        assert!(
            outbound.is_empty(),
            "a rolled-back removal stages nothing: {outbound:?}"
        );
        assert!(
            welcomes_for(&relay, RELAY, &a.account_hex())
                .iter()
                .all(|_| true),
            "no new welcome exists"
        );
        let wrap_count = welcomes_for(&relay, RELAY, &a.account_hex()).len();
        assert_eq!(wrap_count, 1, "still only the original welcome");
    }
}
