//! Agent text stream session behavior: the receive-role capability
//! advertisement, joining a group that requires `receive`, the decoded
//! policy snapshot, and the engine's refusal when a policy would require a
//! role we do not hold.

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
mod agent_stream_tests {
    use super::support::*;
    use mk_core::agent_stream::{AgentStreamPolicy, StreamRoles};
    use mk_core::{CreateGroup, GroupId, Invitee, SessionError};

    const RELAY: &str = "ws://127.0.0.1:7777";

    /// The values one named 30443 tag carries (`["tag", v1, v2, …]`).
    fn tag_values(event: &mk_core::Event, name: &str) -> Vec<String> {
        event
            .tags
            .iter()
            .find(|tag| tag.first().map(String::as_str) == Some(name))
            .map(|tag| tag[1..].to_vec())
            .unwrap_or_default()
    }

    /// Create a group with `components`, deliver the Welcomes.
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
                name: "stream".to_owned(),
                description: String::new(),
                relays: vec![RELAY.to_owned()],
                invitees: invites,
                admins: Vec::new(),
                components,
            })
            .await
            .expect("create_group");
        flush(creator, relay).await;
        gid
    }

    async fn join_via_welcome(member: &mut Member, relay: &FakeRelay) {
        let welcomes = welcomes_for(relay, RELAY, &member.account_hex());
        assert!(!welcomes.is_empty(), "a welcome must be waiting");
        ingest_backlog(member, &welcomes).await;
        member.session.tick().await.expect("tick");
        flush(member, relay).await;
    }

    /// The published KeyPackage advertises the receive extension `0xf2d1`
    /// and the new app components — and never the send/fanout extensions or
    /// media v1.
    #[tokio::test]
    async fn key_package_advertisement() {
        let relay = FakeRelay::new();
        let mut a = open_member().await;
        publish_identity(&mut a, &relay, RELAY).await;
        let kp = key_package_of(&relay, RELAY, &a.account_hex());

        let extensions = tag_values(&kp, "mls_extensions");
        assert!(extensions.contains(&"0xf2d1".to_owned()), "receive role");
        assert!(!extensions.contains(&"0xf2d2".to_owned()), "no send");
        assert!(!extensions.contains(&"0xf2d4".to_owned()), "no fanout");

        let proposals = tag_values(&kp, "mls_proposals");
        assert!(proposals.contains(&"0x0008".to_owned()));
        assert!(proposals.contains(&"0x000a".to_owned()));

        let components = tag_values(&kp, "app_components");
        for id in [
            "0x8001", "0x8003", "0x8004", "0x8006", "0x8009", "0x800b", "0x800c", "0x800d",
        ] {
            assert!(components.contains(&id.to_owned()), "missing {id}");
        }
        assert!(!components.contains(&"0x8008".to_owned()), "no media v1");
        assert_eq!(components.len(), 8, "exact component set");
    }

    /// A group whose `0x8006` policy requires the receive role accepts our
    /// KeyPackage; both sides see the decoded policy in the snapshot.
    #[tokio::test]
    async fn required_receive_group_accepts_us() {
        let relay = FakeRelay::new();
        let mut a = open_member().await;
        let mut b = open_member().await;
        publish_identity(&mut a, &relay, RELAY).await;
        publish_identity(&mut b, &relay, RELAY).await;

        let policy = AgentStreamPolicy::user_to_agent_default();
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
            let decoded = snapshot.agent_text_stream.expect("policy");
            assert_eq!(decoded.required_roles, StreamRoles::RECEIVE);
            assert!(decoded.allowed_roles.contains(StreamRoles::RECEIVE));
            assert!(decoded.allowed_roles.contains(StreamRoles::SEND));
            assert!(!decoded.required_roles.contains(StreamRoles::SEND));
        }
    }

    /// A group that requires a role we do not hold refuses the invitation:
    /// every member must advertise each `required_member_roles` capability,
    /// and our KeyPackage carries only `0xf2d1`. The refusal is the engine's
    /// missing-capabilities error mapped to `SessionError`, nothing is
    /// staged, and no Welcome reaches B.
    #[tokio::test]
    async fn requiring_unheld_role_is_refused() {
        let relay = FakeRelay::new();
        let mut a = open_member().await;
        let mut b = open_member().await;
        publish_identity(&mut a, &relay, RELAY).await;
        publish_identity(&mut b, &relay, RELAY).await;

        let send_and_receive = AgentStreamPolicy {
            required_roles: StreamRoles::RECEIVE.union(StreamRoles::SEND),
            allowed_roles: StreamRoles::RECEIVE.union(StreamRoles::SEND),
            ..AgentStreamPolicy::user_to_agent_default()
        };
        let err = {
            let invites = vec![Invitee {
                key_package: key_package_of(&relay, RELAY, &b.account_hex()),
                inbox_relays: inbox_relays_of(&relay, RELAY, &b.account_hex()),
            }];
            a.session
                .create_group(CreateGroup {
                    name: "stream".to_owned(),
                    description: String::new(),
                    relays: vec![RELAY.to_owned()],
                    invitees: invites,
                    admins: Vec::new(),
                    components: vec![send_and_receive.to_component().expect("component")],
                })
                .await
                .expect_err("must refuse")
        };
        assert!(
            matches!(err, SessionError::MissingRequiredCapabilities),
            "expected a capability refusal, got {err:?}"
        );
        assert_eq!(a.session.take_outbound().expect("outbound").len(), 0);
        assert!(
            welcomes_for(&relay, RELAY, &b.account_hex()).is_empty(),
            "no Welcome may reach a member that lacks a required role"
        );
    }

    /// Tightening `required_member_roles` mid-group would leave every
    /// resulting-epoch member short of a required role — the spec's
    /// invariant applies to commits too, so the session refuses before the
    /// engine: `MissingRequiredCapabilities`, no staged commit, no epoch
    /// move, and the snapshot still decodes the receive-only policy. (The
    /// pinned engine stages the same update; `upstream.rs` pins that.)
    #[tokio::test]
    async fn update_requiring_unheld_role_is_refused() {
        let relay = FakeRelay::new();
        let mut a = open_member().await;
        let mut b = open_member().await;
        publish_identity(&mut a, &relay, RELAY).await;
        publish_identity(&mut b, &relay, RELAY).await;
        let gid = group_with(
            &mut a,
            &relay,
            &[&b],
            vec![
                AgentStreamPolicy::user_to_agent_default()
                    .to_component()
                    .expect("component"),
            ],
        )
        .await;
        join_via_welcome(&mut b, &relay).await;
        let epochs: Vec<u64> = [&a, &b]
            .iter()
            .map(|m| m.session.group(&gid).expect("group").expect("exists").epoch)
            .collect();

        let send_and_receive = AgentStreamPolicy {
            required_roles: StreamRoles::RECEIVE.union(StreamRoles::SEND),
            allowed_roles: StreamRoles::RECEIVE.union(StreamRoles::SEND),
            ..AgentStreamPolicy::user_to_agent_default()
        };
        let err = a
            .session
            .update_app_components(
                &gid,
                vec![send_and_receive.to_component().expect("component")],
            )
            .await
            .expect_err("must refuse");
        assert!(
            matches!(err, SessionError::MissingRequiredCapabilities),
            "expected a capability refusal, got {err:?}"
        );
        assert_eq!(a.session.take_outbound().expect("outbound").len(), 0);
        for (member, epoch) in [&a, &b].iter().zip(epochs) {
            let snapshot = member.session.group(&gid).expect("group").expect("exists");
            assert_eq!(snapshot.epoch, epoch, "epoch must not move");
            let policy = snapshot.agent_text_stream.expect("policy");
            assert_eq!(policy.required_roles, StreamRoles::RECEIVE);
        }
    }

    /// Even a solo `create_group` (no invitees to trigger the engine's
    /// KeyPackage check) refuses a policy requiring an unheld role — the
    /// own leaf is always in the resulting epoch and advertises receive
    /// only.
    #[tokio::test]
    async fn solo_create_requiring_unheld_role_is_refused() {
        let relay = FakeRelay::new();
        let mut a = open_member().await;
        publish_identity(&mut a, &relay, RELAY).await;
        let send_and_receive = AgentStreamPolicy {
            required_roles: StreamRoles::RECEIVE.union(StreamRoles::SEND),
            allowed_roles: StreamRoles::RECEIVE.union(StreamRoles::SEND),
            ..AgentStreamPolicy::user_to_agent_default()
        };
        let err = a
            .session
            .create_group(CreateGroup {
                name: "stream".to_owned(),
                description: String::new(),
                relays: vec![RELAY.to_owned()],
                invitees: Vec::new(),
                admins: Vec::new(),
                components: vec![send_and_receive.to_component().expect("component")],
            })
            .await
            .expect_err("must refuse");
        assert!(
            matches!(err, SessionError::MissingRequiredCapabilities),
            "expected a capability refusal, got {err:?}"
        );
        assert_eq!(a.session.take_outbound().expect("outbound").len(), 0);
    }

    /// Receiving produces nothing on the wire: after B drains A's messages,
    /// `take_outbound` stays empty (receive role = pure delivery).
    #[tokio::test]
    async fn receive_produces_no_outbound() {
        let relay = FakeRelay::new();
        let mut a = open_member().await;
        let mut b = open_member().await;
        publish_identity(&mut a, &relay, RELAY).await;
        publish_identity(&mut b, &relay, RELAY).await;
        let gid = group_with(
            &mut a,
            &relay,
            &[&b],
            vec![
                AgentStreamPolicy::user_to_agent_default()
                    .to_component()
                    .expect("component"),
            ],
        )
        .await;
        join_via_welcome(&mut b, &relay).await;
        send_chat(&mut a, &relay, &gid, "hello").await;
        let events = sync_member(&mut b, &relay, RELAY, &gid).await;
        assert!(received(&events).contains(&"hello".to_owned()));
        assert_eq!(b.session.take_outbound().expect("outbound").len(), 0);
    }
}
