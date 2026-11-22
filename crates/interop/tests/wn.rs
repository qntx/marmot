//! White Noise (`wn`/`wnd` at the upstream.json wn pin) ↔ `mk-core` interop
//! scenarios, ported from `spike/js/run-interop.ts`: every claim the spike
//! logged is an assert here. Each test owns its relay, daemon, `WN_HOME`
//! and members; `wnd` is killed and reaped on drop.

#![allow(
    clippy::missing_docs_in_private_items,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::missing_panics_doc,
    clippy::needless_pass_by_value,
    clippy::doc_markdown,
    clippy::excessive_nesting,
    clippy::too_many_lines,
    clippy::cast_possible_truncation,
    clippy::cast_lossless,
    clippy::str_to_string,
    clippy::shadow_unrelated,
    clippy::shadow_reuse,
    clippy::tests_outside_test_module,
    clippy::items_after_statements,
    clippy::clone_on_ref_ptr,
    unreachable_pub,
    reason = "scenario tests: asserts are the assertions, unwraps fail loudly"
)]

use interop as _;

mod support;

use std::sync::Arc;
use std::time::{Duration, Instant};

use mk_core::media::Locator;
use mk_core::{
    AccountId, AppMessage, AttachmentInput, CreateGroup, GroupId, GroupState, Invitee,
    MediaMessage, RelayResult, RelayStatus, SessionEvent,
};
use serde_json::Value;
use support::*;
use tokio::time::sleep;

/// Marmot inner app-event kind for chat (`MARMOT_APP_EVENT_KIND_CHAT`).
const CHAT_KIND: u16 = 9;

fn chat(content: &str) -> AppMessage {
    AppMessage {
        kind: CHAT_KIND,
        tags: Vec::new(),
        content: content.to_owned(),
    }
}

fn hex_group(hex: &str) -> GroupId {
    GroupId(hex::decode(hex).expect("group id hex"))
}

fn wn_invite_gid(rig: &Rig) -> Option<String> {
    let (ok, parsed) = wn_try(
        &rig.home,
        &rig.socket,
        Some(&rig.wn_npub),
        &["groups", "invites"],
    );
    if !ok {
        return None;
    }
    let invites = parsed
        .pointer("/result/invites")
        .and_then(Value::as_array)
        .or_else(|| parsed.pointer("/result").and_then(Value::as_array))?;
    invites.first().and_then(|invite| {
        invite
            .pointer("/group_id")
            .and_then(Value::as_str)
            .or_else(|| invite.pointer("/id").and_then(Value::as_str))
            .map(str::to_owned)
    })
}

fn wn_group_id(created: &Value) -> String {
    created
        .pointer("/result/group_id")
        .and_then(Value::as_str)
        .or_else(|| created.pointer("/result/id").and_then(Value::as_str))
        .expect("wn group id")
        .to_owned()
}

/// mk-core creates a group, invites wn by its fetched `KeyPackage`, wn
/// accepts, both rosters converge. Returns the MLS group id (asserted
/// identical on the wn side).
async fn mk_group_with_wn(
    rig: &Arc<Rig>,
    a: &mut Member,
    name: &str,
    admins: Vec<AccountId>,
) -> GroupId {
    let wn_kp = until(
        || {
            let rig = Arc::clone(rig);
            async move { fetch_key_package(&rig, &rig.wn_pub).await }
        },
        "wn key package on relay",
        60_000,
        1_500,
    )
    .await;
    let gid = a
        .session
        .create_group(CreateGroup {
            name: name.to_owned(),
            description: String::new(),
            relays: vec![rig.url.clone()],
            invitees: vec![Invitee {
                key_package: wn_kp,
                inbox_relays: vec![rig.url.clone()],
            }],
            admins,
            components: Vec::new(),
        })
        .await
        .expect("create_group");
    flush(a, rig).await;

    // wnd picks up the gift-wrapped Welcome → the invite must name our
    // exact MLS group id.
    let wn_gid = until(
        || {
            let rig = Arc::clone(rig);
            async move { wn_invite_gid(&rig) }
        },
        "wn invite appears",
        90_000,
        1_500,
    )
    .await;
    assert_eq!(wn_gid, gid.to_string(), "group id match");
    wn(
        &rig.home,
        &rig.socket,
        Some(&rig.wn_npub),
        &["groups", "accept", &wn_gid],
    );

    // Roster visible on the wn side, then the documented fanout settle —
    // the only fixed wait the spike proved necessary (INTEROP.md).
    let wn_gid_ref = wn_gid.clone();
    until(
        || {
            let rig = Arc::clone(rig);
            let wn_gid = wn_gid_ref.clone();
            async move { (wn_members(&rig, &wn_gid).len() >= 2).then_some(()) }
        },
        "wn roster shows 2 members",
        90_000,
        1_500,
    )
    .await;
    sleep(Duration::from_millis(WND_SETTLE_MS)).await;

    // mk-core applies the join commit → roster {A, wn}.
    until_member(
        a,
        |a| {
            let gid = gid.clone();
            let rig = Arc::clone(rig);
            Box::pin(async move {
                let _ = sync(&mut *a, &rig, &gid).await;
                a.roster(&gid).contains(&rig.wn_pub).then_some(())
            })
        },
        "mk roster includes wn",
        90_000,
    )
    .await;
    let roster = a.roster(&gid);
    assert_eq!(roster.len(), 2, "roster {roster:?}");
    assert!(roster.contains(&rig.wn_pub), "roster contains wn");
    assert!(roster.contains(&a.account_hex()), "roster contains self");
    gid
}

/// A joins a wn-created group via the gift-wrapped Welcome. Asserts
/// `GroupJoined` for the exact group id.
async fn join_wn_group(rig: &Arc<Rig>, a: &mut Member, gid: &GroupId) {
    let account_hex = a.account_hex();
    let wanted = gid.clone();
    until_member(
        a,
        |a| {
            let account_hex = account_hex.clone();
            let wanted = wanted.clone();
            let rig = Arc::clone(rig);
            Box::pin(async move {
                let wraps = fetch_welcomes(&rig, &account_hex).await;
                let events = ingest(&mut *a, wraps).await;
                events
                    .iter()
                    .any(|event| {
                        matches!(event, SessionEvent::GroupJoined { group } if *group == wanted)
                    })
                    .then_some(())
            })
        },
        "welcome join",
        90_000,
    )
    .await;
}

/// wn → mk-core one way: wn sends `text`, member must decode it.
async fn wn_says(rig: &Arc<Rig>, member: &mut Member, gid: &GroupId, wn_gid: &str, text: &str) {
    wn_send(rig, wn_gid, text);
    let wanted = text.to_owned();
    until_member(
        member,
        |member| {
            let gid = gid.clone();
            let rig = Arc::clone(rig);
            let wanted = wanted.clone();
            Box::pin(async move {
                let events = sync(&mut *member, &rig, &gid).await;
                received(&events)
                    .iter()
                    .any(|content| content.contains(&wanted))
                    .then_some(())
            })
        },
        "mk decodes wn message",
        90_000,
    )
    .await;
}

/// mk-core → wn one way: member sends `text`, `wn messages list` must
/// show it.
async fn member_says(rig: &Arc<Rig>, member: &mut Member, gid: &GroupId, wn_gid: &str, text: &str) {
    member
        .session
        .send_message(gid, chat(text))
        .await
        .expect("send_message");
    flush(member, rig).await;
    let wanted = text.to_owned();
    let wn_gid = wn_gid.to_owned();
    until(
        || {
            let rig = Arc::clone(rig);
            let wanted = wanted.clone();
            let wn_gid = wn_gid.clone();
            async move {
                wn_messages(&rig, &wn_gid, 600)
                    .iter()
                    .any(|message| message.to_string().contains(&wanted))
                    .then_some(())
            }
        },
        "wn lists mk message",
        120_000,
        2_000,
    )
    .await;
}

// ---- scenario 1: mk-core creates a group and invites wn -------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs wn/wnd built from the upstream.json wn pin; see CONTRIBUTING"]
async fn scenario1_mk_core_invites_wn() {
    let start = Instant::now();
    let rig = Arc::new(rig("s1").await);
    let mut a = open_member().await;
    publish_identity(&mut a, &rig).await;

    let gid = mk_group_with_wn(&rig, &mut a, "interop-1", Vec::new()).await;
    let wn_gid = gid.to_string();

    wn_says(&rig, &mut a, &gid, &wn_gid, "hello-from-wn").await;
    member_says(&rig, &mut a, &gid, &wn_gid, "hello-from-mk-core").await;

    rig.relay.shutdown();
    eprintln!("SCENARIO-1 PASS {:.1}s", start.elapsed().as_secs_f64());
}

// ---- scenario 2: wn creates a group and invites mk-core -------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs wn/wnd built from the upstream.json wn pin; see CONTRIBUTING"]
async fn scenario2_wn_invites_mk_core() {
    let start = Instant::now();
    let rig = Arc::new(rig("s2").await);
    let mut a = open_member().await;
    publish_identity(&mut a, &rig).await;
    let a_npub = a.npub();
    let a_hex = a.account_hex();

    let created = wn(
        &rig.home,
        &rig.socket,
        Some(&rig.wn_npub),
        &["groups", "create", "interop-2", &a_npub],
    );
    let wn_gid = wn_group_id(&created);
    let gid = hex_group(&wn_gid);

    // The Welcome arrives gift-wrapped at our inbox relay; joining under
    // wn's group id proves both sides share the same MLS group id.
    join_wn_group(&rig, &mut a, &gid).await;
    until_member(
        &mut a,
        |a| {
            let gid = gid.clone();
            let rig = Arc::clone(&rig);
            Box::pin(async move {
                let _ = sync(&mut *a, &rig, &gid).await;
                a.roster(&gid).contains(&rig.wn_pub).then_some(())
            })
        },
        "mk roster under wn group id",
        90_000,
    )
    .await;
    let roster = a.roster(&gid);
    assert!(
        roster.contains(&rig.wn_pub) && roster.contains(&a_hex),
        "s2 roster {{wn, A}}: {roster:?}"
    );

    // Same documented settle: wnd installs post-join subscriptions before
    // our first application message.
    sleep(Duration::from_millis(WND_SETTLE_MS)).await;

    member_says(&rig, &mut a, &gid, &wn_gid, "s2-hello-from-mk-core").await;
    wn_says(&rig, &mut a, &gid, &wn_gid, "s2-hello-from-wn").await;

    rig.relay.shutdown();
    eprintln!("SCENARIO-2 PASS {:.1}s", start.elapsed().as_secs_f64());
}

// ---- scenario 3: bulk flow + membership changes ---------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs wn/wnd built from the upstream.json wn pin; see CONTRIBUTING"]
async fn scenario3_bulk_and_membership() {
    let start = Instant::now();
    let rig = Arc::new(rig("s3").await);
    let mut a = open_member().await;
    publish_identity(&mut a, &rig).await;
    let a_hex = a.account_hex();

    let gid = mk_group_with_wn(&rig, &mut a, "interop-3", Vec::new()).await;
    let wn_gid = gid.to_string();

    // wn sends its share through the CLI (CLI-bound, ~35 ms/msg in the
    // spike — 25, not 250).
    const WN_MSGS: usize = 25;
    let t_bulk = Instant::now();
    for i in 0..WN_MSGS {
        wn_send(&rig, &wn_gid, &format!("bulk-wn-{i}"));
    }
    eprintln!(
        "s3: wn sent {WN_MSGS} in {:.1}s",
        t_bulk.elapsed().as_secs_f64()
    );

    // mk-core sends the rest.
    const MK_MSGS: usize = 500 - WN_MSGS;
    let t_mk = Instant::now();
    for i in 0..MK_MSGS {
        a.session
            .send_message(&gid, chat(&format!("bulk-mk-{i}")))
            .await
            .expect("send_message");
        flush(&mut a, &rig).await;
    }
    eprintln!(
        "s3: mk sent {MK_MSGS} in {:.1}s",
        t_mk.elapsed().as_secs_f64()
    );

    // One newest-first backlog ingest decodes every wn message.
    let route = a.route_hex(&gid);
    let t_ingest = Instant::now();
    let backlog = fetch_group(&rig, &route).await;
    let events = ingest(&mut a, backlog.clone()).await;
    flush(&mut a, &rig).await;
    let decoded = received(&events)
        .iter()
        .filter(|content| content.contains("bulk-wn-"))
        .count();
    eprintln!(
        "s3: ingested {} events in {:.1}s, decoded {decoded}/{WN_MSGS} wn",
        backlog.len(),
        t_ingest.elapsed().as_secs_f64()
    );
    assert_eq!(
        decoded, WN_MSGS,
        "s3 decoded {decoded}/{WN_MSGS} wn messages"
    );

    // wn lists every mk-core message.
    let wn_gid_ref = &wn_gid;
    until_observed(
        || {
            let rig = Arc::clone(&rig);
            let wn_gid = wn_gid_ref;
            async move {
                let listed = wn_messages(&rig, wn_gid, 600)
                    .iter()
                    .filter(|m| m.to_string().contains("bulk-mk-"))
                    .count();
                (listed >= MK_MSGS, listed)
            }
        },
        "wn lists mk bulk",
        180_000,
        2_000,
    )
    .await;

    // mk-core adds a second mk-core member C.
    let mut c = open_member().await;
    publish_identity(&mut c, &rig).await;
    let c_hex = c.account_hex();
    let c_kp = fetch_key_package(&rig, &c_hex)
        .await
        .expect("C key package");
    a.session
        .invite(
            &gid,
            vec![Invitee {
                key_package: c_kp,
                inbox_relays: vec![rig.url.clone()],
            }],
        )
        .await
        .expect("invite C");
    flush(&mut a, &rig).await;
    join_wn_group(&rig, &mut c, &gid).await;
    // C must ingest the group's backlog and drive its own convergence —
    // the spike's root-caused harness rule: add/remove commits stay
    // `Buffered` until the new member's outbox is flushed.
    until_member(
        &mut c,
        |c| {
            let gid = gid.clone();
            let rig = Arc::clone(&rig);
            Box::pin(async move {
                let _ = sync(&mut *c, &rig, &gid).await;
                (c.roster(&gid).len() == 3).then_some(())
            })
        },
        "C roster shows 3 members",
        120_000,
    )
    .await;

    // mk-core removes wn; both members converge on roster {A, C}.
    let wn_account = AccountId::from_hex(&rig.wn_pub).expect("wn hex");
    a.session
        .remove_members(&gid, &[wn_account])
        .await
        .expect("remove_members");
    flush(&mut a, &rig).await;
    a.session
        .send_message(&gid, chat("post-removal-secret"))
        .await
        .expect("post-removal send");
    flush(&mut a, &rig).await;

    // A authored the secret, so it only needs the roster drop; C must
    // both drop wn from the roster and decode the post-removal message —
    // the same pass satisfies both because delivery only happens after
    // the removal commit applies.
    until_member(
        &mut a,
        |a| {
            let gid = gid.clone();
            let rig = Arc::clone(&rig);
            Box::pin(async move {
                let _ = sync(&mut *a, &rig, &gid).await;
                (a.roster(&gid).len() == 2 && !a.roster(&gid).contains(&rig.wn_pub)).then_some(())
            })
        },
        "A roster drops wn",
        120_000,
    )
    .await;
    until_member(
        &mut c,
        |c| {
            let gid = gid.clone();
            let rig = Arc::clone(&rig);
            Box::pin(async move {
                let events = sync(&mut *c, &rig, &gid).await;
                let roster_ok = c.roster(&gid).len() == 2 && !c.roster(&gid).contains(&rig.wn_pub);
                let got = received(&events)
                    .iter()
                    .any(|content| content.contains("post-removal-secret"));
                (roster_ok && got).then_some(())
            })
        },
        "C drops wn and reads post-removal message",
        120_000,
    )
    .await;

    // wn observes its removal and keeps no post-removal plaintext.
    let wn_gid_ref = &wn_gid;
    until(
        || {
            let rig = Arc::clone(&rig);
            let wn_gid = wn_gid_ref;
            async move {
                (!wn_members(&rig, wn_gid)
                    .iter()
                    .any(|m| m.to_string().contains(&rig.wn_pub)))
                .then_some(())
            }
        },
        "wn roster drops wn",
        120_000,
        2_000,
    )
    .await;
    assert!(
        !wn_messages(&rig, &wn_gid, 600)
            .iter()
            .any(|m| m.to_string().contains("post-removal-secret")),
        "s3 wn cannot read post-removal-secret"
    );

    // wn, as admin of a group it founded, removes mk-core A. With the new
    // capability set our KeyPackage satisfies the group's requirements.
    let a_npub = a.npub();
    let created = wn(
        &rig.home,
        &rig.socket,
        Some(&rig.wn_npub),
        &["groups", "create", "interop-3b", &a_npub],
    );
    let wn_gid2 = wn_group_id(&created);
    let gid2 = hex_group(&wn_gid2);
    join_wn_group(&rig, &mut a, &gid2).await;
    until_member(
        &mut a,
        |a| {
            let gid2 = gid2.clone();
            let rig = Arc::clone(&rig);
            Box::pin(async move {
                let _ = sync(&mut *a, &rig, &gid2).await;
                a.roster(&gid2).contains(&rig.wn_pub).then_some(())
            })
        },
        "A joins wn group 2",
        90_000,
    )
    .await;

    wn(
        &rig.home,
        &rig.socket,
        Some(&rig.wn_npub),
        &["groups", "remove-members", &wn_gid2, &a_npub],
    );
    until_member(
        &mut a,
        |a| {
            let gid2 = gid2.clone();
            let rig = Arc::clone(&rig);
            let a_hex = a_hex.clone();
            Box::pin(async move {
                let _ = sync(&mut *a, &rig, &gid2).await;
                let snapshot = a.session.group(&gid2).expect("group");
                let out = snapshot.is_none_or(|s| {
                    s.state == GroupState::Removed
                        || !s.members.iter().any(|m| m.account.to_hex() == a_hex)
                });
                out.then_some(())
            })
        },
        "A observes own removal",
        120_000,
    )
    .await;

    rig.relay.shutdown();
    eprintln!("SCENARIO-3 PASS {:.1}s", start.elapsed().as_secs_f64());
}

// ---- scenario 4: capability requirements ---------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs wn/wnd built from the upstream.json wn pin; see CONTRIBUTING"]
async fn scenario4_capability_requirements() {
    let start = Instant::now();
    let rig = Arc::new(rig("s4").await);
    let mut a = open_member().await;
    publish_identity(&mut a, &rig).await;
    let a_npub = a.npub();
    let a_hex = a.account_hex();

    // Our `KeyPackage` advertises exactly what mk-core implements:
    // proposals {0x0008, 0x000a}, extension 0xf2d1 (receive), app
    // components {0x8001,0x8003,0x8004,0x8006,0x8009,0x800b,0x800c,0x800d},
    // and never the send/fanout/media-v1 ids.
    let kp = fetch_key_package(&rig, &a_hex)
        .await
        .expect("A key package");
    let advertised = |name: &str| -> Vec<String> {
        kp.tags
            .iter()
            .find(|tag| tag.first().map(String::as_str) == Some(name))
            .map(|tag| tag[1..].to_vec())
            .unwrap_or_default()
    };
    let mut proposals = advertised("mls_proposals");
    let mut extensions = advertised("mls_extensions");
    let mut components = advertised("app_components");
    proposals.sort();
    extensions.sort();
    components.sort();
    eprintln!(
        "s4: our KP advertises proposals={proposals:?} extensions={extensions:?} app_components={components:?}"
    );
    assert_eq!(proposals, ["0x0008", "0x000a"], "s4 proposal set");
    assert_eq!(
        components,
        [
            "0x8001", "0x8003", "0x8004", "0x8006", "0x8009", "0x800b", "0x800c", "0x800d"
        ],
        "s4 app-component set"
    );
    assert!(
        extensions.contains(&"0xf2d1".to_owned()),
        "s4 must advertise the receive extension: {extensions:?}"
    );
    for forbidden in ["0xf2d2", "0xf2d4", "0x8008"] {
        assert!(
            !proposals
                .iter()
                .chain(&extensions)
                .chain(&components)
                .any(|v| v == forbidden),
            "s4 honest advertisement must not include {forbidden}"
        );
    }

    // (a) wn invites our `KeyPackage` into a fresh group -> we join.
    let created = wn(
        &rig.home,
        &rig.socket,
        Some(&rig.wn_npub),
        &["groups", "create", "interop-4", &a_npub],
    );
    let wn_gid = wn_group_id(&created);
    let gid = hex_group(&wn_gid);
    join_wn_group(&rig, &mut a, &gid).await;
    until_member(
        &mut a,
        |a| {
            let gid = gid.clone();
            let rig = Arc::clone(&rig);
            Box::pin(async move {
                let _ = sync(&mut *a, &rig, &gid).await;
                a.roster(&gid).contains(&rig.wn_pub).then_some(())
            })
        },
        "A joins wn group (s4a)",
        90_000,
    )
    .await;

    // (b) a second wn identity on the same wnd: a group founded by two wn
    // devices requires 0x800d; adding us must still succeed, we join, and
    // one message travels each way.
    let second = wn(&rig.home, &rig.socket, None, &["create-identity"]);
    let second_npub = second
        .pointer("/result/npub")
        .and_then(Value::as_str)
        .expect("create-identity npub")
        .to_owned();
    wn(
        &rig.home,
        &rig.socket,
        Some(&second_npub),
        &["keys", "publish"],
    );
    let created_b = wn(
        &rig.home,
        &rig.socket,
        Some(&rig.wn_npub),
        &["groups", "create", "interop-4b", &second_npub],
    );
    let wn_gid_b = wn_group_id(&created_b);
    let shown = wn(
        &rig.home,
        &rig.socket,
        Some(&rig.wn_npub),
        &["groups", "show", &wn_gid_b],
    );
    eprintln!("s4(b): group state: {shown}");
    // The wn-founded group must require 0x800d — that is the whole point of
    // (b): our KeyPackage has to satisfy it for add-members to succeed.
    let required = shown
        .pointer("/result/mls/required_app_components")
        .and_then(Value::as_array)
        .expect("groups show must report required_app_components");
    assert!(
        required.iter().any(|v| v.as_u64() == Some(32_781)),
        "0x800d must be required: {required:?}"
    );
    wn(
        &rig.home,
        &rig.socket,
        Some(&rig.wn_npub),
        &["groups", "add-members", &wn_gid_b, &a_npub],
    );
    let gid_b = hex_group(&wn_gid_b);
    join_wn_group(&rig, &mut a, &gid_b).await;
    until_member(
        &mut a,
        |a| {
            let gid_b = gid_b.clone();
            let rig = Arc::clone(&rig);
            Box::pin(async move {
                let _ = sync(&mut *a, &rig, &gid_b).await;
                a.roster(&gid_b).contains(&rig.wn_pub).then_some(())
            })
        },
        "A joins the 0x800d group (s4b)",
        120_000,
    )
    .await;
    sleep(Duration::from_millis(WND_SETTLE_MS)).await;
    member_says(&rig, &mut a, &gid_b, &wn_gid_b, "s4b-hello-from-mk-core").await;
    wn_says(&rig, &mut a, &gid_b, &wn_gid_b, "s4b-hello-from-wn").await;

    rig.relay.shutdown();
    eprintln!("SCENARIO-4 PASS {:.1}s", start.elapsed().as_secs_f64());
}

// ---- scenario 5: encrypted media round trip -------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs wn/wnd built from the upstream.json wn pin; see CONTRIBUTING"]
async fn scenario5_media_round_trip() {
    let start = Instant::now();
    let rig = Arc::new(rig("s5").await);
    let blossom = BlossomServer::new();
    let mut a = open_member().await;
    publish_identity(&mut a, &rig).await;
    let a_npub = a.npub();

    // Deterministic non-trivial plaintext (200 KiB).
    let plaintext: Vec<u8> = (0u32..200 * 1024).map(|n| (n % 251) as u8).collect();
    let file = std::env::temp_dir().join(format!("s5-plain-{}.bin", std::process::id()));
    std::fs::write(&file, &plaintext).expect("write media file");

    // wn founds the group (its defaults carry the encrypted-media-v2
    // policy) and invites us.
    let created = wn(
        &rig.home,
        &rig.socket,
        Some(&rig.wn_npub),
        &["groups", "create", "interop-5", &a_npub],
    );
    let wn_gid = wn_group_id(&created);
    let gid = hex_group(&wn_gid);
    join_wn_group(&rig, &mut a, &gid).await;
    until_member(
        &mut a,
        |a| {
            let gid = gid.clone();
            let rig = Arc::clone(&rig);
            Box::pin(async move {
                let _ = sync(&mut *a, &rig, &gid).await;
                a.roster(&gid).contains(&rig.wn_pub).then_some(())
            })
        },
        "A joins wn group (s5)",
        90_000,
    )
    .await;
    assert!(
        a.session
            .group(&gid)
            .expect("group")
            .expect("exists")
            .encrypted_media
            .is_some(),
        "s5 group must carry the encrypted-media policy"
    );
    sleep(Duration::from_millis(WND_SETTLE_MS)).await;

    // wn → mk-core: wn encrypts + uploads to the dev Blossom server and
    // sends the imeta reference; we ingest, fetch by locator, open.
    let file_str = file.to_str().expect("utf8 path").to_owned();
    let uploaded = wn(
        &rig.home,
        &rig.socket,
        Some(&rig.wn_npub),
        &[
            "media",
            "upload",
            &wn_gid,
            &file_str,
            "--server",
            &blossom.url,
            "--send",
            "--message",
            "s5-wn-media",
        ],
    );
    eprintln!("s5: wn upload result: {uploaded}");
    let (reference, transport_id, group_id) = until_member(
        &mut a,
        |a| {
            let gid = gid.clone();
            let rig = Arc::clone(&rig);
            Box::pin(async move {
                let events = sync(&mut *a, &rig, &gid).await;
                events.iter().find_map(|event| match event {
                    SessionEvent::MessageReceived { group, message }
                        if !message.attachments.is_empty() =>
                    {
                        let reference = message.attachments.iter().find_map(|r| r.as_ref().ok())?;
                        Some((reference.clone(), message.transport_id, group.clone()))
                    }
                    _ => None,
                })
            })
        },
        "mk receives wn media attachment",
        120_000,
    )
    .await;
    eprintln!(
        "s5: wn->mk attachment locators={:?} media_type={}",
        reference.locators, reference.media_type
    );
    let blob_hash = hex::encode(reference.ciphertext_sha256);
    let ciphertext = blossom
        .get(&blob_hash)
        .expect("wn's upload stored on the dev server");
    let opened = a
        .session
        .open_attachment(&group_id, &transport_id, &reference, &ciphertext)
        .expect("open_attachment");
    assert_eq!(opened, plaintext, "s5 wn->mk plaintext matches");

    // mk-core → wn: seal, "upload" into the dev server's store, send the
    // reference with the loopback locator, then `wn media download`.
    let sealed = a
        .session
        .seal_attachment(
            &gid,
            AttachmentInput {
                plaintext: plaintext.clone(),
                media_type: "application/octet-stream".to_owned(),
                filename: "s5.bin".to_owned(),
                dim: None,
                thumbhash: None,
            },
        )
        .expect("seal_attachment");
    let cipher_hash = hex::encode(sealed.ciphertext_sha256);
    blossom.put(&cipher_hash, &sealed.ciphertext);
    a.session
        .send_media(
            &gid,
            MediaMessage {
                caption: "s5-mk-media".to_owned(),
                attachments: vec![(
                    sealed.pending,
                    vec![Locator {
                        kind: "blossom-v1".to_owned(),
                        value: format!("{}/{cipher_hash}", blossom.url),
                    }],
                )],
                tags: Vec::new(),
            },
        )
        .await
        .expect("send_media");
    flush(&mut a, &rig).await;

    let plain_hash = sha256_hex(&plaintext);
    let out_path = std::env::temp_dir().join(format!("s5-out-{}.bin", std::process::id()));
    let out_str = out_path.to_str().expect("utf8 path").to_owned();
    let downloaded = until(
        || {
            let rig = Arc::clone(&rig);
            let wn_gid = wn_gid.clone();
            let plain_hash = plain_hash.clone();
            let out_str = out_str.clone();
            async move {
                let (ok, parsed) = wn_try(
                    &rig.home,
                    &rig.socket,
                    Some(&rig.wn_npub),
                    &[
                        "media",
                        "download",
                        &wn_gid,
                        &plain_hash,
                        "--output",
                        &out_str,
                    ],
                );
                (ok && out_path_exists(&out_str)).then_some(parsed)
            }
        },
        "wn media download",
        120_000,
        3_000,
    )
    .await;
    eprintln!("s5: wn download result: {downloaded}");
    assert_eq!(
        std::fs::read(&out_path).expect("read downloaded"),
        plaintext,
        "s5 mk->wn plaintext matches"
    );

    rig.relay.shutdown();
    eprintln!("SCENARIO-5 PASS {:.1}s", start.elapsed().as_secs_f64());
}

fn out_path_exists(path: &str) -> bool {
    std::path::Path::new(path).is_file()
}

// ---- scenario 6: agent text stream anchors --------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs wn/wnd built from the upstream.json wn pin; see CONTRIBUTING"]
async fn scenario6_stream_anchors() {
    let start = Instant::now();
    let rig = Arc::new(rig("s6").await);
    let mut a = open_member().await;
    publish_identity(&mut a, &rig).await;
    let a_npub = a.npub();

    let created = wn(
        &rig.home,
        &rig.socket,
        Some(&rig.wn_npub),
        &["groups", "create", "interop-6", &a_npub],
    );
    let wn_gid = wn_group_id(&created);
    let gid = hex_group(&wn_gid);
    join_wn_group(&rig, &mut a, &gid).await;
    until_member(
        &mut a,
        |a| {
            let gid = gid.clone();
            let rig = Arc::clone(&rig);
            Box::pin(async move {
                let _ = sync(&mut *a, &rig, &gid).await;
                a.roster(&gid).contains(&rig.wn_pub).then_some(())
            })
        },
        "A joins wn group (s6)",
        90_000,
    )
    .await;
    sleep(Duration::from_millis(WND_SETTLE_MS)).await;

    // wn anchors a stream start (no broker needs to run).
    let stream_id = "07".repeat(32);
    let started = wn(
        &rig.home,
        &rig.socket,
        Some(&rig.wn_npub),
        &[
            "stream",
            "start",
            &wn_gid,
            "--stream-id",
            &stream_id,
            "--quic-candidate",
            "quic://127.0.0.1:4450",
        ],
    );
    eprintln!("s6: wn stream start result: {started}");
    // `stream start` reports transport message ids; the kind-9 final
    // references the inner app-event id, which `wn messages list` exposes as
    // `message_id` on the kind-1200 row.
    let start_event_id = until(
        || {
            let rig = Arc::clone(&rig);
            let wn_gid = wn_gid.clone();
            let stream_id = stream_id.clone();
            async move {
                wn_messages(&rig, &wn_gid, 50).into_iter().find_map(|m| {
                    let stream = m.pointer("/agent_text_stream")?;
                    if stream.get("kind") == Some(&Value::from("start"))
                        && stream.get("stream_id") == Some(&Value::from(stream_id.as_str()))
                    {
                        m.get("message_id")?.as_str().map(str::to_owned)
                    } else {
                        None
                    }
                })
            }
        },
        "wn projects the kind-1200 start row",
        60_000,
        1_000,
    )
    .await;

    let anchor = until_member(
        &mut a,
        |a| {
            let gid = gid.clone();
            let rig = Arc::clone(&rig);
            let stream_id = stream_id.clone();
            Box::pin(async move {
                let events = sync(&mut *a, &rig, &gid).await;
                events.iter().find_map(|event| match event {
                    SessionEvent::AgentStreamStarted { start, .. }
                        if hex::encode(start.stream_id) == stream_id =>
                    {
                        Some(start.clone())
                    }
                    _ => None,
                })
            })
        },
        "mk surfaces AgentStreamStarted",
        120_000,
    )
    .await;
    assert_eq!(hex::encode(anchor.stream_id), stream_id);
    assert_eq!(anchor.id, start_event_id, "start anchor ids match");

    // wn commits the final: kind-9 with stream-final anchors.
    let transcript_hash = "ab".repeat(32);
    let finished = wn(
        &rig.home,
        &rig.socket,
        Some(&rig.wn_npub),
        &[
            "stream",
            "finish",
            &wn_gid,
            "--stream-id",
            &stream_id,
            "--start-event-id",
            &start_event_id,
            "--transcript-hash",
            &transcript_hash,
            "--chunk-count",
            "3",
            "final text",
        ],
    );
    eprintln!("s6: wn stream finish result: {finished}");

    let (content, final_) = until_member(
        &mut a,
        |a| {
            let gid = gid.clone();
            let rig = Arc::clone(&rig);
            let stream_id = stream_id.clone();
            Box::pin(async move {
                let events = sync(&mut *a, &rig, &gid).await;
                events.iter().find_map(|event| match event {
                    SessionEvent::MessageReceived { message, .. } => {
                        let final_ = message.stream_final?;
                        (hex::encode(final_.stream_id) == stream_id)
                            .then(|| (message.content.clone(), final_))
                    }
                    _ => None,
                })
            })
        },
        "mk surfaces stream final",
        120_000,
    )
    .await;
    assert_eq!(content, "final text");
    assert_eq!(hex::encode(final_.stream_id), stream_id);
    assert_eq!(hex::encode(final_.start_event_id), start_event_id);
    assert_eq!(hex::encode(final_.transcript_hash), transcript_hash);
    assert_eq!(final_.chunk_count, 3);

    // Receive role is pure delivery: nothing staged for the wire.
    assert!(
        a.session.take_outbound().expect("outbound").is_empty(),
        "s6 receive must produce no outbound"
    );

    rig.relay.shutdown();
    eprintln!("SCENARIO-6 PASS {:.1}s", start.elapsed().as_secs_f64());
}

// ---- scenario 19: consumed KeyPackage rotates ------------------------------

/// First value of `event`'s `name` tag.
fn tag_value<'a>(event: &'a mk_core::Event, name: &str) -> Option<&'a str> {
    event
        .tags
        .iter()
        .find(|tag| tag.first().map(String::as_str) == Some(name))
        .and_then(|tag| tag.get(1))
        .map(String::as_str)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs wn/wnd built from the upstream.json wn pin; see CONTRIBUTING"]
async fn scenario19_consumed_key_package_rotates() {
    let start = Instant::now();
    let rig = Arc::new(rig("s19").await);
    let mut a = open_member().await;
    publish_identity(&mut a, &rig).await;
    let a_hex = a.account_hex();
    let a_npub = a.npub();

    let first = fetch_key_package(&rig, &a_hex)
        .await
        .expect("first key package");
    let first_d = tag_value(&first, "d").expect("d tag").to_owned();
    let first_i = tag_value(&first, "i").map(str::to_owned);

    // wn invites us; the join consumes our promoted KeyPackage.
    let created = wn(
        &rig.home,
        &rig.socket,
        Some(&rig.wn_npub),
        &["groups", "create", "interop-19a", &a_npub],
    );
    let wn_gid = wn_group_id(&created);
    let gid = hex_group(&wn_gid);
    join_wn_group(&rig, &mut a, &gid).await;

    // Within one tick the lifecycle publishes a replacement to the same
    // slot: same `d`, newer `created_at`, different `i`.
    let first_created = first.created_at;
    let replacement = until_member(
        &mut a,
        |a| {
            let rig = Arc::clone(&rig);
            let gid = gid.clone();
            let a_hex = a_hex.clone();
            let first_d = first_d.clone();
            let first_i = first_i.clone();
            Box::pin(async move {
                let _ = sync(&mut *a, &rig, &gid).await;
                fetch(&rig, key_package_filter(&a_hex))
                    .await
                    .into_iter()
                    .find(|event| {
                        tag_value(event, "d") == Some(first_d.as_str())
                            && event.created_at > first_created
                            && tag_value(event, "i").map(str::to_owned) != first_i
                    })
            })
        },
        "same-slot KeyPackage replacement on the relay",
        90_000,
    )
    .await;

    // The replacement heads the slot, so wn's second invite selects it.
    let newest = fetch_key_package(&rig, &a_hex)
        .await
        .expect("relay key package");
    assert_eq!(newest.id, replacement.id, "the replacement heads the slot");
    let created = wn(
        &rig.home,
        &rig.socket,
        Some(&rig.wn_npub),
        &["groups", "create", "interop-19b", &a_npub],
    );
    let wn_gid2 = wn_group_id(&created);
    let gid2 = hex_group(&wn_gid2);
    join_wn_group(&rig, &mut a, &gid2).await;

    member_says(&rig, &mut a, &gid2, &wn_gid2, "s19-hello-from-mk-core").await;
    wn_says(&rig, &mut a, &gid2, &wn_gid2, "s19-hello-from-wn").await;

    rig.relay.shutdown();
    eprintln!("SCENARIO-19 PASS {:.1}s", start.elapsed().as_secs_f64());
}

// ---- scenario 17: second device signs out ----------------------------

/// S17: two devices of one account in one wn group; the second device
/// signs out — its `SelfRemove` commits, wn shows one leaf left for the
/// account, the signed-out device ends `Removed`, and the remaining
/// device's roster is unchanged.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs wn/wnd built from the upstream.json wn pin; see CONTRIBUTING"]
async fn scenario17_second_device_signs_out() {
    let start = Instant::now();
    let rig = Arc::new(rig("s17").await);

    // Two devices of one account: the same secret over separate stores,
    // each with its own KeyPackage slot.
    let signer = Arc::new(mk_core::testing::LocalSigner::from_secret([0x5d; 32]).expect("secret"));
    let mut a1 = open_member_with(Arc::clone(&signer)).await;
    let mut a2 = open_member_with(signer).await;
    publish_identity(&mut a1, &rig).await;
    publish_identity(&mut a2, &rig).await;
    let a_npub = a1.npub();
    let a_hex = a1.account_hex();
    assert_eq!(a2.account_hex(), a_hex, "one account, two devices");

    let created = wn(
        &rig.home,
        &rig.socket,
        Some(&rig.wn_npub),
        &["groups", "create", "interop-17", &a_npub],
    );
    let wn_gid = wn_group_id(&created);
    let gid = hex_group(&wn_gid);
    join_wn_group(&rig, &mut a1, &gid).await;
    join_wn_group(&rig, &mut a2, &gid).await;

    // The second device signs out: its leaf's SelfRemove is staged and
    // published; wn commits it, leaving one leaf for the account.
    let report = a2
        .session
        .sign_out(std::slice::from_ref(&rig.url))
        .await
        .expect("sign_out");
    assert_eq!(report.left, vec![gid.clone()], "the member group is left");
    assert_eq!(report.kept_admin_leaves, Vec::<GroupId>::new());
    flush(&mut a2, &rig).await;

    let wn_gid_ref = &wn_gid;
    let npub_ref = &a_npub;
    until(
        || {
            let rig = Arc::clone(&rig);
            async move {
                (wn_members(&rig, wn_gid_ref)
                    .iter()
                    .filter(|member| member.to_string().contains(npub_ref))
                    .count()
                    == 1)
                    .then_some(())
            }
        },
        "wn roster shows one leaf for our account",
        90_000,
        800,
    )
    .await;

    // The signed-out device follows the commit to `Removed`; the account
    // keeps a leaf through a1, so `access` is `RemovedDevice`.
    let gid2 = gid.clone();
    until_member(
        &mut a2,
        |a| {
            let gid2 = gid2.clone();
            let rig = Arc::clone(&rig);
            Box::pin(async move {
                let _ = sync(&mut *a, &rig, &gid2).await;
                let removed = a.session.group(&gid2).expect("group").is_some_and(|s| {
                    s.state == GroupState::Removed
                        && s.access == mk_core::GroupAccess::RemovedDevice
                });
                removed.then_some(())
            })
        },
        "a2 is removed (device-scoped)",
        120_000,
    )
    .await;

    // The remaining device keeps the group: roster {wn, our leaf}.
    let events = sync(&mut a1, &rig, &gid).await;
    let _ = events;
    let roster = a1.roster(&gid);
    assert!(
        roster.contains(&rig.wn_pub) && roster.contains(&a_hex),
        "a1 roster {{wn, A}}: {roster:?}"
    );

    rig.relay.shutdown();
    eprintln!("SCENARIO-17 PASS {:.1}s", start.elapsed().as_secs_f64());
}

/// S21 (D4): after the host reports the post-join fetch done, the session
/// runs its quiet window and sampled jitter, then authors exactly one
/// self-update commit wn accepts — messaging survives in both directions.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "needs WN_BIN and WND_BIN; CI runs it"]
async fn scenario21_post_join_self_update() {
    let start = Instant::now();
    let rig = Arc::new(rig("s21").await);
    let mut a = open_member().await;
    publish_identity(&mut a, &rig).await;
    let a_npub = a.npub();

    let created = wn(
        &rig.home,
        &rig.socket,
        Some(&rig.wn_npub),
        &["groups", "create", "interop-21", &a_npub],
    );
    let wn_gid = wn_group_id(&created);
    let gid = hex_group(&wn_gid);
    join_wn_group(&rig, &mut a, &gid).await;

    // The host's post-join history fetch is done (EOSE on every relay):
    // quiet (60 s) plus the sampled jitter gate one self-update commit.
    a.session.history_fetched(&gid).expect("history_fetched");
    let gid_ref = gid.clone();
    until_member(
        &mut a,
        |a| {
            let rig = Arc::clone(&rig);
            let gid = gid_ref.clone();
            Box::pin(async move {
                let mut events = sync(&mut *a, &rig, &gid).await;
                events.append(&mut a.session.drain_events().expect("drain"));
                events
                    .iter()
                    .any(|event| {
                        matches!(event, SessionEvent::SelfUpdated { group } if *group == gid)
                    })
                    .then_some(())
            })
        },
        "self-update commit after quiet + jitter",
        240_000,
    )
    .await;

    // Both directions keep working after the rotation.
    member_says(&rig, &mut a, &gid, &wn_gid, "s21-after-self-update").await;
    wn_says(&rig, &mut a, &gid, &wn_gid, "s21-wn-replies").await;

    rig.relay.shutdown();
    eprintln!("SCENARIO-21 PASS {:.1}s", start.elapsed().as_secs_f64());
}

// ---- scenario 20: stall detection + consented repair ------------------

/// S20 (D2): wn is the admin of a group holding two devices of one
/// account of ours. wn renames the group (one Commit) and sends three
/// messages; the commit is withheld from device 1's feed only. Three
/// deferred events suspect the stall once; one unfloored refetch still
/// lacking the commit flags `access: Stalled` while device 2 stays
/// `Active`. wn then removes and re-invites the account: device 2's
/// removed copy reinstalls from its new Welcome without consent, device
/// 1's stalled copy gets a consent-required `RejoinOffered`, and after
/// `confirm_rejoin` both devices exchange messages with wn again.
///
/// Our first device creates the group and seats wn as an admin rather
/// than joining a wn-created one: a copy installed through a Welcome
/// keeps `local_copy_welcome_created_at`, and the stall counter's
/// `transport_message_predates_local_copy` filter drops every event
/// inside the five-minute post-Welcome clock-skew window — on wall-clock
/// time nothing wn emits during the test would ever count. A
/// self-created copy has no Welcome floor, so the withheld wn commit
/// defers the chats for real.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "needs wn/wnd built from the upstream.json wn pin; see CONTRIBUTING"]
async fn scenario20_stall_and_repair() {
    let start = Instant::now();
    let rig = Arc::new(rig("s20").await);

    // Two devices of one account; each holds a leaf of the group.
    let signer = Arc::new(mk_core::testing::LocalSigner::from_secret([0x20; 32]).expect("secret"));
    let mut a1 = open_member_with(Arc::clone(&signer)).await;
    let mut a2 = open_member_with(signer).await;
    publish_identity(&mut a1, &rig).await;
    publish_identity(&mut a2, &rig).await;
    let a_npub = a1.npub();
    let a_hex = a1.account_hex();

    // a1 founds the group with wn as an admin: the founding invitees are
    // wn's KeyPackage plus one fresh KeyPackage per own device.
    let wn_account = AccountId::from_hex(&rig.wn_pub).expect("wn hex");
    let wn_kp = until(
        || {
            let rig = Arc::clone(&rig);
            async move { fetch_key_package(&rig, &rig.wn_pub).await }
        },
        "wn key package on relay",
        60_000,
        1_500,
    )
    .await;
    // a2's founding-invitee KeyPackage: the creator's own leaf is already
    // a member, so only a2 needs an invitee. The relay can also hold a
    // same-slot replacement (same leaf signature key — a Commit cannot
    // add two leaves with one key), so select per `d` slot and drop a1's
    // slot entirely.
    let a1_slot = hex::encode(a1.session.key_package_status().expect("status").slot);
    let a2_kp = until(
        || {
            let rig = Arc::clone(&rig);
            let a_hex = a_hex.clone();
            let a1_slot = a1_slot.clone();
            async move {
                let kps = fetch(&rig, key_package_filter(&a_hex)).await;
                mk_core::key_package::select_key_packages(
                    AccountId::from_hex(&a_hex).expect("hex"),
                    &kps,
                    std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .expect("clock")
                        .as_secs(),
                )
                .into_iter()
                .find(|event| tag_value(event, "d") != Some(a1_slot.as_str()))
            }
        },
        "a2's KeyPackage on relay",
        60_000,
        1_500,
    )
    .await;
    let mut invitees = vec![Invitee {
        key_package: wn_kp,
        inbox_relays: vec![rig.url.clone()],
    }];
    invitees.push(Invitee {
        key_package: a2_kp,
        inbox_relays: vec![rig.url.clone()],
    });
    let gid = a1
        .session
        .create_group(CreateGroup {
            name: "interop-20".to_owned(),
            description: String::new(),
            relays: vec![rig.url.clone()],
            invitees,
            admins: vec![wn_account],
            components: Vec::new(),
        })
        .await
        .expect("create_group");
    flush(&mut a1, &rig).await;
    let wn_gid = gid.to_string();

    // a2 installs its copy from its own wrap of the founding invite.
    let gid_a2 = gid.clone();
    let account_hex2 = a_hex.clone();
    until_member(
        &mut a2,
        |a| {
            let gid = gid_a2.clone();
            let account_hex = account_hex2.clone();
            let rig = Arc::clone(&rig);
            Box::pin(async move {
                let wraps = fetch_welcomes(&rig, &account_hex).await;
                let events = ingest(&mut *a, wraps).await;
                events
                    .iter()
                    .any(|event| {
                        matches!(event, SessionEvent::GroupJoined { group } if *group == gid)
                    })
                    .then_some(())
            })
        },
        "a2 welcome join",
        90_000,
    )
    .await;

    // wn accepts the invite; the roster shows three leaves.
    let found = until(
        || {
            let rig = Arc::clone(&rig);
            async move { wn_invite_gid(&rig) }
        },
        "wn invite appears",
        90_000,
        1_500,
    )
    .await;
    assert_eq!(found, wn_gid, "group id match");
    wn(
        &rig.home,
        &rig.socket,
        Some(&rig.wn_npub),
        &["groups", "accept", &wn_gid],
    );
    let wn_gid_ref = wn_gid.clone();
    until(
        || {
            let rig = Arc::clone(&rig);
            let wn_gid = wn_gid_ref.clone();
            async move { (wn_members(&rig, &wn_gid).len() >= 2).then_some(()) }
        },
        "wn roster shows both accounts",
        90_000,
        1_500,
    )
    .await;
    sleep(Duration::from_millis(WND_SETTLE_MS)).await;

    // Both devices converge on the {wn, A, A} roster.
    for (i, member) in [&mut a1, &mut a2].into_iter().enumerate() {
        let gid_ref = gid.clone();
        let wn_pub = rig.wn_pub.clone();
        until_member(
            member,
            |a| {
                let gid = gid_ref.clone();
                let wn_pub = wn_pub.clone();
                let rig = Arc::clone(&rig);
                Box::pin(async move {
                    let _ = sync(&mut *a, &rig, &gid).await;
                    a.roster(&gid).contains(&wn_pub).then_some(())
                })
            },
            if i == 0 {
                "a1 roster includes wn"
            } else {
                "a2 roster includes wn"
            },
            90_000,
        )
        .await;
    }

    // Everything currently on the group route is already ingested.
    let route = a1.route_hex(&gid);
    let seen: std::collections::BTreeSet<mk_core::EventId> = fetch_group(&rig, &route)
        .await
        .into_iter()
        .map(|event| event.id)
        .collect();

    // The rename is one commit; the three chats ride it. Capture the
    // commit before the chats go out — picking "the earliest fresh event"
    // afterwards is not deterministic because a chat that shares the
    // commit's `created_at` second can sort first by id. Everything the
    // rename step publishes is withheld, so the chats cannot reach their
    // epoch and stay transport-deferred.
    wn(
        &rig.home,
        &rig.socket,
        Some(&rig.wn_npub),
        &["groups", "rename", &wn_gid, "interop-20-renamed"],
    );
    let last_seen = std::cell::RefCell::new(std::collections::BTreeSet::<mk_core::EventId>::new());
    let dropped: std::collections::BTreeSet<mk_core::EventId> = until(
        || {
            let rig = Arc::clone(&rig);
            let seen = seen.clone();
            let route = route.clone();
            let last_seen = &last_seen;
            async move {
                let fresh: std::collections::BTreeSet<mk_core::EventId> = fetch_group(&rig, &route)
                    .await
                    .into_iter()
                    .filter(|event| !seen.contains(&event.id))
                    .map(|event| event.id)
                    .collect();
                // The commit set is trusted only once it stays unchanged
                // between two polls — a stray that arrives before the
                // rename lands must not close the window early.
                let mut last = last_seen.borrow_mut();
                if !fresh.is_empty() && fresh == *last {
                    Some(fresh)
                } else {
                    *last = fresh;
                    None
                }
            }
        },
        "wn's rename commit on the route",
        90_000,
        800,
    )
    .await;
    for i in 0..3 {
        wn_send(&rig, &wn_gid, &format!("s20-m{i}"));
    }
    let without_commit: Vec<mk_core::Event> = until(
        || {
            let rig = Arc::clone(&rig);
            let seen = seen.clone();
            let dropped = dropped.clone();
            let route = route.clone();
            async move {
                let fresh: Vec<mk_core::Event> = fetch_group(&rig, &route)
                    .await
                    .into_iter()
                    .filter(|event| !seen.contains(&event.id) && !dropped.contains(&event.id))
                    .collect();
                (fresh.len() >= 3).then_some(fresh)
            }
        },
        "wn's three chats on the route",
        90_000,
        800,
    )
    .await;

    // Exactly one suspicion across the withheld feed on device 1.
    let events = ingest(&mut a1, without_commit).await;
    let suspected = events
        .iter()
        .filter(
            |event| matches!(event, SessionEvent::GroupStallSuspected { group } if *group == gid),
        )
        .count();
    assert_eq!(suspected, 1, "three deferred events suspect once");

    // The one refetch still cannot land the missing commit: the counter
    // stays full at the same epoch and the copy flags `Stalled`.
    let refetch: Vec<mk_core::Event> = fetch_group(&rig, &route)
        .await
        .into_iter()
        .filter(|event| !dropped.contains(&event.id))
        .collect();
    let _ = ingest(&mut a1, refetch).await;
    a1.session.history_fetched(&gid).expect("history_fetched");
    let events = a1.session.drain_events().expect("drain");
    let stalled = events
        .iter()
        .filter(|event| matches!(event, SessionEvent::GroupStalled { group } if *group == gid))
        .count();
    assert_eq!(stalled, 1, "one stalled event after the refetch");
    let snapshot = a1.session.group(&gid).expect("group").expect("snapshot");
    assert_eq!(snapshot.access, mk_core::GroupAccess::Stalled);

    // Device 2 followed the whole feed and stays Active.
    let _ = sync(&mut a2, &rig, &gid).await;
    let snapshot = a2.session.group(&gid).expect("group").expect("snapshot");
    assert_eq!(snapshot.access, mk_core::GroupAccess::Active);

    // Repair: wn removes the whole account, then re-invites it. Device 2
    // must apply the removal before the re-add wraps reach it — arriving
    // while it still held a leaf would park a consent-required offer.
    wn(
        &rig.home,
        &rig.socket,
        Some(&rig.wn_npub),
        &["groups", "remove-members", &wn_gid, &a_npub],
    );
    let a2_events = Arc::new(std::sync::Mutex::new(Vec::<SessionEvent>::new()));
    let gid_a2 = gid.clone();
    let a2_events_ref = Arc::clone(&a2_events);
    until_member(
        &mut a2,
        |a| {
            let gid = gid_a2.clone();
            let rig = Arc::clone(&rig);
            let a2_events = Arc::clone(&a2_events_ref);
            Box::pin(async move {
                let events = sync(&mut *a, &rig, &gid).await;
                a2_events.lock().expect("events").extend(events);
                let removed = a.session.group(&gid).expect("group").is_some_and(|s| {
                    matches!(
                        s.access,
                        mk_core::GroupAccess::Removed | mk_core::GroupAccess::RemovedDevice
                    )
                });
                removed.then_some(())
            })
        },
        "a2 removed by wn",
        120_000,
    )
    .await;
    wn(
        &rig.home,
        &rig.socket,
        Some(&rig.wn_npub),
        &["groups", "add-members", &wn_gid, &a_npub],
    );

    // Device 2's removed copy reinstalls from its new Welcome on its own —
    // no consent, no `RejoinOffered` across the whole repair.
    let gid_a2 = gid.clone();
    let account_hex2 = a_hex.clone();
    let a2_events_ref = Arc::clone(&a2_events);
    until_member(
        &mut a2,
        |a| {
            let gid = gid_a2.clone();
            let account_hex = account_hex2.clone();
            let rig = Arc::clone(&rig);
            let a2_events = Arc::clone(&a2_events_ref);
            Box::pin(async move {
                let wraps = fetch_welcomes(&rig, &account_hex).await;
                let events = ingest(&mut *a, wraps).await;
                a2_events
                    .lock()
                    .expect("events")
                    .extend(events.iter().cloned());
                let joined = events.iter().any(
                    |event| matches!(event, SessionEvent::GroupJoined { group } if *group == gid),
                );
                let active = a
                    .session
                    .group(&gid)
                    .expect("group")
                    .is_some_and(|s| s.access == mk_core::GroupAccess::Active);
                (joined || active).then_some(())
            })
        },
        "a2 auto-installs the re-add welcome",
        120_000,
    )
    .await;
    assert!(
        !a2_events
            .lock()
            .expect("events")
            .iter()
            .any(|event| matches!(event, SessionEvent::RejoinOffered { .. })),
        "a healthy sibling never sees a consent offer"
    );
    let snapshot = a2.session.group(&gid).expect("group").expect("snapshot");
    assert_eq!(snapshot.access, mk_core::GroupAccess::Active);
    flush(&mut a2, &rig).await;

    // Device 1's stalled copy cannot apply the removal commit, so the
    // re-add wrap parks a consent-required offer on it.
    let gid_a1 = gid.clone();
    let account_hex1 = a_hex.clone();
    until_member(
        &mut a1,
        |a| {
            let gid = gid_a1.clone();
            let account_hex = account_hex1.clone();
            let rig = Arc::clone(&rig);
            Box::pin(async move {
                let wraps = fetch_welcomes(&rig, &account_hex).await;
                let events = ingest(&mut *a, wraps).await;
                events
                    .iter()
                    .any(|event| {
                        matches!(event, SessionEvent::RejoinOffered { group } if *group == gid)
                    })
                    .then_some(())
            })
        },
        "a1 rejoin offer",
        120_000,
    )
    .await;
    let offers = a1.session.rejoin_offers(&gid).expect("rejoin_offers");
    assert_eq!(offers.len(), 1, "one re-add offer for a1");
    assert_eq!(offers[0].author, wn_account, "the admin authored it");
    a1.session
        .confirm_rejoin(&gid, &offers[0].offer, &offers[0].token)
        .await
        .expect("confirm_rejoin");
    let events = a1.session.drain_events().expect("drain");
    assert!(
        events
            .iter()
            .any(|event| matches!(event, SessionEvent::GroupJoined { group } if *group == gid)),
        "confirm installs the replacement copy"
    );
    let snapshot = a1.session.group(&gid).expect("group").expect("snapshot");
    assert_eq!(snapshot.access, mk_core::GroupAccess::Active);
    flush(&mut a1, &rig).await;

    // wn's next message reaches both devices at the repaired epoch; a
    // message from each device reaches wn.
    wn_send(&rig, &wn_gid, "s20-after-rejoin");
    let wanted = "s20-after-rejoin".to_owned();
    for (i, member) in [&mut a1, &mut a2].into_iter().enumerate() {
        let gid_ref = gid.clone();
        let wanted = wanted.clone();
        until_member(
            member,
            |a| {
                let gid = gid_ref.clone();
                let wanted = wanted.clone();
                let rig = Arc::clone(&rig);
                Box::pin(async move {
                    let events = sync(&mut *a, &rig, &gid).await;
                    received(&events)
                        .iter()
                        .any(|content| content.contains(&wanted))
                        .then_some(())
                })
            },
            if i == 0 {
                "a1 reads wn after rejoin"
            } else {
                "a2 reads wn after rejoin"
            },
            90_000,
        )
        .await;
    }
    member_says(&rig, &mut a1, &gid, &wn_gid, "s20-d1-says").await;
    member_says(&rig, &mut a2, &gid, &wn_gid, "s20-d2-says").await;

    rig.relay.shutdown();
    eprintln!("SCENARIO-20 PASS {:.1}s", start.elapsed().as_secs_f64());
}

// ---- scenario 22: the admin re-adds a stuck member --------------------

/// S22 (D2): we are the admin of a group holding a wn member. One of our
/// commits is withheld from the relay — our session hears `Accepted`
/// while wn never learns the epoch — and a chat rides it that wn cannot
/// read. `readd_member` stages the removal, the removal's confirmation
/// stages exactly one Invite, and wn's stalled copy surfaces a
/// consent-required rejoin offer through `groups recovery-status`;
/// `confirm-rejoin` installs the replacement copy and the round trip
/// recovers. Whatever wn publishes toward recovery on its own (kind-1159
/// requests), our side must stage nothing in response.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs wn/wnd built from the upstream.json wn pin; see CONTRIBUTING"]
async fn scenario22_readd_member() {
    let start = Instant::now();
    let rig = Arc::new(rig("s22").await);
    let mut a = open_member().await;
    publish_identity(&mut a, &rig).await;
    let gid = mk_group_with_wn(&rig, &mut a, "interop-22", Vec::new()).await;
    let wn_gid = gid.to_string();

    // A rename is one self-contained commit at the new epoch. It never
    // reaches the relay, but the session hears `Accepted` so the publish
    // resolves and our epoch moves — wn is stuck behind it for good.
    a.session
        .update_group_profile(&gid, Some("interop-22-moved".to_owned()), None)
        .await
        .expect("rename");
    if let Some(seq) = a.session.awaiting_durability() {
        a.session.acknowledge_durable(seq);
    }
    let withheld = a.session.take_outbound().expect("take_outbound");
    assert!(!withheld.is_empty(), "the rename stages an outbound commit");
    for item in &withheld {
        let results: Vec<RelayResult> = item
            .relays
            .iter()
            .map(|relay| RelayResult {
                relay: relay.clone(),
                status: RelayStatus::Accepted,
            })
            .collect();
        a.session
            .report_publish(&item.id, &results)
            .await
            .expect("report_publish");
    }
    a.session
        .send_message(&gid, chat("s22-unreadable"))
        .await
        .expect("chat");
    flush(&mut a, &rig).await;

    // Whatever wn does about being stuck (its kind-1159 recovery
    // requests, if it issues them), our side stays inert: several
    // catch-up passes must stage nothing.
    for _ in 0..3 {
        sleep(Duration::from_secs(2)).await;
        let route = a.route_hex(&gid);
        let backlog = fetch_group(&rig, &route).await;
        let _ = ingest(&mut a, backlog).await;
        a.session.tick().await.expect("tick");
        let staged = a.session.take_outbound().expect("take_outbound");
        assert!(
            staged.is_empty(),
            "a recovery request must stage nothing: {staged:?}"
        );
    }

    // Re-add the stuck account: `Removed` — the Invite follows once the
    // removal commit confirms.
    let wn_account = AccountId::from_hex(&rig.wn_pub).expect("wn hex");
    let wn_kp = until(
        || {
            let rig = Arc::clone(&rig);
            async move { fetch_key_package(&rig, &rig.wn_pub).await }
        },
        "wn key package on relay",
        60_000,
        1_500,
    )
    .await;
    let outcome = a
        .session
        .readd_member(
            &gid,
            wn_account,
            vec![Invitee {
                key_package: wn_kp,
                inbox_relays: vec![rig.url.clone()],
            }],
        )
        .await
        .expect("readd_member");
    assert!(
        matches!(outcome, mk_core::ReaddOutcome::Removed),
        "wn still holds a leaf: {outcome:?}"
    );
    // Ticks drive the removal to confirmation and stage the one Invite;
    // flushes publish both to the relay and wn's inbox.
    for _ in 0..8 {
        flush(&mut a, &rig).await;
        a.session.tick().await.expect("tick");
        flush(&mut a, &rig).await;
    }

    // wn's stalled copy holds the offer: `recovery-status` lists it with
    // the token `confirm-rejoin` needs.
    let wn_gid_ref = wn_gid.clone();
    let (welcome_id, token) = until(
        || {
            let rig = Arc::clone(&rig);
            let wn_gid = wn_gid_ref.clone();
            async move {
                let (ok, parsed) = wn_try(
                    &rig.home,
                    &rig.socket,
                    Some(&rig.wn_npub),
                    &["groups", "recovery-status", &wn_gid],
                );
                if !ok {
                    return None;
                }
                let offers = parsed
                    .pointer("/result/rejoin_invitations")
                    .and_then(Value::as_array)?;
                let offer = offers.first()?;
                let welcome_id = offer
                    .pointer("/welcome_id_hex")
                    .and_then(Value::as_str)?
                    .to_owned();
                let token = offer
                    .pointer("/local_state_token")
                    .and_then(Value::as_str)?
                    .to_owned();
                Some((welcome_id, token))
            }
        },
        "wn rejoin offer in recovery-status",
        120_000,
        2_000,
    )
    .await;
    wn(
        &rig.home,
        &rig.socket,
        Some(&rig.wn_npub),
        &[
            "groups",
            "confirm-rejoin",
            &welcome_id,
            "--local-state-token",
            &token,
            "--confirm",
        ],
    );

    // The round trip recovers: wn reads our next message and we read wn's.
    member_says(&rig, &mut a, &gid, &wn_gid, "s22-after-readd").await;
    wn_says(&rig, &mut a, &gid, &wn_gid, "s22-wn-replies").await;

    rig.relay.shutdown();
    eprintln!("SCENARIO-22 PASS {:.1}s", start.elapsed().as_secs_f64());
}
