//! White Noise (`wn`/`wnd` at the pinned MDK rev) ↔ `mk-core` interop
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
    MediaMessage, SessionEvent,
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
async fn mk_group_with_wn(rig: &Arc<Rig>, a: &mut Member, name: &str) -> GroupId {
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
            admins: Vec::new(),
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
#[ignore = "needs wn/wnd from the pinned MDK rev; see CONTRIBUTING"]
async fn scenario1_mk_core_invites_wn() {
    let start = Instant::now();
    let rig = Arc::new(rig("s1").await);
    let mut a = open_member().await;
    publish_identity(&mut a, &rig).await;

    let gid = mk_group_with_wn(&rig, &mut a, "interop-1").await;
    let wn_gid = gid.to_string();

    wn_says(&rig, &mut a, &gid, &wn_gid, "hello-from-wn").await;
    member_says(&rig, &mut a, &gid, &wn_gid, "hello-from-mk-core").await;

    rig.relay.shutdown();
    eprintln!("SCENARIO-1 PASS {:.1}s", start.elapsed().as_secs_f64());
}

// ---- scenario 2: wn creates a group and invites mk-core -------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs wn/wnd from the pinned MDK rev; see CONTRIBUTING"]
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
#[ignore = "needs wn/wnd from the pinned MDK rev; see CONTRIBUTING"]
async fn scenario3_bulk_and_membership() {
    let start = Instant::now();
    let rig = Arc::new(rig("s3").await);
    let mut a = open_member().await;
    publish_identity(&mut a, &rig).await;
    let a_hex = a.account_hex();

    let gid = mk_group_with_wn(&rig, &mut a, "interop-3").await;
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
#[ignore = "needs wn/wnd from the pinned MDK rev; see CONTRIBUTING"]
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
#[ignore = "needs wn/wnd from the pinned MDK rev; see CONTRIBUTING"]
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
#[ignore = "needs wn/wnd from the pinned MDK rev; see CONTRIBUTING"]
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
