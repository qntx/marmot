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

use mk_core::{AccountId, AppMessage, CreateGroup, GroupId, GroupState, Invitee, SessionEvent};
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

/// Extract `"required"`/`"had"` style lists from a wn refusal payload for
/// the scenario report.
fn refusal_ids(parsed: &Value) -> String {
    let error = parsed.pointer("/error").cloned().unwrap_or(Value::Null);
    let compact = serde_json::to_string(&error).unwrap_or_else(|_| parsed.to_string());
    if compact == "null" {
        parsed.to_string()
    } else {
        compact
    }
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

    let (ok, created) = wn_try(
        &rig.home,
        &rig.socket,
        Some(&rig.wn_npub),
        &["groups", "create", "interop-2", &a_npub],
    );
    if !ok {
        let text = refusal_ids(&created);
        assert!(
            text.contains("missing_required_capabilities"),
            "s2 refusal must be capability negotiation: {text}"
        );
        rig.relay.shutdown();
        eprintln!(
            "SCENARIO-2 REFUSED {text} {:.1}s",
            start.elapsed().as_secs_f64()
        );
        return;
    }
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
    for i in 0..MK_MSGS {
        a.session
            .send_message(&gid, chat(&format!("bulk-mk-{i}")))
            .await
            .expect("send_message");
        flush(&mut a, &rig).await;
    }

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
    until(
        || {
            let rig = Arc::clone(&rig);
            let wn_gid = wn_gid_ref;
            async move {
                (wn_messages(&rig, wn_gid, 600)
                    .iter()
                    .filter(|m| m.to_string().contains("bulk-mk-"))
                    .count()
                    >= MK_MSGS)
                    .then_some(())
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

    // wn, as admin of a group it founded, removes mk-core A. If wn's
    // capability preflight refuses our honest `KeyPackage`, assert the
    // precise refusal instead — that refusal is the evidence.
    let a_npub = a.npub();
    let (ok, created) = wn_try(
        &rig.home,
        &rig.socket,
        Some(&rig.wn_npub),
        &["groups", "create", "interop-3b", &a_npub],
    );
    if !ok {
        let text = refusal_ids(&created);
        assert!(
            text.contains("missing_required_capabilities"),
            "s3b refusal must be capability negotiation: {text}"
        );
        rig.relay.shutdown();
        eprintln!(
            "SCENARIO-3 PASS + 3b REFUSED ({text}) {:.1}s",
            start.elapsed().as_secs_f64()
        );
        return;
    }
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

    // Record exactly what our `KeyPackage` advertises — only what mk-core
    // implements (no 0x8006/0x800B/0xF2D1 padding).
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
    let proposals = advertised("mls_proposals");
    let extensions = advertised("mls_extensions");
    let components = advertised("app_components");
    eprintln!(
        "s4: our KP advertises proposals={proposals:?} extensions={extensions:?} app_components={components:?}"
    );
    for padding in ["0xf2d1", "0x8006", "0x800b", "62161", "32774", "32779"] {
        assert!(
            !proposals
                .iter()
                .chain(&extensions)
                .chain(&components)
                .any(|v| v == padding),
            "s4 honest advertisement must not include {padding}"
        );
    }

    // (a) wn inviting our `KeyPackage` into a NEW group: the group's
    // RequiredCapabilities are computed from the founding set
    // (INTEROP.md / mdk#2628), so wn may accept with a reduced requirement
    // — record and assert the actual behaviour either way.
    let (ok_new, created) = wn_try(
        &rig.home,
        &rig.socket,
        Some(&rig.wn_npub),
        &["groups", "create", "interop-4", &a_npub],
    );
    if ok_new {
        let wn_gid = wn_group_id(&created);
        let gid = hex_group(&wn_gid);
        join_wn_group(&rig, &mut a, &gid).await;
        eprintln!("s4(a): wn accepted our KP into a fresh group {wn_gid} — join processed");
    } else {
        let text = refusal_ids(&created);
        assert!(
            text.contains("missing_required_capabilities"),
            "s4a refusal must be capability negotiation: {text}"
        );
        eprintln!("s4(a): REFUSED fresh-group invite: {text}");
    }

    // (b) wn inviting our `KeyPackage` into an EXISTING group founded by
    // members advertising the full wn set: the requirement is fixed at
    // creation, so this must name what we lack.
    let founded = wn(
        &rig.home,
        &rig.socket,
        Some(&rig.wn_npub),
        &["groups", "create", "interop-4b"],
    );
    let full_gid = wn_group_id(&founded);
    let (ok_add, added) = wn_try(
        &rig.home,
        &rig.socket,
        Some(&rig.wn_npub),
        &["groups", "add-members", &full_gid, &a_npub],
    );
    if ok_add {
        eprintln!("s4(b): wn accepted our KP into the full-capability group {full_gid}");
    } else {
        let text = refusal_ids(&added);
        assert!(
            text.contains("missing_required_capabilities"),
            "s4b refusal must be capability negotiation: {text}"
        );
        let had = added
            .pointer("/error/had")
            .map(Value::to_string)
            .unwrap_or_default();
        assert!(
            !had.contains("32774") && !had.contains("32779") && !had.contains("62161"),
            "s4b `had` must lack 0x8006/0x800B/0xF2D1: {had}"
        );
        eprintln!("s4(b): REFUSED add to full-capability group: {text}");
    }

    rig.relay.shutdown();
    eprintln!(
        "SCENARIO-4 {} {:.1}s",
        if ok_new && ok_add { "PASS" } else { "REFUSED" },
        start.elapsed().as_secs_f64()
    );
}
