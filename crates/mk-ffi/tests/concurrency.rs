//! Concurrent callers on one handle: jobs serialize FIFO on the engine
//! thread, so mixed traffic from several native threads gets a
//! well-formed answer for every call.

#![allow(unsafe_code, reason = "the tests exercise the C entry points")]
#![allow(
    unused_crate_dependencies,
    clippy::borrow_as_ptr,
    clippy::cast_lossless,
    clippy::cast_possible_truncation,
    clippy::clone_on_ref_ptr,
    clippy::disallowed_methods,
    clippy::doc_markdown,
    clippy::excessive_nesting,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::manual_dangling_ptr,
    clippy::manual_let_else,
    clippy::map_unwrap_or,
    clippy::match_wildcard_for_single_variants,
    clippy::missing_const_for_fn,
    clippy::missing_docs_in_private_items,
    clippy::missing_fields_in_debug,
    clippy::missing_panics_doc,
    clippy::multiple_unsafe_ops_per_block,
    clippy::needless_pass_by_value,
    clippy::option_if_let_else,
    clippy::panic,
    clippy::shadow_reuse,
    clippy::shadow_unrelated,
    clippy::significant_drop_tightening,
    clippy::tests_outside_test_module,
    clippy::too_many_lines,
    clippy::undocumented_unsafe_blocks,
    clippy::unnecessary_wraps,
    clippy::unwrap_in_result,
    clippy::unwrap_used,
    unreachable_pub,
    dead_code,
    reason = "test code"
)]

mod support;

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use marmot_ffi::{MkBridge, mk_bridge_free, mk_bridge_new};
use mk_bridge::protocol::*;
use mk_core::testing::LocalSigner;
use support::*;

/// The `now` every concurrent caller carries. Racing threads cannot
/// promise a monotonic sequence — fetch order is not arrival order —
/// but `set_now` only rejects a value strictly below the last one, so
/// one constant time stays valid under any interleaving.
const SHARED_NOW: Time = Time {
    unix_ms: 1_700_000_000_000,
    monotonic_ms: 1_000_000,
};

/// `take_outbound` + `report_publish` over raw calls — the publish
/// round a `Member`'s helper would run, shared-clock version.
fn ffi_publish(handle: *mut MkBridge, signer: &LocalSigner, relay: &mut FakeRelay) {
    for _ in 0..64 {
        let response = ffi_pumped_call(
            handle,
            signer,
            SHARED_NOW,
            &Request::TakeOutbound(NoArgs {}),
        );
        let Response::Ok { result } = response else {
            panic!("take_outbound failed: {response:?}");
        };
        let OpResult::Outbound(OutboundResult { items }) = *result else {
            panic!("take_outbound result: {result:?}");
        };
        if items.is_empty() {
            return;
        }
        for item in items {
            let results = relay.publish(&item);
            let response = ffi_pumped_call(
                handle,
                signer,
                SHARED_NOW,
                &Request::ReportPublish(ReportPublishRequest {
                    id: item.id,
                    results,
                }),
            );
            assert!(
                matches!(response, Response::Ok { .. }),
                "report_publish: {response:?}"
            );
        }
    }
    panic!("publish did not settle")
}

/// 8 threads × 25 calls of `groups`/`tick`/`awaiting_durability` against
/// one handle while the main thread sends messages: every response must
/// decode as a well-formed `ok` answer, and the total count must match.
#[test]
#[cfg_attr(miri, ignore = "miri cannot run secp256k1-sys C code (session open)")]
fn concurrent_callers_serialize_on_one_handle() {
    const THREADS: usize = 8;
    const CALLS_PER_THREAD: usize = 25;

    let secret = [7u8; 32];
    let mut relay = FakeRelay::new();
    let mut member = Member::with_secret(secret);
    member.open();
    member.publish_identity(&mut relay);
    let mut other = Member::new();
    other.open();
    other.publish_identity(&mut relay);
    let group = create_group_with(&mut member, &mut relay, &other);

    // Everything past this point shares the one constant `now` (the
    // member's own counter is lower, so it never regresses callers).
    let handle = member.handle as usize;
    let answered = Arc::new(AtomicUsize::new(0));
    let workers: Vec<_> = (0..THREADS)
        .map(|thread| {
            let answered = Arc::clone(&answered);
            std::thread::spawn(move || {
                let signer = LocalSigner::from_secret(secret).expect("signer");
                let handle = handle as *mut MkBridge;
                for index in 0..CALLS_PER_THREAD {
                    let request = match (thread + index) % 3 {
                        0 => Request::Groups(NoArgs {}),
                        1 => Request::Tick(NoArgs {}),
                        _ => Request::AwaitingDurability(NoArgs {}),
                    };
                    let response = ffi_pumped_call(handle, &signer, SHARED_NOW, &request);
                    assert!(
                        matches!(response, Response::Ok { .. }),
                        "thread {thread} call {index}: {response:?}"
                    );
                    answered.fetch_add(1, Ordering::Relaxed);
                }
            })
        })
        .collect();

    // Meanwhile the caller thread sends real traffic through the same
    // handle; the sends interleave with the readers' jobs.
    {
        let signer = LocalSigner::from_secret(secret).expect("signer");
        let handle = member.handle;
        for index in 0..20 {
            let response = ffi_pumped_call(
                handle,
                &signer,
                SHARED_NOW,
                &Request::SendMessage(SendMessageRequest {
                    group: group.clone(),
                    message: AppMessage {
                        kind: 9,
                        tags: Vec::new(),
                        content: format!("concurrent-{index}"),
                    },
                }),
            );
            assert!(
                matches!(response, Response::Ok { .. }),
                "send {index}: {response:?}"
            );
            ffi_publish(handle, &signer, &mut relay);
        }
    }
    for worker in workers {
        worker.join().expect("worker");
    }
    assert_eq!(
        answered.load(Ordering::Relaxed),
        THREADS * CALLS_PER_THREAD,
        "every call must get an answer"
    );
    // Every commit the serialized jobs produced is still seq-ordered.
    {
        let state = member.state.lock().unwrap();
        let seqs: Vec<u64> = state.history.iter().map(|batch| batch.seq.get()).collect();
        for window in seqs.windows(2) {
            assert!(
                window[1] > window[0],
                "commit seqs not increasing: {seqs:?}"
            );
        }
    }
    member.free();
    other.free();
}

/// `free` racing a storm of callers on one handle: every call resolves
/// its engine `Arc` through the registry before `free` removes it, so
/// every answer is a well-formed `not_open`/`closed`/`poisoned` envelope and the
/// detached engine still releases the host context exactly once.
#[test]
fn free_racing_callers_answers_every_call() {
    const THREADS: usize = 4;
    const CALLS_PER_THREAD: usize = 25;

    let (_state, releases, ctx) = host_ctx();
    let vtable = vtable();
    let handle = unsafe { mk_bridge_new(&vtable, ctx) };
    assert!(!handle.is_null());
    let addr = handle as usize;
    let workers: Vec<_> = (0..THREADS)
        .map(|_| {
            std::thread::spawn(move || {
                let envelope = call_envelope(SHARED_NOW, &Request::Groups(NoArgs {}));
                let handle = addr as *mut MkBridge;
                for _ in 0..CALLS_PER_THREAD {
                    let kind = error_kind_of(&ffi_call(handle, &envelope));
                    // `poisoned`: the job landed behind the shutdown `free`
                    // enqueued — the documented answer for that ordering.
                    assert!(
                        matches!(kind.as_str(), "not_open" | "closed" | "poisoned"),
                        "unexpected answer: {kind}"
                    );
                }
            })
        })
        .collect();
    unsafe { mk_bridge_free(handle) };
    for worker in workers {
        worker.join().expect("worker");
    }
    wait_for_release(&releases);
    assert_eq!(releases.load(Ordering::Relaxed), 1);
}

/// Create a group on `member` inviting `invitee`; return the group id.
fn create_group_with(member: &mut Member, relay: &mut FakeRelay, invitee: &Member) -> Vec<u8> {
    let key_package = relay.key_package_of(&invitee.account_hex());
    let response = member.call(Request::CreateGroup(CreateGroupRequest {
        name: "concurrent".to_owned(),
        description: String::new(),
        relays: vec![RELAY.to_owned()],
        invitees: vec![Invitee {
            key_package,
            inbox_relays: vec![RELAY.to_owned()],
        }],
        admins: vec![],
        components: vec![],
    }));
    let Response::Ok { result } = response else {
        panic!("create_group: {response:?}");
    };
    let OpResult::GroupCreated(created) = *result else {
        panic!("create_group result: {result:?}");
    };
    let group = created.group.id.clone();
    member.publish(relay);
    group
}
