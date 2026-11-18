//! Host-callback ordering: `commit` invocations arrive strictly in `seq`
//! order, they run on the engine thread (never the caller's), and every
//! commit a call produces is delivered before that call returns.

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

use mk_bridge::protocol::*;
use support::*;

/// Every `commit` lands on the `mk-engine` thread — not the calling
/// thread — and batch `seq`s are strictly increasing in delivery order.
#[test]
#[cfg_attr(miri, ignore = "miri cannot run secp256k1-sys C code (session open)")]
fn commits_run_on_engine_thread_in_seq_order() {
    let mut relay = FakeRelay::new();
    let mut member = Member::new();
    let caller = std::thread::current().id();
    member.open();
    member.publish_identity(&mut relay);
    member.call(Request::Tick(NoArgs {}));

    let state = member.state.lock().unwrap();
    assert!(
        state.history.len() >= 2,
        "the open + publish flow must commit batches"
    );
    let seqs: Vec<u64> = state.history.iter().map(|batch| batch.seq.get()).collect();
    for window in seqs.windows(2) {
        assert!(
            window[1] > window[0],
            "commit seqs not increasing: {seqs:?}"
        );
    }
    assert!(
        state.commit_threads.iter().all(|name| name == "mk-engine"),
        "commits off the engine thread: {:?}",
        state.commit_threads
    );
    assert!(
        state.commit_thread_ids.iter().all(|id| *id != caller),
        "a commit ran on the caller thread"
    );
}

/// A commit produced while servicing a call is delivered before that
/// ABI call returns: the host records each batch synchronously inside
/// `commit`, so the observed history after `call` returns must already
/// cover everything the call's own `awaiting_durability` reports.
#[test]
#[cfg_attr(miri, ignore = "miri cannot run secp256k1-sys C code (session open)")]
fn commits_are_delivered_before_the_call_returns() {
    let mut relay = FakeRelay::new();
    let mut member = Member::new();
    member.open();
    member.publish_identity(&mut relay);
    let delivered = member.state.lock().unwrap().history.len();
    assert!(delivered > 0, "publish_identity commits nothing?");
    let response = member.call(Request::AwaitingDurability(NoArgs {}));
    let Response::Ok { result } = &response else {
        panic!("awaiting_durability failed: {response:?}");
    };
    let OpResult::AwaitingDurability(AwaitingDurabilityResult { seq }) = **result else {
        panic!("awaiting_durability result: {result:?}");
    };
    let state = member.state.lock().unwrap();
    let max_seq = state
        .history
        .iter()
        .map(|batch| batch.seq.get())
        .max()
        .expect("commits exist");
    if let Some(seq) = seq {
        assert!(
            seq <= max_seq,
            "awaited seq {seq} exceeds delivered max {max_seq} — a commit \
             escaped the call that produced it"
        );
    }
}
