//! Session-API coverage: every public method on `Session` and
//! `PendingProof` must map to a bridge `request.type` or to an explicit
//! elsewhere surface (`open`, `ready`, `needs_account_proof`, `drain`, the
//! host clock). The method set is scraped out of `mk-core`'s
//! `session.rs` so a new public method fails here until the table is
//! updated.

#![allow(
    clippy::missing_docs_in_private_items,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::unwrap_in_result,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::needless_pass_by_value,
    clippy::tests_outside_test_module,
    clippy::shadow_reuse,
    clippy::shadow_unrelated,
    clippy::doc_markdown,
    clippy::excessive_nesting,
    clippy::too_many_lines,
    clippy::missing_const_for_fn,
    clippy::map_unwrap_or,
    clippy::future_not_send,
    reason = "test code"
)]
use std::collections::BTreeMap;

use dhat as _;
use hex as _;
use mk_bridge::protocol::REQUEST_TYPES;
use mk_core as _;
use serde as _;
use serde_json as _;
use thiserror as _;
use tokio as _;
use tracing as _;

const SESSION_RS: &str = include_str!("../../mk-core/src/session.rs");

/// How a session method is surfaced over the bridge protocol.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Mapping {
    /// A `call` `request.type` (must appear in `REQUEST_TYPES`).
    Request(&'static str),
    /// Surfaced outside `call`; the string names where.
    Elsewhere(&'static str),
}

use Mapping::{Elsewhere, Request};

/// The maintained method → surface table, checked for set equality.
const TABLE: &[(&str, &str, Mapping)] = &[
    (
        "PendingProof",
        "request",
        Elsewhere("needs_account_proof.unsigned"),
    ),
    (
        "PendingProof",
        "provide_account_proof",
        Request("submit_proof"),
    ),
    ("Session", "open", Elsewhere("open")),
    ("Session", "account", Elsewhere("ready.account")),
    ("Session", "now", Elsewhere("host clock")),
    ("Session", "acknowledge_durable", Request("ack_durable")),
    (
        "Session",
        "awaiting_durability",
        Request("awaiting_durability"),
    ),
    ("Session", "take_outbound", Request("take_outbound")),
    ("Session", "report_publish", Request("report_publish")),
    (
        "Session",
        "publish_key_package",
        Request("publish_key_package"),
    ),
    (
        "Session",
        "publish_inbox_relays",
        Request("publish_inbox_relays"),
    ),
    ("Session", "create_group", Request("create_group")),
    ("Session", "invite", Request("invite")),
    ("Session", "remove_members", Request("remove_members")),
    ("Session", "leave", Request("leave")),
    ("Session", "disband", Elsewhere("protocol v3")),
    ("Session", "enable_disbanding", Elsewhere("protocol v3")),
    ("Session", "forget_group", Elsewhere("protocol v3")),
    ("Session", "self_update", Request("self_update")),
    (
        "Session",
        "update_group_profile",
        Request("update_group_profile"),
    ),
    (
        "Session",
        "update_app_components",
        Request("update_app_components"),
    ),
    ("Session", "send_message", Request("send_message")),
    ("Session", "send_intent", Elsewhere("protocol v3")),
    ("Session", "set_admin", Elsewhere("protocol v3")),
    ("Session", "set_group_avatar", Elsewhere("protocol v3")),
    ("Session", "set_retention", Elsewhere("protocol v3")),
    ("Session", "seal_attachment", Request("seal_attachment")),
    ("Session", "send_media", Request("send_media")),
    ("Session", "open_attachment", Request("open_attachment")),
    ("Session", "ingest", Request("ingest")),
    ("Session", "ingest_batch", Request("ingest")),
    ("Session", "tick", Request("tick")),
    ("Session", "drain_events", Elsewhere("drain")),
    (
        "Session",
        "acknowledge_messages",
        Request("acknowledge_messages"),
    ),
    ("Session", "groups", Request("groups")),
    ("Session", "group", Request("group")),
    (
        "Session",
        "outbound_fanout_list_count",
        Elsewhere("testing feature"),
    ),
];

/// The public method names declared directly inside `impl <name> { ... }`
/// (brace-matched; nested impl blocks are not expected in session.rs).
fn public_methods(source: &str, impl_name: &str) -> Vec<String> {
    let needle = format!("impl {impl_name} {{");
    let start = source
        .find(&needle)
        .unwrap_or_else(|| panic!("{needle} exists in session.rs"))
        + needle.len();
    let mut depth = 1usize;
    let mut methods = Vec::new();
    for line in source[start..].lines() {
        depth += line.matches('{').count();
        depth = depth.saturating_sub(line.matches('}').count());
        if depth == 0 {
            break;
        }
        let trimmed = line.trim_start();
        for prefix in ["pub async fn ", "pub const fn ", "pub fn "] {
            if let Some(rest) = trimmed.strip_prefix(prefix) {
                let name: String = rest
                    .chars()
                    .take_while(|ch| ch.is_alphanumeric() || *ch == '_')
                    .collect();
                assert_ne!(name, "", "method name parsed");
                methods.push(name);
            }
        }
    }
    assert_eq!(depth, 0, "{impl_name} impl block is balanced");
    methods.sort();
    methods
}

#[test]
fn session_methods_all_mapped() {
    let table: BTreeMap<(&str, &str), Mapping> =
        TABLE.iter().map(|(ty, m, map)| ((*ty, *m), *map)).collect();
    for impl_name in ["Session", "PendingProof"] {
        let in_source: Vec<String> = public_methods(SESSION_RS, impl_name);
        let in_table: Vec<String> = table
            .keys()
            .filter(|(ty, _)| *ty == impl_name)
            .map(|(_, method)| (*method).to_owned())
            .collect();
        assert_eq!(
            in_source, in_table,
            "{impl_name} public methods vs coverage table"
        );
    }
    for ((ty, method), mapping) in &table {
        if let Request(tag) = mapping {
            assert!(
                REQUEST_TYPES.contains(tag),
                "{ty}::{method} maps to request `{tag}`, absent from REQUEST_TYPES"
            );
        }
    }
}
