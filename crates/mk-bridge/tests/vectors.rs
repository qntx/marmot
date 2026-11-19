//! Vector checks: every wire enum's canonical encodings live under
//! `vectors/bridge/`, one JSON object per enum mapping each variant tag to
//! one canonical encoding. Each entry must decode, re-encode byte-for-byte
//! (as `serde_json::Value`), carry its own tag as the key, and the key set
//! must equal the enum's full serde-derived variant set — so a new wire
//! variant without a vector fails here.

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
use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use dhat as _;
use hex as _;
use mk_bridge::BridgeError;
use mk_bridge::protocol::*;
use mk_core as _;
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::{Value, json};
use thiserror as _;
use tokio as _;
use tracing as _;

fn vector_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../vectors/bridge")
}

fn read_object(name: &str) -> serde_json::Map<String, Value> {
    let text = std::fs::read_to_string(vector_dir().join(name))
        .unwrap_or_else(|error| panic!("read {name}: {error}"));
    let value: Value = serde_json::from_str(&text).expect("vector file is JSON");
    value
        .as_object()
        .expect("vector file is a JSON object")
        .clone()
}

/// The full variant set serde derives for `T`: feed it an invalid tag and
/// read the `expected` list out of the error (backquoted names; the first
/// backquoted token is the invalid input itself).
fn derive_variants<T: DeserializeOwned>(probe: Value) -> BTreeSet<String> {
    let Err(error) = serde_json::from_value::<T>(probe) else {
        panic!("probe tag must not decode");
    };
    let message = error.to_string();
    message
        .split('`')
        .skip(3)
        .step_by(2)
        .map(str::to_owned)
        .collect()
}

/// The variant tag of a decoded value: the internal tag field for
/// object-form encodings, the whole string for fieldless enums.
fn variant_tag(value: &Value, tag_key: &str) -> String {
    match value {
        Value::String(tag) => tag.clone(),
        Value::Object(map) => map
            .get(tag_key)
            .and_then(Value::as_str)
            .unwrap_or_else(|| panic!("encoded variant carries `{tag_key}`"))
            .to_owned(),
        other => panic!("encoded variant is an object or string: {other}"),
    }
}

/// Check one vector file against `T`: per entry decode → re-encode →
/// `Value` equality, key equals the decoded variant's tag, and the key set
/// equals serde's full variant list.
fn check<T: DeserializeOwned + Serialize>(name: &str, tag_key: &str, probe: Value) {
    let entries = read_object(name);
    let mut keys = BTreeSet::new();
    for (key, encoded) in &entries {
        let decoded: T = serde_json::from_value(encoded.clone())
            .unwrap_or_else(|error| panic!("{name}[{key}] decodes: {error}"));
        let reencoded = serde_json::to_value(&decoded)
            .unwrap_or_else(|error| panic!("{name}[{key}] re-encodes: {error}"));
        assert_eq!(*encoded, reencoded, "{name}[{key}] round-trips");
        assert_eq!(
            *key,
            variant_tag(&reencoded, tag_key),
            "{name}[{key}] key equals its variant tag"
        );
        keys.insert(key.clone());
    }
    assert_eq!(
        keys,
        derive_variants::<T>(probe),
        "{name} key set equals serde's variant set"
    );
}

fn tagged_probe() -> Value {
    json!({"type": "\u{0}"})
}

#[test]
fn request_variants() {
    check::<Request>("request.json", "type", tagged_probe());
    let variants = derive_variants::<Request>(tagged_probe());
    let listed: BTreeSet<String> = REQUEST_TYPES.iter().map(|tag| (*tag).to_owned()).collect();
    assert_eq!(variants, listed, "REQUEST_TYPES covers every variant");
}

#[test]
fn result_variants() {
    check::<OpResult>("result.json", "type", tagged_probe());
}

#[test]
fn response_variants() {
    check::<Response>("response.json", "type", tagged_probe());
}

#[test]
fn open_response_variants() {
    check::<OpenResponse>("open_response.json", "type", tagged_probe());
}

#[test]
fn session_event_variants() {
    check::<SessionEvent>("event.json", "type", tagged_probe());
}

#[test]
fn error_variants() {
    check::<BridgeError>("error.json", "kind", json!({"kind": "\u{0}"}));
}

#[test]
fn signer_op_variants() {
    check::<SignerOp>("signer_op.json", "type", tagged_probe());
}

#[test]
fn signer_result_variants() {
    check::<SignerResult>("signer_result.json", "type", tagged_probe());
}

#[test]
fn send_outcome_variants() {
    check::<SendOutcome>("send_outcome.json", "type", tagged_probe());
}

#[test]
fn publish_resolution_variants() {
    check::<PublishResolution>(
        "publish_resolution.json",
        "type",
        Value::String("\u{0}".to_owned()),
    );
}

#[test]
fn input_outcome_variants() {
    check::<InputOutcome>("input_outcome.json", "type", tagged_probe());
}

#[test]
fn availability_category_variants() {
    check::<AvailabilityCategory>(
        "availability_category.json",
        "type",
        Value::String("\u{0}".to_owned()),
    );
}

#[test]
fn group_change_variants() {
    check::<GroupChange>("group_change.json", "type", tagged_probe());
}

#[test]
fn relay_status_variants() {
    check::<RelayStatus>(
        "relay_status.json",
        "type",
        Value::String("\u{0}".to_owned()),
    );
}

#[test]
fn outbound_purpose_variants() {
    check::<OutboundPurpose>(
        "outbound_purpose.json",
        "type",
        Value::String("\u{0}".to_owned()),
    );
}

#[test]
fn group_state_variants() {
    check::<GroupState>(
        "group_state.json",
        "type",
        Value::String("\u{0}".to_owned()),
    );
}

#[test]
fn local_state_variants() {
    check::<LocalState>(
        "local_state.json",
        "type",
        Value::String("\u{0}".to_owned()),
    );
}

#[test]
fn attachment_result_variants() {
    check::<AttachmentResult>("attachment_result.json", "type", tagged_probe());
}

#[test]
fn config_round_trip() {
    let text = std::fs::read_to_string(vector_dir().join("config.json")).expect("config.json");
    let vector: Value = serde_json::from_str(&text).expect("config.json is JSON");
    let config: Config = serde_json::from_value(vector.clone()).expect("config decodes");
    assert_eq!(
        vector,
        serde_json::to_value(config).expect("config encodes")
    );
}

#[test]
fn call_round_trip() {
    let text = std::fs::read_to_string(vector_dir().join("call.json")).expect("call.json");
    let vector: Value = serde_json::from_str(&text).expect("call.json is JSON");
    let envelope: CallEnvelope = serde_json::from_value(vector.clone()).expect("call decodes");
    assert_eq!(
        vector,
        serde_json::to_value(envelope).expect("call encodes")
    );
}
