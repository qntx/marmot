//! Store-level contract tests: the batch codec, schema-version handling at
//! open, commit-sequence monotonicity, transaction/savepoint semantics, and
//! reopen-from-host-rows persistence.

#![allow(
    unused_crate_dependencies,
    reason = "every integration-test binary links the full dev-dependency set"
)]
#![allow(
    clippy::unwrap_used,
    clippy::unwrap_in_result,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::cast_possible_truncation,
    clippy::many_single_char_names,
    clippy::missing_const_for_fn,
    clippy::use_self,
    clippy::default_trait_access,
    clippy::needless_for_each,
    clippy::manual_is_multiple_of,
    clippy::useless_let_if_seq,
    clippy::unneeded_struct_pattern,
    clippy::shadow_unrelated,
    clippy::missing_docs_in_private_items,
    clippy::too_many_lines,
    clippy::similar_names,
    clippy::cast_lossless,
    clippy::assertions_on_result_states,
    clippy::ignored_unit_patterns,
    clippy::items_after_statements,
    clippy::cast_possible_wrap,
    clippy::cast_sign_loss,
    clippy::redundant_clone,
    clippy::struct_excessive_bools,
    clippy::doc_markdown,
    clippy::missing_panics_doc,
    clippy::single_call_fn,
    clippy::implicit_hasher,
    clippy::redundant_type_annotations,
    clippy::field_reassign_with_default,
    clippy::tests_outside_test_module,
    clippy::clone_on_ref_ptr,
    clippy::str_to_string,
    clippy::missing_assert_message,
    reason = "test code"
)]
// `unused_crate_dependencies` counts per binary; the dev-dependency set is
// shared across all of them, so each file binds the crates it does not use.
use base64 as _;
use cgka_engine as _;
use getrandom as _;
use hex as _;
use nostr as _;
use openmls_rust_crypto as _;
use proptest as _;
use storage_sqlite as _;
use tokio as _;
use transport_nostr_peeler as _;

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use cgka_traits::capabilities::GroupCapabilities;
use cgka_traits::group::{Group, ProtocolProfile};
use cgka_traits::message::{MessageRecord, MessageState};
use cgka_traits::storage::StorageError;
use cgka_traits::storage::{GroupStorage, MessageStorage, StorageProvider};
use cgka_traits::types::{EpochId, GroupId, MessageId};
use mk_core::{BatchCodecError, KvBatch, KvHost, KvOp, KvStorageProvider, OpenError};

#[derive(Debug, Default)]
struct TestHost {
    rows: Mutex<BTreeMap<Vec<u8>, Vec<u8>>>,
    batches: Mutex<Vec<KvBatch>>,
}

impl TestHost {
    fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    fn durable_rows(&self) -> Vec<(Vec<u8>, Vec<u8>)> {
        self.rows
            .lock()
            .expect("host rows")
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }

    fn seqs(&self) -> Vec<u64> {
        self.batches
            .lock()
            .expect("host batches")
            .iter()
            .map(|batch| batch.seq.get())
            .collect()
    }

    fn batches(&self) -> Vec<KvBatch> {
        self.batches.lock().expect("host batches").clone()
    }
}

impl KvHost for TestHost {
    fn commit(&self, batch: KvBatch) {
        self.batches
            .lock()
            .expect("host batches")
            .push(batch.clone());
        let mut rows = self.rows.lock().expect("host rows");
        for op in batch.ops {
            match op {
                KvOp::Put { key, value } => {
                    rows.insert(key, value);
                }
                KvOp::Delete { key } => {
                    rows.remove(&key);
                }
            }
        }
    }
}

fn open_provider() -> (KvStorageProvider, Arc<TestHost>) {
    let host = TestHost::new();
    let provider = KvStorageProvider::open(Vec::new(), host.clone()).expect("provider opens");
    (provider, host)
}

fn gid(n: u8) -> GroupId {
    GroupId::new(vec![n; 16])
}

fn mid(n: u8) -> MessageId {
    MessageId::new(vec![n; 32])
}

fn group(n: u8) -> Group {
    Group {
        id: gid(n),
        name: format!("g{n}"),
        description: String::new(),
        epoch: EpochId(1),
        members: Vec::new(),
        required_capabilities: GroupCapabilities::default(),
        protocol_profile: ProtocolProfile::Current,
        removed: false,
        unrecoverable: false,
        disbanded: None,
        join_epoch: EpochId(1),
        local_copy_install_epoch: EpochId(1),
        self_removed_sibling_slots: Default::default(),
        local_copy_welcome_created_at: None,
        device_roster_departures: Vec::new(),
    }
}

fn record(n: u8, group: u8, state: MessageState) -> MessageRecord {
    MessageRecord {
        id: mid(n),
        group_id: gid(group),
        epoch: EpochId(1),
        state,
        payload: vec![n; 8],
        deferred_peel: None,
    }
}

// --- batch codec ---

#[test]
fn batch_codec_round_trip() {
    let (provider, host) = open_provider();
    provider.put_group(&group(1)).expect("group");
    let batch = host
        .batches()
        .into_iter()
        .nth(1)
        .expect("a committed batch");
    let decoded = KvBatch::decode(&batch.encode()).expect("decode");
    assert_eq!(decoded.seq, batch.seq);
    assert_eq!(decoded.ops.len(), batch.ops.len());
    assert_eq!(decoded.encode(), batch.encode());
}

#[test]
fn batch_codec_rejects_bad_inputs() {
    // A hand-built one-op batch: [v=1][seq=1][ops=1][tag=0][len=1][k][len=1][v]
    let mut good = vec![1u8];
    good.extend_from_slice(&1u64.to_be_bytes());
    good.extend_from_slice(&1u32.to_be_bytes());
    good.push(0);
    good.extend_from_slice(&1u32.to_be_bytes());
    good.push(b'k');
    good.extend_from_slice(&1u32.to_be_bytes());
    good.push(b'v');

    // Wrong version.
    let mut bad_version = good.clone();
    bad_version[0] = 99;
    assert!(matches!(
        KvBatch::decode(&bad_version),
        Err(BatchCodecError::UnsupportedVersion { .. })
    ));

    // Truncation at several points.
    for cut in [0, 1, 5, 9, good.len() - 1] {
        assert!(
            KvBatch::decode(&good[..cut]).is_err(),
            "truncated batch at {cut} is rejected"
        );
    }

    // Trailing bytes.
    let mut trailing = good.clone();
    trailing.push(0);
    assert!(matches!(
        KvBatch::decode(&trailing),
        Err(BatchCodecError::TrailingBytes { .. })
    ));

    // Unknown op tag: tag sits right after the op-count header (1+8+4 = 13).
    let mut bad_tag = good.clone();
    bad_tag[13] = 42;
    assert!(matches!(
        KvBatch::decode(&bad_tag),
        Err(BatchCodecError::UnknownOpTag { .. })
    ));

    // A key length that overflows the remaining input.
    let mut huge_len = good.clone();
    huge_len[14..18].copy_from_slice(&u32::MAX.to_be_bytes());
    assert!(KvBatch::decode(&huge_len).is_err());
}

#[test]
fn batch_decode_rejects_zero_ops() {
    let mut bytes = vec![1u8];
    bytes.extend_from_slice(&1u64.to_be_bytes());
    bytes.extend_from_slice(&0u32.to_be_bytes());
    assert!(KvBatch::decode(&bytes).is_err());
}

#[test]
fn batch_decode_bounds_op_count_allocation() {
    // Regression for the kv_batch fuzz OOM: a declared op count of u32::MAX
    // over a short input must fail truncated, not try to allocate for it.
    let mut bytes = vec![1u8];
    bytes.extend_from_slice(&1u64.to_be_bytes());
    bytes.extend_from_slice(&u32::MAX.to_be_bytes());
    bytes.extend_from_slice(&[0u8; 32]);
    assert!(matches!(
        KvBatch::decode(&bytes),
        Err(BatchCodecError::Truncated)
    ));
}

// --- schema version at open ---

#[test]
fn fresh_open_writes_schema_version_and_meta() {
    let (provider, host) = open_provider();
    drop(provider);
    let rows = host.durable_rows();
    // Reopen over the same rows must accept the version it wrote.
    let _provider = KvStorageProvider::open(rows, host).expect("reopen");
}

#[test]
fn open_rejects_mismatched_schema_version() {
    let (_provider, host) = open_provider();
    let mut rows = host.durable_rows();
    // Find the schema-version meta row and bump it.
    let version_key = {
        let mut k = vec![0u8]; // Table::Meta discriminant
        k.extend_from_slice(&14u32.to_be_bytes()); // len of "schema-version"
        k.extend_from_slice(b"schema-version");
        k
    };
    let mut touched = false;
    for (key, value) in &mut rows {
        if *key == version_key {
            *value = 999u32.to_be_bytes().to_vec();
            touched = true;
        }
    }
    assert!(touched, "schema-version row exists");
    assert!(matches!(
        KvStorageProvider::open(rows, host),
        Err(OpenError::UnsupportedSchemaVersion { found: 999 })
    ));
}

#[test]
fn open_rejects_rows_without_version() {
    let host = TestHost::new();
    // A stray row but no schema-version row.
    let rows = vec![(vec![42u8, 0, 0, 0, 1, 65], vec![1u8])];
    assert!(matches!(
        KvStorageProvider::open(rows, host),
        Err(OpenError::MissingSchemaVersion)
    ));
}

#[test]
fn open_rejects_malformed_version_row() {
    let host = TestHost::new();
    let mut key = vec![0u8];
    key.extend_from_slice(&14u32.to_be_bytes());
    key.extend_from_slice(b"schema-version");
    let rows = vec![(key, vec![1, 2])]; // not 4 bytes
    assert!(matches!(
        KvStorageProvider::open(rows, host),
        Err(OpenError::MalformedMeta { .. })
    ));
}

// --- commit sequence monotonicity ---

#[test]
fn batch_seq_increments_per_committed_unit() {
    let (provider, host) = open_provider();
    provider.put_group(&group(1)).expect("put group");
    provider.put_group(&group(2)).expect("put group");
    let seqs = host.seqs();
    // First batch carries the schema meta rows; each put is one more.
    assert_eq!(seqs.len(), 3);
    assert_eq!(seqs, vec![1, 2, 3]);
}

#[test]
fn empty_transaction_emits_no_batch() {
    let (provider, host) = open_provider();
    let before = host.seqs().len();
    provider
        .with_transaction(|_| Ok::<_, StorageError>(()))
        .expect("empty tx");
    assert_eq!(host.seqs().len(), before);
}

#[test]
fn nested_transaction_emits_one_batch() {
    let (provider, host) = open_provider();
    let before = host.seqs().len();
    provider
        .with_transaction(|p| {
            p.put_group(&group(1))?;
            p.with_transaction(|inner| inner.put_group(&group(2)))
        })
        .expect("nested tx");
    assert_eq!(host.seqs().len(), before + 1);
}

#[test]
fn rolled_back_transaction_emits_no_batch() {
    let (provider, host) = open_provider();
    let before = host.seqs().len();
    let result = provider.with_transaction(|p| -> Result<(), StorageError> {
        p.put_group(&group(1))?;
        Err(StorageError::Backend("boom".into()))
    });
    assert!(result.is_err());
    assert_eq!(host.seqs().len(), before);
    assert_eq!(provider.list_groups().expect("list"), Vec::new());
}

// --- savepoints ---

#[test]
fn group_savepoint_rolls_back_its_scope_only() {
    let (provider, _host) = open_provider();
    provider.put_group(&group(1)).expect("put group");
    provider
        .with_group_savepoint(&gid(1), "sp", |p| -> Result<(), StorageError> {
            p.put_message(&record(9, 1, MessageState::Created))?;
            Err(StorageError::Backend("fail".into()))
        })
        .unwrap_err();
    // The rolled-back message is gone but the group row survives.
    assert!(provider.get_group(&gid(1)).is_ok());
    assert!(matches!(
        provider.get_message(&mid(9)),
        Err(StorageError::NotFound)
    ));
}

// --- reopen ---

#[test]
fn reopen_observes_identical_state() {
    let (provider, host) = open_provider();
    provider.put_group(&group(1)).expect("group");
    provider
        .put_message(&record(5, 1, MessageState::Processed))
        .expect("message");

    let reopened = KvStorageProvider::open(host.durable_rows(), host).expect("reopen");
    assert_eq!(
        reopened.list_groups().expect("groups"),
        provider.list_groups().expect("groups")
    );
    assert_eq!(
        reopened.list_group_records().expect("records"),
        provider.list_group_records().expect("records")
    );
    let record = reopened.get_message(&mid(5)).expect("row");
    assert_eq!(record.state, MessageState::Processed);
}
