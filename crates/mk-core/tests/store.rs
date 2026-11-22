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
use chacha20poly1305 as _;
use getrandom as _;
use hex as _;
use hkdf as _;
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
use cgka_traits::storage::{
    ChangeNotDelivered, ChangeOrigin, CommittedMembers, CommittedRosterStorage,
    CommittedRosterWrite, GroupStateCheckpointRef, GroupStorage, MessageStorage,
    OutboundIntentStorage, RosterRevision, StorageError, StorageProvider,
};
use cgka_traits::transport::Timestamp;
use cgka_traits::types::{EpochId, GroupId, MemberId, MessageId};
use mk_core::{BatchCodecError, KvBatch, KvHost, KvOp, KvStorageProvider, OpenError};
use openmls::prelude::GroupId as MlsGroupId;
use openmls_traits::storage::{
    CURRENT_VERSION, Entity as MlsEntity, StorageProvider as MlsStorage, traits as mls_traits,
};
use serde::{Deserialize, Serialize};

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
    provider.put_group_keeping_roster(&group(1)).expect("group");
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
fn open_rejects_version_one_rows() {
    // Version 1 was the pre-index message layout; it is refused at open,
    // never migrated.
    let (_provider, host) = open_provider();
    let mut rows = host.durable_rows();
    let version_key = {
        let mut k = vec![0u8]; // Table::Meta discriminant
        k.extend_from_slice(&14u32.to_be_bytes()); // len of "schema-version"
        k.extend_from_slice(b"schema-version");
        k
    };
    let mut touched = false;
    for (key, value) in &mut rows {
        if *key == version_key {
            *value = 1u32.to_be_bytes().to_vec();
            touched = true;
        }
    }
    assert!(touched, "schema-version row exists");
    assert!(matches!(
        KvStorageProvider::open(rows, host),
        Err(OpenError::UnsupportedSchemaVersion { found: 1 })
    ));
}

#[test]
fn open_rejects_version_two_rows() {
    // Version 2 was the decimal-array `Group` row encoding; it is refused
    // at open, never migrated.
    let (_provider, host) = open_provider();
    let mut rows = host.durable_rows();
    let version_key = {
        let mut k = vec![0u8]; // Table::Meta discriminant
        k.extend_from_slice(&14u32.to_be_bytes()); // len of "schema-version"
        k.extend_from_slice(b"schema-version");
        k
    };
    let mut touched = false;
    for (key, value) in &mut rows {
        if *key == version_key {
            *value = 2u32.to_be_bytes().to_vec();
            touched = true;
        }
    }
    assert!(touched, "schema-version row exists");
    assert!(matches!(
        KvStorageProvider::open(rows, host),
        Err(OpenError::UnsupportedSchemaVersion { found: 2 })
    ));
}

#[test]
fn open_rejects_version_three_rows() {
    // Version 3 was the decimal-array `Group` row layout with
    // `Table::OutboundRetry`; it is refused at open, never migrated.
    let (_provider, host) = open_provider();
    let mut rows = host.durable_rows();
    let version_key = {
        let mut k = vec![0u8]; // Table::Meta discriminant
        k.extend_from_slice(&14u32.to_be_bytes()); // len of "schema-version"
        k.extend_from_slice(b"schema-version");
        k
    };
    let mut touched = false;
    for (key, value) in &mut rows {
        if *key == version_key {
            *value = 3u32.to_be_bytes().to_vec();
            touched = true;
        }
    }
    assert!(touched, "schema-version row exists");
    assert!(matches!(
        KvStorageProvider::open(rows, host),
        Err(OpenError::UnsupportedSchemaVersion { found: 3 })
    ));
}

#[test]
fn open_rejects_version_four_rows() {
    // Version 4 predates the delivered-message index and send-pacing
    // tables; it is refused at open, never migrated.
    let (_provider, host) = open_provider();
    let mut rows = host.durable_rows();
    let version_key = {
        let mut k = vec![0u8]; // Table::Meta discriminant
        k.extend_from_slice(&14u32.to_be_bytes()); // len of "schema-version"
        k.extend_from_slice(b"schema-version");
        k
    };
    let mut touched = false;
    for (key, value) in &mut rows {
        if *key == version_key {
            *value = 4u32.to_be_bytes().to_vec();
            touched = true;
        }
    }
    assert!(touched, "schema-version row exists");
    assert!(matches!(
        KvStorageProvider::open(rows, host),
        Err(OpenError::UnsupportedSchemaVersion { found: 4 })
    ));
}

#[test]
fn open_rejects_version_five_rows() {
    // Version 5 predates the changes-not-delivered table; it is refused at
    // open, never migrated.
    let (_provider, host) = open_provider();
    let mut rows = host.durable_rows();
    let version_key = {
        let mut k = vec![0u8]; // Table::Meta discriminant
        k.extend_from_slice(&14u32.to_be_bytes()); // len of "schema-version"
        k.extend_from_slice(b"schema-version");
        k
    };
    let mut touched = false;
    for (key, value) in &mut rows {
        if *key == version_key {
            *value = 5u32.to_be_bytes().to_vec();
            touched = true;
        }
    }
    assert!(touched, "schema-version row exists");
    assert!(matches!(
        KvStorageProvider::open(rows, host),
        Err(OpenError::UnsupportedSchemaVersion { found: 5 })
    ));
}

#[test]
fn open_rejects_version_six_rows() {
    // Version 6 predates the KeyPackage lifecycle slot (`key-package-slot`
    // meta row removed) and the `key_package_republish` purpose; it is
    // refused at open, never migrated.
    let (_provider, host) = open_provider();
    let mut rows = host.durable_rows();
    let version_key = {
        let mut k = vec![0u8]; // Table::Meta discriminant
        k.extend_from_slice(&14u32.to_be_bytes()); // len of "schema-version"
        k.extend_from_slice(b"schema-version");
        k
    };
    let mut touched = false;
    for (key, value) in &mut rows {
        if *key == version_key {
            *value = 6u32.to_be_bytes().to_vec();
            touched = true;
        }
    }
    assert!(touched, "schema-version row exists");
    assert!(matches!(
        KvStorageProvider::open(rows, host),
        Err(OpenError::UnsupportedSchemaVersion { found: 6 })
    ));
}

#[test]
fn open_rejects_version_seven_rows() {
    // Version 7 predates the D2 stall-detector row (`Table::GroupStall`);
    // it is refused at open, never migrated.
    let (_provider, host) = open_provider();
    let mut rows = host.durable_rows();
    let version_key = {
        let mut k = vec![0u8]; // Table::Meta discriminant
        k.extend_from_slice(&14u32.to_be_bytes()); // len of "schema-version"
        k.extend_from_slice(b"schema-version");
        k
    };
    let mut touched = false;
    for (key, value) in &mut rows {
        if *key == version_key {
            *value = 7u32.to_be_bytes().to_vec();
            touched = true;
        }
    }
    assert!(touched, "schema-version row exists");
    assert!(matches!(
        KvStorageProvider::open(rows, host),
        Err(OpenError::UnsupportedSchemaVersion { found: 7 })
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
    provider
        .put_group_keeping_roster(&group(1))
        .expect("put group");
    provider
        .put_group_keeping_roster(&group(2))
        .expect("put group");
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
            p.put_group_keeping_roster(&group(1))?;
            p.with_transaction(|inner| inner.put_group_keeping_roster(&group(2)))
        })
        .expect("nested tx");
    assert_eq!(host.seqs().len(), before + 1);
}

#[test]
fn rolled_back_transaction_emits_no_batch() {
    let (provider, host) = open_provider();
    let before = host.seqs().len();
    let result = provider.with_transaction(|p| -> Result<(), StorageError> {
        p.put_group_keeping_roster(&group(1))?;
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
    provider
        .put_group_keeping_roster(&group(1))
        .expect("put group");
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

// --- engine write generation (`mls_write_generation`) ---
//
// The generation the engine reuses a cached `MlsGroup` under: the low half
// counts `OpenMLS`-row writes at land time, the high half counts changes no
// cached object could have written through (discarded units, restores).

/// Serializable stand-in for `OpenMLS` entities, like model.rs's `V`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct V(u8);

impl MlsEntity<CURRENT_VERSION> for V {}
impl mls_traits::TreeSync<CURRENT_VERSION> for V {}

fn mls_gid(n: u8) -> MlsGroupId {
    MlsGroupId::from_slice(gid(n).as_slice())
}

fn generation(provider: &KvStorageProvider) -> u64 {
    provider.mls_write_generation().expect("generation tracked")
}

fn openmls_put(provider: &KvStorageProvider, g: u8, v: u8) {
    provider
        .mls_storage()
        .write_tree::<MlsGroupId, V>(&mls_gid(g), &V(v))
        .expect("openmls write");
}

/// A write to a non-`OpenMLS` table never moves the generation — bare or
/// inside a committed unit.
#[test]
fn non_openmls_writes_leave_generation() {
    let (provider, _host) = open_provider();
    let before = generation(&provider);
    provider.put_group_keeping_roster(&group(1)).expect("group");
    provider
        .put_message(&record(1, 1, MessageState::Created))
        .expect("message");
    provider
        .with_transaction(|p| {
            p.put_group_keeping_roster(&group(2))?;
            p.put_message(&record(2, 2, MessageState::Created))
        })
        .expect("tx");
    assert_eq!(generation(&provider), before);
}

/// An `OpenMLS` write moves the generation at write time — bare, and inside
/// a unit before its commit lands; the commit itself moves nothing.
#[test]
fn openmls_writes_move_generation() {
    let (provider, _host) = open_provider();
    let before = generation(&provider);
    openmls_put(&provider, 1, 7);
    let after_bare = generation(&provider);
    assert_ne!(after_bare, before);

    let mut inside = before;
    provider
        .with_transaction(|p| {
            assert_eq!(
                generation(p),
                after_bare,
                "an unrelated commit moved nothing"
            );
            openmls_put(p, 1, 8);
            inside = generation(p);
            assert_ne!(inside, after_bare, "the write moved it inside the unit");
            Ok::<_, StorageError>(())
        })
        .expect("tx");
    assert_eq!(
        generation(&provider),
        inside,
        "the commit itself moves nothing"
    );
}

/// A rolled-back unit that held an `OpenMLS` write moves the generation
/// again — the reset half — so a cached object loaded inside the unit
/// cannot read as current. A rollback of non-`OpenMLS` writes alone
/// changes nothing.
#[test]
fn rollback_after_openmls_write_moves_generation() {
    let (provider, _host) = open_provider();
    provider.put_group_keeping_roster(&group(1)).expect("group");
    let before = generation(&provider);
    let mut inside = before;
    let result = provider.with_transaction(|p| -> Result<(), StorageError> {
        openmls_put(p, 1, 7);
        inside = generation(p);
        Err(StorageError::Backend("abort".into()))
    });
    assert!(result.is_err());
    assert_ne!(inside, before, "the write moved it inside the unit");
    let after = generation(&provider);
    assert_ne!(after, before, "the discarded unit moved it again");
    assert_ne!(after, inside, "the reset half moved too");

    let before = generation(&provider);
    let result = provider.with_transaction(|p| -> Result<(), StorageError> {
        p.put_message(&record(3, 1, MessageState::Created))?;
        Err(StorageError::Backend("abort".into()))
    });
    assert!(result.is_err());
    assert_eq!(generation(&provider), before, "no OpenMLS write, no move");
}

/// The same for `with_group_savepoint` — a real rollback scope even nested
/// inside a committed transaction — and a committed savepoint inside a
/// rolled-back transaction still counts, since its writes die with the
/// outer discard.
#[test]
fn savepoint_rollback_after_openmls_write_moves_generation() {
    let (provider, _host) = open_provider();
    provider.put_group_keeping_roster(&group(1)).expect("group");

    let before = generation(&provider);
    let result = provider.with_group_savepoint(&gid(1), "sp", |p| -> Result<(), StorageError> {
        openmls_put(p, 1, 7);
        Err(StorageError::Backend("abort".into()))
    });
    assert!(result.is_err());
    assert_ne!(generation(&provider), before);

    // Rolled-back savepoint inside a committed transaction.
    let before = generation(&provider);
    provider
        .with_transaction(|p| {
            let inner = p.with_group_savepoint(&gid(1), "sp", |s| -> Result<(), StorageError> {
                openmls_put(s, 1, 8);
                Err(StorageError::Backend("abort".into()))
            });
            assert!(inner.is_err());
            Ok::<_, StorageError>(())
        })
        .expect("outer commits");
    assert_ne!(generation(&provider), before);

    // Committed savepoint inside a rolled-back transaction: the write it
    // folded up dies with the outer discard, so the generation still moves.
    let before = generation(&provider);
    let result = provider.with_transaction(|p| -> Result<(), StorageError> {
        p.with_group_savepoint(&gid(1), "sp", |s| -> Result<(), StorageError> {
            openmls_put(s, 1, 9);
            Ok(())
        })?;
        Err(StorageError::Backend("outer abort".into()))
    });
    assert!(result.is_err());
    assert_ne!(generation(&provider), before);
}

/// Every group `OpenMLS` restore moves the generation even when the
/// restored rows equal the live rows — snapshot, state-snapshot,
/// checkpoint, and keeping-live restores all funnel through the reset.
#[test]
fn openmls_restores_move_generation() {
    let (provider, _host) = open_provider();
    provider.put_group_keeping_roster(&group(1)).expect("group");
    openmls_put(&provider, 1, 7);
    provider
        .create_group_snapshot(&gid(1), "snap")
        .expect("snapshot");
    provider
        .create_group_state_snapshot(&gid(1), "ss")
        .expect("state snapshot");
    provider
        .create_group_state_checkpoint(
            &gid(1),
            &GroupStateCheckpointRef {
                id: "cp".to_owned(),
                resulting_epoch: EpochId(7),
            },
        )
        .expect("checkpoint");

    let check = |label: &str, restore: &mut dyn FnMut(&KvStorageProvider)| {
        let before = generation(&provider);
        restore(&provider);
        assert_ne!(generation(&provider), before, "{label} moved it");
    };
    check("rollback_group_to_snapshot", &mut |p| {
        p.rollback_group_to_snapshot(&gid(1), "snap")
            .expect("restore");
    });
    check("rollback_group_state_to_snapshot", &mut |p| {
        p.rollback_group_state_to_snapshot(&gid(1), "ss")
            .expect("restore");
    });
    check("restore_group_state_checkpoint", &mut |p| {
        p.restore_group_state_checkpoint(&gid(1), "cp")
            .expect("restore");
    });
    check("rollback_group_state_keeping_live_records", &mut |p| {
        p.rollback_group_state_keeping_live_records(&gid(1), "snap")
            .expect("restore");
    });
}

/// A write refused inside `with_read_snapshot` moves nothing.
#[test]
fn refused_write_inside_read_snapshot_leaves_generation() {
    let (provider, _host) = open_provider();
    let before = generation(&provider);
    provider
        .with_read_snapshot(|p| -> Result<(), StorageError> {
            assert!(
                p.mls_storage()
                    .write_tree::<MlsGroupId, V>(&mls_gid(1), &V(9))
                    .is_err(),
                "writes are refused inside a read snapshot"
            );
            Ok(())
        })
        .expect("read snapshot");
    assert_eq!(generation(&provider), before);
}

// --- committed rosters (mdk#3273) ---
//
// The KV mirrors of storage-sqlite's committed_rosters tests: one row per
// group (revision + normalized members), one account-wide counter row.

fn members(ids: &[u8]) -> CommittedMembers {
    CommittedMembers::from_ids(ids.iter().map(|id| MemberId::new(vec![*id; 32])))
}

fn revision(write: CommittedRosterWrite) -> u64 {
    match write {
        CommittedRosterWrite::Unchanged(revision)
        | CommittedRosterWrite::Recorded(revision)
        | CommittedRosterWrite::Replaced(revision) => revision.0,
    }
}

/// A roster write moves the revision exactly when it changes the members,
/// and a known empty roster differs from no roster.
#[test]
fn roster_revision_moves_only_when_members_change() {
    let (provider, _host) = open_provider();
    provider
        .put_group_with_roster(&group(1), &members(&[0, 1, 2]))
        .expect("group + roster");
    let recorded = provider.committed_roster(&gid(1)).expect("roster").unwrap();
    assert_eq!(recorded.members, members(&[0, 1, 2]));

    let again = provider
        .put_committed_roster(&gid(1), &members(&[2, 0, 1, 0]))
        .expect("unchanged");
    assert_eq!(again, CommittedRosterWrite::Unchanged(recorded.revision));

    let replaced = provider
        .put_committed_roster(&gid(1), &members(&[0, 3]))
        .expect("replaced");
    assert!(
        matches!(replaced, CommittedRosterWrite::Replaced(rev) if rev > recorded.revision),
        "{replaced:?}"
    );
    let stored = provider.committed_roster(&gid(1)).expect("roster").unwrap();
    assert_eq!(stored.members, members(&[0, 3]));
    assert_eq!(revision(replaced), stored.revision.0);

    let emptied = provider
        .put_committed_roster(&gid(1), &members(&[]))
        .expect("emptied");
    let stored = provider.committed_roster(&gid(1)).expect("roster").unwrap();
    assert_eq!(stored.members.ids(), []);
    assert_eq!(revision(emptied), stored.revision.0);
    assert!(stored.revision.0 > revision(replaced));

    // `put_group_keeping_roster` writes no roster.
    provider.put_group_keeping_roster(&group(2)).expect("group");
    assert_eq!(provider.committed_roster(&gid(2)).expect("roster"), None);
    assert_eq!(
        provider
            .committed_roster_revision(&gid(2))
            .expect("revision"),
        None
    );
}

/// A group forgotten or deleted and then joined again gets a revision no
/// earlier copy held.
#[test]
fn forgotten_or_deleted_group_rejoined_never_reuses_revision() {
    let (provider, _host) = open_provider();
    provider
        .put_group_with_roster(&group(1), &members(&[0]))
        .expect("group + roster");
    let mut highest = revision(
        provider
            .put_committed_roster(&gid(1), &members(&[0, 1]))
            .expect("roster"),
    );

    assert!(
        provider
            .forget_group_local(&gid(1), Timestamp(10))
            .expect("forget")
    );
    assert_eq!(provider.committed_roster(&gid(1)).expect("roster"), None);
    provider.complete_group_local_reset(&gid(1)).expect("reset");
    provider
        .put_group_with_roster(&group(1), &members(&[0]))
        .expect("rejoin");
    let rejoined = provider.committed_roster(&gid(1)).expect("roster").unwrap();
    assert!(
        rejoined.revision.0 > highest,
        "{rejoined:?} after {highest}"
    );
    highest = rejoined.revision.0;

    provider.delete_group(&gid(1)).expect("delete");
    assert_eq!(provider.committed_roster(&gid(1)).expect("roster"), None);
    provider
        .put_group_with_roster(&group(1), &members(&[0]))
        .expect("rejoin");
    let rejoined = provider.committed_roster(&gid(1)).expect("roster").unwrap();
    assert!(
        rejoined.revision.0 > highest,
        "{rejoined:?} after {highest}"
    );
}

/// Rolling a group back onto a snapshot restores its record, never its
/// committed roster.
#[test]
fn group_rollback_leaves_committed_roster() {
    let (provider, _host) = open_provider();
    let anchor = group(1);
    provider
        .put_group_with_roster(&anchor, &members(&[0]))
        .expect("anchor");
    provider
        .create_group_snapshot(&gid(1), "anchor")
        .expect("snapshot");

    let mut live = group(1);
    live.epoch = EpochId(2);
    live.name = "live".to_owned();
    provider
        .put_group_with_roster(&live, &members(&[0, 1]))
        .expect("live");
    let roster = provider.committed_roster(&gid(1)).expect("roster").unwrap();
    provider
        .rollback_group_to_snapshot(&gid(1), "anchor")
        .expect("rollback");

    assert_eq!(provider.get_group(&gid(1)).expect("group"), anchor);
    assert_eq!(
        provider.committed_roster(&gid(1)).expect("roster"),
        Some(roster)
    );
}

/// A roster write refused mid-unit takes the group record with it: a
/// `put_group_with_roster` whose roster half cannot complete leaves no
/// group row behind.
#[test]
fn failed_roster_write_leaves_no_record() {
    let (provider, host) = open_provider();
    // Corrupt the would-be roster row for gid(1) directly in the durable
    // rows (Table::CommittedRoster discriminant 37 + one gid segment), then
    // reopen so the provider's mirror holds the bad value.
    let mut key = vec![37u8];
    key.extend_from_slice(&16u32.to_be_bytes());
    key.extend_from_slice(gid(1).as_slice());
    host.rows
        .lock()
        .expect("host rows")
        .insert(key, b"not-a-roster".to_vec());
    drop(provider);
    let provider = KvStorageProvider::open(host.durable_rows(), host).expect("reopen");

    let result = provider.put_group_with_roster(&group(1), &members(&[0, 1]));
    assert!(
        matches!(&result, Err(StorageError::Serialization(_))),
        "{result:?}"
    );
    assert!(matches!(
        provider.get_group(&gid(1)),
        Err(StorageError::NotFound)
    ));
}

/// The roster write joins the caller's transaction: a later step that
/// fails takes the record and its roster back together.
#[test]
fn roster_write_rolls_back_with_callers_transaction() {
    let (provider, _host) = open_provider();
    let result = provider.with_transaction(|p| -> Result<(), StorageError> {
        p.put_group_with_roster(&group(1), &members(&[0, 1]))?;
        Err(StorageError::Backend("later step refused".into()))
    });
    assert!(
        matches!(&result, Err(StorageError::Backend(reason)) if reason == "later step refused"),
        "{result:?}"
    );
    assert!(matches!(
        provider.get_group(&gid(1)),
        Err(StorageError::NotFound)
    ));
    assert_eq!(provider.committed_roster(&gid(1)).expect("roster"), None);
}

/// A refused `put_committed_roster` is its own unit inside a caller's
/// transaction: it leaves no roster, consumes no revision, and the caller
/// still commits — matching sqlite's savepoint.
#[test]
fn refused_roster_write_inside_committed_transaction_leaves_nothing() {
    let (provider, _host) = open_provider();
    provider.put_group_keeping_roster(&group(1)).expect("group");
    provider
        .with_transaction(|p| {
            let refused = p.put_committed_roster(&gid(2), &members(&[0, 1]));
            assert!(
                matches!(&refused, Err(StorageError::Backend(_))),
                "{refused:?}"
            );
            Ok::<_, StorageError>(())
        })
        .expect("caller commits");
    assert_eq!(provider.committed_roster(&gid(2)).expect("roster"), None);
    assert_eq!(
        provider
            .committed_roster_revision(&gid(2))
            .expect("revision"),
        None
    );
    assert_eq!(provider.get_group(&gid(1)).expect("group"), group(1));
    // The refused write consumed no revision: the first roster still
    // takes revision 1.
    assert_eq!(
        provider
            .put_committed_roster(&gid(1), &members(&[0]))
            .expect("roster"),
        CommittedRosterWrite::Recorded(RosterRevision(1))
    );
}

// --- changes not delivered (mdk#2332) ---
//
// The KV mirror of storage-sqlite's `cgka_changes_not_delivered` (migration
// 0210): one row per rolled-back commit, keyed by commit id account-wide,
// listed per group in insertion order, kept until acknowledged or the group
// record goes — and outside every snapshot and checkpoint.

fn change_not_delivered(commit: u8, group: u8) -> ChangeNotDelivered {
    ChangeNotDelivered {
        group_id: gid(group),
        commit_id: mid(commit),
        origin: ChangeOrigin::Direct,
        intent_kind: "update_group_data".to_owned(),
        reason: "not_delivered".to_owned(),
        at_ms: 1_000,
    }
}

/// Inserts list oldest-first; a delete frees the commit id and the reinsert
/// takes a new position at the tail.
#[test]
fn changes_not_delivered_list_in_insertion_order() {
    let (provider, _host) = open_provider();
    provider.put_group_keeping_roster(&group(1)).expect("group");
    provider.put_group_keeping_roster(&group(2)).expect("group");
    provider
        .put_change_not_delivered(&change_not_delivered(1, 1))
        .expect("record 1");
    provider
        .put_change_not_delivered(&change_not_delivered(2, 1))
        .expect("record 2");
    provider
        .put_change_not_delivered(&change_not_delivered(3, 2))
        .expect("record 3");
    assert_eq!(
        provider
            .changes_not_delivered(&gid(1))
            .expect("list")
            .iter()
            .map(|record| record.commit_id.clone())
            .collect::<Vec<_>>(),
        vec![mid(1), mid(2)]
    );

    assert!(
        provider
            .delete_change_not_delivered(&gid(1), &mid(1))
            .expect("delete")
    );
    provider
        .put_change_not_delivered(&change_not_delivered(1, 1))
        .expect("reinsert");
    assert_eq!(
        provider
            .changes_not_delivered(&gid(1))
            .expect("list")
            .iter()
            .map(|record| record.commit_id.clone())
            .collect::<Vec<_>>(),
        vec![mid(2), mid(1)]
    );
    assert_eq!(
        provider
            .changes_not_delivered(&gid(2))
            .expect("list")
            .iter()
            .map(|record| record.commit_id.clone())
            .collect::<Vec<_>>(),
        vec![mid(3)]
    );
}

/// `ON CONFLICT(commit_id) DO NOTHING`: a second put under the same commit
/// id — even for another group — keeps the first record.
#[test]
fn duplicate_change_not_delivered_keeps_first() {
    let (provider, _host) = open_provider();
    provider.put_group_keeping_roster(&group(1)).expect("group");
    provider.put_group_keeping_roster(&group(2)).expect("group");
    provider
        .put_change_not_delivered(&change_not_delivered(1, 1))
        .expect("first");
    let mut second = change_not_delivered(1, 2);
    second.reason = "refused".to_owned();
    provider
        .put_change_not_delivered(&second)
        .expect("kept as the first");
    assert_eq!(
        provider.changes_not_delivered(&gid(1)).expect("list"),
        vec![change_not_delivered(1, 1)]
    );
    assert_eq!(
        provider.changes_not_delivered(&gid(2)).expect("list"),
        Vec::<ChangeNotDelivered>::new()
    );
}

/// The delete names the group: true once, false for a repeat and for a
/// commit id stored under another group.
#[test]
fn delete_change_not_delivered_scoped_by_group() {
    let (provider, _host) = open_provider();
    provider.put_group_keeping_roster(&group(1)).expect("group");
    provider.put_group_keeping_roster(&group(2)).expect("group");
    provider
        .put_change_not_delivered(&change_not_delivered(1, 1))
        .expect("record");
    assert!(
        !provider
            .delete_change_not_delivered(&gid(2), &mid(1))
            .expect("wrong group")
    );
    assert!(
        provider
            .delete_change_not_delivered(&gid(1), &mid(1))
            .expect("delete")
    );
    assert!(
        !provider
            .delete_change_not_delivered(&gid(1), &mid(1))
            .expect("again")
    );
    assert!(
        !provider
            .delete_change_not_delivered(&gid(1), &mid(9))
            .expect("absent")
    );
}

/// Rewinds neither drop nor restore a record: snapshot rollback, state
/// rollback and checkpoint restore all leave the table alone.
#[test]
fn changes_not_delivered_survive_rewinds() {
    let (provider, _host) = open_provider();
    provider.put_group_keeping_roster(&group(1)).expect("group");
    provider
        .put_change_not_delivered(&change_not_delivered(1, 1))
        .expect("earlier record");
    provider
        .create_group_snapshot(&gid(1), "snap")
        .expect("snapshot");
    provider
        .create_group_state_checkpoint(
            &gid(1),
            &GroupStateCheckpointRef {
                id: "cp".to_owned(),
                resulting_epoch: EpochId(7),
            },
        )
        .expect("checkpoint");
    provider
        .put_change_not_delivered(&change_not_delivered(2, 1))
        .expect("later record");

    provider
        .rollback_group_to_snapshot(&gid(1), "snap")
        .expect("rollback");
    assert_eq!(
        provider
            .changes_not_delivered(&gid(1))
            .expect("list")
            .iter()
            .map(|record| record.commit_id.clone())
            .collect::<Vec<_>>(),
        vec![mid(1), mid(2)]
    );
    provider
        .restore_group_state_checkpoint(&gid(1), "cp")
        .expect("checkpoint restore");
    assert_eq!(
        provider.changes_not_delivered(&gid(1)).expect("list").len(),
        2
    );
}

/// Rows go with the group record: `delete_group` and `forget_group_local`
/// both cascade them.
#[test]
fn changes_not_delivered_go_with_the_group() {
    let (provider, _host) = open_provider();
    provider.put_group_keeping_roster(&group(1)).expect("group");
    provider.put_group_keeping_roster(&group(2)).expect("group");
    provider
        .put_change_not_delivered(&change_not_delivered(1, 1))
        .expect("record 1");
    provider
        .put_change_not_delivered(&change_not_delivered(2, 2))
        .expect("record 2");

    provider.delete_group(&gid(1)).expect("delete");
    assert_eq!(
        provider.changes_not_delivered(&gid(1)).expect("list"),
        Vec::<ChangeNotDelivered>::new()
    );
    assert_eq!(
        provider.changes_not_delivered(&gid(2)).expect("list").len(),
        1
    );

    provider
        .forget_group_local(&gid(2), Timestamp(10))
        .expect("forget");
    assert_eq!(
        provider.changes_not_delivered(&gid(2)).expect("list"),
        Vec::<ChangeNotDelivered>::new()
    );
}

/// A put inside a caller's transaction rolls back with it; sqlite's
/// `FOREIGN KEY(group_id)` refuses a record for a missing group.
#[test]
fn change_not_delivered_write_rolls_back_and_needs_the_group() {
    let (provider, _host) = open_provider();
    provider.put_group_keeping_roster(&group(1)).expect("group");
    let result = provider.with_transaction(|p| -> Result<(), StorageError> {
        p.put_change_not_delivered(&change_not_delivered(1, 1))?;
        Err(StorageError::Backend("later step refused".into()))
    });
    assert!(
        matches!(&result, Err(StorageError::Backend(reason)) if reason == "later step refused"),
        "{result:?}"
    );
    assert_eq!(
        provider.changes_not_delivered(&gid(1)).expect("list"),
        Vec::<ChangeNotDelivered>::new()
    );

    let refused = provider.put_change_not_delivered(&change_not_delivered(2, 9));
    assert!(
        matches!(&refused, Err(StorageError::Backend(reason)) if reason == "FOREIGN KEY constraint failed"),
        "{refused:?}"
    );
}

/// `mls_storage_failure` and `has_atomic_group_savepoints` mirror sqlite:
/// a stored value that did not decode is not a storage failure; the
/// overlay savepoint is atomic.
#[test]
fn storage_failure_classification_and_atomicity() {
    let (provider, _host) = open_provider();
    assert!(provider.has_atomic_group_savepoints());
    assert!(
        provider
            .mls_storage_failure(&StorageError::Serialization("bad row".into()))
            .is_none()
    );
    assert!(matches!(
        provider.mls_storage_failure(&StorageError::Backend("io".into())),
        Some(StorageError::Backend(reason)) if reason == "io"
    ));
}

// --- reopen ---

#[test]
fn reopen_observes_identical_state() {
    let (provider, host) = open_provider();
    provider.put_group_keeping_roster(&group(1)).expect("group");
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

#[cfg(feature = "testing")]
mod support;

/// "Decodes nothing it doesn't return": with one Processed
/// application-classified row's value corrupted, every index-driven query
/// that does not return that row must still answer correctly.
#[cfg(feature = "testing")]
mod indexed_queries {
    use cgka_traits::message::MessageState;
    use cgka_traits::storage::{MessageStorage, OpenMlsContentFilter};
    use cgka_traits::types::{EpochId, GroupId as EngineGroupId};
    use mk_core::{CreateGroup, Invitee, KvStorageProvider};

    use super::TestHost;
    use super::support::*;

    const RELAY: &str = "ws://127.0.0.1:7777";
    const TABLE_MESSAGE: u8 = 3;
    const TABLE_MESSAGE_STATE: u8 = 5;
    const STATE_PROCESSED: u8 = 2;
    const CLASS_APPLICATION: u8 = 3;

    /// Decode `keys.rs`'s `[table][u32 len + segment]...` layout.
    fn segments(key: &[u8]) -> Vec<&[u8]> {
        let mut rest = &key[1..];
        let mut out = Vec::new();
        while !rest.is_empty() {
            let len = u32::from_be_bytes(rest[..4].try_into().expect("len")) as usize;
            out.push(&rest[4..4 + len]);
            rest = &rest[4 + len..];
        }
        out
    }

    /// The `keys.rs` encoder, reimplemented at the test boundary.
    fn row_key(table: u8, segments: &[&[u8]]) -> Vec<u8> {
        let mut out = vec![table];
        for segment in segments {
            out.extend_from_slice(&(segment.len() as u32).to_be_bytes());
            out.extend_from_slice(segment);
        }
        out
    }

    /// The latest Processed application-classified row, located through
    /// the index key alone (state byte 2, class byte 3).
    fn last_processed_application_row_key(rows: &[(Vec<u8>, Vec<u8>)]) -> Option<Vec<u8>> {
        let mut target = None;
        for (key, _) in rows {
            let segs = segments(key);
            if key.first() == Some(&TABLE_MESSAGE_STATE)
                && segs.len() == 7
                && segs[1] == [STATE_PROCESSED]
                && segs[5] == [CLASS_APPLICATION]
            {
                target = Some(row_key(TABLE_MESSAGE, &[segs[0], segs[3], segs[4]]));
            }
        }
        target
    }

    #[tokio::test]
    async fn queries_skip_undecodable_unreturned_rows() {
        let mut a = open_member().await;
        let mut b = open_member().await;
        let relay = FakeRelay::new();
        publish_identity(&mut a, &relay, RELAY).await;
        publish_identity(&mut b, &relay, RELAY).await;
        let gid = a
            .session
            .create_group(CreateGroup {
                name: "index".to_owned(),
                description: String::new(),
                relays: vec![RELAY.to_owned()],
                invitees: vec![Invitee {
                    key_package: key_package_of(&relay, RELAY, &b.account_hex()),
                    inbox_relays: inbox_relays_of(&relay, RELAY, &b.account_hex()),
                }],
                admins: Vec::new(),
                components: Vec::new(),
            })
            .await
            .expect("create_group");
        flush(&mut a, &relay).await;
        let welcomes = welcomes_for(&relay, RELAY, &b.account_hex());
        ingest_backlog(&mut b, &welcomes).await;
        b.session.tick().await.expect("tick");
        flush(&mut b, &relay).await;
        send_chat(&mut a, &relay, &gid, "one").await;
        send_chat(&mut a, &relay, &gid, "two").await;
        sync_member(&mut b, &relay, RELAY, &gid).await;

        let rows = b.host.durable_rows();
        let target =
            last_processed_application_row_key(&rows).expect("a processed application row exists");
        assert_eq!(mk_core::testing::check_message_index(&rows), Ok(()));

        let mut damaged = rows;
        let idx = damaged
            .iter()
            .position(|(key, _)| *key == target)
            .expect("target row present");
        damaged[idx].1 = vec![0xFF, 0xFE];
        let provider =
            KvStorageProvider::open(damaged, TestHost::new()).expect("reopen over damaged rows");
        let engine_gid = EngineGroupId::new(gid.0.clone());

        // Positive control: the damaged row is decoded when it is returned.
        assert!(
            provider.list_messages(&engine_gid, EpochId(0)).is_err(),
            "list_messages returns the damaged row and must fail"
        );
        assert!(
            provider
                .has_messages_in_states(&engine_gid, &[MessageState::Processed], EpochId(0))
                .expect("has probe answers from the index")
        );
        assert!(
            !provider
                .has_messages_in_states(&engine_gid, &[MessageState::Failed], EpochId(0))
                .expect("absent state answers false")
        );
        assert!(
            !provider
                .has_messages_in_states(&engine_gid, &[], EpochId(0))
                .expect("empty states answer false")
        );
        assert_eq!(
            provider
                .list_deferred_message_metadata(&engine_gid)
                .expect("deferred listing answers from the state index")
                .len(),
            0,
            "deferred listing must not decode unreturned rows"
        );
        assert_eq!(
            provider
                .list_messages_in_states(&engine_gid, &[MessageState::Created], EpochId(0))
                .expect("other states list cleanly")
                .len(),
            0
        );
        let kept = provider
            .list_openmls_messages_in_states(
                &engine_gid,
                &[MessageState::Processed],
                EpochId(0),
                OpenMlsContentFilter::WithoutProcessedApplications,
            )
            .expect("filtered listing excludes the damaged row");
        let target_segments = segments(&target);
        assert!(
            kept.iter()
                .all(|record| record.id.as_slice() != target_segments[2])
        );
        let mut visited = 0;
        provider
            .visit_messages_in_states(
                &engine_gid,
                &[MessageState::Processed],
                EpochId(0),
                &mut |_| {
                    visited += 1;
                    false
                },
            )
            .expect("early-exit visit stops before the damaged row");
        assert_eq!(visited, 1);
    }
}

/// `stored_key_package_bundles` hands out the bare entity bytes —
/// exactly what cgka-engine feeds `serde_json::from_slice::<KeyPackageBundle>`.
#[cfg(feature = "testing")]
mod key_package_bundles {
    use cgka_traits::storage::KeyPackageBundleStorage;
    use mk_core::KvStorageProvider;

    use super::TestHost;
    use super::support::*;

    const RELAY: &str = "ws://127.0.0.1:7777";

    #[tokio::test]
    async fn bundles_are_bare_entity_bytes_and_deletable() {
        let mut member = open_member().await;
        let relay = FakeRelay::new();
        publish_identity(&mut member, &relay, RELAY).await;

        let rows = member.host.durable_rows();
        let provider = KvStorageProvider::open(rows, TestHost::new()).expect("reopen");
        let bundles = provider
            .stored_key_package_bundles()
            .expect("bundle listing");
        assert!(!bundles.is_empty(), "publish_key_package stored bundles");
        for bundle in &bundles {
            serde_json::from_slice::<openmls::prelude::KeyPackageBundle>(&bundle.value)
                .expect("bundle value parses as a KeyPackageBundle");
        }

        let target = bundles[0].storage_key.clone();
        provider
            .delete_stored_key_package_bundle(&target)
            .expect("delete stored bundle");
        let remaining = provider
            .stored_key_package_bundles()
            .expect("bundle listing after delete");
        assert_eq!(remaining.len(), bundles.len() - 1);
        assert!(remaining.iter().all(|bundle| bundle.storage_key != target));
    }
}
