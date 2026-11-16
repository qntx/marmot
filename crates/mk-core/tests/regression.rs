//! Regression: group-scoped `OpenMLS` rows must round-trip through Marmot
//! snapshots, checkpoints, rollbacks and `delete_group`. The row's index
//! segment and every scan segment are the RAW MLS group id bytes — if the
//! two derivations ever diverge (index under serde bytes, scan under raw
//! bytes or vice versa) every assertion below fails.

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
use cgka_traits::storage::{GroupStorage, MessageStorage, StorageProvider};
use cgka_traits::types::{EpochId, GroupId};
use mk_core::{KvBatch, KvHost, KvOp, KvStorageProvider};
use openmls::group::{GroupId as MlsGroupId, MlsGroupJoinConfig};
use openmls::prelude::LeafNodeIndex;
use openmls_traits::storage::StorageProvider as _;

#[derive(Debug, Default)]
struct TestHost {
    rows: Mutex<BTreeMap<Vec<u8>, Vec<u8>>>,
}

impl TestHost {
    /// What a reopen sees; used by the reopen assertion below.
    fn durable_rows(&self) -> Vec<(Vec<u8>, Vec<u8>)> {
        self.rows
            .lock()
            .expect("host rows")
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }
}

impl KvHost for TestHost {
    fn commit(&self, batch: KvBatch) {
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

fn provider_with_group() -> (KvStorageProvider, Arc<TestHost>, GroupId, MlsGroupId) {
    let host = Arc::new(TestHost::default());
    let provider = KvStorageProvider::open(Vec::new(), host.clone()).expect("open");
    let mls_id = MlsGroupId::from_slice(b"regression-group-id");
    let gid = GroupId::new(mls_id.as_slice().to_vec());
    provider
        .put_group(&Group {
            id: gid.clone(),
            name: "regression".into(),
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
        })
        .expect("put group");
    (provider, host, gid, mls_id)
}

fn join_config() -> MlsGroupJoinConfig {
    MlsGroupJoinConfig::builder().build()
}

#[test]
fn group_scoped_openmls_rows_survive_snapshot_rollback() {
    let (provider, _host, gid, mls_id) = provider_with_group();
    let mls = provider.mls_storage();

    // Write two group-scoped OpenMLS rows through the OpenMLS trait.
    mls.write_mls_join_config(&mls_id, &join_config())
        .expect("write join config");
    mls.write_own_leaf_index(&mls_id, &LeafNodeIndex::new(3))
        .expect("write leaf index");

    provider
        .create_group_snapshot(&gid, "s1")
        .expect("snapshot");

    // More group-scoped writes after the snapshot.
    mls.write_own_leaf_index(&mls_id, &LeafNodeIndex::new(4))
        .expect("write leaf index v2");

    provider
        .rollback_group_to_snapshot(&gid, "s1")
        .expect("rollback");

    // Every OpenMLS row is restored exactly: pre-snapshot rows back,
    // post-snapshot writes gone.
    assert!(
        mls.mls_group_join_config::<_, MlsGroupJoinConfig>(&mls_id)
            .expect("read join config")
            .is_some()
    );
    assert_eq!(
        mls.own_leaf_index::<_, LeafNodeIndex>(&mls_id)
            .expect("read leaf index"),
        Some(LeafNodeIndex::new(3))
    );
}

#[test]
fn group_scoped_openmls_rows_survive_checkpoint_restore() {
    let (provider, _host, gid, mls_id) = provider_with_group();
    let mls = provider.mls_storage();

    mls.write_mls_join_config(&mls_id, &join_config())
        .expect("write join config");

    provider
        .create_group_state_checkpoint(
            &gid,
            &cgka_traits::storage::GroupStateCheckpointRef {
                id: "cp1".into(),
                resulting_epoch: EpochId(7),
            },
        )
        .expect("checkpoint");

    mls.write_own_leaf_index(&mls_id, &LeafNodeIndex::new(9))
        .expect("write leaf index");

    provider
        .restore_group_state_checkpoint(&gid, "cp1")
        .expect("restore checkpoint");

    assert!(
        mls.mls_group_join_config::<_, MlsGroupJoinConfig>(&mls_id)
            .expect("read join config")
            .is_some()
    );
    assert!(
        mls.own_leaf_index::<_, LeafNodeIndex>(&mls_id)
            .expect("read leaf index")
            .is_none(),
        "post-checkpoint openmls write is rolled back"
    );
}

#[test]
fn delete_group_removes_group_scoped_openmls_rows() {
    let (provider, _host, gid, mls_id) = provider_with_group();
    let mls = provider.mls_storage();

    mls.write_mls_join_config(&mls_id, &join_config())
        .expect("write join config");
    mls.write_own_leaf_index(&mls_id, &LeafNodeIndex::new(1))
        .expect("write leaf index");

    provider.delete_group(&gid).expect("delete group");

    assert!(
        mls.mls_group_join_config::<_, MlsGroupJoinConfig>(&mls_id)
            .expect("read join config")
            .is_none()
    );
    assert!(
        mls.own_leaf_index::<_, LeafNodeIndex>(&mls_id)
            .expect("read leaf index")
            .is_none()
    );
}

#[test]
fn reopen_after_committed_openmls_writes_restores_index() {
    let host = Arc::new(TestHost::default());
    let provider = KvStorageProvider::open(Vec::new(), host.clone()).expect("open");
    let mls_id = MlsGroupId::from_slice(b"reopen-group");
    provider
        .mls_storage()
        .write_mls_join_config(&mls_id, &join_config())
        .expect("write join config");

    let reopened = KvStorageProvider::open(host.durable_rows(), host).expect("reopen");
    assert!(
        reopened
            .mls_storage()
            .mls_group_join_config::<_, MlsGroupJoinConfig>(&mls_id)
            .expect("read join config")
            .is_some()
    );
}

#[test]
fn savepoint_failure_restores_group_scoped_openmls_rows() {
    let (provider, _host, gid, mls_id) = provider_with_group();
    let mls = provider.mls_storage();

    mls.write_own_leaf_index(&mls_id, &LeafNodeIndex::new(2))
        .expect("write leaf index");

    let mls_store = provider.mls_storage().clone();
    let inner_id = mls_id.clone();
    let result: Result<(), cgka_traits::storage::StorageError> =
        provider.with_group_savepoint(&gid, "sp", move |_| {
            mls_store
                .write_own_leaf_index(&inner_id, &LeafNodeIndex::new(5))
                .map_err(|error| cgka_traits::storage::StorageError::Backend(error.to_string()))?;
            Err(cgka_traits::storage::StorageError::Backend("fail".into()))
        });
    assert!(result.is_err());
    assert_eq!(
        provider
            .mls_storage()
            .own_leaf_index::<_, LeafNodeIndex>(&mls_id)
            .expect("read leaf index"),
        Some(LeafNodeIndex::new(2))
    );
}
