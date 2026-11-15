#![no_main]

use std::sync::Arc;

use cgka_traits::capabilities::Feature;
use cgka_traits::message::MessageState;
use cgka_traits::storage::{
    AccountDeviceHistoryKeyStorage, AccountDeviceSignerStorage, CapabilityStorage,
    ConvergencePassStorage, ConvergencePolicyStorage, DeferredPeelGenerationStorage,
    DeviceGroupWelcomeStorage, DisbandCandidateStorage, DisbandRequestStorage,
    DisbandTombstoneStorage, GroupStorage, KeyPackageBundleStorage, LeaveRequestStorage,
    MaintenanceStorage, MemberValidationCacheStorage, MessageStorage, OutboundFanoutStorage,
    OutboundIntentStorage, StorageProvider, WelcomeStorage,
};
use cgka_traits::{EpochId, GroupId, MemberId, MessageId};
use libfuzzer_sys::fuzz_target;
use mk_core::KvStorageProvider;
use mk_core::testing::RecordingHost;

/// Row bytes plus the id pool the sub-storage probes draw from.
type Derived = (Vec<u8>, Vec<(Vec<u8>, Vec<u8>)>);

/// Derive `(key, value)` rows from the input: `[klen u16][key][vlen u16][value]`.
fn rows(data: &[u8]) -> Derived {
    let (ids, rest) = data.split_at(data.len().min(64));
    let mut rows = Vec::new();
    let mut cursor = rest;
    while cursor.len() >= 2 {
        let klen = u16::from_le_bytes([cursor[0], cursor[1]]) as usize;
        if cursor.len() < 2 + klen + 2 {
            break;
        }
        let (key, tail) = cursor[2..].split_at(klen);
        let vlen = u16::from_le_bytes([tail[0], tail[1]]) as usize;
        if tail.len() < 2 + vlen {
            break;
        }
        let (value, tail) = tail[2..].split_at(vlen);
        rows.push((key.to_vec(), value.to_vec()));
        cursor = tail;
    }
    (ids.to_vec(), rows)
}

fuzz_target!(|data: &[u8]| {
    let (ids, rows) = rows(data);
    let host = Arc::new(RecordingHost::new());
    let Ok(provider) = KvStorageProvider::open(rows, host) else {
        return;
    };
    let group_id = GroupId::new(ids.get(..32).unwrap_or(&ids).to_vec());
    let message_id = MessageId::new(ids.get(32..).unwrap_or(&ids).to_vec());
    let member_id = MemberId::new(ids.get(16..48).unwrap_or(&ids).to_vec());
    let epoch = EpochId(ids.first().copied().unwrap_or(0).into());

    // Group-scoped reads.
    let _ = provider.get_group(&group_id);
    let _ = provider.list_groups();
    let _ = provider.list_group_records();
    let _ = provider.invitation_admission(&group_id);
    let _ = provider.is_group_forgotten(&group_id);
    let _ = provider.group_local_reset_cutoff(&group_id);
    let _ = provider.list_transport_group_routes();

    // Message-ledger reads.
    let _ = provider.get_message(&message_id);
    let _ = provider.list_messages(&group_id, epoch);
    let _ = provider.has_messages_in_states(&group_id, &[MessageState::Processed], epoch);
    let _ = provider.list_messages_in_states(
        &group_id,
        &[MessageState::Sent, MessageState::Retryable],
        epoch,
    );
    let _ = provider.list_pending_application_events();
    let _ = provider.pending_application_authority_batch(Some(&message_id), 8);
    let _ = provider.has_ingress_dedup_marker(&message_id);
    let _ = provider.has_processed_transport_id(&message_id);
    let _ = provider.list_group_snapshots(&group_id);
    let _ = provider.group_snapshot_fingerprint(&group_id, "fuzz");
    let _ = provider.list_group_state_checkpoints(&group_id);

    // Outbound/intent/fanout reads.
    let _ = provider.list_queued_outbound_intents(&group_id);
    let _ = provider.own_commit_intent(&message_id);
    let _ = provider.list_own_commit_intents(None);
    let _ = provider.list_own_commit_intents(Some(&group_id));
    let _ = provider.outbound_fanout(&message_id);
    let _ = provider.list_outbound_fanouts();
    let _ = provider.list_outbound_fanouts_for_group(&group_id);

    // Lifecycle, welcome and capability reads.
    let _ = provider.leave_request(&group_id);
    let _ = provider.disband_request(&group_id);
    let _ = provider.list_disband_candidates(&group_id);
    let _ = provider.list_disband_tombstones();
    let _ = provider.list_welcomes();
    let _ = provider.feature_requirement(&Feature("fuzz-feature"));
    let _ = provider.convergence_policy(&group_id);
    let _ = provider.validated_tree_marker(&group_id);
    let _ = provider.account_device_signer(&member_id);
    let _ = provider.account_device_history_key(&member_id);
    let _ = provider.device_group_welcome_expectation(&group_id);
    let _ = provider.device_group_welcome_expectations();
    let _ = provider.stored_key_package_bundles();
    if let Some(storage) = provider.private_key_package_storage() {
        let _ = storage.issued_private_key_package(ids.as_slice());
        let _ = storage.list_issued_private_key_packages();
    }
    if let Some(storage) = provider.roster_tag_key_storage() {
        let _ = storage.roster_tag_keys(&group_id, 0);
    }
    if let Some(storage) = provider.sibling_add_storage() {
        let _ = storage.sibling_adds();
    }
    let _ = provider.key_package_lifecycle();
    let _ = provider.group_maintenance(&group_id);
    let _ = provider.maintenance_obligation(&message_id);
    let _ = provider.list_maintenance_obligations();
    let _ = provider.list_maintenance_obligations_for_group(&group_id);
    let _ = provider.group_evolution(&message_id);
    let _ = provider.list_group_evolutions();
    let _ = provider.list_group_evolutions_for_group(&group_id);
    let _ = provider.transport_fanout(&message_id);
    let _ = provider.list_transport_fanouts();
    let _ = provider.periodic_maintenance_policy();
    let _ = provider.convergence_pass(&group_id);
    let _ = provider.list_convergence_passes();
    let _ = provider.deferred_peel_generation(&group_id);
    let _ = provider.pending_ref_high_water();
    let _ = provider.mls_write_generation();
    let _ = provider.group_replay_state_fingerprint(&group_id);

    // The OpenMLS row family: `MlsGroup::load` drives the group_state,
    // tree/context, own-leaf, epoch-secret, proposal-queue and key-package
    // decoders against whatever rows the input placed there.
    let mls_gid = openmls::group::GroupId::from_slice(&ids);
    let _ = openmls::prelude::MlsGroup::load(provider.mls_storage(), &mls_gid);
});
