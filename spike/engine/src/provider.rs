//! `cgka_traits::StorageProvider` over the host KV.
//!
//! Table layout (binary keys, `ns` prefix per table, big-endian seq where
//! ordering matters):
//!   grp/<gid>            Group record
//!   iad/<gid>            InvitationAdmission
//!   msg/<gid>/<seq08>/<id>   MessageRecord, insertion ordered
//!   midx/<msgid>         message key index for id lookups
//!   papp/<seq08>/<msgid>     pending GroupEvent (insertion ordered)
//!   pauth/<msgid>        PendingAppMessageAuthority
//!   ddm/<msgid>          ingress dedup marker
//!   pti/<transport_id>   processed transport id
//!   int/<msgid>          QueuedOutboundIntent
//!   oci/<commitid>       OwnCommitIntent
//!   fan/<msgid>          OutboundFanout
//!   lvr/<gid>            LeaveRequest
//!   dbr/<gid>            DisbandRequest
//!   dbc/<gid>/<member>   DisbandCandidate
//!   dbt/<gid>            DisbandTombstone
//!   wel/<msgid>          PendingWelcome
//!   cap/<feature>        CapabilityRequirement
//!   cpo/<gid>            convergence policy bytes
//!   mvc/<gid>            member validation marker
//!   ads/<memberid>       AccountDeviceSignerBinding
//!   ahk/<memberid>       AccountDeviceHistoryKey
//!   dgw/<gid>            DeviceGroupWelcomeExpectation
//!   mnt/*                MaintenanceStorage tables
//!   cps/<gid>            DurableConvergencePass
//!   dpg/<gid>            DeferredPeelGeneration
//!   rt/<tgid>            TransportGroupRoute
//!   for/<gid>            forgotten-group cutoff (Timestamp)
//!   hiw                  pending-ref high water (u64be)
//!   snap/<gid>/<name>    snapshot image blob
//!   chk/<gid>/<ckpt>     canonical-state image blob (checkpoint)
//!   chki/<gid>/<ckpt>    GroupStateCheckpointRef
//!   mls/<label>/<build_key(label,entity_key)>   OpenMLS rows
//!   gml/<group_key>/<storage_key>  group index for OpenMLS rows

use std::sync::Arc;

use cgka_traits::app_event::PendingAppMessageAuthority;
use cgka_traits::capabilities::{Capability, CapabilityRequirement, Feature, RequirementLevel};
use cgka_traits::convergence_pass::DurableConvergencePass;
use cgka_traits::engine::GroupEvent;
use cgka_traits::group::{DisbandTombstone, Group, InvitationAdmission};
use cgka_traits::maintenance::{
    DurableGroupEvolution, DurableTransportFanout, GroupMaintenanceState, KeyPackageLifecycleState,
    MaintenanceObligation, PeriodicMaintenancePolicy,
};
use cgka_traits::message::{MessageRecord, MessageState};
use cgka_traits::reporting;
use cgka_traits::storage::*;
use cgka_traits::transport_adapter::OutboundFanout;
use cgka_traits::types::{Backend, EpochId, GroupId, MemberId, MessageId};
use cgka_traits::welcome::PendingWelcome;
use cgka_traits::{MarmotAppEvent, Timestamp};
use serde::Serialize;
use serde::de::DeserializeOwned;
use sha2::{Digest, Sha256};
use zeroize::Zeroizing;

use crate::kv::KvHost;
use crate::mls::KvOpenMlsStorage;
use crate::store::KvStore;

const NS_GRP: &[u8] = b"grp/";
const NS_IAD: &[u8] = b"iad/";
const NS_MSG: &[u8] = b"msg/";
const NS_MIDX: &[u8] = b"midx/";
const NS_PAPP: &[u8] = b"papp/";
const NS_PAUTH: &[u8] = b"pauth/";
const NS_DDM: &[u8] = b"ddm/";
const NS_PTI: &[u8] = b"pti/";
const NS_INT: &[u8] = b"int/";
const NS_OCI: &[u8] = b"oci/";
const NS_FAN: &[u8] = b"fan/";
const NS_LVR: &[u8] = b"lvr/";
const NS_DBR: &[u8] = b"dbr/";
const NS_DBC: &[u8] = b"dbc/";
const NS_DBT: &[u8] = b"dbt/";
const NS_WEL: &[u8] = b"wel/";
const NS_CAP: &[u8] = b"cap/";
const NS_CPO: &[u8] = b"cpo/";
const NS_MVC: &[u8] = b"mvc/";
const NS_ADS: &[u8] = b"ads/";
const NS_AHK: &[u8] = b"ahk/";
const NS_DGW: &[u8] = b"dgw/";
const NS_MNT: &[u8] = b"mnt/";
const NS_CPS: &[u8] = b"cps/";
const NS_DPG: &[u8] = b"dpg/";
const NS_RT: &[u8] = b"rt/";
const NS_FOR: &[u8] = b"for/";
const NS_SNAP: &[u8] = b"snap/";
const NS_CHKI: &[u8] = b"chki/";
const NS_CHKD: &[u8] = b"chkd/";
const K_HIW: &[u8] = b"hiw";

pub(crate) const MLS_NS: &[u8] = b"mls/";
pub(crate) const GML_NS: &[u8] = b"gml/";

fn k(ns: &[u8], parts: &[&[u8]]) -> Vec<u8> {
    let mut out = ns.to_vec();
    for p in parts {
        out.extend_from_slice(&(p.len() as u32).to_be_bytes());
        out.extend_from_slice(p);
    }
    out
}

fn ser<T: Serialize>(v: &T) -> StorageResult<Vec<u8>> {
    serde_json::to_vec(v).map_err(|e| StorageError::Backend(format!("encode: {e}")))
}

fn de<T: DeserializeOwned>(bytes: &[u8]) -> StorageResult<T> {
    serde_json::from_slice(bytes).map_err(|e| StorageError::Backend(format!("decode: {e}")))
}

#[derive(Clone)]
pub struct KvStorageProvider {
    store: Arc<KvStore>,
    mls: KvOpenMlsStorage,
}

impl KvStorageProvider {
    pub fn open(host: Arc<dyn KvHost>) -> Self {
        let store = Arc::new(KvStore::open(host));
        Self {
            store: store.clone(),
            mls: KvOpenMlsStorage::new(store),
        }
    }

    fn get_de<T: DeserializeOwned>(&self, key: &[u8]) -> StorageResult<Option<T>> {
        self.store.get(key).map(|b| de(&b)).transpose()
    }

    fn put_ser<T: Serialize>(&self, key: Vec<u8>, v: &T) -> StorageResult<()> {
        self.store.put(key, ser(v)?);
        Ok(())
    }

    fn get_req<T: DeserializeOwned>(&self, key: &[u8]) -> StorageResult<T> {
        match self.store.get(key) {
            Some(b) => de(&b),
            None => Err(StorageError::NotFound),
        }
    }

    /// Canonical (snapshot-covered) row prefixes for a group: everything a
    /// state snapshot/checkpoint captures and restores.
    fn canonical_prefixes(gid: &GroupId) -> Vec<Vec<u8>> {
        let g = gid.as_slice();
        vec![
            k(NS_GRP, &[g]),
            k(NS_IAD, &[g]),
            k(NS_CPO, &[g]),
            k(NS_MVC, &[g]),
            k(NS_CPS, &[g]),
            k(NS_DPG, &[g]),
        ]
    }

    /// Capture all live canonical + ledger/outbound rows for `gid` into one
    /// image. Full snapshots also cover the message ledger and outbound queue.
    fn capture_group(&self, gid: &GroupId, canonical_only: bool) -> Vec<(Vec<u8>, Vec<u8>)> {
        let g = gid.as_slice();
        let mut rows: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        for prefix in Self::canonical_prefixes(gid) {
            if let Some(v) = self.store.get(&prefix) {
                rows.push((prefix.clone(), v));
            }
        }
        // OpenMLS rows indexed under this group.
        let gml_prefix = k(GML_NS, &[&crate::mls::group_key_json(g)]);
        for (gk, _) in self.store.scan_prefix(&gml_prefix) {
            // gk tail = storage key
            let storage_key = &gk[GML_NS.len() + 4 + crate::mls::group_key_json(g).len()..];
            if let Some(v) = self.store.get(storage_key) {
                rows.push((storage_key.to_vec(), v));
            }
            rows.push((gk, vec![]));
        }
        if canonical_only {
            return rows;
        }
        // Message ledger: msg/<gid>/<seq>/<id>
        rows.extend(self.store.scan_prefix(&k(NS_MSG, &[g])));
        // Outbound intents / own-commit intents / fanouts filtered by gid.
        for (key, v) in self.store.scan_prefix(NS_INT) {
            if de::<QueuedOutboundIntent>(&v)
                .map(|r| r.group_id.as_slice() == g)
                .unwrap_or(false)
            {
                rows.push((key, v));
            }
        }
        for (key, v) in self.store.scan_prefix(NS_OCI) {
            if de::<OwnCommitIntent>(&v)
                .map(|r| r.group_id.as_slice() == g)
                .unwrap_or(false)
            {
                rows.push((key, v));
            }
        }
        for (key, v) in self.store.scan_prefix(NS_FAN) {
            if de::<cgka_traits::transport_adapter::OutboundFanout>(&v)
                .map(|r| r.group_id().map(|id| id.as_slice() == g).unwrap_or(false))
                .unwrap_or(false)
            {
                rows.push((key, v));
            }
        }
        // Pending application events keyed by this group.
        for (key, v) in self.store.scan_prefix(NS_PAPP) {
            if pending_event_group(&v).is_some_and(|pg| pg == g) {
                rows.push((key, v));
            }
        }
        rows.extend(self.store.scan_prefix(&k(NS_LVR, &[g])));
        rows.extend(self.store.scan_prefix(&k(NS_DBR, &[g])));
        rows.extend(self.store.scan_prefix(&k(NS_DBC, &[g])));
        rows.extend(self.store.scan_prefix(&k(NS_DBT, &[g])));
        rows.extend(self.store.scan_prefix(&k(NS_RT, &[g])));
        rows
    }

    /// Delete every live row a restore of `gid` with the same scope owns.
    fn clear_group(&self, gid: &GroupId, canonical_only: bool) {
        let g = gid.as_slice();
        for prefix in Self::canonical_prefixes(gid) {
            self.store.delete(&prefix);
        }
        let gml_prefix = k(GML_NS, &[&crate::mls::group_key_json(g)]);
        for (gk, _) in self.store.scan_prefix(&gml_prefix) {
            let storage_key = &gk[GML_NS.len() + 4 + crate::mls::group_key_json(g).len()..];
            self.store.delete(storage_key);
            self.store.delete(&gk);
        }
        if canonical_only {
            return;
        }
        self.store.delete_prefix(&k(NS_MSG, &[g]));
        for (key, v) in self.store.scan_prefix(NS_INT) {
            if de::<QueuedOutboundIntent>(&v)
                .map(|r| r.group_id.as_slice() == g)
                .unwrap_or(false)
            {
                self.store.delete(&key);
            }
        }
        for (key, v) in self.store.scan_prefix(NS_OCI) {
            if de::<OwnCommitIntent>(&v)
                .map(|r| r.group_id.as_slice() == g)
                .unwrap_or(false)
            {
                self.store.delete(&key);
            }
        }
        for (key, v) in self.store.scan_prefix(NS_FAN) {
            if de::<cgka_traits::transport_adapter::OutboundFanout>(&v)
                .map(|r| r.group_id().map(|id| id.as_slice() == g).unwrap_or(false))
                .unwrap_or(false)
            {
                self.store.delete(&key);
            }
        }
        for (key, v) in self.store.scan_prefix(NS_PAPP) {
            if pending_event_group(&v).is_some_and(|pg| pg == g) {
                self.store.delete(&key);
            }
        }
        self.store.delete(&k(NS_LVR, &[g]));
        self.store.delete(&k(NS_DBR, &[g]));
        self.store.delete_prefix(&k(NS_DBC, &[g]));
        self.store.delete(&k(NS_DBT, &[g]));
        self.store.delete_prefix(&k(NS_RT, &[g]));
    }

    fn write_snapshot(&self, gid: &GroupId, name: &str, canonical_only: bool) -> StorageResult<()> {
        let rows = self.capture_group(gid, canonical_only);
        let image = ser(&rows)?;
        self.store
            .put(k(NS_SNAP, &[gid.as_slice(), name.as_bytes()]), image);
        Ok(())
    }

    fn restore_snapshot(
        &self,
        gid: &GroupId,
        name: &str,
        canonical_only: bool,
    ) -> StorageResult<()> {
        let key = k(NS_SNAP, &[gid.as_slice(), name.as_bytes()]);
        let Some(image) = self.store.get(&key) else {
            return Err(StorageError::SnapshotMissing(name.to_owned()));
        };
        let rows: Vec<(Vec<u8>, Vec<u8>)> = de(&image)?;
        self.clear_group(gid, canonical_only);
        for (k2, v) in rows {
            self.store.put(k2, v);
        }
        Ok(())
    }

    fn update_high_water(&self, pending: u64) {
        if let Err(e) = self.store.transact::<(), StorageError, _>(|| {
            let cur: u64 = self
                .store
                .get(K_HIW)
                .and_then(|b| b.as_slice().try_into().ok().map(u64::from_be_bytes))
                .unwrap_or(0);
            if pending > cur {
                self.store
                    .put(K_HIW.to_vec(), pending.to_be_bytes().to_vec());
            }
            Ok(())
        }) {
            let _ = e;
        }
    }
}

fn pending_event_group(v: &[u8]) -> Option<Vec<u8>> {
    let event = de::<GroupEvent>(v).ok()?;
    match event {
        GroupEvent::MessageReceived { group_id, .. } | GroupEvent::GroupJoined { group_id, .. } => {
            Some(group_id.as_slice().to_vec())
        }
        _ => None,
    }
}

fn pending_event_key(event: &GroupEvent) -> StorageResult<Vec<u8>> {
    let (gid, mid) = match event {
        GroupEvent::MessageReceived {
            group_id,
            message_id,
            ..
        } => (group_id, message_id),
        GroupEvent::GroupJoined {
            group_id,
            via_welcome,
            ..
        } => (group_id, via_welcome),
        _ => {
            return Err(StorageError::Backend(
                "pending application outbox accepts only MessageReceived and GroupJoined events"
                    .into(),
            ));
        }
    };
    Ok(k(b"", &[mid.as_slice(), gid.as_slice()]))
}

// Manual codecs for the three cross-boundary records that carry no serde
// impls (key material or deliberately non-serializing shapes). Encoded as
// serde_json tuples of their serde-able parts.
type InvitationAdmissionTuple = (Vec<u8>, bool, Option<MessageId>);
type TransportGroupRouteTuple = (Vec<u8>, GroupId, EpochId);
type AccountDeviceHistoryKeyTuple = (MemberId, u16, Vec<u8>, Vec<u8>);

fn admission_to_tuple(a: &InvitationAdmission) -> InvitationAdmissionTuple {
    (
        a.generation.to_vec(),
        a.created_locally,
        a.via_welcome.clone(),
    )
}

fn admission_from_tuple(t: InvitationAdmissionTuple) -> InvitationAdmission {
    InvitationAdmission {
        generation: t.0.try_into().unwrap_or([0; 32]),
        created_locally: t.1,
        via_welcome: t.2,
    }
}

fn route_to_tuple(r: &TransportGroupRoute) -> TransportGroupRouteTuple {
    (
        r.transport_group_id.clone(),
        r.group_id.clone(),
        r.source_epoch,
    )
}

fn route_from_tuple(t: TransportGroupRouteTuple) -> TransportGroupRoute {
    TransportGroupRoute {
        transport_group_id: t.0,
        group_id: t.1,
        source_epoch: t.2,
    }
}

fn history_key_to_tuple(k: &AccountDeviceHistoryKey) -> AccountDeviceHistoryKeyTuple {
    (
        k.marmot_identity.clone(),
        k.ciphersuite,
        k.public_key.clone(),
        k.private_key.to_vec(),
    )
}

fn history_key_from_tuple(t: AccountDeviceHistoryKeyTuple) -> AccountDeviceHistoryKey {
    AccountDeviceHistoryKey {
        marmot_identity: t.0,
        ciphersuite: t.1,
        public_key: t.2,
        private_key: Zeroizing::new(t.3),
    }
}

impl GroupStorage for KvStorageProvider {
    fn invitation_admission(&self, id: &GroupId) -> StorageResult<Option<InvitationAdmission>> {
        Ok(self
            .get_de::<InvitationAdmissionTuple>(&k(NS_IAD, &[id.as_slice()]))?
            .map(admission_from_tuple))
    }

    fn put_invitation_admission(
        &self,
        id: &GroupId,
        admission: Option<&InvitationAdmission>,
    ) -> StorageResult<()> {
        let key = k(NS_IAD, &[id.as_slice()]);
        match admission {
            Some(a) => self.put_ser(key, &admission_to_tuple(a)),
            None => {
                self.store.delete(&key);
                Ok(())
            }
        }
    }

    fn put_group(&self, group: &Group) -> StorageResult<()> {
        self.put_ser(k(NS_GRP, &[group.id.as_slice()]), group)
    }

    fn get_group(&self, id: &GroupId) -> StorageResult<Group> {
        self.get_req(&k(NS_GRP, &[id.as_slice()]))
    }

    fn delete_group(&self, id: &GroupId) -> StorageResult<()> {
        // Collect id-keyed indexes before the group-scoped rows go.
        let mut orphan_keys = Vec::new();
        for (_, v) in self.store.scan_prefix(&k(NS_MSG, &[id.as_slice()])) {
            if let Ok(record) = de::<MessageRecord>(&v) {
                orphan_keys.push(k(NS_MIDX, &[record.id.as_slice()]));
                orphan_keys.push(k(NS_DDM, &[record.id.as_slice()]));
            }
        }
        self.clear_group(id, false);
        for key in orphan_keys {
            self.store.delete(&key);
        }
        self.store.delete(&k(NS_DGW, &[id.as_slice()]));
        self.store.delete(&k(NS_FOR, &[id.as_slice()]));
        Ok(())
    }

    fn forget_group_local(&self, id: &GroupId, at: Timestamp) -> StorageResult<bool> {
        let key = k(NS_FOR, &[id.as_slice()]);
        let existed = self.store.get(&key).is_some();
        self.store.put(key, at.0.to_be_bytes().to_vec());
        Ok(!existed)
    }

    fn is_group_forgotten(&self, id: &GroupId) -> StorageResult<bool> {
        Ok(self.store.get(&k(NS_FOR, &[id.as_slice()])).is_some())
    }

    fn group_local_reset_cutoff(&self, id: &GroupId) -> StorageResult<Option<Timestamp>> {
        Ok(self
            .store
            .get(&k(NS_FOR, &[id.as_slice()]))
            .and_then(|b| b.as_slice().try_into().ok().map(u64::from_be_bytes))
            .map(Timestamp))
    }

    fn complete_group_local_reset(&self, id: &GroupId) -> StorageResult<()> {
        self.delete_group(id)?;
        self.store.delete(&k(NS_FOR, &[id.as_slice()]));
        Ok(())
    }

    fn list_groups(&self) -> StorageResult<Vec<GroupId>> {
        self.store
            .scan_prefix(NS_GRP)
            .into_iter()
            .map(|(_, v)| de::<Group>(&v).map(|g| g.id))
            .collect()
    }

    fn list_group_records(&self) -> StorageResult<Vec<Group>> {
        self.store
            .scan_prefix(NS_GRP)
            .into_iter()
            .map(|(_, v)| de(&v))
            .collect()
    }

    fn put_transport_group_route(
        &self,
        transport_group_id: &[u8],
        group_id: &GroupId,
        source_epoch: EpochId,
    ) -> StorageResult<()> {
        self.put_ser(
            k(NS_RT, &[transport_group_id]),
            &route_to_tuple(&TransportGroupRoute {
                transport_group_id: transport_group_id.to_vec(),
                group_id: group_id.clone(),
                source_epoch,
            }),
        )
    }

    fn list_transport_group_routes(&self) -> StorageResult<Vec<TransportGroupRoute>> {
        self.store
            .scan_prefix(NS_RT)
            .into_iter()
            .map(|(_, v)| de::<TransportGroupRouteTuple>(&v).map(route_from_tuple))
            .collect()
    }

    fn delete_transport_group_route(&self, transport_group_id: &[u8]) -> StorageResult<()> {
        self.store.delete(&k(NS_RT, &[transport_group_id]));
        Ok(())
    }

    fn delete_transport_group_routes_below_epoch(
        &self,
        group_id: &GroupId,
        cutoff: EpochId,
    ) -> StorageResult<()> {
        for (key, v) in self.store.scan_prefix(NS_RT) {
            if de::<TransportGroupRouteTuple>(&v)
                .map(route_from_tuple)
                .map(|r| r.group_id.as_slice() == group_id.as_slice() && r.source_epoch < cutoff)
                .unwrap_or(false)
            {
                self.store.delete(&key);
            }
        }
        Ok(())
    }

    fn delete_transport_group_routes_for_group(&self, group_id: &GroupId) -> StorageResult<()> {
        for (key, v) in self.store.scan_prefix(NS_RT) {
            if de::<TransportGroupRouteTuple>(&v)
                .map(|r| r.1.as_slice() == group_id.as_slice())
                .unwrap_or(false)
            {
                self.store.delete(&key);
            }
        }
        Ok(())
    }
}

impl MessageStorage for KvStorageProvider {
    fn put_message(&self, record: &MessageRecord) -> StorageResult<()> {
        // A re-put of an existing id overwrites in place, preserving its
        // original insertion position (sqlite's INSERT OR REPLACE on the
        // keyed row).
        let idx = k(NS_MIDX, &[record.id.as_slice()]);
        let key = match self.store.get(&idx) {
            Some(existing) => existing,
            None => {
                let seq = self.store.next_seq();
                let key = k(
                    NS_MSG,
                    &[
                        record.group_id.as_slice(),
                        &seq.to_be_bytes(),
                        record.id.as_slice(),
                    ],
                );
                self.store.put(idx, key.clone());
                key
            }
        };
        self.store.put(key, ser(record)?);
        Ok(())
    }

    fn get_message(&self, id: &MessageId) -> StorageResult<MessageRecord> {
        let Some(key) = self.store.get(&k(NS_MIDX, &[id.as_slice()])) else {
            return Err(StorageError::NotFound);
        };
        self.get_req(&key)
    }

    fn delete_message(&self, id: &MessageId) -> StorageResult<()> {
        if let Some(key) = self.store.get(&k(NS_MIDX, &[id.as_slice()])) {
            self.store.delete(&key);
            self.store.delete(&k(NS_MIDX, &[id.as_slice()]));
        }
        Ok(())
    }

    fn update_message_state(&self, id: &MessageId, new_state: MessageState) -> StorageResult<()> {
        let mut record = self.get_message(id)?;
        record.state = new_state;
        let key = self
            .store
            .get(&k(NS_MIDX, &[id.as_slice()]))
            .ok_or(StorageError::NotFound)?;
        self.store.put(key, ser(&record)?);
        Ok(())
    }

    fn list_messages(
        &self,
        group_id: &GroupId,
        at_or_after_epoch: EpochId,
    ) -> StorageResult<Vec<MessageRecord>> {
        self.store
            .scan_prefix(&k(NS_MSG, &[group_id.as_slice()]))
            .into_iter()
            .map(|(_, v)| de::<MessageRecord>(&v))
            .filter(|r| {
                r.as_ref()
                    .map(|rec| rec.epoch >= at_or_after_epoch)
                    .unwrap_or(true)
            })
            .collect()
    }

    fn put_pending_application_event(&self, event: &GroupEvent) -> StorageResult<()> {
        let mid_key = pending_event_key(event)?;
        let seq = self.store.next_seq();
        // Mirrors storage-sqlite: an unresolved-authority MessageReceived whose
        // kind requires source authority also writes the authority request row.
        if let GroupEvent::MessageReceived {
            group_id,
            message_id,
            epoch,
            sender,
            payload,
            authority,
            retention,
            ..
        } = event
        {
            let pauth_key = k(NS_PAUTH, &[message_id.as_slice()]);
            if authority.is_none()
                && MarmotAppEvent::decode(payload)
                    .is_ok_and(|e| reporting::requires_source_authority(e.kind))
            {
                let request = PendingAppMessageAuthority {
                    group_id: group_id.clone(),
                    message_id: message_id.clone(),
                    epoch: *epoch,
                    sender: sender.clone(),
                    payload_digest: Sha256::digest(payload).into(),
                    retention: *retention,
                };
                self.store.put(pauth_key, ser(&request)?);
            } else {
                self.store.delete(&pauth_key);
            }
        }
        let key = k(NS_PAPP, &[&seq.to_be_bytes(), &mid_key]);
        self.store.put(key, ser(event)?);
        Ok(())
    }

    fn list_pending_application_events(&self) -> StorageResult<Vec<GroupEvent>> {
        self.store
            .scan_prefix(NS_PAPP)
            .into_iter()
            .map(|(_, v)| de(&v))
            .collect()
    }

    fn pending_application_authority_batch(
        &self,
        after: Option<&MessageId>,
        limit: usize,
    ) -> StorageResult<Vec<PendingAppMessageAuthority>> {
        let mut out = Vec::new();
        for (_, v) in self.store.scan_prefix(NS_PAUTH) {
            let request: PendingAppMessageAuthority = de(&v)?;
            if after.is_some_and(|a| request.message_id.as_slice() <= a.as_slice()) {
                continue;
            }
            out.push(request);
            if out.len() >= limit {
                break;
            }
        }
        Ok(out)
    }

    fn delete_pending_application_events(&self, ids: &[MessageId]) -> StorageResult<()> {
        let id_set: Vec<&[u8]> = ids.iter().map(|i| i.as_slice()).collect();
        for (key, v) in self.store.scan_prefix(NS_PAPP) {
            let mid = match de::<GroupEvent>(&v) {
                Ok(
                    GroupEvent::MessageReceived { message_id, .. }
                    | GroupEvent::GroupJoined {
                        via_welcome: message_id,
                        ..
                    },
                ) => message_id,
                _ => continue,
            };
            if id_set.contains(&mid.as_slice()) {
                self.store.delete(&key);
                self.store.delete(&k(NS_PAUTH, &[mid.as_slice()]));
            }
        }
        Ok(())
    }

    fn put_ingress_dedup_marker(&self, id: &MessageId) -> StorageResult<()> {
        self.store.put(k(NS_DDM, &[id.as_slice()]), vec![]);
        Ok(())
    }

    fn has_ingress_dedup_marker(&self, id: &MessageId) -> StorageResult<bool> {
        Ok(self.store.get(&k(NS_DDM, &[id.as_slice()])).is_some())
    }

    fn put_processed_transport_id(
        &self,
        _group_id: &GroupId,
        transport_id: &MessageId,
    ) -> StorageResult<()> {
        self.store
            .put(k(NS_PTI, &[transport_id.as_slice()]), vec![]);
        Ok(())
    }

    fn has_processed_transport_id(&self, transport_id: &MessageId) -> StorageResult<bool> {
        Ok(self
            .store
            .get(&k(NS_PTI, &[transport_id.as_slice()]))
            .is_some())
    }

    fn create_group_snapshot(&self, group_id: &GroupId, name: &str) -> StorageResult<()> {
        self.write_snapshot(group_id, name, false)
    }

    fn create_group_state_snapshot(&self, group_id: &GroupId, name: &str) -> StorageResult<()> {
        self.write_snapshot(group_id, name, true)
    }

    fn list_group_snapshots(&self, group_id: &GroupId) -> StorageResult<Vec<String>> {
        let mut names = Vec::new();
        for (key, _) in self.store.scan_prefix(&k(NS_SNAP, &[group_id.as_slice()])) {
            // key tail after "snap/" is <gid-len><gid><name-len><name>
            let tail = &key[NS_SNAP.len()..];
            let glen = u32::from_be_bytes(tail[..4].try_into().unwrap_or([0; 4])) as usize;
            let name_bytes = &tail[4 + glen + 4..];
            names.push(String::from_utf8_lossy(name_bytes).into_owned());
        }
        Ok(names)
    }

    fn rollback_group_to_snapshot(&self, group_id: &GroupId, name: &str) -> StorageResult<()> {
        self.restore_snapshot(group_id, name, false)
    }

    fn rollback_group_state_to_snapshot(
        &self,
        group_id: &GroupId,
        name: &str,
    ) -> StorageResult<()> {
        // Prefer a canonical-only image when the named snapshot was captured
        // state-scoped; a full image also restores correctly for state rows.
        self.restore_snapshot(group_id, name, true)
    }

    fn release_group_snapshot(&self, group_id: &GroupId, name: &str) -> StorageResult<()> {
        let key = k(NS_SNAP, &[group_id.as_slice(), name.as_bytes()]);
        if self.store.get(&key).is_none() {
            return Err(StorageError::SnapshotMissing(name.to_owned()));
        }
        self.store.delete(&key);
        Ok(())
    }

    fn create_group_state_checkpoint(
        &self,
        group_id: &GroupId,
        checkpoint: &GroupStateCheckpointRef,
    ) -> StorageResult<()> {
        let g = group_id.as_slice();
        let ref_key = k(NS_CHKI, &[g, checkpoint.id.as_bytes()]);
        if let Some(existing) = self.get_de::<GroupStateCheckpointRef>(&ref_key)? {
            if existing.resulting_epoch != checkpoint.resulting_epoch {
                return Err(StorageError::AlreadyExists);
            }
            return Ok(());
        }
        let rows = self.capture_group(group_id, true);
        let image = ser(&rows)?;
        self.store
            .put(k(NS_CHKD, &[g, checkpoint.id.as_bytes()]), image);
        self.put_ser(ref_key, checkpoint)
    }

    fn restore_group_state_checkpoint(
        &self,
        group_id: &GroupId,
        checkpoint_id: &str,
    ) -> StorageResult<()> {
        let g = group_id.as_slice();
        let key = k(NS_CHKD, &[g, checkpoint_id.as_bytes()]);
        let Some(image) = self.store.get(&key) else {
            return Err(StorageError::SnapshotMissing(checkpoint_id.to_owned()));
        };
        let rows: Vec<(Vec<u8>, Vec<u8>)> = de(&image)?;
        self.clear_group(group_id, true);
        for (k2, v) in rows {
            self.store.put(k2, v);
        }
        Ok(())
    }

    fn list_group_state_checkpoints(
        &self,
        group_id: &GroupId,
    ) -> StorageResult<Vec<GroupStateCheckpointRef>> {
        self.store
            .scan_prefix(&k(NS_CHKI, &[group_id.as_slice()]))
            .into_iter()
            .map(|(_, v)| de(&v))
            .collect()
    }

    fn release_group_state_checkpoint(
        &self,
        group_id: &GroupId,
        checkpoint_id: &str,
    ) -> StorageResult<()> {
        let g = group_id.as_slice();
        self.store
            .delete(&k(NS_CHKI, &[g, checkpoint_id.as_bytes()]));
        self.store
            .delete(&k(NS_CHKD, &[g, checkpoint_id.as_bytes()]));
        Ok(())
    }
}

impl OutboundIntentStorage for KvStorageProvider {
    fn put_queued_outbound_intent(&self, record: &QueuedOutboundIntent) -> StorageResult<()> {
        self.put_ser(k(NS_INT, &[record.id.as_slice()]), record)
    }

    fn list_queued_outbound_intents(
        &self,
        group_id: &GroupId,
    ) -> StorageResult<Vec<QueuedOutboundIntent>> {
        let mut out: Vec<QueuedOutboundIntent> = self
            .store
            .scan_prefix(NS_INT)
            .into_iter()
            .filter_map(|(_, v)| de(&v).ok())
            .filter(|r: &QueuedOutboundIntent| r.group_id.as_slice() == group_id.as_slice())
            .collect();
        out.sort_by_key(|r| (r.created_at_ms, r.id.as_slice().to_vec()));
        Ok(out)
    }

    fn delete_queued_outbound_intent(&self, id: &MessageId) -> StorageResult<()> {
        self.store.delete(&k(NS_INT, &[id.as_slice()]));
        Ok(())
    }

    fn put_own_commit_intent(&self, record: &OwnCommitIntent) -> StorageResult<()> {
        self.put_ser(k(NS_OCI, &[record.commit_id.as_slice()]), record)
    }

    fn own_commit_intent(&self, commit_id: &MessageId) -> StorageResult<Option<OwnCommitIntent>> {
        self.get_de(&k(NS_OCI, &[commit_id.as_slice()]))
    }

    fn list_own_commit_intents(
        &self,
        group_id: Option<&GroupId>,
    ) -> StorageResult<Vec<OwnCommitIntent>> {
        let mut out: Vec<OwnCommitIntent> = self
            .store
            .scan_prefix(NS_OCI)
            .into_iter()
            .filter_map(|(_, v)| de(&v).ok())
            .filter(|r: &OwnCommitIntent| {
                group_id.is_none_or(|g| r.group_id.as_slice() == g.as_slice())
            })
            .collect();
        out.sort_by_key(|r| (r.created_at_ms, r.commit_id.as_slice().to_vec()));
        Ok(out)
    }

    fn delete_own_commit_intent(&self, commit_id: &MessageId) -> StorageResult<()> {
        self.store.delete(&k(NS_OCI, &[commit_id.as_slice()]));
        Ok(())
    }
}

impl OutboundFanoutStorage for KvStorageProvider {
    fn put_outbound_fanout(&self, fanout: &OutboundFanout) -> StorageResult<()> {
        if let Some(pending) = fanout.pending_ref() {
            self.update_high_water(pending.as_u64());
        }
        self.put_ser(k(NS_FAN, &[fanout.message_id().as_slice()]), fanout)
    }

    fn outbound_fanout(&self, message_id: &MessageId) -> StorageResult<Option<OutboundFanout>> {
        self.get_de(&k(NS_FAN, &[message_id.as_slice()]))
    }

    fn list_outbound_fanouts(&self) -> StorageResult<Vec<OutboundFanout>> {
        self.store
            .scan_prefix(NS_FAN)
            .into_iter()
            .map(|(_, v)| de(&v))
            .collect()
    }

    fn list_outbound_fanouts_for_group(
        &self,
        group_id: &GroupId,
    ) -> StorageResult<Vec<OutboundFanout>> {
        Ok(self
            .list_outbound_fanouts()?
            .into_iter()
            .filter(|f| {
                f.group_id()
                    .map(|g| g.as_slice() == group_id.as_slice())
                    .unwrap_or(false)
            })
            .collect())
    }

    fn delete_outbound_fanout(&self, message_id: &MessageId) -> StorageResult<()> {
        self.store.delete(&k(NS_FAN, &[message_id.as_slice()]));
        Ok(())
    }
}

impl LeaveRequestStorage for KvStorageProvider {
    fn put_leave_request(&self, request: &LeaveRequest) -> StorageResult<()> {
        self.put_ser(k(NS_LVR, &[request.group_id.as_slice()]), request)
    }

    fn leave_request(&self, group_id: &GroupId) -> StorageResult<Option<LeaveRequest>> {
        self.get_de(&k(NS_LVR, &[group_id.as_slice()]))
    }

    fn clear_leave_request(&self, group_id: &GroupId) -> StorageResult<()> {
        self.store.delete(&k(NS_LVR, &[group_id.as_slice()]));
        Ok(())
    }
}

impl DisbandRequestStorage for KvStorageProvider {
    fn put_disband_request(&self, request: &DisbandRequest) -> StorageResult<()> {
        self.put_ser(k(NS_DBR, &[request.group_id.as_slice()]), request)
    }

    fn disband_request(&self, group_id: &GroupId) -> StorageResult<Option<DisbandRequest>> {
        self.get_de(&k(NS_DBR, &[group_id.as_slice()]))
    }

    fn clear_disband_request(&self, group_id: &GroupId) -> StorageResult<()> {
        self.store.delete(&k(NS_DBR, &[group_id.as_slice()]));
        Ok(())
    }
}

impl DisbandCandidateStorage for KvStorageProvider {
    fn put_disband_candidate(&self, candidate: &DisbandCandidate) -> StorageResult<()> {
        self.put_ser(
            k(
                NS_DBC,
                &[
                    candidate.group_id.as_slice(),
                    candidate.commit_id.as_slice(),
                ],
            ),
            candidate,
        )
    }

    fn disband_candidate(
        &self,
        group_id: &GroupId,
        commit_id: &MessageId,
    ) -> StorageResult<Option<DisbandCandidate>> {
        self.get_de(&k(NS_DBC, &[group_id.as_slice(), commit_id.as_slice()]))
    }

    fn list_disband_candidates(&self, group_id: &GroupId) -> StorageResult<Vec<DisbandCandidate>> {
        self.store
            .scan_prefix(&k(NS_DBC, &[group_id.as_slice()]))
            .into_iter()
            .map(|(_, v)| de(&v))
            .collect()
    }

    fn clear_disband_candidates(&self, group_id: &GroupId) -> StorageResult<()> {
        self.store.delete_prefix(&k(NS_DBC, &[group_id.as_slice()]));
        Ok(())
    }
}

impl DisbandTombstoneStorage for KvStorageProvider {
    fn put_disband_tombstone(
        &self,
        group_id: &GroupId,
        tombstone: &DisbandTombstone,
    ) -> StorageResult<()> {
        self.put_ser(k(NS_DBT, &[group_id.as_slice()]), tombstone)
    }

    fn disband_tombstone(&self, group_id: &GroupId) -> StorageResult<Option<DisbandTombstone>> {
        self.get_de(&k(NS_DBT, &[group_id.as_slice()]))
    }

    fn list_disband_tombstones(&self) -> StorageResult<Vec<(GroupId, DisbandTombstone)>> {
        let mut out = Vec::new();
        for (key, v) in self.store.scan_prefix(NS_DBT) {
            let tail = &key[NS_DBT.len()..];
            let glen = u32::from_be_bytes(tail[..4].try_into().unwrap_or([0; 4])) as usize;
            let gid = GroupId::new(tail[4..4 + glen].to_vec());
            out.push((gid, de(&v)?));
        }
        Ok(out)
    }

    fn mark_disband_tombstone_announced(&self, group_id: &GroupId) -> StorageResult<()> {
        let key = k(NS_DBT, &[group_id.as_slice()]);
        let Some(mut tombstone) = self.get_de::<DisbandTombstone>(&key)? else {
            return Ok(());
        };
        tombstone.announced = true;
        self.put_ser(key, &tombstone)
    }
}

impl WelcomeStorage for KvStorageProvider {
    fn put_welcome(&self, welcome: &PendingWelcome) -> StorageResult<()> {
        self.put_ser(k(NS_WEL, &[welcome.message_id.as_slice()]), welcome)
    }

    fn take_welcome(&self, id: &MessageId) -> StorageResult<PendingWelcome> {
        let key = k(NS_WEL, &[id.as_slice()]);
        let welcome = self.get_req::<PendingWelcome>(&key)?;
        self.store.delete(&key);
        Ok(welcome)
    }

    fn list_welcomes(&self) -> StorageResult<Vec<PendingWelcome>> {
        self.store
            .scan_prefix(NS_WEL)
            .into_iter()
            .map(|(_, v)| de(&v))
            .collect()
    }
}

impl CapabilityStorage for KvStorageProvider {
    fn register_feature(&self, feature: Feature, req: CapabilityRequirement) -> StorageResult<()> {
        let feature_key = serde_json::to_vec(&feature)
            .map_err(|e| StorageError::Backend(format!("feature key: {e}")))?;
        self.put_ser(
            k(NS_CAP, &[&feature_key]),
            &(req.requires, req.level, req.description.to_owned()),
        )
    }

    fn feature_requirement(
        &self,
        feature: &Feature,
    ) -> StorageResult<Option<CapabilityRequirement>> {
        let feature_key = serde_json::to_vec(feature)
            .map_err(|e| StorageError::Backend(format!("feature key: {e}")))?;
        Ok(self
            .get_de::<(Capability, RequirementLevel, String)>(&k(NS_CAP, &[&feature_key]))?
            .map(|(requires, level, description)| CapabilityRequirement {
                requires,
                level,
                // `description` is a borrowed `&'static str`, so the type has
                // no DeserializeOwned impl. Leaking one short description per
                // registered feature is bounded (feature registry size) and
                // documented in spike/STORAGE.md.
                description: Box::leak(description.into_boxed_str()),
            }))
    }
}

impl ConvergencePolicyStorage for KvStorageProvider {
    fn put_convergence_policy(&self, group_id: &GroupId, policy: &[u8]) -> StorageResult<()> {
        self.store
            .put(k(NS_CPO, &[group_id.as_slice()]), policy.to_vec());
        Ok(())
    }

    fn convergence_policy(&self, group_id: &GroupId) -> StorageResult<Option<Vec<u8>>> {
        Ok(self.store.get(&k(NS_CPO, &[group_id.as_slice()])))
    }
}

impl ConvergencePassStorage for KvStorageProvider {
    fn convergence_pass(
        &self,
        group_id: &GroupId,
    ) -> StorageResult<Option<DurableConvergencePass>> {
        self.get_de(&k(NS_CPS, &[group_id.as_slice()]))
    }

    fn put_convergence_pass(&self, pass: &DurableConvergencePass) -> StorageResult<()> {
        self.put_ser(k(NS_CPS, &[pass.group_id.as_slice()]), pass)
    }

    fn list_convergence_passes(&self) -> StorageResult<Vec<DurableConvergencePass>> {
        self.store
            .scan_prefix(NS_CPS)
            .into_iter()
            .map(|(_, v)| de(&v))
            .collect()
    }

    fn delete_convergence_pass(&self, group_id: &GroupId) -> StorageResult<()> {
        self.store.delete(&k(NS_CPS, &[group_id.as_slice()]));
        Ok(())
    }
}

impl DeferredPeelGenerationStorage for KvStorageProvider {
    fn deferred_peel_generation(
        &self,
        group_id: &GroupId,
    ) -> StorageResult<Option<DeferredPeelGeneration>> {
        self.get_de(&k(NS_DPG, &[group_id.as_slice()]))
    }

    fn put_deferred_peel_generation(
        &self,
        generation: &DeferredPeelGeneration,
    ) -> StorageResult<()> {
        self.put_ser(k(NS_DPG, &[generation.group_id.as_slice()]), generation)
    }

    fn delete_deferred_peel_generation(&self, group_id: &GroupId) -> StorageResult<()> {
        self.store.delete(&k(NS_DPG, &[group_id.as_slice()]));
        Ok(())
    }
}

impl MemberValidationCacheStorage for KvStorageProvider {
    fn put_validated_tree_marker(&self, group_id: &GroupId, marker: &[u8]) -> StorageResult<()> {
        self.store
            .put(k(NS_MVC, &[group_id.as_slice()]), marker.to_vec());
        Ok(())
    }

    fn validated_tree_marker(&self, group_id: &GroupId) -> StorageResult<Option<Vec<u8>>> {
        Ok(self.store.get(&k(NS_MVC, &[group_id.as_slice()])))
    }
}

impl AccountDeviceSignerStorage for KvStorageProvider {
    fn put_account_device_signer(&self, binding: &AccountDeviceSignerBinding) -> StorageResult<()> {
        self.put_ser(k(NS_ADS, &[binding.marmot_identity.as_slice()]), binding)
    }

    fn account_device_signer(
        &self,
        marmot_identity: &MemberId,
    ) -> StorageResult<Option<AccountDeviceSignerBinding>> {
        self.get_de(&k(NS_ADS, &[marmot_identity.as_slice()]))
    }
}

impl AccountDeviceHistoryKeyStorage for KvStorageProvider {
    fn put_account_device_history_key_if_absent(
        &self,
        key: &AccountDeviceHistoryKey,
    ) -> StorageResult<AccountDeviceHistoryKey> {
        let kkey = k(NS_AHK, &[key.marmot_identity.as_slice()]);
        if let Some(existing) = self.get_de::<AccountDeviceHistoryKeyTuple>(&kkey)? {
            return Ok(history_key_from_tuple(existing));
        }
        self.put_ser(kkey, &history_key_to_tuple(key))?;
        Ok(key.clone())
    }

    fn account_device_history_key(
        &self,
        marmot_identity: &MemberId,
    ) -> StorageResult<Option<AccountDeviceHistoryKey>> {
        Ok(self
            .get_de::<AccountDeviceHistoryKeyTuple>(&k(NS_AHK, &[marmot_identity.as_slice()]))?
            .map(history_key_from_tuple))
    }
}

impl DeviceGroupWelcomeStorage for KvStorageProvider {
    fn put_device_group_welcome_expectation(
        &self,
        expectation: &DeviceGroupWelcomeExpectation,
    ) -> StorageResult<()> {
        self.put_ser(k(NS_DGW, &[expectation.group_id.as_slice()]), expectation)
    }

    fn device_group_welcome_expectation(
        &self,
        group_id: &GroupId,
    ) -> StorageResult<Option<DeviceGroupWelcomeExpectation>> {
        self.get_de(&k(NS_DGW, &[group_id.as_slice()]))
    }

    fn device_group_welcome_expectations(
        &self,
    ) -> StorageResult<Vec<DeviceGroupWelcomeExpectation>> {
        self.store
            .scan_prefix(NS_DGW)
            .into_iter()
            .map(|(_, v)| de(&v))
            .collect()
    }

    fn delete_device_group_welcome_expectation(&self, group_id: &GroupId) -> StorageResult<bool> {
        let key = k(NS_DGW, &[group_id.as_slice()]);
        let existed = self.store.get(&key).is_some();
        self.store.delete(&key);
        Ok(existed)
    }
}

impl KeyPackageBundleStorage for KvStorageProvider {
    fn stored_key_package_bundles(&self) -> StorageResult<Vec<StoredKeyPackageBundle>> {
        // Every OpenMLS row under the "KeyPackage" label.
        let prefix = crate::mls::label_prefix(crate::mls::KEY_PACKAGE_LABEL);
        Ok(self
            .store
            .scan_prefix(&prefix)
            .into_iter()
            .map(|(storage_key, value)| StoredKeyPackageBundle {
                storage_key,
                value: zeroize::Zeroizing::new(value),
            })
            .collect())
    }

    fn delete_stored_key_package_bundle(&self, storage_key: &[u8]) -> StorageResult<()> {
        self.store.delete(storage_key);
        Ok(())
    }
}

impl MaintenanceStorage for KvStorageProvider {
    fn key_package_lifecycle(&self) -> StorageResult<Option<KeyPackageLifecycleState>> {
        self.get_de(&k(NS_MNT, &[b"lifecycle"]))
    }

    fn put_key_package_lifecycle(&self, state: &KeyPackageLifecycleState) -> StorageResult<()> {
        self.put_ser(k(NS_MNT, &[b"lifecycle"]), state)
    }

    fn group_maintenance(
        &self,
        group_id: &GroupId,
    ) -> StorageResult<Option<GroupMaintenanceState>> {
        self.get_de(&k(NS_MNT, &[b"gm", group_id.as_slice()]))
    }

    fn put_group_maintenance(&self, state: &GroupMaintenanceState) -> StorageResult<()> {
        self.put_ser(k(NS_MNT, &[b"gm", state.group_id.as_slice()]), state)
    }

    fn delete_group_maintenance(&self, group_id: &GroupId) -> StorageResult<()> {
        self.store.delete(&k(NS_MNT, &[b"gm", group_id.as_slice()]));
        Ok(())
    }

    fn put_maintenance_obligation(&self, record: &MaintenanceObligation) -> StorageResult<()> {
        self.put_ser(k(NS_MNT, &[b"ob", record.id.as_slice()]), record)
    }

    fn maintenance_obligation(
        &self,
        id: &MessageId,
    ) -> StorageResult<Option<MaintenanceObligation>> {
        self.get_de(&k(NS_MNT, &[b"ob", id.as_slice()]))
    }

    fn list_maintenance_obligations(&self) -> StorageResult<Vec<MaintenanceObligation>> {
        self.store
            .scan_prefix(&k(NS_MNT, &[b"ob"]))
            .into_iter()
            .map(|(_, v)| de(&v))
            .collect()
    }

    fn list_maintenance_obligations_for_group(
        &self,
        group_id: &GroupId,
    ) -> StorageResult<Vec<MaintenanceObligation>> {
        Ok(self
            .list_maintenance_obligations()?
            .into_iter()
            .filter(|r| r.group_id.as_slice() == group_id.as_slice())
            .collect())
    }

    fn delete_maintenance_obligation(&self, id: &MessageId) -> StorageResult<()> {
        self.store.delete(&k(NS_MNT, &[b"ob", id.as_slice()]));
        Ok(())
    }

    fn put_group_evolution(&self, record: &DurableGroupEvolution) -> StorageResult<()> {
        if let Some(pending) = record.pending_ref {
            self.update_high_water(pending.as_u64());
        }
        self.put_ser(k(NS_MNT, &[b"ev", record.id.as_slice()]), record)
    }

    fn group_evolution(&self, id: &MessageId) -> StorageResult<Option<DurableGroupEvolution>> {
        self.get_de(&k(NS_MNT, &[b"ev", id.as_slice()]))
    }

    fn list_group_evolutions(&self) -> StorageResult<Vec<DurableGroupEvolution>> {
        self.store
            .scan_prefix(&k(NS_MNT, &[b"ev"]))
            .into_iter()
            .map(|(_, v)| de(&v))
            .collect()
    }

    fn list_group_evolutions_for_group(
        &self,
        group_id: &GroupId,
    ) -> StorageResult<Vec<DurableGroupEvolution>> {
        Ok(self
            .list_group_evolutions()?
            .into_iter()
            .filter(|r| r.group_id.as_slice() == group_id.as_slice())
            .collect())
    }

    fn delete_group_evolution(&self, id: &MessageId) -> StorageResult<()> {
        self.store.delete(&k(NS_MNT, &[b"ev", id.as_slice()]));
        Ok(())
    }

    fn put_transport_fanout(&self, record: &DurableTransportFanout) -> StorageResult<()> {
        self.put_ser(k(NS_MNT, &[b"tf", record.id.as_slice()]), record)
    }

    fn transport_fanout(&self, id: &MessageId) -> StorageResult<Option<DurableTransportFanout>> {
        self.get_de(&k(NS_MNT, &[b"tf", id.as_slice()]))
    }

    fn list_transport_fanouts(&self) -> StorageResult<Vec<DurableTransportFanout>> {
        self.store
            .scan_prefix(&k(NS_MNT, &[b"tf"]))
            .into_iter()
            .map(|(_, v)| de(&v))
            .collect()
    }

    fn delete_transport_fanout(&self, id: &MessageId) -> StorageResult<()> {
        self.store.delete(&k(NS_MNT, &[b"tf", id.as_slice()]));
        Ok(())
    }

    fn periodic_maintenance_policy(&self) -> StorageResult<PeriodicMaintenancePolicy> {
        Ok(self
            .get_de::<PeriodicMaintenancePolicy>(&k(NS_MNT, &[b"policy"]))?
            .unwrap_or_default())
    }

    fn put_periodic_maintenance_policy(
        &self,
        policy: PeriodicMaintenancePolicy,
    ) -> StorageResult<()> {
        self.put_ser(k(NS_MNT, &[b"policy"]), &policy)
    }
}

impl StorageProvider for KvStorageProvider {
    type Mls = KvOpenMlsStorage;

    fn mls_storage(&self) -> &Self::Mls {
        &self.mls
    }

    fn pending_ref_high_water(&self) -> StorageResult<u64> {
        // High-water must never regress; a missing row is a known-empty
        // history, not an error.
        Ok(self
            .store
            .get(K_HIW)
            .and_then(|b| b.as_slice().try_into().ok().map(u64::from_be_bytes))
            .unwrap_or(0))
    }

    fn mls_write_generation(&self) -> Option<u64> {
        Some(self.store.write_generation())
    }

    fn maintenance_storage(&self) -> Option<&dyn MaintenanceStorage> {
        Some(self)
    }

    fn with_transaction<T, E, F>(&self, f: F) -> Result<T, E>
    where
        E: From<StorageError>,
        F: FnOnce(&Self) -> Result<T, E>,
    {
        self.store.transact(|| f(self))
    }

    fn after_commit(&self, callback: Box<dyn FnOnce() + Send + 'static>) {
        self.store.on_after_commit(callback);
    }

    fn with_group_savepoint<T, E, F>(
        &self,
        _group_id: &GroupId,
        _fallback_snapshot: &str,
        f: F,
    ) -> Result<T, E>
    where
        E: From<StorageError>,
        F: FnOnce(&Self) -> Result<T, E>,
    {
        // Layered overlay: same mechanism as with_transaction. On Err every
        // write the closure made is discarded, which is a superset of the
        // contract's allowed group-scoped undo, and an enclosing
        // with_transaction still commits its other work.
        self.store.transact(|| f(self))
    }

    fn backend(&self) -> Backend {
        // `Backend` has only `Sqlite`; no generic variant exists in v0.16.0.
        // Reported as an upstream gap in spike/STORAGE.md.
        Backend::Sqlite
    }
}
