//! The versioned key schema: one encoder for every row key and every scan
//! prefix in the crate.
//!
//! A row key is `[table byte]` followed by length-prefixed segments
//! (`u32 BE` length + bytes). The encoding is prefix-closed: the scan prefix
//! for the first `n` segments of a key is exactly `key(table, &segments[..n])`,
//! so `BTreeMap::range` over a prefix enumerates every row under it. Numeric
//! order segments are fixed-width big-endian so byte order equals numeric
//! order. No other code in the crate concatenates key bytes.

/// One variant per logical table. `Session*` variants reserve namespace for
/// the session layer a later milestone adds; nothing in this milestone writes
/// them.
#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Table {
    /// Schema version, sequence counter, pending-reference high-water.
    Meta = 0,
    /// `cgka_groups` records.
    Group = 1,
    /// `cgka_invitation_admissions`.
    InvitationAdmission = 2,
    /// `cgka_messages` rows, keyed `group/seq/message` so byte order is the
    /// durable insertion order.
    Message = 3,
    /// Message-id to message-row-key index.
    MessageIndex = 4,
    /// Per-group message-state index to message-row-key.
    MessageState = 5,
    /// `pending_application_events`.
    PendingAppEvent = 6,
    /// `pending_application_authority`.
    PendingAppAuthority = 7,
    /// `cgka_ingress_dedup`, bounded FIFO.
    IngressDedup = 8,
    /// `processed_transport_ids`.
    ProcessedTransportId = 9,
    /// `cgka_queued_outbound`.
    QueuedIntent = 10,
    /// `cgka_own_commit_intents`.
    OwnCommitIntent = 11,
    /// `cgka_outbound_fanout`.
    OutboundFanout = 12,
    /// `cgka_leave_requests`.
    LeaveRequest = 13,
    /// `cgka_disband_requests`.
    DisbandRequest = 14,
    /// `cgka_disband_candidates`.
    DisbandCandidate = 15,
    /// `cgka_disband_tombstones`.
    DisbandTombstone = 16,
    /// `cgka_welcomes`.
    Welcome = 17,
    /// `cgka_features`.
    Capability = 18,
    /// `cgka_convergence_policies`.
    ConvergencePolicy = 19,
    /// `cgka_member_validation_cache`.
    MemberValidation = 20,
    /// `cgka_account_device_signers`.
    AccountDeviceSigner = 21,
    /// `cgka_account_device_history_keys`.
    AccountDeviceHistoryKey = 22,
    /// `cgka_device_group_welcome_expectations`.
    DeviceGroupWelcome = 23,
    /// `cgka_maintenance_*` singletons, ordered records, settings.
    Maintenance = 24,
    /// `cgka_convergence_passes`.
    ConvergencePass = 25,
    /// `cgka_deferred_peel_generations`.
    DeferredPeelGeneration = 26,
    /// `cgka_transport_group_routes`.
    TransportRoute = 27,
    /// `locally_forgotten_groups`.
    ForgottenGroup = 28,
    /// `mdk_group_snapshots` images.
    Snapshot = 29,
    /// `cgka_group_state_checkpoints` images.
    Checkpoint = 30,
    /// `openmls_values` rows, `label/storage_key`.
    OpenMlsValue = 31,
    /// `OpenMLS` group index: `raw_group_id/storage_key` for every group-scoped
    /// row, so group delete/snapshot/rollback can enumerate them.
    OpenMlsGroupIndex = 32,
    /// Reserved for the session layer's outbound record table (H2).
    #[allow(dead_code, reason = "reserved for the session outbox table (H2)")]
    SessionOutbox = 33,
    /// Reserved for the session layer's inbound record table (H2).
    #[allow(dead_code, reason = "reserved for the session inbox table (H2)")]
    SessionInbox = 34,
    /// Session-layer link from a regenerated outbound artifact's message id
    /// to the durable queued intent it carries — the restart-safe
    /// replacement for the engine's in-memory `queued_intent_by_message`
    /// (cgka-engine `message_processor/mod.rs:1773`).
    QueuedIntentLink = 35,
    /// Epoch exporter secrets captured per delivered media message, keyed
    /// `group/transport_id` — the only custody of the key material
    /// `open_attachment` needs after the source epoch is pruned.
    MediaSecret = 36,
    /// `cgka_committed_rosters` + `cgka_committed_roster_members`: each
    /// group's committed roster — revision and normalized member list in one
    /// value. Outside every snapshot/checkpoint; deleted with the group.
    CommittedRoster = 37,
    /// `cgka_committed_roster_revision`: one row — the account-wide roster
    /// revision counter. Never reset, never deleted, not group-scoped.
    CommittedRosterRevision = 38,
}

/// Meta row keys inside [`Table::Meta`].
pub(crate) const META_SCHEMA_VERSION: &[u8] = b"schema-version";
/// Meta row holding the monotonic insertion-order counter.
pub(crate) const META_SEQUENCE: &[u8] = b"sequence";
/// Meta row holding the pending-reference high-water.
pub(crate) const META_PENDING_HIGH_WATER: &[u8] = b"pending-ref-high-water";

/// Current store schema version. Bumped when the row format or layout changes;
/// older or newer stores refuse to open (no migration code by design).
pub(crate) const SCHEMA_VERSION: u32 = 1;

/// Encode a row key or scan prefix. `segments` must be nonempty content for a
/// row key; prefixes use fewer segments of the same layout.
pub(crate) fn key(table: Table, segments: &[&[u8]]) -> Vec<u8> {
    let mut out = Vec::with_capacity(1 + segments.iter().map(|s| s.len() + 4).sum::<usize>());
    out.push(table as u8);
    for segment in segments {
        out.extend_from_slice(
            &u32::try_from(segment.len())
                .unwrap_or(u32::MAX)
                .to_be_bytes(),
        );
        out.extend_from_slice(segment);
    }
    out
}

/// Fixed-width big-endian segment so byte order is numeric order.
pub(crate) const fn u64_seg(value: u64) -> [u8; 8] {
    value.to_be_bytes()
}

/// The exclusive end bound for `BTreeMap::range` over `prefix`.
pub(crate) fn prefix_end(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut end = prefix.to_vec();
    loop {
        match end.last_mut() {
            Some(last) if *last == u8::MAX => {
                end.pop();
            }
            Some(last) => {
                *last += 1;
                return Some(end);
            }
            None => return None,
        }
    }
}

/// The single segment decoder: parse a row key into its segments after the
/// table byte. `None` when any length prefix overruns the input or bytes
/// remain unparsed — strict, never partial.
pub(crate) fn segments(row_key: &[u8]) -> Option<Vec<&[u8]>> {
    let mut rest = row_key.get(1..)?;
    let mut out = Vec::new();
    while !rest.is_empty() {
        if rest.len() < 4 {
            return None;
        }
        let (len_bytes, tail) = rest.split_at(4);
        let len = u32::from_be_bytes(len_bytes.try_into().ok()?) as usize;
        if tail.len() < len {
            return None;
        }
        let (segment, next) = tail.split_at(len);
        out.push(segment);
        rest = next;
    }
    Some(out)
}

/// The range `(start, end)` covering all rows under `prefix`, as
/// `(Bound, Bound)` for `BTreeMap::range`.
pub(crate) fn prefix_range(prefix: &[u8]) -> (std::ops::Bound<Vec<u8>>, std::ops::Bound<Vec<u8>>) {
    let end = prefix_end(prefix).map_or(std::ops::Bound::Unbounded, std::ops::Bound::Excluded);
    (std::ops::Bound::Included(prefix.to_vec()), end)
}
