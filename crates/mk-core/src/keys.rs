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
pub(crate) const SCHEMA_VERSION: u32 = 2;

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

/// The range `[start, prefix_end(end_prefix))`: the contiguous tail of the
/// `end_prefix` row set beginning at `start`.
pub(crate) fn tail_range(
    start: Vec<u8>,
    end_prefix: &[u8],
) -> (std::ops::Bound<Vec<u8>>, std::ops::Bound<Vec<u8>>) {
    let end = prefix_end(end_prefix).map_or(std::ops::Bound::Unbounded, std::ops::Bound::Excluded);
    (std::ops::Bound::Included(start), end)
}

/// The `state1` segment of a `Table::MessageState` key. The match is
/// exhaustive on purpose: an upstream `MessageState` addition fails to
/// compile here rather than silently aliasing a byte.
pub(crate) const fn message_state_byte(state: cgka_traits::message::MessageState) -> u8 {
    use cgka_traits::message::MessageState;
    match state {
        MessageState::Sent => 0,
        MessageState::Created => 1,
        MessageState::Processed => 2,
        MessageState::Failed => 3,
        MessageState::Retryable => 4,
        MessageState::ConvergenceDeferred => 5,
        MessageState::PeelDeferred => 6,
        MessageState::EpochInvalidated => 7,
    }
}

/// Inverse of [`message_state_byte`].
pub(crate) const fn message_state_from_byte(
    byte: u8,
) -> Option<cgka_traits::message::MessageState> {
    use cgka_traits::message::MessageState;
    match byte {
        0 => Some(MessageState::Sent),
        1 => Some(MessageState::Created),
        2 => Some(MessageState::Processed),
        3 => Some(MessageState::Failed),
        4 => Some(MessageState::Retryable),
        5 => Some(MessageState::ConvergenceDeferred),
        6 => Some(MessageState::PeelDeferred),
        7 => Some(MessageState::EpochInvalidated),
        _ => None,
    }
}

/// A `Table::MessageState` row key:
/// `[gid, state1, epoch8, seq8, mid, class1, oepoch(0|8 bytes)]`. Within one
/// `gid,state` prefix byte order is `(epoch, seq, mid)`, so `epoch >= e`
/// is one contiguous tail. `class` and `openmls_epoch` are the
/// `openmls_content`/`openmls_epoch` columns of the sqlite layout, computed
/// at write time; `openmls_epoch` is absent (empty segment) for `NONE` and
/// `UNCLASSIFIED` rows.
pub(crate) fn message_state_key(
    group_id: &[u8],
    state: cgka_traits::message::MessageState,
    epoch: u64,
    seq: u64,
    mid: &[u8],
    class: u8,
    openmls_epoch: Option<u64>,
) -> Vec<u8> {
    let state_seg = [message_state_byte(state)];
    let epoch_seg = u64_seg(epoch);
    let seq_seg = u64_seg(seq);
    let class_seg = [class];
    let oepoch_seg = openmls_epoch.map_or_else(Vec::new, |e| e.to_be_bytes().to_vec());
    key(
        Table::MessageState,
        &[
            group_id,
            &state_seg,
            &epoch_seg,
            &seq_seg,
            mid,
            &class_seg,
            &oepoch_seg,
        ],
    )
}

/// The decoded fields of a `Table::MessageState` row key.
pub(crate) struct ParsedMessageStateKey {
    /// Raw group-id segment.
    pub(crate) group_id: Vec<u8>,
    /// Message state the row is indexed under.
    pub(crate) state: cgka_traits::message::MessageState,
    /// Message epoch.
    pub(crate) epoch: u64,
    /// Durable insertion order of the message row.
    pub(crate) seq: u64,
    /// Raw message-id segment.
    pub(crate) mid: Vec<u8>,
    /// `openmls_content` column value.
    pub(crate) class: u8,
    /// `openmls_epoch` column value for classified rows.
    pub(crate) openmls_epoch: Option<u64>,
}

/// Parse a `Table::MessageState` row key; `None` when the layout, the
/// fixed-width segments, or the state byte are malformed.
pub(crate) fn parse_message_state_key(row_key: &[u8]) -> Option<ParsedMessageStateKey> {
    let [gid, state, epoch, seq, mid, class, oepoch]: [&[u8]; 7] =
        segments(row_key)?.try_into().ok()?;
    let [state_byte] = *state else {
        return None;
    };
    let epoch = u64::from_be_bytes(epoch.try_into().ok()?);
    let seq = u64::from_be_bytes(seq.try_into().ok()?);
    let [class] = *class else {
        return None;
    };
    let openmls_epoch = match oepoch.len() {
        0 => None,
        8 => Some(u64::from_be_bytes(oepoch.try_into().ok()?)),
        _ => return None,
    };
    Some(ParsedMessageStateKey {
        group_id: gid.to_vec(),
        state: message_state_from_byte(state_byte)?,
        epoch,
        seq,
        mid: mid.to_vec(),
        class,
        openmls_epoch,
    })
}
