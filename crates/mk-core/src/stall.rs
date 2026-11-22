//! D2's stall detector: durable, bounded evidence that a live group's
//! local copy has fallen behind.
//!
//! A missing commit and a restored stale state are both silent —
//! `transport_deferred` outcomes with no event and no epoch movement. The
//! detector counts distinct kind-445 events routed to a group whose
//! outcome is `unavailable{transport_deferred}` while the group's epoch
//! does not move. At [`STALL_EVENTS`] it asks the host for one refetch
//! (`SessionEvent::GroupStallSuspected`); when the host later reports
//! `history_fetched` and the count is still full at the same epoch, the
//! row is flagged `stalled` and `SessionEvent::GroupStalled` fires once.
//! Epoch progress, a join, a local-copy termination, or an accepted
//! current-epoch message deletes the row; `forget_group` deletes it too.
//! `stalled` is a projection only — it never changes protocol state and
//! never triggers a commit.

use cgka_traits::Timestamp;
use cgka_traits::error::EngineError;
use cgka_traits::storage::{StorageError, StorageResult};
use serde::{Deserialize, Serialize};

use crate::events::SessionEvent;
use crate::keys::{self, Table};
use crate::ser::{self, Tuple};
use crate::session::{Session, SessionError, engine_gid, session_engine_error};
use crate::{EventId, GroupId};

/// Distinct deferred events at one epoch that suspect a stall.
pub const STALL_EVENTS: usize = 3;

/// The refetch handshake stage a group's row is in.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Refetch {
    /// No refetch asked for yet.
    #[default]
    None,
    /// `GroupStallSuspected` was emitted; the host's refetch is open.
    Requested,
    /// The host reported `history_fetched` for the suspicion.
    Done,
}

/// Durable stall evidence for one group (`Table::GroupStall`, keyed by the
/// group id bytes). One row per group, bounded: `events` never exceeds
/// [`STALL_EVENTS`]. `welcome` is the local copy's Welcome stamp: a
/// `GroupJoined` re-presented for the same copy (pending events are
/// at-least-once, so every open replays it) must not clear the row; a
/// reinstalled copy's different stamp does.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct GroupStall {
    /// The group epoch the evidence was counted at.
    pub(crate) epoch: u64,
    /// `local_copy_welcome_created_at` of the copy the evidence belongs
    /// to (`None` for a self-created copy).
    pub(crate) welcome: Option<u64>,
    /// Distinct deferred event ids seen at that epoch.
    pub(crate) events: Vec<EventId>,
    /// Where the one refetch stands.
    pub(crate) refetch: Refetch,
    /// Whether the group was flagged stalled after the refetch.
    pub(crate) stalled: bool,
}

impl GroupStall {
    /// A fresh counter for `epoch` on the copy stamped `welcome`.
    const fn fresh(epoch: u64, welcome: Option<u64>) -> Self {
        Self {
            epoch,
            welcome,
            events: Vec::new(),
            refetch: Refetch::None,
            stalled: false,
        }
    }
}

impl Session {
    /// The group's stall row, when one is stored.
    pub(crate) fn stall_row(&self, group: &GroupId) -> StorageResult<Option<GroupStall>> {
        self.provider
            .row(Table::GroupStall, &[&group.0])?
            .map(|value| ser::de::<GroupStall>(&value))
            .transpose()
    }

    /// Resolve a transport routing id (an event's `#h` tag) to its group
    /// through the transport-route index — the same index ingest writes
    /// and `group_of_message` reads through.
    fn group_of_route(&self, transport_group_id: &[u8]) -> StorageResult<Option<GroupId>> {
        let Some(value) = self
            .provider
            .row(Table::TransportRoute, &[transport_group_id])?
        else {
            return Ok(None);
        };
        let mut tuple = Tuple::new(&value);
        let _order = tuple.u64()?;
        let group = tuple.field()?.to_vec();
        let _source_epoch = tuple.u64()?;
        tuple.finish()?;
        Ok(Some(GroupId(group)))
    }

    /// D2 counting, per admitted kind-445 event whose outcome is
    /// `unavailable{transport_deferred}`. `transport_group_id` is the
    /// event's `#h` routing id; skips unknown groups, ended or broken
    /// copies, and events that predate this local copy. Emits
    /// `GroupStallSuspected` once when the counter fills.
    pub(crate) fn stall_note_deferred(
        &mut self,
        transport_group_id: &[u8],
        event_id: &EventId,
        created_at: u64,
    ) -> Result<(), SessionError> {
        let Some(group) = self.group_of_route(transport_group_id)? else {
            return Ok(());
        };
        let record = match self.engine.group_record(&engine_gid(&group)) {
            Ok(record) => record,
            Err(EngineError::UnknownGroup(_)) => return Ok(()),
            Err(error) => return Err(session_engine_error(error)),
        };
        if record.removed || record.unrecoverable || record.disbanded.is_some() {
            return Ok(());
        }
        if record.transport_message_predates_local_copy(Timestamp(created_at)) {
            return Ok(());
        }
        let welcome = record.local_copy_welcome_created_at.map(|stamp| stamp.0);
        let provider = &self.provider;
        let suspect = provider
            .store()
            .transact(|| -> Result<bool, SessionError> {
                let mut row = match self.stall_row(&group)? {
                    Some(row) if row.epoch == record.epoch.0 && row.welcome == welcome => row,
                    _ => GroupStall::fresh(record.epoch.0, welcome),
                };
                if row.events.contains(event_id) || row.events.len() >= STALL_EVENTS {
                    return Ok(false);
                }
                row.events.push(*event_id);
                let suspect = row.events.len() == STALL_EVENTS && row.refetch == Refetch::None;
                if suspect {
                    row.refetch = Refetch::Requested;
                }
                provider
                    .put_row(Table::GroupStall, &[&group.0], ser::ser(&row)?)
                    .map_err(SessionError::Storage)?;
                Ok(suspect)
            })?;
        if suspect {
            self.tick_events
                .push(SessionEvent::GroupStallSuspected { group });
        }
        Ok(())
    }

    /// Delete the group's row, when one exists (epoch progress, join,
    /// local-copy termination, `forget_group`).
    pub(crate) fn stall_clear(&self, group: &GroupId) -> StorageResult<()> {
        if self.provider.row(Table::GroupStall, &[&group.0])?.is_some() {
            self.provider.del_row(Table::GroupStall, &[&group.0])?;
        }
        Ok(())
    }

    /// Delete the row only when it was counted at `epoch`: an accepted
    /// current-epoch message clears counter and flag; an older-epoch
    /// delivery leaves them alone.
    fn stall_clear_at(&self, group: &GroupId, epoch: u64) -> StorageResult<()> {
        if self.stall_row(group)?.is_some_and(|row| row.epoch == epoch) {
            self.provider.del_row(Table::GroupStall, &[&group.0])?;
        }
        Ok(())
    }

    /// The drain-side clearing half of the detector, run inside
    /// `drain_events`' store unit from `process_mapped`.
    pub(crate) fn stall_clear_for_event(&self, event: &SessionEvent) -> Result<(), SessionError> {
        match event {
            SessionEvent::EpochChanged { group, .. }
            | SessionEvent::LocalCopyTerminated { group, .. } => {
                self.stall_clear(group).map_err(SessionError::Storage)
            }
            SessionEvent::GroupJoined { group } => self.stall_clear_on_join(group),
            SessionEvent::MessageReceived { group, message } => self
                .stall_clear_at(group, message.epoch)
                .map_err(SessionError::Storage),
            SessionEvent::AgentStreamStarted { group, start } => self
                .stall_clear_at(group, start.epoch)
                .map_err(SessionError::Storage),
            _ => Ok(()),
        }
    }

    /// A `GroupJoined` clears the row only when the stored evidence
    /// belongs to a different local copy than the one installed now.
    /// The event is at-least-once: the same copy's join re-presents on
    /// every open until the host acknowledges it, and clearing on that
    /// replay would drop a `requested` refetch or a `stalled` flag the
    /// restart was supposed to keep.
    fn stall_clear_on_join(&self, group: &GroupId) -> Result<(), SessionError> {
        let Some(row) = self.stall_row(group)? else {
            return Ok(());
        };
        let record = match self.engine.group_record(&engine_gid(group)) {
            Ok(record) => record,
            Err(EngineError::UnknownGroup(_)) => {
                self.provider
                    .del_row(Table::GroupStall, &[&group.0])
                    .map_err(SessionError::Storage)?;
                return Ok(());
            }
            Err(error) => return Err(session_engine_error(error)),
        };
        if row.welcome != record.local_copy_welcome_created_at.map(|stamp| stamp.0) {
            self.provider
                .del_row(Table::GroupStall, &[&group.0])
                .map_err(SessionError::Storage)?;
        }
        Ok(())
    }

    /// The `history_fetched` half: a completed fetch ends the open refetch
    /// request; a count that is still full at the same epoch flags the
    /// group `stalled` and emits `GroupStalled` once. A row counted at an
    /// older epoch is dropped — progress already cleared the suspicion.
    pub(crate) fn stall_on_history_fetched(
        &mut self,
        group: &GroupId,
        epoch: u64,
    ) -> Result<(), SessionError> {
        let provider = &self.provider;
        let stalled_now = provider
            .store()
            .transact(|| -> Result<bool, SessionError> {
                let Some(mut row) = self.stall_row(group)? else {
                    return Ok(false);
                };
                if row.epoch != epoch {
                    provider.del_row(Table::GroupStall, &[&group.0])?;
                    return Ok(false);
                }
                let mut changed = row.refetch == Refetch::Requested;
                if changed {
                    row.refetch = Refetch::Done;
                }
                let stalled_now = row.events.len() >= STALL_EVENTS && !row.stalled;
                if stalled_now {
                    row.stalled = true;
                    changed = true;
                }
                if changed {
                    provider.put_row(Table::GroupStall, &[&group.0], ser::ser(&row)?)?;
                }
                Ok(stalled_now)
            })?;
        if stalled_now {
            self.tick_events.push(SessionEvent::GroupStalled {
                group: group.clone(),
            });
        }
        Ok(())
    }

    /// Re-ask for every open refetch once after a restart (C19): the first
    /// drain of a reopened session carries `GroupStallSuspected` for each
    /// row whose refetch is still `requested`.
    pub(crate) fn reissue_stall_requests(&mut self) -> Result<(), SessionError> {
        for (row_key, value) in self
            .provider
            .scan_rows(Table::GroupStall, &[])
            .map_err(SessionError::Storage)?
        {
            let row: GroupStall = ser::de(&value).map_err(SessionError::Storage)?;
            if row.refetch != Refetch::Requested {
                continue;
            }
            let group = keys::segments(&row_key)
                .and_then(|segments| segments.last().copied())
                .ok_or_else(|| {
                    SessionError::Storage(StorageError::Backend("malformed row".into()))
                })?;
            self.tick_events.push(SessionEvent::GroupStallSuspected {
                group: GroupId(group.to_vec()),
            });
        }
        Ok(())
    }
}
