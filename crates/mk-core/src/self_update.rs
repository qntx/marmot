//! D4: the post-join and periodic self-update driver.
//!
//! The engine installs, inside the join transaction, a
//! [`GroupMaintenanceState`] plus a post-join [`MaintenanceObligation`]
//! (`CatchUp`, `operational_target_at` = join + 24 h, engine-sampled jitter
//! and the own-leaf baseline). Nobody completed obligations before D4: this
//! module is the session-side driver, mirroring marmot-session's obligation
//! loop (`runtime.rs` 2850–3360) on the injected clock.
//!
//! Phase walk per obligation: `CatchUp` waits for `history_fetched` or the
//! 300 s EOSE timeout, then `Quiet` for 60 s, then `Jitter` until the
//! persisted `not_before` — sampled once, never resampled across a restart
//! (C15). The gate (`Stable` epoch, no convergence inputs or queued
//! intents, no own commit in flight, no leave or disband in progress, group
//! not unrecoverable) holds the obligation open and retryable; it never
//! fails it. A passed gate sends `SendIntent::SelfUpdate` through the
//! ordinary durable outbox — never paced — and the obligation waits in
//! `PendingPublication` for the evolution's `Confirmed` phase or a changed
//! own-leaf hash (C16). A lost same-epoch race returns the obligation to
//! `Quiet` to re-issue once. Completion writes `last_own_leaf_rotation_at`,
//! samples the next periodic deadline (24–36 days) and emits
//! `SessionEvent::SelfUpdated` — group state and obligation in one storage
//! unit.
//!
//! Periodic rotation enrolment is policy (`periodic_enrolled`); created
//! groups carry no post-join obligation and get only periodic maintenance.
//! Application sends are never blocked by an open obligation.

use sha2::{Digest, Sha256};

use std::collections::HashMap;

use cgka_traits::engine::SendIntent;
use cgka_traits::engine_state::EpochState;
use cgka_traits::error::EngineError;
use cgka_traits::maintenance::{
    DurableGroupEvolution, GroupEvolutionPhase, GroupEvolutionSemantic, MaintenanceObligation,
    MaintenancePhase, MaintenanceTrigger, PostJoinHistoryFetchCompletion,
    PostJoinHistoryFetchOutcome,
};
use cgka_traits::storage::{MaintenanceStorage, StorageError};
use cgka_traits::{CgkaEngine, MaintenanceRandom, MessageId, Timestamp};

use crate::clock::EngineMaintenanceRandom;
use crate::events::SessionEvent;
use crate::session::{Session, SessionError, engine_gid, session_engine_error};
use crate::{GroupAccess, GroupId};

/// A join's history fetch may take this long before the obligation stops
/// waiting (the design's transient `EoseTimeout`).
const EOSE_TIMEOUT_SECS: u64 = 300;
/// Quiet seconds after the fetch and after every reset.
const QUIET_SECS: u64 = 60;
/// Periodic rotation interval bounds, inclusive (MDK `PERIODIC_*`).
const PERIODIC_MIN_SECS: u64 = 24 * 86_400;
const PERIODIC_MAX_SECS: u64 = 36 * 86_400;
/// Periodic obligations sample jitter from this inclusive range at
/// creation (MDK `0..=15 min`).
const PERIODIC_JITTER_MAX_MS: u64 = 15 * 60 * 1_000;
/// Re-check delay for an obligation blocked on the gate or a failed
/// self-update send, and for one waiting on a fanout confirm.
const SELF_UPDATE_RECHECK_SECS: u64 = 60;

const PERIODIC_OBLIGATION_DOMAIN: &[u8] = b"marmot-periodic-self-update-v1";

type EngineGroupId = cgka_traits::GroupId;

fn fold_ms(due: &mut Option<u64>, at_ms: u64) {
    *due = Some(due.map_or(at_ms, |current: u64| current.min(at_ms)));
}

/// Index of the newest queued `SelfUpdated` for the group, if any.
fn self_updated_position(events: &[SessionEvent], wanted: &GroupId) -> Option<usize> {
    events
        .iter()
        .rposition(|event| matches!(event, SessionEvent::SelfUpdated { group } if group == wanted))
}

const fn open_obligation(obligation: &MaintenanceObligation) -> bool {
    !matches!(
        obligation.phase,
        MaintenancePhase::Complete | MaintenancePhase::Failed
    )
}

/// The persisted `not_before` deadline is sampled once from
/// `sampled_jitter_ms`, ceiling-rounded into seconds (C15).
const fn jitter_deadline(now: Timestamp, sampled_jitter_ms: u64) -> Timestamp {
    let jitter_secs = sampled_jitter_ms.saturating_add(999) / 1_000;
    Timestamp(now.0.saturating_add(jitter_secs))
}

impl Session {
    /// Record that this device's post-join history fetch reached
    /// end-of-stored-events and everything it delivered was processed.
    ///
    /// When the group's owed [`PostJoinHistoryFetch`](cgka_traits::maintenance::PostJoinHistoryFetch) is still open this
    /// records a `Fetched` completion and moves the post-join obligation
    /// from `CatchUp` to `Quiet` — both writes in one storage unit. Every
    /// other call is an idempotent no-op (a group with a completed fetch,
    /// or a created group that owes none).
    ///
    /// # Errors
    /// [`SessionError::UnknownGroup`] for a group this device does not
    /// know; [`SessionError::NotAMember`] for a removed or disbanded copy;
    /// [`SessionError`] for storage failures.
    pub fn history_fetched(&mut self, group: &GroupId) -> Result<(), SessionError> {
        let gid = engine_gid(group);
        let snapshot = match self.group(group) {
            Ok(Some(snapshot)) => snapshot,
            Ok(None) | Err(SessionError::Storage(StorageError::NotFound)) => {
                return Err(SessionError::UnknownGroup);
            }
            Err(error) => return Err(error),
        };
        // A quarantined copy reports its record now, but it is still
        // refused like the `None` it answered before this vocabulary.
        if snapshot.access == GroupAccess::Quarantined {
            return Err(SessionError::UnknownGroup);
        }
        if matches!(
            snapshot.access,
            GroupAccess::RemovedDevice | GroupAccess::Removed | GroupAccess::Disbanded
        ) {
            return Err(SessionError::NotAMember);
        }
        let now_ms = self.now_ms();
        let now = Timestamp(now_ms / 1_000);
        let provider = &self.provider;
        provider.store().transact(|| -> Result<(), SessionError> {
            let Some(mut state) = provider
                .group_maintenance(&gid)
                .map_err(SessionError::Storage)?
            else {
                return Ok(());
            };
            let Some(fetch) = state.post_join_history.as_mut() else {
                return Ok(());
            };
            if fetch.completion.is_some() {
                return Ok(());
            }
            fetch.completion = Some(PostJoinHistoryFetchCompletion {
                outcome: PostJoinHistoryFetchOutcome::Fetched,
                completed_at_ms: now_ms,
            });
            let obligation_id = fetch.obligation_id.clone();
            provider
                .put_group_maintenance(&state)
                .map_err(SessionError::Storage)?;
            if let Some(mut obligation) = provider
                .maintenance_obligation(&obligation_id)
                .map_err(SessionError::Storage)?
                && obligation.phase == MaintenancePhase::CatchUp
            {
                obligation.phase = MaintenancePhase::Quiet;
                obligation.quiet_since = Some(now);
                provider
                    .put_maintenance_obligation(&obligation)
                    .map_err(SessionError::Storage)?;
            }
            Ok(())
        })?;
        self.stall_on_history_fetched(group, snapshot.epoch)
    }

    /// The D4 driver, run by [`Session::tick`]: enrols periodic deadlines,
    /// walks every open obligation through its phase machine and answers
    /// the earliest absolute deadline (Unix ms) the caller should wake on.
    /// Only deadlines strictly in the future contribute — anything due now
    /// was acted on this pass or cannot be, so a past deadline never arms
    /// an immediate re-wake.
    ///
    /// Scoping: one obligation scan for the whole pass, live group ids
    /// rather than snapshots, and groups that owe nothing and are not
    /// periodic-enrolled cost exactly one maintenance-row read.
    pub(crate) async fn drive_self_update(&mut self) -> Result<Option<u64>, SessionError> {
        let now_ms = self.now_ms();
        let now = Timestamp(now_ms / 1_000);
        let mut due: Option<u64> = None;

        // One obligation scan for the pass, bucketed by group.
        let mut by_group: HashMap<EngineGroupId, Vec<MaintenanceObligation>> = HashMap::new();
        for obligation in self
            .provider
            .list_maintenance_obligations()
            .map_err(SessionError::Storage)?
        {
            if open_obligation(&obligation) {
                by_group
                    .entry(obligation.group_id.clone())
                    .or_default()
                    .push(obligation);
            }
        }
        for obligations in by_group.values_mut() {
            obligations.sort_by_key(|obligation| obligation.created_at);
        }

        for gid in self.engine.live_group_ids().map_err(session_engine_error)? {
            let mut obligations = by_group.remove(&gid).unwrap_or_default();
            let mut maintenance = self
                .provider
                .group_maintenance(&gid)
                .map_err(SessionError::Storage)?;
            // Skip groups that owe nothing and are not periodic-enrolled:
            // one maintenance-row read, nothing else touched.
            if obligations.is_empty()
                && maintenance
                    .as_ref()
                    .is_none_or(|state| !state.periodic_enrolled)
            {
                continue;
            }
            // The obligation outlives the membership it was minted under:
            // verdict a terminal copy's rows once rather than let the send
            // gate refuse forever.
            if !obligations.is_empty() && self.group_is_terminal(&gid)? {
                self.fail_terminal_obligations(obligations)?;
                continue;
            }
            // (1) Periodic scheduling: only while no obligation is open.
            if let Some(state) = &mut maintenance
                && state.periodic_enrolled
            {
                self.schedule_periodic(&gid, state, &mut obligations, now, now_ms, &mut due)?;
            }

            // (2) Each open obligation, in creation order.
            for obligation in obligations {
                self.drive_obligation(obligation, &gid, &mut maintenance, now, now_ms, &mut due)
                    .await?;
            }
        }
        // Open obligations of groups the engine no longer lists live
        // (removed/disbanded/forgotten) get the same terminal verdict;
        // quarantined copies stay open — they may still recover.
        for (gid, obligations) in by_group {
            if self.group_is_terminal(&gid)? {
                self.fail_terminal_obligations(obligations)?;
            }
        }
        Ok(due)
    }

    /// Whether the group's durable record marks the local copy terminal:
    /// removed, unrecoverable or disbanded. A group with no record at all
    /// counts as terminal — its obligations could never pass the gate.
    fn group_is_terminal(&self, gid: &EngineGroupId) -> Result<bool, SessionError> {
        let record = match self.engine.group_record(gid) {
            Ok(record) => record,
            Err(EngineError::UnknownGroup(_)) => return Ok(true),
            Err(error) => return Err(session_engine_error(error)),
        };
        if record.removed || record.unrecoverable || record.disbanded.is_some() {
            return Ok(true);
        }
        Ok(matches!(
            self.engine.epoch_state(gid),
            Some(EpochState::Disbanded(_) | EpochState::Unrecoverable(_))
        ))
    }

    /// Write an obligation row only when it changed since the value read
    /// this pass (MDK `put_maintenance_obligation_if_changed`): a waiting
    /// obligation must not cost a KV batch per tick.
    fn put_obligation_if_changed(
        &self,
        before: &MaintenanceObligation,
        after: &MaintenanceObligation,
    ) -> Result<(), SessionError> {
        if after != before {
            self.provider
                .put_maintenance_obligation(after)
                .map_err(SessionError::Storage)?;
        }
        Ok(())
    }

    /// Verdict every open obligation of a removed/disbanded copy
    /// `local_member_removed`, in one storage unit.
    fn fail_terminal_obligations(
        &self,
        mut obligations: Vec<MaintenanceObligation>,
    ) -> Result<(), SessionError> {
        if obligations.is_empty() {
            return Ok(());
        }
        let provider = &self.provider;
        provider.store().transact(|| -> Result<(), SessionError> {
            for obligation in &mut obligations {
                obligation.phase = MaintenancePhase::Failed;
                obligation.last_failure_code = Some("local_member_removed".into());
                provider
                    .put_maintenance_obligation(obligation)
                    .map_err(SessionError::Storage)?;
            }
            Ok(())
        })
    }

    /// Periodic enrolment: sample `next_periodic_rotation_at` when absent,
    /// mint the Periodic obligation when due, else fold the deadline.
    fn schedule_periodic(
        &self,
        gid: &EngineGroupId,
        state: &mut cgka_traits::maintenance::GroupMaintenanceState,
        obligations: &mut Vec<MaintenanceObligation>,
        now: Timestamp,
        now_ms: u64,
        due: &mut Option<u64>,
    ) -> Result<(), SessionError> {
        if obligations.is_empty() && state.next_periodic_rotation_at.is_none() {
            let baseline = periodic_baseline(state, &self.list_group_obligations(gid)?, now);
            state.next_periodic_rotation_at = Some(self.fresh_periodic_deadline(gid, baseline)?);
            self.provider
                .put_group_maintenance(state)
                .map_err(SessionError::Storage)?;
        }
        let Some(deadline) = state.next_periodic_rotation_at else {
            return Ok(());
        };
        if obligations.is_empty() && deadline <= now {
            let fresh = self.new_periodic_obligation(gid, deadline, now)?;
            self.provider
                .put_maintenance_obligation(&fresh)
                .map_err(SessionError::Storage)?;
            obligations.push(fresh);
            obligations.sort_by_key(|entry| entry.created_at);
        } else if deadline.0 * 1_000 > now_ms {
            fold_ms(due, deadline.0 * 1_000);
        }
        Ok(())
    }

    /// One obligation's phase transition for this pass.
    async fn drive_obligation(
        &mut self,
        mut obligation: MaintenanceObligation,
        gid: &EngineGroupId,
        maintenance: &mut Option<cgka_traits::maintenance::GroupMaintenanceState>,
        now: Timestamp,
        now_ms: u64,
        due: &mut Option<u64>,
    ) -> Result<(), SessionError> {
        // A live obligation's baseline is its evidence: a leaf that
        // already rotated satisfies it without any commit of ours.
        if let Some(baseline) = &obligation.own_leaf_baseline_hash {
            let current = self
                .engine
                .own_leaf_hash(gid)
                .map_err(session_engine_error)?;
            if baseline.as_slice() != current.as_slice() {
                self.complete_obligation(&mut obligation, now)?;
                return Ok(());
            }
        }
        if obligation
            .operational_target_at
            .is_some_and(|target| target <= now)
            && !obligation.overdue
        {
            obligation.overdue = true;
            self.provider
                .put_maintenance_obligation(&obligation)
                .map_err(SessionError::Storage)?;
        }
        // The 24 h target skips every remaining wait — the gate still
        // applies.
        if obligation.overdue
            && matches!(
                obligation.phase,
                MaintenancePhase::CatchUp
                    | MaintenancePhase::EoseTimeout
                    | MaintenancePhase::Grace
                    | MaintenancePhase::Quiet
                    | MaintenancePhase::Jitter
                    | MaintenancePhase::Overdue
            )
        {
            obligation.phase = MaintenancePhase::Overdue;
            self.try_self_update_send(&mut obligation, gid, now_ms, due)
                .await?;
            return Ok(());
        }
        match obligation.phase {
            MaintenancePhase::CatchUp => {
                self.drive_catchup(&mut obligation, maintenance, now, now_ms, due)?;
            }
            MaintenancePhase::Quiet => {
                self.drive_quiet(&mut obligation, now, now_ms, due)?;
            }
            MaintenancePhase::Jitter => {
                let not_before = obligation.not_before.unwrap_or(now);
                if now < not_before {
                    fold_ms(due, not_before.0 * 1_000);
                } else {
                    self.try_self_update_send(&mut obligation, gid, now_ms, due)
                        .await?;
                }
            }
            MaintenancePhase::Overdue => {
                self.try_self_update_send(&mut obligation, gid, now_ms, due)
                    .await?;
            }
            MaintenancePhase::PendingPublication => {
                self.drive_pending_publication(&mut obligation, gid, now, now_ms, due)?;
            }
            _ => {
                // Retry/SupersededByConvergence/Paused/ClockSkewBlocked
                // states the session driver does not mint: re-arm a
                // bounded re-check so a drifted row cannot stall forever.
                fold_ms(due, now_ms + SELF_UPDATE_RECHECK_SECS * 1_000);
            }
        }
        Ok(())
    }

    /// `CatchUp`: the recorded fetch completion or the 300 s deadline
    /// moves the obligation to `Quiet`; otherwise it waits for the
    /// deadline.
    fn drive_catchup(
        &self,
        obligation: &mut MaintenanceObligation,
        maintenance: &mut Option<cgka_traits::maintenance::GroupMaintenanceState>,
        now: Timestamp,
        now_ms: u64,
        due: &mut Option<u64>,
    ) -> Result<(), SessionError> {
        if let Some(completed_at) = Self::post_join_completion(maintenance.as_ref(), obligation) {
            obligation.phase = MaintenancePhase::Quiet;
            obligation.quiet_since = Some(completed_at);
            self.provider
                .put_maintenance_obligation(obligation)
                .map_err(SessionError::Storage)?;
            return Ok(());
        }
        let timeout = Timestamp(obligation.created_at.0 + EOSE_TIMEOUT_SECS);
        if now >= timeout {
            // The design's transient EoseTimeout: record the
            // deadline-expired completion and start Quiet now, in one
            // storage unit.
            return self.expire_post_join(maintenance, obligation, now_ms, now);
        }
        fold_ms(due, timeout.0 * 1_000);
        Ok(())
    }

    /// `Quiet`: past `quiet_since + 60 s` the obligation enters `Jitter`
    /// with a persisted `not_before` — sampled once, never resampled.
    fn drive_quiet(
        &self,
        obligation: &mut MaintenanceObligation,
        now: Timestamp,
        now_ms: u64,
        due: &mut Option<u64>,
    ) -> Result<(), SessionError> {
        let quiet_since = obligation.quiet_since.unwrap_or(now);
        let quiet_done = Timestamp(quiet_since.0 + QUIET_SECS);
        if now < quiet_done {
            fold_ms(due, quiet_done.0 * 1_000);
            return Ok(());
        }
        obligation.phase = MaintenancePhase::Jitter;
        obligation.not_before = Some(jitter_deadline(now, obligation.sampled_jitter_ms));
        self.provider
            .put_maintenance_obligation(obligation)
            .map_err(SessionError::Storage)?;
        if let Some(at) = obligation.not_before
            && at.0 * 1_000 > now_ms
        {
            fold_ms(due, at.0 * 1_000);
        }
        Ok(())
    }

    /// `PendingPublication`: a `Confirmed` self-update evolution completes
    /// the obligation; a live `Prepared`/`Attempting` one waits (C16 keeps
    /// it open across restart until the confirm re-derives); a `Preparing`
    /// row carries no signed artifact — crash residue that owns nothing —
    /// `SupersededByConvergence` or none — the leaf check decides between
    /// completing and a one-time re-arm to `Quiet`.
    fn drive_pending_publication(
        &mut self,
        obligation: &mut MaintenanceObligation,
        gid: &EngineGroupId,
        now: Timestamp,
        now_ms: u64,
        due: &mut Option<u64>,
    ) -> Result<(), SessionError> {
        let evolutions = self
            .provider
            .list_group_evolutions_for_group(gid)
            .map_err(SessionError::Storage)?;
        let ours = |evolution: &DurableGroupEvolution| {
            matches!(
                &evolution.semantic,
                GroupEvolutionSemantic::SelfUpdate { obligation_id, .. }
                    if obligation_id.as_ref() == Some(&obligation.id)
            )
        };
        if evolutions
            .iter()
            .any(|evolution| ours(evolution) && evolution.phase == GroupEvolutionPhase::Confirmed)
        {
            self.complete_obligation(obligation, now)?;
            return Ok(());
        }
        if evolutions.iter().any(|evolution| {
            ours(evolution)
                && matches!(
                    evolution.phase,
                    GroupEvolutionPhase::Prepared | GroupEvolutionPhase::Attempting
                )
        }) {
            fold_ms(due, now_ms + SELF_UPDATE_RECHECK_SECS * 1_000);
            return Ok(());
        }
        // The evolution was superseded (a lost same-epoch race) or is
        // gone: a changed own leaf still satisfies the obligation.
        let current = self
            .engine
            .own_leaf_hash(gid)
            .map_err(session_engine_error)?;
        if obligation
            .own_leaf_baseline_hash
            .as_ref()
            .is_some_and(|baseline| baseline.as_slice() != current.as_slice())
        {
            self.complete_obligation(obligation, now)?;
            return Ok(());
        }
        obligation.phase = MaintenancePhase::Quiet;
        obligation.quiet_since = Some(now);
        obligation.not_before = None;
        obligation.semantic_rearm_count = obligation.semantic_rearm_count.saturating_add(1);
        self.provider
            .put_maintenance_obligation(obligation)
            .map_err(SessionError::Storage)?;
        fold_ms(due, now_ms + QUIET_SECS * 1_000);
        Ok(())
    }

    /// Gate + send for an obligation whose waits have elapsed. A closed
    /// gate or an engine error leaves the obligation's phase unchanged and
    /// re-arms a bounded re-check — neither ever fails the obligation or
    /// the tick.
    async fn try_self_update_send(
        &mut self,
        obligation: &mut MaintenanceObligation,
        gid: &EngineGroupId,
        now_ms: u64,
        due: &mut Option<u64>,
    ) -> Result<(), SessionError> {
        let open = self.self_update_gate_open(gid).unwrap_or_default();
        if !open {
            // Nothing changed: no row write, just the re-check deadline.
            fold_ms(due, now_ms + SELF_UPDATE_RECHECK_SECS * 1_000);
            return Ok(());
        }
        let sent = self
            .engine
            .send(SendIntent::SelfUpdate {
                group_id: gid.clone(),
            })
            .await;
        let before = obligation.clone();
        if let Ok(result) = sent {
            self.finish_send(result)?;
            obligation.phase = MaintenancePhase::PendingPublication;
            obligation.attempt_count = obligation.attempt_count.saturating_add(1);
            obligation.last_failure_code = None;
        } else {
            // Engine error: never fail tick — record the failure and
            // leave the phase for the next pass.
            obligation.last_failure_code = Some("self_update_send_failed".into());
        }
        self.put_obligation_if_changed(&before, obligation)?;
        fold_ms(due, now_ms + SELF_UPDATE_RECHECK_SECS * 1_000);
        Ok(())
    }

    /// The D4 gate: all conditions must hold for the self-update to be
    /// staged. A `false` answer keeps the obligation open and retryable;
    /// sends are never blocked by an open obligation.
    fn self_update_gate_open(&self, gid: &EngineGroupId) -> Result<bool, SessionError> {
        if !matches!(
            self.engine.epoch_state(gid),
            Some(EpochState::Stable { .. })
        ) {
            return Ok(false);
        }
        if self
            .engine
            .has_pending_convergence_inputs(gid)
            .map_err(session_engine_error)?
            || self
                .engine
                .has_queued_outbound_intents(gid)
                .map_err(session_engine_error)?
        {
            return Ok(false);
        }
        // No outbound fanout whose MLS state is still pending — an own
        // commit in flight holds the gate. A `Preparing` row is not live:
        // it carries no signed artifact, so it owns nothing (MDK
        // maintenance.rs: "without a signed artifact, ordinary Preparing
        // owns nothing"); it is only ever observed as the residue of a
        // crash between the intent write and the staged commit, and
        // marmot-session likewise counts only `Prepared`/`Attempting`.
        if self
            .provider
            .list_group_evolutions_for_group(gid)
            .map_err(SessionError::Storage)?
            .iter()
            .any(|evolution| {
                matches!(
                    evolution.phase,
                    GroupEvolutionPhase::Prepared | GroupEvolutionPhase::Attempting
                )
            })
        {
            return Ok(false);
        }
        if self
            .engine
            .leave_in_progress(gid)
            .map_err(session_engine_error)?
            || self
                .engine
                .disbanding_in_progress(gid)
                .map_err(session_engine_error)?
        {
            return Ok(false);
        }
        let unrecoverable = matches!(
            self.engine.epoch_state(gid),
            Some(EpochState::Unrecoverable(_))
        );
        Ok(!unrecoverable)
    }

    /// Quiet resets after ingest: every accepted valid proposal re-enters
    /// its group's open obligations into `Quiet` (`CatchUp` is not reset —
    /// it waits for the fetch). Called once per ingest/ingest batch.
    pub(crate) fn note_valid_state_bearing_inputs(&mut self) -> Result<(), SessionError> {
        let now = Timestamp(self.now_ms() / 1_000);
        for gid in self.engine.drain_valid_proposal_groups() {
            self.reset_obligations_quiet(&gid, now)?;
        }
        Ok(())
    }

    /// Reset one group's open `Quiet`/`Jitter` obligations — plus a
    /// `CatchUp` whose fetch already completed — to `Quiet`
    /// (`quiet_since = now`, `not_before = None`).
    pub(crate) fn reset_obligations_quiet(
        &self,
        gid: &EngineGroupId,
        now: Timestamp,
    ) -> Result<(), SessionError> {
        let obligations = self
            .provider
            .list_maintenance_obligations_for_group(gid)
            .map_err(SessionError::Storage)?;
        let fetch_done = self
            .provider
            .group_maintenance(gid)
            .map_err(SessionError::Storage)?
            .and_then(|state| state.post_join_history)
            .is_some_and(|fetch| fetch.completion.is_some());
        for mut obligation in obligations {
            if !open_obligation(&obligation) {
                continue;
            }
            let resettable = matches!(
                obligation.phase,
                MaintenancePhase::Quiet | MaintenancePhase::Jitter
            ) || (obligation.phase == MaintenancePhase::CatchUp && fetch_done);
            if !resettable {
                continue;
            }
            obligation.phase = MaintenancePhase::Quiet;
            obligation.quiet_since = Some(now);
            obligation.not_before = None;
            self.provider
                .put_maintenance_obligation(&obligation)
                .map_err(SessionError::Storage)?;
        }
        Ok(())
    }

    /// Inside `drain_events`' storage unit: one `EpochChanged` per group
    /// either completes every open obligation whose baseline leaf already
    /// rotated, or re-enters the resettable ones into `Quiet`.
    pub(crate) fn note_epoch_changed(
        &mut self,
        gid: &EngineGroupId,
        now: Timestamp,
        out: &mut Vec<SessionEvent>,
    ) -> Result<(), SessionError> {
        let obligations = self
            .provider
            .list_maintenance_obligations_for_group(gid)
            .map_err(SessionError::Storage)?;
        // A group whose live leaf is gone (removed/disbanded copy) cannot
        // be judged here — the driver's terminal verdict handles it.
        let Ok(current) = self.engine.own_leaf_hash(gid) else {
            return Ok(());
        };
        let fetch_done = self
            .provider
            .group_maintenance(gid)
            .map_err(SessionError::Storage)?
            .and_then(|state| state.post_join_history)
            .is_some_and(|fetch| fetch.completion.is_some());
        for mut obligation in obligations {
            if !open_obligation(&obligation) {
                continue;
            }
            if obligation
                .own_leaf_baseline_hash
                .as_ref()
                .is_some_and(|baseline| baseline.as_slice() != current.as_slice())
            {
                self.complete_obligation(&mut obligation, now)?;
                // `complete_obligation` pushes SelfUpdated into
                // resolutions, which drain_events appends after this
                // unit — move it into this drain's output now.
                let wanted = GroupId(gid.as_slice().to_vec());
                let position = self_updated_position(&self.resolutions, &wanted);
                out.extend(position.map(|index| self.resolutions.remove(index)));
                continue;
            }
            let resettable = matches!(
                obligation.phase,
                MaintenancePhase::Quiet | MaintenancePhase::Jitter
            ) || (obligation.phase == MaintenancePhase::CatchUp && fetch_done);
            if resettable {
                obligation.phase = MaintenancePhase::Quiet;
                obligation.quiet_since = Some(now);
                obligation.not_before = None;
                self.provider
                    .put_maintenance_obligation(&obligation)
                    .map_err(SessionError::Storage)?;
            }
        }
        Ok(())
    }

    /// The post-join obligation's recorded fetch completion, if the fetch
    /// belongs to this obligation and is done.
    fn post_join_completion(
        maintenance: Option<&cgka_traits::maintenance::GroupMaintenanceState>,
        obligation: &MaintenanceObligation,
    ) -> Option<Timestamp> {
        maintenance
            .and_then(|state| state.post_join_history.clone())
            .filter(|fetch| fetch.obligation_id == obligation.id)
            .and_then(|fetch| fetch.completion)
            .map(|completion| Timestamp(completion.completed_at_ms / 1_000))
    }

    /// The transient `EoseTimeout`: record `DeadlineExpired` on the owed
    /// fetch and move the obligation to `Quiet`, in one storage unit.
    fn expire_post_join(
        &self,
        maintenance: &mut Option<cgka_traits::maintenance::GroupMaintenanceState>,
        obligation: &mut MaintenanceObligation,
        now_ms: u64,
        now: Timestamp,
    ) -> Result<(), SessionError> {
        let provider = &self.provider;
        provider.store().transact(|| -> Result<(), SessionError> {
            if let Some(state) = maintenance
                && let Some(fetch) = state.post_join_history.as_mut()
                && fetch.obligation_id == obligation.id
                && fetch.completion.is_none()
            {
                fetch.completion = Some(PostJoinHistoryFetchCompletion {
                    outcome: PostJoinHistoryFetchOutcome::DeadlineExpired,
                    completed_at_ms: now_ms,
                });
                provider
                    .put_group_maintenance(state)
                    .map_err(SessionError::Storage)?;
            }
            obligation.phase = MaintenancePhase::Quiet;
            obligation.quiet_since = Some(now);
            provider
                .put_maintenance_obligation(obligation)
                .map_err(SessionError::Storage)?;
            Ok(())
        })
    }

    /// MDK `fresh_periodic_deadline`: `baseline + sample(24–36 d)`, bumped
    /// one second at a time while the SHA-256 obligation id collides.
    fn fresh_periodic_deadline(
        &self,
        gid: &EngineGroupId,
        baseline: Timestamp,
    ) -> Result<Timestamp, SessionError> {
        let earliest = baseline.0.saturating_add(PERIODIC_MIN_SECS);
        let latest = baseline.0.saturating_add(PERIODIC_MAX_SECS);
        let first = baseline.0.saturating_add(
            EngineMaintenanceRandom.sample_inclusive(PERIODIC_MIN_SECS, PERIODIC_MAX_SECS),
        );
        let mut deadline = first;
        loop {
            if self
                .provider
                .maintenance_obligation(&periodic_obligation_id(gid, deadline))
                .map_err(SessionError::Storage)?
                .is_none()
            {
                return Ok(Timestamp(deadline));
            }
            deadline = if deadline < latest {
                deadline + 1
            } else {
                earliest
            };
            if deadline == first {
                return Err(SessionError::Internal("periodic deadline exhausted"));
            }
        }
    }

    /// Every obligation row for a group, open or not (the periodic
    /// baseline's local-member-removed check reads Failed rows too).
    fn list_group_obligations(
        &self,
        gid: &EngineGroupId,
    ) -> Result<Vec<MaintenanceObligation>, SessionError> {
        self.provider
            .list_maintenance_obligations_for_group(gid)
            .map_err(SessionError::Storage)
    }

    /// Create the Periodic obligation for a due `next_periodic_rotation_at`
    /// exactly like MDK: SHA-256 id over the domain string, gid and
    /// deadline; phase `Quiet` with `quiet_since = now`; jitter
    /// `0..=15 min`; no operational target.
    fn new_periodic_obligation(
        &self,
        gid: &EngineGroupId,
        deadline: Timestamp,
        now: Timestamp,
    ) -> Result<MaintenanceObligation, SessionError> {
        let own_leaf_baseline_hash = Some(
            self.engine
                .own_leaf_hash(gid)
                .map_err(session_engine_error)?,
        );
        Ok(MaintenanceObligation {
            id: periodic_obligation_id(gid, deadline.0),
            group_id: gid.clone(),
            trigger: MaintenanceTrigger::Periodic,
            phase: MaintenancePhase::Quiet,
            created_at: now,
            operational_target_at: None,
            overdue: false,
            eose_deadline_at: None,
            grace_until: None,
            quiet_since: Some(now),
            own_leaf_baseline_hash,
            sampled_jitter_ms: EngineMaintenanceRandom.sample_inclusive(0, PERIODIC_JITTER_MAX_MS),
            not_before: None,
            attempt_count: 0,
            semantic_rearm_count: 0,
            last_failure_code: None,
        })
    }

    /// Step 3 of the loop: mark the obligation `Complete`, rotate the
    /// group's `last_own_leaf_rotation_at` and sample the next periodic
    /// deadline — group state and obligation in one storage unit — then
    /// emit `SelfUpdated`.
    fn complete_obligation(
        &mut self,
        obligation: &mut MaintenanceObligation,
        now: Timestamp,
    ) -> Result<(), SessionError> {
        let provider = &self.provider;
        provider.store().transact(|| -> Result<(), SessionError> {
            if let Some(mut state) = provider
                .group_maintenance(&obligation.group_id)
                .map_err(SessionError::Storage)?
            {
                state.last_own_leaf_rotation_at = Some(now);
                state.next_periodic_rotation_at = state
                    .periodic_enrolled
                    .then(|| self.fresh_periodic_deadline(&obligation.group_id, now))
                    .transpose()?;
                provider
                    .put_group_maintenance(&state)
                    .map_err(SessionError::Storage)?;
            }
            obligation.phase = MaintenancePhase::Complete;
            obligation.last_failure_code = None;
            provider
                .put_maintenance_obligation(obligation)
                .map_err(SessionError::Storage)?;
            Ok(())
        })?;
        self.resolutions.push(SessionEvent::SelfUpdated {
            group: GroupId(obligation.group_id.as_slice().to_vec()),
        });
        Ok(())
    }
}

/// MDK's periodic baseline: the last own-leaf rotation, reset to `now`
/// after a `local_member_removed` failure so the restored copy restarts
/// the interval rather than firing at once.
fn periodic_baseline(
    state: &cgka_traits::maintenance::GroupMaintenanceState,
    obligations: &[MaintenanceObligation],
    now: Timestamp,
) -> Timestamp {
    if obligations.iter().any(|obligation| {
        obligation.group_id == state.group_id
            && obligation.phase == MaintenancePhase::Failed
            && obligation.last_failure_code.as_deref() == Some("local_member_removed")
    }) {
        return now;
    }
    state.last_own_leaf_rotation_at.unwrap_or(now)
}

/// `SHA-256("marmot-periodic-self-update-v1" ‖ u64be(len gid) ‖ gid ‖
/// u64be(deadline))` — MDK's periodic obligation id.
fn periodic_obligation_id(gid: &EngineGroupId, deadline_secs: u64) -> MessageId {
    let mut hasher = Sha256::new();
    hasher.update(PERIODIC_OBLIGATION_DOMAIN);
    hasher.update((gid.as_slice().len() as u64).to_be_bytes());
    hasher.update(gid.as_slice());
    hasher.update(deadline_secs.to_be_bytes());
    MessageId::new(hasher.finalize().to_vec())
}
