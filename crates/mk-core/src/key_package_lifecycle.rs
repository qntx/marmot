//! `KeyPackage` lifecycle driver (design D3): the engine's
//! [`KeyPackageLifecycleState`] driven by the session, mirroring
//! `marmot-session`'s runtime — `prepare_key_package_replacement`, the
//! `publish_fresh_key_package` promotion block,
//! `sweep_expired_key_package_private_material` and
//! `maintenance_due::key_package_due` (v0.18.0 `runtime.rs`).
//!
//! [`Session::drive_key_package`] runs every step in one pass: sweep
//! expired private material, grow the write set on new relays, re-derive
//! an acknowledged promotion from the durable report row, sign a staged
//! replacement into its durable outbox row, re-offer a failed publication
//! after backoff, and stage a replacement when one is due.
//! [`Session::ensure_key_package`] runs it against the host's current
//! NIP-65 write set; [`Session::tick`] runs it relay-less, where a
//! signer failure leaves the pending unsigned and arms an in-memory
//! retry. The record is re-read from storage at the start of every pass
//! and of every report — the engine's join transaction writes the
//! consumed marker into it — and is never cached.

use std::collections::HashSet;

use cgka_traits::maintenance::{
    KEY_PACKAGE_GENERATION_REVISION, KeyPackageLifecycleState, MaintenancePhase,
    PendingKeyPackageReplacement, RetainedKeyPackagePrivateMaterial, SignedPublicationArtifact,
    TransportFanoutAttemptState, TransportFanoutTarget,
};
use cgka_traits::storage::{MaintenanceStorage, StorageError};
use cgka_traits::transport_adapter::TransportEndpoint;
use cgka_traits::{MaintenanceRandom, MessageId, Timestamp};

use crate::clock::EngineMaintenanceRandom;
use crate::events::SessionEvent;
use crate::key_package::{self, KEY_PACKAGE_MAX_FUTURE_SKEW_SECS, KeyPackageInfo};
use crate::outbox::{
    PubTargetStatus, RelayResult, RelayStatus, SessionPubPurpose, SessionPublication,
};

use crate::provider::KvStorageProvider;
use crate::session::{Session, SessionError, outbound_id_of, session_engine_error};
use crate::types::OutboundId;

/// Replacement refresh bounds (marmot-session
/// `KEY_PACKAGE_REFRESH_MIN/MAX_SECS`): the slot is republished on a
/// sampled 6–7 day period while the device runs, so an inviter can tell a
/// live device from an uninstalled one.
const KEY_PACKAGE_REFRESH_MIN_SECS: u64 = 6 * 24 * 60 * 60;
const KEY_PACKAGE_REFRESH_MAX_SECS: u64 = 7 * 24 * 60 * 60;
/// A current `KeyPackage` inside its last day is replaced before it lapses.
const KEY_PACKAGE_NEAR_EXPIRY_SECS: u64 = 24 * 60 * 60;
/// Pending publish retry backoff: `min(60 s << attempts, 1 h)` from the
/// row's last attempt (step-2 brief).
const PUBLISH_RETRY_BASE_SECS: u64 = 60;
const PUBLISH_RETRY_CAP_SECS: u64 = 60 * 60;
/// After a swallowed signer failure the retry is armed in memory only —
/// a transient refusal writes no durable row.
const SIGN_RETRY_MS: u64 = 5 * 60 * 1_000;

/// Signer-error policy for a driver pass.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SignMode {
    /// `ensure_key_package`: propagate the failure.
    Strict,
    /// `tick`: leave the pending unsigned and arm an in-memory retry.
    Swallow,
}

/// How this device's `KeyPackage` slot stands, read from the durable
/// lifecycle record (never cached).
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct KeyPackageStatus {
    /// The stable replaceable-event slot (`d` tag), 32 bytes.
    pub slot: [u8; 32],
    /// The promoted, acknowledged publication, if any.
    pub current: Option<KeyPackageInfo>,
    /// Whether a replacement is staged or in flight.
    pub pending: bool,
    /// Retired private material still inside its grace period.
    pub retained: u32,
    /// Absolute Unix milliseconds of the earliest lifecycle deadline —
    /// refresh, near-expiry, retained deletion, `not_after` bounds,
    /// publish retry or clock-skew retry. `None` when nothing is
    /// scheduled.
    pub next_due_ms: Option<u64>,
}

impl Session {
    /// Ensure this device's `KeyPackage` slot carries a live publication
    /// for `write_relays`: stage, sign and queue a replacement when one
    /// is due; re-send the remembered signed event to relays the set
    /// gained; promote a pending replacement once a relay acknowledges
    /// it. Returns the resulting [`KeyPackageStatus`].
    ///
    /// # Errors
    /// [`SessionError::InvalidRelayUrl`] for an empty or disallowed write
    /// set; [`SessionError`] for signer, engine or storage failures.
    pub async fn ensure_key_package(
        &mut self,
        write_relays: &[String],
    ) -> Result<KeyPackageStatus, SessionError> {
        self.check_relays(write_relays)?;
        self.drive_key_package(Some(write_relays), SignMode::Strict)
            .await?;
        self.key_package_status()
    }

    /// This device's `KeyPackage` slot standing, re-read from the durable
    /// record on every call.
    ///
    /// # Errors
    /// [`SessionError::Storage`] on a store read failure.
    pub fn key_package_status(&self) -> Result<KeyPackageStatus, SessionError> {
        let lifecycle = self
            .provider
            .key_package_lifecycle()
            .map_err(SessionError::Storage)?;
        let (pending, retained) = lifecycle.as_ref().map_or((false, 0), |lc| {
            (
                lc.pending_replacement.is_some(),
                u32::try_from(lc.retained_private_material.len()).unwrap_or(u32::MAX),
            )
        });
        Ok(KeyPackageStatus {
            slot: slot_bytes(lifecycle.as_ref())?,
            current: current_info(lifecycle.as_ref())?,
            pending,
            retained,
            next_due_ms: match &lifecycle {
                Some(lc) => self.lifecycle_next_due_ms(lc)?,
                None => None,
            },
        })
    }

    /// One lifecycle pass; returns the earliest deadline as absolute Unix
    /// milliseconds for the caller's wake/status answer.
    ///
    /// `write_relays` is `Some` only on the `ensure` path: relay growth
    /// republishes the remembered signed event there, while `tick` works
    /// from the remembered write set alone.
    pub(crate) async fn drive_key_package(
        &mut self,
        write_relays: Option<&[String]>,
        signing: SignMode,
    ) -> Result<Option<u64>, SessionError> {
        let now_secs = self.now_ms() / 1_000;
        let mut lifecycle = self.lifecycle_or_seed()?;
        // (a) Sweep expired private material.
        self.sweep_lifecycle(&mut lifecycle, now_secs)?;
        // (b) Write-set growth re-sends the same signed event.
        if let Some(relays) = write_relays {
            self.grow_lifecycle_write_set(&mut lifecycle, relays)?;
        }
        // (c)+(d) Promotion re-derivation: a durable Accepted on the
        // pending's row promotes even when the report's commit was cut
        // before the lifecycle write (C12).
        self.promote_if_acknowledged(&mut lifecycle, now_secs)?;
        // (e) Sign the staged replacement into its outbox row.
        self.sign_pending(&mut lifecycle, signing).await?;
        // (f) Re-offer a failed pending publication after backoff.
        self.retry_pending(&mut lifecycle, now_secs)?;
        // (g) Stage a due replacement, then sign it as at (e).
        if self.replacement_due(&lifecycle, now_secs)
            && self.stage_replacement(&mut lifecycle, write_relays, now_secs)?
        {
            self.sign_pending(&mut lifecycle, signing).await?;
        }
        // (h) The earliest deadline feeds `next_wake_ms`/`next_due_ms`.
        self.lifecycle_next_due_ms(&lifecycle)
    }

    /// The lifecycle record, seeding a `slot_only` one when the open-time
    /// write has not happened yet (an empty slot is the join-time
    /// sentinel — a fresh random one replaces it). One batch.
    pub(crate) fn lifecycle_or_seed(&self) -> Result<KeyPackageLifecycleState, SessionError> {
        let lifecycle = self
            .provider
            .key_package_lifecycle()
            .map_err(SessionError::Storage)?;
        if let Some(lc) = &lifecycle
            && !lc.stable_slot_id.is_empty()
        {
            return Ok(lc.clone());
        }
        let mut slot = [0u8; 32];
        getrandom::fill(&mut slot).map_err(|_| SessionError::Internal("session rng"))?;
        let seeded = KeyPackageLifecycleState::slot_only(hex::encode(slot));
        let provider = &self.provider;
        provider
            .store()
            .transact(|| provider.put_key_package_lifecycle(&seeded))
            .map_err(SessionError::Storage)?;
        Ok(seeded)
    }

    /// (a) `sweep_expired_key_package_private_material`: current past
    /// `current_not_after`, pending past `not_after` and retained
    /// material past `delete_at()` retire in one engine transaction.
    fn sweep_lifecycle(
        &mut self,
        lifecycle: &mut KeyPackageLifecycleState,
        now_secs: u64,
    ) -> Result<(), SessionError> {
        let now = Timestamp(now_secs);
        let mut retired = Vec::new();
        if lifecycle
            .current_not_after
            .is_some_and(|deadline| deadline <= now)
            && let Some(expired) = lifecycle.current_key_package.take()
        {
            lifecycle.current_key_package_ref = None;
            lifecycle.current_not_before = None;
            lifecycle.current_not_after = None;
            lifecycle.authored_signed_event = None;
            lifecycle.publication_targets.clear();
            lifecycle.refresh_at = None;
            lifecycle.phase = MaintenancePhase::Retry;
            retired.push(expired);
        }
        if lifecycle
            .pending_replacement
            .as_ref()
            .is_some_and(|pending| pending.not_after <= now)
            && let Some(expired) = lifecycle.pending_replacement.take()
        {
            lifecycle.phase = MaintenancePhase::Failed;
            retired.push(expired.key_package);
        }
        let mut kept = Vec::with_capacity(lifecycle.retained_private_material.len());
        for material in lifecycle.retained_private_material.drain(..) {
            if material.delete_at() <= now {
                retired.push(material.key_package);
            } else {
                kept.push(material);
            }
        }
        lifecycle.retained_private_material = kept;
        if retired.is_empty() {
            return Ok(());
        }
        self.engine
            .promote_key_package_lifecycle(&retired, lifecycle)
            .map_err(session_engine_error)
    }

    /// (b) The write set gained relays: the remembered signed event goes
    /// out to them unchanged (`KeyPackageRepublish` for the current,
    /// `KeyPackage` for the pending). Relays that left the set are kept
    /// (spec silent). Lifecycle write and row write commit together.
    fn grow_lifecycle_write_set(
        &mut self,
        lifecycle: &mut KeyPackageLifecycleState,
        relays: &[String],
    ) -> Result<(), SessionError> {
        let mut known: HashSet<&str> = lifecycle
            .publication_targets
            .iter()
            .map(|target| target.endpoint.0.as_str())
            .collect();
        if let Some(pending) = &lifecycle.pending_replacement {
            known.extend(pending.targets.iter().map(|t| t.endpoint.0.as_str()));
        }
        let new_relays: Vec<String> = relays
            .iter()
            .filter(|relay| !known.contains(relay.as_str()))
            .cloned()
            .collect();
        if new_relays.is_empty() {
            return Ok(());
        }
        let mut rows = Vec::new();
        if let Some(artifact) = lifecycle.authored_signed_event.clone() {
            grow_artifact(
                &self.provider,
                &mut lifecycle.publication_targets,
                &artifact,
                &new_relays,
                SessionPubPurpose::KeyPackageRepublish,
                &mut rows,
            )?;
        }
        if let Some(pending) = lifecycle.pending_replacement.as_mut()
            && let Some(artifact) = pending.signed_event.clone()
        {
            grow_artifact(
                &self.provider,
                &mut pending.targets,
                &artifact,
                &new_relays,
                SessionPubPurpose::KeyPackage,
                &mut rows,
            )?;
        }
        let provider = &self.provider;
        provider
            .store()
            .transact(|| -> Result<(), StorageError> {
                provider.put_key_package_lifecycle(lifecycle)?;
                for row in &rows {
                    provider.put_session_publication(row)?;
                }
                Ok(())
            })
            .map_err(SessionError::Storage)?;
        if let Some(seq) = self.provider.last_seq() {
            for row in &rows {
                let id = OutboundId::from_hex(&row.id)
                    .map_err(|_| SessionError::Internal("session publication id"))?;
                self.gate.insert(id, seq);
            }
        }
        Ok(())
    }

    /// (c) Promotion re-derivation: the pending's signed event is
    /// promoted once a relay acknowledgement is durable on its
    /// publication row. Re-checked on every pass so a crash between the
    /// report row update and the promotion loses nothing (C12).
    fn promote_if_acknowledged(
        &mut self,
        lifecycle: &mut KeyPackageLifecycleState,
        now_secs: u64,
    ) -> Result<(), SessionError> {
        let Some(pending) = &lifecycle.pending_replacement else {
            return Ok(());
        };
        let Some(artifact) = &pending.signed_event else {
            return Ok(());
        };
        let id = outbound_id_of(&artifact.id)?;
        let acknowledged = self
            .provider
            .session_publication(&id)
            .map_err(SessionError::Storage)?
            .is_some_and(|row| row.statuses.contains(&PubTargetStatus::Accepted));
        if !acknowledged {
            return Ok(());
        }
        if promote_pending(&mut self.engine, lifecycle, now_secs)? {
            self.resolutions.push(SessionEvent::KeyPackageRotated);
        }
        Ok(())
    }

    /// (e) The staged replacement's exact signed bytes and its durable
    /// outbox row commit together — durable before any network call
    /// (C11).
    async fn sign_pending(
        &mut self,
        lifecycle: &mut KeyPackageLifecycleState,
        signing: SignMode,
    ) -> Result<(), SessionError> {
        if lifecycle
            .pending_replacement
            .as_ref()
            .is_none_or(|pending| pending.signed_event.is_some())
        {
            return Ok(());
        }
        let pending = lifecycle
            .pending_replacement
            .as_ref()
            .ok_or(SessionError::Internal("pending replacement"))?;
        let unsigned = key_package::key_package_event(
            self.account(),
            &pending.key_package.bytes,
            &lifecycle.stable_slot_id,
            self.client_name.as_deref(),
            pending.authored_created_at.0,
        )
        .map_err(|_| SessionError::Internal("key package build"))?;
        let signed = match self.sign_checked(unsigned).await {
            Ok(signed) => signed,
            Err(_) if signing == SignMode::Swallow => {
                self.key_package_sign_retry_ms = Some(self.now_ms() + SIGN_RETRY_MS);
                return Ok(());
            }
            Err(error) => return Err(error),
        };
        let relays: Vec<String> = pending
            .targets
            .iter()
            .map(|target| target.endpoint.0.clone())
            .collect();
        let pending_mut = lifecycle
            .pending_replacement
            .as_mut()
            .ok_or(SessionError::Internal("pending replacement"))?;
        pending_mut.signed_event = Some(SignedPublicationArtifact {
            id: MessageId::new(signed.id.0.to_vec()),
            created_at: Timestamp(signed.created_at),
            bytes: signed.to_json().into_bytes(),
        });
        let publication = SessionPublication {
            id: signed.id.to_hex(),
            event_json: signed.to_json(),
            statuses: vec![PubTargetStatus::Attempting; relays.len()],
            relays,
            purpose: SessionPubPurpose::KeyPackage,
        };
        let provider = &self.provider;
        provider
            .store()
            .transact(|| -> Result<(), StorageError> {
                provider.put_key_package_lifecycle(lifecycle)?;
                provider.put_session_publication(&publication)?;
                Ok(())
            })
            .map_err(SessionError::Storage)?;
        if let Some(seq) = self.provider.last_seq() {
            self.gate.insert(OutboundId(signed.id.0), seq);
        }
        Ok(())
    }

    /// (f) A pending publication whose row finished with no acceptance
    /// is re-offered after `min(60 s << attempt_count, 1 h)` from the
    /// last attempt — the same signed bytes, a fresh row.
    /// `attempt_count` counts staged re-offers and is incremented here
    /// only: the report that closed the row must not also count it.
    fn retry_pending(
        &mut self,
        lifecycle: &mut KeyPackageLifecycleState,
        now_secs: u64,
    ) -> Result<(), SessionError> {
        let Some(pending) = &lifecycle.pending_replacement else {
            return Ok(());
        };
        let Some(artifact) = pending.signed_event.clone() else {
            return Ok(());
        };
        if !pending.targets.iter().all(|target| {
            matches!(
                target.state,
                TransportFanoutAttemptState::AttemptedFailed
                    | TransportFanoutAttemptState::PolicyProhibited
            )
        }) {
            return Ok(());
        }
        let id = outbound_id_of(&artifact.id)?;
        if self
            .provider
            .session_publication(&id)
            .map_err(SessionError::Storage)?
            .is_some()
        {
            return Ok(());
        }
        let Some(retry_at) = pending
            .targets
            .iter()
            .filter_map(|target| target.last_attempt_at)
            .map(|at| {
                at.0.saturating_add(publish_retry_secs(pending.attempt_count))
            })
            .min()
        else {
            return Ok(());
        };
        if retry_at > now_secs {
            return Ok(());
        }
        let relays: Vec<String> = pending
            .targets
            .iter()
            .map(|target| target.endpoint.0.clone())
            .collect();
        let pending_mut = lifecycle
            .pending_replacement
            .as_mut()
            .ok_or(SessionError::Internal("pending replacement"))?;
        pending_mut.attempt_count = pending_mut.attempt_count.saturating_add(1);
        let publication = publication_row(&artifact, &relays, SessionPubPurpose::KeyPackage)?;
        let provider = &self.provider;
        provider
            .store()
            .transact(|| -> Result<(), StorageError> {
                provider.put_key_package_lifecycle(lifecycle)?;
                provider.put_session_publication(&publication)?;
                Ok(())
            })
            .map_err(SessionError::Storage)?;
        if let Some(seq) = self.provider.last_seq() {
            self.gate.insert(id, seq);
        }
        Ok(())
    }

    /// Whether a replacement is due: no current, the current's ref
    /// recorded consumed by a join, a generator-policy upgrade pending,
    /// capabilities drifted, the weekly refresh reached, or the current
    /// inside its last day. A live pending stands in for the replacement
    /// unless it is itself obsolete.
    fn replacement_due(&self, lifecycle: &KeyPackageLifecycleState, now_secs: u64) -> bool {
        if let Some(pending) = &lifecycle.pending_replacement {
            let obsolete = pending.generation_revision < KEY_PACKAGE_GENERATION_REVISION
                || !self
                    .engine
                    .key_package_advertises_current_capabilities(&pending.key_package)
                    .unwrap_or(true);
            if !obsolete {
                return false;
            }
        }
        let Some(current) = &lifecycle.current_key_package else {
            return true;
        };
        lifecycle.last_consumed_key_package_ref == lifecycle.current_key_package_ref
            || !lifecycle.upgrade_rotation_recorded
            || lifecycle.generation_revision < KEY_PACKAGE_GENERATION_REVISION
            || !self
                .engine
                .key_package_advertises_current_capabilities(current)
                .unwrap_or(true)
            || refresh_due(lifecycle, now_secs)
            || lifecycle.current_not_after.is_some_and(|deadline| {
                deadline.0.saturating_sub(now_secs) < KEY_PACKAGE_NEAR_EXPIRY_SECS
            })
    }

    /// (g) Stage the due replacement through the engine's atomic
    /// stage+record op — the C10 boundary — with a `created_at` newer
    /// than every publication the slot has seen. Returns `false` when
    /// nothing should be staged this pass.
    fn stage_replacement(
        &mut self,
        lifecycle: &mut KeyPackageLifecycleState,
        write_relays: Option<&[String]>,
        now_secs: u64,
    ) -> Result<bool, SessionError> {
        // The remembered write set first, else the given relays; when
        // neither is known, `tick` waits for the host's first
        // `ensure_key_package`.
        let mut endpoints: Vec<String> = lifecycle
            .publication_targets
            .iter()
            .map(|target| target.endpoint.0.clone())
            .collect();
        if endpoints.is_empty()
            && let Some(relays) = write_relays
        {
            endpoints.extend(relays.iter().cloned());
        }
        let mut seen = HashSet::new();
        endpoints.retain(|endpoint| seen.insert(endpoint.clone()));
        if endpoints.is_empty() {
            return Ok(false);
        }
        // Newer than every prior publication and any withdrawal: relays
        // drop anything up to that.
        let base = [
            lifecycle.authored_event_created_at,
            lifecycle
                .pending_replacement
                .as_ref()
                .map(|pending| pending.authored_created_at),
            lifecycle.slot_withdrawn_at,
        ]
        .into_iter()
        .flatten()
        .map(|previous| previous.0.saturating_add(1))
        .max()
        .unwrap_or(0);
        let created_at = now_secs.max(base);
        if created_at > now_secs.saturating_add(KEY_PACKAGE_MAX_FUTURE_SKEW_SECS) {
            lifecycle.phase = MaintenancePhase::ClockSkewBlocked;
            let provider = &self.provider;
            provider
                .store()
                .transact(|| provider.put_key_package_lifecycle(lifecycle))
                .map_err(SessionError::Storage)?;
            return Ok(false);
        }
        let refresh_after = EngineMaintenanceRandom
            .sample_inclusive(KEY_PACKAGE_REFRESH_MIN_SECS, KEY_PACKAGE_REFRESH_MAX_SECS);
        let targets = endpoints
            .into_iter()
            .map(|endpoint| TransportFanoutTarget {
                endpoint: TransportEndpoint(endpoint),
                state: TransportFanoutAttemptState::Unattempted,
                attempt_count: 0,
                last_attempt_at: None,
                failure_code: None,
            })
            .collect();
        self.engine
            .stage_key_package_replacement(lifecycle, Timestamp(created_at), refresh_after, targets)
            .map_err(session_engine_error)?;
        Ok(true)
    }

    /// The pending publication's next retry deadline: only while its
    /// outbox row finished unacknowledged (the row itself is dropped on
    /// completion), counted from its last attempt.
    fn pending_retry_due(
        &self,
        pending: &PendingKeyPackageReplacement,
    ) -> Result<Option<u64>, SessionError> {
        let Some(artifact) = &pending.signed_event else {
            return Ok(None);
        };
        let id = outbound_id_of(&artifact.id)?;
        if self
            .provider
            .session_publication(&id)
            .map_err(SessionError::Storage)?
            .is_some()
        {
            return Ok(None);
        }
        Ok(pending
            .targets
            .iter()
            .filter_map(|target| target.last_attempt_at)
            .map(|at| {
                at.0.saturating_add(publish_retry_secs(pending.attempt_count))
            })
            .min())
    }

    /// (h) The earliest lifecycle deadline as absolute Unix
    /// milliseconds: `not_after` bounds, near-expiry, retained deletion,
    /// the weekly refresh, a pending's publish retry or unsigned work,
    /// a due replacement's immediate wake and the clock-skew retry.
    fn lifecycle_next_due_ms(
        &self,
        lifecycle: &KeyPackageLifecycleState,
    ) -> Result<Option<u64>, SessionError> {
        let now_ms = self.now_ms();
        let now_secs = now_ms / 1_000;
        let mut due: Option<u64> = None;
        // Only deadlines ahead of `now`: the driver already ran, so
        // anything due now has been acted on or cannot be this pass —
        // folding it would arm an immediate wake every tick.
        let fold_secs = |earliest: &mut Option<u64>, secs: Option<u64>| {
            if let Some(ms) = secs.map(|secs| secs.saturating_mul(1_000))
                && ms > now_ms
            {
                *earliest = Some(earliest.map_or(ms, |current| current.min(ms)));
            }
        };
        let fold_ms = |earliest: &mut Option<u64>, ms: u64| {
            if ms > now_ms {
                *earliest = Some(earliest.map_or(ms, |current| current.min(ms)));
            }
        };
        // `not_after` bounds and near-expiry are sweep deadlines.
        fold_secs(&mut due, lifecycle.current_not_after.map(|t| t.0));
        fold_secs(
            &mut due,
            lifecycle
                .current_not_after
                .map(|t| t.0.saturating_sub(KEY_PACKAGE_NEAR_EXPIRY_SECS)),
        );
        fold_secs(
            &mut due,
            lifecycle
                .pending_replacement
                .as_ref()
                .map(|pending| pending.not_after.0),
        );
        for material in &lifecycle.retained_private_material {
            fold_secs(&mut due, Some(material.delete_at().0));
        }
        fold_secs(&mut due, refresh_next_due(lifecycle));
        if let Some(pending) = &lifecycle.pending_replacement
            && pending.signed_event.is_some()
        {
            // A finished row with no acceptance re-offers at its
            // backoff.
            fold_secs(&mut due, self.pending_retry_due(pending)?);
        }
        if self.replacement_due(lifecycle, now_secs) {
            // A `created_at` beyond the skew bound retried when the
            // clock catches up.
            let base = [
                lifecycle.authored_event_created_at,
                lifecycle
                    .pending_replacement
                    .as_ref()
                    .map(|pending| pending.authored_created_at),
                lifecycle.slot_withdrawn_at,
            ]
            .into_iter()
            .flatten()
            .map(|previous| previous.0.saturating_add(1))
            .max()
            .unwrap_or(0);
            if base > now_secs.saturating_add(KEY_PACKAGE_MAX_FUTURE_SKEW_SECS) {
                fold_secs(
                    &mut due,
                    Some(base.saturating_sub(KEY_PACKAGE_MAX_FUTURE_SKEW_SECS)),
                );
            }
        }
        if let Some(retry_ms) = self.key_package_sign_retry_ms {
            fold_ms(&mut due, retry_ms);
        }
        Ok(due)
    }
}

/// (d) `publish_fresh_key_package`'s promotion block (runtime.rs
/// 1918–2006): the confirmed pending becomes current, the previous
/// current is retained for its grace or retired, and the record
/// writes atomically with any caller's report-row update. Returns
/// `true` when a promotion happened (callers push
/// [`SessionEvent::KeyPackageRotated`]).
pub(crate) fn promote_pending(
    engine: &mut cgka_engine::Engine<KvStorageProvider>,
    lifecycle: &mut KeyPackageLifecycleState,
    now_secs: u64,
) -> Result<bool, SessionError> {
    let Some(replacement) = lifecycle.pending_replacement.take() else {
        return Ok(false);
    };
    let confirmed_at = Timestamp(now_secs);
    let previous_was_consumed = lifecycle.current_key_package_ref.is_some()
        && lifecycle.current_key_package_ref == lifecycle.last_consumed_key_package_ref;
    // A retained bundle's unconfirmed grace starts at this
    // confirmation (or kept the one its consumption already started).
    for material in &mut lifecycle.retained_private_material {
        if material.awaiting_confirmed_replacement {
            material.awaiting_confirmed_replacement = false;
            if !material.consumed {
                material.replaced_at = confirmed_at;
            }
        }
    }
    let mut retired = Vec::new();
    if let Some(key_package) = lifecycle.current_key_package.take() {
        match (
            lifecycle.current_key_package_ref.take(),
            lifecycle.current_not_after.take(),
        ) {
            (Some(key_package_ref), Some(not_after)) => {
                lifecycle
                    .retained_private_material
                    .push(RetainedKeyPackagePrivateMaterial {
                        key_package,
                        key_package_ref,
                        not_after,
                        replaced_at: confirmed_at,
                        consumed: previous_was_consumed,
                        authored_event_id: lifecycle.authored_event_id.take(),
                        awaiting_confirmed_replacement: false,
                    });
            }
            // Without a lifetime there is no deletion bound to
            // retain it under.
            _ if previous_was_consumed => retired.push(key_package),
            _ => {}
        }
    }
    lifecycle.current_not_before = None;
    let artifact = replacement
        .signed_event
        .clone()
        .ok_or(SessionError::Internal("unsigned pending promote"))?;
    lifecycle.current_key_package = Some(replacement.key_package.clone());
    lifecycle.current_key_package_ref = Some(replacement.key_package_ref.clone());
    lifecycle.current_not_before = Some(replacement.not_before);
    lifecycle.current_not_after = Some(replacement.not_after);
    lifecycle.authored_event_id = Some(artifact.id.clone());
    lifecycle.authored_event_created_at = Some(artifact.created_at);
    if lifecycle
        .slot_withdrawn_at
        .is_none_or(|withdrawn| withdrawn.0 < artifact.created_at.0)
    {
        lifecycle.slot_withdrawn_at = None;
    }
    lifecycle.authored_signed_event = Some(artifact);
    lifecycle.publication_targets = replacement.targets;
    lifecycle.refresh_at = Some(replacement.refresh_at);
    lifecycle.phase = if lifecycle.publication_targets.iter().all(|target| {
        matches!(
            target.state,
            TransportFanoutAttemptState::Accepted | TransportFanoutAttemptState::PolicyProhibited
        )
    }) {
        MaintenancePhase::Complete
    } else {
        MaintenancePhase::Fanout
    };
    lifecycle.upgrade_rotation_recorded = true;
    lifecycle.generation_revision = replacement.generation_revision;
    // A Welcome can consume the replacement before its
    // acknowledgement: the pending's own marker carries through.
    lifecycle.last_consumed_key_package_ref = lifecycle
        .current_key_package_ref
        .clone()
        .filter(|_| replacement.consumed_at.is_some());
    lifecycle.last_consumed_at = replacement.consumed_at;
    engine
        .promote_key_package_lifecycle(&retired, lifecycle)
        .map_err(session_engine_error)?;
    Ok(true)
}

/// Fold one report into a lifecycle record: the pending's signed event
/// promotes on its first `Accepted` (same transaction as the row update,
/// C12/C13); a row for the current event marks `publication_targets`;
/// a completed pending row with no acceptance records the failed
/// attempt. Returns `true` when the caller should also keep the row
/// bookkeeping it already does.
pub(crate) fn note_report(
    lifecycle: &mut KeyPackageLifecycleState,
    publication: &SessionPublication,
    results: &[RelayResult],
    now_secs: u64,
) -> Result<ReportFold, SessionError> {
    let id = OutboundId::from_hex(&publication.id)
        .map_err(|_| SessionError::Internal("session publication id"))?;
    let is_pending = lifecycle
        .pending_replacement
        .as_ref()
        .and_then(|pending| pending.signed_event.as_ref())
        .and_then(|artifact| outbound_id_of(&artifact.id).ok())
        == Some(id);
    let is_current = !is_pending
        && lifecycle
            .authored_signed_event
            .as_ref()
            .and_then(|artifact| outbound_id_of(&artifact.id).ok())
            == Some(id);
    let accepted = results
        .iter()
        .any(|result| result.status == RelayStatus::Accepted);
    let touch = |targets: &mut Vec<TransportFanoutTarget>| {
        for result in results {
            let Some(target) = targets
                .iter_mut()
                .find(|target| target.endpoint.0 == result.relay)
            else {
                continue;
            };
            if result.status == RelayStatus::Accepted {
                target.state = TransportFanoutAttemptState::Accepted;
            } else {
                target.state = TransportFanoutAttemptState::AttemptedFailed;
                target.last_attempt_at = Some(Timestamp(now_secs));
            }
        }
    };
    if is_pending {
        let pending = lifecycle
            .pending_replacement
            .as_mut()
            .ok_or(SessionError::Internal("pending replacement"))?;
        touch(&mut pending.targets);
        if accepted {
            return Ok(ReportFold::Promote);
        }
        return Ok(ReportFold::Updated);
    }
    if is_current {
        touch(&mut lifecycle.publication_targets);
        return Ok(ReportFold::Updated);
    }
    // A superseded (retained) event's row: the row alone updates.
    Ok(ReportFold::RowOnly)
}

/// What a report fold decided for the caller.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ReportFold {
    /// Promote the pending in the same transaction.
    Promote,
    /// The lifecycle record changed; persist it.
    Updated,
    /// Nothing but the publication row changes.
    RowOnly,
}

/// One signed artifact's write-set growth: new relays join the
/// lifecycle target list, and either join the live publication row or a
/// fresh row is built for just those relays.
fn grow_artifact(
    provider: &KvStorageProvider,
    targets: &mut Vec<TransportFanoutTarget>,
    artifact: &SignedPublicationArtifact,
    new_relays: &[String],
    purpose: SessionPubPurpose,
    rows: &mut Vec<SessionPublication>,
) -> Result<(), SessionError> {
    for relay in new_relays {
        targets.push(TransportFanoutTarget {
            endpoint: TransportEndpoint(relay.clone()),
            state: TransportFanoutAttemptState::Unattempted,
            attempt_count: 0,
            last_attempt_at: None,
            failure_code: None,
        });
    }
    let id = outbound_id_of(&artifact.id)?;
    match provider
        .session_publication(&id)
        .map_err(SessionError::Storage)?
    {
        Some(mut row) => {
            for relay in new_relays {
                row.relays.push(relay.clone());
                row.statuses.push(PubTargetStatus::Attempting);
            }
            rows.push(row);
        }
        None => rows.push(publication_row(artifact, new_relays, purpose)?),
    }
    Ok(())
}

/// A durable outbox row for a signed lifecycle artifact.
fn publication_row(
    artifact: &SignedPublicationArtifact,
    relays: &[String],
    purpose: SessionPubPurpose,
) -> Result<SessionPublication, SessionError> {
    Ok(SessionPublication {
        id: outbound_id_of(&artifact.id)?.to_hex(),
        event_json: String::from_utf8(artifact.bytes.clone())
            .map_err(|_| SessionError::Internal("key package artifact"))?,
        relays: relays.to_vec(),
        statuses: vec![PubTargetStatus::Attempting; relays.len()],
        purpose,
    })
}

/// The slot's 32 bytes from its 64 lowercase hex characters.
fn slot_bytes(lifecycle: Option<&KeyPackageLifecycleState>) -> Result<[u8; 32], SessionError> {
    match lifecycle {
        Some(lc) if !lc.stable_slot_id.is_empty() => hex::decode(&lc.stable_slot_id)
            .map_err(|_| SessionError::Internal("key package slot"))?
            .try_into()
            .map_err(|_| SessionError::Internal("key package slot")),
        _ => Ok([0u8; 32]),
    }
}

/// The promoted publication's info, decoded from the retained signed
/// artifact.
fn current_info(
    lifecycle: Option<&KeyPackageLifecycleState>,
) -> Result<Option<KeyPackageInfo>, SessionError> {
    let Some(lc) = lifecycle else {
        return Ok(None);
    };
    if lc.current_key_package.is_none() {
        return Ok(None);
    }
    let Some(artifact) = &lc.authored_signed_event else {
        return Ok(None);
    };
    let json = std::str::from_utf8(&artifact.bytes)
        .map_err(|_| SessionError::Internal("key package artifact"))?;
    let event = crate::types::Event::from_json(json)
        .map_err(|_| SessionError::Internal("key package artifact"))?;
    key_package::key_package_info(&event)
        .map(Some)
        .map_err(|_| SessionError::Internal("key package artifact"))
}

/// The next refresh deadline: the sampled `refresh_at`, capped a week
/// after publication (marmot-session `key_package_refresh_next_due`).
fn refresh_next_due(lifecycle: &KeyPackageLifecycleState) -> Option<u64> {
    [
        lifecycle.refresh_at.map(|at| at.0),
        lifecycle
            .authored_event_created_at
            .map(|at| at.0.saturating_add(KEY_PACKAGE_REFRESH_MAX_SECS)),
    ]
    .into_iter()
    .flatten()
    .min()
}

fn refresh_due(lifecycle: &KeyPackageLifecycleState, now_secs: u64) -> bool {
    refresh_next_due(lifecycle).is_some_and(|at| at <= now_secs)
}

/// `min(60 s << attempt_count, 1 h)`.
fn publish_retry_secs(attempt_count: u32) -> u64 {
    PUBLISH_RETRY_BASE_SECS
        .saturating_mul(1_u64 << attempt_count.min(6))
        .min(PUBLISH_RETRY_CAP_SECS)
}
