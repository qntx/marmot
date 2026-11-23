//! D2's rejoin surface: consented replacement of an active or stalled
//! copy, automatic install once the copy is removed or unrecoverable,
//! and the admin's two-commit re-add of a stuck member.
//!
//! The engine stores a valid Welcome for a held group id as a rejoin
//! offer. A removed or unrecoverable copy installs it without consent —
//! `retry_rejoins_after_trusted_removal`, driven from `ingest`,
//! `ingest_batch` and `tick` ([`Session::retry_rejoins`]). An
//! active-looking copy needs explicit host consent: the wrap ingest
//! surfaces `SessionEvent::RejoinOffered`, [`Session::rejoin_offers`]
//! exposes the authenticated author and a token binding the exact local
//! branch, and [`Session::confirm_rejoin`] replaces the copy inside the
//! engine's own transaction while [`Session::decline_rejoin`] retires the
//! offer for good.
//!
//! Re-adding an account is two commits because the engine refuses an
//! `Invite` of a device that is still a leaf and offers admins only
//! whole-account removal: `readd_member` stages `RemoveMembers` and
//! writes a durable `Readd` row first, so a crash between the commits
//! still completes the `Invite` exactly once. The account's healthy
//! devices reinstall automatically from their new Welcomes; the stuck
//! one is offered consent.

use cgka_traits::engine::{KeyPackage, SendIntent};
use cgka_traits::engine_state::EpochState;
use cgka_traits::error::EngineError;
use cgka_traits::{CgkaEngine, MemberId, MessageId};
use serde::{Deserialize, Serialize};

use crate::events::SessionEvent;
use crate::keys::{self, Table};
use crate::ser;
use crate::session::{
    Invitee, SendOutcome, Session, SessionError, engine_gid, session_engine_error,
};
use crate::{AccountId, Event, EventId, GroupId};

/// How long a transiently held re-add waits before `tick` drives it
/// again.
const READD_RETRY_MS: u64 = 60_000;

/// A stored replacement Welcome awaiting consent, as
/// [`Session::rejoin_offers`] reports it. `token` binds the exact local
/// branch the consent discards; it goes stale the moment the branch
/// moves.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct RejoinOffer {
    /// The offer's id — the kind-1059 wrap's transport event id.
    pub offer: EventId,
    /// The Welcome's authenticated author.
    pub author: AccountId,
    /// The epoch the offered copy would install at.
    pub epoch: u64,
    /// Consent token for [`Session::confirm_rejoin`].
    pub token: [u8; 32],
}

/// What [`Session::readd_member`] staged for the account.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum ReaddOutcome {
    /// `RemoveMembers` staged; the `Invite` follows once the removal
    /// confirms (`drive_readds` owns the second commit).
    Removed,
    /// The account held no leaf, so the `Invite` staged directly.
    Invited,
}

/// One invitee of a `Readd` row: the validated `KeyPackage` event as its
/// NIP-01 JSON plus the inbox relays the Welcome routes to.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ReaddInvitee {
    /// `Event::to_json` of the invitee's kind-30443 `KeyPackage` event.
    pub(crate) key_package: String,
    /// The invitee's kind-10050 inbox relay set.
    pub(crate) inbox_relays: Vec<String>,
}

/// Durable re-add intent (`Table::Readd`, keyed `group/account`), written
/// before `RemoveMembers` stages: the removal commit id is deliberately
/// absent — the row re-derives whether the Invite is still owed from the
/// roster and epoch state, so a lost or rolled-back removal can never
/// stage a second one.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct Readd {
    /// Every fresh slot of the account to invite back.
    pub(crate) invitees: Vec<ReaddInvitee>,
}

/// What one `Readd` row's pass decided.
enum ReaddPass {
    /// The row has no more work.
    Delete,
    /// Nothing to do; the next ordinary wake re-checks.
    Keep,
    /// A transient refusal held the row — arm the retry wake.
    Hold,
}

impl Session {
    /// Every stored replacement Welcome the group offers, with the
    /// consent token of each. Entries whose ids or token are not 32
    /// bytes are skipped — they can never be confirmed through this
    /// surface.
    ///
    /// # Errors
    /// [`SessionError`] for engine failures.
    pub fn rejoin_offers(&self, group: &GroupId) -> Result<Vec<RejoinOffer>, SessionError> {
        let mut out = Vec::new();
        for candidate in self
            .engine
            .pending_group_rejoins_for(Some(&engine_gid(group)))
            .map_err(session_engine_error)?
        {
            let Some(rejoin) = &candidate.rejoin else {
                continue;
            };
            let (Ok(offer), Ok(author), Ok(token)) = (
                <[u8; 32]>::try_from(candidate.message_id.as_slice()),
                <[u8; 32]>::try_from(rejoin.welcomer.as_slice()),
                <[u8; 32]>::try_from(rejoin.local_state_token.as_slice()),
            ) else {
                continue;
            };
            out.push(RejoinOffer {
                offer: EventId(offer),
                author: AccountId(author),
                epoch: rejoin.epoch.0,
                token,
            });
        }
        Ok(out)
    }

    /// Consent to replace this device's copy with the offered one. The
    /// engine validates the token against the live branch and swaps the
    /// copy in its own transaction: a crash lands on either the old copy
    /// or the new one, never neither (C17). `group` and `offer` must pair
    /// a current offer of [`Session::rejoin_offers`].
    ///
    /// # Errors
    /// [`SessionError::InvalidInput`] (`"offer"`) when `offer` names no
    /// stored offer of the group; [`SessionError::RejoinTokenStale`] when
    /// the branch moved since the token was read; [`SessionError`]
    /// otherwise.
    pub async fn confirm_rejoin(
        &mut self,
        group: &GroupId,
        offer: &EventId,
        token: &[u8; 32],
    ) -> Result<(), SessionError> {
        if !self
            .rejoin_offers(group)?
            .iter()
            .any(|entry| entry.offer == *offer)
        {
            return Err(SessionError::InvalidInput("offer"));
        }
        self.engine
            .confirm_group_rejoin(&MessageId::new(offer.0.to_vec()), token)
            .await
            .map_err(|error| match error {
                EngineError::InvalidTransition(transition)
                    if transition.from == "ActiveMember" && transition.to == "JoinWelcome" =>
                {
                    SessionError::RejoinTokenStale
                }
                other => session_engine_error(other),
            })?;
        self.drive().await?;
        self.note_valid_state_bearing_inputs()?;
        Ok(())
    }

    /// Decline a stored offer: the engine deletes it and redelivery of
    /// the same wrap cannot reopen it.
    ///
    /// # Errors
    /// [`SessionError`] for engine failures.
    pub fn decline_rejoin(&mut self, offer: &EventId) -> Result<(), SessionError> {
        self.engine
            .decline_group_rejoin(&MessageId::new(offer.0.to_vec()))
            .map_err(session_engine_error)
    }

    /// Emit `RejoinOffered` for a kind-1059 ingest that reported
    /// `Local(RejoinConfirmationRequired)`: find the stored offer the
    /// wrap became and name its group once.
    pub(crate) fn note_rejoin_offer(&mut self, wrap: &EventId) -> Result<(), SessionError> {
        for candidate in self
            .engine
            .stored_rejoin_welcomes()
            .map_err(session_engine_error)?
        {
            if candidate.message_id.as_slice() == wrap.0 {
                self.tick_events.push(SessionEvent::RejoinOffered {
                    group: GroupId(candidate.group_id.as_slice().to_vec()),
                });
                break;
            }
        }
        Ok(())
    }

    /// Install every held offer whose copy is removed or unrecoverable —
    /// the consent-free path of D2, run after each ingest's `drive` (a
    /// removal applies through convergence first) and in `tick`. Offers
    /// on still-active copies wait for `confirm_rejoin` and are never
    /// installed here. `true` when an offer installed.
    pub(crate) async fn retry_rejoins(&mut self) -> Result<bool, SessionError> {
        if !self
            .engine
            .has_rejoin_offers()
            .map_err(session_engine_error)?
        {
            return Ok(false);
        }
        self.engine
            .retry_rejoins_after_trusted_removal()
            .await
            .map_err(session_engine_error)
    }

    /// Re-announce every stored offer once after a restart: the first
    /// drain of a reopened session carries `RejoinOffered` for each
    /// group holding one.
    pub(crate) fn reissue_rejoin_offers(&mut self) -> Result<(), SessionError> {
        let mut announced = std::collections::HashSet::new();
        for candidate in self
            .engine
            .stored_rejoin_welcomes()
            .map_err(session_engine_error)?
        {
            let group = GroupId(candidate.group_id.as_slice().to_vec());
            if announced.insert(group.clone()) {
                self.tick_events.push(SessionEvent::RejoinOffered { group });
            }
        }
        Ok(())
    }

    /// Re-add an account whose device may hold a stuck or stale copy: the
    /// account is removed whole, then every supplied fresh slot is
    /// invited once the removal confirms. A whole-account removal is the
    /// only shape the spec and the engine offer — the account's healthy
    /// devices lose their leaf briefly and reinstall automatically.
    ///
    /// # Errors
    /// [`SessionError::NotGroupAdmin`] when this account is not a current
    /// admin; [`SessionError::InvalidInput`] (`"account"`) for our own
    /// account, (`"key package"`) for an empty invitee list or a
    /// `KeyPackage` signed by another account; [`SessionError`]
    /// otherwise.
    pub async fn readd_member(
        &mut self,
        group: &GroupId,
        account: AccountId,
        invitees: Vec<Invitee>,
    ) -> Result<ReaddOutcome, SessionError> {
        self.require_admin(group)?;
        if account == self.account() {
            return Err(SessionError::InvalidInput("account"));
        }
        if invitees.is_empty()
            || invitees
                .iter()
                .any(|invitee| invitee.key_package.pubkey != account)
        {
            return Err(SessionError::InvalidInput("key package"));
        }
        // Validation precedes every durable write: inbox relays persist
        // here, and an invalid slot stages nothing.
        let key_packages = invitees
            .iter()
            .map(|invitee| self.invitee_key_package(invitee))
            .collect::<Result<Vec<_>, _>>()?;
        let gid = engine_gid(group);
        let has_leaf = self
            .engine
            .members(&gid)
            .map_err(session_engine_error)?
            .iter()
            .any(|member| member.id.as_slice() == account.0);
        if !has_leaf {
            self.stage_invite(group, key_packages).await?;
            return Ok(ReaddOutcome::Invited);
        }
        // The row lands before the removal stage: a crash between the
        // two commits re-derives the owed Invite from the roster.
        let row = Readd {
            invitees: invitees
                .iter()
                .map(|invitee| ReaddInvitee {
                    key_package: invitee.key_package.to_json(),
                    inbox_relays: invitee.inbox_relays.clone(),
                })
                .collect(),
        };
        self.provider
            .store()
            .transact(|| {
                self.provider
                    .put_row(Table::Readd, &[&group.0, &account.0], ser::ser(&row)?)
            })
            .map_err(SessionError::Storage)?;
        let staged = self
            .engine
            .send(SendIntent::RemoveMembers {
                group_id: gid,
                members: vec![MemberId::new(account.0.to_vec())],
            })
            .await;
        match staged {
            Ok(result) => {
                self.finish_send(result)?;
                Ok(ReaddOutcome::Removed)
            }
            Err(error) => {
                self.provider
                    .del_row(Table::Readd, &[&group.0, &account.0])
                    .map_err(SessionError::Storage)?;
                Err(session_engine_error(error))
            }
        }
    }

    /// Stage an `Invite` of already-validated `key_packages` and finish
    /// the send like [`Session::invite`].
    async fn stage_invite(
        &mut self,
        group: &GroupId,
        key_packages: Vec<KeyPackage>,
    ) -> Result<SendOutcome, SessionError> {
        let result = self
            .engine
            .send(SendIntent::Invite {
                group_id: engine_gid(group),
                key_packages,
                initial_admins: Vec::new(),
            })
            .await
            .map_err(session_engine_error)?;
        self.finish_send(result)
    }

    /// Drive every `Readd` row one step from `tick` (D2 + C17): the
    /// Invite follows the removal's confirmation exactly once. Rows for
    /// gone or unadministered groups retire; an account whose removal
    /// lost or rolled back frees the host to call `readd_member` again.
    /// `Some(READD_RETRY_MS)` is folded into the wake whenever a
    /// transient refusal held a row.
    pub(crate) async fn drive_readds(&mut self) -> Result<Option<u64>, SessionError> {
        let mut hold = false;
        for (row_key, value) in self
            .provider
            .scan_rows(Table::Readd, &[])
            .map_err(SessionError::Storage)?
        {
            let parsed = keys::segments(&row_key).and_then(|segments| {
                let [gid, account]: [&[u8]; 2] = segments.try_into().ok()?;
                Some((GroupId(gid.to_vec()), AccountId(account.try_into().ok()?)))
            });
            let row: Result<Readd, _> = ser::de(&value);
            let (Some((group, account)), Ok(row)) = (parsed, row) else {
                self.provider
                    .store()
                    .transact(|| self.provider.store().delete(&row_key))
                    .map_err(SessionError::Storage)?;
                continue;
            };
            match self.drive_readd(&group, &account, &row).await? {
                ReaddPass::Delete => {
                    self.provider
                        .store()
                        .transact(|| self.provider.store().delete(&row_key))
                        .map_err(SessionError::Storage)?;
                }
                ReaddPass::Keep => {}
                ReaddPass::Hold => hold = true,
            }
        }
        Ok(hold.then_some(READD_RETRY_MS))
    }

    /// One row's step: read the live facts, then stage, keep or retire.
    /// Engine and roster reads that fail transiently hold the row for
    /// the retry wake rather than aborting the whole pass.
    async fn drive_readd(
        &mut self,
        group: &GroupId,
        account: &AccountId,
        row: &Readd,
    ) -> Result<ReaddPass, SessionError> {
        let gid = engine_gid(group);
        let record = match self.engine.group_record(&gid) {
            Ok(record) => record,
            Err(EngineError::UnknownGroup(_)) => return Ok(ReaddPass::Delete),
            Err(error) => return Err(session_engine_error(error)),
        };
        if record.removed || record.disbanded.is_some() {
            return Ok(ReaddPass::Delete);
        }
        match self.admin_set(&gid) {
            Ok(admins) if admins.contains(&self.account().0) => {}
            Ok(_) => return Ok(ReaddPass::Delete),
            Err(_) => return Ok(ReaddPass::Hold),
        }
        let Ok(members) = self.engine.members(&gid) else {
            return Ok(ReaddPass::Hold);
        };
        let has_leaf = members
            .iter()
            .any(|member| member.id.as_slice() == account.0);
        let stable = matches!(
            self.engine.epoch_state(&gid),
            Some(EpochState::Stable { .. })
        );
        if !has_leaf {
            if !stable {
                // The removal is mid-confirm; its own convergence wake
                // re-drives this pass.
                return Ok(ReaddPass::Keep);
            }
            return Ok(match self.readd_stage_invite(group, row).await {
                Ok(()) => ReaddPass::Delete,
                Err(error) if readd_terminal(&error) => ReaddPass::Delete,
                Err(_) => ReaddPass::Hold,
            });
        }
        // The account still holds its leaf: the removal either settled
        // it or never landed. An own commit in flight or a queued intent
        // means "wait"; anything else means the removal rolled back and
        // the row's work is moot — the host may call `readd_member`
        // again.
        let in_flight = !stable
            || self
                .engine
                .has_queued_outbound_intents(&gid)
                .map_err(session_engine_error)?;
        if in_flight {
            return Ok(ReaddPass::Keep);
        }
        Ok(ReaddPass::Delete)
    }

    /// Stage the `Invite` a `Readd` row carries. A stored `KeyPackage`
    /// event that no longer decodes can never succeed — surfaced as
    /// `InvalidInput` so the caller retires the row.
    async fn readd_stage_invite(
        &mut self,
        group: &GroupId,
        row: &Readd,
    ) -> Result<(), SessionError> {
        let mut key_packages = Vec::with_capacity(row.invitees.len());
        for invitee in &row.invitees {
            let event = Event::from_json(&invitee.key_package)
                .map_err(|_| SessionError::InvalidInput("key package"))?;
            key_packages.push(self.invitee_key_package(&Invitee {
                key_package: event,
                inbox_relays: invitee.inbox_relays.clone(),
            })?);
        }
        self.stage_invite(group, key_packages).await?;
        Ok(())
    }
}

/// Whether a staged `Invite` failure ends the re-add: input and verdict
/// errors are terminal, `GroupBusy` and storage transients retry on the
/// next wake.
const fn readd_terminal(error: &SessionError) -> bool {
    !matches!(
        error,
        SessionError::GroupBusy | SessionError::Storage(_) | SessionError::Signer(_)
    )
}
