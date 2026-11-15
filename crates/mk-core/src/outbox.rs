//! Session-owned outbound state: account publications (kinds 30443/10050,
//! which are not engine transport messages), the recipient→inbox-relay map
//! used to route Welcomes, and the public outbox surface
//! ([`Outbound`]/[`RelayResult`]/[`PublishResolution`]).
//!
//! Engine transport artifacts are frozen as MDK [`OutboundFanout`] rows by
//! the engine itself; only account publications live under
//! [`Table::SessionOutbox`], under the same durability gate.

use serde::{Deserialize, Serialize};

use crate::keys::{self, Table};
use crate::provider::KvStorageProvider;
use crate::types::{AccountId, Event, OutboundId};
use cgka_traits::storage::StorageError;

/// One outbound obligation handed to the host transport.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Outbound {
    /// Identity of the obligation (the frozen event's id).
    pub id: OutboundId,
    /// The exact signed event to publish.
    pub event: Event,
    /// Relay URLs to publish to, in target order.
    pub relays: Vec<String>,
    /// Why this artifact exists.
    pub purpose: OutboundPurpose,
}

/// What kind of artifact an [`Outbound`] carries.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum OutboundPurpose {
    /// An encrypted group application message (kind 445).
    ApplicationMessage,
    /// An MLS proposal broadcast (kind 445).
    Proposal,
    /// An MLS commit / group evolution (kind 445).
    Commit,
    /// A NIP-59 gift-wrapped Welcome (kind 1059).
    Welcome,
    /// The account's `KeyPackage` (kind 30443).
    KeyPackage,
    /// The account's inbox relay list (kind 10050).
    InboxRelays,
}

/// One relay's publish verdict from the host transport.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RelayResult {
    /// The relay URL exactly as [`Outbound::relays`] listed it.
    pub relay: String,
    /// What the relay did with the event.
    pub status: RelayStatus,
}

/// Per-relay publish status the host reports.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum RelayStatus {
    /// The relay accepted the event (OK true, or a NIP-01 `duplicate`
    /// acknowledgement — both count as exposure).
    Accepted,
    /// The relay definitively refused the event (OK false with a typed
    /// rejection). Maps to a terminal no-exposure failure on the fanout.
    Rejected,
    /// The relay could not be reached or did not answer (timeout, offline).
    /// The target stays retryable; the event is not assumed exposed.
    Unavailable,
    /// The outcome is ambiguous (connection died mid-write, unknown response
    /// shape). The event may have been exposed: the fanout records possible
    /// exposure so a pending MLS commit is never rolled back.
    Unknown,
}

/// Terminal or in-progress verdict of a [`crate::Session::report_publish`]
/// call.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum PublishResolution {
    /// Targets remain outstanding; the item is handed out again by a later
    /// `take_outbound`.
    Pending,
    /// A pending MLS commit was confirmed on first acceptance.
    Confirmed,
    /// A pending MLS commit was rolled back after every target failed with
    /// no possible exposure.
    RolledBack,
    /// All targets are terminal with no pending MLS work; the record is
    /// released.
    Complete,
}

/// Per-relay status of a session-owned publication.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum PubTargetStatus {
    NotAttempted,
    Attempting,
    Accepted,
    Failed,
    PossiblyExposed,
    RetryableUnavailable,
}

impl PubTargetStatus {
    pub(crate) const fn is_outstanding(self) -> bool {
        matches!(
            self,
            Self::NotAttempted
                | Self::Attempting
                | Self::PossiblyExposed
                | Self::RetryableUnavailable
        )
    }
}

/// A durable account publication: the frozen signed event plus per-relay
/// state. Serialized as JSON into `Table::SessionOutbox`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct SessionPublication {
    /// Event id (hex) — also the row key and the `OutboundId`.
    pub id: String,
    /// Full signed event JSON (frozen bytes the host must publish).
    pub event_json: String,
    /// Relay URLs, positionally aligned with `statuses`.
    pub relays: Vec<String>,
    pub statuses: Vec<PubTargetStatus>,
    /// Why the publication exists; drives `OutboundPurpose`.
    pub purpose: SessionPubPurpose,
}

/// Which account publication a [`SessionPublication`] is.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum SessionPubPurpose {
    KeyPackage,
    InboxRelays,
}

const PUB_DECODE: &str = "kv decode: session publication";
const PUB_ENCODE: &str = "kv encode: session publication";
const INBOX_DECODE: &str = "kv decode: session inbox";
const INBOX_ENCODE: &str = "kv encode: session inbox";
const META_DECODE: &str = "kv decode: session meta";

fn storage_err(message: &'static str) -> StorageError {
    StorageError::Backend(message.to_owned())
}

impl KvStorageProvider {
    /// Persist a session publication (insert or replace), inside the caller's
    /// storage unit when one is open.
    pub(crate) fn put_session_publication(
        &self,
        publication: &SessionPublication,
    ) -> Result<(), StorageError> {
        let value = serde_json::to_vec(publication).map_err(|_| storage_err(PUB_ENCODE))?;
        let id = hex::decode(&publication.id).map_err(|_| storage_err(PUB_DECODE))?;
        self.store()
            .put(keys::key(Table::SessionOutbox, &[&id]), value)
    }

    /// All session publications.
    pub(crate) fn session_publications(&self) -> Result<Vec<SessionPublication>, StorageError> {
        let mut out = Vec::new();
        for (_key, value) in self.store().scan(&keys::key(Table::SessionOutbox, &[]))? {
            let publication: SessionPublication =
                serde_json::from_slice(&value).map_err(|_| storage_err(PUB_DECODE))?;
            out.push(publication);
        }
        Ok(out)
    }

    /// One session publication by event id.
    pub(crate) fn session_publication(
        &self,
        id: &OutboundId,
    ) -> Result<Option<SessionPublication>, StorageError> {
        self.store()
            .get(&keys::key(Table::SessionOutbox, &[&id.0]))?
            .map_or(Ok(None), |value| {
                serde_json::from_slice(&value)
                    .map(Some)
                    .map_err(|_| storage_err(PUB_DECODE))
            })
    }

    /// Delete a session publication after terminal resolution.
    pub(crate) fn delete_session_publication(&self, id: &OutboundId) -> Result<(), StorageError> {
        self.store()
            .delete(&keys::key(Table::SessionOutbox, &[&id.0]))
    }

    /// Record a recipient's inbox relays (invite/create time) so later
    /// Welcomes can be routed.
    pub(crate) fn put_inbox_relays(
        &self,
        account: &AccountId,
        relays: &[String],
    ) -> Result<(), StorageError> {
        let value = serde_json::to_vec(relays).map_err(|_| storage_err(INBOX_ENCODE))?;
        self.store()
            .put(keys::key(Table::SessionInbox, &[&account.0]), value)
    }

    /// The inbox relays recorded for a recipient, if any.
    pub(crate) fn inbox_relays(
        &self,
        account: &AccountId,
    ) -> Result<Option<Vec<String>>, StorageError> {
        self.store()
            .get(&keys::key(Table::SessionInbox, &[&account.0]))?
            .map_or(Ok(None), |value| {
                serde_json::from_slice(&value)
                    .map(Some)
                    .map_err(|_| storage_err(INBOX_DECODE))
            })
    }

    /// Persist the durable link between a regenerated outbound artifact's
    /// message id and the queued intent it carries, inside the caller's
    /// storage unit when one is open. Written alongside the fanout row at
    /// stage time so a restart can still resolve the intent at report time.
    pub(crate) fn put_queued_intent_link(
        &self,
        message_id: &cgka_traits::MessageId,
        group_id: &cgka_traits::GroupId,
        intent_id: &cgka_traits::MessageId,
    ) -> Result<(), StorageError> {
        let mut value = Vec::new();
        crate::ser::put_field(&mut value, group_id.as_slice());
        crate::ser::put_field(&mut value, intent_id.as_slice());
        self.store().put(
            keys::key(Table::QueuedIntentLink, &[message_id.as_slice()]),
            value,
        )
    }

    /// The queued intent a regenerated artifact carries, if recorded.
    pub(crate) fn queued_intent_link(
        &self,
        message_id: &cgka_traits::MessageId,
    ) -> Result<Option<(cgka_traits::GroupId, cgka_traits::MessageId)>, StorageError> {
        let Some(value) = self.store().get(&keys::key(
            Table::QueuedIntentLink,
            &[message_id.as_slice()],
        ))?
        else {
            return Ok(None);
        };
        let mut tuple = crate::ser::Tuple::new(&value);
        let group_id = tuple.field()?;
        let intent_id = tuple.field()?;
        tuple.finish()?;
        Ok(Some((
            cgka_traits::GroupId::new(group_id.to_vec()),
            cgka_traits::MessageId::new(intent_id.to_vec()),
        )))
    }

    /// Drop a queued-intent link once the artifact's publish resolved it.
    pub(crate) fn delete_queued_intent_link(
        &self,
        message_id: &cgka_traits::MessageId,
    ) -> Result<(), StorageError> {
        self.store().delete(&keys::key(
            Table::QueuedIntentLink,
            &[message_id.as_slice()],
        ))
    }

    /// Read a session-owned meta row (raw bytes).
    pub(crate) fn session_meta(&self, name: &[u8]) -> Result<Option<Vec<u8>>, StorageError> {
        self.store().get(&keys::key(Table::Meta, &[name]))
    }

    /// Write a session-owned meta row (raw bytes).
    pub(crate) fn put_session_meta(&self, name: &[u8], value: &[u8]) -> Result<(), StorageError> {
        self.store()
            .put(keys::key(Table::Meta, &[name]), value.to_vec())
    }
}

/// Session meta row holding the stable random `d` slot id for kind-30443
/// `KeyPackage` publications.
pub(crate) const META_KEY_PACKAGE_SLOT: &[u8] = b"key-package-slot";

impl KvStorageProvider {
    /// The `KeyPackage` `d` slot id, generating and persisting one on first use.
    pub(crate) fn key_package_slot(&self) -> Result<[u8; 32], StorageError> {
        if let Some(value) = self.session_meta(META_KEY_PACKAGE_SLOT)? {
            let bytes: [u8; 32] = value
                .as_slice()
                .try_into()
                .map_err(|_| storage_err(META_DECODE))?;
            return Ok(bytes);
        }
        let mut slot = [0u8; 32];
        getrandom::fill(&mut slot).map_err(|_| storage_err("session rng"))?;
        self.put_session_meta(META_KEY_PACKAGE_SLOT, &slot)?;
        Ok(slot)
    }
}
