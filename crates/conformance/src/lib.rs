//! Replay the recorded MDK `cgka-conformance-simulator` vectors through the
//! public `Session` API.
//!
//! `SessionSubject` implements `ConvergenceSubject` on top of `Session`: every
//! mutation, delivery, restart and publication goes through the same public
//! calls a host app would make (`create_group`, `invite`, `ingest`,
//! `take_outbound`, `report_publish`, `tick`, `Session::open` on durable
//! rows). Nothing here reaches into engine or storage internals; capabilities
//! the public surface cannot satisfy are left undeclared so preflight refuses
//! those vectors before they run.

#![allow(
    clippy::missing_docs_in_private_items,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::missing_panics_doc,
    clippy::needless_pass_by_value,
    clippy::doc_markdown,
    clippy::significant_drop_tightening,
    clippy::unreadable_literal,
    clippy::missing_errors_doc,
    clippy::excessive_nesting,
    clippy::stable_sort_primitive,
    clippy::shadow_unrelated,
    clippy::let_unit_value,
    clippy::ignored_unit_patterns,
    clippy::format_push_string,
    unreachable_pub,
    reason = "test harness code"
)]

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use async_trait::async_trait;
#[cfg(test)]
use cgka_conformance_simulator::SubjectCapability;
use cgka_conformance_simulator::{
    AppInvalidationObservation, ClientEventCounts, ClientObservation, ConvergenceFaultSubject,
    ConvergenceSubject, EpochChangeObservation, ScenarioAdminPolicyObservation,
    ScenarioMessageSelectorV2, ScenarioPredicateObservationV2, ScenarioPredicateV2,
    ScenarioTransportClass, SubjectCreateGroup, SubjectDescriptor, SubjectError,
    SubjectInviteMembers, SubjectOutboundArtifact, SubjectOutboundKind, SubjectOutboundOutcome,
    SubjectRemoveMembers, SubjectSelfUpdate, SubjectSendApplication, SubjectUpdateAdminPolicy,
    SubjectUpdateGroupData,
};
use cgka_traits::transport::{Timestamp, TransportEnvelope, TransportMessage, TransportSource};
use cgka_traits::types::{MemberId, MessageId};
#[cfg(test)]
use mk_core::AccountSigner;
use mk_core::testing::{LocalSigner, ManualClock, RecordingHost};
use mk_core::{
    AccountId, AppMessage, Clock, ComponentUpdate, CreateGroup, DestinationPolicy, Event, EventId,
    GroupChange, GroupId, Invitee, Opened, OutboundPurpose, RelayResult, RelayStatus, Session,
    SessionConfig, SessionEvent,
};

/// One relay URL every group publishes to; delivery is in-process.
const RELAY: &str = "wss://conformance.mk-core.test";
/// Monotonic base for every injected clock, mirroring the engine harness's
/// `HARNESS_CONVERGENCE_SETTLED_AT_MS`: convergence cutoffs armed relative to
/// session setup are already due at every tick, so one `tick` settles a pass
/// exactly as the recorded vectors expect.
#[cfg(test)]
const SETTLED_MONOTONIC_MS: u64 = 1_000_000;
/// Bounded settle passes per `tick`, mirroring
/// `HARNESS_CONVERGENCE_DRAIN_PASSES` — a convergence pass can arm the next
/// cutoff, so one drive may not finish the cascade.
const SETTLE_PASSES: usize = 8;
/// `marmot.group.admin-policy.v1` app-component id (`UpdateAppComponents`).
const ADMIN_POLICY_COMPONENT_ID: u16 = 0x8003;

fn subject_error(code: &str, message: impl Into<String>) -> SubjectError {
    SubjectError::new(code, message)
}

/// Map a `SessionError` to the error-code vocabulary the vectors assert
/// (`expect_update_admin_policy_error` compares `SubjectError.code`).
fn other_error(err: impl std::fmt::Debug) -> SubjectError {
    subject_error("session_error", format!("{err:?}"))
}

fn session_error(err: mk_core::SessionError) -> SubjectError {
    use mk_core::SessionError;
    let code = match &err {
        SessionError::NotGroupAdmin => "not_group_admin",
        SessionError::AdminDepletion => "admin_policy",
        SessionError::NotAMember => "not_a_member",
        SessionError::UnknownGroup => "unknown_group",
        SessionError::LeaveAlreadyRequested => "leave_already_requested",
        SessionError::MissingRequiredCapabilities => "missing_required_capabilities",
        SessionError::UnsupportedCiphersuite { .. } => "unsupported_ciphersuite",
        SessionError::Engine { kind } => kind,
        SessionError::GroupBusy => "group_unsettled",
        SessionError::InvalidInput(_) => "invalid_input",
        SessionError::UnknownOutbound => "unknown_outbound",
        _ => "session_error",
    };
    SubjectError::new(code, err.to_string())
}

fn tag<'a>(event: &'a Event, name: &str) -> Option<&'a str> {
    event
        .tags
        .iter()
        .find(|tag| tag.first().map(String::as_str) == Some(name))
        .and_then(|tag| tag.get(1))
        .map(String::as_str)
}

/// One client incarnation: the session plus the injected boundaries needed to
/// reopen it (`restart`) and to know which commits reached durable storage.
#[derive(Debug)]
struct SubjectClient {
    signer: Arc<LocalSigner>,
    clock: Arc<ManualClock>,
    host: Arc<RecordingHost>,
    session: Session,
    key_package: Option<Event>,
    /// KeyPackage lifecycle outbound seen by `sync_outbound` but not yet
    /// reported to the session — reporting is async, so the items wait for
    /// the next async sync to acknowledge every relay at once.
    unreported_key_packages: Vec<(mk_core::OutboundId, Vec<String>)>,
    mailbox: Vec<Event>,
    /// Session events already drained (e.g. to collect ack ids in `tick`)
    /// but still owed to `observe`.
    pending_events: Vec<SessionEvent>,
}

/// A transport object on the queue: the signed event plus the outbound
/// record it came from.
#[derive(Clone, Debug)]
struct InFlight {
    sender: String,
    outbound_id: String,
    event: Event,
    purpose: OutboundPurpose,
}

/// Bookkeeping for one emitted outbound artifact.
#[derive(Debug)]
struct OutboundRecord {
    artifact: SubjectOutboundArtifact,
    relays: Vec<String>,
    outbound_id: mk_core::OutboundId,
    resolution: Option<SubjectOutboundOutcome>,
    /// Recipients this artifact's bytes already reached (mailbox exposures).
    delivered: usize,
    /// Stable pending label when this artifact came from an action-staged
    /// pending publication; used by the no-endpoint exposure checks.
    publication: Option<String>,
}

/// The in-memory bus state the scenario drives through `deliver_all`,
/// `tick`, and the semantic transport faults.
#[derive(Debug)]
pub struct SessionSubject {
    descriptor: SubjectDescriptor,
    clients: BTreeMap<String, SubjectClient>,
    client_order: Vec<String>,
    queue: Vec<InFlight>,
    withheld: BTreeMap<String, Vec<InFlight>>,
    records: BTreeMap<String, OutboundRecord>,
    partition: Option<BTreeSet<String>>,
    scenario_groups: BTreeMap<String, GroupId>,
    active_group: Option<String>,
    label_by_account: BTreeMap<String, String>,
    account_hex_by_label: BTreeMap<String, String>,
}

/// The durability gate: release staged outbound only for batches the
/// `RecordingHost` reports durable, exactly as the host app must.
fn ack_gate(client: &mut SubjectClient) {
    if let Some(seq) = client.session.awaiting_durability() {
        client.session.acknowledge_durable(seq);
    }
}

fn session_config(
    signer: Arc<LocalSigner>,
    clock: Arc<ManualClock>,
    host: Arc<RecordingHost>,
    rows: Vec<(Vec<u8>, Vec<u8>)>,
) -> SessionConfig {
    SessionConfig {
        signer,
        host,
        rows,
        clock,
        destination_policy: DestinationPolicy {
            allow_loopback: true,
        },
        client_name: None,
    }
}

/// Selector matching over the queue: sender, publication label, and
/// transport class. `action_id` is treated as the pending publication label
/// the action introduced (the adapter does not expose a separate input id).
fn flight_matches(
    records: &BTreeMap<String, OutboundRecord>,
    flight: &InFlight,
    selector: &ScenarioMessageSelectorV2,
) -> bool {
    if let Some(sender) = &selector.sender
        && sender != &flight.sender
    {
        return false;
    }
    if let Some(publication) = &selector.publication
        && records
            .get(&flight.outbound_id)
            .and_then(|record| record.publication.as_ref())
            != Some(publication)
    {
        return false;
    }
    if let Some(action_id) = &selector.action_id
        && records
            .get(&flight.outbound_id)
            .and_then(|record| record.publication.as_ref())
            != Some(action_id)
    {
        return false;
    }
    if let Some(class) = selector.class {
        let matched = match class {
            ScenarioTransportClass::Welcome => flight.purpose == OutboundPurpose::Welcome,
            ScenarioTransportClass::Commit => flight.purpose == OutboundPurpose::Commit,
            ScenarioTransportClass::Proposal => flight.purpose == OutboundPurpose::Proposal,
            ScenarioTransportClass::Application => {
                flight.purpose == OutboundPurpose::ApplicationMessage
            }
            ScenarioTransportClass::GroupMessage => flight.purpose != OutboundPurpose::Welcome,
        };
        if !matched {
            return false;
        }
    }
    true
}

impl SessionSubject {
    /// Open one session per client label and publish each client's
    /// `KeyPackage` + inbox-relay list so group/invite actions can address
    /// them, exactly as a host directory would.
    #[cfg(test)]
    #[allow(
        clippy::too_many_lines,
        reason = "the fixture opens and seeds every client in one narrative block"
    )]
    async fn new(labels: &[String]) -> Result<Self, SubjectError> {
        let mut subject = Self {
            descriptor: SubjectDescriptor {
                adapter: "mk-core-session".into(),
                adapter_version: "1".into(),
                storage_backend: "kv".into(),
                capabilities: [
                    SubjectCapability::GroupMutation,
                    SubjectCapability::ApplicationMessaging,
                    SubjectCapability::TransportDelivery,
                    SubjectCapability::EventObservation,
                    SubjectCapability::OutboundPublication,
                    SubjectCapability::CrashReopen,
                    SubjectCapability::AdminPolicyObservation,
                    SubjectCapability::ClientStateAssertion,
                    SubjectCapability::WhiteBoxTransportPartition,
                    SubjectCapability::SemanticTransportFaults,
                ]
                .into_iter()
                .collect(),
            },
            clients: BTreeMap::new(),
            client_order: labels.to_vec(),
            queue: Vec::new(),
            withheld: BTreeMap::new(),
            records: BTreeMap::new(),
            partition: None,
            scenario_groups: BTreeMap::new(),
            active_group: None,
            label_by_account: BTreeMap::new(),
            account_hex_by_label: BTreeMap::new(),
        };
        for label in labels {
            let signer = Arc::new(LocalSigner::new());
            let clock = Arc::new(ManualClock::new());
            clock.set(SETTLED_MONOTONIC_MS, clock.now().unix_ms);
            let host = Arc::new(RecordingHost::new());
            let opened = Session::open(session_config(
                Arc::clone(&signer),
                Arc::clone(&clock),
                Arc::clone(&host),
                Vec::new(),
            ))
            .map_err(other_error)?;
            let session = match opened {
                Opened::Ready(session) => *session,
                Opened::NeedsAccountProof(pending) => {
                    let signed = signer
                        .sign_event(pending.request().clone())
                        .await
                        .map_err(other_error)?;
                    pending
                        .provide_account_proof(&signed)
                        .map_err(|err| subject_error("account_proof", format!("{err:?}")))?
                }
            };
            subject
                .account_hex_by_label
                .insert(label.clone(), signer.account().to_hex());
            subject
                .label_by_account
                .insert(signer.account().to_hex(), label.clone());
            subject.clients.insert(
                label.clone(),
                SubjectClient {
                    signer,
                    clock,
                    host,
                    session,
                    key_package: None,
                    unreported_key_packages: Vec::new(),
                    mailbox: Vec::new(),
                    pending_events: Vec::new(),
                },
            );
        }
        for label in labels {
            {
                let client = subject.client_mut(label)?;
                client
                    .session
                    .ensure_key_package(&[RELAY.to_owned()])
                    .await
                    .map_err(session_error)?;
                client
                    .session
                    .publish_inbox_relays(&[RELAY.to_owned()])
                    .await
                    .map_err(session_error)?;
                ack_gate(client);
                let items = client.session.take_outbound().map_err(session_error)?;
                for item in items {
                    if item.purpose == OutboundPurpose::KeyPackage {
                        client.key_package = Some(item.event.clone());
                    }
                    let results: Vec<RelayResult> = item
                        .relays
                        .iter()
                        .map(|relay| RelayResult {
                            relay: relay.clone(),
                            status: RelayStatus::Accepted,
                        })
                        .collect();
                    client
                        .session
                        .report_publish(&item.id, &results)
                        .await
                        .map_err(session_error)?;
                    ack_gate(client);
                }
                client.session.drain_events().map_err(session_error)?;
            }
            if subject.client(label)?.key_package.is_none() {
                return Err(subject_error(
                    "no_key_package",
                    format!("{label} did not publish a KeyPackage"),
                ));
            }
        }
        Ok(subject)
    }

    /// Pull every newly durable outbound out of the session, record it as an
    /// artifact, and put its event on the delivery queue. `publication` tags
    /// artifacts an action-staged pending produced; engine-generated work
    /// (regenerated intents, convergence commits) passes `None`.
    fn sync_outbound(
        &mut self,
        label: &str,
        publication: Option<&str>,
    ) -> Result<(), SubjectError> {
        let client = self.client_mut(label)?;
        ack_gate(client);
        let items = client.session.take_outbound().map_err(session_error)?;
        for item in items {
            let outbound_id = item.id.to_hex();
            if self.records.contains_key(&outbound_id) {
                continue;
            }
            if matches!(
                item.purpose,
                OutboundPurpose::KeyPackage
                    | OutboundPurpose::KeyPackageRepublish
                    | OutboundPurpose::KeyPackageDeletion
            ) {
                // KeyPackage lifecycle traffic publishes to the account's
                // write relays, not into mailboxes — mirror the setup path:
                // refresh the directory copy, park the relays for the next
                // async sync to report and keep the flight out of the queue
                // so scenario ordering steps never observe it.
                let client = self.client_mut(label)?;
                if item.purpose == OutboundPurpose::KeyPackageDeletion {
                    client.key_package = None;
                } else {
                    client.key_package = Some(item.event.clone());
                }
                if !client
                    .unreported_key_packages
                    .iter()
                    .any(|(id, _)| *id == item.id)
                {
                    client
                        .unreported_key_packages
                        .push((item.id, item.relays.clone()));
                }
                continue;
            }
            let kind = if item.purpose == OutboundPurpose::Welcome {
                SubjectOutboundKind::Welcome
            } else {
                SubjectOutboundKind::GroupMessage
            };
            // Only a Commit carries the pending's confirmation bytes; a
            // Welcome in the same exposure set is non-confirmation (mirrors
            // `message_confirms_pending` in the engine subject).
            let artifact = SubjectOutboundArtifact {
                outbound_id: outbound_id.clone(),
                client: label.to_owned(),
                publication: publication.map(str::to_owned),
                kind,
                message: transport_message(&item.event),
                state_confirmation_required: publication.is_some()
                    && item.purpose == OutboundPurpose::Commit,
                regenerated_queued_intent: false,
            };
            self.records.insert(
                outbound_id.clone(),
                OutboundRecord {
                    artifact,
                    relays: item.relays.clone(),
                    outbound_id: item.id,
                    resolution: None,
                    delivered: 0,
                    publication: publication.map(str::to_owned),
                },
            );
            self.queue.push(InFlight {
                sender: label.to_owned(),
                outbound_id,
                event: item.event,
                purpose: item.purpose,
            });
        }
        ack_gate(self.client_mut(label)?);
        Ok(())
    }

    /// `sync_outbound` plus the asynchronous half of KeyPackage lifecycle
    /// publishing: every relay acknowledges the staged items at once, the
    /// same instant-directory semantics as `ensure_key_package` at setup.
    async fn sync_outbound_reported(
        &mut self,
        label: &str,
        publication: Option<&str>,
    ) -> Result<(), SubjectError> {
        self.sync_outbound(label, publication)?;
        let client = self.client_mut(label)?;
        let pending = std::mem::take(&mut client.unreported_key_packages);
        for (id, relays) in pending {
            let results: Vec<RelayResult> = relays
                .iter()
                .map(|relay| RelayResult {
                    relay: relay.clone(),
                    status: RelayStatus::Accepted,
                })
                .collect();
            client
                .session
                .report_publish(&id, &results)
                .await
                .map_err(session_error)?;
        }
        Ok(())
    }

    /// Recipients for one queued event, mirroring the bus rules: the sender
    /// is skipped, a partition allowlist applies, and a Welcome only reaches
    /// the addressed account.
    fn recipients(&self, flight: &InFlight) -> Vec<String> {
        let p_recipient = tag(&flight.event, "p").map(str::to_owned);
        self.client_order
            .iter()
            .filter(|label| {
                if **label == flight.sender {
                    return false;
                }
                if let Some(allow) = &self.partition
                    && !allow.contains(*label)
                {
                    return false;
                }
                if flight.purpose == OutboundPurpose::Welcome || flight.event.kind == 1059 {
                    return self.account_hex_by_label.get(*label).map(String::as_str)
                        == p_recipient.as_deref();
                }
                true
            })
            .cloned()
            .collect()
    }

    /// Move every queued object into recipient mailboxes (the bus's
    /// `deliver_all`), counting exposures for the no-endpoint check.
    fn deliver_queue(&mut self) {
        let queue = std::mem::take(&mut self.queue);
        for flight in queue {
            for recipient in self.recipients(&flight) {
                self.clients
                    .get_mut(&recipient)
                    .expect("client")
                    .mailbox
                    .push(flight.event.clone());
                if let Some(record) = self.records.get_mut(&flight.outbound_id) {
                    record.delivered += 1;
                }
            }
        }
    }

    /// The account id of a client label.
    fn account_of(&self, label: &str) -> Result<AccountId, SubjectError> {
        self.account_hex_by_label
            .get(label)
            .and_then(|hex_str| hex::decode(hex_str).ok())
            .and_then(|bytes| <[u8; 32]>::try_from(bytes.as_slice()).ok())
            .map(AccountId)
            .ok_or_else(|| subject_error("unknown_client", format!("unknown client {label}")))
    }

    /// The scenario-facing member label for an account id (what vector
    /// expectations name).
    fn label_for_account(&self, account: &AccountId) -> String {
        self.label_by_account
            .get(&account.to_hex())
            .cloned()
            .unwrap_or_else(|| account.to_hex())
    }

    /// The active scenario group's `GroupId`.
    fn active_gid(&self) -> Result<GroupId, SubjectError> {
        let label = self
            .active_group
            .as_ref()
            .ok_or_else(|| subject_error("no_group", "no active scenario group"))?;
        self.scenario_groups
            .get(label)
            .cloned()
            .ok_or_else(|| subject_error("no_group", format!("scenario group {label} not created")))
    }

    fn client(&self, label: &str) -> Result<&SubjectClient, SubjectError> {
        self.clients
            .get(label)
            .ok_or_else(|| subject_error("unknown_client", format!("unknown client {label}")))
    }

    fn client_mut(&mut self, label: &str) -> Result<&mut SubjectClient, SubjectError> {
        self.clients
            .get_mut(label)
            .ok_or_else(|| subject_error("unknown_client", format!("unknown client {label}")))
    }

    /// `Invitee`s for invitee labels from the published KeyPackage directory.
    fn invitees(&self, labels: &[String]) -> Result<Vec<Invitee>, SubjectError> {
        labels
            .iter()
            .map(|label| {
                let client = self.client(label)?;
                Ok(Invitee {
                    key_package: client.key_package.clone().ok_or_else(|| {
                        subject_error("no_key_package", format!("{label} has no KeyPackage"))
                    })?,
                    inbox_relays: vec![RELAY.to_owned()],
                })
            })
            .collect()
    }
}

/// Reconstruct a `TransportMessage` view of an outbound event for the
/// artifact record. Delivery in this adapter uses the event itself; the
/// message is ledger metadata.
fn transport_message(event: &Event) -> TransportMessage {
    let envelope = if event.kind == 1059 {
        TransportEnvelope::Welcome {
            recipient: MemberId::new(
                hex::decode(tag(event, "p").unwrap_or_default()).unwrap_or_default(),
            ),
        }
    } else {
        TransportEnvelope::GroupMessage {
            transport_group_id: hex::decode(tag(event, "h").unwrap_or_default())
                .unwrap_or_default(),
        }
    };
    TransportMessage {
        id: MessageId::new(event.id.0.to_vec()),
        payload: event.content.clone().into_bytes(),
        timestamp: Timestamp(event.created_at),
        causal_deps: Vec::new(),
        source: TransportSource("nostr".into()),
        envelope,
    }
}

#[async_trait]
impl ConvergenceSubject for SessionSubject {
    fn descriptor(&self) -> SubjectDescriptor {
        self.descriptor.clone()
    }

    fn select_scenario_group(
        &mut self,
        group: &str,
        allow_create: bool,
    ) -> Result<(), SubjectError> {
        if self.scenario_groups.contains_key(group) {
            self.active_group = Some(group.to_owned());
            return Ok(());
        }
        if allow_create {
            self.active_group = Some(group.to_owned());
            return Ok(());
        }
        Err(subject_error(
            "unknown_group",
            format!("scenario group {group} does not exist"),
        ))
    }

    async fn create_group(&mut self, action: SubjectCreateGroup<'_>) -> Result<(), SubjectError> {
        let invitees = self.invitees(action.invitees)?;
        let mut admins = Vec::new();
        for admin in action.initial_admins {
            admins.push(self.account_of(admin)?);
        }
        let gid = {
            let client = self.client_mut(action.creator)?;
            client
                .session
                .create_group(CreateGroup {
                    name: action.name.to_owned(),
                    description: String::new(),
                    relays: vec![RELAY.to_owned()],
                    invitees,
                    admins,
                    components: Vec::new(),
                    own_devices: Vec::new(),
                })
                .await
                .map_err(session_error)?
        };
        if let Some(label) = self.active_group.clone() {
            self.scenario_groups.insert(label, gid);
        } else {
            self.scenario_groups.insert("default".into(), gid);
            self.active_group = Some("default".into());
        }
        self.sync_outbound_reported(action.creator, Some(action.pending))
            .await
    }

    async fn invite_members(
        &mut self,
        action: SubjectInviteMembers<'_>,
    ) -> Result<(), SubjectError> {
        let gid = self.active_gid()?;
        let invitees = self.invitees(action.invitees)?;
        self.client_mut(action.inviter)?
            .session
            .invite(&gid, invitees)
            .await
            .map_err(session_error)?;
        self.sync_outbound_reported(action.inviter, Some(action.pending))
            .await
    }

    async fn update_group_data(
        &mut self,
        action: SubjectUpdateGroupData<'_>,
    ) -> Result<(), SubjectError> {
        let gid = self.active_gid()?;
        self.client_mut(action.client)?
            .session
            .update_group_profile(
                &gid,
                action.name.map(str::to_owned),
                action.description.map(str::to_owned),
            )
            .await
            .map_err(session_error)?;
        self.sync_outbound_reported(action.client, Some(action.pending))
            .await
    }

    async fn remove_members(
        &mut self,
        action: SubjectRemoveMembers<'_>,
    ) -> Result<(), SubjectError> {
        let gid = self.active_gid()?;
        let members: Vec<AccountId> = action
            .members
            .iter()
            .map(|label| self.account_of(label))
            .collect::<Result<_, _>>()?;
        self.client_mut(action.remover)?
            .session
            .remove_members(&gid, &members)
            .await
            .map_err(session_error)?;
        self.sync_outbound_reported(action.remover, Some(action.pending))
            .await
    }

    async fn self_update(&mut self, action: SubjectSelfUpdate<'_>) -> Result<(), SubjectError> {
        let gid = self.active_gid()?;
        self.client_mut(action.client)?
            .session
            .self_update(&gid)
            .await
            .map_err(session_error)?;
        self.sync_outbound_reported(action.client, Some(action.pending))
            .await
    }

    async fn update_admin_policy(
        &mut self,
        action: SubjectUpdateAdminPolicy<'_>,
    ) -> Result<(), SubjectError> {
        let gid = self.active_gid()?;
        let mut admins: Vec<[u8; 32]> = Vec::new();
        for label in action.admins {
            admins.push(self.account_of(label)?.0);
        }
        admins.sort();
        admins.dedup();
        // `encode_admin_policy`: QUIC-varint byte length, then the sorted
        // account ids (marmot app-components admin-policy-v1).
        let mut data = Vec::new();
        cgka_traits::app_components::encode_quic_varint(admins.len() as u64 * 32, &mut data);
        for admin in &admins {
            data.extend_from_slice(admin);
        }
        self.client_mut(action.client)?
            .session
            .update_app_components(
                &gid,
                vec![ComponentUpdate {
                    component_id: ADMIN_POLICY_COMPONENT_ID,
                    data,
                }],
            )
            .await
            .map_err(session_error)?;
        self.sync_outbound_reported(action.client, action.pending)
            .await
    }

    async fn send_application(
        &mut self,
        action: SubjectSendApplication<'_>,
    ) -> Result<(), SubjectError> {
        let gid = self.active_gid()?;
        self.client_mut(action.sender)?
            .session
            .send_message(
                &gid,
                AppMessage {
                    kind: 9,
                    tags: Vec::new(),
                    content: action.payload.to_owned(),
                },
            )
            .await
            .map_err(session_error)?;
        self.sync_outbound_reported(action.sender, None).await
    }

    async fn leave(&mut self, _action_id: &str, client: &str) -> Result<(), SubjectError> {
        let gid = self.active_gid()?;
        self.client_mut(client)?
            .session
            .leave(&gid)
            .await
            .map_err(session_error)?;
        self.sync_outbound_reported(client, None).await
    }

    fn deliver_all(&mut self) -> Result<(), SubjectError> {
        self.deliver_queue();
        Ok(())
    }

    async fn tick(&mut self, clients: &[String]) -> Result<(), SubjectError> {
        for label in clients {
            {
                let client = self.client_mut(label)?;
                // One ingest per delivered message, in queue order — the
                // harness's per-message ingest keeps delivery order while a
                // batch re-sorts by (created_at, id).
                let mailbox = std::mem::take(&mut client.mailbox);
                for event in mailbox {
                    client.session.ingest(&event).await.map_err(session_error)?;
                }
                // Bounded settle, mirroring the legacy harness tick: drive a
                // pass, then jump the monotonic clock past the reported wake
                // (`harness_convergence_now_ms` = max(1_000_000, latest armed
                // cutoff)), until no earlier wake remains. The wall clock is
                // untouched so event timestamps stay in the same domain.
                for _ in 0..SETTLE_PASSES {
                    let report = client.session.tick().await.map_err(session_error)?;
                    let Some(delay) = report.next_wake_ms else {
                        break;
                    };
                    let now = client.clock.now();
                    client.clock.set(now.monotonic_ms + delay + 1, now.unix_ms);
                }
                // The app acknowledges every delivered application message;
                // the drained events stay buffered for `observe` (the
                // harness's engine event drain has the same semantics).
                let mut drained = client.session.drain_events().map_err(session_error)?;
                let ids: Vec<EventId> = drained
                    .iter()
                    .filter_map(|event| match event {
                        SessionEvent::MessageReceived { message, .. } => Some(message.transport_id),
                        _ => None,
                    })
                    .collect();
                client.pending_events.append(&mut drained);
                if !ids.is_empty() {
                    client
                        .session
                        .acknowledge_messages(&ids)
                        .map_err(session_error)?;
                }
            }
            self.sync_outbound_reported(label, None).await?;
        }
        Ok(())
    }

    fn poll_outbound(
        &mut self,
        client: &str,
    ) -> Result<Vec<SubjectOutboundArtifact>, SubjectError> {
        self.sync_outbound(client, None)?;
        Ok(self
            .records
            .values()
            .filter(|record| record.artifact.client == client && record.resolution.is_none())
            .map(|record| record.artifact.clone())
            .collect())
    }

    async fn acknowledge_outbound(
        &mut self,
        client: &str,
        outbound_id: &str,
        outcome: SubjectOutboundOutcome,
    ) -> Result<(), SubjectError> {
        self.sync_outbound_reported(client, None).await?;
        let record = self.records.get(outbound_id).ok_or_else(|| {
            subject_error(
                "unknown_outbound",
                format!("unknown outbound artifact {outbound_id}"),
            )
        })?;
        if record.artifact.client != client {
            return Err(subject_error(
                "outbound_client_mismatch",
                format!("outbound artifact {outbound_id} does not belong to {client}"),
            ));
        }
        if let Some(previous) = record.resolution {
            return if previous == outcome {
                Ok(())
            } else {
                Err(subject_error(
                    "outbound_already_resolved",
                    format!("outbound artifact {outbound_id} was already resolved as {previous:?}"),
                ))
            };
        }
        let outbound: mk_core::OutboundId = self.records[outbound_id].outbound_id;
        let publication = self.records[outbound_id].publication.clone();
        match outcome {
            SubjectOutboundOutcome::Accepted => {
                let results: Vec<RelayResult> = self.records[outbound_id]
                    .relays
                    .iter()
                    .map(|relay| RelayResult {
                        relay: relay.clone(),
                        status: RelayStatus::Accepted,
                    })
                    .collect();
                self.client_mut(client)?
                    .session
                    .report_publish(&outbound, &results)
                    .await
                    .map_err(session_error)?;
            }
            SubjectOutboundOutcome::ReachedNoEndpoint => {
                // Mirror `try_fail_publication`'s exposure-set rule: a
                // pending cannot be rolled back once a sibling artifact was
                // accepted, and queued bytes cannot be retracted after any
                // recipient exposure.
                if let Some(publication) = &publication {
                    let sibling_exposed = self.records.values().any(|candidate| {
                        candidate.artifact.client == client
                            && candidate.publication.as_ref() == Some(publication)
                            && (candidate.resolution == Some(SubjectOutboundOutcome::Accepted)
                                || candidate.delivered > 0)
                    });
                    if sibling_exposed {
                        return Err(subject_error(
                            "outbound_already_exposed",
                            format!(
                                "cannot report no-endpoint publication for {outbound_id} after a sibling artifact was accepted or delivered"
                            ),
                        ));
                    }
                } else if self.records[outbound_id].delivered > 0 {
                    return Err(subject_error(
                        "outbound_already_exposed",
                        format!(
                            "cannot report no-endpoint publication after {} recipient exposure(s)",
                            self.records[outbound_id].delivered
                        ),
                    ));
                }
                let results: Vec<RelayResult> = self.records[outbound_id]
                    .relays
                    .iter()
                    .map(|relay| RelayResult {
                        relay: relay.clone(),
                        status: RelayStatus::Rejected,
                    })
                    .collect();
                self.client_mut(client)?
                    .session
                    .report_publish(&outbound, &results)
                    .await
                    .map_err(session_error)?;
                // Retract undelivered queued copies of this artifact.
                self.queue
                    .retain(|flight| flight.outbound_id != outbound_id);
            }
        }
        self.records
            .get_mut(outbound_id)
            .expect("validated record")
            .resolution = Some(outcome);
        ack_gate(self.client_mut(client)?);
        // A confirmed commit releases its post-confirmation welcomes
        // (marmot-session `publish_one_with_post_confirmation_welcomes`);
        // they join the queue tagged with the pending's publication label,
        // like the engine subject's `try_confirm` `deferred_welcomes` send.
        self.sync_outbound_reported(client, publication.as_deref())
            .await?;
        Ok(())
    }

    fn observe(&mut self, clients: &[String]) -> Result<Vec<ClientObservation>, SubjectError> {
        let gid = self.active_gid().ok();
        let mut observations = Vec::new();
        for label in clients {
            let client = self.client_mut(label)?;
            let mut events = std::mem::take(&mut client.pending_events);
            events.extend(client.session.drain_events().map_err(session_error)?);
            let snapshot = gid
                .as_ref()
                .and_then(|gid| client.session.group(gid).ok().flatten());
            let mut counts = ClientEventCounts {
                message_received: 0,
                member_added: 0,
                member_removed: 0,
                epoch_changed: 0,
                app_invalidated: 0,
            };
            let mut received_payloads = Vec::new();
            let mut added_members: Vec<(String, Option<EventId>)> = Vec::new();
            let mut removed_members = Vec::new();
            let mut epoch_changes = Vec::new();
            let mut app_invalidations = Vec::new();
            let mut withdrawn_commits = std::collections::HashSet::new();
            for event in &events {
                match event {
                    SessionEvent::MessageReceived { message, .. } => {
                        counts.message_received += 1;
                        received_payloads.push(message.content.clone());
                    }
                    SessionEvent::MessageInvalidated { epoch, .. } => {
                        counts.app_invalidated += 1;
                        app_invalidations.push(AppInvalidationObservation {
                            epoch: *epoch,
                            reason: String::new(),
                            payload_ref: None,
                        });
                    }
                    SessionEvent::EpochChanged { from, to, .. } => {
                        counts.epoch_changed += 1;
                        epoch_changes.push(EpochChangeObservation {
                            from: *from,
                            to: *to,
                        });
                    }
                    SessionEvent::GroupStateChanged {
                        change,
                        origin_commit,
                        ..
                    } => match change {
                        GroupChange::MemberAdded { account } => {
                            counts.member_added += 1;
                            added_members.push((self.label_for_account(account), *origin_commit));
                        }
                        GroupChange::MemberRemoved { account }
                        | GroupChange::MemberLeft { account } => {
                            counts.member_removed += 1;
                            removed_members.push(self.label_for_account(account));
                        }
                        _ => {}
                    },
                    SessionEvent::GroupStateInvalidated { commit, .. } => {
                        withdrawn_commits.insert(*commit);
                    }
                    SessionEvent::GroupStateRevalidated { commit, .. } => {
                        withdrawn_commits.remove(commit);
                    }
                    _ => {}
                }
            }
            let withdrawn_added_members: Vec<String> = added_members
                .iter()
                .filter(|(_, commit)| {
                    commit.is_some_and(|commit| withdrawn_commits.contains(&commit))
                })
                .map(|(label, _)| label.clone())
                .collect();
            observations.push(ClientObservation {
                client: label.clone(),
                epoch: snapshot.as_ref().map_or(0, |snapshot| snapshot.epoch),
                member_count: snapshot
                    .as_ref()
                    .map_or(0, |snapshot| snapshot.members.len()),
                group_name: snapshot
                    .as_ref()
                    .map_or_else(String::new, |snapshot| snapshot.name.clone()),
                group_description: snapshot
                    .as_ref()
                    .map_or_else(String::new, |snapshot| snapshot.description.clone()),
                canonical_state: None,
                scenario_input_ledger: Vec::new(),
                pending_work: None,
                event_counts: counts,
                received_payloads,
                added_members: added_members.into_iter().map(|(label, _)| label).collect(),
                withdrawn_added_members,
                removed_members,
                epoch_changes,
                app_invalidations,
                convergence_decisions: Vec::new(),
            });
        }
        Ok(observations)
    }

    fn observe_admin_policy(
        &self,
        clients: &[String],
    ) -> Result<Vec<ScenarioAdminPolicyObservation>, SubjectError> {
        let gid = self.active_gid().ok();
        let mut observations = Vec::new();
        for label in clients {
            let client = self.client(label)?;
            let admins: Vec<String> = gid
                .as_ref()
                .and_then(|gid| client.session.group(gid).ok().flatten())
                .map(|snapshot| {
                    snapshot
                        .members
                        .iter()
                        .filter(|member| member.admin)
                        .map(|member| self.label_for_account(&member.account))
                        .collect()
                })
                .unwrap_or_default();
            observations.push(ScenarioAdminPolicyObservation {
                client: label.clone(),
                admins,
            });
        }
        Ok(observations)
    }

    fn evaluate_predicate(
        &mut self,
        predicate: &ScenarioPredicateV2,
    ) -> Result<ScenarioPredicateObservationV2, SubjectError> {
        match predicate {
            ScenarioPredicateV2::ClientState {
                client,
                epoch,
                member_count,
            } => {
                let gid = self.active_gid()?;
                let client_state = self.client(client)?;
                let snapshot = client_state
                    .session
                    .group(&gid)
                    .map_err(session_error)?
                    .ok_or_else(|| {
                        subject_error("client_not_in_group", format!("{client} has no group"))
                    })?;
                let matched = epoch.is_none_or(|epoch| snapshot.epoch == epoch)
                    && member_count.is_none_or(|count| snapshot.members.len() == count);
                Ok(ScenarioPredicateObservationV2 {
                    matched,
                    actual: serde_json::json!({
                        "client": client,
                        "epoch": snapshot.epoch,
                        "member_count": snapshot.members.len(),
                    }),
                })
            }
            _ => Err(subject_error(
                "assertion_evaluation",
                "predicate needs a capability this subject does not declare",
            )),
        }
    }

    fn clear_events(&mut self, clients: &[String]) -> Result<(), SubjectError> {
        for label in clients {
            let client = self.client_mut(label)?;
            client.session.drain_events().map_err(session_error)?;
            client.pending_events.clear();
        }
        Ok(())
    }

    fn restart(&mut self, label: &str) -> Result<(), SubjectError> {
        let (signer, clock, host, rows) = {
            let client = self.client(label)?;
            (
                Arc::clone(&client.signer),
                Arc::clone(&client.clock),
                Arc::clone(&client.host),
                client.host.durable_rows(),
            )
        };
        let opened =
            Session::open(session_config(signer, clock, host, rows)).map_err(other_error)?;
        let session = match opened {
            Opened::Ready(session) => *session,
            Opened::NeedsAccountProof(_) => {
                return Err(subject_error(
                    "restart_needs_proof",
                    "restart must not require a new account proof",
                ));
            }
        };
        let client = self.client_mut(label)?;
        client.session = session;
        // Undrained events do not survive a process restart in the harness.
        client.pending_events.clear();
        ack_gate(client);
        Ok(())
    }

    fn fault_injection(&mut self) -> Option<&mut dyn ConvergenceFaultSubject> {
        Some(self)
    }
}

impl SessionSubject {
    /// The queue index of a selector's `occurrence`-th match, mirroring the
    /// engine subject's `semantic_queue_index`: the selector must constrain
    /// something, a `publication` selector must name emitted artifacts, and a
    /// no-match is `message_selector_no_match`.
    fn queue_index(&self, selector: &ScenarioMessageSelectorV2) -> Result<usize, SubjectError> {
        if !selector.is_semantic() {
            return Err(subject_error(
                "empty_message_selector",
                "message selector must constrain action, publication, sender, or class",
            ));
        }
        if selector.publication.is_some()
            && !self
                .records
                .values()
                .any(|record| record.publication == selector.publication)
        {
            return Err(subject_error(
                "unknown_publication_selector",
                format!(
                    "no emitted transport artifacts match publication {:?}",
                    selector.publication
                ),
            ));
        }
        self.queue
            .iter()
            .enumerate()
            .filter(|(_, flight)| flight_matches(&self.records, flight, selector))
            .nth(selector.occurrence)
            .map(|(index, _)| index)
            .ok_or_else(|| {
                subject_error(
                    "message_selector_no_match",
                    format!("no queued transport object matches {selector:?}"),
                )
            })
    }
}

impl ConvergenceFaultSubject for SessionSubject {
    fn set_partition(&mut self, allow: &[String]) -> Result<(), SubjectError> {
        self.partition = Some(allow.iter().cloned().collect());
        Ok(())
    }

    fn clear_partition(&mut self) -> Result<(), SubjectError> {
        self.partition = None;
        Ok(())
    }

    fn omit_message(&mut self, selector: &ScenarioMessageSelectorV2) -> Result<(), SubjectError> {
        let index = self.queue_index(selector)?;
        self.queue.remove(index);
        Ok(())
    }

    fn duplicate_message(
        &mut self,
        selector: &ScenarioMessageSelectorV2,
    ) -> Result<(), SubjectError> {
        let index = self.queue_index(selector)?;
        // `duplicate_queued`: the copy goes immediately after the original
        // so FIFO delivery observes the duplicate next.
        let copy = self.queue[index].clone();
        self.queue.insert(index + 1, copy);
        Ok(())
    }

    fn withhold_message(
        &mut self,
        selector: &ScenarioMessageSelectorV2,
        label: &str,
    ) -> Result<(), SubjectError> {
        // `delay_queued`: exactly the selected occurrence is held.
        let index = self.queue_index(selector)?;
        let flight = self.queue.remove(index);
        self.withheld
            .entry(label.to_owned())
            .or_default()
            .push(flight);
        Ok(())
    }

    fn release_withheld(&mut self, label: &str) -> Result<(), SubjectError> {
        let held = self.withheld.remove(label).ok_or_else(|| {
            subject_error(
                "unknown_withheld_messages",
                format!("withheld-message label {label} does not exist"),
            )
        })?;
        self.queue.extend(held);
        Ok(())
    }

    fn reorder_messages(
        &mut self,
        order: &[ScenarioMessageSelectorV2],
    ) -> Result<(), SubjectError> {
        // `reorder_queued`: the selectors must name every queued message
        // exactly once — a full permutation, not a partial pull-forward.
        let indices = order
            .iter()
            .map(|selector| self.queue_index(selector))
            .collect::<Result<Vec<_>, _>>()?;
        let mut seen = vec![false; self.queue.len()];
        let mut reordered = Vec::with_capacity(self.queue.len());
        for index in indices {
            if index >= self.queue.len() || seen[index] {
                return Err(subject_error(
                    "invalid_semantic_queue_order",
                    "semantic reorder must select every queued message exactly once",
                ));
            }
            seen[index] = true;
            reordered.push(self.queue[index].clone());
        }
        if reordered.len() != self.queue.len() {
            return Err(subject_error(
                "invalid_semantic_queue_order",
                "semantic reorder must select every queued message exactly once",
            ));
        }
        self.queue = reordered;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::fs;
    use std::path::PathBuf;

    use cgka_conformance_simulator::{
        SubjectCapability, VectorFixture, compile_scenario, preflight_compiled_scenario,
        run_scenario_report_with_subject,
    };

    use super::SessionSubject;
    use cgka_conformance_simulator::{ConvergenceSubject, TraceExpectation};

    /// One vector's outcome for the report: passed, refused by preflight
    /// (with the capability it needed), or failed with a precise reason.
    enum Outcome {
        Passed,
        Refused(Vec<SubjectCapability>),
        /// The recorded trace asserts `ProtocolProfile::Legacy` semantics the
        /// `Current`-profile `Session` deliberately does not have (the
        /// founding commit rolls back to epoch 0 on a no-endpoint create
        /// publication instead of staying canonical).
        ProfileGated,
        Failed(String),
        /// A recorded byte-level decoder fixture, not a scenario vector.
        ByteFixture,
    }

    fn vectors_dir() -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../vectors/mdk")
            .canonicalize()
            .expect("vectors/mdk")
    }

    /// Every recorded fixture on disk (top level + incidents), sorted.
    fn recorded_vectors() -> Vec<(String, PathBuf)> {
        let dir = vectors_dir();
        let mut out = Vec::new();
        for entry in fs::read_dir(&dir).expect("vectors/mdk") {
            let path = entry.expect("entry").path();
            if path.extension().is_some_and(|ext| ext == "json") {
                let name = path
                    .file_name()
                    .expect("name")
                    .to_string_lossy()
                    .into_owned();
                out.push((name.clone(), path));
            }
        }
        for subdir in ["incidents", "byte-fixtures"] {
            let sub = dir.join(subdir);
            if sub.is_dir() {
                for entry in fs::read_dir(&sub).expect("subdir") {
                    let path = entry.expect("entry").path();
                    if path.extension().is_some_and(|ext| ext == "json") {
                        let name = format!(
                            "{subdir}/{}",
                            path.file_name().expect("name").to_string_lossy()
                        );
                        out.push((name, path));
                    }
                }
            }
        }
        out.sort_by(|a, b| a.0.cmp(&b.0));
        out
    }

    /// The manifest's enumerated artifact names — every one must resolve to
    /// a recorded file we classify; nothing may be silently skipped.
    fn manifest_artifacts() -> Vec<String> {
        #[derive(serde::Deserialize)]
        struct Manifest {
            entries: Vec<ManifestEntry>,
        }
        #[derive(serde::Deserialize)]
        struct ManifestEntry {
            artifact: Option<String>,
        }
        let text =
            fs::read_to_string(vectors_dir().join("manifest.v1.json")).expect("manifest.v1.json");
        let manifest: Manifest = serde_json::from_str(&text).expect("manifest parses");
        manifest
            .entries
            .into_iter()
            .filter_map(|entry| entry.artifact)
            // Entries may name artifacts outside vectors/ (e.g. the Tamarin
            // policy corpus); the recorded mirror only covers vector files.
            .filter(|artifact| !artifact.starts_with(".."))
            .collect()
    }

    /// Capabilities a vector's recorded expectations require but the public
    /// `Session` surface cannot satisfy — the expectation-level twin of
    /// `required_capabilities` for steps. `ConvergenceDecision`, exact
    /// canonical state, scenario-input ledger and pending-work snapshots need
    /// the engine subject's forensic capture; decryptability probes mutate
    /// engine state; account-devices needs 0x800d support.
    fn expectation_capabilities(fixture: &VectorFixture) -> Vec<SubjectCapability> {
        let mut capabilities = BTreeSet::new();
        for expectation in &fixture.expected_outcomes {
            match expectation {
                TraceExpectation::ConvergenceDecision { .. }
                | TraceExpectation::ClientsExactlyEquivalent { .. }
                | TraceExpectation::ClientsNotEquivalent { .. }
                | TraceExpectation::ScenarioInputLedger { .. } => {
                    capabilities.insert(SubjectCapability::ExactConformanceObservation);
                }
                TraceExpectation::NoPendingWork { .. }
                | TraceExpectation::NoPendingWorkExceptRetainedJoinCommit { .. } => {
                    capabilities.insert(SubjectCapability::ExactConformanceObservation);
                    capabilities.insert(SubjectCapability::StructuralProgress);
                }
                TraceExpectation::ClientsBidirectionallyDecryptable { .. } => {
                    capabilities.insert(SubjectCapability::ActiveDecryptabilityProbe);
                }
                TraceExpectation::AccountDevicesState { .. } => {
                    capabilities.insert(SubjectCapability::AccountDevices);
                }
                _ => {}
            }
        }
        if let Some(trace) = &fixture.expected_trace {
            for observation in &trace.observations {
                if observation.canonical_state.is_some()
                    || !observation.scenario_input_ledger.is_empty()
                    || !observation.convergence_decisions.is_empty()
                {
                    capabilities.insert(SubjectCapability::ExactConformanceObservation);
                }
                if observation.pending_work.is_some() {
                    capabilities.insert(SubjectCapability::StructuralProgress);
                }
            }
        }
        capabilities.into_iter().collect()
    }

    /// `true` when the recorded trace requires `ProtocolProfile::Legacy`
    /// create semantics — a no-endpoint "create" publication that rolls the
    /// group back to epoch 0. Under `ProtocolProfile::Current` the founding
    /// state is canonical as soon as `create_group` returns, so the recorded
    /// outcome cannot be reproduced.
    fn requires_legacy_create(fixture: &VectorFixture) -> bool {
        let rolled_back_create =
            |pending: &str, resolution: &str| pending == "create" && resolution == "rolled_back";
        fixture
            .expected_outcomes
            .iter()
            .any(|expectation| match expectation {
                TraceExpectation::PendingResolution {
                    pending,
                    resolution,
                    ..
                } => rolled_back_create(pending, resolution),
                _ => false,
            })
            || fixture.expected_trace.as_ref().is_some_and(|trace| {
                trace
                    .pending_resolutions
                    .iter()
                    .any(|pending| rolled_back_create(&pending.pending, &pending.resolution))
            })
    }

    async fn run_one(path: &PathBuf) -> Result<Outcome, String> {
        let text = fs::read_to_string(path).map_err(|err| format!("read: {err}"))?;
        let fixture: VectorFixture =
            serde_json::from_str(&text).map_err(|err| format!("parse: {err}"))?;
        if requires_legacy_create(&fixture) {
            return Ok(Outcome::ProfileGated);
        }
        // Build the subject up front so compile errors and unsupported
        // capabilities classify before any action executes.
        let mut subject = SessionSubject::new(&fixture.scenario.clients)
            .await
            .map_err(|err| format!("subject build: {err}"))?;
        let compiled = compile_scenario(&fixture.scenario)
            .map_err(|err| format!("compile: {}", err.message))?;
        let descriptor = subject.descriptor();
        // Enumerate every missing capability: step requirements (preflight)
        // plus expectations the public surface cannot observe.
        let mut missing: BTreeSet<SubjectCapability> = compiled
            .actions
            .iter()
            .flat_map(|action| action.schedule.required_capabilities.iter().copied())
            .filter(|capability| !descriptor.supports(*capability))
            .collect();
        if missing.is_empty() {
            let _ = preflight_compiled_scenario(&compiled, &descriptor)
                .map_err(|err| format!("preflight: {err}"))?;
        }
        missing.extend(
            expectation_capabilities(&fixture)
                .into_iter()
                .filter(|capability| !descriptor.supports(*capability)),
        );
        if !missing.is_empty() {
            return Ok(Outcome::Refused(missing.into_iter().collect()));
        }
        match run_scenario_report_with_subject(
            &fixture.scenario,
            fixture.expected_trace.clone(),
            fixture.expected_outcomes.clone(),
            &mut subject,
        )
        .await
        {
            Ok(report) => {
                if report.expectation_failures.is_empty() && report.invariant_failures.is_empty() {
                    Ok(Outcome::Passed)
                } else {
                    let mut reasons = Vec::new();
                    for entry in &report.step_log {
                        if let cgka_conformance_simulator::ScenarioStepStatus::Failed {
                            kind,
                            message,
                            ..
                        } = &entry.status
                        {
                            reasons.push(format!(
                                "step {} ({}): {}: {}",
                                entry.step_index, entry.step_type, kind, message
                            ));
                        }
                    }
                    for failure in &report.expectation_failures {
                        reasons.push(format!("expectation: {failure:?}"));
                    }
                    for failure in &report.invariant_failures {
                        reasons.push(format!("invariant: {failure:?}"));
                    }
                    Ok(Outcome::Failed(reasons.join(" | ")))
                }
            }
            Err(err) => Ok(Outcome::Failed(format!(
                "step {:?} {}: {}",
                err.step_index, err.kind, err.message
            ))),
        }
    }

    /// Vectors refused because their recorded expectations name observation
    /// or mutation surfaces the public `Session` API cannot provide, on top
    /// of any step-level capability gaps. The list is explicit so a vector
    /// that newly passes, newly refuses, or refuses differently is noticed.
    const EXPECTED_REFUSALS: &[(&str, &[SubjectCapability])] = &[
        (
            "account-devices-activation.v1.json",
            &[
                SubjectCapability::AccountDevices,
                SubjectCapability::ExactConformanceObservation,
                SubjectCapability::StructuralProgress,
            ],
        ),
        (
            "account-devices-activation-loses-to-unsupported-invite.v1.json",
            &[
                SubjectCapability::AccountDevices,
                SubjectCapability::ExactConformanceObservation,
                SubjectCapability::StructuralProgress,
            ],
        ),
        (
            "account-devices-activation-needs-support.v1.json",
            &[
                SubjectCapability::AccountDevices,
                SubjectCapability::ExactConformanceObservation,
                SubjectCapability::StructuralProgress,
            ],
        ),
        (
            "account-devices-activation-wins-over-unsupported-invite.v1.json",
            &[
                SubjectCapability::AccountDevices,
                SubjectCapability::ExactConformanceObservation,
                SubjectCapability::StructuralProgress,
            ],
        ),
        (
            "account-devices-founding.v1.json",
            &[
                SubjectCapability::AccountDevices,
                SubjectCapability::ExactConformanceObservation,
                SubjectCapability::StructuralProgress,
            ],
        ),
        (
            "account-devices-own-device-removal-loses-to-admin-remove.v1.json",
            &[
                SubjectCapability::AccountDevices,
                SubjectCapability::ExactConformanceObservation,
                SubjectCapability::StructuralProgress,
            ],
        ),
        (
            "admin-handover.v1.json",
            &[
                SubjectCapability::ExactConformanceObservation,
                SubjectCapability::StructuralProgress,
            ],
        ),
        (
            "concurrent-invite-fork-recovery.v1.json",
            &[SubjectCapability::ExactConformanceObservation],
        ),
        (
            "conversation.v1.json",
            &[
                SubjectCapability::ExactConformanceObservation,
                SubjectCapability::StructuralProgress,
            ],
        ),
        (
            "convergence-committer-selected.v1.json",
            &[SubjectCapability::ExactConformanceObservation],
        ),
        (
            "convergence-witness-selected.v1.json",
            &[SubjectCapability::ExactConformanceObservation],
        ),
        (
            "cross-route-own-commit-recovery.v1.json",
            &[
                SubjectCapability::ExactConformanceObservation,
                SubjectCapability::ActiveDecryptabilityProbe,
                SubjectCapability::VirtualTime,
                SubjectCapability::StructuralProgress,
            ],
        ),
        (
            "deferred-tick-catchup.v1.json",
            &[
                SubjectCapability::ExactConformanceObservation,
                SubjectCapability::StructuralProgress,
            ],
        ),
        (
            "group-data-fork-recovery.v1.json",
            &[SubjectCapability::ExactConformanceObservation],
        ),
        (
            "group-profile-update.v1.json",
            &[
                SubjectCapability::ExactConformanceObservation,
                SubjectCapability::ActiveDecryptabilityProbe,
                SubjectCapability::StructuralProgress,
            ],
        ),
        (
            "incidents/convergence-incident.v1.json",
            &[SubjectCapability::ExactConformanceObservation],
        ),
        (
            "incidents/fork-recovery-incident.v1.json",
            &[SubjectCapability::ExactConformanceObservation],
        ),
        (
            "incidents/membership-fork-recovery-incident.v1.json",
            &[SubjectCapability::ExactConformanceObservation],
        ),
        (
            "incremental-growth.v1.json",
            &[
                SubjectCapability::ExactConformanceObservation,
                SubjectCapability::AssertionEvaluation,
                SubjectCapability::StructuralProgress,
            ],
        ),
        (
            "latecomer-forward-secrecy.v1.json",
            &[
                SubjectCapability::ExactConformanceObservation,
                SubjectCapability::AssertionEvaluation,
                SubjectCapability::StructuralProgress,
            ],
        ),
        (
            "leaver-removal-secrecy.v1.json",
            &[
                SubjectCapability::ExactConformanceObservation,
                SubjectCapability::AssertionEvaluation,
                SubjectCapability::StructuralProgress,
            ],
        ),
        (
            "multigroup-isolation.v1.json",
            &[
                SubjectCapability::MultiGroup,
                SubjectCapability::ExactConformanceObservation,
                SubjectCapability::AssertionEvaluation,
            ],
        ),
        (
            "route-equivalence.v1.json",
            &[
                SubjectCapability::ExactConformanceObservation,
                SubjectCapability::ActiveDecryptabilityProbe,
                SubjectCapability::StructuralProgress,
            ],
        ),
    ];

    /// Vectors whose recorded trace asserts `ProtocolProfile::Legacy`
    /// semantics a `Current`-profile `Session` cannot reproduce.
    const PROFILE_REFUSALS: &[&str] = &["publish-fail.v1.json"];

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn mdk_recorded_vectors() {
        let recorded = recorded_vectors();
        let manifest = manifest_artifacts();
        for artifact in &manifest {
            assert!(
                recorded.iter().any(|(name, _)| name == artifact),
                "manifest artifact {artifact} has no recorded file"
            );
        }
        let mut results: Vec<(String, Outcome)> = Vec::new();
        for (name, path) in &recorded {
            if name == "manifest.v1.json" {
                continue;
            }
            if name.starts_with("byte-fixtures/") {
                results.push((name.clone(), Outcome::ByteFixture));
                continue;
            }
            let outcome = run_one(path).await.unwrap_or_else(Outcome::Failed);
            results.push((name.clone(), outcome));
        }
        let mut report = String::new();
        let mut failures = Vec::new();
        for (name, outcome) in &results {
            match outcome {
                Outcome::Passed => {
                    report.push_str(&format!("PASS {name}\n"));
                }
                Outcome::Refused(caps) => {
                    let caps: Vec<String> = caps.iter().map(|c| c.as_str().into()).collect();
                    report.push_str(&format!("REFUSED {name} ({})\n", caps.join(", ")));
                }
                Outcome::ProfileGated => {
                    report.push_str(&format!(
                        "REFUSED {name} (protocol_profile: legacy create semantics)\n"
                    ));
                }
                Outcome::Failed(reason) => {
                    report.push_str(&format!("FAILED {name}: {reason}\n"));
                    failures.push(format!("{name}: {reason}"));
                }
                Outcome::ByteFixture => {
                    report.push_str(&format!(
                        "BYTE-FIXTURE {name} (decoder fixture, not a scenario)\n"
                    ));
                }
            }
        }
        eprintln!("\n=== mdk vector results ===\n{report}");
        // Refusals must match the explicit expected list: a vector that
        // starts running (or newly refuses) is a status change to notice.
        for (expected, _) in EXPECTED_REFUSALS {
            let refused = results
                .iter()
                .any(|(name, outcome)| name == expected && matches!(outcome, Outcome::Refused(_)));
            assert!(
                refused,
                "{expected} is no longer refused by preflight — update EXPECTED_REFUSALS"
            );
        }
        for (name, outcome) in &results {
            match outcome {
                Outcome::Refused(caps) => {
                    let expected = EXPECTED_REFUSALS
                        .iter()
                        .find(|(expected, _)| expected == name)
                        .unwrap_or_else(|| {
                            panic!("{name} refused by preflight but is not in EXPECTED_REFUSALS")
                        });
                    let expected_set: BTreeSet<_> = expected.1.iter().copied().collect();
                    let actual_set: BTreeSet<_> = caps.iter().copied().collect();
                    assert_eq!(
                        expected_set, actual_set,
                        "{name} refused with unexpected capability set"
                    );
                }
                Outcome::ProfileGated => {
                    assert!(
                        PROFILE_REFUSALS.contains(&name.as_str()),
                        "{name} is profile-gated but not in PROFILE_REFUSALS"
                    );
                }
                _ => {}
            }
        }
        for expected in PROFILE_REFUSALS {
            let gated = results.iter().any(|(name, outcome)| {
                name == expected && matches!(outcome, Outcome::ProfileGated)
            });
            assert!(
                gated,
                "{expected} is no longer profile-gated — update PROFILE_REFUSALS"
            );
        }
        assert!(
            failures.is_empty(),
            "{} vector(s) failed:\n{}",
            failures.len(),
            failures.join("\n")
        );
    }
}
