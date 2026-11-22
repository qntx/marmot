//! The bridge: owns one [`Session`], surfaces every host interaction as
//! bytes, and suspends engine work across `call`s when the account signer
//! defers an operation.

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

use mk_core as core;
use mk_core::{CommitSeq, KvBatch, Opened, PendingProof, Session, SessionError};
use serde_json::Value;

use crate::clock::CellClock;
use crate::error::{BridgeError, open_error, proof_error, session_error};
use crate::host::{Host, QueueHost, lock};
use crate::protocol::{
    AcknowledgeMessagesRequest, AttachmentData, AwaitingDurabilityResult, CallEnvelope, Config,
    ConfigProbe, ConfirmRejoinRequest, CreateGroupRequest, DeclineRejoinRequest, DisbandRequest,
    DiscardAttachmentRequest, DrainResponse, EnableDisbandingRequest, EnsureKeyPackageRequest,
    ForgetGroupRequest, GroupCreatedResult, GroupResult, GroupsResult, HistoryFetchedRequest,
    InboxRelaysResult, IngestRequest, IngestedResult, InviteRequest, KeyPackagesResult,
    LeaveRequest, MAX_REQUEST_BYTES, MediaAttachmentRef, MessageSentResult, OpResult,
    OpenAttachmentRequest, OpenResponse, OutboundResult, PROTOCOL_VERSION,
    PublishInboxRelaysRequest, PublishResolutionResult, REQUEST_TYPES, ReaddMemberRequest,
    ReaddResult, ReadyResult, RejoinOffersRequest, RejoinOffersResult, RemoveMembersRequest,
    ReportPublishRequest, Request, Response, SealAttachmentRequest, SealedAttachmentResult,
    SelectKeyPackagesRequest, SelfUpdateRequest, SendIntentRequest, SendMediaRequest,
    SendMessageRequest, SendOutcomeResult, SetAdminRequest, SetGroupAvatarRequest,
    SetRetentionRequest, SignOutRequest, SignerReply, SignerRequest, SignerResult, TickedResult,
    UpdateAppComponentsRequest, UpdateGroupProfileRequest, decode, encode, invalid, reason_label,
};
use crate::rows::decode_rows;
use crate::signer::{QueueSigner, SignerState};

/// Attachment-handle table capacity; `seal_attachment` past it fails with
/// `attachment_limit`.
const ATTACHMENT_LIMIT: usize = 64;

/// An owned-session operation future: takes the `Box<Session>` in, returns
/// it with the outcome. Polled by the bridge only, with a noop waker —
/// progress comes exclusively from stored signer replies.
type OpFuture = Pin<Box<dyn Future<Output = (Box<Session>, Result<OpResult, SessionError>)>>>;

/// A suspended engine operation plus the attachment handles its success
/// must consume.
struct SuspendedOp {
    /// The owned-session operation.
    future: OpFuture,
    /// Attachment handles `send_media` consumes on success.
    handles: Vec<u32>,
}

impl std::fmt::Debug for SuspendedOp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SuspendedOp").finish_non_exhaustive()
    }
}

/// Bridge lifecycle. Transitions: `Fresh` --open--> `AwaitingProof` |
/// `Ready`; `Ready` <=> `Suspended`; any -> `Poisoned`; `close` -> `Closed`.
#[derive(Debug)]
enum State {
    /// `open` not yet called.
    Fresh,
    /// `open` returned `needs_account_proof`.
    AwaitingProof(Box<PendingProof>),
    /// The session is usable.
    Ready(Box<Session>),
    /// An operation awaits host signer replies.
    Suspended(SuspendedOp),
    /// `close` was called.
    Closed,
    /// Host failure or internal invariant break; terminal.
    Poisoned,
}

impl State {
    const fn label(&self) -> &'static str {
        match self {
            Self::Fresh => "fresh",
            Self::AwaitingProof(_) => "awaiting_proof",
            Self::Ready(_) => "ready",
            Self::Suspended(_) => "suspended",
            Self::Closed => "closed",
            Self::Poisoned => "poisoned",
        }
    }
}

/// A byte-protocol bridge over one [`Session`]. See the crate docs for the
/// host contract.
pub struct Bridge<H: Host> {
    host: H,
    /// Batches the session's `QueueHost` committed, awaiting flush.
    queue: Arc<Mutex<VecDeque<KvBatch>>>,
    /// The clock cell `CellClock` reads.
    time: Arc<Mutex<core::Time>>,
    /// Signer request bookkeeping for the suspended operation.
    signer_state: Arc<Mutex<SignerState>>,
    /// Highest batch sequence flushed to the host.
    last_committed: u64,
    /// Flushed-but-unacknowledged sequences, raw -> typed.
    unacked: BTreeMap<u64, CommitSeq>,
    /// `ack_durable` received while suspended, applied on completion.
    pending_ack: u64,
    /// Pending attachments waiting for `send_media`, by handle. Only the
    /// epoch-pinned reference is retained — never the ciphertext blob.
    attachments: HashMap<u32, core::PendingAttachment>,
    /// Next attachment handle.
    next_handle: u32,
    state: State,
}

/// Diagnostics name the lifecycle and counters only — queued rows,
/// attachments and session state never reach a debug string.
impl<H: Host> std::fmt::Debug for Bridge<H> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Bridge")
            .field("state", &self.state.label())
            .field("last_committed", &self.last_committed)
            .field("pending_ack", &self.pending_ack)
            .field("attachments", &self.attachments.len())
            .finish_non_exhaustive()
    }
}

impl<H: Host> Bridge<H> {
    /// A fresh bridge over `host`.
    #[must_use]
    pub fn new(host: H) -> Self {
        Self {
            host,
            queue: Arc::new(Mutex::new(VecDeque::new())),
            time: Arc::new(Mutex::new(core::Time {
                unix_ms: 0,
                monotonic_ms: 0,
            })),
            signer_state: Arc::new(Mutex::new(SignerState::default())),
            last_committed: 0,
            unacked: BTreeMap::new(),
            pending_ack: 0,
            attachments: HashMap::new(),
            next_handle: 0,
            state: State::Fresh,
        }
    }

    /// Open the session from durable host rows.
    #[must_use]
    pub fn open(&mut self, config: &[u8]) -> Vec<u8> {
        encode(&self.open_inner(config))
    }

    /// Dispatch one `call` envelope.
    #[must_use]
    pub fn call(&mut self, request: &[u8]) -> Vec<u8> {
        encode(&self.call_inner(request))
    }

    /// Drain queued session events.
    #[must_use]
    pub fn drain(&mut self) -> Vec<u8> {
        encode(&self.drain_inner())
    }

    /// Release the session. Flush is best-effort; a flush failure leaves
    /// the bridge poisoned rather than silently closed.
    pub fn close(&mut self) {
        if matches!(self.state, State::Poisoned | State::Closed) {
            return;
        }
        if self.flush().is_ok() && !matches!(self.state, State::Poisoned) {
            self.state = State::Closed;
        }
    }

    fn open_inner(&mut self, config: &[u8]) -> OpenResponse {
        if config.len() > MAX_REQUEST_BYTES {
            return Self::open_error(BridgeError::TooLarge {
                limit: MAX_REQUEST_BYTES as u64,
            });
        }
        let probe = match decode::<ConfigProbe>(config) {
            Ok(probe) => probe,
            Err(error) => return Self::open_error(error),
        };
        if probe.protocol_version != PROTOCOL_VERSION {
            return Self::open_error(BridgeError::ProtocolVersionMismatch {
                expected: PROTOCOL_VERSION,
                got: probe.protocol_version,
            });
        }
        if !matches!(self.state, State::Fresh) {
            return Self::open_error(BridgeError::AlreadyOpen);
        }
        let config = match decode::<Config>(config) {
            Ok(config) => config,
            Err(error) => return Self::open_error(error),
        };
        if !self.set_now(config.now.into()) {
            return Self::open_error(BridgeError::ClockRegressed);
        }
        let Ok(rows) = self.host.load() else {
            self.state = State::Poisoned;
            return Self::open_error(BridgeError::HostFailed);
        };
        let Ok(rows) = decode_rows(&rows) else {
            return Self::open_error(BridgeError::HostRowsInvalid);
        };
        let session_config = core::SessionConfig {
            signer: Arc::new(QueueSigner::new(
                core::AccountId(config.account),
                Arc::clone(&self.signer_state),
            )),
            host: Arc::new(QueueHost::new(Arc::clone(&self.queue))),
            rows,
            clock: Arc::new(CellClock::new(Arc::clone(&self.time))),
            destination_policy: core::DestinationPolicy {
                allow_loopback: config.allow_loopback,
            },
            client_name: config.client_name,
        };
        match Session::open(session_config) {
            Err(error) => Self::open_error(open_error(&error)),
            Ok(Opened::Ready(session)) => {
                self.state = State::Ready(session);
                if self.flush().is_err() {
                    return Self::open_error(BridgeError::HostFailed);
                }
                self.ready_response()
            }
            Ok(Opened::NeedsAccountProof(proof)) => {
                let unsigned = proof.request().clone().into();
                self.state = State::AwaitingProof(proof);
                if self.flush().is_err() {
                    return Self::open_error(BridgeError::HostFailed);
                }
                OpenResponse::NeedsAccountProof { unsigned }
            }
        }
    }

    fn call_inner(&mut self, request: &[u8]) -> Response {
        if request.len() > MAX_REQUEST_BYTES {
            return Self::error_response(BridgeError::TooLarge {
                limit: MAX_REQUEST_BYTES as u64,
            });
        }
        let envelope = match decode::<CallEnvelope>(request) {
            Ok(envelope) => envelope,
            Err(error) => return Self::error_response(error),
        };
        if !self.set_now(envelope.now.into()) {
            return Self::error_response(BridgeError::ClockRegressed);
        }
        let tag = envelope
            .request
            .get("type")
            .and_then(Value::as_str)
            .map(str::to_owned);
        match tag.as_deref() {
            Some(tag) if REQUEST_TYPES.contains(&tag) => {}
            _ => return Self::error_response(BridgeError::Unsupported),
        }
        if let Some(error) = self.state_gate(tag.as_deref()) {
            return Self::error_response(error);
        }
        match serde_json::from_value::<Request>(envelope.request) {
            Ok(parsed) => self.dispatch(parsed),
            Err(error) => Self::error_response(invalid(reason_label(&error))),
        }
    }

    /// Gate a tagged request on the current lifecycle state. Returns the
    /// error to answer, or `None` to proceed.
    fn state_gate(&self, tag: Option<&str>) -> Option<BridgeError> {
        match &self.state {
            State::Fresh => Some(BridgeError::NotOpen),
            State::Closed => Some(BridgeError::Closed),
            State::Poisoned => Some(BridgeError::Poisoned),
            State::AwaitingProof(_) => match tag {
                Some("submit_proof") => None,
                _ => Some(BridgeError::AwaitingProof),
            },
            State::Suspended(_) => match tag {
                Some("signer_reply" | "ack_durable") => None,
                _ => Some(BridgeError::Busy),
            },
            State::Ready(_) => None,
        }
    }

    fn dispatch(&mut self, request: Request) -> Response {
        if matches!(self.state, State::Suspended(_)) {
            return match request {
                Request::SignerReply(args) => self.resume(args.replies),
                Request::AckDurable(args) => self.deferred_ack(args.seq),
                // `state_gate` admits only these two while suspended.
                _ => Self::internal("state_gate"),
            };
        }
        if matches!(self.state, State::AwaitingProof(_)) {
            return match request {
                Request::SubmitProof(args) => self.submit_proof(&args.event.into()),
                _ => Self::internal("state_gate"),
            };
        }
        match request {
            Request::SubmitProof(_) => Self::error_response(BridgeError::AlreadyOpen),
            Request::AckDurable(args) => self.ack_durable(args.seq),
            Request::AwaitingDurability(_) => self.sync_op(|session| {
                Ok(OpResult::AwaitingDurability(AwaitingDurabilityResult {
                    seq: session.awaiting_durability().map(CommitSeq::get),
                }))
            }),
            Request::TakeOutbound(_) => self.sync_op(|session| {
                Ok(OpResult::Outbound(OutboundResult {
                    items: session
                        .take_outbound()?
                        .into_iter()
                        .map(Into::into)
                        .collect(),
                }))
            }),
            Request::AcknowledgeMessages(args) => self.acknowledge_messages(&args),
            Request::Groups(_) => self.sync_op(|session| {
                Ok(OpResult::Groups(GroupsResult {
                    groups: session.groups()?.into_iter().map(Into::into).collect(),
                }))
            }),
            Request::Group(args) => {
                let group = core::GroupId(args.group);
                self.sync_op(move |session| {
                    Ok(OpResult::Group(GroupResult {
                        group: session
                            .group(&group)?
                            .map(|snapshot| Box::new(snapshot.into())),
                    }))
                })
            }
            Request::SealAttachment(args) => self.seal_attachment(args),
            Request::SendMedia(args) => self.send_media(args),
            Request::DiscardAttachment(args) => self.discard_attachment(args),
            Request::OpenAttachment(args) => self.open_attachment(args),
            Request::SignerReply(_) => Self::error_response(BridgeError::NoPendingSigner),
            Request::ReportPublish(args) => self.run(op_report_publish(args), Vec::new()),
            Request::EnsureKeyPackage(args) => self.run(op_ensure_key_package(args), Vec::new()),
            Request::KeyPackageStatus(_) => self.key_package_status(),
            Request::SignOut(args) => self.run(op_sign_out(args), Vec::new()),
            Request::PublishInboxRelays(args) => {
                self.run(op_publish_inbox_relays(args), Vec::new())
            }
            Request::CreateGroup(args) => self.run(op_create_group_args(args), Vec::new()),
            Request::Invite(args) => self.run(op_invite(args), Vec::new()),
            Request::RemoveMembers(args) => self.run(op_remove_members(args), Vec::new()),
            Request::Leave(args) => self.run(op_leave(args), Vec::new()),
            Request::SelfUpdate(args) => self.run(op_self_update(args), Vec::new()),
            Request::HistoryFetched(args) => self.history_fetched(args),
            Request::UpdateGroupProfile(args) => {
                self.run(op_update_group_profile(args), Vec::new())
            }
            Request::UpdateAppComponents(args) => {
                self.run(op_update_app_components(args), Vec::new())
            }
            Request::SendMessage(args) => self.run(op_send_message(args), Vec::new()),
            Request::SendIntent(args) => self.run(op_send_intent(args), Vec::new()),
            Request::SetAdmin(args) => self.run(op_set_admin(args), Vec::new()),
            Request::SetGroupAvatar(args) => self.run(op_set_group_avatar(args), Vec::new()),
            Request::SetRetention(args) => self.run(op_set_retention(args), Vec::new()),
            Request::Disband(args) => self.run(op_disband(args), Vec::new()),
            Request::EnableDisbanding(args) => self.run(op_enable_disbanding(args), Vec::new()),
            Request::ForgetGroup(args) => self.forget_group(args),
            Request::SelectKeyPackages(args) => self.select_key_packages(args),
            Request::RejoinOffers(args) => self.rejoin_offers(args),
            Request::ConfirmRejoin(args) => self.run(op_confirm_rejoin(args), Vec::new()),
            Request::DeclineRejoin(args) => self.decline_rejoin(args),
            Request::ReaddMember(args) => self.run(op_readd_member(args), Vec::new()),
            Request::Ingest(args) => self.run(op_ingest(args), Vec::new()),
            Request::Tick(_) => self.run(op_tick(), Vec::new()),
        }
    }

    /// `submit_proof` in `AwaitingProof`: the proof either completes the
    /// open or hands the pending proof back with a typed rejection.
    fn submit_proof(&mut self, event: &core::Event) -> Response {
        let State::AwaitingProof(pending) = std::mem::replace(&mut self.state, State::Poisoned)
        else {
            return Self::internal("state");
        };
        match pending.provide_account_proof(event) {
            Ok(session) => {
                self.state = State::Ready(Box::new(session));
                if self.flush().is_err() {
                    return Self::error_response(BridgeError::HostFailed);
                }
                self.ready_result()
            }
            Err(rejected) => {
                self.state = State::AwaitingProof(rejected.pending);
                Self::error_response(proof_error(&rejected.error))
            }
        }
    }

    /// `ack_durable` in `Ready`: validate against `last_committed`, apply
    /// the covered `CommitSeq` (cumulative — a lower or repeated seq is a
    /// no-op `done`).
    fn ack_durable(&mut self, seq: u64) -> Response {
        if seq > self.last_committed {
            return Self::error_response(BridgeError::UnknownSeq);
        }
        if let Some(commit) = self.resolve_seq(seq)
            && let State::Ready(session) = &mut self.state
        {
            session.acknowledge_durable(commit);
        }
        if self.flush().is_err() {
            return Self::error_response(BridgeError::HostFailed);
        }
        Response::Ok {
            result: Box::new(OpResult::Done),
        }
    }

    /// `ack_durable` while suspended: validate now, apply when the
    /// operation completes.
    fn deferred_ack(&mut self, seq: u64) -> Response {
        if seq > self.last_committed {
            return Self::error_response(BridgeError::UnknownSeq);
        }
        self.pending_ack = self.pending_ack.max(seq);
        Response::Ok {
            result: Box::new(OpResult::Done),
        }
    }

    /// `signer_reply` while suspended: validate every reply against the
    /// pending set, store them, re-poll the operation. Validation is
    /// all-or-nothing: any bad reply leaves the operation untouched.
    fn resume(&mut self, replies: Vec<SignerReply>) -> Response {
        let State::Suspended(op) = std::mem::replace(&mut self.state, State::Poisoned) else {
            return Self::internal("state");
        };
        let validation = {
            let state = lock(&self.signer_state);
            let mut seen = HashSet::with_capacity(replies.len());
            replies
                .iter()
                .find_map(|reply| reply_error(&state, reply, &mut seen))
        };
        if let Some(error) = validation {
            self.state = State::Suspended(op);
            return Self::error_response(error);
        }
        {
            let mut state = lock(&self.signer_state);
            for reply in replies {
                state.answer(reply.id, reply.result);
            }
        }
        self.poll_op(op)
    }

    /// `seal_attachment`: keep the sealed pending handle for `send_media`.
    fn seal_attachment(&mut self, args: SealAttachmentRequest) -> Response {
        if self.attachments.len() >= ATTACHMENT_LIMIT || self.next_handle == u32::MAX {
            return Self::error_response(BridgeError::AttachmentLimit);
        }
        let handle = self.next_handle;
        let group = core::GroupId(args.group);
        let outcome = match &mut self.state {
            State::Ready(session) => session.seal_attachment(&group, args.attachment.into()),
            _ => return Self::internal("state"),
        };
        match outcome {
            Err(error) => {
                if self.flush().is_err() {
                    return Self::error_response(BridgeError::HostFailed);
                }
                Self::error_response(session_error(&error))
            }
            Ok(sealed) => {
                let result = SealedAttachmentResult {
                    ciphertext: sealed.ciphertext,
                    ciphertext_sha256: sealed.ciphertext_sha256,
                    handle,
                };
                self.attachments.insert(handle, sealed.pending);
                self.next_handle = handle.saturating_add(1);
                if self.flush().is_err() {
                    return Self::error_response(BridgeError::HostFailed);
                }
                Response::Ok {
                    result: Box::new(OpResult::SealedAttachment(result)),
                }
            }
        }
    }

    /// `send_media`: resolve handles into sealed pending attachments, run
    /// the send, consume handles only on success.
    fn send_media(&mut self, args: SendMediaRequest) -> Response {
        let mut attachments = Vec::with_capacity(args.attachments.len());
        let mut handles = Vec::with_capacity(args.attachments.len());
        for attachment in &args.attachments {
            let MediaAttachmentRef { handle, locators } = attachment;
            let Some(entry) = self.attachments.get(handle) else {
                return Self::error_response(BridgeError::UnknownAttachment);
            };
            attachments.push((
                entry.clone(),
                locators
                    .iter()
                    .cloned()
                    .map(core::media::Locator::from)
                    .collect::<Vec<_>>(),
            ));
            handles.push(*handle);
        }
        let message = core::MediaMessage {
            caption: args.caption,
            attachments,
            tags: args.tags,
        };
        let group = core::GroupId(args.group);
        self.run(
            op_sent(move |session| {
                Box::pin(async move { session.send_media(&group, message).await })
            }),
            handles,
        )
    }

    /// `forget_group`: drop the group's local rows, keep the tombstone.
    fn forget_group(&mut self, args: ForgetGroupRequest) -> Response {
        let group = core::GroupId(args.group);
        self.sync_op(|session| {
            session.forget_group(&group)?;
            Ok(OpResult::Done)
        })
    }

    /// `select_key_packages`: the session's D1 selection over the host's
    /// fetched publications and deletions.
    fn select_key_packages(&mut self, args: SelectKeyPackagesRequest) -> Response {
        let account = core::AccountId(args.account);
        let events: Vec<core::Event> = args.events.into_iter().map(Into::into).collect();
        let group = args.group.map(core::GroupId);
        self.sync_op(move |session| {
            Ok(OpResult::KeyPackages(KeyPackagesResult {
                events: session
                    .select_key_packages(account, &events, group.as_ref())?
                    .into_iter()
                    .map(Into::into)
                    .collect(),
            }))
        })
    }

    /// `key_package_status`: the account `KeyPackage` slot's lifecycle
    /// standing, read-only.
    fn key_package_status(&mut self) -> Response {
        self.sync_op(|session| {
            Ok(OpResult::KeyPackageStatus(
                session.key_package_status()?.into(),
            ))
        })
    }

    /// `history_fetched`: close the owed post-join fetch obligation.
    fn history_fetched(&mut self, args: HistoryFetchedRequest) -> Response {
        let group = core::GroupId(args.group);
        self.sync_op(|session| {
            session.history_fetched(&group)?;
            Ok(OpResult::Done)
        })
    }

    /// `rejoin_offers`: the group's stored replacement-Welcome offers.
    fn rejoin_offers(&mut self, args: RejoinOffersRequest) -> Response {
        let group = core::GroupId(args.group);
        self.sync_op(|session| {
            Ok(OpResult::RejoinOffers(RejoinOffersResult {
                offers: session
                    .rejoin_offers(&group)?
                    .into_iter()
                    .map(Into::into)
                    .collect(),
            }))
        })
    }

    /// `decline_rejoin`: retire a stored offer.
    fn decline_rejoin(&mut self, args: DeclineRejoinRequest) -> Response {
        let offer = core::EventId(args.offer);
        self.sync_op(|session| {
            session.decline_rejoin(&offer)?;
            Ok(OpResult::Done)
        })
    }

    /// `discard_attachment`: drop one sealed handle.
    fn discard_attachment(&mut self, args: DiscardAttachmentRequest) -> Response {
        if self.attachments.remove(&args.handle).is_none() {
            return Self::error_response(BridgeError::UnknownAttachment);
        }
        Response::Ok {
            result: Box::new(OpResult::Done),
        }
    }

    /// `open_attachment`: decrypt a media reference's blob.
    fn open_attachment(&mut self, args: OpenAttachmentRequest) -> Response {
        let group = core::GroupId(args.group);
        let message = core::EventId(args.message);
        let reference: core::media::MediaReference = args.reference.into();
        self.sync_op(|session| {
            Ok(OpResult::Attachment(AttachmentData {
                plaintext: session.open_attachment(
                    &group,
                    &message,
                    &reference,
                    &args.ciphertext,
                )?,
            }))
        })
    }

    /// `acknowledge_messages`: release durable pending-message rows.
    fn acknowledge_messages(&mut self, args: &AcknowledgeMessagesRequest) -> Response {
        let ids: Vec<core::EventId> = args.ids.iter().map(|id| core::EventId(*id)).collect();
        self.sync_op(|session| {
            session.acknowledge_messages(&ids)?;
            Ok(OpResult::Done)
        })
    }

    /// Run a synchronous session operation, flush, respond.
    fn sync_op(
        &mut self,
        op: impl FnOnce(&mut Session) -> Result<OpResult, SessionError>,
    ) -> Response {
        let outcome = match &mut self.state {
            State::Ready(session) => op(session),
            _ => return Self::internal("state"),
        };
        if self.flush().is_err() {
            return Self::error_response(BridgeError::HostFailed);
        }
        match outcome {
            Ok(result) => Response::Ok {
                result: Box::new(result),
            },
            Err(error) => Self::error_response(session_error(&error)),
        }
    }

    /// Run an async session operation: take the session out, poll once,
    /// and either restore it with the result or suspend for signer
    /// replies.
    fn run(&mut self, build: impl FnOnce(Box<Session>) -> OpFuture, handles: Vec<u32>) -> Response {
        let State::Ready(session) = std::mem::replace(&mut self.state, State::Poisoned) else {
            return Self::internal("state");
        };
        let suspended = SuspendedOp {
            future: build(session),
            handles,
        };
        self.poll_op(suspended)
    }

    /// Poll an operation once. Ready completes the operation; Pending with
    /// unanswered signer requests stays suspended; Pending with none is an
    /// invariant break.
    fn poll_op(&mut self, mut op: SuspendedOp) -> Response {
        let waker = Waker::noop();
        let mut context = Context::from_waker(waker);
        match op.future.as_mut().poll(&mut context) {
            Poll::Ready((session, outcome)) => self.finish_op(session, outcome, op.handles),
            Poll::Pending => {
                let unanswered: Vec<SignerRequest> = lock(&self.signer_state).unanswered();
                if unanswered.is_empty() {
                    self.state = State::Poisoned;
                    return Self::internal("stalled");
                }
                self.state = State::Suspended(op);
                if self.flush().is_err() {
                    return Self::error_response(BridgeError::HostFailed);
                }
                Response::SignerPending {
                    requests: unanswered,
                }
            }
        }
    }

    /// Complete an operation: clear its signer bookkeeping, apply a
    /// deferred `ack_durable`, consume attachment handles on success,
    /// restore the session, flush, respond.
    fn finish_op(
        &mut self,
        mut session: Box<Session>,
        outcome: Result<OpResult, SessionError>,
        handles: Vec<u32>,
    ) -> Response {
        lock(&self.signer_state).clear();
        if self.pending_ack > 0 {
            if let Some(commit) = self.resolve_seq(self.pending_ack) {
                session.acknowledge_durable(commit);
            }
            self.pending_ack = 0;
        }
        if outcome.is_ok() {
            for handle in handles {
                self.attachments.remove(&handle);
            }
        }
        self.state = State::Ready(session);
        if self.flush().is_err() {
            return Self::error_response(BridgeError::HostFailed);
        }
        match outcome {
            Ok(result) => Response::Ok {
                result: Box::new(result),
            },
            Err(error) => Self::error_response(session_error(&error)),
        }
    }

    /// Flush queued batches to the host in order. A commit failure poisons
    /// the bridge.
    fn flush(&mut self) -> Result<(), ()> {
        loop {
            let batch = {
                let mut queue = lock(&self.queue);
                queue.pop_front()
            };
            let Some(batch) = batch else {
                return Ok(());
            };
            if self.host.commit(&batch.encode()).is_ok() {
                self.last_committed = batch.seq.get();
                self.unacked.insert(batch.seq.get(), batch.seq);
            } else {
                self.state = State::Poisoned;
                return Err(());
            }
        }
    }

    /// Record `now`; refuse a monotonic regression.
    fn set_now(&self, now: core::Time) -> bool {
        let mut current = lock(&self.time);
        if now.monotonic_ms < current.monotonic_ms {
            return false;
        }
        *current = now;
        true
    }

    /// The `CommitSeq` to acknowledge for raw `seq`: the highest tracked
    /// sequence `<= seq` (cumulative ack). Entries it covers are dropped.
    fn resolve_seq(&mut self, seq: u64) -> Option<CommitSeq> {
        let commit = self
            .unacked
            .range(..=seq)
            .next_back()
            .map(|(_, commit)| *commit)?;
        let covered: Vec<u64> = self.unacked.range(..=seq).map(|(raw, _)| *raw).collect();
        for raw in covered {
            self.unacked.remove(&raw);
        }
        Some(commit)
    }

    fn ready_response(&self) -> OpenResponse {
        let State::Ready(session) = &self.state else {
            return Self::open_error(BridgeError::Internal {
                site: "state".to_owned(),
            });
        };
        OpenResponse::Ready {
            account: session.account().0,
        }
    }

    fn ready_result(&self) -> Response {
        let State::Ready(session) = &self.state else {
            return Self::internal("state");
        };
        Response::Ok {
            result: Box::new(OpResult::Ready(ReadyResult {
                account: session.account().0,
            })),
        }
    }

    fn drain_inner(&mut self) -> DrainResponse {
        let outcome = match &mut self.state {
            State::Fresh => return Self::drain_error(BridgeError::NotOpen),
            State::AwaitingProof(_) => return Self::drain_error(BridgeError::AwaitingProof),
            State::Suspended(_) => return Self::drain_error(BridgeError::Busy),
            State::Closed => return Self::drain_error(BridgeError::Closed),
            State::Poisoned => return Self::drain_error(BridgeError::Poisoned),
            State::Ready(session) => session.drain_events(),
        };
        if self.flush().is_err() {
            return Self::drain_error(BridgeError::HostFailed);
        }
        match outcome {
            Ok(events) => DrainResponse::Events {
                events: events.into_iter().map(Into::into).collect(),
            },
            Err(error) => Self::drain_error(session_error(&error)),
        }
    }

    fn internal(site: &'static str) -> Response {
        Self::error_response(BridgeError::Internal {
            site: site.to_owned(),
        })
    }

    fn open_error(error: BridgeError) -> OpenResponse {
        OpenResponse::Error {
            error: Box::new(error),
        }
    }

    fn error_response(error: BridgeError) -> Response {
        Response::Error {
            error: Box::new(error),
        }
    }

    fn drain_error(error: BridgeError) -> DrainResponse {
        DrainResponse::Error {
            error: Box::new(error),
        }
    }
}

/// `report_publish` as an operation future.
fn op_report_publish(args: ReportPublishRequest) -> impl FnOnce(Box<Session>) -> OpFuture {
    let id = core::OutboundId(args.id);
    let results: Vec<core::RelayResult> = args
        .results
        .into_iter()
        .map(|result| core::RelayResult {
            relay: result.relay,
            status: result.status.into(),
        })
        .collect();
    move |mut session| {
        Box::pin(async move {
            let resolution = session.report_publish(&id, &results).await;
            (
                session,
                resolution.map(|resolution| {
                    OpResult::PublishResolution(PublishResolutionResult {
                        resolution: resolution.into(),
                    })
                }),
            )
        })
    }
}

/// `ensure_key_package` as an operation future: one lifecycle pass over
/// the host's NIP-65 write set, answered with the slot's standing.
fn op_ensure_key_package(args: EnsureKeyPackageRequest) -> impl FnOnce(Box<Session>) -> OpFuture {
    move |mut session| {
        Box::pin(async move {
            let status = session.ensure_key_package(&args.write_relays).await;
            (
                session,
                status.map(|status| OpResult::KeyPackageStatus(status.into())),
            )
        })
    }
}

/// `sign_out` as an operation future: one sign-out pass, the same
/// suspension contract as `ensure_key_package`.
fn op_sign_out(args: SignOutRequest) -> impl FnOnce(Box<Session>) -> OpFuture {
    move |mut session| {
        Box::pin(async move {
            let report = session.sign_out(&args.write_relays).await;
            (
                session,
                report.map(|report| OpResult::SignOut(report.into())),
            )
        })
    }
}

/// `publish_inbox_relays` as an operation future.
fn op_publish_inbox_relays(
    args: PublishInboxRelaysRequest,
) -> impl FnOnce(Box<Session>) -> OpFuture {
    move |mut session| {
        Box::pin(async move {
            let id = session.publish_inbox_relays(&args.relays).await;
            (
                session,
                id.map(|id| OpResult::InboxRelays(InboxRelaysResult { outbound_id: id.0 })),
            )
        })
    }
}

/// `create_group` as an operation future (args already converted).
fn op_create_group(session: Box<Session>, req: core::CreateGroup) -> OpFuture {
    let mut session = session;
    Box::pin(async move {
        let created = session.create_group(req).await;
        let snapshot = created.and_then(|group| {
            session
                .group(&group)?
                .ok_or(SessionError::Internal("group snapshot"))
        });
        (
            session,
            snapshot.map(|snapshot| {
                OpResult::GroupCreated(GroupCreatedResult {
                    group: Box::new(snapshot.into()),
                })
            }),
        )
    })
}

/// `invite` as an operation future.
fn op_invite(args: InviteRequest) -> impl FnOnce(Box<Session>) -> OpFuture {
    let group = core::GroupId(args.group);
    let invitees: Vec<core::Invitee> = args.invitees.into_iter().map(core::Invitee::from).collect();
    op_send(move |session| Box::pin(async move { session.invite(&group, invitees).await }))
}

/// `confirm_rejoin` as an operation future.
fn op_confirm_rejoin(args: ConfirmRejoinRequest) -> impl FnOnce(Box<Session>) -> OpFuture {
    let group = core::GroupId(args.group);
    let offer = core::EventId(args.offer);
    move |mut session| {
        Box::pin(async move {
            let result = session.confirm_rejoin(&group, &offer, &args.token).await;
            (session, result.map(|()| OpResult::Done))
        })
    }
}

/// `readd_member` as an operation future.
fn op_readd_member(args: ReaddMemberRequest) -> impl FnOnce(Box<Session>) -> OpFuture {
    let group = core::GroupId(args.group);
    let account = core::AccountId(args.account);
    let invitees: Vec<core::Invitee> = args.invitees.into_iter().map(core::Invitee::from).collect();
    move |mut session| {
        Box::pin(async move {
            let result = session.readd_member(&group, account, invitees).await;
            (
                session,
                result.map(|outcome| {
                    OpResult::Readd(ReaddResult {
                        outcome: outcome.into(),
                    })
                }),
            )
        })
    }
}

/// `ingest` as an operation future.
fn op_ingest(args: IngestRequest) -> impl FnOnce(Box<Session>) -> OpFuture {
    let events: Vec<core::Event> = args.events.into_iter().map(Into::into).collect();
    move |mut session| {
        Box::pin(async move {
            let reports = session.ingest_batch(events).await;
            (
                session,
                reports.map(|reports| {
                    OpResult::Ingested(IngestedResult {
                        reports: reports.into_iter().map(Into::into).collect(),
                    })
                }),
            )
        })
    }
}

/// `tick` as an operation future.
fn op_tick() -> impl FnOnce(Box<Session>) -> OpFuture {
    |mut session| {
        Box::pin(async move {
            let report = session.tick().await;
            (
                session,
                report.map(|report| {
                    OpResult::Ticked(TickedResult {
                        next_wake_ms: report.next_wake_ms,
                    })
                }),
            )
        })
    }
}

/// Wrap a `SendOutcome`-returning session method as an operation future.
///
/// The method's returned future borrows the session mutably, so callers box
/// it over an `async move` that also moves owned arguments in (the borrow
/// would otherwise escape the closure environment). The wrapper drives it
/// inside an `async move` that owns and returns the `Box<Session>`.
fn op_send(
    call: impl for<'a> FnOnce(
        &'a mut Session,
    ) -> Pin<
        Box<dyn Future<Output = Result<core::SendOutcome, SessionError>> + 'a>,
    > + 'static,
) -> impl FnOnce(Box<Session>) -> OpFuture {
    move |mut session| {
        Box::pin(async move {
            let outcome = call(&mut session).await;
            (
                session,
                outcome.map(|outcome| {
                    OpResult::SendOutcome(SendOutcomeResult {
                        outcome: outcome.into(),
                    })
                }),
            )
        })
    }
}

/// Wrap a `SentMessage`-returning session method as an operation future,
/// answering `message_sent` (the same borrow discipline as [`op_send`]).
fn op_sent(
    call: impl for<'a> FnOnce(
        &'a mut Session,
    ) -> Pin<
        Box<dyn Future<Output = Result<core::SentMessage, SessionError>> + 'a>,
    > + 'static,
) -> impl FnOnce(Box<Session>) -> OpFuture {
    move |mut session| {
        Box::pin(async move {
            let sent = call(&mut session).await;
            (
                session,
                sent.map(|sent| {
                    OpResult::MessageSent(MessageSentResult {
                        outcome: sent.outcome.into(),
                        message: sent.message.into(),
                    })
                }),
            )
        })
    }
}

/// Whether a reply's result kind fits the pending operation's kind.
const fn reply_fits(op: &crate::protocol::SignerOp, result: &SignerResult) -> bool {
    match result {
        SignerResult::Signed { .. } => {
            matches!(op, crate::protocol::SignerOp::SignEvent { .. })
        }
        SignerResult::Text { .. } => matches!(
            op,
            crate::protocol::SignerOp::Nip44Encrypt { .. }
                | crate::protocol::SignerOp::Nip44Decrypt { .. }
        ),
        SignerResult::Refused | SignerResult::Failed => true,
        // Only a `nip44_decrypt` has a payload the signer can report
        // undecryptable; on `sign_event`/`nip44_encrypt` it is nonsense.
        SignerResult::InvalidPayload => {
            matches!(op, crate::protocol::SignerOp::Nip44Decrypt { .. })
        }
    }
}

/// `remove_members` as an operation future.
fn op_remove_members(args: RemoveMembersRequest) -> impl FnOnce(Box<Session>) -> OpFuture {
    let members: Vec<core::AccountId> =
        args.members.iter().map(|id| core::AccountId(*id)).collect();
    let group = core::GroupId(args.group);
    op_send(move |session| Box::pin(async move { session.remove_members(&group, &members).await }))
}

/// `leave` as an operation future.
fn op_leave(args: LeaveRequest) -> impl FnOnce(Box<Session>) -> OpFuture {
    let group = core::GroupId(args.group);
    op_send(move |session| Box::pin(async move { session.leave(&group).await }))
}

/// `self_update` as an operation future.
fn op_self_update(args: SelfUpdateRequest) -> impl FnOnce(Box<Session>) -> OpFuture {
    let group = core::GroupId(args.group);
    op_send(move |session| Box::pin(async move { session.self_update(&group).await }))
}

/// `update_group_profile` as an operation future.
fn op_update_group_profile(
    args: UpdateGroupProfileRequest,
) -> impl FnOnce(Box<Session>) -> OpFuture {
    let group = core::GroupId(args.group);
    op_send(move |session| {
        Box::pin(async move {
            session
                .update_group_profile(&group, args.name, args.description)
                .await
        })
    })
}

/// `update_app_components` as an operation future.
fn op_update_app_components(
    args: UpdateAppComponentsRequest,
) -> impl FnOnce(Box<Session>) -> OpFuture {
    let group = core::GroupId(args.group);
    let updates: Vec<core::ComponentUpdate> = args
        .updates
        .into_iter()
        .map(core::ComponentUpdate::from)
        .collect();
    op_send(move |session| {
        Box::pin(async move { session.update_app_components(&group, updates).await })
    })
}

/// `send_message` as an operation future.
fn op_send_message(args: SendMessageRequest) -> impl FnOnce(Box<Session>) -> OpFuture {
    let group = core::GroupId(args.group);
    let message: core::AppMessage = args.message.into();
    op_sent(move |session| Box::pin(async move { session.send_message(&group, message).await }))
}

/// `send_intent` as an operation future.
fn op_send_intent(args: SendIntentRequest) -> impl FnOnce(Box<Session>) -> OpFuture {
    let group = core::GroupId(args.group);
    let intent: core::MessageIntent = args.intent.into();
    op_sent(move |session| Box::pin(async move { session.send_intent(&group, intent).await }))
}

/// `set_admin` as an operation future.
fn op_set_admin(args: SetAdminRequest) -> impl FnOnce(Box<Session>) -> OpFuture {
    let group = core::GroupId(args.group);
    let account = core::AccountId(args.account);
    let admin = args.admin;
    op_send(move |session| Box::pin(async move { session.set_admin(&group, account, admin).await }))
}

/// `set_group_avatar` as an operation future.
fn op_set_group_avatar(args: SetGroupAvatarRequest) -> impl FnOnce(Box<Session>) -> OpFuture {
    let group = core::GroupId(args.group);
    let avatar = args.avatar.map(core::GroupAvatar::from);
    op_send(move |session| Box::pin(async move { session.set_group_avatar(&group, avatar).await }))
}

/// `set_retention` as an operation future.
fn op_set_retention(args: SetRetentionRequest) -> impl FnOnce(Box<Session>) -> OpFuture {
    let group = core::GroupId(args.group);
    let seconds = args.seconds;
    op_send(move |session| Box::pin(async move { session.set_retention(&group, seconds).await }))
}

/// `disband` as an operation future.
fn op_disband(args: DisbandRequest) -> impl FnOnce(Box<Session>) -> OpFuture {
    let group = core::GroupId(args.group);
    op_send(move |session| Box::pin(async move { session.disband(&group).await }))
}

/// `enable_disbanding` as an operation future.
fn op_enable_disbanding(args: EnableDisbandingRequest) -> impl FnOnce(Box<Session>) -> OpFuture {
    let group = core::GroupId(args.group);
    op_send(move |session| Box::pin(async move { session.enable_disbanding(&group).await }))
}

/// Validate one signer reply against the pending set.
fn reply_error(
    state: &SignerState,
    reply: &SignerReply,
    seen: &mut HashSet<u32>,
) -> Option<BridgeError> {
    if !state.awaitable(reply.id) || !seen.insert(reply.id) {
        return Some(BridgeError::UnknownSignerRequest);
    }
    let fits = state
        .pending
        .get(&reply.id)
        .is_none_or(|op| reply_fits(op, &reply.result));
    if fits { None } else { Some(invalid("value")) }
}

/// `create_group` argument conversion.
fn op_create_group_args(args: CreateGroupRequest) -> impl FnOnce(Box<Session>) -> OpFuture {
    let req = core::CreateGroup {
        name: args.name,
        description: args.description,
        relays: args.relays,
        invitees: args.invitees.into_iter().map(core::Invitee::from).collect(),
        own_devices: args
            .own_devices
            .into_iter()
            .map(core::Invitee::from)
            .collect(),
        admins: args.admins.iter().map(|id| core::AccountId(*id)).collect(),
        components: args
            .components
            .into_iter()
            .map(core::ComponentUpdate::from)
            .collect(),
    };
    move |session| op_create_group(session, req)
}
