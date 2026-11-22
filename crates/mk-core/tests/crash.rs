//! Crash-consistency tests: the nine restart boundaries of
//! `3rdparty/marmot/foundation/conformance.md` ("Crash and restart
//! scenarios") backed by `3rdparty/marmot/protocol-core/durability.md`.
//!
//! A deterministic step driver runs a flow against `N` members over the
//! in-process `FakeRelay`. Every external side effect goes through the
//! public `Session` API: publishing only happens for `take_outbound` items
//! after the durability gate, and the driver acks only the seq the session
//! itself reports via `awaiting_durability` (the `RecordingHost` applied it
//! synchronously).
//!
//! For each flow the driver first runs an uninterrupted reference,
//! recording how many host batches the member under test emitted during
//! the flow (`N`; session bootstrap during `open` is setup, not part of
//! the flow). Then for every `k` in `1..=N` and both crash variants it
//! replays the flow:
//!
//! - `Drop`: `RecordingHost::fail_after(setup + k - 1)` — flow batch `k`
//!   and everything after is never durable. The process is cut right after
//!   the call that emitted batch `k` returns (the probe checks the
//!   attempted-batch count between every session call); the session is
//!   dropped and nothing it computed after the cut is observed — no
//!   publish, no ack.
//! - `Durable`: `fail_after(setup + k)` — batch `k` is durable but the
//!   process dies before the ack reaches the session (reopened sessions
//!   start `acked` at 0 and re-release durable obligations anyway).
//!
//! After a cut the member reopens from `RecordingHost::durable_rows` —
//! `reopen_member` asserts the two-phase open does not ask for a fresh
//! account proof — the remaining steps run, then every member drains to
//! quiescence. The restarted world is compared with the uninterrupted
//! reference at complete observer boundaries: per member and group the
//! lifecycle state, epoch, member/admin sets and profile; the net
//! application-payload multiset after invalidations; exactly-once
//! effective delivery by transport id (an acked id must never be
//! re-delivered); zero outstanding obligations; and the same count of
//! distinct authored events, which catches a replacement Commit — a retry
//! republishes the identical event id and is deduplicated.
//!
//! Boundary map (see the report table for the same mapping):
//!
//! - 1 prepared-not-published: cuts landing inside `Create`/`Send`/
//!   `Invite`/`Leave` staging, before the next `Flush` — covered by the
//!   `Drop` variant of every staging batch.
//! - 2 acknowledgement-uncertain: `Drop` cuts at the `report_publish`
//!   resolution batch — the relay already accepted the bytes; the durable
//!   state keeps one unresolved obligation that must be retried
//!   byte-identically (authored-count equality) and applied once.
//! - 3 confirmed-not-applied and 7 selected-branch-applying: the engine
//!   performs the multi-part apply inside one storage unit, so there is
//!   no intermediate commit to cut at — `apply_is_one_commit` counts
//!   batches per applying call to show it; every reopened durable prefix
//!   yields a complete prior-or-selected projection.
//! - 4 applied-derived-work-incomplete: `Durable` cuts at the resolution
//!   batch — unobserved effects (deferred Welcomes, events) re-emit with
//!   stable identities and no acked message is delivered twice.
//! - 5 collecting and 6 frozen-resolving-batch: convergence input
//!   collected across `Deliver`/`Tick` boundaries is cut at every emitted
//!   batch; settlement after restart produces the same canonical
//!   snapshot.
//! - 8 missing-or-corrupt material: `missing_selection_material`
//!   deletes one row family from the durable rows before reopen.
//! - 9 local gates / terminal effects: the leave flow cuts while
//!   `Leaving` (admin step-down-and-leave) and the remove flow cuts while
//!   the removed member realizes its local-leaf removal. Disband is not
//!   in the session API (M5) — there is nothing to interrupt.
//! - 10 KeyPackage staging: cuts inside `ensure_key_package`'s
//!   `stage_key_package_replacement` transaction — either the whole
//!   pending row (new bundle + lifecycle record) is durable or none of
//!   it is; a reopen never observes a half-staged replacement. Covered
//!   by both cut variants at every `KeyPackage` step batch.
//! - 11 signed-not-published: cuts after the signed KeyPackage outbound
//!   batch — the durable rows re-release the identical signed event, so
//!   the authored-event multiset stays byte-identical.
//! - 12 acknowledgement-uncertain promotion: cuts at the
//!   `report_publish` batch that atomically promotes the pending
//!   KeyPackage — a reopen re-derives the promotable state from the
//!   durable row and the resumed report promotes exactly once.
//! - 13 promotion atomicity: the report row update and
//!   `promote_key_package_lifecycle` commit in one `transact`, so no cut
//!   exposes a mixed old/new `current` — pinned by the `key_package`
//!   observer projection at every boundary.
//! - 14 consumed-not-replaced: cuts after a Welcome join commit is
//!   durable but before the next `Tick` staged the replacement — the
//!   reopened session re-reads the `last_consumed_key_package_ref`
//!   marker and the resumed `Tick` stages the same-slot replacement.
//! - 18 sign-out withdrawal: cuts inside the `sign_out` batch — the
//!   kind-5 staging, the deletion of every `KeyPackage` bundle and the
//!   `slot_withdrawn_at` write commit in one `transact`, so a reopen
//!   either keeps the whole live slot or the fully withdrawn one; a cut
//!   after the batch still owes the same signed kind-5 from the durable
//!   outbound row.
//! - 15 self-update jitter: the `Tick` entering `Jitter` persists
//!   `not_before` — a cut either side of that batch reopens to the same
//!   deadline; the sampled jitter is never resampled.
//! - 16 self-update published-not-confirmed: cuts after the self-update
//!   fanout's publish batches but before the confirm is observed reopen
//!   to the open `PendingPublication` obligation; the identical durable
//!   commit confirms and the obligation completes exactly once —
//!   authored-event equality pins that no second self-update Commit was
//!   authored.
//!
//! mdk#3524 (fixed by mdk#3531, v0.18.0): at the leaving-flow cuts where
//! the step-down's staging is durable but its fanout is not, the reopened
//! leaver still keeps the never-published step-down's `Sent` row and
//! re-drives and confirms a byte-identical step-down — but `Sent` is no
//! longer a convergence commit input (`OPENMLS_COMMIT_INPUT_STATES`
//! excludes it), so the next pass no longer takes the stale row as a
//! branch, never invalidates the confirmed copy, and never re-issues a
//! step-down whose confirm would collide on the first confirm's
//! checkpoint (`AlreadyExists`, blobs differing only in `OpenMLS`
//! `added_at`). `tests/upstream.rs` pins the fixed behaviour on
//! `Engine<SqliteAccountStorage>`; a `ConfirmConflict` from
//! `report_publish` is a test failure again, obligations must clear, and
//! no divergent terminal state is excused.

#![allow(
    clippy::missing_docs_in_private_items,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::needless_pass_by_value,
    clippy::tests_outside_test_module,
    clippy::shadow_reuse,
    clippy::shadow_unrelated,
    clippy::doc_markdown,
    clippy::too_many_lines,
    clippy::excessive_nesting,
    clippy::cast_possible_truncation,
    clippy::print_stderr,
    clippy::significant_drop_tightening,
    reason = "test code"
)]

use mk_core as _;
use tokio as _;

use base64 as _;
use cgka_engine as _;
use cgka_traits as _;
use chacha20poly1305 as _;
use getrandom as _;
use hex as _;
use hkdf as _;
use nostr as _;
use openmls as _;
use openmls_rust_crypto as _;
use openmls_traits as _;
use proptest as _;
use serde as _;
use serde_json as _;
use sha2 as _;
use storage_sqlite as _;
use thiserror as _;
use transport_nostr_peeler as _;
use url as _;
use zeroize as _;

#[cfg(feature = "testing")]
mod support;

#[cfg(feature = "testing")]
mod crash {
    use std::collections::{BTreeMap, BTreeSet};

    use mk_core::{
        AppMessage, CreateGroup, EventId, GroupId, GroupSnapshot, GroupState, Invitee, Opened,
        RelayResult, RelayStatus, Session, SessionEvent,
    };

    use super::support::*;

    const RELAY: &str = "ws://127.0.0.1:7777";

    /// One driver action. Member indices are stable labels.
    #[derive(Clone, Debug)]
    enum Step {
        /// `ensure_key_package` — one staging call; `Flush` publishes.
        KeyPackage(usize),
        /// `publish_inbox_relays` — one staging call; `Flush` publishes.
        InboxRelays(usize),
        /// `create_group(name, invitees, admin indices)`.
        Create(usize, &'static str, &'static [usize], &'static [usize]),
        /// `invite(member)` — the invitee must have identity published.
        Invite(usize, usize),
        /// `remove_members([member])`.
        Remove(usize, usize),
        /// `leave()`.
        Leave(usize),
        /// `sign_out(write_relays)` — one atomic withdrawal; `Flush`
        /// publishes the kind-5.
        SignOut(usize),
        /// `history_fetched(group)` — records the post-join fetch's EOSE.
        HistoryFetched(usize),
        /// Advance the injected clock by `ms` — no session call, so it
        /// emits no cuttable batch.
        Advance(usize, u64),
        /// `self_update()`.
        SelfUpdate(usize),
        /// `update_group_profile(name, description)`.
        UpdateProfile(usize, &'static str, &'static str),
        /// `send_message(text)` — advances the clock 1 s for ordering.
        Send(usize, &'static str),
        /// Fetch welcomes + group backlog, `ingest_batch`, drain, ack.
        Deliver(usize),
        /// `tick` then advance the clock so cutoffs come due.
        Tick(usize),
        /// Ack the gate, publish + report everything until empty.
        Flush(usize),
        /// Publish nothing; report every pending outbound all-failed.
        Reject(usize),
        /// `Deliver`+`Tick`+`Flush` rounds until the member settles.
        Settle(usize),
    }

    /// Which crash variant a cut run injects.
    #[derive(Clone, Copy)]
    enum CutVariant {
        /// Flow batches `k..` are never durable.
        Drop,
        /// Flow batch `k` is durable; the process dies before the ack.
        Durable,
    }

    #[derive(Clone, Copy)]
    struct Cut {
        member: usize,
        k: usize,
        variant: CutVariant,
    }

    /// Observable world state, collected across the whole run.
    struct World {
        relay: FakeRelay,
        members: Vec<Member>,
        gid: Option<GroupId>,
        /// Transport id → payload content for every `MessageReceived`
        /// effect this member observed, across incarnations.
        delivered: Vec<BTreeMap<EventId, String>>,
        /// Transport ids withdrawn by `MessageInvalidated`.
        invalidated: Vec<BTreeSet<EventId>>,
        /// Transport ids the app acknowledged in the current incarnation —
        /// a lost ack batch legitimately re-delivers after a restart, so
        /// restart clears this for the re-delivery check only.
        acked: Vec<BTreeSet<EventId>>,
        /// `(kind, purpose, recipient)` of every event this member
        /// published, deduplicated by event id. `recipient` is the event's
        /// `p` tag for addressed artifacts (Welcomes), empty otherwise.
        authored: Vec<BTreeMap<EventId, (u16, &'static str, String)>>,
    }

    /// The comparable observer projection of a finished run.
    #[derive(Debug)]
    struct Observed {
        members: Vec<MemberObs>,
        /// Per-batch table bytes on the measured member's host
        /// (diagnostics only).
        batches: Vec<BTreeMap<u8, usize>>,
    }

    #[derive(Debug)]
    struct MemberObs {
        groups: Vec<GroupObs>,
        /// Payload content multiset net of invalidations.
        net_payloads: BTreeMap<String, usize>,
        /// Published `(kind, purpose, recipient-index)` tuples — the
        /// multiset used to derive which artifacts a cut run never
        /// emitted. `usize::MAX` marks an unaddressed artifact.
        authored_events: BTreeSet<(u16, &'static str, usize)>,
        /// No outstanding obligations (`awaiting_durability` and
        /// `take_outbound` both empty).
        obligations_clear: bool,
        /// `key_package_status` projection — `(current.is_some(),
        /// pending, retained)`; `None` when the lifecycle row is
        /// unreadable. Event ids are not comparable across runs, only
        /// the lifecycle shape is.
        key_package: Option<(bool, bool, u32)>,
    }

    #[derive(PartialEq, Debug)]
    struct GroupObs {
        state: GroupState,
        epoch: u64,
        members: BTreeSet<usize>,
        admins: BTreeSet<usize>,
        name: String,
        description: String,
    }

    /// The artifact's addressed account — a Welcome's `p` tag; empty for
    /// group-feed events.
    fn p_tag(event: &mk_core::Event) -> String {
        event
            .tags
            .iter()
            .find(|tag| tag.first().map(String::as_str) == Some("p"))
            .and_then(|tag| tag.get(1))
            .cloned()
            .unwrap_or_default()
    }

    const fn op_key(op: &mk_core::KvOp) -> &Vec<u8> {
        match op {
            mk_core::KvOp::Put { key, .. } | mk_core::KvOp::Delete { key } => key,
        }
    }

    const fn purpose_name(purpose: mk_core::OutboundPurpose) -> &'static str {
        match purpose {
            mk_core::OutboundPurpose::ApplicationMessage => "app",
            mk_core::OutboundPurpose::Proposal => "proposal",
            mk_core::OutboundPurpose::Commit => "commit",
            mk_core::OutboundPurpose::Welcome => "welcome",
            mk_core::OutboundPurpose::KeyPackage => "key-package",
            mk_core::OutboundPurpose::KeyPackageRepublish => "key-package-republish",
            mk_core::OutboundPurpose::InboxRelays => "inbox-relays",
            _ => "other",
        }
    }

    struct Runner {
        world: World,
        cut: Option<Cut>,
        /// Batches on the cut member's host when the fault was armed —
        /// session bootstrap is setup, not flow work.
        base: usize,
        halted: bool,
        /// Flow step index the crash interrupted, for the alternative
        /// (abandonment) reference.
        halt_step: Option<usize>,
        /// Outbound obligations the restarted session surfaced from
        /// durable rows before the flow resumed.
        restart_obligations: usize,
        /// `(publisher, purpose, recipient-index)` tuples this run never
        /// publishes — models staged artifacts whose rows never became
        /// durable.
        suppress: BTreeSet<(usize, &'static str, usize)>,
        /// Flow step index whose staged artifacts are resolved as
        /// published but never reach the wire — models "canonical write
        /// durable, the step's staged obligation rows not".
        withhold_step: Option<usize>,
    }

    impl Runner {
        /// Stop the run right after the session call that emitted the
        /// cut member's `k`th flow batch. Called between every session
        /// call, including inside compound steps.
        fn probe(&mut self, member: usize) -> bool {
            if self.halted {
                return true;
            }
            let Some(cut) = self.cut else {
                return false;
            };
            if cut.member == member
                && self.world.members[member].host.batches().len() >= self.base + cut.k
            {
                self.halted = true;
            }
            self.halted
        }

        /// The group id if `member` durably has the group — a member
        /// cannot act on a group it never durably joined.
        fn gid_for(&self, member: usize) -> Option<GroupId> {
            let gid = self.world.gid.clone()?;
            self.world.members[member]
                .session
                .group(&gid)
                .ok()
                .flatten()
                .map(|_| gid)
        }

        /// Report one publish verdict set. Any error is a test failure —
        /// `ConfirmConflict` remains mapped for the retained-checkpoint
        /// reconfirm collision (mdk#3525) but no longer has a reachable
        /// route in this matrix since mdk#3531.
        async fn report(
            &mut self,
            member: usize,
            id: &mk_core::OutboundId,
            results: &[RelayResult],
        ) {
            self.world.members[member]
                .session
                .report_publish(id, results)
                .await
                .unwrap_or_else(|error| panic!("report_publish: {error}"));
        }

        /// Drain session events into the effect ledgers.
        fn drain(&mut self, member: usize) {
            for event in self.world.members[member].session.drain_events().unwrap() {
                match event {
                    SessionEvent::MessageReceived { message, .. } => {
                        assert!(
                            !self.world.acked[member].contains(&message.transport_id),
                            "member {member} re-delivered acked message {}",
                            message.transport_id.to_hex()
                        );
                        self.world.delivered[member].insert(message.transport_id, message.content);
                    }
                    SessionEvent::MessageInvalidated { message, .. } => {
                        self.world.invalidated[member].insert(message);
                    }
                    _ => {}
                }
            }
        }

        /// Resolve every staged obligation as relay-accepted without
        /// putting it on the wire — the observable twin of staged rows
        /// that died before reaching durable storage.
        async fn withhold(&mut self, member: usize) {
            let session = &mut self.world.members[member].session;
            if let Some(seq) = session.awaiting_durability() {
                session.acknowledge_durable(seq);
            }
            for item in session.take_outbound().expect("take_outbound") {
                let results: Vec<RelayResult> = item
                    .relays
                    .iter()
                    .map(|relay| RelayResult {
                        relay: relay.clone(),
                        status: RelayStatus::Accepted,
                    })
                    .collect();
                self.report(member, &item.id, &results).await;
                self.drain(member);
            }
        }

        /// Whether this run withholds `item` from the wire (the staged
        /// artifact it would publish was never durable).
        fn suppressed(&self, member: usize, item: &mk_core::Outbound) -> bool {
            let recipient = self
                .world
                .members
                .iter()
                .position(|member| member.account_hex() == p_tag(&item.event))
                .unwrap_or(usize::MAX);
            self.suppress
                .contains(&(member, purpose_name(item.purpose), recipient))
        }

        /// Ack the gate, publish + report until `take_outbound` is empty.
        async fn flush(&mut self, member: usize) {
            for _ in 0..64 {
                let session = &mut self.world.members[member].session;
                if let Some(seq) = session.awaiting_durability() {
                    session.acknowledge_durable(seq);
                }
                let outbound = session.take_outbound().expect("take_outbound");
                if outbound.is_empty() {
                    break;
                }
                for item in outbound {
                    if self.suppressed(member, &item) {
                        continue;
                    }
                    let results = self.world.relay.publish(&item);
                    self.world.authored[member].insert(
                        item.event.id,
                        (
                            item.event.kind,
                            purpose_name(item.purpose),
                            p_tag(&item.event),
                        ),
                    );
                    eprintln!(
                        "  report member {member} {} {} {}",
                        item.event.kind,
                        purpose_name(item.purpose),
                        item.id.to_hex()
                    );
                    self.report(member, &item.id, &results).await;
                    self.drain(member);
                    if self.probe(member) {
                        return;
                    }
                }
            }
        }

        /// Report every pending outbound all-failed without publishing.
        async fn reject(&mut self, member: usize) {
            let session = &mut self.world.members[member].session;
            if let Some(seq) = session.awaiting_durability() {
                session.acknowledge_durable(seq);
            }
            for item in session.take_outbound().expect("take_outbound") {
                let results: Vec<RelayResult> = item
                    .relays
                    .iter()
                    .map(|relay| RelayResult {
                        relay: relay.clone(),
                        status: RelayStatus::Rejected,
                    })
                    .collect();
                self.report(member, &item.id, &results).await;
                self.drain(member);
                if self.probe(member) {
                    return;
                }
            }
        }

        /// Fetch welcomes + the group backlog, ingest, drain, ack.
        async fn deliver(&mut self, member: usize) {
            let mut events = welcomes_for(
                &self.world.relay,
                RELAY,
                &self.world.members[member].account_hex(),
            );
            if let Some(gid) = &self.world.gid
                && let Ok(Some(snapshot)) = self.world.members[member].session.group(gid)
            {
                events.extend(group_events(
                    &self.world.relay,
                    RELAY,
                    &hex::encode(snapshot.nostr_group_id),
                ));
            }
            if events.is_empty() {
                return;
            }
            self.world.members[member]
                .session
                .ingest_batch(events)
                .await
                .expect("ingest_batch");
            self.drain(member);
            if self.probe(member) {
                return;
            }
            let session = &mut self.world.members[member].session;
            if let Some(seq) = session.awaiting_durability() {
                session.acknowledge_durable(seq);
            }
            // Ack the ids this drain produced — everything in the ledger
            // not yet acked.
            let fresh: Vec<EventId> = self.world.delivered[member]
                .keys()
                .copied()
                .filter(|id| !self.world.acked[member].contains(id))
                .collect();
            if !fresh.is_empty() {
                self.world.members[member]
                    .session
                    .acknowledge_messages(&fresh)
                    .expect("acknowledge_messages");
                self.world.acked[member].extend(fresh);
                self.probe(member);
            }
        }

        /// One `tick`, then advance the injected clock so time-armed
        /// convergence cutoffs come due — like a host timer.
        async fn tick(&mut self, member: usize) {
            self.world.members[member]
                .session
                .tick()
                .await
                .expect("tick");
            self.drain(member);
            if self.probe(member) {
                return;
            }
            self.world.members[member].clock.advance(2_000);
        }

        /// Deliver + tick + flush rounds until the member settles.
        async fn settle(&mut self, member: usize) {
            for _ in 0..6 {
                self.deliver(member).await;
                if self.halted {
                    return;
                }
                self.tick(member).await;
                if self.halted {
                    return;
                }
                self.flush(member).await;
                if self.halted {
                    return;
                }
            }
        }

        /// One flow step; returns early when the crash probe fired.
        async fn step(&mut self, index: usize, step: &Step) {
            let step_member = match *step {
                Step::KeyPackage(m)
                | Step::InboxRelays(m)
                | Step::Create(m, ..)
                | Step::Invite(m, ..)
                | Step::Remove(m, ..)
                | Step::Leave(m)
                | Step::SignOut(m)
                | Step::HistoryFetched(m)
                | Step::Advance(m, _)
                | Step::SelfUpdate(m)
                | Step::UpdateProfile(m, ..)
                | Step::Send(m, ..)
                | Step::Deliver(m)
                | Step::Tick(m)
                | Step::Flush(m)
                | Step::Reject(m)
                | Step::Settle(m) => m,
            };
            match *step {
                Step::KeyPackage(m) => {
                    self.world.members[m]
                        .session
                        .ensure_key_package(&[RELAY.to_owned()])
                        .await
                        .expect("key package");
                    self.drain(m);
                    self.probe(m);
                }
                Step::InboxRelays(m) => {
                    self.world.members[m]
                        .session
                        .publish_inbox_relays(&[RELAY.to_owned()])
                        .await
                        .expect("inbox relays");
                    self.drain(m);
                    self.probe(m);
                }
                Step::Create(m, name, invitees, admins) => {
                    if self.world.gid.is_some() {
                        return;
                    }
                    // An invitee whose KeyPackage or inbox-relay list was
                    // never published cannot be invited — the call itself
                    // cannot be made.
                    let invites: Option<Vec<Invitee>> = invitees
                        .iter()
                        .map(|&i| {
                            let account = self.world.members[i].account_hex();
                            let inbox_relays = inbox_relays_of(&self.world.relay, RELAY, &account);
                            if inbox_relays.is_empty() {
                                return None;
                            }
                            Some(Invitee {
                                key_package: maybe_key_package_of(
                                    &self.world.relay,
                                    RELAY,
                                    &account,
                                )?,
                                inbox_relays,
                            })
                        })
                        .collect();
                    let Some(invites) = invites else {
                        return;
                    };
                    let admin_accounts = admins
                        .iter()
                        .map(|&i| self.world.members[i].signer.account())
                        .collect();
                    let gid = self.world.members[m]
                        .session
                        .create_group(CreateGroup {
                            name: name.to_owned(),
                            description: String::new(),
                            relays: vec![RELAY.to_owned()],
                            invitees: invites,
                            admins: admin_accounts,
                            components: Vec::new(),
                        })
                        .await
                        .expect("create_group");
                    self.world.gid = Some(gid);
                    self.drain(m);
                    self.probe(m);
                }
                Step::Invite(m, j) => {
                    let Some(gid) = self.gid_for(m) else {
                        return;
                    };
                    let account = self.world.members[j].account_hex();
                    let inbox_relays = inbox_relays_of(&self.world.relay, RELAY, &account);
                    let Some(key_package) =
                        maybe_key_package_of(&self.world.relay, RELAY, &account)
                            .filter(|_| !inbox_relays.is_empty())
                    else {
                        return;
                    };
                    let invitee = Invitee {
                        key_package,
                        inbox_relays,
                    };
                    self.world.members[m]
                        .session
                        .invite(&gid, vec![invitee])
                        .await
                        .expect("invite");
                    self.drain(m);
                    self.probe(m);
                }
                Step::Remove(m, j) => {
                    let Some(gid) = self.gid_for(m) else {
                        return;
                    };
                    let account = self.world.members[j].signer.account();
                    self.world.members[m]
                        .session
                        .remove_members(&gid, &[account])
                        .await
                        .expect("remove_members");
                    self.drain(m);
                    self.probe(m);
                }
                Step::Leave(m) => {
                    let Some(gid) = self.gid_for(m) else {
                        return;
                    };
                    self.world.members[m]
                        .session
                        .leave(&gid)
                        .await
                        .expect("leave");
                    self.drain(m);
                    self.probe(m);
                }
                Step::SignOut(m) => {
                    self.world.members[m]
                        .session
                        .sign_out(&[RELAY.to_owned()])
                        .await
                        .expect("sign_out");
                    self.drain(m);
                    self.probe(m);
                }
                Step::HistoryFetched(m) => {
                    let Some(gid) = self.gid_for(m) else {
                        return;
                    };
                    self.world.members[m]
                        .session
                        .history_fetched(&gid)
                        .expect("history_fetched");
                    self.drain(m);
                    self.probe(m);
                }
                Step::Advance(m, ms) => {
                    self.world.members[m].clock.advance(ms);
                }
                Step::SelfUpdate(m) => {
                    let Some(gid) = self.gid_for(m) else {
                        return;
                    };
                    self.world.members[m]
                        .session
                        .self_update(&gid)
                        .await
                        .expect("self_update");
                    self.drain(m);
                    self.probe(m);
                }
                Step::UpdateProfile(m, name, description) => {
                    let Some(gid) = self.gid_for(m) else {
                        return;
                    };
                    self.world.members[m]
                        .session
                        .update_group_profile(
                            &gid,
                            Some(name.to_owned()),
                            Some(description.to_owned()),
                        )
                        .await
                        .expect("update_group_profile");
                    self.drain(m);
                    self.probe(m);
                }
                Step::Send(m, text) => {
                    let Some(gid) = self.gid_for(m) else {
                        return;
                    };
                    self.world.members[m].clock.advance(1_000);
                    self.world.members[m]
                        .session
                        .send_message(
                            &gid,
                            AppMessage {
                                kind: 9,
                                tags: Vec::new(),
                                content: text.to_owned(),
                            },
                        )
                        .await
                        .expect("send_message");
                    self.drain(m);
                    self.probe(m);
                }
                Step::Deliver(m) => self.deliver(m).await,
                Step::Tick(m) => self.tick(m).await,
                Step::Flush(m) => self.flush(m).await,
                Step::Reject(m) => self.reject(m).await,
                Step::Settle(m) => self.settle(m).await,
            }
            if !self.halted && self.withhold_step == Some(index) {
                self.withhold(step_member).await;
            }
        }

        /// Post-cut restart: durable rows only, then continue. Disarms
        /// the cut — a crash happens once per run.
        async fn restart(&mut self, member: usize) {
            self.world.members[member].host.resume();
            self.world.members[member] = reopen_member(&self.world.members[member]).await;
            // Ack bookkeeping is app-side state that dies with the process;
            // a lost ack batch re-delivers the message (at-least-once).
            self.world.acked[member].clear();
            self.cut = None;
            // The restart's own statement of outstanding obligations:
            // anything the durable rows still hold must be discharged for
            // the run to land on the uninterrupted reference; an empty set
            // means the interrupted step's preparation never persisted, so
            // abandoning it is legal (durability.md boundary 1).
            let released = self.world.members[member]
                .session
                .take_outbound()
                .expect("take_outbound");
            self.restart_obligations = released.len();
            for item in released {
                if self.suppressed(member, &item) {
                    continue;
                }
                let results = self.world.relay.publish(&item);
                self.world.authored[member].insert(
                    item.event.id,
                    (
                        item.event.kind,
                        purpose_name(item.purpose),
                        p_tag(&item.event),
                    ),
                );
                self.report(member, &item.id, &results).await;
                self.drain(member);
            }
            self.flush(member).await;
        }

        /// Snapshot the observer projection of the whole world.
        fn observe(&mut self) -> Observed {
            let accounts: Vec<mk_core::AccountId> = self
                .world
                .members
                .iter()
                .map(|member| member.signer.account())
                .collect();
            let label_of = |account: mk_core::AccountId| -> usize {
                accounts
                    .iter()
                    .position(|candidate| *candidate == account)
                    .unwrap_or(usize::MAX)
            };
            let account_hexes: Vec<String> =
                self.world.members.iter().map(Member::account_hex).collect();
            let account_index = |recipient: &String| -> usize {
                account_hexes
                    .iter()
                    .position(|hex| hex == recipient)
                    .unwrap_or(usize::MAX)
            };
            let mut members = Vec::new();
            for (m, member) in self.world.members.iter_mut().enumerate() {
                let mut groups: Vec<GroupObs> = member
                    .session
                    .groups()
                    .expect("groups")
                    .iter()
                    .map(|snapshot: &GroupSnapshot| GroupObs {
                        state: snapshot.state,
                        epoch: snapshot.epoch,
                        members: snapshot
                            .members
                            .iter()
                            .map(|member| label_of(member.account))
                            .collect(),
                        admins: snapshot
                            .members
                            .iter()
                            .filter(|member| member.admin)
                            .map(|member| label_of(member.account))
                            .collect(),
                        name: snapshot.name.clone(),
                        description: snapshot.description.clone(),
                    })
                    .collect();
                groups.sort_by_key(|group| group.epoch);
                let mut net_payloads: BTreeMap<String, usize> = BTreeMap::new();
                for (id, content) in &self.world.delivered[m] {
                    if !self.world.invalidated[m].contains(id) {
                        *net_payloads.entry(content.clone()).or_default() += 1;
                    }
                }
                let obligations_clear = member.session.awaiting_durability().is_none()
                    && member
                        .session
                        .take_outbound()
                        .expect("take_outbound")
                        .is_empty();
                let key_package = member
                    .session
                    .key_package_status()
                    .ok()
                    .map(|status| (status.current.is_some(), status.pending, status.retained));
                members.push(MemberObs {
                    groups,
                    net_payloads,
                    authored_events: self.world.authored[m]
                        .values()
                        .map(|(kind, name, recipient)| (*kind, *name, account_index(recipient)))
                        .collect(),
                    obligations_clear,
                    key_package,
                });
            }
            Observed {
                members,
                batches: Vec::new(),
            }
        }
    }

    /// Run `flow` for `member_count` members, optionally cutting member
    /// `measure`'s flow batch `k` (a `Cut` is always on `measure`).
    /// Returns the observer projection, the number of flow batches the
    /// measured member emitted, the interrupted step index if cut, the
    /// restart obligation count.
    async fn run(
        flow: &[Step],
        member_count: usize,
        measure: usize,
        cut: Option<Cut>,
        suppress: &BTreeSet<(usize, &'static str, usize)>,
        withhold_step: Option<usize>,
    ) -> (Observed, usize, Option<usize>, usize) {
        let relay = FakeRelay::new();
        let mut members = Vec::new();
        for _ in 0..member_count {
            members.push(open_member().await);
        }
        let world = World {
            relay,
            delivered: vec![BTreeMap::new(); member_count],
            invalidated: vec![BTreeSet::new(); member_count],
            acked: vec![BTreeSet::new(); member_count],
            authored: vec![BTreeMap::new(); member_count],
            members,
            gid: None,
        };
        let base = world.members[measure].host.batches().len();
        if std::env::var_os("CRASH_DUMP_BATCHES").is_some() {
            std::eprintln!("base={base}");
        }
        if let Some(cut) = cut {
            let keep = match cut.variant {
                CutVariant::Drop => base + cut.k - 1,
                CutVariant::Durable => base + cut.k,
            };
            world.members[cut.member].host.fail_after(keep);
        }
        let mut runner = Runner {
            world,
            cut,
            base,
            halted: false,
            halt_step: None,
            restart_obligations: 0,
            suppress: suppress.clone(),
            withhold_step,
        };

        let mut steps = flow.iter().enumerate();
        for (index, step) in steps.by_ref() {
            runner.step(index, step).await;
            if runner.halted {
                runner.halt_step = Some(index);
                runner.restart(measure).await;
                runner.halted = false;
                break;
            }
        }
        for (index, step) in steps {
            runner.step(index, step).await;
        }

        // Drain all work until quiescent.
        for _ in 0..8 {
            for member in 0..member_count {
                runner.deliver(member).await;
                runner.tick(member).await;
                runner.flush(member).await;
            }
        }

        let mut observed = runner.observe();
        for member in &runner.world.members {
            mk_core::testing::check_message_index(&member.host.durable_rows())
                .expect("message index consistent at run end");
            // C10's proof: an orphan bundle staged by a cut would go
            // unseen — every reopen+settle must hold exactly
            // `current + pending + retained` bundles.
            if let Ok(status) = member.session.key_package_status() {
                let expected = usize::from(status.current.is_some())
                    + usize::from(status.pending)
                    + status.retained as usize;
                assert_eq!(
                    mk_core::testing::key_package_bundle_count(&member.host.durable_rows()),
                    expected,
                    "bundles == current + pending + retained"
                );
            }
        }
        observed.batches = runner.world.members[measure]
            .host
            .batches()
            .iter()
            .map(|batch| {
                let mut tables: BTreeMap<u8, usize> = BTreeMap::new();
                for op in &batch.ops {
                    let key = op_key(op);
                    *tables.entry(key[0]).or_default() += 1;
                }
                tables
            })
            .collect();
        let flow_batches = runner.world.members[measure].host.batches().len() - runner.base;
        (
            observed,
            flow_batches,
            runner.halt_step,
            runner.restart_obligations,
        )
    }

    /// Observer equality against one reference world: canonical group
    /// snapshots and the delivered payload multiset. Published-artifact
    /// sets are deliberately not compared — a restart may legally
    /// abandon a staged obligation or re-drive an intent in a different
    /// wire form (proposal vs commit); the canonical outcome is what the
    /// durability contract pins (conformance.md: "compare observable
    /// state, not bytes").
    fn equivalent(a: &Observed, b: &Observed) -> bool {
        a.members.len() == b.members.len()
            && a.members.iter().zip(b.members.iter()).all(|(a, b)| {
                a.groups == b.groups
                    && a.net_payloads == b.net_payloads
                    && a.key_package == b.key_package
            })
    }

    /// Reference run once, then every `(k, variant)` cut of `cut_member`.
    /// A restarted run must equal the uninterrupted reference (the
    /// obligation survived or was retried byte-identically) — or, when
    /// the interrupted step was a pure staging call and the staging
    /// batch never became durable, the alternative reference with that
    /// step's preparation legally abandoned.
    async fn matrix(flow: &[Step], members: usize, cut_member: usize, label: &str) {
        let (reference, n, ..) = run(flow, members, cut_member, None, &BTreeSet::new(), None).await;
        assert!(n > 0, "{label}: reference emitted no flow batches");
        for k in 1..=n {
            for variant in [CutVariant::Drop, CutVariant::Durable] {
                let name = match variant {
                    CutVariant::Drop => "drop",
                    CutVariant::Durable => "durable",
                };
                let ctx = format!("{label} member {cut_member} batch {k}/{n} ({name})");
                if std::env::var_os("CRASH_DUMP").is_some() {
                    eprintln!("=== {ctx}");
                }
                let (observed, _, halt_step, restart_obligations) = run(
                    flow,
                    members,
                    cut_member,
                    Some(Cut {
                        member: cut_member,
                        k,
                        variant,
                    }),
                    &BTreeSet::new(),
                    None,
                )
                .await;
                for (m, member) in observed.members.iter().enumerate() {
                    assert!(
                        member.obligations_clear,
                        "{ctx}: member {m} still has outstanding obligations"
                    );
                }
                if equivalent(&reference, &observed) {
                    continue;
                }
                let step = halt_step.map(|index| &flow[index]);
                eprintln!(
                    "{ctx} diverged (halted at {step:?}, restart \
                    obligations {restart_obligations})"
                );
                eprintln!("reference: {reference:?}");
                eprintln!("observed:  {observed:?}");
                eprintln!("cut member batches: {:?}", observed.batches);
                assert!(
                    matches_alternative(
                        flow, members, cut_member, &reference, &observed, halt_step, &ctx
                    )
                    .await,
                    "{ctx}: observed world matches neither the reference,                     nor the flow without the abandoned step ({step:?}),                     nor the flow with its staged obligations resolved but \
                    unpublished, nor the reference with the missing \
                    artifacts never emitted, nor the flow with the \
                    reject it escaped never injected"
                );
            }
        }
    }

    /// Every remaining legal explanation for a diverged restart run is a
    /// model where the cut abandoned some work that never became durable.
    /// Try each in turn; the first match wins.
    async fn matches_alternative(
        flow: &[Step],
        members: usize,
        cut_member: usize,
        reference: &Observed,
        observed: &Observed,
        halt_step: Option<usize>,
        ctx: &str,
    ) -> bool {
        // First legal model: the interrupted step's preparation was never
        // durable, so it is as if the step never ran (durability.md
        // boundary 1 abandonment).
        eprintln!("{ctx} trying shortened");
        let mut shortened = flow.to_vec();
        shortened.remove(halt_step.expect("halt_step"));
        let (alternative, ..) = run(
            &shortened,
            members,
            cut_member,
            None,
            &BTreeSet::new(),
            None,
        )
        .await;
        if equivalent(&alternative, observed) {
            return true;
        }
        // Second legal model: the interrupted step's canonical write
        // survived but everything it had staged at the cut never became
        // durable — resolve those obligations as accepted without ever
        // publishing them.
        eprintln!("{ctx} trying withhold-step");
        let (withheld_step, ..) =
            run(flow, members, cut_member, None, &BTreeSet::new(), halt_step).await;
        if equivalent(&withheld_step, observed) {
            return true;
        }
        // Or a publish round was part-way through when the cut landed:
        // the artifacts missing from the wire are exactly those whose
        // rows never became durable — withhold them.
        let mut suppress: BTreeSet<(usize, &'static str, usize)> = BTreeSet::new();
        for (m, (reference_member, observed_member)) in reference
            .members
            .iter()
            .zip(observed.members.iter())
            .enumerate()
        {
            for (_, name, recipient) in reference_member
                .authored_events
                .difference(&observed_member.authored_events)
            {
                suppress.insert((m, name, *recipient));
            }
        }
        eprintln!("{ctx} trying suppress-diff");
        let (withheld, ..) = run(flow, members, cut_member, None, &suppress, None).await;
        if equivalent(&withheld, observed) {
            return true;
        }
        // Final legal model: the cut dropped the staged fanout rows a
        // later injected all-rejected report would have hit while the
        // durable intent survived, so the fault found nothing and the
        // regenerated obligation resolved under the next ordinary report.
        let halt = halt_step.expect("halt_step");
        for (index, step) in flow.iter().enumerate() {
            if index <= halt || !matches!(step, Step::Reject(m) if *m == cut_member) {
                continue;
            }
            eprintln!("{ctx} trying reject-dropped at {index}");
            let mut without_reject = flow.to_vec();
            without_reject.remove(index);
            let (alternative, ..) = run(
                &without_reject,
                members,
                cut_member,
                None,
                &BTreeSet::new(),
                None,
            )
            .await;
            if equivalent(&alternative, observed) {
                return true;
            }
        }
        false
    }

    /// Publisher-side cuts over create → welcome → two-way messaging.
    /// Boundaries 1 (staging), 2 (lost resolution ack), 3 (apply
    /// atomicity), 4 (post-resolution effect re-emission).
    #[tokio::test(flavor = "current_thread")]
    async fn restart_boundaries_publish_lifecycle() {
        const FLOW: &[Step] = &[
            Step::KeyPackage(0),
            Step::Flush(0),
            Step::InboxRelays(0),
            Step::Flush(0),
            Step::KeyPackage(1),
            Step::Flush(1),
            Step::InboxRelays(1),
            Step::Flush(1),
            Step::Create(0, "crash-publish", &[1], &[]),
            Step::Flush(0),
            Step::Deliver(1),
            Step::Tick(1),
            Step::Flush(1),
            Step::Deliver(0),
            Step::Tick(0),
            Step::Send(0, "a-1"),
            Step::Flush(0),
            Step::Deliver(1),
            Step::Tick(1),
            Step::Send(1, "b-1"),
            Step::Flush(1),
            Step::Deliver(0),
            Step::Tick(0),
        ];
        matrix(FLOW, 2, 0, "publish-lifecycle").await;
    }

    /// KeyPackage lifecycle cuts: both variants at every batch of the
    /// invitee's `ensure` staging (C10), signed-outbound release (C11),
    /// report/promotion transaction (C12, C13) and the post-consumption
    /// `Tick` that stages the same-slot replacement (C14). The
    /// `key_package` observer projection pins `current`/`pending`/
    /// `retained` equality; authored-event equality pins C11's
    /// byte-identical re-release.
    #[tokio::test(flavor = "current_thread")]
    async fn restart_boundaries_key_package_lifecycle() {
        const FLOW: &[Step] = &[
            Step::KeyPackage(0),
            Step::Flush(0),
            Step::InboxRelays(0),
            Step::Flush(0),
            Step::KeyPackage(1),
            Step::InboxRelays(1),
            Step::Flush(1),
            Step::Create(1, "crash-key-package", &[0], &[]),
            Step::Flush(1),
            Step::Deliver(0),
            Step::Tick(0),
            Step::Flush(0),
            Step::Deliver(1),
            Step::Tick(1),
        ];
        matrix(FLOW, 2, 0, "key-package-lifecycle").await;
    }

    /// Sign-out withdrawal: cuts inside the atomic `sign_out` batch
    /// (C18) — a reopen either keeps the whole live slot or the fully
    /// withdrawn one, and a cut after the batch still owes the same
    /// signed kind-5. The `key_package` projection `(current, pending,
    /// retained)` and the bundle count pin both sides.
    /// Self-update obligation cuts (C15/C16): the `Tick` entering `Jitter`
    /// persists `not_before` — a cut either side of that batch reopens to
    /// the same deadline (never resampled). Cuts after the self-update's
    /// fanout publish but before its confirm reopen to the open
    /// `PendingPublication` obligation; the identical durable commit
    /// confirms and the obligation completes exactly once — authored-event
    /// equality pins that no second self-update Commit was authored.
    #[tokio::test(flavor = "current_thread")]
    async fn restart_boundaries_sign_out() {
        const FLOW: &[Step] = &[
            Step::KeyPackage(0),
            Step::Flush(0),
            Step::SignOut(0),
            Step::Flush(0),
        ];
        matrix(FLOW, 1, 0, "sign-out").await;
    }

    /// Post-join self-update cuts (C15/C16): the joiner's obligation
    /// walks `CatchUp` → `Quiet` → `Jitter` (`Advance` past the quiet
    /// window; the post-join sampled jitter is bounded by 30 s, so
    /// `Advance(31_000)` always reaches `not_before`) → gate →
    /// `PendingPublication` → publish → confirm → `Complete`. Both cut
    /// variants at every batch.
    #[tokio::test(flavor = "current_thread")]
    async fn restart_boundaries_self_update() {
        const FLOW: &[Step] = &[
            Step::KeyPackage(0),
            Step::Flush(0),
            Step::InboxRelays(0),
            Step::Flush(0),
            Step::KeyPackage(1),
            Step::Flush(1),
            Step::InboxRelays(1),
            Step::Flush(1),
            Step::Create(0, "crash-self-update", &[1], &[]),
            Step::Flush(0),
            Step::Deliver(1),
            Step::HistoryFetched(1),
            Step::Advance(1, 61_000),
            Step::Tick(1),
            Step::Advance(1, 31_000),
            Step::Tick(1),
            Step::Flush(1),
            Step::Deliver(1),
            Step::Tick(1),
            Step::Flush(1),
        ];
        matrix(FLOW, 2, 0, "self-update").await;
    }

    /// Joiner-side cuts over the same flow — the multi-part apply of an
    /// incoming commit/welcome is observer-atomic from a durable prefix.
    /// Boundaries 3, 4, 7.
    #[tokio::test(flavor = "current_thread")]
    async fn restart_boundaries_apply_side() {
        const FLOW: &[Step] = &[
            Step::KeyPackage(0),
            Step::Flush(0),
            Step::InboxRelays(0),
            Step::Flush(0),
            Step::KeyPackage(1),
            Step::Flush(1),
            Step::InboxRelays(1),
            Step::Flush(1),
            Step::Create(0, "crash-apply", &[1], &[]),
            Step::Flush(0),
            Step::Deliver(1),
            Step::Tick(1),
            Step::Flush(1),
            Step::Deliver(0),
            Step::Tick(0),
            Step::Send(0, "a-1"),
            Step::Flush(0),
            Step::Deliver(1),
            Step::Tick(1),
            Step::Send(1, "b-1"),
            Step::Flush(1),
            Step::Deliver(0),
            Step::Tick(0),
        ];
        matrix(FLOW, 2, 1, "apply-side").await;
    }

    /// Concurrent commits plus a removal: cuts land while convergence is
    /// collecting input (5), while a frozen batch resolves (6), while the
    /// selected branch applies (7), and while the removed member realizes
    /// its local-leaf removal (9).
    #[tokio::test(flavor = "current_thread")]
    async fn restart_boundaries_convergence_and_removal() {
        const FLOW: &[Step] = &[
            Step::KeyPackage(0),
            Step::Flush(0),
            Step::InboxRelays(0),
            Step::Flush(0),
            Step::KeyPackage(1),
            Step::Flush(1),
            Step::InboxRelays(1),
            Step::Flush(1),
            Step::KeyPackage(2),
            Step::Flush(2),
            Step::InboxRelays(2),
            Step::Flush(2),
            Step::Create(0, "crash-convergence", &[1], &[]),
            Step::Flush(0),
            Step::Deliver(1),
            Step::Tick(1),
            Step::Flush(1),
            Step::Deliver(0),
            Step::Tick(0),
            Step::Invite(0, 2),
            Step::Flush(0),
            Step::Deliver(2),
            Step::Tick(2),
            Step::Flush(2),
            Step::Deliver(0),
            Step::Deliver(1),
            Step::Tick(0),
            Step::Tick(1),
            Step::UpdateProfile(0, "renamed", "a description"),
            Step::Flush(0),
            Step::SelfUpdate(1),
            Step::Flush(1),
            Step::Deliver(0),
            Step::Deliver(1),
            Step::Deliver(2),
            // Concurrent commits on the same parent epoch.
            Step::Send(0, "a-fork"),
            Step::Send(1, "b-fork"),
            Step::Flush(0),
            Step::Flush(1),
            Step::Deliver(0),
            Step::Deliver(1),
            Step::Deliver(2),
            Step::Tick(0),
            Step::Tick(1),
            Step::Tick(2),
            Step::Settle(0),
            Step::Settle(1),
            Step::Settle(2),
            Step::Remove(0, 1),
            Step::Flush(0),
            Step::Deliver(1),
            Step::Deliver(2),
            Step::Tick(1),
            Step::Tick(2),
            Step::Settle(0),
            Step::Settle(1),
            Step::Settle(2),
            Step::Send(0, "post-removal"),
            Step::Flush(0),
            Step::Deliver(2),
            Step::Tick(2),
        ];
        matrix(FLOW, 3, 0, "convergence-admin").await;
        matrix(FLOW, 3, 1, "convergence-removed").await;
    }

    /// Admin step-down-and-leave on the leaver (boundary 9's `Leaving`
    /// interrupt) — the leaver publishes the step-down and the
    /// `SelfRemove`, the remaining member converges to a one-member
    /// group with itself as sole admin.
    #[tokio::test(flavor = "current_thread")]
    async fn restart_boundary_leaving() {
        const FLOW: &[Step] = &[
            Step::KeyPackage(0),
            Step::Flush(0),
            Step::InboxRelays(0),
            Step::Flush(0),
            Step::KeyPackage(1),
            Step::Flush(1),
            Step::InboxRelays(1),
            Step::Flush(1),
            Step::Create(0, "crash-leave", &[1], &[1]),
            Step::Flush(0),
            Step::Deliver(1),
            Step::Tick(1),
            Step::Flush(1),
            Step::Deliver(0),
            Step::Tick(0),
            Step::Send(0, "before-leave"),
            Step::Flush(0),
            Step::Deliver(1),
            Step::Tick(1),
            Step::Leave(1),
            Step::Flush(1),
            Step::Deliver(0),
            Step::Tick(0),
            Step::Settle(0),
            Step::Settle(1),
            Step::Send(0, "post-leave"),
            Step::Flush(0),
            Step::Deliver(0),
            Step::Tick(0),
        ];
        matrix(FLOW, 2, 1, "leaving").await;
    }

    /// Failed publication then retry (boundary 2's other half: the
    /// obligation resolves `RolledBack`, the regenerated intent confirms
    /// on retry). Cuts on the publisher around both resolutions.
    #[tokio::test(flavor = "current_thread")]
    async fn restart_boundaries_publish_failure() {
        const FLOW: &[Step] = &[
            Step::KeyPackage(0),
            Step::Flush(0),
            Step::InboxRelays(0),
            Step::Flush(0),
            Step::KeyPackage(1),
            Step::Flush(1),
            Step::InboxRelays(1),
            Step::Flush(1),
            Step::Create(0, "crash-retry", &[1], &[]),
            Step::Flush(0),
            Step::Deliver(1),
            Step::Tick(1),
            Step::Flush(1),
            Step::Deliver(0),
            Step::Tick(0),
            Step::Send(0, "will-fail-once"),
            Step::Reject(0),
            Step::Tick(0),
            Step::Flush(0),
            Step::Deliver(1),
            Step::Tick(1),
            Step::Settle(0),
            Step::Settle(1),
        ];
        matrix(FLOW, 2, 0, "publish-failure").await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn debug_batch_tables() {
        let mut a = open_member().await;
        a.session
            .ensure_key_package(&[RELAY.to_owned()])
            .await
            .expect("key package");
        let mut reopened = reopen_member(&a).await;
        let out = reopened.session.take_outbound().expect("take_outbound");
        eprintln!(
            "reopen take_outbound: {:?}",
            out.iter()
                .map(|o| (o.event.kind, o.purpose))
                .collect::<Vec<_>>()
        );
        let out2 = reopened.session.take_outbound().expect("take_outbound2");
        eprintln!("reopen take_outbound2: {out2:?}");

        let relay = FakeRelay::new();
        let mut a = open_member().await;
        publish_identity(&mut a, &relay, RELAY).await;
        for (i, batch) in a.host.batches().iter().enumerate() {
            let mut tables: BTreeMap<u8, usize> = BTreeMap::new();
            for op in &batch.ops {
                let key = op_key(op);
                *tables.entry(key[0]).or_default() += 1;
            }
            eprintln!("batch {i} seq={} tables={tables:?}", batch.seq);
        }
    }

    /// Boundary 3/7 reachability evidence: the engine performs a
    /// commit's canonical apply inside one storage unit — there is no
    /// intermediate host commit to cut at. Count batches per applying
    /// call on the joiner: one batch per `ingest_batch` of a single
    /// commit, one for the `report_publish` resolution.
    #[tokio::test(flavor = "current_thread")]
    async fn apply_is_one_commit() {
        let relay = FakeRelay::new();
        let mut a = open_member().await;
        let mut b = open_member().await;
        publish_identity(&mut a, &relay, RELAY).await;
        publish_identity(&mut b, &relay, RELAY).await;

        {
            let invitees = vec![Invitee {
                key_package: key_package_of(&relay, RELAY, &b.account_hex()),
                inbox_relays: inbox_relays_of(&relay, RELAY, &b.account_hex()),
            }];
            a.session
                .create_group(CreateGroup {
                    name: "crash-atomic".to_owned(),
                    description: String::new(),
                    relays: vec![RELAY.to_owned()],
                    invitees,
                    admins: Vec::new(),
                    components: Vec::new(),
                })
                .await
                .expect("create_group")
        };

        // The founding publish resolution: Current-profile creation
        // already made the group canonical at `create_group`, so the
        // resolution commit is bookkeeping only — no canonical write.
        let before = a.host.batches().len();
        flush(&mut a, &relay).await;
        let publish = &a.host.batches()[before..];
        assert_eq!(
            canonical_batches(publish),
            0,
            "the founding publish resolution must not write canonical state again"
        );

        // The joiner's whole welcome ingest: the canonical apply is one
        // commit; derived work (route registration, message-state
        // updates) lands in later commits — each a boundary-4 cut point
        // the matrix covers.
        let welcomes = welcomes_for(&relay, RELAY, &b.account_hex());
        assert_eq!(welcomes.len(), 1);
        let before = b.host.batches().len();
        b.session
            .ingest_batch(welcomes)
            .await
            .expect("ingest welcome");
        let join = &b.host.batches()[before..];
        for (i, batch) in join.iter().enumerate() {
            let mut tables: BTreeMap<u8, usize> = BTreeMap::new();
            for op in &batch.ops {
                let key = op_key(op);
                *tables.entry(key[0]).or_default() += 1;
            }
            eprintln!("join batch {i}: {tables:?}");
        }
        assert_eq!(
            canonical_batches(join),
            1,
            "the welcome join must mutate canonical state in exactly one commit (got {join:?} batches)"
        );
    }

    /// Batches touching `Table::Group` (discriminant 1) — the canonical
    /// group state write.
    fn canonical_batches(batches: &[mk_core::KvBatch]) -> usize {
        batches
            .iter()
            .filter(|batch| {
                batch.ops.iter().any(|op| {
                    let key = op_key(op);
                    key[0] == 1
                })
            })
            .count()
    }

    /// Boundary 8: delete one selection-relevant row family from the
    /// durable rows before reopen. Recovery must refuse, flag, or
    /// block — it must never guess the only remaining local branch or
    /// silently present a different canonical state.
    #[tokio::test(flavor = "current_thread")]
    async fn missing_selection_material() {
        // Row-key first byte = table discriminant (src/keys.rs `Table`):
        // group canonical state = 1, message/admitted input = 3,
        // ingress dedup = 8, processed transport id = 9,
        // queued intent = 10, own-commit intent = 11,
        // outbound fanout = 12, retained snapshot = 29,
        // session outbox (acknowledgement-uncertain fanout) = 33.
        const TABLE_GROUP: u8 = 1;
        const TABLE_MESSAGE: u8 = 3;
        const TABLE_QUEUED_INTENT: u8 = 10;
        const TABLE_SNAPSHOT: u8 = 29;
        const TABLE_SESSION_OUTBOX: u8 = 33;

        let relay = FakeRelay::new();
        let mut a = open_member().await;
        let mut b = open_member().await;
        publish_identity(&mut a, &relay, RELAY).await;
        publish_identity(&mut b, &relay, RELAY).await;

        let invitees = vec![Invitee {
            key_package: key_package_of(&relay, RELAY, &b.account_hex()),
            inbox_relays: inbox_relays_of(&relay, RELAY, &b.account_hex()),
        }];
        a.session
            .create_group(CreateGroup {
                name: "crash-b8".to_owned(),
                description: String::new(),
                relays: vec![RELAY.to_owned()],
                invitees,
                admins: Vec::new(),
                components: Vec::new(),
            })
            .await
            .expect("create_group");
        flush(&mut a, &relay).await;
        let gid = a.session.groups().expect("groups")[0].id.clone();
        send_chat(&mut a, &relay, &gid, "pre-loss").await;

        // Reference: intact reopen shows the full coherent snapshot.
        let intact = reopen_member(&a).await;
        let intact_groups = intact.session.groups().expect("groups");
        assert_eq!(intact_groups.len(), 1);

        // What is actually present, for the report.
        let rows = a.host.durable_rows();
        let mut families: BTreeMap<u8, usize> = BTreeMap::new();
        for (key, _) in &rows {
            *families.entry(key[0]).or_default() += 1;
        }
        eprintln!("b8 durable families: {families:?}");

        for table in [
            TABLE_GROUP,
            TABLE_MESSAGE,
            TABLE_QUEUED_INTENT,
            TABLE_SNAPSHOT,
            TABLE_SESSION_OUTBOX,
        ] {
            let removed = families.get(&table).copied().unwrap_or(0);
            if removed == 0 {
                eprintln!("b8 table {table}: no rows present — skipped");
                continue;
            }
            let damaged: Vec<(Vec<u8>, Vec<u8>)> = rows
                .iter()
                .filter(|(key, _)| key[0] != table)
                .cloned()
                .collect();
            let outcome = match Session::open(session_config(
                std::sync::Arc::clone(&a.signer),
                std::sync::Arc::clone(&a.clock),
                std::sync::Arc::clone(&a.host),
                damaged,
            ))
            .expect("open")
            {
                Opened::Ready(mut session) => {
                    let groups = session.groups().expect("groups");
                    let coherent = groups
                        .iter()
                        .map(|g| (g.state, g.epoch, g.members.len()))
                        .collect::<Vec<_>>()
                        == intact_groups
                            .iter()
                            .map(|g| (g.state, g.epoch, g.members.len()))
                            .collect::<Vec<_>>();
                    let flagged = groups.iter().any(|g| {
                        matches!(g.state, GroupState::Recovering | GroupState::Unrecoverable)
                    }) || session
                        .drain_events()
                        .unwrap()
                        .iter()
                        .any(|e| matches!(e, SessionEvent::HydrationQuarantined { .. }));
                    if groups.is_empty() {
                        format!("group-silently-absent ({removed} rows removed)")
                    } else if flagged {
                        "flagged (recovering/unrecoverable/quarantined)".to_owned()
                    } else if coherent {
                        "coherent-equivalent".to_owned()
                    } else {
                        panic!(
                            "b8 table {table}: reopened Stable with a different canonical \
                             projection — a guessed branch, not recovery"
                        );
                    }
                }
                Opened::NeedsAccountProof(_) => {
                    // Losing the account binding is a defined refusal —
                    // better than guessing; still surfaced for the report.
                    "proof-required".to_owned()
                }
            };
            eprintln!("b8 table {table}: {outcome}");
        }
    }
}
