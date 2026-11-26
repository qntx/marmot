/**
 * `MarmotClient` — the application surface: one `Engine` behind the `Pump`, typed errors, typed
 * events. Public ops run through the pump; relay lookups run outside it; every relay URL passes
 * `safeRelayUrl` before it can open a connection. The signer must support NIP-44 extended length
 * (spec `transports/nostr.md`), including through NIP-46 request/response events and NIP-07/NIP-55
 * extensions: `open` probes it once per account per client state — a self round trip of a
 * 65,536-byte plaintext — caches the pass as `nip44Extended` in the persisted state, and rejects
 * `signer_refused` or `signer_unsupported` before the engine opens; clearing the `CursorStore`
 * re-probes. Peer-supplied media locators on received messages are filtered through `safeMediaUrl`
 * and a group avatar URL that fails it is dropped — hosts using `@qntx/marmot/backend` directly get
 * the raw values and must apply `safeMediaUrl` (or `mk_core::media::Locator::destination` +
 * `check_resolved_address` in Rust) before fetching; DNS-resolved addresses remain the host's
 * check, same as relays.
 */

import { Engine } from "../backend/engine.ts";
import type { Clock, Signer } from "../backend/engine.ts";
import type { Backend } from "../backend/port.ts";
import type {
  AppMessage,
  AttachmentInput,
  ComponentUpdate,
  GroupAvatar,
  GroupSnapshot,
  InputReport,
  Invitee,
  KeyPackageStatusResult,
  SignOutResult,
  MediaReference,
  NostrEvent,
  ReaddOutcome,
  RejoinOffer,
  SendOutcome,
  SentMessage,
} from "../backend/protocol.ts";
import * as groups from "../groups/index.ts";
import type { UploadPort } from "../groups/index.ts";
import { observeSigner, probeNip44Extended } from "../invites/index.ts";
import type { InviteEvent } from "../invites/index.ts";
import { KEY_PACKAGE_KIND, lookupInvitees, writeRelaysOf } from "../keys/index.ts";
import type { CursorStore, KvDriver } from "../storage/driver.ts";
import { fetchEvents } from "../transport/fetch.ts";
import type { RelayPort, Timer } from "../transport/index.ts";
import { GIFT_WRAP_KIND, safeMediaUrl, safeRelayUrl, wireEventBytes } from "../transport/index.ts";
import { verifyInbound } from "../transport/verify.ts";
import { MarmotError, toMarmotError } from "./errors.ts";
import type { MarmotErrorKind } from "./errors.ts";
import { MAX_INBOUND_EVENT_BYTES, Pump } from "./pump.ts";
import type { ClientMessage, DeliveryEvent, DropEvent, GroupEvent } from "./pump.ts";
import { decodeClientState, emptyClientState, encodeClientState } from "./state.ts";
import type { ClientState, InboxOwner } from "./state.ts";

export type MarmotClientOptions = {
  backend: Backend;
  kv: KvDriver;
  cursors: CursorStore;
  signer: Signer;
  clock: Clock;
  timer: Timer;
  relays: RelayPort;
  /**
   * Who owns the account's NIP-17 gift-wrap inbox. `"client"`: the pump opens the kind-1059
   * subscriptions, tracks inbox cursors/floors and unwraps every wrap itself. `"host"`: the host
   * runs its own NIP-17 inbox — no kind-1059 subscription ever opens — and forwards each wrap that
   * unwrapped to a kind-444 rumor through `ingestWrap`. Recorded in the client state; a reopen in
   * the other mode is rejected `wrong_inbox_owner`.
   */
  inbox: InboxOwner;
  /**
   * Read relays used to fetch this account's NIP-65 list for `ensureKeyPackage`. When non-empty,
   * `open` runs `ensureKeyPackage` once in the background (errors surface via `onError`).
   */
  lookupRelays?: string[];
  allowLoopback?: boolean;
};

type CreateGroupArgs = Parameters<Engine["createGroup"]>[0];

/** The daily `KeyPackage` slot audit cadence. */
const SLOT_AUDIT_INTERVAL_MS = 24 * 60 * 60 * 1000;

type MediaInput = {
  caption: string;
  attachments: AttachmentInput[];
  tags?: string[][];
};

export class MarmotClient {
  private readonly options: MarmotClientOptions;
  private readonly engine: Engine;
  private readonly observed: ReturnType<typeof observeSigner>;
  private readonly pump: Pump;
  private readonly allowLoopback: boolean;
  private closing = false;
  private closePromise: Promise<void> | undefined;
  private slotAuditCancel: (() => void) | undefined;

  private constructor(
    options: MarmotClientOptions,
    engine: Engine,
    observed: ReturnType<typeof observeSigner>,
    state: ClientState,
  ) {
    this.options = options;
    this.engine = engine;
    this.observed = observed;
    this.account = options.signer.pubkey;
    this.allowLoopback = options.allowLoopback ?? false;
    this.pump = new Pump({
      engine,
      relays: options.relays,
      timer: options.timer,
      clock: options.clock,
      cursors: options.cursors,
      state,
      account: options.signer.pubkey,
      allowLoopback: this.allowLoopback,
    });
  }

  static async open(options: MarmotClientOptions): Promise<MarmotClient> {
    const observed = observeSigner(options.signer);
    let stored: ClientState | undefined;
    try {
      stored = decodeClientState(await options.cursors.load());
    } catch {
      stored = undefined;
    }
    // The stored state names its inbox owner; a reopen in the other mode is rejected before
    // the probe and before the engine opens. Switching modes deliberately means clearing the
    // CursorStore — an undecodable blob (v3 included) adopts the option's mode.
    if (stored !== undefined && stored.inbox !== options.inbox) {
      throw new MarmotError("wrong_inbox_owner");
    }
    let state: ClientState = stored ?? emptyClientState(options.inbox);
    // The spec requires NIP-44 extended length of every signer a client uses: probe once per
    // client state, before anything opens, and cache the pass as `nip44Extended`.
    if (!state.nip44Extended) {
      const verdict = await probeNip44Extended(options.signer);
      if (verdict === "refused") {
        throw new MarmotError("signer_refused");
      }
      if (verdict === "unsupported") {
        throw new MarmotError("signer_unsupported");
      }
      state = { ...state, nip44Extended: true };
      try {
        await options.cursors.save(encodeClientState(state));
      } catch (error) {
        throw toMarmotError(error);
      }
    }
    let engine: Engine;
    try {
      engine = await Engine.open({
        backend: options.backend,
        kv: options.kv,
        clock: options.clock,
        signer: observed.signer,
        allowLoopback: options.allowLoopback ?? false,
      });
    } catch (error) {
      throw toMarmotError(error, observed.take());
    }
    const client = new MarmotClient(options, engine, observed, state);
    client.pump.start();
    if ((options.lookupRelays?.length ?? 0) > 0) {
      void (async () => {
        try {
          await client.ensureKeyPackage();
        } catch (error) {
          // `no_relay_list` is expected before the account's kind-10002
          // exists anywhere: the slot simply stays unstaged until a later
          // explicit `ensureKeyPackage` with a write set.
          if (!(error instanceof MarmotError && error.kind === "no_relay_list")) {
            client.pump.emitError(toMarmotError(error, observed.take()));
          }
        }
      })();
      void client.runSlotAudit();
      client.scheduleSlotAudit();
    }
    return client;
  }

  readonly account: string;

  async close(): Promise<void> {
    this.closing = true;
    this.slotAuditCancel?.();
    this.slotAuditCancel = undefined;
    this.closePromise ??= (async () => {
      try {
        await this.pump.close();
      } finally {
        await this.options.kv.close();
      }
    })();
    return this.closePromise;
  }

  onMessages(listener: (batch: ClientMessage[]) => void | Promise<void>): () => void {
    return this.pump.onMessages(listener);
  }

  onGroupChange(listener: (event: GroupEvent) => void): () => void {
    return this.pump.onGroupChange(listener);
  }

  /** `joined` per installed Welcome; `pending` per refused/failed unwrap (see `InviteEvent`). */
  onInvite(listener: (event: InviteEvent) => void): () => void {
    return this.pump.onInvite(listener);
  }

  onDelivery(listener: (event: DeliveryEvent) => void): () => void {
    return this.pump.onDelivery(listener);
  }

  onDropped(listener: (event: DropEvent) => void): () => void {
    return this.pump.onDropped(listener);
  }

  onError(listener: (error: MarmotError) => void): () => void {
    return this.pump.onError(listener);
  }

  /**
   * The daily slot audit: reschedule first so a run's outcome never loses the timer, then audit
   * with `open`'s error policy.
   */
  private scheduleSlotAudit(): void {
    this.slotAuditCancel = this.options.timer.set(SLOT_AUDIT_INTERVAL_MS, () => {
      if (!this.closing) {
        this.scheduleSlotAudit();
        void this.runSlotAudit();
      }
    });
  }

  /** One slot-audit pass; `no_relay_list` is expected and silent. */
  private async runSlotAudit(): Promise<void> {
    try {
      await this.auditKeyPackageSlot();
    } catch (error) {
      if (!(error instanceof MarmotError && error.kind === "no_relay_list")) {
        this.pump.emitError(toMarmotError(error));
      }
    }
  }

  private guard(): void {
    if (this.closing) {
      throw new MarmotError("closed");
    }
  }

  private async op<T>(job: (engine: Engine) => Promise<T>): Promise<T> {
    this.guard();
    return this.pump.run(async () => {
      // Reset, then run: a verdict belongs to the job that observed the signer call.
      this.observed.take();
      try {
        return await job(this.engine);
      } catch (error) {
        throw toMarmotError(error, this.observed.take());
      }
    });
  }

  /**
   * Drive the account `KeyPackage` slot's lifecycle: the NIP-65 write set is fetched from this
   * account's newest kind-10002 on `options.lookupRelays`, then `ensure_key_package` runs inside
   * the op queue. `no_relay_list` when the lookup relays are missing or the list is empty.
   */
  async ensureKeyPackage(): Promise<KeyPackageStatusResult> {
    this.guard();
    const safe = (this.options.lookupRelays ?? []).filter((relay) =>
      safeRelayUrl(relay, this.allowLoopback),
    );
    let write: string[] | undefined;
    try {
      write = await writeRelaysOf(
        this.options.relays,
        this.options.timer,
        this.account,
        safe,
        this.allowLoopback,
      );
    } catch (error) {
      throw toMarmotError(error);
    }
    if (write === undefined || write.length === 0) {
      throw new MarmotError("no_relay_list");
    }
    return this.op(async (engine) => engine.ensureKeyPackage(write));
  }

  /**
   * Audit this device's `KeyPackage` slot for publications it did not make: fetches every
   * kind-30443 for the slot from the account's NIP-65 write set and hands it to the engine, which
   * outranks any foreign publication inside the skew window. A write relay whose read completed
   * empty (`eose`, nothing returned) is reported too: when it accepted this device's day-old
   * current publication, the engine stages a new KeyPackage to refill the slot. `no_relay_list`
   * when the lookup relays are missing or the list is empty.
   */
  async auditKeyPackageSlot(): Promise<KeyPackageStatusResult> {
    this.guard();
    const safe = (this.options.lookupRelays ?? []).filter((relay) =>
      safeRelayUrl(relay, this.allowLoopback),
    );
    let write: string[] | undefined;
    try {
      write = await writeRelaysOf(
        this.options.relays,
        this.options.timer,
        this.account,
        safe,
        this.allowLoopback,
      );
    } catch (error) {
      throw toMarmotError(error);
    }
    if (write === undefined || write.length === 0) {
      throw new MarmotError("no_relay_list");
    }
    const status = await this.keyPackageStatus();
    const { events, emptyRelays } = await fetchEvents(
      this.options.relays,
      this.options.timer,
      write,
      {
        kinds: [KEY_PACKAGE_KIND],
        authors: [this.account],
        "#d": [status.slot],
      },
    );
    return this.op(async (engine) => engine.auditKeyPackageSlot(events, emptyRelays));
  }

  /**
   * Sign this device out: withdraw the `KeyPackage` slot (kind-5 to the write set), leave every
   * group this device is a non-admin member of and delete every `KeyPackage` bundle, in one durable
   * transaction. `no_relay_list` when the lookup relays are missing or the list is empty. Admin
   * groups this account alone administers stay in `keptAdminLeaves`.
   */
  async signOut(): Promise<SignOutResult> {
    this.guard();
    const safe = (this.options.lookupRelays ?? []).filter((relay) =>
      safeRelayUrl(relay, this.allowLoopback),
    );
    let write: string[] | undefined;
    try {
      write = await writeRelaysOf(
        this.options.relays,
        this.options.timer,
        this.account,
        safe,
        this.allowLoopback,
      );
    } catch (error) {
      throw toMarmotError(error);
    }
    if (write === undefined || write.length === 0) {
      throw new MarmotError("no_relay_list");
    }
    return this.op(async (engine) => engine.signOut(write));
  }

  /** The account `KeyPackage` slot's lifecycle standing. */
  async keyPackageStatus(): Promise<KeyPackageStatusResult> {
    return this.op(async (engine) => engine.keyPackageStatus());
  }

  /**
   * Publish the account's NIP-17 inbox relay list; every relay must pass `safeRelayUrl`. Persists
   * the list so reconcile subscribes for gift wraps. Client-owned inbox only: under `inbox: "host"`
   * the host's own NIP-17 inbox owns the account's kind-10050 — publishing here would overwrite it
   * — so the call rejects `wrong_inbox_owner` before any engine call.
   */
  async publishInboxRelays(relays: string[]): Promise<void> {
    this.guard();
    if (this.options.inbox === "host") {
      throw new MarmotError("wrong_inbox_owner");
    }
    for (const relay of relays) {
      if (!safeRelayUrl(relay, this.allowLoopback)) {
        throw new MarmotError("unsafe_relay");
      }
    }
    await this.op(async (engine) => {
      await engine.publishInboxRelays(relays);
    });
    this.pump.setInbox(relays);
  }

  /**
   * Resolve an account's published KeyPackages for inviting; non-found maps to a typed error.
   * Selection runs against `group`'s invite requirements when given, creation requirements
   * otherwise — relay I/O stays outside the pump while `select_key_packages` runs inside it.
   */
  async lookupInvitees(pubkey: string, lookupRelays: string[], group?: string): Promise<Invitee[]> {
    this.guard();
    const safe = lookupRelays.filter((relay) => safeRelayUrl(relay, this.allowLoopback));
    let lookup;
    try {
      lookup = await lookupInvitees(
        this.options.relays,
        this.options.timer,
        pubkey,
        safe,
        this.allowLoopback,
        async (events) =>
          this.op(async (engine) => engine.selectKeyPackages(pubkey, events, group)),
      );
    } catch (error) {
      throw toMarmotError(error);
    }
    if (lookup.type === "found") {
      return lookup.invitees;
    }
    const kind: MarmotErrorKind =
      lookup.type === "no_relay_list"
        ? "no_relay_list"
        : lookup.type === "no_inbox_relays"
          ? "no_inbox_relays"
          : "no_key_package";
    throw new MarmotError(kind);
  }

  async createGroup(
    args: CreateGroupArgs & { includeOwnDevices?: boolean },
  ): Promise<GroupSnapshot> {
    for (const relay of args.relays) {
      if (!safeRelayUrl(relay, this.allowLoopback)) {
        throw new MarmotError("unsafe_relay");
      }
    }
    const { includeOwnDevices = true, ...rest } = args;
    let ownDevices = rest.ownDevices ?? [];
    const lookupRelays = this.options.lookupRelays ?? [];
    if (includeOwnDevices && lookupRelays.length > 0) {
      // Sibling slots found the group with us — a MAY: any lookup
      // failure simply founds without own devices.
      try {
        const status = await this.keyPackageStatus();
        const invitees = await this.lookupInvitees(this.account, lookupRelays);
        ownDevices = invitees.filter(
          (invitee) => invitee.keyPackage.tags.find((tag) => tag[0] === "d")?.[1] !== status.slot,
        );
      } catch (error) {
        const tolerated =
          error instanceof MarmotError &&
          (error.kind === "no_relay_list" ||
            error.kind === "no_key_package" ||
            error.kind === "no_inbox_relays");
        if (!tolerated) {
          throw error;
        }
      }
    }
    return this.op(async (engine) =>
      this.gateAvatar(await groups.createGroup(engine, { ...rest, ownDevices })),
    );
  }

  async invite(group: string, invitees: Invitee[]): Promise<SendOutcome> {
    return this.op(async (engine) => groups.invite(engine, group, invitees));
  }

  async removeMembers(group: string, members: string[]): Promise<SendOutcome> {
    return this.op(async (engine) => groups.removeMembers(engine, group, members));
  }

  async leave(group: string): Promise<SendOutcome> {
    return this.op(async (engine) => {
      const outcome = await groups.leave(engine, group);
      // A leave arms the delayed self-remove auto-commit: queue a tick to arm its wake.
      if (outcome.type === "leave_requested") {
        this.pump.requestTick();
      }
      return outcome;
    });
  }

  async selfUpdate(group: string): Promise<SendOutcome> {
    return this.op(async (engine) => groups.selfUpdate(engine, group));
  }

  /** The group's stored replacement-Welcome offers awaiting this device's consent. */
  async rejoinOffers(group: string): Promise<RejoinOffer[]> {
    return this.op(async (engine) => groups.rejoinOffers(engine, group));
  }

  /** Consent to a stored replacement Welcome — installs the new copy. */
  async confirmRejoin(group: string, offer: string, token: string): Promise<void> {
    return this.op(async (engine) => groups.confirmRejoin(engine, group, offer, token));
  }

  /** Decline a stored replacement Welcome — retires the offer. */
  async declineRejoin(offer: string): Promise<void> {
    return this.op(async (engine) => groups.declineRejoin(engine, offer));
  }

  /**
   * Remove a stuck member account, then re-invite it once the removal confirms. `lookupRelays`
   * feeds the invitee `KeyPackage` lookup exactly like `invite` callers get it.
   */
  async readdMember(group: string, account: string, lookupRelays: string[]): Promise<ReaddOutcome> {
    const invitees = await this.lookupInvitees(account, lookupRelays, group);
    return this.op(async (engine) => groups.readdMember(engine, group, account, invitees));
  }

  async updateGroupProfile(
    group: string,
    profile: { name?: string | undefined; description?: string | undefined },
  ): Promise<SendOutcome> {
    return this.op(async (engine) => groups.updateGroupProfile(engine, group, profile));
  }

  async updateAppComponents(group: string, updates: ComponentUpdate[]): Promise<SendOutcome> {
    return this.op(async (engine) => groups.updateAppComponents(engine, group, updates));
  }

  async sendMessage(group: string, message: AppMessage): Promise<SentMessage> {
    return this.op(async (engine) => groups.sendMessage(engine, group, message));
  }

  /** Send a chat message; `replyTo` adds the `e`/`q` reply pair. */
  async sendChat(group: string, content: string, replyTo?: string): Promise<SentMessage> {
    return this.op(async (engine) => groups.sendChat(engine, group, content, replyTo));
  }

  /** React to `target` with `emoji`. */
  async react(group: string, target: string, emoji: string): Promise<SentMessage> {
    return this.op(async (engine) => groups.react(engine, group, target, emoji));
  }

  /** Withdraw own reactions by event id. */
  async unreact(group: string, reactions: string[]): Promise<SentMessage> {
    return this.op(async (engine) => groups.unreact(engine, group, reactions));
  }

  /** Replace `target`'s content. */
  async editMessage(group: string, target: string, content: string): Promise<SentMessage> {
    return this.op(async (engine) => groups.editMessage(engine, group, target, content));
  }

  /** Delete `target` (admin removal shape when this account is an admin). */
  async deleteMessage(group: string, target: string): Promise<SentMessage> {
    return this.op(async (engine) => groups.deleteMessage(engine, group, target));
  }

  /** Admin removal of `target`. */
  async removeMessage(group: string, target: string): Promise<SentMessage> {
    return this.op(async (engine) => groups.removeMessage(engine, group, target));
  }

  /** Grant or revoke `account`'s admin flag. */
  async setAdmin(group: string, account: string, admin: boolean): Promise<SendOutcome> {
    return this.op(async (engine) => groups.setAdmin(engine, group, account, admin));
  }

  /** Step down: revoke this account's own admin flag. */
  async stepDown(group: string): Promise<SendOutcome> {
    return this.op(async (engine) => groups.stepDown(engine, group, this.account));
  }

  /** Set or clear the group's avatar; `undefined` clears. */
  async setGroupAvatar(group: string, avatar: GroupAvatar | undefined): Promise<SendOutcome> {
    return this.op(async (engine) => groups.setGroupAvatar(engine, group, avatar));
  }

  /** Set the group's message retention seconds; `0` disables. */
  async setRetention(group: string, seconds: bigint): Promise<SendOutcome> {
    return this.op(async (engine) => groups.setRetention(engine, group, seconds));
  }

  /** Request the group's terminal disband; subscriptions reconcile afterwards. */
  async disband(group: string): Promise<SendOutcome> {
    return this.op(async (engine) => {
      const outcome = await groups.disband(engine, group);
      this.pump.flagReconcile();
      // A disband request arms deferred engine work: queue a tick so it reaches an
      // outbound commit without waiting on unrelated traffic.
      if (outcome.type === "disband_requested") {
        this.pump.requestTick();
      }
      return outcome;
    });
  }

  /** Require the lifecycle component on a group that predates it. */
  async enableDisbanding(group: string): Promise<SendOutcome> {
    return this.op(async (engine) => groups.enableDisbanding(engine, group));
  }

  /** Forget the group locally; subscriptions reconcile away from it. */
  async forgetGroup(group: string): Promise<void> {
    await this.op(async (engine) => {
      await groups.forgetGroup(engine, group);
      this.pump.flagReconcile();
    });
  }

  /**
   * Seal in the pump, upload outside it, send back in it. An upload failure discards every sealed
   * handle before rejecting `upload_failed`.
   */
  async sendMedia(group: string, media: MediaInput, upload: UploadPort): Promise<SentMessage> {
    const sealed = await this.op(async (engine) =>
      groups.sealAttachments(engine, group, media.attachments),
    );
    try {
      const refs = await groups.uploadSealed(upload, sealed, media.attachments);
      return await this.op(async (engine) =>
        groups.sendSealedMedia(engine, group, media.caption, refs, media.tags ?? []),
      );
    } catch (error) {
      try {
        await this.op(async (engine) => groups.discardSealed(engine, sealed));
      } catch {
        // Discard failures must not mask the upload failure.
      }
      throw toMarmotError(error);
    }
  }

  async openAttachment(
    group: string,
    message: string,
    reference: MediaReference,
    ciphertext: Uint8Array,
  ): Promise<Uint8Array> {
    return this.op(async (engine) => engine.openAttachment(group, message, reference, ciphertext));
  }

  /**
   * Host-owned inbox only. The host unwrapped this kind-1059 event itself, saw a kind-444 rumor,
   * and forwards the untouched wrap. The engine peels and validates it exactly as it does for a
   * wrap delivered by its own subscription.
   *
   * The wrap goes through the serialized queue and the normal ingest path: the returned report is
   * the wrap's own `InputReport` (`accepted` for a Welcome this device installs, `rejected{
   * duplicate}` when the wrap was already ingested, `unavailable{signer_refused|signer_failed}`
   * when the signer would not open it — the invite surfaces as `pending` and re-delivery is the
   * host re-forwarding the wrap). Costs one extra NIP-59 unwrap (two signer NIP-44 decrypts) per
   * forwarded Welcome — the host already paid the first — so hosts must forward only wraps whose
   * rumor is kind 444. Session events (`joined`, `message_received`) dispatch after the ingest is
   * durable. Non-1059 kinds, forged ids/signatures and events over the inbound size cap reject
   * `invalid_input` without reaching the engine; `wrong_inbox_owner` when the client owns the
   * inbox.
   */
  async ingestWrap(wrap: NostrEvent): Promise<InputReport> {
    this.guard();
    if (this.options.inbox !== "host") {
      throw new MarmotError("wrong_inbox_owner");
    }
    if (
      wrap.kind !== GIFT_WRAP_KIND ||
      wrap.content.length > MAX_INBOUND_EVENT_BYTES ||
      wireEventBytes(wrap) > MAX_INBOUND_EVENT_BYTES ||
      !verifyInbound(wrap)
    ) {
      throw new MarmotError("invalid_input");
    }
    this.observed.take();
    try {
      return await this.pump.ingestWrap(wrap);
    } catch (error) {
      throw toMarmotError(error, this.observed.take());
    }
  }

  /**
   * Replay pending invites: close the kind-1059 inbox subscriptions and reopen them at the floored
   * cursor, so the relays redeliver wraps the signer refused or failed to unwrap. The client stores
   * no wraps, only each inbox key's floor (the earliest unopened wrap's `createdAt`, persisted with
   * the cursors), so a refused invite re-surfaces as `pending` after any restart or this call until
   * it is opened, permanently rejected, or its `createdAt` is 30 days old; redelivery goes through
   * the normal ingest path, and a floor older than 30 days is dropped, so an invite left unopened
   * that long is lost on this device. Nothing retries on its own: a signer that keeps refusing
   * costs one replay per call, reconnect or restart. Client-owned inbox only — under `inbox:
   * "host"` there is no inbox subscription and no floor, so the call rejects `wrong_inbox_owner`.
   */
  async retryInvites(): Promise<void> {
    this.guard();
    if (this.options.inbox === "host") {
      throw new MarmotError("wrong_inbox_owner");
    }
    await this.pump.retryInvites();
  }

  /**
   * Every stored local copy — live, terminated (`removed`, `disbanded`) and quarantined alike — in
   * group-id byte order. A terminated or quarantined copy stays listed until `forgetGroup`, and
   * `access` alone names its state; hosts that want only live chats filter on `access`.
   */
  async groups(): Promise<GroupSnapshot[]> {
    return this.op(async (engine) => {
      const snapshots = await engine.groups();
      return snapshots.map((snapshot) => this.gateAvatar(snapshot));
    });
  }

  async group(id: string): Promise<GroupSnapshot | undefined> {
    return this.op(async (engine) => {
      const snapshot = await engine.group(id);
      return snapshot === undefined ? undefined : this.gateAvatar(snapshot);
    });
  }

  /**
   * A peer-set avatar URL reaches the host verbatim from group state: return `avatar: undefined`
   * when it fails `safeMediaUrl`, so a group cannot point the host at a private or metadata
   * address.
   */
  private gateAvatar(snapshot: GroupSnapshot): GroupSnapshot {
    const { live } = snapshot;
    if (
      live === undefined ||
      live.avatar === undefined ||
      safeMediaUrl(live.avatar.url, this.allowLoopback)
    ) {
      return snapshot;
    }
    return { ...snapshot, live: { ...live, avatar: undefined } };
  }
}
