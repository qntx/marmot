/**
 * `Engine` — the typed, async wrapper over the byte-oriented bridge `Backend` port. It owns the
 * durability wiring (commit → `DurableKv` → `ack_durable`), the signer suspension loop, and
 * operation serialization.
 */

import {
  decodeDrain,
  decodeOpenResponse,
  decodeResponse,
  encodeCall,
  encodeConfig,
} from "./codec.ts";
import { BridgeError, DecodeError } from "./errors.ts";
import { encodeRows } from "./kv.ts";
import type { DurableKv } from "./kv.ts";
import { match } from "./match.ts";
import type { Backend, BridgeHandle, BridgeHost } from "./port.ts";
import type {
  AppMessage,
  AttachmentInput,
  BridgeRequest,
  ClockReading,
  ComponentUpdate,
  GroupAvatar,
  GroupSnapshot,
  InputReport,
  Invitee,
  KeyPackageStatusResult,
  MediaAttachmentRef,
  MediaReference,
  MessageIntent,
  NostrEvent,
  OpResult,
  Outbound,
  PublishResolution,
  ReaddOutcome,
  RejoinOffer,
  RelayResult,
  SealedAttachmentResult,
  SendOutcome,
  SentMessage,
  SessionEvent,
  SignOutResult,
  SignerOp,
  SignerReply,
  SignerResult,
  UnsignedEvent,
} from "./protocol.ts";
import { PROTOCOL_VERSION } from "./protocol.ts";

/** A signer may throw this to report a refused operation (`refused` reply). */
export class SignerRefusedError extends Error {
  constructor() {
    super("signer refused the operation");
    this.name = "SignerRefusedError";
  }
}

/**
 * A signer may throw this from `nip44Decrypt` when the payload itself does not decrypt or
 * authenticate (`invalid_payload` reply): the wrap is permanently undecryptable, so the ingest is
 * rejected and remembered — not a retryable signer failure.
 */
export class SignerInvalidPayloadError extends Error {
  constructor() {
    super("payload failed to decrypt");
    this.name = "SignerInvalidPayloadError";
  }
}

/**
 * The account-signer surface the engine drives on `signer_pending`. Both NIP-44 directions must
 * support extended length — plaintexts of at least 65,536 bytes under the 6-byte `00 00 ‖
 * u32be(len)` prefix (spec `transports/nostr.md`) — including when the signer is a NIP-46 remote
 * signer (the requirement covers its request/response events) or a NIP-07/NIP-55 extension;
 * `MarmotClient.open` probes this once per client state and rejects `signer_unsupported` otherwise,
 * and `@qntx/marmot/testing` `checkSignerNip44` is the host conformance check.
 */
export type Signer = {
  /** The account this session signs for, 32-byte hex. */
  readonly pubkey: string;
  signEvent: (unsigned: UnsignedEvent) => Promise<NostrEvent>;
  nip44Encrypt: (peer: string, plaintext: string) => Promise<string>;
  /**
   * `nip44Decrypt` MUST throw `SignerInvalidPayloadError` when the payload itself fails to decrypt
   * (MAC/padding/version/base64); throw `SignerRefusedError` for a refusal/lock; anything else is
   * `failed` and retryable.
   */
  nip44Decrypt: (peer: string, ciphertext: string) => Promise<string>;
};

/** The engine's injected clock. */
export type Clock = {
  now: () => ClockReading;
};

/** `Engine.open` options. */
export type EngineOptions = {
  backend: Backend;
  kv: DurableKv;
  signer: Signer;
  clock: Clock;
  /** Optional NIP-89 client tag on `KeyPackage` publications. */
  clientName?: string;
  /** Permit loopback/private relay endpoints (tests and local relays). */
  allowLoopback?: boolean;
};

const UNEXPECTED_RESULT = () => new BridgeError({ kind: "internal", site: "unexpected_result" });
const CLOSED = new BridgeError({ kind: "closed" });
const MISS = Symbol("no-result");

function readSeq(batch: Uint8Array): bigint {
  if (batch.length < 9 || batch[0] !== 1) {
    throw new DecodeError("batch", "header");
  }
  return new DataView(batch.buffer, batch.byteOffset, batch.byteLength).getBigUint64(1);
}

/**
 * A session over an opened bridge handle. Methods are serialized — at most one request is in
 * flight, matching the bridge's own one-in-flight rule.
 */
export class Engine {
  private readonly backend: Backend;
  private readonly kv: DurableKv;
  private readonly signer: Signer;
  private readonly clock: Clock;
  private readonly options: EngineOptions;
  private handle: BridgeHandle | undefined = undefined;
  private failed: BridgeError | undefined = undefined;
  private queue: Promise<void> = Promise.resolve();
  private readonly pendingCommits = new Set<Promise<void>>();
  private deferredAcks: bigint[] | undefined = undefined;
  private lastCommitSeq = 0n;

  private constructor(options: EngineOptions) {
    this.backend = options.backend;
    this.kv = options.kv;
    this.signer = options.signer;
    this.clock = options.clock;
    this.options = options;
  }

  /** Load durable rows, open the bridge, and complete the proof flow. */
  static async open(options: EngineOptions): Promise<Engine> {
    const engine = new Engine(options);
    await engine.start();
    return engine;
  }

  private async start(): Promise<void> {
    const rows = await this.kv.load();
    let payload: Uint8Array | undefined = encodeRows(rows);
    const host: BridgeHost = {
      load: () => {
        const snapshot = payload;
        payload = undefined;
        if (snapshot === undefined) {
          throw new BridgeError({ kind: "internal", site: "host_load" });
        }
        return snapshot;
      },
      commit: (batch) => {
        this.onCommit(batch);
      },
    };
    // Acks defer until the session is ready: the bridge rejects `ack_durable` while
    // `awaiting_proof`, and with an async `open` an open-time commit's `kv.commit` can resolve
    // before the open promise does — before the handle is even installed. The buffer therefore
    // goes in before `create`, not after `open`.
    const deferred: bigint[] = [];
    this.deferredAcks = deferred;
    let handle: BridgeHandle | undefined;
    try {
      handle = this.backend.create(host);
      const response = decodeOpenResponse(
        await handle.open(
          encodeConfig({
            protocolVersion: PROTOCOL_VERSION,
            account: this.signer.pubkey,
            now: this.clock.now(),
            allowLoopback: this.options.allowLoopback ?? false,
            clientName: this.options.clientName,
          }),
        ),
      );
      if (response.type === "error") {
        throw new BridgeError(response.error);
      }
      this.handle = handle;
      if (this.failed !== undefined) {
        throw this.failed;
      }
      if (response.type === "needs_account_proof") {
        const signed = await this.signer.signEvent(response.unsigned);
        const result = await this.runOp({ type: "submit_proof", event: signed });
        if (result.type !== "ready") {
          throw UNEXPECTED_RESULT();
        }
      }
      this.deferredAcks = undefined;
      for (const seq of deferred) {
        const tracked = this.ack(seq);
        this.pendingCommits.add(tracked);
        void this.prune(tracked);
      }
    } catch (error) {
      if (this.handle === handle) {
        this.handle = undefined;
      }
      try {
        await handle?.close();
      } catch {
        // The startup error takes precedence over a close failure.
      }
      throw error;
    } finally {
      this.deferredAcks = undefined;
    }
  }

  private markFailed(): void {
    if (this.failed !== undefined) {
      return;
    }
    this.failed = new BridgeError({ kind: "host_failed" });
    const { handle } = this;
    this.handle = undefined;
    if (handle !== undefined) {
      void (async () => {
        try {
          await handle.close();
        } catch {
          // The failure that marked the engine takes precedence over a close error.
        }
      })();
    }
  }

  private onCommit(batch: Uint8Array): void {
    let seq: bigint;
    try {
      seq = readSeq(batch);
    } catch {
      this.markFailed();
      return;
    }
    // Commits arrive strictly in `seq` order — a kv driver applies them in call order, so a
    // reordered batch would corrupt the durable state rather than just arrive late.
    if (seq <= this.lastCommitSeq) {
      this.markFailed();
      return;
    }
    this.lastCommitSeq = seq;
    const tracked = (async () => {
      try {
        await this.kv.commit(batch);
        await this.ack(seq);
      } catch {
        this.markFailed();
      }
    })();
    this.pendingCommits.add(tracked);
    void this.prune(tracked);
  }

  /** Drop a settled commit from the tracking set so it cannot grow with traffic. */
  private async prune(commit: Promise<void>): Promise<void> {
    await commit;
    this.pendingCommits.delete(commit);
  }

  /**
   * Send `ack_durable` outside the op queue: the bridge accepts it while an unrelated op is
   * suspended on the signer or still in flight. A non-`done` answer, a decode failure, or a
   * rejection moves the engine to failed.
   */
  private async ack(seq: bigint): Promise<void> {
    if (this.failed !== undefined) {
      return;
    }
    if (this.deferredAcks !== undefined) {
      this.deferredAcks.push(seq);
      return;
    }
    const { handle } = this;
    if (handle === undefined) {
      return;
    }
    try {
      const response = decodeResponse(
        await handle.call(encodeCall(this.clock.now(), { type: "ack_durable", seq })),
      );
      if (response.type !== "ok" || response.result.type !== "done") {
        this.markFailed();
      }
    } catch {
      this.markFailed();
    }
  }

  private async runSigner(op: SignerOp): Promise<SignerResult> {
    try {
      return await this.dispatchSigner(op);
    } catch (error) {
      if (error instanceof SignerRefusedError) {
        return { type: "refused" };
      }
      if (error instanceof SignerInvalidPayloadError) {
        return { type: "invalid_payload" };
      }
      return { type: "failed" };
    }
  }

  private async dispatchSigner(op: SignerOp): Promise<SignerResult> {
    return match(op, {
      sign_event: async (op) => ({
        type: "signed",
        event: await this.signer.signEvent(op.unsigned),
      }),
      nip44_encrypt: async (op) => ({
        type: "text",
        text: await this.signer.nip44Encrypt(op.peer, op.plaintext),
      }),
      nip44_decrypt: async (op) => ({
        type: "text",
        text: await this.signer.nip44Decrypt(op.peer, op.ciphertext),
      }),
    });
  }

  private async call(request: BridgeRequest): Promise<OpResult> {
    const { handle } = this;
    if (handle === undefined) {
      throw this.failed ?? CLOSED;
    }
    let response = decodeResponse(await handle.call(encodeCall(this.clock.now(), request)));
    while (response.type === "signer_pending") {
      const replies: SignerReply[] = [];
      for (const pending of response.requests) {
        // oxlint-disable-next-line no-await-in-loop
        replies.push({ id: pending.id, result: await this.runSigner(pending.op) });
        // A commit failure during the suspension already closed the handle.
        if (this.failed !== undefined) {
          throw this.failed;
        }
      }
      response = decodeResponse(
        // oxlint-disable-next-line no-await-in-loop
        await handle.call(encodeCall(this.clock.now(), { type: "signer_reply", replies })),
      );
    }
    if (response.type === "error") {
      throw new BridgeError(response.error);
    }
    return response.result;
  }

  private async serialized<T>(body: () => T | Promise<T>): Promise<T> {
    const prior = this.queue;
    const run = (async () => {
      await prior;
      if (this.failed !== undefined) {
        throw this.failed;
      }
      return body();
    })();
    this.queue = (async () => {
      try {
        await run;
      } catch {
        // The rejection is delivered to the op's own caller; the chain moves on.
      }
    })();
    return run;
  }

  private async runOp(request: BridgeRequest): Promise<OpResult> {
    return this.serialized(async () => this.call(request));
  }

  private async op<T>(
    request: BridgeRequest,
    pick: (result: OpResult) => T | typeof MISS,
  ): Promise<T> {
    const picked = pick(await this.runOp(request));
    if (picked === MISS) {
      throw UNEXPECTED_RESULT();
    }
    return picked;
  }

  /** Release gated outbound items for publishing. */
  async takeOutbound(): Promise<Outbound[]> {
    return this.op({ type: "take_outbound" }, (r) => (r.type === "outbound" ? r.items : MISS));
  }

  /** Report publish verdicts for one outbound item. */
  async reportPublish(id: string, results: RelayResult[]): Promise<PublishResolution> {
    return this.op({ type: "report_publish", id, results }, (r) =>
      r.type === "publish_resolution" ? r.resolution : MISS,
    );
  }

  /**
   * Drive the account `KeyPackage` slot's lifecycle against the NIP-65 write set: stage/sign/queue
   * a replacement when due, republish to gained relays, promote an acknowledged pending. Answers
   * the slot's standing.
   */
  async ensureKeyPackage(writeRelays: string[]): Promise<KeyPackageStatusResult> {
    return this.op({ type: "ensure_key_package", writeRelays }, (r) =>
      r.type === "key_package_status"
        ? {
            slot: r.slot,
            current: r.current,
            pending: r.pending,
            retained: r.retained,
            nextDueMs: r.nextDueMs,
          }
        : MISS,
    );
  }

  /**
   * Audit the account `KeyPackage` slot for publications this device did not make: `events` is the
   * host's read-back of kind-30443 for the slot, own and foreign alike. `emptyRelays` is the write
   * relays whose read of the slot completed — eose — and returned nothing (a failed, closed or
   * timed-out read must not be listed). The session stages a strictly newer replacement when a
   * foreign publication inside the skew window outranks the current one, or when a listed relay
   * accepted a day-old current publication, and answers the slot's standing.
   */
  async auditKeyPackageSlot(
    events: NostrEvent[],
    emptyRelays: string[],
  ): Promise<KeyPackageStatusResult> {
    return this.op({ type: "audit_key_package_slot", events, emptyRelays }, (r) =>
      r.type === "key_package_status"
        ? {
            slot: r.slot,
            current: r.current,
            pending: r.pending,
            retained: r.retained,
            nextDueMs: r.nextDueMs,
          }
        : MISS,
    );
  }

  /**
   * Withdraw the account `KeyPackage` slot (kind-5 to the write set), leave every group this device
   * is a non-admin member of and delete every `KeyPackage` bundle — device sign-out. The
   * withdrawal, the bundle deletion and the dropped `KeyPackage` outbox rows land in one store
   * transaction; a repeat call never stages a second kind-5.
   */
  async signOut(writeRelays: string[]): Promise<SignOutResult> {
    return this.op({ type: "sign_out", writeRelays }, (r) =>
      r.type === "sign_out" ? { left: r.left, keptAdminLeaves: r.keptAdminLeaves } : MISS,
    );
  }

  /** The account `KeyPackage` slot's lifecycle standing. */
  async keyPackageStatus(): Promise<KeyPackageStatusResult> {
    return this.op({ type: "key_package_status" }, (r) =>
      r.type === "key_package_status"
        ? {
            slot: r.slot,
            current: r.current,
            pending: r.pending,
            retained: r.retained,
            nextDueMs: r.nextDueMs,
          }
        : MISS,
    );
  }

  /** Build, sign and stage the account's kind-10050 inbox relay list. */
  async publishInboxRelays(relays: string[]): Promise<string> {
    return this.op({ type: "publish_inbox_relays", relays }, (r) =>
      r.type === "inbox_relays" ? r.outboundId : MISS,
    );
  }

  /** Create a group; resolves with the new group's full snapshot. */
  async createGroup(args: {
    name: string;
    description: string;
    relays: string[];
    invitees?: Invitee[];
    ownDevices?: Invitee[];
    admins?: string[];
    components?: ComponentUpdate[];
  }): Promise<GroupSnapshot> {
    return this.op(
      {
        type: "create_group",
        name: args.name,
        description: args.description,
        relays: args.relays,
        invitees: args.invitees ?? [],
        own_devices: args.ownDevices ?? [],
        admins: args.admins ?? [],
        components: args.components ?? [],
      },
      (r) => (r.type === "group_created" ? r.group : MISS),
    );
  }

  /** Invite accounts to a group. */
  async invite(group: string, invitees: Invitee[]): Promise<SendOutcome> {
    return this.op({ type: "invite", group, invitees }, (r) =>
      r.type === "send_outcome" ? r.outcome : MISS,
    );
  }

  /** Remove member accounts from a group. */
  async removeMembers(group: string, members: string[]): Promise<SendOutcome> {
    return this.op({ type: "remove_members", group, members }, (r) =>
      r.type === "send_outcome" ? r.outcome : MISS,
    );
  }

  /** Request to leave a group. */
  async leave(group: string): Promise<SendOutcome> {
    return this.op({ type: "leave", group }, (r) => (r.type === "send_outcome" ? r.outcome : MISS));
  }

  /** Refresh this device's own membership (self-update commit). */
  async selfUpdate(group: string): Promise<SendOutcome> {
    return this.op({ type: "self_update", group }, (r) =>
      r.type === "send_outcome" ? r.outcome : MISS,
    );
  }

  /** Report the group's post-join history fetch completed (EOSE on every subscribed relay). */
  async historyFetched(group: string): Promise<void> {
    return this.op({ type: "history_fetched", group }, (r) =>
      r.type === "done" ? undefined : MISS,
    );
  }

  /** Update the group profile; `null` leaves a field unchanged. */
  async updateGroupProfile(
    group: string,
    name: string | undefined,
    description: string | undefined,
  ): Promise<SendOutcome> {
    return this.op({ type: "update_group_profile", group, name, description }, (r) =>
      r.type === "send_outcome" ? r.outcome : MISS,
    );
  }

  /** Apply app-component updates. */
  async updateAppComponents(group: string, updates: ComponentUpdate[]): Promise<SendOutcome> {
    return this.op({ type: "update_app_components", group, updates }, (r) =>
      r.type === "send_outcome" ? r.outcome : MISS,
    );
  }

  /** Send an application message. */
  async sendMessage(group: string, message: AppMessage): Promise<SentMessage> {
    return this.op({ type: "send_message", group, message }, (r) =>
      r.type === "message_sent" ? { outcome: r.outcome, message: r.message } : MISS,
    );
  }

  /** Send a typed application message (`MessageIntent`). */
  async sendIntent(group: string, intent: MessageIntent): Promise<SentMessage> {
    return this.op({ type: "send_intent", group, intent }, (r) =>
      r.type === "message_sent" ? { outcome: r.outcome, message: r.message } : MISS,
    );
  }

  /** Grant or revoke a member's admin flag. */
  async setAdmin(group: string, account: string, admin: boolean): Promise<SendOutcome> {
    return this.op({ type: "set_admin", group, account, admin }, (r) =>
      r.type === "send_outcome" ? r.outcome : MISS,
    );
  }

  /** Set or clear the group's avatar component; `undefined` clears. */
  async setGroupAvatar(group: string, avatar: GroupAvatar | undefined): Promise<SendOutcome> {
    return this.op({ type: "set_group_avatar", group, avatar }, (r) =>
      r.type === "send_outcome" ? r.outcome : MISS,
    );
  }

  /** Set the group's message retention seconds; `0` disables. */
  async setRetention(group: string, seconds: bigint): Promise<SendOutcome> {
    return this.op({ type: "set_retention", group, seconds }, (r) =>
      r.type === "send_outcome" ? r.outcome : MISS,
    );
  }

  /** Request the group's terminal disband. */
  async disband(group: string): Promise<SendOutcome> {
    return this.op({ type: "disband", group }, (r) =>
      r.type === "send_outcome" ? r.outcome : MISS,
    );
  }

  /** Require the lifecycle component on a group that predates it. */
  async enableDisbanding(group: string): Promise<SendOutcome> {
    return this.op({ type: "enable_disbanding", group }, (r) =>
      r.type === "send_outcome" ? r.outcome : MISS,
    );
  }

  /** Forget the group locally; the forgotten-group tombstone stays. */
  async forgetGroup(group: string): Promise<void> {
    return this.op({ type: "forget_group", group }, (r) => (r.type === "done" ? undefined : MISS));
  }

  /** The group's stored replacement-Welcome offers awaiting consent. */
  async rejoinOffers(group: string): Promise<RejoinOffer[]> {
    return this.op({ type: "rejoin_offers", group }, (r) =>
      r.type === "rejoin_offers" ? r.offers : MISS,
    );
  }

  /** Consent to a stored replacement Welcome — installs the new copy. */
  async confirmRejoin(group: string, offer: string, token: string): Promise<void> {
    return this.op({ type: "confirm_rejoin", group, offer, token }, (r) =>
      r.type === "done" ? undefined : MISS,
    );
  }

  /** Decline a stored replacement Welcome — retires the offer. */
  async declineRejoin(offer: string): Promise<void> {
    return this.op({ type: "decline_rejoin", offer }, (r) =>
      r.type === "done" ? undefined : MISS,
    );
  }

  /** Remove a stuck member account, then re-invite it once the removal confirms. */
  async readdMember(group: string, account: string, invitees: Invitee[]): Promise<ReaddOutcome> {
    return this.op({ type: "readd_member", group, account, invitees }, (r) =>
      r.type === "readd" ? r.outcome : MISS,
    );
  }

  /** Encrypt an attachment and pin it to the current media epoch. */
  async sealAttachment(
    group: string,
    attachment: AttachmentInput,
  ): Promise<SealedAttachmentResult> {
    return this.op({ type: "seal_attachment", group, attachment }, (r) =>
      r.type === "sealed_attachment"
        ? {
            ciphertext: r.ciphertext,
            ciphertextSha256: r.ciphertextSha256,
            handle: r.handle,
          }
        : MISS,
    );
  }

  /** Send a media message carrying sealed attachments. */
  async sendMedia(
    group: string,
    caption: string,
    attachments: MediaAttachmentRef[],
    tags: string[][] = [],
  ): Promise<SentMessage> {
    return this.op({ type: "send_media", group, caption, attachments, tags }, (r) =>
      r.type === "message_sent" ? { outcome: r.outcome, message: r.message } : MISS,
    );
  }

  /** Drop one sealed attachment handle. */
  async discardAttachment(handle: number): Promise<void> {
    return this.op({ type: "discard_attachment", handle }, (r) =>
      r.type === "done" ? undefined : MISS,
    );
  }

  /** Decrypt a media attachment reference. */
  async openAttachment(
    group: string,
    message: string,
    reference: MediaReference,
    ciphertext: Uint8Array,
  ): Promise<Uint8Array> {
    return this.op({ type: "open_attachment", group, message, reference, ciphertext }, (r) =>
      r.type === "attachment" ? r.plaintext : MISS,
    );
  }

  /** Admit inbound transport events; resolves with per-input reports. */
  async ingest(events: NostrEvent[]): Promise<InputReport[]> {
    return this.op({ type: "ingest", events }, (r) => (r.type === "ingested" ? r.reports : MISS));
  }

  /** Run one maintenance/convergence pass; ms until the next wake or null. */
  async tick(): Promise<bigint | undefined> {
    return this.op({ type: "tick" }, (r) => (r.type === "ticked" ? r.nextWakeMs : MISS));
  }

  /** Drain queued session events. */
  async drain(): Promise<SessionEvent[]> {
    return this.serialized(async () => {
      const { handle } = this;
      if (handle === undefined) {
        throw CLOSED;
      }
      const response = decodeDrain(await handle.drain());
      if (response.type === "error") {
        throw new BridgeError(response.error);
      }
      return response.events;
    });
  }

  /** Acknowledge delivered application messages (at-least-once). */
  async acknowledgeMessages(ids: string[]): Promise<void> {
    return this.op({ type: "acknowledge_messages", ids }, (r) =>
      r.type === "done" ? undefined : MISS,
    );
  }

  /**
   * Snapshot every stored local copy — live and terminated (`removed`, `disbanded`) alike — in
   * group-id byte order. A terminated copy stays listed until `forgetGroup`; hosts that want only
   * live chats filter on `state`/`access`.
   */
  async groups(): Promise<GroupSnapshot[]> {
    return this.op({ type: "groups" }, (r) => (r.type === "groups" ? r.groups : MISS));
  }

  /** Snapshot one group, or `null` when the device holds no copy of it. */
  async group(id: string): Promise<GroupSnapshot | undefined> {
    return this.op({ type: "group", group: id }, (r) => (r.type === "group" ? r.group : MISS));
  }

  /**
   * Select `account`'s key packages from its fetched kind-30443 publications and kind-5 deletion
   * requests, ranked best first. `group` filters by that group's invite requirements; without one,
   * creation requirements apply.
   */
  async selectKeyPackages(
    account: string,
    events: NostrEvent[],
    group?: string,
  ): Promise<NostrEvent[]> {
    return this.op({ type: "select_key_packages", account, events, group }, (r) =>
      r.type === "key_packages" ? r.events : MISS,
    );
  }

  /** The durability gate's outstanding watermark, or null when idle. */
  async awaitingDurability(): Promise<bigint | undefined> {
    return this.op({ type: "awaiting_durability" }, (r) =>
      r.type === "awaiting_durability" ? r.seq : MISS,
    );
  }

  /** Resolve once every commit issued so far has been acked; rejects if the engine failed. */
  async whenDurable(): Promise<void> {
    await Promise.all(this.pendingCommits);
    if (this.failed !== undefined) {
      throw this.failed;
    }
  }

  /** Wait for in-flight ops and commits, then close the handle. */
  async close(): Promise<void> {
    const prior = this.queue;
    const run = (async () => {
      await prior;
      if (this.handle === undefined) {
        return;
      }
      await Promise.all(this.pendingCommits);
      const { handle } = this;
      this.handle = undefined;
      await handle?.close();
    })();
    this.queue = (async () => {
      try {
        await run;
      } catch {
        // The rejection is delivered to close's own caller; the chain moves on.
      }
    })();
    await run;
  }
}
