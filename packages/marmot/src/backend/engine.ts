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
import type { Backend, BridgeHandle, BridgeHost } from "./port.ts";
import type {
  AppMessage,
  AttachmentInput,
  BridgeRequest,
  ClockReading,
  ComponentUpdate,
  GroupSnapshot,
  InputReport,
  Invitee,
  KeyPackageResult,
  MediaAttachmentRef,
  MediaReference,
  NostrEvent,
  OpResult,
  Outbound,
  PublishResolution,
  RelayResult,
  SealedAttachmentResult,
  SendOutcome,
  SessionEvent,
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

/** The account-signer surface the engine drives on `signer_pending`. */
export type Signer = {
  /** The account this session signs for, 32-byte hex. */
  readonly pubkey: string;
  signEvent: (unsigned: UnsignedEvent) => Promise<NostrEvent>;
  nip44Encrypt: (peer: string, plaintext: string) => Promise<string>;
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

/**
 * Statically exhaustive dispatch on a `type`-tagged union: `cases` must provide an arm for every
 * variant, and each arm receives the value narrowed to that variant.
 */
function match<T extends { readonly type: string }, R>(
  value: T,
  cases: { [K in T["type"]]: (variant: Extract<T, { type: K }>) => R },
): R {
  // oxlint-disable-next-line no-unsafe-type-assertion -- the table maps each tag to an arm narrowed to that variant
  const arms = cases as unknown as Record<string, (variant: T) => R>;
  const arm = arms[value.type];
  if (arm === undefined) {
    throw new DecodeError("match", "unreachable");
  }
  return arm(value);
}

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
    const payload = encodeRows(rows);
    const host: BridgeHost = {
      load: () => payload,
      commit: (batch) => {
        this.onCommit(batch);
      },
    };
    const handle = this.backend.create(host);
    const response = decodeOpenResponse(
      handle.open(
        encodeConfig({
          protocolVersion: PROTOCOL_VERSION,
          account: this.signer.pubkey,
          now: this.clock.now(),
          allowLoopback: this.options.allowLoopback ?? false,
          clientName: this.options.clientName ?? undefined,
        }),
      ),
    );
    if (response.type === "error") {
      handle.close();
      throw new BridgeError(response.error);
    }
    this.handle = handle;
    if (response.type === "needs_account_proof") {
      const signed = await this.signer.signEvent(response.unsigned);
      const result = await this.runOp({ type: "submit_proof", event: signed });
      if (result.type !== "ready") {
        this.handle = undefined;
        handle.close();
        throw UNEXPECTED_RESULT();
      }
    }
  }

  private markFailed(): void {
    if (this.failed !== undefined) {
      return;
    }
    this.failed = new BridgeError({ kind: "host_failed" });
    const { handle } = this;
    this.handle = undefined;
    handle?.close();
  }

  private onCommit(batch: Uint8Array): void {
    let seq: bigint;
    try {
      seq = readSeq(batch);
    } catch {
      this.markFailed();
      return;
    }
    const tracked = (async () => {
      try {
        await this.kv.commit(batch);
        this.ack(seq);
      } catch {
        this.markFailed();
      }
    })();
    this.pendingCommits.add(tracked);
  }

  /**
   * Send `ack_durable` outside the op queue: the bridge accepts it while an unrelated op is
   * suspended on the signer. A non-`done` answer or a decode failure moves the engine to failed.
   */
  private ack(seq: bigint): void {
    if (this.failed !== undefined) {
      return;
    }
    const { handle } = this;
    if (handle === undefined) {
      return;
    }
    try {
      const response = decodeResponse(
        handle.call(encodeCall(this.clock.now(), { type: "ack_durable", seq })),
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
      return error instanceof SignerRefusedError ? { type: "refused" } : { type: "failed" };
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
    let response = decodeResponse(handle.call(encodeCall(this.clock.now(), request)));
    while (response.type === "signer_pending") {
      const replies: SignerReply[] = [];
      for (const pending of response.requests) {
        // oxlint-disable-next-line no-await-in-loop
        replies.push({ id: pending.id, result: await this.runSigner(pending.op) });
      }
      response = decodeResponse(
        handle.call(encodeCall(this.clock.now(), { type: "signer_reply", replies })),
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

  /** Build, sign and stage the account's kind-30443 `KeyPackage`. */
  async publishKeyPackage(relays: string[]): Promise<KeyPackageResult> {
    return this.op({ type: "publish_key_package", relays }, (r) =>
      r.type === "key_package"
        ? {
            keyPackage: r.keyPackage,
            keyPackageRef: r.keyPackageRef,
            eventId: r.eventId,
            account: r.account,
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
  async sendMessage(group: string, message: AppMessage): Promise<SendOutcome> {
    return this.op({ type: "send_message", group, message }, (r) =>
      r.type === "send_outcome" ? r.outcome : MISS,
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
  ): Promise<SendOutcome> {
    return this.op({ type: "send_media", group, caption, attachments, tags }, (r) =>
      r.type === "send_outcome" ? r.outcome : MISS,
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
    return this.serialized(() => {
      const { handle } = this;
      if (handle === undefined) {
        throw CLOSED;
      }
      const response = decodeDrain(handle.drain());
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

  /** Snapshot every live group. */
  async groups(): Promise<GroupSnapshot[]> {
    return this.op({ type: "groups" }, (r) => (r.type === "groups" ? r.groups : MISS));
  }

  /** Snapshot one group, or `null` when not live. */
  async group(id: string): Promise<GroupSnapshot | undefined> {
    return this.op({ type: "group", group: id }, (r) => (r.type === "group" ? r.group : MISS));
  }

  /** The durability gate's outstanding watermark, or null when idle. */
  async awaitingDurability(): Promise<bigint | undefined> {
    return this.op({ type: "awaiting_durability" }, (r) =>
      r.type === "awaiting_durability" ? r.seq : MISS,
    );
  }

  /** Resolve once every commit issued so far has been acked (or failed). */
  async whenDurable(): Promise<void> {
    const pending = [...this.pendingCommits];
    await Promise.all(pending);
    for (const entry of pending) {
      this.pendingCommits.delete(entry);
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
      handle?.close();
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
