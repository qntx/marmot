/**
 * `MarmotClient` — the application surface: one `Engine` behind the `Pump`, typed errors, typed
 * events. Public ops run through the pump; relay lookups run outside it; every relay URL passes
 * `safeRelayUrl` before it can open a connection.
 */

import { Engine } from "../backend/engine.ts";
import type { Clock, Signer } from "../backend/engine.ts";
import type { Backend } from "../backend/port.ts";
import type {
  AppMessage,
  AttachmentInput,
  ComponentUpdate,
  GroupSnapshot,
  Invitee,
  MediaReference,
  SendOutcome,
} from "../backend/protocol.ts";
import * as groups from "../groups/index.ts";
import type { UploadPort } from "../groups/index.ts";
import { observeSigner } from "../invites/index.ts";
import type { InviteEvent } from "../invites/index.ts";
import { lookupInvitees, writeRelaysOf } from "../keys/index.ts";
import type { CursorStore, KvDriver } from "../storage/driver.ts";
import type { RelayPort, Timer } from "../transport/index.ts";
import { safeRelayUrl } from "../transport/index.ts";
import { MarmotError, toMarmotError } from "./errors.ts";
import type { MarmotErrorKind } from "./errors.ts";
import { Pump } from "./pump.ts";
import type { ClientMessage, DeliveryEvent, GroupEvent } from "./pump.ts";
import { decodeClientState } from "./state.ts";
import type { ClientState } from "./state.ts";

export type MarmotClientOptions = {
  backend: Backend;
  kv: KvDriver;
  cursors: CursorStore;
  signer: Signer;
  clock: Clock;
  timer: Timer;
  relays: RelayPort;
  allowLoopback?: boolean;
};

type CreateGroupArgs = Parameters<Engine["createGroup"]>[0];

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
      takeVerdict: observed.take,
    });
  }

  static async open(options: MarmotClientOptions): Promise<MarmotClient> {
    const observed = observeSigner(options.signer);
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
    let state: ClientState;
    try {
      state = decodeClientState(await options.cursors.load());
    } catch {
      state = { cursors: {}, inbox: [] };
    }
    const client = new MarmotClient(options, engine, observed, state);
    client.pump.start();
    return client;
  }

  readonly account: string;

  async close(): Promise<void> {
    this.closing = true;
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

  onInvite(listener: (event: InviteEvent) => void): () => void {
    return this.pump.onInvite(listener);
  }

  onDelivery(listener: (event: DeliveryEvent) => void): () => void {
    return this.pump.onDelivery(listener);
  }

  onError(listener: (error: MarmotError) => void): () => void {
    return this.pump.onError(listener);
  }

  private guard(): void {
    if (this.closing) {
      throw new MarmotError("closed");
    }
  }

  private async op<T>(job: (engine: Engine) => Promise<T>): Promise<T> {
    this.guard();
    return this.pump.run(async () => {
      // Reset, then run: a verdict belongs to the job that observed the signer call. Taking it
      // inside the serialized body keeps a later ingest from stealing it for a `blocked` invite.
      this.observed.take();
      try {
        return await job(this.engine);
      } catch (error) {
        throw toMarmotError(error, this.observed.take());
      }
    });
  }

  /** Publish the account KeyPackage to its NIP-65 write relays discovered via `lookupRelays`. */
  async publishKeyPackage(lookupRelays: string[]): Promise<void> {
    this.guard();
    const safe = lookupRelays.filter((relay) => safeRelayUrl(relay, this.allowLoopback));
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
    await this.op(async (engine) => {
      await engine.publishKeyPackage(write);
    });
  }

  /**
   * Publish the account's NIP-17 inbox relay list; every relay must pass `safeRelayUrl`. Persists
   * the list so reconcile subscribes for gift wraps.
   */
  async publishInboxRelays(relays: string[]): Promise<void> {
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

  /** Resolve an account's published KeyPackages for inviting; non-found maps to a typed error. */
  async lookupInvitees(pubkey: string, lookupRelays: string[]): Promise<Invitee[]> {
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

  async createGroup(args: CreateGroupArgs): Promise<GroupSnapshot> {
    for (const relay of args.relays) {
      if (!safeRelayUrl(relay, this.allowLoopback)) {
        throw new MarmotError("unsafe_relay");
      }
    }
    return this.op(async (engine) => groups.createGroup(engine, args));
  }

  async invite(group: string, invitees: Invitee[]): Promise<SendOutcome> {
    return this.op(async (engine) => groups.invite(engine, group, invitees));
  }

  async removeMembers(group: string, members: string[]): Promise<SendOutcome> {
    return this.op(async (engine) => groups.removeMembers(engine, group, members));
  }

  async leave(group: string): Promise<SendOutcome> {
    return this.op(async (engine) => groups.leave(engine, group));
  }

  async selfUpdate(group: string): Promise<SendOutcome> {
    return this.op(async (engine) => groups.selfUpdate(engine, group));
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

  async sendMessage(group: string, message: AppMessage): Promise<SendOutcome> {
    return this.op(async (engine) => groups.sendMessage(engine, group, message));
  }

  /**
   * Seal in the pump, upload outside it, send back in it. An upload failure discards every sealed
   * handle before rejecting `upload_failed`.
   */
  async sendMedia(group: string, media: MediaInput, upload: UploadPort): Promise<SendOutcome> {
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

  async groups(): Promise<GroupSnapshot[]> {
    return this.op(async (engine) => engine.groups());
  }

  async group(id: string): Promise<GroupSnapshot | undefined> {
    return this.op(async (engine) => engine.group(id));
  }
}
