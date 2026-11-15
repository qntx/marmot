// Local member driver: wraps the wasm Client directly. Used by the Bun
// driver; the Chromium page host (web/member-host.js) ports the same logic
// with an IndexedDB backend.

import {
  SecretKey,
  Keys,
  signEvent,
  finalizeEvent,
  hexToBytes,
  bytesToHex,
} from "@qntx/nostr";
import * as nip44 from "@qntx/nostr/nips/nip44";
import type { RelayEvent } from "./relay.js";
import type {
  CommitOp,
  KvBackend,
  MemberDriver,
  SendResult,
  IngestOutcome,
  DrainedEvent,
  ConvergencePass,
} from "./member.js";

/** Deterministic account key from a seed label (sha256 via WebCrypto). */
export async function accountKeys(seed: string): Promise<Keys> {
  const digest = await crypto.subtle.digest(
    "SHA-256",
    new TextEncoder().encode(`marmot-spike-account:${seed}`),
  );
  return Keys.fromSecretKey(new Uint8Array(digest));
}

/** The JS signer object the wasm Client expects — the key never enters wasm. */
export function makeJsSigner(keys: Keys) {
  const sk = keys.secretKey.bytes;
  return {
    publicKey(): string {
      return keys.publicKey;
    },
    // SYNC hook: the engine calls this inside fresh_key_package/self-update.
    signAccountIdentityProof(bytes: Uint8Array): Uint8Array {
      const unsigned = JSON.parse(new TextDecoder().decode(bytes));
      const signed = signEvent(unsigned, keys.secretKey);
      return hexToBytes(signed.sig);
    },
    async signEvent(unsignedJson: string): Promise<string> {
      const unsigned = JSON.parse(unsignedJson);
      return JSON.stringify(signEvent(unsigned, keys.secretKey));
    },
    async nip44Encrypt(peer: string, plaintext: string): Promise<string> {
      return nip44.encryptToPubkey(plaintext, sk, peer);
    },
    async nip44Decrypt(peer: string, payload: string): Promise<string> {
      return nip44.decryptFromPubkey(payload, sk, peer);
    },
  };
}

/** Sign a kind-30443 KeyPackage event the wasm side prepared. */
export function finalizeKeyPackage(
  unsignedFields: {
    kind: number;
    created_at: number;
    content: string;
    tags: string[][];
  },
  keys: Keys,
): RelayEvent {
  return finalizeEvent(
    {
      kind: unsignedFields.kind,
      created_at: unsignedFields.created_at,
      tags: unsignedFields.tags,
      content: unsignedFields.content,
    },
    keys.secretKey,
  ) as unknown as RelayEvent;
}

export interface LocalMemberOpts {
  Client: any; // the wasm-bindgen Client class
  keys: Keys;
  backend: KvBackend;
  nowMs?: number;
  bareCapabilities?: boolean;
  omitComponents?: number[];
}

/**
 * A member = wasm Client over a KvBackend. The commit queue preserves the
 * publish-before-apply rule: onCommit is sync and only enqueues; flush()
 * awaits durability and is awaited before returning publishable results.
 */
export class LocalMember implements MemberDriver {
  client: any;
  backend: KvBackend;
  keys: Keys;
  private pendingCommits: Promise<void> = Promise.resolve();
  private Client: any;
  private nowMs?: number;
  private bareCapabilities?: boolean;
  private omitComponents?: number[];
  readonly timings: Record<string, { n: number; ms: number }> = {};

  private async timed<T>(name: string, f: () => Promise<T>): Promise<T> {
    const t0 = performance.now();
    try {
      return await f();
    } finally {
      const t = (this.timings[name] ??= { n: 0, ms: 0 });
      t.n++;
      t.ms += performance.now() - t0;
    }
  }

  private constructor(opts: LocalMemberOpts) {
    this.Client = opts.Client;
    this.keys = opts.keys;
    this.backend = opts.backend;
    this.nowMs = opts.nowMs;
    this.bareCapabilities = opts.bareCapabilities;
    this.omitComponents = opts.omitComponents;
  }

  static async open(opts: LocalMemberOpts): Promise<LocalMember> {
    const m = new LocalMember(opts);
    const entries = await opts.backend.loadAll();
    const onCommit = (batch: CommitOp[]) => {
      m.pendingCommits = m.pendingCommits.then(() => opts.backend.commit(batch));
    };
    m.client = opts.Client.open({
      entries,
      onCommit,
      signer: makeJsSigner(opts.keys),
      nowMs: opts.nowMs,
      bareCapabilities: opts.bareCapabilities,
      omitComponents: opts.omitComponents,
    });
    await m.flush();
    return m;
  }

  /** Await every queued commit batch — call before publishing events. */
  async flush(): Promise<void> {
    await this.pendingCommits;
  }

  async restart(): Promise<void> {
    const entries = await this.backend.loadAll();
    const onCommit = (batch: CommitOp[]) => {
      this.pendingCommits = this.pendingCommits.then(() =>
        this.backend.commit(batch),
      );
    };
    this.client = this.Client.open({
      entries,
      onCommit,
      signer: makeJsSigner(this.keys),
      nowMs: this.nowMs,
    });
    await this.flush();
  }

  async publishKeyPackage(): Promise<RelayEvent> {
    const fields = await this.client.publishKeyPackage();
    await this.flush();
    return finalizeKeyPackage(fields, this.keys);
  }

  async createGroup(
    name: string,
    relays: string[],
    keyPackageEvents: RelayEvent[],
  ): Promise<{ groupId: string; result: SendResult }> {
    const out = await this.client.createGroup({ name, relays, keyPackageEvents });
    await this.flush();
    const { groupId, ...result } = out;
    return { groupId, result };
  }

  async send(groupId: string, intent: unknown): Promise<SendResult> {
    const out = await this.timed("send", () => this.client.send({ groupId, intent }));
    await this.flush();
    return out;
  }

  async ingest(ev: RelayEvent): Promise<IngestOutcome> {
    const out = await this.timed("ingest", () => this.client.ingest(ev));
    await this.flush();
    return out;
  }

  async advance(groupId: string): Promise<ConvergencePass> {
    const out = await this.timed("advance", () => this.client.advanceConvergence(groupId));
    await this.flush();
    return out;
  }

  async confirmPublished(pending: unknown): Promise<void> {
    await this.timed("confirmPublished", () => this.client.confirmPublished(pending));
    await this.flush();
  }

  async publishFailed(pending: unknown): Promise<void> {
    await this.timed("publishFailed", () => this.client.publishFailed(pending));
    await this.flush();
  }

  async drainEvents(): Promise<DrainedEvent[]> {
    const out = await this.timed("drainEvents", () => this.client.drainEvents());
    await this.flush();
    return out;
  }

  async members(groupId: string): Promise<string[]> {
    return this.client.members(groupId);
  }

  async epoch(groupId: string): Promise<number> {
    return this.client.epoch(groupId);
  }

  async memberId(): Promise<string> {
    return this.client.memberId();
  }

  wasmMemoryBytes(): number {
    return this.client.wasmMemoryBytes();
  }
}
