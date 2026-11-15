// Shared member driver: the "SDK" layer above the wasm Client, identical
// semantics to spike/engine/src/harness.rs. Used directly by the Bun driver
// and by the Chromium page host (member-host.js ports this same logic).

import type { RelayEvent } from "./relay.js";

export interface CommitOp {
  op: "put" | "delete";
  key: string;
  value?: string;
}

export interface KvBackend {
  /** All persisted entries at open (hex key/value pairs). */
  loadAll(): Promise<[string, string][]>;
  /** Apply one atomic batch; must be durable before publish. */
  commit(batch: CommitOp[]): Promise<void>;
}

export class MapBackend implements KvBackend {
  map = new Map<string, string>();
  batches = 0;

  constructor(entries?: [string, string][]) {
    if (entries) for (const [k, v] of entries) this.map.set(k, v);
  }

  async loadAll(): Promise<[string, string][]> {
    return [...this.map.entries()];
  }

  async commit(batch: CommitOp[]): Promise<void> {
    for (const op of batch) {
      if (op.op === "put") this.map.set(op.key, op.value!);
      else this.map.delete(op.key);
    }
    this.batches++;
  }

  snapshot(): [string, string][] {
    return [...this.map.entries()];
  }
}

export interface SendResult {
  type: string;
  events: RelayEvent[];
  pending?: unknown;
  debug?: string;
}

export interface IngestOutcome {
  outcome: string;
  category?: string;
}

export interface DrainedEvent {
  type: string;
  groupId?: string;
  messageId?: string;
  sender?: string;
  epoch?: number;
  payload?: string;
  decoded?: {
    author: string;
    createdAt: number;
    kind: number;
    content: string;
  };
}

export interface ConvergencePass {
  settled: boolean;
  pendingGroups: number;
  results: SendResult[];
}

/** The uniform driver the scenario drives — local wasm or a browser page. */
export interface MemberDriver {
  restart(): Promise<void>;
  publishKeyPackage(): Promise<RelayEvent>;
  createGroup(
    name: string,
    relays: string[],
    keyPackageEvents: RelayEvent[],
  ): Promise<{ groupId: string; result: SendResult }>;
  send(groupId: string, intent: unknown): Promise<SendResult>;
  ingest(ev: RelayEvent): Promise<IngestOutcome>;
  advance(groupId: string): Promise<ConvergencePass>;
  confirmPublished(pending: unknown): Promise<void>;
  publishFailed(pending: unknown): Promise<void>;
  drainEvents(): Promise<DrainedEvent[]>;
  members(groupId: string): Promise<string[]>;
  epoch(groupId: string): Promise<number>;
  memberId(): Promise<string>;
}

/** Aggregate ingest counts, matching the Rust harness's IngestCounts. */
export class IngestCounts {
  processed = 0;
  buffered = 0;
  deferred = 0;
  ownEcho = 0;
  ignored = 0;
  local = 0;
  stale = 0;
  rejected = 0;
  other = 0;

  add(o: IngestOutcome) {
    switch (o.outcome) {
      case "processed":
        this.processed++;
        break;
      case "buffered":
        this.buffered++;
        break;
      case "deferred":
        this.deferred++;
        break;
      case "ignored":
        if (o.category === "ownEcho") this.ownEcho++;
        else this.ignored++;
        break;
      case "localState":
        this.local++;
        break;
      case "stale":
        this.stale++;
        break;
      case "rejected":
        this.rejected++;
        break;
      default:
        this.other++;
    }
  }

  line(): string {
    return `processed=${this.processed} buffered=${this.buffered} deferred=${this.deferred} own_echo=${this.ownEcho} ignored=${this.ignored} local=${this.local} stale=${this.stale} rejected=${this.rejected} other=${this.other}`;
  }
}

/** publish_send_result port: publish every event, then resolve the pending. */
export async function publishResult(
  member: MemberDriver,
  result: SendResult,
  publish: (ev: RelayEvent) => boolean,
): Promise<{ confirmed: boolean; pending?: unknown }> {
  let published = true;
  for (const ev of result.events) {
    if (!publish(ev)) published = false;
  }
  if (result.pending !== undefined) {
    if (published) {
      await member.confirmPublished(result.pending);
      return { confirmed: true, pending: result.pending };
    }
    await member.publishFailed(result.pending);
    return { confirmed: false, pending: result.pending };
  }
  return { confirmed: published, pending: result.pending };
}

/** flush_outbound port: drive convergence until no publishable work remains. */
export async function flushOutbound(
  member: MemberDriver,
  gid: string,
  publish: (ev: RelayEvent) => boolean,
): Promise<number> {
  let published = 0;
  for (let i = 0; i < 16; i++) {
    const pass = await member.advance(gid);
    for (const result of pass.results) {
      for (const ev of result.events) {
        if (publish(ev)) published++;
      }
      if (result.pending !== undefined) {
        await member.confirmPublished(result.pending);
      }
    }
    if (pass.settled && pass.pendingGroups === 0 && pass.results.length === 0) {
      break;
    }
  }
  return published;
}
