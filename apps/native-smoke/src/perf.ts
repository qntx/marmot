/**
 * `perf` mode: the M4 measurements on the real native backend, Release builds only. Each case
 * posts one result; long cases also post `milestone` journal records (progress, and the points
 * where the host driver samples process memory).
 *
 * Measured spans are `performance.now()` deltas around the named call only — setup, publish loops
 * and drains sit outside the span unless a field says otherwise. Members whose storage is not
 * under measurement (KeyPackage-only members) use `memoryKv`; every measured member uses
 * `sqliteKv` over op-sqlite with the chacha cipher, like the flow mode.
 */

import { BridgeError, Engine } from "@qntx/marmot/backend";
import type { NostrEvent, SendOutcome, Signer } from "@qntx/marmot/backend";
import type { KvDriver } from "@qntx/marmot/storage";
import { localLocks, sqliteKv } from "@qntx/marmot/storage";
import { fakeRelay, manualClock, memoryKv } from "@qntx/marmot/testing";
import type { FakeRelay, ManualClock } from "@qntx/marmot/testing";
import { load } from "@qntx/marmot-native";

import { hexToBytes } from "./b64.ts";
import { chachaCipher } from "./cipher.ts";
import { post } from "./collector.ts";
import { openDb } from "./kv.ts";
import type { RunParams } from "./params.ts";
import { testSigner } from "./signer.ts";

const RELAY = "wss://relay.example";

type Member = {
  name: string;
  engine: Engine;
  clock: ManualClock;
  signer: Signer;
  /** Wall-clock ms spent inside this member's signer callbacks so far. */
  signerMs: () => number;
};

type Stats = { n: number; min: number; median: number; p90: number; max: number; mean: number };

function stats(values: number[]): Stats {
  const sorted = [...values].sort((x, y) => x - y);
  const at = (q: number): number => sorted[Math.min(sorted.length - 1, Math.floor(q * sorted.length))] ?? 0;
  const sum = sorted.reduce((acc, value) => acc + value, 0);
  return {
    n: sorted.length,
    min: sorted[0] ?? 0,
    median: at(0.5),
    p90: at(0.9),
    max: sorted.at(-1) ?? 0,
    mean: sorted.length === 0 ? 0 : sum / sorted.length,
  };
}

const round = (ms: number): number => Math.round(ms * 100) / 100;

/** A nostr-tools signer that accumulates the wall time spent inside its callbacks. */
function timedSigner(secret?: Uint8Array): { signer: Signer; spentMs: () => number } {
  const inner = testSigner(secret);
  let spent = 0;
  const timed =
    <A extends unknown[], R>(body: (...args: A) => Promise<R>) =>
    async (...args: A): Promise<R> => {
      const started = performance.now();
      try {
        return await body(...args);
      } finally {
        spent += performance.now() - started;
      }
    };
  return {
    spentMs: () => spent,
    signer: {
      pubkey: inner.pubkey,
      signEvent: timed(inner.signEvent),
      nip44Encrypt: timed(inner.nip44Encrypt),
      nip44Decrypt: timed(inner.nip44Decrypt),
    },
  };
}

class Bench {
  readonly relay: FakeRelay = fakeRelay();
  private readonly backend = load();
  private readonly cipher = chachaCipher(crypto.getRandomValues(new Uint8Array(32)));
  private readonly lock = localLocks();
  private readonly drivers: KvDriver[] = [];

  constructor(private readonly run: string) {}

  async sqlite(name: string, key?: Uint8Array): Promise<KvDriver> {
    const db = `${this.run}-${name}`;
    const driver = await sqliteKv(openDb(db), {
      name: db,
      lock: this.lock,
      cipher: key === undefined ? this.cipher : chachaCipher(key),
    });
    this.drivers.push(driver);
    return driver;
  }

  async member(
    name: string,
    storage: "sqlite" | "memory",
    options: { secret?: Uint8Array; key?: Uint8Array; clock?: ManualClock } = {},
  ): Promise<Member> {
    const { signer, spentMs } = timedSigner(options.secret);
    const clock = options.clock ?? manualClock();
    const kv = storage === "sqlite" ? await this.sqlite(name, options.key) : memoryKv();
    const engine = await Engine.open({ backend: this.backend, kv, signer, clock, allowLoopback: true });
    return { name, engine, clock, signer, signerMs: spentMs };
  }

  /** The host delivery loop: publish everything gated, drain the sender's events. */
  async publish(member: Member): Promise<number> {
    let published = 0;
    for (let pass = 0; pass < 64; pass += 1) {
      await member.engine.whenDurable();
      const items = await member.engine.takeOutbound();
      if (items.length === 0) {
        break;
      }
      for (const item of items) {
        await member.engine.reportPublish(item.id, this.relay.publish(item));
        published += 1;
      }
    }
    await this.drainAck(member);
    return published;
  }

  /** Drain events and acknowledge delivered messages; returns the delivered count. */
  async drainAck(member: Member): Promise<number> {
    const drained = await member.engine.drain();
    const ids = drained.flatMap((event) =>
      event.type === "message_received" ? [event.message.transportId] : [],
    );
    if (ids.length > 0) {
      await member.engine.acknowledgeMessages(ids);
    }
    return ids.length;
  }

  async identity(member: Member): Promise<void> {
    await member.engine.ensureKeyPackage([RELAY]);
    await this.publish(member);
    await member.engine.publishInboxRelays([RELAY]);
    await this.publish(member);
  }

  /** Ingest what the relay holds for `member` on `route`, tick, advance 2 s, publish — to quiescence. */
  async sync(member: Member, route: string): Promise<void> {
    for (let pass = 0; pass < 64; pass += 1) {
      const fresh = this.relay.groupEvents(route, member.name);
      if (fresh.length > 0) {
        await member.engine.ingest(fresh);
        await this.drainAck(member);
      }
      const wake = await member.engine.tick();
      member.clock.advance(2000);
      const published = await this.publish(member);
      if (fresh.length === 0 && published === 0 && !(wake !== undefined && wake <= 2000n)) {
        return;
      }
    }
    throw new Error(`sync for ${member.name} did not quiesce`);
  }

  /**
   * One application message, retrying a transient `group_busy` after a sync. Returns the measured
   * span of the call that succeeded, its outcome, and how many busy retries preceded it.
   */
  async send(member: Member, group: string, route: string, content: string): Promise<{ ms: number; outcome: SendOutcome; retries: number }> {
    for (let attempt = 0; ; attempt += 1) {
      const started = performance.now();
      try {
        const outcome = await member.engine.sendMessage(group, { kind: 9, tags: [], content });
        return { ms: performance.now() - started, outcome, retries: attempt };
      } catch (error) {
        if (attempt >= 63 || !(error instanceof BridgeError) || error.kind !== "group_busy") {
          throw error;
        }
        await this.sync(member, route);
      }
    }
  }

  count445(route: string): number {
    return this.relay.fetch((event) => event.kind === 445 && event.tags.some((tag) => tag[0] === "h" && tag[1] === route)).length;
  }

  /** A and B in a fresh two-member group, both on sqlite, B joined. */
  async pair(tag: string): Promise<{ a: Member; b: Member; group: string; route: string }> {
    const a = await this.member(`${tag}-a`, "sqlite");
    const b = await this.member(`${tag}-b`, "sqlite");
    await this.identity(a);
    await this.identity(b);
    const created = await a.engine.createGroup({
      name: tag,
      description: "",
      relays: [RELAY],
      invitees: [{ keyPackage: this.relay.keyPackageOf(b.signer.pubkey), inboxRelays: [RELAY] }],
    });
    await this.publish(a);
    await b.engine.ingest(this.relay.welcomesFor(b.signer.pubkey));
    await this.sync(b, created.nostrGroupId);
    await this.sync(a, created.nostrGroupId);
    return { a, b, group: created.id, route: created.nostrGroupId };
  }

  /** `count` KeyPackage-only members (memoryKv), identities published, engines closed. */
  async keyPackages(tag: string, count: number): Promise<string[]> {
    const pubkeys: string[] = [];
    for (let i = 0; i < count; i += 1) {
      const member = await this.member(`${tag}-kp${i}`, "memory");
      await this.identity(member);
      await member.engine.close();
      pubkeys.push(member.signer.pubkey);
    }
    return pubkeys;
  }

  invitee(pubkey: string): { keyPackage: NostrEvent; inboxRelays: string[] } {
    return { keyPackage: this.relay.keyPackageOf(pubkey), inboxRelays: [RELAY] };
  }

  async close(members: Member[]): Promise<void> {
    for (const member of members) {
      await member.engine.close();
    }
    for (const driver of this.drivers) {
      await driver.close();
    }
  }
}

async function milestone(params: RunParams, body: Record<string, unknown>): Promise<void> {
  await post(`/${params.run}/journal`, { kind: "milestone", ...body });
}

/** ADR-031 timing 2: KeyPackage generation (a fresh device's first `ensure_key_package`, signer included and reported). */
async function kp(params: RunParams): Promise<Record<string, unknown>> {
  const bench = new Bench(params.run);
  const members: Member[] = [];
  const runs: number[] = [];
  const signer: number[] = [];
  for (let i = 0; i < params.reps; i += 1) {
    const a = await bench.member(`kp${i}`, "sqlite");
    members.push(a);
    const before = a.signerMs();
    const started = performance.now();
    await a.engine.ensureKeyPackage([RELAY]);
    runs.push(performance.now() - started);
    signer.push(a.signerMs() - before);
    await bench.publish(a);
  }
  await bench.close(members);
  return {
    case: "kp",
    runsMs: runs.map(round),
    signerMs: signer.map(round),
    stats: stats(runs),
    statsWithoutFirst: stats(runs.slice(1)),
    signerStats: stats(signer),
  };
}

/**
 * ADR-031 timing 1: 500 kind-445 into one receiver. `ingestCallMs` is the single `ingest` call;
 * `deliveredMs` runs from that call's start until all 500 messages are drained and acknowledged
 * (including the tick rounds the deferred-peel budget may need).
 */
async function ingest500(params: RunParams): Promise<Record<string, unknown>> {
  const bench = new Bench(params.run);
  const reps: Array<Record<string, number>> = [];
  for (let rep = 0; rep < params.reps; rep += 1) {
    const { a, b, group, route } = await bench.pair(`i${rep}`);
    for (let i = 0; i < params.messages; i += 1) {
      a.clock.advance(1000);
      await bench.send(a, group, route, `m${i}`);
      await bench.publish(a);
    }
    const events = bench.relay.groupEvents(route, b.name);
    const started = performance.now();
    await b.engine.ingest(events);
    const ingestCallMs = performance.now() - started;
    let delivered = await bench.drainAck(b);
    const afterIngestCall = delivered;
    let rounds = 0;
    while (delivered < params.messages && rounds < 64) {
      await b.engine.tick();
      b.clock.advance(2000);
      delivered += await bench.drainAck(b);
      rounds += 1;
    }
    const deliveredMs = performance.now() - started;
    if (delivered !== params.messages) {
      throw new Error(`ingest500 rep ${rep}: delivered ${delivered}/${params.messages}`);
    }
    reps.push({ events: events.length, ingestCallMs: round(ingestCallMs), deliveredMs: round(deliveredMs), afterIngestCall, tickRounds: rounds });
    await a.engine.close();
    await b.engine.close();
  }
  await bench.close([]);
  return {
    case: "ingest500",
    reps,
    ingestCall: stats(reps.map((rep) => rep["ingestCallMs"] ?? 0)),
    delivered: stats(reps.map((rep) => rep["deliveredMs"] ?? 0)),
  };
}

/**
 * ADR-031 timing 3: the Commit adding one member to a 50-member group. `inviteMs` is the `invite`
 * call (the commit is built, signed and staged); `confirmedMs` adds the publish loop until the
 * commit and Welcome are reported. `params.reps` consecutive adds: 50→51, 51→52, …
 */
async function add50(params: RunParams): Promise<Record<string, unknown>> {
  const bench = new Bench(params.run);
  const a = await bench.member("add-a", "sqlite");
  await bench.identity(a);
  const initial = await bench.keyPackages("add", params.members - 1);
  const extra = await bench.keyPackages("add-x", params.reps);
  const createStarted = performance.now();
  const created = await a.engine.createGroup({
    name: "add",
    description: "",
    relays: [RELAY],
    invitees: initial.map((pubkey) => bench.invitee(pubkey)),
  });
  const createMs = performance.now() - createStarted;
  await bench.publish(a);
  await bench.sync(a, created.nostrGroupId);
  const adds: Array<Record<string, unknown>> = [];
  for (const pubkey of extra) {
    const before = (await a.engine.group(created.id))?.members.length ?? 0;
    a.clock.advance(1000);
    const started = performance.now();
    const outcome = await a.engine.invite(created.id, [bench.invitee(pubkey)]);
    const inviteMs = performance.now() - started;
    await bench.publish(a);
    const confirmedMs = performance.now() - started;
    await bench.sync(a, created.nostrGroupId);
    const after = (await a.engine.group(created.id))?.members.length ?? 0;
    adds.push({ membersBefore: before, membersAfter: after, outcome: outcome.type, inviteMs: round(inviteMs), confirmedMs: round(confirmedMs) });
  }
  await bench.close([a]);
  return {
    case: "add50",
    createWithInviteesMs: round(createMs),
    createInvitees: initial.length,
    adds,
    invite: stats(adds.map((add) => Number(add["inviteMs"]))),
  };
}

/**
 * Per-send time against group history: A sends `messages` into a two-member group (publishing
 * each); `history` h is the number of application messages already sent. Windows report the
 * median of the sends at h-2..h+2.
 */
async function send(params: RunParams): Promise<Record<string, unknown>> {
  const bench = new Bench(params.run);
  const { a, b, group, route } = await bench.pair("send");
  const perSend: number[] = [];
  let retries = 0;
  const outcomes: Record<string, number> = {};
  for (let i = 0; i < params.messages; i += 1) {
    a.clock.advance(1000);
    const sent = await bench.send(a, group, route, `m${i}`);
    perSend.push(sent.ms);
    retries += sent.retries;
    outcomes[sent.outcome.type] = (outcomes[sent.outcome.type] ?? 0) + 1;
    await bench.publish(a);
    if ((i + 1) % 250 === 0) {
      await milestone(params, { sent: i + 1, lastMs: round(sent.ms) });
    }
  }
  const windows: Record<string, Stats> = {};
  for (const h of params.windows) {
    if (h + 2 < perSend.length) {
      windows[String(h)] = stats(perSend.slice(Math.max(0, h - 2), h + 3));
    }
  }
  await bench.close([a, b]);
  return { case: "send", messages: params.messages, windows, busyRetries: retries, outcomes, perSendMs: perSend.map(round) };
}

/**
 * Memory, step 1 (`memgen`): `members`-member group — A (sender), R (the measured receiver, keyed
 * by the host so `memrecv` can reopen it) and KeyPackage-only members. R joins and closes; A
 * sends up to `messages` (or until `timeLimitSec`), and the kind-445 stream is appended to
 * `<db>-stream` every 500 sends. A send whose message never reaches the relay after five tick and
 * publish rounds stops the run with `stalled` — the evidence is the result itself.
 */
async function memgen(params: RunParams): Promise<Record<string, unknown>> {
  const bench = new Bench(params.run);
  const started = performance.now();
  const r = await bench.member(params.db, "sqlite", { secret: hexToBytes(params.secret ?? ""), key: hexToBytes(params.key ?? "") });
  const a = await bench.member("mem-a", "sqlite");
  await bench.identity(a);
  await bench.identity(r);
  const others = await bench.keyPackages("mem", params.members - 2);
  await milestone(params, { phase: "identities", elapsedMs: round(performance.now() - started) });
  const createStarted = performance.now();
  const created = await a.engine.createGroup({
    name: "memory",
    description: "",
    relays: [RELAY],
    invitees: [r.signer.pubkey, ...others].map((pubkey) => bench.invitee(pubkey)),
  });
  const createMs = performance.now() - createStarted;
  const route = created.nostrGroupId;
  await bench.publish(a);
  await r.engine.ingest(bench.relay.welcomesFor(r.signer.pubkey));
  await bench.sync(r, route);
  await bench.sync(a, route);
  const reading = r.clock.now();
  await r.engine.close();
  await milestone(params, { phase: "joined", createMs: round(createMs), elapsedMs: round(performance.now() - started) });

  const stream = openDb(`${params.run}-${params.db}-stream`);
  await stream.execute("CREATE TABLE IF NOT EXISTS events (seq INTEGER PRIMARY KEY, json TEXT NOT NULL)");
  await stream.execute("CREATE TABLE IF NOT EXISTS meta (k TEXT PRIMARY KEY, v TEXT NOT NULL)");
  const putMeta = async (k: string, v: string): Promise<void> => {
    await stream.execute("INSERT OR REPLACE INTO meta (k, v) VALUES (?, ?)", [k, v]);
  };
  await putMeta("clock_unix_ms", reading.unixMs.toString());
  await putMeta("clock_monotonic_ms", reading.monotonicMs.toString());
  let seq = 0;
  const flush = async (sent: number): Promise<void> => {
    const fresh = bench.relay.groupEvents(route, r.name);
    await stream.transaction(async (tx) => {
      for (const event of fresh) {
        await tx.execute("INSERT INTO events (seq, json) VALUES (?, ?)", [seq, JSON.stringify({ ...event, createdAt: event.createdAt.toString() })]);
        seq += 1;
      }
    });
    await putMeta("messages", String(sent));
    await putMeta("events", String(seq));
  };

  const perSend: number[] = [];
  const outcomes: Record<string, number> = {};
  const sendStarted = performance.now();
  let stalled = false;
  let sent = 0;
  for (; sent < params.messages; sent += 1) {
    if ((performance.now() - sendStarted) / 1000 > params.timeLimitSec) {
      break;
    }
    const before = bench.count445(route);
    a.clock.advance(1000);
    const result = await bench.send(a, created.id, route, `m${sent}`);
    perSend.push(result.ms);
    outcomes[result.outcome.type] = (outcomes[result.outcome.type] ?? 0) + 1;
    await bench.publish(a);
    for (let pass = 0; pass < 5 && bench.count445(route) === before; pass += 1) {
      const wake = await a.engine.tick();
      a.clock.advance(Number(wake ?? 2000n));
      await bench.publish(a);
    }
    if (bench.count445(route) === before) {
      stalled = true;
      sent += 1;
      break;
    }
    if ((sent + 1) % 500 === 0) {
      await flush(sent + 1);
      await milestone(params, { sent: sent + 1, windowMs: stats(perSend.slice(-500)), elapsedMs: round(performance.now() - started) });
    }
  }
  await flush(sent);
  await bench.close([a]);
  return {
    case: "memgen",
    members: params.members,
    requested: params.messages,
    sent,
    events: seq,
    stalled,
    outcomes,
    createWithInviteesMs: round(createMs),
    perSend: stats(perSend),
    perSendFirst50: stats(perSend.slice(0, 50)),
    perSendLast50: stats(perSend.slice(-50)),
    elapsedMs: round(performance.now() - started),
  };
}

/**
 * Memory, step 2 (`memrecv`, a fresh process): R reopens from its sqlite rows and ingests the
 * `memgen` stream in 500-event batches, draining and acknowledging after each. Milestones mark
 * `opened` (baseline) and every batch; after the result the process stays alive so the host can
 * sample its resident memory.
 */
async function memrecv(params: RunParams): Promise<Record<string, unknown>> {
  const source = params.source;
  if (source === undefined) {
    throw new Error("memrecv needs a `source` run id");
  }
  // `memgen` named R's database `<its run>-<db>`: reopen it under the source run.
  const bench = new Bench(source);
  const stream = openDb(`${source}-${params.db}-stream`);
  const meta = new Map(
    (await stream.execute("SELECT k, v FROM meta")).rows.map((row) => [String(row["k"]), String(row["v"])]),
  );
  const clock = manualClock({ unixMs: BigInt(meta.get("clock_unix_ms") ?? "0"), monotonicMs: BigInt(meta.get("clock_monotonic_ms") ?? "0") });
  const openStarted = performance.now();
  const r = await bench.member(params.db, "sqlite", { secret: hexToBytes(params.secret ?? ""), key: hexToBytes(params.key ?? ""), clock });
  const openMs = performance.now() - openStarted;
  const messages = Number(meta.get("messages") ?? "0");
  const total = Number(meta.get("events") ?? "0");
  await milestone(params, { phase: "opened", openMs: round(openMs), events: total });
  let delivered = 0;
  const batches: number[] = [];
  const started = performance.now();
  for (let offset = 0; offset < total; offset += 500) {
    const rows = (await stream.execute("SELECT json FROM events WHERE seq >= ? AND seq < ? ORDER BY seq", [offset, offset + 500])).rows;
    const events = rows.map((row) => {
      const parsed = JSON.parse(String(row["json"])) as Omit<NostrEvent, "createdAt"> & { createdAt: string };
      return { ...parsed, createdAt: BigInt(parsed.createdAt) };
    });
    const batchStarted = performance.now();
    await r.engine.ingest(events);
    delivered += await bench.drainAck(r);
    batches.push(performance.now() - batchStarted);
    await milestone(params, { phase: "batch", ingested: offset + events.length, delivered, batchMs: round(batches.at(-1) ?? 0) });
  }
  for (let pass = 0; pass < 64 && delivered < messages; pass += 1) {
    await r.engine.tick();
    r.clock.advance(2000);
    delivered += await bench.drainAck(r);
  }
  return {
    case: "memrecv",
    messages,
    events: total,
    delivered,
    openMs: round(openMs),
    ingestMs: round(performance.now() - started),
    batchMs: batches.map(round),
    holdForMemorySample: true,
  };
}

const CASES: Record<string, (params: RunParams) => Promise<Record<string, unknown>>> = {
  kp,
  ingest500,
  add50,
  send,
  memgen,
  memrecv,
};

export async function runPerfMode(params: RunParams): Promise<Record<string, unknown>> {
  const body = CASES[params.case];
  if (body === undefined) {
    throw new Error(`unknown perf case ${params.case}`);
  }
  return body(params);
}
