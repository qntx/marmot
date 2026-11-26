/**
 * `client-e2e` — shared `MarmotClient` end-to-end scenarios over the release wasm backend. Both
 * runners (`tests/wasm` on Node, `tests/bun` on Bun) drive the same code: real `setTimeout` timers,
 * real wall clocks (with a shared, bumpable offset per client), the in-memory `fakeRelay` network,
 * and `memoryKv`/`memoryCursorStore` persistence.
 */

import type { Backend, Clock, Signer, GroupSnapshot, NostrEvent } from "@qntx/marmot/backend";
import { SignerRefusedError } from "@qntx/marmot/backend";
import {
  GROUP_OVERLAP_SECS,
  MAX_RELAYS,
  MAX_SUBSCRIPTIONS,
  MarmotClient,
  MarmotError,
  decodeClientState,
} from "@qntx/marmot/client";
import type {
  ClientMessage,
  DeliveryEvent,
  GroupEvent,
  InviteEvent,
  MarmotErrorKind,
  RelayFilter,
  RelayPort,
} from "@qntx/marmot/client";
import { countingTimer, fakeRelay, memoryCursorStore, memoryKv } from "@qntx/marmot/testing";
import type { FakeRelayNetwork, MemoryCursorStore, MemoryKv } from "@qntx/marmot/testing";

import { testSigner } from "../signer.ts";
import { releaseBackend } from "./env.ts";

export const RELAY_1 = "wss://relay-1.example/";
export const RELAY_2 = "wss://relay-2.example/";
export const RELAY_3 = "wss://relay-3.example/";
/**
 * Relay URLs in the engine's canonical form (trailing slash) — every layer compares strings
 * exactly.
 */
/** Identity lookup + inbox relay for every scenario (NIP-65 10002, KeyPackage, NIP-17 10050). */
export const LOOKUP: string = RELAY_3;
export const INBOX: string = RELAY_3;

const RELAY_LIST_KIND = 10002;
const SEND_ATTEMPTS = 400;
const RETRY_MS = 20;
export const POLL_MS = 20;
const DEFAULT_TIMEOUT_MS = 120_000;

/** Shared per-client clock offset (ms) the test can bump; both readings move, monotonic grows. */
export type Skew = { offsetMs: number };

/** The spec clock: unix = Date.now() + offset; monotonic = elapsed + offset (never regresses). */
export function e2eClock(skew: Skew): Clock {
  const start = Date.now();
  return {
    now: () => ({
      unixMs: BigInt(Date.now() + skew.offsetMs),
      // Negative skew would make early monotonic readings negative — the codec rejects those, so
      // clamp at zero; the clock still never regresses.
      monotonicMs: BigInt(Math.max(0, Date.now() - start + skew.offsetMs)),
    }),
  };
}

/** Persistence backing one client lifetime; reopen/crash derive the next stores from it. */
export type Stores = { kv: MemoryKv; cursors: MemoryCursorStore };

export function freshStores(): Stores {
  return { kv: memoryKv(), cursors: memoryCursorStore() };
}

export type CountingTimer = ReturnType<typeof countingTimer>;

export type MessageHandler = (batch: ClientMessage[]) => void | Promise<void>;

/** What a client lifetime collects for assertions. */
export type ClientHandle = {
  name: string;
  /** Network port key; `crash` moves the handle to `<name>-<generation>`. */
  portName: string;
  client: MarmotClient;
  signer: Signer;
  skew: Skew;
  stores: Stores;
  timer: CountingTimer;
  network: FakeRelayNetwork;
  received: ClientMessage[];
  invites: InviteEvent[];
  groupChanges: GroupEvent[];
  deliveries: DeliveryEvent[];
  errors: MarmotError[];
  /** Contents sent via `send`, for (sent ∪ received) set assertions. */
  sent: Set<string>;
  /** The `onMessages` hook for the next instance; the default resolves (acked). */
  onMessages?: MessageHandler | undefined;
  /** Optional port override (wrapped transports); defaults to `network.port(portName)`. */
  port?: RelayPort | undefined;
  /** True while `restart`/`crash` swaps the instance; `send` waits it out. */
  restarting: boolean;
  generation: number;
  /** Abandoned instances (crashes); their engine keeps running but their port is dead. */
  zombies: MarmotClient[];
};

export type StartOptions = {
  name: string;
  network: FakeRelayNetwork;
  stores?: Stores;
  signer?: Signer;
  skew?: Skew;
  port?: RelayPort;
  onMessages?: MessageHandler;
};

type HandleSeed = Omit<ClientHandle, "client">;

export async function startClient(options: StartOptions): Promise<ClientHandle> {
  const seed: HandleSeed = {
    name: options.name,
    portName: options.name,
    signer: options.signer ?? testSigner(),
    skew: options.skew ?? { offsetMs: 0 },
    stores: options.stores ?? freshStores(),
    timer: countingTimer(),
    network: options.network,
    received: [],
    invites: [],
    groupChanges: [],
    deliveries: [],
    errors: [],
    sent: new Set(),
    port: options.port,
    onMessages: options.onMessages,
    restarting: false,
    generation: 0,
    zombies: [],
  };
  return { ...seed, client: await openInstance(seed) };
}

async function openInstance(handle: HandleSeed): Promise<MarmotClient> {
  const backend: Backend = await releaseBackend();
  const client = await MarmotClient.open({
    backend,
    kv: handle.stores.kv,
    cursors: handle.stores.cursors,
    signer: handle.signer,
    clock: e2eClock(handle.skew),
    timer: handle.timer,
    relays: handle.port ?? handle.network.port(handle.portName),
    lookupRelays: [LOOKUP],
    allowLoopback: false,
  });
  client.onMessages(async (batch) => {
    handle.received.push(...batch);
    await handle.onMessages?.(batch);
  });
  client.onInvite((event) => {
    handle.invites.push(event);
  });
  client.onGroupChange((event) => {
    handle.groupChanges.push(event);
  });
  client.onDelivery((event) => {
    handle.deliveries.push(event);
  });
  client.onError((error) => {
    handle.errors.push(error);
  });
  return client;
}

export async function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => {
    setTimeout(resolve, ms);
  });
}

export async function waitFor(
  predicate: () => boolean | Promise<boolean>,
  what: string,
  timeoutMs: number = DEFAULT_TIMEOUT_MS,
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    // oxlint-disable-next-line no-await-in-loop -- polling by design
    if (await predicate()) {
      return;
    }
    // oxlint-disable-next-line no-await-in-loop -- polling by design
    await sleep(POLL_MS);
  }
  throw new Error(`waitFor timed out after ${timeoutMs}ms: ${what}`);
}

/**
 * Publish the account's NIP-65 relay list (kind-10002, `["r", relay]` tags) onto the lookup relay,
 * then the client's own `ensureKeyPackage` + `publishInboxRelays`.
 */
export async function identity(handle: ClientHandle): Promise<void> {
  const event = await handle.signer.signEvent({
    pubkey: handle.signer.pubkey,
    createdAt: BigInt(Math.floor(Date.now() / 1000)),
    kind: RELAY_LIST_KIND,
    tags: [["r", LOOKUP]],
    content: "",
  });
  await handle.network.port(handle.portName).publish(LOOKUP, event);
  await handle.client.ensureKeyPackage();
  await handle.client.publishInboxRelays([INBOX]);
  // The ops resolve once durable; their publish pass is async — wait until the relay actually
  // holds both events so a following `lookupInvitees` cannot read an empty store.
  await waitFor(
    () =>
      handle.network
        .events(LOOKUP)
        .some((stored) => stored.kind === 30443 && stored.pubkey === handle.signer.pubkey) &&
      handle.network
        .events(INBOX)
        .some((stored) => stored.kind === 10050 && stored.pubkey === handle.signer.pubkey),
    `${handle.name} key package + inbox relays stored`,
  );
}

/**
 * Clean restart: `close()`, then reopen on a fresh driver pair seeded with the durable rows/bytes.
 * (`memoryKv` is terminal after `close` — a new driver over the same rows is the same store.)
 */
/** Reopen on a fresh driver pair seeded with the durable rows/bytes and a fresh timer. */
export async function reopen(handle: ClientHandle): Promise<void> {
  handle.restarting = true;
  try {
    handle.stores = {
      kv: memoryKv(handle.stores.kv.rows()),
      cursors: memoryCursorStore(handle.stores.cursors.bytes()),
    };
    handle.timer = countingTimer();
    handle.client = await openInstance(handle);
  } finally {
    handle.restarting = false;
  }
}

export async function restart(handle: ClientHandle): Promise<void> {
  await handle.client.close();
  await reopen(handle);
}

/**
 * The kill half of `crash`: the port dies (sockets and in-flight writes gone), the instance is
 * abandoned, and the next lifetime's stores/port name are prepared — but nothing reopens yet, so
 * events published while silenced are missed live and arrive only on `wake`'s replay.
 */
export function silence(handle: ClientHandle): void {
  handle.restarting = true;
  handle.generation += 1;
  handle.zombies.push(handle.client);
  handle.network.kill(handle.portName);
  handle.portName = `${handle.name}-${handle.generation}`;
  handle.stores = {
    kv: memoryKv(handle.stores.kv.rows()),
    cursors: memoryCursorStore(handle.stores.cursors.bytes()),
  };
  handle.timer = countingTimer();
}

/** The reopen half of `crash`: a fresh instance over the prepared durable rows. */
export async function wake(handle: ClientHandle): Promise<void> {
  try {
    handle.client = await openInstance(handle);
  } finally {
    handle.restarting = false;
  }
}

/**
 * Process death: `network.kill` severs the old port (its sockets and in-flight writes are gone),
 * the instance is abandoned without closing, and it reopens over exactly what was durable —
 * `memoryKv(old.rows())` + `memoryCursorStore(old.bytes())` — on a fresh port name and timer.
 */
export async function crash(handle: ClientHandle): Promise<void> {
  silence(handle);
  await wake(handle);
}

/**
 * Send one app message, retrying `group_busy` and `send_rate_exceeded` with 20 ms sleeps (bounded).
 * The pacing cap — 50 per group per `created_at` second, 30 s forward skew — only refuses a send
 * beyond 1,550 per group inside 30 s; retrying after a short sleep is the documented host
 * behaviour, exactly like `group_busy`.
 */
export async function send(handle: ClientHandle, group: string, text: string): Promise<void> {
  for (let attempt = 0; attempt < SEND_ATTEMPTS; attempt += 1) {
    if (handle.restarting) {
      // oxlint-disable-next-line no-await-in-loop -- wait out the instance swap
      await waitFor(() => !handle.restarting, `${handle.name} instance swap`, 30_000);
    }
    try {
      // oxlint-disable-next-line no-await-in-loop -- a send is sequential by design
      await handle.client.sendMessage(group, { kind: 9, tags: [], content: text });
      handle.sent.add(text);
      return;
    } catch (error) {
      // A send racing a mid-stream restart dies as `closed` before staging — safe to retry.
      const transient =
        error instanceof MarmotError &&
        (error.kind === "group_busy" ||
          error.kind === "send_rate_exceeded" ||
          (error.kind === "closed" && handle.restarting));
      if (transient) {
        // oxlint-disable-next-line no-await-in-loop -- bounded app-level retry
        await sleep(RETRY_MS);
        continue;
      }
      throw error;
    }
  }
  throw new Error(`${handle.name}: send stayed group_busy after ${SEND_ATTEMPTS} attempts`);
}

/** Message contents the handle's listener saw for `group` (delivery order). */
export function contents(handle: ClientHandle, group: string): string[] {
  return handle.received
    .filter((entry) => entry.group === group)
    .map((entry) => entry.message.content);
}

/** Sent ∪ received message contents for `group`. */
export function transcript(handle: ClientHandle, group: string): Set<string> {
  return new Set([...handle.sent, ...contents(handle, group)]);
}

/** Duplicate deliveries to this handle's listener for `group`. */
export function duplicates(handle: ClientHandle, group: string): string[] {
  const seen = new Set<string>();
  const dupes: string[] = [];
  for (const content of contents(handle, group)) {
    if (seen.has(content)) {
      dupes.push(content);
    }
    seen.add(content);
  }
  return dupes;
}

/**
 * Every handle's `group(id)` snapshot agrees on epoch, members (sorted by account), name, relays
 * and nostrGroupId; every member's (sent ∪ received) content set equals `expected` when given.
 */
function snapshotShape(snapshot: GroupSnapshot): string {
  const admins = snapshot.live?.admins ?? [];
  return JSON.stringify({
    epoch: snapshot.epoch.toString(),
    members: snapshot.members
      .map((member) => `${member.account}:${admins.includes(member.account) ? 1 : 0}`)
      .toSorted(),
    name: snapshot.name,
    relays: [...(snapshot.live?.relays ?? [])].toSorted(),
    nostrGroupId: snapshot.live?.nostrGroupId,
  });
}

export async function assertConverged(
  handles: ClientHandle[],
  group: string,
  expected?: Set<string>,
): Promise<GroupSnapshot> {
  const read = async (): Promise<GroupSnapshot[]> => {
    const snapshots: GroupSnapshot[] = [];
    for (const handle of handles) {
      // oxlint-disable-next-line no-await-in-loop -- snapshots are read sequentially
      const snapshot = await handle.client.group(group);
      if (snapshot === undefined) {
        throw new Error(`${handle.name}: group ${group} missing`);
      }
      snapshots.push(snapshot);
    }
    return snapshots;
  };
  let snapshots = await read();
  if (snapshots[0] === undefined) {
    throw new Error("assertConverged needs at least one handle");
  }
  await waitFor(async () => {
    snapshots = await read();
    const [head] = snapshots;
    return (
      head !== undefined &&
      snapshots.every((snapshot) => snapshotShape(snapshot) === snapshotShape(head))
    );
  }, "group snapshots converge");

  const [first] = snapshots;
  if (first === undefined) {
    throw new Error("assertConverged needs at least one handle");
  }
  for (const snapshot of snapshots.slice(1)) {
    if (snapshotShape(snapshot) !== snapshotShape(first)) {
      throw new Error(`snapshots diverged:\n${snapshotShape(first)}\n${snapshotShape(snapshot)}`);
    }
  }
  if (expected !== undefined) {
    for (const handle of handles) {
      const got = transcript(handle, group);
      const missing = [...expected].filter((content) => !got.has(content));
      const extra = [...got].filter((content) => !expected.has(content));
      if (missing.length > 0 || extra.length > 0) {
        throw new Error(
          `${handle.name}: transcript missing=${missing.length} extra=${extra.length}`,
        );
      }
    }
  }
  return first;
}

/** Fail on any collected `onError` whose kind is not in `allowed`. */
export function assertQuiet(handles: ClientHandle[], allowed: MarmotErrorKind[] = []): void {
  const allowedSet = new Set<MarmotErrorKind>(allowed);
  for (const handle of handles) {
    const bad = handle.errors.filter((error) => !allowedSet.has(error.kind));
    if (bad.length > 0) {
      throw new Error(
        `${handle.name}: unexpected onError kinds ${bad.map((error) => error.kind).join(",")}`,
      );
    }
  }
}

/** Close every live client, then assert the leak contract per client port. */
export async function closeAll(handles: ClientHandle[], network: FakeRelayNetwork): Promise<void> {
  for (const handle of handles) {
    // oxlint-disable-next-line no-await-in-loop -- closes are sequential
    await handle.client.close();
  }
  for (const handle of handles) {
    const open = network.openSubscriptions(handle.portName);
    if (open !== 0) {
      throw new Error(`${handle.portName}: ${open} open subscriptions after close`);
    }
    if (handle.timer.pending() !== 0) {
      throw new Error(`${handle.name}: ${handle.timer.pending()} pending timers after close`);
    }
  }
}

/** Per-client peak assertions from the spec's subscription bounds. */
export function assertPeaks(handles: ClientHandle[], network: FakeRelayNetwork): void {
  for (const handle of handles) {
    const peakSubs = network.peakSubscriptions(handle.portName);
    if (peakSubs > MAX_SUBSCRIPTIONS) {
      throw new Error(`${handle.portName}: peakSubscriptions ${peakSubs} > ${MAX_SUBSCRIPTIONS}`);
    }
    const peakRelays = network.peakRelays(handle.portName);
    if (peakRelays > MAX_RELAYS) {
      throw new Error(`${handle.portName}: peakRelays ${peakRelays} > ${MAX_RELAYS}`);
    }
  }
}

export type ScenarioReport = {
  ms: number;
  delivered: Record<string, number>;
  peaks: Record<string, { subs: number; relays: number }>;
};

function peaksOf(
  handles: ClientHandle[],
  network: FakeRelayNetwork,
): Record<string, { subs: number; relays: number }> {
  const peaks: Record<string, { subs: number; relays: number }> = {};
  for (const handle of handles) {
    peaks[handle.portName] = {
      subs: network.peakSubscriptions(handle.portName),
      relays: network.peakRelays(handle.portName),
    };
  }
  return peaks;
}

function deliveredOf(handles: ClientHandle[], group: string): Record<string, number> {
  const delivered: Record<string, number> = {};
  for (const handle of handles) {
    delivered[handle.portName] = contents(handle, group).length;
  }
  return delivered;
}

/** A two-member group over RELAY_1+RELAY_2, identities on the lookup relay. */
export async function openPair(
  network: FakeRelayNetwork,
  options?: {
    a?: Partial<StartOptions> | undefined;
    b?: Partial<StartOptions> | undefined;
    name?: string;
  },
): Promise<{ a: ClientHandle; b: ClientHandle; group: string }> {
  const a = await startClient({ name: "a", network, ...options?.a });
  const b = await startClient({ name: "b", network, ...options?.b });
  await identity(a);
  await identity(b);
  const invitees = await a.client.lookupInvitees(b.signer.pubkey, [LOOKUP]);
  const snapshot = await a.client.createGroup({
    name: options?.name ?? "e2e",
    description: "",
    relays: [RELAY_1, RELAY_2],
    invitees,
  });
  await waitFor(
    () => b.invites.some((event) => event.type === "joined" && event.group === snapshot.id),
    "b joined invite",
  );
  await waitFor(async () => (await b.client.group(snapshot.id)) !== undefined, "b group snapshot");
  return { a, b, group: snapshot.id };
}

/** `count` sequential sends, restarting the handle once at `restartAt` when given. */
async function sendLoop(
  handle: ClientHandle,
  group: string,
  count: number,
  tag: string,
  restartAt?: number,
): Promise<void> {
  for (let i = 0; i < count; i += 1) {
    if (restartAt !== undefined && i === restartAt) {
      // oxlint-disable-next-line no-await-in-loop -- a mid-stream restart is the scenario
      await restart(handle);
    }
    // oxlint-disable-next-line no-await-in-loop -- sends are sequential per sender
    await send(handle, group, `${tag}#${i}`);
  }
}

/** Wait until every handle's transcript covers `expected` contents. */
async function waitForTranscripts(
  handles: ClientHandle[],
  group: string,
  expected: Set<string>,
): Promise<void> {
  await waitFor(
    () =>
      handles.every((handle) => [...expected].every((text) => transcript(handle, group).has(text))),
    `transcripts cover ${expected.size} contents`,
  );
}

function expectedSet(prefix: string, count: number): string[] {
  return Array.from({ length: count }, (_, i) => `${prefix}#${i}`);
}

/** Scenario 1: 500 each way, mid-stream restarts, C joins then is removed, converge. */
export async function scenarioMain(): Promise<ScenarioReport> {
  const start = Date.now();
  const network = fakeRelay(11);
  const { a, b, group } = await openPair(network);
  const c = await startClient({ name: "c", network });
  await identity(c);
  // Restart C mid-stream too — it is a live client even before it joins the group.
  const cRestart = (async () => {
    await waitFor(() => a.sent.size >= 250 && b.sent.size >= 250, "midpoint reached");
    return restart(c);
  })();
  await Promise.all([
    sendLoop(a, group, 500, "A>B", 250),
    sendLoop(b, group, 500, "B>A", 250),
    cRestart,
  ]);
  const expected = new Set([...expectedSet("A>B", 500), ...expectedSet("B>A", 500)]);
  await waitForTranscripts([a, b], group, expected);
  // C joins, then is removed.
  const invitees = await a.client.lookupInvitees(c.signer.pubkey, [LOOKUP]);
  await a.client.invite(group, invitees);
  await waitFor(
    () => c.invites.some((event) => event.type === "joined" && event.group === group),
    "c joined invite",
  );
  await waitFor(async () => (await c.client.group(group)) !== undefined, "c group snapshot");
  await a.client.removeMembers(group, [c.signer.pubkey]);
  await waitFor(async () => {
    const snapshot = await c.client.group(group);
    return (
      snapshot === undefined ||
      snapshot.access === "removed" ||
      snapshot.access === "removed_device"
    );
  }, "c removed");
  // A and B converge on the full transcript; C is gone from the member set.
  const snapshot = await assertConverged([a, b], group, expected);
  if (snapshot.members.some((member) => member.account === c.signer.pubkey)) {
    throw new Error("c still in member set");
  }
  for (const handle of [a, b]) {
    if (duplicates(handle, group).length > 0) {
      throw new Error(`${handle.name}: duplicate deliveries`);
    }
  }
  assertQuiet([a, b, c]);
  assertPeaks([a, b, c], network);
  const report = {
    ms: Date.now() - start,
    delivered: deliveredOf([a, b], group),
    peaks: peaksOf([a, b, c], network),
  };
  await closeAll([a, b, c], network);
  return report;
}

/** Scenario 2/3/4: 40 each way under a relay `mode`, then converge without duplicates. */
export async function scenarioMode(
  modes: Array<[string, "drop" | "reject" | "shuffle"]>,
): Promise<ScenarioReport> {
  const start = Date.now();
  const network = fakeRelay(23);
  for (const [relay, mode] of modes) {
    network.mode(relay, mode);
  }
  const { a, b, group } = await openPair(network);
  await Promise.all([sendLoop(a, group, 40, "A>B"), sendLoop(b, group, 40, "B>A")]);
  const expected = new Set([...expectedSet("A>B", 40), ...expectedSet("B>A", 40)]);
  await waitForTranscripts([a, b], group, expected);
  await assertConverged([a, b], group, expected);
  for (const handle of [a, b]) {
    const dupes = duplicates(handle, group);
    if (dupes.length > 0) {
      throw new Error(`${handle.name}: ${dupes.length} duplicate deliveries`);
    }
  }
  assertQuiet([a, b]);
  assertPeaks([a, b], network);
  const report = {
    ms: Date.now() - start,
    delivered: deliveredOf([a, b], group),
    peaks: peaksOf([a, b], network),
  };
  await closeAll([a, b], network);
  return report;
}

export type CatchupReport = ScenarioReport & {
  messages: number;
  servedDelta: number;
  storedTotal: number;
  catchupMs: number;
};

/**
 * Scenario 5: 20 pre-offline messages, B goes dark, A sends 500, B catches up — without refetching
 * from zero (cursor − overlap excludes the pre-offline events).
 */
export async function scenarioCatchup(): Promise<CatchupReport> {
  const start = Date.now();
  const skewA: Skew = { offsetMs: 0 };
  const skewB: Skew = { offsetMs: 0 };
  const network = fakeRelay(31);
  const { a, b, group } = await openPair(network, {
    a: { skew: skewA },
    b: { skew: skewB },
  });
  await Promise.all([sendLoop(a, group, 10, "A>B"), sendLoop(b, group, 10, "B>A")]);
  const preOffline = new Set([...expectedSet("A>B", 10), ...expectedSet("B>A", 10)]);
  await waitForTranscripts([a, b], group, preOffline);
  // Jump both clocks an hour and send one marker B ingests before sleeping — its cursor moves past
  // the pre-offline events, which is exactly what makes the no-refetch assertion meaningful.
  skewA.offsetMs += 3_600_000;
  skewB.offsetMs += 3_600_000;
  await send(a, group, "marker");
  await waitFor(() => contents(b, group).includes("marker"), "b received marker");
  // The 20 pre-offline messages and join commits must precede every group cursor − overlap —
  // decode B's persisted blob and compare against the stored events' createdAt.
  const preOfflineIds = new Set(
    b.received
      .filter((entry) => entry.group === group && entry.message.content !== "marker")
      .map((entry) => entry.message.transportId),
  );
  const stored = [...network.events(RELAY_1), ...network.events(RELAY_2)];
  let maxPreOffline = 0n;
  for (const event of stored) {
    if (preOfflineIds.has(event.id) && event.createdAt > maxPreOffline) {
      maxPreOffline = event.createdAt;
    }
  }
  await b.client.close();
  const persisted = b.stores.cursors.bytes();
  if (persisted === undefined) {
    throw new Error("b persisted no cursor blob");
  }
  const { cursors } = decodeClientState(persisted);
  let groupCursorKeys = 0;
  for (const [key, cursor] of Object.entries(cursors)) {
    if (!key.startsWith("group|")) {
      continue;
    }
    groupCursorKeys += 1;
    if (BigInt(cursor) - BigInt(GROUP_OVERLAP_SECS) <= maxPreOffline) {
      throw new Error(`cursor ${key}=${cursor} does not clear the pre-offline window`);
    }
  }
  if (groupCursorKeys === 0) {
    throw new Error("persisted state holds no group cursor keys");
  }
  const servedBefore = network.served(b.portName);
  const offline = Array.from({ length: 500 }, (_, i) => `offline#${i}`);
  for (const text of offline) {
    // oxlint-disable-next-line no-await-in-loop -- sequential sends while b is dark
    await send(a, group, text);
  }
  const reopenedAt = Date.now();
  b.stores = {
    kv: memoryKv(b.stores.kv.rows()),
    cursors: memoryCursorStore(b.stores.cursors.bytes()),
  };
  b.timer = countingTimer();
  b.client = await openInstance(b);
  const all = new Set([...preOffline, "marker", ...offline]);
  await waitFor(
    () => [...offline].every((text) => contents(b, group).includes(text)),
    "b caught up on 500 messages",
    300_000,
  );
  const catchupMs = Date.now() - reopenedAt;
  await waitForTranscripts([a, b], group, all);
  // Not a from-zero refetch: served during catch-up covers the 500 but is strictly less than every
  // group event stored on b's relays.
  const servedDelta = network.served(b.portName) - servedBefore;
  const storedTotal =
    network.events(RELAY_1).filter((event) => event.kind === 445).length +
    network.events(RELAY_2).filter((event) => event.kind === 445).length;
  if (servedDelta < 500) {
    throw new Error(`catch-up served ${servedDelta} < 500`);
  }
  if (servedDelta >= storedTotal) {
    throw new Error(`catch-up refetched from zero: served ${servedDelta} >= stored ${storedTotal}`);
  }
  for (const text of preOffline) {
    // 0 or 1 deliveries are both correct — own sends are never echoed to the sender.
    if (contents(b, group).filter((entry) => entry === text).length > 1) {
      throw new Error(`pre-offline content redelivered: ${text}`);
    }
  }
  await assertConverged([a, b], group, all);
  assertQuiet([a, b]);
  assertPeaks([a, b], network);
  const report = {
    ms: Date.now() - start,
    delivered: deliveredOf([a, b], group),
    peaks: peaksOf([a, b], network),
    messages: 500,
    servedDelta,
    storedTotal,
    catchupMs,
  };
  await closeAll([a, b], network);
  return report;
}

/** Scenario 6: A at +300 s, C at −300 s (both inside the 600 s overlap); B restarts mid-stream. */
export async function scenarioClockSkew(): Promise<ScenarioReport> {
  const start = Date.now();
  const network = fakeRelay(41);
  const skewA: Skew = { offsetMs: 300_000 };
  const skewC: Skew = { offsetMs: -300_000 };
  const a = await startClient({ name: "a", network, skew: skewA });
  const b = await startClient({ name: "b", network });
  const c = await startClient({ name: "c", network, skew: skewC });
  await identity(a);
  await identity(b);
  await identity(c);
  const invitees = [
    ...(await a.client.lookupInvitees(b.signer.pubkey, [LOOKUP])),
    ...(await a.client.lookupInvitees(c.signer.pubkey, [LOOKUP])),
  ];
  const snapshot = await a.client.createGroup({
    name: "skew",
    description: "",
    relays: [RELAY_1, RELAY_2],
    invitees,
  });
  const group = snapshot.id;
  await waitFor(
    () =>
      b.invites.some((event) => event.type === "joined" && event.group === group) &&
      c.invites.some((event) => event.type === "joined" && event.group === group),
    "b and c joined",
  );
  await waitFor(async () => (await c.client.group(group)) !== undefined, "c group snapshot");
  await Promise.all([sendLoop(a, group, 40, "A>B"), sendLoop(b, group, 40, "B>A", 20)]);
  const expected = new Set([...expectedSet("A>B", 40), ...expectedSet("B>A", 40)]);
  await waitForTranscripts([a, b, c], group, expected);
  await assertConverged([a, b, c], group, expected);
  assertQuiet([a, b, c]);
  assertPeaks([a, b, c], network);
  const report = {
    ms: Date.now() - start,
    delivered: deliveredOf([a, b, c], group),
    peaks: peaksOf([a, b, c], network),
  };
  await closeAll([a, b, c], network);
  return report;
}

/**
 * A `testSigner` whose first `nip44Decrypt` after the open probe answers `SignerRefusedError`, then
 * works — the probe's self round trip is call 1, so the first Welcome unwrap is call 2.
 */
function refusingSigner(): Signer {
  const inner = testSigner();
  let decrypts = 0;
  return {
    pubkey: inner.pubkey,
    signEvent: inner.signEvent,
    nip44Encrypt: inner.nip44Encrypt,
    nip44Decrypt: async (peer, ciphertext) => {
      decrypts += 1;
      if (decrypts === 2) {
        throw new SignerRefusedError();
      }
      return inner.nip44Decrypt(peer, ciphertext);
    },
  };
}

/**
 * A `RelayPort` whose dials are refused while `gate.refused` holds: `connect` answers false and
 * `publish` answers `not_sent` without touching the network; subscriptions pass through.
 */
function gatedPort(inner: RelayPort, gate: { refused: boolean }): RelayPort {
  return {
    publish: async (relay, event) => (gate.refused ? "not_sent" : inner.publish(relay, event)),
    connect: async (relay) => (gate.refused ? false : inner.connect(relay)),
    subscribe: (relay, filters, handlers) => inner.subscribe(relay, filters, handlers),
  };
}

/**
 * Scenario 7: C's signer refuses its first Welcome decrypt — `pending{signer_refused}` — then
 * `retryInvites()` replays the wrap and C joins; messages flow both ways afterwards.
 */
export async function scenarioInviteRetry(): Promise<ScenarioReport> {
  const start = Date.now();
  const network = fakeRelay(53);
  const a = await startClient({ name: "a", network });
  const b = await startClient({ name: "b", network });
  const c = await startClient({ name: "c", network, signer: refusingSigner() });
  await identity(a);
  await identity(b);
  await identity(c);
  const invitees = [
    ...(await a.client.lookupInvitees(b.signer.pubkey, [LOOKUP])),
    ...(await a.client.lookupInvitees(c.signer.pubkey, [LOOKUP])),
  ];
  const snapshot = await a.client.createGroup({
    name: "invite-retry",
    description: "",
    relays: [RELAY_1, RELAY_2],
    invitees,
  });
  const group = snapshot.id;
  await waitFor(
    () => b.invites.some((event) => event.type === "joined" && event.group === group),
    "b joined invite",
  );
  await waitFor(
    () => c.invites.some((event) => event.type === "pending" && event.reason === "signer_refused"),
    "c pending invite",
  );
  await c.client.retryInvites();
  await waitFor(
    () => c.invites.some((event) => event.type === "joined" && event.group === group),
    "c joined invite",
  );
  await waitFor(async () => (await c.client.group(group)) !== undefined, "c group snapshot");
  await send(a, group, "a>c");
  await waitFor(() => contents(c, group).includes("a>c"), "c received a>c");
  await send(c, group, "c>a");
  await waitFor(() => contents(a, group).includes("c>a"), "a received c>a");
  const expected = new Set(["a>c", "c>a"]);
  await assertConverged([a, b, c], group, expected);
  assertQuiet([a, b, c]);
  assertPeaks([a, b, c], network);
  const report = {
    ms: Date.now() - start,
    delivered: deliveredOf([a, b, c], group),
    peaks: peaksOf([a, b, c], network),
  };
  await closeAll([a, b, c], network);
  return report;
}

/**
 * Scenario 8: A's port refuses every dial — an app message and a commit stay `pending` and nothing
 * is transmitted; back online the message is delivered exactly once and the commit's profile change
 * lands.
 */
export async function scenarioOfflineSend(): Promise<ScenarioReport> {
  const start = Date.now();
  const network = fakeRelay(67);
  const gate = { refused: false };
  const { a, b, group } = await openPair(network, {
    a: { port: gatedPort(network.port("a"), gate) },
  });
  gate.refused = true;
  const commits = () => network.published().filter((entry) => entry.event.kind === 445).length;
  const publishedBefore = commits();
  await send(a, group, "offline-msg");
  await a.client.updateGroupProfile(group, { name: "renamed", description: "" });
  // Both items report pending and keep retrying; no publish reaches the relay network.
  await waitFor(() => {
    const pending = a.deliveries.filter((entry) => entry.resolution === "pending");
    return new Set(pending.map((entry) => entry.outbound)).size >= 2;
  }, "a pending outbound");
  if (commits() !== publishedBefore) {
    throw new Error("an event was transmitted while dials were refused");
  }
  if (contents(b, group).includes("offline-msg")) {
    throw new Error("offline-msg reached b while a was offline");
  }
  gate.refused = false;
  await waitFor(() => contents(b, group).includes("offline-msg"), "b received offline-msg");
  await waitFor(async () => {
    const snapshot = await b.client.group(group);
    return snapshot?.name === "renamed";
  }, "commit applied");
  await waitFor(
    () => a.deliveries.some((entry) => entry.resolution === "confirmed"),
    "a confirmed delivery",
  );
  if (contents(b, group).filter((entry) => entry === "offline-msg").length !== 1) {
    throw new Error("offline-msg delivered more than once");
  }
  await assertConverged([a, b], group, new Set(["offline-msg"]));
  assertQuiet([a, b]);
  assertPeaks([a, b], network);
  const report = {
    ms: Date.now() - start,
    delivered: deliveredOf([a, b], group),
    peaks: peaksOf([a, b], network),
  };
  await closeAll([a, b], network);
  return report;
}

/**
 * Scenario 9: a commit staged while the port refuses dials meets a rejecting relay when the dials
 * recover — `rolled_back`, the profile is unchanged, and a later send still lands.
 */
export async function scenarioOfflineCommitRollback(): Promise<ScenarioReport> {
  const start = Date.now();
  const network = fakeRelay(71);
  const gate = { refused: false };
  const { a, b, group } = await openPair(network, {
    a: { port: gatedPort(network.port("a"), gate) },
  });
  gate.refused = true;
  const commits = () => network.published().filter((entry) => entry.event.kind === 445).length;
  const publishedBefore = commits();
  const snapshotBefore = await a.client.group(group);
  const epochBefore = snapshotBefore?.epoch;
  await a.client.selfUpdate(group);
  await waitFor(
    () => a.deliveries.some((entry) => entry.resolution === "pending"),
    "a pending outbound",
  );
  if (commits() !== publishedBefore) {
    throw new Error("an event was transmitted while dials were refused");
  }
  gate.refused = false;
  // The dial recovers but the relays refuse the commit — every target is terminal, so the
  // commit rolls back instead of hanging on retry.
  network.mode(RELAY_1, "reject");
  network.mode(RELAY_2, "reject");
  await waitFor(
    () => a.deliveries.some((entry) => entry.resolution === "rolled_back"),
    "a rolled_back delivery",
  );
  network.mode(RELAY_1, "normal");
  network.mode(RELAY_2, "normal");
  const aSnapshot = await a.client.group(group);
  if (aSnapshot?.epoch !== epochBefore) {
    throw new Error("rolled-back commit did not return the group to its prior epoch");
  }
  await send(a, group, "after-rollback");
  await waitFor(() => contents(b, group).includes("after-rollback"), "b received after-rollback");
  await assertConverged([a, b], group, new Set(["after-rollback"]));
  assertQuiet([a, b]);
  assertPeaks([a, b], network);
  const report = {
    ms: Date.now() - start,
    delivered: deliveredOf([a, b], group),
    peaks: peaksOf([a, b], network),
  };
  await closeAll([a, b], network);
  return report;
}

/**
 * Scenario 10: B's five chats sit on the group relays before C joins; every pre-join kind-445 C
 * ingests reports `predates_local_copy` — the engine retains it for re-peel but the relay copy is
 * redundant — so the group cursor credits it like an accepted event: after a restart C's kind-445
 * `since` clears the newest pre-join stamp and the range is never replayed.
 */
export async function scenarioPreJoinCursor(): Promise<ScenarioReport> {
  const start = Date.now();
  // The whole pre-join era runs three days in the past: the Welcome rumor's `created_at` comes
  // from the ambient wall clock at wrap build (the engine stamps it, not the injected clock), so
  // only events stamped well below real `now + 300 s` classify `predates_local_copy`
  // deterministically.
  const skew: Skew = { offsetMs: -259_200_000 };
  const network = fakeRelay(101);
  const { a, b, group } = await openPair(network, { a: { skew }, b: { skew } });
  const snapshot = await a.client.group(group);
  const route = snapshot?.live?.nostrGroupId;
  if (snapshot === undefined || route === undefined) {
    throw new Error("a holds no group snapshot");
  }
  // The pre-join feed spans ~20 minutes, so its older half sits outside the 600 s resubscribe
  // overlap — giving the replay check below a cohort the fixed cursor must protect.
  for (let i = 0; i < 2; i += 1) {
    // oxlint-disable-next-line no-await-in-loop -- ordered pre-join feed
    await send(b, group, `pre-join#${i}`);
  }
  skew.offsetMs += 1_200_000;
  for (let i = 2; i < 5; i += 1) {
    // oxlint-disable-next-line no-await-in-loop -- ordered pre-join feed
    await send(b, group, `pre-join#${i}`);
  }
  await waitFor(() => contents(a, group).includes("pre-join#4"), "a received the pre-join chats");
  let newestPreJoin = 0;
  let earliestPreJoin = Number.MAX_SAFE_INTEGER;
  for (const relay of snapshot.live?.relays ?? []) {
    for (const event of network.events(relay)) {
      if (event.kind !== 445) {
        continue;
      }
      if (Number(event.createdAt) > newestPreJoin) {
        newestPreJoin = Number(event.createdAt);
      }
      if (Number(event.createdAt) < earliestPreJoin) {
        earliestPreJoin = Number(event.createdAt);
      }
    }
  }
  // Clocks return to real time, so the invite's Welcome rumor stamps real `now` — ~3 days after
  // the newest feed stamp and ~3 days before C's own traffic.
  skew.offsetMs = 0;

  // C's port records every subscribe call and, per sub, every delivered event — across restarts.
  const cSubs: Array<{ relay: string; filters: RelayFilter[] }> = [];
  const cDeliveries: Array<{ sub: number; event: NostrEvent }> = [];
  const inner = network.port("c");
  const c = await startClient({
    name: "c",
    network,
    skew,
    port: {
      publish: async (relay, event) => inner.publish(relay, event),
      connect: async (relay) => inner.connect(relay),
      subscribe: (relay, filters, handlers) => {
        const sub = cSubs.push({ relay, filters }) - 1;
        return inner.subscribe(relay, filters, {
          ...handlers,
          event: (event) => {
            cDeliveries.push({ sub, event });
            handlers.event(event);
          },
        });
      },
    },
  });
  await identity(c);
  const invitees = await a.client.lookupInvitees(c.signer.pubkey, [LOOKUP]);
  await a.client.invite(group, invitees);
  await waitFor(
    () => c.invites.some((event) => event.type === "joined" && event.group === group),
    "c joined invite",
  );
  const isRoute445 = (filter: RelayFilter): boolean =>
    filter.kinds?.includes(445) === true && (filter["#h"] ?? []).includes(route);
  // C has saved a group cursor for the route. Its value is the discriminator: a credited
  // `predates_local_copy` backlog lets it reach the newest stamped event (the invite commit
  // credits it further still), while a capped `transport_deferred` backlog pins it at the
  // earliest deferred `createdAt − 1` — no post-join traffic is sent, precisely so nothing newer
  // re-credits past a cap.
  await waitFor(() => {
    const sub = cSubs.find((entry) => entry.filters.some(isRoute445));
    const bytes = c.stores.cursors.bytes();
    return (
      sub !== undefined &&
      bytes !== undefined &&
      decodeClientState(bytes).cursors[`group|${route}|${sub.relay}`] !== undefined
    );
  }, "c saved a group cursor for the route");

  const subsBefore = cSubs.length;
  await restart(c);
  await waitFor(
    () => cSubs.slice(subsBefore).some((entry) => entry.filters.some(isRoute445)),
    "c reopened the kind-445 subscription for the route",
  );
  let reopened: { relay: string; filters: RelayFilter[] } | undefined;
  let reopenedIndex = -1;
  for (const [index, entry] of cSubs.entries()) {
    if (index >= subsBefore && entry.filters.some(isRoute445)) {
      reopened = entry;
      reopenedIndex = index;
    }
  }
  if (reopened === undefined) {
    throw new Error("c reopened no kind-445 subscription for the route");
  }
  // The discriminating observable is the reopened filter's `since`. A credited
  // `predates_local_copy` backlog leaves the cursor at or past the newest stamped event, so
  // `since = cursor − 600` reaches the pre-join overlap boundary; a capped `transport_deferred`
  // backlog pins the cursor at the earliest deferred `createdAt − 1`, so `since` sits below the
  // whole range and refetches all of it.
  const since = reopened.filters.find(isRoute445)?.since;
  const overlapBoundary = newestPreJoin - GROUP_OVERLAP_SECS;
  if (earliestPreJoin >= overlapBoundary) {
    throw new Error(`the pre-join feed does not span the overlap boundary ${overlapBoundary}`);
  }
  if (since === undefined || since < overlapBoundary) {
    throw new Error(
      `reopened since ${String(since)} does not reach the pre-join overlap boundary ${overlapBoundary}`,
    );
  }
  const replayed = cDeliveries.filter(
    (delivery) =>
      delivery.sub === reopenedIndex && Number(delivery.event.createdAt) < overlapBoundary,
  );
  if (replayed.length > 0) {
    throw new Error(`the reopened sub replayed ${replayed.length} events older than the overlap`);
  }

  assertQuiet([a, b, c]);
  assertPeaks([a, b, c], network);
  const report = {
    ms: Date.now() - start,
    delivered: deliveredOf([a, b, c], group),
    peaks: peaksOf([a, b, c], network),
  };
  await closeAll([a, b, c], network);
  return report;
}
