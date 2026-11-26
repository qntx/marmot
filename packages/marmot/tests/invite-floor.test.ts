/**
 * The sticky inbox floor: a gift wrap the signer refused or failed to open holds its inbox cursor
 * key below it — across later accepted wraps, the 32-wrap budget and restarts — until a replay
 * whose `since` covered it reaches EOSE without a signer-unavailable verdict, and is given up after
 * `DEFERRED_PEEL_RESIDENCE_SECS`. One relay network, cursor store and clock are shared across the
 * `MarmotClient.open` calls of a test, so a second open is a real restart.
 */

import { describe, expect, test } from "vite-plus/test";

import { finalizeEvent, Keys } from "@qntx/nostr";

import {
  decodeCallValue,
  encodeDrainResponse,
  encodeOpenResponse,
  encodeResponse,
} from "../src/backend/codec.ts";
import type { Json } from "../src/backend/codec.ts";
import type { Signer } from "../src/backend/engine.ts";
import type { Backend, BridgeHandle } from "../src/backend/port.ts";
import type {
  BridgeRequest,
  BridgeResponse,
  DrainResponse,
  GroupSnapshot,
  InputOutcome,
  NostrEvent,
} from "../src/backend/protocol.ts";
import { utf8Decode, utf8Encode } from "../src/backend/utf8.ts";
import { MarmotClient } from "../src/client/client.ts";
import {
  DEFERRED_PEEL_RESIDENCE_SECS,
  GIFT_WRAP_BUDGET,
  GIFT_WRAP_WINDOW_MS,
  RESUBSCRIBE_DELAY_MS,
} from "../src/client/pump.ts";
import { decodeClientState, encodeClientState } from "../src/client/state.ts";
import type { ClientState } from "../src/client/state.ts";
import type { InviteEvent } from "../src/invites/index.ts";
import { manualClock } from "../src/testing/clock.ts";
import type { ManualClock } from "../src/testing/clock.ts";
import { memoryCursorStore } from "../src/testing/cursors.ts";
import { memoryKv } from "../src/testing/memory.ts";
import { fakeRelay } from "../src/testing/relay.ts";
import { manualTimer } from "../src/testing/timer.ts";
import type { ManualTimer } from "../src/testing/timer.ts";
import {
  GIFT_WRAP_OVERLAP_SECS,
  GROUP_OVERLAP_SECS,
  groupCursorKey,
  inboxCursorKey,
} from "../src/transport/plan.ts";
import type { RelayFilter, RelayPort, SubscriptionHandlers } from "../src/transport/port.ts";

const ACCOUNT = "a".repeat(64);
const RELAY = "wss://r1.example";
const RELAY2 = "wss://r2.example";
const GROUP = "cc".repeat(32);
const ROUTE = "dd".repeat(32);
const DONE: BridgeResponse = { type: "ok", result: { type: "done" } };
const KEYS = Keys.fromSecretKey(new Uint8Array(32).fill(5));
const KEY = inboxCursorKey(RELAY);
const KEY2 = inboxCursorKey(RELAY2);

const DAY = 86_400;
/** `manualClock`'s default wall reading, seconds. */
const NOW = 1_700_000_000;
/** The seeded inbox cursor: five days ago. */
const C0 = NOW - 5 * DAY;
/** The refused wrap, inside the seeded sub's overlap window. */
const T1 = C0 - 10_000;
/** An accepted wrap more than the overlap window newer than T1. */
const T2 = T1 + GIFT_WRAP_OVERLAP_SECS + 10_000;

function event(partial: { kind: number; createdAt: number; tags: string[][]; content?: string }) {
  const wire = finalizeEvent(
    {
      kind: partial.kind,
      tags: partial.tags,
      content: partial.content ?? "",
      created_at: partial.createdAt,
    },
    KEYS,
  );
  const evt: NostrEvent = {
    id: wire.id,
    pubkey: wire.pubkey,
    createdAt: BigInt(wire.created_at),
    kind: wire.kind,
    tags: wire.tags.map((tag) => [...tag]),
    content: wire.content,
    sig: wire.sig,
  };
  return evt;
}

const wrap = (createdAt: number, content = ""): NostrEvent =>
  event({ kind: 1059, createdAt, tags: [["p", ACCOUNT]], content });

const REFUSED: InputOutcome = { type: "unavailable", category: "signer_refused" };
const FAILED: InputOutcome = { type: "unavailable", category: "signer_failed" };
const ACCEPTED: InputOutcome = { type: "accepted" };

type Script = {
  /** Per event id; anything unlisted is accepted. */
  outcomes: Map<string, InputOutcome>;
  drains: DrainResponse[];
  groups: GroupSnapshot[];
  calls: BridgeRequest[];
};

const freshScript = (): Script => ({ outcomes: new Map(), drains: [], groups: [], calls: [] });

function scriptedBackend(script: Script): Backend {
  return {
    name: "scripted",
    create: () => {
      const handle: BridgeHandle = {
        open: async () => {
          await Promise.resolve();
          return utf8Encode(
            JSON.stringify(encodeOpenResponse({ type: "ready", account: ACCOUNT })),
          );
        },
        call: async (bytes) => {
          await Promise.resolve();
          const { request } = decodeCallValue(JSON.parse(utf8Decode(bytes)) as Json);
          script.calls.push(request);
          const response = ((): BridgeResponse => {
            switch (request.type) {
              case "tick":
                return { type: "ok", result: { type: "ticked", nextWakeMs: undefined } };
              case "ingest":
                return {
                  type: "ok",
                  result: {
                    type: "ingested",
                    reports: request.events.map((e) => ({
                      event: e.id,
                      outcome: script.outcomes.get(e.id) ?? ACCEPTED,
                    })),
                  },
                };
              case "take_outbound":
                return { type: "ok", result: { type: "outbound", items: [] } };
              case "groups":
                return { type: "ok", result: { type: "groups", groups: script.groups } };
              case "group":
                return { type: "ok", result: { type: "group", group: script.groups[0] } };
              case "ensure_key_package":
              case "audit_key_package_slot":
              case "key_package_status":
                return {
                  type: "ok",
                  result: {
                    type: "key_package_status",
                    slot: "11".repeat(32),
                    current: undefined,
                    pending: false,
                    retained: 0,
                    nextDueMs: undefined,
                  },
                };
              default:
                return DONE;
            }
          })();
          return utf8Encode(JSON.stringify(encodeResponse(response)));
        },
        drain: async () => {
          await Promise.resolve();
          return utf8Encode(
            JSON.stringify(
              encodeDrainResponse(script.drains.shift() ?? { type: "events", events: [] }),
            ),
          );
        },
        close: async () => {
          await Promise.resolve();
        },
      };
      return handle;
    },
  };
}

function signer(): Signer {
  return {
    pubkey: ACCOUNT,
    signEvent: async (unsigned) =>
      Promise.resolve({
        id: "e".repeat(64),
        pubkey: unsigned.pubkey,
        createdAt: unsigned.createdAt,
        kind: unsigned.kind,
        tags: unsigned.tags,
        content: unsigned.content,
        sig: "5".repeat(128),
      }),
    nip44Encrypt: async (_peer, plaintext) => Promise.resolve(`e${plaintext}`),
    nip44Decrypt: async (_peer, ciphertext) =>
      Promise.resolve(ciphertext.startsWith("e") ? ciphertext.slice(1) : ciphertext),
  };
}

/** One recorded subscribe; `withheld` subs never see EOSE until the test fires it. */
type Sub = { relay: string; filters: RelayFilter[]; handlers: SubscriptionHandlers };

/** What survives a restart: relay stores, cursor bytes, kv rows, the wall clock. */
type World = {
  network: ReturnType<typeof fakeRelay>;
  cursors: ReturnType<typeof memoryCursorStore>;
  kv: ReturnType<typeof memoryKv>;
  clock: ManualClock;
  subs: Sub[];
  /** While set, relay EOSEs are swallowed (a replay cut short before EOSE). */
  withholdEose: boolean;
};

const world = (): World => ({
  network: fakeRelay(),
  cursors: memoryCursorStore(),
  kv: memoryKv(),
  clock: manualClock(),
  subs: [],
  withholdEose: false,
});

type Fixture = { client: MarmotClient; timer: ManualTimer; invites: InviteEvent[] };

async function spawn(w: World, script: Script, name: string): Promise<Fixture> {
  const timer = manualTimer(w.clock);
  const inner = w.network.port(name);
  const port: RelayPort = {
    publish: async (relay, evt) => inner.publish(relay, evt),
    connect: async (relay) => inner.connect(relay),
    subscribe: (relay, filters, handlers) => {
      const sub: Sub = { relay, filters, handlers };
      w.subs.push(sub);
      return inner.subscribe(relay, filters, {
        event: handlers.event,
        closed: handlers.closed,
        eose: () => {
          if (!w.withholdEose) {
            handlers.eose();
          }
        },
      });
    },
  };
  const client = await MarmotClient.open({
    backend: scriptedBackend(script),
    kv: w.kv,
    cursors: w.cursors,
    signer: signer(),
    clock: w.clock,
    timer,
    relays: port,
    allowLoopback: true,
  });
  const invites: InviteEvent[] = [];
  client.onInvite((invite) => {
    invites.push(invite);
  });
  return { client, timer, invites };
}

const seed = async (w: World, state: Partial<ClientState>): Promise<void> => {
  await w.cursors.save(
    encodeClientState({ cursors: {}, floors: {}, inbox: [RELAY], nip44Extended: true, ...state }),
  );
};

/** Store `events` on `relay` before any client subscribes (a relay's retained backlog). */
const store = async (w: World, relay: string, events: NostrEvent[]): Promise<void> => {
  for (const evt of events) {
    // oxlint-disable-next-line no-await-in-loop -- publish order is the relay's replay order
    await w.network.port("peer").publish(relay, evt);
  }
};

const macrotask = async (): Promise<void> =>
  new Promise((resolve) => {
    setTimeout(resolve);
  });

/** Let queued work settle, then fire timers due within `ms` (the clock follows the timer). */
async function settle(timer: ManualTimer, ms = 0): Promise<void> {
  await macrotask();
  for (let round = 0; round < 8; round += 1) {
    const next = timer.next();
    if (next === undefined || next > ms) {
      break;
    }
    timer.tick(ms);
    // oxlint-disable-next-line no-await-in-loop -- each tick's fallout must drain before the next
    await macrotask();
  }
}

const persisted = (w: World): ClientState => decodeClientState(w.cursors.bytes());

const inboxSubs = (w: World, relay = RELAY): Sub[] =>
  w.subs.filter(
    (sub) => sub.relay === relay && sub.filters.some((f) => f.kinds?.includes(1059) === true),
  );

const ingests = (script: Script): number =>
  script.calls.filter((call) => call.type === "ingest").length;

/** The event count of every `ingest` call, in order. */
const ingestSizes = (script: Script): number[] =>
  script.calls.flatMap((call) => (call.type === "ingest" ? [call.events.length] : []));

const PENDING_REFUSED: InviteEvent = { type: "pending", reason: "signer_refused" };

describe("sticky inbox floor", () => {
  test("(A) a refused wrap replays after restart: pending again, retryInvites joins", async () => {
    const w = world();
    await seed(w, { cursors: { [KEY]: C0 } });
    const refused = wrap(T1);

    const s1 = freshScript();
    s1.outcomes.set(refused.id, REFUSED);
    const f1 = await spawn(w, s1, "gen1");
    await settle(f1.timer, 5000);
    await w.network.port("peer").publish(RELAY, refused);
    await settle(f1.timer, 5000);
    expect(f1.invites).toStrictEqual([PENDING_REFUSED]);
    expect(persisted(w).cursors[KEY]).toBe(T1 - 1);
    await f1.client.close();

    const s2 = freshScript();
    s2.outcomes.set(refused.id, REFUSED);
    const f2 = await spawn(w, s2, "gen2");
    await settle(f2.timer, 5000);
    expect(inboxSubs(w).at(-1)?.filters[0]?.since).toBe(T1 - 1 - GIFT_WRAP_OVERLAP_SECS);
    expect(f2.invites).toStrictEqual([PENDING_REFUSED]);

    s2.outcomes.set(refused.id, ACCEPTED);
    s2.drains.push({ type: "events", events: [{ type: "group_joined", group: GROUP }] });
    await f2.client.retryInvites();
    await settle(f2.timer, 5000);
    expect(f2.invites).toStrictEqual([PENDING_REFUSED, { type: "joined", group: GROUP }]);
    expect(persisted(w).cursors[KEY]).toBe(T1);
    await f2.client.close();
  });

  test("(B) a later accepted wrap past the overlap window cannot move the cursor past the floor", async () => {
    const w = world();
    await seed(w, { cursors: { [KEY]: C0 } });
    const refused = wrap(T1);
    const accepted = wrap(T2);

    const s1 = freshScript();
    s1.outcomes.set(refused.id, REFUSED);
    const f1 = await spawn(w, s1, "gen1");
    await settle(f1.timer, 5000);
    await w.network.port("peer").publish(RELAY, refused);
    await settle(f1.timer, 5000);
    expect(f1.invites).toStrictEqual([PENDING_REFUSED]);
    await w.network.port("peer").publish(RELAY, accepted);
    await settle(f1.timer, 5000);
    expect(ingests(s1)).toBe(2);
    await f1.client.close();

    // Restart: the floored cursor still covers the refused wrap, so the relay replays it.
    const s2 = freshScript();
    s2.outcomes.set(refused.id, REFUSED);
    const f2 = await spawn(w, s2, "gen2");
    await settle(f2.timer, 5000);
    expect(inboxSubs(w).at(-1)?.filters[0]?.since).toBe(T1 - 1 - GIFT_WRAP_OVERLAP_SECS);
    expect(f2.invites).toStrictEqual([PENDING_REFUSED]);
    // The accepted wrap is credited only up to the floor, before and after the restart.
    expect(persisted(w).cursors[KEY]).toBe(T1 - 1);

    s2.outcomes.set(refused.id, ACCEPTED);
    s2.drains.push({ type: "events", events: [{ type: "group_joined", group: GROUP }] });
    await f2.client.retryInvites();
    await settle(f2.timer, 5000);
    expect(f2.invites).toStrictEqual([PENDING_REFUSED, { type: "joined", group: GROUP }]);
    expect(persisted(w).cursors[KEY]).toBe(T2);
    await f2.client.close();
  });

  test("a covering replay that reaches EOSE with every wrap opened releases the floor", async () => {
    const w = world();
    await seed(w, { cursors: { [KEY]: T1 - 1 }, floors: { [KEY]: T1 } });
    await store(w, RELAY, [wrap(T1), wrap(T2)]);
    const s = freshScript();
    const f = await spawn(w, s, "gen1");
    await settle(f.timer, 5000);
    expect(inboxSubs(w)[0]?.filters[0]?.since).toBe(T1 - 1 - GIFT_WRAP_OVERLAP_SECS);
    expect(f.invites).toStrictEqual([]);
    expect(persisted(w).floors).toStrictEqual({});
    expect(persisted(w).cursors[KEY]).toBe(T2);
    await f.client.close();
  });

  test("a replay closed before EOSE changes nothing; the reopened one settles it", async () => {
    const w = world();
    await seed(w, { cursors: { [KEY]: T1 - 1 }, floors: { [KEY]: T1 } });
    await store(w, RELAY, [wrap(T1), wrap(T2)]);
    const s = freshScript();
    w.withholdEose = true;
    const f = await spawn(w, s, "gen1");
    await settle(f.timer, 5000);
    expect(ingests(s)).toBe(1);
    // The relay ends the replay before EOSE: no evidence, the floor stands.
    inboxSubs(w)[0]?.handlers.closed("done");
    await settle(f.timer, 0);
    expect(persisted(w).floors).toStrictEqual({ [KEY]: T1 });
    expect(persisted(w).cursors[KEY]).toBe(T1 - 1);

    w.withholdEose = false;
    await settle(f.timer, RESUBSCRIBE_DELAY_MS);
    expect(inboxSubs(w)).toHaveLength(2);
    expect(persisted(w).floors).toStrictEqual({});
    expect(persisted(w).cursors[KEY]).toBe(T2);
    await f.client.close();
  });

  test("the floor moves to the earliest wrap of the covering pass that is still unopened", async () => {
    const w = world();
    const t3 = T1 + 1000;
    await seed(w, { cursors: { [KEY]: T1 - 1 }, floors: { [KEY]: T1 } });
    const first = wrap(T1);
    const second = wrap(t3);
    await store(w, RELAY, [first, second, wrap(T2)]);
    const s = freshScript();
    s.outcomes.set(second.id, FAILED);
    const f = await spawn(w, s, "gen1");
    await settle(f.timer, 5000);
    expect(f.invites).toStrictEqual([{ type: "pending", reason: "signer_failed" }]);
    expect(persisted(w).floors).toStrictEqual({ [KEY]: t3 });
    expect(persisted(w).cursors[KEY]).toBe(t3 - 1);
    await f.client.close();
  });

  test("a floor older than the residence horizon is dropped at open and its wrap given up", async () => {
    const w = world();
    const old = NOW - DEFERRED_PEEL_RESIDENCE_SECS - DAY;
    const recent = NOW - DEFERRED_PEEL_RESIDENCE_SECS + DAY;
    await seed(w, {
      cursors: { [KEY]: old - 1, [KEY2]: recent - 1 },
      floors: { [KEY]: old, [KEY2]: recent },
      inbox: [RELAY, RELAY2],
    });
    const oldWrap = wrap(old);
    const recentWrap = wrap(recent);
    await store(w, RELAY, [oldWrap]);
    await store(w, RELAY2, [recentWrap]);
    const s = freshScript();
    // Still refusing: the old wrap fails, the recent one is refused.
    s.outcomes.set(oldWrap.id, FAILED);
    s.outcomes.set(recentWrap.id, REFUSED);
    const f = await spawn(w, s, "gen1");
    await settle(f.timer, 5000);
    // The given-up wrap surfaces no pending, sets no floor and credits its key.
    expect(f.invites).toStrictEqual([PENDING_REFUSED]);
    expect(persisted(w).floors).toStrictEqual({ [KEY2]: recent });
    expect(persisted(w).cursors[KEY]).toBe(old);
    expect(persisted(w).cursors[KEY2]).toBe(recent - 1);
    await f.client.close();
  });

  test("a v2 cursor blob is a full refetch with no floors", async () => {
    const w = world();
    await w.cursors.save(
      utf8Encode(
        JSON.stringify({ v: 2, cursors: { [KEY]: C0 }, inbox: [RELAY], nip44Extended: true }),
      ),
    );
    const f = await spawn(w, freshScript(), "gen1");
    await settle(f.timer, 5000);
    // The v2 blob decodes empty: no inbox list, so no inbox subscription and no floors.
    expect(inboxSubs(w)).toHaveLength(0);
    expect(persisted(w).floors).toStrictEqual({});
    expect(persisted(w).cursors).toStrictEqual({});
    await f.client.close();
  });

  test("a signer that keeps refusing never resubscribes on its own within a session", async () => {
    const w = world();
    await seed(w, { cursors: { [KEY]: C0 } });
    const a = wrap(T1);
    const b = wrap(T1 + 500);
    await store(w, RELAY, [a, b]);
    const s = freshScript();
    s.outcomes.set(a.id, REFUSED);
    s.outcomes.set(b.id, REFUSED);
    const f = await spawn(w, s, "gen1");
    await settle(f.timer, 5000);
    expect(inboxSubs(w)).toHaveLength(1);
    expect(ingests(s)).toBe(1);
    for (let round = 0; round < 12; round += 1) {
      // oxlint-disable-next-line no-await-in-loop -- one simulated minute per round
      await settle(f.timer, 60_000);
    }
    expect(inboxSubs(w)).toHaveLength(1);
    expect(ingests(s)).toBe(1);
    expect(f.invites).toStrictEqual([PENDING_REFUSED]);
    expect(persisted(w).floors).toStrictEqual({ [KEY]: T1 });

    // Only an explicit retry replays, once.
    await f.client.retryInvites();
    for (let round = 0; round < 12; round += 1) {
      // oxlint-disable-next-line no-await-in-loop -- one simulated minute per round
      await settle(f.timer, 60_000);
    }
    expect(inboxSubs(w)).toHaveLength(2);
    expect(ingests(s)).toBe(2);
    expect(f.invites).toStrictEqual([PENDING_REFUSED, PENDING_REFUSED]);
    expect(persisted(w).floors).toStrictEqual({ [KEY]: T1 });
    expect(persisted(w).cursors[KEY]).toBe(T1 - 1);
    await f.client.close();
  });

  test("a budget-deferred floor wrap stays covered while buffered and re-floors on its ingest", async () => {
    const w = world();
    await seed(w, { cursors: { [KEY]: T1 - 1 }, floors: { [KEY]: T1 } });
    const refused = wrap(T1);
    const newer = Array.from({ length: GIFT_WRAP_BUDGET }, (_v, i) => wrap(T1 + 1 + i));
    // Replay order is store order: the floor wrap comes last and misses the first window.
    await store(w, RELAY, [...newer, refused]);
    const s = freshScript();
    s.outcomes.set(refused.id, REFUSED);
    const f = await spawn(w, s, "gen1");
    await settle(f.timer, 25);
    expect(ingestSizes(s)).toStrictEqual([GIFT_WRAP_BUDGET]);
    // The pass settled the floor (none of its ingested wraps was refused), but the buffered wrap
    // still holds the cursor where `since` covers it.
    expect(persisted(w).floors).toStrictEqual({});
    expect(persisted(w).cursors[KEY]).toBe(T1 - 1);

    await settle(f.timer, GIFT_WRAP_WINDOW_MS);
    expect(ingestSizes(s)).toStrictEqual([GIFT_WRAP_BUDGET, 1]);
    expect(f.invites).toStrictEqual([PENDING_REFUSED]);
    expect(persisted(w).floors).toStrictEqual({ [KEY]: T1 });
    expect(persisted(w).cursors[KEY]).toBe(T1 - 1);
    expect(inboxSubs(w)).toHaveLength(1);
    await f.client.close();
  });

  test("group keys never carry a floor and keep the per-batch cap", async () => {
    const w = world();
    const key = groupCursorKey(ROUTE, RELAY);
    await seed(w, { cursors: { [key]: C0 }, inbox: [] });
    const s = freshScript();
    s.groups = [
      {
        id: GROUP,
        access: "active",
        name: "g",
        description: "",
        epoch: 1n,
        members: [],
        live: {
          state: "stable",
          admins: [],
          ownAccountAdmin: true,
          relays: [RELAY],
          nostrGroupId: ROUTE,
          encryptedMedia: undefined,
          agentTextStream: undefined,
          avatar: undefined,
          retentionSeconds: 0n,
          disbandEnabled: true,
          disbanding: false,
        },
      },
    ];
    const deferred = event({ kind: 445, createdAt: C0 - 100, tags: [["h", ROUTE]] });
    const later = event({ kind: 445, createdAt: C0 + GROUP_OVERLAP_SECS, tags: [["h", ROUTE]] });
    s.outcomes.set(deferred.id, { type: "unavailable", category: "transport_deferred" });
    const f = await spawn(w, s, "gen1");
    await settle(f.timer, 5000);
    await w.network.port("peer").publish(RELAY, deferred);
    await settle(f.timer, 5000);
    expect(persisted(w).cursors[key]).toBe(C0 - 101);
    await w.network.port("peer").publish(RELAY, later);
    await settle(f.timer, 5000);
    // Today's rule: the cap holds only within its batch.
    expect(persisted(w).cursors[key]).toBe(C0 + GROUP_OVERLAP_SECS);
    expect(persisted(w).floors).toStrictEqual({});
    await f.client.close();
  });
});
