/**
 * Malicious-relay tests: a scripted `RelayPort` (events, EOSE and closes pushed at will; every
 * subscribe and close recorded) drives `MarmotClient` against a fake backend whose ingest can be
 * held. The cases pin the pump's defenses: bounded inbound buffering, oversized-event drops and
 * cursor rules.
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
import { SignerInvalidPayloadError } from "../src/backend/engine.ts";
import { encodeKvBatch } from "../src/backend/kv.ts";
import type { Backend, BridgeHandle } from "../src/backend/port.ts";
import type {
  BridgeRequest,
  BridgeResponse,
  DrainResponse,
  GroupSnapshot,
  InputReport,
  NostrEvent,
  Outbound,
  PublishResolution,
  SignerRequest,
} from "../src/backend/protocol.ts";
import { utf8Decode, utf8Encode } from "../src/backend/utf8.ts";
import { MarmotClient } from "../src/client/client.ts";
import {
  GIFT_WRAP_BUDGET,
  GIFT_WRAP_WINDOW_MS,
  INGEST_BATCH_MAX,
  MAX_PENDING_INBOUND,
  RESUBSCRIBE_DELAY_MS,
} from "../src/client/pump.ts";
import { decodeClientState, encodeClientState } from "../src/client/state.ts";
import { manualClock } from "../src/testing/clock.ts";
import { memoryCursorStore } from "../src/testing/cursors.ts";
import { memoryKv } from "../src/testing/memory.ts";
import { manualTimer } from "../src/testing/timer.ts";
import type { ManualTimer } from "../src/testing/timer.ts";
import {
  GIFT_WRAP_OVERLAP_SECS,
  GROUP_OVERLAP_SECS,
  groupCursorKey,
  inboxCursorKey,
} from "../src/transport/plan.ts";
import type { RelayFilter, RelayPort, SubscriptionHandlers } from "../src/transport/port.ts";
import { verifyInbound } from "../src/transport/verify.ts";

const ACCOUNT = "a".repeat(64);
const RELAY = "wss://r1.example";
const RELAY2 = "wss://r2.example";
const GROUP = "cc".repeat(32);
const ROUTE = "dd".repeat(32);
const DONE: BridgeResponse = { type: "ok", result: { type: "done" } };

const KEYS = Keys.fromSecretKey(new Uint8Array(32).fill(7));

/**
 * Genuinely signed inbound events — the pump verifies id + signature before dedupe, so fixtures
 * cannot fake them. Signing and verifying dominate this file's runtime, so every event is cached by
 * its parameters: a repeated parameter set returns the same object, which also hits the pump's
 * WeakSet verification cache when a later test delivers it. Set `kind`/`createdAt`/`tags`/
 * `content` here, before signing; the id, pubkey and sig come out of `finalizeEvent`.
 */
const signed = new Map<string, NostrEvent>();

function event(partial: {
  kind?: number;
  createdAt?: bigint;
  tags?: string[][];
  content?: string;
}): NostrEvent {
  const key = `${partial.kind ?? 445}:${partial.createdAt ?? 1000n}:${JSON.stringify(
    partial.tags ?? [],
  )}:${partial.content ?? ""}`;
  const hit = signed.get(key);
  if (hit !== undefined) {
    return hit;
  }
  const wire = finalizeEvent(
    {
      kind: partial.kind ?? 445,
      tags: partial.tags ?? [],
      content: partial.content ?? "",
      created_at: Number(partial.createdAt ?? 1000n),
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
  signed.set(key, evt);
  return evt;
}

const groupEvent = (createdAt = 1000n, content = ""): NostrEvent =>
  event({ kind: 445, createdAt, tags: [["h", ROUTE]], content });

/**
 * One shared pool of distinct events for the flood and batch tests: ids differ by content,
 * timestamps cycle 600–999 (below the 10 000 seeded cursor, above the 500 one so a backlog fold is
 * visible), and every test delivers the same objects — each signs once and each verifies once in
 * the pump's WeakSet for the whole file.
 */
const FILL_TOP = 999;
const fill = (i: number): NostrEvent => groupEvent(600n + BigInt(i % 400), `f${i}`);

// Warm the shared pool at module load: the per-test timeouts must cover the assertions, not a
// thousand signatures and verifications. `verifyInbound` marks each object once, so every later
// delivery hits the pump's WeakSet fast path.
const FILL_COUNT = MAX_PENDING_INBOUND + 1;
for (let i = 0; i < FILL_COUNT; i += 1) {
  verifyInbound(fill(i));
}

const wrapEvent = (createdAt = 1000n, content = ""): NostrEvent =>
  event({ kind: 1059, createdAt, tags: [["p", ACCOUNT]], content });

function snapshot(relays: string[] = [RELAY]): GroupSnapshot {
  return {
    id: GROUP,
    name: "g",
    description: "",
    epoch: 1n,
    state: "stable",
    access: "active",
    members: [],
    relays,
    nostrGroupId: ROUTE,
    ownAccountAdmin: true,
    encryptedMedia: undefined,
    agentTextStream: undefined,
    avatar: undefined,
    retentionSeconds: 0n,
    disbandEnabled: true,
    disbanding: false,
  };
}

/**
 * One subscription the relay accepted. `closes` counts client-side close calls; relay-initiated
 * ends go through `close`.
 */
type ScriptedSub = {
  relay: string;
  filters: RelayFilter[];
  handlers: SubscriptionHandlers;
  open: boolean;
  closes: number;
};

/**
 * A relay pool under the test's finger: `subscribe` records the filters and hands back a close that
 * only stops delivery (per the `RelayPort` contract, `closed` never fires for it); `event`, `eose`
 * and `terminate` push whatever the script wants, ignoring the filters — a hostile relay is not
 * obliged to honour them.
 */
function scriptedRelays(): {
  port: RelayPort;
  subs: ScriptedSub[];
  openSubs: () => ScriptedSub[];
} {
  const subs: ScriptedSub[] = [];
  return {
    subs,
    openSubs: () => subs.filter((sub) => sub.open),
    port: {
      publish: async () => {
        await Promise.resolve();
        return "accepted";
      },
      connect: async () => {
        await Promise.resolve();
        return true;
      },
      subscribe: (relay, filters, handlers) => {
        const sub: ScriptedSub = { relay, filters, handlers, open: true, closes: 0 };
        subs.push(sub);
        return () => {
          sub.closes += 1;
          sub.open = false;
        };
      },
    },
  };
}

const push = (sub: ScriptedSub, event: NostrEvent): void => {
  if (sub.open) {
    sub.handlers.event(event);
  }
};

const pushEose = (sub: ScriptedSub): void => {
  if (sub.open) {
    sub.handlers.eose();
  }
};

/** A relay-initiated close: `closed` fires and the sub is dead from then on. */
const terminate = (sub: ScriptedSub, reason = "done"): void => {
  if (sub.open) {
    sub.open = false;
    sub.handlers.closed(reason);
  }
};

type Script = {
  /** FIFO drain responses; the queue falls back to empty events. */
  drains: DrainResponse[];
  /** The ingest reports for the next ingest; falls back to one `accepted` per event. */
  reports: InputReport[] | "auto";
  /** When set, every `ingest` call parks on this gate before answering. */
  ingestGate: (() => Promise<void>) | undefined;
  /**
   * When set, the next `ingest` suspends on these signer requests; the follow-up `signer_reply`
   * then answers the ingested reports for the suspended call.
   */
  signerRequests: SignerRequest[] | undefined;
  /** The response for the `signer_reply` resolving a suspended ingest. */
  signerReplyResponse: BridgeResponse | undefined;
  /** Items `take_outbound` returns on the next call, then empty. */
  outbound: Outbound[];
  /** FIFO `report_publish` resolutions; falls back to `complete`. */
  resolutions: PublishResolution[];
  /** `groups` answer. */
  groups: GroupSnapshot[];
  /** `tick` answer (ms until wake). */
  nextWake: bigint | undefined;
  /** Emit `host.commit` (seq ascending) when these request types run. */
  commitOn: Array<BridgeRequest["type"]>;
  calls: BridgeRequest[];
};

function freshScript(): Script {
  return {
    drains: [],
    reports: "auto",
    ingestGate: undefined,
    signerRequests: undefined,
    signerReplyResponse: undefined,
    outbound: [],
    resolutions: [],
    groups: [],
    nextWake: undefined,
    commitOn: [],
    calls: [],
  };
}

function scriptedBackend(script: Script): Backend {
  let seq = 0n;
  return {
    name: "scripted",
    create: (host) => {
      const handle: BridgeHandle = {
        open: async () => {
          await Promise.resolve();
          return utf8Encode(
            JSON.stringify(encodeOpenResponse({ type: "ready", account: ACCOUNT })),
          );
        },
        call: async (bytes) => {
          const { request } = decodeCallValue(JSON.parse(utf8Decode(bytes)) as Json);
          script.calls.push(request);
          if (request.type === "ingest" && script.ingestGate !== undefined) {
            await script.ingestGate();
          }
          const response: BridgeResponse = ((): BridgeResponse => {
            switch (request.type) {
              case "tick":
                return {
                  type: "ok",
                  result: { type: "ticked", nextWakeMs: script.nextWake },
                };
              case "ingest": {
                const reports =
                  script.reports === "auto"
                    ? request.events.map((e) => ({
                        event: e.id,
                        outcome: { type: "accepted" as const },
                      }))
                    : script.reports;
                const ingested: BridgeResponse = {
                  type: "ok",
                  result: { type: "ingested", reports },
                };
                if (script.signerRequests !== undefined) {
                  const requests = script.signerRequests;
                  script.signerRequests = undefined;
                  script.signerReplyResponse = ingested;
                  return { type: "signer_pending", requests };
                }
                return ingested;
              }
              case "signer_reply":
                return script.signerReplyResponse ?? DONE;
              case "take_outbound": {
                const items = script.outbound;
                script.outbound = [];
                return { type: "ok", result: { type: "outbound", items } };
              }
              case "report_publish": {
                const resolution = script.resolutions.shift() ?? "complete";
                return { type: "ok", result: { type: "publish_resolution", resolution } };
              }
              case "groups":
                return { type: "ok", result: { type: "groups", groups: script.groups } };
              case "group":
                return {
                  type: "ok",
                  result: { type: "group", group: script.groups[0] },
                };
              default:
                return DONE;
            }
          })();
          if (script.commitOn.includes(request.type)) {
            seq += 1n;
            host.commit(
              encodeKvBatch({
                seq,
                ops: [{ type: "put", key: Uint8Array.of(1), value: Uint8Array.of(1) }],
              }),
            );
          }
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

type Fixture = {
  client: MarmotClient;
  script: Script;
  timer: ManualTimer;
  network: ReturnType<typeof scriptedRelays>;
  cursors: ReturnType<typeof memoryCursorStore>;
  kv: ReturnType<typeof memoryKv>;
  clock: ReturnType<typeof manualClock>;
};

async function open(script: Script, blob?: Uint8Array, hostSigner?: Signer): Promise<Fixture> {
  const timer = manualTimer();
  const network = scriptedRelays();
  const cursors = memoryCursorStore();
  const kv = memoryKv();
  const clock = manualClock();
  if (blob !== undefined) {
    await cursors.save(blob);
  }
  const client = await MarmotClient.open({
    backend: scriptedBackend(script),
    kv,
    cursors,
    signer: hostSigner ?? signer(),
    clock,
    timer,
    relays: network.port,
    allowLoopback: true,
  });
  return { client, script, timer, network, cursors, kv, clock };
}

const macrotask = async (): Promise<void> =>
  new Promise((resolve) => {
    setTimeout(resolve);
  });

/** Let queued work settle, then fire due timers — same discipline as `client.test.ts`. */
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

/** The events of every `ingest` call, in order. */
const ingested = (script: Script): NostrEvent[] =>
  script.calls.flatMap((call) => (call.type === "ingest" ? [...call.events] : []));

const groupSubs = (network: ReturnType<typeof scriptedRelays>): ScriptedSub[] =>
  network.subs.filter((sub) => sub.filters.some((filter) => filter.kinds?.includes(445) === true));

const mustSub = (subs: ScriptedSub[]): ScriptedSub => {
  const [sub] = subs;
  if (sub === undefined) {
    throw new Error("expected an open subscription");
  }
  return sub;
};

const mustSubOn = (subs: ScriptedSub[], relay: string): ScriptedSub => {
  const sub = subs.find((candidate) => candidate.relay === relay);
  if (sub === undefined) {
    throw new Error("expected an open subscription on the relay");
  }
  return sub;
};

const inboxSubs = (network: ReturnType<typeof scriptedRelays>): ScriptedSub[] =>
  network.subs.filter((sub) => sub.filters.some((filter) => filter.kinds?.includes(1059) === true));

/** One parked ingest: `ok` lets it answer, `fail` makes the call reject. */
type IngestGate = { ok: () => void; fail: () => void };

const popGate = (gates: IngestGate[]): IngestGate => {
  const gate = gates.shift();
  if (gate === undefined) {
    throw new Error("expected a parked ingest");
  }
  return gate;
};

describe("malicious relay", () => {
  test("an event flood while ingest is held is dropped past the pending cap", async () => {
    const script = freshScript();
    script.groups = [snapshot()];
    let releaseIngest: () => void = () => undefined;
    const hold = new Promise<void>((resolve) => {
      releaseIngest = resolve;
    });
    script.ingestGate = async () => hold;
    const key = groupCursorKey(ROUTE, RELAY);
    const fixture = await open(
      script,
      encodeClientState({ nip44Extended: true, cursors: { [key]: 10_000 }, inbox: [RELAY] }),
    );
    await settle(fixture.timer, 5000);
    const sub = mustSub(groupSubs(fixture.network));

    // Distinct valid kind-445 events past the pending cap while ingest never answers.
    // Their createdAts stay below the seeded cursor so the durable cursor never moves.
    for (let i = 0; i < MAX_PENDING_INBOUND + 1; i += 1) {
      push(sub, fill(i));
    }
    await macrotask();
    // The delivering subscription was closed by the pump, so later pushes land nowhere.
    expect(sub.open).toBe(false);
    expect(sub.closes).toBeGreaterThan(0);

    releaseIngest();
    await settle(fixture.timer, RESUBSCRIBE_DELAY_MS);
    // At most the pending cap ever reached the engine.
    expect(ingested(script).length).toBeLessThanOrEqual(4 * INGEST_BATCH_MAX);

    // The sub reopens once the resubscribe delay fires, at the durable cursor.
    const reopened = groupSubs(fixture.network).at(-1);
    expect(reopened).not.toBe(sub);
    expect(reopened?.filters[0]?.since).toBe(10_000 - GROUP_OVERLAP_SECS);
    await fixture.client.close();
  }, 30_000);

  test("an oversized inbound event never reaches ingest and never moves the cursor", async () => {
    const script = freshScript();
    script.groups = [snapshot()];
    const fixture = await open(
      script,
      encodeClientState({ nip44Extended: true, cursors: {}, inbox: [RELAY] }),
    );
    await settle(fixture.timer, 5000);
    const sub = mustSub(groupSubs(fixture.network));
    const small = groupEvent(2000n);
    push(sub, small);
    push(
      sub,
      event({
        kind: 445,
        createdAt: 3000n,
        tags: [["h", ROUTE]],
        content: "x".repeat(2 * 1024 * 1024),
      }),
    );
    await settle(fixture.timer, 25);
    // Only the small event ingested; the oversized one was dropped before buffering.
    expect(ingested(script).map((e) => e.id)).toStrictEqual([small.id]);
    expect(sub.open).toBe(true);
    await fixture.client.close();
  });

  test("a newest-first backlog closed before EOSE never moves the cursor", async () => {
    const script = freshScript();
    script.groups = [snapshot()];
    const key = groupCursorKey(ROUTE, RELAY);
    const fixture = await open(
      script,
      encodeClientState({ nip44Extended: true, cursors: { [key]: 500 }, inbox: [] }),
    );
    await settle(fixture.timer, 5000);
    const sub = mustSub(groupSubs(fixture.network));
    expect(sub.filters[0]?.since).toBe(Math.max(0, 500 - GROUP_OVERLAP_SECS));

    // Stored events replay newest-first; the relay dies before EOSE.
    for (const at of [2000n, 1999n, 1998n, 1997n]) {
      push(sub, groupEvent(at));
    }
    await settle(fixture.timer, 25);
    expect(ingested(script)).toHaveLength(4);
    terminate(sub);
    await settle(fixture.timer, RESUBSCRIBE_DELAY_MS);

    // The persisted cursor is untouched — nothing past it was ever confirmed.
    expect(decodeClientState(fixture.cursors.bytes()).cursors[key]).toBe(500);
    const reopened = groupSubs(fixture.network).at(-1);
    expect(reopened).not.toBe(sub);
    expect(reopened?.filters[0]?.since).toBe(Math.max(0, 500 - GROUP_OVERLAP_SECS));
    await fixture.client.close();
  });

  test("a newest-first inbox backlog closed before EOSE never moves the cursor", async () => {
    const script = freshScript();
    const key = inboxCursorKey(RELAY);
    const fixture = await open(
      script,
      encodeClientState({ nip44Extended: true, cursors: { [key]: 200_000 }, inbox: [RELAY] }),
    );
    await settle(fixture.timer, 5000);
    const sub = mustSub(inboxSubs(fixture.network));
    expect(sub.filters[0]?.since).toBe(200_000 - GIFT_WRAP_OVERLAP_SECS);

    for (const at of [300_000n, 290_000n, 280_000n]) {
      push(sub, wrapEvent(at));
    }
    await settle(fixture.timer, 25);
    expect(ingested(script)).toHaveLength(3);
    terminate(sub);
    await settle(fixture.timer, RESUBSCRIBE_DELAY_MS);

    expect(decodeClientState(fixture.cursors.bytes()).cursors[key]).toBe(200_000);
    const reopened = inboxSubs(fixture.network).at(-1);
    expect(reopened).not.toBe(sub);
    expect(reopened?.filters[0]?.since).toBe(200_000 - GIFT_WRAP_OVERLAP_SECS);
    await fixture.client.close();
  });

  test("a backlog folds into the cursor at EOSE only after the ingest is durable", async () => {
    const script = freshScript();
    script.groups = [snapshot()];
    script.commitOn = ["ingest"];
    const key = groupCursorKey(ROUTE, RELAY);
    const fixture = await open(
      script,
      encodeClientState({ nip44Extended: true, cursors: { [key]: 500 }, inbox: [] }),
    );
    await settle(fixture.timer, 5000);
    const sub = mustSub(groupSubs(fixture.network));

    // Backlog events ingest but the durable ack is held: no cursor move yet.
    fixture.kv.hold();
    for (const at of [2000n, 1999n, 1998n]) {
      push(sub, groupEvent(at));
    }
    await settle(fixture.timer, 25);
    expect(ingested(script)).toHaveLength(3);
    pushEose(sub);
    await settle(fixture.timer, 0);
    expect(decodeClientState(fixture.cursors.bytes()).cursors[key]).toBe(500);

    // Once durable, the EOSE job folds the backlog max in.
    fixture.kv.release();
    await settle(fixture.timer, 0);
    expect(decodeClientState(fixture.cursors.bytes()).cursors[key]).toBe(2000);
    await fixture.client.close();
  });

  test("a mid-flight backlog folds only after the last pre-EOSE batch is durable", async () => {
    const script = freshScript();
    script.groups = [snapshot()];
    const key = groupCursorKey(ROUTE, RELAY);
    const fixture = await open(
      script,
      encodeClientState({ nip44Extended: true, cursors: { [key]: 500 }, inbox: [] }),
    );
    await settle(fixture.timer, 5000);
    const sub = mustSub(groupSubs(fixture.network));

    // Three full batches of newest-first backlog; every ingest parks on its own gate.
    const gates: IngestGate[] = [];
    script.ingestGate = async () => {
      await new Promise<void>((resolve, reject) => {
        gates.push({
          ok: resolve,
          fail: () => {
            reject(new Error("ingest down"));
          },
        });
      });
    };
    // Two full parked batches (a short tail would wait for the ingest delay): the fold waits
    // for the last one to go durable.
    for (let i = 0; i < 2 * INGEST_BATCH_MAX; i += 1) {
      push(sub, fill(i));
    }
    await settle(fixture.timer, 0);
    // EOSE lands while every pre-EOSE batch is still parked in the queue.
    pushEose(sub);
    await settle(fixture.timer, 0);

    const cursor = (): number | undefined =>
      decodeClientState(fixture.cursors.bytes()).cursors[key];
    // Release batches one at a time: nothing persists until the last one is durable.
    popGate(gates).ok();
    await settle(fixture.timer, 0);
    expect(cursor()).toBe(500);
    popGate(gates).ok();
    await settle(fixture.timer, 0);
    expect(cursor()).toBe(FILL_TOP);
    await fixture.client.close();
  });

  test("a failed pre-EOSE batch leaves the backlog unapplied at EOSE", async () => {
    const script = freshScript();
    script.groups = [snapshot()];
    const key = groupCursorKey(ROUTE, RELAY);
    const fixture = await open(
      script,
      encodeClientState({ nip44Extended: true, cursors: { [key]: 500 }, inbox: [] }),
    );
    await settle(fixture.timer, 5000);
    const sub = mustSub(groupSubs(fixture.network));

    const gates: IngestGate[] = [];
    script.ingestGate = async () => {
      await new Promise<void>((resolve, reject) => {
        gates.push({
          ok: resolve,
          fail: () => {
            reject(new Error("ingest down"));
          },
        });
      });
    };
    // Two full batches (a short tail would wait for the ingest delay instead of flushing),
    // from the shared pool — the events were signed and verified once already.
    for (let i = 0; i < 2 * INGEST_BATCH_MAX; i += 1) {
      push(sub, fill(i));
    }
    await settle(fixture.timer, 0);
    pushEose(sub);

    // Batch 1 lands, batch 2's ingest fails — the whole backlog is suspect: nothing applies.
    popGate(gates).ok();
    await settle(fixture.timer, 0);
    popGate(gates).fail();
    await settle(fixture.timer, 0);
    expect(decodeClientState(fixture.cursors.bytes()).cursors[key]).toBe(500);
    await fixture.client.close();
  });

  test("an EOSE with no events moves nothing and saves nothing", async () => {
    const script = freshScript();
    script.groups = [snapshot()];
    const key = groupCursorKey(ROUTE, RELAY);
    const seeded = encodeClientState({ nip44Extended: true, cursors: { [key]: 500 }, inbox: [] });
    const fixture = await open(script, seeded);
    await settle(fixture.timer, 5000);
    const sub = mustSub(groupSubs(fixture.network));

    pushEose(sub);
    await settle(fixture.timer, 25);
    expect(decodeClientState(fixture.cursors.bytes()).cursors[key]).toBe(500);
    // An empty backlog never schedules a save — the persisted blob is the seeded one.
    expect(fixture.cursors.bytes()).toStrictEqual(seeded);
    await fixture.client.close();
  });

  test("a duplicate EOSE is idempotent", async () => {
    const script = freshScript();
    script.groups = [snapshot()];
    const key = groupCursorKey(ROUTE, RELAY);
    const fixture = await open(
      script,
      encodeClientState({ nip44Extended: true, cursors: { [key]: 500 }, inbox: [] }),
    );
    await settle(fixture.timer, 5000);
    const sub = mustSub(groupSubs(fixture.network));

    push(sub, groupEvent(2000n));
    pushEose(sub);
    pushEose(sub);
    await settle(fixture.timer, 25);
    expect(decodeClientState(fixture.cursors.bytes()).cursors[key]).toBe(2000);
    expect(ingested(script)).toHaveLength(1);
    await fixture.client.close();
  });

  test("a group subscription lost before EOSE holds history_fetched until its reopen delivers", async () => {
    const script = freshScript();
    script.groups = [snapshot([RELAY, RELAY2])];
    const fixture = await open(
      script,
      encodeClientState({ nip44Extended: true, cursors: {}, inbox: [] }),
    );
    await settle(fixture.timer, 5000);
    const history = (): BridgeRequest[] =>
      script.calls.filter((call) => call.type === "history_fetched");
    const first = mustSubOn(groupSubs(fixture.network), RELAY);

    // One relay dies mid-replay; the other's EOSE alone does not cover the group.
    push(first, groupEvent(2000n));
    terminate(first);
    pushEose(mustSubOn(groupSubs(fixture.network), RELAY2));
    await settle(fixture.timer, 25);
    expect(history()).toHaveLength(0);

    await settle(fixture.timer, RESUBSCRIBE_DELAY_MS);
    const reopened = mustSubOn(
      fixture.network.openSubs().filter((sub) => sub !== first),
      RELAY,
    );
    pushEose(reopened);
    await settle(fixture.timer, 25);
    expect(history()).toStrictEqual([{ type: "history_fetched", group: GROUP }]);
    await fixture.client.close();
  });

  test("live events after an early EOSE advance the cursor normally", async () => {
    const script = freshScript();
    script.groups = [snapshot()];
    const key = groupCursorKey(ROUTE, RELAY);
    const fixture = await open(
      script,
      encodeClientState({ nip44Extended: true, cursors: { [key]: 500 }, inbox: [] }),
    );
    await settle(fixture.timer, 5000);
    const sub = mustSub(groupSubs(fixture.network));

    pushEose(sub);
    push(sub, groupEvent(3000n));
    await settle(fixture.timer, 25);
    expect(decodeClientState(fixture.cursors.bytes()).cursors[key]).toBe(3000);
    await fixture.client.close();
  });

  test("a subscription that never sends EOSE never moves the cursor and stays bounded", async () => {
    const script = freshScript();
    script.groups = [snapshot()];
    const key = groupCursorKey(ROUTE, RELAY);
    const fixture = await open(
      script,
      encodeClientState({ nip44Extended: true, cursors: { [key]: 500 }, inbox: [] }),
    );
    await settle(fixture.timer, 5000);
    const sub = mustSub(groupSubs(fixture.network));

    // One event past the pending cap while ingest is held — the cap binds the backlog.
    let releaseIngest: () => void = () => undefined;
    const hold = new Promise<void>((resolve) => {
      releaseIngest = resolve;
    });
    script.ingestGate = async () => hold;
    for (let i = 0; i < MAX_PENDING_INBOUND + 1; i += 1) {
      push(sub, fill(i));
    }
    await macrotask();
    expect(sub.open).toBe(false);
    releaseIngest();
    await settle(fixture.timer, RESUBSCRIBE_DELAY_MS);

    expect(ingested(script).length).toBeLessThanOrEqual(4 * INGEST_BATCH_MAX);
    // No EOSE ever arrived on either sub — nothing folded, the seed cursor stands.
    expect(decodeClientState(fixture.cursors.bytes()).cursors[key]).toBe(500);
    await fixture.client.close();
  }, 30_000);

  test("live events older than the cursor do not move it back", async () => {
    const script = freshScript();
    script.groups = [snapshot()];
    const key = groupCursorKey(ROUTE, RELAY);
    const fixture = await open(
      script,
      encodeClientState({ nip44Extended: true, cursors: { [key]: 5000 }, inbox: [] }),
    );
    await settle(fixture.timer, 5000);
    const sub = mustSub(groupSubs(fixture.network));

    pushEose(sub);
    push(sub, groupEvent(4000n));
    await settle(fixture.timer, 25);
    expect(decodeClientState(fixture.cursors.bytes()).cursors[key]).toBe(5000);
    await fixture.client.close();
  });

  test("a duplicate event id through two relays ingests once and credits both keys", async () => {
    const script = freshScript();
    script.groups = [snapshot([RELAY, RELAY2])];
    const key1 = groupCursorKey(ROUTE, RELAY);
    const key2 = groupCursorKey(ROUTE, RELAY2);
    const fixture = await open(
      script,
      encodeClientState({ nip44Extended: true, cursors: { [key1]: 500, [key2]: 500 }, inbox: [] }),
    );
    await settle(fixture.timer, 5000);
    const subs = groupSubs(fixture.network);
    expect(subs).toHaveLength(2);

    // The same id arrives pre-EOSE through both relays: one ingest, both keys credited.
    const shared = groupEvent(3000n);
    for (const sub of subs) {
      push(sub, shared);
    }
    await settle(fixture.timer, 25);
    expect(ingested(script).map((e) => e.id)).toStrictEqual([shared.id]);
    for (const sub of subs) {
      pushEose(sub);
    }
    await settle(fixture.timer, 25);
    const { cursors } = decodeClientState(fixture.cursors.bytes());
    expect(cursors[key1]).toBe(3000);
    expect(cursors[key2]).toBe(3000);
    await fixture.client.close();
  });

  test("events off the subscription's route or kind are ignored", async () => {
    const script = freshScript();
    script.groups = [snapshot()];
    const key = groupCursorKey(ROUTE, RELAY);
    const fixture = await open(
      script,
      encodeClientState({ nip44Extended: true, cursors: { [key]: 500 }, inbox: [] }),
    );
    await settle(fixture.timer, 5000);
    const sub = mustSub(groupSubs(fixture.network));

    push(sub, event({ kind: 445, createdAt: 3000n, tags: [["h", "z".repeat(64)]] }));
    push(sub, event({ kind: 1059, createdAt: 3000n, tags: [["h", ROUTE]] }));
    await settle(fixture.timer, 25);
    expect(ingested(script)).toHaveLength(0);
    expect(decodeClientState(fixture.cursors.bytes()).cursors[key]).toBe(500);
    await fixture.client.close();
  });

  test("a future createdAt clamps to the clock", async () => {
    const script = freshScript();
    script.groups = [snapshot()];
    const key = groupCursorKey(ROUTE, RELAY);
    const fixture = await open(
      script,
      encodeClientState({ nip44Extended: true, cursors: { [key]: 500 }, inbox: [] }),
    );
    await settle(fixture.timer, 5000);
    const sub = mustSub(groupSubs(fixture.network));

    pushEose(sub);
    push(sub, groupEvent(1_800_000_000n));
    await settle(fixture.timer, 25);
    expect(decodeClientState(fixture.cursors.bytes()).cursors[key]).toBe(1_700_000_000);
    await fixture.client.close();
  });

  test("an unavailable outcome in the backlog caps the key at earliest createdAt − 1", async () => {
    const script = freshScript();
    script.groups = [snapshot()];
    const accepted = groupEvent(6000n);
    const deferred = groupEvent(2000n);
    script.reports = [
      { event: accepted.id, outcome: { type: "accepted" } },
      {
        event: deferred.id,
        outcome: { type: "unavailable", category: "transport_deferred" },
      },
    ];
    const key = groupCursorKey(ROUTE, RELAY);
    const fixture = await open(
      script,
      encodeClientState({ nip44Extended: true, cursors: { [key]: 5000 }, inbox: [] }),
    );
    await settle(fixture.timer, 5000);
    const sub = mustSub(groupSubs(fixture.network));

    push(sub, accepted);
    push(sub, deferred);
    await settle(fixture.timer, 25);
    // The backlog folds only at EOSE.
    expect(decodeClientState(fixture.cursors.bytes()).cursors[key]).toBe(5000);
    pushEose(sub);
    await settle(fixture.timer, 25);
    // max(5000, 6000) capped at 2000 − 1.
    expect(decodeClientState(fixture.cursors.bytes()).cursors[key]).toBe(1999);
    await fixture.client.close();
  });

  test("a SignerInvalidPayloadError wrap is rejected permanently — invalid_payload reply, no pending invite, cursor folds past it", async () => {
    const script = freshScript();
    script.groups = [snapshot()];
    const wrap = wrapEvent(300_000n);
    // The backend suspends the wrap ingest on a nip44_decrypt…
    script.signerRequests = [
      { id: 0, op: { type: "nip44_decrypt", peer: ACCOUNT, ciphertext: "garbage" } },
    ];
    // …and reports it permanently rejected once the signer answer lands.
    script.reports = [
      { event: wrap.id, outcome: { type: "rejected", category: "invalid_encoding" } },
    ];
    const key = inboxCursorKey(RELAY);
    const fixture = await open(
      script,
      encodeClientState({ nip44Extended: true, cursors: { [key]: 200_000 }, inbox: [RELAY] }),
      { ...signer(), nip44Decrypt: async () => Promise.reject(new SignerInvalidPayloadError()) },
    );
    const invites: unknown[] = [];
    fixture.client.onInvite((invite) => {
      invites.push(invite);
    });
    await settle(fixture.timer, 5000);
    const sub = mustSub(inboxSubs(fixture.network));

    push(sub, wrap);
    pushEose(sub);
    await settle(fixture.timer, 25);

    // The engine answered the pending decrypt with `invalid_payload`.
    const reply = script.calls.find(
      (call): call is Extract<BridgeRequest, { type: "signer_reply" }> =>
        call.type === "signer_reply",
    );
    expect(reply?.replies).toStrictEqual([{ id: 0, result: { type: "invalid_payload" } }]);
    // The wrap's rejection is the backend's verdict, not an invite condition.
    expect(invites).toStrictEqual([]);
    // A permanent rejection folds into the cursor normally (an unavailable
    // outcome would cap it at createdAt − 1 instead).
    expect(decodeClientState(fixture.cursors.bytes()).cursors[key]).toBe(300_000);
    await fixture.client.close();
  });

  test("a forged same-id copy cannot shadow the genuine event or credit its relay", async () => {
    const script = freshScript();
    script.groups = [snapshot([RELAY, RELAY2])];
    const honestKey = groupCursorKey(ROUTE, RELAY);
    const hostileKey = groupCursorKey(ROUTE, RELAY2);
    const fixture = await open(
      script,
      encodeClientState({
        nip44Extended: true,
        cursors: { [honestKey]: 400, [hostileKey]: 400 },
        inbox: [],
      }),
    );
    await settle(fixture.timer, 5000);
    const subs = groupSubs(fixture.network);
    const subHonest = mustSubOn(subs, RELAY);
    const subHostile = mustSubOn(subs, RELAY2);

    const genuine = groupEvent(500n, "genuine");
    // A same-id copy: the committed fields carry the genuine id, but the tampered sig and
    // createdAt no longer verify.
    const forged: NostrEvent = {
      ...genuine,
      createdAt: 1_700_000_000n,
      sig: "ff".repeat(64),
    };

    // The hostile relay delivers the forgery first; the genuine copy follows inside the same
    // flush window — the order a first-object-wins dedupe would prefer for the attack.
    push(subHostile, forged);
    push(subHonest, genuine);
    for (const sub of subs) {
      pushEose(sub);
    }
    await settle(fixture.timer, 25);

    // The forgery never entered the buffer: the genuine event is what ingested.
    expect(ingested(script).map((e) => e.id)).toStrictEqual([genuine.id]);
    expect(ingested(script)[0]?.sig).toBe(genuine.sig);
    expect(ingested(script)[0]?.createdAt).toBe(500n);
    const { cursors } = decodeClientState(fixture.cursors.bytes());
    // The honest relay is credited the genuine createdAt; the hostile relay's forgery
    // credited nothing.
    expect(cursors[honestKey]).toBe(500);
    expect(cursors[hostileKey]).toBe(400);
    await fixture.client.close();
  });

  test("an event whose id or signature does not verify is dropped before dedupe", async () => {
    const script = freshScript();
    script.groups = [snapshot()];
    const key = groupCursorKey(ROUTE, RELAY);
    const fixture = await open(
      script,
      encodeClientState({ nip44Extended: true, cursors: { [key]: 500 }, inbox: [] }),
    );
    await settle(fixture.timer, 5000);
    const sub = mustSub(groupSubs(fixture.network));

    // A bad id commitment and a bad signature, pushed inside one flush window.
    const badId: NostrEvent = { ...groupEvent(3000n), id: "ab".repeat(32) };
    const badSig: NostrEvent = { ...groupEvent(4000n), sig: "00".repeat(64) };
    push(sub, badId);
    push(sub, badSig);
    pushEose(sub);
    await settle(fixture.timer, 25);

    // Neither entered the buffer: no ingest, no cursor credit, the sub stays open.
    expect(ingested(script)).toHaveLength(0);
    expect(decodeClientState(fixture.cursors.bytes()).cursors[key]).toBe(500);
    expect(sub.open).toBe(true);
    await fixture.client.close();
  });

  test("a gift-wrap flood budgets ingests to the window", async () => {
    const script = freshScript();
    const key = inboxCursorKey(RELAY);
    const fixture = await open(
      script,
      encodeClientState({ nip44Extended: true, cursors: { [key]: 100 }, inbox: [RELAY] }),
    );
    await settle(fixture.timer, 5000);
    const sub = mustSub(inboxSubs(fixture.network));

    for (let i = 0; i < GIFT_WRAP_BUDGET + 8; i += 1) {
      push(sub, wrapEvent(1_000_000n, `flood${i}`));
    }
    await settle(fixture.timer, 25);
    // Only the window's budget ingested; the eight-wrap tail stays buffered.
    expect(ingested(script)).toHaveLength(GIFT_WRAP_BUDGET);

    // The deferred tail lands only after the budget window resets — nothing is lost.
    fixture.clock.advance(GIFT_WRAP_WINDOW_MS);
    await settle(fixture.timer, GIFT_WRAP_WINDOW_MS);
    expect(ingested(script)).toHaveLength(GIFT_WRAP_BUDGET + 8);
    await fixture.client.close();
  });

  test("a budget-deferred inbox backlog caps the folded cursor below the deferred wraps", async () => {
    const script = freshScript();
    const key = inboxCursorKey(RELAY);
    const fixture = await open(
      script,
      encodeClientState({ nip44Extended: true, cursors: { [key]: 100 }, inbox: [RELAY] }),
    );
    await settle(fixture.timer, 5000);
    const sub = mustSub(inboxSubs(fixture.network));

    // 40 wraps newest-first; the eight oldest (createdAt 600–607) defer past the budget.
    for (let i = 0; i < 40; i += 1) {
      push(sub, wrapEvent(BigInt(639 - i), `defer${i}`));
    }
    pushEose(sub);
    await settle(fixture.timer, 0);
    // The fold ran after the 32-wrap batch went durable: the still-buffered tail caps the
    // cursor at min(deferred createdAt) − 1, not the backlog's 639.
    expect(decodeClientState(fixture.cursors.bytes()).cursors[key]).toBe(599);

    // Closing before the window resets is safe: the reopened `since` covers the deferred tail.
    const saved = fixture.cursors.bytes();
    await fixture.client.close();
    const reopened = await open(freshScript(), saved);
    await settle(reopened.timer, 5000);
    const sub2 = mustSub(inboxSubs(reopened.network));
    expect(sub2.filters[0]?.since).toBeLessThanOrEqual(599);
    await reopened.client.close();
  });

  test("a live wrap deferred beside an older one keeps the cursor capped until it is ingested", async () => {
    const script = freshScript();
    const key = inboxCursorKey(RELAY);
    const fixture = await open(
      script,
      encodeClientState({ nip44Extended: true, cursors: { [key]: 100 }, inbox: [RELAY] }),
    );
    await settle(fixture.timer, 5000);
    const sub = mustSub(inboxSubs(fixture.network));
    pushEose(sub);

    // 40 live wraps: 32 ingest now, the eight oldest (600–607) defer — the live advance caps
    // the cursor at 599 even though the ingested batch reached 639.
    for (let i = 0; i < 40; i += 1) {
      push(sub, wrapEvent(BigInt(639 - i), `live${i}`));
    }
    await settle(fixture.timer, 25);
    expect(decodeClientState(fixture.cursors.bytes()).cursors[key]).toBe(599);

    // A newer live wrap buffers under the same spent budget; the cursor stays capped.
    push(sub, wrapEvent(700n, "tail"));
    await settle(fixture.timer, 0);
    expect(decodeClientState(fixture.cursors.bytes()).cursors[key]).toBe(599);

    fixture.clock.advance(GIFT_WRAP_WINDOW_MS);
    await settle(fixture.timer, GIFT_WRAP_WINDOW_MS);
    expect(decodeClientState(fixture.cursors.bytes()).cursors[key]).toBe(700);
    await fixture.client.close();
  });

  test("group events in a wrap flood ingest without the wrap budget", async () => {
    const script = freshScript();
    script.groups = [snapshot()];
    const fixture = await open(
      script,
      encodeClientState({ nip44Extended: true, cursors: {}, inbox: [RELAY] }),
    );
    await settle(fixture.timer, 5000);
    const wrapSub = mustSub(inboxSubs(fixture.network));
    const groupSub = mustSub(groupSubs(fixture.network));

    for (let i = 0; i < 40; i += 1) {
      push(wrapSub, wrapEvent(1_000_000n + BigInt(i), `mix${i}`));
    }
    for (let i = 0; i < 10; i += 1) {
      push(groupSub, fill(i));
    }
    await settle(fixture.timer, 25);

    const batch = ingested(script);
    expect(batch.filter((event) => event.kind === 445)).toHaveLength(10);
    expect(batch.filter((event) => event.kind === 1059)).toHaveLength(GIFT_WRAP_BUDGET);
    await fixture.client.close();
  });

  test("a buffered older event stalls advancement but never moves a cursor back", async () => {
    const script = freshScript();
    script.groups = [snapshot()];
    const key = groupCursorKey(ROUTE, RELAY);
    const fixture = await open(
      script,
      encodeClientState({ nip44Extended: true, cursors: { [key]: 1000 }, inbox: [] }),
    );
    await settle(fixture.timer, 5000);
    const sub = mustSub(groupSubs(fixture.network));
    pushEose(sub);

    const gates: IngestGate[] = [];
    script.ingestGate = async () => {
      await new Promise<void>((resolve, reject) => {
        gates.push({
          ok: resolve,
          fail: () => {
            reject(new Error("ingest down"));
          },
        });
      });
    };
    const cursor = (): number | undefined =>
      decodeClientState(fixture.cursors.bytes()).cursors[key];

    // A newer live event ingests while an out-of-order older event is still buffered.
    push(sub, groupEvent(1500n));
    await settle(fixture.timer, 25);
    push(sub, groupEvent(500n));
    popGate(gates).ok();
    await settle(fixture.timer, 0);
    // The buffered createdAt-500 event caps the fold at 499 — but the cursor was 1000,
    // and the sub's `since` already covers the older event, so nothing moves back.
    expect(cursor()).toBe(1000);
    await settle(fixture.timer, 25);
    popGate(gates).ok();
    await settle(fixture.timer, 0);
    expect(cursor()).toBe(1000);
    await fixture.client.close();
  });

  test("a stall suspicion refetches the group once from scratch", async () => {
    const script = freshScript();
    script.groups = [snapshot([RELAY, RELAY2])];
    const groupKey1 = groupCursorKey(ROUTE, RELAY);
    const groupKey2 = groupCursorKey(ROUTE, RELAY2);
    const inboxKey = inboxCursorKey(RELAY);
    const fixture = await open(
      script,
      encodeClientState({
        nip44Extended: true,
        cursors: { [groupKey1]: 1000, [groupKey2]: 1000, [inboxKey]: 500 },
        inbox: [RELAY],
      }),
    );
    await settle(fixture.timer, 5000);
    const history = (): BridgeRequest[] =>
      script.calls.filter((call) => call.type === "history_fetched");
    for (const sub of groupSubs(fixture.network)) {
      pushEose(sub);
    }
    await settle(fixture.timer, 25);
    expect(history()).toHaveLength(1);
    const changes: string[] = [];
    fixture.client.onGroupChange((event) => changes.push(event.type));

    // The session suspects the group: its route cursors reset, the inbox
    // cursor stays, the live subs close, and the event never surfaces.
    script.drains.push({
      type: "events",
      events: [{ type: "group_stall_suspected", group: GROUP }],
    });
    await fixture.client.groups();
    await settle(fixture.timer, 25);
    const saved = decodeClientState(fixture.cursors.bytes()).cursors;
    expect(saved[groupKey1]).toBeUndefined();
    expect(saved[groupKey2]).toBeUndefined();
    expect(saved[inboxKey]).toBe(500);
    expect(changes).toStrictEqual([]);
    await settle(fixture.timer, RESUBSCRIBE_DELAY_MS);

    // The reopened subs carry no `since` — an unfloored replay — and the
    // refetch reports through `history_fetched` only once every relay's
    // EOSE has landed.
    const reopened = groupSubs(fixture.network).filter((sub) => sub.open);
    expect(reopened.length).toBeGreaterThan(0);
    for (const sub of reopened) {
      expect(sub.filters[0]?.since).toBeUndefined();
    }
    expect(history()).toHaveLength(1);
    pushEose(mustSubOn(reopened, RELAY));
    await settle(fixture.timer, 25);
    expect(history()).toHaveLength(1);
    pushEose(mustSubOn(reopened, RELAY2));
    await settle(fixture.timer, 25);
    expect(history()).toStrictEqual([
      { type: "history_fetched", group: GROUP },
      { type: "history_fetched", group: GROUP },
    ]);
    await fixture.client.close();
  });

  test("a historyFetched queued before the suspicion is dropped", async () => {
    const script = freshScript();
    script.groups = [snapshot([RELAY])];
    const fixture = await open(
      script,
      encodeClientState({
        nip44Extended: true,
        cursors: { [groupCursorKey(ROUTE, RELAY)]: 1000 },
        inbox: [],
      }),
    );
    await settle(fixture.timer, 5000);
    const sub = mustSub(groupSubs(fixture.network));
    const history = (): BridgeRequest[] =>
      script.calls.filter((call) => call.type === "history_fetched");

    // Park one ingest on the op queue so the EOSE's historyFetched job
    // waits behind it — the position a suspicion lands in mid-flight.
    const gates: IngestGate[] = [];
    script.ingestGate = async () => {
      await new Promise<void>((resolve, reject) => {
        gates.push({
          ok: resolve,
          fail: () => {
            reject(new Error("ingest down"));
          },
        });
      });
    };
    push(sub, groupEvent(1500n));
    await settle(fixture.timer, 25);
    pushEose(sub);
    await settle(fixture.timer, 0);

    // The parked ingest's own drain delivers the suspicion: the queued
    // report is stale by the time it runs and must be skipped.
    script.drains.push({
      type: "events",
      events: [{ type: "group_stall_suspected", group: GROUP }],
    });
    popGate(gates).ok();
    await settle(fixture.timer, 25);
    expect(history()).toHaveLength(0);

    // The reopened subs' EOSE is the only report that lands.
    const reopened = groupSubs(fixture.network).filter((entry) => entry.open);
    expect(reopened.length).toBeGreaterThan(0);
    for (const entry of reopened) {
      expect(entry.filters[0]?.since).toBeUndefined();
    }
    pushEose(mustSub(reopened));
    await settle(fixture.timer, 25);
    expect(history()).toStrictEqual([{ type: "history_fetched", group: GROUP }]);
    await fixture.client.close();
  });

  test("group_stalled reaches onGroupChange", async () => {
    const script = freshScript();
    script.groups = [snapshot()];
    const fixture = await open(
      script,
      encodeClientState({ nip44Extended: true, cursors: {}, inbox: [] }),
    );
    await settle(fixture.timer, 5000);
    const changes: string[] = [];
    fixture.client.onGroupChange((event) => changes.push(event.type));
    script.drains.push({
      type: "events",
      events: [{ type: "group_stalled", group: GROUP }],
    });
    await fixture.client.groups();
    expect(changes).toStrictEqual(["group_stalled"]);
    await fixture.client.close();
  });

  test("rejoin_offered reaches onGroupChange", async () => {
    const script = freshScript();
    script.groups = [snapshot()];
    const fixture = await open(
      script,
      encodeClientState({ nip44Extended: true, cursors: {}, inbox: [] }),
    );
    await settle(fixture.timer, 5000);
    const changes: string[] = [];
    fixture.client.onGroupChange((event) => changes.push(event.type));
    script.drains.push({
      type: "events",
      events: [{ type: "rejoin_offered", group: GROUP }],
    });
    await fixture.client.groups();
    expect(changes).toStrictEqual(["rejoin_offered"]);
    await fixture.client.close();
  });
});
