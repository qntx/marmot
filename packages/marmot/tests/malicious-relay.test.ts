/**
 * Malicious-relay tests: a scripted `RelayPort` (events, EOSE and closes pushed at will; every
 * subscribe and close recorded) drives `MarmotClient` against a fake backend whose ingest can be
 * held. The cases pin the pump's defenses: bounded inbound buffering, oversized-event drops and
 * cursor rules.
 */

import { describe, expect, test } from "vite-plus/test";

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
import { INGEST_BATCH_MAX, RESUBSCRIBE_DELAY_MS } from "../src/client/pump.ts";
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

const ACCOUNT = "a".repeat(64);
const RELAY = "wss://r1.example";
const RELAY2 = "wss://r2.example";
const GROUP = "cc".repeat(32);
const ROUTE = "dd".repeat(32);
const DONE: BridgeResponse = { type: "ok", result: { type: "done" } };

function event(partial: Partial<NostrEvent> & { id: string }): NostrEvent {
  return {
    pubkey: "bb".repeat(32),
    createdAt: 1000n,
    kind: 445,
    tags: [],
    content: "",
    sig: "00".repeat(64),
    ...partial,
  };
}

const groupEvent = (id: string, createdAt = 1000n): NostrEvent =>
  event({ id, kind: 445, createdAt, tags: [["h", ROUTE]] });

const wrapEvent = (id: string, createdAt = 1000n): NostrEvent =>
  event({ id, kind: 1059, createdAt, tags: [["p", ACCOUNT]] });

function snapshot(relays: string[] = [RELAY]): GroupSnapshot {
  return {
    id: GROUP,
    name: "g",
    description: "",
    epoch: 1n,
    state: "stable",
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
    nip44Encrypt: async () => Promise.resolve("cipher"),
    nip44Decrypt: async () => Promise.resolve("plain"),
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
      encodeClientState({ cursors: { [key]: 10_000 }, inbox: [RELAY] }),
    );
    await settle(fixture.timer, 5000);
    const sub = mustSub(groupSubs(fixture.network));

    // 100 000 distinct valid-looking kind-445 events while ingest never answers.
    // Their createdAts stay below the seeded cursor so the durable cursor never moves.
    for (let i = 0; i < 100_000; i += 1) {
      push(sub, groupEvent(i.toString(16).padStart(64, "0"), BigInt(i % 8000)));
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
    const fixture = await open(script, encodeClientState({ cursors: {}, inbox: [RELAY] }));
    await settle(fixture.timer, 5000);
    const sub = mustSub(groupSubs(fixture.network));
    push(sub, groupEvent("77".repeat(32), 2000n));
    push(
      sub,
      event({
        id: "78".repeat(32),
        kind: 445,
        createdAt: 3000n,
        tags: [["h", ROUTE]],
        content: "x".repeat(2 * 1024 * 1024),
      }),
    );
    await settle(fixture.timer, 25);
    // Only the small event ingested; the oversized one was dropped before buffering.
    expect(ingested(script).map((e) => e.id)).toStrictEqual(["77".repeat(32)]);
    expect(sub.open).toBe(true);
    await fixture.client.close();
  });

  test("a newest-first backlog closed before EOSE never moves the cursor", async () => {
    const script = freshScript();
    script.groups = [snapshot()];
    const key = groupCursorKey(ROUTE, RELAY);
    const fixture = await open(script, encodeClientState({ cursors: { [key]: 500 }, inbox: [] }));
    await settle(fixture.timer, 5000);
    const sub = mustSub(groupSubs(fixture.network));
    expect(sub.filters[0]?.since).toBe(Math.max(0, 500 - GROUP_OVERLAP_SECS));

    // Stored events replay newest-first; the relay dies before EOSE.
    for (const [i, at] of [2000n, 1999n, 1998n, 1997n].entries()) {
      push(sub, groupEvent((i + 1).toString(16).padStart(64, "0"), at));
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
      encodeClientState({ cursors: { [key]: 200_000 }, inbox: [RELAY] }),
    );
    await settle(fixture.timer, 5000);
    const sub = mustSub(inboxSubs(fixture.network));
    expect(sub.filters[0]?.since).toBe(200_000 - GIFT_WRAP_OVERLAP_SECS);

    for (const [i, at] of [300_000n, 290_000n, 280_000n].entries()) {
      push(sub, wrapEvent((i + 1).toString(16).padStart(64, "0"), at));
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
    const fixture = await open(script, encodeClientState({ cursors: { [key]: 500 }, inbox: [] }));
    await settle(fixture.timer, 5000);
    const sub = mustSub(groupSubs(fixture.network));

    // Backlog events ingest but the durable ack is held: no cursor move yet.
    fixture.kv.hold();
    for (const [i, at] of [2000n, 1999n, 1998n].entries()) {
      push(sub, groupEvent((i + 1).toString(16).padStart(64, "0"), at));
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
    const fixture = await open(script, encodeClientState({ cursors: { [key]: 500 }, inbox: [] }));
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
    const total = 3 * INGEST_BATCH_MAX;
    for (let i = 0; i < total; i += 1) {
      push(sub, groupEvent(i.toString(16).padStart(64, "0"), BigInt(total - i)));
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
    expect(cursor()).toBe(500);
    popGate(gates).ok();
    await settle(fixture.timer, 0);
    expect(cursor()).toBe(total);
    await fixture.client.close();
  });

  test("a failed pre-EOSE batch leaves the backlog unapplied at EOSE", async () => {
    const script = freshScript();
    script.groups = [snapshot()];
    const key = groupCursorKey(ROUTE, RELAY);
    const fixture = await open(script, encodeClientState({ cursors: { [key]: 500 }, inbox: [] }));
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
    for (let i = 0; i < 2 * INGEST_BATCH_MAX; i += 1) {
      push(sub, groupEvent(i.toString(16).padStart(64, "0"), BigInt(1000 + i)));
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
    const seeded = encodeClientState({ cursors: { [key]: 500 }, inbox: [] });
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
    const fixture = await open(script, encodeClientState({ cursors: { [key]: 500 }, inbox: [] }));
    await settle(fixture.timer, 5000);
    const sub = mustSub(groupSubs(fixture.network));

    push(sub, groupEvent("01".repeat(32), 2000n));
    pushEose(sub);
    pushEose(sub);
    await settle(fixture.timer, 25);
    expect(decodeClientState(fixture.cursors.bytes()).cursors[key]).toBe(2000);
    expect(ingested(script)).toHaveLength(1);
    await fixture.client.close();
  });

  test("live events after an early EOSE advance the cursor normally", async () => {
    const script = freshScript();
    script.groups = [snapshot()];
    const key = groupCursorKey(ROUTE, RELAY);
    const fixture = await open(script, encodeClientState({ cursors: { [key]: 500 }, inbox: [] }));
    await settle(fixture.timer, 5000);
    const sub = mustSub(groupSubs(fixture.network));

    pushEose(sub);
    push(sub, groupEvent("01".repeat(32), 3000n));
    await settle(fixture.timer, 25);
    expect(decodeClientState(fixture.cursors.bytes()).cursors[key]).toBe(3000);
    await fixture.client.close();
  });

  test("a subscription that never sends EOSE never moves the cursor and stays bounded", async () => {
    const script = freshScript();
    script.groups = [snapshot()];
    const key = groupCursorKey(ROUTE, RELAY);
    const fixture = await open(script, encodeClientState({ cursors: { [key]: 500 }, inbox: [] }));
    await settle(fixture.timer, 5000);
    const sub = mustSub(groupSubs(fixture.network));

    // Ten batches' worth of backlog while ingest is held — the pending cap binds.
    let releaseIngest: () => void = () => undefined;
    const hold = new Promise<void>((resolve) => {
      releaseIngest = resolve;
    });
    script.ingestGate = async () => hold;
    for (let i = 0; i < 10 * INGEST_BATCH_MAX; i += 1) {
      push(sub, groupEvent(i.toString(16).padStart(64, "0"), BigInt(i % 4000)));
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
    const fixture = await open(script, encodeClientState({ cursors: { [key]: 5000 }, inbox: [] }));
    await settle(fixture.timer, 5000);
    const sub = mustSub(groupSubs(fixture.network));

    pushEose(sub);
    push(sub, groupEvent("01".repeat(32), 4000n));
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
      encodeClientState({ cursors: { [key1]: 500, [key2]: 500 }, inbox: [] }),
    );
    await settle(fixture.timer, 5000);
    const subs = groupSubs(fixture.network);
    expect(subs).toHaveLength(2);

    // The same id arrives pre-EOSE through both relays: one ingest, both keys credited.
    const shared = groupEvent("02".repeat(32), 3000n);
    for (const sub of subs) {
      push(sub, shared);
    }
    await settle(fixture.timer, 25);
    expect(ingested(script).map((e) => e.id)).toStrictEqual(["02".repeat(32)]);
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
    const fixture = await open(script, encodeClientState({ cursors: { [key]: 500 }, inbox: [] }));
    await settle(fixture.timer, 5000);
    const sub = mustSub(groupSubs(fixture.network));

    push(
      sub,
      event({ id: "03".repeat(32), kind: 445, createdAt: 3000n, tags: [["h", "z".repeat(64)]] }),
    );
    push(sub, event({ id: "04".repeat(32), kind: 1059, createdAt: 3000n, tags: [["h", ROUTE]] }));
    await settle(fixture.timer, 25);
    expect(ingested(script)).toHaveLength(0);
    expect(decodeClientState(fixture.cursors.bytes()).cursors[key]).toBe(500);
    await fixture.client.close();
  });

  test("a future createdAt clamps to the clock", async () => {
    const script = freshScript();
    script.groups = [snapshot()];
    const key = groupCursorKey(ROUTE, RELAY);
    const fixture = await open(script, encodeClientState({ cursors: { [key]: 500 }, inbox: [] }));
    await settle(fixture.timer, 5000);
    const sub = mustSub(groupSubs(fixture.network));

    pushEose(sub);
    push(sub, groupEvent("05".repeat(32), 1_800_000_000n));
    await settle(fixture.timer, 25);
    expect(decodeClientState(fixture.cursors.bytes()).cursors[key]).toBe(1_700_000_000);
    await fixture.client.close();
  });

  test("an unavailable outcome in the backlog caps the key at earliest createdAt − 1", async () => {
    const script = freshScript();
    script.groups = [snapshot()];
    script.reports = [
      { event: "06".repeat(32), outcome: { type: "accepted" } },
      {
        event: "07".repeat(32),
        outcome: { type: "unavailable", category: "transport_deferred" },
      },
    ];
    const key = groupCursorKey(ROUTE, RELAY);
    const fixture = await open(script, encodeClientState({ cursors: { [key]: 5000 }, inbox: [] }));
    await settle(fixture.timer, 5000);
    const sub = mustSub(groupSubs(fixture.network));

    push(sub, groupEvent("06".repeat(32), 6000n));
    push(sub, groupEvent("07".repeat(32), 2000n));
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
    const wrapId = "08".repeat(32);
    // The backend suspends the wrap ingest on a nip44_decrypt…
    script.signerRequests = [
      { id: 0, op: { type: "nip44_decrypt", peer: ACCOUNT, ciphertext: "garbage" } },
    ];
    // …and reports it permanently rejected once the signer answer lands.
    script.reports = [
      { event: wrapId, outcome: { type: "rejected", category: "invalid_encoding" } },
    ];
    const key = inboxCursorKey(RELAY);
    const fixture = await open(
      script,
      encodeClientState({ cursors: { [key]: 200_000 }, inbox: [RELAY] }),
      { ...signer(), nip44Decrypt: async () => Promise.reject(new SignerInvalidPayloadError()) },
    );
    const invites: unknown[] = [];
    fixture.client.onInvite((invite) => {
      invites.push(invite);
    });
    await settle(fixture.timer, 5000);
    const sub = mustSub(inboxSubs(fixture.network));

    push(sub, wrapEvent(wrapId, 300_000n));
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
});
