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
} from "../src/backend/protocol.ts";
import { utf8Decode, utf8Encode } from "../src/backend/utf8.ts";
import { MarmotClient } from "../src/client/client.ts";
import { INGEST_BATCH_MAX, RESUBSCRIBE_DELAY_MS } from "../src/client/pump.ts";
import { encodeClientState } from "../src/client/state.ts";
import { manualClock } from "../src/testing/clock.ts";
import { memoryCursorStore } from "../src/testing/cursors.ts";
import { memoryKv } from "../src/testing/memory.ts";
import { manualTimer } from "../src/testing/timer.ts";
import type { ManualTimer } from "../src/testing/timer.ts";
import { GROUP_OVERLAP_SECS, groupCursorKey } from "../src/transport/plan.ts";
import type { RelayFilter, RelayPort, SubscriptionHandlers } from "../src/transport/port.ts";

const ACCOUNT = "a".repeat(64);
const RELAY = "wss://r1.example";
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

type Script = {
  /** FIFO drain responses; the queue falls back to empty events. */
  drains: DrainResponse[];
  /** The ingest reports for the next ingest; falls back to one `accepted` per event. */
  reports: InputReport[] | "auto";
  /** When set, every `ingest` call parks on this gate before answering. */
  ingestGate: (() => Promise<void>) | undefined;
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
                return { type: "ok", result: { type: "ingested", reports } };
              }
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

async function open(script: Script, blob?: Uint8Array): Promise<Fixture> {
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
    signer: signer(),
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
});
