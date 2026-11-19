import { describe, expect, test } from "vite-plus/test";

import {
  decodeCallValue,
  encodeDrainResponse,
  encodeOpenResponse,
  encodeResponse,
} from "../src/backend/codec.ts";
import type { Json } from "../src/backend/codec.ts";
import type { Signer } from "../src/backend/engine.ts";
import { SignerRefusedError } from "../src/backend/engine.ts";
import { encodeKvBatch } from "../src/backend/kv.ts";
import type { Backend, BridgeHandle, BridgeHost } from "../src/backend/port.ts";
import type {
  AttachmentInput,
  BridgeRequest,
  BridgeResponse,
  DrainResponse,
  GroupSnapshot,
  InputReport,
  NostrEvent,
  Outbound,
  PublishResolution,
  SignerOp,
} from "../src/backend/protocol.ts";
import { utf8Decode, utf8Encode } from "../src/backend/utf8.ts";
import { MarmotClient } from "../src/client/client.ts";
import type { MarmotError } from "../src/client/errors.ts";
import { decodeClientState, encodeClientState } from "../src/client/state.ts";
import { manualClock } from "../src/testing/clock.ts";
import { memoryCursorStore } from "../src/testing/cursors.ts";
import { memoryKv } from "../src/testing/memory.ts";
import { fakeRelay } from "../src/testing/relay.ts";
import { manualTimer } from "../src/testing/timer.ts";
import type { ManualTimer } from "../src/testing/timer.ts";
import { groupCursorKey, inboxCursorKey } from "../src/transport/plan.ts";
import type { RelayFilter, RelayPort } from "../src/transport/port.ts";

const ACCOUNT = "a".repeat(64);
const RELAY = "wss://r1.example";
const RELAY2 = "wss://r2.example";
const LOOKUP = "wss://lookup.example";
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

const wrap = (id: string): NostrEvent => event({ id, kind: 1059, tags: [["p", ACCOUNT]] });

const msg = (id: string) => ({
  id,
  transportId: id,
  sender: "bb".repeat(32),
  content: "hi",
  kind: 9n,
  tags: [] as string[][],
  epoch: 1n,
  createdAt: 1000n,
  streamFinal: undefined,
  attachments: [],
});

const groupEvent = (id: string, createdAt = 1000n): NostrEvent =>
  event({ id, kind: 445, createdAt, tags: [["h", ROUTE]] });

function snapshot(): GroupSnapshot {
  return {
    id: GROUP,
    name: "g",
    description: "",
    epoch: 1n,
    state: "stable",
    members: [],
    relays: [RELAY],
    nostrGroupId: ROUTE,
    ownAccountAdmin: true,
    encryptedMedia: undefined,
    agentTextStream: undefined,
  };
}

type Script = {
  /** FIFO drain responses; the queue falls back to empty events. */
  drains: DrainResponse[];
  /** The ingest reports for the next ingest; falls back to one `accepted` per event. */
  reports: InputReport[] | "auto";
  /** Items `take_outbound` returns on the next call, then empty. */
  outbound: Outbound[];
  /** FIFO `report_publish` resolutions; falls back to `complete`. */
  resolutions: PublishResolution[];
  /** `groups` answer. */
  groups: GroupSnapshot[];
  /** `tick` answer (ms until wake). */
  nextWake: bigint | undefined;
  /** `send_outcome` answer for send-type ops. */
  sent: BridgeResponse;
  /** Emit `host.commit` (seq ascending) when these request types run. */
  commitOn: Array<BridgeRequest["type"]>;
  /** Suspend the next `ingest` on this signer op; the reply returns the reports. */
  suspendOnIngest: SignerOp | undefined;
  calls: BridgeRequest[];
};

function scriptedBackend(script: Script): { backend: Backend; closed: () => boolean } {
  let closed = false;
  let seq = 0n;
  let stashed: BridgeResponse | undefined;
  const inflight = new Map<string, Outbound>();
  const done = (): BridgeResponse => DONE;
  const backend: Backend = {
    name: "scripted",
    create: (host: BridgeHost) => {
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
          if (request.type === "signer_reply" && stashed !== undefined) {
            const response = stashed;
            stashed = undefined;
            return utf8Encode(JSON.stringify(encodeResponse(response)));
          }
          const response = ((): BridgeResponse => {
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
                const suspend = script.suspendOnIngest;
                if (suspend !== undefined) {
                  script.suspendOnIngest = undefined;
                  stashed = ingested;
                  return {
                    type: "signer_pending",
                    requests: [{ id: 1, op: suspend }],
                  };
                }
                return ingested;
              }
              case "take_outbound": {
                const items = script.outbound;
                script.outbound = [];
                for (const item of items) {
                  inflight.set(item.id, item);
                }
                return { type: "ok", result: { type: "outbound", items } };
              }
              case "report_publish": {
                const resolution = script.resolutions.shift() ?? "complete";
                // A pending item stays in the outbox: the next take re-serves it.
                if (resolution === "pending") {
                  const item = inflight.get(request.id);
                  if (item !== undefined) {
                    inflight.delete(item.id);
                    script.outbound.push(item);
                  }
                } else {
                  inflight.delete(request.id);
                }
                return {
                  type: "ok",
                  result: { type: "publish_resolution", resolution },
                };
              }
              case "groups":
                return { type: "ok", result: { type: "groups", groups: script.groups } };
              case "group":
                return {
                  type: "ok",
                  result: { type: "group", group: script.groups[0] },
                };
              case "publish_key_package":
                return {
                  type: "ok",
                  result: {
                    type: "key_package",
                    keyPackage: Uint8Array.of(1),
                    keyPackageRef: "00".repeat(32),
                    eventId: "00".repeat(32),
                    account: ACCOUNT,
                  },
                };
              case "publish_inbox_relays":
                return {
                  type: "ok",
                  result: { type: "inbox_relays", outboundId: "00".repeat(32) },
                };
              case "seal_attachment":
                return {
                  type: "ok",
                  result: {
                    type: "sealed_attachment",
                    ciphertext: Uint8Array.of(9),
                    ciphertextSha256: "ab".repeat(32),
                    handle: script.calls.filter((c) => c.type === "seal_attachment").length,
                  },
                };
              case "send_media":
                return script.sent;
              case "send_message":
              case "create_group":
                return request.type === "create_group"
                  ? { type: "ok", result: { type: "group_created", group: snapshot() } }
                  : script.sent;
              default:
                return done();
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
          closed = true;
          await Promise.resolve();
        },
      };
      return handle;
    },
  };
  return { backend, closed: () => closed };
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

function freshScript(): Script {
  return {
    drains: [],
    reports: "auto",
    outbound: [],
    resolutions: [],
    groups: [],
    nextWake: undefined,
    sent: { type: "ok", result: { type: "send_outcome", outcome: { type: "queued" } } },
    commitOn: [],
    suspendOnIngest: undefined,
    calls: [],
  };
}

type Fixture = {
  client: MarmotClient;
  script: Script;
  timer: ManualTimer;
  network: ReturnType<typeof fakeRelay>;
  cursors: ReturnType<typeof memoryCursorStore>;
  kv: ReturnType<typeof memoryKv>;
  clock: ReturnType<typeof manualClock>;
  /** Every subscribe call the client made, in order. */
  subs: Array<{ relay: string; filters: RelayFilter[] }>;
};

async function open(script: Script, blob?: Uint8Array, sign?: Signer): Promise<Fixture> {
  const timer = manualTimer();
  const network = fakeRelay();
  const cursors = memoryCursorStore();
  const kv = memoryKv();
  const clock = manualClock();
  const subs: Array<{ relay: string; filters: RelayFilter[] }> = [];
  if (blob !== undefined) {
    await cursors.save(blob);
  }
  const inner = network.port("c");
  const port: RelayPort = {
    publish: async (relay, event) => inner.publish(relay, event),
    subscribe: (relay, filters, handlers) => {
      subs.push({ relay, filters });
      return inner.subscribe(relay, filters, handlers);
    },
  };
  const client = await MarmotClient.open({
    backend: scriptedBackend(script).backend,
    kv,
    cursors,
    signer: sign ?? signer(),
    clock,
    timer,
    relays: port,
    allowLoopback: true,
  });
  return { client, script, timer, network, cursors, kv, clock, subs };
}

const macrotask = async (): Promise<void> =>
  new Promise((resolve) => {
    setTimeout(resolve);
  });

/**
 * Let queued work (relay delivery, serialized pump ops, publish reports) settle, then fire due
 * timers. A real macrotask yield flushes the entire microtask queue — a fixed `await
 * Promise.resolve()` count cannot bound op-queue tails (drain → dispatch → delivery → ack).
 */
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

const callsOf = (script: Script, type: BridgeRequest["type"]): BridgeRequest[] =>
  script.calls.filter((call) => call.type === type);

/** The event lists of every `ingest` call, in order. */
const ingests = (script: Script): NostrEvent[][] =>
  script.calls.flatMap((call) => (call.type === "ingest" ? [call.events] : []));

describe("MarmotClient", () => {
  test("open subscribes for gift wraps on the persisted inbox relays", async () => {
    const fixture = await open(freshScript(), encodeClientState({ cursors: {}, inbox: [RELAY] }));
    await settle(fixture.timer, 5000);
    expect(fixture.network.openSubscriptions("c")).toBe(1);
    await fixture.client.close();
  });

  test("publishInboxRelays persists the list and subscribes", async () => {
    const fixture = await open(freshScript());
    await fixture.client.publishInboxRelays([RELAY]);
    await settle(fixture.timer, 5000);
    expect(fixture.network.openSubscriptions("c")).toBe(1);
    const state = fixture.cursors.bytes();
    expect(state).toBeDefined();
    await fixture.client.close();
  });

  test("an inbound wrap ingests and a joined invite fires + a 445 subscription opens", async () => {
    const script = freshScript();
    script.drains.push(
      { type: "events", events: [] }, // publishInboxRelays' post-op drain
      { type: "events", events: [] }, // the serialized takeOutbound job's drain
      { type: "events", events: [{ type: "group_joined", group: GROUP }] },
    );
    script.groups = [snapshot()];
    const fixture = await open(script, encodeClientState({ cursors: {}, inbox: [RELAY] }));
    await settle(fixture.timer, 5000);

    const invites: unknown[] = [];
    const groupChanges: string[] = [];
    fixture.client.onInvite((e) => {
      invites.push(e);
    });
    fixture.client.onGroupChange((e) => {
      groupChanges.push(e.type);
    });
    // A wrap addressed to the account arrives over the inbox subscription.
    await fixture.network.port("peer").publish(RELAY, wrap("aa".repeat(32)));
    await settle(fixture.timer, 5000);

    expect(callsOf(script, "ingest").length).toBeGreaterThan(0);
    expect(invites).toStrictEqual([{ type: "joined", group: GROUP }]);
    expect(groupChanges).toContain("group_joined");
    // Reconcile opened a kind-445 subscription for the group's route.
    expect(fixture.network.openSubscriptions("c")).toBe(2);
    await fixture.client.close();
  });

  test("a group event delivers a message and the listener's ack reaches the engine", async () => {
    const script = freshScript();
    const msg = {
      id: "e".repeat(64),
      transportId: `f1${"0".repeat(62)}`,
      sender: "bb".repeat(32),
      content: "hi",
      kind: 9n,
      tags: [] as string[][],
      epoch: 1n,
      createdAt: 1000n,
      streamFinal: undefined,
      attachments: [],
    };
    script.drains.push(
      { type: "events", events: [] }, // open's first drain
      { type: "events", events: [] }, // the serialized takeOutbound job's drain
      { type: "events", events: [{ type: "message_received", group: GROUP, message: msg }] },
    );
    script.groups = [snapshot()];
    const fixture = await open(script, encodeClientState({ cursors: {}, inbox: [RELAY] }));
    await settle(fixture.timer, 5000);

    const errors: string[] = [];
    fixture.client.onError((e) => errors.push(`${e.kind}:${e.message}`));
    const batches: string[][] = [];
    fixture.client.onMessages(async (batch) => {
      batches.push(batch.map((m) => m.message.content));
      return Promise.resolve();
    });
    await fixture.network.port("peer").publish(RELAY, groupEvent("11".repeat(32)));
    await settle(fixture.timer, 5000);

    expect(batches).toStrictEqual([["hi"]]);
    const acks = callsOf(script, "acknowledge_messages");
    expect(acks).toHaveLength(1);
    await fixture.client.close();
  });

  test("a rejecting messages listener skips the ack and emits handler_failed", async () => {
    const script = freshScript();
    const msg = {
      id: "e".repeat(64),
      transportId: `f2${"0".repeat(62)}`,
      sender: "bb".repeat(32),
      content: "hi",
      kind: 9n,
      tags: [] as string[][],
      epoch: 1n,
      createdAt: 1000n,
      streamFinal: undefined,
      attachments: [],
    };
    script.drains.push(
      { type: "events", events: [] },
      { type: "events", events: [] }, // the serialized takeOutbound job's drain
      { type: "events", events: [{ type: "message_received", group: GROUP, message: msg }] },
    );
    script.groups = [snapshot()];
    const fixture = await open(script, encodeClientState({ cursors: {}, inbox: [RELAY] }));
    await settle(fixture.timer, 5000);

    const errors: MarmotError[] = [];
    fixture.client.onError((error) => errors.push(error));
    fixture.client.onMessages(async () => Promise.reject(new Error("handler boom")));
    await fixture.network.port("peer").publish(RELAY, groupEvent("22".repeat(32)));
    await settle(fixture.timer, 5000);

    expect(errors.map((e) => e.kind)).toContain("handler_failed");
    expect(callsOf(script, "acknowledge_messages")).toHaveLength(0);
    await fixture.client.close();
  });

  test("outbound items publish through the port and report verdicts", async () => {
    const script = freshScript();
    const outbound: Outbound = {
      id: `0b${"0".repeat(62)}`,
      event: event({ id: "33".repeat(32), kind: 445, tags: [["h", ROUTE]] }),
      relays: [RELAY],
      purpose: "application_message",
    };
    script.outbound = [outbound];
    const fixture = await open(script, encodeClientState({ cursors: {}, inbox: [RELAY] }));
    await settle(fixture.timer, 5000);
    expect(fixture.network.published()).toStrictEqual([
      { client: "c", relay: RELAY, event: outbound.event },
    ]);
    const reports = callsOf(script, "report_publish");
    expect(reports).toHaveLength(1);
    expect(reports[0]).toMatchObject({ id: `0b${"0".repeat(62)}` });
    await fixture.client.close();
  });

  test("an unavailable verdict keeps the item pending and a later tick retries it", async () => {
    const script = freshScript();
    script.resolutions.push("pending", "complete");
    const outbound: Outbound = {
      id: `0b${"1".repeat(62)}`,
      event: event({ id: "44".repeat(32), kind: 445, tags: [["h", ROUTE]] }),
      relays: [RELAY],
      purpose: "application_message",
    };
    script.outbound = [outbound];
    const fixture = await open(script, encodeClientState({ cursors: {}, inbox: [RELAY] }));
    fixture.network.mode(RELAY, "down");
    await settle(fixture.timer, 0);
    // Down: the publish still ran (the port answers unavailable), reported pending.
    expect(callsOf(script, "report_publish")).toHaveLength(1);
    expect(fixture.network.published()).toHaveLength(1);
    // The dedicated retry timer is 5 s out — nothing before it.
    await settle(fixture.timer, 4999);
    expect(fixture.network.published()).toHaveLength(1);
    // At the tick the engine re-serves the pending item and the publish lands.
    fixture.network.mode(RELAY, "normal");
    await settle(fixture.timer, 5000);
    expect(fixture.network.published()).toHaveLength(2);
    expect(callsOf(script, "take_outbound").length).toBeGreaterThanOrEqual(2);
    await fixture.client.close();
  });

  test("an unavailable publish retries on its own timer, not the engine wake", async () => {
    const script = freshScript();
    // The engine asks for a wake a minute out — the outbox retry must not wait for it.
    script.nextWake = 60_000n;
    const outbound: Outbound = {
      id: `0e${"0".repeat(62)}`,
      event: event({ id: "88".repeat(32), kind: 445, tags: [["h", ROUTE]] }),
      relays: [RELAY],
      purpose: "application_message",
    };
    script.outbound = [outbound];
    const fixture = await open(script, encodeClientState({ cursors: {}, inbox: [RELAY] }));
    fixture.network.mode(RELAY, "down");
    await settle(fixture.timer, 0);
    const takes = callsOf(script, "take_outbound").length;
    expect(takes).toBeGreaterThan(0);
    // Nothing due before the 5 s retry timer — the 60 s wake does not pre-empt it.
    await settle(fixture.timer, 4999);
    expect(callsOf(script, "take_outbound")).toHaveLength(takes);
    fixture.network.mode(RELAY, "normal");
    await settle(fixture.timer, 5000);
    expect(callsOf(script, "take_outbound").length).toBeGreaterThan(takes);
    await fixture.client.close();
  });

  test("a relay dropping the subscription schedules a re-plan", async () => {
    const script = freshScript();
    script.groups = [snapshot()];
    const fixture = await open(script, encodeClientState({ cursors: {}, inbox: [RELAY] }));
    await settle(fixture.timer, 5000);
    // Inbox 1059 + group 445 = two open subscriptions.
    expect(fixture.network.openSubscriptions("c")).toBe(2);
    fixture.network.mode(RELAY, "down");
    await settle(fixture.timer, 5000);
    expect(fixture.network.openSubscriptions("c")).toBe(0);
    fixture.network.mode(RELAY, "normal");
    // The relay-initiated close schedules a reconcile after 5 s.
    fixture.timer.tick(5001);
    await settle(fixture.timer, 5000);
    expect(fixture.network.openSubscriptions("c")).toBe(2);
    await fixture.client.close();
  });

  test("createGroup refuses unsafe relay urls before touching the engine", async () => {
    const fixture = await open(freshScript());
    await expect(
      fixture.client.createGroup({
        name: "x",
        description: "",
        relays: ["wss://10.0.0.1"],
      }),
    ).rejects.toMatchObject({ kind: "unsafe_relay" });
    expect(callsOf(fixture.script, "create_group")).toHaveLength(0);
    await fixture.client.close();
  });

  test("lookupInvitees maps non-found answers to typed errors", async () => {
    const fixture = await open(freshScript());
    await expect(fixture.client.lookupInvitees("f".repeat(64), [LOOKUP])).rejects.toMatchObject({
      kind: "no_relay_list",
    });
    await fixture.client.close();
  });

  test("ops after close reject with closed", async () => {
    const fixture = await open(freshScript());
    await fixture.client.close();
    await expect(fixture.client.groups()).rejects.toMatchObject({ kind: "closed" });
    await expect(
      fixture.client.sendMessage(GROUP, { kind: 9, tags: [], content: "x" }),
    ).rejects.toMatchObject({ kind: "closed" });
  });

  test("close tears down every subscription", async () => {
    const script = freshScript();
    script.groups = [snapshot()];
    const fixture = await open(script, encodeClientState({ cursors: {}, inbox: [RELAY] }));
    await settle(fixture.timer, 5000);
    expect(fixture.network.openSubscriptions("c")).toBe(2);
    await fixture.client.close();
    expect(fixture.network.openSubscriptions("c")).toBe(0);
  });

  test("reopen restores inbox subscriptions from the persisted state", async () => {
    const script = freshScript();
    const first = await open(script, encodeClientState({ cursors: {}, inbox: [RELAY2] }));
    await settle(first.timer, 5000);
    const blob = first.cursors.bytes();
    await first.client.close();

    const second = await open(freshScript(), blob);
    await settle(second.timer, 5000);
    expect(second.network.openSubscriptions("c")).toBe(1);
    await second.client.close();
  });

  test("uncovered groups emit subscription_limit", async () => {
    const script = freshScript();
    // 17 groups on 17 distinct relays overflows MAX_RELAYS.
    script.groups = Array.from({ length: 17 }, (_, i) => ({
      ...snapshot(),
      id: `${i.toString(16).padStart(2, "0")}${"c".repeat(62)}`,
      nostrGroupId: `${i.toString(16).padStart(2, "0")}${"d".repeat(62)}`,
      relays: [`wss://overflow-${i}.example`],
    }));
    const fixture = await open(script, encodeClientState({ cursors: {}, inbox: [] }));
    const errors: MarmotError[] = [];
    fixture.client.onError((error) => errors.push(error));
    // setInbox flags a re-plan; the post-op reconcile hits the relay cap.
    await fixture.client.publishInboxRelays([RELAY]);
    await settle(fixture.timer, 5000);
    expect(errors.map((e) => e.kind)).toContain("subscription_limit");
    await fixture.client.close();
  });

  test("nothing is taken before whenDurable resolves", async () => {
    const script = freshScript();
    script.commitOn.push("tick");
    const fixture = await open(script);
    // The initial zero-delay tick commits; durability is parked before it completes.
    fixture.kv.hold();
    await settle(fixture.timer, 5000);
    expect(fixture.kv.batches()).toHaveLength(0);
    expect(callsOf(script, "take_outbound")).toHaveLength(0);
    fixture.kv.release();
    await settle(fixture.timer, 5000);
    expect(fixture.kv.batches()).toHaveLength(1);
    expect(callsOf(script, "take_outbound").length).toBeGreaterThan(0);
    await fixture.client.close();
  });

  test("a pending resolution never re-takes immediately; the retry comes on the armed wake", async () => {
    const script = freshScript();
    script.nextWake = 5000n;
    script.resolutions.push("pending");
    const outbound: Outbound = {
      id: `0d${"0".repeat(62)}`,
      event: event({ id: "66".repeat(32), kind: 445, tags: [["h", ROUTE]] }),
      relays: [RELAY],
      purpose: "application_message",
    };
    script.outbound = [outbound];
    const fixture = await open(script, encodeClientState({ cursors: {}, inbox: [RELAY] }));
    // Zero-delay open tick: take, publish, report → pending. No re-take afterwards.
    await settle(fixture.timer, 0);
    await settle(fixture.timer, 0);
    expect(callsOf(script, "take_outbound")).toHaveLength(1);
    // The armed wake is 5 s out — nothing fires before it.
    await settle(fixture.timer, 4999);
    expect(callsOf(script, "take_outbound")).toHaveLength(1);
    // The wake tick runs the outbox pass again.
    await settle(fixture.timer, 5000);
    expect(callsOf(script, "take_outbound").length).toBeGreaterThan(1);
    await fixture.client.close();
  });

  test("ack waits for the listener's promise to resolve", async () => {
    const script = freshScript();
    script.drains.push(
      { type: "events", events: [] },
      { type: "events", events: [] }, // the serialized takeOutbound job's drain
      {
        type: "events",
        events: [{ type: "message_received", group: GROUP, message: msg(`f3${"0".repeat(62)}`) }],
      },
    );
    script.groups = [snapshot()];
    const fixture = await open(script, encodeClientState({ cursors: {}, inbox: [RELAY] }));
    await settle(fixture.timer, 5000);

    let release: () => void = () => undefined;
    const gate = new Promise<void>((resolve) => {
      release = resolve;
    });
    const batches: string[][] = [];
    fixture.client.onMessages(async (batch) => {
      await gate;
      batches.push(batch.map((m) => m.message.content));
    });
    await fixture.network.port("peer").publish(RELAY, groupEvent("77".repeat(32)));
    await settle(fixture.timer, 5000);
    expect(callsOf(script, "acknowledge_messages")).toHaveLength(0);
    release();
    await settle(fixture.timer, 5000);
    expect(batches).toStrictEqual([["hi"]]);
    expect(callsOf(script, "acknowledge_messages")).toHaveLength(1);
    await fixture.client.close();
  });

  test("the buffer flushes at 256 without waiting for the delay timer", async () => {
    const script = freshScript();
    script.groups = [snapshot()];
    const fixture = await open(script, encodeClientState({ cursors: {}, inbox: [RELAY] }));
    await settle(fixture.timer, 5000);
    const peer = fixture.network.port("peer");
    for (let i = 0; i < 260; i += 1) {
      // oxlint-disable-next-line no-await-in-loop -- delivery order matters for the cursor set
      await peer.publish(RELAY, groupEvent(`e${i.toString(16).padStart(63, "0")}`));
    }
    // The 256-event cap flushes inline — no timer needed.
    await macrotask();
    const first = ingests(script);
    expect(first).toHaveLength(1);
    expect(first[0]).toHaveLength(256);
    // The four that arrived after the cap ride the deferred zero-delay flush.
    await settle(fixture.timer, 0);
    const all = ingests(script);
    expect(all).toHaveLength(2);
    expect(all[1]).toHaveLength(4);
    await fixture.client.close();
  });

  test("two events inside the delay window ride one ingest batch", async () => {
    const script = freshScript();
    script.groups = [snapshot()];
    const fixture = await open(script, encodeClientState({ cursors: {}, inbox: [RELAY] }));
    await settle(fixture.timer, 5000);
    const peer = fixture.network.port("peer");
    await peer.publish(RELAY, groupEvent("71".repeat(32)));
    await peer.publish(RELAY, groupEvent("72".repeat(32)));
    await settle(fixture.timer, 0);
    expect(ingests(script)).toHaveLength(0);
    await settle(fixture.timer, 25);
    const batches = ingests(script);
    expect(batches).toHaveLength(1);
    expect(batches[0]).toHaveLength(2);
    await fixture.client.close();
  });

  test("a 500-message backlog over successive zero-delay ticks delivers in full", async () => {
    const script = freshScript();
    script.groups = [snapshot()];
    const chunk = (c: number): DrainResponse => ({
      type: "events",
      events: Array.from({ length: 100 }, (_, i) => ({
        type: "message_received" as const,
        group: GROUP,
        message: msg((c * 100 + i).toString(16).padStart(64, "0")),
      })),
    });
    script.drains.push({ type: "events", events: [] }, ...[0, 1, 2, 3, 4].map(chunk));
    // The engine keeps asking for zero-delay ticks — the deferred-peel model.
    script.nextWake = 0n;
    const fixture = await open(script, encodeClientState({ cursors: {}, inbox: [RELAY] }));
    const batches: string[][] = [];
    fixture.client.onMessages(async (batch) => {
      batches.push(batch.map((m) => m.message.content));
      return Promise.resolve();
    });
    await settle(fixture.timer, 5000);
    expect(batches.flat()).toHaveLength(500);
    const acked = script.calls.flatMap((call) =>
      // oxlint-disable-next-line vitest/no-conditional-in-test -- narrowing is a data filter
      call.type === "acknowledge_messages" ? call.ids : [],
    );
    expect(acked).toHaveLength(500);
    await fixture.client.close();
  });

  test("an unavailable outcome caps the cursor at earliest createdAt − 1", async () => {
    const script = freshScript();
    script.groups = [snapshot()];
    const fixture = await open(script, encodeClientState({ cursors: {}, inbox: [RELAY] }));
    await settle(fixture.timer, 5000);
    const key = groupCursorKey(ROUTE, RELAY);
    const peer = fixture.network.port("peer");
    await peer.publish(RELAY, groupEvent("08".repeat(32), 2000n));
    await settle(fixture.timer, 25);
    expect(decodeClientState(fixture.cursors.bytes()).cursors[key]).toBe(2000);
    script.reports = [
      { event: "09".repeat(32), outcome: { type: "unavailable", category: "transport_deferred" } },
    ];
    await peer.publish(RELAY, groupEvent("09".repeat(32), 1500n));
    await settle(fixture.timer, 25);
    expect(decodeClientState(fixture.cursors.bytes()).cursors[key]).toBe(1499);
    await fixture.client.close();
  });

  test("a future createdAt clamps the cursor to now", async () => {
    const script = freshScript();
    script.groups = [snapshot()];
    const fixture = await open(script, encodeClientState({ cursors: {}, inbox: [RELAY] }));
    await settle(fixture.timer, 5000);
    const key = groupCursorKey(ROUTE, RELAY);
    const nowSecs = Number(fixture.clock.now().unixMs / 1000n);
    await fixture.network
      .port("peer")
      .publish(RELAY, groupEvent("0a".repeat(32), BigInt(nowSecs) + 1_000_000_000n));
    await settle(fixture.timer, 25);
    expect(decodeClientState(fixture.cursors.bytes()).cursors[key]).toBe(nowSecs);
    await fixture.client.close();
  });

  test("persisted cursors drive the resubscribe since minus overlap", async () => {
    const script = freshScript();
    script.groups = [snapshot()];
    const state = encodeClientState({
      cursors: {
        [inboxCursorKey(RELAY)]: 200_000,
        [groupCursorKey(ROUTE, RELAY)]: 10_000,
      },
      inbox: [RELAY],
    });
    const fixture = await open(script, state);
    await settle(fixture.timer, 5000);
    const inboxSub = fixture.subs.find((sub) =>
      sub.filters.some((filter) => filter.kinds?.includes(1059) === true),
    );
    const groupSub = fixture.subs.find((sub) =>
      sub.filters.some((filter) => filter.kinds?.includes(445) === true),
    );
    expect(inboxSub?.filters[0]?.since).toBe(200_000 - 172_800);
    expect(groupSub?.filters[0]?.since).toBe(10_000 - 600);
    await fixture.client.close();
  });

  test("200 random group churns stay inside the relay and subscription bounds", async () => {
    const script = freshScript();
    script.groups = [snapshot()];
    const fixture = await open(script, encodeClientState({ cursors: {}, inbox: [RELAY] }));
    await settle(fixture.timer, 5000);
    const errors: MarmotError[] = [];
    fixture.client.onError((error) => errors.push(error));
    let seed = 7;
    const rnd = (): number => {
      seed = (seed * 1_664_525 + 1_013_904_223) % 4_294_967_296;
      return seed / 4_294_967_296;
    };
    const pool = Array.from({ length: 8 }, (_, i) => ({
      ...snapshot(),
      id: `${i.toString(16).padStart(2, "0")}${"c".repeat(62)}`,
      nostrGroupId: `${i.toString(16).padStart(2, "0")}${"d".repeat(62)}`,
      relays: [`wss://g${i % 4}.example`],
    }));
    const live = new Set<number>([0]);
    for (let i = 0; i < 200; i += 1) {
      const index = Math.floor(rnd() * pool.length);
      // oxlint-disable-next-line vitest/no-conditional-in-test -- a random churn is a random branch
      if (live.has(index)) {
        live.delete(index);
      } else {
        live.add(index);
      }
      script.groups = pool.filter((_, j) => live.has(j));
      // A churn event flags the reconcile that follows the op's drain.
      script.drains.push({
        type: "events",
        events: [{ type: "group_joined", group: GROUP }],
      });
      // oxlint-disable-next-line no-await-in-loop -- churns must stay serialized
      await fixture.client.groups();
      // oxlint-disable-next-line no-await-in-loop -- let the reconcile settle before the next churn
      await macrotask();
    }
    await settle(fixture.timer, 5000);
    expect(fixture.network.peakSubscriptions("c")).toBeLessThanOrEqual(64);
    expect(fixture.network.peakRelays("c")).toBeLessThanOrEqual(16);
    // With only four group relays in play the input always fits: every referenced
    // relay holds a 445 subscription and nothing reports subscription_limit.
    const relaysNeeded = new Set(script.groups.flatMap((snap) => snap.relays));
    expect(fixture.network.openRelays("c")).toBeGreaterThanOrEqual(relaysNeeded.size);
    expect(errors.map((e) => e.kind)).not.toContain("subscription_limit");
    await fixture.client.close();
  });

  test("close is idempotent and leaves zero pending timers", async () => {
    const script = freshScript();
    script.groups = [snapshot()];
    const fixture = await open(script, encodeClientState({ cursors: {}, inbox: [RELAY] }));
    await settle(fixture.timer, 5000);
    // An inbound wrap arms the 25 ms flush timer.
    await fixture.network.port("peer").publish(RELAY, wrap("aa".repeat(32)));
    await macrotask();
    expect(fixture.timer.pending()).toBeGreaterThan(0);
    await fixture.client.close();
    await fixture.client.close();
    expect(fixture.timer.pending()).toBe(0);
    expect(fixture.network.openSubscriptions()).toBe(0);
  });

  test.each([
    {
      verdict: "refused",
      reason: "signer_refused",
      fail: () => {
        throw new SignerRefusedError();
      },
    },
    {
      verdict: "failed",
      reason: "signer_failed",
      fail: () => {
        throw new Error("signer broke");
      },
    },
  ])("a $verdict signer during wrap ingest emits a blocked invite", async ({ reason, fail }) => {
    const sign = signer();
    sign.nip44Decrypt = async () => {
      fail();
      return Promise.resolve("plain");
    };
    const script = freshScript();
    const wrapId = "9a".repeat(32);
    script.reports = [
      { event: wrapId, outcome: { type: "rejected", category: "invalid_encoding" } },
    ];
    script.suspendOnIngest = {
      type: "nip44_decrypt",
      peer: "bb".repeat(32),
      ciphertext: "x",
    };
    const fixture = await open(script, encodeClientState({ cursors: {}, inbox: [RELAY] }), sign);
    const invites: unknown[] = [];
    fixture.client.onInvite((invite) => {
      invites.push(invite);
    });
    await fixture.network.port("peer").publish(RELAY, wrap(wrapId));
    await settle(fixture.timer, 25);
    expect(invites).toStrictEqual([{ type: "blocked", reason, wrap: wrapId }]);
    await fixture.client.close();
  });

  test("a failed media upload discards every sealed handle and rejects upload_failed", async () => {
    const fixture = await open(freshScript(), encodeClientState({ cursors: {}, inbox: [RELAY] }));
    const attachments: AttachmentInput[] = [
      {
        plaintext: Uint8Array.of(1),
        mediaType: "image/png",
        filename: "a.png",
        dim: undefined,
        thumbhash: undefined,
      },
      {
        plaintext: Uint8Array.of(2),
        mediaType: "image/png",
        filename: "b.png",
        dim: undefined,
        thumbhash: undefined,
      },
    ];
    const upload = async () =>
      Promise.reject(new Error(`upload to wss://${"e".repeat(63)}.example failed`));
    const error = await fixture.client
      .sendMedia(GROUP, { attachments, caption: "hi" }, upload)
      .then(
        () => undefined,
        (error_: unknown) => error_,
      );
    expect(error).toMatchObject({ kind: "upload_failed" });
    expect((error as Error).message).not.toContain("wss://");
    expect(callsOf(fixture.script, "discard_attachment")).toHaveLength(2);
    await fixture.client.close();
  });

  test("an outbound item with an unsafe relay is locally rejected with no contact", async () => {
    const script = freshScript();
    script.outbound = [
      {
        id: `0c${"0".repeat(62)}`,
        event: event({ id: "55".repeat(32), kind: 445, tags: [["h", ROUTE]] }),
        relays: ["wss://10.0.0.1:8443"],
        purpose: "application_message",
      },
    ];
    const fixture = await open(script, encodeClientState({ cursors: {}, inbox: [RELAY] }));
    await settle(fixture.timer, 5000);
    expect(fixture.network.published()).toHaveLength(0);
    const reports = callsOf(script, "report_publish");
    expect(reports).toHaveLength(1);
    expect(reports[0]).toMatchObject({
      results: [{ relay: "wss://10.0.0.1:8443", status: "rejected" }],
    });
    await fixture.client.close();
  });

  test("error messages never embed a 64-hex run or a relay url", async () => {
    const fixture = await open(freshScript());
    const messages: string[] = [];
    const collect = (error: unknown): void => {
      // oxlint-disable-next-line vitest/no-conditional-in-test -- narrowing the thrown value
      if (error instanceof Error) {
        messages.push(error.message);
      }
    };
    await fixture.client
      .createGroup({ name: "x", description: "", relays: ["wss://10.0.0.1"] })
      .then(() => undefined, collect);
    await fixture.client.lookupInvitees("f".repeat(64), [LOOKUP]).then(() => undefined, collect);
    await fixture.client.close();
    await fixture.client.groups().then(() => undefined, collect);
    expect(messages.length).toBeGreaterThan(0);
    for (const message of messages) {
      expect(message).not.toMatch(/[0-9a-f]{64}/);
      expect(message).not.toMatch(/wss?:\/\//);
    }
  });
});
