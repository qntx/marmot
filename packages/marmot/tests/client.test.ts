import { describe, expect, test } from "vite-plus/test";

import { finalizeEvent, Keys } from "@qntx/nostr";

import {
  decodeCallValue,
  encodeDrainResponse,
  encodeOpenResponse,
  encodeResponse,
} from "../src/backend/codec.ts";
import type { Json } from "../src/backend/codec.ts";
import { SignerRefusedError } from "../src/backend/engine.ts";
import type { Signer } from "../src/backend/engine.ts";
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
  Semantics,
  WireError,
} from "../src/backend/protocol.ts";
import { utf8Decode, utf8Encode } from "../src/backend/utf8.ts";
import { MarmotClient } from "../src/client/client.ts";
import { MarmotError } from "../src/client/errors.ts";
import { COMMIT_SIZE_FLOOR_BYTES, MAX_TRACKED_COMMITS } from "../src/client/pump.ts";
import { decodeClientState, encodeClientState } from "../src/client/state.ts";
import { manualClock } from "../src/testing/clock.ts";
import { memoryCursorStore } from "../src/testing/cursors.ts";
import { memoryKv } from "../src/testing/memory.ts";
import { fakeRelay } from "../src/testing/relay.ts";
import { manualTimer } from "../src/testing/timer.ts";
import type { ManualTimer } from "../src/testing/timer.ts";
import { wireEventBytes } from "../src/transport/nostr.ts";
import { groupCursorKey, inboxCursorKey } from "../src/transport/plan.ts";
import type { RelayFilter, RelayPort } from "../src/transport/port.ts";

const ACCOUNT = "a".repeat(64);
const RELAY = "wss://r1.example";
const RELAY2 = "wss://r2.example";
const LOOKUP = "wss://lookup.example";
const GROUP = "cc".repeat(32);
const ROUTE = "dd".repeat(32);
const DONE: BridgeResponse = { type: "ok", result: { type: "done" } };

const KEYS = Keys.fromSecretKey(new Uint8Array(32).fill(5));

/**
 * A genuinely signed wire event — the pump verifies id + signature before dedupe, so fixtures
 * cannot fake them. Set `kind`/`createdAt`/`tags`/`content` here, before signing; the id, pubkey
 * and sig come out of `finalizeEvent`.
 */
function event(partial: {
  kind?: number;
  createdAt?: bigint;
  tags?: string[][];
  content?: string;
}): NostrEvent {
  const wire = finalizeEvent(
    {
      kind: partial.kind ?? 445,
      tags: partial.tags ?? [],
      content: partial.content ?? "",
      created_at: Number(partial.createdAt ?? 1000n),
    },
    KEYS,
  );
  return {
    id: wire.id,
    pubkey: wire.pubkey,
    createdAt: BigInt(wire.created_at),
    kind: wire.kind,
    tags: wire.tags.map((tag) => [...tag]),
    content: wire.content,
    sig: wire.sig,
  };
}

const wrap = (createdAt = 1000n): NostrEvent =>
  event({ kind: 1059, createdAt, tags: [["p", ACCOUNT]] });

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
  semantics: { type: "chat" as const, replyTo: undefined },
  expiresAt: undefined,
});

const groupEvent = (createdAt = 1000n, content = ""): NostrEvent =>
  event({ kind: 445, createdAt, tags: [["h", ROUTE]], content });

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
    avatar: undefined,
    retentionSeconds: 0n,
    disbandEnabled: true,
    disbanding: false,
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
  /**
   * Items the engine holds live after a `pending`/`confirmed` report — withheld from
   * `take_outbound` until its own deadline. A test pushes them back into `outbound` to model the
   * deadline arriving.
   */
  deferred: Outbound[];
  /** `groups` answer. */
  groups: GroupSnapshot[];
  /** `tick` answer (ms until wake). */
  nextWake: bigint | undefined;
  /** `message_sent`/`send_outcome` answer for send-type ops. */
  sent: BridgeResponse;
  /** `send_outcome` answer for membership/lifecycle ops. */
  outcome: BridgeResponse;
  /** `key_packages` answer: `auto` selects the kind-30443 inputs. */
  keyPackages: NostrEvent[] | "auto";
  /** Wire errors to answer for the matching request types. */
  errors: Partial<Record<BridgeRequest["type"], WireError["kind"]>>;
  /** Emit `host.commit` (seq ascending) when these request types run. */
  commitOn: Array<BridgeRequest["type"]>;
  calls: BridgeRequest[];
};

function scriptedBackend(script: Script): {
  backend: Backend;
  closed: () => boolean;
  opened: () => number;
} {
  let closed = false;
  let opened = 0;
  let seq = 0n;
  const inflight = new Map<string, Outbound>();
  const done = (): BridgeResponse => DONE;
  const backend: Backend = {
    name: "scripted",
    create: (host: BridgeHost) => {
      const handle: BridgeHandle = {
        open: async () => {
          opened += 1;
          await Promise.resolve();
          return utf8Encode(
            JSON.stringify(encodeOpenResponse({ type: "ready", account: ACCOUNT })),
          );
        },
        call: async (bytes) => {
          await Promise.resolve();
          const { request } = decodeCallValue(JSON.parse(utf8Decode(bytes)) as Json);
          script.calls.push(request);
          const injected = script.errors[request.type];
          const response: BridgeResponse =
            injected === undefined
              ? ((): BridgeResponse => {
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
                      const item = inflight.get(request.id);
                      inflight.delete(request.id);
                      // A pending or confirmed-but-outstanding fanout stays live in the engine,
                      // withheld from `take_outbound` until its own deadline.
                      if (
                        item !== undefined &&
                        (resolution === "pending" || resolution === "confirmed")
                      ) {
                        script.deferred.push(item);
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
                    case "send_intent":
                      return script.sent;
                    case "set_admin":
                    case "set_group_avatar":
                    case "set_retention":
                    case "disband":
                    case "enable_disbanding":
                      return script.outcome;
                    case "select_key_packages":
                      return {
                        type: "ok",
                        result: {
                          type: "key_packages",
                          events:
                            script.keyPackages === "auto"
                              ? request.events.filter((e) => e.kind === 30_443)
                              : script.keyPackages,
                        },
                      };
                    case "send_message":
                    case "create_group":
                      return request.type === "create_group"
                        ? { type: "ok", result: { type: "group_created", group: snapshot() } }
                        : script.sent;
                    default:
                      return done();
                  }
                })()
              : { type: "error", error: { kind: injected } as WireError };
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
  return { backend, closed: () => closed, opened: () => opened };
}

/** An echo signer: `nip44Decrypt` undoes `nip44Encrypt`, so the open probe round-trips. */
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

/** A pre-NIP-44-extended signer: it cannot round-trip the 65,536-byte probe plaintext. */
function legacySigner(): Signer {
  return {
    ...signer(),
    nip44Encrypt: async () => Promise.resolve("cipher"),
    nip44Decrypt: async () => Promise.resolve("plain"),
  };
}

/** A signer that refuses its NIP-44 legs, like a locked NIP-07 extension. */
function refusingSigner(): Signer {
  return {
    ...signer(),
    nip44Encrypt: async () => Promise.reject(new SignerRefusedError()),
    nip44Decrypt: async () => Promise.reject(new SignerRefusedError()),
  };
}

/** An echo signer that counts its NIP-44 invocations, so tests can detect a skipped probe. */
function countingSigner(): { sign: Signer; nip44Calls: () => number } {
  const base = signer();
  let calls = 0;
  return {
    sign: {
      ...base,
      nip44Encrypt: async (peer, plaintext) => {
        calls += 1;
        return base.nip44Encrypt(peer, plaintext);
      },
      nip44Decrypt: async (peer, ciphertext) => {
        calls += 1;
        return base.nip44Decrypt(peer, ciphertext);
      },
    },
    nip44Calls: () => calls,
  };
}

function freshScript(): Script {
  return {
    drains: [],
    reports: "auto",
    outbound: [],
    resolutions: [],
    deferred: [],
    groups: [],
    nextWake: undefined,
    errors: {},
    keyPackages: "auto",
    outcome: {
      type: "ok",
      result: { type: "send_outcome", outcome: { type: "queued" } },
    },
    sent: {
      type: "ok",
      result: {
        type: "message_sent",
        outcome: { type: "queued" },
        message: {
          id: "aa".repeat(32),
          pubkey: "bb".repeat(32),
          createdAt: 1000n,
          kind: 9,
          tags: [],
          content: "hi",
        },
      },
    },
    commitOn: [],
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
    connect: async (relay) => inner.connect(relay),
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
    const fixture = await open(
      freshScript(),
      encodeClientState({ nip44Extended: true, cursors: {}, inbox: [RELAY] }),
    );
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
    const fixture = await open(
      script,
      encodeClientState({ nip44Extended: true, cursors: {}, inbox: [RELAY] }),
    );
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
    await fixture.network.port("peer").publish(RELAY, wrap());
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
      semantics: { type: "chat" as const, replyTo: undefined },
      expiresAt: undefined,
    };
    script.drains.push(
      { type: "events", events: [] }, // open's first drain
      { type: "events", events: [] }, // the serialized takeOutbound job's drain
      { type: "events", events: [{ type: "message_received", group: GROUP, message: msg }] },
    );
    script.groups = [snapshot()];
    const fixture = await open(
      script,
      encodeClientState({ nip44Extended: true, cursors: {}, inbox: [RELAY] }),
    );
    await settle(fixture.timer, 5000);

    const errors: string[] = [];
    fixture.client.onError((e) => errors.push(`${e.kind}:${e.message}`));
    const batches: string[][] = [];
    fixture.client.onMessages(async (batch) => {
      batches.push(batch.map((m) => m.message.content));
      return Promise.resolve();
    });
    await fixture.network.port("peer").publish(RELAY, groupEvent());
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
      semantics: { type: "chat" as const, replyTo: undefined },
      expiresAt: undefined,
    };
    script.drains.push(
      { type: "events", events: [] },
      { type: "events", events: [] }, // the serialized takeOutbound job's drain
      { type: "events", events: [{ type: "message_received", group: GROUP, message: msg }] },
    );
    script.groups = [snapshot()];
    const fixture = await open(
      script,
      encodeClientState({ nip44Extended: true, cursors: {}, inbox: [RELAY] }),
    );
    await settle(fixture.timer, 5000);

    const errors: MarmotError[] = [];
    fixture.client.onError((error) => errors.push(error));
    fixture.client.onMessages(async () => Promise.reject(new Error("handler boom")));
    await fixture.network.port("peer").publish(RELAY, groupEvent());
    await settle(fixture.timer, 5000);

    expect(errors.map((e) => e.kind)).toContain("handler_failed");
    expect(callsOf(script, "acknowledge_messages")).toHaveLength(0);
    await fixture.client.close();
  });

  test("outbound items publish through the port and report verdicts", async () => {
    const script = freshScript();
    const outbound: Outbound = {
      id: `0b${"0".repeat(62)}`,
      event: event({ kind: 445, tags: [["h", ROUTE]] }),
      relays: [RELAY],
      purpose: "application_message",
    };
    script.outbound = [outbound];
    const fixture = await open(
      script,
      encodeClientState({ nip44Extended: true, cursors: {}, inbox: [RELAY] }),
    );
    await settle(fixture.timer, 5000);
    expect(fixture.network.published()).toStrictEqual([
      { client: "c", relay: RELAY, event: outbound.event },
    ]);
    const reports = callsOf(script, "report_publish");
    expect(reports).toHaveLength(1);
    expect(reports[0]).toMatchObject({ id: `0b${"0".repeat(62)}` });
    await fixture.client.close();
  });

  test("a pending publish waits for the engine wake; there is no fixed retry tick", async () => {
    const script = freshScript();
    // The engine paces the retry itself — the pump's only wake is `nextWakeMs`.
    script.nextWake = 60_000n;
    script.resolutions.push("pending", "complete");
    const outbound: Outbound = {
      id: `0e${"0".repeat(62)}`,
      event: event({ kind: 445, tags: [["h", ROUTE]] }),
      relays: [RELAY],
      purpose: "application_message",
    };
    script.outbound = [outbound];
    const fixture = await open(
      script,
      encodeClientState({ nip44Extended: true, cursors: {}, inbox: [RELAY] }),
    );
    fixture.network.mode(RELAY, "down");
    await settle(fixture.timer, 0);
    // Down: the publish ran (unavailable) and reported pending; the engine parks the
    // item, so the take that follows every report finds nothing to re-serve.
    expect(callsOf(script, "report_publish")).toHaveLength(1);
    expect(fixture.network.published()).toHaveLength(1);
    expect(callsOf(script, "take_outbound").length).toBeGreaterThanOrEqual(2);
    // The wake is armed at the engine's 60 s — the old fixed 5 s retry tick is gone.
    fixture.network.mode(RELAY, "normal");
    await settle(fixture.timer, 5000);
    expect(fixture.network.published()).toHaveLength(1);
    // The engine re-serves the item; the armed wake's tick takes and publishes it.
    script.outbound.push(...script.deferred.splice(0));
    await settle(fixture.timer, 60_000);
    expect(fixture.network.published()).toHaveLength(2);
    expect(callsOf(script, "report_publish")).toHaveLength(2);
    await fixture.client.close();
  });

  test("a relay dropping the subscription schedules a re-plan", async () => {
    const script = freshScript();
    script.groups = [snapshot()];
    const fixture = await open(
      script,
      encodeClientState({ nip44Extended: true, cursors: {}, inbox: [RELAY] }),
    );
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
    const fixture = await open(
      script,
      encodeClientState({ nip44Extended: true, cursors: {}, inbox: [RELAY] }),
    );
    await settle(fixture.timer, 5000);
    expect(fixture.network.openSubscriptions("c")).toBe(2);
    await fixture.client.close();
    expect(fixture.network.openSubscriptions("c")).toBe(0);
  });

  test("reopen restores inbox subscriptions from the persisted state", async () => {
    const script = freshScript();
    const first = await open(
      script,
      encodeClientState({ nip44Extended: true, cursors: {}, inbox: [RELAY2] }),
    );
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
    const fixture = await open(
      script,
      encodeClientState({ nip44Extended: true, cursors: {}, inbox: [] }),
    );
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

  test("a pending resolution is withheld until the engine re-serves it at the armed wake", async () => {
    const script = freshScript();
    script.nextWake = 5000n;
    script.resolutions.push("pending", "complete");
    const outbound: Outbound = {
      id: `0d${"0".repeat(62)}`,
      event: event({ kind: 445, tags: [["h", ROUTE]] }),
      relays: [RELAY],
      purpose: "application_message",
    };
    script.outbound = [outbound];
    const fixture = await open(
      script,
      encodeClientState({ nip44Extended: true, cursors: {}, inbox: [RELAY] }),
    );
    // Zero-delay open tick: take, publish, report → pending. The re-take that follows
    // every report finds nothing: the engine withholds the item until its deadline.
    await settle(fixture.timer, 0);
    await settle(fixture.timer, 0);
    expect(callsOf(script, "take_outbound").length).toBeGreaterThanOrEqual(2);
    expect(fixture.network.published()).toHaveLength(1);
    // The armed wake is 5 s out — nothing fires before it.
    await settle(fixture.timer, 4999);
    expect(fixture.network.published()).toHaveLength(1);
    // The engine re-serves the item; the wake tick's take publishes it.
    script.outbound.push(...script.deferred.splice(0));
    await settle(fixture.timer, 5000);
    expect(fixture.network.published()).toHaveLength(2);
    await fixture.client.close();
  });

  test("a refused dial reports not_sent to the engine", async () => {
    const script = freshScript();
    script.resolutions.push("pending");
    const outbound: Outbound = {
      id: `0c${"0".repeat(62)}`,
      event: event({ kind: 445, tags: [["h", ROUTE]] }),
      relays: [RELAY],
      purpose: "application_message",
    };
    script.outbound = [outbound];
    const fixture = await open(
      script,
      encodeClientState({ nip44Extended: true, cursors: {}, inbox: [RELAY] }),
    );
    fixture.network.mode(RELAY, "refuse");
    await settle(fixture.timer, 0);
    const reports = callsOf(script, "report_publish");
    expect(reports).toHaveLength(1);
    expect(reports[0]).toMatchObject({
      results: [{ relay: RELAY, status: "not_sent" }],
    });
    await fixture.client.close();
  });

  test("a commit over the size floor is refused locally on a reachable relay", async () => {
    const script = freshScript();
    script.resolutions.push("rolled_back");
    const outbound: Outbound = {
      id: `0f${"0".repeat(62)}`,
      event: event({
        kind: 445,
        tags: [["h", ROUTE]],
        content: "x".repeat(COMMIT_SIZE_FLOOR_BYTES),
      }),
      relays: [RELAY],
      purpose: "commit",
    };
    script.outbound = [outbound];
    const fixture = await open(
      script,
      encodeClientState({ nip44Extended: true, cursors: {}, inbox: [RELAY] }),
    );
    const errors: MarmotError[] = [];
    fixture.client.onError((error) => {
      errors.push(error);
    });
    await settle(fixture.timer, 5000);
    // Nothing was transmitted — the connect probe answered and the commit was refused locally.
    expect(fixture.network.published()).toHaveLength(0);
    const reports = callsOf(script, "report_publish");
    expect(reports).toHaveLength(1);
    expect(reports[0]).toMatchObject({
      results: [{ relay: RELAY, status: "rejected" }],
    });
    expect(errors.map((e) => e.kind)).toStrictEqual(["commit_too_large"]);
    expect(errors[0]?.message).not.toContain(RELAY);
    await fixture.client.close();
  });

  test("an over-floor commit on an unreachable relay reports not_sent without the size error", async () => {
    const script = freshScript();
    script.resolutions.push("pending");
    const outbound: Outbound = {
      id: `10${"0".repeat(62)}`,
      event: event({
        kind: 445,
        tags: [["h", ROUTE]],
        content: "x".repeat(COMMIT_SIZE_FLOOR_BYTES),
      }),
      relays: [RELAY],
      purpose: "commit",
    };
    script.outbound = [outbound];
    const fixture = await open(
      script,
      encodeClientState({ nip44Extended: true, cursors: {}, inbox: [RELAY] }),
    );
    const errors: MarmotError[] = [];
    fixture.client.onError((error) => {
      errors.push(error);
    });
    fixture.network.mode(RELAY, "refuse");
    await settle(fixture.timer, 0);
    expect(fixture.network.published()).toHaveLength(0);
    const reports = callsOf(script, "report_publish");
    expect(reports).toHaveLength(1);
    expect(reports[0]).toMatchObject({
      results: [{ relay: RELAY, status: "not_sent" }],
    });
    expect(errors.map((e) => e.kind)).not.toContain("commit_too_large");
    await fixture.client.close();
  });

  test("a commit at exactly the size floor publishes normally", async () => {
    const script = freshScript();
    const base = event({ kind: 445, tags: [["h", ROUTE]] });
    const pad = COMMIT_SIZE_FLOOR_BYTES - wireEventBytes(base);
    const outbound: Outbound = {
      id: `11${"0".repeat(62)}`,
      event: event({ kind: 445, tags: [["h", ROUTE]], content: "x".repeat(pad) }),
      relays: [RELAY],
      purpose: "commit",
    };
    expect(wireEventBytes(outbound.event)).toBe(COMMIT_SIZE_FLOOR_BYTES);
    script.outbound = [outbound];
    const fixture = await open(
      script,
      encodeClientState({ nip44Extended: true, cursors: {}, inbox: [RELAY] }),
    );
    await settle(fixture.timer, 5000);
    expect(fixture.network.published()).toStrictEqual([
      { client: "c", relay: RELAY, event: outbound.event },
    ]);
    const reports = callsOf(script, "report_publish");
    expect(reports).toHaveLength(1);
    expect(reports[0]).toMatchObject({
      results: [{ relay: RELAY, status: "accepted" }],
    });
    await fixture.client.close();
  });

  test("a relay rejection after a commit confirms warns commit_refused", async () => {
    const script = freshScript();
    script.resolutions.push("confirmed", "pending");
    const outbound: Outbound = {
      id: `12${"0".repeat(62)}`,
      event: event({ kind: 445, tags: [["h", ROUTE]] }),
      relays: [RELAY],
      purpose: "commit",
    };
    script.outbound = [outbound];
    const fixture = await open(
      script,
      encodeClientState({ nip44Extended: true, cursors: {}, inbox: [RELAY] }),
    );
    const errors: MarmotError[] = [];
    fixture.client.onError((error) => {
      errors.push(error);
    });
    await settle(fixture.timer, 0);
    expect(errors.map((e) => e.kind)).not.toContain("commit_refused");
    // The engine re-serves the still-outstanding fanout; the relay now refuses it.
    fixture.network.mode(RELAY, "reject");
    script.outbound.push(...script.deferred.splice(0));
    await fixture.client.groups();
    await settle(fixture.timer, 5000);
    expect(callsOf(script, "report_publish")).toHaveLength(2);
    expect(errors.map((e) => e.kind)).toContain("commit_refused");
    await fixture.client.close();
  });

  test("an early rejection a relay later reverses does not warn", async () => {
    const script = freshScript();
    script.resolutions.push("pending", "confirmed");
    const outbound: Outbound = {
      id: `13${"0".repeat(62)}`,
      event: event({ kind: 445, tags: [["h", ROUTE]] }),
      relays: [RELAY],
      purpose: "commit",
    };
    script.outbound = [outbound];
    const fixture = await open(
      script,
      encodeClientState({ nip44Extended: true, cursors: {}, inbox: [RELAY] }),
    );
    const errors: MarmotError[] = [];
    fixture.client.onError((error) => {
      errors.push(error);
    });
    fixture.network.mode(RELAY, "reject");
    await settle(fixture.timer, 0);
    expect(callsOf(script, "report_publish")).toHaveLength(1);
    // The retry reaches the same relay and is accepted — the refusal is reversed.
    fixture.network.mode(RELAY, "normal");
    script.outbound.push(...script.deferred.splice(0));
    await fixture.client.groups();
    await settle(fixture.timer, 5000);
    expect(callsOf(script, "report_publish")).toHaveLength(2);
    expect(errors.map((e) => e.kind)).not.toContain("commit_refused");
    await fixture.client.close();
  });

  test("a rolled-back commit never warns", async () => {
    const script = freshScript();
    script.resolutions.push("rolled_back");
    const outbound: Outbound = {
      id: `14${"0".repeat(62)}`,
      event: event({ kind: 445, tags: [["h", ROUTE]] }),
      relays: [RELAY],
      purpose: "commit",
    };
    script.outbound = [outbound];
    const fixture = await open(
      script,
      encodeClientState({ nip44Extended: true, cursors: {}, inbox: [RELAY] }),
    );
    const errors: MarmotError[] = [];
    fixture.client.onError((error) => {
      errors.push(error);
    });
    fixture.network.mode(RELAY, "reject");
    await settle(fixture.timer, 0);
    expect(callsOf(script, "report_publish")).toHaveLength(1);
    expect(errors.map((e) => e.kind)).not.toContain("commit_refused");
    await fixture.client.close();
  });

  test("commit bookkeeping evicts the oldest entry past MAX_TRACKED_COMMITS", async () => {
    const script = freshScript();
    const commitEvent = event({ kind: 445, tags: [["h", ROUTE]] });
    const evicted: Outbound = {
      id: `01${"0".repeat(62)}`,
      event: commitEvent,
      relays: [RELAY],
      purpose: "commit",
    };
    const tracked: Outbound = {
      id: `02${"0".repeat(62)}`,
      event: commitEvent,
      relays: [RELAY],
      purpose: "commit",
    };
    const fillers: Outbound[] = Array.from({ length: MAX_TRACKED_COMMITS - 1 }, (_, i) => ({
      id: `f0${i.toString(16).padStart(62, "0")}`,
      event: commitEvent,
      relays: [RELAY],
      purpose: "commit",
    }));
    // 257 commits confirm in order: the first falls out of the 256-entry window.
    script.outbound = [evicted, ...fillers, tracked];
    script.resolutions.push(...script.outbound.map((): PublishResolution => "confirmed"));
    const fixture = await open(
      script,
      encodeClientState({ nip44Extended: true, cursors: {}, inbox: [RELAY] }),
    );
    const errors: MarmotError[] = [];
    fixture.client.onError((error) => {
      errors.push(error);
    });
    await settle(fixture.timer, 0);
    expect(callsOf(script, "report_publish")).toHaveLength(MAX_TRACKED_COMMITS + 1);

    // A refusal against the evicted commit warns nothing; against a tracked one it warns.
    fixture.network.mode(RELAY, "reject");
    script.outbound.push(evicted);
    await fixture.client.groups();
    await settle(fixture.timer, 0);
    expect(errors.map((e) => e.kind)).not.toContain("commit_refused");
    script.outbound.push(tracked);
    await fixture.client.groups();
    await settle(fixture.timer, 0);
    expect(errors.map((e) => e.kind)).toContain("commit_refused");
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
    const fixture = await open(
      script,
      encodeClientState({ nip44Extended: true, cursors: {}, inbox: [RELAY] }),
    );
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
    await fixture.network.port("peer").publish(RELAY, groupEvent());
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
    const fixture = await open(
      script,
      encodeClientState({ nip44Extended: true, cursors: {}, inbox: [RELAY] }),
    );
    await settle(fixture.timer, 5000);
    const peer = fixture.network.port("peer");
    for (let i = 0; i < 260; i += 1) {
      // oxlint-disable-next-line no-await-in-loop -- delivery order matters for the cursor set
      await peer.publish(RELAY, groupEvent(1000n, i.toString()));
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
  }, 30_000);

  test("two events inside the delay window ride one ingest batch", async () => {
    const script = freshScript();
    script.groups = [snapshot()];
    const fixture = await open(
      script,
      encodeClientState({ nip44Extended: true, cursors: {}, inbox: [RELAY] }),
    );
    await settle(fixture.timer, 5000);
    const peer = fixture.network.port("peer");
    await peer.publish(RELAY, groupEvent(1000n, "71"));
    await peer.publish(RELAY, groupEvent(1000n, "72"));
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
    const fixture = await open(
      script,
      encodeClientState({ nip44Extended: true, cursors: {}, inbox: [RELAY] }),
    );
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
    const fixture = await open(
      script,
      encodeClientState({ nip44Extended: true, cursors: {}, inbox: [RELAY] }),
    );
    await settle(fixture.timer, 5000);
    const key = groupCursorKey(ROUTE, RELAY);
    const peer = fixture.network.port("peer");
    await peer.publish(RELAY, groupEvent(2000n));
    await settle(fixture.timer, 25);
    expect(decodeClientState(fixture.cursors.bytes()).cursors[key]).toBe(2000);
    const deferred = groupEvent(1500n);
    script.reports = [
      { event: deferred.id, outcome: { type: "unavailable", category: "transport_deferred" } },
    ];
    await peer.publish(RELAY, deferred);
    await settle(fixture.timer, 25);
    expect(decodeClientState(fixture.cursors.bytes()).cursors[key]).toBe(1499);
    await fixture.client.close();
  });

  test("a future createdAt clamps the cursor to now", async () => {
    const script = freshScript();
    script.groups = [snapshot()];
    const fixture = await open(
      script,
      encodeClientState({ nip44Extended: true, cursors: {}, inbox: [RELAY] }),
    );
    await settle(fixture.timer, 5000);
    const key = groupCursorKey(ROUTE, RELAY);
    const nowSecs = Number(fixture.clock.now().unixMs / 1000n);
    await fixture.network.port("peer").publish(RELAY, groupEvent(BigInt(nowSecs) + 1_000_000_000n));
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
      nip44Extended: true,
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
    const fixture = await open(
      script,
      encodeClientState({ nip44Extended: true, cursors: {}, inbox: [RELAY] }),
    );
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
    const fixture = await open(
      script,
      encodeClientState({ nip44Extended: true, cursors: {}, inbox: [RELAY] }),
    );
    await settle(fixture.timer, 5000);
    // An inbound wrap arms the 25 ms flush timer.
    await fixture.network.port("peer").publish(RELAY, wrap());
    await macrotask();
    expect(fixture.timer.pending()).toBeGreaterThan(0);
    await fixture.client.close();
    await fixture.client.close();
    expect(fixture.timer.pending()).toBe(0);
    expect(fixture.network.openSubscriptions()).toBe(0);
  });

  test.each([{ reason: "signer_refused" as const }, { reason: "signer_failed" as const }])(
    "a $reason wrap ingest emits a retryable pending invite",
    async ({ reason }) => {
      const script = freshScript();
      const wrapEvent = wrap();
      script.reports = [
        { event: wrapEvent.id, outcome: { type: "unavailable", category: reason } },
      ];
      const fixture = await open(
        script,
        encodeClientState({ nip44Extended: true, cursors: {}, inbox: [RELAY] }),
      );
      const invites: unknown[] = [];
      fixture.client.onInvite((invite) => {
        invites.push(invite);
      });
      await fixture.network.port("peer").publish(RELAY, wrapEvent);
      await settle(fixture.timer, 25);
      // Pending carries no wrap id — the client stores no wraps; redelivery is a replay.
      expect(invites).toStrictEqual([{ type: "pending", reason }]);
      await fixture.client.close();
    },
  );

  test("retryInvites closes and reopens the inbox subscription at the capped cursor", async () => {
    const script = freshScript();
    const wrapEvent = wrap(190_000n);
    const fixture = await open(
      script,
      encodeClientState({
        nip44Extended: true,
        cursors: { [inboxCursorKey(RELAY)]: 200_000 },
        inbox: [RELAY],
      }),
    );
    await settle(fixture.timer, 5000);
    expect(fixture.network.openSubscriptions("c")).toBe(1);
    // The unavailable report caps the inbox cursor at the wrap's createdAt − 1.
    script.reports = [
      { event: wrapEvent.id, outcome: { type: "unavailable", category: "signer_refused" } },
    ];
    await fixture.network.port("peer").publish(RELAY, wrapEvent);
    await settle(fixture.timer, 5000);
    expect(decodeClientState(fixture.cursors.bytes()).cursors[inboxCursorKey(RELAY)]).toBe(189_999);
    await fixture.client.retryInvites();
    await settle(fixture.timer, 5000);
    const inboxSubs = fixture.subs.filter((sub) =>
      sub.filters.some((filter) => filter.kinds?.includes(1059) === true),
    );
    // Closed and reopened once, at the capped cursor minus the gift-wrap overlap.
    expect(inboxSubs).toHaveLength(2);
    expect(inboxSubs[1]?.filters[0]?.since).toBe(189_999 - 172_800);
    expect(fixture.network.openSubscriptions("c")).toBe(1);
    await fixture.client.close();
  });

  test("a failed media upload discards every sealed handle and rejects upload_failed", async () => {
    const fixture = await open(
      freshScript(),
      encodeClientState({ nip44Extended: true, cursors: {}, inbox: [RELAY] }),
    );
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
        event: event({ kind: 445, tags: [["h", ROUTE]] }),
        relays: ["wss://10.0.0.1:8443"],
        purpose: "application_message",
      },
    ];
    const fixture = await open(
      script,
      encodeClientState({ nip44Extended: true, cursors: {}, inbox: [RELAY] }),
    );
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

  test("sendMessage returns the sent inner event", async () => {
    const fixture = await open(freshScript());
    const sent = await fixture.client.sendMessage(GROUP, { kind: 9, tags: [], content: "x" });
    expect(sent.outcome).toStrictEqual({ type: "queued" });
    expect(sent.message.id).toBe("aa".repeat(32));
    expect(sent.message.kind).toBe(9);
    await fixture.client.close();
  });

  test("typed intents issue send_intent requests", async () => {
    const fixture = await open(freshScript());
    const reply = "cc".repeat(32);
    const target = "dd".repeat(32);
    const reactions = ["ee".repeat(32)];
    await fixture.client.sendChat(GROUP, "hi", reply);
    await fixture.client.react(GROUP, target, "👍");
    await fixture.client.unreact(GROUP, reactions);
    await fixture.client.editMessage(GROUP, target, "edited");
    await fixture.client.deleteMessage(GROUP, target);
    await fixture.client.removeMessage(GROUP, target);
    const intents = callsOf(fixture.script, "send_intent").map(
      (r) => (r as { intent: unknown }).intent,
    );
    expect(intents).toStrictEqual([
      { type: "chat", content: "hi", replyTo: reply },
      { type: "reaction", target, emoji: "👍" },
      { type: "unreact", reactions },
      { type: "edit", target, content: "edited" },
      { type: "delete", target },
      { type: "remove", target },
    ]);
    await fixture.client.close();
  });

  test("admin, avatar, retention, and lifecycle ops issue the v3 requests", async () => {
    const fixture = await open(freshScript());
    const avatar = { url: "https://example.com/a.png", dim: "64x64", thumbhash: undefined };
    await fixture.client.setAdmin(GROUP, "bb".repeat(32), true);
    await fixture.client.stepDown(GROUP);
    await fixture.client.setGroupAvatar(GROUP, avatar);
    await fixture.client.setGroupAvatar(GROUP, undefined);
    await fixture.client.setRetention(GROUP, 3600n);
    await fixture.client.disband(GROUP);
    await fixture.client.enableDisbanding(GROUP);
    const admins = callsOf(fixture.script, "set_admin").map((c) => {
      const call = c as { account: string; admin: boolean };
      return { account: call.account, admin: call.admin };
    });
    expect(admins).toStrictEqual([
      { account: "bb".repeat(32), admin: true },
      { account: fixture.client.account, admin: false },
    ]);
    const avatars = callsOf(fixture.script, "set_group_avatar").map(
      (c) => (c as { avatar: unknown }).avatar,
    );
    expect(avatars).toStrictEqual([avatar, undefined]);
    expect(
      callsOf(fixture.script, "set_retention").map((c) => (c as { seconds: bigint }).seconds),
    ).toStrictEqual([3600n]);
    expect(callsOf(fixture.script, "disband")).toHaveLength(1);
    expect(callsOf(fixture.script, "enable_disbanding")).toHaveLength(1);
    await fixture.client.close();
  });

  test("forgetGroup forgets and reconciles subscriptions away", async () => {
    const script = freshScript();
    script.groups = [snapshot()];
    const fixture = await open(
      script,
      encodeClientState({ nip44Extended: true, cursors: {}, inbox: [RELAY] }),
    );
    await settle(fixture.timer, 5000);
    expect(fixture.network.openSubscriptions()).toBe(2);
    script.groups = [];
    // The op's own post-run reconcile drops the group's subscription, so it is
    // already closed by the time `forgetGroup` resolves — no settle, no other op.
    await fixture.client.forgetGroup(GROUP);
    expect(callsOf(fixture.script, "forget_group")).toHaveLength(1);
    expect(fixture.network.openSubscriptions()).toBe(1);
    await settle(fixture.timer, 5000);
    expect(fixture.network.openSubscriptions()).toBe(1);
    await fixture.client.close();
  });

  test("app_event_dropped reaches onDropped, not onMessages or onGroupChange", async () => {
    const script = freshScript();
    script.drains.push({
      type: "events",
      events: [
        {
          type: "app_event_dropped",
          group: GROUP,
          reason: "unverifiable",
        },
      ],
    });
    const fixture = await open(script);
    const drops: Array<{ group: string; reason: string }> = [];
    const messages: unknown[] = [];
    const changes: unknown[] = [];
    fixture.client.onDropped((event) => drops.push(event));
    fixture.client.onMessages(async (batch) => {
      messages.push(batch);
      return Promise.resolve();
    });
    fixture.client.onGroupChange((event) => changes.push(event));
    await settle(fixture.timer, 5000);
    expect(drops).toStrictEqual([{ group: GROUP, reason: "unverifiable" }]);
    expect(messages).toHaveLength(0);
    expect(changes).toHaveLength(0);
    await fixture.client.close();
  });

  test("message_received carries semantics and expiresAt through", async () => {
    const script = freshScript();
    const target = "dd".repeat(32);
    const incoming = {
      ...msg("12".repeat(32)),
      semantics: { type: "reaction" as const, target, emoji: "🔥" },
      expiresAt: 3600n,
    };
    script.drains.push(
      { type: "events", events: [] },
      { type: "events", events: [] },
      {
        type: "events",
        events: [{ type: "message_received", group: GROUP, message: incoming }],
      },
    );
    script.groups = [snapshot()];
    const fixture = await open(
      script,
      encodeClientState({ nip44Extended: true, cursors: {}, inbox: [RELAY] }),
    );
    await settle(fixture.timer, 5000);
    const batches: Array<Array<{ semantics: unknown; expiresAt: bigint | undefined }>> = [];
    fixture.client.onMessages(async (batch) => {
      batches.push(batch.map((entry) => entry.message));
      return Promise.resolve();
    });
    await fixture.network.port("peer").publish(RELAY, groupEvent());
    await settle(fixture.timer, 5000);
    expect(batches[0]?.[0]?.semantics).toStrictEqual({ type: "reaction", target, emoji: "🔥" });
    expect(batches[0]?.[0]?.expiresAt).toBe(3600n);
    await fixture.client.close();
  });

  test("message_received keeps only locators that pass safeMediaUrl", async () => {
    const script = freshScript();
    const reference = {
      locators: [
        { kind: "url", value: "https://blossom.example/abc" },
        { kind: "url", value: "http://10.0.0.1/x" },
        { kind: "url", value: "https://169.254.169.254/y" },
      ],
      ciphertextSha256: "aa".repeat(32),
      plaintextSha256: "bb".repeat(32),
      nonce: "cc".repeat(12),
      mediaType: "image/png",
      filename: "a.png",
      dim: undefined,
      thumbhash: undefined,
    };
    const incoming = {
      ...msg("14".repeat(32)),
      attachments: [
        { type: "ok" as const, reference },
        { type: "rejected" as const, reason: "malformed_field" as const },
      ],
    };
    script.drains.push(
      { type: "events", events: [] },
      { type: "events", events: [] },
      {
        type: "events",
        events: [{ type: "message_received", group: GROUP, message: incoming }],
      },
    );
    script.groups = [snapshot()];
    const fixture = await open(
      script,
      encodeClientState({ nip44Extended: true, cursors: {}, inbox: [RELAY] }),
    );
    await settle(fixture.timer, 5000);
    const batches: Array<Array<{ attachments: unknown }>> = [];
    fixture.client.onMessages(async (batch) => {
      batches.push(batch.map((entry) => entry.message));
      return Promise.resolve();
    });
    await fixture.network.port("peer").publish(RELAY, groupEvent());
    await settle(fixture.timer, 5000);
    // The attachment survives with only the safe locator; the rejected entry passes through.
    expect(batches[0]?.[0]?.attachments).toStrictEqual([
      {
        type: "ok",
        reference: {
          ...reference,
          locators: [{ kind: "url", value: "https://blossom.example/abc" }],
        },
      },
      { type: "rejected", reason: "malformed_field" },
    ]);
    await fixture.client.close();
  });

  test("groups() drops an avatar whose URL fails safeMediaUrl", async () => {
    const script = freshScript();
    script.groups = [
      {
        ...snapshot(),
        avatar: { url: "https://10.0.0.1/a.png", dim: undefined, thumbhash: undefined },
      },
      {
        ...snapshot(),
        id: "ee".repeat(32),
        avatar: { url: "https://blossom.example/a.png", dim: undefined, thumbhash: undefined },
      },
    ];
    const fixture = await open(
      script,
      encodeClientState({ nip44Extended: true, cursors: {}, inbox: [] }),
    );
    const groupSnapshots = await fixture.client.groups();
    const avatars = groupSnapshots.map((g) => g.avatar);
    expect(avatars).toStrictEqual([
      undefined,
      { url: "https://blossom.example/a.png", dim: undefined, thumbhash: undefined },
    ]);
    await fixture.client.close();
  });

  test("new bridge errors map to the new client kinds", async () => {
    const script = freshScript();
    script.errors["disband"] = "disbanding_not_enabled";
    script.errors["forget_group"] = "group_disbanding";
    const fixture = await open(script);
    await expect(fixture.client.disband(GROUP)).rejects.toMatchObject({
      kind: "disbanding_not_enabled",
    });
    await expect(fixture.client.forgetGroup(GROUP)).rejects.toMatchObject({
      kind: "group_disbanding",
    });
    await fixture.client.close();
  });

  test("modifier semantics reach onMessages", async () => {
    const script = freshScript();
    const target = "dd".repeat(32);
    const cases: Semantics[] = [
      { type: "edit", target, content: "edited" },
      { type: "deletion", targets: [target] },
      { type: "unreact", reactions: [target] },
    ];
    script.drains.push(
      { type: "events", events: [] },
      { type: "events", events: [] },
      {
        type: "events",
        events: cases.map((semantics, i) => ({
          type: "message_received" as const,
          group: GROUP,
          message: { ...msg((i + 1).toString(16).padStart(2, "0").repeat(32)), semantics },
        })),
      },
    );
    script.groups = [snapshot()];
    const fixture = await open(
      script,
      encodeClientState({ nip44Extended: true, cursors: {}, inbox: [RELAY] }),
    );
    await settle(fixture.timer, 5000);
    const batches: Array<Array<{ semantics: unknown }>> = [];
    fixture.client.onMessages(async (batch) => {
      batches.push(batch.map((entry) => entry.message));
      return Promise.resolve();
    });
    await fixture.network.port("peer").publish(RELAY, groupEvent());
    await settle(fixture.timer, 5000);
    expect(batches[0]?.map((entry) => entry.semantics)).toStrictEqual(cases);
    await fixture.client.close();
  });

  test("message_expired reaches onGroupChange like message_invalidated", async () => {
    const script = freshScript();
    const message = "aa".repeat(32);
    const transportId = "cc".repeat(32);
    script.drains.push({
      type: "events",
      events: [{ type: "message_expired", group: GROUP, message, transportId }],
    });
    const fixture = await open(script);
    const changes: Array<{ type: string; message?: string; transportId?: string }> = [];
    const messages: unknown[] = [];
    fixture.client.onGroupChange((event) => changes.push(event));
    fixture.client.onMessages(async (batch) => {
      messages.push(batch);
      return Promise.resolve();
    });
    await settle(fixture.timer, 5000);
    expect(changes).toStrictEqual([
      { type: "message_expired", group: GROUP, message, transportId },
    ]);
    expect(messages).toHaveLength(0);
    await fixture.client.close();
  });

  test("send_rate_exceeded maps to the client kind", async () => {
    const script = freshScript();
    script.errors["send_intent"] = "send_rate_exceeded";
    const fixture = await open(script);
    await expect(fixture.client.sendChat(GROUP, "hi")).rejects.toMatchObject({
      kind: "send_rate_exceeded",
    });
    await fixture.client.close();
  });
});

type ProbeOutcome = {
  client: MarmotClient | undefined;
  error: unknown;
  opened: () => number;
  cursors: ReturnType<typeof memoryCursorStore>;
  kv: ReturnType<typeof memoryKv>;
};

/** Like `open` but captures a rejection and exposes the backend's `open` count. */
async function probeOpen(
  script: Script,
  options: { blob?: Uint8Array | undefined; sign: Signer },
): Promise<ProbeOutcome> {
  const cursors = memoryCursorStore();
  const kv = memoryKv();
  const { backend, opened } = scriptedBackend(script);
  if (options.blob !== undefined) {
    await cursors.save(options.blob);
  }
  const result = await MarmotClient.open({
    backend,
    kv,
    cursors,
    signer: options.sign,
    clock: manualClock(),
    timer: manualTimer(),
    relays: fakeRelay().port("c"),
    allowLoopback: true,
  }).then(
    (client) => ({ client, error: undefined }),
    (error: unknown) => ({ client: undefined, error }),
  );
  return { ...result, opened, cursors, kv };
}

describe("MarmotClient.open — the NIP-44 extended-length probe", () => {
  test("a signer that cannot round-trip the probe rejects signer_unsupported before open", async () => {
    const outcome = await probeOpen(freshScript(), { sign: legacySigner() });
    expect(outcome.error).toBeInstanceOf(MarmotError);
    expect((outcome.error as MarmotError).kind).toBe("signer_unsupported");
    expect(outcome.opened()).toBe(0);
    expect(outcome.kv.batches()).toHaveLength(0);
  });

  test("a refusing signer rejects signer_refused before the engine opens", async () => {
    const outcome = await probeOpen(freshScript(), { sign: refusingSigner() });
    expect(outcome.error).toBeInstanceOf(MarmotError);
    expect((outcome.error as MarmotError).kind).toBe("signer_refused");
    expect(outcome.opened()).toBe(0);
    expect(outcome.kv.batches()).toHaveLength(0);
  });

  test("a passing probe persists nip44Extended and a second open does not probe", async () => {
    const { sign, nip44Calls } = countingSigner();
    const first = await probeOpen(freshScript(), { sign });
    expect(first.error).toBeUndefined();
    expect(nip44Calls()).toBe(2);
    const state = decodeClientState(first.cursors.bytes());
    expect(state.nip44Extended).toBe(true);
    await first.client?.close();

    const second = await probeOpen(freshScript(), { sign, blob: first.cursors.bytes() });
    expect(second.error).toBeUndefined();
    expect(nip44Calls()).toBe(2);
    await second.client?.close();
  });

  test("a v1 blob decodes to the empty state and probes again", async () => {
    const { sign, nip44Calls } = countingSigner();
    const v1 = utf8Encode('{"v":1,"cursors":{"inbox|wss://r":9},"inbox":["wss://r"]}');
    const outcome = await probeOpen(freshScript(), { sign, blob: v1 });
    expect(outcome.error).toBeUndefined();
    expect(nip44Calls()).toBe(2);
    const state = decodeClientState(outcome.cursors.bytes());
    expect(state.cursors).toStrictEqual({});
    expect(state.nip44Extended).toBe(true);
    await outcome.client?.close();
  });
});
