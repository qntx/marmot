/**
 * Host-owned inbox: `MarmotClient` opened `inbox: "host"` never subscribes for kind-1059 — the
 * host's own NIP-17 inbox unwraps gift wraps and forwards Welcome wraps through `ingestWrap`, which
 * peels them through the serialized ingest path. The mode is recorded in the v4 client state blob
 * and a reopen in the other mode is rejected `wrong_inbox_owner` before the engine opens.
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
  InputReport,
  NostrEvent,
} from "../src/backend/protocol.ts";
import { utf8Decode, utf8Encode } from "../src/backend/utf8.ts";
import { MarmotClient } from "../src/client/client.ts";
import { MarmotError } from "../src/client/errors.ts";
import { MAX_INBOUND_EVENT_BYTES } from "../src/client/pump.ts";
import { decodeClientState } from "../src/client/state.ts";
import type { ClientState, InboxOwner } from "../src/client/state.ts";
import type { InviteEvent } from "../src/invites/index.ts";
import { manualClock } from "../src/testing/clock.ts";
import type { ManualClock } from "../src/testing/clock.ts";
import { memoryCursorStore } from "../src/testing/cursors.ts";
import { memoryKv } from "../src/testing/memory.ts";
import { fakeRelay } from "../src/testing/relay.ts";
import { manualTimer } from "../src/testing/timer.ts";
import type { ManualTimer } from "../src/testing/timer.ts";
import type { RelayFilter, RelayPort, SubscriptionHandlers } from "../src/transport/port.ts";

const ACCOUNT = "a".repeat(64);
const RELAY = "wss://r1.example";
const GROUP = "cc".repeat(32);
const ROUTE = "dd".repeat(32);
const DONE: BridgeResponse = { type: "ok", result: { type: "done" } };
const KEYS = Keys.fromSecretKey(new Uint8Array(32).fill(5));

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

type Script = {
  /** Per event id; anything unlisted is accepted. */
  outcomes: Map<string, InputOutcome>;
  drains: DrainResponse[];
  groups: GroupSnapshot[];
  calls: BridgeRequest[];
  opens: number;
};

const freshScript = (): Script => ({
  outcomes: new Map(),
  drains: [],
  groups: [],
  calls: [],
  opens: 0,
});

function scriptedBackend(script: Script): Backend {
  return {
    name: "scripted",
    create: () => {
      const handle: BridgeHandle = {
        open: async () => {
          script.opens += 1;
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
                      outcome: script.outcomes.get(e.id) ?? { type: "accepted" },
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

/** One recorded subscribe. */
type Sub = { relay: string; filters: RelayFilter[]; handlers: SubscriptionHandlers };

/** What survives a restart: relay stores, cursor bytes, kv rows, the wall clock. */
type World = {
  network: ReturnType<typeof fakeRelay>;
  cursors: ReturnType<typeof memoryCursorStore>;
  kv: ReturnType<typeof memoryKv>;
  clock: ManualClock;
  subs: Sub[];
};

const world = (): World => ({
  network: fakeRelay(),
  cursors: memoryCursorStore(),
  kv: memoryKv(),
  clock: manualClock(),
  subs: [],
});

type Fixture = { client: MarmotClient; timer: ManualTimer; invites: InviteEvent[] };

function port(w: World, name: string): RelayPort {
  const inner = w.network.port(name);
  return {
    publish: async (relay, evt) => inner.publish(relay, evt),
    connect: async (relay) => inner.connect(relay),
    subscribe: (relay, filters, handlers) => {
      const sub: Sub = { relay, filters, handlers };
      w.subs.push(sub);
      return inner.subscribe(relay, filters, handlers);
    },
  };
}

async function spawn(w: World, script: Script, name: string, inbox: InboxOwner): Promise<Fixture> {
  const timer = manualTimer(w.clock);
  const client = await MarmotClient.open({
    backend: scriptedBackend(script),
    kv: w.kv,
    cursors: w.cursors,
    signer: signer(),
    clock: w.clock,
    timer,
    relays: port(w, name),
    inbox,
    allowLoopback: true,
  });
  const invites: InviteEvent[] = [];
  client.onInvite((invite) => {
    invites.push(invite);
  });
  return { client, timer, invites };
}

/** An open that may reject: the error and the script's backend-open count. */
async function tryOpen(
  w: World,
  script: Script,
  name: string,
  inbox: InboxOwner,
): Promise<{ client: MarmotClient | undefined; error: unknown }> {
  const timer = manualTimer(w.clock);
  return MarmotClient.open({
    backend: scriptedBackend(script),
    kv: w.kv,
    cursors: w.cursors,
    signer: signer(),
    clock: w.clock,
    timer,
    relays: port(w, name),
    inbox,
    allowLoopback: true,
  })
    .then((client) => ({ client, error: undefined }))
    .catch((error: unknown) => ({ client: undefined, error }));
}

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

const persisted = (w: World): ClientState => {
  const state = decodeClientState(w.cursors.bytes());
  if (state === undefined) {
    throw new Error("persisted blob did not decode");
  }
  return state;
};

const liveGroup = (): GroupSnapshot => ({
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
});

const ingests = (script: Script): number =>
  script.calls.filter((call) => call.type === "ingest").length;

/** Every `kinds` value the client subscribed with, across all recorded subs. */
const subKinds = (w: World): number[] =>
  w.subs.flatMap((sub) => sub.filters.flatMap((filter) => filter.kinds ?? []));

describe("host-owned inbox", () => {
  test("host mode opens no kind-1059 subscription and persists its mode", async () => {
    const w = world();
    const s = freshScript();
    s.groups = [liveGroup()];
    const f = await spawn(w, s, "gen1", "host");
    await settle(f.timer, 5000);

    // The group still gets its kind-445 sub; nothing ever asks for 1059.
    const kinds = new Set(subKinds(w));
    expect(kinds.has(1059)).toBe(false);
    expect(kinds.has(445)).toBe(true);

    const state = persisted(w);
    expect(state.inbox).toBe("host");
    expect(state.inboxRelays).toStrictEqual([]);
    expect(state.floors).toStrictEqual({});
    expect(Object.keys(state.cursors).some((key) => key.startsWith("inbox|"))).toBe(false);
    await f.client.close();
  });

  test("host mode: retryInvites and publishInboxRelays reject wrong_inbox_owner", async () => {
    const w = world();
    const s = freshScript();
    const f = await spawn(w, s, "gen1", "host");
    await settle(f.timer, 5000);

    await expect(f.client.retryInvites()).rejects.toMatchObject({
      kind: "wrong_inbox_owner",
    });
    await expect(f.client.publishInboxRelays([RELAY])).rejects.toMatchObject({
      kind: "wrong_inbox_owner",
    });
    // Neither touched the engine: no ingest, no publish_inbox_relays call.
    expect(ingests(s)).toBe(0);
    expect(s.calls.some((call) => call.type === "publish_inbox_relays")).toBe(false);
    await f.client.close();
  });

  test("client mode: ingestWrap rejects wrong_inbox_owner without reaching the engine", async () => {
    const w = world();
    const s = freshScript();
    const f = await spawn(w, s, "gen1", "client");
    await settle(f.timer, 5000);

    await expect(f.client.ingestWrap(wrap(1_700_000_000))).rejects.toMatchObject({
      kind: "wrong_inbox_owner",
    });
    expect(ingests(s)).toBe(0);
    await f.client.close();
  });

  test("open in the other mode rejects wrong_inbox_owner before the engine opens", async () => {
    const w = world();
    const s1 = freshScript();
    const f1 = await spawn(w, s1, "gen1", "client");
    await settle(f1.timer, 5000);
    expect(persisted(w).inbox).toBe("client");
    await f1.client.close();

    const s2 = freshScript();
    const rejected = await tryOpen(w, s2, "gen2", "host");
    expect(rejected.client).toBeUndefined();
    expect(rejected.error).toBeInstanceOf(MarmotError);
    expect((rejected.error as MarmotError).kind).toBe("wrong_inbox_owner");
    expect(s2.opens).toBe(0);

    // The same blob reopens fine in its own mode.
    const f2 = await spawn(w, freshScript(), "gen3", "client");
    await settle(f2.timer, 5000);
    await f2.client.close();
  });

  test("open client over a host blob rejects wrong_inbox_owner before the engine opens", async () => {
    const w = world();
    const s1 = freshScript();
    const f1 = await spawn(w, s1, "gen1", "host");
    await settle(f1.timer, 5000);
    expect(persisted(w).inbox).toBe("host");
    await f1.client.close();

    const s2 = freshScript();
    const rejected = await tryOpen(w, s2, "gen2", "client");
    expect(rejected.client).toBeUndefined();
    expect(rejected.error).toBeInstanceOf(MarmotError);
    expect((rejected.error as MarmotError).kind).toBe("wrong_inbox_owner");
    expect(s2.opens).toBe(0);

    const f2 = await spawn(w, freshScript(), "gen3", "host");
    await settle(f2.timer, 5000);
    await f2.client.close();
  });

  test("an undecodable or v3 blob adopts the option's mode", async () => {
    const w = world();
    // A v3 blob (with the old `inbox` relay-list shape) decodes to undefined: the host-mode
    // open starts empty — no inbox relays, so no kind-1059 subscription.
    await w.cursors.save(
      utf8Encode(
        JSON.stringify({
          v: 3,
          cursors: { "inbox|wss://r1.example": 42 },
          floors: {},
          inbox: [RELAY],
          nip44Extended: true,
        }),
      ),
    );
    const s1 = freshScript();
    const f1 = await spawn(w, s1, "gen1", "host");
    await settle(f1.timer, 5000);
    const kinds = subKinds(w);
    expect(kinds).not.toContain(1059);
    // v3 decodes to undefined: the empty host-mode state ran the probe and saved a v4 blob.
    expect(persisted(w).inbox).toBe("host");
    await f1.client.close();

    const w2 = world();
    await w2.cursors.save(utf8Encode("{not json"));
    const f2 = await spawn(w2, freshScript(), "gen1", "client");
    await settle(f2.timer, 5000);
    expect(persisted(w2).inbox).toBe("client");
    await f2.client.close();
  });

  test("ingestWrap rejects invalid_input for non-1059, forged and oversize events", async () => {
    const w = world();
    const s = freshScript();
    const f = await spawn(w, s, "gen1", "host");
    await settle(f.timer, 5000);
    const valid = wrap(1_700_000_000);

    const wrongKind = event({ kind: 445, createdAt: 1_700_000_000, tags: [["h", ROUTE]] });
    const badSig: NostrEvent = { ...valid, sig: "0".repeat(128) };
    const badId: NostrEvent = { ...valid, id: "0".repeat(64) };
    const oversize = wrap(1_700_000_000, "x".repeat(MAX_INBOUND_EVENT_BYTES + 1));

    for (const bad of [wrongKind, badSig, badId, oversize]) {
      // oxlint-disable-next-line no-await-in-loop -- each case is its own assertion
      await expect(f.client.ingestWrap(bad)).rejects.toMatchObject({ kind: "invalid_input" });
    }
    expect(ingests(s)).toBe(0);
    await f.client.close();
  });

  test("a signer_refused wrap reports unavailable, emits pending and writes no floor", async () => {
    const w = world();
    const s = freshScript();
    const f = await spawn(w, s, "gen1", "host");
    await settle(f.timer, 5000);

    const refused = wrap(1_700_000_000);
    s.outcomes.set(refused.id, REFUSED);
    const report = await f.client.ingestWrap(refused);
    const expected: InputReport = { event: refused.id, outcome: REFUSED };
    expect(report).toStrictEqual(expected);
    expect(f.invites).toStrictEqual([{ type: "pending", reason: "signer_refused" }]);
    const state = persisted(w);
    expect(state.floors).toStrictEqual({});
    expect(state.cursors).toStrictEqual({});
    await f.client.close();
  });

  test("an accepted wrap returns its report and dispatch follows after durability", async () => {
    const w = world();
    const s = freshScript();
    const f = await spawn(w, s, "gen1", "host");
    await settle(f.timer, 5000);

    const welcome = wrap(1_700_000_000);
    s.drains.push({ type: "events", events: [{ type: "group_joined", group: GROUP }] });
    const report = await f.client.ingestWrap(welcome);
    expect(report).toStrictEqual({ event: welcome.id, outcome: { type: "accepted" } });
    expect(f.invites).toStrictEqual([{ type: "joined", group: GROUP }]);
    await f.client.close();
  });
});
