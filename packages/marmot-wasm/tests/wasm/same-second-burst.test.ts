/**
 * Paced send burst — the upstream same-second input this file used to pin can no longer come from
 * our session: per-group send pacing caps `created_at` at 50 messages per second and 30 seconds of
 * forward skew, so a same-second flood of >100 events needs a sender that does not pace. That
 * limitation — sorted ingest of a >100 same-second foreign batch loses its shuffled tail to
 * `stale_epoch` — is now pinned in Rust against the raw MDK engine by
 * `crates/mk-core/tests/engine.rs::same_second_burst_sorted_loses_tail_publish_order_delivers`.
 *
 * What this file pins instead: our own paced sender, clock frozen, produces a burst that spreads
 * across distinct `created_at` seconds at the 50-per-second cap inside the 30-second skew bound,
 * and a receiver that ingests the whole burst shuffled in ONE batch delivers every message with
 * zero `stale` — the per-second blocks stay inside the out-of-order tolerance that defeated the old
 * same-second flood.
 */

import { writeFileSync } from "node:fs";
import { fileURLToPath } from "node:url";

import { describe, expect, test } from "vite-plus/test";

import { Engine } from "@qntx/marmot/backend";
import type { NostrEvent, SessionEvent } from "@qntx/marmot/backend";
import { fakeRelay, manualClock, memoryKv } from "@qntx/marmot/testing";
import type { FakeRelayNetwork, ManualClock } from "@qntx/marmot/testing";

import { releaseBackend } from "../shared/env.ts";
import { testSigner } from "../signer.ts";

const RELAY = "wss://relay.example";
const MESSAGES = 250;
/** Send pacing: at most this many messages per `created_at` second per group. */
const PER_SECOND_CAP = 50;
/** Send pacing: `created_at` may lead the clock by at most this many seconds. */
const SKEW_SECONDS = 30;

async function publish(engine: Engine, network: FakeRelayNetwork): Promise<void> {
  const port = network.port("test");
  for (let round = 0; round < 64; round += 1) {
    // oxlint-disable-next-line no-await-in-loop -- the flow is sequential by design
    await engine.whenDurable();
    // oxlint-disable-next-line no-await-in-loop -- the flow is sequential by design
    const items = await engine.takeOutbound();
    if (items.length === 0) {
      return;
    }
    for (const item of items) {
      const results = [];
      for (const relay of item.relays) {
        // oxlint-disable-next-line no-await-in-loop -- relay verdicts are sequential
        results.push({ relay, status: await port.publish(relay, item.event) });
      }
      // oxlint-disable-next-line no-await-in-loop -- the flow is sequential by design
      await engine.reportPublish(item.id, results);
    }
  }
  throw new Error("publish did not quiesce in 64 rounds");
}

async function publishIdentity(engine: Engine, network: FakeRelayNetwork): Promise<void> {
  await engine.ensureKeyPackage([RELAY]);
  await publish(engine, network);
  await engine.publishInboxRelays([RELAY]);
  await publish(engine, network);
}

async function routeHex(engine: Engine, group: string): Promise<string> {
  const snapshot = await engine.group(group);
  if (snapshot === undefined) {
    throw new Error("group snapshot missing");
  }
  return snapshot.nostrGroupId;
}

function messageReceived(events: SessionEvent[]): number {
  return events.filter((event) => event.type === "message_received").length;
}

async function drainAck(engine: Engine): Promise<number> {
  const drained = await engine.drain();
  const ids = drained
    .map((event) => (event.type === "message_received" ? event.message.transportId : undefined))
    .filter((id): id is string => id !== undefined);
  if (ids.length > 0) {
    await engine.acknowledgeMessages(ids);
  }
  return messageReceived(drained);
}

type Pair = {
  a: Engine;
  b: Engine;
  aClock: ManualClock;
  bClock: ManualClock;
  relay: FakeRelayNetwork;
  group: string;
};

/** A + B open, identities published, A's group created, B joined and synced quiet. */
async function openPair(): Promise<Pair> {
  const backend = await releaseBackend();
  const relay = fakeRelay();
  const aClock = manualClock();
  const a = await Engine.open({
    backend,
    kv: memoryKv(),
    clock: aClock,
    signer: testSigner(),
    allowLoopback: true,
  });
  const bClock = manualClock();
  const bSigner = testSigner();
  const b = await Engine.open({
    backend,
    kv: memoryKv(),
    clock: bClock,
    signer: bSigner,
    allowLoopback: true,
  });
  await publishIdentity(a, relay);
  await publishIdentity(b, relay);
  const created = await a.createGroup({
    name: "burst",
    description: "",
    relays: [RELAY],
    invitees: [
      {
        keyPackage: relay.keyPackageOf(bSigner.pubkey),
        inboxRelays: [RELAY],
      },
    ],
  });
  const group = created.id;
  await publish(a, relay);
  await b.drain();
  await b.ingest(relay.welcomesFor(bSigner.pubkey));
  for (let round = 0; round < 64; round += 1) {
    // oxlint-disable-next-line no-await-in-loop -- join settles over ticks
    if ((await b.group(group)) !== undefined) {
      break;
    }
    // oxlint-disable-next-line no-await-in-loop -- join settles over ticks
    await b.tick();
  }
  if ((await b.group(group)) === undefined) {
    throw new Error("b never joined the group");
  }
  await b.drain();
  await a.drain();
  return { a, b, aClock, bClock, relay, group };
}

/**
 * A sends `count` messages as fast as the pacing allocator allows on its frozen clock: each send
 * spills into a later `created_at` second once the current second is full. Returns the published
 * events in relay order.
 */
async function sendBurst(pair: Pair, count: number): Promise<NostrEvent[]> {
  const route = await routeHex(pair.b, pair.group);
  const baseline = new Set(pair.relay.groupEvents(route, "b").map((event) => event.id));
  for (let i = 0; i < count; i += 1) {
    // oxlint-disable-next-line no-await-in-loop -- the burst is sequential by design
    await pair.a.sendMessage(pair.group, { kind: 9, tags: [], content: `burst#${i}` });
    // oxlint-disable-next-line no-await-in-loop -- publish each before the next send
    await publish(pair.a, pair.relay);
  }
  return pair.relay.groupEvents(route, "b").filter((event) => !baseline.has(event.id));
}

/** Deterministic Fisher–Yates shuffle (seeded LCG — the lint gate bans bitwise ops). */
function shuffled<T>(items: T[], seed: number): T[] {
  const out = [...items];
  let state = ((seed % 4294967296) + 4294967296) % 4294967296;
  const next = (): number => {
    state = (state * 1664525 + 1013904223) % 4294967296;
    return state / 4294967296;
  };
  for (let i = out.length - 1; i > 0; i -= 1) {
    const j = Math.floor(next() * (i + 1));
    const tmp = out[i] as T;
    out[i] = out[j] as T;
    out[j] = tmp;
  }
  return out;
}

/** Tick + drain until a round adds nothing — deferred peels are bounded by tick rounds. */
async function collectDelivered(
  engine: Engine,
  clock: ManualClock,
  soFar: number,
): Promise<number> {
  let delivered = soFar;
  for (let round = 0; round < 64; round += 1) {
    // oxlint-disable-next-line no-await-in-loop -- settle rounds are sequential
    await engine.tick();
    clock.advance(2000);
    // oxlint-disable-next-line no-await-in-loop -- settle rounds are sequential
    const fresh = await drainAck(engine);
    delivered += fresh;
    if (fresh === 0) {
      return delivered;
    }
  }
  throw new Error("drain did not quiesce in 64 rounds");
}

describe("paced send burst (release wasm)", () => {
  test(
    "a 250-message paced burst spreads across seconds and delivers whole " +
      "from one shuffled ingest",
    { timeout: 120_000 },
    async () => {
      const pair = await openPair();
      const events = await sendBurst(pair, MESSAGES);
      expect(events).toHaveLength(MESSAGES);

      // The burst must honour the pacing shape: >=5 distinct seconds at
      // <=50 per second, inside the 30 s forward-skew bound.
      const created = events.map((event) => Number(event.createdAt));
      const distinct = [...new Set(created)];
      const perSecond = distinct.map(
        (second) => created.filter((value) => value === second).length,
      );
      expect(distinct.length).toBeGreaterThanOrEqual(5);
      expect(Math.max(...perSecond)).toBeLessThanOrEqual(PER_SECOND_CAP);
      expect(Math.max(...created) - Math.min(...created)).toBeLessThanOrEqual(SKEW_SECONDS);

      // One shuffled ingest of the whole burst delivers every message.
      const reports = await pair.b.ingest(shuffled(events, 0x5eed));
      const stale = reports.filter((report) => report.outcome.type === "stale").length;
      const delivered = await collectDelivered(pair.b, pair.bClock, await drainAck(pair.b));

      writeFileSync(
        fileURLToPath(new URL("../../../../target/same-second-burst.txt", import.meta.url)),
        `${MESSAGES} paced sends on a frozen clock, one shuffled ingest:\n` +
          `  created_at histogram: ${distinct
            .toSorted((x, y) => x - y)
            .map((second) => `${second}:${created.filter((value) => value === second).length}`)
            .join(", ")}\n` +
          `  delivered=${delivered} stale=${stale}\n`,
      );
      expect(stale).toBe(0);
      expect(delivered).toBe(MESSAGES);
    },
  );
});
