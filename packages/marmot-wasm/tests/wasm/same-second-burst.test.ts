/**
 * Same-second-burst — pins the observed engine limitation behind the flaky catch-up stalls: fed
 * more than 100 application messages that share one `created_at` second in a single `ingest` call,
 * the engine loses part of the batch to `stale`/`stale_epoch`. `ingest_batch` sorts by
 * `(created_at, id)`, so same-second events go in by random event id instead of sender-ratchet
 * order, and OpenMLS refuses the stragglers that land beyond its 100-generation out-of-order
 * tolerance (`DEFAULT_OUT_OF_ORDER_TOLERANCE`). Feeding the same events one per `ingest` call in
 * publish order delivers all of them. mk-core/engine behavior, pinned — not worked around.
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
/** Same-second app messages beyond this many lose their shuffled tail to `stale_epoch`. */
const OUT_OF_ORDER_TOLERANCE = 100;

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
  await engine.publishKeyPackage([RELAY]);
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

/** A sends `count` messages without advancing its clock — every event shares one second. */
async function sendBurst(pair: Pair, count: number): Promise<NostrEvent[]> {
  const route = await routeHex(pair.b, pair.group);
  const baseline = new Set(pair.relay.groupEvents(route, "b").map((event) => event.id));
  for (let i = 0; i < count; i += 1) {
    // oxlint-disable-next-line no-await-in-loop -- the burst is sequential by design
    await pair.a.sendMessage(pair.group, { kind: 9, tags: [], content: `burst#${i}` });
    // oxlint-disable-next-line no-await-in-loop -- publish each before the next send
    await publish(pair.a, pair.relay);
  }
  const events = pair.relay.groupEvents(route, "b").filter((event) => !baseline.has(event.id));
  const seconds = new Set(events.map((event) => event.createdAt));
  if (seconds.size !== 1) {
    throw new Error(`burst spans ${seconds.size} created_at seconds, expected 1`);
  }
  return events;
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

describe("same-second burst (release wasm)", () => {
  test(
    "one ingest call fed >100 same-second messages reports a stale tail and loses it " +
      "(engine limitation)",
    { timeout: 120_000 },
    async () => {
      const pair = await openPair();
      const events = await sendBurst(pair, MESSAGES);
      expect(events).toHaveLength(MESSAGES);

      const reports = await pair.b.ingest(events);
      const stale = reports.filter((report) => report.outcome.type === "stale").length;
      const accepted = reports.filter((report) => report.outcome.type === "accepted").length;
      const delivered = await collectDelivered(pair.b, pair.bClock, await drainAck(pair.b));

      writeFileSync(
        fileURLToPath(new URL("../../../../target/same-second-burst.txt", import.meta.url)),
        `one ingest call, ${MESSAGES} same-second events ` +
          `(tolerance ${OUT_OF_ORDER_TOLERANCE} generations):\n` +
          `  accepted=${accepted} stale=${stale} delivered=${delivered}\n`,
      );
      expect(stale).toBeGreaterThan(0);
      expect(delivered + stale).toBe(MESSAGES);
    },
  );

  test(
    "the same >100 same-second events, fed one per ingest call in publish order, all deliver",
    { timeout: 120_000 },
    async () => {
      const pair = await openPair();
      const events = await sendBurst(pair, MESSAGES);
      expect(events).toHaveLength(MESSAGES);

      let stale = 0;
      let delivered = 0;
      for (const event of events) {
        // oxlint-disable-next-line no-await-in-loop -- ingest order is the variable under test
        const reports = await pair.b.ingest([event]);
        stale += reports.filter((report) => report.outcome.type === "stale").length;
        // oxlint-disable-next-line no-await-in-loop -- ack per the host receive contract
        delivered += await drainAck(pair.b);
      }
      delivered = await collectDelivered(pair.b, pair.bClock, delivered);

      expect(stale).toBe(0);
      expect(delivered).toBe(MESSAGES);
    },
  );
});
