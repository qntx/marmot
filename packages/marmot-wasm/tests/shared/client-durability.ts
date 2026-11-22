/**
 * `client-durability` — crash/durability suites over the release wasm backend, driven by both the
 * Node and Bun runners: the nothing-published-before-durable gate, the takeOutbound→reportPublish
 * kill window, kills at each pump stage, and ack semantics across restarts.
 */

import { hexEncode } from "@qntx/marmot/backend";
import type { NostrEvent } from "@qntx/marmot/backend";
import type { RelayPort } from "@qntx/marmot/client";
import { fakeRelay } from "@qntx/marmot/testing";

import {
  assertConverged,
  assertQuiet,
  contents,
  crash,
  openPair,
  RELAY_1,
  RELAY_2,
  restart,
  send,
  sleep,
  waitFor,
} from "./client-e2e.ts";
import type { ClientHandle } from "./client-e2e.ts";

export type DurabilityReport = {
  ms: number;
  /** Scenario 7: publishes checked for the event id inside utf8 durable rows; found/total. */
  rowsContainIds?: { found: number; total: number };
};

const utf8 = new TextDecoder("utf-8", { fatal: false });

/** A `RelayPort` that runs `check` on every published event before forwarding it. */
function checkedPort(port: RelayPort, check: (event: NostrEvent) => void): RelayPort {
  return {
    publish: async (relay, event) => {
      check(event);
      return port.publish(relay, event);
    },
    connect: async (relay) => port.connect(relay),
    subscribe: (relay, filters, handlers) => port.subscribe(relay, filters, handlers),
  };
}

/**
 * Scenario 7 — nothing is published before it is durable: hold the kv, `sendMessage` still
 * resolves, 300 ms go by with no send event published, release, the publish lands. While it does,
 * every publish is checked for the event id inside the kv's utf8 durable rows.
 */
export async function durabilityGateBeforePublish(): Promise<DurabilityReport> {
  const start = Date.now();
  const network = fakeRelay(101);
  const rowsFound: string[] = [];
  const rowsChecked: string[] = [];
  const aBox: { current?: ClientHandle } = {};
  const wrapped = checkedPort(network.port("a"), (event) => {
    rowsChecked.push(event.id);
    const durable = (aBox.current?.stores.kv.rows() ?? []).map(
      ([key, value]) => `${hexEncode(key)}${utf8.decode(value)}`,
    );
    if (durable.some((row) => row.includes(event.id))) {
      rowsFound.push(event.id);
    }
  });
  const { a, b, group } = await openPair(network, { a: { port: wrapped } });
  aBox.current = a;
  a.stores.kv.hold();
  const commits = () => network.published().filter((entry) => entry.event.kind === 445).length;
  const publishedBefore = commits();
  await send(a, group, "gated");
  await sleep(300);
  const gated = commits();
  if (gated !== publishedBefore) {
    throw new Error(`published while the commit was held (${gated - publishedBefore} events)`);
  }
  a.stores.kv.release();
  await waitFor(
    () => contents(b, group).includes("gated"),
    "the gated message delivered after release",
  );
  await assertConverged([a, b], group);
  assertQuiet([a, b]);
  const report = {
    ms: Date.now() - start,
    rowsContainIds: { found: rowsFound.length, total: rowsChecked.length },
  };
  await a.client.close();
  await b.client.close();
  return report;
}

/**
 * Scenario 8 — kill between `takeOutbound` and `reportPublish`: hold the network, send, crash the
 * sender while its publish is parked, release after the new instance opened; the outbox republishes
 * and B sees the message exactly once.
 */
export async function killBetweenTakeAndReport(): Promise<DurabilityReport> {
  const start = Date.now();
  const network = fakeRelay(103);
  const { a, b, group } = await openPair(network);
  network.hold();
  const publishedBefore = network.published().length;
  const oldPort = a.portName;
  const sent = send(a, group, "kill-window");
  await waitFor(() => network.heldPublishes() > 0, "a held publish is pending");
  await crash(a);
  network.release();
  await sent;
  await waitFor(() => network.published().length > publishedBefore, "the republished send landed");
  await waitFor(() => contents(b, group).includes("kill-window"), "b received the message");
  // The stored copy came from the reopened instance's port — the dead port stored nothing.
  const storedIds = new Set(
    [...network.events(RELAY_1), ...network.events(RELAY_2)].map((entry) => entry.id),
  );
  const afterKill = network.published().slice(publishedBefore);
  if (!afterKill.some((entry) => entry.client === a.portName && storedIds.has(entry.event.id))) {
    throw new Error(`no stored copy published by the reopened port ${a.portName}`);
  }
  if (afterKill.some((entry) => entry.client === oldPort)) {
    throw new Error(`the dead port ${oldPort} reached the network after the kill`);
  }
  const deliveries = contents(b, group).filter((text) => text === "kill-window").length;
  if (deliveries !== 1) {
    throw new Error(`b received the message ${deliveries} times`);
  }
  await assertConverged([a, b], group);
  assertQuiet([a, b]);
  const report = { ms: Date.now() - start };
  await a.client.close();
  await b.client.close();
  return report;
}

export type KillStage = "buffered" | "commit" | "cursor" | "handler";

/**
 * Scenario 9 — kill B at each pump stage, reopen, converge: (a) inbound buffered, (b) ingest done
 * but kv commit parked, (c) durable but cursor save parked, (d) `onMessages` never resolving.
 */
export async function killAtStage(stage: KillStage): Promise<DurabilityReport> {
  const start = Date.now();
  const network = fakeRelay(107 + stage.length);
  const { a, b, group } = await openPair(network, {
    b:
      stage === "handler"
        ? {
            onMessages: async () =>
              new Promise<void>(() => {
                // Intentionally never settles — the handler stage never acks.
              }),
          }
        : undefined,
  });
  const tag = `stage-${stage}`;
  const servedBefore = network.served(b.portName);
  if (stage === "commit") {
    b.stores.kv.hold();
  }
  if (stage === "cursor") {
    b.stores.cursors.hold();
  }
  const sent = send(a, group, tag);
  switch (stage) {
    case "buffered":
      // Crash the instant the relay delivers — the 25 ms flush window is almost surely still open.
      await waitFor(() => network.served(b.portName) > servedBefore, "b served the event");
      await crash(b);
      break;

    case "commit":
      await waitFor(() => network.served(b.portName) > servedBefore, "b served the event");
      // Give the flush + ingest a moment so the commit is the parked stage, not the ingest call.
      await sleep(80);
      await crash(b);
      break;

    case "cursor":
      await waitFor(() => contents(b, group).includes(tag), "b's listener saw the message");
      // The cursor save covering this ingest is parked inside memoryCursorStore.
      await sleep(50);
      await crash(b);
      break;

    case "handler":
      await waitFor(() => contents(b, group).includes(tag), "b's listener saw the message");
      b.onMessages = undefined; // the new instance acks normally
      await crash(b);
      break;
  }
  await sent;
  await waitFor(() => contents(b, group).includes(tag), `${tag} delivered after reopen`);
  const deliveries = contents(b, group).filter((text) => text === tag).length;
  // Buffered/commit kills lose or redeliver at most the one unacked delivery; cursor/handler kills
  // may legitimately redeliver once (unacked on death).
  if (deliveries > 2) {
    throw new Error(`${tag} delivered ${deliveries} times`);
  }
  await assertConverged([a, b], group);
  assertQuiet([a, b]);
  const report = { ms: Date.now() - start };
  await a.client.close();
  await b.client.close();
  return report;
}

/**
 * Scenario 10 — ack semantics: a resolving handler → restart → no redelivery; a throwing handler →
 * `handler_failed` emitted → restart → the unacked messages come back.
 */
export async function ackSemantics(): Promise<DurabilityReport> {
  const start = Date.now();
  const network = fakeRelay(113);
  const { a, b, group } = await openPair(network);
  // Resolving handler: restart does not redeliver.
  await send(a, group, "ack-me");
  await waitFor(() => contents(b, group).includes("ack-me"), "ack-me delivered");
  await restart(b);
  await send(a, group, "after-restart");
  await waitFor(() => contents(b, group).includes("after-restart"), "post-restart delivery");
  const acked = contents(b, group).filter((text) => text === "ack-me").length;
  if (acked !== 1) {
    throw new Error(`acked message delivered ${acked} times after restart`);
  }
  // Throwing handler: `handler_failed` surfaces and the message is redelivered after restart.
  b.onMessages = () => {
    throw new Error("listener explosion");
  };
  await send(a, group, "unacked");
  await waitFor(() => contents(b, group).includes("unacked"), "unacked delivered");
  await waitFor(
    () => b.errors.some((error) => error.kind === "handler_failed"),
    "handler_failed emitted",
  );
  b.onMessages = undefined;
  await restart(b);
  await waitFor(
    () => contents(b, group).filter((text) => text === "unacked").length === 2,
    "unacked redelivered after restart",
  );
  await assertConverged([a, b], group);
  assertQuiet([a, b], ["handler_failed"]);
  const report = { ms: Date.now() - start };
  await a.client.close();
  await b.client.close();
  return report;
}

/** The `rowsContainIds` field is optional on the report — narrow it for the spec runners. */
export function rowsContainIds(report: DurabilityReport): { found: number; total: number } {
  if (report.rowsContainIds === undefined) {
    throw new Error("durability report missing rowsContainIds");
  }
  return report.rowsContainIds;
}
