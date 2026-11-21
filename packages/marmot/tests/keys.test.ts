import { describe, expect, test } from "vite-plus/test";

import type { NostrEvent } from "../src/backend/protocol.ts";
import { inboxRelaysOf, lookupInvitees, writeRelaysOf } from "../src/keys/index.ts";
import type { InviteeLookup } from "../src/keys/index.ts";
import { fakeRelay } from "../src/testing/relay.ts";
import { manualTimer } from "../src/testing/timer.ts";

const LOOKUP = "wss://lookup.example";
const R1 = "wss://r1.example";
const R2 = "wss://r2.example";
const PUB = "aa".repeat(32);

function event(partial: Partial<NostrEvent> & { id: string }): NostrEvent {
  return {
    pubkey: PUB,
    createdAt: 1000n,
    kind: 10002,
    tags: [],
    content: "",
    sig: "00".repeat(64),
    ...partial,
  };
}

/** Publish `events` to `relay` and return the network's port for one client. */
async function serve(relay: string, events: NostrEvent[]) {
  const network = fakeRelay();
  const writer = network.port("author");
  for (const stored of events) {
    // oxlint-disable-next-line no-await-in-loop -- publishes serialize like the real port
    await writer.publish(relay, stored);
  }
  return network.port("reader");
}

const timer = manualTimer();

function found(lookup: InviteeLookup): Extract<InviteeLookup, { type: "found" }> {
  if (lookup.type !== "found") {
    throw new Error(`expected found, got ${lookup.type}`);
  }
  return lookup;
}

describe("writeRelaysOf", () => {
  test("no relay list → undefined", async () => {
    const port = await serve(LOOKUP, []);
    await expect(writeRelaysOf(port, timer, PUB, [LOOKUP], false)).resolves.toBeUndefined();
  });

  test("write-marked and unmarked r tags are kept, read-only dropped", async () => {
    const port = await serve(LOOKUP, [
      event({
        id: "01".repeat(32),
        tags: [
          ["r", R1],
          ["r", R2, "write"],
          ["r", "wss://read-only.example", "read"],
        ],
      }),
    ]);
    await expect(writeRelaysOf(port, timer, PUB, [LOOKUP], false)).resolves.toStrictEqual([
      `${R1}/`,
      `${R2}/`,
    ]);
  });

  test("the newest kind-10002 wins; a createdAt tie picks the lower id", async () => {
    const port = await serve(LOOKUP, [
      event({ id: "03".repeat(32), createdAt: 2000n, tags: [["r", R2]] }),
      event({ id: "01".repeat(32), createdAt: 1000n, tags: [["r", R1]] }),
      event({ id: "02".repeat(32), createdAt: 2000n, tags: [["r", "wss://older.example"]] }),
    ]);
    // Same createdAt (2000): id "02…" < "03…" wins.
    await expect(writeRelaysOf(port, timer, PUB, [LOOKUP], false)).resolves.toStrictEqual([
      "wss://older.example/",
    ]);
  });

  test("unsafe relays in the list are filtered out of the result", async () => {
    const port = await serve(LOOKUP, [
      event({
        id: "01".repeat(32),
        tags: [
          ["r", R1],
          ["r", "wss://10.0.0.1"],
        ],
      }),
    ]);
    await expect(writeRelaysOf(port, timer, PUB, [LOOKUP], false)).resolves.toStrictEqual([
      `${R1}/`,
    ]);
  });

  test("events by another author or another kind do not count", async () => {
    const port = await serve(LOOKUP, [
      event({ id: "01".repeat(32), pubkey: "bb".repeat(32), tags: [["r", R1]] }),
      event({ id: "02".repeat(32), kind: 10050, tags: [["relay", R1]] }),
    ]);
    await expect(writeRelaysOf(port, timer, PUB, [LOOKUP], false)).resolves.toBeUndefined();
  });
});

describe("inboxRelaysOf", () => {
  test("the newest kind-10050 relay tags", async () => {
    const port = await serve(LOOKUP, [
      event({ id: "01".repeat(32), kind: 10050, createdAt: 1000n, tags: [["relay", R1]] }),
      event({ id: "02".repeat(32), kind: 10050, createdAt: 2000n, tags: [["relay", R2]] }),
    ]);
    await expect(inboxRelaysOf(port, timer, PUB, [LOOKUP], false)).resolves.toStrictEqual([
      `${R2}/`,
    ]);
  });
});

describe("lookupInvitees", () => {
  const kp = (id: string, createdAt: bigint, slot?: string): NostrEvent =>
    event({
      id,
      kind: 30_443,
      createdAt,
      tags: slot === undefined ? [] : [["d", slot]],
    });
  const deletion = (id: string, k?: string): NostrEvent =>
    event({
      id,
      kind: 5,
      createdAt: 2000n,
      tags: k === undefined ? [] : [["k", k]],
    });
  const relayList = (relays: string[]): NostrEvent =>
    event({ id: "f0".repeat(32), tags: relays.map((relay) => ["r", relay]) });
  const dmList = (relays: string[]): NostrEvent =>
    event({ id: "f1".repeat(32), kind: 10050, tags: relays.map((relay) => ["relay", relay]) });
  /** The engine's stand-in: keeps every fetched publication. */
  const keepKeyPackages = async (events: NostrEvent[]): Promise<NostrEvent[]> =>
    Promise.resolve(events.filter((entry) => entry.kind === 30_443));

  test("no kind-10002 → no_relay_list", async () => {
    const port = await serve(LOOKUP, []);
    await expect(
      lookupInvitees(port, timer, PUB, [LOOKUP], false, keepKeyPackages),
    ).resolves.toStrictEqual({
      type: "no_relay_list",
    });
  });

  test("an empty selection → no_key_package", async () => {
    const port = await serve(LOOKUP, [relayList([LOOKUP])]);
    await expect(
      lookupInvitees(port, timer, PUB, [LOOKUP], false, async () => Promise.resolve([])),
    ).resolves.toStrictEqual({
      type: "no_key_package",
    });
  });

  test("a KeyPackage but no inbox list → no_inbox_relays", async () => {
    const network = fakeRelay();
    const writer = network.port("a");
    await writer.publish(LOOKUP, relayList([LOOKUP]));
    await writer.publish(`${LOOKUP}/`, kp("02".repeat(32), 1000n));
    const port = network.port("r");
    await expect(
      lookupInvitees(port, timer, PUB, [LOOKUP], false, keepKeyPackages),
    ).resolves.toStrictEqual({
      type: "no_inbox_relays",
    });
  });

  test("selection sees publications and k-tagged deletions, picks the invitees", async () => {
    const network = fakeRelay();
    const writer = network.port("a");
    await writer.publish(LOOKUP, relayList([LOOKUP]));
    await writer.publish(LOOKUP, dmList([R1, R2]));
    const picked = kp("04".repeat(32), 2000n, "a");
    const packages = [kp("03".repeat(32), 1000n, "a"), picked, kp("05".repeat(32), 1500n, "b")];
    for (const kpEvent of packages) {
      // The KeyPackage fetch happens on the normalized write relay.
      // oxlint-disable-next-line no-await-in-loop -- publishes serialize like the real port
      await writer.publish(`${LOOKUP}/`, kpEvent);
    }
    // A 30443-scoped deletion is fetched too; an untagged one is not.
    await writer.publish(`${LOOKUP}/`, deletion("06".repeat(32), "30443"));
    await writer.publish(`${LOOKUP}/`, deletion("07".repeat(32)));

    let received: NostrEvent[] = [];
    const port = network.port("r");
    const lookup = found(
      await lookupInvitees(port, timer, PUB, [LOOKUP], false, async (events) => {
        received = events;
        return Promise.resolve([picked]);
      }),
    );
    expect(received.map((entry) => entry.id).toSorted()).toStrictEqual(
      [...packages.map((entry) => entry.id), "06".repeat(32)].toSorted(),
    );
    expect(lookup.invitees).toHaveLength(1);
    expect(lookup.invitees[0]?.keyPackage.id).toBe("04".repeat(32));
    expect(lookup.invitees[0]?.inboxRelays).toStrictEqual([`${R1}/`, `${R2}/`]);
  });
});
