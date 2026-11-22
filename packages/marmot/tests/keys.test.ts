import { describe, expect, test } from "vite-plus/test";

import { finalizeEvent, Keys } from "@qntx/nostr";

import type { NostrEvent } from "../src/backend/protocol.ts";
import { inboxRelaysOf, lookupInvitees, writeRelaysOf } from "../src/keys/index.ts";
import type { InviteeLookup } from "../src/keys/index.ts";
import { fakeRelay } from "../src/testing/relay.ts";
import { manualTimer } from "../src/testing/timer.ts";

const LOOKUP = "wss://lookup.example";
const R1 = "wss://r1.example";
const R2 = "wss://r2.example";

const KEYS = Keys.fromSecretKey(new Uint8Array(32).fill(9));
const OTHER = Keys.fromSecretKey(new Uint8Array(32).fill(8));
const PUB = KEYS.publicKey;

/**
 * A genuinely signed event — `fetchEvents` verifies id + signature before dedupe, so fixtures
 * cannot fake them. The id comes out of `finalizeEvent`; `content` distinguishes fixtures.
 */
function event(
  partial: {
    kind?: number;
    createdAt?: bigint;
    tags?: string[][];
    content: string;
  },
  keys = KEYS,
): NostrEvent {
  const wire = finalizeEvent(
    {
      kind: partial.kind ?? 10002,
      tags: partial.tags ?? [],
      content: partial.content,
      created_at: Number(partial.createdAt ?? 1000n),
    },
    keys,
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

/**
 * A forgery attributed to `PUB`: a newer relay list with the victim's pubkey but a signature that
 * cannot verify, so `verifyInbound` drops it.
 */
function forgedRelayList(relays: string[]): NostrEvent {
  return {
    id: "ef".repeat(32),
    pubkey: PUB,
    createdAt: 9000n,
    kind: 10002,
    tags: relays.map((relay) => ["r", relay]),
    content: "",
    sig: "00".repeat(64),
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

/** The relay on whichever of two same-`createdAt` events carries the lower id. */
function lowerIdRelay(a: NostrEvent, relayA: string, b: NostrEvent, relayB: string): string {
  return a.id < b.id ? relayA : relayB;
}

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
        content: "list",
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
    const older = event({ content: "older", createdAt: 1000n, tags: [["r", R1]] });
    const tiedA = event({ content: "tied-a", createdAt: 2000n, tags: [["r", R2]] });
    const tiedB = event({
      content: "tied-b",
      createdAt: 2000n,
      tags: [["r", "wss://older.example"]],
    });
    const port = await serve(LOOKUP, [tiedA, older, tiedB]);
    const relay = lowerIdRelay(tiedA, R2, tiedB, "wss://older.example");
    await expect(writeRelaysOf(port, timer, PUB, [LOOKUP], false)).resolves.toStrictEqual([
      `${relay}/`,
    ]);
  });

  test("unsafe relays in the list are filtered out of the result", async () => {
    const port = await serve(LOOKUP, [
      event({
        content: "unsafe",
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
      event({ content: "other-author", tags: [["r", R1]] }, OTHER),
      event({ content: "other-kind", kind: 10050, tags: [["relay", R1]] }),
    ]);
    await expect(writeRelaysOf(port, timer, PUB, [LOOKUP], false)).resolves.toBeUndefined();
  });

  test("a forged kind-10002 with a newer created_at does not steer the lookup", async () => {
    const genuine = event({ content: "genuine", createdAt: 1000n, tags: [["r", R1]] });
    const port = await serve(LOOKUP, [genuine, forgedRelayList(["wss://evil.example"])]);
    await expect(writeRelaysOf(port, timer, PUB, [LOOKUP], false)).resolves.toStrictEqual([
      `${R1}/`,
    ]);
  });
});

describe("inboxRelaysOf", () => {
  test("the newest kind-10050 relay tags", async () => {
    const port = await serve(LOOKUP, [
      event({ content: "old", kind: 10050, createdAt: 1000n, tags: [["relay", R1]] }),
      event({ content: "new", kind: 10050, createdAt: 2000n, tags: [["relay", R2]] }),
    ]);
    await expect(inboxRelaysOf(port, timer, PUB, [LOOKUP], false)).resolves.toStrictEqual([
      `${R2}/`,
    ]);
  });
});

describe("lookupInvitees", () => {
  const kp = (content: string, createdAt: bigint, slot?: string): NostrEvent =>
    event({
      content,
      kind: 30_443,
      createdAt,
      tags: slot === undefined ? [] : [["d", slot]],
    });
  const deletion = (content: string, k?: string): NostrEvent =>
    event({
      content,
      kind: 5,
      createdAt: 2000n,
      tags: k === undefined ? [] : [["k", k]],
    });
  const relayList = (relays: string[]): NostrEvent =>
    event({ content: "write-list", tags: relays.map((relay) => ["r", relay]) });
  const dmList = (relays: string[]): NostrEvent =>
    event({ content: "dm-list", kind: 10050, tags: relays.map((relay) => ["relay", relay]) });
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
    await writer.publish(`${LOOKUP}/`, kp("kp", 1000n));
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
    const picked = kp("kp-b", 2000n, "a");
    const packages = [kp("kp-a", 1000n, "a"), picked, kp("kp-c", 1500n, "b")];
    for (const kpEvent of packages) {
      // The KeyPackage fetch happens on the normalized write relay.
      // oxlint-disable-next-line no-await-in-loop -- publishes serialize like the real port
      await writer.publish(`${LOOKUP}/`, kpEvent);
    }
    // A 30443-scoped deletion is fetched too; an untagged one is not.
    const scopedDeletion = deletion("del-scoped", "30443");
    await writer.publish(`${LOOKUP}/`, scopedDeletion);
    await writer.publish(`${LOOKUP}/`, deletion("del-untagged"));

    let received: NostrEvent[] = [];
    const port = network.port("r");
    const lookup = found(
      await lookupInvitees(port, timer, PUB, [LOOKUP], false, async (events) => {
        received = events;
        return Promise.resolve([picked]);
      }),
    );
    expect(received.map((entry) => entry.id).toSorted()).toStrictEqual(
      [...packages.map((entry) => entry.id), scopedDeletion.id].toSorted(),
    );
    expect(lookup.invitees).toHaveLength(1);
    expect(lookup.invitees[0]?.keyPackage.id).toBe(picked.id);
    expect(lookup.invitees[0]?.inboxRelays).toStrictEqual([`${R1}/`, `${R2}/`]);
  });
});
