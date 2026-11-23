import { describe, expect, test } from "vite-plus/test";

import { finalizeEvent, Keys } from "@qntx/nostr";

import type { NostrEvent } from "../src/backend/protocol.ts";
import { fakeRelay } from "../src/testing/relay.ts";
import { manualTimer } from "../src/testing/timer.ts";
import { FETCH_TIMEOUT_MS, fetchEvents } from "../src/transport/fetch.ts";
import type { RelayFilter } from "../src/transport/port.ts";

const RELAY = "wss://r1.example";
const RELAY2 = "wss://r2.example";

const KEYS = Keys.fromSecretKey(new Uint8Array(32).fill(5));

/**
 * A genuinely signed event — `fetchEvents` verifies id + signature before dedupe, so fixtures
 * cannot fake them. The id comes out of `finalizeEvent`; `content` distinguishes fixtures.
 */
function event(partial: {
  kind?: number;
  createdAt?: bigint;
  tags?: string[][];
  content: string;
}): NostrEvent {
  const wire = finalizeEvent(
    {
      kind: partial.kind ?? 10002,
      tags: partial.tags ?? [],
      content: partial.content,
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

/**
 * A forgery: copies `genuine`'s claimed id and pubkey but alters the signed fields, so neither the
 * id hash nor the signature verifies.
 */
function forged(genuine: NostrEvent, content: string): NostrEvent {
  return { ...genuine, createdAt: genuine.createdAt + 1n, content };
}

/** A forged event under a fresh id: everything but the id is copied, so the id hash fails. */
function forgedWithNewId(genuine: NostrEvent, content: string): NostrEvent {
  return { ...genuine, id: "ff".repeat(32), createdAt: genuine.createdAt + 1n, content };
}

const flush = async (): Promise<void> => {
  await Promise.resolve();
  await Promise.resolve();
  await Promise.resolve();
};

describe("fetchEvents", () => {
  test("collects replayed events per relay until eose, deduplicated by id", async () => {
    const network = fakeRelay();
    const port = network.port("t");
    const shared = event({ content: "shared" });
    const second = event({ content: "second" });
    await port.publish(RELAY, shared);
    await port.publish(RELAY, second);
    await port.publish(RELAY2, shared); // same event on both relays
    await port.publish(RELAY2, event({ content: "wrong-kind", kind: 7 })); // kind mismatch
    const timer = manualTimer();
    const result = await fetchEvents(port, timer, [RELAY, RELAY2], {
      kinds: [10002],
      authors: [KEYS.publicKey],
    });
    const ids = result.map((e) => e.id).toSorted();
    expect(ids).toStrictEqual([shared.id, second.id].toSorted());
    expect(timer.pending()).toBe(0);
  });

  test("a closed subscription also ends that relay's wait", async () => {
    const network = fakeRelay();
    const port = network.port("t");
    const stored = event({ content: "stored" });
    await port.publish(RELAY, stored);
    network.mode(RELAY2, "down"); // subscribes → closed("down") immediately
    const timer = manualTimer();
    const result = await fetchEvents(port, timer, [RELAY, RELAY2], { kinds: [10002] });
    expect(result.map((e) => e.id)).toStrictEqual([stored.id]);
    expect(timer.pending()).toBe(0);
  });

  test("the timeout fires and closes subscriptions that never ended", async () => {
    const network = fakeRelay();
    const port = network.port("t");
    // A silent port: subscribes but never emits eose/closed.
    const silent = {
      publish: async () => Promise.resolve("accepted" as const),
      connect: async () => Promise.resolve(false),
      subscribe: () => () => {},
    };
    const timer = manualTimer();
    const pending = fetchEvents(silent, timer, [RELAY], { kinds: [1] });
    await flush();
    expect(timer.pending()).toBe(1);
    timer.tick(FETCH_TIMEOUT_MS);
    const result = await pending;
    expect(result).toStrictEqual([]);
    void port;
  });

  test("an empty relay list resolves immediately with no subscriptions", async () => {
    const timer = manualTimer();
    const result = await fetchEvents(fakeRelay().port("t"), timer, [], { kinds: [1] });
    expect(result).toStrictEqual([]);
    expect(timer.pending()).toBe(0);
  });

  test("filters pass through to the port verbatim", async () => {
    const seen: RelayFilter[][] = [];
    const port = {
      publish: async () => Promise.resolve("accepted" as const),
      connect: async () => Promise.resolve(false),
      subscribe: (_relay: string, filters: RelayFilter[], handlers: { eose: () => void }) => {
        seen.push(filters);
        handlers.eose();
        return () => undefined;
      },
    };
    const timer = manualTimer();
    const filter = { kinds: [443], authors: ["bb".repeat(32)], limit: 1 };
    await fetchEvents(port, timer, [RELAY, RELAY2], filter);
    expect(seen).toStrictEqual([[filter], [filter]]);
  });

  test("an event that fails verification never enters the result", async () => {
    const network = fakeRelay();
    const port = network.port("t");
    const genuine = event({ content: "genuine" });
    await port.publish(RELAY, genuine);
    await port.publish(RELAY, forgedWithNewId(genuine, "forged"));
    const result = await fetchEvents(port, manualTimer(), [RELAY], { kinds: [10002] });
    expect(result).toStrictEqual([genuine]);
  });

  test("a forged same-id copy arriving after the genuine KeyPackage does not replace it", async () => {
    const network = fakeRelay();
    const port = network.port("t");
    const genuine = event({ kind: 30_443, content: "genuine-kp" });
    await port.publish(RELAY, genuine);
    await port.publish(RELAY2, forged(genuine, "forged-kp"));
    const result = await fetchEvents(port, manualTimer(), [RELAY, RELAY2], {
      kinds: [30_443],
      authors: [KEYS.publicKey],
    });
    expect(result).toStrictEqual([genuine]);
  });

  test("a forged same-id copy arriving before the genuine KeyPackage does not shadow it", async () => {
    const network = fakeRelay();
    const port = network.port("t");
    const genuine = event({ kind: 30_443, content: "genuine-kp" });
    await port.publish(RELAY, forged(genuine, "forged-kp"));
    await port.publish(RELAY2, genuine);
    const result = await fetchEvents(port, manualTimer(), [RELAY, RELAY2], {
      kinds: [30_443],
      authors: [KEYS.publicKey],
    });
    expect(result).toStrictEqual([genuine]);
  });
});
