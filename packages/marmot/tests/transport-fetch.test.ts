import { describe, expect, test } from "vite-plus/test";

import type { NostrEvent } from "../src/backend/protocol.ts";
import { fakeRelay } from "../src/testing/relay.ts";
import { manualTimer } from "../src/testing/timer.ts";
import { FETCH_TIMEOUT_MS, fetchEvents } from "../src/transport/fetch.ts";
import type { RelayFilter } from "../src/transport/port.ts";

const RELAY = "wss://r1.example";
const RELAY2 = "wss://r2.example";

function event(id: string, partial?: Partial<NostrEvent>): NostrEvent {
  return {
    id,
    pubkey: "aa".repeat(32),
    createdAt: 1000n,
    kind: 10002,
    tags: [],
    content: "",
    sig: "00".repeat(64),
    ...partial,
  };
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
    const shared = event("01".repeat(32));
    await port.publish(RELAY, shared);
    await port.publish(RELAY, event("02".repeat(32)));
    await port.publish(RELAY2, shared); // same event on both relays
    await port.publish(RELAY2, event("03".repeat(32), { kind: 7 })); // kind mismatch
    const timer = manualTimer();
    const result = await fetchEvents(port, timer, [RELAY, RELAY2], {
      kinds: [10002],
      authors: ["aa".repeat(32)],
    });
    const ids = result.map((e) => e.id).toSorted();
    expect(ids).toStrictEqual(["01".repeat(32), "02".repeat(32)]);
    expect(timer.pending()).toBe(0);
  });

  test("a closed subscription also ends that relay's wait", async () => {
    const network = fakeRelay();
    const port = network.port("t");
    await port.publish(RELAY, event("01".repeat(32)));
    network.mode(RELAY2, "down"); // subscribes → closed("down") immediately
    const timer = manualTimer();
    const result = await fetchEvents(port, timer, [RELAY, RELAY2], { kinds: [10002] });
    expect(result.map((e) => e.id)).toStrictEqual(["01".repeat(32)]);
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
});
