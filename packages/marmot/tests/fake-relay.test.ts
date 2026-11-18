import { describe, expect, test } from "vite-plus/test";

import type { NostrEvent } from "../src/backend/protocol.ts";
import { fakeRelay } from "../src/testing/relay.ts";

const RELAY = "wss://r1.example";

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

const silent = {
  event: () => undefined,
  eose: () => undefined,
  closed: () => undefined,
};

describe("fakeRelay kill", () => {
  test("a held publish settles unavailable after kill and stores nothing", async () => {
    const network = fakeRelay();
    const port = network.port("dead");
    network.hold();
    const verdict = port.publish(RELAY, event({ id: "aa".repeat(32) }));
    expect(network.heldPublishes()).toBe(1);
    network.kill("dead");
    await expect(verdict).resolves.toBe("unavailable");
    expect(network.events(RELAY)).toHaveLength(0);
    expect(network.published()).toHaveLength(0);
  });

  test("kill closes subscriptions silently and a dead port cannot publish or subscribe", async () => {
    const network = fakeRelay();
    const dead = network.port("dead");
    const live = network.port("live");
    const deadClosed: string[] = [];
    dead.subscribe(RELAY, [{ kinds: [445] }], {
      ...silent,
      closed: (reason) => deadClosed.push(reason),
    });
    expect(network.openSubscriptions("dead")).toBe(1);
    network.kill("dead");
    expect(network.openSubscriptions("dead")).toBe(0);
    expect(deadClosed).toHaveLength(0);
    await expect(dead.publish(RELAY, event({ id: "cc".repeat(32) }))).resolves.toBe("unavailable");
    expect(network.events(RELAY)).toHaveLength(0);
    expect(network.published()).toHaveLength(0);
    let noopDelivered = 0;
    dead.subscribe(RELAY, [{ kinds: [445] }], {
      ...silent,
      event: () => {
        noopDelivered += 1;
      },
    });
    expect(network.openSubscriptions("dead")).toBe(0);
    let liveDelivered = 0;
    live.subscribe(RELAY, [{ kinds: [445] }], {
      ...silent,
      event: () => {
        liveDelivered += 1;
      },
    });
    await new Promise((resolve) => {
      setTimeout(resolve);
    });
    await expect(live.publish(RELAY, event({ id: "dd".repeat(32) }))).resolves.toBe("accepted");
    await new Promise((resolve) => {
      setTimeout(resolve);
    });
    expect(liveDelivered).toBe(1);
    expect(noopDelivered).toBe(0);
    expect(network.events(RELAY)).toHaveLength(1);
  });
});
