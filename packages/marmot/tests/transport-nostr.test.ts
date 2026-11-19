import { describe, expect, test } from "vite-plus/test";

import { finalizeEvent, Keys, Pool } from "@qntx/nostr";
import type { Event } from "@qntx/nostr";
import { createFakeRelayNetwork } from "@qntx/nostr/testing";

import type { NostrEvent } from "../src/backend/protocol.ts";
import { nostrRelayPort } from "../src/transport/nostr.ts";
import type { RelayFilter } from "../src/transport/port.ts";

const RELAY = "wss://r1.example";
const ROUTE = "bb".repeat(32);

const keys = Keys.generate();

/** A properly signed kind-445 event — the fake relay verifies signatures on EVENT writes. */
function signedEvent(createdAt: number, content = "x"): Event {
  return finalizeEvent({ kind: 445, tags: [["h", ROUTE]], content, created_at: createdAt }, keys);
}

function toWireEvent(event: Event): NostrEvent {
  return {
    id: event.id,
    pubkey: event.pubkey,
    createdAt: BigInt(event.created_at),
    kind: event.kind,
    tags: event.tags.map((tag) => [...tag]),
    content: event.content,
    sig: event.sig,
  };
}

function fixture(defaults?: Parameters<typeof createFakeRelayNetwork>[0]) {
  const network = createFakeRelayNetwork(defaults);
  const pool = new Pool({ websocketImplementation: network.websocketImplementation });
  const port = nostrRelayPort(pool);
  return { network, pool, port };
}

/** Resolve when `probe` holds, polling the event loop — bounded so a failure is legible. */
async function waitFor(probe: () => boolean): Promise<void> {
  for (let i = 0; i < 200; i += 1) {
    if (probe()) {
      return;
    }
    // oxlint-disable-next-line no-await-in-loop -- the probe polls one event-loop turn at a time
    await new Promise<void>((resolve) => {
      setTimeout(resolve, 5);
    });
  }
  throw new Error("condition not met within 1s");
}

describe("nostrRelayPort.publish", () => {
  test("ok maps to accepted and the relay stores the event", async () => {
    const { network, port } = fixture();
    const event = signedEvent(1000);
    await expect(port.publish(RELAY, toWireEvent(event))).resolves.toBe("accepted");
    expect(network.relay(RELAY).events()).toStrictEqual([event]);
    network.close();
  });

  test("a rate-limited verdict maps to rejected", async () => {
    const { network, port } = fixture();
    network.relay(RELAY, { rateLimited: true });
    await expect(port.publish(RELAY, toWireEvent(signedEvent(1000)))).resolves.toBe("rejected");
    network.close();
  });

  test("an insecure ws:// URL fails in the pool and maps to unavailable", async () => {
    const { network, port } = fixture();
    await expect(
      port.publish("ws://insecure.example", toWireEvent(signedEvent(1000))),
    ).resolves.toBe("unavailable");
    network.close();
  });
});

describe("nostrRelayPort.subscribe", () => {
  test("replays seeded events then eose, then live events; our close fires no closed", async () => {
    const { network, port } = fixture();
    const seeded = signedEvent(1000, "stored");
    network.relay(RELAY).seed([seeded]);

    const got: NostrEvent[] = [];
    const closings: string[] = [];
    let eosed = false;
    const filters: RelayFilter[] = [{ kinds: [445], "#h": [ROUTE], since: 7, limit: 3 }];
    const close = port.subscribe(RELAY, filters, {
      event: (event) => got.push(event),
      eose: () => {
        eosed = true;
      },
      closed: (reason) => closings.push(reason),
    });

    await waitFor(() => eosed);
    expect(got).toStrictEqual([toWireEvent(seeded)]);

    const live = signedEvent(2000, "live");
    network.relay(RELAY).inject(live);
    await waitFor(() => got.length === 2);
    expect(got[1]).toStrictEqual(toWireEvent(live));

    close();
    await new Promise<void>((resolve) => {
      setTimeout(resolve, 20);
    });
    expect(closings).toStrictEqual([]);
    network.close();
  });

  test("a relay-initiated closeSubscriptions surfaces closed", async () => {
    const { network, port } = fixture();
    const closings: string[] = [];
    let eosed = false;
    port.subscribe(RELAY, [{ kinds: [445] }], {
      event: () => undefined,
      eose: () => {
        eosed = true;
      },
      closed: (reason) => closings.push(reason),
    });
    await waitFor(() => eosed);
    network.relay(RELAY).closeSubscriptions("relay going away");
    await waitFor(() => closings.length === 1);
    expect(closings[0]).toContain("relay going away");
    network.close();
  });
});
