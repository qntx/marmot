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

/** A WebSocket implementation whose dial is refused: error and close, never open. */
class RefusedSocket {
  static readonly OPEN = 1;
  static readonly CONNECTING = 0;
  static readonly CLOSING = 2;
  static readonly CLOSED = 3;
  readonly readyState = 0;
  private readonly listeners = new Map<string, Array<(ev: unknown) => void>>();

  constructor(_url: string) {
    queueMicrotask(() => {
      for (const type of ["error", "close"]) {
        for (const listener of this.listeners.get(type) ?? []) {
          listener({});
        }
      }
    });
  }

  send(): void {}
  close(): void {}
  addEventListener(type: string, listener: (ev: unknown) => void): void {
    const list = this.listeners.get(type) ?? [];
    list.push(listener);
    this.listeners.set(type, list);
  }

  removeEventListener(type: string, listener: (ev: unknown) => void): void {
    this.listeners.set(
      type,
      (this.listeners.get(type) ?? []).filter((entry) => entry !== listener),
    );
  }
}

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

  test("an insecure ws:// URL fails before transmission and maps to not_sent", async () => {
    const { network, port } = fixture();
    await expect(
      port.publish("ws://insecure.example", toWireEvent(signedEvent(1000))),
    ).resolves.toBe("not_sent");
    network.close();
  });

  test("a refused dial maps to not_sent and connect answers false", async () => {
    const pool = new Pool({
      websocketImplementation: RefusedSocket,
      connectTimeoutMs: 100,
    });
    const port = nostrRelayPort(pool);
    await expect(port.connect(RELAY)).resolves.toBe(false);
    await expect(port.publish(RELAY, toWireEvent(signedEvent(1000)))).resolves.toBe("not_sent");
  });

  test("a relay that never answers OK maps to unavailable", async () => {
    const network = createFakeRelayNetwork();
    // The relay holds every reply well past the pool's publish timeout: the frame went out.
    network.relay(RELAY, { latencyMs: 500 });
    const pool = new Pool({
      websocketImplementation: network.websocketImplementation,
      publishTimeoutMs: 50,
    });
    const port = nostrRelayPort(pool);
    await expect(port.connect(RELAY)).resolves.toBe(true);
    await expect(port.publish(RELAY, toWireEvent(signedEvent(1000)))).resolves.toBe("unavailable");
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
