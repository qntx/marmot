/**
 * Retry soak — the durable outbound pacing over 600 simulated seconds against an always-
 * `unavailable` relay. The session publishes on its backoff cadence (1, 2, 4, 8, 16, 32, 60, 60 s
 * …): attempts land at t = 0, 1, 3, 7, 15, 31, 63, 123, 183, …, 543 s — 15 in total — driven
 * entirely by `tick().nextWakeMs` on the manual clock/timer pair.
 */

import { describe, expect, test } from "vite-plus/test";

import { MarmotClient } from "@qntx/marmot/client";
import type { RelayPort } from "@qntx/marmot/client";
import { manualClock, manualTimer, memoryCursorStore, memoryKv } from "@qntx/marmot/testing";

import { releaseBackend } from "../shared/env.ts";
import { testSigner } from "../signer.ts";

const RELAY = "wss://relay.example";

describe("outbound retry soak (release wasm)", () => {
  test(
    "one item over 600 simulated seconds makes exactly 15 publish attempts",
    { timeout: 120_000 },
    async () => {
      const clock = manualClock();
      const timer = manualTimer(clock);
      let attempts = 0;
      const port: RelayPort = {
        publish: async () => {
          attempts += 1;
          await Promise.resolve();
          return "unavailable";
        },
        connect: async () => {
          await Promise.resolve();
          return false;
        },
        subscribe: () => () => undefined,
      };
      const client = await MarmotClient.open({
        backend: await releaseBackend(),
        kv: memoryKv(),
        cursors: memoryCursorStore(),
        signer: testSigner(),
        clock,
        timer,
        relays: port,
        inbox: "client",
        allowLoopback: false,
      });
      try {
        await client.publishInboxRelays([RELAY]);
        // Flush the open tick and the first take — attempt 1 lands at t = 0.
        await timer.advance(0);
        expect(attempts).toBe(1);
        // Backoff 1, 2, 4, 8, 16, 32, 60, 60 s → t = 1, 3, 7, 15, 31, 63, 123, …, 543 s.
        await timer.advance(600_000);
        expect(attempts).toBe(15);
      } finally {
        await client.close();
      }
    },
  );
});
