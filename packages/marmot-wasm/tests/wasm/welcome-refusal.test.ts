/**
 * Welcome-refusal — what `ingest` reports when the signer's `nip44Decrypt` refuses (or fails) a
 * gift wrap, and whether redelivery of the same wrap recovers, in-session and across a reopen.
 *
 * Contract (release artifact): the refused/failed unwrap is `unavailable` with `signer_refused` or
 * `signer_failed`, rolled back in a store savepoint and retryable — a re-ingest with a working
 * signer unwraps and joins, in-session and after a reopen.
 */

import { generateSecretKey } from "nostr-tools";
import { describe, expect, test } from "vite-plus/test";

import { Engine, SignerRefusedError } from "@qntx/marmot/backend";
import type { InputReport, NostrEvent, SessionEvent, Signer } from "@qntx/marmot/backend";
import { fakeRelay, manualClock, memoryKv } from "@qntx/marmot/testing";
import type { FakeRelayNetwork } from "@qntx/marmot/testing";

import { releaseBackend } from "../shared/env.ts";
import { testSigner } from "../signer.ts";

const RELAY = "wss://relay.example";

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

type Refusal = "refused" | "failed";

type RefusalProbe = {
  signer: Signer;
  state: { refuse: boolean; calls: number };
};

/** Wrap `base` so `nip44Decrypt` throws while `refuse` is set; `calls` counts every invocation. */
function refusalProbe(base: Signer, kind: Refusal): RefusalProbe {
  const state = { refuse: false, calls: 0 };
  return {
    state,
    signer: {
      pubkey: base.pubkey,
      signEvent: async (unsigned) => base.signEvent(unsigned),
      nip44Encrypt: async (peer, plaintext) => base.nip44Encrypt(peer, plaintext),
      nip44Decrypt: async (peer, ciphertext) => {
        state.calls += 1;
        await Promise.resolve();
        if (state.refuse) {
          throw kind === "refused" ? new SignerRefusedError() : new Error("test signer failure");
        }
        return base.nip44Decrypt(peer, ciphertext);
      },
    },
  };
}

type Stage = {
  reports: InputReport[];
  events: SessionEvent[];
  groups: number;
  decryptCalls: number;
};

async function snapshot(
  engine: Engine,
  reports: InputReport[],
  decryptCalls: number,
): Promise<Stage> {
  const events = await engine.drain();
  const groups = await engine.groups();
  return {
    reports,
    events,
    groups: groups.length,
    decryptCalls,
  };
}

type Pair = {
  a: Engine;
  b: Engine;
  probe: RefusalProbe;
  relay: FakeRelayNetwork;
  kvB: ReturnType<typeof memoryKv>;
  bSecret: Uint8Array;
};

async function openPair(kind: Refusal): Promise<Pair> {
  const backend = await releaseBackend();
  const relay = fakeRelay();
  const kvB = memoryKv();
  const bSecret = generateSecretKey();
  const probe = refusalProbe(testSigner(bSecret), kind);
  const a = await Engine.open({
    backend,
    kv: memoryKv(),
    clock: manualClock(),
    signer: testSigner(),
    allowLoopback: true,
  });
  const b = await Engine.open({
    backend,
    kv: kvB,
    clock: manualClock(),
    signer: probe.signer,
    allowLoopback: true,
  });
  return { a, b, probe, relay, kvB, bSecret };
}

/** A → B group invite; returns the published gift wraps addressed to B. */
async function inviteB(pair: Pair): Promise<NostrEvent[]> {
  await publishIdentity(pair.a, pair.relay);
  await publishIdentity(pair.b, pair.relay);
  // Flush B's own publish_resolved session events so a later drain reflects only the ingest.
  await pair.b.drain();
  await pair.a.createGroup({
    name: "refusal",
    description: "",
    relays: [RELAY],
    invitees: [
      {
        keyPackage: pair.relay.keyPackageOf(pair.probe.signer.pubkey),
        inboxRelays: [RELAY],
      },
    ],
  });
  await publish(pair.a, pair.relay);
  return pair.relay.welcomesFor(pair.probe.signer.pubkey);
}

describe("welcome refusal (release wasm)", () => {
  test(
    "a refused welcome is unavailable signer_refused and a working signer re-ingests and joins",
    { timeout: 120_000 },
    async () => {
      const pair = await openPair("refused");
      const wraps = await inviteB(pair);
      expect(wraps).toHaveLength(1);

      pair.probe.state.refuse = true;
      const first = await snapshot(pair.b, await pair.b.ingest(wraps), pair.probe.state.calls);
      expect(first.reports).toStrictEqual([
        { event: wraps[0]?.id, outcome: { type: "unavailable", category: "signer_refused" } },
      ]);
      expect(first.events).toStrictEqual([]);
      expect(first.groups).toBe(0);
      expect(first.decryptCalls).toBe(1);

      pair.probe.state.refuse = false;
      const callsBefore = pair.probe.state.calls;
      const second = await snapshot(
        pair.b,
        await pair.b.ingest(wraps),
        pair.probe.state.calls - callsBefore,
      );
      expect(second.reports).toStrictEqual([
        { event: wraps[0]?.id, outcome: { type: "accepted" } },
      ]);
      expect(second.events.some((event) => event.type === "group_joined")).toBe(true);
      expect(second.groups).toBe(1);
      expect(second.decryptCalls).toBeGreaterThan(0);
    },
  );

  test(
    "a refused welcome re-ingests and joins after close and reopen on the same kv",
    {
      timeout: 120_000,
    },
    async () => {
      const pair = await openPair("refused");
      const wraps = await inviteB(pair);
      expect(wraps).toHaveLength(1);

      pair.probe.state.refuse = true;
      const first = await snapshot(pair.b, await pair.b.ingest(wraps), pair.probe.state.calls);
      expect(first.reports).toStrictEqual([
        { event: wraps[0]?.id, outcome: { type: "unavailable", category: "signer_refused" } },
      ]);
      expect(first.groups).toBe(0);
      expect(first.decryptCalls).toBe(1);

      await pair.b.close();
      const reopenedProbe = refusalProbe(testSigner(pair.bSecret), "refused");
      const b2 = await Engine.open({
        backend: await releaseBackend(),
        kv: memoryKv(pair.kvB.rows()),
        clock: manualClock(),
        signer: reopenedProbe.signer,
        allowLoopback: true,
      });
      await b2.drain();
      const second = await snapshot(b2, await b2.ingest(wraps), reopenedProbe.state.calls);
      expect(second.reports).toStrictEqual([
        { event: wraps[0]?.id, outcome: { type: "accepted" } },
      ]);
      expect(second.groups).toBe(1);
      expect(second.decryptCalls).toBeGreaterThan(0);
      await b2.close();
    },
  );

  test(
    "a signer failure is unavailable signer_failed and retries identically",
    { timeout: 120_000 },
    async () => {
      const pair = await openPair("failed");
      const wraps = await inviteB(pair);
      expect(wraps).toHaveLength(1);

      pair.probe.state.refuse = true;
      const first = await snapshot(pair.b, await pair.b.ingest(wraps), pair.probe.state.calls);
      expect(first.reports).toStrictEqual([
        { event: wraps[0]?.id, outcome: { type: "unavailable", category: "signer_failed" } },
      ]);
      expect(first.events).toStrictEqual([]);
      expect(first.groups).toBe(0);
      expect(first.decryptCalls).toBe(1);

      pair.probe.state.refuse = false;
      const callsBefore = pair.probe.state.calls;
      const second = await snapshot(
        pair.b,
        await pair.b.ingest(wraps),
        pair.probe.state.calls - callsBefore,
      );
      expect(second.reports).toStrictEqual([
        { event: wraps[0]?.id, outcome: { type: "accepted" } },
      ]);
      expect(second.groups).toBe(1);
      expect(second.decryptCalls).toBeGreaterThan(0);
    },
  );
});
