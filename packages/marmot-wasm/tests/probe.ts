/**
 * Offline flow-cost instrumentation: times every bridge `open`/`call` by request type, the Engine
 * methods behind the host send loop (`sendMessage`, `whenDurable`, `takeOutbound`,
 * `reportPublish`), and splits `Engine.open` into `kv.load` / `encodeRows` / bridge `open`. Run
 * with `bun tests/probe.ts` — not part of any suite.
 */

import { Engine, encodeRows, utf8Decode } from "@qntx/marmot/backend";
import type { Backend, BridgeHandle, BridgeHost, Rows } from "@qntx/marmot/backend";
import type { KvDriver } from "@qntx/marmot/storage";
import { fakeRelay, manualClock, memoryKv } from "@qntx/marmot/testing";
import type { FakeRelay } from "@qntx/marmot/testing";

import { releaseBackend } from "./shared/env.ts";
import { testSigner } from "./signer.ts";

const RELAY = "wss://relay.example";
const MESSAGES = 500;
const CHECKPOINTS = [0, 99, 249, 499];

function out(line: string): void {
  // oxlint-disable-next-line no-console -- probe output
  console.log(line);
}

type Entry = { label: string; ms: number; tag: number };
const entries: Entry[] = [];
let tag = -1;

function rec<T>(label: string, body: () => T): T {
  const started = performance.now();
  const out = body();
  entries.push({ label, ms: performance.now() - started, tag });
  return out;
}

async function recA<T>(label: string, body: () => Promise<T>): Promise<T> {
  const started = performance.now();
  const out = await body();
  entries.push({ label, ms: performance.now() - started, tag });
  return out;
}

function requestType(request: Uint8Array): string {
  const parsed: unknown = JSON.parse(utf8Decode(request));
  if (typeof parsed === "object" && parsed !== null && "request" in parsed) {
    const envelope = parsed.request;
    if (typeof envelope === "object" && envelope !== null && "type" in envelope) {
      const { type } = envelope;
      if (typeof type === "string") {
        return type;
      }
    }
  }
  return "?";
}

function timedBackend(inner: Backend): Backend {
  return {
    name: `${inner.name}+probe`,
    create(host: BridgeHost): BridgeHandle {
      const handle = inner.create(host);
      return {
        open: (config) => rec("bridge:open", () => handle.open(config)),
        call: (request) => rec(`bridge:call:${requestType(request)}`, () => handle.call(request)),
        drain: () => handle.drain(),
        close: () => handle.close(),
      };
    },
  };
}

function timedKv(inner: KvDriver, onRows?: (rows: Rows) => void): KvDriver {
  return {
    load: async () => {
      const rows = await recA("kv.load", async () => inner.load());
      onRows?.(rows);
      return rows;
    },
    commit: async (batch) => inner.commit(batch),
    close: async () => inner.close(),
  };
}

async function publish(engine: Engine, relay: FakeRelay): Promise<void> {
  for (let round = 0; round < 64; round += 1) {
    // oxlint-disable-next-line no-await-in-loop
    await recA("engine.whenDurable", async () => engine.whenDurable());
    // oxlint-disable-next-line no-await-in-loop
    const items = await recA("engine.takeOutbound", async () => engine.takeOutbound());
    if (items.length === 0) {
      return;
    }
    for (const item of items) {
      const results = relay.publish(item);
      // oxlint-disable-next-line no-await-in-loop
      await recA("engine.reportPublish", async () => engine.reportPublish(item.id, results));
    }
  }
  throw new Error("publish did not settle");
}

function summarize(prefix: string, at: number | undefined): string {
  const sums = new Map<string, { n: number; ms: number }>();
  for (const entry of entries) {
    if (at !== undefined && entry.tag !== at) {
      continue;
    }
    const acc = sums.get(entry.label) ?? { n: 0, ms: 0 };
    acc.n += 1;
    acc.ms += entry.ms;
    sums.set(entry.label, acc);
  }
  const lines = [...sums.entries()]
    .toSorted(([a], [b]) => a.localeCompare(b))
    .map(
      ([label, { n, ms }]) =>
        `  ${label.padEnd(34)} n=${String(n).padStart(4)} ${ms.toFixed(2)} ms`,
    );
  return `${prefix}\n${lines.join("\n")}`;
}

const backend = timedBackend(await releaseBackend());
const relay = fakeRelay();
const kvA = memoryKv();
const kvB = memoryKv();

async function openMember(name: string, kv: KvDriver, signer = testSigner()): Promise<Engine> {
  return Engine.open({
    backend,
    kv,
    clock: manualClock(),
    signer,
    allowLoopback: true,
    clientName: name,
  });
}

tag = -2;
const signerA = testSigner();
const signerB = testSigner();
let rowsA: Rows = [];
const engineA = await openMember(
  "a",
  timedKv(kvA, (rows) => {
    rowsA = rows;
  }),
  signerA,
);
const engineB = await openMember("b", timedKv(kvB), signerB);
await engineA.publishKeyPackage([RELAY]);
await publish(engineA, relay);
await engineA.publishInboxRelays([RELAY]);
await publish(engineA, relay);
await engineB.publishKeyPackage([RELAY]);
await publish(engineB, relay);
await engineB.publishInboxRelays([RELAY]);
await publish(engineB, relay);

const group = await engineA.createGroup({
  name: "probe",
  description: "",
  relays: [RELAY],
  invitees: [{ keyPackage: relay.keyPackageOf(signerB.pubkey), inboxRelays: [RELAY] }],
});
await publish(engineA, relay);
await engineB.ingest(relay.welcomesFor(signerB.pubkey));
await publish(engineB, relay);
const snapshot = await engineA.group(group.id);
const route = snapshot?.nostrGroupId;
if (route === undefined) {
  throw new Error("route missing");
}
await engineA.ingest(relay.groupEvents(route, "a"));
await engineA.drain();
out(summarize("setup (open + identity + create + join)", -2));

for (let i = 0; i < MESSAGES; i += 1) {
  tag = i;
  // oxlint-disable-next-line no-await-in-loop
  await recA("engine.sendMessage", async () =>
    engineA.sendMessage(group.id, { kind: 9, tags: [], content: `m${i}` }),
  );
  // oxlint-disable-next-line no-await-in-loop
  await publish(engineA, relay);
}

for (const checkpoint of CHECKPOINTS) {
  out(summarize(`send iteration ${checkpoint + 1}`, checkpoint));
}
const sendOnly = new Map<string, { n: number; ms: number }>();
for (const entry of entries) {
  if (entry.tag < 0 || entry.tag >= MESSAGES) {
    continue;
  }
  const acc = sendOnly.get(entry.label) ?? { n: 0, ms: 0 };
  acc.n += 1;
  acc.ms += entry.ms;
  sendOnly.set(entry.label, acc);
}
out(
  `send stage totals (all 500 iterations)\n${[...sendOnly.entries()]
    .toSorted(([a], [b]) => a.localeCompare(b))
    .map(
      ([label, { n, ms }]) =>
        `  ${label.padEnd(34)} n=${String(n).padStart(4)} ${ms.toFixed(2)} ms (avg ${(ms / n).toFixed(3)})`,
    )
    .join("\n")}`,
);

tag = -3;
await engineA.close();
const reopenStart = performance.now();
const reopened = await openMember(
  "a",
  timedKv(kvA, (rows) => {
    rowsA = rows;
  }),
  signerA,
);
const reopenMs = performance.now() - reopenStart;
rec("encodeRows:extra", () => encodeRows(rowsA));
const encode = entries.at(-1);
out(`reopen: total=${reopenMs.toFixed(2)} ms`);
out(summarize("reopen calls", -3));
out(`encodeRows of ${rowsA.length} rows: ${encode?.ms.toFixed(2) ?? "?"} ms`);
await reopened.close();
await engineB.close();
