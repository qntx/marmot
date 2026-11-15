// Send/ingest breakdown: where do the ~97 s go?
// `bun spike/js/bench.ts` — 2 members, 200 A→B app messages.
import init, { Client } from "../wasm/pkg/marmot_spike_wasm.js";
import { writeFileSync, mkdirSync } from "node:fs";
import { FakeRelay, KIND_GIFT_WRAP, KIND_GROUP_MESSAGE } from "./src/relay.js";
import { accountKeys, LocalMember } from "./src/local-member.js";
import { MapBackend, flushOutbound, publishResult } from "./src/member.js";

await init({ module_or_path: await Bun.file(new URL("../wasm/pkg/marmot_spike_wasm_bg.wasm", import.meta.url)).arrayBuffer() });

const relay = new FakeRelay();
const pub = (e: any) => relay.tryPublish(e);
const a = await LocalMember.open({ Client, keys: await accountKeys("alice"), backend: new MapBackend() });
const b = await LocalMember.open({ Client, keys: await accountKeys("bob"), backend: new MapBackend() });
const bKp = await b.publishKeyPackage();
const created = await a.createGroup("bench", ["ws://x"], [bKp]);
const gid = created.groupId;
await publishResult(a, created.result, pub);
await b.ingest(relay.fetchAsc(KIND_GIFT_WRAP, undefined, (await accountKeys("bob")).publicKey)[0]);
await a.drainEvents(); await b.drainEvents();

const N = 200;
const sendMs: number[] = [], ingestMs: number[] = [], flushMs: number[] = [];
let sendResultEvents = 0, flushPasses = 0, flushPublished = 0;

const t0 = performance.now();
for (let i = 0; i < N; i++) {
  const t = performance.now();
  const r = await a.send(gid, { type: "appMessage", content: `bench-${i}`, createdAt: 1700000000 + i });
  sendMs.push(performance.now() - t);
  sendResultEvents += r.events.length;
  if (r.pending !== undefined) await a.confirmPublished(r.pending);
  for (const ev of r.events) pub(ev);
}
const sendTotal = performance.now() - t0;

// One flush after all sends — does it do work?
const tF = performance.now();
flushPublished = await flushOutbound(a, gid, pub);
flushMs.push(performance.now() - tF);
flushPasses++;

const events = relay.fetchAsc(KIND_GROUP_MESSAGE);
const t1 = performance.now();
for (const ev of events) {
  const t = performance.now();
  await b.ingest(ev);
  ingestMs.push(performance.now() - t);
}
const ingestTotal = performance.now() - t1;

const tF2 = performance.now();
await flushOutbound(b, gid, pub);
const flushIngestTotal = performance.now() - tF2;

const q = (xs: number[], p: number) => xs.toSorted((x, y) => x - y)[Math.min(xs.length - 1, Math.floor(xs.length * p))];
const out = {
  n: N,
  send: { total: sendTotal, median: q(sendMs, 0.5), p95: q(sendMs, 0.95), max: Math.max(...sendMs) },
  ingest: { events: events.length, total: ingestTotal, median: q(ingestMs, 0.5), p95: q(ingestMs, 0.95), max: Math.max(...ingestMs) },
  flushAfterSends: { ms: flushMs[0], eventsPublished: flushPublished },
  flushAfterIngest: { ms: flushIngestTotal },
  sendResultEvents,
};
console.log(JSON.stringify(out, null, 2));
mkdirSync(new URL("../results", import.meta.url).pathname, { recursive: true });
writeFileSync(new URL("../results/bench-bun.json", import.meta.url).pathname, JSON.stringify(out, null, 2));
