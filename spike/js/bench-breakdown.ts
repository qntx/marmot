// Timing breakdown: where does scenario time go?
// `bun spike/js/bench-breakdown.ts`
import init, { Client } from "../wasm/pkg/marmot_spike_wasm.js";
import { writeFileSync } from "node:fs";
import { FakeRelay, KIND_GIFT_WRAP, KIND_GROUP_MESSAGE } from "./src/relay.js";
import { accountKeys, LocalMember } from "./src/local-member.js";
import { flushOutbound, MapBackend, publishResult } from "./src/member.js";

const wasmPath = new URL("../wasm/pkg/marmot_spike_wasm_bg.wasm", import.meta.url);
await init({ module_or_path: await Bun.file(wasmPath).arrayBuffer() });

const med = (xs: number[]) => {
  const s = [...xs].sort((a, b) => a - b);
  return s[Math.floor(s.length / 2)];
};
const p95 = (xs: number[]) => {
  const s = [...xs].sort((a, b) => a - b);
  return s[Math.floor(s.length * 0.95)];
};

const relay = new FakeRelay();
const publish = (ev: any) => relay.tryPublish(ev);
const mkMember = async (seed: string, entries?: [string, string][]) =>
  LocalMember.open({
    Client,
    keys: await accountKeys(seed),
    backend: new MapBackend(entries),
  });

const a = await mkMember("a");
const b = await mkMember("b");
const kpA = await a.publishKeyPackage();
const kpB = await b.publishKeyPackage();
relay.tryPublish(kpA);
relay.tryPublish(kpB);

const { groupId: gid, result: created } = await a.createGroup("bench", ["ws://spike-relay.local:7777"], [kpB]);
await publishResult(a, created, publish);
const bPk = b.keys.publicKey;
for (const ev of relay.fetchAsc(KIND_GIFT_WRAP, undefined, bPk)) await b.ingest(ev);
await flushOutbound(b, gid, publish);
await flushOutbound(a, gid, publish);

// 1. per-send timing, 100 app messages
const sendMs: number[] = [];
for (let i = 0; i < 100; i++) {
  const t0 = performance.now();
  const r = await a.send(gid, {
    type: "appMessage",
    content: `m-${i}`,
    createdAt: 1_700_000_000 + i,
  });
  sendMs.push(performance.now() - t0);
  await publishResult(a, r, publish);
}

// 2. per-ingest timing on B (ascending backlog)
const all445 = relay.fetchAsc(KIND_GROUP_MESSAGE);
const hTag = all445[0].tags.find((t) => t[0] === "h")![1];
const backlog = relay.fetchAsc(KIND_GROUP_MESSAGE, hTag);
const ingestMs: number[] = [];
for (const ev of backlog) {
  const t0 = performance.now();
  await b.ingest(ev);
  ingestMs.push(performance.now() - t0);
}

// 3. advanceConvergence pass timing on a settle-heavy step: remove B
const bId = await b.memberId();
const rm = await a.send(gid, { type: "removeMembers", memberIds: [bId] });
for (const ev of rm.events ?? []) relay.tryPublish(ev);
if (rm.pending) await a.confirmPublished(rm.pending);
const passMs: number[] = [];
for (const m of [a, b]) {
  for (let i = 0; i < 16; i++) {
    const t0 = performance.now();
    const pass = await m.advance(gid);
    passMs.push(performance.now() - t0);
    for (const r of pass.results) {
      for (const ev of r.events) relay.tryPublish(ev);
      if (r.pending !== undefined) await m.confirmPublished(r.pending);
    }
    if (pass.settled && pass.pendingGroups === 0 && pass.results.length === 0) break;
  }
  for (const ev of relay.fetchAsc(KIND_GROUP_MESSAGE, hTag)) await m.ingest(ev);
}

const out = {
  n: { sends: sendMs.length, ingests: ingestMs.length, advancePasses: passMs.length },
  sendMs: { median: +med(sendMs).toFixed(3), p95: +p95(sendMs).toFixed(3), max: +Math.max(...sendMs).toFixed(3) },
  ingestMs: { median: +med(ingestMs).toFixed(3), p95: +p95(ingestMs).toFixed(3), max: +Math.max(...ingestMs).toFixed(3) },
  advancePassMs: { median: +med(passMs).toFixed(3), p95: +p95(passMs).toFixed(3), max: +Math.max(...passMs).toFixed(3), total: +passMs.reduce((s, x) => s + x, 0).toFixed(1) },
  wasmMemoryBytes: a.wasmMemoryBytes(),
};
console.log(JSON.stringify(out, null, 2));
writeFileSync(new URL("../results/bench-breakdown.json", import.meta.url).pathname, JSON.stringify(out, null, 2));
