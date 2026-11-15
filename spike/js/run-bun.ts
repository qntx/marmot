// Bun driver: `bun spike/js/run-bun.ts`
// Runs the shared scenario over the wasm build in-process, then measures.
import init, { Client } from "../wasm/pkg/marmot_spike_wasm.js";
import { brotliCompressSync, gzipSync, constants as zc } from "node:zlib";
import { execFileSync } from "node:child_process";
import { mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { FakeRelay, KIND_GROUP_MESSAGE } from "./src/relay.js";
import { runScenario } from "./src/scenario.js";
import { accountKeys, LocalMember } from "./src/local-member.js";
import { MapBackend } from "./src/member.js";

const wasmPath = new URL("../wasm/pkg/marmot_spike_wasm_bg.wasm", import.meta.url);
await init({ module_or_path: await Bun.file(wasmPath).arrayBuffer() });

const resultsDir = new URL("../results/", import.meta.url).pathname;
mkdirSync(resultsDir, { recursive: true });

const rows: string[] = [];
const row = (s: string, d: string) => {
  const line = `| ${s.padEnd(58)} | ${d} |`;
  console.log(line);
  rows.push(line);
};
const assert = (c: unknown, m: string) => {
  if (!c) throw new Error(`assert failed: ${m}`);
};

const members = new Map<string, LocalMember>();
const relay = new FakeRelay();
let lastSnapshot: [string, string][] = [];

const env = {
  relay,
  async makeMember(seed: string) {
    const m = await LocalMember.open({
      Client,
      keys: await accountKeys(seed),
      backend: new MapBackend(),
    });
    members.set(seed, m);
    return m;
  },
  async spawnReader(seed: string, entries: [string, string][]) {
    const m = await LocalMember.open({
      Client,
      keys: await accountKeys(seed),
      backend: new MapBackend(entries),
    });
    members.set(`${seed}-reader-${members.size}`, m);
    return m;
  },
  async snapshot(m: LocalMember) {
    lastSnapshot = await (m as LocalMember).backend.loadAll();
    return lastSnapshot;
  },
  assert,
  row,
  time: () => performance.now(),
  async pubKeyOf(m: LocalMember) {
    return (m as LocalMember).keys.publicKey;
  },
};

console.log("=== M0 wasm flow — Bun ===");
const t0 = performance.now();
await runScenario(env as any);
row("scenario total", `${(performance.now() - t0).toFixed(0)}ms`);
const agg: Record<string, { n: number; ms: number }> = {};
for (const m of members.values())
  for (const [k, v] of Object.entries(m.timings)) {
    const a = (agg[k] ??= { n: 0, ms: 0 });
    a.n += v.n;
    a.ms += v.ms;
  }
for (const [k, v] of Object.entries(agg).sort((x, y) => y[1].ms - x[1].ms))
  row(`api ${k}`, `n=${v.n} total=${v.ms.toFixed(0)}ms med=${(v.ms / v.n).toFixed(2)}ms`);

// ---- Measurements ----------------------------------------------------------
const results: Record<string, unknown> = { rows };
const wasmBytes = readFileSync(wasmPath);
const optPath = `${resultsDir}/marmot_spike_wasm_opt.wasm`;
execFileSync(
  new URL("./node_modules/binaryen/bin/wasm-opt", import.meta.url).pathname,
  [
    "-Oz",
    "--enable-mutable-globals",
    "--enable-sign-ext",
    "--enable-nontrapping-float-to-int",
    "--enable-bulk-memory",
    "-o",
    optPath,
    wasmPath.pathname,
  ],
);
const optBytes = readFileSync(optPath);
const glueBytes = readFileSync(
  new URL("../wasm/pkg/marmot_spike_wasm.js", import.meta.url),
);
const sizes = (b: Uint8Array) => ({
  raw: b.length,
  gzip9: gzipSync(b, { level: 9 }).length,
  brotli11: brotliCompressSync(b, {
    params: { [zc.BROTLI_PARAM_QUALITY]: 11 },
  }).length,
});
results.wasmSize = {
  bindgen: sizes(wasmBytes),
  wasmOptOz: sizes(optBytes),
  jsGlue: { raw: glueBytes.length, gzip9: gzipSync(glueBytes, { level: 9 }).length },
};
console.log("wasm sizes:", JSON.stringify(results.wasmSize));

// KeyPackage generation, n = 24
const mKeys = await accountKeys("perf-kp");
const kpMember = await LocalMember.open({ Client, keys: mKeys, backend: new MapBackend() });
const kpTimes: number[] = [];
for (let i = 0; i < 24; i++) {
  const t = performance.now();
  await kpMember.publishKeyPackage();
  kpTimes.push(performance.now() - t);
}
const median = (xs: number[]) => xs.toSorted((a, b) => a - b)[Math.floor(xs.length / 2)];
results.keyPackageMs = { n: kpTimes.length, median: median(kpTimes), times: kpTimes };
console.log(`keyPackage median ${median(kpTimes).toFixed(1)}ms (n=24)`);

// 500-message ascending ingest on a fresh reader opened on B's pre-ingest
// snapshot (captured by env.snapshot at scenario step 3 — lastSnapshot).
const ingestReader = await LocalMember.open({
  Client,
  keys: await accountKeys("bob"),
  backend: new MapBackend(lastSnapshot),
});
const allEvents = relay.fetchAsc(KIND_GROUP_MESSAGE);
const tIngest = performance.now();
let ingested = 0;
for (const ev of allEvents) {
  await ingestReader.ingest(ev);
  ingested++;
}
const ingestTotal = performance.now() - tIngest;
results.ingestMs = {
  events: ingested,
  totalMs: ingestTotal,
  perMessageMs: ingestTotal / ingested,
};
console.log(`ingest ${ingested} events in ${ingestTotal.toFixed(0)}ms`);

// 50-member group: 49 KPs in one create; then add 1.
const big = await LocalMember.open({ Client, keys: await accountKeys("big"), backend: new MapBackend() });
const joiners: LocalMember[] = [];
const bigRelay = new FakeRelay();
bigRelay.tryPublish(await big.publishKeyPackage());
const kpEvents = [];
for (let i = 0; i < 49; i++) {
  const j = await LocalMember.open({ Client, keys: await accountKeys(`j${i}`), backend: new MapBackend() });
  joiners.push(j);
  const kp = await j.publishKeyPackage();
  bigRelay.tryPublish(kp);
  kpEvents.push(kp);
}
let t = performance.now();
const bigCreated = await big.createGroup("m0-50", ["ws://spike-relay.local:7777"], kpEvents);
results.create50Ms = performance.now() - t;
console.log(`create 50-member group: ${results.create50Ms}ms`);
const bigGid = bigCreated.groupId;
const bigJoin = joiners[0];
const w = bigCreated.result.events.filter((e) =>
  e.tags.some((t) => t[0] === "p" && t[1] === joiners[0].keys.publicKey),
);
t = performance.now();
await bigJoin.ingest(w[0]);
results.welcomeJoin50Ms = performance.now() - t;
console.log(`joiner[0] welcome processing: ${results.welcomeJoin50Ms}ms`);

// add 1 member to the 50-member group
const extra = await LocalMember.open({ Client, keys: await accountKeys("extra"), backend: new MapBackend() });
const extraKp = await extra.publishKeyPackage();
t = performance.now();
const addRes = await big.send(bigGid, { type: "invite", keyPackageEvents: [extraKp] });
results.add1CommitMs = performance.now() - t;
console.log(`add-1 commit: ${results.add1CommitMs}ms`);
const addEvent = addRes.events.find((e) => e.kind === KIND_GROUP_MESSAGE)!;
const t2 = performance.now();
await joiners[1].ingest(addEvent);
results.add1IngestMs = performance.now() - t2;
console.log(`existing member ingest of add-1 commit: ${results.add1IngestMs}ms`);

results.memory = {
  rss: process.memoryUsage().rss,
  heapUsed: process.memoryUsage().heapUsed,
  wasmBufferBytes: kpMember.wasmMemoryBytes(),
};
console.log("memory:", JSON.stringify(results.memory));

writeFileSync(`${resultsDir}/bun.json`, JSON.stringify(results, null, 2));
console.log(`wrote ${resultsDir}/bun.json`);
