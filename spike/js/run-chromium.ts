// Chromium driver: `bun spike/js/run-chromium.ts`
// Serves spike/js statically, runs the shared scenario against real headless
// Chromium via Playwright. B lives on its own page with an IndexedDB KV host;
// B's mid-ingest restart is a real page.reload() in the same context.

import { chromium } from "playwright";
import { mkdirSync, writeFileSync, statSync, existsSync } from "node:fs";
import { join } from "node:path";
import { FakeRelay, KIND_GROUP_MESSAGE } from "./src/relay.js";
import { runScenario } from "./src/scenario.js";
import type { MemberDriver } from "./src/member.js";

const root = new URL(".", import.meta.url).pathname;
const resultsDir = join(root, "..", "results");
mkdirSync(resultsDir, { recursive: true });

// Copy the generated wasm pkg and bundle the page host (Bun resolves the
// @qntx/nostr imports; the wasm glue stays external and is served as-is).
import { cpSync } from "node:fs";
import { execFileSync } from "node:child_process";
cpSync(join(root, "..", "wasm", "pkg"), join(root, "pkg"), { recursive: true });
execFileSync("bun", [
  "build",
  "web/member-host.js",
  "--outdir",
  "web/dist",
  "--target",
  "browser",
  "--external",
  "/pkg/*",
], { cwd: root });

const MIME: Record<string, string> = {
  ".html": "text/html",
  ".js": "text/javascript",
  ".mjs": "text/javascript",
  ".wasm": "application/wasm",
  ".json": "application/json",
};

const server = Bun.serve({
  port: 0,
  async fetch(req) {
    const url = new URL(req.url);
    let path = url.pathname === "/" ? "/web/page.html" : url.pathname;
    if (path.startsWith("/nostr/")) {
      path = `/node_modules/@qntx/nostr/dist${path.slice("/nostr".length)}`;
    }
    const file = join(root, path);
    if (!file.startsWith(root) || !existsSync(file) || statSync(file).isDirectory()) {
      return new Response("not found", { status: 404 });
    }
    const ext = path.slice(path.lastIndexOf("."));
    return new Response(Bun.file(file), {
      headers: { "content-type": MIME[ext] ?? "application/octet-stream" },
    });
  },
});
const base = `http://127.0.0.1:${server.port}`;

// MemberDriver backed by window.__member.call(name, method, arg) in a page.
class PageDriver implements MemberDriver {
  constructor(
    private page: any,
    private name: string,
  ) {}

  private call(method: string, arg?: unknown) {
    return this.page.evaluate(
      ([n, m, a]: [string, string, unknown]) => (window as any).__member.call(n, m, a),
      [this.name, method, arg] as const,
    );
  }

  async restart(): Promise<void> {
    // Real reload: the page re-inits wasm and re-opens B's client from IDB.
    await this.page.reload({ waitUntil: "load" });
    await this.page.waitForFunction(() => (window as any).__member?.ready);
  }

  async publishKeyPackage() {
    return this.call("publishKeyPackage");
  }
  async createGroup(name: string, relays: string[], keyPackageEvents: unknown[]) {
    return this.call("createGroup", { groupName: name, relays, keyPackageEvents });
  }
  async send(groupId: string, intent: unknown) {
    return this.call("send", { groupId, intent });
  }
  async ingest(ev: unknown) {
    return this.call("ingest", ev);
  }
  async advance(groupId: string) {
    return this.call("advance", groupId);
  }
  async confirmPublished(pending: unknown) {
    return this.call("confirmPublished", pending);
  }
  async publishFailed(pending: unknown) {
    return this.call("publishFailed", pending);
  }
  async drainEvents() {
    return this.call("drainEvents");
  }
  async members(groupId: string) {
    return this.call("members", groupId);
  }
  async epoch(groupId: string) {
    return this.call("epoch", groupId);
  }
  async memberId() {
    return this.call("memberId");
  }
}

const rows: string[] = [];
const row = (s: string, d: string) => {
  const line = `| ${s.padEnd(58)} | ${d} |`;
  console.log(line);
  rows.push(line);
};
const assert = (c: unknown, m: string) => {
  if (!c) throw new Error(`assert failed: ${m}`);
};

const browser = await chromium.launch({ headless: true });
const context = await browser.newContext();
const pageMain = await context.newPage();
const pageB = await context.newPage();
pageMain.on("console", (m: any) => {
  if (m.type() === "error") console.error("[main]", m.text());
});
pageB.on("console", (m: any) => {
  if (m.type() === "error") console.error("[b]", m.text());
});
await pageMain.goto(`${base}/web/page.html?slot=main`);
await pageB.goto(`${base}/web/page.html?slot=b`);
for (const p of [pageMain, pageB]) {
  await p.waitForFunction(() => (window as any).__member?.ready);
}

// A and C run on pageMain, B on pageB (IDB backend, reloadable).
async function openOn(page: any, name: string, seed: string, entries?: [string, string][]) {
  await page.evaluate(
    ([n, s, e]) => (window as any).__member.call(n, "open", { seed: s, entries: e }),
    [name, seed, entries],
  );
}
const drivers = new Map<string, PageDriver>();

const relay = new FakeRelay();
let lastSnapshot: [string, string][] = [];

const env = {
  relay,
  async makeMember(seed: string) {
    const page = seed === "bob" ? pageB : pageMain;
    await openOn(page, seed, seed);
    const d = new PageDriver(page, seed);
    drivers.set(seed, d);
    return d;
  },
  async spawnReader(seed: string, entries: [string, string][]) {
    // Second in-memory client on the B page over the entries snapshot.
    const name = `${seed}-reader`;
    await openOn(pageB, name, seed, entries);
    return new PageDriver(pageB, name);
  },
  async snapshot(m: PageDriver) {
    lastSnapshot = await (m as any).page.evaluate(
      (n) => (window as any).__member.call(n, "entries"),
      (m as any).name,
    );
    return lastSnapshot;
  },
  assert,
  row,
  time: () => performance.now(),
  async pubKeyOf(m: PageDriver) {
    return (m as any).page.evaluate(
      (n) => (window as any).__member.call(n, "pubkey"),
      (m as any).name,
    );
  },
};

console.log("=== M0 wasm flow — Chromium (real headless, IDB host) ===");
const t0 = performance.now();
await runScenario(env as any);
row("scenario total", `${(performance.now() - t0).toFixed(0)}ms`);

// ---- Measurements ----------------------------------------------------------
const results: Record<string, unknown> = { rows };

// Clean 500-message ascending ingest on B's pre-ingest snapshot — measured
// INSIDE the page in one evaluate (comparable to Bun's in-process number).
const allEvents = relay.fetchAsc(KIND_GROUP_MESSAGE);
await openOn(pageB, "ingestReader", "bob", lastSnapshot);
const inPage = await pageB.evaluate(async (events) => {
  const times: number[] = [];
  const t0 = performance.now();
  for (const ev of events) {
    const t = performance.now();
    await (window as any).__member.call("ingestReader", "ingest", ev);
    times.push(performance.now() - t);
  }
  return { totalMs: performance.now() - t0, times };
}, allEvents);
const sorted = [...inPage.times].sort((x, y) => x - y);
results.ingestMs = {
  events: allEvents.length,
  totalMs: inPage.totalMs,
  perMessageMs: inPage.totalMs / allEvents.length,
  medianMs: sorted[Math.floor(sorted.length / 2)],
  p95Ms: sorted[Math.floor(sorted.length * 0.95)],
  note: "in-page, single evaluate",
};
console.log(
  `chromium ingest ${allEvents.length} events in ${inPage.totalMs.toFixed(0)}ms (in-page)`,
);

// KeyPackage generation n=24 (inside the page, single evaluate for clean ms).
const kpRes = await pageB.evaluate(async () => {
  const times: number[] = [];
  for (let i = 0; i < 24; i++) {
    const t = performance.now();
    await (window as any).__member.call("perf-kp", "publishKeyPackage");
    times.push(performance.now() - t);
  }
  return times;
}).catch(async () => {
  await openOn(pageB, "perf-kp", "perf-kp");
  return pageB.evaluate(async () => {
    const times: number[] = [];
    for (let i = 0; i < 24; i++) {
      const t = performance.now();
      await (window as any).__member.call("perf-kp", "publishKeyPackage");
      times.push(performance.now() - t);
    }
    return times;
  });
});
const median = (xs: number[]) => xs.sort((a, b) => a - b)[Math.floor(xs.length / 2)];
results.keyPackageMs = { n: kpRes.length, median: median(kpRes), times: kpRes };
console.log(`chromium keyPackage median ${median(kpRes).toFixed(1)}ms (n=24)`);

// 50-member group + add-1, measured entirely inside the page (one evaluate,
// in-memory backends — the IDB path is already proven by B's restart).
results.group50 = await pageB.evaluate(async () => {
  const M = (window as any).__member;
  const out: Record<string, number> = {};
  await M.call("big", "open", { seed: "big", entries: [] });
  const joiners: string[] = [];
  const kps: unknown[] = [];
  for (let i = 0; i < 49; i++) {
    const name = `j${i}`;
    await M.call(name, "open", { seed: name, entries: [] });
    joiners.push(name);
    kps.push(await M.call(name, "publishKeyPackage"));
  }
  let t = performance.now();
  const created = await M.call("big", "createGroup", {
    groupName: "m0-50",
    relays: ["ws://spike-relay.local:7777"],
    keyPackageEvents: kps,
  });
  out.create50Ms = performance.now() - t;
  const gid = created.groupId;
  const j0pk = await M.call(joiners[0], "pubkey");
  const welcome = created.result.events.find((e: any) =>
    e.tags.some((tg: string[]) => tg[0] === "p" && tg[1] === j0pk),
  );
  t = performance.now();
  await M.call(joiners[0], "ingest", welcome);
  out.welcomeJoin50Ms = performance.now() - t;

  await M.call("extra", "open", { seed: "extra", entries: [] });
  const extraKp = await M.call("extra", "publishKeyPackage");
  t = performance.now();
  const add = await M.call("big", "send", {
    groupId: gid,
    intent: { type: "invite", keyPackageEvents: [extraKp] },
  });
  out.add1CommitMs = performance.now() - t;
  const addEv = add.events.find((e: any) => e.kind === 445);
  t = performance.now();
  await M.call(joiners[1], "ingest", addEv);
  out.add1IngestMs = performance.now() - t;
  return out;
});
console.log("chromium 50-member:", JSON.stringify(results.group50));

// Memory via CDP.
const session = await context.newCDPSession(pageB);
await session.send("Performance.enable");
const metrics = await session.send("Performance.getMetrics");
const heap = metrics.metrics.find((m: any) => m.name === "JSHeapUsedSize")?.value;
results.memory = {
  JSHeapUsedSize: heap,
  wasmBufferBytes: await pageB.evaluate(() =>
    (window as any).__member.call("bob", "wasmMemoryBytes"),
  ),
};
console.log("chromium memory:", JSON.stringify(results.memory));

writeFileSync(join(resultsDir, "chromium.json"), JSON.stringify(results, null, 2));
console.log(`wrote ${resultsDir}/chromium.json`);

await browser.close();
server.stop();
