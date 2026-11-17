import { randomBytes } from "node:crypto";
import { mkdirSync, writeFileSync } from "node:fs";
import { fileURLToPath } from "node:url";

import { expect, test } from "@playwright/test";
import { generateSecretKey } from "nostr-tools";

const key = (): string => randomBytes(32).toString("base64");
const secret = (): string => Buffer.from(generateSecretKey()).toString("base64");

function median(values: number[]): number {
  const sorted = values.toSorted((a, b) => a - b);
  return sorted[Math.floor(sorted.length / 2)] ?? 0;
}

type PerfOut = {
  coldLoad?: Array<{ totalMs: number; fetchMs: number; instantiateMs: number }>;
  reopen10Groups?: { samples: number[]; medianMs: number };
  ingest500?: { samples: number[]; medianMs: number };
  memory10k?: Record<string, unknown>;
};

test("chromium perf: cold load, reopen, ingest, memory", async ({ browser }) => {
  test.setTimeout(1_200_000);
  const out: PerfOut = {};

  // ---- cold load(): fresh context + navigation, split fetch / instantiate ----
  const cold: Array<{ totalMs: number; fetchMs: number; instantiateMs: number }> = [];
  for (let i = 0; i < 3; i += 1) {
    // oxlint-disable-next-line no-await-in-loop
    const context = await browser.newContext();
    // oxlint-disable-next-line no-await-in-loop
    const page = await context.newPage();
    const started = Date.now();
    // oxlint-disable-next-line no-await-in-loop
    await page.goto("/");
    // oxlint-disable-next-line no-await-in-loop
    const split = await page.evaluate(
      async (a: { k: string; s: string }) => globalThis.__marmot.init(a.k, a.s),
      { k: key(), s: secret() },
    );
    cold.push({ totalMs: Date.now() - started, ...split });
    // oxlint-disable-next-line no-await-in-loop
    await context.close();
  }
  out.coldLoad = cold;

  // ---- Engine.open on an account holding 10 groups: median of 5 reopens ----
  const context = await browser.newContext();
  const page = await context.newPage();
  await page.goto("/");
  await page.evaluate(async (a: { k: string; s: string }) => globalThis.__marmot.init(a.k, a.s), {
    k: key(),
    s: secret(),
  });
  await page.evaluate(async (name) => globalThis.__marmot.openEngine(name), "perf-10g");
  for (let g = 0; g < 10; g += 1) {
    // oxlint-disable-next-line no-await-in-loop
    await page.evaluate(async (name) => globalThis.__marmot.createGroup(name), `g${g}`);
  }
  await page.evaluate(async () => globalThis.__marmot.closeEngine());
  const reopenMs: number[] = [];
  for (let i = 0; i < 5; i += 1) {
    // oxlint-disable-next-line no-await-in-loop
    const ms = await page.evaluate(async (name) => {
      const t0 = performance.now();
      await globalThis.__marmot.openEngine(name);
      const elapsed = performance.now() - t0;
      await globalThis.__marmot.closeEngine();
      return elapsed;
    }, "perf-10g");
    reopenMs.push(ms);
  }
  out.reopen10Groups = { samples: reopenMs, medianMs: median(reopenMs) };

  // ---- ingest of 500 messages in one call: median of 3, must be < 1000 ms ----
  const ingestMs: number[] = [];
  for (let i = 0; i < 3; i += 1) {
    // oxlint-disable-next-line no-await-in-loop
    const result = await page.evaluate(
      async (a: { db: string; g: number; m: number }) =>
        globalThis.__marmot.receiveScenario(a.db, a.g, a.m),
      { db: `perf-ingest-${i}`, g: 1, m: 500 },
    );
    ingestMs.push(result.ingestMs[0] ?? 0);
  }
  out.ingest500 = { samples: ingestMs, medianMs: median(ingestMs) };
  expect(median(ingestMs)).toBeLessThan(1000);

  // ---- memory after 10,000 received messages (20 groups x 500, batched 500) ----
  const memoryStarted = Date.now();
  const received = await page.evaluate(
    async (a: { db: string; g: number; m: number }) =>
      globalThis.__marmot.receiveScenario(a.db, a.g, a.m),
    { db: "perf-mem", g: 20, m: 500 },
  );
  const stats = await page.evaluate(async (name) => globalThis.__marmot.dbStats(name), "perf-mem");
  const uaMemory = await page.evaluate(async () => globalThis.__marmot.memory());
  const cdp = await context.newCDPSession(page);
  await cdp.send("Performance.enable");
  const { metrics } = await cdp.send("Performance.getMetrics");
  const heap = metrics.find((metric) => metric.name === "JSHeapUsedSize")?.value;
  out.memory10k = {
    wallMs: Date.now() - memoryStarted,
    pageWallMs: received.wallMs,
    idbRows: stats.rows,
    idbBytes: stats.bytes,
    jsHeapUsedBytes: heap,
    uaMemoryBytes: uaMemory.uaMemoryBytes,
  };
  await context.close();

  const dir = fileURLToPath(new URL("../../../target", import.meta.url));
  mkdirSync(dir, { recursive: true });
  const path = fileURLToPath(new URL("../../../target/perf-chromium.json", import.meta.url));
  writeFileSync(path, `${JSON.stringify(out, null, 2)}\n`);
  // oxlint-disable-next-line no-console
  console.log(`perf-chromium: ${JSON.stringify(out)}`);
});
