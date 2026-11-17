import { randomBytes } from "node:crypto";
import { mkdirSync, writeFileSync } from "node:fs";
import { fileURLToPath } from "node:url";

import { test } from "@playwright/test";
import { generateSecretKey } from "nostr-tools";

const key = (): string => randomBytes(32).toString("base64");
const secret = (): string => Buffer.from(generateSecretKey()).toString("base64");

test("chromium memory: 10,000 received messages", async ({ playwright }) => {
  test.setTimeout(1_200_000);
  // The headless shell lacks the memory-measurement backend; the full chromium
  // build reports `measureUserAgentSpecificMemory` (requires COOP/COEP isolation).
  const browser = await playwright.chromium.launch({ channel: "chromium" });
  const context = await browser.newContext();
  const page = await context.newPage();
  await page.goto("/");
  await page.evaluate(async (a: { k: string; s: string }) => globalThis.__marmot.init(a.k, a.s), {
    k: key(),
    s: secret(),
  });

  // 20 groups x 500 messages, ingested in batches of 500.
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
  const out = {
    memory10k: {
      wallMs: Date.now() - memoryStarted,
      pageWallMs: received.wallMs,
      idbRows: stats.rows,
      idbBytes: stats.bytes,
      jsHeapUsedBytes: heap,
      uaMemoryBytes: uaMemory.uaMemoryBytes,
      uaMemoryBreakdown: uaMemory.breakdown,
    },
  };
  await context.close();
  await browser.close();

  const dir = fileURLToPath(new URL("../../../target", import.meta.url));
  mkdirSync(dir, { recursive: true });
  const path = fileURLToPath(new URL("../../../target/perf-chromium-memory.json", import.meta.url));
  writeFileSync(path, `${JSON.stringify(out, null, 2)}\n`);
  // oxlint-disable-next-line no-console
  console.log(`perf-chromium-memory: ${JSON.stringify(out)}`);
});
