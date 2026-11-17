import { randomBytes } from "node:crypto";

import { expect, test } from "@playwright/test";
import { generateSecretKey } from "nostr-tools";

test("runFlow over IndexedDB-backed members", async ({ page }) => {
  test.setTimeout(300_000);
  const key = randomBytes(32).toString("base64");
  const secret = Buffer.from(generateSecretKey()).toString("base64");
  await page.goto("/");
  await page.evaluate(async (a: { k: string; s: string }) => globalThis.__marmot.init(a.k, a.s), {
    k: key,
    s: secret,
  });
  const timings = await page.evaluate(async (messages) => globalThis.__marmot.flow(messages), 500);
  // oxlint-disable-next-line no-console
  console.log(`chromium flow timings: ${JSON.stringify(timings)}`);
  expect(timings.send).toBeGreaterThan(0);
  expect(timings.ingest).toBeGreaterThanOrEqual(0);
});
