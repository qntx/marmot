import { randomBytes } from "node:crypto";

import { expect, test } from "@playwright/test";
import { generateSecretKey } from "nostr-tools";

const key = (): string => randomBytes(32).toString("base64");
const secret = (): string => Buffer.from(generateSecretKey()).toString("base64");

test("two tabs on one database: the Web Lock serializes ownership", async ({ browser }) => {
  test.setTimeout(120_000);
  const context = await browser.newContext();
  const tab1 = await context.newPage();
  const tab2 = await context.newPage();

  const db = "locks-db";
  await tab1.goto("/");
  await tab1.evaluate(async (a: { k: string; s: string }) => globalThis.__marmot.init(a.k, a.s), {
    k: key(),
    s: secret(),
  });
  await tab2.goto("/");
  await tab2.evaluate(async (a: { k: string; s: string }) => globalThis.__marmot.init(a.k, a.s), {
    k: key(),
    s: secret(),
  });

  // Tab 1 owns the database; tab 2's open is refused with `locked`.
  await tab1.evaluate(async (name) => globalThis.__marmot.openEngine(name), db);
  const groupId = await tab1.evaluate(
    async (name) => globalThis.__marmot.createGroup(name),
    "tabs",
  );
  expect(await tab2.evaluate(async (name) => globalThis.__marmot.kvOpen(name), db)).toBe("locked");

  // Tab 1's engine is untouched: still sends and still reopens.
  await tab1.evaluate(
    async (a: { g: string; t: string }) => globalThis.__marmot.sendOnce(a.g, a.t),
    {
      g: groupId,
      t: "still-alive",
    },
  );
  await tab1.evaluate(async (name) => globalThis.__marmot.reopenEngine(name), db);
  await tab1.evaluate(
    async (a: { g: string; t: string }) => globalThis.__marmot.sendOnce(a.g, a.t),
    {
      g: groupId,
      t: "after-reopen",
    },
  );

  // After tab 1 closes its driver the lock frees; tab 2 opens.
  await tab1.evaluate(async () => globalThis.__marmot.closeEngine());
  expect(await tab2.evaluate(async (name) => globalThis.__marmot.kvOpen(name), db)).toBe("ok");

  // A page that dies without closing releases its Web Locks — tab 2 can take over.
  await tab1.evaluate(async (name) => globalThis.__marmot.openEngine(name), db);
  await tab1.close();
  await expect
    .poll(async () => tab2.evaluate(async (name) => globalThis.__marmot.kvOpen(name), db), {
      timeout: 10_000,
    })
    .toBe("ok");

  await tab2.close();
  await context.close();
});
