import { randomBytes } from "node:crypto";
import { writeFileSync } from "node:fs";
import { fileURLToPath } from "node:url";

import { expect, test } from "@playwright/test";
import type { Page } from "@playwright/test";
import { generateSecretKey, getPublicKey } from "nostr-tools";

import { serveFakeRelay } from "@qntx/nostr/testing";

async function sendBoth(pageA: Page, pageB: Page, group: string, i: number): Promise<void> {
  await Promise.all([
    pageA.evaluate(async (a) => globalThis.__marmotClient.send(a.g, a.t), {
      g: group,
      t: `a>${i}`,
    }),
    pageB.evaluate(async (a) => globalThis.__marmotClient.send(a.g, a.t), {
      g: group,
      t: `b>${i}`,
    }),
  ]);
}

async function transcript(page: Page): Promise<string[]> {
  return page.evaluate(() => globalThis.__marmotClient.transcript());
}

async function waitForTranscript(page: Page, expected: Set<string>, what: string): Promise<void> {
  const deadline = Date.now() + 120_000;
  while (Date.now() < deadline) {
    // oxlint-disable-next-line no-await-in-loop -- polling by design
    const got = new Set(await transcript(page));
    if ([...expected].every((text) => got.has(text))) {
      return;
    }
    // oxlint-disable-next-line no-await-in-loop -- polling by design
    await new Promise((resolve) => {
      setTimeout(resolve, 100);
    });
  }
  const got = new Set(await transcript(page));
  const missing = [...expected].filter((text) => !got.has(text));
  throw new Error(
    `transcript did not cover ${what}; missing ${missing.length}: ${missing.slice(0, 12).join(",")}; had ${got.size}`,
  );
}

test("two browser clients exchange over a real relay, B reloads mid-conversation", async ({
  browser,
}) => {
  test.setTimeout(300_000);
  const served = await serveFakeRelay({ host: "127.0.0.1", port: 0 });
  const { url } = served;
  const secretA = generateSecretKey();
  const secretB = generateSecretKey();
  const keyA = randomBytes(32);
  const keyB = randomBytes(32);
  const contextA = await browser.newContext();
  const contextB = await browser.newContext();
  try {
    const pageA = await contextA.newPage();
    const pageB = await contextB.newPage();
    for (const [tag, page] of [
      ["A", pageA],
      ["B", pageB],
    ] as const) {
      page.on("pageerror", (error) => {
        // oxlint-disable-next-line no-console -- page crashes are test-relevant output
        console.log(`[${tag}] pageerror: ${error.message}`);
      });
    }
    await pageA.goto("/client");
    await pageB.goto("/client");
    await pageA.evaluate(
      async (a: { k: string; s: string }) => globalThis.__marmotClient.init(a.k, a.s),
      {
        k: keyA.toString("base64"),
        s: Buffer.from(secretA).toString("base64"),
      },
    );
    await pageB.evaluate(
      async (a: { k: string; s: string }) => globalThis.__marmotClient.init(a.k, a.s),
      {
        k: keyB.toString("base64"),
        s: Buffer.from(secretB).toString("base64"),
      },
    );
    await Promise.all([
      pageA.evaluate(async (u: string) => globalThis.__marmotClient.open(u), url),
      pageB.evaluate(async (u: string) => globalThis.__marmotClient.open(u), url),
    ]);
    await Promise.all([
      pageA.evaluate(async () => globalThis.__marmotClient.identity()),
      pageB.evaluate(async () => globalThis.__marmotClient.identity()),
    ]);
    const group = await pageA.evaluate(
      async (a: { url: string; invitee: string }) =>
        globalThis.__marmotClient.createGroup("browser", [a.url], a.invitee, [a.url]),
      { url, invitee: getPublicKey(secretB) },
    );
    // B joins (the wrap lands over the real socket), then 15 messages each way.
    const joinedDeadline = Date.now() + 60_000;
    for (;;) {
      // oxlint-disable-next-line no-await-in-loop -- polling by design
      const seen = await pageB.evaluate(() => globalThis.__marmotClient.invitesSeen());
      if (seen.includes("joined") || Date.now() > joinedDeadline) {
        break;
      }
      // oxlint-disable-next-line no-await-in-loop -- polling by design
      await new Promise((resolve) => {
        setTimeout(resolve, 100);
      });
    }
    for (let i = 0; i < 15; i += 1) {
      // oxlint-disable-next-line no-await-in-loop -- the exchange is sequential
      await sendBoth(pageA, pageB, group, i);
    }
    // B reloads immediately — mid-conversation, publishes possibly in flight. The outbound ids
    // already resolved delivered are the "seen before the reload" boundary below.
    const deliveredPreReload = new Set(
      await pageB.evaluate(() => globalThis.__marmotClient.deliveredIds()),
    );
    await pageB.reload();
    await pageB.evaluate(
      async (a: { k: string; s: string }) => globalThis.__marmotClient.init(a.k, a.s),
      {
        k: keyB.toString("base64"),
        s: Buffer.from(secretB).toString("base64"),
      },
    );
    await pageB.evaluate(async (u: string) => globalThis.__marmotClient.open(u), url);
    // The remaining 15 each way.
    for (let i = 15; i < 30; i += 1) {
      // oxlint-disable-next-line no-await-in-loop -- the exchange is sequential
      await sendBoth(pageA, pageB, group, i);
    }
    const dumpDiag = async (): Promise<void> => {
      const sentMapB = await pageB.evaluate(() => globalThis.__marmotClient.sentMapEntries());
      const deliveredB = await pageB.evaluate(() => globalThis.__marmotClient.deliveredIds());
      const stored = served.relay
        .events()
        .map((e) => `${e.id.slice(0, 16)} k${e.kind} t${e.created_at}`);
      const frames = served.relay.clientMessages().map((f) => JSON.stringify(f));
      const out = [
        `=== B sentMap (${sentMapB.length}) ===`,
        ...sentMapB.map(([text, ids]) => `${text} -> ${ids.join(",")}`),
        `=== B delivered (${deliveredB.length}) ===`,
        ...deliveredB,
        `=== relay stored (${stored.length}) ===`,
        ...stored,
        `=== relay inbox (${frames.length}) ===`,
        ...frames,
      ];
      const path = fileURLToPath(
        new URL("../../../target/browser-reload-diag.txt", import.meta.url),
      );
      writeFileSync(path, `${out.join("\n")}\n`);
    };
    const sentMap = new Map(await pageB.evaluate(() => globalThis.__marmotClient.sentMapEntries()));
    const preB = Array.from({ length: 15 }, (_, i) => `b>${i}`);
    const postB = Array.from({ length: 15 }, (_, i) => `b>${i + 15}`);
    const aTexts = Array.from({ length: 30 }, (_, i) => `a>${i}`);
    const mandatoryOnA = new Set([
      ...aTexts,
      ...postB,
      ...preB.filter((text) => (sentMap.get(text) ?? []).some((id) => deliveredPreReload.has(id))),
    ]);
    const bReceivedOnA = async (): Promise<Set<string>> => {
      const received = await pageA.evaluate(() => globalThis.__marmotClient.received());
      return new Set(received.filter((text) => text.startsWith("b>")));
    };
    const reachedRelay = (): Set<string> => {
      const stored = new Set(served.relay.events().map((e) => e.id));
      return new Set(
        [...sentMap.keys()].filter((text) =>
          (sentMap.get(text) ?? []).some((id) => stored.has(id)),
        ),
      );
    };
    try {
      // Mandatory on A: its own sends, every post-reload send, and every send B already saw
      // delivered before the reload. In-flight sends resolve once durable; a reload inside the
      // take→report window can legitimately lose the batch entirely — those arrive once or never.
      await waitForTranscript(pageA, mandatoryOnA, "mandatory on A");
      const full = new Set([...aTexts, ...preB, ...postB]);
      await waitForTranscript(pageB, full, "all 60 on B");
      // Converge A's received B-set with exactly the B sends that reached the relay.
      const deadline = Date.now() + 120_000;
      for (;;) {
        // oxlint-disable-next-line no-await-in-loop -- polling by design
        const [got, reached] = [await bReceivedOnA(), reachedRelay()];
        if (reached.size > 0 && [...reached].every((text) => got.has(text))) {
          break;
        }
        if (Date.now() > deadline) {
          throw new Error(
            `A did not consume the stored B sends; reached=${reached.size} got=${got.size}`,
          );
        }
        // oxlint-disable-next-line no-await-in-loop -- polling by design
        await new Promise((resolve) => {
          setTimeout(resolve, 100);
        });
      }
      const got = await bReceivedOnA();
      const reached = reachedRelay();
      expect(got).toStrictEqual(reached);
      // No duplication on A.
      const receivedA = await pageA.evaluate(() => globalThis.__marmotClient.received());
      expect(receivedA.length).toBe(new Set(receivedA).size);
      const transcriptA = await transcript(pageA);
      expect(new Set(transcriptA)).toStrictEqual(new Set([...aTexts, ...reached]));
      const transcriptB = await transcript(pageB);
      expect(new Set(transcriptB)).toStrictEqual(full);
    } catch (error) {
      await dumpDiag();
      throw error;
    }
    const [snapA, snapB] = await Promise.all([
      pageA.evaluate(async (g: string) => globalThis.__marmotClient.snapshot(g), group),
      pageB.evaluate(async (g: string) => globalThis.__marmotClient.snapshot(g), group),
    ]);
    expect(snapA).toStrictEqual(snapB);
    for (const page of [pageA, pageB]) {
      // oxlint-disable-next-line no-await-in-loop -- two pages, sequential reads
      const errors = await page.evaluate(() => globalThis.__marmotClient.errorsSeen());
      expect(errors).toStrictEqual([]);
    }
    await Promise.all([
      pageA.evaluate(async () => globalThis.__marmotClient.close()),
      pageB.evaluate(async () => globalThis.__marmotClient.close()),
    ]);
  } finally {
    await contextA.close();
    await contextB.close();
    await served.close();
  }
});
