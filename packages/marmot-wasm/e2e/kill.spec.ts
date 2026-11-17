import { randomBytes } from "node:crypto";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { chromium, expect, test } from "@playwright/test";
import type { BrowserContext, Page } from "@playwright/test";
import { generateSecretKey } from "nostr-tools";

import { decodeKvBatch } from "@qntx/marmot/backend";
import type { KvBatch } from "@qntx/marmot/backend";

/* oxlint-disable no-bitwise, prefer-math-trunc -- mulberry32 is defined on 32-bit ops */
function mulberry32(seed: number): () => number {
  let state = seed | 0;
  return () => {
    state = (state + 0x6d2b79f5) | 0;
    let t = Math.imul(state ^ (state >>> 15), 1 | state);
    t ^= t + Math.imul(t ^ (t >>> 7), 61 | t);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}
/* oxlint-enable no-bitwise, prefer-math-trunc */

const sleep = async (ms: number): Promise<void> =>
  new Promise((resolve) => {
    setTimeout(() => {
      resolve();
    }, ms);
  });

function b64decode(text: string): Uint8Array {
  return new Uint8Array(Buffer.from(text, "base64"));
}

/** True when `haystack` contains `needle` as a byte substring. */
function contains(haystack: Uint8Array, needle: Uint8Array): boolean {
  outer: for (let i = 0; i + needle.length <= haystack.length; i += 1) {
    for (let j = 0; j < needle.length; j += 1) {
      if (haystack[i + j] !== needle[j]) {
        continue outer;
      }
    }
    return true;
  }
  return false;
}

/** Whether `buf` contains `id` as ASCII hex or raw bytes (mirrors `contains_id` in the suite). */
function containsId(buf: Uint8Array, idHex: string): boolean {
  const raw = new Uint8Array(idHex.length / 2);
  for (let i = 0; i < raw.length; i += 1) {
    raw[i] = Number.parseInt(idHex.slice(i * 2, i * 2 + 2), 16);
  }
  return contains(buf, new TextEncoder().encode(idHex)) || contains(buf, raw);
}

/**
 * Apply `batches` (seq order) into a key → value map; returns one snapshot per prefix so
 * `apply(batches[1..k])` comparisons are linear.
 */
function applyAll(batches: KvBatch[]): Array<Map<string, Uint8Array>> {
  const current = new Map<string, Uint8Array>();
  const prefixes: Array<Map<string, Uint8Array>> = [new Map(current)];
  for (const batch of batches) {
    for (const op of batch.ops) {
      const key = Buffer.from(op.key).toString("base64");
      if (op.type === "put") {
        current.set(key, op.value);
      } else {
        current.delete(key);
      }
    }
    prefixes.push(new Map(current));
  }
  return prefixes;
}

function mapsEqual(a: Map<string, Uint8Array>, b: Map<string, Uint8Array>): boolean {
  if (a.size !== b.size) {
    return false;
  }
  for (const [key, value] of a) {
    const other = b.get(key);
    if (other === undefined || !Buffer.from(value).equals(Buffer.from(other))) {
      return false;
    }
  }
  return true;
}

test("kill during commit loses nothing acknowledged", async () => {
  test.setTimeout(1_800_000);
  const ITERATIONS = 50;
  const seed = Number(process.env["KILL_SEED"] ?? "20260209");
  const prng = mulberry32(seed);
  // oxlint-disable-next-line no-console
  console.log(`kill-during-write seed=${seed} iterations=${ITERATIONS}`);

  const userDataDir = mkdtempSync(join(tmpdir(), "marmot-kill-"));
  let context: BrowserContext | undefined = await chromium.launchPersistentContext(userDataDir, {
    headless: true,
  });
  const killsByType = { page: 0, context: 0 };
  const gaps: number[] = [];

  try {
    for (let i = 0; i < ITERATIONS; i += 1) {
      const dbName = `kill-${i}`;
      const key = randomBytes(32).toString("base64");
      const secret = Buffer.from(generateSecretKey()).toString("base64");
      const batches: Array<{ seq: string; bytes: string }> = [];
      const acked: string[] = [];
      const published: Array<{ eventId: string; watermark: bigint }> = [];

      // oxlint-disable-next-line no-await-in-loop
      let page: Page = await context.newPage();
      // oxlint-disable-next-line no-await-in-loop
      await page.exposeFunction("recordBatch", (seq: string, bytes: string) => {
        batches.push({ seq, bytes });
      });
      // oxlint-disable-next-line no-await-in-loop
      await page.exposeFunction("recordAck", (seq: string) => {
        acked.push(seq);
      });
      // oxlint-disable-next-line no-await-in-loop
      await page.exposeFunction(
        "publishOutbound",
        (_outboundId: string, eventId: string, watermark: string) => {
          published.push({ eventId, watermark: BigInt(watermark) });
        },
      );
      // oxlint-disable-next-line no-await-in-loop
      await page.goto("/");
      // oxlint-disable-next-line no-await-in-loop
      await page.evaluate(
        async (a: { k: string; s: string }) => globalThis.__marmot.init(a.k, a.s),
        {
          k: key,
          s: secret,
        },
      );
      // oxlint-disable-next-line no-await-in-loop
      await page.evaluate(async (name) => globalThis.__marmot.startKillLoop(name), dbName);

      // Let at least one batch land, then kill at the seeded delay.
      const started = Date.now();
      const delayMs = 20 + Math.floor(prng() * 400);
      while (batches.length === 0 && Date.now() - started < 15_000) {
        // oxlint-disable-next-line no-await-in-loop
        await sleep(5);
      }
      // oxlint-disable-next-line no-await-in-loop
      await sleep(delayMs);

      if (i % 2 === 0) {
        killsByType.page += 1;
        // oxlint-disable-next-line no-await-in-loop
        await page.close();
      } else {
        killsByType.context += 1;
        // oxlint-disable-next-line no-await-in-loop
        await context.close();
        // oxlint-disable-next-line no-await-in-loop
        context = await chromium.launchPersistentContext(userDataDir, { headless: true });
      }
      // oxlint-disable-next-line no-await-in-loop
      page = await context.newPage();

      // Reopen in a fresh page: dump raw state and open an Engine (must succeed).
      // oxlint-disable-next-line no-await-in-loop
      await page.goto("/");
      // oxlint-disable-next-line no-await-in-loop
      await page.evaluate(
        async (a: { k: string; s: string }) => globalThis.__marmot.init(a.k, a.s),
        {
          k: key,
          s: secret,
        },
      );
      // oxlint-disable-next-line no-await-in-loop
      const rows = await page.evaluate(async (name) => globalThis.__marmot.dumpRows(name), dbName);
      // oxlint-disable-next-line no-await-in-loop
      await page.evaluate(async (name) => globalThis.__marmot.openEngine(name), dbName);
      // oxlint-disable-next-line no-await-in-loop
      await page.evaluate(async () => globalThis.__marmot.closeEngine());
      // oxlint-disable-next-line no-await-in-loop
      await page.close();

      // Node-side: decode every recorded batch and find the durable prefix k.
      const decoded: KvBatch[] = batches.map(({ bytes }) => decodeKvBatch(b64decode(bytes)));
      for (let j = 1; j < decoded.length; j += 1) {
        expect((decoded[j]?.seq ?? 0n) > (decoded[j - 1]?.seq ?? 0n)).toBe(true);
      }
      const store = new Map<string, Uint8Array>(rows.map((row) => [row.key, b64decode(row.value)]));
      const prefixes = applyAll(decoded);
      let k = -1;
      for (let j = 0; j < prefixes.length; j += 1) {
        const prefix = prefixes[j];
        if (prefix !== undefined && mapsEqual(prefix, store)) {
          k = j;
        }
      }
      const lastAcked = acked.length > 0 ? BigInt(acked.at(-1) ?? "0") : 0n;
      expect(
        k,
        `iteration ${i}: durable rows must equal apply(batches[1..k])`,
      ).toBeGreaterThanOrEqual(0);
      const kSeq = k > 0 ? (decoded[k - 1]?.seq ?? 0n) : 0n;
      expect(
        kSeq >= lastAcked,
        `iteration ${i}: prefix ${kSeq} must cover last acked ${lastAcked}`,
      ).toBe(true);
      gaps.push(decoded.length - k);

      // Every published event's id lives in a value of apply(batches[1..w]).
      for (const { eventId, watermark } of published) {
        let found = false;
        for (const batch of decoded) {
          if (batch.seq > watermark) {
            break;
          }
          for (const op of batch.ops) {
            if (op.type === "put" && containsId(op.value, eventId)) {
              found = true;
            }
          }
        }
        expect(
          found,
          `iteration ${i}: published ${eventId} durable by watermark ${watermark}`,
        ).toBe(true);
      }
    }
  } finally {
    await context.close();
    rmSync(userDataDir, { recursive: true, force: true });
  }

  const min = gaps.length > 0 ? Math.min(...gaps) : 0;
  const max = gaps.length > 0 ? Math.max(...gaps) : 0;
  // oxlint-disable-next-line no-console
  console.log(
    `kill-during-write: ${ITERATIONS} iterations, kills page=${killsByType.page} context=${killsByType.context}, k-lastAcked min=${min} max=${max}`,
  );
});
