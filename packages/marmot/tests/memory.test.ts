import { describe, expect, test } from "vite-plus/test";

import { encodeKvBatch } from "../src/backend/kv.ts";
import { StorageError } from "../src/storage/driver.ts";
import { memoryKv } from "../src/testing/memory.ts";

const RESOLVED = Symbol("resolved");

async function capture(promise: Promise<unknown>): Promise<unknown> {
  try {
    await promise;
    return RESOLVED;
  } catch (error) {
    return error;
  }
}

function batch(seq: bigint, key: number, value: number[]) {
  return encodeKvBatch({
    seq,
    ops: [{ type: "put", key: Uint8Array.of(key), value: Uint8Array.of(...value) }],
  });
}

describe("memoryKv", () => {
  test("commits make rows durable and load returns them", async () => {
    const kv = memoryKv();
    await kv.commit(batch(1n, 1, [10]));
    await kv.commit(batch(2n, 2, [20, 30]));
    expect(kv.rows()).toStrictEqual([
      [Uint8Array.of(1), Uint8Array.of(10)],
      [Uint8Array.of(2), Uint8Array.of(20, 30)],
    ]);
    expect(kv.batches()).toHaveLength(2);
    await kv.close();
  });

  test("hold keeps commits pending until release", async () => {
    const kv = memoryKv();
    kv.hold();
    let done = false;
    const pending = (async () => {
      await kv.commit(batch(1n, 1, [1]));
      done = true;
    })();
    await Promise.resolve();
    expect(done).toBe(false);
    kv.release();
    await pending;
    expect(done).toBe(true);
    await kv.close();
  });

  test("failNext rejects the next commit with io", async () => {
    const kv = memoryKv();
    kv.failNext();
    const outcome = await capture(kv.commit(batch(1n, 1, [1])));
    expect(outcome).toBeInstanceOf(StorageError);
    expect(outcome).toMatchObject({ kind: "io" });
    await kv.close();
  });

  test("a failed commit makes every later commit fail with io", async () => {
    const kv = memoryKv();
    await kv.commit(batch(1n, 1, [10]));
    kv.failNext();
    const failed = await capture(kv.commit(batch(2n, 2, [20])));
    const later = await capture(kv.commit(batch(3n, 3, [30])));
    expect(failed).toMatchObject({ kind: "io" });
    expect(later).toMatchObject({ kind: "io" });
    // The poisoned batch and everything after it never became durable.
    expect(kv.rows()).toStrictEqual([[Uint8Array.of(1), Uint8Array.of(10)]]);
    expect(kv.batches()).toHaveLength(1);
    await kv.close();
  });

  test("commits queued behind a failure reject on release", async () => {
    const kv = memoryKv();
    kv.hold();
    kv.failNext();
    const first = kv.commit(batch(1n, 1, [10]));
    const second = kv.commit(batch(2n, 2, [20]));
    kv.release();
    const outcomes = await Promise.all([capture(first), capture(second)]);
    for (const outcome of outcomes) {
      expect(outcome).toMatchObject({ kind: "io" });
    }
    expect(kv.rows()).toStrictEqual([]);
    await kv.close();
  });
});
