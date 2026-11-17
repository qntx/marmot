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
});
