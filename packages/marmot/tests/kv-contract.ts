/**
 * The `KvDriver` contract suite, run once per driver (`indexeddb.test.ts` over fake-indexeddb,
 * `tests/bun/sqlite.test.ts` over `bun:sqlite`). Every case asserts the same guarantee for both
 * drivers: sealed-at-rest values, terminal failure, strict commit order, lock lifecycle.
 *
 * The suite takes the test framework's `describe`/`test`/`expect` explicitly so one file serves
 * vitest and `bun test`; `expect` only needs the matchers below.
 */

import { encodeKvBatch } from "../src/backend/kv.ts";
import type { Rows } from "../src/backend/kv.ts";
import { StorageError } from "../src/storage/driver.ts";
import type { Cipher, KvDriver, LockPort } from "../src/storage/driver.ts";

/** What a driver file provides: open the driver, plus raw access bypassing it. */
export type KvHarness = {
  /** Open (creating if needed) the driver's store under `name` and its `marmot:<name>` lock. */
  open: (name: string, options: { lock: LockPort; cipher: Cipher }) => Promise<KvDriver>;
  /** Read every stored (sealed) value in key order, bypassing the driver. */
  rawValues: (name: string) => Promise<unknown[]>;
  /** Write one raw row, bypassing the driver. */
  rawPut: (name: string, key: Uint8Array, value: unknown) => Promise<void>;
  /** A stored value that is not binary — must decode as `corrupt_row` on load. */
  corruptValue: unknown;
};

/** The framework primitives the suite needs. */
export type TestApi = {
  describe: (name: string, fn: () => void) => void;
  test: (name: string, fn: () => void | Promise<void>) => void;
  expect: (value: unknown) => {
    toStrictEqual: (expected: unknown) => void;
    toBe: (expected: unknown) => void;
    toHaveLength: (expected: number) => void;
  };
};

const SEAL_PREFIX = Uint8Array.of(0x9d, 0x4c);

/** A key-binding check byte: `open` under a different key rejects. */
function keyCheck(key: Uint8Array): number {
  let sum = 0;
  for (const byte of key) {
    sum = (sum + byte) % 256;
  }
  return sum;
}

/** Rotating cipher with a magic prefix and a key check; `open` rejects a wrong prefix or key. */
export const cipher: Cipher = {
  seal: async (key, plain) => {
    await Promise.resolve();
    const out = new Uint8Array(plain.length + 3);
    out.set(SEAL_PREFIX);
    out[2] = keyCheck(key);
    for (const [i, byte] of plain.entries()) {
      out[i + 3] = (byte + 165) % 256;
    }
    return out;
  },
  open: async (key, sealed) => {
    await Promise.resolve();
    if (
      sealed.length < 3 ||
      sealed[0] !== SEAL_PREFIX[0] ||
      sealed[1] !== SEAL_PREFIX[1] ||
      sealed[2] !== keyCheck(key)
    ) {
      throw new Error("not sealed");
    }
    const out = new Uint8Array(sealed.length - 3);
    for (let i = 3; i < sealed.length; i += 1) {
      out[i - 3] = ((sealed[i] ?? 0) + 91) % 256;
    }
    return out;
  },
};

/** Real WebCrypto AES-GCM bound to the row key via `additionalData`; stored as IV ‖ ciphertext. */
function aesGcmCipher(keyBytes: Uint8Array): Cipher {
  const key = crypto.subtle.importKey("raw", new Uint8Array(keyBytes), { name: "AES-GCM" }, false, [
    "encrypt",
    "decrypt",
  ]);
  return {
    seal: async (rowKey, plain) => {
      const iv = crypto.getRandomValues(new Uint8Array(12));
      const sealed = new Uint8Array(
        await crypto.subtle.encrypt(
          { name: "AES-GCM", iv, additionalData: new Uint8Array(rowKey) },
          await key,
          new Uint8Array(plain),
        ),
      );
      const out = new Uint8Array(iv.length + sealed.length);
      out.set(iv);
      out.set(sealed, iv.length);
      return out;
    },
    open: async (rowKey, sealed) => {
      return new Uint8Array(
        await crypto.subtle.decrypt(
          {
            name: "AES-GCM",
            iv: new Uint8Array(sealed.subarray(0, 12)),
            additionalData: new Uint8Array(rowKey),
          },
          await key,
          new Uint8Array(sealed.subarray(12)),
        ),
      );
    },
  };
}

/** A cipher that throws a plain error on its `nth` (1-based) seal call. */
function flakyCipher(nth: number): Cipher {
  let seals = 0;
  return {
    seal: async (key, plain) => {
      seals += 1;
      if (seals === nth) {
        throw new Error("seal unavailable");
      }
      return cipher.seal(key, plain);
    },
    open: cipher.open,
  };
}

/** A lock port that grants each name to one holder until released. */
export function fakeLock(): { port: LockPort; held: () => boolean } {
  const held = new Set<string>();
  return {
    held: () => held.size > 0,
    port: {
      acquire: async (name) => {
        await Promise.resolve();
        if (held.has(name)) {
          return undefined;
        }
        held.add(name);
        return () => {
          held.delete(name);
        };
      },
    },
  };
}

function batch(seq: bigint, ops: Array<{ type: "put" | "delete"; key: number; value?: number[] }>) {
  return encodeKvBatch({
    seq,
    ops: ops.map((op) =>
      op.type === "put"
        ? { type: "put", key: Uint8Array.of(op.key), value: Uint8Array.of(...(op.value ?? [])) }
        : { type: "delete", key: Uint8Array.of(op.key) },
    ),
  });
}

function sortedHex(rows: Rows): string[] {
  return rows.map(([key, value]) => `${[...key].join(".")}=${[...value].join(".")}`).toSorted();
}

const RESOLVED = Symbol("resolved");

/** Await `promise`, returning the rejection or a sentinel — lets tests assert without try/catch. */
export async function capture(promise: Promise<unknown>): Promise<unknown> {
  try {
    await promise;
    return RESOLVED;
  } catch (error) {
    return error;
  }
}

/** The StorageError kind of a captured outcome ("other" when it is not a StorageError). */
export function kindOf(outcome: unknown): string {
  return outcome instanceof StorageError ? outcome.kind : "other";
}

let counter = 0;
function dbName(prefix: string): string {
  counter += 1;
  return `${prefix}-${counter}`;
}

function asBytes(value: unknown): Uint8Array {
  if (value instanceof Uint8Array) {
    return value;
  }
  if (value instanceof ArrayBuffer) {
    return new Uint8Array(value);
  }
  throw new Error("stored value is not binary");
}

function containsSubsequence(haystack: Uint8Array, needle: Uint8Array): boolean {
  if (needle.length === 0) {
    return true;
  }
  for (let i = 0; i + needle.length <= haystack.length; i += 1) {
    let found = true;
    for (let j = 0; j < needle.length; j += 1) {
      if (haystack[i + j] !== needle[j]) {
        found = false;
        break;
      }
    }
    if (found) {
      return true;
    }
  }
  return false;
}

function plainAppearsIn(value: unknown): boolean {
  const bytes = asBytes(value);
  return containsSubsequence(bytes, Uint8Array.of(10, 20, 30, 40));
}

function roundTrip(value: unknown): number[] {
  return Array.from(asBytes(value).slice(3), (b) => (b + 91) % 256);
}

/** Register the whole contract suite for `harness` under `label`. */
export function kvDriverContract(t: TestApi, label: string, harness: KvHarness): void {
  const { describe, test, expect } = t;
  const open = async (name: string, lock: LockPort, activeCipher: Cipher = cipher) =>
    harness.open(name, { lock, cipher: activeCipher });

  describe(label, () => {
    test("round trip: commits persist and reload", async () => {
      const name = dbName("rt");
      const lock = fakeLock();
      const kv = await open(name, lock.port);
      await kv.commit(batch(1n, [{ type: "put", key: 1, value: [10, 20] }]));
      await kv.commit(batch(2n, [{ type: "put", key: 2, value: [30] }]));
      await kv.commit(batch(3n, [{ type: "delete", key: 1 }]));
      await kv.close();

      const again = await open(name, lock.port);
      expect(sortedHex(await again.load())).toStrictEqual(["2=30"]);
      await again.close();
    });

    test("cipher installed: no plaintext value appears in the store", async () => {
      const name = dbName("cipher");
      const lock = fakeLock();
      const kv = await open(name, lock.port);
      await kv.commit(batch(1n, [{ type: "put", key: 7, value: [10, 20, 30, 40] }]));
      await kv.close();

      const values = await harness.rawValues(name);
      expect(values.map(plainAppearsIn)).toStrictEqual([false]);
      expect(values.map(roundTrip)).toStrictEqual([[10, 20, 30, 40]]);
    });

    test("non-binary stored rows decode as corrupt_row", async () => {
      const name = dbName("corrupt");
      const lock = fakeLock();
      const kv = await open(name, lock.port);
      await kv.close();
      await harness.rawPut(name, Uint8Array.of(9), harness.corruptValue);

      const again = await open(name, lock.port);
      const outcome = await capture(again.load());
      expect(kindOf(outcome)).toBe("corrupt_row");
      await again.close();
    });

    test("a sealed row swapped under another key decodes as undecryptable_row", async () => {
      const name = dbName("swap");
      const lock = fakeLock();
      const aead = aesGcmCipher(crypto.getRandomValues(new Uint8Array(32)));
      const kv = await open(name, lock.port, aead);
      await kv.commit(
        batch(1n, [
          { type: "put", key: 1, value: [10, 20] },
          { type: "put", key: 2, value: [30, 40] },
        ]),
      );
      await kv.close();

      // A clean reopen still round-trips: seal and open pass the right key.
      const clean = await open(name, lock.port, aead);
      expect(sortedHex(await clean.load())).toStrictEqual(["1=10.20", "2=30.40"]);
      await clean.close();

      // Swap the stored values behind the driver's back: each row key now
      // points at a value sealed for the other key. rawValues is in key
      // order, so index 0 is key 1 and index 1 is key 2.
      const values = await harness.rawValues(name);
      expect(values).toHaveLength(2);
      await harness.rawPut(name, Uint8Array.of(1), values[1]);
      await harness.rawPut(name, Uint8Array.of(2), values[0]);

      const again = await open(name, lock.port, aead);
      const outcome = await capture(again.load());
      expect(kindOf(outcome)).toBe("undecryptable_row");
      await again.close();
    });

    test("sealed-foreign bytes decode as undecryptable_row", async () => {
      const name = dbName("undecryptable");
      const lock = fakeLock();
      const kv = await open(name, lock.port);
      await kv.commit(batch(1n, [{ type: "put", key: 1, value: [1, 2] }]));
      await kv.close();
      // Binary but not produced by the cipher (wrong prefix).
      await harness.rawPut(name, Uint8Array.of(9), Uint8Array.of(1, 2, 3, 4));

      const again = await open(name, lock.port);
      const outcome = await capture(again.load());
      expect(kindOf(outcome)).toBe("undecryptable_row");
      await again.close();
    });

    test("second open while the lock is held fails with locked", async () => {
      const name = dbName("locked");
      const lock = fakeLock();
      const kv = await open(name, lock.port);
      const outcome = await capture(open(name, lock.port));
      expect(kindOf(outcome)).toBe("locked");
      await kv.close();
    });

    test("commits apply strictly in order", async () => {
      const name = dbName("order");
      const lock = fakeLock();
      const kv = await open(name, lock.port);
      // Issued without awaiting: the commit chain must still serialize apply.
      const first = kv.commit(
        batch(1n, [
          { type: "put", key: 1, value: [1] },
          { type: "put", key: 2, value: [2] },
        ]),
      );
      const second = kv.commit(batch(2n, [{ type: "delete", key: 1 }]));
      const third = kv.commit(batch(3n, [{ type: "put", key: 3, value: [3] }]));
      await Promise.all([first, second, third]);
      expect(sortedHex(await kv.load())).toStrictEqual(["2=2", "3=3"]);
      await kv.close();
    });

    test("a failed commit makes every later commit reject with io without applying", async () => {
      const name = dbName("poison");
      const lock = fakeLock();
      const kv = await open(name, lock.port, flakyCipher(2));
      await kv.commit(batch(1n, [{ type: "put", key: 1, value: [1] }]));
      // Commit 2 fails inside apply; commit 3 was already queued behind it.
      const failed = kv.commit(batch(2n, [{ type: "put", key: 2, value: [2] }]));
      const queued = kv.commit(batch(3n, [{ type: "put", key: 3, value: [3] }]));
      const later = capture(kv.commit(batch(4n, [{ type: "put", key: 4, value: [4] }])));
      const outcomes = await Promise.all([capture(failed), capture(queued), later]);
      expect(outcomes.map(kindOf)).toStrictEqual(["io", "io", "io"]);
      expect(sortedHex(await kv.load())).toStrictEqual(["1=1"]);
      await kv.close();
    });

    test("close waits for pending commits and releases the lock", async () => {
      const name = dbName("close");
      const lock = fakeLock();
      const kv = await open(name, lock.port);
      const pending = kv.commit(batch(1n, [{ type: "put", key: 1, value: [1] }]));
      await kv.close();
      await pending;
      expect(lock.held()).toBe(false);
      const outcome = await capture(kv.commit(batch(2n, [{ type: "put", key: 1 }])));
      expect(kindOf(outcome)).toBe("closed");
      // The lock is free, so reopening works and sees the committed row.
      const again = await open(name, lock.port);
      expect(sortedHex(await again.load())).toStrictEqual(["1=1"]);
      await again.close();
    });
  });
}
