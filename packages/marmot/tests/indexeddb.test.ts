import { indexedDB } from "fake-indexeddb";
import { describe, expect, test } from "vite-plus/test";

import { encodeKvBatch } from "../src/backend/kv.ts";
import type { Rows } from "../src/backend/kv.ts";
import { StorageError } from "../src/storage/driver.ts";
import type { Cipher, LockPort } from "../src/storage/driver.ts";
import { indexedDbKv } from "../src/storage/indexeddb.ts";
import type { IdbFactoryLike } from "../src/storage/indexeddb.ts";

const factory: IdbFactoryLike = indexedDB;

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
const cipher: Cipher = {
  seal: async (key, plain) => {
    const out = new Uint8Array(plain.length + 3);
    out.set(SEAL_PREFIX);
    out[2] = keyCheck(key);
    for (const [i, byte] of plain.entries()) {
      out[i + 3] = (byte + 165) % 256;
    }
    return Promise.resolve(out);
  },
  open: async (key, sealed) => {
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
    return Promise.resolve(out);
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
function fakeLock(): { port: LockPort; held: () => boolean } {
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
async function capture(promise: Promise<unknown>): Promise<unknown> {
  try {
    await promise;
    return RESOLVED;
  } catch (error) {
    return error;
  }
}

/** The StorageError kind of a captured outcome ("other" when it is not a StorageError). */
function kindOf(outcome: unknown): string {
  return outcome instanceof StorageError ? outcome.kind : "other";
}

let counter = 0;
function dbName(prefix: string): string {
  counter += 1;
  return `${prefix}-${counter}`;
}

type IdbReq<T> = { readonly result: T; addEventListener: (type: string, l: () => void) => void };

async function once<T>(request: IdbReq<T>): Promise<T> {
  return new Promise((resolve, reject) => {
    request.addEventListener("success", () => {
      resolve(request.result);
    });
    request.addEventListener("error", () => {
      reject(new Error("idb request failed"));
    });
  });
}

type RawDb = {
  transaction: (
    store: string,
    mode: string,
  ) => {
    objectStore: (name: string) => {
      getAll: () => IdbReq<unknown[]>;
      getAllKeys: () => IdbReq<unknown[]>;
      put: (value: unknown, key: unknown) => IdbReq<unknown>;
    };
    addEventListener: (type: string, listener: () => void) => void;
  };
  close: () => void;
};

async function openRaw(name: string): Promise<RawDb> {
  return new Promise((resolve, reject) => {
    const req = indexedDB.open(name);
    req.addEventListener("success", () => {
      resolve(req.result);
    });
    req.addEventListener("error", () => {
      reject(new Error("open failed"));
    });
  });
}

/** Read every stored value out of the `rows` store, bypassing the driver. */
async function rawValues(name: string): Promise<unknown[]> {
  const db = await openRaw(name);
  const txn = db.transaction("rows", "readonly");
  const store = txn.objectStore("rows");
  const values = await once(store.getAll());
  db.close();
  return values;
}

/** Write a raw row into the store, bypassing the driver. */
async function rawPut(name: string, key: unknown, value: unknown): Promise<void> {
  const db = await openRaw(name);
  const txn = db.transaction("rows", "readwrite");
  txn.objectStore("rows").put(value, key);
  await new Promise<void>((resolve, reject) => {
    txn.addEventListener("complete", () => {
      resolve();
    });
    txn.addEventListener("error", () => {
      reject(new Error("put failed"));
    });
  });
  db.close();
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

describe("indexedDbKv", () => {
  test("round trip: commits persist and reload", async () => {
    const name = dbName("rt");
    const lock = fakeLock();
    const kv = await indexedDbKv(factory, name, { lock: lock.port, cipher });
    await kv.commit(batch(1n, [{ type: "put", key: 1, value: [10, 20] }]));
    await kv.commit(batch(2n, [{ type: "put", key: 2, value: [30] }]));
    await kv.commit(batch(3n, [{ type: "delete", key: 1 }]));
    await kv.close();

    const again = await indexedDbKv(factory, name, { lock: lock.port, cipher });
    expect(sortedHex(await again.load())).toStrictEqual(["2=30"]);
    await again.close();
  });

  test("cipher installed: no plaintext value appears in the store", async () => {
    const name = dbName("cipher");
    const lock = fakeLock();
    const kv = await indexedDbKv(factory, name, { lock: lock.port, cipher });
    await kv.commit(batch(1n, [{ type: "put", key: 7, value: [10, 20, 30, 40] }]));
    await kv.close();

    const values = await rawValues(name);
    expect(values.map(plainAppearsIn)).toStrictEqual([false]);
    expect(values.map(roundTrip)).toStrictEqual([[10, 20, 30, 40]]);
  });

  test("non-binary stored rows decode as corrupt_row", async () => {
    const name = dbName("corrupt");
    const lock = fakeLock();
    const kv = await indexedDbKv(factory, name, { lock: lock.port, cipher });
    await kv.close();
    await rawPut(name, Uint8Array.of(9), "a-string-value");

    const again = await indexedDbKv(factory, name, { lock: lock.port, cipher });
    const outcome = await capture(again.load());
    expect(kindOf(outcome)).toBe("corrupt_row");
    await again.close();
  });

  test("a sealed row swapped under another key decodes as undecryptable_row", async () => {
    const name = dbName("swap");
    const lock = fakeLock();
    const aead = aesGcmCipher(crypto.getRandomValues(new Uint8Array(32)));
    const kv = await indexedDbKv(factory, name, { lock: lock.port, cipher: aead });
    await kv.commit(
      batch(1n, [
        { type: "put", key: 1, value: [10, 20] },
        { type: "put", key: 2, value: [30, 40] },
      ]),
    );
    await kv.close();

    // A clean reopen still round-trips: seal and open pass the right key.
    const clean = await indexedDbKv(factory, name, { lock: lock.port, cipher: aead });
    expect(sortedHex(await clean.load())).toStrictEqual(["1=10.20", "2=30.40"]);
    await clean.close();

    // Swap the stored values behind the driver's back: each row key now
    // points at a value sealed for the other key. getAll/getAllKeys share
    // key order, so index 0 is key 1 and index 1 is key 2.
    const values = await rawValues(name);
    expect(values).toHaveLength(2);
    await rawPut(name, Uint8Array.of(1), values[1]);
    await rawPut(name, Uint8Array.of(2), values[0]);

    const again = await indexedDbKv(factory, name, { lock: lock.port, cipher: aead });
    const outcome = await capture(again.load());
    expect(kindOf(outcome)).toBe("undecryptable_row");
    await again.close();
  });

  test("sealed-foreign bytes decode as undecryptable_row", async () => {
    const name = dbName("undecryptable");
    const lock = fakeLock();
    const kv = await indexedDbKv(factory, name, { lock: lock.port, cipher });
    await kv.commit(batch(1n, [{ type: "put", key: 1, value: [1, 2] }]));
    await kv.close();
    // Binary but not produced by the cipher (wrong prefix).
    await rawPut(name, Uint8Array.of(9), Uint8Array.of(1, 2, 3, 4));

    const again = await indexedDbKv(factory, name, { lock: lock.port, cipher });
    const outcome = await capture(again.load());
    expect(kindOf(outcome)).toBe("undecryptable_row");
    await again.close();
  });

  test("second open while the lock is held fails with locked", async () => {
    const name = dbName("locked");
    const lock = fakeLock();
    const kv = await indexedDbKv(factory, name, { lock: lock.port, cipher });
    const outcome = await capture(indexedDbKv(factory, name, { lock: lock.port, cipher }));
    expect(kindOf(outcome)).toBe("locked");
    await kv.close();
  });

  test("commits apply strictly in order", async () => {
    const name = dbName("order");
    const lock = fakeLock();
    const kv = await indexedDbKv(factory, name, { lock: lock.port, cipher });
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
    const kv = await indexedDbKv(factory, name, { lock: lock.port, cipher: flakyCipher(2) });
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
    const kv = await indexedDbKv(factory, name, { lock: lock.port, cipher });
    const pending = kv.commit(batch(1n, [{ type: "put", key: 1, value: [1] }]));
    await kv.close();
    await pending;
    expect(lock.held()).toBe(false);
    const outcome = await capture(kv.commit(batch(2n, [{ type: "put", key: 1 }])));
    expect(kindOf(outcome)).toBe("closed");
    // The lock is free, so reopening works and sees the committed row.
    const again = await indexedDbKv(factory, name, { lock: lock.port, cipher });
    expect(sortedHex(await again.load())).toStrictEqual(["1=1"]);
    await again.close();
  });
});
