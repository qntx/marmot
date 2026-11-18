import { indexedDB } from "fake-indexeddb";
import { describe, expect, test } from "vite-plus/test";

import { indexedDbCursorStore } from "../src/storage/cursors.ts";
import { StorageError } from "../src/storage/driver.ts";
import type { Cipher } from "../src/storage/driver.ts";

/** A trivial deterministic test cipher: shift by the row key's first byte. */
const cipher: Cipher = {
  seal: async (key, plain) => {
    const k = key[0] ?? 0;
    const out = new Uint8Array(plain.length);
    for (const [i, b] of plain.entries()) {
      out[i] = (b + k) % 256;
    }
    return Promise.resolve(out);
  },
  open: async (key, sealed) => {
    const k = key[0] ?? 0;
    const out = new Uint8Array(sealed.length);
    for (const [i, b] of sealed.entries()) {
      out[i] = (b - k + 256) % 256;
    }
    return Promise.resolve(out);
  },
};

const open = async (name: string) => indexedDbCursorStore(indexedDB, name, { cipher });

describe("indexedDbCursorStore", () => {
  test("an empty store loads undefined", async () => {
    const store = await open("cursors-empty");
    await expect(store.load()).resolves.toBeUndefined();
    await store.close();
  });

  test("save then load round-trips the blob", async () => {
    const store = await open("cursors-roundtrip");
    const blob = Uint8Array.of(1, 2, 3, 250);
    await store.save(blob);
    await expect(store.load()).resolves.toStrictEqual(blob);
    await store.close();
  });

  test("a reopened store over the same database sees the saved blob", async () => {
    const name = "cursors-reopen";
    const first = await open(name);
    await first.save(Uint8Array.of(9, 8, 7));
    await first.close();
    const second = await open(name);
    await expect(second.load()).resolves.toStrictEqual(Uint8Array.of(9, 8, 7));
    await second.close();
  });

  test("a later save overwrites", async () => {
    const store = await open("cursors-overwrite");
    await store.save(Uint8Array.of(1));
    await store.save(Uint8Array.of(2, 2));
    await expect(store.load()).resolves.toStrictEqual(Uint8Array.of(2, 2));
    await store.close();
  });

  test("a store that cannot decrypt the row loads undefined (foreign cipher)", async () => {
    const name = "cursors-foreign";
    const first = await indexedDbCursorStore(indexedDB, name, { cipher });
    await first.save(Uint8Array.of(42));
    await first.close();
    // A cipher that always rejects simulates an undecryptable row.
    const rejecting: Cipher = {
      seal: cipher.seal,
      open: async () => Promise.reject(new Error("undecryptable")),
    };
    const second = await indexedDbCursorStore(indexedDB, name, { cipher: rejecting });
    await expect(second.load()).resolves.toBeUndefined();
    await second.close();
  });

  test("operations after close throw StorageError('closed')", async () => {
    const store = await open("cursors-closed");
    await store.close();
    await expect(store.load()).rejects.toThrow(StorageError);
    await expect(store.save(Uint8Array.of(1))).rejects.toThrow(StorageError);
  });
});
