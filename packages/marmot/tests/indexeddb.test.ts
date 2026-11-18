import { indexedDB } from "fake-indexeddb";
import { describe, expect, test } from "vite-plus/test";

import { indexedDbKv } from "../src/storage/indexeddb.ts";
import type { IdbFactoryLike } from "../src/storage/indexeddb.ts";
import { kvDriverContract } from "./kv-contract.ts";

const factory: IdbFactoryLike = indexedDB;

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

/** Read every stored value out of the `rows` store in key order, bypassing the driver. */
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

kvDriverContract({ describe, test, expect }, "indexedDbKv", {
  open: async (name, options) => indexedDbKv(factory, name, options),
  rawValues,
  rawPut,
  corruptValue: "a-string-value",
});
