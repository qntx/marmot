/**
 * `memoryKv` — an in-memory `KvDriver` for tests, with hooks to inspect the durable state and to
 * script durability timing and failures.
 */

import { hexEncode } from "../backend/hex.ts";
import { decodeKvBatch } from "../backend/kv.ts";
import type { Rows } from "../backend/kv.ts";
import { StorageError } from "../storage/driver.ts";
import type { KvDriver } from "../storage/driver.ts";

/** A `KvDriver` over a `Map`, plus test hooks. */
export type MemoryKv = {
  /** The current durable rows, in insertion order. */
  rows: () => Rows;
  /** Every batch that became durable, in order. */
  batches: () => Uint8Array[];
  /** Hold durability: subsequent `commit` calls stay pending. */
  hold: () => void;
  /** Release held commits, letting them resolve. */
  release: () => void;
  /** Make the next `commit` reject with `io`; every later commit fails too. */
  failNext: () => void;
} & KvDriver;

/** Build a `MemoryKv`, optionally seeded with initial durable `rows`. */
export function memoryKv(initial?: Rows): MemoryKv {
  const store = new Map<string, Uint8Array>();
  const keys = new Map<string, Uint8Array>();
  for (const [key, value] of initial ?? []) {
    const id = hexEncode(key);
    store.set(id, value);
    keys.set(id, key);
  }
  const committed: Uint8Array[] = [];
  let released = true;
  const waiters: Array<() => void> = [];
  let failNext = false;
  let poisoned = false;
  let closed = false;
  const chain: Array<Promise<unknown>> = [];

  const driver: MemoryKv = {
    load: async (): Promise<Rows> => {
      await Promise.resolve();
      return driver.rows();
    },

    async commit(batch: Uint8Array): Promise<void> {
      if (closed) {
        throw new StorageError("closed");
      }
      const tracked = (async () => {
        if (!released) {
          await new Promise<void>((resolve) => {
            waiters.push(resolve);
          });
        }
        // The host contract is terminal: a batch lost to a failed commit may be a
        // dependency of every later batch, so nothing applies after a failure.
        if (poisoned) {
          throw new StorageError("io");
        }
        if (failNext) {
          failNext = false;
          poisoned = true;
          throw new StorageError("io");
        }
        for (const op of decodeKvBatch(batch).ops) {
          const id = hexEncode(op.key);
          if (op.type === "put") {
            store.set(id, op.value);
            keys.set(id, op.key);
          } else {
            store.delete(id);
            keys.delete(id);
          }
        }
        committed.push(batch);
      })();
      chain.push(tracked);
      await tracked;
    },

    rows(): Rows {
      const rows: Array<[Uint8Array, Uint8Array]> = [];
      for (const [id, value] of store) {
        const key = keys.get(id);
        if (key !== undefined) {
          rows.push([key, value]);
        }
      }
      return rows;
    },

    batches(): Uint8Array[] {
      return [...committed];
    },

    hold(): void {
      released = false;
    },

    release(): void {
      released = true;
      const pending = waiters.splice(0);
      for (const waiter of pending) {
        waiter();
      }
    },

    failNext(): void {
      failNext = true;
    },

    async close(): Promise<void> {
      closed = true;
      await Promise.allSettled(chain);
    },
  };
  return driver;
}
