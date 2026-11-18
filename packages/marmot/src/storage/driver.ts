/**
 * The storage driver port: `DurableKv` plus lifecycle, and the injected cipher and lock
 * dependencies `indexedDbKv` needs.
 */

import type { DurableKv } from "../backend/kv.ts";

/** A `DurableKv` with an explicit close that releases its resources. */
export type KvDriver = {
  /** Wait for pending commits, then release the store and its lock. */
  close: () => Promise<void>;
} & DurableKv;

/**
 * Value encryption for stored rows; installed by the host. `key` is the row key: implementations
 * must authenticate it as associated data so a value sealed for one key cannot be replayed under
 * another — `open` must reject when `sealed` was produced for a different key.
 */
export type Cipher = {
  /** Encrypt one plaintext row value, authenticated to `key`. */
  seal: (key: Uint8Array, plain: Uint8Array) => Promise<Uint8Array>;
  /**
   * Decrypt one sealed row value under `key`; must reject on undecryptable input or when `sealed`
   * was produced for a different key.
   */
  open: (key: Uint8Array, sealed: Uint8Array) => Promise<Uint8Array>;
};

/** A named-lock port (`webLocks` in browsers, a fake in tests). */
export type LockPort = {
  /**
   * Acquire `name`. Resolves with the release function, or `undefined` when the lock is held
   * elsewhere.
   */
  acquire: (name: string) => Promise<(() => void) | undefined>;
};

/** Why a storage operation failed. */
export type StorageErrorKind = "locked" | "corrupt_row" | "undecryptable_row" | "closed" | "io";

/** A storage-layer failure. Messages carry no row bytes or keys. */
export class StorageError extends Error {
  readonly kind: StorageErrorKind;

  constructor(kind: StorageErrorKind) {
    super(`storage error: ${kind}`);
    this.name = "StorageError";
    this.kind = kind;
  }
}
