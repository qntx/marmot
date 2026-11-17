/**
 * `indexedDbKv` — a `KvDriver` over IndexedDB with an injected cipher and lock. Row values are
 * sealed before they reach the store; keys are stored as out-of-line binary. Both options are
 * required: there is no silent plaintext default.
 */

import { decodeKvBatch } from "../backend/kv.ts";
import type { Rows } from "../backend/kv.ts";
import { StorageError } from "./driver.ts";
import type { Cipher, KvDriver, LockPort } from "./driver.ts";

/* Minimal structural IndexedDB types — declared locally so `src` needs no
 * DOM lib; real `IDB*` objects satisfy these shapes. */

type IdbRequestLike<T, E extends string = "success" | "error"> = {
  readonly result: T;
  addEventListener: (type: E, listener: () => void) => void;
};

type IdbOpenRequestLike = IdbRequestLike<IdbDatabaseLike, "success" | "error" | "upgradeneeded">;

type IdbTransactionLike = {
  objectStore: (name: string) => IdbObjectStoreLike;
  addEventListener: (type: "complete" | "error" | "abort", listener: () => void) => void;
};

type IdbObjectStoreLike = {
  getAll: () => IdbRequestLike<unknown[]>;
  getAllKeys: () => IdbRequestLike<unknown[]>;
  put: (value: unknown, key: unknown) => IdbRequestLike<unknown>;
  delete: (key: unknown) => IdbRequestLike<unknown>;
};

type IdbDatabaseLike = {
  transaction: (
    store: string,
    mode: "readonly" | "readwrite",
    options?: { durability?: string },
  ) => IdbTransactionLike;
  createObjectStore: (name: string) => unknown;
  close: () => void;
};

/** What `indexedDbKv` needs of an `IDBFactory` (or the fake in tests). */
export type IdbFactoryLike = {
  open: (name: string, version?: number) => IdbOpenRequestLike;
};

export type IndexedDbKvOptions = {
  /** Required: acquires `marmot:<name>`; a second holder sees `locked`. */
  lock: LockPort;
  /** Required: every stored value passes through the cipher. */
  cipher: Cipher;
};

const STORE = "rows";

async function requestOnce<T>(request: IdbRequestLike<T>): Promise<T> {
  return new Promise<T>((resolve, reject) => {
    request.addEventListener("success", () => {
      resolve(request.result);
    });
    request.addEventListener("error", () => {
      reject(new StorageError("io"));
    });
  });
}

async function openDatabase(factory: IdbFactoryLike, name: string): Promise<IdbDatabaseLike> {
  return new Promise<IdbDatabaseLike>((resolve, reject) => {
    const request = factory.open(name, 1);
    request.addEventListener("upgradeneeded", () => {
      request.result.createObjectStore(STORE);
    });
    request.addEventListener("success", () => {
      resolve(request.result);
    });
    request.addEventListener("error", () => {
      reject(new StorageError("io"));
    });
  });
}

function idbKey(key: Uint8Array): Uint8Array {
  return key;
}

function toBytes(value: unknown): Uint8Array | undefined {
  if (value instanceof Uint8Array) {
    return value;
  }
  if (value instanceof ArrayBuffer) {
    return new Uint8Array(value);
  }
  return undefined;
}

async function txnDone(txn: IdbTransactionLike): Promise<void> {
  return new Promise((resolve, reject) => {
    txn.addEventListener("complete", resolve);
    txn.addEventListener("error", () => {
      reject(new StorageError("io"));
    });
    txn.addEventListener("abort", () => {
      reject(new StorageError("io"));
    });
  });
}

/** Open (creating if needed) the `rows` store in database `name` behind the `marmot:<name>` lock. */
export async function indexedDbKv(
  factory: IdbFactoryLike,
  name: string,
  options: IndexedDbKvOptions,
): Promise<KvDriver> {
  const release = await options.lock.acquire(`marmot:${name}`);
  if (release === undefined) {
    throw new StorageError("locked");
  }
  let db: IdbDatabaseLike;
  try {
    db = await openDatabase(factory, name);
  } catch (error) {
    release();
    throw error;
  }
  return new IndexedDbKv(db, options.cipher, release);
}

class IndexedDbKv implements KvDriver {
  private readonly db: IdbDatabaseLike;
  private readonly cipher: Cipher;
  private readonly release: () => void;
  private chain: Promise<void> = Promise.resolve();
  private poisoned = false;
  private closed = false;

  constructor(db: IdbDatabaseLike, cipher: Cipher, release: () => void) {
    this.db = db;
    this.cipher = cipher;
    this.release = release;
  }

  async load(): Promise<Rows> {
    this.requireOpen();
    const txn = this.db.transaction(STORE, "readonly");
    const store = txn.objectStore(STORE);
    const [keys, sealed] = await Promise.all([
      requestOnce(store.getAllKeys()),
      requestOnce(store.getAll()),
    ]);
    const pairs = keys.map((key, i) => {
      const rowKey = toBytes(key);
      const rowValue = toBytes(sealed[i]);
      if (rowKey === undefined || rowValue === undefined) {
        throw new StorageError("corrupt_row");
      }
      return [rowKey, rowValue] as const;
    });
    const plains = await Promise.all(
      pairs.map(async ([, value]) => {
        try {
          return await this.cipher.open(value);
        } catch {
          throw new StorageError("undecryptable_row");
        }
      }),
    );
    const rows: Array<[Uint8Array, Uint8Array]> = [];
    for (const [i, [key]] of pairs.entries()) {
      const plain = plains[i];
      if (plain === undefined) {
        throw new StorageError("corrupt_row");
      }
      rows.push([key, plain]);
    }
    return rows;
  }

  async commit(batch: Uint8Array): Promise<void> {
    if (this.closed) {
      throw new StorageError("closed");
    }
    const prior = this.chain;
    const run = (async () => {
      await prior;
      // Queued before close(): `close` waits on `chain`, so the batch still applies —
      // unless an earlier commit failed. The host contract is terminal: a lost batch
      // may be a dependency of every later one, so nothing applies after a failure.
      if (this.poisoned) {
        throw new StorageError("io");
      }
      try {
        await this.apply(batch);
      } catch (error) {
        this.poisoned = true;
        throw error instanceof StorageError ? error : new StorageError("io");
      }
    })();
    this.chain = (async () => {
      try {
        await run;
      } catch {
        // The rejection is delivered to the caller; the chain moves on.
      }
    })();
    return run;
  }

  /**
   * Seal every put value _before_ opening the transaction — IDB txns auto-commit across awaits —
   * then apply the ops in one `readwrite` txn.
   */
  private async apply(batch: Uint8Array): Promise<void> {
    const decoded = decodeKvBatch(batch);
    const sealed = await Promise.all(
      decoded.ops.map(async (op) => (op.type === "put" ? this.cipher.seal(op.value) : undefined)),
    );
    const txn = this.db.transaction(STORE, "readwrite", { durability: "strict" });
    const store = txn.objectStore(STORE);
    for (const [i, op] of decoded.ops.entries()) {
      const value = sealed[i];
      if (op.type === "put" && value !== undefined) {
        store.put(value, idbKey(op.key));
      } else if (op.type === "delete") {
        store.delete(idbKey(op.key));
      }
    }
    await txnDone(txn);
  }

  async close(): Promise<void> {
    if (this.closed) {
      return;
    }
    this.closed = true;
    await this.chain;
    this.db.close();
    this.release();
  }

  private requireOpen(): void {
    if (this.closed) {
      throw new StorageError("closed");
    }
  }
}
