/**
 * `sqliteKv` — a `KvDriver` over a minimal SQLite surface (op-sqlite's `DB`, a `node:sqlite` or
 * `bun:sqlite` adapter) with an injected cipher and lock. Same contract as `indexedDbKv`: row
 * values are sealed before they reach the store, the `marmot:<name>` lock is required, and the
 * driver is terminal after a failed commit.
 *
 * The database handle is host-owned: `close()` waits for the commit chain and releases the lock but
 * never closes the database.
 */

import { decodeKvBatch } from "../backend/kv.ts";
import type { Rows } from "../backend/kv.ts";
import { CommitChain } from "./commit-chain.ts";
import { StorageError } from "./driver.ts";
import type { Cipher, KvDriver, LockPort } from "./driver.ts";

/** A bind parameter the SQLite surface accepts. */
// oxlint-disable-next-line no-restricted-types -- SQLite parameters may bind NULL
export type SqliteValue = Uint8Array | string | number | null;

/** One statement execution. */
export type SqliteExecutor = {
  execute: (
    sql: string,
    params?: SqliteValue[],
  ) => Promise<{ rows: Array<Record<string, unknown>> }>;
};

/** The minimal SQLite surface `sqliteKv` drives. */
export type SqlitePort = SqliteExecutor & {
  /** Run `body` in one transaction: commit when it resolves, roll back when it rejects. */
  transaction: (body: (tx: SqliteExecutor) => Promise<void>) => Promise<void>;
};

export type SqliteKvOptions = {
  /** The store name; the driver acquires the `marmot:<name>` lock. */
  name: string;
  /** Required: acquires `marmot:<name>`; a second holder sees `locked`. */
  lock: LockPort;
  /** Required: every stored value passes through the cipher. */
  cipher: Cipher;
};

function toBytes(value: unknown): Uint8Array | undefined {
  if (value instanceof Uint8Array) {
    return value;
  }
  if (value instanceof ArrayBuffer) {
    return new Uint8Array(value);
  }
  return undefined;
}

/** Open the `marmot_rows` table in `db` behind the `marmot:<name>` lock. */
export async function sqliteKv(db: SqlitePort, options: SqliteKvOptions): Promise<KvDriver> {
  const release = await options.lock.acquire(`marmot:${options.name}`);
  if (release === undefined) {
    throw new StorageError("locked");
  }
  try {
    await db.execute("PRAGMA synchronous = FULL");
    await db.execute(
      "CREATE TABLE IF NOT EXISTS marmot_rows (key BLOB PRIMARY KEY NOT NULL, value BLOB NOT NULL) WITHOUT ROWID",
    );
  } catch {
    release();
    throw new StorageError("io");
  }
  return new SqliteKv(db, options.cipher, release);
}

class SqliteKv implements KvDriver {
  private readonly db: SqlitePort;
  private readonly cipher: Cipher;
  private readonly release: () => void;
  private readonly chain = new CommitChain();

  constructor(db: SqlitePort, cipher: Cipher, release: () => void) {
    this.db = db;
    this.cipher = cipher;
    this.release = release;
  }

  async load(): Promise<Rows> {
    if (this.chain.isClosed) {
      throw new StorageError("closed");
    }
    let rows: Array<Record<string, unknown>>;
    try {
      ({ rows } = await this.db.execute("SELECT key, value FROM marmot_rows"));
    } catch {
      throw new StorageError("io");
    }
    const pairs = rows.map((row) => {
      const key = toBytes(row["key"]);
      const sealed = toBytes(row["value"]);
      if (key === undefined || sealed === undefined) {
        throw new StorageError("corrupt_row");
      }
      return [key, sealed] as const;
    });
    const plains = await Promise.all(
      pairs.map(async ([key, sealed]) => {
        try {
          return await this.cipher.open(key, sealed);
        } catch {
          throw new StorageError("undecryptable_row");
        }
      }),
    );
    const out: Array<[Uint8Array, Uint8Array]> = [];
    for (const [i, [key]] of pairs.entries()) {
      const plain = plains[i];
      if (plain === undefined) {
        throw new StorageError("corrupt_row");
      }
      out.push([key, plain]);
    }
    return out;
  }

  async commit(batch: Uint8Array): Promise<void> {
    return this.chain.commit(batch, async (bytes) => this.apply(bytes));
  }

  /**
   * Seal every put value _before_ opening the transaction, then apply the ops in one
   * `db.transaction` so a batch lands atomically.
   */
  private async apply(batch: Uint8Array): Promise<void> {
    const decoded = decodeKvBatch(batch);
    const sealed = await Promise.all(
      decoded.ops.map(async (op) =>
        op.type === "put" ? this.cipher.seal(op.key, op.value) : undefined,
      ),
    );
    try {
      await this.db.transaction(async (tx) => {
        for (const [i, op] of decoded.ops.entries()) {
          const value = sealed[i];
          if (op.type === "put" && value !== undefined) {
            // oxlint-disable-next-line no-await-in-loop -- the ops run strictly in order inside one transaction
            await tx.execute("INSERT OR REPLACE INTO marmot_rows (key, value) VALUES (?, ?)", [
              op.key,
              value,
            ]);
          } else if (op.type === "delete") {
            // oxlint-disable-next-line no-await-in-loop -- the ops run strictly in order inside one transaction
            await tx.execute("DELETE FROM marmot_rows WHERE key = ?", [op.key]);
          }
        }
      });
    } catch (error) {
      throw error instanceof StorageError ? error : new StorageError("io");
    }
  }

  async close(): Promise<void> {
    if (this.chain.isClosed) {
      return;
    }
    await this.chain.close();
    this.release();
  }
}
