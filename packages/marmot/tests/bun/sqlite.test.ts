import { Database } from "bun:sqlite";
import { describe, expect, test } from "bun:test";

import { encodeKvBatch } from "../../src/backend/kv.ts";
import type { KvDriver } from "../../src/storage/driver.ts";
import { localLocks } from "../../src/storage/locks.ts";
import { sqliteKv } from "../../src/storage/sqlite.ts";
import type { SqliteExecutor, SqlitePort } from "../../src/storage/sqlite.ts";
import { capture, cipher, fakeLock, kindOf, kvDriverContract } from "../kv-contract.ts";

/* A `SqlitePort` over `bun:sqlite`'s `Database` — the same adapter shape a
 * `node:sqlite` `DatabaseSync` host would use: `execute` prepares and reads
 * all rows; `transaction` wraps the body in BEGIN IMMEDIATE/COMMIT/ROLLBACK.
 * The db handle is host-owned: the map keeps one in-memory database per
 * name so a driver `close`/`open` cycle sees the same store. */

const databases = new Map<string, Database>();

function dbFor(name: string): Database {
  let db = databases.get(name);
  if (db === undefined) {
    db = new Database(":memory:");
    databases.set(name, db);
  }
  return db;
}

function executor(db: Database): SqliteExecutor {
  return {
    execute: async (sql, params = []) => {
      await Promise.resolve();
      const rows = db.query(sql).all(...params) as Array<Record<string, unknown>>;
      return { rows };
    },
  };
}

function sqlitePort(db: Database): SqlitePort {
  const base = executor(db);
  return {
    ...base,
    transaction: async (body) => {
      db.exec("BEGIN IMMEDIATE");
      try {
        await body(base);
        db.exec("COMMIT");
      } catch (error) {
        db.exec("ROLLBACK");
        throw error;
      }
    },
  };
}

/** A recording adapter: remembers every statement the driver issues. */
function recordingPort(db: Database): SqlitePort & { statements: string[] } {
  const statements: string[] = [];
  const base = executor(db);
  return {
    statements,
    execute: async (sql, params) => {
      statements.push(sql);
      return base.execute(sql, params);
    },
    transaction: async (body) => {
      db.exec("BEGIN IMMEDIATE");
      try {
        await body({
          execute: async (sql, params) => {
            statements.push(sql);
            return base.execute(sql, params);
          },
        });
        db.exec("COMMIT");
      } catch (error) {
        db.exec("ROLLBACK");
        throw error;
      }
    },
  };
}

/** A port whose transaction executor throws a plain error on its `nth` execute. */
function flakyTxPort(db: Database, nth: number): SqlitePort & { txLog: string[] } {
  const txLog: string[] = [];
  const base = executor(db);
  return {
    txLog,
    ...base,
    transaction: async (body) => {
      db.exec("BEGIN IMMEDIATE");
      txLog.push("BEGIN");
      let calls = 0;
      const tx: SqliteExecutor = {
        execute: async (sql, params) => {
          calls += 1;
          if (calls === nth) {
            throw new Error("disk full");
          }
          return base.execute(sql, params);
        },
      };
      try {
        await body(tx);
        db.exec("COMMIT");
        txLog.push("COMMIT");
      } catch (error) {
        db.exec("ROLLBACK");
        txLog.push("ROLLBACK");
        throw error;
      }
    },
  };
}

/* op-sqlite 19.0.5's shapes, copied verbatim — the package never imports a
 * SQLite library; this type-level check keeps `SqlitePort` compatible. */
type OpScalar = string | number | boolean | null | ArrayBuffer | Uint8Array;
type OpQueryResult = { rows: Array<Record<string, OpScalar>> };
type OpTransaction = {
  execute: (query: string, params?: OpScalar[]) => Promise<OpQueryResult>;
};
type OpSqliteDb = OpTransaction & {
  transaction: (fn: (tx: OpTransaction) => Promise<void>) => Promise<void>;
};

kvDriverContract({ describe, test, expect }, "sqliteKv", {
  open: async (name, options) => sqliteKv(sqlitePort(dbFor(name)), { name, ...options }),
  rawValues: async (name) => {
    await Promise.resolve();
    const rows = dbFor(name).query("SELECT value FROM marmot_rows ORDER BY key").all() as Array<{
      value: unknown;
    }>;
    return rows.map((row) => row.value);
  },
  rawPut: async (name, key, value) => {
    await Promise.resolve();
    dbFor(name).run("INSERT OR REPLACE INTO marmot_rows (key, value) VALUES (?, ?)", key, value);
  },
  corruptValue: "a-string-value",
});

describe("sqliteKv", () => {
  test("PRAGMA synchronous = FULL and the marmot_rows table are set at open", async () => {
    const name = "pragma-1";
    const port = recordingPort(dbFor(name));
    const kv = await sqliteKv(port, { name, lock: fakeLock().port, cipher });
    expect(port.statements.slice(0, 2)).toStrictEqual([
      "PRAGMA synchronous = FULL",
      "CREATE TABLE IF NOT EXISTS marmot_rows (key BLOB PRIMARY KEY NOT NULL, value BLOB NOT NULL) WITHOUT ROWID",
    ]);
    await kv.close();
  });

  test("one transaction per batch: a failing statement rolls every op back", async () => {
    const name = "atomic-1";
    const port = flakyTxPort(dbFor(name), 2);
    const kv = await sqliteKv(port, { name, lock: fakeLock().port, cipher });
    const batch = encodeKvBatch({
      seq: 1n,
      ops: [
        { type: "put", key: Uint8Array.of(1), value: Uint8Array.of(10) },
        { type: "put", key: Uint8Array.of(2), value: Uint8Array.of(20) },
      ],
    });
    const outcome = await capture(kv.commit(batch));
    expect(kindOf(outcome)).toBe("io");
    expect(port.txLog).toStrictEqual(["BEGIN", "ROLLBACK"]);
    const rows = dbFor(name).query("SELECT key FROM marmot_rows").all();
    expect(rows).toHaveLength(0);
    await kv.close();
  });

  test("close does not close the host-owned database", async () => {
    const name = "host-owned-1";
    const db = dbFor(name);
    const kv = await sqliteKv(sqlitePort(db), { name, lock: fakeLock().port, cipher });
    await kv.close();
    // The database still answers queries: the host, not the driver, owns it.
    const rows = db.query("SELECT count(*) AS n FROM marmot_rows").all() as Array<{ n: number }>;
    expect(rows[0]?.n).toBe(0);
  });

  test("localLocks grants one holder per name and frees it on release", async () => {
    const name = "locks-1";
    const locks = localLocks();
    const kv = await sqliteKv(sqlitePort(dbFor(name)), { name, lock: locks, cipher });
    const second = await capture(
      sqliteKv(sqlitePort(dbFor(name)), {
        name,
        lock: locks,
        cipher,
      }),
    );
    expect(kindOf(second)).toBe("locked");
    await kv.close();
    const again: KvDriver = await sqliteKv(sqlitePort(dbFor(name)), {
      name,
      lock: locks,
      cipher,
    });
    await again.close();
  });

  test("op-sqlite's DB shape is assignable to SqlitePort", () => {
    const db = null as unknown as OpSqliteDb;
    const port: SqlitePort = db;
    expect(port).toBe(db);
  });
});
