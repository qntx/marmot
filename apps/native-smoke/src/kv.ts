/**
 * op-sqlite → `SqlitePort`: the `DB` returned by `open` is structurally the port (`execute` +
 * `transaction`); this adapter exists to normalize the row-value typing and to keep the open()
 * call site in one place. `dbName` maps to a file in the app's own sandbox, durable across
 * process kills.
 */

import { open } from "@op-engineering/op-sqlite";

import type { SqlitePort, SqliteValue } from "@qntx/marmot/storage";

export function openDb(name: string): SqlitePort {
  const db = open({ name: `${name}.db` });
  return {
    execute: async (sql: string, params?: SqliteValue[]) => {
      const result = await db.execute(sql, params ?? []);
      return { rows: result.rows as Array<Record<string, unknown>> };
    },
    transaction: (body) => db.transaction(body),
  };
}
