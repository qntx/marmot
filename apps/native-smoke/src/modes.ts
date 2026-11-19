/**
 * Mode bodies for the deep-link driver. `flow` posts its result and returns; `kill` runs forever
 * (the host kills the process); `verify` dumps decrypted rows, proves Engine recovery and posts.
 */

import { Engine } from "@qntx/marmot/backend";
import type { KvDriver } from "@qntx/marmot/storage";
import { localLocks, sqliteKv } from "@qntx/marmot/storage";
import { fakeRelay, manualClock, runFlow } from "@qntx/marmot/testing";
import type { FakeRelay } from "@qntx/marmot/testing";
import { load } from "@qntx/marmot-native";

import { checkingBackend } from "./backend-check.ts";
import type { BackendCheck } from "./backend-check.ts";
import type { RunParams } from "./params.ts";
import { chachaCipher } from "./cipher.ts";
import { post } from "./collector.ts";
import { b64encode, hexToBytes } from "./b64.ts";
import { openDb } from "./kv.ts";
import { recordingKv } from "./kill-records.ts";
import { testSigner } from "./signer.ts";

const RELAY = "wss://relay.example";

/** Publish everything the engine has gated, journaling each publish before the relay sees it. */
async function publishAll(
  engine: Engine,
  relay: FakeRelay,
  run: string,
  acked?: () => bigint,
): Promise<void> {
  for (let round = 0; round < 64; round += 1) {
    // oxlint-disable-next-line no-await-in-loop -- sequential by design
    await engine.whenDurable();
    // oxlint-disable-next-line no-await-in-loop -- sequential by design
    const items = await engine.takeOutbound();
    if (items.length === 0) {
      return;
    }
    for (const item of items) {
      // oxlint-disable-next-line no-await-in-loop -- sequential by design
      await post(`/${run}/journal`, {
        kind: "publish",
        id: item.id,
        eventId: item.event.id,
        watermark: (acked?.() ?? 0n).toString(),
      });
      const results = relay.publish(item);
      // oxlint-disable-next-line no-await-in-loop -- sequential by design
      await engine.reportPublish(item.id, results);
    }
  }
  throw new Error("publishAll did not settle");
}

function memberKv(run: string): (name: string) => Promise<KvDriver> {
  const key = crypto.getRandomValues(new Uint8Array(32));
  const cipher = chachaCipher(key);
  const lock = localLocks();
  const drivers = new Map<string, KvDriver>();
  return async (name) => {
    // The engine never owns kv.close: a reopen asks the factory for the same name again, so
    // the prior driver must be closed (and its lock released) here before re-acquiring it.
    await drivers.get(name)?.close();
    const driver = await sqliteKv(openDb(`${run}-${name}`), {
      name: `${run}-${name}`,
      lock,
      cipher,
    });
    drivers.set(name, driver);
    return driver;
  };
}

function checked(run: string): BackendCheck {
  return checkingBackend(load(), (event) => post(`/${run}/journal`, event));
}

function failOnViolations(check: BackendCheck): void {
  const violations = check.violations();
  if (violations.length > 0) {
    throw new Error(`backend contract violations: ${violations.join("; ")}`);
  }
}

/** `flow`: the shared scenario over the real native backend + sqliteKv, checked end to end. */
export async function runFlowMode(params: RunParams): Promise<Record<string, unknown>> {
  const check = checked(params.run);
  const timings = await runFlow({
    backend: check.backend,
    kv: memberKv(params.run),
    signer: () => testSigner(),
    messages: params.messages,
  });
  failOnViolations(check);
  return { timings };
}

/**
 * `kill`: engine on a fresh per-iteration sqlite db, one group, send+publish forever. The journal
 * is the contract — the host SIGKILLs the process once the first batch lands.
 */
export async function runKillMode(params: RunParams): Promise<never> {
  const key = hexToBytes(params.key ?? "");
  const secret = hexToBytes(params.secret ?? "");
  const cipher = chachaCipher(key);
  const { kv, acked } = recordingKv(
    await sqliteKv(openDb(params.db), { name: params.db, lock: localLocks(), cipher }),
    params.run,
  );
  const relay = fakeRelay();
  const clock = manualClock();
  const check = checked(params.run);
  const engine = await Engine.open({
    backend: check.backend,
    kv,
    clock,
    signer: testSigner(secret),
    allowLoopback: true,
  });
  const group = await engine.createGroup({
    name: "kill",
    description: "",
    relays: [RELAY],
    invitees: [],
  });
  await publishAll(engine, relay, params.run, acked);
  for (let i = 0; ; i += 1) {
    clock.advance(1);
    // oxlint-disable-next-line no-await-in-loop -- the kill loop is sequential forever
    await engine.sendMessage(group.id, { kind: 9, tags: [], content: `k${i}` });
    // oxlint-disable-next-line no-await-in-loop -- the kill loop is sequential forever
    await publishAll(engine, relay, params.run, acked);
  }
}

/** `verify` on a killed db: decrypted row dump, then Engine must open over the same rows. */
export async function runVerifyMode(params: RunParams): Promise<Record<string, unknown>> {
  const key = hexToBytes(params.key ?? "");
  const secret = hexToBytes(params.secret ?? "");
  const cipher = chachaCipher(key);
  const db = openDb(params.db);
  const result = await db.execute("SELECT key, value FROM marmot_rows");
  const rows: Array<{ key: string; value: string }> = [];
  for (const row of result.rows) {
    const rowKey = toBytes(row["key"]);
    const sealed = toBytes(row["value"]);
    // oxlint-disable-next-line no-await-in-loop -- decrypt serially, one row at a time
    const plain = await cipher.open(rowKey, sealed);
    rows.push({ key: b64encode(rowKey), value: b64encode(plain) });
  }
  await post(`/${params.run}/journal`, { kind: "dump", rows });
  const check = checked(params.run);
  const kv = await sqliteKv(db, { name: params.db, lock: localLocks(), cipher });
  const engine = await Engine.open({
    backend: check.backend,
    kv,
    clock: manualClock(),
    signer: testSigner(secret),
    allowLoopback: true,
  });
  await engine.close();
  await kv.close();
  failOnViolations(check);
  return { rows: rows.length };
}

function toBytes(value: unknown): Uint8Array {
  if (value instanceof Uint8Array) {
    return value;
  }
  if (value instanceof ArrayBuffer) {
    return new Uint8Array(value);
  }
  throw new Error("marmot_rows holds a non-binary column");
}
