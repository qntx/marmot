/// <reference lib="dom" />
/// <reference lib="dom.iterable" />
/**
 * Browser harness for the Playwright suites. Bundled with Bun.build, served as `/harness.js`, and
 * driven through `window.__marmot`. Everything here is test code: the AES-GCM cipher is
 * deliberately simple (random IV per value, IV ‖ ciphertext) and the Node-facing callbacks are
 * injected by `page.exposeFunction`.
 */

import { Engine, decodeKvBatch } from "@qntx/marmot/backend";
import type { Backend } from "@qntx/marmot/backend";
import { indexedDbKv, webLocks } from "@qntx/marmot/storage";
import type { Cipher, IdbFactoryLike, KvDriver, LockManagerLike } from "@qntx/marmot/storage";
import { fakeRelay, manualClock, memoryKv, runFlow } from "@qntx/marmot/testing";
import type { FakeRelay } from "@qntx/marmot/testing";

/**
 * The packed loader+glue, reached at runtime as `/marmot.mjs` (served from the `dist/` bundle
 * `e2e/build.ts` emits). The glue and the `.wasm` are a build-matched pair — wasm-bindgen shim
 * names are build-local — so the bundle must execute the shipped glue, not the committed source
 * copy. The runtime import keeps lint and typecheck independent of `dist/`.
 */
import type { load } from "../../src/index.ts";
import { testSigner } from "../../tests/signer.ts";
import type { Harness } from "../types.ts";

type MarmotWasmModule = { load: typeof load };

let loader: Promise<MarmotWasmModule> | undefined;
async function wasmModule(): Promise<MarmotWasmModule> {
  const { href } = new URL("marmot.mjs", location.href);
  loader ??=
    // oxlint-disable-next-line no-unsafe-type-assertion -- the served module is packed src/index.ts
    import(href) as Promise<MarmotWasmModule>;
  return loader;
}

const RELAY = "wss://relay.example";
const STORE = "rows";

function b64encode(bytes: Uint8Array): string {
  let text = "";
  for (const byte of bytes) {
    text += String.fromCodePoint(byte);
  }
  return btoa(text);
}

function b64decode(text: string): Uint8Array {
  const raw = atob(text);
  const out = new Uint8Array(raw.length);
  for (let i = 0; i < raw.length; i += 1) {
    out[i] = raw.codePointAt(i) ?? 0;
  }
  return out;
}

/**
 * Test cipher: AES-GCM with a fresh 96-bit IV per value, the row key as `additionalData`; stored as
 * IV ‖ ciphertext.
 */
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
      const iv = new Uint8Array(sealed.subarray(0, 12));
      return new Uint8Array(
        await crypto.subtle.decrypt(
          { name: "AES-GCM", iv, additionalData: new Uint8Array(rowKey) },
          await key,
          new Uint8Array(sealed.subarray(12)),
        ),
      );
    },
  };
}

let backend: Backend | undefined;
let aesKey: Uint8Array | undefined;
let pageSigner = testSigner();

function requireBackend(): Backend {
  if (backend === undefined) {
    throw new Error("harness.init first");
  }
  return backend;
}

/* The DOM typings for `indexedDB`/`navigator.locks` are wider than the structural ports the
 * driver declares — narrow them explicitly at this one boundary. */
// oxlint-disable-next-line no-unsafe-type-assertion
const IDB = indexedDB as unknown as IdbFactoryLike;
// oxlint-disable-next-line no-unsafe-type-assertion
const LOCKS = navigator.locks as unknown as LockManagerLike;

async function idbKv(name: string): Promise<KvDriver> {
  if (aesKey === undefined) {
    throw new Error("harness.init first");
  }
  return indexedDbKv(IDB, name, {
    lock: webLocks(LOCKS),
    cipher: aesGcmCipher(aesKey),
  });
}

/** Publish everything the engine has gated, recording each publish through the Node hook. */
async function publishAll(engine: Engine, relay: FakeRelay, acked?: () => bigint): Promise<void> {
  for (let round = 0; round < 64; round += 1) {
    // oxlint-disable-next-line no-await-in-loop
    await engine.whenDurable();
    // oxlint-disable-next-line no-await-in-loop
    const items = await engine.takeOutbound();
    if (items.length === 0) {
      return;
    }
    for (const item of items) {
      // oxlint-disable-next-line no-await-in-loop
      await globalThis.publishOutbound?.(item.id, item.event.id, (acked?.() ?? 0n).toString());
      const results = relay.publish(item);
      // oxlint-disable-next-line no-await-in-loop
      await engine.reportPublish(item.id, results);
    }
  }
  throw new Error("publishAll did not settle");
}

/**
 * A KvDriver wrapper that reports every commit to Node: `recordBatch` before the inner commit,
 * `recordAck` after it resolves — both through a serial chain so Node sees commit order.
 */
function recordingKv(inner: KvDriver): { kv: KvDriver; acked: () => bigint } {
  let lastAcked = 0n;
  let chain = Promise.resolve();
  return {
    acked: () => lastAcked,
    kv: {
      load: async () => inner.load(),
      close: async () => inner.close(),
      commit: async (batch) => {
        // A serial chain, not an inline await: Node must observe commits in order.
        // oxlint-disable-next-line prefer-await-to-then, always-return
        chain = chain.then(async () => {
          const { seq } = decodeKvBatch(batch);
          await globalThis.recordBatch?.(seq.toString(), b64encode(batch));
          await inner.commit(batch);
          await globalThis.recordAck?.(seq.toString());
          if (seq > lastAcked) {
            lastAcked = seq;
          }
        });
        return chain;
      },
    },
  };
}

type EngineState = {
  engine: Engine;
  relay: FakeRelay;
  clock: ReturnType<typeof manualClock>;
  driver: KvDriver;
};

const state = { current: undefined as EngineState | undefined };

async function openEngine(dbName: string, kv?: KvDriver): Promise<EngineState> {
  const relay = fakeRelay();
  const clock = manualClock();
  const driver = kv ?? (await idbKv(dbName));
  const engine = await Engine.open({
    backend: requireBackend(),
    kv: driver,
    clock,
    signer: pageSigner,
    allowLoopback: true,
  });
  state.current = { engine, relay, clock, driver };
  return state.current;
}

const idbGet = async <T>(request: IDBRequest<T>): Promise<T> =>
  new Promise<T>((resolve, reject) => {
    request.addEventListener("success", () => {
      resolve(request.result);
    });
    request.addEventListener("error", () => {
      reject(new Error("idb read failed"));
    });
  });

/** Read every row of database `name` straight from IndexedDB (bypasses the lock). */
async function rawStore(name: string): Promise<{ keys: unknown[]; values: unknown[] }> {
  const db = await new Promise<IDBDatabase>((resolve, reject) => {
    const request = indexedDB.open(name, 1);
    request.addEventListener("success", () => {
      resolve(request.result);
    });
    request.addEventListener("error", () => {
      reject(new Error("idb open failed"));
    });
  });
  try {
    const txn = db.transaction(STORE, "readonly");
    const store = txn.objectStore(STORE);
    const keys = await idbGet(store.getAllKeys());
    const values = await idbGet(store.getAll());
    return { keys, values };
  } finally {
    db.close();
  }
}

function toBytes(value: unknown): Uint8Array {
  if (value instanceof Uint8Array) {
    return value;
  }
  if (value instanceof ArrayBuffer) {
    return new Uint8Array(value);
  }
  if (ArrayBuffer.isView(value)) {
    return new Uint8Array(value.buffer, value.byteOffset, value.byteLength);
  }
  throw new Error("unexpected idb key/value shape");
}

const api: Harness = {
  /** Fetch + instantiate the wasm; installs the injected AES key and account secret. */
  async init(
    keyBase64: string,
    secretBase64: string,
  ): Promise<{ fetchMs: number; instantiateMs: number }> {
    aesKey = b64decode(keyBase64);
    pageSigner = testSigner(b64decode(secretBase64));
    const t0 = performance.now();
    const response = await fetch("/marmot_wasm_bg.wasm");
    const bytes = new Uint8Array(await response.arrayBuffer());
    const t1 = performance.now();
    const module_ = await wasmModule();
    backend = await module_.load(bytes);
    const t2 = performance.now();
    return { fetchMs: t1 - t0, instantiateMs: t2 - t1 };
  },

  /** RunFlow with indexedDbKv per member (each member its own database). */
  async flow(messages) {
    const drivers = new Map<string, Promise<KvDriver>>();
    try {
      return await runFlow({
        backend: requireBackend(),
        kv: async (name) => {
          let driver = drivers.get(name);
          if (driver === undefined) {
            driver = idbKv(`flow-${name}`);
            drivers.set(name, driver);
          }
          return driver;
        },
        signer: testSigner,
        messages,
      });
    } finally {
      for (const driver of drivers.values()) {
        // oxlint-disable-next-line no-await-in-loop
        const resolved = await driver;
        // oxlint-disable-next-line no-await-in-loop
        await resolved.close();
      }
    }
  },

  /** Try `indexedDbKv(name)`; resolves "ok" (driver closed) or the StorageError kind. */
  async kvOpen(name) {
    try {
      const driver = await idbKv(name);
      await driver.close();
      return "ok";
    } catch (error) {
      if (error instanceof Error && "kind" in error && typeof error.kind === "string") {
        return error.kind;
      }
      throw error;
    }
  },

  /** Open an Engine on `dbName` (IndexedDB + lock + cipher); keeps it in state. */
  async openEngine(dbName) {
    if (state.current !== undefined) {
      throw new Error("engine already open");
    }
    await openEngine(dbName);
  },

  /** Create a group (no invitees), publish its fanout; returns the group id. */
  async createGroup(name) {
    const { current } = state;
    if (current === undefined) {
      throw new Error("openEngine first");
    }
    const group = await current.engine.createGroup({
      name,
      description: "",
      relays: [RELAY],
      invitees: [],
    });
    await publishAll(current.engine, current.relay);
    return group.id;
  },

  /** Send one message to `groupId` and publish the resulting outbound. */
  async sendOnce(groupId, text) {
    const { current } = state;
    if (current === undefined) {
      throw new Error("openEngine first");
    }
    current.clock.advance(1000);
    await current.engine.sendMessage(groupId, { kind: 9, tags: [], content: text });
    await publishAll(current.engine, current.relay);
  },

  /** Close the current engine + its driver (releases the Web Lock). */
  async closeEngine() {
    const { current } = state;
    if (current === undefined) {
      return;
    }
    state.current = undefined;
    await current.engine.close();
    // Engine.close releases the bridge handle; the driver is ours — close it to drop the lock.
    await current.driver.close();
  },

  /** Close, then reopen on the same database. */
  async reopenEngine(dbName) {
    await api.closeEngine();
    await api.openEngine(dbName);
  },

  /**
   * The kill-during-write loop: engine on `dbName`, one group, then send+publish forever. Returns
   * once the loop has started; the page dying is the stop condition. The `recordingKv` wrapper
   * streams every batch and ack to Node; every publish reports its watermark.
   */
  async startKillLoop(dbName) {
    const { kv, acked } = recordingKv(await idbKv(dbName));
    const current = await openEngine(dbName, kv);
    const group = await current.engine.createGroup({
      name: "kill",
      description: "",
      relays: [RELAY],
      invitees: [],
    });
    await publishAll(current.engine, current.relay, acked);
    const loop = (async () => {
      for (let i = 0; ; i += 1) {
        current.clock.advance(1);
        // oxlint-disable-next-line no-await-in-loop
        await current.engine.sendMessage(group.id, { kind: 9, tags: [], content: `k${i}` });
        // oxlint-disable-next-line no-await-in-loop
        await publishAll(current.engine, current.relay, acked);
      }
    })();
    void loop;
  },

  /** Decrypted rows of database `name` as base64 pairs, read straight from IndexedDB. */
  async dumpRows(name) {
    if (aesKey === undefined) {
      throw new Error("harness.init first");
    }
    const { keys, values } = await rawStore(name);
    const cipher = aesGcmCipher(aesKey);
    const rows: Array<{ key: string; value: string }> = [];
    for (let i = 0; i < keys.length; i += 1) {
      // oxlint-disable-next-line no-await-in-loop
      const plain = await cipher.open(toBytes(keys[i]), toBytes(values[i]));
      rows.push({ key: b64encode(toBytes(keys[i])), value: b64encode(plain) });
    }
    return rows;
  },

  /** Raw (sealed) store statistics for `name`: row count and total stored bytes. */
  async dbStats(name) {
    const { keys, values } = await rawStore(name);
    let bytes = 0;
    for (const value of values) {
      bytes += toBytes(value).length;
    }
    return { rows: keys.length, bytes };
  },

  /**
   * `count` groups × `messages` messages: the receiver is IDB-backed (`dbName`), the sender is
   * memoryKv. Per group: sender invites the receiver, receiver ingests the welcome, sender sends
   * `messages` and publishes; the receiver then ingests the whole backlog in ONE `ingest` call.
   * Returns per-group ingest ms and total wall ms.
   */
  async receiveScenario(dbName, groups, messages) {
    const started = performance.now();
    const receiverSigner = testSigner();
    const senderSigner = testSigner();
    const ingestMs: number[] = [];
    // One driver for the whole scenario: the engine never owns kv.close, so reopening the
    // receiver across groups reuses this driver rather than racing the lock.
    const receiverKv = await idbKv(dbName);
    for (let g = 0; g < groups; g += 1) {
      const receiverRelay = fakeRelay();
      // oxlint-disable-next-line no-await-in-loop
      const receiver = await Engine.open({
        backend: requireBackend(),
        kv: receiverKv,
        clock: manualClock(),
        signer: receiverSigner,
        allowLoopback: true,
      });
      // oxlint-disable-next-line no-await-in-loop
      const sender = await Engine.open({
        backend: requireBackend(),
        kv: memoryKv(),
        clock: manualClock(),
        signer: senderSigner,
        allowLoopback: true,
      });
      // oxlint-disable-next-line no-await-in-loop
      await receiver.publishKeyPackage([RELAY]);
      // oxlint-disable-next-line no-await-in-loop
      await publishAll(receiver, receiverRelay);
      // oxlint-disable-next-line no-await-in-loop
      const group = await sender.createGroup({
        name: `g${g}`,
        description: "",
        relays: [RELAY],
        invitees: [
          {
            keyPackage: receiverRelay.keyPackageOf(receiverSigner.pubkey),
            inboxRelays: [RELAY],
          },
        ],
      });
      // oxlint-disable-next-line no-await-in-loop
      await publishAll(sender, receiverRelay);
      // oxlint-disable-next-line no-await-in-loop
      await receiver.ingest(receiverRelay.welcomesFor(receiverSigner.pubkey));
      // oxlint-disable-next-line no-await-in-loop
      await receiver.tick();
      // oxlint-disable-next-line no-await-in-loop
      const snap = await sender.group(group.id);
      const route = snap?.nostrGroupId;
      if (route === undefined) {
        throw new Error("route missing");
      }
      for (let i = 0; i < messages; i += 1) {
        // oxlint-disable-next-line no-await-in-loop
        await sender.sendMessage(group.id, { kind: 9, tags: [], content: `g${g}m${i}` });
        // oxlint-disable-next-line no-await-in-loop
        await publishAll(sender, receiverRelay);
      }
      const backlog = receiverRelay.groupEvents(route, "r");
      const t0 = performance.now();
      // oxlint-disable-next-line no-await-in-loop
      await receiver.ingest(backlog);
      ingestMs.push(performance.now() - t0);
      // oxlint-disable-next-line no-await-in-loop
      await receiver.drain();
      // oxlint-disable-next-line no-await-in-loop
      await receiver.close();
      // oxlint-disable-next-line no-await-in-loop
      await sender.close();
    }
    await receiverKv.close();
    return { ingestMs, wallMs: performance.now() - started };
  },

  /** Browser memory readouts the spec can combine with CDP metrics. */
  async memory() {
    try {
      const result = await performance.measureUserAgentSpecificMemory?.();
      return { uaMemoryBytes: result?.bytes, breakdown: result?.breakdown };
    } catch {
      return { uaMemoryBytes: undefined, breakdown: undefined };
    }
  },
};

globalThis.__marmot = api;
