import { describe, expect, test } from "vite-plus/test";

import {
  decodeCallValue,
  decodeConfigValue,
  encodeDrainResponse,
  encodeOpenResponse,
  encodeResponse,
} from "../src/backend/codec.ts";
import type { Json } from "../src/backend/codec.ts";
import { Engine, SignerRefusedError } from "../src/backend/engine.ts";
import type { Clock, Signer } from "../src/backend/engine.ts";
import { BridgeError } from "../src/backend/errors.ts";
import { encodeKvBatch } from "../src/backend/kv.ts";
import type { Backend, BridgeHandle, BridgeHost } from "../src/backend/port.ts";
import type {
  BridgeRequest,
  BridgeResponse,
  DrainResponse,
  NostrEvent,
  OpenResponse,
  UnsignedEvent,
} from "../src/backend/protocol.ts";
import { utf8Decode, utf8Encode } from "../src/backend/utf8.ts";
import { memoryKv } from "../src/testing/memory.ts";

const ACCOUNT = "a".repeat(64);
const DONE: BridgeResponse = { type: "ok", result: { type: "done" } };
const TICKED: BridgeResponse = { type: "ok", result: { type: "ticked", nextWakeMs: undefined } };

function wire(value: Json): Uint8Array {
  return utf8Encode(JSON.stringify(value));
}

function parse(bytes: Uint8Array): unknown {
  return JSON.parse(utf8Decode(bytes));
}

function signedEvent(unsigned: UnsignedEvent): NostrEvent {
  return {
    id: "b".repeat(64),
    pubkey: unsigned.pubkey,
    createdAt: unsigned.createdAt,
    kind: unsigned.kind,
    tags: unsigned.tags,
    content: unsigned.content,
    sig: "5".repeat(128),
  };
}

function unsigned(): UnsignedEvent {
  return { pubkey: ACCOUNT, createdAt: 1n, kind: 9, tags: [], content: "" };
}

function gate(): { promise: Promise<void>; open: () => void } {
  let open: () => void = () => {};
  const promise = new Promise<void>((resolve) => {
    open = resolve;
  });
  return { promise, open };
}

const RESOLVED = Symbol("resolved");

/** Await `promise`, returning the rejection or a sentinel — lets tests assert without try/catch. */
async function capture(promise: Promise<unknown>): Promise<unknown> {
  try {
    await promise;
    return RESOLVED;
  } catch (error) {
    return error;
  }
}

function putBatch(seq: bigint): Uint8Array {
  return encodeKvBatch({
    seq,
    ops: [{ type: "put", key: Uint8Array.of(1), value: Uint8Array.of(2) }],
  });
}

type Handler = (request: BridgeRequest, host: BridgeHost) => BridgeResponse;

/** Route requests to per-type handlers; unhandled types answer `done`. */
function routes(map: Partial<Record<BridgeRequest["type"], Handler>>): Handler {
  return (request, host) => {
    const handler = map[request.type];
    return handler === undefined ? DONE : handler(request, host);
  };
}

/** Tick → signer_pending, first signer_reply → signer_pending again, then `ticked`. */
function twoRoundSigner(): Handler {
  return (request) => {
    if (request.type === "tick") {
      return {
        type: "signer_pending",
        requests: [{ id: 1, op: { type: "nip44_encrypt", peer: "c".repeat(64), plaintext: "a" } }],
      };
    }
    if (request.type === "signer_reply" && request.replies[0]?.id === 1) {
      return {
        type: "signer_pending",
        requests: [{ id: 2, op: { type: "nip44_decrypt", peer: "c".repeat(64), ciphertext: "x" } }],
      };
    }
    if (request.type === "signer_reply") {
      return TICKED;
    }
    return DONE;
  };
}

/** Tick → host.commit(batch) then `after(request)`'s response. */
function commitOnTick(batch: Uint8Array, after: BridgeResponse): Handler {
  return (request, host) => {
    if (request.type === "tick") {
      host.commit(batch);
      return after;
    }
    return DONE;
  };
}

/** Tick → commit + signer_pending; ack_durable → done; signer_reply → ticked. */
function commitAndSuspend(batch: Uint8Array): Handler {
  return (request, host) => {
    if (request.type === "tick") {
      host.commit(batch);
      return {
        type: "signer_pending",
        requests: [{ id: 1, op: { type: "sign_event", unsigned: unsigned() } }],
      };
    }
    if (request.type === "signer_reply") {
      return TICKED;
    }
    return DONE;
  };
}

/** Tick → signer_pending with two sign_event requests; signer_reply → ticked. */
function twoSignEvents(): Handler {
  return (request) => {
    if (request.type === "tick") {
      return {
        type: "signer_pending",
        requests: [
          { id: 1, op: { type: "sign_event", unsigned: unsigned() } },
          { id: 2, op: { type: "sign_event", unsigned: unsigned() } },
        ],
      };
    }
    if (request.type === "signer_reply") {
      return TICKED;
    }
    return DONE;
  };
}

type Script = {
  open?: OpenResponse;
  onCall?: Handler;
  drain?: DrainResponse;
};

function fakeBackend(script: Script): {
  backend: Backend;
  calls: BridgeRequest[];
  configs: unknown[];
  closed: () => boolean;
} {
  const calls: BridgeRequest[] = [];
  const configs: unknown[] = [];
  let closed = false;
  const backend: Backend = {
    name: "fake",
    create: (host) => {
      const handle: BridgeHandle = {
        open: (config) => {
          configs.push(decodeConfigValue(parse(config)));
          return wire(encodeOpenResponse(script.open ?? { type: "ready", account: ACCOUNT }));
        },
        call: (bytes) => {
          const { request } = decodeCallValue(parse(bytes));
          calls.push(request);
          const response = script.onCall?.(request, host) ?? DONE;
          return wire(encodeResponse(response));
        },
        drain: () => wire(encodeDrainResponse(script.drain ?? { type: "events", events: [] })),
        close: () => {
          closed = true;
        },
      };
      return handle;
    },
  };
  return { backend, calls, configs, closed: () => closed };
}

const clock: Clock = { now: () => ({ unixMs: 1_700_000_000_000n, monotonicMs: 42_000n }) };

/** The `ack_durable` calls a fake backend observed. */
function ackSeqs(calls: BridgeRequest[]): bigint[] {
  return calls.filter((c) => c.type === "ack_durable").map((c) => c.seq);
}

/** The wire kind of a captured outcome ("other" when it is not a BridgeError). */
function kindOf(outcome: unknown): string {
  return outcome instanceof BridgeError ? outcome.kind : "other";
}

/** SignEvent that refuses the first request and throws on the second. */
function refuseThenBlow(): Signer["signEvent"] {
  let count = 0;
  return async () => {
    await Promise.resolve();
    count += 1;
    if (count === 1) {
      throw new SignerRefusedError();
    }
    throw new Error("boom");
  };
}

function signer(overrides?: Partial<Signer>): Signer {
  return {
    pubkey: ACCOUNT,
    signEvent: async (u) => Promise.resolve(signedEvent(u)),
    nip44Encrypt: async () => Promise.resolve("cipher"),
    nip44Decrypt: async () => Promise.resolve("plain"),
    ...overrides,
  };
}

describe("Engine", () => {
  test("loads rows, opens, and completes the proof flow", async () => {
    const { backend, calls, configs } = fakeBackend({
      open: { type: "needs_account_proof", unsigned: unsigned() },
      onCall: routes({
        submit_proof: () => ({ type: "ok", result: { type: "ready", account: ACCOUNT } }),
      }),
    });
    let signed = 0;
    const engine = await Engine.open({
      backend,
      kv: memoryKv(),
      clock,
      signer: signer({
        signEvent: async (u) => {
          signed += 1;
          return Promise.resolve(signedEvent(u));
        },
      }),
    });
    expect(signed).toBe(1);
    expect(calls[0]?.type).toBe("submit_proof");
    expect(configs).toHaveLength(1);
    await engine.close();
  });

  test("drives multi-round signer_pending in order", async () => {
    const ops: string[] = [];
    const { backend, calls } = fakeBackend({ onCall: twoRoundSigner() });
    const engine = await Engine.open({
      backend,
      kv: memoryKv(),
      clock,
      signer: signer({
        nip44Encrypt: async (_peer, plaintext) => {
          ops.push(`encrypt:${plaintext}`);
          return Promise.resolve("out-a");
        },
        nip44Decrypt: async (_peer, ciphertext) => {
          ops.push(`decrypt:${ciphertext}`);
          return Promise.resolve("out-b");
        },
      }),
    });
    await expect(engine.tick()).resolves.toBeUndefined();
    expect(ops).toStrictEqual(["encrypt:a", "decrypt:x"]);
    const replies = calls.filter((c) => c.type === "signer_reply");
    expect(replies).toStrictEqual([
      {
        type: "signer_reply",
        replies: [{ id: 1, result: { type: "text", text: "out-a" } }],
      },
      {
        type: "signer_reply",
        replies: [{ id: 2, result: { type: "text", text: "out-b" } }],
      },
    ]);
    await engine.close();
  });

  test("SignerRefusedError maps to refused, other throws to failed", async () => {
    const { backend, calls } = fakeBackend({ onCall: twoSignEvents() });
    const engine = await Engine.open({
      backend,
      kv: memoryKv(),
      clock,
      signer: signer({ signEvent: refuseThenBlow() }),
    });
    await engine.tick();
    const reply = calls.find((c) => c.type === "signer_reply");
    expect(reply).toStrictEqual({
      type: "signer_reply",
      replies: [
        { id: 1, result: { type: "refused" } },
        { id: 2, result: { type: "failed" } },
      ],
    });
    await engine.close();
  });

  test("acks seq from the batch header only after kv.commit resolves", async () => {
    const kv = memoryKv();
    kv.hold();
    const { backend, calls } = fakeBackend({ onCall: commitOnTick(putBatch(7n), TICKED) });
    const engine = await Engine.open({ backend, kv, clock, signer: signer() });
    await engine.tick();
    await Promise.resolve();
    expect(calls.every((c) => c.type !== "ack_durable")).toBe(true);
    kv.release();
    await engine.whenDurable();
    expect(ackSeqs(calls)).toStrictEqual([7n]);
    await engine.close();
  });

  test("ack is sent while another op is suspended on the signer", async () => {
    const kv = memoryKv();
    kv.hold();
    const suspend = gate();
    const { backend, calls } = fakeBackend({ onCall: commitAndSuspend(putBatch(9n)) });
    const engine = await Engine.open({
      backend,
      kv,
      clock,
      signer: signer({
        signEvent: async (u) => {
          await suspend.promise;
          return signedEvent(u);
        },
      }),
    });
    const ticking = engine.tick();
    // Let tick() reach signer_pending, then let the commit land and observe the ack.
    await new Promise((resolve) => {
      setTimeout(resolve);
    });
    kv.release();
    await engine.whenDurable();
    expect(ackSeqs(calls)).toStrictEqual([9n]);
    expect(calls.every((c) => c.type !== "signer_reply")).toBe(true);
    suspend.open();
    await ticking;
    await engine.close();
  });

  test("a failed commit poisons the engine and every later method", async () => {
    const kv = memoryKv();
    kv.failNext();
    const { backend, closed } = fakeBackend({ onCall: commitOnTick(putBatch(3n), TICKED) });
    const engine = await Engine.open({ backend, kv, clock, signer: signer() });
    await engine.tick();
    await engine.whenDurable();
    const outcomes = await Promise.all([
      capture(engine.tick()),
      capture(engine.groups()),
      capture(engine.takeOutbound()),
      capture(engine.drain()),
    ]);
    expect(outcomes.map(kindOf)).toStrictEqual([
      "host_failed",
      "host_failed",
      "host_failed",
      "host_failed",
    ]);
    expect(closed()).toBe(true);
    await engine.close();
  });

  test("serializes operations in call order", async () => {
    const order: string[] = [];
    const { backend } = fakeBackend({
      onCall: routes({
        take_outbound: (request) => {
          order.push(request.type);
          return { type: "ok", result: { type: "outbound", items: [] } };
        },
        groups: (request) => {
          order.push(request.type);
          return { type: "ok", result: { type: "groups", groups: [] } };
        },
        tick: (request) => {
          order.push(request.type);
          return TICKED;
        },
      }),
    });
    const engine = await Engine.open({ backend, kv: memoryKv(), clock, signer: signer() });
    await Promise.all([engine.takeOutbound(), engine.groups(), engine.tick()]);
    expect(order).toStrictEqual(["take_outbound", "groups", "tick"]);
    await engine.close();
  });

  test("whenDurable resolves once outstanding commits are acked", async () => {
    const kv = memoryKv();
    kv.hold();
    const { backend } = fakeBackend({ onCall: commitOnTick(putBatch(4n), TICKED) });
    const engine = await Engine.open({ backend, kv, clock, signer: signer() });
    await engine.tick();
    const outcome = (async () => {
      await engine.whenDurable();
      return "durable";
    })();
    kv.release();
    await expect(outcome).resolves.toBe("durable");
    await engine.close();
  });

  test("close releases the handle and rejects later methods", async () => {
    const { backend, closed } = fakeBackend({});
    const engine = await Engine.open({ backend, kv: memoryKv(), clock, signer: signer() });
    await engine.close();
    expect(closed()).toBe(true);
    const outcomes = await Promise.all([capture(engine.tick()), capture(engine.groups())]);
    expect(outcomes.map(kindOf)).toStrictEqual(["closed", "closed"]);
  });

  test("wire error responses surface as BridgeError with the wire kind", async () => {
    const { backend } = fakeBackend({
      onCall: () => ({ type: "error", error: { kind: "busy" } }),
    });
    const engine = await Engine.open({ backend, kv: memoryKv(), clock, signer: signer() });
    const outcome = await capture(engine.tick());
    expect(outcome).toBeInstanceOf(BridgeError);
    expect(outcome).toMatchObject({ kind: "busy" });
    await engine.close();
  });
});
