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
import { encodeKvBatch, encodeRows } from "../src/backend/kv.ts";
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
import type { MemoryKv } from "../src/testing/memory.ts";

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

type Handler = (
  request: BridgeRequest,
  host: BridgeHost,
) => BridgeResponse | Promise<BridgeResponse>;

/** Route requests to per-type handlers; unhandled types answer `done`. */
function routes(map: Partial<Record<BridgeRequest["type"], Handler>>): Handler {
  return async (request, host) => {
    const handler = map[request.type];
    if (handler === undefined) {
      return DONE;
    }
    const response = await handler(request, host);
    return response;
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
  open?: OpenResponse | ((host: BridgeHost) => OpenResponse | Promise<OpenResponse>);
  onCall?: Handler;
  onCreate?: (host: BridgeHost) => void;
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
      script.onCreate?.(host);
      const handle: BridgeHandle = {
        open: async (config) => {
          configs.push(decodeConfigValue(parse(config)));
          const scripted = script.open;
          const response =
            typeof scripted === "function"
              ? await scripted(host)
              : (scripted ?? { type: "ready", account: ACCOUNT });
          return wire(encodeOpenResponse(response));
        },
        call: async (bytes) => {
          const { request } = decodeCallValue(parse(bytes));
          calls.push(request);
          const response = await (script.onCall?.(request, host) ?? DONE);
          return wire(encodeResponse(response));
        },
        drain: async () => {
          await Promise.resolve();
          return wire(encodeDrainResponse(script.drain ?? { type: "events", events: [] }));
        },
        close: async () => {
          closed = true;
          await Promise.resolve();
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
    expect(kindOf(await capture(engine.whenDurable()))).toBe("host_failed");
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

  test("a commit failure while suspended rejects the op with host_failed", async () => {
    const kv = memoryKv();
    kv.failNext();
    const suspend = gate();
    const { backend } = fakeBackend({ onCall: commitAndSuspend(putBatch(5n)) });
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
    const ticking = capture(engine.tick());
    // Let the op reach signer_pending and the failed commit mark the engine.
    await new Promise((resolve) => {
      setTimeout(resolve);
    });
    suspend.open();
    expect(kindOf(await ticking)).toBe("host_failed");
    await engine.close();
  });

  test("a failed submit_proof closes the handle", async () => {
    const { backend, closed } = fakeBackend({
      open: { type: "needs_account_proof", unsigned: unsigned() },
      onCall: routes({
        submit_proof: () => ({ type: "error", error: { kind: "invalid_request", reason: "x" } }),
      }),
    });
    const outcome = await capture(
      Engine.open({ backend, kv: memoryKv(), clock, signer: signer() }),
    );
    expect(kindOf(outcome)).toBe("invalid_request");
    expect(closed()).toBe(true);
  });

  test("a throwing proof signer closes the handle", async () => {
    const { backend, closed } = fakeBackend({
      open: { type: "needs_account_proof", unsigned: unsigned() },
    });
    const outcome = await capture(
      Engine.open({
        backend,
        kv: memoryKv(),
        clock,
        signer: signer({
          signEvent: async () => {
            await Promise.resolve();
            throw new Error("no keys");
          },
        }),
      }),
    );
    expect(outcome).toBeInstanceOf(Error);
    expect(kindOf(outcome)).toBe("other");
    expect(closed()).toBe(true);
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

  test("an open-time commit is acked even when open settles after kv.commit", async () => {
    const inner = memoryKv();
    let lastCommit: Promise<void> = Promise.resolve();
    const kv: MemoryKv = {
      ...inner,
      commit: async (batch) => {
        lastCommit = inner.commit(batch);
        await lastCommit;
      },
    };
    const { backend, calls } = fakeBackend({
      open: async (host) => {
        host.commit(putBatch(11n));
        // The open promise settles strictly after the commit's durability resolved — the ack
        // had to wait out `open` in the deferred buffer, not ride the handle.
        await lastCommit;
        return { type: "ready", account: ACCOUNT };
      },
    });
    const engine = await Engine.open({ backend, kv, clock, signer: signer() });
    await engine.whenDurable();
    expect(ackSeqs(calls)).toStrictEqual([11n]);
    await engine.close();
  });

  test("a non-increasing commit seq moves the engine to failed", async () => {
    const { backend, closed } = fakeBackend({
      onCall: routes({
        tick: (_request, host) => {
          host.commit(putBatch(5n));
          host.commit(putBatch(5n));
          return TICKED;
        },
      }),
    });
    const engine = await Engine.open({ backend, kv: memoryKv(), clock, signer: signer() });
    await engine.tick();
    expect(kindOf(await capture(engine.whenDurable()))).toBe("host_failed");
    expect(closed()).toBe(true);
    expect(kindOf(await capture(engine.tick()))).toBe("host_failed");
    await engine.close();
  });

  test("the host hands the durable rows over exactly once", async () => {
    const kv = memoryKv([[Uint8Array.of(1), Uint8Array.of(2)]]);
    const snapshots: Uint8Array[] = [];
    let secondThrew = false;
    const { backend } = fakeBackend({
      onCreate: (host) => {
        snapshots.push(host.load());
        try {
          host.load();
        } catch {
          secondThrew = true;
        }
      },
    });
    const engine = await Engine.open({ backend, kv, clock, signer: signer() });
    expect(snapshots).toStrictEqual([encodeRows(kv.rows())]);
    expect(secondThrew).toBe(true);
    await engine.close();
  });

  test("whenDurable waits for the ack call, not just kv.commit", async () => {
    const ackGate = gate();
    const { backend, calls } = fakeBackend({
      onCall: routes({
        tick: (_request, host) => {
          host.commit(putBatch(4n));
          return TICKED;
        },
        ack_durable: async () => {
          await ackGate.promise;
          return DONE;
        },
      }),
    });
    const engine = await Engine.open({ backend, kv: memoryKv(), clock, signer: signer() });
    await engine.tick();
    let durable = false;
    const waiting = (async () => {
      await engine.whenDurable();
      durable = true;
    })();
    // Let kv.commit resolve and the ack reach the bridge.
    await new Promise((resolve) => {
      setTimeout(resolve);
    });
    expect(ackSeqs(calls)).toStrictEqual([4n]);
    expect(durable).toBe(false);
    ackGate.open();
    await waiting;
    expect(durable).toBe(true);
    await engine.close();
  });

  test("an ack goes out while the op call that produced it is still pending", async () => {
    const opGate = gate();
    const { backend, calls } = fakeBackend({
      onCall: routes({
        tick: async (_request, host) => {
          host.commit(putBatch(6n));
          await opGate.promise;
          return TICKED;
        },
      }),
    });
    const engine = await Engine.open({ backend, kv: memoryKv(), clock, signer: signer() });
    const ticking = engine.tick();
    // Let the tick call start, the commit land, and the concurrent ack call be issued.
    await new Promise((resolve) => {
      setTimeout(resolve);
    });
    expect(ackSeqs(calls)).toStrictEqual([6n]);
    opGate.open();
    await ticking;
    await engine.whenDurable();
    await engine.close();
  });
});
