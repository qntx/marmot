import { describe, expect, test } from "vite-plus/test";

import { utf8Encode } from "@qntx/marmot/backend";
import type { BridgeHost } from "@qntx/marmot/backend";

import { bindMarmotNative } from "../src/bind.ts";
import type { MarmotBridge, MarmotNative } from "../src/specs/marmot-native.nitro.ts";

const POISONED = utf8Encode('{"type":"error","error":{"kind":"poisoned"}}');

type Created = {
  rows: ArrayBuffer;
  commit: (batch: ArrayBuffer) => void;
};

type BridgeScript = {
  open?: (config: ArrayBuffer) => ArrayBuffer | Error;
  call?: (request: ArrayBuffer) => ArrayBuffer | Error;
  drain?: () => ArrayBuffer | Error;
  close?: () => Error | undefined;
};

/**
 * A scripted `MarmotNative` fake: `create` records its arguments and hands out a bridge whose
 * methods return the scripted bytes or reject with the scripted error — mirroring what the C++ side
 * would surface as a rejected promise.
 */
function fakeNative(script: BridgeScript): { native: MarmotNative; created: Created[] } {
  const created: Created[] = [];
  const native: MarmotNative = {
    name: "MarmotNative",
    equals: () => false,
    dispose: () => undefined,
    create: (rows, commit) => {
      created.push({ rows, commit });
      // Mirrors `HybridMarmotBridge::_opened`: at most one open may reach the ABI.
      let opened = false;
      const bridge: MarmotBridge = {
        name: "MarmotBridge",
        equals: () => false,
        dispose: () => undefined,
        open: async (config) => {
          await Promise.resolve();
          if (opened) {
            throw new Error("marmot: open may run once per bridge");
          }
          opened = true;
          const out = script.open?.(config) ?? config;
          if (out instanceof Error) {
            throw out;
          }
          return out;
        },
        call: async (request) => {
          await Promise.resolve();
          const out = script.call?.(request) ?? request;
          if (out instanceof Error) {
            throw out;
          }
          return out;
        },
        drain: async () => {
          await Promise.resolve();
          const out = script.drain?.() ?? new ArrayBuffer(0);
          if (out instanceof Error) {
            throw out;
          }
          return out;
        },
        close: async () => {
          await Promise.resolve();
          const out = script.close?.();
          if (out instanceof Error) {
            throw out;
          }
        },
      };
      return bridge;
    },
  };
  return { native, created };
}

function fakeHost(rows: Uint8Array): {
  host: BridgeHost;
  state: { loads: number; commits: Uint8Array[] };
} {
  const state = { loads: 0, commits: [] as Uint8Array[] };
  return {
    state,
    host: {
      load: () => {
        state.loads += 1;
        return rows;
      },
      commit: (batch) => {
        state.commits.push(batch);
      },
    },
  };
}

function bytes(view: ArrayBuffer | Uint8Array): number[] {
  return [...new Uint8Array(view instanceof ArrayBuffer ? view : view.buffer)];
}

/** The rows bytes of the first `create` call, or an empty list when create never ran. */
function firstRows(created: Created[]): number[] {
  const first = created.at(0);
  if (first === undefined) {
    return [];
  }
  return bytes(first.rows);
}

/** The first forwarded commit batch's bytes, or an empty list when none arrived. */
function firstCommit(commits: Uint8Array[]): number[] {
  const first = commits.at(0);
  if (first === undefined) {
    return [];
  }
  return [...first];
}

describe("bindMarmotNative", () => {
  test("names the backend marmot-native", () => {
    expect(bindMarmotNative(fakeNative({}).native).name).toBe("marmot-native");
  });

  test("create calls host.load once and hands the rows bytes to native.create", () => {
    const { native, created } = fakeNative({});
    const backend = bindMarmotNative(native);
    const { host, state } = fakeHost(Uint8Array.of(1, 2, 3));
    backend.create(host);
    expect(state.loads).toBe(1);
    expect(created).toHaveLength(1);
    expect(firstRows(created)).toStrictEqual([1, 2, 3]);
  });

  test("a rows view into a larger buffer passes only the view's bytes", () => {
    const { native, created } = fakeNative({});
    const backend = bindMarmotNative(native);
    const big = Uint8Array.of(9, 9, 1, 2, 3, 9, 9);
    const { host } = fakeHost(big.subarray(2, 5));
    backend.create(host);
    expect(firstRows(created)).toStrictEqual([1, 2, 3]);
  });

  test("the native commit callback forwards batches to host.commit as Uint8Array", () => {
    const { native, created } = fakeNative({});
    const backend = bindMarmotNative(native);
    const { host, state } = fakeHost(Uint8Array.of(0));
    backend.create(host);
    created.at(0)?.commit(Uint8Array.of(7, 8, 9).buffer);
    expect(state.commits).toHaveLength(1);
    const [batch] = state.commits;
    expect(batch instanceof Uint8Array).toBe(true);
    expect(firstCommit(state.commits)).toStrictEqual([7, 8, 9]);
  });

  test("open/call/drain resolve the native ArrayBuffers as Uint8Array", async () => {
    const { native } = fakeNative({
      open: () => Uint8Array.of(10, 11).buffer,
      call: () => Uint8Array.of(12, 13).buffer,
      drain: () => Uint8Array.of(14, 15).buffer,
    });
    const handle = bindMarmotNative(native).create(fakeHost(Uint8Array.of(0)).host);
    expect([...(await handle.open(Uint8Array.of(1)))]).toStrictEqual([10, 11]);
    expect([...(await handle.call(Uint8Array.of(2)))]).toStrictEqual([12, 13]);
    expect([...(await handle.drain())]).toStrictEqual([14, 15]);
  });

  test("a request view is passed to the native side as exact bytes", async () => {
    let seen: number[] = [];
    const { native } = fakeNative({
      call: (request) => {
        seen = bytes(request);
        return request;
      },
    });
    const handle = bindMarmotNative(native).create(fakeHost(Uint8Array.of(0)).host);
    const big = Uint8Array.of(9, 5, 6, 9);
    await handle.call(big.subarray(1, 3));
    expect(seen).toStrictEqual([5, 6]);
  });

  test("a second open rejects — the native side allows one open per bridge", async () => {
    const { native } = fakeNative({});
    const handle = bindMarmotNative(native).create(fakeHost(Uint8Array.of(0)).host);
    expect([...(await handle.open(Uint8Array.of(1)))]).toStrictEqual([1]);
    await expect(handle.open(Uint8Array.of(2))).resolves.toStrictEqual(POISONED);
  });

  test("a native rejection poisons the backend: every later call resolves poisoned", async () => {
    const { native } = fakeNative({ call: () => new Error("engine dead") });
    const backend = bindMarmotNative(native);
    const { host } = fakeHost(Uint8Array.of(0));
    const first = backend.create(host);
    const second = backend.create(host);
    await expect(first.call(Uint8Array.of(1))).resolves.toStrictEqual(POISONED);
    await expect(first.open(Uint8Array.of(1))).resolves.toStrictEqual(POISONED);
    await expect(first.drain()).resolves.toStrictEqual(POISONED);
    await expect(second.call(Uint8Array.of(1))).resolves.toStrictEqual(POISONED);
  });

  test("close resolves on a poisoned backend and swallows native failure once", async () => {
    const { native } = fakeNative({
      call: () => new Error("engine dead"),
      close: () => new Error("close failed"),
    });
    const handle = bindMarmotNative(native).create(fakeHost(Uint8Array.of(0)).host);
    await handle.call(Uint8Array.of(1));
    await handle.close();
    const { native: healthy } = fakeNative({ close: () => new Error("close failed") });
    const other = bindMarmotNative(healthy).create(fakeHost(Uint8Array.of(0)).host);
    await other.close();
    await expect(other.call(Uint8Array.of(1))).resolves.toStrictEqual(POISONED);
  });
});
