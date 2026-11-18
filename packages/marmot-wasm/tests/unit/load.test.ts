import { describe, expect, test } from "vite-plus/test";

import { decodeOpenResponse, decodeResponse, utf8Encode } from "@qntx/marmot/backend";
import type { BridgeHost } from "@qntx/marmot/backend";

import { createBackend, load } from "../../src/index.ts";
import type { BridgeClass } from "../../src/index.ts";

const host: BridgeHost = {
  load: () => new Uint8Array([0]),
  commit: () => undefined,
};

/** A `MarmotBridge` stand-in that traps on `call`. */
const TrapBridge: BridgeClass = class {
  constructor() {}
  open = () => new Uint8Array([1]);
  call = (): Uint8Array => {
    throw new Error("unreachable trap");
  };
  drain = () => new Uint8Array([2]);
  close = () => undefined;
  free = () => undefined;
};

describe("createBackend", () => {
  test("a throwing call poisons the backend on every handle", () => {
    const backend = createBackend(TrapBridge);
    const first = backend.create(host);
    const second = backend.create(host);
    const trapped = decodeResponse(first.call(utf8Encode("{}")));
    expect(trapped).toStrictEqual({ type: "error", error: { kind: "poisoned" } });
    const outcomes = [decodeResponse(second.call(utf8Encode("{}"))), decodeResponse(first.drain())];
    for (const response of outcomes) {
      expect(response).toStrictEqual({ type: "error", error: { kind: "poisoned" } });
    }
    const open = decodeOpenResponse(second.open(utf8Encode("{}")));
    expect(open).toStrictEqual({ type: "error", error: { kind: "poisoned" } });
    first.close();
    second.close();
  });
});

describe("load", () => {
  test("rejects on invalid bytes", async () => {
    await expect(load(new Uint8Array([0, 97, 115, 109]))).rejects.toThrow(
      /expected|invalid|magic/i,
    );
  });
});
