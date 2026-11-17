/** Shared panic assertions: trap the test-panic module, then prove poison on every surface. */

import { decodeOpenResponse, decodeResponse, encodeRows, utf8Decode } from "@qntx/marmot/backend";
import type { BridgeHost } from "@qntx/marmot/backend";

import { createBackend } from "../../src/index.ts";
import init, { MarmotBridge, triggerPanic } from "../generated/marmot_wasm.js";
import { testPanicWasm } from "./env.ts";

const host: BridgeHost = {
  load: () => encodeRows([]),
  commit: () => undefined,
};

function poisonedKind(bytes: Uint8Array): string {
  const response = decodeResponse(bytes);
  return response.type === "error" ? response.error.kind : "other";
}

/** Returns the assertions both runners replay; `thrown` wraps triggerPanic per harness. */
export function panicFlow(thrown: (body: () => void) => void): {
  step1: () => Promise<void>;
  step2: () => void;
  step3: () => void;
} {
  let first: ReturnType<ReturnType<typeof createBackend>["create"]> | undefined;
  let second: typeof first;
  return {
    step1: async () => {
      await init({ module_or_path: testPanicWasm() });
      const backend = createBackend(MarmotBridge);
      first = backend.create(host);
      second = backend.create(host);
      thrown(() => triggerPanic());
    },
    step2: () => {
      // Every later call on every handle answers the encoded poisoned error.
      if (first === undefined || second === undefined) {
        throw new Error("handles were not created");
      }
      const outcomes = [
        poisonedKind(first.call(new Uint8Array([1]))),
        poisonedKind(second.call(new Uint8Array([2]))),
        poisonedKind(first.drain()),
      ];
      for (const kind of outcomes) {
        if (kind !== "poisoned") {
          throw new Error(`expected poisoned, got ${kind}`);
        }
      }
      const open = decodeOpenResponse(first.open(new Uint8Array([0])));
      if (open.type !== "error" || open.error.kind !== "poisoned") {
        throw new Error("open envelope is not poisoned");
      }
    },
    step3: () => {
      // The raw exported class proves the Rust-side flag answers poisoned too.
      const raw = new MarmotBridge(host.load, host.commit);
      const bytes = raw.call(new Uint8Array([3]));
      const text = utf8Decode(bytes);
      if (!text.includes('"poisoned"')) {
        throw new Error(`raw call did not answer poisoned: ${text}`);
      }
    },
  };
}
