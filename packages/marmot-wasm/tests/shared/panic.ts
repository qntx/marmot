/** Shared panic assertions: trap the test-panic module, then prove poison on every surface. */

import { fileURLToPath } from "node:url";

import { decodeOpenResponse, decodeResponse, encodeRows, utf8Decode } from "@qntx/marmot/backend";
import type { BridgeHost } from "@qntx/marmot/backend";

import { createBackend } from "../../src/index.ts";
import type { BridgeClass } from "../../src/index.ts";
import { testPanicWasm } from "./env.ts";

/**
 * The test-panic glue module, declared structurally. `build:wasm` emits it into `tests/generated`
 * (gitignored), so the suite reaches it through a runtime import of a computed path — lint and
 * typecheck see no build output.
 */
type PanicModule = {
  default: (input: { module_or_path: Uint8Array }) => Promise<unknown>;
  MarmotBridge: BridgeClass;
  triggerPanic: () => void;
};

async function panicGlue(): Promise<PanicModule> {
  const path = fileURLToPath(new URL("../generated/marmot_wasm.js", import.meta.url));
  // oxlint-disable-next-line no-unsafe-type-assertion -- generated module, declared above
  return (await import(path)) as PanicModule;
}

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
  step2: () => Promise<void>;
  step3: () => void;
} {
  let glue: PanicModule | undefined;
  let first: ReturnType<ReturnType<typeof createBackend>["create"]> | undefined;
  let second: typeof first;
  return {
    step1: async () => {
      const mod = await panicGlue();
      glue = mod;
      await mod.default({ module_or_path: testPanicWasm() });
      const backend = createBackend(mod.MarmotBridge);
      first = backend.create(host);
      second = backend.create(host);
      thrown(() => mod.triggerPanic());
    },
    step2: async () => {
      // Every later call on every handle answers the encoded poisoned error.
      if (first === undefined || second === undefined) {
        throw new Error("handles were not created");
      }
      const outcomes = [
        poisonedKind(await first.call(new Uint8Array([1]))),
        poisonedKind(await second.call(new Uint8Array([2]))),
        poisonedKind(await first.drain()),
      ];
      for (const kind of outcomes) {
        if (kind !== "poisoned") {
          throw new Error(`expected poisoned, got ${kind}`);
        }
      }
      const open = decodeOpenResponse(await first.open(new Uint8Array([0])));
      if (open.type !== "error" || open.error.kind !== "poisoned") {
        throw new Error("open envelope is not poisoned");
      }
    },
    step3: () => {
      // The raw exported class proves the Rust-side flag answers poisoned too.
      if (glue === undefined) {
        throw new Error("glue was not loaded");
      }
      const raw = new glue.MarmotBridge(host.load, host.commit);
      const bytes = raw.call(new Uint8Array([3]));
      const text = utf8Decode(bytes);
      if (!text.includes('"poisoned"')) {
        throw new Error(`raw call did not answer poisoned: ${text}`);
      }
    },
  };
}
