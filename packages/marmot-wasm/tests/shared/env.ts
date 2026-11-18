/**
 * The shared suite environment: load the shipped artifact's bytes, wrap them in a `Backend`, and
 * run `runFlow` against `memoryKv` drivers — identical under Node (vitest) and Bun.
 */

import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";

import type { Backend } from "@qntx/marmot/backend";
import { memoryKv, runFlow } from "@qntx/marmot/testing";
import type { FlowTimings } from "@qntx/marmot/testing";

import { load } from "../../src/index.ts";
import { testSigner } from "../signer.ts";

/** The shipped artifact bytes — Node and Bun both read them via node:fs. */
export function releaseWasm(): Uint8Array {
  return new Uint8Array(
    readFileSync(fileURLToPath(new URL("../../dist/marmot_wasm_bg.wasm", import.meta.url))),
  );
}

export function testPanicWasm(): Uint8Array {
  return new Uint8Array(
    readFileSync(fileURLToPath(new URL("../generated/marmot_wasm_bg.wasm", import.meta.url))),
  );
}

/** A fresh `Backend` over the shipped wasm bytes. */
export async function releaseBackend(): Promise<Backend> {
  // `load` caches init; each test wants its own backend only, so it is fine.
  return load(releaseWasm());
}

/** Run the shared flow with `messages` messages; returns timings. */
export async function runReleaseFlow(messages: number): Promise<FlowTimings> {
  const backend = await releaseBackend();
  const stores = new Map<string, ReturnType<typeof memoryKv>>();
  return runFlow({
    backend,
    kv: (name) => {
      let store = stores.get(name);
      if (store === undefined) {
        store = memoryKv();
        stores.set(name, store);
      }
      return store;
    },
    signer: testSigner,
    messages,
  });
}

export { createBackend } from "../../src/index.ts";
