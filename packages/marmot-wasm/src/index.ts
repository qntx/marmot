/**
 * `@qntx/marmot-wasm` — the `mk-wasm` artifact behind a `Backend`. `load()` instantiates the module
 * once and hands out a byte `Backend` for `Engine`: the synchronous wasm glue is wrapped in
 * promises to satisfy the async port.
 */

import { utf8Encode } from "@qntx/marmot/backend";
import type { Backend, BridgeHandle, BridgeHost } from "@qntx/marmot/backend";

import init, { MarmotBridge } from "./generated/marmot_wasm.js";

/** `{"type":"error","error":{"kind":"poisoned"}}` — valid for open, call and drain alike. */
const POISONED = utf8Encode('{"type":"error","error":{"kind":"poisoned"}}');

/** The `MarmotBridge` constructor surface the loader needs (satisfied by every glue build). */
export type BridgeClass = new (
  load: () => Uint8Array,
  commit: (batch: Uint8Array) => void,
) => {
  open: (config: Uint8Array) => Uint8Array;
  call: (request: Uint8Array) => Uint8Array;
  drain: () => Uint8Array;
  close: () => void;
  free: () => void;
};

/**
 * Wrap an instantiated bridge class as a `Backend`. `create` takes the `host.load` snapshot up
 * front and feeds it to the bridge once. A throw out of any wasm call poisons the backend: every
 * later `open`/`call`/`drain` on every handle resolves the encoded `poisoned` envelope, matching
 * the Rust-side flag checked after a trap.
 */
export function createBackend(Bridge: BridgeClass): Backend {
  let poisoned = false;
  const guard = (body: () => Uint8Array): Uint8Array => {
    if (poisoned) {
      return POISONED;
    }
    try {
      return body();
    } catch {
      poisoned = true;
      return POISONED;
    }
  };
  return {
    name: "marmot-wasm",
    create: (host: BridgeHost): BridgeHandle => {
      let snapshot: Uint8Array | undefined = host.load();
      const inner = new Bridge(
        () => {
          const rows = snapshot;
          snapshot = undefined;
          if (rows === undefined) {
            throw new Error("host.load called twice");
          }
          return rows;
        },
        (batch) => {
          host.commit(batch);
        },
      );
      return {
        open: async (config) => {
          await Promise.resolve();
          return guard(() => inner.open(config));
        },
        call: async (request) => {
          await Promise.resolve();
          return guard(() => inner.call(request));
        },
        drain: async () => {
          await Promise.resolve();
          return guard(() => inner.drain());
        },
        close: async () => {
          await Promise.resolve();
          if (poisoned) {
            return;
          }
          try {
            inner.close();
          } catch {
            poisoned = true;
          }
        },
      };
    },
  };
}

let initialized: Promise<unknown> | undefined;

/**
 * Instantiate the wasm module once and return its `Backend`.
 *
 * `source` feeds `wasm-bindgen`'s init directly: a `Response` (streaming fetch result), raw bytes,
 * a `WebAssembly.Module`, or a `URL`. The default resolves `marmot_wasm_bg.wasm` next to this
 * module, which suits browsers and Bun; Node callers pass the file's bytes.
 */
export async function load(
  source?: Response | ArrayBufferView | ArrayBuffer | URL,
): Promise<Backend> {
  initialized ??= init({
    module_or_path: source ?? new URL("marmot_wasm_bg.wasm", import.meta.url),
  });
  try {
    await initialized;
  } catch (error) {
    initialized = undefined;
    throw error;
  }
  return createBackend(MarmotBridge);
}
