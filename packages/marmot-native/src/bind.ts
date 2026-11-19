import { utf8Encode } from "@qntx/marmot/backend";
import type { Backend, BridgeHandle, BridgeHost } from "@qntx/marmot/backend";

import type { MarmotNative } from "./specs/marmot-native.nitro.ts";

/** `{"type":"error","error":{"kind":"poisoned"}}` — valid for open, call and drain alike. */
const POISONED = utf8Encode('{"type":"error","error":{"kind":"poisoned"}}');

/**
 * The exact `ArrayBuffer` the view reads: the backing buffer itself when the view covers it,
 * otherwise a fresh copy — a view into a larger buffer must never leak past the ABI (the C++ side
 * copies the whole buffer it is handed).
 */
function asArrayBuffer(view: Uint8Array): ArrayBuffer {
  const { buffer, byteOffset, byteLength } = view;
  if (buffer instanceof ArrayBuffer && byteOffset === 0 && byteLength === buffer.byteLength) {
    return buffer;
  }
  return new Uint8Array(view).buffer;
}

/**
 * Bind a `MarmotNative` HybridObject as a `Backend`. `create` takes the `host.load` snapshot up
 * front and feeds it to the native side once. Any rejection out of the native side poisons the
 * backend: every later `open`/`call`/`drain` on every handle resolves the encoded `poisoned`
 * envelope, matching `@qntx/marmot-wasm`'s backend.
 */
export function bindMarmotNative(native: MarmotNative): Backend {
  let poisoned = false;
  const guard = async (run: () => Promise<ArrayBuffer>): Promise<Uint8Array> => {
    if (poisoned) {
      return POISONED;
    }
    try {
      return new Uint8Array(await run());
    } catch {
      poisoned = true;
      return POISONED;
    }
  };
  return {
    name: "marmot-native",
    create: (host: BridgeHost): BridgeHandle => {
      const bridge = native.create(asArrayBuffer(host.load()), (batch) => {
        host.commit(new Uint8Array(batch));
      });
      return {
        open: async (config) => guard(async () => bridge.open(asArrayBuffer(config))),
        call: async (request) => guard(async () => bridge.call(asArrayBuffer(request))),
        drain: async () => guard(async () => bridge.drain()),
        close: async () => {
          if (poisoned) {
            return;
          }
          try {
            await bridge.close();
          } catch {
            poisoned = true;
          }
        },
      };
    },
  };
}
