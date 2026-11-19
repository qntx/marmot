/**
 * `@qntx/marmot-native` — the `mk-ffi` cdylib behind a `Backend`. `load()` binds the autolinked
 * `MarmotNative` HybridObject: native bridge calls run on Nitro's thread pool while the engine
 * serializes on its dedicated thread, so the async port never parks the JS thread.
 */

import { NitroModules } from "react-native-nitro-modules";

import type { Backend } from "@qntx/marmot/backend";

import { bindMarmotNative } from "./bind.ts";
import type { MarmotNative } from "./specs/marmot-native.nitro.ts";

export { bindMarmotNative } from "./bind.ts";

/**
 * The mk-ffi native backend: iOS loads `MarmotFfi.xcframework`, Android `libmarmot_ffi.so` — both
 * behind one C++ HybridObject pair (`create`/`open`/`call`/`drain`/`close` mapping 1:1 onto the C
 * ABI). Plug the returned `Backend` into `Engine.open` the same way as `@qntx/marmot-wasm`'s.
 */
export function load(): Backend {
  return bindMarmotNative(NitroModules.createHybridObject<MarmotNative>("MarmotNative"));
}
