/**
 * `MARMOT_SMOKE_NO_NATIVE=1` builds (app-size delta only) resolve
 * `@qntx/marmot-native` here: the native package is excluded from autolinking,
 * so `load()` can never produce a working backend — any mode that reaches it
 * fails fast and visibly.
 */

import type { Backend } from "@qntx/marmot/backend";

export function load(): Backend {
  throw new Error("@qntx/marmot-native is stubbed out (MARMOT_SMOKE_NO_NATIVE=1)");
}
