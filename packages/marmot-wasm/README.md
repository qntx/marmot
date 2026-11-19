# @qntx/marmot-wasm

WASM build of the marmot bridge. `load()` instantiates the module and
returns a `Backend` for `@qntx/marmot`'s `Engine`.

```ts
import { load } from "@qntx/marmot-wasm";
import { Engine } from "@qntx/marmot/backend";

const backend = await load(); // browsers/Bun: default URL next to the module
// Node: pass bytes — await load(readFileSync(...))
const engine = await Engine.open({ backend, kv, signer, clock });
```

After a trap, the backend poisons: every later `open`/`call`/`drain` on
every handle returns the encoded `{"type":"error","error":{"kind":"poisoned"}}`
envelope.

The shipped release build (`opt-level = "z"` + `wasm-opt -Oz`) measured
5,334,073 bytes raw and 2,754,832 bytes gzipped (gzip -9, 2026-11);
`check:size` enforces the 3,000,000-byte gzip gate and asserts the release
artifact does not export the `test-panic` feature's `triggerPanic`.

Regenerate with `bun run build:wasm` (requires Rust + the pinned
wasm-bindgen CLI); `src/generated/` is the committed wasm-bindgen
`--target web` glue and `check:generated` diffs it against fresh
output, normalizing the per-build `wasm_bindgen_<16 hex>` shim
hash — it is not reproducible across machines (the `.wasm` is not
byte-identical either, e.g. secp256k1 C objects differ per host
clang), while every other difference still fails.
