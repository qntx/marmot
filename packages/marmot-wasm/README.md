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

Regenerate with `bun run build:wasm` (requires Rust + the pinned
wasm-bindgen CLI); `src/generated/` is the committed wasm-bindgen
`--target web` glue and `check:generated` diffs it.
