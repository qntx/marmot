# @qntx/marmot-native

The React Native `Backend` for `@qntx/marmot`: the mk-ffi C ABI behind one C++
[Nitro](https://nitro.margelo.com) HybridObject pair, iOS and Android alike.

`load()` returns a `Backend` you plug into `Engine.open` exactly like the
`@qntx/marmot-wasm` backend:

```ts
import { Engine } from "@qntx/marmot";
import { load } from "@qntx/marmot-native";
import { sqliteKv, localLocks } from "@qntx/marmot/storage";

const engine = await Engine.open({
  backend: load(),
  kv: await sqliteKv(db, {
    name: "marmot",
    lock: localLocks(),
    cipher,
  }),
  // ...signer, transport, etc.
});
```

`sqliteKv` drives any `SqlitePort` — op-sqlite's `DB` is assignable to it
directly. The database handle stays host-owned; `kv.close()` only releases the
named lock and the commit chain.

## How calls run

`open`/`call`/`drain`/`close` map 1:1 onto the `mk_bridge_*` C ABI and run on
Nitro's shared thread pool, so the blocking bridge calls never park the JS
thread. Every `MarmotBridge` owns one dedicated engine thread inside mk-ffi;
jobs serialize FIFO on it, and the byte protocol is exactly the `mk-bridge`
protocol `@qntx/marmot-wasm` speaks.

Commit ordering is a contract of the ABI: the engine thread delivers every
`commit` notification synchronously before the `mk_bridge_*` call that produced
it returns, and Nitro dispatches both JS callbacks and promise resolutions
through the runtime's FIFO `CallInvoker` queue — so `kv.commit` sees batches in
sequence order and always before the producing call's promise resolves.

Any native-side rejection poisons the whole backend (mirroring
`@qntx/marmot-wasm`): later `open`/`call`/`drain` resolve the encoded
`poisoned` envelope; `close` still resolves.

## Artifacts

- `ios/MarmotFfi.xcframework` — dynamic `MarmotFfi.framework` slices for arm64
  device and arm64 simulator (vendored by `MarmotNative.podspec`).
- `android/src/main/jniLibs/<abi>/libmarmot_ffi.so` — `arm64-v8a`,
  `armeabi-v7a` and `x86_64` (IMPORTED by `android/CMakeLists.txt`, 16 KB
  page aligned).

Both are gitignored build products. Rebuild them from the repository root —
see `CONTRIBUTING.md` for prerequisites (Xcode, Android NDK 27.1.12297006,
rustup targets):

```sh
bun run build:native                # iOS + Android
bun run build:native --ios --android --host
```

Every artifact links only the seven `mk_*` symbols; the build fails the
exact-set export gate otherwise.
