# Contributing

## Prerequisites

- **Bun 1.4** — package scripts, tests, and tooling (`devEngines`).
- **Rust 1.99.1** — via the root `rust-toolchain.toml` (and each standalone
  project's own copy under `crates/interop`, `crates/conformance`, `fuzz/`).
- `cargo-deny` — supply-chain check (`cargo install cargo-deny`).
- `cargo-hack` — per-feature check (`cargo install cargo-hack`).
- **Android NDK `27.1.12297006`** — for the `aarch64-linux-android` check
  (`secp256k1-sys` compiles C) and for `build:native --android`; point
  `CC_aarch64_linux_android` at the NDK clang, or set `ANDROID_NDK_HOME` /
  install under `~/Library/Android/sdk/ndk/`.
- **Xcode** — only for `build:native --ios` (the xcframework step uses
  `xcodebuild` and the iOS SDK clang).
- **rustup targets** — only for `build:native`:
  `rustup target add aarch64-apple-ios aarch64-apple-ios-sim
  aarch64-linux-android x86_64-linux-android`
- **Rust nightly + cargo-fuzz 0.13** — only for fuzzing
  (`cargo install cargo-fuzz`).
- **wasm-bindgen-cli** — only for `build:wasm`; the version must equal
  `Cargo.lock`'s `wasm-bindgen` (currently 0.2.130). Install it beside the
  worktree as `cargo install wasm-bindgen-cli --version <locked> --locked
  --root <worktree>/target/wasm-bindgen-cli`; the build script finds it
  there or on `PATH` and fails clearly on a mismatch.
- **binaryen** — installed by `bun install` (`binaryen` devDependency);
  `build:wasm` runs its `wasm-opt -Oz` on the release artifact.
- **Playwright Chromium** — only for `test:browser`
  (`bunx playwright install --with-deps chromium`).
- **Odin `dev-2026-10`, rev `84bc3fc`** — only for `crates/conformance`
  (its simulator dep compiles `marmot-markdown`) and for building `wn`/`wnd`
  for interop. Install with MDK's checksum-pinned installer:

  ```sh
  # inside a checkout of marmot-protocol/mdk at the pinned rev
  bash scripts/install-odin.sh "$PWD/target/odin-bin"
  export ODIN="$PWD/target/odin-bin/odin"
  odin version   # dev-2026-10-nightly:84bc3fc
  ```

## Repository layout

```text
packages/marmot/      @qntx/marmot — platform-neutral TypeScript SDK
                      (src/{core,backend,storage,testing})
packages/marmot-wasm/ @qntx/marmot-wasm — loader around the mk-wasm
                      artifact (committed wasm-bindgen glue, gitignored
                      .wasm)
crates/mk-core/       pure-Rust core: KV storage provider + Session API
crates/mk-bridge/     byte-oriented JSON host protocol over mk-core
crates/mk-wasm/       wasm-bindgen shell over mk-bridge
crates/mk-ffi/        C ABI over mk-bridge (marmot_ffi.h); one engine
                      thread per handle, the only crate with `unsafe`
packages/marmot-native/ @qntx/marmot-native — React Native Backend:
                      C++ Nitro HybridObjects over the mk-ffi prebuilts
                      (ios/MarmotFfi.xcframework, android jniLibs)
crates/interop/       standalone project — wn/wnd scenario tests against
                      the public Session API (excluded from the workspace)
crates/conformance/   standalone project — recorded MDK vectors replayed
                      through Session (excluded; needs Odin)
fuzz/                 standalone cargo-fuzz project (excluded); corpus/ is
                      gitignored — seeds are generated (see Fuzzing below)
vectors/              recorded copies frozen at the pinned revs (see
                      vectors/README.md): spec/ spec text, mdk/ MDK vectors
upstream.json         the one pins file: spec rev + MDK tag/rev and their
                      3rdparty/ checkout paths
3rdparty/             local upstream checkouts — gitignored, never committed
scripts/              repo tooling (check-version, sync-versions,
                      check-pins); script tests run under the root vp test
docs/                 local-only design docs — never committed
```

## Local gate

Run before opening a pull request:

```bash
bun install && bun run lint && bun run typecheck && bun run test && bun run build
bun run check:pins
cargo metadata --no-deps --format-version 1 >/dev/null
cargo fmt --all --check
cargo clippy -p mk-core --all-targets --all-features -- -D warnings
cargo clippy -p mk-core --all-targets --no-default-features -- -D warnings
cargo test -p mk-core --all-features --release
cargo hack check -p mk-core --each-feature --no-dev-deps
RUSTDOCFLAGS="-D warnings" cargo doc -p mk-core --no-deps --all-features
cargo deny check
cargo check -p mk-core --target wasm32-unknown-unknown
cargo check -p mk-core --target aarch64-apple-ios
CC_aarch64_linux_android=<ndk-clang> cargo check -p mk-core --target aarch64-linux-android
```

`lint` = `vp check` (oxlint + oxfmt) + `taplo fmt --check` +
`scripts/check-version.ts`. `check:pins` verifies that local `3rdparty/`
checkouts sit at the revs recorded in `upstream.json` and that the recorded
copies under `vectors/` are byte-identical to the pinned sources; absent
checkouts are skipped (CI has no 3rdparty).

## Conformance vectors (`crates/conformance`)

Replays the recorded MDK scenario vectors under `vectors/mdk/` through the
public `mk_core::Session` API; each vector is classified `PASS`, `REFUSED`
(with a reasoned capability it cannot exercise), or `BYTE-FIXTURE`.

```sh
cd crates/conformance
ODIN=<path-to-odin> cargo test --release   # normal test, not ignored
cargo fmt --check && cargo clippy --all-targets -- -D warnings && cargo deny check
```

## White Noise interop (`crates/interop`)

Six scenario tests (`tests/wn.rs`) drive the public `Session` API against a
real White Noise `wn`/`wnd` pair built from the pinned MDK rev, over a real
in-process NIP-01 WebSocket relay (`nostr-sdk`'s `LocalRelay` from the
`erskingardner/rust-nostr` fork MDK itself pins). `wnd` is spawned and
reaped by the test; a missing or wrong-version binary under `--ignored` is
a hard failure, never a silent skip.

Build `wn`/`wnd` from the pinned MDK rev (`upstream.json` → `mdk.tag`/`rev`;
`marmot-markdown` needs the pinned Odin above):

```sh
ODIN=<path-to-odin> cargo install --locked \
  --git https://github.com/marmot-protocol/mdk \
  --rev a2e0558a634bbd20607cdd5afb93abc6220ee58d \
  --root spike/target/wn wn-cli
```

`cargo install` takes ~12 min cold; `wn --version` must report `wn 0.16.0`
(the test asserts it against `upstream.json` `mdk.tag`).

```sh
cd crates/interop
cargo test                                     # compiles/lints; scenarios ignored
WN_BIN=$PWD/../../spike/target/wn/bin/wn \
WND_BIN=$PWD/../../spike/target/wn/bin/wnd \
  cargo test --release -- --ignored --test-threads=1 --nocapture
```

Each run uses a fresh temporary `WN_HOME` (`WN_SECRET_STORE=file`,
`WN_ALLOW_LOOPBACK_RELAYS=1`,
`WN_ALLOW_LOOPBACK_BLOB_ENDPOINTS=1` — scenario 5's dev
Blossom server binds loopback), removed when the `Wnd` guard drops; `wnd`
logs land under `crates/interop/target/wn-logs/`. Per-scenario
`SCENARIO-N PASS|REFUSED <evidence> <seconds>` lines print to stderr.
Scenarios 1–6 drive the public `Session` API; scenario 7
(`tests/bridge.rs`) replays the wn-invites-us flow with every local
action driven through `mk-bridge` request/response bytes — the same
surface wasm and C hosts use.

## Fuzzing (`fuzz/`)

Nine libFuzzer targets cover every decoder we own (kv batch, kv rows,
event JSON, account proof, key-package event, imeta parse, agent-stream
tags, plus the mk-bridge request and config decoders). The seed corpus is generated
from real session artifacts and is gitignored — regenerate it when the row
formats change:

```sh
cd fuzz
cargo run --features corpus-gen --bin gen_corpus   # seeds corpus/<target>/
cargo +nightly fuzz run <target> -- -max_total_time=60
```

CI (`ci-rust-fuzz`) runs every target for 60 s starting from an empty
corpus — no seeds needed there.

## Native build (`packages/marmot-native`)

`bun run build:native [--ios] [--android] [--host]` (no flag = `--ios
--android`) builds the `mk-ffi` staticlib with `--profile native`
(panic=unwind — the FFI boundary's `catch_unwind` needs it; `release` is
panic=abort) and relinks each shared artifact itself:

- `--ios` (macOS + Xcode): `aarch64-apple-ios` and `aarch64-apple-ios-sim`
  only → two dynamic `MarmotFfi.framework`s (install name
  `@rpath/MarmotFfi.framework/MarmotFfi`, min iOS 15.1) →
  `xcodebuild -create-xcframework` →
  `packages/marmot-native/ios/MarmotFfi.xcframework`.
- `--android` (NDK `27.1.12297006`): `arm64-v8a` and `x86_64` →
  `libmarmot_ffi.so` under `android/src/main/jniLibs/<abi>/`, linked with
  `-z max-page-size=16384` and `-soname,libmarmot_ffi.so`, then
  `llvm-strip --strip-unneeded`. The script fails unless SONAME is set and
  every LOAD segment is ≥ 16 KB aligned (`llvm-readelf -lW`).
- `--host`: the host shared library under `target/native/` — the CI export
  gate (`.github/workflows/ci.yml` `native` job runs it on Ubuntu).

The final link is ours (not rustc's): rustc's cdylib link unions its own
export list — every `#[no_mangle]` item in the dep graph — with any list we
add, so secp256k1-sys symbols would leak into the dynamic table. Relinking
the staticlib with `-force_load`/`--whole-archive` plus a generated
`-exported_symbols_list`/`--version-script` makes the dynamic table exactly
the seven `mk_*` symbols; the script asserts that set per artifact (which
also proves `mk_bridge_test_panic` never ships) and prints size + SHA-256.

## Native smoke (`apps/native-smoke`)

`apps/native-smoke` is a standalone Expo dev-client app (private, own
`bun.lock`, excluded from version lockstep via `STANDALONE_APPS` in
`scripts/check-version.ts`) that proves the native stack end to end:
`@qntx/marmot-native` (Nitro C++ HybridObject → mk-ffi engine thread) +
`sqliteKv` over `@op-engineering/op-sqlite`, bundled to Hermes bytecode in a
Release build. The app runs one mode per launch from
`marmotsmoke://run?mode=…` (iOS passes `Documents/run.json` instead —
`simctl openurl` needs a consent dialog) and reports to a host collector on
`127.0.0.1:8100`:

- `flow` — the shared `runFlow` scenario (create, join, 500 messages,
  close/reopen, add, remove, failed-publish retry) through the public
  `Engine` API.
- `kill`/`verify` — the kill-during-write loop from `marmot-wasm`'s
  `e2e/kill.spec.ts`: journal batches/acks/publishes out of process, kill
  mid-write (`kill -9` of the app process on iOS, `am force-stop` on
  Android), relaunch, dump the decrypted durable rows, and check the
  prefix/ack/publish invariants.

Prerequisites: the mk-ffi prebuilts (`bun run build:native --ios
--android`), Xcode + an iOS runtime for the simulator, and for Android the
SDK with the 16 KB-page image
`system-images;android-37.0;google_apis_playstore_ps16k;arm64-v8a` plus
Android Studio's JBR as `JAVA_HOME`.

```sh
cd apps/native-smoke
bun install
bun run smoke:ios        # Release build, flow + kill loop on a private simulator
bun run smoke:android    # Release APK, flow + kill loop on a private 16 KB emulator
```

Driver flags: `--flow` (flow only), `--kill` (kill loop only),
`--iterations=N` (default 30), `--seed=N` (default 20260209, the browser
suite's seed). Results land under `apps/native-smoke/target/`:
`flow-<platform>.json`, `kill-<platform>.json`, `kill-<platform>-<i>.json`.
Local packages are synced into `node_modules` by `scripts/sync-local.ts`
(file copies — Expo autolinking does not follow the repo symlinks); rerun
it after changing `packages/marmot` or `packages/marmot-native`.

Shared-machine discipline: the iOS driver takes a simulator slot under
`~/Desktop/qntx/meowl.social/.cache/sim-slots/` (max 3, 40-minute hold,
released on exit) on its own `marmot-m4` device; Android uses a private
`marmot_m4` AVD on console port 5600 (`emulator-5600`) and only ever
addresses `-s emulator-5600`. The collector port is shared — run one driver
at a time.

## Commits

English Conventional Commits: `type(scope): subject` (`feat`, `fix`, `docs`,
`style`, `refactor`, `perf`, `test`, `chore`, `ci`, `build`, `revert`), subject
in imperative mood, no period, ≤ 50 chars; `BREAKING CHANGE:` footer for
breaking changes.

## Documentation policy

`docs/` is local-only and must never be committed; only `docs/index.mdx` and
`docs/meta.json` are exempt should they be added.

## Versioning and release

npm packages and the Cargo workspace are versioned lockstep:
`[workspace.package].version` equals every package.json version.
`bump.config.ts` bumps `packages/marmot/package.json` and `Cargo.toml`
(`[workspace.package].version` plus any internal `=<version>` pins) and runs
`bun scripts/sync-versions.ts`, which syncs internal `@qntx/*` ranges and
refreshes `bun.lock` (and `Cargo.lock` once it exists). `release` runs
`bumpp`, which pushes a `release/vX.Y.Z` branch and opens the release pull
request via `gh`. After the release PR merges, tag the merge commit and push
the tag:

```sh
git tag -a vX.Y.Z <merge-sha> -m vX.Y.Z && git push origin vX.Y.Z
```

`scripts/check-version.ts` fails the lint gate when the internal `@qntx/*`
ranges, the Cargo workspace version, member `version.workspace = true`
inheritance, internal `=<version>` pins, Cargo.lock workspace-member entries,
or bun.lock workspace entries drift from the manifests. The standalone
projects (`crates/interop`, `crates/conformance`, `fuzz/`) are excluded
from member checks — they version independently (`0.0.0`).

## License

Contributions are dual-licensed under MIT OR Apache-2.0.
