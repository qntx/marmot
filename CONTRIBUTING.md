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
  aarch64-linux-android armv7-linux-androideabi x86_64-linux-android`
- **Rust nightly + cargo-fuzz 0.13** — only for fuzzing
  (`cargo install cargo-fuzz`).
- **wasm-bindgen-cli** — only for `build:wasm`; the version must equal
  `Cargo.lock`'s `wasm-bindgen` (currently 0.2.130). Install it beside the
  worktree as `cargo install wasm-bindgen-cli --version <locked> --locked
  --root <worktree>/target/wasm-bindgen-cli`; the build script finds it
  there or on `PATH` and fails clearly on a mismatch.
- **binaryen** — installed by `bun install` (`binaryen` devDependency);
  `build:wasm` runs its `wasm-opt -Oz` on the release artifact.
- **Homebrew LLVM** — only on macOS, only for `build:wasm` from scratch:
  Apple `ar` silently produces an empty archive for wasm objects, so
  `secp256k1-sys` links with no symbols. Set
  `CC_wasm32_unknown_unknown=/opt/homebrew/opt/llvm/bin/clang
  AR_wasm32_unknown_unknown=/opt/homebrew/opt/llvm/bin/llvm-ar
  RANLIB_wasm32_unknown_unknown=/opt/homebrew/opt/llvm/bin/llvm-ranlib`
  and `cargo clean -p secp256k1-sys --target wasm32-unknown-unknown`
  before the first build. CI uses GNU `ar` and is unaffected.
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
upstream.json         the one pins file: spec rev + MDK tag/rev, the
                      separate `wn` pin for White Noise and the `nosfry`
                      pin (relay release + Marmot profile digests) —
                      and their 3rdparty/ checkout paths
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
`scripts/check-version.ts` + `scripts/check-source-rules.ts`. `check:pins` verifies that local `3rdparty/`
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

Fifteen scenario tests (`tests/wn.rs`, S1–S6 and S14–S22) drive the
public `Session` API against a real White Noise `wn`/`wnd` pair built
from the pinned MDK rev, over a real in-process NIP-01 WebSocket relay
(`nostr-sdk`'s `LocalRelay` from the `erskingardner/rust-nostr` fork MDK
itself pins). `wnd` is spawned and reaped by the test; a missing or
wrong-version binary under `--ignored` is a hard failure, never a
silent skip.

Build `wn`/`wnd` from the pinned MDK rev (`upstream.json` → `wn.rev`;
`marmot-markdown` needs the pinned Odin above):

```sh
ODIN=<path-to-odin> cargo install --locked \
  --git https://github.com/marmot-protocol/mdk \
  --rev 34646a577807f2b112ee031f3c164ec58aa18b64 \
  --root spike/target/wn wn-cli
```

`cargo install` takes ~12 min cold; `wn --version` must report `wn 0.18.0`
(the test asserts it against `upstream.json` `wn.tag`). In CI the `wn` job
builds the pair from the pin and `interop-rust`/`interop-wasm` run the
scenarios on every PR with the binaries cached per pin; the advisory
`interop-nightly.yml` workflow repins to MDK master
(`scripts/repin-mdk.ts`) and runs them there too.

The TypeScript side covers all twenty-two scenarios (S1–S22) from
`packages/marmot-wasm` (after `build:wasm`):

```sh
WN_BIN=<path>/wn WND_BIN=<path>/wnd \
  bun run --cwd packages/marmot-wasm test:interop
```

Both suites take `INTEROP_RELAY`. `nip11` (the default) has the in-process
relay answer NIP-11 with a `limitation.max_limit` (strfry's 500 for the
TypeScript fake relay, the Rust `LocalRelay`'s configured 10,000 filter
cap), as production relays do. `bare` serves no NIP-11 — wnd's post-join
history sweep then cannot prove a slice complete, and S2 of `test:interop`
loses our message in a large share of runs (9/20 on wn 0.17.0 locally; a
White Noise defect, measured by the non-required CI job
`interop-known-upstream`).

```sh
cd crates/interop
cargo test                                     # compiles/lints; scenarios ignored
WN_BIN=$PWD/../../spike/target/wn/bin/wn \
WND_BIN=$PWD/../../spike/target/wn/bin/wnd \
  cargo test --release -- --ignored --test-threads=1 --nocapture
```

`INTEROP_RELAY_URL=<ws-url>` skips the in-process relay and the NIP-11
front entirely: every party — the client, `wnd`, the sessions — dials
that relay, and welcome fetches read through per-account connections
that answer NIP-42 (kind 1059 is commonly read-gated to author and
recipient on real relays; `INTEROP_RELAY` must stay unset). The
advisory `interop-nosfry.yml` workflow runs the suite nightly against
the nosfry release pinned in `upstream.json` (`nosfry`), configured by
its `contrib/marmot/nosfry.toml` profile. Locally, with a nosfry
listening on `ws://127.0.0.1:7777`:

```sh
INTEROP_RELAY_URL=ws://127.0.0.1:7777 \
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
Scenarios 1–6 and 14–22 drive the public `Session` API; scenario 7
(`tests/bridge.rs`) replays the wn-invites-us flow with every local
action driven through `mk-bridge` request/response bytes — the same
surface wasm and C hosts use.

Scenario 17 asserts the sign-out contract only (wn's roster keeps one
leaf for our account, the signed-out device ends removed, the remaining
device's roster is unchanged). Chat after the sign-out is not part of it:
our remaining device and wn both auto-commit the SelfRemove, and
convergence of that race is an MDK defect — reproduced deterministically
by the ignored `commit_before_referenced_proposal_converges` in
`crates/mk-core/tests/fork_convergence.rs`.

## Fuzzing (`fuzz/`)

Fourteen libFuzzer targets cover every decoder we own (kv batch, kv rows,
event JSON, account proof, key-package event, key-package selection,
imeta parse, agent-stream tags, semantics, session rows, media decrypt,
plus the mk-bridge request, config and FFI call decoders). The seed
corpus is generated
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
panic=abort) and relinks each shared artifact itself. `--ios` and
`--android` also run in CI (`.github/workflows/native.yml`, both
platforms):

- `--ios` (macOS + Xcode): `aarch64-apple-ios` and `aarch64-apple-ios-sim`
  only → two dynamic `MarmotFfi.framework`s (install name
  `@rpath/MarmotFfi.framework/MarmotFfi`, min iOS 15.1) →
  `xcodebuild -create-xcframework` →
  `packages/marmot-native/ios/MarmotFfi.xcframework`.
- `--android` (NDK `27.1.12297006`): `arm64-v8a`, `armeabi-v7a` and `x86_64` →
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
- `perf` — the measurement cases in `src/perf.ts` (`kp`, `ingest500`,
  `add50`, `send`, `memgen`, `memrecv`), one fresh process per case, with
  device/host metadata, load averages, and process memory sampled around
  the memory cases.

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

Perf runs reuse the same drivers — `bun run perf:ios` /
`bun run perf:android` (`--perf`), with `--cases=kp,ingest500,...` to pick
a subset, `--members=N` to override the `memgen` member count (labelled
`-m<N>` in the output filename), `--source=<run>` plus
`--db/--key/--secret` to point `memrecv` at a previous `memgen` database,
and `--keep-emulator`/`--keep-device` to leave the device up between
runs. Results: `target/perf-<platform>-<case>[-m<N>].json`.

`bun run size` (`scripts/size.ts`) builds the Release app with and without
`@qntx/marmot-native` (temporary `expo.autolinking.exclude` + the
`MARMOT_SMOKE_NO_NATIVE` metro stub) and reports APK/`.app`/zip/native
library sizes to `target/appsize.json`.

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
`[workspace.package].version` equals every package.json version, and
internal `@qntx/*` ranges are `^<version>` in `dependencies`/
`peerDependencies` and `<version>` in `devDependencies`.

1. `bun run release` — bumpp bumps all three package.json files and
   `Cargo.toml` (`[workspace.package].version` plus any internal
   `=<version>` pins), `bun scripts/sync-versions.ts` refreshes the
   internal ranges and the `bun.lock`/`Cargo.lock` lockfiles, and bumpp
   pushes a `release/vX.Y.Z` branch and opens the release pull request
   via `gh`.
2. Merge the release PR.
3. Tag the merge commit and push the tag:

   ```sh
   git tag -a vX.Y.Z <merge-sha> -m vX.Y.Z && git push origin vX.Y.Z
   ```

4. The Publish workflow (`publish-npm.yml`) does the rest: verify (the
   tag equals every version, the commit is on main, lockstep holds),
   `native.yml` iOS + Android release builds from cache-free sources
   with the exported-symbol gates and attestation, the wasm built twice
   and compared byte for byte, `check:size`/`check:generated`, three
   `npm publish --provenance` jobs, then the GitHub release whose notes
   gain the wasm/native/tarball sha256 table. The publish and
   attestation jobs are the only `id-token` holders and run only on
   `v*.*.*` tag pushes inside the `npm-release` environment — the repo
   must define that environment with a deployment policy admitting only
   `v*.*.*` tags plus required reviewers, and each npm Trusted Publisher
   is bound to the repository, `publish-npm.yml` and the `npm-release`
   environment, so a token minted outside it is refused.

Dry-run the pipeline from any branch:

```sh
gh workflow run publish-npm.yml --ref <branch>
```

(it also runs on PRs touching the pipeline).

The first release of a package that does not exist on npm yet: trusted
publishing needs the package to exist, so an owner publishes the
CI-built tarball from that run's `npm-pack-packages-<name>` artifact
once (`npm publish <tgz> --access public`), then adds the Trusted
Publisher (`qntx/marmot`, workflow `publish-npm.yml`, environment
`npm-release`). Nothing is built locally.

`scripts/check-version.ts` fails the lint gate when the internal `@qntx/*`
ranges, the Cargo workspace version, member `version.workspace = true`
inheritance, internal `=<version>` pins, Cargo.lock workspace-member entries,
or bun.lock workspace entries drift from the manifests. The standalone
projects (`crates/interop`, `crates/conformance`, `fuzz/`) are excluded
from member checks — they version independently (`0.0.0`).

## License

Contributions are dual-licensed under MIT OR Apache-2.0.
