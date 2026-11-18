# AGENTS.md

- Do not preserve backward compatibility. Remove obsolete paths instead of adding compatibility layers, fallbacks, or migrations.
- Choose the simplest implementation that fully meets the current requirements. Avoid speculative abstractions, configuration, and indirection.
- Grow the system in layers. Start from the smallest version that works end to end, and add each new capability on top of a product that already works. Never trade a working product for unfinished complexity.
- Keep components modular and concerns clearly separated.
- Prefer established, well-maintained libraries when they reduce overall complexity or improve reliability. Do not reimplement common functionality without a clear reason.
- Lean on the dependencies already in the project before writing your own implementation or adding packages. Do not assume a library lacks a capability without checking its documentation and types.
- Make architectural decisions for the long term. Do not accept a stopgap that only works for now and is meant to be replaced later.

## Architecture invariants

- One-directional layering inside `packages/marmot/src`, enforced by lint (`LAYERS` in `vite.config.ts`): `core` (types, errors, registries) is the leaf; `backend` (the engine port) adds `core`; `storage` and `transport` add `core`/`backend`; `groups`, `invites`, `keys`, `media` add `core`/`backend`/`storage`/`transport`; `client` composes all of them; `testing` may import every layer except `client`. `src/media` is unbuilt; every other layer exists. `client` is the application surface (`MarmotClient` + the serialized `Pump`) and must not be imported by any other layer — tests drive it through `@qntx/marmot/client` or `@qntx/marmot/testing`.
- The `Pump` is the only owner of the `Engine`: outbound is published only from `takeOutbound` after `whenDurable`, to exactly the item's relays, and reported with the exact relay strings; sync cursors advance only after the covering ingest is durable and persist through `CursorStore` as one versioned blob (undecodable = full refetch); every relay URL passes `safeRelayUrl` before any connection; a refused/failed Welcome unwrap is terminal in the engine, so the client surfaces a `blocked` invite event.
- `crates/mk-bridge` is the only host protocol over `mk-core` — `crates/mk-wasm` (wasm-bindgen) and any future C ABI marshal nothing themselves; they call the bridge. Any change to an existing message shape bumps `PROTOCOL_VERSION`, and every protocol variant has a canonical vector in `vectors/bridge`. Outbound artifacts publish only via `take_outbound` after `ack_durable`. `mk-bridge` is `#![forbid(unsafe_code)]` and reads no ambient clock — the nostr `universal-time` provider inside `mk-wasm` is the one dependency-required exception.
- `@qntx/marmot-wasm` ships the `wasm` profile release artifact (opt-level z + `wasm-opt -Oz`); `check:size` enforces a 3,000,000-byte gzip gate. KV drivers are terminal after a failed `commit` (a later batch may depend on the lost one). `indexedDbKv` requires an injected cipher and lock; the cipher authenticates each row key as associated data so sealed values cannot be swapped between rows.
- `crates/mk-core` is the pure-Rust core (no bindings yet): inside it the layering is `kv` (the injected `KvHost` port and batch codec) → `store` (namespaced keyspace, layered pending-write views, transactions/savepoints/read snapshots) → `provider`/`mls`/`snapshot` (cgka-engine `StorageProvider` impl, OpenMLS row codec, snapshot restore) → the session modules (`session`/`events`/`outbox`/`key_package`/`clock`/`signer`/`types`). One key encoder (`keys.rs`), one value codec (serde_json); a schema version row at open, never migrations; a durability gate (staged batches release only on `acknowledge_durable`); two-phase identity (`Session::open` → `NeedsAccountProof` → `provide_account_proof`).
- mk-core is `publish = false` and takes no `tokio` dependency itself — the session is sans-IO; every I/O is an injected trait (`KvHost`, `AccountSigner`, `Clock`, relay publishing via `Outbound`). Native engine paths do need a Tokio time driver in the embedder's runtime (MDK requirement, tested via `#[tokio::test]`).
- mk-core advertises only capabilities it implements — the KeyPackage carries `mls_proposals {0x0008, 0x000a}`, `mls_extensions {0x0006, 0xf2d1}` (`0x0006` ratchet_tree, inherent to MLS; `0xf2d1` agent text stream QUIC receive, `Optional`) and an `app_components` tag restricted to private-use ids `{0x8001, 0x8003, 0x8004, 0x8006, 0x8009, 0x800b, 0x800c, 0x800d}` (the tag must equal the private-use subset of the decoded metadata — White Noise's `require_multi_value_key_package_tag_matches` enforces it). Never pad the tag with unimplemented ids: `0xf2d2`/`0xf2d4` (stream send/fanout) and `0x8008` (media v1) are never advertised.
- `crates/interop`, `crates/conformance` and `fuzz/` are standalone Cargo projects (own workspace, lockfile, toolchain/deny/lint configs, `publish = false`), excluded from the root workspace and from `check-version.ts` member checks; `crates/conformance` needs the pinned Odin toolchain (`dev-2026-10` rev `84bc3fc`) at build time because its simulator dep compiles `marmot-markdown`. `vectors/mdk/` holds byte-for-byte MDK vector copies verified by `bun run check:pins`.
- Fuzz corpora are generated, not committed (`fuzz/corpus/` is gitignored): `cargo run -F corpus-gen --bin gen_corpus` seeds real session artifacts; CI fuzzes from an empty corpus.
- All I/O is injected (WebSocket, fetch, storage drivers, timers); `packages/*/src/` holds no ambient singletons.
- `packages/*/src/` is platform-neutral and Hermes V1-safe: no Node-only or browser-only globals, no `node:*` imports, no ES2023+ array methods (src typechecks against es2022).
- The protocol engine is upstream MDK (Rust), pinned by tag and rev in `upstream.json` — a dependency, never patched in place. Defects are reported upstream with a reproduction; pin bumps are deliberate.
- `src/core` registries and the error vocabulary mirror the pinned spec (`upstream.json` → `spec.rev`). The spec text they encode is recorded byte-for-byte under `vectors/spec/`; package tests parse the recorded copies and assert set-equality with the constants, so constants only drift when the pin moves.
- npm packages and crates are versioned lockstep: `[workspace.package].version` equals every package.json version, members inherit `version.workspace = true`, internal path deps pin `=<version>`, and `scripts/check-version.ts` + `scripts/sync-versions.ts` keep Cargo.toml/Cargo.lock/bun.lock in sync on bump.
- `docs/` is local-only and never pushed: the whole directory is gitignored except `docs/index.mdx` and `docs/meta.json`; never commit any other file under `docs/`. `3rdparty/` holds local upstream checkouts and is never committed.
- Security: no secret leaves the engine backend except as a write to the injected, host-encrypted KV; logs and diagnostics carry no account ids, group ids, message ids, relay URLs, pubkeys, payloads, or key material; every URL taken from group state passes destination-safety (SSRF) checks; `cargo deny` gates every crate and the nine fuzz targets in `fuzz/` gate every decoder we own; no `unwrap`/`expect`/panics outside tests; storage errors never embed serde/codec messages (they can echo payload fragments).

## Dependency policy

Runtime dependencies use caret ranges so consumers can deduplicate them against the rest of their dependency tree; devDependencies stay exact-pinned for reproducible builds. Review upstream changelogs on every bump.

## Commands

Local gate before a pull request:

```bash
bun install && bun run lint && bun run typecheck && bun run test && bun run build
bun run check:pins   # verifies local 3rdparty checkouts against
                     # upstream.json; absent checkouts are skipped
cargo metadata --no-deps --format-version 1 >/dev/null
```

`lint` covers `vp check` (oxlint + oxfmt), `taplo fmt --check` and the version
lockstep check.

Rust gate (what CI runs — see `CONTRIBUTING.md` for prerequisites):

```bash
cargo fmt --all --check
cargo clippy -p mk-core -p mk-bridge -p mk-wasm --all-features --all-targets -- -D warnings
cargo clippy -p mk-core -p mk-bridge -p mk-wasm --no-default-features --all-targets -- -D warnings
cargo test -p mk-core --all-features --release
cargo test -p mk-bridge --release
cargo hack check -p mk-core -p mk-bridge -p mk-wasm --each-feature --no-dev-deps
RUSTDOCFLAGS="-D warnings" cargo doc -p mk-core -p mk-bridge -p mk-wasm --no-deps --all-features
cargo deny check
for t in wasm32-unknown-unknown aarch64-apple-ios; do
  cargo check -p mk-core -p mk-bridge --target "$t"
done
# android: secp256k1-sys needs the NDK clang (macOS homebrew NDK or SDK):
# CC_aarch64_linux_android=<ndk>/toolchains/llvm/prebuilt/*/bin/aarch64-linux-android<api>-clang
cargo check -p mk-core -p mk-bridge --target aarch64-linux-android

# wasm (see CONTRIBUTING.md for wasm-bindgen/binaryen/Playwright):
bun run --cwd packages/marmot-wasm build:wasm
bun run --cwd packages/marmot-wasm check:size
bun run --cwd packages/marmot-wasm check:generated   # diffs committed glue vs fresh output
                                                   # with the per-build wasm_bindgen shim
                                                   # hash normalized (not reproducible
                                                   # across machines)
bun run --cwd packages/marmot-wasm test:wasm
bun run --cwd packages/marmot-wasm test:browser

(cd crates/interop && cargo fmt --check && cargo clippy --all-targets -- -D warnings && cargo test && cargo deny check)
(cd crates/conformance && ODIN=<path>/odin cargo fmt --check && cargo clippy --all-targets -- -D warnings && cargo test --release && cargo deny check)
(cd fuzz && cargo +nightly fuzz build)
```

Ignored suites (need local binaries/toolchains): the wn/wnd interop
scenarios (`WN_BIN`/`WND_BIN`, see `CONTRIBUTING.md`) and anything that
needs Odin or nightly — run them before landing changes in their area.
