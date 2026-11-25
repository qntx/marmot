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
- The `Pump` is the only owner of the `Engine`: outbound is published only from `takeOutbound` after `whenDurable`, to exactly the item's relays, and reported with the exact relay strings; sync cursors advance only after the covering ingest is durable and persist through `CursorStore` as one versioned blob (undecodable = full refetch); every relay URL passes `safeRelayUrl` before any connection; a signer-refused/failed Welcome unwrap is `unavailable` (`signer_refused`/`signer_failed`), rolled back in a store savepoint and retryable — the client emits a `pending` invite event and `retryInvites()` reopens the inbox subscriptions at the capped cursor so the relays replay the wraps; a signer `invalid_payload` verdict (`SignerInvalidPayloadError`) is a permanent rejection remembered in the bounded (4096, FIFO) ingress-dedup ring, never retried. The pump verifies every inbound event's id and signature before dedupe, buffering or cursor credit (a `RelayPort` need not verify); at most 32 kind-1059 wraps reach the engine per 10 s (deferred, never dropped), and no cursor advances past an event still buffered for its key. Once every relay the current plan subscribes a group on has delivered its first EOSE, the pump reports `historyFetched` for that group through the serialized queue, after the covering ingest — once per group per Pump lifetime and once after each stall refetch, no timer (the session's 300 s deadline covers relays that never answer): `group_stall_suspected` bumps the group's refetch generation, deletes its group cursor keys (never inbox keys), closes its kind-445 subs and lets them reopen without `since`, and an EOSE queued under an older generation reports nothing. One-shot `fetchEvents` reads (relay lists, KeyPackages, the slot audit) verify each fetched event the same way. `MarmotClient.open` requires a signer that passes the NIP-44 extended-length self round trip (spec `transports/nostr.md`), cached as `nip44Extended` in the client state; `@qntx/marmot/testing` `checkSignerNip44` is the host conformance check.
- The session owns publish-retry pacing: durable backoff rows withhold retrying items from `take_outbound` and fold their deadlines into `tick().next_wake_ms`, so the pump schedules engine wakes from `nextWakeMs` only (no fixed retry timer). `not_sent` is a pre-send failure (MDK `pre_send`) — nothing was transmitted, no exposure is recorded, and the item stays retryable.
- `crates/mk-bridge` is the only host protocol over `mk-core` — `crates/mk-wasm` (wasm-bindgen) and `crates/mk-ffi` (C ABI) marshal nothing themselves; they call the bridge. Any change to an existing message shape bumps `PROTOCOL_VERSION`, and every protocol variant has a canonical vector in `vectors/bridge`. Outbound artifacts publish only via `take_outbound` after `ack_durable`. `mk-bridge` is `#![forbid(unsafe_code)]` and reads no ambient clock — the nostr `universal-time` provider inside `mk-wasm` is the one dependency-required exception. `mk-ffi` is the only crate allowed `unsafe`, confined to its `abi` module with a `// SAFETY:` comment per block; its handles are registry tokens (never dereferenced), so `mk_bridge_free` may race in-flight calls and a freed handle answers `not_open`. Every `AccountSigner` reply to `sign_event` is checked against the request (pubkey, kind, tags, content, created_at, id) and signature-verified before use.
- `@qntx/marmot-wasm` ships the `wasm` profile release artifact (opt-level z + `wasm-opt -Oz`); `check:size` enforces a 3,000,000-byte gzip gate. The `.wasm` is never committed — the Publish workflow builds it twice from the tagged source and ships it only when both builds are byte-identical, and `stage-wasm` (`prepack`) refuses a tarball whose glue and wasm do not pair. Only the in-repo `publish-*`/`attest-*` jobs of `publish-npm.yml` hold `id-token`, on `v*` tag pushes in the `npm-release` environment (tag-only deployment policy, required reviewers; the npm Trusted Publishers are bound to it); every `uses:` under `.github` is pinned by commit SHA. KV drivers are terminal after a failed `commit` (a later batch may depend on the lost one). `indexedDbKv` and `sqliteKv` share one contract: an injected cipher and lock are required, and the cipher authenticates each row key as associated data so sealed values cannot be swapped between rows. `sqliteKv` drives an injected `SqlitePort` and never closes the host-owned database.
- `@qntx/marmot-native` ships the mk-ffi C ABI behind C++ Nitro HybridObjects (one `mk-bridge` protocol, no marshaling of its own): iOS consumes `MarmotFfi.xcframework`, Android `libmarmot_ffi.so` jniLibs (ABIs arm64-v8a, armeabi-v7a, x86_64; CI `native.yml` builds both platforms) — both are gitignored `bun run build:native` products (profile `native`, panic=unwind) and must export exactly the seven `mk_*` symbols; `nitrogen/generated/` is committed and `bun run codegen` must be a no-op.
- `crates/mk-core` is the pure-Rust core (no bindings yet): inside it the layering is `kv` (the injected `KvHost` port and batch codec) → `store` (namespaced keyspace, layered pending-write views, transactions/savepoints/read snapshots) → `provider`/`mls`/`snapshot` (cgka-engine `StorageProvider` impl, OpenMLS row codec, snapshot restore) → the session modules (`session`/`events`/`semantics`/`outbox`/`delivered`/`pacing`/`key_package`/`key_package_lifecycle`/`self_update`/`stall`/`rejoin`/`clock`/`signer`/`types`). `key_package` selects invitee KeyPackages exactly as MDK's `select_key_packages_from_records` (plus a signature check on every candidate). `key_package_lifecycle` drives MDK's `KeyPackageLifecycleState` — one slot per device, no meta row (schema 7) — from `ensure_key_package` and `tick`: the pending promotes to current on its first relay acceptance, expiry/refresh/consumption/capability drift stage a replacement, write-set growth re-sends the same signed event; `sign_out` withdraws the slot with one kind-5 and retires every held bundle in one transaction, leaves non-admin groups and reports admin groups to the host; `audit_key_package_slot` takes the host's read-back of the slot's kind-30443 publications and, when a foreign own-account publication inside the 300 s skew window outranks the device's own, stages a strictly newer replacement through the same lifecycle path (beyond-skew ones are ignored). `self_update` completes MDK's `MaintenanceObligation`s (post-join after `history_fetched` or 300 s, 60 s quiet, persisted jitter, convergence gate, 24 h fallback; periodic every 24–36 d) through the normal outbox and never blocks sends. `stall` is the D2 durable per-group stall detector (`Table::GroupStall`, schema 8): distinct deferred kind-445 ids bounded at 3 at the group's current epoch drive one `GroupStallSuspected` refetch request; a `history_fetched` that leaves the counter full at the same epoch flags `stalled` (`GroupStalled` once) which `GroupSnapshot.access` reports, and epoch progress, a join, copy termination, `forget_group`, or an accepted current-epoch message deletes the row — a projection only, never a commit. `Session::group()` aggregates the engine's per-leaf roster into one `MemberSnapshot` per account (`devices` = leaf count, first-appearance order; the disbanded tombstone aggregates `record.members` the same way) and projects `access` (`active`/`removed_device`/`removed`/`unrecoverable`/`stalled`) — a sibling leaf leaving fires no member event, so hosts re-read the snapshot on `epoch_changed`; `Session::groups()` lists every stored local copy — live and terminated, group-id byte order — until `forget_group` removes it (quarantined copies stay unlisted). `create_group` takes `own_devices` — same-account sibling slots founded as extra leaves — and both `create_group` and `invite` refuse a same-account `KeyPackage` anywhere else before staging: a same-account Add is never committed by us after founding. `rejoin` is D2's rejoin surface: stored replacement Welcomes surface as `RejoinOffered` (on ingest and once at open) and take `confirm_rejoin`/`decline_rejoin` consent while a copy stays active, `retry_rejoins` installs them consent-free once the copy is removed or unrecoverable, and `readd_member` + `drive_readds` (`Table::Readd` row) complete the admin's remove-then-invite exactly once across crashes. `semantics` is pure: typed send intents, receive classification and modifier resolution of inner app events (reply/reaction/edit/delete/unreact/admin removal); kind 5 and kind 1009 modifiers authorize against the durable `DeliveredMessage` index (schema 5) and hold in memory — bounded 128 entries and 4 MiB per group, 300 s — until their targets arrive; inner app events of reserved device kinds 452..=477 drop as `DropReason::Reserved`. `pacing` caps one group's sends at 50 per `created_at` second (durable `SendPacing` row) with 30 s of forward skew. A queued intent the engine drains is named on its fanout at staging (`OutboundFanout::set_queued_intent`, schema 9) — the record MDK's drain and hydrate guards read — and a drained application message or proposal retires its intent on first acceptance or once no target is outstanding; a retryable failure never re-arms it while the fanout is stored. One key encoder (`keys.rs`), one value codec (serde_json); a schema version row at open, never migrations; a durability gate (staged batches release only on `acknowledge_durable`); two-phase identity (`Session::open` → `NeedsAccountProof` → `provide_account_proof`).
- mk-core is `publish = false` and takes no `tokio` dependency itself — the session is sans-IO; every I/O is an injected trait (`KvHost`, `AccountSigner`, `Clock`, relay publishing via `Outbound`). Native engine paths do need a Tokio time driver in the embedder's runtime (MDK requirement, tested via `#[tokio::test]`).
- mk-core advertises only capabilities it implements — the KeyPackage carries `mls_proposals {0x0008, 0x000a}`, `mls_extensions {0x0006, 0xf2d1}` (`0x0006` ratchet_tree, inherent to MLS; `0xf2d1` agent text stream QUIC receive, `Optional`) and an `app_components` tag restricted to private-use ids `{0x8001, 0x8003, 0x8004, 0x8006, 0x8009, 0x800b, 0x800c, 0x800d}` (the tag must equal the private-use subset of the decoded metadata — White Noise's `require_multi_value_key_package_tag_matches` enforces it). Never pad the tag with unimplemented ids: `0xf2d2`/`0xf2d4` (stream send/fanout) and `0x8008` (media v1) are never advertised.
- `crates/interop`, `crates/conformance` and `fuzz/` are standalone Cargo projects (own workspace, lockfile, toolchain/deny/lint configs, `publish = false`), excluded from the root workspace and from `check-version.ts` member checks; `crates/conformance` needs the pinned Odin toolchain (`dev-2026-10` rev `84bc3fc`) at build time because its simulator dep compiles `marmot-markdown`. `vectors/mdk/` holds byte-for-byte MDK vector copies verified by `bun run check:pins`.
- Fuzz corpora are generated, not committed (`fuzz/corpus/` is gitignored): `cargo run -F corpus-gen --bin gen_corpus` seeds real session artifacts; CI fuzzes from an empty corpus.
- All I/O is injected (WebSocket, fetch, storage drivers, timers); `packages/*/src/` holds no ambient singletons.
- `packages/*/src/` is platform-neutral and Hermes V1-safe: no Node-only or browser-only globals, no `node:*` imports, no ES2023+ array methods (src typechecks against es2022).
- The protocol engine is upstream MDK (Rust), pinned by tag and rev in `upstream.json` — a dependency, never patched in place. Defects are reported upstream with a reproduction; pin bumps are deliberate. White Noise (`wn`/`wnd`, MDK's `wn-cli`) is pinned separately as `wn` in `upstream.json` (tag + rev) and CI builds it from source. nosfry is pinned as `nosfry` in `upstream.json` (tag + rev + release-tarball and Marmot-profile SHA-256, the profile checked by `check:pins` when `3rdparty/nosfry` sits at the rev); the advisory `interop-nosfry` workflow runs `crates/interop` against it through `INTEROP_RELAY_URL`.
- `src/core` registries and the error vocabulary mirror the pinned spec (`upstream.json` → `spec.rev`). The spec text they encode is recorded byte-for-byte under `vectors/spec/`; package tests parse the recorded copies and assert set-equality with the constants, so constants only drift when the pin moves.
- npm packages and crates are versioned lockstep: `[workspace.package].version` equals every package.json version, members inherit `version.workspace = true`, internal path deps pin `=<version>`, and `scripts/check-version.ts` + `scripts/sync-versions.ts` keep Cargo.toml/Cargo.lock/bun.lock in sync on bump.
- `docs/` is local-only and never pushed: the whole directory is gitignored except `docs/index.mdx` and `docs/meta.json`; never commit any other file under `docs/`. `3rdparty/` holds local upstream checkouts and is never committed.
- Security: no secret leaves the engine backend except as a write to the injected, host-encrypted KV; logs and diagnostics carry no account ids, group ids, message ids, relay URLs, pubkeys, payloads, or key material; every URL taken from group state passes destination-safety (SSRF) checks — relays through `safeRelayUrl`/`relay_allowed`, peer media locators and avatar URLs through `safeMediaUrl` before the client hands them to the host; `allow_loopback` admits loopback only, never private, link-local or special-use addresses; `cargo deny` gates every crate (`unsound = "all"`; every ignore is per id with a reason and a revisit condition) and the fourteen fuzz targets in `fuzz/` gate every decoder we own; no `unwrap`/`expect`/panics outside tests; storage errors never embed serde/codec messages (they can echo payload fragments).

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

`lint` covers `vp check` (oxlint + oxfmt), `taplo fmt --check`, the version
lockstep check and `scripts/check-source-rules.ts` (unsafe confined to
`mk-ffi/src/abi.rs` with `// SAFETY:`, no logging in library sources).

Rust gate (what CI runs — see `CONTRIBUTING.md` for prerequisites):

```bash
cargo fmt --all --check
cargo clippy -p mk-core -p mk-bridge -p mk-wasm -p mk-ffi --all-features --all-targets -- -D warnings
cargo clippy -p mk-core -p mk-bridge -p mk-wasm -p mk-ffi --no-default-features --all-targets -- -D warnings
cargo test -p mk-core --all-features --release
cargo test -p mk-bridge --release
cargo test -p mk-ffi --all-features --release
cargo hack check -p mk-core -p mk-bridge -p mk-wasm -p mk-ffi --each-feature --no-dev-deps
RUSTDOCFLAGS="-D warnings" cargo doc -p mk-core -p mk-bridge -p mk-wasm -p mk-ffi --no-deps --all-features
cargo deny check
for t in wasm32-unknown-unknown aarch64-apple-ios; do
  cargo check -p mk-core -p mk-bridge --target "$t"
done
for t in aarch64-apple-ios aarch64-apple-ios-sim; do
  cargo check -p mk-ffi --target "$t"
done
# android: secp256k1-sys needs the NDK clang (macOS homebrew NDK or SDK):
# CC_aarch64_linux_android=<ndk>/toolchains/llvm/prebuilt/*/bin/aarch64-linux-android<api>-clang
for t in aarch64-linux-android armv7-linux-androideabi x86_64-linux-android; do
  cargo check -p mk-core -p mk-bridge -p mk-ffi --target "$t"
done

# wasm (see CONTRIBUTING.md for wasm-bindgen/binaryen/Playwright):
bun run --cwd packages/marmot-wasm build:wasm
bun run --cwd packages/marmot-wasm check:size
bun run --cwd packages/marmot-wasm check:generated   # diffs committed glue vs fresh output
                                                   # with the per-build wasm_bindgen shim
                                                   # hash normalized (not reproducible
                                                   # across machines)
bun run --cwd packages/marmot-wasm test:wasm
bun run --cwd packages/marmot-wasm test:browser

bun run build:native               # mk-ffi prebuilts for marmot-native: --ios
                                   # --android --host (prereqs in CONTRIBUTING.md)

(cd crates/interop && cargo fmt --check && cargo clippy --all-targets -- -D warnings && cargo test && cargo deny check)
(cd crates/conformance && export ODIN=<path>/odin && cargo fmt --check && cargo clippy --all-targets -- -D warnings && cargo test --release && cargo deny check)
(cd fuzz && cargo +nightly fuzz build --target "$(rustc -vV | sed -n 's/^host: //p')")
                                                  # cargo-fuzz defaults to x86_64 on arm64 macOS
```

Ignored suites: the wn/wnd interop scenarios run in CI (`interop-rust`,
`interop-wasm`) against wn/wnd built from the upstream.json `wn` pin;
locally they need `WN_BIN`/`WND_BIN` (see `CONTRIBUTING.md`). Anything
that needs Odin or nightly stays local — run it before landing changes
in its area.
