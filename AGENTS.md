# AGENTS.md

- Do not preserve backward compatibility. Remove obsolete paths instead of adding compatibility layers, fallbacks, or migrations.
- Choose the simplest implementation that fully meets the current requirements. Avoid speculative abstractions, configuration, and indirection.
- Grow the system in layers. Start from the smallest version that works end to end, and add each new capability on top of a product that already works. Never trade a working product for unfinished complexity.
- Keep components modular and concerns clearly separated.
- Prefer established, well-maintained libraries when they reduce overall complexity or improve reliability. Do not reimplement common functionality without a clear reason.
- Lean on the dependencies already in the project before writing your own implementation or adding packages. Do not assume a library lacks a capability without checking its documentation and types.
- Make architectural decisions for the long term. Do not accept a stopgap that only works for now and is meant to be replaced later.

## Architecture invariants

- One-directional layering inside `packages/marmot/src`, enforced by lint (`LAYERS` in `vite.config.ts`): `core` (types, errors, registries) is the leaf; `backend` (the engine port) adds `core`; `storage` and `transport` add `core`/`backend`; `groups`, `invites`, `keys`, `media` add `core`/`backend`/`storage`/`transport`; `client` composes all of them; `testing` may import every layer except `client`. Only `src/core` exists today.
- All I/O is injected (WebSocket, fetch, storage drivers, timers); `packages/*/src/` holds no ambient singletons.
- `packages/*/src/` is platform-neutral and Hermes V1-safe: no Node-only or browser-only globals, no `node:*` imports, no ES2023+ array methods (src typechecks against es2022).
- The protocol engine is upstream MDK (Rust), pinned by tag and rev in `upstream.json` — a dependency, never patched in place. Defects are reported upstream with a reproduction; pin bumps are deliberate.
- `src/core` registries and the error vocabulary mirror the pinned spec (`upstream.json` → `spec.rev`). The spec text they encode is recorded byte-for-byte under `vectors/spec/`; package tests parse the recorded copies and assert set-equality with the constants, so constants only drift when the pin moves.
- npm packages and crates are versioned lockstep: `[workspace.package].version` equals every package.json version, members inherit `version.workspace = true`, internal path deps pin `=<version>`, and `scripts/check-version.ts` + `scripts/sync-versions.ts` keep Cargo.toml/Cargo.lock/bun.lock in sync on bump.
- `docs/` is local-only and never pushed: the whole directory is gitignored except `docs/index.mdx` and `docs/meta.json`; never commit any other file under `docs/`. `3rdparty/` holds local upstream checkouts and is never committed.
- Security: no secret leaves the engine backend except as a write to the injected, host-encrypted KV; logs and diagnostics carry no account ids, group ids, message ids, relay URLs, pubkeys, payloads, or key material; every URL taken from group state passes destination-safety (SSRF) checks; `cargo deny` and fuzz targets gate every decoder we own once crates land; no `unwrap`/`expect`/panics outside tests.

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
lockstep check. Cargo fmt/clippy/deny and the per-crate feature matrix join
the gate when the first crate lands.
