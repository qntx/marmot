# Vectors

`spec/` holds byte-for-byte copies of Marmot spec documents the
implementation is tested against — frozen at the spec rev pinned in
`upstream.json` (`spec.rev`, currently `f825024`). Do not edit them; they
move only when the pin moves. `bun run check:pins` verifies them against a
local `3rdparty/marmot` checkout when present.

- `spec/foundation/registries.md` — every registered id (component ids,
  extension types, event kinds, ALPN, exporter labels). Parsed by
  `packages/marmot/tests/registries.test.ts`, which asserts set-equality with
  the constants in `src/core/registries.ts`.
- `spec/foundation/errors.md` — the shared result/rejection vocabulary
  (input categories, dispositions, named outcomes). Parsed by
  `packages/marmot/tests/errors.test.ts` the same way.

Shared cross-language test vectors (MDK conformance replays, our own) land
here alongside `parity.json` in later milestones.

- `mdk/` — byte-for-byte copies of the MDK cgka-conformance-simulator vector
  fixtures (top-level `*.v1.json`, `incidents/*.json`, `manifest.v1.json`),
  frozen at the MDK pin in `upstream.json` (`mdk.rev`, tag `v0.16.0`).
  Provenance (repository, tag, rev, source path, SHA-256 per file) lives in
  `mdk/PROVENANCE.md`; `bun run check:pins` verifies the copies against a
  local `3rdparty/mdk` checkout when present. `crates/mk-core/tests/conformance.rs`
  replays them through the public `Session` API.
