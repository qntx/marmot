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
