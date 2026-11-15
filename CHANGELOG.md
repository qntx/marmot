# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

A `vX.Y.Z` tag runs `publish-npm.yml` (publishing `packages/marmot` via npm
trusted publishing / OIDC).

## [Unreleased]

### Added

- Repository skeleton: bun + vite-plus workspace, `@qntx/oxlint` /
  `@qntx/oxfmt` / `@qntx/tsconfig`, empty Cargo workspace with shared lint
  profiles, lint-enforced `packages/marmot` layer graph, lockstep version
  tooling, `qntx/workflows` CI and release workflows.
- `@qntx/marmot` core registries and result vocabulary mirroring the Marmot
  spec at `f825024` (app component ids, extension-draft ids, extension types,
  event kinds, ALPN identifiers, exporter labels; input categories,
  dispositions, named outcomes), tested against byte-for-byte recorded spec
  copies under `vectors/spec/foundation/`.
- `upstream.json` pins (`spec` marmot-protocol/marmot `f825024`, `mdk` v0.16.0
  `a2e0558`) and `bun run check:pins`, which verifies local `3rdparty/`
  checkouts and the recorded spec copies.
