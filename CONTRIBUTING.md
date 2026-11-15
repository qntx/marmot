# Contributing

## Prerequisites

- **Bun 1.4** — package scripts, tests, and tooling (`devEngines`).
- **Rust 1.99** — via the root `rust-toolchain.toml`. Only needed once crates
  land (M2); the toolchain file is already pinned so `cargo metadata` works.
- `cargo-deny` for the supply-chain check, once `crates/` exists
  (`cargo install cargo-deny`).

## Repository layout

```text
packages/marmot/      @qntx/marmot — platform-neutral TypeScript SDK
                      (today: src/core registries + result vocabulary)
vectors/              recorded spec copies frozen at the pinned rev (see
                      vectors/README.md); shared cross-language vectors later
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
```

`lint` = `vp check` (oxlint + oxfmt) + `taplo fmt --check` +
`scripts/check-version.ts`. `check:pins` verifies that local `3rdparty/`
checkouts sit at the revs recorded in `upstream.json` and that the recorded
spec copies under `vectors/spec/` are byte-identical to the pinned spec
checkout; absent checkouts are skipped (CI has no 3rdparty).

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
or bun.lock workspace entries drift from the manifests.

## License

Contributions are dual-licensed under MIT OR Apache-2.0.
