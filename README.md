# marmot

Marmot (MLS over Nostr) SDK for web, iOS and Android.

**Status: pre-alpha.** `crates/mk-core` provides the pure-Rust core: a
persistent KV storage provider for the pinned MDK engine and a public
`Session` API (account-device session with injected storage, signing, clock
and transport boundaries). `crates/mk-bridge` surfaces that API as a
byte-oriented JSON host protocol; `crates/mk-wasm` packages it as a
wasm-bindgen artifact shipped inside `@qntx/marmot-wasm`, which returns a
`Backend` for `@qntx/marmot`'s `Engine`.

```text
mk-core -> mk-bridge -> mk-wasm -> @qntx/marmot-wasm
                \
                 -> @qntx/marmot (backend port, storage drivers, testing)
```

- `packages/marmot` — platform-neutral TypeScript: `core` registries and
  result vocabulary, `backend` (`Engine`, wire codecs), `storage`
  (`indexedDbKv` with an injected row-key-bound cipher + Web Locks), `testing` (`memoryKv`,
  `manualClock`, `fakeRelay`, `runFlow`).
- `packages/marmot-wasm` — `@qntx/marmot-wasm`: the loader around the
  release wasm artifact (raw 5,332,329 B, 2,754,446 B gzipped).

## wasm build and tests

```bash
bun run --cwd packages/marmot-wasm build:wasm        # mk-wasm release + glue + pack
bun run --cwd packages/marmot-wasm check:size        # gzip -9 <= 3,000,000 B
bun run --cwd packages/marmot-wasm check:generated   # committed glue is current
bun run --cwd packages/marmot-wasm test:wasm         # Node + Bun flow and panic suites
bun run --cwd packages/marmot-wasm test:browser      # Chromium e2e (Playwright)
bun run --cwd packages/marmot-wasm test:browser:memory  # opt-in 10k-message memory run
```

See [CONTRIBUTING.md](CONTRIBUTING.md) for the wasm toolchain prerequisites
(wasm-bindgen CLI at the `Cargo.lock` version, `binaryen`, Playwright
Chromium).

Layering rules and invariants for contributors live in
[AGENTS.md](AGENTS.md); changes are tracked in [CHANGELOG.md](CHANGELOG.md);
how to build and test is in [CONTRIBUTING.md](CONTRIBUTING.md).

## License

Licensed under either of:

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or <https://www.apache.org/licenses/LICENSE-2.0>)
- MIT License ([LICENSE-MIT](LICENSE-MIT) or <https://opensource.org/licenses/MIT>)

at your option.

Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion in this project shall be dual-licensed as above, without any additional terms or conditions.

---

<div align="center">

A **[QuantX](https://qntx.org)** open-source project.

<a href="https://qntx.org"><img alt="QuantX" width="369" src="https://raw.githubusercontent.com/qntx/.github/main/profile/qntx.svg" /></a>

Code is law. We write both.

</div>
