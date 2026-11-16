# marmot

Marmot (MLS over Nostr) SDK for web, iOS and Android.

**Status: pre-alpha.** `crates/mk-core` provides the pure-Rust core: a
persistent KV storage provider for the pinned MDK engine and a public
`Session` API (account-device session with injected storage, signing, clock
and transport boundaries); there are no platform bindings yet.
`packages/marmot` currently ships typed spec registries and the shared
result vocabulary only.
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
