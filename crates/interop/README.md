# crates/interop — White Noise ↔ mk-core interop scenarios

Scenario tests (`tests/wn.rs`) driving the public `mk_core::Session` API
against a real White Noise `wn`/`wnd` pair over an in-process NIP-01 relay.
Standalone project (own workspace/lockfile); the scenario tests are
`#[ignore]`d and need `WN_BIN`/`WND_BIN` — CI runs them (`interop-rust`)
against `wn`/`wnd` built from the upstream.json `wn` pin. With
`INTEROP_RELAY_URL=<ws-url>` the same suite runs against an external
relay instead of the in-process one (welcome fetches then answer
NIP-42 per account); the advisory `interop-nosfry` workflow uses that
to run it nightly against the pinned nosfry release. Locally, see
the repository-root
[CONTRIBUTING.md](../../CONTRIBUTING.md#white-noise-interop-cratesinterop).
