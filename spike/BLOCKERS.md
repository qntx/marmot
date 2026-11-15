# Blockers and smells inventory — MDK v0.16.0 (rev a2e0558) + spike

Facts only. `mdk:` paths = MDK v0.16.0 source; `spike:` = this spike.

## Async runtime

- `cgka-engine` declares `tokio = { features = ["time"] }` (lib) and
  `["macros","rt-multi-thread"]` (dev) — `mdk:crates/cgka-engine/Cargo.toml:49,60,74`.
  The engine library itself builds on wasm32 without a tokio runtime: its
  async engine calls are plain `async fn` on the caller's executor (the
  spike drives them on wasm-bindgen-futures' microtask executor). Tokio
  `time`/`rt` enters only through test/simulator targets and the CLI.
- `transport-nostr-peeler` is the same shape
  (`mdk:crates/transport-nostr-peeler/Cargo.toml:24,30` — tokio is dev-only).
- A React Native backend therefore needs an executor for the async engine —
  tokio `current_thread` or `futures::executor` both suffice; nothing in the
  engine requires a multi-thread runtime.
- Sync `MarmotNostrSigner` futures must be `Send` — on wasm32 `JsFuture` is
  `!Send`, bridged with `send_wrapper` (`spike:wasm/src/lib.rs`). Safe only
  because wasm32 is single-threaded.

## Nostr crate coupling

- MDK pins the **erskingardner/rust-nostr fork** at rev
  `e93cc06b5624f06c21ecd8111a124176eb0c7589` for `nostr`, `nostr-sdk`, and
  `nostr-gossip` (`mdk:Cargo.toml:134-136`). Any host type that must unify
  (events, `Keys`, signers) must come from the same fork/revision — crates.io
  `nostr` produces incompatible types.
- The engine proper uses `nostr` types lightly (event DTOs, key handling);
  the peeler uses it for NIP-59 gift wrap, NIP-44, and event signing
  (`mdk:crates/transport-nostr-peeler/src/{nip59,signer,event}.rs`).
- The fork requires a `universal-time` provider on wasm32 — link error
  without `define_time_provider!` (`spike:wasm/src/lib.rs` `JsTimeProvider`).

## Time sources

- `web-time` is the engine's `Instant` source (`mdk:crates/cgka-engine/
  src/message_processor/mod.rs:83`, Cargo.toml:41) — wasm-safe.
- `universal-time` (via the nostr fork) needed a provider we supply.
- `gloo-timers` futures feature in cgka-engine (`Cargo.toml:54`) — wasm-only
  timers, fine on web.
- `chrono` appears only in `crates/cli` and `marmot-observability`, not in
  the engine/peeler path.
- Engine callers inject a `ConvergenceClock` + maintenance wall-clock/
  randomness; the spike binds `JsWallClock` + `ManualConvergenceClock`.

## Randomness

- `getrandom = "0.2"` with `js` feature (`mdk:crates/cgka-engine/
  Cargo.toml:53`) — required for wasm32; the spike also enables it for the
  routing id and key-package nonces.
- OpenMLS/RustCrypto randomness flows through `rand_core`; `rand 0.8`
  `thread_rng` appears in test/dev code, not on the wasm engine path.

## Threads

- No `std::thread`/`rayon` on the engine library path (greps come back
  test-only). `StorageProvider::Mls` must be `Send + Sync`
  (`mdk:crates/traits/src/storage.rs:1283`) and `AccountIdentityProofSigner`
  requires `Send + Sync` (`mdk:crates/cgka-engine/src/
  account_identity_proof.rs:193`) — on wasm32 these bounds are satisfiable
  but force `send_wrapper` around JS futures.

## wasm32 traps (compiles → traps/links wrong)

1. `universal-time` provider missing → link error (fixed via
   `define_time_provider!`).
2. `serde_wasm_bindgen` default `Map` output → `{}` returns (fixed via
   `Serializer::json_compatible()`).
3. `Promise::from(non-promise)` never resolves — JS async hooks must return
   real Promises.
4. `getrandom` without `js` → trap at first RNG call — feature is on.
5. No `SystemTime::now`/`Instant` traps: engine uses `web_time`.
   `maintenance.rs:101` uses `SystemTime` — compiled only on non-wasm paths
   (the wasm build links cleanly, so it is cfg'd or unused there).

## Sync storage + sync signer

- **Every `StorageProvider` method is sync** (`mdk:crates/traits/src/
  storage.rs`) — IndexedDB is async, so the spike mirrors all state in an
  in-memory `BTreeMap` loaded once at `Client.open` and hands the host one
  atomic commit batch per committed unit (`spike:engine/src/provider.rs`).
  Durability consequence documented in `STORAGE.md`: the batch must persist
  before publish (publish-before-apply).
- **`AccountIdentityProofSigner::sign_account_identity_proof` is sync**
  (`mdk:crates/cgka-engine/src/account_identity_proof.rs:193-197`), called
  inside `fresh_key_package`/self-update leaf construction. The request
  embeds a fresh MLS signature pubkey + timestamp → **cannot be pre-signed**.
  A remote NIP-07/46 signer (async) cannot serve it; the JS side must hold a
  synchronously-signing key, or MDK needs an async seam. Report item.

## Smells

- `Backend` enum has exactly one variant, `Sqlite` (`mdk:crates/traits/src/
  types.rs:91-94`) — a non-SQLite provider must lie in `backend()`. Upstream
  gap, cosmetic but observable.
- `CapabilityRequirement.description` is `&'static str` → the spike's
  provider `Box::leak`s bounded strings to satisfy it
  (`spike:engine/src/provider.rs:1208`). Bounded leak, noted.
- 2 explicit stubs, both in the spike's `MarmotNostrSigner`:
  `nip04_encrypt`/`nip04_decrypt` (`spike:wasm/src/lib.rs:246-268`) return
  distinct errors; unreachable because NIP-59 wrap/peel uses NIP-44 only.

## Capability surface (from interop)

- Bare `EngineBuilder` KPs lack wn-required capabilities — proposal 10
  (MIP-03 SelfRemove), extension 0xF2D1, components 0x8006/0x800B/0x800D.
  Fix exists (`feature_registry` + `supported_app_components`), but note
  proposal 10 lives in the *app* registry (`marmot-app::app_feature_registry`,
  `mdk:crates/marmot-app/src/lib.rs:8330`), not the engine — SDKs must
  mirror it or accept reduced groups.
- Advertising 0x8006/0xF2D1 claims QUIC stream support the spike does not
  implement — fine for interop probing, not for production.

## Licenses — wasm dependency tree (210 crates)

`cargo tree --target wasm32-unknown-unknown -p marmot-spike-wasm` +
`cargo metadata` licenses, counted:

| license | crates |
|---|---|
| MIT OR Apache-2.0 / Apache-2.0 OR MIT | 136 |
| MIT | 25 |
| Unicode-3.0 / (MIT OR Apache-2.0) AND Unicode-3.0 | 20 |
| CC0-1.0 / Unlicense OR MIT | 9 |
| Apache-2.0 (incl. Apache-2.0/MIT, MIT/Apache-2.0) | 12 |
| BSD-2/3-Clause (incl. `BSD-2-Clause OR Apache-2.0 OR MIT`) | 6 |
| MPL-2.0 | 3 |
| Zlib OR Apache-2.0 OR MIT | 1 |
| NONE (our own `marmot-spike`, `marmot-spike-wasm`) | 2 |

**Nothing outside MIT/Apache-2.0/BSD/ISC/Zlib/Unicode/CC0/Unlicense/MPL.**
No copyleft, no proprietary, no GPL-family licenses in the wasm tree.
