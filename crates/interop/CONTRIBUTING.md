# crates/interop — White Noise ↔ mk-core interop scenarios

Four scenario tests (`tests/wn.rs`) drive the public `mk-core` `Session`
API against a real White Noise `wn`/`wnd` pair built from the pinned MDK
rev, over a real in-process NIP-01 WebSocket relay
(`nostr-sdk`'s `LocalRelay` from the `erskingardner/rust-nostr` fork MDK
itself pins — one nostr stack, no second nostr version). `wnd` is spawned
and reaped by the test itself; a missing or wrong-version binary under
`--ignored` is a hard failure, never a silent skip.

## Build `wn`/`wnd` from the pinned MDK rev

The pin lives in `upstream.json` (`mdk.tag` / `mdk.rev`).

`marmot-markdown` (a `wn-cli` dependency) compiles Odin sources at build
time and needs the exact pinned Odin toolchain: **Odin `dev-2026-10`, rev
`84bc3fc`** (`marmot-markdown/build.rs` `ODIN_REVISION`, also
`mdk/scripts/install-odin.sh`). Install it with MDK's own checksum-pinned
installer (macOS arm64, Linux x86_64/arm64, Windows x86_64):

```sh
# inside a checkout of marmot-protocol/mdk at the pinned rev
bash scripts/install-odin.sh "$PWD/target/odin-bin"
export ODIN="$PWD/target/odin-bin/odin"
odin version   # dev-2026-10-nightly:84bc3fc
```

Then build and install the CLI into this repository (outside `target/` —
`crates/interop/target/` is the crate's own build dir):

```sh
ODIN=<path-to-odin> cargo install --locked \
  --git https://github.com/marmot-protocol/mdk \
  --rev a2e0558a634bbd20607cdd5afb93abc6220ee58d \
  --root spike/target/wn wn-cli
```

`cargo install` takes ~12 min on a cold cache; `wn --version` must report
`wn 0.16.0` (the test asserts it against `upstream.json` `mdk.tag`).

Prebuilt binaries already exist at `spike/target/wn/bin/{wn,wnd}` from
earlier spike work — point `WN_BIN`/`WND_BIN` at them.

## Run

```sh
# crate compiles and lints without the binaries; scenario tests are ignored
cd crates/interop
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test
cargo deny check

# full scenario run (serial: scenarios share the loopback relay model)
WN_BIN=$PWD/../../spike/target/wn/bin/wn \
WND_BIN=$PWD/../../spike/target/wn/bin/wnd \
  cargo test --release --manifest-path crates/interop/Cargo.toml \
  -- --ignored --test-threads=1 --nocapture
```

Per-scenario `SCENARIO-N PASS|REFUSED <evidence> <seconds>` lines print to
stderr. `wnd` output and daemon logs land under
`crates/interop/target/wn-logs/<scenario>-<pid>/` (not committed).
Each run uses a fresh temporary `WN_HOME` under `$TMPDIR` with
`WN_SECRET_STORE=file` and `WN_ALLOW_LOOPBACK_RELAYS=1`; the home is
removed when the test's `Wnd` guard drops.

## What the scenarios assert

Ported from `spike/js/run-interop.ts`; see `spike/INTEROP.md` for the
evidence the original run recorded.

1. `scenario1_mk_core_invites_wn` — mk-core creates a group and invites
   wn by its fetched KeyPackage; the invite wn reports names the same MLS
   group id; both rosters show `{A, wn}`; messages decode both ways.
2. `scenario2_wn_invites_mk_core` — wn creates a group inviting our
   account; we join through the gift-wrapped Welcome under wn's group id;
   messages decode both ways. If wn's capability preflight refuses our
   honest KeyPackage the test asserts the exact
   `missing_required_capabilities` refusal instead.
3. `scenario3_bulk_and_membership` — wn sends 25 CLI messages, mk-core
   sends 475; one newest-first backlog `ingest_batch` decodes 25/25;
   `wn messages list` shows 475/475; a second mk-core member joins and
   converges; mk-core removes wn (wn's roster drops it, wn has no
   post-removal plaintext, the new member decodes the post-removal
   message); wn admin removes our account from a wn-founded group — or
   asserts the capability refusal if the invite is rejected.
4. `scenario4_capability_requirements` — records what wn requires of
   invitees and what our KeyPackage advertises (honest set only:
   proposals `{8, 10}`, extensions `{6}`, app components
   `{0x8001, 0x8003, 0x8004, 0x8009, 0x800C, 0x800D}` — never padded with
   `0x8006`/`0x800B`/`0xF2D1`), then asserts wn's actual behaviour when
   inviting our KeyPackage into a fresh group and into an existing group
   founded by full-capability members (mdk#2628: `RequiredCapabilities`
   is computed from the founding set).
