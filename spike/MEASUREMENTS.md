# M0 Phase B measurements — wasm32, Bun, Chromium

Same `profile.wasm` build (`opt-level z`, lto, codegen-units 1, strip) for both
runtimes. Raw data: `results/bun.json`, `results/chromium.json`. Medians
reported where n > 1.

## wasm / JS glue size

| artifact | raw | gzip -9 | brotli -q 11 |
|---|---|---|---|
| `marmot_spike_wasm_bg.wasm` (post wasm-bindgen) | 5 652 968 | 2 536 020 | 2 141 151 |
| same after `wasm-opt -Oz` | 5 074 027 | 2 670 235 | 2 237 651 |
| `marmot_spike_wasm.js` glue | 37 175 | 7 833 | — |

`wasm-opt` needed `--enable-mutable-globals --enable-sign-ext
--enable-nontrapping-float-to-int --enable-bulk-memory`.

**Finding:** `wasm-opt -Oz` shrinks the raw binary 10.2 % but makes the
*compressed* size ~5 % *larger* (gzip 2.54→2.67 MB, brotli 2.14→2.24 MB). For a
web SDK shipped over HTTP(S) the unoptimized-for-compression post-bindgen
binary is the better artifact; `-Oz` only wins if shipping raw.

## Runtime measurements

| metric | Bun 1.4.4 | Chromium 142 (headless) |
|---|---|---|
| scenario total (7 steps, 500 msgs) | ~97 s | ~99 s |
| KeyPackage generation | median 1.0 ms (n = 24) | median 1.1 ms (n = 24) |
| ascending ingest, 507 events | 199 ms total, 0.39 ms/msg | 320 ms total, 0.63 ms/msg |
| 50-member create (49 KPs, one commit) | 342 ms | 404 ms |
| joiner Welcome processing (50-member) | 29 ms | 36 ms |
| add-1-member commit (50-member group) | 168 ms | 205 ms |
| existing-member ingest of add-1 commit | 0.12 ms | 0.30 ms |
| peak memory | rss 275 MB, heapUsed 175 MB, wasm buffer 57.7 MB | JSHeapUsed 19.5 MB, wasm buffer 45.9 MB |

Chromium ingest includes a `page.evaluate` round-trip per event; the pure
in-wasm cost is closer to Bun's number. The scenario total is dominated by 500
sequential `send` + `ingest` calls over the JS boundary.

## wasm32 traps / blockers encountered

1. **`universal-time` provider missing** — link error
   `a time provider is required. Use define_time_provider!`.
   The erskingardner/rust-nostr fork needs a `universal_time` provider on
   `wasm32-unknown-unknown`. Fixed by `JsTimeProvider` in `spike/wasm` backed
   by `web-time` + `universal_time::define_time_provider!`.
2. **`serde_wasm_bindgen::to_value` emits JS `Map`** — every return value
   stringified to `{}`. Fixed with
   `Serializer::json_compatible()` (`to_js` in `spike/wasm/src/lib.rs`).
3. **`MarmotNostrSigner` futures must be `Send`, `JsFuture` is `!Send`** —
   bridged with `send_wrapper` (futures feature); safe only because wasm32 is
   single-threaded.
4. **`AccountIdentityProofSigner::sign_account_identity_proof` is SYNC** — a
   NIP-07/46-style remote signer cannot serve it. The JS signer must hold a
   key able to sign synchronously (documented in the Client.open docstring).
5. **Non-Promise return to a `JsFuture` await** — a sync JS callback wrapped in
   `Promise::from` does not become a promise; every signer async hook must
   return a real Promise (Chromium pageerror `getObject(...).then is not a
   function`).
6. **kind-445 `created_at` binds the inner app event's** (`peeler.rs`, package
   E). Backlog sort order is only meaningful if inner `created_at` differs per
   message — a constant `createdAt` degenerates sort to the random event-id
   tiebreak and reproduces the newest-first loss.
7. **`wasm-opt` feature flags** — see above; without them binaryen's
   validator rejects `i64.trunc_sat_f64_*` (nontrapping-fptoint).

## Reproduce

```sh
# build (CC needed only if a C dep appears; none did)
cd spike
cargo build -p marmot-spike-wasm --target wasm32-unknown-unknown --profile wasm
wasm-bindgen --target web --out-dir wasm/pkg \
  target/wasm32-unknown-unknown/wasm/marmot_spike_wasm.wasm

# native reference flow
cargo test --release --manifest-path engine/Cargo.toml -- --nocapture

# Bun driver + measurements → spike/results/bun.json
bun spike/js/run-bun.ts

# Chromium driver (Playwright; real headless Chromium + IndexedDB + reload)
bun spike/js/run-chromium.ts

# wasm-opt comparison
spike/js/node_modules/binaryen/bin/wasm-opt -Oz \
  --enable-mutable-globals --enable-sign-ext \
  --enable-nontrapping-float-to-int --enable-bulk-memory \
  -o spike/results/marmot_spike_wasm_opt.wasm \
  spike/wasm/pkg/marmot_spike_wasm_bg.wasm
```

## Binding-choice evidence

`cargo tree --target wasm32-unknown-unknown -e normal -i wasm-bindgen` shows
`wasm-bindgen` is already a transitive dependency of the engine stack:
`getrandom 0.2/js` ← `cgka-engine`, `openmls/js` + `openmls_rust_crypto`,
`gloo-timers` ← `cgka-engine`, `web-time`, the erskingardner `nostr` fork, and
`wasm-bindgen-futures`. The glue exists in the artifact regardless; a C ABI
would add an app-owned executor, pointer/byte ownership plumbing, and a
promise bridge for every async engine call — pure cost, no dependency saving.
So: wasm-bindgen + wasm-bindgen-futures, `--target web` (one artifact serving
Bun and the browser — Bun loads the same `init({module_or_path})` entry).

## C0: send/ingest/convergence breakdown (bench-breakdown.json)

Focused 100-message benchmark (`js/bench-breakdown.ts`), Bun:

| op | n | median ms | p95 ms | max ms |
|---|---|---|---|---|
| `send` (JS → kind-445 event) | 100 | 1.901 | 9.834 | 45.744 |
| `ingest` (cold backlog) | 100 | 0.897 | 8.420 | 38.904 |
| `advanceConvergence` pass | 2 | 1.641 | 1.641 | 1.641 |

The isolated benchmark's convergence cost is ~2 ms total (2 passes) — the
scenario's long tail is **not** the convergence driver and not the JS
boundary. Aggregated scenario timings (results/bun.json `apiTimings`):
`ingest` 6 299 calls ≈ 298.7 s median 47.4 ms — every step re-ingests the
whole accumulated backlog per member, so ingest against an increasingly hot
ledger dominates (hot-ledger median ≈ 47 ms vs cold-backlog ≈ 0.9 ms). That
is harness amplification, not an engine property: a real SDK ingests each
event once. `send` 508 calls ≈ 2.74 s median 5.4 ms; `advance` 40 calls
≈ 8.4 s median 210 ms (driven per-step, includes commit fanout waits).

## In-page Chromium ingest

`run-chromium.ts` now measures the full 507-event ascending backlog inside a
single `page.evaluate` (no per-event round-trip): **342.2 ms total,
0.675 ms/event** (chromium.json `ingestMs`). Comparable to Bun's 199 ms /
0.39 ms; both far under any 1 s threshold.

## Size attribution

Current artifact (profile.wasm, strip on) section profile
(`wasm/pkg/marmot_spike_wasm_bg.wasm`, 5 662 935 B):

| section | bytes | % |
|---|---|---|
| code (sec10) | 4 035 837 | 71.3 |
| data (sec11) | 1 595 917 | 28.2 |
| function `name` custom section | 498 | ≈0 |
| producers | 60 | ≈0 |

`strip` already removes function names and debug info — the
`wasm-named` profile (same build, `strip = false`, `debug = false`)
keeps a 5 135 334-byte `name` section (47.6 % of the named artifact),
so `strip` is the single biggest cheap lever and is already in the
profile. The remaining data section (~1.6 MB) is Rust metadata:
source/panic paths, panic/assert strings, type and symbol paths,
MLS/Marmot error strings, URL/encoding tables — not removable without
code changes.

## Crate-level attribution (twiggy, `wasm-named`, all 19 464 rows)

`twiggy top --format csv` on the post-bindgen named artifact
(`target/pkg-named/marmot_spike_wasm_bg.wasm`, 10 792 860 B), aggregated
by first crate token in each symbol path; generic instantiations count
toward the crate of the type they instantiate. Raw CSV:
`results/twiggy-named-all.csv`. Code section ≈ 4 053 590 B.

| crate bucket | shallow bytes | % of code |
|---|---|---|
| std/core/support (std, alloc, hashbrown, misc support crates) | 1 062 100 | 26.2 % |
| MDK (`cgka_engine`+`cgka_traits`+`marmot_forensics`+`transport_nostr_peeler`+`fs_private`) | 986 840 | 24.3 % |
| serde stack (`serde`, `serde_json`, `serde_wasm_bindgen`, `serde_bytes`) | 841 171 | 20.8 % |
| OpenMLS stack (`openmls` + traits/credential/memory-storage helpers) | 424 306 | 10.5 % |
| RustCrypto/HPKE (dalek, p256/p384/k256, secp256k1 C, aes/chacha20, hpke-rs, libcrux) | 403 512 | 10.0 % |
| **marmot_spike (ours: provider + binding)** | **191 008** | **4.7 %** |
| url/idna/unicode (`url`, `idna`, `icu_*` via the nostr fork's URL parsing) | 44 366 | 1.1 % |
| wasm-bindgen/JS glue | 32 958 | 0.8 % |
| nostr fork (erskingardner) | 32 307 | 0.8 % |
| tls_codec/postcard, encoding, time, tracing, futures, other | ~26 000 | <0.7 % |

## Levers measured (each number = a build actually run)

| lever | raw | gzip -9 | brotli -11 | verdict |
|---|---|---|---|---|
| baseline bindgen (`opt-level = "z"`, strip) | 5 662 935 | 2 537 058 | 2 144 310 | **best compressed artifact** |
| Rust `opt-level = "s"` (same profile otherwise) | 6 289 827 | 2 759 354 | 2 254 250 | strictly worse than `z` (+9 % gzip): lose |
| Rust `opt-level = 3` (same profile otherwise) | 7 420 879 | 3 174 347 | 2 515 598 | much worse (+25 % gzip): lose |
| `opt-level = "z"` WITHOUT strip (`wasm-named`) | 10 792 860 | 3 067 086 | 2 371 481 | strip saves ~5.1 MB raw / ~0.5 MB gzip |
| `wasm-opt -Os` on the `z` build | 5 140 006 | 2 669 444 | 2 236 202 | raw −9 %, compressed +5 %: lose |
| `wasm-opt -Oz` on the `z` build | 5 079 852 | 2 671 696 | 2 241 506 | same, worse |
| `panic = "abort"` | — | — | — | no-op: wasm32 panic is abort already; `catch_unwind` appears only in MDK tests (`candidate_replay.rs`, `rewind_restore_fault.rs`), never in production code paths |

**Floor:** 5.66 MB raw / **2.54 MB gzip** / 2.14 MB brotli as built. To hit
1.5 MB gzip (≈ 3.3 MB raw at the observed ratio) the code section would
have to shrink by ~2.2 MB raw — more than std/core (1.06 MB) + serde
(0.84 MB) **combined**, and every one of those bytes is pulled in by MDK's
own dependency graph (the engine needs serde + OpenMLS + RustCrypto; the
nostr fork pulls url/idna). **Our own crate contributes 191 KB of code
(4.7 %)** — deleting all of it would save well under 0.1 MB gzip. The
1.5 MB threshold is therefore **not reachable without changing MDK
itself** (e.g. a feature-gated lite engine surface, replacing
`serde_json` event codecs, or pruning forensics), not through profile or
post-processing knobs on our side.
