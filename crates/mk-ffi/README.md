# mk-ffi

`mk-ffi` is the C ABI shell over [`mk-bridge`](../mk-bridge) for native
iOS/Android hosts (the React Native backend). It marshals nothing itself:
bytes in → `mk_bridge::Bridge` → bytes out, identical envelopes to the
wasm shell. The generated, committed header is
[`include/marmot_ffi.h`](include/marmot_ffi.h); regenerate it with

```bash
cbindgen --config crates/mk-ffi/cbindgen.toml --crate mk-ffi \
    --output crates/mk-ffi/include/marmot_ffi.h crates/mk-ffi
```

`cargo test -p mk-ffi --test header` regenerates it in memory and asserts
byte equality, so the suite is the freshness check.

It is the only crate in the workspace allowed `unsafe`, and only inside
its `abi` module — every `unsafe` block carries a `// SAFETY:` comment
stating the contract it relies on.

## Threading model

One `mk_bridge_new` spawns one dedicated engine thread (named
`mk-engine`) and moves a `Bridge` onto it; the `!Send` bridge never
leaves that thread. `open`/`call`/`drain` send a job over a channel and
block the calling thread on a per-job reply — jobs run strictly FIFO, so
concurrent callers from several native threads serialize. Host `load` /
`commit` callbacks fire synchronously inside the job that produced them,
so every commit of a call is delivered before that ABI call returns.

`mk_bridge_close` enqueues a shutdown job, waits for the engine thread to
run `Bridge::close` and exit, then joins it. `mk_bridge_free` never
blocks: it enqueues shutdown on a second sender and detaches the thread,
which still runs `release` exactly once after the bridge is dropped.
Every entry point first clones an `Arc` of the shared engine state, so a
call already in flight keeps that state alive after `free` released the
handle — it either gets its reply (if its job reached the engine before
the shutdown) or an encoded `poisoned` envelope.

A panicking job is contained by `catch_unwind`: the bridge is dropped on
the engine thread, the handle is marked poisoned, later calls answer the
encoded `poisoned` envelope, and `close`/`free` still work. The
`test-panic` feature adds `mk_bridge_test_panic` to exercise this; it is
never enabled in shipped builds.

## Tokio runtime requirements

Measured in `tests/timing.rs` — the evidence behind the engine thread's
runtime setup. The only Tokio API reachable through `mk-bridge` is
`cgka-engine`'s `deadline::timeout` on the deferred-peel reingest path
(the probe defers a signed-but-undecryptable kind-445 row, changes the
peel context, and lets a send's admission sweep re-ingest it under the
foreground budget):

- `bare_thread_panics_inside_engine_work` — no runtime in context:
  `there is no reactor running, must be called from the context of a
  Tokio 1.x runtime`.
- `runtime_without_time_panics` — a `current_thread` runtime entered
  without `enable_time`: `A Tokio 1.x context was found, but timers are
  disabled. Call enable_time on the runtime builder to enable timers.`
- `entered_undriven_runtime_passes` — a `current_thread` runtime with
  `enable_time()` entered for the thread's life but never driven: the
  whole flow passes. The bridge polls its futures with a noop waker, so
  progress never needs the reactor to run — only the timer registration
  the driver context provides.
- `undriven_deadline_only_fires_after_a_driver_turn` — an undriven
  driver lets a `tokio::time::sleep` stay `Pending` past its real-time
  deadline; one driver turn (`runtime.block_on(yield_now())`) releases
  it on the next poll.

So the engine thread owns a `new_current_thread().enable_time()` runtime
and turns the driver once per job (`yield_now` inside `block_on` before
the bridge runs), giving an elapsed deadline the same chance to fire it
gets on wasm, where gloo timers run between calls.

## Engine-thread stack size

8 MiB (`ENGINE_STACK_BYTES`). A single bridge poll descends the whole
`Session` → `cgka-engine` → OpenMLS poll chain on the stack (the futures
are boxed, the poll frames are not): measured, a
create→invite→join→send flow overflows a 2 MiB engine stack in debug
builds and fits comfortably in 4 MiB. 8 MiB ships the same headroom a
platform main-thread stack gets.

## Miri

`cargo +nightly miri test -p mk-ffi` covers the session-free subset:
most of `tests/inputs.rs`, `tests/lifecycle.rs` and all of
`tests/panic.rs` (they use raw handles and a fixed config, never a
signer) plus `undriven_deadline_only_fires_after_a_driver_turn` in
`tests/timing.rs`. Everything that opens a session links
`secp256k1-sys` C code Miri cannot execute, and `tests/header.rs` reads
files (Miri isolation), so those carry `#[cfg_attr(miri, ignore)]` with
the reason.

## Building for native targets

Shipped artifacts build with the `native` profile — `panic = "unwind"`
is required (the release profile aborts, which would defeat
`catch_unwind` containment):

```bash
cargo build -p mk-ffi --profile native --target aarch64-apple-ios
cargo build -p mk-ffi --profile native --target aarch64-apple-ios-sim
cargo build -p mk-ffi --profile native --target aarch64-linux-android
cargo build -p mk-ffi --profile native --target x86_64-linux-android
```

Android needs the NDK clang (see the repository `AGENTS.md` /
`CONTRIBUTING.md`). Assembling the staticlib into an xcframework or a
JNI `.so` is a local packaging step, not a CI gate.
