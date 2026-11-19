/**
 * marmot_ffi — C ABI over the mk-bridge byte protocol.
 *
 * Ownership and threading contract:
 * - `mk_bridge_new` copies the vtable and spawns one dedicated engine
 *   thread per handle. `open`/`call`/`drain` may run on any thread;
 *   jobs serialize FIFO, so every commit a call produces is delivered
 *   through the vtable before that call returns.
 * - Host callbacks run synchronously on the engine thread and must not
 *   reenter the API for the same handle.
 * - Every response `MkBytes` is Rust-owned; release it exactly once
 *   with `mk_bytes_free`. `out` may be NULL — the call still runs and
 *   nothing is written.
 * - `mk_bridge_close` blocks until the engine thread ran the bridge's
 *   close and exited; `mk_bridge_free` never blocks — without a close
 *   it enqueues shutdown and detaches the thread, which still runs
 *   `release` exactly once. `free` runs at most once per handle; calls
 *   already in flight finish on the detached engine's state.
 * - A panicking job poisons only its handle: later calls answer an
 *   encoded `poisoned` envelope and `release` still runs once.
 */

#pragma once

#include <stdarg.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>

/**
 * One account-device bridge behind the C ABI. Opaque; create with
 * `mk_bridge_new`, destroy with `mk_bridge_free`.
 *
 * `open`/`call`/`drain` may be called from any thread — they serialize
 * on the handle's job channel. `mk_bridge_free` may run while calls
 * are in flight: every entry clones this `Arc` first, so the engine
 * state outlives the box until the last in-flight call returns.
 */
typedef struct MkBridge MkBridge;

/**
 * The host storage callbacks every handle drives on its engine thread.
 *
 * `mk_bridge_new` copies the vtable; `ctx` is passed back verbatim to
 * every callback. All callbacks run synchronously on the engine thread
 * inside the `mk_bridge_*` call that triggered them, so a `commit`
 * produced by a call is always delivered before that call returns.
 * `load`/`commit` return 0 on success; any other value fails the bridge
 * (`host_failed`, then `poisoned`). A null `load`/`commit` fails the
 * same way; a null `release` is skipped.
 *
 * `load` writes a buffer the bridge copies synchronously: it must stay
 * valid until the `mk_bridge_open` call that triggered it returns.
 * `commit` bytes are valid only during the callback. `release` runs
 * exactly once, on the engine thread, after the bridge was dropped —
 * except when `mk_bridge_new` itself failed, where it never runs.
 */
typedef struct MkHostVtable {
  /**
   * Serve the durable rows blob; see the type docs.
   */
  int32_t (*load)(void *ctx, const uint8_t **ptr, size_t *len);
  /**
   * Accept one committed batch; see the type docs.
   */
  int32_t (*commit)(void *ctx, const uint8_t *ptr, size_t len);
  /**
   * Release `ctx`; see the type docs.
   */
  void (*release)(void *ctx);
} MkHostVtable;

/**
 * A Rust-owned byte buffer handed to C.
 *
 * The buffer is heap-allocated by this library; release it exactly once
 * with `mk_bytes_free`. `ptr` may be a non-null dangling value when
 * `len` is 0 — passing it to `mk_bytes_free` remains correct.
 */
typedef struct MkBytes {
  /**
   * Buffer start.
   */
  uint8_t *ptr;
  /**
   * Buffer length in bytes.
   */
  size_t len;
} MkBytes;

#ifdef __cplusplus
extern "C" {
#endif // __cplusplus

/**
 * Create a bridge handle and its engine thread; NULL on a null vtable,
 * a failed spawn or a failed runtime build (`release` never runs then).
 *
 * # Safety
 *
 * `vtable`, when non-null, must name a readable `MkHostVtable`. The
 * callbacks it carries and the `ctx` ownership follow the
 * `MkHostVtable` contract.
 */
struct MkBridge *mk_bridge_new(const struct MkHostVtable *vtable, void *ctx);

/**
 * `open` the session; writes the encoded open response to `out`.
 *
 * # Safety
 *
 * `handle` must come from `mk_bridge_new` (null answers `not_open`),
 * the input must follow the `input_slice` contract, and `out`, when
 * non-null, must name writable `MkBytes` storage.
 */
void mk_bridge_open(struct MkBridge *handle, const uint8_t *ptr, size_t len, struct MkBytes *out);

/**
 * Dispatch one `call` envelope; writes the encoded response to `out`.
 *
 * # Safety
 *
 * Same contract as `mk_bridge_open`.
 */
void mk_bridge_call(struct MkBridge *handle, const uint8_t *ptr, size_t len, struct MkBytes *out);

/**
 * Drain queued session events; writes the encoded drain response to
 * `out`.
 *
 * # Safety
 *
 * Same handle/`out` contract as `mk_bridge_open`; takes no input.
 */
void mk_bridge_drain(struct MkBridge *handle, struct MkBytes *out);

/**
 * Close the session: blocks until the engine thread ran `Bridge::close`
 * and exited. Idempotent; null is a no-op. Afterwards `open`/`call`/
 * `drain` answer `closed` (or `poisoned` on a poisoned handle).
 *
 * # Safety
 *
 * `handle` must come from `mk_bridge_new` and not have been freed.
 */
void mk_bridge_close(struct MkBridge *handle);

/**
 * Destroy the handle. Never blocks; null is a no-op.
 *
 * Without a prior `mk_bridge_close` it enqueues close+shutdown and
 * detaches the engine thread, which still runs `release` once. `free`
 * may run at most once; calls already in flight keep the engine state
 * alive through their own `Arc` and finish (or answer `poisoned` if
 * their job lands behind the shutdown).
 *
 * # Safety
 *
 * `handle` must come from `mk_bridge_new` and not have been freed;
 * `free` may run at most once.
 */
void mk_bridge_free(struct MkBridge *handle);

/**
 * Release an `MkBytes` produced by this library. `{NULL, 0}` and any
 * zero-length buffer are safe; freeing a buffer not produced here is a
 * caller error.
 *
 * # Safety
 *
 * `bytes` must have been produced by this library and not freed before.
 */
void mk_bytes_free(struct MkBytes bytes);

#ifdef __cplusplus
}  // extern "C"
#endif  // __cplusplus
