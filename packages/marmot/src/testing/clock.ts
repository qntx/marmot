/**
 * `manualClock` — a `Clock` whose readings only move when the test advances them. Mirrors the
 * per-member clocks the mk-bridge `full_flow` drives: same default epoch, `advance` moves both
 * readings.
 */

import type { Clock } from "../backend/engine.ts";
import type { ClockReading } from "../backend/protocol.ts";

/** A `Clock` plus an explicit advance. */
export type ManualClock = Clock & {
  /** Move both unix and monotonic readings forward by `ms`. */
  advance: (ms: number) => void;
};

/** Build a `ManualClock`, seeded like the Rust test members (or from `start`). */
export function manualClock(start?: Partial<ClockReading>): ManualClock {
  let unixMs = start?.unixMs ?? 1_700_000_000_000n;
  let monotonicMs = start?.monotonicMs ?? 1000n;
  return {
    now: () => ({ unixMs, monotonicMs }),
    advance: (ms) => {
      const delta = BigInt(ms);
      unixMs += delta;
      monotonicMs += delta;
    },
  };
}
