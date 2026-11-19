/**
 * `memoryCursorStore` — an in-memory `CursorStore` for tests: `bytes` returns the last saved blob,
 * `saves` counts `save` calls, and `hold`/`release` park `save` promises so tests can observe
 * ordering across a durability gate.
 */

import type { CursorStore } from "../storage/driver.ts";

export type MemoryCursorStore = CursorStore & {
  /** The raw persisted blob (what `save` last wrote). */
  bytes: () => Uint8Array | undefined;
  /** How many `save` calls have completed. */
  saves: () => number;
  /** Make subsequent `save` calls park until `release`. */
  hold: () => void;
  /** Release every parked `save`. */
  release: () => void;
};

export function memoryCursorStore(initial?: Uint8Array): MemoryCursorStore {
  let blob = initial;
  let saved = 0;
  let held = false;
  const waiters: Array<() => void> = [];
  return {
    load: async () => {
      await Promise.resolve();
      return blob;
    },
    save: async (bytes) => {
      if (held) {
        await new Promise<void>((resolve) => {
          waiters.push(resolve);
        });
      }
      blob = bytes;
      saved += 1;
    },
    bytes: () => blob,
    saves: () => saved,
    hold: () => {
      held = true;
    },
    release: () => {
      held = false;
      const pending = waiters.splice(0);
      for (const resolve of pending) {
        resolve();
      }
    },
  };
}
