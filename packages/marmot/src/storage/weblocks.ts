/**
 * `webLocks` — a `LockPort` over the Web Locks API. Holding a lock = an unresolved promise returned
 * from the request callback; the release function resolves it.
 */

import type { LockPort } from "./driver.ts";

/** What `webLocks` needs of a `LockManager`. */
export type LockManagerLike = {
  request: <T>(
    name: string,
    options: { ifAvailable: boolean },
    callback: (lock: unknown) => T | Promise<T>,
  ) => Promise<T>;
};

/** Build a `LockPort` over `manager` (e.g. `navigator.locks`). */
export function webLocks(manager: LockManagerLike): LockPort {
  return {
    acquire: async (name) => {
      let release: (() => void) | undefined;
      const held = new Promise<void>((resolve) => {
        release = resolve;
      });
      const outcome: { ok?: boolean; error?: unknown; wake?: () => void } = {};
      const signal = new Promise<void>((resolve) => {
        outcome.wake = resolve;
      });
      void (async () => {
        try {
          await manager.request(name, { ifAvailable: true }, async (lock) => {
            outcome.ok = typeof lock === "object" && lock !== null;
            outcome.wake?.();
            if (outcome.ok) {
              await held;
            }
          });
        } catch (error) {
          outcome.error = error;
          outcome.wake?.();
        }
      })();
      await signal;
      if ("error" in outcome) {
        throw outcome.error;
      }
      return outcome.ok === true ? release : undefined;
    },
  };
}
