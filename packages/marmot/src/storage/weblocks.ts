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
      const acquired = new Promise<boolean>((resolve) => {
        void manager.request(name, { ifAvailable: true }, async (lock) => {
          if (typeof lock !== "object" || lock === null) {
            resolve(false);
            return;
          }
          resolve(true);
          await held;
        });
      });
      return (await acquired) ? release : undefined;
    },
  };
}
