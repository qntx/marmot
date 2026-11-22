/**
 * `localLocks` — a `LockPort` over process-local memory for hosts without Web Locks (React Native):
 * each name grants one holder until its release function runs.
 *
 * The exclusion only covers this one `LockPort` instance inside this one JS realm: two instances
 * (or two realms sharing a native process) grant the same name independently, so share one instance
 * per process and prefer `webLocks` wherever more than one tab or realm can hold the storage.
 * Multiple OS processes — for example an iOS app and its notification-service extension — need a
 * host `LockPort` backed by a real file lock (`flock`, `NSFileCoordinator`); nothing in this module
 * can exclude them.
 */

import type { LockPort } from "./driver.ts";

/** Build a `LockPort` whose named locks live in an in-memory set. */
export function localLocks(): LockPort {
  const held = new Map<string, symbol>();
  return {
    acquire: async (name) => {
      await Promise.resolve();
      if (held.has(name)) {
        return undefined;
      }
      const grant = Symbol(name);
      held.set(name, grant);
      // Idempotent and ownership-bound: a stale release sees a different (or no) grant and
      // must not free a lock a later holder now owns.
      return () => {
        if (held.get(name) === grant) {
          held.delete(name);
        }
      };
    },
  };
}
