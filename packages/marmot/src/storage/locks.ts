/**
 * `localLocks` — a `LockPort` over process-local memory for hosts without Web Locks (React Native):
 * each name grants one holder until its release function runs.
 */

import type { LockPort } from "./driver.ts";

/** Build a `LockPort` whose named locks live in an in-memory set. */
export function localLocks(): LockPort {
  const held = new Set<string>();
  return {
    acquire: async (name) => {
      await Promise.resolve();
      if (held.has(name)) {
        return undefined;
      }
      held.add(name);
      return () => {
        held.delete(name);
      };
    },
  };
}
