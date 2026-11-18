/**
 * `fetchEvents` — a bounded one-shot fetch across relays: subscribe on every relay, collect until
 * each ends (eose or closed) or the timeout fires. Used for relay-list and KeyPackage lookups.
 */

import type { NostrEvent } from "../backend/protocol.ts";
import type { RelayFilter, RelayPort, Timer } from "./port.ts";

/** Hard bound on one `fetchEvents` call. */
export const FETCH_TIMEOUT_MS = 5000;

/**
 * Fetch `filter` on every relay in `relays` (callers pass only safe relays), resolving when each
 * subscription ended or `FETCH_TIMEOUT_MS` elapsed. Results are de-duplicated by event id.
 */
export async function fetchEvents(
  port: RelayPort,
  timer: Timer,
  relays: string[],
  filter: RelayFilter,
): Promise<NostrEvent[]> {
  return new Promise<NostrEvent[]>((resolve) => {
    const byId = new Map<string, NostrEvent>();
    const unique = [...new Set(relays)];
    const closers: Array<() => void> = [];
    let remaining = unique.length;
    let done = false;
    const finish = (): void => {
      if (done) {
        return;
      }
      done = true;
      cancelTimer();
      for (const close of closers) {
        close();
      }
      resolve([...byId.values()]);
    };
    const cancelTimer = timer.set(FETCH_TIMEOUT_MS, finish);
    if (remaining === 0) {
      finish();
      return;
    }
    // Each relay's `end` is made by the factory so the loop body declares no function.
    const makeEnd = (): (() => void) => {
      let ended = false;
      return (): void => {
        if (ended) {
          return;
        }
        ended = true;
        remaining -= 1;
        if (remaining === 0) {
          finish();
        }
      };
    };
    for (const relay of unique) {
      const end = makeEnd();
      const close = port.subscribe(relay, [filter], {
        event: (event) => {
          byId.set(event.id, event);
        },
        eose: end,
        closed: end,
      });
      if (done) {
        // A synchronous eose/closed finished the fetch mid-loop: this sub never joined.
        close();
      } else {
        closers.push(close);
      }
    }
  });
}
