/**
 * `fetchEvents` — a bounded one-shot fetch across relays: subscribe on every relay, collect until
 * each ends (eose or closed) or the timeout fires. Used for relay-list and KeyPackage lookups. Also
 * answers which relays finished empty, so a caller can tell "the relay serves nothing in the slot"
 * from a read that never completed.
 */

import type { NostrEvent } from "../backend/protocol.ts";
import type { RelayFilter, RelayPort, Timer } from "./port.ts";
import { verifyInbound } from "./verify.ts";

/** Hard bound on one `fetchEvents` call. */
export const FETCH_TIMEOUT_MS = 5000;

/** One `fetchEvents` outcome: the collected events plus the relays that finished empty. */
export type FetchResult = {
  /** The collected events, de-duplicated by event id. */
  events: NostrEvent[];
  /**
   * Relays — unique, in input order — whose subscription signalled `eose` before the fetch finished
   * and that delivered no verified event during the fetch. A relay that `closed`, that never
   * answered before `FETCH_TIMEOUT_MS`, or that delivered any verified event is not listed; a
   * forged (unverified) event does not count as delivered.
   */
  emptyRelays: string[];
};

/**
 * Fetch `filter` on every relay in `relays` (callers pass only safe relays), resolving when each
 * subscription ended or `FETCH_TIMEOUT_MS` elapsed. Results are de-duplicated by event id.
 */
export async function fetchEvents(
  port: RelayPort,
  timer: Timer,
  relays: string[],
  filter: RelayFilter,
): Promise<FetchResult> {
  return new Promise<FetchResult>((resolve) => {
    const byId = new Map<string, NostrEvent>();
    const unique = [...new Set(relays)];
    const closers: Array<() => void> = [];
    const eoseRelays = new Set<string>();
    const servedRelays = new Set<string>();
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
      resolve({
        events: [...byId.values()],
        emptyRelays: unique.filter((relay) => eoseRelays.has(relay) && !servedRelays.has(relay)),
      });
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
          // The port is not required to verify; a forged event must not enter the dedupe
          // buffer — a same-id copy would shadow the genuine event and a forged relay list
          // with a later created_at would steer the caller.
          if (verifyInbound(event)) {
            servedRelays.add(relay);
            byId.set(event.id, event);
          }
        },
        eose: () => {
          eoseRelays.add(relay);
          end();
        },
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
