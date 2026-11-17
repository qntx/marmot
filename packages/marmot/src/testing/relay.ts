/**
 * `fakeRelay` — an in-memory stand-in for the relay set: it records every published `Outbound`,
 * answers routing queries by `h`/`p` tags, and can be told to report every target `rejected`
 * (terminal) or `unavailable` (retryable).
 */

import type { NostrEvent, Outbound, RelayResult } from "../backend/protocol.ts";

/** One published outbound item, in publish order. */
export type Published = {
  item: Outbound;
};

/** An in-memory relay set with verdict switches. */
export type FakeRelay = {
  /** Every item handed to `publish`, in order. */
  readonly publishes: Published[];
  /** Store `item` and return the per-relay verdicts to report back. */
  publish: (item: Outbound) => RelayResult[];
  /** Every stored event passing `match`, in publish order. */
  fetch: (match: (event: NostrEvent) => boolean) => NostrEvent[];
  /** Events stored from `cursor` onward (a position returned earlier). */
  since: (cursor: number) => NostrEvent[];
  /**
   * Kind-1059 gift wraps addressed (`p` tag) to `accountHex` — only the ones this account has not
   * been handed before (per-account delivery cursor).
   */
  welcomesFor: (accountHex: string) => NostrEvent[];
  /**
   * Kind-445 group events routed (`h` tag) to `routeHex` — only the ones `subscriber` has not been
   * handed before (per-(subscriber, route) delivery cursor).
   */
  groupEvents: (routeHex: string, subscriber: string) => NostrEvent[];
  /** The newest kind-30443 KeyPackage event authored by `accountHex`; throws when absent. */
  keyPackageOf: (accountHex: string) => NostrEvent;
  /** Report every target `rejected` (terminal). */
  rejectAll: boolean;
  /** Report every target `unavailable` (retryable, never terminal). */
  down: boolean;
};

function tag(event: NostrEvent, name: string): string | undefined {
  return event.tags.find((entry) => entry[0] === name)?.[1];
}

/** Build an empty `FakeRelay`. */
export function fakeRelay(): FakeRelay {
  const cursors = new Map<string, number>();
  const take = (key: string, events: NostrEvent[]): NostrEvent[] => {
    const cursor = cursors.get(key) ?? 0;
    cursors.set(key, events.length);
    return events.slice(cursor);
  };
  const relay: FakeRelay = {
    publishes: [],
    rejectAll: false,
    down: false,

    publish(item) {
      relay.publishes.push({ item });
      return item.relays.map((url) => ({
        relay: url,
        status: relay.rejectAll ? "rejected" : relay.down ? "unavailable" : "accepted",
      }));
    },

    fetch(match) {
      return relay.publishes.map((entry) => entry.item.event).filter(match);
    },

    since(cursor) {
      return relay.publishes.slice(cursor).map((entry) => entry.item.event);
    },

    welcomesFor(accountHex) {
      return take(
        `w:${accountHex}`,
        relay.fetch((event) => event.kind === 1059 && tag(event, "p") === accountHex),
      );
    },

    groupEvents(routeHex, subscriber) {
      return take(
        `g:${subscriber}:${routeHex}`,
        relay.fetch((event) => event.kind === 445 && tag(event, "h") === routeHex),
      );
    },

    keyPackageOf(accountHex) {
      const events = relay.fetch((event) => event.kind === 30_443 && event.pubkey === accountHex);
      const newest = events.at(-1);
      if (newest === undefined) {
        throw new Error("account published no KeyPackage");
      }
      return newest;
    },
  };
  return relay;
}
