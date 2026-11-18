/**
 * `planSubscriptions` — the pure subscription planner. Given the account's inbox relays, the live
 * groups (route + relay candidates, already safe-filtered) and the persisted per-cursor `since`
 * values, it produces the deterministic set of open subscriptions under the relay and subscription
 * caps, plus the routes it could not cover.
 */

import type { RelayFilter } from "./port.ts";

/** Kind-445 group events. */
export const GROUP_EVENT_KIND = 445;
/** Kind-1059 NIP-59 gift wraps. */
export const GIFT_WRAP_KIND = 1059;
/** Distinct relays with open subscriptions (sockets). */
export const MAX_RELAYS = 16;
/** Open subscriptions across all relays. */
export const MAX_SUBSCRIPTIONS = 64;
/** Relays per group route; the spec does not require fetching from every relay. */
export const RELAYS_PER_GROUP = 2;
/** One filter per group route, packed per relay. */
export const FILTERS_PER_SUBSCRIPTION = 8;
/** Resubscribe overlap tolerating sender clock skew on group events. */
export const GROUP_OVERLAP_SECS = 600;
/**
 * NIP-59 wrap timestamps are tweaked up to two days into the past ("SHOULD be tweaked … up to two
 * days in the past"; MDK `transport-nostr-adapter` `NIP59_TIMESTAMP_TWEAK_SECS = 172_800`, which
 * widens its inbox `since` by the same amount).
 */
export const GIFT_WRAP_OVERLAP_SECS = 172_800;

export type PlanGroup = {
  /** The group's `#h` route hex. */
  route: string;
  /** Candidate relays, already safe-filtered. */
  relays: string[];
};

export type PlanInput = {
  account: string;
  /** The account's inbox relays, already safe-filtered. */
  inboxRelays: string[];
  groups: PlanGroup[];
  /** Persisted cursor lookup; unix seconds. */
  cursor: (key: string) => number | undefined;
};

export type PlannedSubscription = {
  /** Identity for diffing: relay + kind + sorted routes (NOT since). */
  key: string;
  relay: string;
  kind: 445 | 1059;
  /** Group routes this subscription covers; empty for 1059. */
  routes: string[];
  filters: RelayFilter[];
};

export type Plan = { subscriptions: PlannedSubscription[]; uncovered: string[] };

export function inboxCursorKey(relay: string): string {
  return `inbox|${relay}`;
}

export function groupCursorKey(route: string, relay: string): string {
  return `group|${route}|${relay}`;
}

/** Deterministic subscription plan; routes not covered appear in `uncovered`. */
export function planSubscriptions(input: PlanInput): Plan {
  const selected: string[] = [];
  const selectedSet = new Set<string>();
  const inbox = new Set<string>();
  const routesByRelay = new Map<string, Set<string>>();
  const assignedByRoute = new Map<string, string[]>();
  let totalSubs = 0;

  for (const relay of input.inboxRelays) {
    if (selectedSet.has(relay)) {
      continue;
    }
    if (selected.length >= MAX_RELAYS) {
      break;
    }
    selected.push(relay);
    selectedSet.add(relay);
    inbox.add(relay);
  }
  totalSubs = inbox.size;

  const subsOn = (relay: string): number =>
    (inbox.has(relay) ? 1 : 0) +
    Math.ceil((routesByRelay.get(relay)?.size ?? 0) / FILTERS_PER_SUBSCRIPTION);

  const groups = [...input.groups].sort((a, b) => (a.route < b.route ? -1 : 1));
  for (const group of groups) {
    assignedByRoute.set(group.route, []);
  }

  const tryAssign = (group: PlanGroup, relay: string): boolean => {
    const assigned = assignedByRoute.get(group.route);
    if (assigned === undefined || assigned.includes(relay)) {
      return false;
    }
    if (!selectedSet.has(relay) && selected.length >= MAX_RELAYS) {
      return false;
    }
    const before = subsOn(relay);
    let routes = routesByRelay.get(relay);
    if (routes === undefined) {
      routes = new Set();
      routesByRelay.set(relay, routes);
    }
    routes.add(group.route);
    const delta = subsOn(relay) - before;
    if (totalSubs + delta > MAX_SUBSCRIPTIONS) {
      routes.delete(group.route);
      return false;
    }
    if (!selectedSet.has(relay)) {
      selected.push(relay);
      selectedSet.add(relay);
    }
    assigned.push(relay);
    totalSubs += delta;
    return true;
  };

  // Pass 1: one relay per group — the first of its relays already selected, else its first relay
  // when the relay cap allows.
  for (const group of groups) {
    const selectedRelay = group.relays.find((relay) => selectedSet.has(relay));
    const candidate = selectedRelay ?? group.relays[0];
    if (candidate !== undefined) {
      tryAssign(group, candidate);
    }
  }
  // Pass 2: fill each group up to RELAYS_PER_GROUP the same way, first fit per relay order.
  for (const group of groups) {
    for (const relay of group.relays) {
      const assigned = assignedByRoute.get(group.route);
      if (assigned === undefined || assigned.length >= RELAYS_PER_GROUP) {
        break;
      }
      tryAssign(group, relay);
    }
  }

  const subscriptions: PlannedSubscription[] = [];
  for (const relay of selected) {
    if (inbox.has(relay)) {
      const filter: RelayFilter = { kinds: [GIFT_WRAP_KIND], "#p": [input.account] };
      const cursor = input.cursor(inboxCursorKey(relay));
      if (cursor !== undefined) {
        filter.since = Math.max(0, cursor - GIFT_WRAP_OVERLAP_SECS);
      }
      subscriptions.push({
        key: `${relay}|${GIFT_WRAP_KIND}`,
        relay,
        kind: GIFT_WRAP_KIND,
        routes: [],
        filters: [filter],
      });
    }
    const routes = [...(routesByRelay.get(relay) ?? [])].sort();
    for (let i = 0; i < routes.length; i += FILTERS_PER_SUBSCRIPTION) {
      const chunk = routes.slice(i, i + FILTERS_PER_SUBSCRIPTION);
      const filters = chunk.map((route): RelayFilter => {
        const filter: RelayFilter = { kinds: [GROUP_EVENT_KIND], "#h": [route] };
        const cursor = input.cursor(groupCursorKey(route, relay));
        if (cursor !== undefined) {
          filter.since = Math.max(0, cursor - GROUP_OVERLAP_SECS);
        }
        return filter;
      });
      subscriptions.push({
        key: `${relay}|${GROUP_EVENT_KIND}|${chunk.join(",")}`,
        relay,
        kind: GROUP_EVENT_KIND,
        routes: chunk,
        filters,
      });
    }
  }

  const uncovered = groups
    .filter((group) => (assignedByRoute.get(group.route)?.length ?? 0) === 0)
    .map((group) => group.route);
  return { subscriptions, uncovered };
}
