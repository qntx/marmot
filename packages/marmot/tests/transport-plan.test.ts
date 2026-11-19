import { describe, expect, test } from "vite-plus/test";

import {
  FILTERS_PER_SUBSCRIPTION,
  GIFT_WRAP_OVERLAP_SECS,
  GROUP_OVERLAP_SECS,
  MAX_RELAYS,
  MAX_SUBSCRIPTIONS,
  RELAYS_PER_GROUP,
  groupCursorKey,
  inboxCursorKey,
  planSubscriptions,
} from "../src/transport/plan.ts";
import type { PlanGroup, PlanInput } from "../src/transport/plan.ts";

const ACCOUNT = "a".repeat(64);
const R = (n: number): string => `wss://r${n}.example`;
const G = (n: number): string => n.toString(16).padStart(64, "0");

function input(partial: Partial<PlanInput>): PlanInput {
  return {
    account: ACCOUNT,
    inboxRelays: [],
    groups: [],
    cursor: () => undefined,
    ...partial,
  };
}

function group(route: string, relays: string[]): PlanGroup {
  return { route, relays };
}

describe("planSubscriptions — inbox", () => {
  test("one 1059 subscription per inbox relay, #p = account", () => {
    const plan = planSubscriptions(input({ inboxRelays: [R(1), R(2)] }));
    expect(plan.subscriptions).toHaveLength(2);
    for (const [i, relay] of [R(1), R(2)].entries()) {
      const sub = plan.subscriptions[i];
      expect(sub?.relay).toBe(relay);
      expect(sub?.kind).toBe(1059);
      expect(sub?.routes).toStrictEqual([]);
      expect(sub?.filters).toStrictEqual([{ kinds: [1059], "#p": [ACCOUNT] }]);
    }
    expect(plan.uncovered).toStrictEqual([]);
  });

  test("an inbox cursor widens since by the gift-wrap overlap, clamped at 0", () => {
    const cursors = new Map([[inboxCursorKey(R(1)), 100_000]]);
    const plan = planSubscriptions(
      input({ inboxRelays: [R(1)], cursor: (key) => cursors.get(key) }),
    );
    expect(plan.subscriptions[0]?.filters[0]?.since).toBe(
      Math.max(0, 100_000 - GIFT_WRAP_OVERLAP_SECS),
    );
    // A cursor younger than the overlap clamps at 0.
    const young = planSubscriptions(
      input({
        inboxRelays: [R(1)],
        cursor: () => GIFT_WRAP_OVERLAP_SECS - 1,
      }),
    );
    expect(young.subscriptions[0]?.filters[0]?.since).toBe(0);
  });

  test("no cursor → no since", () => {
    const plan = planSubscriptions(input({ inboxRelays: [R(1)] }));
    expect(plan.subscriptions[0]?.filters[0]?.since).toBeUndefined();
  });
});

describe("planSubscriptions — groups", () => {
  test("one group prefers an already-selected relay, then fills to RELAYS_PER_GROUP", () => {
    const plan = planSubscriptions(
      input({ inboxRelays: [R(1)], groups: [group(G(1), [R(1), R(9)])] }),
    );
    const kinds = plan.subscriptions.map((sub) => sub.kind);
    expect(kinds).toStrictEqual([1059, 445, 445]);
    const [, first] = plan.subscriptions;
    expect(first?.relay).toBe(R(1));
    expect(first?.routes).toStrictEqual([G(1)]);
    expect(first?.filters).toStrictEqual([{ kinds: [445], "#h": [G(1)] }]);
    expect(plan.subscriptions[2]?.relay).toBe(R(9));
  });

  test("a group fills up to RELAYS_PER_GROUP relays, preferring already-selected ones", () => {
    const plan = planSubscriptions(
      input({
        inboxRelays: [R(1)],
        groups: [group(G(1), [R(2), R(1), R(3)])],
      }),
    );
    const subs445 = plan.subscriptions.filter((sub) => sub.kind === 445);
    expect(subs445.map((sub) => sub.relay).toSorted()).toStrictEqual([R(1), R(2)]);
    expect(RELAYS_PER_GROUP).toBe(2);
  });

  test("groups sort by route; relay order within a group is kept", () => {
    const plan = planSubscriptions(
      input({
        groups: [group(G(9), [R(1)]), group(G(2), [R(2)]), group(G(5), [R(3)])],
      }),
    );
    const subs445 = plan.subscriptions.filter((sub) => sub.kind === 445);
    expect(subs445.map((sub) => sub.relay)).toStrictEqual([R(2), R(3), R(1)]);
  });

  test("one group cursor applies the group overlap per route+relay key", () => {
    const cursors = new Map([[groupCursorKey(G(1), R(1)), 10_000]]);
    const plan = planSubscriptions(
      input({ groups: [group(G(1), [R(1)])], cursor: (key) => cursors.get(key) }),
    );
    expect(plan.subscriptions[0]?.filters[0]?.since).toBe(10_000 - GROUP_OVERLAP_SECS);
  });

  test("routes pack FILTERS_PER_SUBSCRIPTION per 445 subscription, sorted", () => {
    const count = FILTERS_PER_SUBSCRIPTION + 3;
    const groups = Array.from({ length: count }, (_, i) => group(G(i + 1), [R(1)]));
    const plan = planSubscriptions(input({ groups }));
    const subs445 = plan.subscriptions.filter((sub) => sub.kind === 445);
    expect(subs445).toHaveLength(2);
    expect(subs445[0]?.filters).toHaveLength(FILTERS_PER_SUBSCRIPTION);
    expect(subs445[1]?.filters).toHaveLength(3);
    const routes = subs445.flatMap((sub) => sub.routes);
    expect([...routes].toSorted()).toStrictEqual(routes);
  });
});

describe("planSubscriptions — caps", () => {
  test("inbox relays beyond MAX_RELAYS are dropped", () => {
    const relays = Array.from({ length: MAX_RELAYS + 4 }, (_, i) => R(i + 1));
    const plan = planSubscriptions(input({ inboxRelays: relays }));
    expect(plan.subscriptions.filter((sub) => sub.kind === 1059)).toHaveLength(MAX_RELAYS);
  });

  test("group relays never push past MAX_RELAYS sockets", () => {
    const relays = Array.from({ length: MAX_RELAYS }, (_, i) => R(i + 1));
    const groups = relays.map((relay, i) => group(G(i + 1), [relay]));
    // Every group needs its own distinct relay — all 16 fit exactly.
    const plan = planSubscriptions(input({ groups }));
    expect(new Set(plan.subscriptions.map((sub) => sub.relay)).size).toBeLessThanOrEqual(
      MAX_RELAYS,
    );
    expect(plan.uncovered).toStrictEqual([]);
    // A 17th group on a fresh relay cannot be covered.
    const overflow = planSubscriptions(input({ groups: [...groups, group(G(99), [R(99)])] }));
    expect(overflow.uncovered).toStrictEqual([G(99)]);
  });

  test("subscriptions never exceed MAX_SUBSCRIPTIONS", () => {
    const groups = Array.from(
      { length: MAX_SUBSCRIPTIONS * FILTERS_PER_SUBSCRIPTION + 1 },
      (_, i) => group(G(i + 1), [R(1)]),
    );
    const plan = planSubscriptions(input({ groups }));
    expect(plan.subscriptions.length).toBeLessThanOrEqual(MAX_SUBSCRIPTIONS);
    // One relay → FILTERS_PER_SUBSCRIPTION routes per sub; the rest are uncovered.
    const covered = plan.subscriptions.flatMap((sub) => sub.routes).length;
    expect(covered).toBe(MAX_SUBSCRIPTIONS * FILTERS_PER_SUBSCRIPTION);
    expect(plan.uncovered).toHaveLength(1);
  });

  test("a group with no relays lands in uncovered", () => {
    const plan = planSubscriptions(input({ groups: [group(G(1), [])] }));
    expect(plan.uncovered).toStrictEqual([G(1)]);
  });
});

describe("planSubscriptions — determinism", () => {
  test("the same input plans identically regardless of group order", () => {
    const groups = [group(G(3), [R(1), R(2)]), group(G(1), [R(4)]), group(G(2), [R(3)])];
    const a = planSubscriptions(input({ inboxRelays: [R(1)], groups }));
    const b = planSubscriptions(input({ inboxRelays: [R(1)], groups: groups.toReversed() }));
    expect(a).toStrictEqual(b);
  });

  test("subscription keys ignore since so cursor churn does not replan", () => {
    const first = planSubscriptions(input({ groups: [group(G(1), [R(1)])], cursor: () => 5000 }));
    const second = planSubscriptions(input({ groups: [group(G(1), [R(1)])], cursor: () => 9000 }));
    expect(first.subscriptions.map((sub) => sub.key)).toStrictEqual(
      second.subscriptions.map((sub) => sub.key),
    );
  });
});

describe("planSubscriptions — randomized churn", () => {
  test("200 seeded-random join/leave plans stay inside every bound", () => {
    let seed = 11;
    const rnd = (): number => {
      seed = (seed * 1_664_525 + 1_013_904_223) % 4_294_967_296;
      return seed / 4_294_967_296;
    };
    // Eight relays and sixteen routes keep the input inside every cap, so
    // coverage must always be total.
    const pool = Array.from({ length: 16 }, (_, i) =>
      group(G(i + 1), [R((i % 8) + 1), R(((i + 3) % 8) + 1), R(((i + 5) % 8) + 1)]),
    );
    const live = new Set<number>([0]);
    for (let i = 0; i < 200; i += 1) {
      const index = Math.floor(rnd() * pool.length);
      // oxlint-disable-next-line vitest/no-conditional-in-test -- a random churn is a random branch
      if (live.has(index)) {
        live.delete(index);
      } else {
        live.add(index);
      }
      const groups = pool.filter((_, j) => live.has(j));
      const relaysOfRoute = new Map(groups.map((g) => [g.route, new Set(g.relays)]));
      const plan = planSubscriptions(input({ inboxRelays: [R(9)], groups }));
      expect(plan.subscriptions.length).toBeLessThanOrEqual(MAX_SUBSCRIPTIONS);
      const relaysUsed = new Set(plan.subscriptions.map((sub) => sub.relay));
      expect(relaysUsed.size).toBeLessThanOrEqual(MAX_RELAYS);
      // No planned route falls outside the input, and every route rides one of
      // the relays its group offered.
      for (const sub of plan.subscriptions) {
        for (const route of sub.routes) {
          expect(relaysOfRoute.get(route)?.has(sub.relay)).toBe(true);
        }
      }
      // The input always fits: nothing goes uncovered.
      expect(plan.uncovered).toStrictEqual([]);
    }
  });
});
