/**
 * `fakeRelay` — an in-memory relay network for tests. Every client sees the same stored events;
 * publish verdicts and deliveries follow a per-relay `mode`. Delivery is asynchronous (microtask):
 * replay of stored events matching the subscription's filters, then `eose`, then live events as
 * they are published.
 */

import type { NostrEvent } from "../backend/protocol.ts";
import type {
  PublishStatus,
  RelayFilter,
  RelayPort,
  SubscriptionHandlers,
} from "../transport/port.ts";

export type FakeRelayMode = "normal" | "drop" | "reject" | "down" | "refuse" | "shuffle";

/** An in-memory relay network with per-relay verdict switches and per-client counters. */
export type FakeRelayNetwork = {
  /** The `RelayPort` one client drives; deliveries and subscriptions are counted per client. */
  port: (client: string) => RelayPort;
  /**
   * Set a relay's verdict mode. `drop`: publish answers accepted but stores/delivers nothing.
   * `reject`: answers rejected, stores nothing. `down`: answers unavailable; open subscriptions get
   * `closed("down")` and new ones close immediately. `refuse`: the dial is refused — `connect`
   * answers false and publish answers `not_sent`; open subscriptions get `closed("refuse")` and new
   * ones close immediately. `shuffle`: stored replay is served in seeded-random order with every
   * event delivered twice; live events are delivered twice too.
   */
  mode: (relay: string, mode: FakeRelayMode) => void;
  /** Hold publish answers (pending promises) until `release`. */
  hold: () => void;
  /** How many publishes are parked on `hold` right now. */
  heldPublishes: () => number;
  /** Release held publish answers. */
  release: () => void;
  /**
   * Process death for `client`: its subscriptions vanish without a `closed` call, its held or
   * future publishes settle `"unavailable"` without storing or delivering anything, and future
   * `subscribe` calls on its port are no-ops.
   */
  kill: (client: string) => void;
  /** The events stored on `relay`, in publish order. */
  events: (relay: string) => NostrEvent[];
  /** Every publish attempt, in order, attributed to the publishing client. */
  published: () => Array<{ client: string; relay: string; event: NostrEvent }>;
  /** EVENT deliveries to `client` (replay + live). */
  served: (client: string) => number;
  /** Currently open subscriptions (all clients, or one). */
  openSubscriptions: (client?: string) => number;
  /** Distinct relays with an open subscription (all clients, or one). */
  openRelays: (client?: string) => number;
  /** Most subscriptions ever open at once (all clients, or one). */
  peakSubscriptions: (client?: string) => number;
  /** Most relays ever holding an open subscription at once (all clients, or one). */
  peakRelays: (client?: string) => number;
  /**
   * Kind-1059 gift wraps addressed (`p` tag) to `accountHex`, across every relay's store — only the
   * ones this account has not been handed before (per-account delivery cursor).
   */
  welcomesFor: (accountHex: string) => NostrEvent[];
  /**
   * Kind-445 group events routed (`h` tag) to `routeHex` across every relay's store — only the ones
   * `subscriber` has not been handed before (per-(subscriber, route) delivery cursor).
   */
  groupEvents: (routeHex: string, subscriber: string) => NostrEvent[];
  /** The newest kind-30443 KeyPackage event authored by `accountHex`; throws when absent. */
  keyPackageOf: (accountHex: string) => NostrEvent;
};

type StoredSub = {
  client: string;
  relay: string;
  filters: RelayFilter[];
  handlers: SubscriptionHandlers;
  open: boolean;
};

function tag(event: NostrEvent, name: string): string | undefined {
  return event.tags.find((entry) => entry[0] === name)?.[1];
}

function matchFilter(filter: RelayFilter, event: NostrEvent): boolean {
  if (filter.kinds !== undefined && !filter.kinds.includes(event.kind)) {
    return false;
  }
  if (filter.authors !== undefined && !filter.authors.includes(event.pubkey)) {
    return false;
  }
  for (const name of ["#p", "#h"] as const) {
    const wanted = filter[name];
    if (wanted !== undefined) {
      const values = new Set(event.tags.filter((t) => t[0] === name.slice(1)).map((t) => t[1]));
      if (!wanted.some((v) => values.has(v))) {
        return false;
      }
    }
  }
  return !(filter.since !== undefined && Number(event.createdAt) < filter.since);
}

/** Events matching any filter, `limit` applied per filter, deduplicated by id, store order. */
function replayEvents(store: NostrEvent[], filters: RelayFilter[]): NostrEvent[] {
  const picked = new Map<string, NostrEvent>();
  for (const filter of filters) {
    let count = 0;
    for (const event of store) {
      if (filter.limit !== undefined && count >= filter.limit) {
        break;
      }
      if (matchFilter(filter, event)) {
        count += 1;
        if (!picked.has(event.id)) {
          picked.set(event.id, event);
        }
      }
    }
  }
  return [...picked.values()];
}

/** Small seeded LCG for deterministic `shuffle` ordering (no bitwise ops — the lint gate bans them). */
function mulberry32(seed: number): () => number {
  let state = ((seed % 4294967296) + 4294967296) % 4294967296;
  return () => {
    state = (state * 1664525 + 1013904223) % 4294967296;
    return state / 4294967296;
  };
}

/** Build a `FakeRelayNetwork`; `seed` drives the `shuffle` mode's replay order. */
export function fakeRelay(seed = 1): FakeRelayNetwork {
  const modes = new Map<string, FakeRelayMode>();
  const stores = new Map<string, NostrEvent[]>();
  const all: Array<{ client: string; relay: string; event: NostrEvent }> = [];
  const subs = new Set<StoredSub>();
  const servedCount = new Map<string, number>();
  const cursors = new Map<string, number>();
  const peaksSubs = new Map<string, number>();
  const peaksRelays = new Map<string, number>();
  const dead = new Set<string>();
  let held = false;
  const waiters: Array<{ client: string; resolve: () => void }> = [];
  const rand = mulberry32(seed);

  const modeOf = (relay: string): FakeRelayMode => modes.get(relay) ?? "normal";

  const storeOf = (relay: string): NostrEvent[] => {
    let store = stores.get(relay);
    if (store === undefined) {
      store = [];
      stores.set(relay, store);
    }
    return store;
  };

  const bumpPeak = (client: string): void => {
    let open = 0;
    const relays = new Set<string>();
    for (const sub of subs) {
      if (sub.client === client && sub.open) {
        open += 1;
        relays.add(sub.relay);
      }
    }
    peaksSubs.set(client, Math.max(peaksSubs.get(client) ?? 0, open));
    peaksRelays.set(client, Math.max(peaksRelays.get(client) ?? 0, relays.size));
  };

  const deliver = (sub: StoredSub, event: NostrEvent): void => {
    queueMicrotask(() => {
      if (!sub.open) {
        return;
      }
      servedCount.set(sub.client, (servedCount.get(sub.client) ?? 0) + 1);
      sub.handlers.event(event);
    });
  };

  const matching = (sub: StoredSub, event: NostrEvent): boolean =>
    sub.filters.some((filter) => matchFilter(filter, event));

  const publish = async (
    client: string,
    relay: string,
    event: NostrEvent,
  ): Promise<PublishStatus> => {
    if (dead.has(client)) {
      return "unavailable";
    }
    if (held) {
      await new Promise<void>((resolve) => {
        waiters.push({ client, resolve });
      });
    }
    if (dead.has(client)) {
      return "unavailable";
    }
    all.push({ client, relay, event });
    const mode = modeOf(relay);
    if (mode === "refuse") {
      return "not_sent";
    }
    if (mode === "down") {
      return "unavailable";
    }
    if (mode === "reject") {
      return "rejected";
    }
    if (mode === "drop") {
      return "accepted";
    }
    storeOf(relay).push(event);
    for (const sub of subs) {
      if (sub.relay === relay && sub.open && matching(sub, event)) {
        deliver(sub, event);
        if (mode === "shuffle") {
          deliver(sub, event);
        }
      }
    }
    return "accepted";
  };

  const subscribe = (
    client: string,
    relay: string,
    filters: RelayFilter[],
    handlers: SubscriptionHandlers,
  ): (() => void) => {
    if (dead.has(client)) {
      return () => undefined;
    }
    const mode = modeOf(relay);
    if (mode === "down" || mode === "refuse") {
      queueMicrotask(() => {
        handlers.closed(mode);
      });
      return () => undefined;
    }
    const sub: StoredSub = { client, relay, filters, handlers, open: true };
    subs.add(sub);
    bumpPeak(client);
    queueMicrotask(() => {
      if (!sub.open) {
        return;
      }
      let replay = replayEvents(storeOf(relay), filters);
      if (modeOf(relay) === "shuffle") {
        replay = [...replay].sort(() => rand() - 0.5);
        for (const event of replay) {
          deliver(sub, event);
          deliver(sub, event);
        }
      } else {
        for (const event of replay) {
          deliver(sub, event);
        }
      }
      queueMicrotask(() => {
        if (sub.open) {
          sub.handlers.eose();
        }
      });
    });
    return () => {
      if (sub.open) {
        sub.open = false;
        subs.delete(sub);
      }
    };
  };

  const allEvents = (): NostrEvent[] => {
    const seen = new Set<string>();
    const out: NostrEvent[] = [];
    for (const store of stores.values()) {
      for (const event of store) {
        if (!seen.has(event.id)) {
          seen.add(event.id);
          out.push(event);
        }
      }
    }
    return out;
  };

  const take = (key: string, events: NostrEvent[]): NostrEvent[] => {
    const cursor = cursors.get(key) ?? 0;
    cursors.set(key, events.length);
    return events.slice(cursor);
  };

  const network: FakeRelayNetwork = {
    port(client) {
      return {
        publish: async (relay, event) => publish(client, relay, event),
        connect: async (relay) => {
          await Promise.resolve();
          return !dead.has(client) && modeOf(relay) !== "refuse";
        },
        subscribe: (relay, filters, handlers) => subscribe(client, relay, filters, handlers),
      };
    },

    mode(relay, mode) {
      modes.set(relay, mode);
      if (mode === "down" || mode === "refuse") {
        for (const sub of subs) {
          if (sub.relay === relay && sub.open) {
            sub.open = false;
            subs.delete(sub);
            queueMicrotask(() => {
              sub.handlers.closed(mode);
            });
          }
        }
      }
    },

    hold() {
      held = true;
    },

    heldPublishes() {
      return waiters.length;
    },

    release() {
      held = false;
      const pending = waiters.splice(0);
      for (const waiter of pending) {
        waiter.resolve();
      }
    },

    kill(client) {
      dead.add(client);
      for (const sub of subs) {
        if (sub.client === client && sub.open) {
          sub.open = false;
          subs.delete(sub);
        }
      }
      for (let i = waiters.length - 1; i >= 0; i -= 1) {
        const waiter = waiters[i];
        if (waiter !== undefined && waiter.client === client) {
          waiters.splice(i, 1);
          waiter.resolve();
        }
      }
    },

    events(relay) {
      return [...storeOf(relay)];
    },

    published() {
      return [...all];
    },

    served(client) {
      return servedCount.get(client) ?? 0;
    },

    openSubscriptions(client) {
      let count = 0;
      for (const sub of subs) {
        if (sub.open && (client === undefined || sub.client === client)) {
          count += 1;
        }
      }
      return count;
    },

    openRelays(client) {
      const relays = new Set<string>();
      for (const sub of subs) {
        if (sub.open && (client === undefined || sub.client === client)) {
          relays.add(sub.relay);
        }
      }
      return relays.size;
    },

    peakSubscriptions(client) {
      if (client !== undefined) {
        return peaksSubs.get(client) ?? 0;
      }
      let max = 0;
      for (const value of peaksSubs.values()) {
        max = Math.max(max, value);
      }
      return max;
    },

    peakRelays(client) {
      if (client !== undefined) {
        return peaksRelays.get(client) ?? 0;
      }
      let max = 0;
      for (const value of peaksRelays.values()) {
        max = Math.max(max, value);
      }
      return max;
    },

    welcomesFor(accountHex) {
      return take(
        `w:${accountHex}`,
        allEvents().filter((event) => event.kind === 1059 && tag(event, "p") === accountHex),
      );
    },

    groupEvents(routeHex, subscriber) {
      return take(
        `g:${subscriber}:${routeHex}`,
        allEvents().filter((event) => event.kind === 445 && tag(event, "h") === routeHex),
      );
    },

    keyPackageOf(accountHex) {
      const events = allEvents().filter(
        (event) => event.kind === 30_443 && event.pubkey === accountHex,
      );
      const newest = events.at(-1);
      if (newest === undefined) {
        throw new Error("account published no KeyPackage");
      }
      return newest;
    },
  };
  return network;
}
