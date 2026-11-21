/**
 * The transport port: the minimum relay-pool surface the client needs, plus the injected timer. All
 * relay I/O goes through `RelayPort`; the pool implementation (`nostrRelayPort`, a fake, or an
 * app-provided adapter) is injected.
 */

import type { NostrEvent } from "../backend/protocol.ts";

/**
 * Per-relay publish verdict, mapped onto the engine's `RelayStatus` vocabulary. `not_sent`: the
 * event was not transmitted to this relay — a pre-send failure such as a refused dial, which
 * records no possible exposure.
 */
export type PublishStatus = "accepted" | "rejected" | "unavailable" | "not_sent";

/** A NIP-01 filter as the client composes it. */
export type RelayFilter = {
  kinds?: number[];
  authors?: string[];
  "#p"?: string[];
  "#h"?: string[];
  "#k"?: string[];
  since?: number;
  limit?: number;
};

export type SubscriptionHandlers = {
  /**
   * One inbound event. Events need not be verified by the port: the pump verifies id + signature
   * before dedupe.
   */
  event: (event: NostrEvent) => void;
  /** The relay signalled end of stored events. */
  eose: () => void;
  /** The relay ended the subscription; never fires for our own close. */
  closed: (reason: string) => void;
};

/** The minimum the client needs from a relay pool. `publish` never rejects. */
export type RelayPort = {
  /** Publish `event` to one relay; resolves with that relay's verdict. */
  publish: (relay: string, event: NostrEvent) => Promise<PublishStatus>;
  /**
   * Establish a connection to `relay` when none is open; resolves `true` once a connection is
   * established, `false` when the dial fails. Never rejects.
   */
  connect: (relay: string) => Promise<boolean>;
  /**
   * Open one subscription on `relay`; returns the close function. `closed` fires only for
   * relay-initiated ends — closing via the returned function must not produce a `closed` call.
   */
  subscribe: (relay: string, filters: RelayFilter[], handlers: SubscriptionHandlers) => () => void;
};

/** Injected timer: schedule `run` after `ms`; the returned function cancels it. */
export type Timer = { set: (ms: number, run: () => void) => () => void };
