/**
 * `nostrRelayPort` — the `RelayPort` adapter over a `@qntx/nostr` `Pool`. The pool is injected
 * fully configured by the app: websocket implementation, event verifier, timeouts, reconnect and
 * insecure-origin policy live there, not here.
 */

import { verifyEvent } from "@qntx/nostr";
import type { Event, Filter, Pool } from "@qntx/nostr";

import type { NostrEvent } from "../backend/protocol.ts";
import { utf8Encode } from "../backend/utf8.ts";
import type { PublishStatus, RelayFilter, RelayPort, SubscriptionHandlers } from "./port.ts";
import { markVerified, toWire } from "./verify.ts";

function fromWire(event: Event): NostrEvent {
  return {
    id: event.id,
    pubkey: event.pubkey,
    createdAt: BigInt(event.created_at),
    kind: event.kind,
    tags: event.tags.map((tag) => [...tag]),
    content: event.content,
    sig: event.sig,
  };
}

function toFilter(filter: RelayFilter): Filter {
  return {
    ...(filter.kinds === undefined ? {} : { kinds: filter.kinds }),
    ...(filter.authors === undefined ? {} : { authors: filter.authors }),
    ...(filter["#p"] === undefined ? {} : { "#p": filter["#p"] }),
    ...(filter["#h"] === undefined ? {} : { "#h": filter["#h"] }),
    ...(filter.since === undefined ? {} : { since: filter.since }),
    ...(filter.limit === undefined ? {} : { limit: filter.limit }),
  };
}

/** The event's NIP-01 wire-JSON UTF-8 size — the measure relay size limits are classed by. */
export function wireEventBytes(event: NostrEvent): number {
  return utf8Encode(JSON.stringify(toWire(event))).length;
}

/** Adapt `pool` to the `RelayPort` contract: publish never rejects, `closed` is relay-initiated. */
export function nostrRelayPort(pool: Pool): RelayPort {
  return {
    publish: async (relay, event): Promise<PublishStatus> => {
      try {
        await pool.ensureRelay(relay);
      } catch {
        // The pool never established the connection, so nothing was transmitted.
        return "not_sent";
      }
      try {
        const [result] = await pool.publish([relay], toWire(event));
        if (result?.status === "ok") {
          return "accepted";
        }
        if (result?.status === "rejected") {
          return "rejected";
        }
        // `failed` and throws mean the frame may already be on the wire.
        return "unavailable";
      } catch {
        return "unavailable";
      }
    },
    connect: async (relay): Promise<boolean> => {
      try {
        await pool.ensureRelay(relay);
        return true;
      } catch {
        return false;
      }
    },
    subscribe: (relay, filters, handlers: SubscriptionHandlers) => {
      let closedByUs = false;
      const closer = pool.subscribe([relay], filters.map(toFilter), {
        onEvent: (event) => {
          // The pool's verifier is host-configured and may be absent: verify the wire object
          // here (a WeakSet hit when the pool already did) so the pump can trust the mark.
          if (!verifyEvent(event)) {
            return;
          }
          const converted = fromWire(event);
          markVerified(converted);
          handlers.event(converted);
        },
        onEose: () => {
          handlers.eose();
        },
        onClose: (reason) => {
          if (!closedByUs) {
            handlers.closed(reason);
          }
        },
      });
      return () => {
        closedByUs = true;
        closer.close();
      };
    },
  };
}
