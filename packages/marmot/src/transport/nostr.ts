/**
 * `nostrRelayPort` — the `RelayPort` adapter over a `@qntx/nostr` `Pool`. The pool is injected
 * fully configured by the app: websocket implementation, event verifier, timeouts, reconnect and
 * insecure-origin policy live there, not here.
 */

import type { Event, Filter, Pool } from "@qntx/nostr";

import type { NostrEvent } from "../backend/protocol.ts";
import type { PublishStatus, RelayFilter, RelayPort, SubscriptionHandlers } from "./port.ts";

function toWire(event: NostrEvent): Event {
  return {
    id: event.id,
    pubkey: event.pubkey,
    created_at: Number(event.createdAt),
    kind: event.kind,
    tags: event.tags,
    content: event.content,
    sig: event.sig,
  };
}

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

/** Adapt `pool` to the `RelayPort` contract: publish never rejects, `closed` is relay-initiated. */
export function nostrRelayPort(pool: Pool): RelayPort {
  return {
    publish: async (relay, event): Promise<PublishStatus> => {
      try {
        const [result] = await pool.publish([relay], toWire(event));
        if (result?.status === "ok") {
          return "accepted";
        }
        if (result?.status === "rejected") {
          return "rejected";
        }
        return "unavailable";
      } catch {
        return "unavailable";
      }
    },
    subscribe: (relay, filters, handlers: SubscriptionHandlers) => {
      let closedByUs = false;
      const closer = pool.subscribe([relay], filters.map(toFilter), {
        onEvent: (event) => {
          handlers.event(fromWire(event));
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
