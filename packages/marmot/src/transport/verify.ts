/**
 * `verifyInbound` — the pump-side id+signature gate for inbound events. A `RelayPort` is not
 * required to verify what it delivers (the `Pool`'s verifier is host-configured), so the pump
 * re-checks id hash + BIP-340 itself before an event may enter the dedupe buffer or credit a cursor
 * key. `markVerified` lets `nostrRelayPort` hand over objects the pool already verified through the
 * shared `@qntx/nostr` WeakSet caches, so a verified wire event costs one WeakSet hit here instead
 * of a full verify.
 *
 * Module-private: nothing in this file is re-exported through `transport/index.ts`.
 */

import { verifyEvent } from "@qntx/nostr";
import type { Event } from "@qntx/nostr";

import type { NostrEvent } from "../backend/protocol.ts";

const verified = new WeakSet<NostrEvent>();

/** The `NostrEvent`'s `@qntx/nostr` wire shape (shared by `nostrRelayPort`). */
export function toWire(event: NostrEvent): Event {
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

/**
 * Record `event` as verified — used ONLY by `nostrRelayPort` after `verifyEvent` accepted the
 * pool's wire object (a WeakSet-cache hit there when the pool ran its default verifier).
 */
export function markVerified(event: NostrEvent): void {
  verified.add(event);
}

/**
 * Whether `event`'s id and signature verify. Cache hit for objects `markVerified` marked; otherwise
 * a full `verifyEvent` (id hash + BIP-340), cached on success.
 */
export function verifyInbound(event: NostrEvent): boolean {
  if (verified.has(event)) {
    return true;
  }
  if (!verifyEvent(toWire(event))) {
    return false;
  }
  verified.add(event);
  return true;
}
