/**
 * `keys` — relay-list and KeyPackage lookups over the `RelayPort`: the account's NIP-65 write
 * relays, its NIP-17 inbox relays, and invitee resolution for `createGroup`/`invite`. Every relay
 * URL taken from a list — and every caller-provided lookup relay — passes `safeRelayUrl` first.
 */

import type { Event } from "@qntx/nostr";
import { parseDmRelayList } from "@qntx/nostr/nips/nip17";
import { parseRelayList, writeRelays } from "@qntx/nostr/nips/nip65";

import type { Invitee, NostrEvent } from "../backend/protocol.ts";
import { fetchEvents } from "../transport/fetch.ts";
import type { RelayPort, Timer } from "../transport/port.ts";
import { safeRelayUrl } from "../transport/url.ts";

const RELAY_LIST_KIND = 10002;
const DM_RELAY_LIST_KIND = 10050;
const KEY_PACKAGE_KIND = 30443;
const DELETION_KIND = 5;

/** Convert to the `@qntx/nostr` `Event` shape — only what its parsers consume. */
function toNostrEvent(event: NostrEvent): Event {
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

/** Newest-first: `createdAt` desc, lower id wins a tie. */
function newest(events: NostrEvent[]): NostrEvent | undefined {
  let best: NostrEvent | undefined;
  for (const event of events) {
    if (
      best === undefined ||
      event.createdAt > best.createdAt ||
      (event.createdAt === best.createdAt && event.id < best.id)
    ) {
      best = event;
    }
  }
  return best;
}

function safe(relays: string[], allowLoopback: boolean): string[] {
  return relays.filter((relay) => safeRelayUrl(relay, allowLoopback));
}

async function fetchAuthored(
  port: RelayPort,
  timer: Timer,
  pubkey: string,
  kind: number,
  lookupRelays: string[],
  allowLoopback: boolean,
): Promise<NostrEvent[]> {
  const relays = safe(lookupRelays, allowLoopback);
  const events = await fetchEvents(port, timer, relays, { kinds: [kind], authors: [pubkey] });
  return events.filter((event) => event.pubkey === pubkey && event.kind === kind);
}

/**
 * The account's NIP-65 write relays (marker `write` or omitted), from its newest kind-10002 on
 * `lookupRelays` — safe-filtered. `undefined` when no relay list was found.
 */
export async function writeRelaysOf(
  port: RelayPort,
  timer: Timer,
  pubkey: string,
  lookupRelays: string[],
  allowLoopback: boolean,
): Promise<string[] | undefined> {
  const events = await fetchAuthored(
    port,
    timer,
    pubkey,
    RELAY_LIST_KIND,
    lookupRelays,
    allowLoopback,
  );
  const list = newest(events);
  if (list === undefined) {
    return undefined;
  }
  return safe(writeRelays(parseRelayList(toNostrEvent(list))), allowLoopback);
}

/** The account's NIP-17 inbox relays from its newest kind-10050 — safe-filtered. */
export async function inboxRelaysOf(
  port: RelayPort,
  timer: Timer,
  pubkey: string,
  lookupRelays: string[],
  allowLoopback: boolean,
): Promise<string[] | undefined> {
  const events = await fetchAuthored(
    port,
    timer,
    pubkey,
    DM_RELAY_LIST_KIND,
    lookupRelays,
    allowLoopback,
  );
  const list = newest(events);
  if (list === undefined) {
    return undefined;
  }
  return safe(parseDmRelayList(toNostrEvent(list)), allowLoopback);
}

export type InviteeLookup =
  | { type: "found"; invitees: Invitee[] }
  | { type: "no_relay_list" }
  | { type: "no_inbox_relays" }
  | { type: "no_key_package" };

/**
 * Resolve `pubkey`'s published KeyPackages for an invite: its NIP-65 write relays → its kind-30443
 * publications and kind-5 deletion requests → `select` (the engine's D1 selection) → its NIP-17
 * inbox relays → one `Invitee` per selected publication. An empty selection answers
 * `no_key_package`.
 */
export async function lookupInvitees(
  port: RelayPort,
  timer: Timer,
  pubkey: string,
  lookupRelays: string[],
  allowLoopback: boolean,
  select: (events: NostrEvent[]) => Promise<NostrEvent[]>,
): Promise<InviteeLookup> {
  const write = await writeRelaysOf(port, timer, pubkey, lookupRelays, allowLoopback);
  if (write === undefined || write.length === 0) {
    return { type: "no_relay_list" };
  }
  const [keyPackages, deletions] = await Promise.all([
    fetchEvents(port, timer, write, { kinds: [KEY_PACKAGE_KIND], authors: [pubkey] }),
    fetchEvents(port, timer, write, {
      kinds: [DELETION_KIND],
      authors: [pubkey],
      "#k": [String(KEY_PACKAGE_KIND)],
    }),
  ]);
  const fetched = [...keyPackages, ...deletions].filter(
    (event) =>
      event.pubkey === pubkey && (event.kind === KEY_PACKAGE_KIND || event.kind === DELETION_KIND),
  );
  const selected = await select(fetched);
  if (selected.length === 0) {
    return { type: "no_key_package" };
  }
  const inboxRelays = await inboxRelaysOf(port, timer, pubkey, lookupRelays, allowLoopback);
  if (inboxRelays === undefined || inboxRelays.length === 0) {
    return { type: "no_inbox_relays" };
  }
  return {
    type: "found",
    invitees: selected.map((keyPackage) => ({ keyPackage, inboxRelays })),
  };
}
