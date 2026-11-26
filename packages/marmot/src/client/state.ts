/**
 * The client's persisted sync state: one versioned JSON blob
 * (`{"v":4,"inbox":"client"|"host","cursors":{...},"floors":{...},"inboxRelays":[...],"nip44Extended":bool}`)
 * behind the `CursorStore`. Decoding is total — anything missing, malformed, wrong version (v3
 * blobs included) or carrying an unknown `inbox` mode yields `undefined`, never a throw: the open
 * adopts the option's mode on an empty state (a full refetch and, while `nip44Extended` is unset,
 * one signer probe at open).
 */

import { utf8Decode, utf8Encode } from "../backend/utf8.ts";

/** Who owns the account's NIP-17 gift-wrap inbox: this client's subscriptions, or the host's. */
export type InboxOwner = "client" | "host";

export type ClientState = {
  /** Cursor key → the newest synchronized `createdAt` (unix seconds). */
  cursors: Record<string, number>;
  /**
   * Inbox cursor key → the earliest `createdAt` (unix seconds) of a kind-1059 wrap whose ingest
   * ended signer-unavailable (`signer_refused`/`signer_failed`) and is not resolved since. The
   * key's cursor never passes `floor − 1` while it stands; group keys never carry one.
   */
  floors: Record<string, number>;
  /** The inbox owner this state was opened under; a reopen in the other mode is rejected. */
  inbox: InboxOwner;
  /** The account's persisted inbox relays (client-owned inbox only). */
  inboxRelays: string[];
  /**
   * The signer passed the open-time NIP-44 extended-length self round trip (spec
   * `transports/nostr.md`).
   */
  nip44Extended: boolean;
};

const VERSION = 4;

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

/** The empty state for `inbox`: no cursors, no floors, no persisted inbox relays. */
export function emptyClientState(inbox: InboxOwner): ClientState {
  return { cursors: {}, floors: {}, inbox, inboxRelays: [], nip44Extended: false };
}

/** `key → non-negative integer seconds`, or `undefined` when any value is not one. */
function seconds(record: Record<string, unknown>): Record<string, number> | undefined {
  const out: Record<string, number> = {};
  for (const [key, value] of Object.entries(record)) {
    if (typeof value !== "number" || !Number.isInteger(value) || value < 0) {
      return undefined;
    }
    out[key] = value;
  }
  return out;
}

/** Decode the stored blob; every malformed input yields `undefined`. */
export function decodeClientState(bytes: Uint8Array | undefined): ClientState | undefined {
  if (bytes === undefined) {
    return undefined;
  }
  let parsed: unknown;
  try {
    parsed = JSON.parse(utf8Decode(bytes));
  } catch {
    return undefined;
  }
  if (
    !isRecord(parsed) ||
    parsed["v"] !== VERSION ||
    (parsed["inbox"] !== "client" && parsed["inbox"] !== "host") ||
    !isRecord(parsed["cursors"]) ||
    !isRecord(parsed["floors"])
  ) {
    return undefined;
  }
  if (!Array.isArray(parsed["inboxRelays"]) || typeof parsed["nip44Extended"] !== "boolean") {
    return undefined;
  }
  const cursors = seconds(parsed["cursors"]);
  const floors = seconds(parsed["floors"]);
  if (cursors === undefined || floors === undefined) {
    return undefined;
  }
  const inboxRelays: string[] = [];
  for (const relay of parsed["inboxRelays"]) {
    if (typeof relay !== "string") {
      return undefined;
    }
    inboxRelays.push(relay);
  }
  return {
    cursors,
    floors,
    inbox: parsed["inbox"],
    inboxRelays,
    nip44Extended: parsed["nip44Extended"],
  };
}

/** Encode `state` as the versioned blob for `CursorStore.save`. */
export function encodeClientState(state: ClientState): Uint8Array {
  return utf8Encode(
    JSON.stringify({
      v: VERSION,
      inbox: state.inbox,
      cursors: state.cursors,
      floors: state.floors,
      inboxRelays: state.inboxRelays,
      nip44Extended: state.nip44Extended,
    }),
  );
}
