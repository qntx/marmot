/**
 * The client's persisted sync state: one versioned JSON blob
 * (`{"v":3,"cursors":{...},"floors":{...},"inbox":[...],"nip44Extended":bool}`) behind the
 * `CursorStore`. Decoding is total — anything missing, malformed, wrong version (a v2 blob
 * included) or wrong type yields the empty state (a full refetch and, while `nip44Extended` is
 * unset, one signer probe at open), never a throw.
 */

import { utf8Decode, utf8Encode } from "../backend/utf8.ts";

export type ClientState = {
  /** Cursor key → the newest synchronized `createdAt` (unix seconds). */
  cursors: Record<string, number>;
  /**
   * Inbox cursor key → the earliest `createdAt` (unix seconds) of a kind-1059 wrap whose ingest
   * ended signer-unavailable (`signer_refused`/`signer_failed`) and is not resolved since. The
   * key's cursor never passes `floor − 1` while it stands; group keys never carry one.
   */
  floors: Record<string, number>;
  /** The account's persisted inbox relays. */
  inbox: string[];
  /**
   * The signer passed the open-time NIP-44 extended-length self round trip (spec
   * `transports/nostr.md`).
   */
  nip44Extended: boolean;
};

const VERSION = 3;

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function empty(): ClientState {
  return { cursors: {}, floors: {}, inbox: [], nip44Extended: false };
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

/** Decode the stored blob; every malformed input yields the empty state. */
export function decodeClientState(bytes: Uint8Array | undefined): ClientState {
  if (bytes === undefined) {
    return empty();
  }
  let parsed: unknown;
  try {
    parsed = JSON.parse(utf8Decode(bytes));
  } catch {
    return empty();
  }
  if (
    !isRecord(parsed) ||
    parsed["v"] !== VERSION ||
    !isRecord(parsed["cursors"]) ||
    !isRecord(parsed["floors"])
  ) {
    return empty();
  }
  if (!Array.isArray(parsed["inbox"]) || typeof parsed["nip44Extended"] !== "boolean") {
    return empty();
  }
  const cursors = seconds(parsed["cursors"]);
  const floors = seconds(parsed["floors"]);
  if (cursors === undefined || floors === undefined) {
    return empty();
  }
  const inbox: string[] = [];
  for (const relay of parsed["inbox"]) {
    if (typeof relay !== "string") {
      return empty();
    }
    inbox.push(relay);
  }
  return { cursors, floors, inbox, nip44Extended: parsed["nip44Extended"] };
}

/** Encode `state` as the versioned blob for `CursorStore.save`. */
export function encodeClientState(state: ClientState): Uint8Array {
  return utf8Encode(
    JSON.stringify({
      v: VERSION,
      cursors: state.cursors,
      floors: state.floors,
      inbox: state.inbox,
      nip44Extended: state.nip44Extended,
    }),
  );
}
