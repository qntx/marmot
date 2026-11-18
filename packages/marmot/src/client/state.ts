/**
 * The client's persisted sync state: one versioned JSON blob (`{"v":1,"cursors":{...},"inbox":[]}`)
 * behind the `CursorStore`. Decoding is total — anything missing, malformed, wrong version or wrong
 * type yields the empty state (a full refetch), never a throw.
 */

import { utf8Decode, utf8Encode } from "../backend/utf8.ts";

export type ClientState = {
  /** Cursor key → the newest synchronized `createdAt` (unix seconds). */
  cursors: Record<string, number>;
  /** The account's persisted inbox relays. */
  inbox: string[];
};

const VERSION = 1;

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function empty(): ClientState {
  return { cursors: {}, inbox: [] };
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
  if (!isRecord(parsed) || parsed["v"] !== VERSION || !isRecord(parsed["cursors"])) {
    return empty();
  }
  if (!Array.isArray(parsed["inbox"])) {
    return empty();
  }
  const cursors: Record<string, number> = {};
  for (const [key, value] of Object.entries(parsed["cursors"])) {
    if (typeof value !== "number" || !Number.isInteger(value) || value < 0) {
      return empty();
    }
    cursors[key] = value;
  }
  const inbox: string[] = [];
  for (const relay of parsed["inbox"]) {
    if (typeof relay !== "string") {
      return empty();
    }
    inbox.push(relay);
  }
  return { cursors, inbox };
}

/** Encode `state` as the versioned blob for `CursorStore.save`. */
export function encodeClientState(state: ClientState): Uint8Array {
  return utf8Encode(JSON.stringify({ v: VERSION, cursors: state.cursors, inbox: state.inbox }));
}
