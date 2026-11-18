/**
 * Error types for the backend layer. Messages carry only fixed labels — never input bytes, ids,
 * relay URLs, keys, or payloads.
 */

import type { WireError } from "./protocol.ts";

/** A wire payload failed a structural or value check. */
export class DecodeError extends Error {
  /** Dotted field path where the check failed (e.g. `request.invitees[0]`). */
  readonly path: string;
  /** The fixed refusal label (e.g. `hex`, `missing`, `unknown_field`). */
  readonly reason: string;

  constructor(path: string, reason: string) {
    super(`decode failed at ${path}: ${reason}`);
    this.name = "DecodeError";
    this.path = path;
    this.reason = reason;
  }
}

function describe(error: WireError): string {
  const parts: string[] = [error.kind];
  if ("reason" in error) {
    parts.push(error.reason);
  }
  if ("detail" in error) {
    parts.push(error.detail);
  }
  if ("site" in error) {
    parts.push(error.site);
  }
  if ("field" in error) {
    parts.push(error.field);
  }
  return `bridge error: ${parts.join(": ")}`;
}

/** A `call`/`open` answered with the wire `error` variant. */
export class BridgeError extends Error {
  /** The decoded wire error. */
  readonly wire: WireError;

  constructor(wire: WireError) {
    super(describe(wire));
    this.name = "BridgeError";
    this.wire = wire;
  }

  /** The wire error's `kind` discriminant. */
  get kind(): WireError["kind"] {
    return this.wire.kind;
  }
}
