/**
 * `MarmotError` — the client's typed error surface. Messages carry fixed labels only (`marmot:
 * <kind>` / `marmot: engine: <code>`): no ids, URLs, or payloads. `toMarmotError` maps engine,
 * signer, storage and upload failures onto the kind vocabulary; `cause` keeps the original.
 */

import { SignerRefusedError } from "../backend/engine.ts";
import { BridgeError, DecodeError } from "../backend/errors.ts";
import type { WireError } from "../backend/protocol.ts";
import { UploadFailedError } from "../groups/index.ts";
import { StorageError } from "../storage/driver.ts";

export type MarmotErrorKind =
  | "closed"
  | "signer_refused"
  | "signer_failed"
  | "unsafe_relay"
  | "no_relay_list"
  | "no_inbox_relays"
  | "no_key_package"
  | "group_busy"
  | "unknown_group"
  | "not_a_member"
  | "not_group_admin"
  | "disbanding_not_enabled"
  | "group_disbanding"
  | "invalid_input"
  | "media_epoch_stale"
  | "upload_failed"
  | "storage"
  | "handler_failed"
  | "subscription_limit"
  | "commit_too_large"
  | "commit_refused"
  | "send_rate_exceeded"
  | "engine";

export class MarmotError extends Error {
  readonly kind: MarmotErrorKind;
  /** The engine wire-error kind when `kind` is `engine`; otherwise undefined. */
  readonly code: string | undefined;

  constructor(kind: MarmotErrorKind, code?: string, cause?: unknown) {
    super(code === undefined ? `marmot: ${kind}` : `marmot: ${kind}: ${code}`);
    this.name = "MarmotError";
    this.kind = kind;
    this.code = code;
    if (cause !== undefined) {
      this.cause = cause;
    }
  }
}

/** Bridge error kinds that map one-to-one onto `MarmotErrorKind`s. */
const BRIDGE_KIND_MAP: Partial<Record<WireError["kind"], MarmotErrorKind>> = {
  group_busy: "group_busy",
  unknown_group: "unknown_group",
  not_a_member: "not_a_member",
  not_group_admin: "not_group_admin",
  disbanding_not_enabled: "disbanding_not_enabled",
  group_disbanding: "group_disbanding",
  invalid_input: "invalid_input",
  media_epoch_stale: "media_epoch_stale",
  closed: "closed",
  host_failed: "storage",
  storage: "storage",
  send_rate_exceeded: "send_rate_exceeded",
};

/**
 * Map any thrown value to a `MarmotError`. `signerVerdict` is the `observeSigner` verdict for the
 * failing call: an otherwise-unrecognizable error raised while the signer refused or failed maps to
 * `signer_refused` / `signer_failed` (Engine.open calls the signer directly).
 */
export function toMarmotError(error: unknown, signerVerdict?: "refused" | "failed"): MarmotError {
  if (error instanceof MarmotError) {
    return error;
  }
  if (error instanceof SignerRefusedError) {
    return new MarmotError("signer_refused", undefined, error);
  }
  if (error instanceof UploadFailedError) {
    return new MarmotError("upload_failed", undefined, error);
  }
  if (error instanceof StorageError) {
    return new MarmotError("storage", undefined, error);
  }
  if (error instanceof DecodeError) {
    return new MarmotError("engine", "decode", error);
  }
  if (error instanceof BridgeError) {
    const { wire } = error;
    if (wire.kind === "signer") {
      return new MarmotError(
        wire.reason === "refused" ? "signer_refused" : "signer_failed",
        undefined,
        error,
      );
    }
    const kind = BRIDGE_KIND_MAP[wire.kind];
    if (kind !== undefined) {
      return new MarmotError(kind, undefined, error);
    }
    return new MarmotError("engine", wire.kind, error);
  }
  if (signerVerdict !== undefined) {
    return new MarmotError(
      signerVerdict === "refused" ? "signer_refused" : "signer_failed",
      undefined,
      error,
    );
  }
  return new MarmotError("engine", "unknown", error);
}
