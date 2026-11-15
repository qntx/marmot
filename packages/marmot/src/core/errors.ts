/**
 * Shared result and rejection vocabulary — mirrors the spec's `foundation/errors.md` at the
 * revision pinned in `upstream.json`. The spec names shared categories; it does not require local
 * APIs to use these exact enum names.
 */

/**
 * Why an input did not produce application content. `transport_deferred` and `resource_refused` are
 * pre-convergence availability categories (the inner protocol input is not yet recovered — such an
 * object MUST NOT be labelled `stale`, `invalid_encoding`, or a terminal `duplicate`);
 * `transport_rejected` is an outbound transport outcome, not an inbound rejection; the rest are
 * inbound rejection categories.
 */
export const INPUT_CATEGORIES = [
  "duplicate",
  "own_echo",
  "wrong_recipient",
  "unknown_group",
  "stale_epoch",
  "invalid_encoding",
  "invalid_signature",
  "unsupported_required_feature",
  "authorization_failed",
  "missing_history",
  "transport_deferred",
  "resource_refused",
  "transport_rejected",
] as const;

/** One of the spec's shared input categories. */
export type InputCategory = (typeof INPUT_CATEGORIES)[number];

/**
 * The four convergence dispositions assigned to an input that passes the branch-independent
 * admission gate. `stale` is reserved for input admitted to convergence that can no longer affect
 * the group; a disposition is a live classification that MAY change as missing state arrives, and
 * every input has exactly one current disposition. `delivered` (application output of an `accepted`
 * app message) and `dropped` are not dispositions.
 */
export const DISPOSITIONS = ["accepted", "deferred", "stale", "invalidated"] as const;

/** One of the four convergence dispositions. */
export type Disposition = (typeof DISPOSITIONS)[number];

/**
 * Named convergence outcomes from protocol-core documents. Each maps to exactly one disposition and
 * one category: `BeyondAnchor` is window exclusion by design (source epoch older than the retained
 * anchor — never processed); `MissingRetainedAnchor` is storage loss inside the rollback horizon —
 * the input stays deferred, and the group becomes `Unrecoverable` until a verified repair path
 * exists.
 */
export const NAMED_OUTCOMES = {
  BeyondAnchor: { disposition: "stale", category: "stale_epoch" },
  MissingRetainedAnchor: { disposition: "deferred", category: "missing_history" },
} as const;

/** A PascalCase convergence outcome name from a protocol-core document. */
export type NamedOutcome = keyof typeof NAMED_OUTCOMES;
