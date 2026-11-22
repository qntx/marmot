/**
 * `invites` — invite-facing helpers: the typed `InviteEvent`, `pendingInvites`, which surfaces
 * signer-refused/failed gift-wrap unwraps as retryable `pending` events, and `observeSigner`, which
 * records signer refusals/failures so client ops map them to typed errors. A refused or failed
 * unwrap is `unavailable` in the engine — rolled back and retryable — so the client emits `pending`
 * and offers `retryInvites`, which reopens the inbox subscriptions at the capped cursor and lets
 * the relays replay the wraps. A `SignerInvalidPayloadError` unwrap is a successful signer
 * invocation whose verdict is on the payload: the engine rejects the wrap permanently, so it is
 * neither a `pending` condition nor a recorded verdict.
 */

import { SignerInvalidPayloadError, SignerRefusedError } from "../backend/engine.ts";
import type { Signer } from "../backend/engine.ts";
import type { InputReport } from "../backend/protocol.ts";

export type InviteEvent =
  | { type: "joined"; group: string }
  | { type: "pending"; reason: "signer_refused" | "signer_failed" };

/**
 * Wrap `signer` so the first refused/failed call since the last `take()` is recorded.
 * `SignerRefusedError` records "refused", any other throw "failed" — except
 * `SignerInvalidPayloadError`, which records nothing (the signer worked; the wrap is rejected
 * permanently). The error is always rethrown unchanged.
 */
export function observeSigner(signer: Signer): {
  signer: Signer;
  take: () => "refused" | "failed" | undefined;
} {
  let verdict: "refused" | "failed" | undefined;
  const guard = async <T>(call: () => Promise<T>): Promise<T> => {
    try {
      return await call();
    } catch (error) {
      if (!(error instanceof SignerInvalidPayloadError)) {
        verdict ??= error instanceof SignerRefusedError ? "refused" : "failed";
      }
      throw error;
    }
  };
  return {
    signer: {
      pubkey: signer.pubkey,
      signEvent: async (unsigned) => guard(async () => signer.signEvent(unsigned)),
      nip44Encrypt: async (peer, plaintext) =>
        guard(async () => signer.nip44Encrypt(peer, plaintext)),
      nip44Decrypt: async (peer, ciphertext) =>
        guard(async () => signer.nip44Decrypt(peer, ciphertext)),
    },
    take: () => {
      const current = verdict;
      verdict = undefined;
      return current;
    },
  };
}

/**
 * One `pending` invite per distinct signer-unavailable reason among `reports`. The wraps stay
 * undelivered — any `unavailable` report caps its cursor at its earliest `createdAt − 1`, so a
 * reopened subscription replays them.
 */
export function pendingInvites(reports: InputReport[]): InviteEvent[] {
  const reasons = new Set<"signer_refused" | "signer_failed">();
  for (const report of reports) {
    const { outcome } = report;
    if (
      outcome.type === "unavailable" &&
      (outcome.category === "signer_refused" || outcome.category === "signer_failed")
    ) {
      reasons.add(outcome.category);
    }
  }
  return [...reasons].map((reason) => ({ type: "pending", reason }));
}
