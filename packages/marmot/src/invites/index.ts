/**
 * `invites` — invite-facing helpers: the typed `InviteEvent`, `pendingInvites`, which surfaces
 * signer-refused/failed gift-wrap unwraps as retryable `pending` events, and `observeSigner`, which
 * records signer refusals/failures so client ops map them to typed errors. A refused or failed
 * unwrap is `unavailable` in the engine — rolled back and retryable — so the client emits `pending`
 * and holds that inbox cursor under the wrap (a sticky floor in the client state), so it
 * re-surfaces after any restart or `retryInvites` until it is opened, permanently rejected, or its
 * `createdAt` is 30 days old. Under a host-owned inbox no floor exists — the `pending` event is the
 * whole signal and re-delivery is the host re-forwarding the wrap. A `SignerInvalidPayloadError`
 * unwrap is a successful signer invocation whose verdict is on the payload: the engine rejects the
 * wrap permanently, so it is neither a `pending` condition nor a recorded verdict.
 */

import { SignerInvalidPayloadError, SignerRefusedError } from "../backend/engine.ts";
import type { Signer } from "../backend/engine.ts";
import type { InputReport } from "../backend/protocol.ts";

/**
 * `joined`: a Welcome was installed. `pending`: the signer refused or failed to open a gift wrap;
 * nothing is stored but the wrap's inbox cursor floor, so the same invite re-surfaces as `pending`
 * after any restart or `retryInvites()` until it is opened (`joined`), permanently rejected, or its
 * `createdAt` is 30 days old (`DEFERRED_PEEL_RESIDENCE_SECS`) — then it is given up and never
 * replayed again by this device.
 */
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
 * One `pending` invite per distinct signer-unavailable reason among `reports`. On the subscription
 * path the wraps stay undelivered — the pump holds each one's inbox cursor under it (a sticky
 * floor), so a reopened subscription replays them; the pump leaves out wraps it has given up on (30
 * days old). A host-forwarded wrap (`Pump.ingestWrap`) reports all of them: no floor exists there —
 * re-delivery is the host re-forwarding the wrap.
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

/**
 * One self round trip at the NIP-44 extended-length boundary (spec `transports/nostr.md`): the
 * signer must encrypt and decrypt its own 65,536-byte plaintext exactly — that size needs the
 * 6-byte `00 00 ‖ u32be(len)` prefix a legacy signer cannot produce. `SignerRefusedError` maps to
 * "refused" (retryable: unlock and open again); any other throw or a mismatched round trip maps to
 * "unsupported". `MarmotClient.open` runs this once per client state and caches the pass as
 * `nip44Extended`.
 */
export async function probeNip44Extended(
  signer: Signer,
): Promise<"ok" | "refused" | "unsupported"> {
  const plaintext = "a".repeat(65_536);
  try {
    const ciphertext = await signer.nip44Encrypt(signer.pubkey, plaintext);
    const recovered = await signer.nip44Decrypt(signer.pubkey, ciphertext);
    return recovered === plaintext ? "ok" : "unsupported";
  } catch (error) {
    return error instanceof SignerRefusedError ? "refused" : "unsupported";
  }
}
