/**
 * `invites` — invite-facing helpers: the typed `InviteEvent`, the `observeSigner` wrapper that
 * records signer refusals/failures, and `blockedInvites`, which attributes rejected gift wraps to a
 * refusing signer. A refused or failed wrap is terminal at the engine (redelivery is a `duplicate`,
 * never retried), so the client surfaces a `blocked` event rather than keeping it.
 */

import { SignerRefusedError } from "../backend/engine.ts";
import type { Signer } from "../backend/engine.ts";
import type { InputReport } from "../backend/protocol.ts";

export type InviteEvent =
  | { type: "joined"; group: string }
  | { type: "blocked"; reason: "signer_refused" | "signer_failed"; wrap: string };

/**
 * Wrap `signer` so the first refused/failed call since the last `take()` is recorded.
 * `SignerRefusedError` records "refused", any other throw "failed"; the error is always rethrown
 * unchanged.
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
      verdict ??= error instanceof SignerRefusedError ? "refused" : "failed";
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
 * The blocked invites of one ingest batch: every input that is a gift wrap (`wraps` holds the
 * kind-1059 ids of the batch) reported `rejected: invalid_encoding` while the signer refused or
 * failed. A `undefined` verdict means the rejections cannot be attributed to the signer.
 */
export function blockedInvites(
  reports: InputReport[],
  wraps: ReadonlySet<string>,
  verdict: "refused" | "failed" | undefined,
): InviteEvent[] {
  if (verdict === undefined) {
    return [];
  }
  const reason = verdict === "refused" ? "signer_refused" : "signer_failed";
  const blocked: InviteEvent[] = [];
  for (const report of reports) {
    if (
      report.outcome.type === "rejected" &&
      report.outcome.category === "invalid_encoding" &&
      wraps.has(report.event)
    ) {
      blocked.push({ type: "blocked", reason, wrap: report.event });
    }
  }
  return blocked;
}
