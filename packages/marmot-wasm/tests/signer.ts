/**
 * A `Signer` over `nostr-tools` — real NIP-01 signing and NIP-44 v2 encryption, so the wasm suites
 * exercise genuine gift-wrap and event flows. Test-only; never shipped.
 */

import { finalizeEvent, generateSecretKey, getPublicKey, nip44 } from "nostr-tools";

import type { Signer, NostrEvent, UnsignedEvent } from "@qntx/marmot/backend";

/** A `Signer` holding `secret` (a fresh random secret key when omitted). */
export function testSigner(secret: Uint8Array = generateSecretKey()): Signer {
  const conversationKey = (peer: string): Uint8Array =>
    nip44.v2.utils.getConversationKey(secret, peer);
  return {
    pubkey: getPublicKey(secret),
    signEvent: async (unsigned: UnsignedEvent): Promise<NostrEvent> => {
      await Promise.resolve();
      const event = finalizeEvent(
        {
          kind: unsigned.kind,
          created_at: Number(unsigned.createdAt),
          tags: unsigned.tags,
          content: unsigned.content,
        },
        secret,
      );
      return {
        id: event.id,
        pubkey: event.pubkey,
        createdAt: BigInt(event.created_at),
        kind: event.kind,
        tags: event.tags,
        content: event.content,
        sig: event.sig,
      };
    },
    nip44Encrypt: async (peer, plaintext) => {
      await Promise.resolve();
      return nip44.v2.encrypt(plaintext, conversationKey(peer));
    },
    nip44Decrypt: async (peer, ciphertext) => {
      await Promise.resolve();
      return nip44.v2.decrypt(ciphertext, conversationKey(peer));
    },
  };
}
