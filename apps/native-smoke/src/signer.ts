/** A `Signer` over nostr-tools — same real NIP-01 + NIP-44 v2 surface the wasm suites drive. */

import { finalizeEvent, generateSecretKey, getPublicKey, nip44 } from "nostr-tools";

import type { NostrEvent, Signer, UnsignedEvent } from "@qntx/marmot/backend";

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
