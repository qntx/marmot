/**
 * The smoke-app `Cipher`: chacha20poly1305 (the @noble/ciphers nostr-tools already pins) with a
 * random 96-bit nonce per value and the row key authenticated as additional data; the stored form
 * is nonce ‖ ciphertext, matching the e2e harness's IV ‖ ciphertext convention.
 */

import { chacha20poly1305 } from "@noble/ciphers/chacha.js";

import type { Cipher } from "@qntx/marmot/storage";

function randomNonce(): Uint8Array {
  const nonce = new Uint8Array(12);
  crypto.getRandomValues(nonce);
  return nonce;
}

export function chachaCipher(key: Uint8Array): Cipher {
  return {
    seal: async (rowKey, plain) => {
      await Promise.resolve();
      const nonce = randomNonce();
      const sealed = chacha20poly1305(key, nonce, rowKey).encrypt(plain);
      const out = new Uint8Array(nonce.length + sealed.length);
      out.set(nonce);
      out.set(sealed, nonce.length);
      return out;
    },
    open: async (rowKey, sealed) => {
      await Promise.resolve();
      return chacha20poly1305(key, sealed.subarray(0, 12), rowKey).decrypt(
        sealed.subarray(12),
      );
    },
  };
}
