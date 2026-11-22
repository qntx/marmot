import { describe, expect, test } from "vite-plus/test";

import { getPublicKey } from "@qntx/nostr";
import { decryptFromPubkey, encryptToPubkey } from "@qntx/nostr/nips/nip44";

import type { Signer } from "../src/backend/engine.ts";
import {
  checkSignerNip44,
  NIP44_EXTENDED_SIZES,
  Nip44ConformanceError,
} from "../src/testing/nip44.ts";

const SECRET = new Uint8Array(32).fill(9);

/** A `Signer` whose NIP-44 legs are real `@qntx/nostr` v2 calls under `secret`. */
function keysSigner(secret: Uint8Array): Signer {
  return {
    pubkey: getPublicKey(secret),
    signEvent: async (unsigned) =>
      Promise.resolve({
        id: "e".repeat(64),
        pubkey: unsigned.pubkey,
        createdAt: unsigned.createdAt,
        kind: unsigned.kind,
        tags: unsigned.tags,
        content: unsigned.content,
        sig: "5".repeat(128),
      }),
    nip44Encrypt: async (peer, plaintext) =>
      Promise.resolve(encryptToPubkey(plaintext, secret, peer)),
    nip44Decrypt: async (peer, ciphertext) =>
      Promise.resolve(
        decryptFromPubkey(ciphertext, secret, peer, { maxPayloadChars: ciphertext.length }),
      ),
  };
}

/** A pre-NIP-44-extended signer: refuses any plaintext past the legacy u16 limit. */
function legacySigner(secret: Uint8Array): Signer {
  const inner = keysSigner(secret);
  return {
    ...inner,
    nip44Encrypt: async (peer, plaintext) => {
      if (plaintext.length > 65_535) {
        throw new Error("plaintext too long");
      }
      return inner.nip44Encrypt(peer, plaintext);
    },
  };
}

/** Encrypts only the first 65,535 bytes, dropping the rest without an error. */
function truncatingEncryptSigner(secret: Uint8Array): Signer {
  const inner = keysSigner(secret);
  return {
    ...inner,
    nip44Encrypt: async (peer, plaintext) =>
      Promise.resolve(encryptToPubkey(plaintext.slice(0, 65_535), secret, peer)),
  };
}

/** Decrypts correctly but returns only the first 65,535 bytes of plaintext. */
function truncatingDecryptSigner(secret: Uint8Array): Signer {
  const inner = keysSigner(secret);
  return {
    ...inner,
    nip44Decrypt: async (peer, ciphertext) => {
      const plaintext = await inner.nip44Decrypt(peer, ciphertext);
      return plaintext.slice(0, 65_535);
    },
  };
}

describe("checkSignerNip44", () => {
  test("a real @qntx/nostr signer passes", async () => {
    await expect(checkSignerNip44(keysSigner(SECRET))).resolves.toBeUndefined();
  });

  test("a legacy signer that throws past 65,535 bytes fails at nip44Encrypt", async () => {
    await expect(checkSignerNip44(legacySigner(SECRET))).rejects.toThrow(Nip44ConformanceError);
    await expect(checkSignerNip44(legacySigner(SECRET))).rejects.toThrow(
      "signer nip44Encrypt failed for a 65536-byte plaintext (NIP-44 extended length)",
    );
  });

  test("a signer that silently truncates the plaintext fails at nip44Encrypt", async () => {
    await expect(checkSignerNip44(truncatingEncryptSigner(SECRET))).rejects.toThrow(
      Nip44ConformanceError,
    );
    await expect(checkSignerNip44(truncatingEncryptSigner(SECRET))).rejects.toThrow("nip44Encrypt");
  });

  test("a signer whose decrypt truncates fails at nip44Decrypt", async () => {
    const failure = await checkSignerNip44(truncatingDecryptSigner(SECRET)).then(
      () => undefined,
      (error: unknown) => error,
    );
    expect(failure).toBeInstanceOf(Nip44ConformanceError);
    expect((failure as Nip44ConformanceError).step).toBe("nip44Decrypt");
    expect((failure as Nip44ConformanceError).size).toBe(NIP44_EXTENDED_SIZES[0]);
  });
});
