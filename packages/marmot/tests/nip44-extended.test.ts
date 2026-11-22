import { readFileSync } from "node:fs";
import { join } from "node:path";

import { chacha20 } from "@noble/ciphers/chacha.js";
import { sha256 } from "@noble/hashes/sha2.js";
import { describe, expect, test } from "vite-plus/test";

import { calcPaddedLen, decrypt, encrypt, getMessageKeys } from "@qntx/nostr/nips/nip44";

import { hexDecode, hexEncode } from "../src/backend/hex.ts";
import { utf8Decode, utf8Encode } from "../src/backend/utf8.ts";

const VECTORS = join(import.meta.dirname, "..", "..", "..", "vectors", "nip44");

type VectorCase = { len: number; plaintext_sha256: string; payload_sha256: string };
type Vectors = { conversation_key: string; nonce: string; cases: VectorCase[] };

const vectors = JSON.parse(readFileSync(join(VECTORS, "extended-length.json"), "utf8")) as Vectors;
const conversationKey = hexDecode(vectors.conversation_key);
const nonce = hexDecode(vectors.nonce);

const EXTENDED_PREFIX_THRESHOLD = 65_536;

function base64Decode(payload: string): Uint8Array {
  const binary = atob(payload);
  const bytes = new Uint8Array(binary.length);
  for (let i = 0; i < binary.length; i += 1) {
    bytes[i] = binary.codePointAt(i) ?? 0;
  }
  return bytes;
}

/** Decode a payload and strip the chacha20 layer: `prefix | plaintext | padding`. */
function paddedBlock(payload: string): { padded: Uint8Array; decodedLength: number } {
  const decoded = base64Decode(payload);
  const { chacha_key, chacha_nonce } = getMessageKeys(conversationKey, nonce);
  const ciphertext = decoded.subarray(1 + 32, -32);
  return { padded: chacha20(chacha_key, chacha_nonce, ciphertext), decodedLength: decoded.length };
}

describe("NIP-44 extended-length vectors", () => {
  test("encrypt reproduces every recorded payload and decrypt round-trips", () => {
    for (const vector of vectors.cases) {
      const plaintext = "a".repeat(vector.len);
      expect(hexEncode(sha256(utf8Encode(plaintext)))).toBe(vector.plaintext_sha256);
      const payload = encrypt(plaintext, conversationKey, nonce);
      expect(hexEncode(sha256(utf8Encode(payload)))).toBe(vector.payload_sha256);
      expect(decrypt(payload, conversationKey)).toBe(plaintext);
    }
  });

  test("the padded block carries a 2-byte u16be prefix below 65,536", () => {
    const legacy = vectors.cases.filter((vector) => vector.len < EXTENDED_PREFIX_THRESHOLD);
    expect(legacy).toHaveLength(1);
    for (const vector of legacy) {
      const payload = encrypt("a".repeat(vector.len), conversationKey, nonce);
      const { padded, decodedLength } = paddedBlock(payload);
      const view = new DataView(padded.buffer, padded.byteOffset, padded.byteLength);
      expect(view.getUint16(0)).toBe(vector.len);
      expect(decodedLength).toBe(1 + 32 + 2 + calcPaddedLen(vector.len) + 32);
      expect(utf8Decode(padded.subarray(2, 2 + vector.len))).toBe("a".repeat(vector.len));
    }
  });

  test("the padded block carries the 6-byte extended prefix at 65,536 and up", () => {
    const extended = vectors.cases.filter((vector) => vector.len >= EXTENDED_PREFIX_THRESHOLD);
    expect(extended).toHaveLength(2);
    for (const vector of extended) {
      const payload = encrypt("a".repeat(vector.len), conversationKey, nonce);
      const { padded, decodedLength } = paddedBlock(payload);
      const view = new DataView(padded.buffer, padded.byteOffset, padded.byteLength);
      expect(view.getUint16(0)).toBe(0);
      expect(view.getUint32(2)).toBe(vector.len);
      expect(decodedLength).toBe(1 + 32 + 6 + calcPaddedLen(vector.len) + 32);
      expect(utf8Decode(padded.subarray(6, 6 + vector.len))).toBe("a".repeat(vector.len));
    }
  });
});
