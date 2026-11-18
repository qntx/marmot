/** Lowercase hex transcoding for wire byte fields. */

import { DecodeError } from "./errors.ts";

const NIBBLES = "0123456789abcdef";

/** Encode bytes as a lowercase hex string. */
export function hexEncode(bytes: Uint8Array): string {
  let out = "";
  for (const byte of bytes) {
    out += NIBBLES[Math.floor(byte / 16)];
    out += NIBBLES[byte % 16];
  }
  return out;
}

function nibble(char: string, path: string): number {
  const value = NIBBLES.indexOf(char);
  if (value === -1) {
    throw new DecodeError(path, "hex");
  }
  return value;
}

/** Decode a lowercase hex string, rejecting odd lengths and other alphabets. */
export function hexDecode(text: string, path = "input"): Uint8Array {
  if (text.length % 2 !== 0) {
    throw new DecodeError(path, "hex");
  }
  const bytes = new Uint8Array(text.length / 2);
  for (let i = 0; i < text.length; i += 2) {
    bytes[i / 2] = nibble(text[i] ?? "", path) * 16 + nibble(text[i + 1] ?? "", path);
  }
  return bytes;
}
