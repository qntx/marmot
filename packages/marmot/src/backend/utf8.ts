/**
 * Strict UTF-8 transcoding. Written by hand because Hermes has no `TextDecoder`/`TextEncoder`: the
 * decoder rejects malformed input rather than substituting U+FFFD, matching the Rust bridge's
 * `from_utf8` gate.
 */

import { DecodeError } from "./errors.ts";

const SURROGATE_MIN = 0xd800;
const SURROGATE_MAX = 0xdfff;
const CONT_MIN = 0x80;
const CONT_MAX = 0xbf;

/** Encode `text` as UTF-8 bytes. Lone surrogates are rejected. */
export function utf8Encode(text: string): Uint8Array {
  const out: number[] = [];
  let i = 0;
  while (i < text.length) {
    const cp = text.codePointAt(i) ?? 0;
    i += cp > 0xffff ? 2 : 1;
    if (cp >= SURROGATE_MIN && cp <= SURROGATE_MAX) {
      throw new DecodeError("input", "lone surrogate");
    }
    if (cp < 0x80) {
      out.push(cp);
    } else if (cp < 0x800) {
      out.push(0xc0 + Math.floor(cp / 0x40), 0x80 + (cp % 0x40));
    } else if (cp < 0x10000) {
      out.push(
        0xe0 + Math.floor(cp / 0x1000),
        0x80 + (Math.floor(cp / 0x40) % 0x40),
        0x80 + (cp % 0x40),
      );
    } else {
      out.push(
        0xf0 + Math.floor(cp / 0x40000),
        0x80 + (Math.floor(cp / 0x1000) % 0x40),
        0x80 + (Math.floor(cp / 0x40) % 0x40),
        0x80 + (cp % 0x40),
      );
    }
  }
  return Uint8Array.from(out);
}

function continuation(bytes: Uint8Array, index: number): number {
  const byte = bytes[index];
  if (byte === undefined || byte < CONT_MIN || byte > CONT_MAX) {
    throw new DecodeError("input", "invalid utf8");
  }
  return byte - CONT_MIN;
}

/** Decode `bytes` as UTF-8, throwing `DecodeError` on malformed input. */
export function utf8Decode(bytes: Uint8Array): string {
  const units: number[] = [];
  let i = 0;
  while (i < bytes.length) {
    const first = bytes[i];
    if (first === undefined) {
      break;
    }
    let cp: number;
    if (first < 0x80) {
      cp = first;
      i += 1;
    } else if (first >= 0xc2 && first <= 0xdf) {
      cp = (first - 0xc0) * 0x40 + continuation(bytes, i + 1);
      i += 2;
    } else if (first >= 0xe0 && first <= 0xef) {
      cp = (first - 0xe0) * 0x1000 + continuation(bytes, i + 1) * 0x40 + continuation(bytes, i + 2);
      if (cp < 0x800 || (cp >= SURROGATE_MIN && cp <= SURROGATE_MAX)) {
        throw new DecodeError("input", "invalid utf8");
      }
      i += 3;
    } else if (first >= 0xf0 && first <= 0xf4) {
      cp =
        (first - 0xf0) * 0x40000 +
        continuation(bytes, i + 1) * 0x1000 +
        continuation(bytes, i + 2) * 0x40 +
        continuation(bytes, i + 3);
      if (cp < 0x10000 || cp > 0x10ffff) {
        throw new DecodeError("input", "invalid utf8");
      }
      i += 4;
    } else {
      throw new DecodeError("input", "invalid utf8");
    }
    units.push(cp);
  }
  let text = "";
  const CHUNK = 0x8000;
  for (let start = 0; start < units.length; start += CHUNK) {
    const slice = units.slice(start, start + CHUNK);
    text += String.fromCodePoint(...slice);
  }
  return text;
}
