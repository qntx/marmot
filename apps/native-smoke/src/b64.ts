/** Minimal base64 — Hermes has no atob/btoa. */

const ALPHABET = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

export function b64encode(bytes: Uint8Array): string {
  let out = "";
  for (let i = 0; i < bytes.length; i += 3) {
    const a = bytes[i] ?? 0;
    const b = bytes[i + 1];
    const c = bytes[i + 2];
    out += ALPHABET[a >> 2] ?? "";
    out += ALPHABET[((a & 3) << 4) | ((b ?? 0) >> 4)] ?? "";
    out += b === undefined ? "=" : (ALPHABET[((b & 15) << 2) | ((c ?? 0) >> 6)] ?? "");
    out += c === undefined ? "=" : (ALPHABET[c & 63] ?? "");
  }
  return out;
}

export function b64decode(text: string): Uint8Array {
  const clean = text.replace(/=+$/, "");
  const out = new Uint8Array(Math.floor((clean.length * 3) / 4));
  let o = 0;
  for (let i = 0; i < clean.length; i += 4) {
    const chunk = [...clean.slice(i, i + 4)].map((ch) => ALPHABET.indexOf(ch));
    const a = chunk[0] ?? 0;
    const b = chunk[1] ?? 0;
    const c = chunk[2] ?? 0;
    const d = chunk[3] ?? 0;
    const triple = (a << 18) | (b << 12) | (c << 6) | d;
    if (o < out.length) out[o] = (triple >> 16) & 0xff;
    o += 1;
    if (o < out.length) out[o] = (triple >> 8) & 0xff;
    o += 1;
    if (o < out.length) out[o] = triple & 0xff;
    o += 1;
  }
  return out;
}

export function hexToBytes(hex: string): Uint8Array {
  const out = new Uint8Array(hex.length / 2);
  for (let i = 0; i < out.length; i += 1) {
    out[i] = Number.parseInt(hex.slice(i * 2, i * 2 + 2), 16);
  }
  return out;
}
