import { describe, expect, test } from "vite-plus/test";

import { DecodeError } from "../src/backend/errors.ts";
import { hexDecode, hexEncode } from "../src/backend/hex.ts";
import { decodeKvBatch, decodeRows, encodeKvBatch, encodeRows } from "../src/backend/kv.ts";
import { utf8Decode, utf8Encode } from "../src/backend/utf8.ts";

describe("utf8", () => {
  test("round-trips ASCII, multibyte and supplementary-plane text", () => {
    for (const text of ["", "hello", "héllo wörld", "中文测试", "emoji 😀🎉", "\0"]) {
      expect(utf8Decode(utf8Encode(text))).toBe(text);
    }
  });

  test("encode produces valid UTF-8 bytes", () => {
    expect([...utf8Encode("é")]).toStrictEqual([0xc3, 0xa9]);
    expect([...utf8Encode("中")]).toStrictEqual([0xe4, 0xb8, 0xad]);
    expect([...utf8Encode("😀")]).toStrictEqual([0xf0, 0x9f, 0x98, 0x80]);
  });

  test("rejects lone surrogates on encode", () => {
    expect(() => utf8Encode("a\uD800b")).toThrow(DecodeError);
    expect(() => utf8Encode("a\uDCFFb")).toThrow(DecodeError);
  });

  test("rejects malformed sequences on decode", () => {
    // truncated multi-byte
    expect(() => utf8Decode(Uint8Array.of(0xc3))).toThrow(DecodeError);
    // invalid continuation
    expect(() => utf8Decode(Uint8Array.of(0xc3, 0x28))).toThrow(DecodeError);
    // overlong encoding of 'A'
    expect(() => utf8Decode(Uint8Array.of(0xc0, 0xaf))).toThrow(DecodeError);
    // encoded surrogate
    expect(() => utf8Decode(Uint8Array.of(0xed, 0xa0, 0x80))).toThrow(DecodeError);
    // out of range
    expect(() => utf8Decode(Uint8Array.of(0xf4, 0x90, 0x80, 0x80))).toThrow(DecodeError);
  });
});

describe("hex", () => {
  test("round-trips and stays lowercase", () => {
    const bytes = Uint8Array.of(0, 1, 0xab, 0xff, 0x10);
    expect(hexEncode(bytes)).toBe("0001abff10");
    expect([...hexDecode("0001abff10")]).toStrictEqual([...bytes]);
  });

  test("rejects uppercase, odd length and non-hex", () => {
    expect(() => hexDecode("AB")).toThrow(DecodeError);
    expect(() => hexDecode("abc")).toThrow(DecodeError);
    expect(() => hexDecode("zz")).toThrow(DecodeError);
  });
});

describe("kv codecs", () => {
  test("rows round-trip", () => {
    const rows: Array<[Uint8Array, Uint8Array]> = [
      [Uint8Array.of(1, 2), Uint8Array.of(3, 4, 5)],
      [Uint8Array.of(0xaa), Uint8Array.of(0xbb)],
    ];
    expect(decodeRows(encodeRows(rows))).toStrictEqual(rows);
  });

  test("rows rejects truncation and trailing bytes", () => {
    expect(() => decodeRows(Uint8Array.of(1, 0, 0))).toThrow(DecodeError);
    expect(() => decodeRows(Uint8Array.of(1, 0, 0, 0, 0, 9))).toThrow(DecodeError);
    expect(() => decodeRows(Uint8Array.of(2))).toThrow(DecodeError);
  });

  test("batch round-trip", () => {
    const batch = {
      seq: 42n,
      ops: [
        { type: "put" as const, key: Uint8Array.of(1), value: Uint8Array.of(9, 8) },
        { type: "delete" as const, key: Uint8Array.of(2) },
      ],
    };
    expect(decodeKvBatch(encodeKvBatch(batch))).toStrictEqual(batch);
  });

  test("batch decodes the header seq as u64 big-endian", () => {
    const batch = {
      seq: 0x0102030405060708n,
      ops: [{ type: "delete" as const, key: Uint8Array.of(0) }],
    };
    const bytes = encodeKvBatch(batch);
    expect(bytes[0]).toBe(1);
    expect(bytes.slice(1, 9)).toStrictEqual(Uint8Array.of(1, 2, 3, 4, 5, 6, 7, 8));
    expect(decodeKvBatch(bytes).seq).toBe(0x0102030405060708n);
  });

  test("batch rejects empty ops, bad tags, truncation and trailing bytes", () => {
    expect(() => decodeKvBatch(encodeKvBatch({ seq: 1n, ops: [] }))).toThrow(DecodeError);
    const bad = encodeKvBatch({ seq: 1n, ops: [{ type: "delete", key: Uint8Array.of(1) }] });
    const badTag = Uint8Array.from(bad);
    badTag[13] = 9;
    expect(() => decodeKvBatch(badTag)).toThrow(DecodeError);
    expect(() => decodeKvBatch(bad.slice(0, -1))).toThrow(DecodeError);
    expect(() => decodeKvBatch(Uint8Array.from([...bad, 0]))).toThrow(DecodeError);
    const wrongVersion = Uint8Array.from(bad);
    wrongVersion[0] = 2;
    expect(() => decodeKvBatch(wrongVersion)).toThrow(DecodeError);
  });
});
