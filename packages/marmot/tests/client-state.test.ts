import { describe, expect, test } from "vite-plus/test";

import { utf8Encode } from "../src/backend/utf8.ts";
import { decodeClientState, encodeClientState } from "../src/client/state.ts";

const EMPTY = { cursors: {}, inbox: [] };

function decode(text: string) {
  return decodeClientState(utf8Encode(text));
}

describe("decodeClientState — malformed inputs all yield the empty state", () => {
  const malformed: Array<[string, Uint8Array | undefined]> = [
    ["undefined", undefined],
    ["empty bytes", new Uint8Array(0)],
    ["invalid utf-8", Uint8Array.of(0xff, 0xfe, 0xfd)],
    ["invalid json", utf8Encode("{not json")],
    ["json null", utf8Encode("null")],
    ["json scalar", utf8Encode("42")],
    ["json array", utf8Encode("[]")],
    ["missing v", utf8Encode('{"cursors":{},"inbox":[]}')],
    ["wrong v", utf8Encode('{"v":2,"cursors":{},"inbox":[]}')],
    ["v as string", utf8Encode('{"v":"1","cursors":{},"inbox":[]}')],
    ["missing cursors", utf8Encode('{"v":1,"inbox":[]}')],
    ["cursors not object", utf8Encode('{"v":1,"cursors":[],"inbox":[]}')],
    ["cursors null", utf8Encode('{"v":1,"cursors":null,"inbox":[]}')],
    ["cursor value string", utf8Encode('{"v":1,"cursors":{"k":"5"},"inbox":[]}')],
    ["cursor value float", utf8Encode('{"v":1,"cursors":{"k":1.5},"inbox":[]}')],
    ["cursor value negative", utf8Encode('{"v":1,"cursors":{"k":-1},"inbox":[]}')],
    ["missing inbox", utf8Encode('{"v":1,"cursors":{}}')],
    ["inbox not array", utf8Encode('{"v":1,"cursors":{},"inbox":{}}')],
    ["inbox non-string", utf8Encode('{"v":1,"cursors":{},"inbox":[7]}')],
  ];

  test.each(malformed)("%s", (_label, bytes) => {
    expect(decodeClientState(bytes)).toStrictEqual(EMPTY);
  });
});

describe("decodeClientState — valid states", () => {
  test("a fresh blob decodes to empty cursors and inbox", () => {
    expect(decode('{"v":1,"cursors":{},"inbox":[]}')).toStrictEqual(EMPTY);
  });

  test("cursors and inbox round-trip, extra keys tolerated", () => {
    const state = { cursors: { "inbox|wss://r": 42, "group|a|wss://r": 7 }, inbox: ["wss://r"] };
    const decoded = decodeClientState(encodeClientState(state));
    expect(decoded).toStrictEqual(state);
    const withExtra = decode('{"v":1,"cursors":{"k":1},"inbox":["wss://r"],"future":true}');
    expect(withExtra).toStrictEqual({ cursors: { k: 1 }, inbox: ["wss://r"] });
  });
});

describe("encodeClientState", () => {
  test("the blob is the versioned {v,cursors,inbox} shape", () => {
    const bytes = encodeClientState({ cursors: { k: 9 }, inbox: [] });
    expect(decodeClientState(bytes)).toStrictEqual({ cursors: { k: 9 }, inbox: [] });
    expect(new TextDecoder().decode(bytes)).toBe('{"v":1,"cursors":{"k":9},"inbox":[]}');
  });
});
