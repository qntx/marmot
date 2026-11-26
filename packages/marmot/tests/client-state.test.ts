import { describe, expect, test } from "vite-plus/test";

import { utf8Encode } from "../src/backend/utf8.ts";
import { decodeClientState, encodeClientState } from "../src/client/state.ts";

const EMPTY = { cursors: {}, floors: {}, inbox: [], nip44Extended: false };

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
    ["missing v", utf8Encode('{"cursors":{},"floors":{},"inbox":[],"nip44Extended":false}')],
    ["v1 blob", utf8Encode('{"v":1,"cursors":{"k":9},"inbox":["wss://r"],"nip44Extended":true}')],
    ["wrong v", utf8Encode('{"v":4,"cursors":{},"floors":{},"inbox":[],"nip44Extended":false}')],
    // The v2 shape (no floors) is a full refetch — no migration.
    ["v2 blob", utf8Encode('{"v":2,"cursors":{"k":9},"inbox":["wss://r"],"nip44Extended":true}')],
    [
      "v2 shape stamped v3",
      utf8Encode('{"v":3,"cursors":{"k":9},"inbox":["wss://r"],"nip44Extended":true}'),
    ],
    [
      "floors not object",
      utf8Encode('{"v":3,"cursors":{},"floors":[],"inbox":[],"nip44Extended":false}'),
    ],
    [
      "floor value string",
      utf8Encode('{"v":3,"cursors":{},"floors":{"k":"5"},"inbox":[],"nip44Extended":false}'),
    ],
    [
      "floor value negative",
      utf8Encode('{"v":3,"cursors":{},"floors":{"k":-1},"inbox":[],"nip44Extended":false}'),
    ],
    [
      "v as string",
      utf8Encode('{"v":"2","cursors":{},"floors":{},"inbox":[],"nip44Extended":false}'),
    ],
    ["missing cursors", utf8Encode('{"v":3,"inbox":[],"nip44Extended":false}')],
    ["cursors not object", utf8Encode('{"v":3,"cursors":[],"inbox":[],"nip44Extended":false}')],
    ["cursors null", utf8Encode('{"v":3,"cursors":null,"inbox":[],"nip44Extended":false}')],
    [
      "cursor value string",
      utf8Encode('{"v":3,"cursors":{"k":"5"},"floors":{},"inbox":[],"nip44Extended":false}'),
    ],
    [
      "cursor value float",
      utf8Encode('{"v":3,"cursors":{"k":1.5},"floors":{},"inbox":[],"nip44Extended":false}'),
    ],
    [
      "cursor value negative",
      utf8Encode('{"v":3,"cursors":{"k":-1},"floors":{},"inbox":[],"nip44Extended":false}'),
    ],
    ["missing inbox", utf8Encode('{"v":3,"cursors":{},"nip44Extended":false}')],
    [
      "inbox not array",
      utf8Encode('{"v":3,"cursors":{},"floors":{},"inbox":{},"nip44Extended":false}'),
    ],
    [
      "inbox non-string",
      utf8Encode('{"v":3,"cursors":{},"floors":{},"inbox":[7],"nip44Extended":false}'),
    ],
    ["missing nip44Extended", utf8Encode('{"v":3,"cursors":{},"floors":{},"inbox":[]}')],
    [
      "nip44Extended not boolean",
      utf8Encode('{"v":3,"cursors":{},"floors":{},"inbox":[],"nip44Extended":"yes"}'),
    ],
  ];

  test.each(malformed)("%s", (_label, bytes) => {
    expect(decodeClientState(bytes)).toStrictEqual(EMPTY);
  });
});

describe("decodeClientState — valid states", () => {
  test("a fresh blob decodes to empty cursors and inbox", () => {
    expect(
      decode('{"v":3,"cursors":{},"floors":{},"inbox":[],"nip44Extended":false}'),
    ).toStrictEqual(EMPTY);
  });

  test("cursors, floors, inbox and the probe flag round-trip, extra keys tolerated", () => {
    const state = {
      cursors: { "inbox|wss://r": 42, "group|a|wss://r": 7 },
      floors: { "inbox|wss://r": 43 },
      inbox: ["wss://r"],
      nip44Extended: true,
    };
    const decoded = decodeClientState(encodeClientState(state));
    expect(decoded).toStrictEqual(state);
    const withExtra = decode(
      '{"v":3,"cursors":{"k":1},"floors":{},"inbox":["wss://r"],"nip44Extended":true,"future":true}',
    );
    expect(withExtra).toStrictEqual({
      cursors: { k: 1 },
      floors: {},
      inbox: ["wss://r"],
      nip44Extended: true,
    });
  });
});

describe("encodeClientState", () => {
  test("the blob is the versioned {v,cursors,floors,inbox,nip44Extended} shape", () => {
    const bytes = encodeClientState({
      cursors: { k: 9 },
      floors: { k: 10 },
      inbox: [],
      nip44Extended: true,
    });
    expect(decodeClientState(bytes)).toStrictEqual({
      cursors: { k: 9 },
      floors: { k: 10 },
      inbox: [],
      nip44Extended: true,
    });
    expect(new TextDecoder().decode(bytes)).toBe(
      '{"v":3,"cursors":{"k":9},"floors":{"k":10},"inbox":[],"nip44Extended":true}',
    );
  });
});
