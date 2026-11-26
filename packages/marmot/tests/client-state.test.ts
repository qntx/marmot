import { describe, expect, test } from "vite-plus/test";

import { utf8Encode } from "../src/backend/utf8.ts";
import { decodeClientState, encodeClientState } from "../src/client/state.ts";

function decode(text: string) {
  return decodeClientState(utf8Encode(text));
}

describe("decodeClientState — malformed inputs all yield undefined", () => {
  const malformed: Array<[string, Uint8Array | undefined]> = [
    ["undefined", undefined],
    ["empty bytes", new Uint8Array(0)],
    ["invalid utf-8", Uint8Array.of(0xff, 0xfe, 0xfd)],
    ["invalid json", utf8Encode("{not json")],
    ["json null", utf8Encode("null")],
    ["json scalar", utf8Encode("42")],
    ["json array", utf8Encode("[]")],
    [
      "missing v",
      utf8Encode(
        '{"inbox":"client","cursors":{},"floors":{},"inboxRelays":[],"nip44Extended":false}',
      ),
    ],
    ["v1 blob", utf8Encode('{"v":1,"cursors":{"k":9},"inbox":["wss://r"],"nip44Extended":true}')],
    // The v2 shape (no floors) is a full refetch — no migration.
    ["v2 blob", utf8Encode('{"v":2,"cursors":{"k":9},"inbox":["wss://r"],"nip44Extended":true}')],
    // A v3 blob — relay list under `inbox`, no owner — decodes to undefined: a full refetch
    // plus one signer probe, and the open adopts the option's mode.
    [
      "v3 blob",
      utf8Encode(
        '{"v":3,"cursors":{"k":9},"floors":{"k":10},"inbox":["wss://r"],"nip44Extended":true}',
      ),
    ],
    [
      "wrong v",
      utf8Encode(
        '{"v":5,"inbox":"client","cursors":{},"floors":{},"inboxRelays":[],"nip44Extended":false}',
      ),
    ],
    [
      "floors not object",
      utf8Encode(
        '{"v":4,"inbox":"client","cursors":{},"floors":[],"inboxRelays":[],"nip44Extended":false}',
      ),
    ],
    [
      "floor value string",
      utf8Encode(
        '{"v":4,"inbox":"client","cursors":{},"floors":{"k":"5"},"inboxRelays":[],"nip44Extended":false}',
      ),
    ],
    [
      "floor value negative",
      utf8Encode(
        '{"v":4,"inbox":"client","cursors":{},"floors":{"k":-1},"inboxRelays":[],"nip44Extended":false}',
      ),
    ],
    [
      "v as string",
      utf8Encode(
        '{"v":"4","inbox":"client","cursors":{},"floors":{},"inboxRelays":[],"nip44Extended":false}',
      ),
    ],
    [
      "missing cursors",
      utf8Encode('{"v":4,"inbox":"client","inboxRelays":[],"nip44Extended":false}'),
    ],
    [
      "cursors not object",
      utf8Encode('{"v":4,"inbox":"client","cursors":[],"inboxRelays":[],"nip44Extended":false}'),
    ],
    [
      "cursors null",
      utf8Encode('{"v":4,"inbox":"client","cursors":null,"inboxRelays":[],"nip44Extended":false}'),
    ],
    [
      "cursor value string",
      utf8Encode(
        '{"v":4,"inbox":"client","cursors":{"k":"5"},"floors":{},"inboxRelays":[],"nip44Extended":false}',
      ),
    ],
    [
      "cursor value float",
      utf8Encode(
        '{"v":4,"inbox":"client","cursors":{"k":1.5},"floors":{},"inboxRelays":[],"nip44Extended":false}',
      ),
    ],
    [
      "cursor value negative",
      utf8Encode(
        '{"v":4,"inbox":"client","cursors":{"k":-1},"floors":{},"inboxRelays":[],"nip44Extended":false}',
      ),
    ],
    [
      "missing inbox mode",
      utf8Encode('{"v":4,"cursors":{},"floors":{},"inboxRelays":[],"nip44Extended":false}'),
    ],
    [
      "unknown inbox mode",
      utf8Encode(
        '{"v":4,"inbox":"relay","cursors":{},"floors":{},"inboxRelays":[],"nip44Extended":false}',
      ),
    ],
    [
      "inbox mode not a string",
      utf8Encode(
        '{"v":4,"inbox":7,"cursors":{},"floors":{},"inboxRelays":[],"nip44Extended":false}',
      ),
    ],
    [
      "missing inboxRelays",
      utf8Encode('{"v":4,"inbox":"client","cursors":{},"floors":{},"nip44Extended":false}'),
    ],
    [
      "inboxRelays not array",
      utf8Encode(
        '{"v":4,"inbox":"client","cursors":{},"floors":{},"inboxRelays":{},"nip44Extended":false}',
      ),
    ],
    [
      "inboxRelays non-string",
      utf8Encode(
        '{"v":4,"inbox":"client","cursors":{},"floors":{},"inboxRelays":[7],"nip44Extended":false}',
      ),
    ],
    [
      "missing nip44Extended",
      utf8Encode('{"v":4,"inbox":"client","cursors":{},"floors":{},"inboxRelays":[]}'),
    ],
    [
      "nip44Extended not boolean",
      utf8Encode(
        '{"v":4,"inbox":"client","cursors":{},"floors":{},"inboxRelays":[],"nip44Extended":"yes"}',
      ),
    ],
    // A v4 blob stamped with the v3 relay-list field name is missing `inboxRelays`.
    [
      "v4 shape stamped with v3 field",
      utf8Encode('{"v":4,"inbox":["wss://r"],"cursors":{},"floors":{},"nip44Extended":false}'),
    ],
  ];

  test.each(malformed)("%s", (_label, bytes) => {
    expect(decodeClientState(bytes)).toBeUndefined();
  });
});

describe("decodeClientState — valid states", () => {
  test("a fresh blob decodes to empty cursors and inbox relays", () => {
    expect(
      decode(
        '{"v":4,"inbox":"client","cursors":{},"floors":{},"inboxRelays":[],"nip44Extended":false}',
      ),
    ).toStrictEqual({
      inbox: "client",
      cursors: {},
      floors: {},
      inboxRelays: [],
      nip44Extended: false,
    });
    expect(
      decode(
        '{"v":4,"inbox":"host","cursors":{},"floors":{},"inboxRelays":[],"nip44Extended":true}',
      ),
    ).toStrictEqual({
      inbox: "host",
      cursors: {},
      floors: {},
      inboxRelays: [],
      nip44Extended: true,
    });
  });

  test("mode, cursors, floors, inbox relays and the probe flag round-trip", () => {
    const state = {
      inbox: "client" as const,
      cursors: { "inbox|wss://r": 42, "group|a|wss://r": 7 },
      floors: { "inbox|wss://r": 43 },
      inboxRelays: ["wss://r"],
      nip44Extended: true,
    };
    const decoded = decodeClientState(encodeClientState(state));
    expect(decoded).toStrictEqual(state);
    const withExtra = decode(
      '{"v":4,"inbox":"client","cursors":{"k":1},"floors":{},"inboxRelays":["wss://r"],"nip44Extended":true,"future":true}',
    );
    expect(withExtra).toStrictEqual({
      inbox: "client",
      cursors: { k: 1 },
      floors: {},
      inboxRelays: ["wss://r"],
      nip44Extended: true,
    });
  });
});

describe("encodeClientState", () => {
  test("the blob is the versioned {v,inbox,cursors,floors,inboxRelays,nip44Extended} shape", () => {
    const bytes = encodeClientState({
      inbox: "host",
      cursors: { k: 9 },
      floors: { k: 10 },
      inboxRelays: [],
      nip44Extended: true,
    });
    expect(decodeClientState(bytes)).toStrictEqual({
      inbox: "host",
      cursors: { k: 9 },
      floors: { k: 10 },
      inboxRelays: [],
      nip44Extended: true,
    });
    expect(new TextDecoder().decode(bytes)).toBe(
      '{"v":4,"inbox":"host","cursors":{"k":9},"floors":{"k":10},"inboxRelays":[],"nip44Extended":true}',
    );
  });
});
