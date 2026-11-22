import { readFileSync } from "node:fs";
import { join } from "node:path";

import { describe, expect, test } from "vite-plus/test";

import {
  decodeAttachmentResult,
  decodeAvailability,
  decodeCallValue,
  decodeConfigValue,
  decodeDropReason,
  decodeGroupAccess,
  decodeGroupChange,
  decodeGroupState,
  decodeInputOutcome,
  decodeLocalState,
  decodeMessageIntent,
  decodeOpResult,
  decodeOpenResponseValue,
  decodeOutboundPurpose,
  decodePublishResolution,
  decodeRelayStatus,
  decodeRequest,
  decodeResponseValue,
  decodeSemantics,
  decodeSendOutcome,
  decodeSessionEvent,
  decodeSignerOp,
  decodeSignerResult,
  decodeWireError,
  encodeAttachmentResult,
  encodeCallValue,
  encodeConfigValue,
  encodeGroupChange,
  encodeInputOutcome,
  encodeMessageIntent,
  encodeOpResult,
  encodeOpenResponse,
  encodeRequest,
  encodeResponse,
  encodeSemantics,
  encodeSendOutcome,
  encodeSessionEvent,
  encodeSignerOp,
  encodeSignerResult,
  encodeWireError,
} from "../src/backend/codec.ts";
import {
  ATTACHMENT_RESULT_TYPES,
  AVAILABILITY_VALUES,
  DROP_REASONS,
  ERROR_KINDS,
  GROUP_ACCESSES,
  GROUP_CHANGE_TYPES,
  GROUP_STATES,
  INPUT_OUTCOME_TYPES,
  LOCAL_STATES,
  MESSAGE_INTENT_TYPES,
  OPEN_RESPONSE_TYPES,
  OUTBOUND_PURPOSES,
  PROTOCOL_VERSION,
  PUBLISH_RESOLUTIONS,
  RELAY_STATUSES,
  REQUEST_TYPES,
  RESPONSE_TYPES,
  RESULT_TYPES,
  SEMANTICS_TYPES,
  SEND_OUTCOME_TYPES,
  SESSION_EVENT_TYPES,
  SIGNER_OP_TYPES,
  SIGNER_RESULT_TYPES,
} from "../src/backend/protocol.ts";

const VECTORS = join(import.meta.dirname, "..", "..", "..", "vectors", "bridge");
const PROTOCOL_RS = join(
  import.meta.dirname,
  "..",
  "..",
  "..",
  "crates",
  "mk-bridge",
  "src",
  "protocol.rs",
);

function vector(name: string): Record<string, unknown> {
  const parsed: unknown = JSON.parse(readFileSync(join(VECTORS, `${name}.json`), "utf8"));
  if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed)) {
    throw new Error(`vector ${name} is not an object`);
  }
  return parsed as Record<string, unknown>;
}

type UnionCase = {
  file: string;
  types: ReadonlyArray<string>;
  decode: (value: unknown) => unknown;
  run: (value: unknown) => unknown;
};

function caseOf<T>(
  file: string,
  types: ReadonlyArray<string>,
  decode: (value: unknown) => T,
  encode: (value: T) => unknown,
): UnionCase {
  return { file, types, decode, run: (value) => encode(decode(value)) };
}

/** The decoded variant tag: `type` for tagged unions, `kind` for errors, the value for fieldless. */
function tagOf(decoded: unknown): string {
  if (typeof decoded === "string") {
    return decoded;
  }
  if (typeof decoded === "object" && decoded !== null && "type" in decoded) {
    return String(decoded.type);
  }
  if (typeof decoded === "object" && decoded !== null && "kind" in decoded) {
    return String(decoded.kind);
  }
  throw new Error("decoded value has no variant tag");
}

const UNION_CASES = [
  caseOf("request", REQUEST_TYPES, decodeRequest, encodeRequest),
  caseOf("result", RESULT_TYPES, decodeOpResult, encodeOpResult),
  caseOf("response", RESPONSE_TYPES, decodeResponseValue, encodeResponse),
  caseOf("open_response", OPEN_RESPONSE_TYPES, decodeOpenResponseValue, encodeOpenResponse),
  caseOf("event", SESSION_EVENT_TYPES, decodeSessionEvent, encodeSessionEvent),
  caseOf("group_change", GROUP_CHANGE_TYPES, decodeGroupChange, encodeGroupChange),
  caseOf("input_outcome", INPUT_OUTCOME_TYPES, decodeInputOutcome, encodeInputOutcome),
  caseOf("send_outcome", SEND_OUTCOME_TYPES, decodeSendOutcome, encodeSendOutcome),
  caseOf("message_intent", MESSAGE_INTENT_TYPES, decodeMessageIntent, encodeMessageIntent),
  caseOf("semantics", SEMANTICS_TYPES, decodeSemantics, encodeSemantics),
  caseOf("drop_reason", DROP_REASONS, decodeDropReason, (value) => value),
  caseOf("signer_op", SIGNER_OP_TYPES, decodeSignerOp, encodeSignerOp),
  caseOf("signer_result", SIGNER_RESULT_TYPES, decodeSignerResult, encodeSignerResult),
  caseOf(
    "attachment_result",
    ATTACHMENT_RESULT_TYPES,
    decodeAttachmentResult,
    encodeAttachmentResult,
  ),
  caseOf("relay_status", RELAY_STATUSES, decodeRelayStatus, (value) => value),
  caseOf("availability_category", AVAILABILITY_VALUES, decodeAvailability, (value) => value),
  caseOf("outbound_purpose", OUTBOUND_PURPOSES, decodeOutboundPurpose, (value) => value),
  caseOf("group_state", GROUP_STATES, decodeGroupState, (value) => value),
  caseOf("group_access", GROUP_ACCESSES, decodeGroupAccess, (value) => value),
  caseOf("local_state", LOCAL_STATES, decodeLocalState, (value) => value),
  caseOf("publish_resolution", PUBLISH_RESOLUTIONS, decodePublishResolution, (value) => value),
];

const ENTRIES = UNION_CASES.flatMap((c) =>
  Object.entries(vector(c.file)).map(([key, raw]) => ({ file: c.file, key, raw, c })),
);

const ERROR_ENTRIES = Object.entries(vector("error")).map(([key, raw]) => ({ key, raw }));

/** Parse `pub const PROTOCOL_VERSION: u32 = N;` out of the mk-bridge source. */
function protocolVersionOf(source: string): number {
  const found = /pub const PROTOCOL_VERSION: u32 = (\d+);/.exec(source);
  if (found === null || found[1] === undefined) {
    throw new Error("PROTOCOL_VERSION not found in protocol.rs");
  }
  return Number(found[1]);
}

describe("bridge wire vectors", () => {
  test("PROTOCOL_VERSION matches mk-bridge", () => {
    expect(protocolVersionOf(readFileSync(PROTOCOL_RS, "utf8"))).toBe(PROTOCOL_VERSION);
  });

  test.each(UNION_CASES)("$file covers every variant tag", (c) => {
    expect(Object.keys(vector(c.file)).toSorted()).toStrictEqual([...c.types].toSorted());
  });

  test.each(ENTRIES)(
    "$file.$key decodes to its key and re-encodes byte-equivalent",
    ({ c, key, raw }) => {
      expect(tagOf(c.decode(raw))).toBe(key);
      expect(c.run(raw)).toStrictEqual(raw);
    },
  );

  test("error covers every kind", () => {
    expect(Object.keys(vector("error")).toSorted()).toStrictEqual([...ERROR_KINDS].toSorted());
  });

  test.each(ERROR_ENTRIES)(
    "error.$key decodes to its kind and re-encodes byte-equivalent",
    ({ key, raw }) => {
      const decoded = decodeWireError(raw);
      expect(decoded.kind).toBe(key);
      expect(encodeWireError(decoded)).toStrictEqual(raw);
    },
  );

  test("config round-trips", () => {
    const parsed: unknown = JSON.parse(readFileSync(join(VECTORS, "config.json"), "utf8"));
    expect(encodeConfigValue(decodeConfigValue(parsed))).toStrictEqual(parsed);
  });

  test("call round-trips", () => {
    const parsed: unknown = JSON.parse(readFileSync(join(VECTORS, "call.json"), "utf8"));
    expect(encodeCallValue(decodeCallValue(parsed))).toStrictEqual(parsed);
  });
});
