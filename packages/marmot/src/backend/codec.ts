/**
 * Hand-written JSON narrowers and encoders for the mk-bridge wire protocol. No `any`, no assertions
 * on decoded data: every value is checked by a narrower that throws `DecodeError` with a fixed
 * `path`/`reason` and never echoes the offending input.
 */

import { DecodeError } from "./errors.ts";
import { hexDecode, hexEncode } from "./hex.ts";
import { match } from "./match.ts";
import type {
  AgentStreamAnchor,
  AgentStreamPolicy,
  AppMessage,
  AttachmentInput,
  AttachmentRejection,
  AttachmentResult,
  BlobEndpoint,
  BridgeConfig,
  BridgeRequest,
  BridgeResponse,
  CallEnvelope,
  ClockReading,
  ComponentUpdate,
  DrainResponse,
  EncryptedMediaPolicy,
  GroupChange,
  GroupSnapshot,
  GroupState,
  InputOutcome,
  InputReport,
  Invitee,
  LocalState,
  Locator,
  MediaAttachmentRef,
  MediaReference,
  MemberSnapshot,
  NostrEvent,
  OpResult,
  OpenResponse,
  Outbound,
  OutboundPurpose,
  PublishResolution,
  ReceivedMessage,
  RelayResult,
  RelayStatus,
  SendOutcome,
  SessionEvent,
  SignerOp,
  SignerReply,
  SignerRequest,
  SignerResult,
  StreamFinal,
  UnsignedEvent,
  WireError,
  WireInputCategory,
} from "./protocol.ts";
import {
  ATTACHMENT_REJECTIONS,
  ERROR_KINDS,
  GROUP_STATES,
  INPUT_CATEGORY_VALUES,
  LOCAL_STATES,
  OUTBOUND_PURPOSES,
  PUBLISH_RESOLUTIONS,
  RELAY_STATUSES,
} from "./protocol.ts";
import { utf8Decode, utf8Encode } from "./utf8.ts";

/** A plain JSON value: what encoders produce before `JSON.stringify` — `null` is a wire value. */
// oxlint-disable-next-line no-restricted-types
export type Json = null | boolean | number | string | Json[] | { [key: string]: Json };

function fail(path: string, reason: string): never {
  throw new DecodeError(path, reason);
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function rec(value: unknown, path: string, keys: ReadonlyArray<string>): Record<string, unknown> {
  if (!isRecord(value)) {
    fail(path, "object");
  }
  for (const key of Object.keys(value)) {
    if (!keys.includes(key)) {
      fail(`${path}.${key}`, "unknown_field");
    }
  }
  return value;
}

function field(object: Record<string, unknown>, key: string, path: string): unknown {
  if (!(key in object)) {
    fail(`${path}.${key}`, "missing");
  }
  return object[key];
}

function str(value: unknown, path: string): string {
  if (typeof value !== "string") {
    fail(path, "string");
  }
  return value;
}

function bool(value: unknown, path: string): boolean {
  if (typeof value !== "boolean") {
    fail(path, "boolean");
  }
  return value;
}

function uint(value: unknown, path: string): number {
  if (typeof value !== "number" || !Number.isSafeInteger(value) || value < 0) {
    fail(path, "number");
  }
  return value;
}

const DECIMAL = /^[0-9]+$/;

/** `u64_str`: a decimal string with no sign or padding characters. */
function u64(value: unknown, path: string): bigint {
  if (typeof value !== "string" || !DECIMAL.test(value)) {
    fail(path, "number");
  }
  return BigInt(value);
}

function optU64(value: unknown, path: string): bigint | undefined {
  if (value === null || value === undefined) {
    return undefined;
  }
  return u64(value, path);
}

function hex(value: unknown, path: string): Uint8Array {
  if (typeof value !== "string") {
    fail(path, "hex");
  }
  return hexDecode(value, path);
}

/** A fixed-length hex identifier kept as a validated lowercase string. */
function hexN(value: unknown, path: string, bytes: number): string {
  const text = str(value, path);
  if (text.length !== bytes * 2) {
    fail(path, "hex");
  }
  hexDecode(text, path);
  return text;
}

function hex32(value: unknown, path: string): string {
  return hexN(value, path, 32);
}

function hex64(value: unknown, path: string): string {
  return hexN(value, path, 64);
}

function optHex32(value: unknown, path: string): string | undefined {
  if (value === null || value === undefined) {
    return undefined;
  }
  return hex32(value, path);
}

function optStr(value: unknown, path: string): string | undefined {
  if (value === null || value === undefined) {
    return undefined;
  }
  return str(value, path);
}

function arr<T>(value: unknown, path: string, narrow: (item: unknown, itemPath: string) => T): T[] {
  if (!Array.isArray(value)) {
    fail(path, "array");
  }
  return value.map((item, index) => narrow(item, `${path}[${index}]`));
}

function strList(value: unknown, path: string): string[] {
  return arr(value, path, str);
}

function tags(value: unknown, path: string): string[][] {
  return arr(value, path, (item, itemPath) => strList(item, itemPath));
}

function oneOf<T extends string>(value: unknown, path: string, names: ReadonlyArray<T>): T {
  const text = str(value, path);
  const hit = names.find((name) => name === text);
  if (hit === undefined) {
    fail(path, "value");
  }
  return hit;
}

function tag(object: Record<string, unknown>, path: string): string {
  return str(field(object, "type", path), `${path}.type`);
}

/* ---- shared value types ---- */

const TIME_KEYS = ["unix_ms", "monotonic_ms"];

export function decodeTime(value: unknown, path = "now"): ClockReading {
  const o = rec(value, path, TIME_KEYS);
  return {
    unixMs: u64(field(o, "unix_ms", path), `${path}.unix_ms`),
    monotonicMs: u64(field(o, "monotonic_ms", path), `${path}.monotonic_ms`),
  };
}

export function encodeTime(reading: ClockReading): Json {
  return { unix_ms: reading.unixMs.toString(), monotonic_ms: reading.monotonicMs.toString() };
}

const EVENT_KEYS = ["id", "pubkey", "created_at", "kind", "tags", "content", "sig"];

export function decodeEvent(value: unknown, path = "event"): NostrEvent {
  const o = rec(value, path, EVENT_KEYS);
  return {
    id: hex32(field(o, "id", path), `${path}.id`),
    pubkey: hex32(field(o, "pubkey", path), `${path}.pubkey`),
    createdAt: u64(field(o, "created_at", path), `${path}.created_at`),
    kind: uint(field(o, "kind", path), `${path}.kind`),
    tags: tags(field(o, "tags", path), `${path}.tags`),
    content: str(field(o, "content", path), `${path}.content`),
    sig: hex64(field(o, "sig", path), `${path}.sig`),
  };
}

export function encodeEvent(event: NostrEvent): Json {
  return {
    id: event.id,
    pubkey: event.pubkey,
    created_at: event.createdAt.toString(),
    kind: event.kind,
    tags: event.tags,
    content: event.content,
    sig: event.sig,
  };
}

const UNSIGNED_KEYS = ["pubkey", "created_at", "kind", "tags", "content"];

export function decodeUnsignedEvent(value: unknown, path = "unsigned"): UnsignedEvent {
  const o = rec(value, path, UNSIGNED_KEYS);
  return {
    pubkey: hex32(field(o, "pubkey", path), `${path}.pubkey`),
    createdAt: u64(field(o, "created_at", path), `${path}.created_at`),
    kind: uint(field(o, "kind", path), `${path}.kind`),
    tags: tags(field(o, "tags", path), `${path}.tags`),
    content: str(field(o, "content", path), `${path}.content`),
  };
}

export function encodeUnsignedEvent(event: UnsignedEvent): Json {
  return {
    pubkey: event.pubkey,
    created_at: event.createdAt.toString(),
    kind: event.kind,
    tags: event.tags,
    content: event.content,
  };
}

const LOCATOR_KEYS = ["kind", "value"];

function decodeLocator(value: unknown, path: string): Locator {
  const o = rec(value, path, LOCATOR_KEYS);
  return {
    kind: str(field(o, "kind", path), `${path}.kind`),
    value: str(field(o, "value", path), `${path}.value`),
  };
}

function encodeLocator(locator: Locator): Json {
  return { kind: locator.kind, value: locator.value };
}

const REFERENCE_KEYS = [
  "locators",
  "ciphertext_sha256",
  "plaintext_sha256",
  "nonce",
  "media_type",
  "filename",
  "dim",
  "thumbhash",
];

export function decodeMediaReference(value: unknown, path = "reference"): MediaReference {
  const o = rec(value, path, REFERENCE_KEYS);
  return {
    locators: arr(field(o, "locators", path), `${path}.locators`, decodeLocator),
    ciphertextSha256: hex32(field(o, "ciphertext_sha256", path), `${path}.ciphertext_sha256`),
    plaintextSha256: hex32(field(o, "plaintext_sha256", path), `${path}.plaintext_sha256`),
    nonce: hexN(field(o, "nonce", path), `${path}.nonce`, 12),
    mediaType: str(field(o, "media_type", path), `${path}.media_type`),
    filename: str(field(o, "filename", path), `${path}.filename`),
    dim: optStr(o["dim"], `${path}.dim`),
    thumbhash: optStr(o["thumbhash"], `${path}.thumbhash`),
  };
}

export function encodeMediaReference(reference: MediaReference): Json {
  return {
    locators: reference.locators.map(encodeLocator),
    ciphertext_sha256: reference.ciphertextSha256,
    plaintext_sha256: reference.plaintextSha256,
    nonce: reference.nonce,
    media_type: reference.mediaType,
    filename: reference.filename,
    dim: reference.dim ?? null,
    thumbhash: reference.thumbhash ?? null,
  };
}

export function decodeAttachmentRejection(value: unknown, path = "reason"): AttachmentRejection {
  return oneOf(value, path, ATTACHMENT_REJECTIONS);
}

export function decodeAttachmentResult(value: unknown, path = "attachment"): AttachmentResult {
  const o = rec(value, path, ["type", "reference", "reason"]);
  switch (tag(o, path)) {
    case "ok":
      return {
        type: "ok",
        reference: decodeMediaReference(field(o, "reference", path), `${path}.reference`),
      };
    case "rejected":
      return {
        type: "rejected",
        reason: decodeAttachmentRejection(field(o, "reason", path), `${path}.reason`),
      };
    default:
      return fail(`${path}.type`, "value");
  }
}

export function encodeAttachmentResult(result: AttachmentResult): Json {
  return match(result, {
    ok: (result) => ({ type: "ok", reference: encodeMediaReference(result.reference) }),
    rejected: (result) => ({ type: "rejected", reason: result.reason }),
  });
}

const ENDPOINT_KEYS = ["locator_kind", "base_url"];

function decodeBlobEndpoint(value: unknown, path: string): BlobEndpoint {
  const o = rec(value, path, ENDPOINT_KEYS);
  return {
    locatorKind: str(field(o, "locator_kind", path), `${path}.locator_kind`),
    baseUrl: str(field(o, "base_url", path), `${path}.base_url`),
  };
}

function encodeBlobEndpoint(endpoint: BlobEndpoint): Json {
  return { locator_kind: endpoint.locatorKind, base_url: endpoint.baseUrl };
}

const MEDIA_POLICY_KEYS = ["allowed_locator_kinds", "endpoints"];

function decodeMediaPolicy(value: unknown, path: string): EncryptedMediaPolicy {
  const o = rec(value, path, MEDIA_POLICY_KEYS);
  return {
    allowedLocatorKinds: strList(
      field(o, "allowed_locator_kinds", path),
      `${path}.allowed_locator_kinds`,
    ),
    endpoints: arr(field(o, "endpoints", path), `${path}.endpoints`, decodeBlobEndpoint),
  };
}

function encodeMediaPolicy(policy: EncryptedMediaPolicy): Json {
  return {
    allowed_locator_kinds: policy.allowedLocatorKinds,
    endpoints: policy.endpoints.map(encodeBlobEndpoint),
  };
}

const STREAM_POLICY_KEYS = [
  "required_roles",
  "allowed_roles",
  "max_plaintext_frame_len",
  "replay_ttl_secs",
];

function decodeStreamPolicy(value: unknown, path: string): AgentStreamPolicy {
  const o = rec(value, path, STREAM_POLICY_KEYS);
  return {
    requiredRoles: uint(field(o, "required_roles", path), `${path}.required_roles`),
    allowedRoles: uint(field(o, "allowed_roles", path), `${path}.allowed_roles`),
    maxPlaintextFrameLen: uint(
      field(o, "max_plaintext_frame_len", path),
      `${path}.max_plaintext_frame_len`,
    ),
    replayTtlSecs: uint(field(o, "replay_ttl_secs", path), `${path}.replay_ttl_secs`),
  };
}

function encodeStreamPolicy(policy: AgentStreamPolicy): Json {
  return {
    required_roles: policy.requiredRoles,
    allowed_roles: policy.allowedRoles,
    max_plaintext_frame_len: policy.maxPlaintextFrameLen,
    replay_ttl_secs: policy.replayTtlSecs,
  };
}

const ATTACHMENT_INPUT_KEYS = ["plaintext", "media_type", "filename", "dim", "thumbhash"];

export function decodeAttachmentInput(value: unknown, path = "attachment"): AttachmentInput {
  const o = rec(value, path, ATTACHMENT_INPUT_KEYS);
  return {
    plaintext: hex(field(o, "plaintext", path), `${path}.plaintext`),
    mediaType: str(field(o, "media_type", path), `${path}.media_type`),
    filename: str(field(o, "filename", path), `${path}.filename`),
    dim: optStr(o["dim"], `${path}.dim`),
    thumbhash: optStr(o["thumbhash"], `${path}.thumbhash`),
  };
}

export function encodeAttachmentInput(input: AttachmentInput): Json {
  return {
    plaintext: hexEncode(input.plaintext),
    media_type: input.mediaType,
    filename: input.filename,
    dim: input.dim ?? null,
    thumbhash: input.thumbhash ?? null,
  };
}

const INVITEE_KEYS = ["key_package", "inbox_relays"];

export function decodeInvitee(value: unknown, path = "invitee"): Invitee {
  const o = rec(value, path, INVITEE_KEYS);
  return {
    keyPackage: decodeEvent(field(o, "key_package", path), `${path}.key_package`),
    inboxRelays: strList(field(o, "inbox_relays", path), `${path}.inbox_relays`),
  };
}

export function encodeInvitee(invitee: Invitee): Json {
  return {
    key_package: encodeEvent(invitee.keyPackage),
    inbox_relays: invitee.inboxRelays,
  };
}

const COMPONENT_KEYS = ["component_id", "data"];

export function decodeComponentUpdate(value: unknown, path = "update"): ComponentUpdate {
  const o = rec(value, path, COMPONENT_KEYS);
  return {
    componentId: uint(field(o, "component_id", path), `${path}.component_id`),
    data: hex(field(o, "data", path), `${path}.data`),
  };
}

export function encodeComponentUpdate(update: ComponentUpdate): Json {
  return { component_id: update.componentId, data: hexEncode(update.data) };
}

const APP_MESSAGE_KEYS = ["kind", "tags", "content"];

export function decodeAppMessage(value: unknown, path = "message"): AppMessage {
  const o = rec(value, path, APP_MESSAGE_KEYS);
  return {
    kind: uint(field(o, "kind", path), `${path}.kind`),
    tags: tags(field(o, "tags", path), `${path}.tags`),
    content: str(field(o, "content", path), `${path}.content`),
  };
}

export function encodeAppMessage(message: AppMessage): Json {
  return { kind: message.kind, tags: message.tags, content: message.content };
}

const MEDIA_REF_KEYS = ["handle", "locators"];

export function decodeMediaAttachmentRef(value: unknown, path = "attachment"): MediaAttachmentRef {
  const o = rec(value, path, MEDIA_REF_KEYS);
  return {
    handle: uint(field(o, "handle", path), `${path}.handle`),
    locators: arr(field(o, "locators", path), `${path}.locators`, decodeLocator),
  };
}

export function encodeMediaAttachmentRef(ref: MediaAttachmentRef): Json {
  return { handle: ref.handle, locators: ref.locators.map(encodeLocator) };
}

const RELAY_RESULT_KEYS = ["relay", "status"];

export function decodeRelayResult(value: unknown, path = "result"): RelayResult {
  const o = rec(value, path, RELAY_RESULT_KEYS);
  return {
    relay: str(field(o, "relay", path), `${path}.relay`),
    status: oneOf(field(o, "status", path), `${path}.status`, RELAY_STATUSES),
  };
}

export function encodeRelayResult(result: RelayResult): Json {
  return { relay: result.relay, status: result.status };
}

export function decodeRelayStatus(value: unknown, path = "status"): RelayStatus {
  return oneOf(value, path, RELAY_STATUSES);
}

export function decodePublishResolution(value: unknown, path = "resolution"): PublishResolution {
  return oneOf(value, path, PUBLISH_RESOLUTIONS);
}

export function decodeOutboundPurpose(value: unknown, path = "purpose"): OutboundPurpose {
  return oneOf(value, path, OUTBOUND_PURPOSES);
}

const OUTBOUND_KEYS = ["id", "event", "relays", "purpose"];

export function decodeOutbound(value: unknown, path = "outbound"): Outbound {
  const o = rec(value, path, OUTBOUND_KEYS);
  return {
    id: hex32(field(o, "id", path), `${path}.id`),
    event: decodeEvent(field(o, "event", path), `${path}.event`),
    relays: strList(field(o, "relays", path), `${path}.relays`),
    purpose: decodeOutboundPurpose(field(o, "purpose", path), `${path}.purpose`),
  };
}

export function encodeOutbound(outbound: Outbound): Json {
  return {
    id: outbound.id,
    event: encodeEvent(outbound.event),
    relays: outbound.relays,
    purpose: outbound.purpose,
  };
}

export function decodeGroupState(value: unknown, path = "state"): GroupState {
  return oneOf(value, path, GROUP_STATES);
}

export function decodeLocalState(value: unknown, path = "state"): LocalState {
  return oneOf(value, path, LOCAL_STATES);
}

export function decodeInputCategory(value: unknown, path = "category"): WireInputCategory {
  return oneOf(value, path, INPUT_CATEGORY_VALUES);
}

export function decodeInputOutcome(value: unknown, path = "outcome"): InputOutcome {
  const o = rec(value, path, ["type", "category", "state"]);
  const type = tag(o, path);
  switch (type) {
    case "deferred": {
      const raw = field(o, "category", path);
      return {
        type,
        category:
          raw === null || raw === undefined
            ? undefined
            : decodeInputCategory(raw, `${path}.category`),
      };
    }
    case "stale":
      return {
        type,
        category: decodeInputCategory(field(o, "category", path), `${path}.category`),
      };
    case "rejected":
      return {
        type,
        category: decodeInputCategory(field(o, "category", path), `${path}.category`),
      };
    case "unavailable":
      return {
        type,
        category: decodeInputCategory(field(o, "category", path), `${path}.category`),
      };
    case "local":
      return {
        type: "local",
        state: decodeLocalState(field(o, "state", path), `${path}.state`),
      };
    case "accepted":
    case "recovery_offered":
    case "unmapped":
      return { type };
    default:
      return fail(`${path}.type`, "value");
  }
}

export function encodeInputOutcome(outcome: InputOutcome): Json {
  return match(outcome, {
    deferred: (outcome) => ({ type: "deferred", category: outcome.category ?? null }),
    stale: (outcome) => ({ type: outcome.type, category: outcome.category }),
    rejected: (outcome) => ({ type: outcome.type, category: outcome.category }),
    unavailable: (outcome) => ({ type: outcome.type, category: outcome.category }),
    local: (outcome) => ({ type: "local", state: outcome.state }),
    accepted: (outcome) => ({ type: outcome.type }),
    recovery_offered: (outcome) => ({ type: outcome.type }),
    unmapped: (outcome) => ({ type: outcome.type }),
  });
}

const INPUT_REPORT_KEYS = ["event", "outcome"];

export function decodeInputReport(value: unknown, path = "report"): InputReport {
  const o = rec(value, path, INPUT_REPORT_KEYS);
  return {
    event: hex32(field(o, "event", path), `${path}.event`),
    outcome: decodeInputOutcome(field(o, "outcome", path), `${path}.outcome`),
  };
}

export function encodeInputReport(report: InputReport): Json {
  return { event: report.event, outcome: encodeInputOutcome(report.outcome) };
}

const STREAM_FINAL_KEYS = ["stream_id", "start_event_id", "transcript_hash", "chunk_count"];

function decodeStreamFinal(value: unknown, path: string): StreamFinal {
  const o = rec(value, path, STREAM_FINAL_KEYS);
  return {
    streamId: hex32(field(o, "stream_id", path), `${path}.stream_id`),
    startEventId: hex32(field(o, "start_event_id", path), `${path}.start_event_id`),
    transcriptHash: hex32(field(o, "transcript_hash", path), `${path}.transcript_hash`),
    chunkCount: u64(field(o, "chunk_count", path), `${path}.chunk_count`),
  };
}

function encodeStreamFinal(final: StreamFinal): Json {
  return {
    stream_id: final.streamId,
    start_event_id: final.startEventId,
    transcript_hash: final.transcriptHash,
    chunk_count: final.chunkCount.toString(),
  };
}

const MESSAGE_KEYS = [
  "id",
  "sender",
  "created_at",
  "kind",
  "tags",
  "content",
  "epoch",
  "transport_id",
  "attachments",
  "stream_final",
];

export function decodeReceivedMessage(value: unknown, path = "message"): ReceivedMessage {
  const o = rec(value, path, MESSAGE_KEYS);
  const streamFinal = field(o, "stream_final", path);
  return {
    id: str(field(o, "id", path), `${path}.id`),
    sender: hex32(field(o, "sender", path), `${path}.sender`),
    createdAt: u64(field(o, "created_at", path), `${path}.created_at`),
    kind: u64(field(o, "kind", path), `${path}.kind`),
    tags: tags(field(o, "tags", path), `${path}.tags`),
    content: str(field(o, "content", path), `${path}.content`),
    epoch: u64(field(o, "epoch", path), `${path}.epoch`),
    transportId: hex32(field(o, "transport_id", path), `${path}.transport_id`),
    attachments: arr(field(o, "attachments", path), `${path}.attachments`, decodeAttachmentResult),
    streamFinal:
      streamFinal === null || streamFinal === undefined
        ? undefined
        : decodeStreamFinal(streamFinal, `${path}.stream_final`),
  };
}

export function encodeReceivedMessage(message: ReceivedMessage): Json {
  return {
    id: message.id,
    sender: message.sender,
    created_at: message.createdAt.toString(),
    kind: message.kind.toString(),
    tags: message.tags,
    content: message.content,
    epoch: message.epoch.toString(),
    transport_id: message.transportId,
    attachments: message.attachments.map(encodeAttachmentResult),
    stream_final: message.streamFinal === undefined ? null : encodeStreamFinal(message.streamFinal),
  };
}

const ANCHOR_KEYS = ["id", "sender", "created_at", "epoch", "transport_id", "stream_id", "parent"];

export function decodeAgentStreamAnchor(value: unknown, path = "start"): AgentStreamAnchor {
  const o = rec(value, path, ANCHOR_KEYS);
  return {
    id: str(field(o, "id", path), `${path}.id`),
    sender: hex32(field(o, "sender", path), `${path}.sender`),
    createdAt: u64(field(o, "created_at", path), `${path}.created_at`),
    epoch: u64(field(o, "epoch", path), `${path}.epoch`),
    transportId: hex32(field(o, "transport_id", path), `${path}.transport_id`),
    streamId: hex32(field(o, "stream_id", path), `${path}.stream_id`),
    parent: optHex32(o["parent"], `${path}.parent`),
  };
}

export function encodeAgentStreamAnchor(anchor: AgentStreamAnchor): Json {
  return {
    id: anchor.id,
    sender: anchor.sender,
    created_at: anchor.createdAt.toString(),
    epoch: anchor.epoch.toString(),
    transport_id: anchor.transportId,
    stream_id: anchor.streamId,
    parent: anchor.parent ?? null,
  };
}

export function decodeGroupChange(value: unknown, path = "change"): GroupChange {
  const o = rec(value, path, [
    "type",
    "account",
    "old_name",
    "new_name",
    "old_seconds",
    "new_seconds",
  ]);
  const type = tag(o, path);
  switch (type) {
    case "member_added":
    case "member_removed":
    case "member_left":
    case "admin_added":
    case "admin_removed":
      return { type, account: hex32(field(o, "account", path), `${path}.account`) };
    case "renamed":
      return {
        type,
        oldName: str(field(o, "old_name", path), `${path}.old_name`),
        newName: str(field(o, "new_name", path), `${path}.new_name`),
      };
    case "retention_changed":
      return {
        type,
        oldSeconds: u64(field(o, "old_seconds", path), `${path}.old_seconds`),
        newSeconds: u64(field(o, "new_seconds", path), `${path}.new_seconds`),
      };
    case "avatar_changed":
    case "disbanded":
    case "unmapped":
      return { type };
    default:
      return fail(`${path}.type`, "value");
  }
}

export function encodeGroupChange(change: GroupChange): Json {
  return match(change, {
    member_added: (change) => ({ type: change.type, account: change.account }),
    member_removed: (change) => ({ type: change.type, account: change.account }),
    member_left: (change) => ({ type: change.type, account: change.account }),
    admin_added: (change) => ({ type: change.type, account: change.account }),
    admin_removed: (change) => ({ type: change.type, account: change.account }),
    renamed: (change) => ({ type: "renamed", old_name: change.oldName, new_name: change.newName }),
    retention_changed: (change) => ({
      type: "retention_changed",
      old_seconds: change.oldSeconds.toString(),
      new_seconds: change.newSeconds.toString(),
    }),
    avatar_changed: (change) => ({ type: change.type }),
    disbanded: (change) => ({ type: change.type }),
    unmapped: (change) => ({ type: change.type }),
  });
}

const SESSION_EVENT_KEYS = [
  "type",
  "group",
  "message",
  "start",
  "epoch",
  "from",
  "to",
  "actor",
  "change",
  "origin_commit",
  "commit",
  "voluntary",
  "intent",
  "intent_kind",
  "reason",
  "outbound",
  "resolution",
];

export function decodeSessionEvent(value: unknown, path = "event"): SessionEvent {
  const o = rec(value, path, SESSION_EVENT_KEYS);
  const group = (p: string) => hexEncode(hex(field(o, "group", p), `${p}.group`));
  const type = tag(o, path);
  switch (type) {
    case "message_received":
      return {
        type,
        group: group(path),
        message: decodeReceivedMessage(field(o, "message", path), `${path}.message`),
      };
    case "agent_stream_started":
      return {
        type,
        group: group(path),
        start: decodeAgentStreamAnchor(field(o, "start", path), `${path}.start`),
      };
    case "message_invalidated":
      return {
        type,
        group: group(path),
        message: hex32(field(o, "message", path), `${path}.message`),
        epoch: u64(field(o, "epoch", path), `${path}.epoch`),
      };
    case "group_created":
    case "group_joined":
    case "local_copy_restored":
    case "hydration_quarantined":
    case "group_unrecoverable":
      return { type, group: group(path) };
    case "epoch_changed":
      return {
        type,
        group: group(path),
        from: u64(field(o, "from", path), `${path}.from`),
        to: u64(field(o, "to", path), `${path}.to`),
      };
    case "group_state_changed":
      return {
        type,
        group: group(path),
        epoch: u64(field(o, "epoch", path), `${path}.epoch`),
        actor: optHex32(o["actor"], `${path}.actor`),
        change: decodeGroupChange(field(o, "change", path), `${path}.change`),
        originCommit: optHex32(o["origin_commit"], `${path}.origin_commit`),
      };
    case "group_state_invalidated":
    case "group_state_revalidated":
      return {
        type,
        group: group(path),
        epoch: u64(field(o, "epoch", path), `${path}.epoch`),
        commit: hex32(field(o, "commit", path), `${path}.commit`),
      };
    case "local_copy_terminated":
      return {
        type,
        group: group(path),
        voluntary: bool(field(o, "voluntary", path), `${path}.voluntary`),
      };
    case "queued_intent_refused":
      return {
        type,
        group: group(path),
        intent: hex32(field(o, "intent", path), `${path}.intent`),
        intentKind: str(field(o, "intent_kind", path), `${path}.intent_kind`),
        reason: str(field(o, "reason", path), `${path}.reason`),
      };
    case "hydration_recovered":
    case "pending_commit_recovered":
      return {
        type,
        group: group(path),
        epoch: u64(field(o, "epoch", path), `${path}.epoch`),
      };
    case "publish_resolved":
      return {
        type,
        outbound: hex32(field(o, "outbound", path), `${path}.outbound`),
        resolution: decodePublishResolution(field(o, "resolution", path), `${path}.resolution`),
      };
    case "unmapped":
      return { type };
    default:
      return fail(`${path}.type`, "value");
  }
}

export function encodeSessionEvent(event: SessionEvent): Json {
  return match(event, {
    message_received: (event) => ({
      type: "message_received",
      group: event.group,
      message: encodeReceivedMessage(event.message),
    }),
    agent_stream_started: (event) => ({
      type: "agent_stream_started",
      group: event.group,
      start: encodeAgentStreamAnchor(event.start),
    }),
    message_invalidated: (event) => ({
      type: "message_invalidated",
      group: event.group,
      message: event.message,
      epoch: event.epoch.toString(),
    }),
    group_created: (event) => ({ type: event.type, group: event.group }),
    group_joined: (event) => ({ type: event.type, group: event.group }),
    local_copy_restored: (event) => ({ type: event.type, group: event.group }),
    hydration_quarantined: (event) => ({ type: event.type, group: event.group }),
    group_unrecoverable: (event) => ({ type: event.type, group: event.group }),
    epoch_changed: (event) => ({
      type: "epoch_changed",
      group: event.group,
      from: event.from.toString(),
      to: event.to.toString(),
    }),
    group_state_changed: (event) => ({
      type: "group_state_changed",
      group: event.group,
      epoch: event.epoch.toString(),
      actor: event.actor ?? null,
      change: encodeGroupChange(event.change),
      origin_commit: event.originCommit ?? null,
    }),
    group_state_invalidated: (event) => ({
      type: event.type,
      group: event.group,
      epoch: event.epoch.toString(),
      commit: event.commit,
    }),
    group_state_revalidated: (event) => ({
      type: event.type,
      group: event.group,
      epoch: event.epoch.toString(),
      commit: event.commit,
    }),
    local_copy_terminated: (event) => ({
      type: "local_copy_terminated",
      group: event.group,
      voluntary: event.voluntary,
    }),
    queued_intent_refused: (event) => ({
      type: "queued_intent_refused",
      group: event.group,
      intent: event.intent,
      intent_kind: event.intentKind,
      reason: event.reason,
    }),
    hydration_recovered: (event) => ({
      type: event.type,
      group: event.group,
      epoch: event.epoch.toString(),
    }),
    pending_commit_recovered: (event) => ({
      type: event.type,
      group: event.group,
      epoch: event.epoch.toString(),
    }),
    publish_resolved: (event) => ({
      type: "publish_resolved",
      outbound: event.outbound,
      resolution: event.resolution,
    }),
    unmapped: () => ({ type: "unmapped" }),
  });
}

const MEMBER_KEYS = ["account", "admin"];

function decodeMember(value: unknown, path: string): MemberSnapshot {
  const o = rec(value, path, MEMBER_KEYS);
  return {
    account: hex32(field(o, "account", path), `${path}.account`),
    admin: bool(field(o, "admin", path), `${path}.admin`),
  };
}

function encodeMember(member: MemberSnapshot): Json {
  return { account: member.account, admin: member.admin };
}

const SNAPSHOT_KEYS = [
  "id",
  "name",
  "description",
  "epoch",
  "state",
  "members",
  "relays",
  "nostr_group_id",
  "own_account_admin",
  "encrypted_media",
  "agent_text_stream",
];

export function decodeGroupSnapshot(value: unknown, path = "group"): GroupSnapshot {
  const o = rec(value, path, SNAPSHOT_KEYS);
  const media = field(o, "encrypted_media", path);
  const stream = field(o, "agent_text_stream", path);
  return {
    id: hexEncode(hex(field(o, "id", path), `${path}.id`)),
    name: str(field(o, "name", path), `${path}.name`),
    description: str(field(o, "description", path), `${path}.description`),
    epoch: u64(field(o, "epoch", path), `${path}.epoch`),
    state: decodeGroupState(field(o, "state", path), `${path}.state`),
    members: arr(field(o, "members", path), `${path}.members`, decodeMember),
    relays: strList(field(o, "relays", path), `${path}.relays`),
    nostrGroupId: hex32(field(o, "nostr_group_id", path), `${path}.nostr_group_id`),
    ownAccountAdmin: bool(field(o, "own_account_admin", path), `${path}.own_account_admin`),
    encryptedMedia:
      media === null || media === undefined
        ? undefined
        : decodeMediaPolicy(media, `${path}.encrypted_media`),
    agentTextStream:
      stream === null || stream === undefined
        ? undefined
        : decodeStreamPolicy(stream, `${path}.agent_text_stream`),
  };
}

export function encodeGroupSnapshot(snapshot: GroupSnapshot): Json {
  return {
    id: snapshot.id,
    name: snapshot.name,
    description: snapshot.description,
    epoch: snapshot.epoch.toString(),
    state: snapshot.state,
    members: snapshot.members.map(encodeMember),
    relays: snapshot.relays,
    nostr_group_id: snapshot.nostrGroupId,
    own_account_admin: snapshot.ownAccountAdmin,
    encrypted_media:
      snapshot.encryptedMedia === undefined ? null : encodeMediaPolicy(snapshot.encryptedMedia),
    agent_text_stream:
      snapshot.agentTextStream === undefined ? null : encodeStreamPolicy(snapshot.agentTextStream),
  };
}

export function decodeSignerOp(value: unknown, path = "op"): SignerOp {
  const o = rec(value, path, ["type", "unsigned", "peer", "plaintext", "ciphertext"]);
  const type = tag(o, path);
  switch (type) {
    case "sign_event":
      return {
        type,
        unsigned: decodeUnsignedEvent(field(o, "unsigned", path), `${path}.unsigned`),
      };
    case "nip44_encrypt":
      return {
        type,
        peer: hex32(field(o, "peer", path), `${path}.peer`),
        plaintext: str(field(o, "plaintext", path), `${path}.plaintext`),
      };
    case "nip44_decrypt":
      return {
        type,
        peer: hex32(field(o, "peer", path), `${path}.peer`),
        ciphertext: str(field(o, "ciphertext", path), `${path}.ciphertext`),
      };
    default:
      return fail(`${path}.type`, "value");
  }
}

export function encodeSignerOp(op: SignerOp): Json {
  return match(op, {
    sign_event: (op) => ({ type: "sign_event", unsigned: encodeUnsignedEvent(op.unsigned) }),
    nip44_encrypt: (op) => ({ type: "nip44_encrypt", peer: op.peer, plaintext: op.plaintext }),
    nip44_decrypt: (op) => ({
      type: "nip44_decrypt",
      peer: op.peer,
      ciphertext: op.ciphertext,
    }),
  });
}

const SIGNER_REQUEST_KEYS = ["id", "op"];

export function decodeSignerRequest(value: unknown, path = "request"): SignerRequest {
  const o = rec(value, path, SIGNER_REQUEST_KEYS);
  return {
    id: uint(field(o, "id", path), `${path}.id`),
    op: decodeSignerOp(field(o, "op", path), `${path}.op`),
  };
}

export function encodeSignerRequest(request: SignerRequest): Json {
  return { id: request.id, op: encodeSignerOp(request.op) };
}

export function decodeSignerResult(value: unknown, path = "result"): SignerResult {
  const o = rec(value, path, ["type", "event", "text"]);
  const type = tag(o, path);
  switch (type) {
    case "signed":
      return { type, event: decodeEvent(field(o, "event", path), `${path}.event`) };
    case "text":
      return { type, text: str(field(o, "text", path), `${path}.text`) };
    case "refused":
    case "failed":
      return { type };
    default:
      return fail(`${path}.type`, "value");
  }
}

export function encodeSignerResult(result: SignerResult): Json {
  return match(result, {
    signed: (result) => ({ type: "signed", event: encodeEvent(result.event) }),
    text: (result) => ({ type: "text", text: result.text }),
    refused: (result) => ({ type: result.type }),
    failed: (result) => ({ type: result.type }),
  });
}

const SIGNER_REPLY_KEYS = ["id", "result"];

export function decodeSignerReply(value: unknown, path = "reply"): SignerReply {
  const o = rec(value, path, SIGNER_REPLY_KEYS);
  return {
    id: uint(field(o, "id", path), `${path}.id`),
    result: decodeSignerResult(field(o, "result", path), `${path}.result`),
  };
}

export function encodeSignerReply(reply: SignerReply): Json {
  return { id: reply.id, result: encodeSignerResult(reply.result) };
}

export function decodeSendOutcome(value: unknown, path = "outcome"): SendOutcome {
  const o = rec(value, path, ["type", "ids"]);
  const type = tag(o, path);
  switch (type) {
    case "staged":
      return {
        type,
        ids: arr(field(o, "ids", path), `${path}.ids`, (item, p) => hex32(item, p)),
      };
    case "queued":
    case "no_change":
    case "leave_requested":
    case "unmapped":
      return { type };
    default:
      return fail(`${path}.type`, "value");
  }
}

export function encodeSendOutcome(outcome: SendOutcome): Json {
  return match(outcome, {
    staged: (outcome) => ({ type: "staged", ids: outcome.ids }),
    queued: (outcome) => ({ type: outcome.type }),
    no_change: (outcome) => ({ type: outcome.type }),
    leave_requested: (outcome) => ({ type: outcome.type }),
    unmapped: (outcome) => ({ type: outcome.type }),
  });
}

export function decodeWireError(value: unknown, path = "error"): WireError {
  const o = rec(value, path, [
    "kind",
    "detail",
    "site",
    "field",
    "reason",
    "expected",
    "got",
    "required",
    "limit",
  ]);
  const kind = oneOf(field(o, "kind", path), `${path}.kind`, ERROR_KINDS);
  switch (kind) {
    case "engine":
      return { kind, detail: str(field(o, "detail", path), `${path}.detail`) };
    case "internal":
      return { kind, site: str(field(o, "site", path), `${path}.site`) };
    case "invalid_input":
      return { kind, field: str(field(o, "field", path), `${path}.field`) };
    case "invalid_request":
    case "media":
    case "proof_rejected":
    case "signer":
      return { kind, reason: str(field(o, "reason", path), `${path}.reason`) };
    case "open_failed":
      return {
        kind,
        reason: str(field(o, "reason", path), `${path}.reason`),
        detail: str(field(o, "detail", path), `${path}.detail`),
      };
    case "protocol_version_mismatch":
      return {
        kind,
        expected: uint(field(o, "expected", path), `${path}.expected`),
        got: uint(field(o, "got", path), `${path}.got`),
      };
    case "unsupported_ciphersuite":
      return {
        kind,
        required: uint(field(o, "required", path), `${path}.required`),
        got: uint(field(o, "got", path), `${path}.got`),
      };
    case "too_large":
      return { kind, limit: u64(field(o, "limit", path), `${path}.limit`) };
    default:
      return { kind };
  }
}

export function encodeWireError(error: WireError): Json {
  switch (error.kind) {
    case "engine":
      return { kind: "engine", detail: error.detail };
    case "internal":
      return { kind: "internal", site: error.site };
    case "invalid_input":
      return { kind: "invalid_input", field: error.field };
    case "invalid_request":
    case "media":
    case "proof_rejected":
    case "signer":
      return { kind: error.kind, reason: error.reason };
    case "open_failed":
      return { kind: "open_failed", reason: error.reason, detail: error.detail };
    case "protocol_version_mismatch":
      return {
        kind: "protocol_version_mismatch",
        expected: error.expected,
        got: error.got,
      };
    case "unsupported_ciphersuite":
      return {
        kind: "unsupported_ciphersuite",
        required: error.required,
        got: error.got,
      };
    case "too_large":
      return { kind: "too_large", limit: error.limit.toString() };
    default:
      return { kind: error.kind };
  }
}

export function decodeRequest(value: unknown, path = "request"): BridgeRequest {
  const o = rec(value, path, [
    "type",
    "event",
    "seq",
    "id",
    "results",
    "relays",
    "name",
    "description",
    "invitees",
    "admins",
    "components",
    "group",
    "members",
    "updates",
    "message",
    "attachment",
    "caption",
    "attachments",
    "tags",
    "handle",
    "reference",
    "ciphertext",
    "events",
    "ids",
    "replies",
  ]);
  const type = tag(o, path);
  const group = (p: string) => hexEncode(hex(field(o, "group", p), `${p}.group`));
  switch (type) {
    case "submit_proof":
      return { type, event: decodeEvent(field(o, "event", path), `${path}.event`) };
    case "ack_durable":
      return { type, seq: u64(field(o, "seq", path), `${path}.seq`) };
    case "report_publish":
      return {
        type,
        id: hex32(field(o, "id", path), `${path}.id`),
        results: arr(field(o, "results", path), `${path}.results`, decodeRelayResult),
      };
    case "publish_key_package":
    case "publish_inbox_relays":
      return { type, relays: strList(field(o, "relays", path), `${path}.relays`) };
    case "create_group":
      return {
        type,
        name: str(field(o, "name", path), `${path}.name`),
        description: str(field(o, "description", path), `${path}.description`),
        relays: strList(field(o, "relays", path), `${path}.relays`),
        invitees: arr(field(o, "invitees", path), `${path}.invitees`, decodeInvitee),
        admins: arr(field(o, "admins", path), `${path}.admins`, (item, p) => hex32(item, p)),
        components: arr(field(o, "components", path), `${path}.components`, decodeComponentUpdate),
      };
    case "invite":
      return {
        type,
        group: group(path),
        invitees: arr(field(o, "invitees", path), `${path}.invitees`, decodeInvitee),
      };
    case "remove_members":
      return {
        type,
        group: group(path),
        members: arr(field(o, "members", path), `${path}.members`, (item, p) => hex32(item, p)),
      };
    case "leave":
    case "self_update":
    case "group":
      return { type, group: group(path) };
    case "update_group_profile":
      return {
        type,
        group: group(path),
        name: optStr(o["name"], `${path}.name`),
        description: optStr(o["description"], `${path}.description`),
      };
    case "update_app_components":
      return {
        type,
        group: group(path),
        updates: arr(field(o, "updates", path), `${path}.updates`, decodeComponentUpdate),
      };
    case "send_message":
      return {
        type,
        group: group(path),
        message: decodeAppMessage(field(o, "message", path), `${path}.message`),
      };
    case "seal_attachment":
      return {
        type,
        group: group(path),
        attachment: decodeAttachmentInput(field(o, "attachment", path), `${path}.attachment`),
      };
    case "send_media":
      return {
        type,
        group: group(path),
        caption: str(field(o, "caption", path), `${path}.caption`),
        attachments: arr(
          field(o, "attachments", path),
          `${path}.attachments`,
          decodeMediaAttachmentRef,
        ),
        tags: tags(field(o, "tags", path), `${path}.tags`),
      };
    case "discard_attachment":
      return { type, handle: uint(field(o, "handle", path), `${path}.handle`) };
    case "open_attachment":
      return {
        type,
        group: group(path),
        message: hex32(field(o, "message", path), `${path}.message`),
        reference: decodeMediaReference(field(o, "reference", path), `${path}.reference`),
        ciphertext: hex(field(o, "ciphertext", path), `${path}.ciphertext`),
      };
    case "ingest":
      return {
        type,
        events: arr(field(o, "events", path), `${path}.events`, decodeEvent),
      };
    case "acknowledge_messages":
      return {
        type,
        ids: arr(field(o, "ids", path), `${path}.ids`, (item, p) => hex32(item, p)),
      };
    case "signer_reply":
      return {
        type,
        replies: arr(field(o, "replies", path), `${path}.replies`, decodeSignerReply),
      };
    case "awaiting_durability":
    case "take_outbound":
    case "tick":
    case "groups":
      return { type };
    default:
      return fail(`${path}.type`, "value");
  }
}

export function encodeRequest(request: BridgeRequest): Json {
  return match(request, {
    submit_proof: (request) => ({ type: "submit_proof", event: encodeEvent(request.event) }),
    ack_durable: (request) => ({ type: "ack_durable", seq: request.seq.toString() }),
    report_publish: (request) => ({
      type: "report_publish",
      id: request.id,
      results: request.results.map(encodeRelayResult),
    }),
    publish_key_package: (request) => ({ type: request.type, relays: request.relays }),
    publish_inbox_relays: (request) => ({ type: request.type, relays: request.relays }),
    create_group: (request) => ({
      type: "create_group",
      name: request.name ?? null,
      description: request.description ?? null,
      relays: request.relays,
      invitees: request.invitees.map(encodeInvitee),
      admins: request.admins,
      components: request.components.map(encodeComponentUpdate),
    }),
    invite: (request) => ({
      type: "invite",
      group: request.group,
      invitees: request.invitees.map(encodeInvitee),
    }),
    remove_members: (request) => ({
      type: "remove_members",
      group: request.group,
      members: request.members,
    }),
    leave: (request) => ({ type: request.type, group: request.group }),
    self_update: (request) => ({ type: request.type, group: request.group }),
    group: (request) => ({ type: request.type, group: request.group }),
    update_group_profile: (request) => ({
      type: "update_group_profile",
      group: request.group,
      name: request.name ?? null,
      description: request.description ?? null,
    }),
    update_app_components: (request) => ({
      type: "update_app_components",
      group: request.group,
      updates: request.updates.map(encodeComponentUpdate),
    }),
    send_message: (request) => ({
      type: "send_message",
      group: request.group,
      message: encodeAppMessage(request.message),
    }),
    seal_attachment: (request) => ({
      type: "seal_attachment",
      group: request.group,
      attachment: encodeAttachmentInput(request.attachment),
    }),
    send_media: (request) => ({
      type: "send_media",
      group: request.group,
      caption: request.caption,
      attachments: request.attachments.map(encodeMediaAttachmentRef),
      tags: request.tags,
    }),
    discard_attachment: (request) => ({ type: "discard_attachment", handle: request.handle }),
    open_attachment: (request) => ({
      type: "open_attachment",
      group: request.group,
      message: request.message,
      reference: encodeMediaReference(request.reference),
      ciphertext: hexEncode(request.ciphertext),
    }),
    ingest: (request) => ({ type: "ingest", events: request.events.map(encodeEvent) }),
    acknowledge_messages: (request) => ({ type: "acknowledge_messages", ids: request.ids }),
    signer_reply: (request) => ({
      type: "signer_reply",
      replies: request.replies.map(encodeSignerReply),
    }),
    awaiting_durability: (request) => ({ type: request.type }),
    take_outbound: (request) => ({ type: request.type }),
    tick: (request) => ({ type: request.type }),
    groups: (request) => ({ type: request.type }),
  });
}

export function decodeOpResult(value: unknown, path = "result"): OpResult {
  const o = rec(value, path, [
    "type",
    "account",
    "seq",
    "items",
    "resolution",
    "key_package",
    "key_package_ref",
    "event_id",
    "outbound_id",
    "group",
    "groups",
    "outcome",
    "ciphertext",
    "ciphertext_sha256",
    "handle",
    "plaintext",
    "reports",
    "next_wake_ms",
  ]);
  const type = tag(o, path);
  switch (type) {
    case "ready":
      return { type, account: hex32(field(o, "account", path), `${path}.account`) };
    case "done":
      return { type };
    case "awaiting_durability":
      return { type, seq: optU64(o["seq"], `${path}.seq`) };
    case "outbound":
      return {
        type,
        items: arr(field(o, "items", path), `${path}.items`, decodeOutbound),
      };
    case "publish_resolution":
      return {
        type,
        resolution: decodePublishResolution(field(o, "resolution", path), `${path}.resolution`),
      };
    case "key_package":
      return {
        type,
        keyPackage: hex(field(o, "key_package", path), `${path}.key_package`),
        keyPackageRef: str(field(o, "key_package_ref", path), `${path}.key_package_ref`),
        eventId: hex32(field(o, "event_id", path), `${path}.event_id`),
        account: hex32(field(o, "account", path), `${path}.account`),
      };
    case "inbox_relays":
      return {
        type,
        outboundId: hex32(field(o, "outbound_id", path), `${path}.outbound_id`),
      };
    case "group_created":
      return {
        type,
        group: decodeGroupSnapshot(field(o, "group", path), `${path}.group`),
      };
    case "send_outcome":
      return {
        type,
        outcome: decodeSendOutcome(field(o, "outcome", path), `${path}.outcome`),
      };
    case "sealed_attachment":
      return {
        type,
        ciphertext: hex(field(o, "ciphertext", path), `${path}.ciphertext`),
        ciphertextSha256: hex32(field(o, "ciphertext_sha256", path), `${path}.ciphertext_sha256`),
        handle: uint(field(o, "handle", path), `${path}.handle`),
      };
    case "attachment":
      return { type, plaintext: hex(field(o, "plaintext", path), `${path}.plaintext`) };
    case "ingested":
      return {
        type,
        reports: arr(field(o, "reports", path), `${path}.reports`, decodeInputReport),
      };
    case "ticked":
      return {
        type,
        nextWakeMs: optU64(o["next_wake_ms"], `${path}.next_wake_ms`),
      };
    case "groups":
      return {
        type,
        groups: arr(field(o, "groups", path), `${path}.groups`, decodeGroupSnapshot),
      };
    case "group": {
      const snapshot = field(o, "group", path);
      return {
        type,
        group:
          snapshot === null || snapshot === undefined
            ? undefined
            : decodeGroupSnapshot(snapshot, `${path}.group`),
      };
    }
    default:
      return fail(`${path}.type`, "value");
  }
}

export function encodeOpResult(result: OpResult): Json {
  return match(result, {
    ready: (result) => ({ type: "ready", account: result.account }),
    done: () => ({ type: "done" }),
    awaiting_durability: (result) => ({
      type: "awaiting_durability",
      seq: result.seq === undefined ? null : result.seq.toString(),
    }),
    outbound: (result) => ({ type: "outbound", items: result.items.map(encodeOutbound) }),
    publish_resolution: (result) => ({ type: "publish_resolution", resolution: result.resolution }),
    key_package: (result) => ({
      type: "key_package",
      key_package: hexEncode(result.keyPackage),
      key_package_ref: result.keyPackageRef,
      event_id: result.eventId,
      account: result.account,
    }),
    inbox_relays: (result) => ({ type: "inbox_relays", outbound_id: result.outboundId }),
    group_created: (result) => ({
      type: "group_created",
      group: encodeGroupSnapshot(result.group),
    }),
    send_outcome: (result) => ({
      type: "send_outcome",
      outcome: encodeSendOutcome(result.outcome),
    }),
    sealed_attachment: (result) => ({
      type: "sealed_attachment",
      ciphertext: hexEncode(result.ciphertext),
      ciphertext_sha256: result.ciphertextSha256,
      handle: result.handle,
    }),
    attachment: (result) => ({ type: "attachment", plaintext: hexEncode(result.plaintext) }),
    ingested: (result) => ({ type: "ingested", reports: result.reports.map(encodeInputReport) }),
    ticked: (result) => ({
      type: "ticked",
      next_wake_ms: result.nextWakeMs === undefined ? null : result.nextWakeMs.toString(),
    }),
    groups: (result) => ({ type: "groups", groups: result.groups.map(encodeGroupSnapshot) }),
    group: (result) => ({
      type: "group",
      group: result.group === undefined ? null : encodeGroupSnapshot(result.group),
    }),
  });
}

export function decodeOpenResponseValue(value: unknown, path = "open"): OpenResponse {
  const o = rec(value, path, ["type", "account", "unsigned", "error"]);
  const type = tag(o, path);
  switch (type) {
    case "ready":
      return { type, account: hex32(field(o, "account", path), `${path}.account`) };
    case "needs_account_proof":
      return {
        type,
        unsigned: decodeUnsignedEvent(field(o, "unsigned", path), `${path}.unsigned`),
      };
    case "error":
      return { type, error: decodeWireError(field(o, "error", path), `${path}.error`) };
    default:
      return fail(`${path}.type`, "value");
  }
}

export function encodeOpenResponse(response: OpenResponse): Json {
  return match(response, {
    ready: (response) => ({ type: "ready", account: response.account }),
    needs_account_proof: (response) => ({
      type: "needs_account_proof",
      unsigned: encodeUnsignedEvent(response.unsigned),
    }),
    error: (response) => ({ type: "error", error: encodeWireError(response.error) }),
  });
}

export function decodeResponseValue(value: unknown, path = "response"): BridgeResponse {
  const o = rec(value, path, ["type", "result", "requests", "error"]);
  const type = tag(o, path);
  switch (type) {
    case "ok":
      return { type, result: decodeOpResult(field(o, "result", path), `${path}.result`) };
    case "signer_pending":
      return {
        type,
        requests: arr(field(o, "requests", path), `${path}.requests`, decodeSignerRequest),
      };
    case "error":
      return { type, error: decodeWireError(field(o, "error", path), `${path}.error`) };
    default:
      return fail(`${path}.type`, "value");
  }
}

export function encodeResponse(response: BridgeResponse): Json {
  return match(response, {
    ok: (response) => ({ type: "ok", result: encodeOpResult(response.result) }),
    signer_pending: (response) => ({
      type: "signer_pending",
      requests: response.requests.map(encodeSignerRequest),
    }),
    error: (response) => ({ type: "error", error: encodeWireError(response.error) }),
  });
}

export function decodeDrainValue(value: unknown, path = "drain"): DrainResponse {
  const o = rec(value, path, ["type", "events", "error"]);
  const type = tag(o, path);
  switch (type) {
    case "events":
      return {
        type,
        events: arr(field(o, "events", path), `${path}.events`, decodeSessionEvent),
      };
    case "error":
      return { type, error: decodeWireError(field(o, "error", path), `${path}.error`) };
    default:
      return fail(`${path}.type`, "value");
  }
}

export function encodeDrainResponse(response: DrainResponse): Json {
  return match(response, {
    events: (response) => ({ type: "events", events: response.events.map(encodeSessionEvent) }),
    error: (response) => ({ type: "error", error: encodeWireError(response.error) }),
  });
}

const CONFIG_KEYS = ["protocol_version", "account", "now", "allow_loopback", "client_name"];

export function decodeConfigValue(value: unknown, path = "config"): BridgeConfig {
  const o = rec(value, path, CONFIG_KEYS);
  return {
    protocolVersion: uint(field(o, "protocol_version", path), `${path}.protocol_version`),
    account: hex32(field(o, "account", path), `${path}.account`),
    now: decodeTime(field(o, "now", path), `${path}.now`),
    allowLoopback: bool(field(o, "allow_loopback", path), `${path}.allow_loopback`),
    clientName: optStr(o["client_name"], `${path}.client_name`),
  };
}

export function encodeConfigValue(config: BridgeConfig): Json {
  return {
    protocol_version: config.protocolVersion,
    account: config.account,
    now: encodeTime(config.now),
    allow_loopback: config.allowLoopback,
    client_name: config.clientName ?? null,
  };
}

const CALL_KEYS = ["now", "request"];

export function decodeCallValue(value: unknown, path = "call"): CallEnvelope {
  const o = rec(value, path, CALL_KEYS);
  return {
    now: decodeTime(field(o, "now", path), `${path}.now`),
    request: decodeRequest(field(o, "request", path), `${path}.request`),
  };
}

export function encodeCallValue(envelope: CallEnvelope): Json {
  return { now: encodeTime(envelope.now), request: encodeRequest(envelope.request) };
}

function parseJson(bytes: Uint8Array, path: string): unknown {
  const text = utf8Decode(bytes);
  try {
    return JSON.parse(text);
  } catch {
    throw new DecodeError(path, "json");
  }
}

function encodeJson(value: Json): Uint8Array {
  return utf8Encode(JSON.stringify(value));
}

/** Encode the `open` config to wire bytes. */
export function encodeConfig(config: BridgeConfig): Uint8Array {
  return encodeJson(encodeConfigValue(config));
}

/** Encode one `call` envelope to wire bytes. */
export function encodeCall(now: ClockReading, request: BridgeRequest): Uint8Array {
  return encodeJson(encodeCallValue({ now, request }));
}

/** Decode the `open` response envelope from wire bytes. */
export function decodeOpenResponse(bytes: Uint8Array): OpenResponse {
  return decodeOpenResponseValue(parseJson(bytes, "open"), "open");
}

/** Decode the `call` response envelope from wire bytes. */
export function decodeResponse(bytes: Uint8Array): BridgeResponse {
  return decodeResponseValue(parseJson(bytes, "response"), "response");
}

/** Decode the `drain` response envelope from wire bytes. */
export function decodeDrain(bytes: Uint8Array): DrainResponse {
  return decodeDrainValue(parseJson(bytes, "drain"), "drain");
}
