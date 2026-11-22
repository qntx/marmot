/**
 * TypeScript mirror of the mk-bridge JSON wire protocol (`PROTOCOL_VERSION` 6). Decoded
 * representation: `u64` fields arrive as decimal strings and decode to `bigint`; identifiers (event
 * ids, pubkeys, group ids, nostr group ids, signatures, outbound ids, hashes, nonces) decode to
 * validated lowercase hex `string`s; payload blobs (`key_package`, component `data`, attachment
 * `plaintext`/`ciphertext`) decode to `Uint8Array`. Fieldless wire enums are string unions; tagged
 * unions use the wire `type` name.
 *
 * Every `*_TYPES` array is checked against its union's tag type both ways by the `Complete<>` alias
 * (`satisfies` can't annotate these declarations under `isolatedDeclarations`): a tag missing from
 * the array or an array entry that is not a tag is a compile error.
 */

import type { InputCategory } from "../core/errors.ts";

/** Fails to compile unless `T` is `never`: tags a `*_TYPES` array misses. */
export type Complete<T extends never> = T;

/** Wire protocol version. Bumped on any breaking schema change. */
export const PROTOCOL_VERSION = 6;

/** One reading of the injected clock (decimal strings on the wire). */
export type ClockReading = {
  /** Milliseconds since the Unix epoch. */
  unixMs: bigint;
  /** Milliseconds on a monotonic scale. */
  monotonicMs: bigint;
};

/** A signed NIP-01 event. */
export type NostrEvent = {
  /** Event id, 32-byte hex. */
  id: string;
  /** Signer's account key, 32-byte hex. */
  pubkey: string;
  /** Unix seconds. */
  createdAt: bigint;
  /** Nostr kind. */
  kind: number;
  /** Tag list: each entry is an array of strings. */
  tags: string[][];
  /** Content string. */
  content: string;
  /** BIP-340 schnorr signature over `id`, 64-byte hex. */
  sig: string;
};

/** An unsigned NIP-01 event: everything except `id` and `sig`. */
export type UnsignedEvent = {
  pubkey: string;
  createdAt: bigint;
  kind: number;
  tags: string[][];
  content: string;
};

/** One way to fetch an attachment: a `locator <kind> <value>` field. */
export type Locator = {
  kind: string;
  value: string;
};

/** A validated `encrypted-media-v2` attachment reference. */
export type MediaReference = {
  locators: Locator[];
  /** SHA-256 of the ciphertext blob, hex. */
  ciphertextSha256: string;
  /** SHA-256 of the plaintext, hex. */
  plaintextSha256: string;
  /** The 12-byte AEAD nonce, hex. */
  nonce: string;
  mediaType: string;
  filename: string;
  /** Optional `WxH` dimensions. */
  dim: string | undefined;
  /** Optional thumbhash. */
  thumbhash: string | undefined;
};

/** The stable category of one rejected `imeta` attachment. */
export type AttachmentRejection =
  | "invalid_structure"
  | "unsupported_format"
  | "missing_field"
  | "duplicate_field"
  | "malformed_field"
  | "unmapped";

export const ATTACHMENT_REJECTIONS = [
  "invalid_structure",
  "unsupported_format",
  "missing_field",
  "duplicate_field",
  "malformed_field",
  "unmapped",
] as const;

export type _AttachmentRejectionComplete = Complete<
  Exclude<AttachmentRejection, (typeof ATTACHMENT_REJECTIONS)[number]>
>;

/** One `imeta` attachment entry on a received message. */
export type AttachmentResult =
  | { type: "ok"; reference: MediaReference }
  | { type: "rejected"; reason: AttachmentRejection };

export const ATTACHMENT_RESULT_TYPES = ["ok", "rejected"] as const;

export type _AttachmentResultComplete = Complete<
  Exclude<AttachmentResult["type"], (typeof ATTACHMENT_RESULT_TYPES)[number]>
>;

/** One default blob-store endpoint of the group's media policy. */
export type BlobEndpoint = {
  locatorKind: string;
  baseUrl: string;
};

/** The group's `encrypted-media-v2` policy (`0x800b` component). */
export type EncryptedMediaPolicy = {
  allowedLocatorKinds: string[];
  endpoints: BlobEndpoint[];
};

/** The group's `0x8006` agent text stream policy (raw role bitmasks). */
export type AgentStreamPolicy = {
  requiredRoles: number;
  allowedRoles: number;
  maxPlaintextFrameLen: number;
  replayTtlSecs: number;
};

/** One attachment to seal: plaintext plus `imeta` metadata. */
export type AttachmentInput = {
  plaintext: Uint8Array;
  mediaType: string;
  filename: string;
  dim: string | undefined;
  thumbhash: string | undefined;
};

/** One invitee of a group creation or invite. */
export type Invitee = {
  /** The invitee's kind-30443 `KeyPackage` event. */
  keyPackage: NostrEvent;
  /** The invitee's inbox relays (its kind-10050 set). */
  inboxRelays: string[];
};

/** An app-component update. */
export type ComponentUpdate = {
  componentId: number;
  data: Uint8Array;
};

/** An outbound application message. */
export type AppMessage = {
  kind: number;
  tags: string[][];
  content: string;
};

/** A sealed attachment referenced by `send_media`. */
export type MediaAttachmentRef = {
  handle: number;
  locators: Locator[];
};

/** The group's `0x8007` avatar-url component fields. */
export type GroupAvatar = {
  url: string;
  /** Optional `WxH` hint. */
  dim: string | undefined;
  /** Optional thumbhash hint. */
  thumbhash: string | undefined;
};

/** A typed application-message intent (`send_intent`). */
export type MessageIntent =
  | { type: "chat"; content: string; replyTo: string | undefined }
  | { type: "reaction"; target: string; emoji: string }
  | { type: "unreact"; reactions: string[] }
  | { type: "edit"; target: string; content: string }
  | { type: "delete"; target: string }
  | { type: "remove"; target: string };

export const MESSAGE_INTENT_TYPES = [
  "chat",
  "reaction",
  "unreact",
  "edit",
  "delete",
  "remove",
] as const;

export type _MessageIntentComplete = Complete<
  Exclude<MessageIntent["type"], (typeof MESSAGE_INTENT_TYPES)[number]>
>;

/** The inner app event a send produced, including its canonical id. */
export type SentEvent = {
  /** Canonical inner event id, hex. */
  id: string;
  /** Signer's account key, hex. */
  pubkey: string;
  /** Unix seconds. */
  createdAt: bigint;
  /** Nostr kind. */
  kind: number;
  tags: string[][];
  content: string;
};

/** The typed meaning of a received application message. */
export type Semantics =
  | { type: "chat"; replyTo: string | undefined }
  | { type: "reaction"; target: string; emoji: string }
  | { type: "removal"; target: string }
  | { type: "edit"; target: string; content: string }
  | { type: "deletion"; targets: string[] }
  | { type: "unreact"; reactions: string[] }
  | { type: "other" }
  | { type: "unmapped" };

export const SEMANTICS_TYPES = [
  "chat",
  "reaction",
  "removal",
  "edit",
  "deletion",
  "unreact",
  "other",
  "unmapped",
] as const;

export type _SemanticsComplete = Complete<
  Exclude<Semantics["type"], (typeof SEMANTICS_TYPES)[number]>
>;

/** Why a received application event was dropped instead of delivered. */
export type DropReason = "expired" | "unauthorized" | "unverifiable" | "malformed" | "unmapped";

export const DROP_REASONS = [
  "expired",
  "unauthorized",
  "unverifiable",
  "malformed",
  "unmapped",
] as const;

export type _DropReasonComplete = Complete<Exclude<DropReason, (typeof DROP_REASONS)[number]>>;

/** What a send produced. */
export type SendOutcome =
  | { type: "staged"; ids: string[] }
  | { type: "queued" }
  | { type: "no_change" }
  | { type: "leave_requested" }
  | { type: "disband_requested" }
  | { type: "unmapped" };

export const SEND_OUTCOME_TYPES = [
  "staged",
  "queued",
  "no_change",
  "leave_requested",
  "disband_requested",
  "unmapped",
] as const;

export type _SendOutcomeComplete = Complete<
  Exclude<SendOutcome["type"], (typeof SEND_OUTCOME_TYPES)[number]>
>;

/** Per-relay publish status the host reports. */
export type RelayStatus = "accepted" | "rejected" | "unavailable" | "not_sent" | "unknown";

export const RELAY_STATUSES = [
  "accepted",
  "rejected",
  "unavailable",
  "not_sent",
  "unknown",
] as const;

export type _RelayStatusComplete = Complete<Exclude<RelayStatus, (typeof RELAY_STATUSES)[number]>>;

/** One relay's publish verdict from the host transport. */
export type RelayResult = {
  /** The relay URL exactly as the outbound item listed it. */
  relay: string;
  status: RelayStatus;
};

/** Terminal or in-progress verdict of a `report_publish` call. */
export type PublishResolution = "pending" | "confirmed" | "rolled_back" | "complete" | "unmapped";

export const PUBLISH_RESOLUTIONS = [
  "pending",
  "confirmed",
  "rolled_back",
  "complete",
  "unmapped",
] as const;

export type _PublishResolutionComplete = Complete<
  Exclude<PublishResolution, (typeof PUBLISH_RESOLUTIONS)[number]>
>;

/** What kind of artifact an `Outbound` carries. */
export type OutboundPurpose =
  | "application_message"
  | "proposal"
  | "commit"
  | "welcome"
  | "key_package"
  | "key_package_republish"
  | "inbox_relays"
  | "unmapped";

export const OUTBOUND_PURPOSES = [
  "application_message",
  "proposal",
  "commit",
  "welcome",
  "key_package",
  "key_package_republish",
  "inbox_relays",
  "unmapped",
] as const;

export type _OutboundPurposeComplete = Complete<
  Exclude<OutboundPurpose, (typeof OUTBOUND_PURPOSES)[number]>
>;

/** One outbound obligation handed to the host transport. */
export type Outbound = {
  /** Identity of the obligation (the frozen event's id), hex. */
  id: string;
  /** The exact signed event to publish. */
  event: NostrEvent;
  /** Relay URLs to publish to, in target order. */
  relays: string[];
  purpose: OutboundPurpose;
};

/** A group's lifecycle bucket for UI projection. */
export type GroupState =
  | "stable"
  | "pending_publish"
  | "recovering"
  | "unrecoverable"
  | "removed"
  | "disbanded"
  | "unmapped";

export const GROUP_STATES = [
  "stable",
  "pending_publish",
  "recovering",
  "unrecoverable",
  "removed",
  "disbanded",
  "unmapped",
] as const;

export type _GroupStateComplete = Complete<Exclude<GroupState, (typeof GROUP_STATES)[number]>>;

/** A convergence disposition (spec `foundation/errors.md` vocabulary). */
export type WireDisposition = "accepted" | "deferred" | "stale" | "invalidated" | "unmapped";

export const DISPOSITION_VALUES = [
  "accepted",
  "deferred",
  "stale",
  "invalidated",
  "unmapped",
] as const;

export type _WireDispositionComplete = Complete<
  Exclude<WireDisposition, (typeof DISPOSITION_VALUES)[number]>
>;

/** A wire input category: the spec vocabulary plus `unmapped`. */
export type WireInputCategory = InputCategory | "unmapped";

export const INPUT_CATEGORY_VALUES = [
  "duplicate",
  "own_echo",
  "wrong_recipient",
  "unknown_group",
  "stale_epoch",
  "invalid_encoding",
  "invalid_signature",
  "unsupported_required_feature",
  "authorization_failed",
  "missing_history",
  "transport_deferred",
  "resource_refused",
  "transport_rejected",
  "unmapped",
] as const;

export type _WireInputCategoryComplete = Complete<
  Exclude<WireInputCategory, (typeof INPUT_CATEGORY_VALUES)[number]>
>;

/** The availability cause behind an `unavailable` input outcome. */
export type WireAvailability =
  | "transport_deferred"
  | "resource_refused"
  | "signer_refused"
  | "signer_failed"
  | "unmapped";

export const AVAILABILITY_VALUES = [
  "transport_deferred",
  "resource_refused",
  "signer_refused",
  "signer_failed",
  "unmapped",
] as const;

export type _WireAvailabilityComplete = Complete<
  Exclude<WireAvailability, (typeof AVAILABILITY_VALUES)[number]>
>;

/** A canonical local ingest condition. */
export type LocalState = "rejoin_confirmation_required" | "removed" | "quarantined" | "unmapped";

export const LOCAL_STATES = [
  "rejoin_confirmation_required",
  "removed",
  "quarantined",
  "unmapped",
] as const;

export type _LocalStateComplete = Complete<Exclude<LocalState, (typeof LOCAL_STATES)[number]>>;

/** The typed outcome of one admitted input. */
export type InputOutcome =
  | { type: "accepted" }
  | { type: "deferred"; category: WireInputCategory | undefined }
  | { type: "stale"; category: WireInputCategory }
  | { type: "rejected"; category: WireInputCategory }
  | { type: "unavailable"; category: WireAvailability }
  | { type: "local"; state: LocalState }
  | { type: "recovery_offered" }
  | { type: "unmapped" };

export const INPUT_OUTCOME_TYPES = [
  "accepted",
  "deferred",
  "stale",
  "rejected",
  "unavailable",
  "local",
  "recovery_offered",
  "unmapped",
] as const;

export type _InputOutcomeComplete = Complete<
  Exclude<InputOutcome["type"], (typeof INPUT_OUTCOME_TYPES)[number]>
>;

/** Per-input report of `ingest`. */
export type InputReport = {
  /** The transport event that was processed, hex. */
  event: string;
  outcome: InputOutcome;
};

/** The four anchors of a delivered agent text stream final. */
export type StreamFinal = {
  streamId: string;
  startEventId: string;
  transcriptHash: string;
  /** How many QUIC chunks the transcript spanned. */
  chunkCount: bigint;
};

/** A decoded group application message. */
export type ReceivedMessage = {
  /** Canonical inner id. */
  id: string;
  /** Sender account, hex. */
  sender: string;
  createdAt: bigint;
  /** Inner kind. */
  kind: bigint;
  tags: string[][];
  content: string;
  /** MLS epoch the message was sent under. */
  epoch: bigint;
  /** The transport event that carried it, hex. */
  transportId: string;
  /** One entry per `imeta` tag, in tag order. */
  attachments: AttachmentResult[];
  streamFinal: StreamFinal | undefined;
  /** The typed meaning of the inner event. */
  semantics: Semantics;
  /** Source-epoch retention expiry. */
  expiresAt: bigint | undefined;
};

/** The anchors of a delivered agent text stream start (kind 1200). */
export type AgentStreamAnchor = {
  /** Canonical inner id of the kind-1200 event. */
  id: string;
  sender: string;
  createdAt: bigint;
  epoch: bigint;
  transportId: string;
  streamId: string;
  /** The prompt message this stream answers, when tagged, hex. */
  parent: string | undefined;
  /** Source-epoch retention expiry. */
  expiresAt: bigint | undefined;
};

/** The state delta a `group_state_changed` event carries. */
export type GroupChange =
  | { type: "member_added"; account: string }
  | { type: "member_removed"; account: string }
  | { type: "member_left"; account: string }
  | { type: "admin_added"; account: string }
  | { type: "admin_removed"; account: string }
  | { type: "renamed"; oldName: string; newName: string }
  | { type: "avatar_changed" }
  | { type: "retention_changed"; oldSeconds: bigint; newSeconds: bigint }
  | { type: "disbanded" }
  | { type: "unmapped" };

export const GROUP_CHANGE_TYPES = [
  "member_added",
  "member_removed",
  "member_left",
  "admin_added",
  "admin_removed",
  "renamed",
  "avatar_changed",
  "retention_changed",
  "disbanded",
  "unmapped",
] as const;

export type _GroupChangeComplete = Complete<
  Exclude<GroupChange["type"], (typeof GROUP_CHANGE_TYPES)[number]>
>;

/** Events the session surfaces to the host. */
export type SessionEvent =
  | { type: "message_received"; group: string; message: ReceivedMessage }
  | { type: "agent_stream_started"; group: string; start: AgentStreamAnchor }
  | { type: "message_invalidated"; group: string; message: string; epoch: bigint }
  | { type: "message_expired"; group: string; message: string; transportId: string }
  | { type: "group_created"; group: string }
  | { type: "group_joined"; group: string }
  | { type: "epoch_changed"; group: string; from: bigint; to: bigint }
  | {
      type: "group_state_changed";
      group: string;
      epoch: bigint;
      actor: string | undefined;
      change: GroupChange;
      originCommit: string | undefined;
      /** Canonical system-row id, deterministic across members. */
      rowId: string;
    }
  | { type: "group_state_invalidated"; group: string; epoch: bigint; commit: string }
  | { type: "group_state_revalidated"; group: string; epoch: bigint; commit: string }
  | { type: "local_copy_terminated"; group: string; voluntary: boolean }
  | { type: "local_copy_restored"; group: string }
  | {
      type: "queued_intent_refused";
      group: string;
      intent: string;
      intentKind: string;
      reason: string;
    }
  | { type: "hydration_quarantined"; group: string }
  | { type: "hydration_recovered"; group: string; epoch: bigint }
  | { type: "pending_commit_recovered"; group: string; epoch: bigint }
  | { type: "group_unrecoverable"; group: string }
  | { type: "publish_resolved"; outbound: string; resolution: PublishResolution }
  | { type: "app_event_dropped"; group: string; reason: DropReason }
  | { type: "key_package_rotated" }
  | { type: "unmapped" };

export const SESSION_EVENT_TYPES = [
  "message_received",
  "agent_stream_started",
  "message_invalidated",
  "message_expired",
  "group_created",
  "group_joined",
  "epoch_changed",
  "group_state_changed",
  "group_state_invalidated",
  "group_state_revalidated",
  "local_copy_terminated",
  "local_copy_restored",
  "queued_intent_refused",
  "hydration_quarantined",
  "hydration_recovered",
  "pending_commit_recovered",
  "group_unrecoverable",
  "publish_resolved",
  "app_event_dropped",
  "key_package_rotated",
  "unmapped",
] as const;

export type _SessionEventComplete = Complete<
  Exclude<SessionEvent["type"], (typeof SESSION_EVENT_TYPES)[number]>
>;

/** A published `KeyPackage`'s identity, as reported by `key_package_status`. */
export type KeyPackageInfo = {
  /** Raw `KeyPackage` bytes. */
  keyPackage: Uint8Array;
  /** `KeyPackageRef` (hex) as asserted by the `i` tag. */
  keyPackageRef: string;
  /** The event id that published it, hex. */
  eventId: string;
  /** The publisher's account, hex. */
  account: string;
};

/** One member in a `GroupSnapshot`. */
export type MemberSnapshot = {
  account: string;
  admin: boolean;
};

/** A UI-facing snapshot of one group. */
export type GroupSnapshot = {
  /** The MLS group id, hex. */
  id: string;
  name: string;
  description: string;
  epoch: bigint;
  state: GroupState;
  members: MemberSnapshot[];
  /** The group's Nostr routing relays. */
  relays: string[];
  /** The 32-byte Nostr route id (`#h` filter value), hex. */
  nostrGroupId: string;
  ownAccountAdmin: boolean;
  encryptedMedia: EncryptedMediaPolicy | undefined;
  agentTextStream: AgentStreamPolicy | undefined;
  /** The group's avatar component, when set. */
  avatar: GroupAvatar | undefined;
  /** Message retention seconds (`0` = disabled). */
  retentionSeconds: bigint;
  /** Whether the group requires the `0x800c` lifecycle component. */
  disbandEnabled: boolean;
  /** Whether a disband is in progress. */
  disbanding: boolean;
};

/** An account-signer operation the host must perform. */
export type SignerOp =
  | { type: "sign_event"; unsigned: UnsignedEvent }
  | { type: "nip44_encrypt"; peer: string; plaintext: string }
  | { type: "nip44_decrypt"; peer: string; ciphertext: string };

export const SIGNER_OP_TYPES = ["sign_event", "nip44_encrypt", "nip44_decrypt"] as const;

export type _SignerOpComplete = Complete<
  Exclude<SignerOp["type"], (typeof SIGNER_OP_TYPES)[number]>
>;

/** One deferred account-signer operation surfaced to the host. */
export type SignerRequest = {
  /** Request id (monotonic per bridge). */
  id: number;
  op: SignerOp;
};

/** The result the host reports for a signer operation. */
export type SignerResult =
  | { type: "signed"; event: NostrEvent }
  | { type: "text"; text: string }
  | { type: "refused" }
  | { type: "failed" }
  | { type: "invalid_payload" };

export const SIGNER_RESULT_TYPES = [
  "signed",
  "text",
  "refused",
  "failed",
  "invalid_payload",
] as const;

export type _SignerResultComplete = Complete<
  Exclude<SignerResult["type"], (typeof SIGNER_RESULT_TYPES)[number]>
>;

/** One host answer to a `SignerRequest`. */
export type SignerReply = {
  id: number;
  result: SignerResult;
};

/** A `call` request: one variant per `Session` surface plus open/sign glue. */
export type BridgeRequest =
  | { type: "submit_proof"; event: NostrEvent }
  | { type: "ack_durable"; seq: bigint }
  | { type: "awaiting_durability" }
  | { type: "take_outbound" }
  | { type: "report_publish"; id: string; results: RelayResult[] }
  | { type: "ensure_key_package"; writeRelays: string[] }
  | { type: "key_package_status" }
  | { type: "publish_inbox_relays"; relays: string[] }
  | {
      type: "create_group";
      name: string;
      description: string;
      relays: string[];
      invitees: Invitee[];
      admins: string[];
      components: ComponentUpdate[];
    }
  | { type: "invite"; group: string; invitees: Invitee[] }
  | { type: "remove_members"; group: string; members: string[] }
  | { type: "leave"; group: string }
  | { type: "self_update"; group: string }
  | {
      type: "update_group_profile";
      group: string;
      name: string | undefined;
      description: string | undefined;
    }
  | { type: "update_app_components"; group: string; updates: ComponentUpdate[] }
  | { type: "send_message"; group: string; message: AppMessage }
  | { type: "send_intent"; group: string; intent: MessageIntent }
  | { type: "set_admin"; group: string; account: string; admin: boolean }
  | { type: "set_group_avatar"; group: string; avatar: GroupAvatar | undefined }
  | { type: "set_retention"; group: string; seconds: bigint }
  | { type: "disband"; group: string }
  | { type: "enable_disbanding"; group: string }
  | { type: "forget_group"; group: string }
  | {
      type: "select_key_packages";
      account: string;
      events: NostrEvent[];
      /** The group whose invite requirements filter candidates; `undefined` = creation requirements. */
      group: string | undefined;
    }
  | { type: "seal_attachment"; group: string; attachment: AttachmentInput }
  | {
      type: "send_media";
      group: string;
      caption: string;
      attachments: MediaAttachmentRef[];
      tags: string[][];
    }
  | { type: "discard_attachment"; handle: number }
  | {
      type: "open_attachment";
      group: string;
      message: string;
      reference: MediaReference;
      ciphertext: Uint8Array;
    }
  | { type: "ingest"; events: NostrEvent[] }
  | { type: "tick" }
  | { type: "acknowledge_messages"; ids: string[] }
  | { type: "groups" }
  | { type: "group"; group: string }
  | { type: "signer_reply"; replies: SignerReply[] };

/** Every `request.type` this protocol version implements, canonical order. */
export const REQUEST_TYPES = [
  "ack_durable",
  "acknowledge_messages",
  "awaiting_durability",
  "create_group",
  "disband",
  "discard_attachment",
  "enable_disbanding",
  "ensure_key_package",
  "forget_group",
  "group",
  "groups",
  "ingest",
  "invite",
  "key_package_status",
  "leave",
  "open_attachment",
  "publish_inbox_relays",
  "remove_members",
  "report_publish",
  "seal_attachment",
  "select_key_packages",
  "self_update",
  "send_intent",
  "send_media",
  "send_message",
  "set_admin",
  "set_group_avatar",
  "set_retention",
  "signer_reply",
  "submit_proof",
  "take_outbound",
  "tick",
  "update_app_components",
  "update_group_profile",
] as const;

export type _BridgeRequestComplete = Complete<
  Exclude<BridgeRequest["type"], (typeof REQUEST_TYPES)[number]>
>;

/** `ensure_key_package`/`key_package_status` result: the slot's lifecycle standing. */
export type KeyPackageStatusResult = {
  /** The stable replaceable-event slot (`d` tag), hex. */
  slot: string;
  /** The promoted, acknowledged publication, or `undefined`. */
  current: KeyPackageInfo | undefined;
  /** Whether a replacement is staged or in flight. */
  pending: boolean;
  /** Retired private material still inside its grace period. */
  retained: number;
  /** Absolute Unix milliseconds of the earliest lifecycle deadline, or `undefined`. */
  nextDueMs: bigint | undefined;
};

/** `seal_attachment` result: the blob to upload plus the send-time handle. */
export type SealedAttachmentResult = {
  ciphertext: Uint8Array;
  ciphertextSha256: string;
  handle: number;
};

/** What a `send_*` call produced: the outcome plus the sender's inner event. */
export type SentMessage = {
  outcome: SendOutcome;
  message: SentEvent;
};

/** The typed result of a successful `call`, internally tagged by `type`. */
export type OpResult =
  | { type: "ready"; account: string }
  | { type: "done" }
  | { type: "awaiting_durability"; seq: bigint | undefined }
  | { type: "outbound"; items: Outbound[] }
  | { type: "publish_resolution"; resolution: PublishResolution }
  | {
      type: "key_package_status";
      slot: string;
      current: KeyPackageInfo | undefined;
      pending: boolean;
      retained: number;
      nextDueMs: bigint | undefined;
    }
  | { type: "inbox_relays"; outboundId: string }
  | { type: "group_created"; group: GroupSnapshot }
  | { type: "send_outcome"; outcome: SendOutcome }
  | { type: "message_sent"; outcome: SendOutcome; message: SentEvent }
  | { type: "sealed_attachment"; ciphertext: Uint8Array; ciphertextSha256: string; handle: number }
  | { type: "attachment"; plaintext: Uint8Array }
  | { type: "ingested"; reports: InputReport[] }
  | { type: "ticked"; nextWakeMs: bigint | undefined }
  | { type: "groups"; groups: GroupSnapshot[] }
  | { type: "group"; group: GroupSnapshot | undefined }
  | { type: "key_packages"; events: NostrEvent[] };

export const RESULT_TYPES = [
  "ready",
  "done",
  "awaiting_durability",
  "outbound",
  "publish_resolution",
  "key_package_status",
  "inbox_relays",
  "group_created",
  "send_outcome",
  "message_sent",
  "sealed_attachment",
  "attachment",
  "ingested",
  "ticked",
  "groups",
  "group",
  "key_packages",
] as const;

export type _OpResultComplete = Complete<Exclude<OpResult["type"], (typeof RESULT_TYPES)[number]>>;

/** Error `kind`s that carry no fields beyond `kind` itself. */
export type PlainErrorKind =
  | "admin_depletion"
  | "already_open"
  | "attachment_limit"
  | "awaiting_proof"
  | "busy"
  | "clock_regressed"
  | "closed"
  | "confirm_conflict"
  | "disbanding_not_enabled"
  | "group_busy"
  | "group_disbanding"
  | "host_failed"
  | "host_rows_invalid"
  | "invalid_relay_url"
  | "invalid_transport_event"
  | "leave_already_requested"
  | "media_epoch_stale"
  | "media_epoch_unsettled"
  | "media_not_enabled"
  | "media_secret_unavailable"
  | "missing_required_capabilities"
  | "no_pending_signer"
  | "not_a_member"
  | "not_group_admin"
  | "not_open"
  | "poisoned"
  | "reserved_app_event"
  | "send_rate_exceeded"
  | "storage"
  | "unknown_attachment"
  | "unknown_group"
  | "unknown_outbound"
  | "unknown_seq"
  | "unknown_signer_request"
  | "unsupported";

/** A decoded wire error: `kind` discriminates; a few kinds carry labels. */
export type WireError =
  | { kind: "engine"; detail: string }
  | { kind: "internal"; site: string }
  | { kind: "invalid_input"; field: string }
  | { kind: "invalid_request"; reason: string }
  | { kind: "media"; reason: string }
  | { kind: "open_failed"; reason: string; detail: string }
  | { kind: "proof_rejected"; reason: string }
  | { kind: "signer"; reason: string }
  | { kind: "protocol_version_mismatch"; expected: number; got: number }
  | { kind: "unsupported_ciphersuite"; required: number; got: number }
  | { kind: "too_large"; limit: bigint }
  | { kind: PlainErrorKind };

/** Every `error.kind` this protocol version implements, canonical order. */
export const ERROR_KINDS = [
  "admin_depletion",
  "already_open",
  "attachment_limit",
  "awaiting_proof",
  "busy",
  "clock_regressed",
  "closed",
  "confirm_conflict",
  "disbanding_not_enabled",
  "engine",
  "group_busy",
  "group_disbanding",
  "host_failed",
  "host_rows_invalid",
  "internal",
  "invalid_input",
  "invalid_relay_url",
  "invalid_request",
  "invalid_transport_event",
  "leave_already_requested",
  "media",
  "media_epoch_stale",
  "media_epoch_unsettled",
  "media_not_enabled",
  "media_secret_unavailable",
  "missing_required_capabilities",
  "no_pending_signer",
  "not_a_member",
  "not_group_admin",
  "not_open",
  "open_failed",
  "poisoned",
  "proof_rejected",
  "protocol_version_mismatch",
  "reserved_app_event",
  "send_rate_exceeded",
  "signer",
  "storage",
  "too_large",
  "unknown_attachment",
  "unknown_group",
  "unknown_outbound",
  "unknown_seq",
  "unknown_signer_request",
  "unsupported",
  "unsupported_ciphersuite",
] as const;

export type _WireErrorComplete = Complete<Exclude<WireError["kind"], (typeof ERROR_KINDS)[number]>>;

/** The `open` config: account binding, first clock reading and policy. */
export type BridgeConfig = {
  /** Must equal `PROTOCOL_VERSION`. */
  protocolVersion: number;
  /** The account this session signs for, 32-byte hex. */
  account: string;
  /** The host's clock reading at open. */
  now: ClockReading;
  /** Permit loopback/private relay endpoints. */
  allowLoopback: boolean;
  /** Optional NIP-89 client tag on `KeyPackage` publications. */
  clientName: string | undefined;
};

/** A `call` envelope: the host's clock reading plus the request. */
export type CallEnvelope = {
  now: ClockReading;
  request: BridgeRequest;
};

/** The `open` response envelope. */
export type OpenResponse =
  | { type: "ready"; account: string }
  | { type: "needs_account_proof"; unsigned: UnsignedEvent }
  | { type: "error"; error: WireError };

export const OPEN_RESPONSE_TYPES = ["ready", "needs_account_proof", "error"] as const;

export type _OpenResponseComplete = Complete<
  Exclude<OpenResponse["type"], (typeof OPEN_RESPONSE_TYPES)[number]>
>;

/** The `call` response envelope. */
export type BridgeResponse =
  | { type: "ok"; result: OpResult }
  | { type: "signer_pending"; requests: SignerRequest[] }
  | { type: "error"; error: WireError };

export const RESPONSE_TYPES = ["ok", "signer_pending", "error"] as const;

export type _BridgeResponseComplete = Complete<
  Exclude<BridgeResponse["type"], (typeof RESPONSE_TYPES)[number]>
>;

/** The `drain` response envelope. */
export type DrainResponse =
  | { type: "events"; events: SessionEvent[] }
  | { type: "error"; error: WireError };

export const DRAIN_RESPONSE_TYPES = ["events", "error"] as const;

export type _DrainResponseComplete = Complete<
  Exclude<DrainResponse["type"], (typeof DRAIN_RESPONSE_TYPES)[number]>
>;
