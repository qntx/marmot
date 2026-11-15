/**
 * Marmot wire registries — typed constants mirroring the spec's `foundation/registries.md` at the
 * revision pinned in `upstream.json` (`spec.rev`). The spec tables are recorded byte-for-byte under
 * `vectors/spec/foundation/` and the package tests assert set-equality both ways, so these
 * constants can only drift when the pin moves.
 */

/**
 * Marmot app components use MLS private-use component ids. `0x800a` was
 * `marmot.authorization.multi-device-join.v1`, the join authorization of an earlier multi-device
 * draft that the spec withdrew: it stays registered so the value is never reassigned, and MUST NOT
 * be emitted or reused (deployed clients still reject GroupContext data under it).
 */
export const APP_COMPONENTS = {
  groupProfileV1: { id: 0x8001, name: "marmot.group.profile.v1", status: "active" },
  groupBlossomImageV1: { id: 0x8002, name: "marmot.group.blossom.image.v1", status: "active" },
  groupAdminPolicyV1: { id: 0x8003, name: "marmot.group.admin-policy.v1", status: "active" },
  transportNostrRoutingV1: {
    id: 0x8004,
    name: "marmot.transport.nostr.routing.v1",
    status: "active",
  },
  groupMessageRetentionV1: {
    id: 0x8005,
    name: "marmot.group.message-retention.v1",
    status: "active",
  },
  groupAgentTextStreamQuicV1: {
    id: 0x8006,
    name: "marmot.group.agent-text-stream.quic.v1",
    status: "active",
  },
  groupAvatarUrlV1: { id: 0x8007, name: "marmot.group.avatar-url.v1", status: "active" },
  groupEncryptedMediaV1: { id: 0x8008, name: "marmot.group.encrypted-media.v1", status: "active" },
  memberAccountIdentityProofV2: {
    id: 0x8009,
    name: "marmot.member.account-identity-proof.v2",
    status: "active",
  },
  withdrawn800a: { id: 0x800a, name: "withdrawn", status: "withdrawn" },
  groupEncryptedMediaV2: { id: 0x800b, name: "marmot.group.encrypted-media.v2", status: "active" },
  groupLifecycleV1: { id: 0x800c, name: "marmot.group.lifecycle.v1", status: "active" },
  groupAccountDevicesV1: { id: 0x800d, name: "marmot.group.account-devices.v1", status: "active" },
  accountDeviceRosterV1: {
    id: 0x800e,
    name: "marmot.account.device-roster.v1",
    status: "active",
  },
  memberDeviceTagV1: { id: 0x800f, name: "marmot.member.device-tag.v1", status: "active" },
} as const;

/** Registered Marmot app component id wire values (including withdrawn). */
export type AppComponentId = (typeof APP_COMPONENTS)[keyof typeof APP_COMPONENTS]["id"];

/** Marmot app component names as written in the spec registry. */
export type AppComponentName = (typeof APP_COMPONENTS)[keyof typeof APP_COMPONENTS]["name"];

/**
 * Upstream MLS extensions-draft assignments Marmot pins (draft-ietf-mls-extensions-10). If the
 * upstream draft reassigns these values, the spec requires an explicit compatibility plan before
 * wire behavior changes. `app_components` is the `app_data_dictionary` carrier component;
 * `last_resort_key_package` is an empty-data marker inside a KeyPackage dictionary, not an MLS
 * extension type.
 */
export const MLS_EXTENSION_DRAFT_IDS = {
  appComponents: { namespace: "ComponentID", id: 0x0001, name: "app_components" },
  safeAad: { namespace: "ComponentID", id: 0x0002, name: "safe_aad" },
  lastResortKeyPackage: { namespace: "ComponentID", id: 0x0004, name: "last_resort_key_package" },
  appDataDictionary: { namespace: "Extension type", id: 0x0006, name: "app_data_dictionary" },
  appDataUpdate: { namespace: "Proposal type", id: 0x0008, name: "app_data_update" },
  appEphemeral: { namespace: "Proposal type", id: 0x0009, name: "app_ephemeral" },
  selfRemove: { namespace: "Proposal type", id: 0x000a, name: "self_remove" },
} as const;

/** Upstream draft id namespaces used by the Marmot profile. */
export type MlsExtensionDraftNamespace =
  (typeof MLS_EXTENSION_DRAFT_IDS)[keyof typeof MLS_EXTENSION_DRAFT_IDS]["namespace"];

/**
 * Marmot custom MLS extension types. `0xf2d1`, `0xf2d2` and `0xf2d4` are the agent-text-stream-QUIC
 * member-role capability markers: they appear solely in `LeafNode.capabilities.extensions` and are
 * never emitted as extension bodies. `0xf2f1` is the superseded v1 account identity proof —
 * component id `0x8009` replaces it — and stays registered so the wire value is never reassigned.
 * `0xf2d3` is intentionally unassigned.
 */
export const MARMOT_EXTENSION_TYPES = {
  agentTextStreamQuicReceiveV1: {
    id: 0xf2d1,
    name: "marmot.feature.agent_text_stream_quic.receive.v1",
    status: "active",
  },
  agentTextStreamQuicSendV1: {
    id: 0xf2d2,
    name: "marmot.feature.agent_text_stream_quic.send.v1",
    status: "active",
  },
  agentTextStreamQuicFanoutV1: {
    id: 0xf2d4,
    name: "marmot.feature.agent_text_stream_quic.fanout.v1",
    status: "active",
  },
  accountIdentityProofV1: {
    id: 0xf2f1,
    name: "marmot.account-identity-proof.v1",
    status: "superseded",
  },
} as const;

/** Registered Marmot extension type wire values (including superseded). */
export type MarmotExtensionType =
  (typeof MARMOT_EXTENSION_TYPES)[keyof typeof MARMOT_EXTENSION_TYPES]["id"];

/**
 * Nostr event kinds Marmot allocates or assigns meaning to. Kinds `450` and `451` are local signing
 * templates and MUST NOT be published to relays; kinds `452`–`456` travel only as rumors
 * gift-wrapped to the sender's own account. Standard transport kinds reused unchanged (1059, 13, 5,
 * 10002, 10050, 30078) live in `transports/nostr.md`, not this table.
 */
export const EVENT_KINDS = {
  chatMessage: { kind: 9, name: "Default chat message", layer: "Marmot app payload" },
  welcomeRumor: { kind: 444, name: "Marmot welcome rumor", layer: "Nostr welcome transport" },
  groupMessage: { kind: 445, name: "Marmot group message", layer: "Nostr group transport" },
  pushNotificationRumor: {
    kind: 446,
    name: "Push notification rumor",
    layer: "Push notification transport",
  },
  pushTokenRequest: { kind: 447, name: "Push token request", layer: "Marmot app payload" },
  pushTokenListResponse: {
    kind: 448,
    name: "Push token list response",
    layer: "Marmot app payload",
  },
  pushTokenRemoval: { kind: 449, name: "Push token removal", layer: "Marmot app payload" },
  accountIdentityProofV2: {
    kind: 450,
    name: "Account identity proof v2 event",
    layer: "Local signing template, not relayed",
  },
  pushOwnerProof: {
    kind: 451,
    name: "Push owner proof event",
    layer: "Local signing template, not relayed",
  },
  devicePairingRequest: {
    kind: 452,
    name: "Device pairing request",
    layer: "Rumor gift-wrapped to own account",
  },
  devicePairingAnswer: {
    kind: 453,
    name: "Device pairing answer",
    layer: "Rumor gift-wrapped to own account",
  },
  devicePairingReveal: {
    kind: 454,
    name: "Device pairing reveal",
    layer: "Rumor gift-wrapped to own account",
  },
  devicePairingConfirm: {
    kind: 455,
    name: "Device pairing confirm",
    layer: "Rumor gift-wrapped to own account",
  },
  devicePairingCancel: {
    kind: 456,
    name: "Device pairing cancel",
    layer: "Rumor gift-wrapped to own account",
  },
  deviceHeartbeat: { kind: 457, name: "Device heartbeat", layer: "Marmot app payload" },
  siblingChatGapFillClaim: {
    kind: 458,
    name: "Sibling chat gap-fill claim",
    layer: "Marmot app payload",
  },
  privateKeyPackageRequest: {
    kind: 459,
    name: "Private KeyPackage request",
    layer: "Marmot app payload",
  },
  privateKeyPackageHandOff: {
    kind: 460,
    name: "Private KeyPackage hand-off",
    layer: "Marmot app payload",
  },
  siblingWelcome: { kind: 461, name: "Sibling Welcome", layer: "Marmot app payload" },
  historyDelivery: { kind: 473, name: "History delivery", layer: "Marmot app payload" },
  smallStateUpdate: { kind: 475, name: "Small-state update", layer: "Marmot app payload" },
  smallStateBroadcastRequest: {
    kind: 476,
    name: "Small-state broadcast request",
    layer: "Marmot app payload",
  },
  messageEdit: { kind: 1009, name: "Message edit", layer: "Marmot app payload" },
  pollResponse: { kind: 1018, name: "Poll response (NIP-88)", layer: "Marmot app payload" },
  poll: { kind: 1068, name: "Poll (NIP-88)", layer: "Marmot app payload" },
  agentTextStreamStart: {
    kind: 1200,
    name: "Agent text stream start",
    layer: "Marmot app payload",
  },
  groupSystemEvent: { kind: 1210, name: "Group system event", layer: "Marmot app payload" },
  contentReport: { kind: 1984, name: "Content report (NIP-56)", layer: "Marmot app payload" },
  reportDismissalLabels: {
    kind: 1985,
    name: "Report dismissal labels (NIP-32)",
    layer: "Marmot app payload",
  },
  adminContentDeletionV1: {
    kind: 4891,
    name: "Admin content deletion v1",
    layer: "Marmot app payload",
  },
  keyPackage: {
    kind: 30443,
    name: "Marmot KeyPackage event",
    layer: "Nostr KeyPackage publication",
  },
} as const;

/** Nostr event kind numbers Marmot allocates or assigns meaning to. */
export type EventKind = (typeof EVENT_KINDS)[keyof typeof EVENT_KINDS]["kind"];

/**
 * ALPN and protocol identifiers registered by the spec's transports. An incompatible transport or
 * control protocol registers a new identifier rather than reinterpreting one of these strings.
 */
export const ALPN_IDENTIFIERS = {
  quicStream: { id: "marmot.quic_stream.v1", use: "direct-path QUIC ALPN" },
  quicBroker: { id: "marmot.quic_broker.v1", use: "broker QUIC ALPN and control identifier" },
} as const;

/**
 * `MLS-Exporter(label, context, length)` invocations registered by the spec. The `label`/`context`
 * pair is the top-level domain separator; owning documents define any additional key context used
 * below the exporter output. Fixed 32-byte outputs feed a 32-byte AEAD key or feature key
 * schedule.
 */
export const EXPORTERS = {
  groupEvent: { label: "marmot", context: "group-event", length: 32 },
  encryptedMedia: { label: "marmot", context: "encrypted-media", length: 32 },
  agentTextStreamQuic: { label: "marmot", context: "agent-text-stream-quic", length: 32 },
  convergenceConformanceV1: {
    label: "marmot",
    context: "convergence-conformance-v1",
    length: 32,
  },
} as const;
