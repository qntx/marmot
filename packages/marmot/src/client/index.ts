/**
 * `@qntx/marmot/client` — `MarmotClient`: the `Engine` behind the serialized `Pump`, injected relay
 * port, timer, clock, KV and cursor store, with typed errors and typed events.
 */

export * from "./client.ts";
export * from "./errors.ts";
export * from "./state.ts";
export type { ClientMessage, DeliveryEvent, GroupEvent } from "./pump.ts";
export {
  INGEST_BATCH_MAX,
  INGEST_DELAY_MS,
  OUTBOX_RETRY_TICK_MS,
  RESUBSCRIBE_DELAY_MS,
} from "./pump.ts";
export type { InviteEvent } from "../invites/index.ts";
export type { UploadPort } from "../groups/index.ts";
export type { CursorStore } from "../storage/driver.ts";
export type {
  PublishStatus,
  RelayFilter,
  RelayPort,
  SubscriptionHandlers,
  Timer,
} from "../transport/index.ts";
export {
  GIFT_WRAP_OVERLAP_SECS,
  GROUP_OVERLAP_SECS,
  MAX_RELAYS,
  MAX_SUBSCRIPTIONS,
  nostrRelayPort,
  safeRelayUrl,
} from "../transport/index.ts";
