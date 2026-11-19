import type { FlowTimings } from "@qntx/marmot/testing";

/** The `window.__marmot` surface the harness bundle installs. */
export type Harness = {
  init: (
    keyBase64: string,
    secretBase64: string,
  ) => Promise<{ fetchMs: number; instantiateMs: number }>;
  flow: (messages: number) => Promise<FlowTimings>;
  kvOpen: (name: string) => Promise<string>;
  openEngine: (dbName: string) => Promise<void>;
  createGroup: (name: string) => Promise<string>;
  sendOnce: (groupId: string, text: string) => Promise<void>;
  closeEngine: () => Promise<void>;
  reopenEngine: (dbName: string) => Promise<void>;
  startKillLoop: (dbName: string) => Promise<void>;
  dumpRows: (dbName: string) => Promise<Array<{ key: string; value: string }>>;
  dbStats: (dbName: string) => Promise<{ rows: number; bytes: number }>;
  receiveScenario: (
    dbName: string,
    groups: number,
    messages: number,
  ) => Promise<{ ingestMs: number[]; wallMs: number }>;
  memory: () => Promise<{ uaMemoryBytes: number | undefined; breakdown: unknown }>;
};

declare global {
  /**
   * Installed by the harness bundle inside the page. Spec files only touch it inside serialized
   * `page.evaluate` callbacks, so it is never read in Node.
   */
  var __marmot: Harness;

  /** Node callbacks injected via `page.exposeFunction`; absent outside the kill spec. */
  var recordBatch: ((seq: string, batchBase64: string) => Promise<void>) | undefined;
  var recordAck: ((seq: string) => Promise<void>) | undefined;
  var publishOutbound:
    | ((outboundId: string, eventId: string, ackedWatermark: string) => Promise<void>)
    | undefined;

  /** Chromium-only memory API — absent from the TS DOM lib at this version. */
  // oxlint-disable-next-line consistent-type-definitions -- global augmentation requires interface merging
  interface Performance {
    measureUserAgentSpecificMemory?: () => Promise<{
      bytes: number;
      breakdown: unknown[];
    }>;
  }
}

/** The `window.__marmotClient` surface the client harness bundle installs (one client per page). */
export type ClientHarness = {
  init: (keyBase64: string, secretBase64: string) => Promise<void>;
  open: (relayUrl: string) => Promise<void>;
  identity: () => Promise<void>;
  createGroup: (
    name: string,
    relays: string[],
    inviteePubkey: string,
    lookupRelays: string[],
  ) => Promise<string>;
  send: (group: string, text: string) => Promise<void>;
  transcript: () => string[];
  received: () => string[];
  sentMapEntries: () => Array<[string, string[]]>;
  deliveredIds: () => string[];
  invitesSeen: () => string[];
  errorsSeen: () => string[];
  snapshot: (group: string) => Promise<unknown>;
  close: () => Promise<void>;
};

declare global {
  var __marmotClient: ClientHarness;
}
