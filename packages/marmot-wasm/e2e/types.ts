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
  memory: () => Promise<{ uaMemoryBytes: number | undefined }>;
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
    measureUserAgentSpecificMemory?: () => Promise<{ bytes: number }>;
  }
}
