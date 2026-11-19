/**
 * The engine port: what a bridge backend (the mk-wasm shell, a native engine thread, or a scripted
 * fake in tests) implements. Every handle call is asynchronous and byte-oriented — the JSON
 * protocol lives on both sides of this seam — while the host callbacks stay synchronous: `load`
 * hands over a snapshot once and `commit` is a notification.
 */

/** The storage callbacks the bridge invokes while running. */
export type BridgeHost = {
  /**
   * The durable rows blob. Backends call it exactly once, synchronously, inside `create` (a
   * snapshot handed over at create).
   */
  load: () => Uint8Array;
  /**
   * One committed batch, a notification: invoked strictly in `seq` order; every commit produced
   * while servicing an `open`/`call`/`drain` is delivered before that promise settles. Durability
   * is acknowledged later through `ack_durable`.
   */
  commit: (batch: Uint8Array) => void;
};

/** One opened bridge session. */
export type BridgeHandle = {
  /** Open with the encoded config; resolves with the encoded `OpenResponse`. */
  open: (config: Uint8Array) => Promise<Uint8Array>;
  /** Run one call envelope; resolves with the encoded `Response`. */
  call: (request: Uint8Array) => Promise<Uint8Array>;
  /** Drain queued session events; resolves with the encoded `DrainResponse`. */
  drain: () => Promise<Uint8Array>;
  /** Release the session. */
  close: () => Promise<void>;
};

/** A bridge backend. */
export type Backend = {
  /** Identifying label for diagnostics (never a secret). */
  readonly name: string;
  /** Create a session over `host`. */
  create: (host: BridgeHost) => BridgeHandle;
};
