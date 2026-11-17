/**
 * The engine port: what a bridge backend (the future mk-wasm shell, or a scripted fake in tests)
 * implements. Every call is synchronous and byte-oriented — the JSON protocol lives on both sides
 * of this seam.
 */

/** The storage callbacks the bridge invokes while running. */
export type BridgeHost = {
  /** Return the durable rows blob written by earlier `commit` calls. */
  load: () => Uint8Array;
  /**
   * Accept one committed batch. Called synchronously from inside the engine; the engine's
   * implementation forwards to the async `DurableKv` and acks once the batch is durable.
   */
  commit: (batch: Uint8Array) => void;
};

/** One opened bridge session. */
export type BridgeHandle = {
  /** Open with the encoded config; returns the encoded `OpenResponse`. */
  open: (config: Uint8Array) => Uint8Array;
  /** Run one call envelope; returns the encoded `Response`. */
  call: (request: Uint8Array) => Uint8Array;
  /** Drain queued session events; returns the encoded `DrainResponse`. */
  drain: () => Uint8Array;
  /** Release the session. */
  close: () => void;
};

/** A bridge backend: wasm today in tests is a scripted fake. */
export type Backend = {
  /** Identifying label for diagnostics (never a secret). */
  readonly name: string;
  /** Create a session over `host`. */
  create: (host: BridgeHost) => BridgeHandle;
};
