/**
 * `CommitChain` — the serial commit discipline the KV drivers share: batches apply strictly in
 * order, a failed batch is terminal (`io` for every later commit — a lost batch may be a dependency
 * of every later one), `closed` rejects commits issued after `close`, and batches queued before
 * `close` still apply because `close` waits on the chain.
 */

import { StorageError } from "./driver.ts";

export class CommitChain {
  private chain: Promise<void> = Promise.resolve();
  private poisoned = false;
  private closed = false;

  get isClosed(): boolean {
    return this.closed;
  }

  /** Queue `apply(batch)` behind every earlier commit; resolves when the batch is durable. */
  async commit(batch: Uint8Array, apply: (batch: Uint8Array) => Promise<void>): Promise<void> {
    if (this.closed) {
      throw new StorageError("closed");
    }
    const prior = this.chain;
    const run = (async () => {
      await prior;
      if (this.poisoned) {
        throw new StorageError("io");
      }
      try {
        await apply(batch);
      } catch (error) {
        this.poisoned = true;
        throw error instanceof StorageError ? error : new StorageError("io");
      }
    })();
    this.chain = (async () => {
      try {
        await run;
      } catch {
        // The rejection is delivered to the caller; the chain moves on.
      }
    })();
    return run;
  }

  /** Mark closed and wait for every queued commit to settle; idempotent. */
  async close(): Promise<void> {
    if (this.closed) {
      return;
    }
    this.closed = true;
    await this.chain;
  }
}
