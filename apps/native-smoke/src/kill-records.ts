/**
 * The out-of-process journal mirror of the browser kill test: every durable-commit batch and ack
 * is POSTed to the host collector through a serial chain so the host observes commit order, and
 * every publish reports its durability watermark before the fake relay sees the event.
 */

import { decodeKvBatch } from "@qntx/marmot/backend";
import type { KvDriver } from "@qntx/marmot/storage";

import { post } from "./collector.ts";
import { b64encode } from "./b64.ts";

export function recordingKv(
  inner: KvDriver,
  run: string,
): { kv: KvDriver; acked: () => bigint } {
  let lastAcked = 0n;
  let chain = Promise.resolve();
  return {
    acked: () => lastAcked,
    kv: {
      load: () => inner.load(),
      close: () => inner.close(),
      commit: (batch) => {
        // A serial chain, not an inline await: the host must observe commits in order.
        chain = chain.then(async () => {
          const { seq } = decodeKvBatch(batch);
          await post(`/${run}/journal`, { kind: "batch", seq: seq.toString(), bytes: b64encode(batch) });
          await inner.commit(batch);
          await post(`/${run}/journal`, { kind: "ack", seq: seq.toString() });
          if (seq > lastAcked) {
            lastAcked = seq;
          }
        });
        return chain;
      },
    },
  };
}
