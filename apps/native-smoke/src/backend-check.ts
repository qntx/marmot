/**
 * A `Backend` wrapper asserting the port contract on every commit: strictly increasing seq per
 * handle, and a commit only ever arrives while one of that handle's open/call/drain promises is
 * still pending (the engine thread delivers every commit before the ABI call that produced it
 * returns, and Nitro dispatches JS callbacks FIFO before the call's promise resolves — a commit
 * landing with no pending call is a violation).
 *
 * Violations are RECORDED, not thrown: throwing inside the JS commit callback would be swallowed
 * by the async dispatch and silently poison the run — the recorded list surfaces in the result
 * instead. `journal` streams backend ops to the collector so a stalled run shows its last op.
 */

import { decodeKvBatch } from "@qntx/marmot/backend";
import type { Backend, BridgeHandle, BridgeHost } from "@qntx/marmot/backend";

export type BackendCheck = {
  backend: Backend;
  violations: () => string[];
};

export function checkingBackend(
  inner: Backend,
  journal?: (event: Record<string, unknown>) => Promise<void>,
): BackendCheck {
  const violations: string[] = [];
  const emit = (event: Record<string, unknown>): void => {
    void journal?.(event).catch(() => undefined);
  };
  let handleId = 0;
  return {
    violations: () => violations,
    backend: {
      name: inner.name,
      create(host: BridgeHost): BridgeHandle {
        const id = ++handleId;
        let lastSeq = -1n;
        let pending = 0;
        const guardedHost: BridgeHost = {
          load: () => host.load(),
          commit: (batch) => {
            const { seq } = decodeKvBatch(batch);
            if (seq <= lastSeq) {
              const text = `handle ${id}: commit seq ${seq} not increasing beyond ${lastSeq}`;
              violations.push(text);
              emit({ kind: "violation", text });
            }
            if (pending === 0) {
              const text = `handle ${id}: commit seq ${seq} with no call pending`;
              violations.push(text);
              emit({ kind: "violation", text });
            }
            if (seq > lastSeq) {
              lastSeq = seq;
            }
            emit({ kind: "backend", op: "commit", handle: id, seq: seq.toString() });
            host.commit(batch);
          },
        };
        const handle = inner.create(guardedHost);
        const track = <T>(op: string, promise: Promise<T>): Promise<T> => {
          pending += 1;
          emit({ kind: "backend", op, handle: id, phase: "start" });
          return promise.finally(() => {
            pending -= 1;
            emit({ kind: "backend", op, handle: id, phase: "settled" });
          });
        };
        const requestType = (request: Uint8Array): string => {
          try {
            const parsed = JSON.parse(new TextDecoder().decode(request)) as { type?: string };
            return parsed.type ?? "?";
          } catch {
            return "?";
          }
        };
        return {
          open: (config) => track("open", handle.open(config)),
          call: (request) => track(`call:${requestType(request)}`, handle.call(request)),
          drain: () => track("drain", handle.drain()),
          close: () => {
            emit({ kind: "backend", op: "close", handle: id, phase: "start" });
            return handle.close().finally(() => {
              emit({ kind: "backend", op: "close", handle: id, phase: "settled" });
            });
          },
        };
      },
    },
  };
}
