/**
 * The durability checks, ported one-for-one from packages/marmot-wasm/e2e/kill.spec.ts: durable
 * rows must equal apply(batches[1..k]) for some prefix k, k must cover the last acknowledged seq,
 * and every published event id must be durable inside a value of some batch at or under its
 * watermark. `gap` = how many recorded batches never reached disk (expected under SIGKILL).
 */

import { decodeKvBatch } from "@qntx/marmot/backend";
import type { KvBatch } from "@qntx/marmot/backend";

import type { DumpRecord, RunJournal } from "./collector.ts";

/* eslint-disable-next-line */
function mulberry32(seed: number): () => number {
  let state = seed | 0;
  return () => {
    state = (state + 0x6d2b79f5) | 0;
    let t = Math.imul(state ^ (state >>> 15), 1 | state);
    t ^= t + Math.imul(t ^ (t >>> 7), 61 | t);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}
export { mulberry32 };

function contains(haystack: Uint8Array, needle: Uint8Array): boolean {
  outer: for (let i = 0; i + needle.length <= haystack.length; i += 1) {
    for (let j = 0; j < needle.length; j += 1) {
      if (haystack[i + j] !== needle[j]) {
        continue outer;
      }
    }
    return true;
  }
  return false;
}

/** Whether `buf` contains `id` as ASCII hex or raw bytes (mirrors `contains_id` in the suite). */
function containsId(buf: Uint8Array, idHex: string): boolean {
  const raw = new Uint8Array(idHex.length / 2);
  for (let i = 0; i < raw.length; i += 1) {
    raw[i] = Number.parseInt(idHex.slice(i * 2, i * 2 + 2), 16);
  }
  return contains(buf, new TextEncoder().encode(idHex)) || contains(buf, raw);
}

function applyAll(batches: KvBatch[]): Map<string, Uint8Array>[] {
  const current = new Map<string, Uint8Array>();
  const prefixes: Map<string, Uint8Array>[] = [new Map(current)];
  for (const batch of batches) {
    for (const op of batch.ops) {
      const key = Buffer.from(op.key).toString("base64");
      if (op.type === "put") {
        current.set(key, op.value);
      } else {
        current.delete(key);
      }
    }
    prefixes.push(new Map(current));
  }
  return prefixes;
}

function mapsEqual(a: Map<string, Uint8Array>, b: Map<string, Uint8Array>): boolean {
  if (a.size !== b.size) {
    return false;
  }
  for (const [key, value] of a) {
    const other = b.get(key);
    if (other === undefined || !Buffer.from(value).equals(Buffer.from(other))) {
      return false;
    }
  }
  return true;
}

export type IterationReport = {
  iteration: number;
  batches: number;
  lastAcked: string;
  durablePrefix: number;
  prefixSeq: string;
  published: number;
  gap: number;
};

/** Run the kill.spec checks on one iteration's journal + decrypted dump; throws on violation. */
export function checkIteration(
  iteration: number,
  journal: RunJournal,
  rows: DumpRecord,
): IterationReport {
  const decoded: KvBatch[] = journal.batches.map(({ bytes }) =>
    decodeKvBatch(new Uint8Array(Buffer.from(bytes, "base64"))),
  );
  for (let j = 1; j < decoded.length; j += 1) {
    const prev = decoded[j - 1]?.seq ?? 0n;
    const seq = decoded[j]?.seq ?? 0n;
    if (seq <= prev) {
      throw new Error(`iteration ${iteration}: batch seq ${seq} not increasing beyond ${prev}`);
    }
  }
  const store = new Map<string, Uint8Array>(
    rows.map((row) => [row.key, new Uint8Array(Buffer.from(row.value, "base64"))]),
  );
  const prefixes = applyAll(decoded);
  let k = -1;
  for (let j = 0; j < prefixes.length; j += 1) {
    const prefix = prefixes[j];
    if (prefix !== undefined && mapsEqual(prefix, store)) {
      k = j;
    }
  }
  const lastAcked = journal.acked.length > 0 ? BigInt(journal.acked.at(-1) ?? "0") : 0n;
  if (k < 0) {
    throw new Error(`iteration ${iteration}: durable rows match no batch prefix`);
  }
  const kSeq = k > 0 ? (decoded[k - 1]?.seq ?? 0n) : 0n;
  if (kSeq < lastAcked) {
    throw new Error(
      `iteration ${iteration}: durable prefix seq ${kSeq} does not cover last acked ${lastAcked}`,
    );
  }
  for (const { eventId, watermark } of journal.published) {
    const limit = BigInt(watermark);
    let found = false;
    for (const batch of decoded) {
      if (batch.seq > limit) {
        break;
      }
      for (const op of batch.ops) {
        if (op.type === "put" && containsId(op.value, eventId)) {
          found = true;
        }
      }
    }
    if (!found) {
      throw new Error(
        `iteration ${iteration}: published ${eventId} not durable by watermark ${watermark}`,
      );
    }
  }
  return {
    iteration,
    batches: decoded.length,
    lastAcked: lastAcked.toString(),
    durablePrefix: k,
    prefixSeq: kSeq.toString(),
    published: journal.published.length,
    gap: decoded.length - k,
  };
}
