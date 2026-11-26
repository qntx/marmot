/**
 * Decoder property tests: a seeded PRNG feeds random bytes and byte-level mutations of valid
 * encodings (valid rows/batches plus the canonical bridge vectors) to every decoder that parses
 * host-produced bytes. Contract: kv/codec decoders either return or throw `DecodeError` — never
 * another exception type, never a hang; accepted rows/batches re-encode byte-identically;
 * `decodeClientState` never throws and every accepted state is well-formed.
 */

import { readFileSync } from "node:fs";
import { join } from "node:path";

import { describe, expect, test } from "vite-plus/test";

import {
  decodeDrain,
  decodeOpenResponse,
  decodeResponse,
  encodeDrainResponse,
} from "../src/backend/codec.ts";
import { DecodeError } from "../src/backend/errors.ts";
import { decodeKvBatch, decodeRows, encodeKvBatch, encodeRows } from "../src/backend/kv.ts";
import type { KvBatch, Rows } from "../src/backend/kv.ts";
import type { DrainResponse } from "../src/backend/protocol.ts";
import { utf8Encode } from "../src/backend/utf8.ts";
import { decodeClientState, encodeClientState } from "../src/client/state.ts";
import type { ClientState } from "../src/client/state.ts";

const VECTORS = join(import.meta.dirname, "..", "..", "..", "vectors", "bridge");

/** Deterministic Park–Miller so a failure replays identically. */
function prng(seed: number): () => number {
  let state = seed % 2147483647;
  if (state <= 0) {
    state += 2147483646;
  }
  return () => {
    state = (state * 16807) % 2147483647;
    return (state - 1) / 2147483646;
  };
}

function at<T>(list: T[], idx: number): T {
  const item = list[idx];
  if (item === undefined) {
    throw new Error("index out of range");
  }
  return item;
}

function vector(name: string): Uint8Array[] {
  const parsed: unknown = JSON.parse(readFileSync(join(VECTORS, `${name}.json`), "utf8"));
  if (typeof parsed !== "object" || parsed === null) {
    throw new Error(`${name}.json is not a variant map`);
  }
  return Object.values(parsed).map((value) => utf8Encode(JSON.stringify(value)));
}

const randomBytes = (pick: (n: number) => number, length: number): Uint8Array =>
  Uint8Array.from({ length }, () => pick(256));

/** One to three byte-level mutations: flip, insert, delete or truncate. */
function mutate(pick: (n: number) => number, bytes: Uint8Array): Uint8Array {
  const out = [...bytes];
  const rounds = 1 + pick(3);
  for (let r = 0; r < rounds; r += 1) {
    const op = pick(4);
    if (op === 0 && out.length > 0) {
      const i = pick(out.length);
      out[i] = (out[i] ?? 0) + 1 + pick(255);
    } else if (op === 1) {
      out.splice(out.length === 0 ? 0 : pick(out.length), 0, pick(256));
    } else if (op === 2 && out.length > 0) {
      out.splice(pick(out.length), 1);
    } else if (out.length > 0) {
      out.length = pick(out.length);
    }
  }
  return Uint8Array.from(out);
}

/** Every third input is raw random bytes; the rest are mutations of a valid encoding. */
function source(
  pick: (n: number) => number,
  valid: Uint8Array[],
  i: number,
  max: number,
): Uint8Array {
  return pick(3) === 0 ? randomBytes(pick, pick(max)) : mutate(pick, at(valid, i % valid.length));
}

type Outcome<T> = { ok: true; value: T } | { ok: false; error: unknown };

function capture<T>(decode: () => T): Outcome<T> {
  try {
    return { ok: true, value: decode() };
  } catch (error) {
    return { ok: false, error };
  }
}

function record<T>(
  outcome: Outcome<T>,
  input: Uint8Array,
  accepted: Array<[Uint8Array, T]>,
  errors: unknown[],
): void {
  if (outcome.ok) {
    accepted.push([input, outcome.value]);
  } else {
    errors.push(outcome.error);
  }
}

function kvOp(pick: (n: number) => number, i: number): KvBatch["ops"][number] {
  return i % 2 === 0
    ? {
        type: "put" as const,
        key: randomBytes(pick, 1 + pick(8)),
        value: randomBytes(pick, pick(32)),
      }
    : { type: "delete" as const, key: randomBytes(pick, 1 + pick(8)) };
}

const nonNegativeInteger = (value: number): boolean => Number.isInteger(value) && value >= 0;

/** `undefined` (rejected input) or a state whose fields are all well-formed. */
const wellFormedState = (state: ClientState | undefined): boolean =>
  state === undefined ||
  (Object.values(state.cursors).every(nonNegativeInteger) &&
    Object.values(state.floors).every(nonNegativeInteger) &&
    (state.inbox === "client" || state.inbox === "host") &&
    state.inboxRelays.every((relay) => typeof relay === "string"));

const ROW_SEED = 0x5eed01;
const BATCH_SEED = 0x5eed02;
const STATE_SEED = 0x5eed03;
const CODEC_SEED = 0x5eed04;
const ITERATIONS = 400;

describe("decoder fuzz", () => {
  test("decodeRows throws only DecodeError and accepted rows re-encode identically", () => {
    const rand = prng(ROW_SEED);
    const pick = (n: number): number => Math.floor(rand() * n);
    const valid = Array.from({ length: 8 }, (_v, i) =>
      encodeRows(
        Array.from({ length: 1 + pick(6) }, (_r, j): [Uint8Array, Uint8Array] => [
          randomBytes(pick, 1 + pick(8) + i),
          randomBytes(pick, pick(32) + j),
        ]),
      ),
    );
    const accepted: Array<[Uint8Array, Rows]> = [];
    const errors: unknown[] = [];
    for (let i = 0; i < ITERATIONS; i += 1) {
      const input = source(pick, valid, i, 64);
      record(
        capture(() => decodeRows(input)),
        input,
        accepted,
        errors,
      );
    }
    expect(errors.every((error) => error instanceof DecodeError)).toBe(true);
    expect(accepted.map(([, rows]) => encodeRows(rows))).toStrictEqual(
      accepted.map(([input]) => input),
    );
    expect(accepted.length).toBeGreaterThan(0);
    expect(accepted.length + errors.length).toBe(ITERATIONS);
  });

  test("decodeKvBatch throws only DecodeError and accepted batches re-encode identically", () => {
    const rand = prng(BATCH_SEED);
    const pick = (n: number): number => Math.floor(rand() * n);
    const valid = Array.from({ length: 8 }, (_v, i) =>
      encodeKvBatch({
        seq: BigInt(i),
        ops: Array.from({ length: 1 + pick(6) }, (_r, j) => kvOp(pick, j + i)),
      }),
    );
    const accepted: Array<[Uint8Array, KvBatch]> = [];
    const errors: unknown[] = [];
    for (let i = 0; i < ITERATIONS; i += 1) {
      const input = source(pick, valid, i, 64);
      record(
        capture(() => decodeKvBatch(input)),
        input,
        accepted,
        errors,
      );
    }
    expect(errors.every((error) => error instanceof DecodeError)).toBe(true);
    expect(accepted.map(([, batch]) => encodeKvBatch(batch))).toStrictEqual(
      accepted.map(([input]) => input),
    );
    expect(accepted.length).toBeGreaterThan(0);
    expect(accepted.length + errors.length).toBe(ITERATIONS);
  });

  test("decodeClientState never throws and accepted states are well-formed", () => {
    const rand = prng(STATE_SEED);
    const pick = (n: number): number => Math.floor(rand() * n);
    const valid = Array.from({ length: 4 }, () =>
      encodeClientState({
        cursors: { [`group|${pick(100)}|r`]: pick(2_000_000_000) },
        floors: { [`inbox|wss://r${pick(9)}.example`]: pick(2_000_000_000) },
        inbox: "client",
        inboxRelays: [`wss://r${pick(9)}.example`],
        nip44Extended: true,
      }),
    );
    const states = Array.from({ length: ITERATIONS }, (_v, i) =>
      decodeClientState(source(pick, valid, i, 64)),
    );
    expect(states).toHaveLength(ITERATIONS);
    expect(states.every(wellFormedState)).toBe(true);
  });

  test("bridge response decoders throw only DecodeError", () => {
    const rand = prng(CODEC_SEED);
    const pick = (n: number): number => Math.floor(rand() * n);
    const drains: DrainResponse[] = [
      { type: "events", events: [] },
      { type: "events", events: [{ type: "group_joined", group: "a".repeat(64) }] },
    ];
    const wireDrain = (r: DrainResponse): Uint8Array =>
      utf8Encode(JSON.stringify(encodeDrainResponse(r)));
    const pools: Array<{ decode: (b: Uint8Array) => unknown; valid: Uint8Array[] }> = [
      { decode: decodeOpenResponse, valid: vector("open_response") },
      { decode: decodeResponse, valid: vector("response") },
      { decode: decodeDrain, valid: drains.map(wireDrain) },
    ];
    const accepted: Array<[Uint8Array, unknown]> = [];
    const errors: unknown[] = [];
    for (let i = 0; i < ITERATIONS; i += 1) {
      const pool = at(pools, i % pools.length);
      const input = source(pick, pool.valid, i, 256);
      record(
        capture(() => pool.decode(input)),
        input,
        accepted,
        errors,
      );
    }
    expect(errors.every((error) => error instanceof DecodeError)).toBe(true);
    expect(accepted.length + errors.length).toBe(ITERATIONS);
  });
});
