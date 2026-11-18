/**
 * The durable-rows codec the bridge's `Host::load` consumes and the mk-core `KvBatch` codec the
 * host must persist, both mirrored byte-for-byte.
 *
 * Rows layout: `[version u8 = 1][count u32 BE]` then per row `[key len u32 BE][key][value len u32
 * BE][value]`.
 *
 * Batch layout: `[version u8 = 1][seq u64 BE][op count u32 BE]` then per op `[tag u8][key len u32
 * BE][key]` with `tag 0` (put) adding `[value len u32 BE][value]`; `tag 1` is delete.
 */

import { DecodeError } from "./errors.ts";

/** One decoded key/value row. */
export type Row = readonly [Uint8Array, Uint8Array];

/** The durable rows a host persists, in store order. */
export type Rows = ReadonlyArray<Row>;

/**
 * What the engine asks a storage driver to provide: `commit` resolves only once the batch is
 * durable. Batches arrive strictly in `seq` order.
 */
export type DurableKv = {
  /** The rows durable at open. */
  load: () => Promise<Rows>;
  /** Persist one committed batch; resolves when durable. */
  commit: (batch: Uint8Array) => Promise<void>;
};

const ROWS_VERSION = 1;
const BATCH_VERSION = 1;

class Cursor {
  private pos = 0;
  private readonly bytes: Uint8Array;

  constructor(bytes: Uint8Array) {
    this.bytes = bytes;
  }

  take(length: number, path: string): Uint8Array {
    const end = this.pos + length;
    if (end > this.bytes.length || end < this.pos) {
      throw new DecodeError(path, "truncated");
    }
    const slice = this.bytes.subarray(this.pos, end);
    this.pos = end;
    return slice;
  }

  u8(path: string): number {
    return this.take(1, path)[0] ?? 0;
  }

  u32(path: string): number {
    const slice = this.take(4, path);
    return new DataView(slice.buffer, slice.byteOffset, 4).getUint32(0);
  }

  u64(path: string): bigint {
    let out = 0n;
    for (const byte of this.take(8, path)) {
      out = out * 256n + BigInt(byte);
    }
    return out;
  }

  get done(): boolean {
    return this.pos === this.bytes.length;
  }
}

function u32be(value: number): Uint8Array {
  const out = new Uint8Array(4);
  new DataView(out.buffer).setUint32(0, value);
  return out;
}

function concat(parts: Uint8Array[]): Uint8Array {
  let total = 0;
  for (const part of parts) {
    total += part.length;
  }
  const out = new Uint8Array(total);
  let at = 0;
  for (const part of parts) {
    out.set(part, at);
    at += part.length;
  }
  return out;
}

/** Encode `rows` in the versioned format `BridgeHost.load` returns. */
export function encodeRows(rows: Rows): Uint8Array {
  const parts: Uint8Array[] = [Uint8Array.of(ROWS_VERSION), u32be(rows.length)];
  for (const [key, value] of rows) {
    parts.push(u32be(key.length), key, u32be(value.length), value);
  }
  return concat(parts);
}

/** Decode a rows blob produced by `encodeRows`. */
export function decodeRows(bytes: Uint8Array): Rows {
  const cursor = new Cursor(bytes);
  if (cursor.u8("rows.version") !== ROWS_VERSION) {
    throw new DecodeError("rows.version", "version");
  }
  const count = cursor.u32("rows.count");
  const rows: Array<[Uint8Array, Uint8Array]> = [];
  for (let i = 0; i < count; i += 1) {
    const key = cursor.take(cursor.u32(`rows[${i}].key.len`), `rows[${i}].key`);
    const value = cursor.take(cursor.u32(`rows[${i}].value.len`), `rows[${i}].value`);
    rows.push([key, value]);
  }
  if (!cursor.done) {
    throw new DecodeError("rows", "trailing");
  }
  return rows;
}

/** One decoded batch op. */
export type KvOp =
  | { type: "put"; key: Uint8Array; value: Uint8Array }
  | { type: "delete"; key: Uint8Array };

/** A decoded `KvBatch`. */
export type KvBatch = {
  seq: bigint;
  ops: KvOp[];
};

/** Decode one committed batch with mk-core strictness. */
export function decodeKvBatch(bytes: Uint8Array): KvBatch {
  const cursor = new Cursor(bytes);
  if (cursor.u8("batch.version") !== BATCH_VERSION) {
    throw new DecodeError("batch.version", "version");
  }
  const seq = cursor.u64("batch.seq");
  const opCount = cursor.u32("batch.ops.count");
  if (opCount === 0) {
    throw new DecodeError("batch.ops", "empty");
  }
  const ops: KvOp[] = [];
  for (let i = 0; i < opCount; i += 1) {
    const path = `batch.ops[${i}]`;
    const tag = cursor.u8(`${path}.tag`);
    if (tag !== 0 && tag !== 1) {
      throw new DecodeError(`${path}.tag`, "value");
    }
    const key = cursor.take(cursor.u32(`${path}.key.len`), `${path}.key`);
    if (tag === 0) {
      const value = cursor.take(cursor.u32(`${path}.value.len`), `${path}.value`);
      ops.push({ type: "put", key, value });
    } else {
      ops.push({ type: "delete", key });
    }
  }
  if (!cursor.done) {
    throw new DecodeError("batch", "trailing");
  }
  return { seq, ops };
}

/** Encode one `KvBatch`; used by test drivers and fake backends. */
export function encodeKvBatch(batch: KvBatch): Uint8Array {
  const header = new Uint8Array(9);
  header[0] = BATCH_VERSION;
  new DataView(header.buffer).setBigUint64(1, batch.seq);
  const parts: Uint8Array[] = [header, u32be(batch.ops.length)];
  for (const op of batch.ops) {
    if (op.type === "put") {
      parts.push(Uint8Array.of(0), u32be(op.key.length), op.key, u32be(op.value.length), op.value);
    } else {
      parts.push(Uint8Array.of(1), u32be(op.key.length), op.key);
    }
  }
  return concat(parts);
}
