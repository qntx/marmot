#!/usr/bin/env bun
/**
 * Size gate for the shipped `@qntx/marmot-wasm` artifact: prints the gzip -9 byte count of
 * dist/marmot_wasm_bg.wasm and fails above the budget. Also asserts the release artifact does not
 * export the test-only `triggerPanic` entry point.
 */

/// <reference types="node" />
import { existsSync, readFileSync } from "node:fs";
import { join, resolve } from "node:path";
import { gzipSync } from "node:zlib";

// lib is es2023 without DOM: declare the WebAssembly surface this gate uses.
declare const WebAssembly: {
  Module: {
    new (bytes: Uint8Array): unknown;
    exports: (module: unknown) => Array<{ name: string }>;
  };
};

const ROOT = resolve(import.meta.dirname, "..");
const WASM = join(ROOT, "packages/marmot-wasm/dist/marmot_wasm_bg.wasm");
const GZIP_BUDGET = 3_000_000;

if (!existsSync(WASM)) {
  console.error(`missing ${WASM} — run \`bun run --cwd packages/marmot-wasm build:wasm\``);
  process.exit(1);
}

const bytes = readFileSync(WASM);

const exports_ = WebAssembly.Module.exports(new WebAssembly.Module(bytes)).map(
  (entry: { name: string }) => entry.name,
);
if (exports_.includes("triggerPanic")) {
  console.error("release artifact unexpectedly exports triggerPanic");
  process.exit(1);
}

const gzip = gzipSync(bytes, { level: 9 }).byteLength;
console.log(`marmot_wasm_bg.wasm: ${bytes.byteLength} bytes raw, ${gzip} bytes gzip -9`);
if (gzip > GZIP_BUDGET) {
  console.error(`gzip budget exceeded: ${gzip} > ${GZIP_BUDGET}`);
  process.exit(1);
}
