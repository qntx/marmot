#!/usr/bin/env bun
/**
 * `check:generated` — the committed wasm-bindgen glue must equal fresh output, modulo the per-build
 * closure-shim hash. `wasm_bindgen_<16 hex>` export names carry a descriptor hash that is not
 * reproducible across machines (the wasm itself is not byte-identical either — e.g. secp256k1 C
 * objects differ per host clang), so both sides are normalized before comparison. Every other
 * difference — exports, signatures, imports, comments — still fails.
 */

import { spawnSync } from "node:child_process";
import { readdirSync, readFileSync } from "node:fs";
import { basename, join, resolve } from "node:path";

const ROOT = resolve(import.meta.dirname, "..");
const GLUE_REL = "packages/marmot-wasm/src/generated";
const GLUE = join(ROOT, GLUE_REL);

const normalize = (text: string): string =>
  text.replaceAll(/wasm_bindgen_[0-9a-f]{16}/g, "wasm_bindgen_SHIM_HASH");

function run(command: string[]): string {
  const result = spawnSync(command[0] ?? "", command.slice(1), {
    cwd: ROOT,
    encoding: "utf8",
    stdio: ["ignore", "pipe", "inherit"],
  });
  if (result.status !== 0) {
    throw new Error(`command failed (${result.status ?? "signal"}): ${command.join(" ")}`);
  }
  return result.stdout;
}

run(["bun", "scripts/build-wasm.ts", "--glue-only"]);

const tracked = run(["git", "ls-files", GLUE_REL])
  .split("\n")
  .filter((line) => line !== "");

let failed = false;
for (const relative of tracked) {
  const committed = run(["git", "show", `HEAD:${relative}`]);
  const current = readFileSync(join(ROOT, relative), "utf8");
  if (normalize(committed) !== normalize(current)) {
    console.error(`check:generated: ${relative} differs from fresh output`);
    failed = true;
  }
}

// A file wasm-bindgen newly emits that nobody committed would otherwise go unchecked.
const ignored = new Set(["marmot_wasm_bg.wasm"]);
const committed = new Set(tracked.map((path) => basename(path)));
for (const file of readdirSync(GLUE)) {
  if (!committed.has(file) && !ignored.has(file)) {
    console.error(`check:generated: ${GLUE_REL}/${file} is generated but not committed`);
    failed = true;
  }
}

if (failed) {
  throw new Error("committed glue is stale — run `bun run build:wasm` and commit src/generated");
}
console.log("check:generated: glue is current (shim-hash normalized)");
