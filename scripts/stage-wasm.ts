#!/usr/bin/env bun
/**
 * Stage the `@qntx/marmot-wasm` artifact into dist/: copy src/generated/marmot_wasm_bg.wasm next to
 * the bundled glue (dist/index.mjs) and prove the two come from one build — every `wasm.<name>` the
 * glue calls must be a module export, and every module import must be defined by the glue. Runs as
 * `prepack`, so a tarball can never ship an unpaired (or missing) wasm.
 */

/// <reference types="node" />
import { copyFileSync, existsSync, readFileSync } from "node:fs";
import { join, resolve } from "node:path";

// lib is es2023 without DOM: declare the WebAssembly surface this gate uses.
declare const WebAssembly: {
  Module: {
    new (bytes: Uint8Array): unknown;
    exports: (module: unknown) => Array<{ name: string }>;
    imports: (module: unknown) => Array<{ name: string }>;
  };
};

const ROOT = resolve(import.meta.dirname, "..");
const PACKAGE = join(ROOT, "packages/marmot-wasm");
const SRC_WASM = join(PACKAGE, "src/generated/marmot_wasm_bg.wasm");
const DIST_GLUE = join(PACKAGE, "dist/index.mjs");
const DIST_WASM = join(PACKAGE, "dist/marmot_wasm_bg.wasm");

/**
 * Names where the glue and the wasm disagree: `wasm.<name>` references in the glue the module does
 * not export, then module import names the glue never defines. An empty list means the pair comes
 * from one build (a foreign pair mismatches at least on the per-build `wasm_bindgen_<hash>` closure
 * shims).
 */
export function unpairedNames(
  glue: string,
  exports: ReadonlySet<string>,
  imports: ReadonlySet<string>,
): string[] {
  const missingExports = new Set<string>();
  for (const [, name = ""] of glue.matchAll(/\bwasm\.([A-Za-z_$][A-Za-z0-9_$]*)/g)) {
    if (!exports.has(name)) {
      missingExports.add(name);
    }
  }
  const missingImports = [...imports].filter((name) => !glue.includes(name));
  return [...missingExports, ...missingImports];
}

if (import.meta.main) {
  let missing = false;
  for (const path of [SRC_WASM, DIST_GLUE]) {
    if (!existsSync(path)) {
      console.error(
        `stage-wasm: missing ${path} — run \`bun run build:wasm\`, or download the CI artifact into src/generated`,
      );
      missing = true;
    }
  }
  if (missing) {
    process.exit(1);
  }

  copyFileSync(SRC_WASM, DIST_WASM);

  const module = new WebAssembly.Module(readFileSync(DIST_WASM));
  const exports = new Set(
    WebAssembly.Module.exports(module).map((entry: { name: string }) => entry.name),
  );
  const imports = new Set(
    WebAssembly.Module.imports(module).map((entry: { name: string }) => entry.name),
  );
  const unpaired = unpairedNames(readFileSync(DIST_GLUE, "utf8"), exports, imports);
  if (unpaired.length > 0) {
    console.error("stage-wasm: dist glue and wasm are not from the same build:");
    for (const name of unpaired) {
      console.error(`  ${name}`);
    }
    console.error("rerun `bun run build:wasm`");
    process.exit(1);
  }
  console.log("stage-wasm: staged paired glue + wasm into dist/");
}
