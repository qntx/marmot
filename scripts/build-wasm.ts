#!/usr/bin/env bun
/**
 * Build the `@qntx/marmot-wasm` artifact: cargo → wasm-bindgen → wasm-opt.
 *
 * Release `cargo build -p mk-wasm --profile wasm --target wasm32-unknown-unknown`, glue →
 * packages/marmot-wasm/src/generated (committed); the .wasm lands next to it, gitignored, and is
 * staged into dist/ by scripts/stage-wasm.ts (prepack). test-panic `cargo build -p mk-wasm
 * --profile wasm-test --features test-panic`, glue + .wasm → packages/marmot-wasm/tests/generated
 * (gitignored).
 *
 * Flags: --glue-only emit only the release glue to target/wasm-glue — no wasm-opt, no test-panic
 * build, src/generated untouched (used by check:generated, which must not clobber the release
 * artifact) --measure build all four size variants (opt-level z/s × wasm-opt on/off) and log raw /
 * gzip -9 / brotli -q 11 bytes to target/wasm-sizes.log
 *
 * The wasm-bindgen CLI is pinned to the Cargo.lock `wasm-bindgen` version: it is installed under
 * target/wasm-bindgen-cli and the script fails on a version mismatch rather than emitting
 * incompatible glue.
 */

/// <reference types="node" />
import { spawnSync } from "node:child_process";
import { existsSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { join, resolve } from "node:path";
import { brotliCompressSync, constants, gzipSync } from "node:zlib";

const ROOT = resolve(import.meta.dirname, "..");
const PACKAGE = join(ROOT, "packages/marmot-wasm");
const GLUE = join(PACKAGE, "src/generated");
const GLUE_ONLY_DIR = join(ROOT, "target/wasm-glue");
const TEST_GLUE = join(PACKAGE, "tests/generated");
const CLI_ROOT = join(ROOT, "target/wasm-bindgen-cli");
const WASM_TARGET = "wasm32-unknown-unknown";

/** Shipped configuration, chosen by `--measure` (see target/wasm-sizes.log). */
const SHIP_OPT_LEVEL = "z";
const SHIP_WASM_OPT = true;

const WASM_OPT_FEATURES = [
  "--enable-mutable-globals",
  "--enable-sign-ext",
  "--enable-bulk-memory",
  "--enable-nontrapping-float-to-int",
  "--enable-reference-types",
];

function run(command: string[], options?: { env?: Record<string, string> }): void {
  const result = spawnSync(command[0] ?? "", command.slice(1), {
    cwd: ROOT,
    env: { ...process.env, ...options?.env },
    stdio: "inherit",
  });
  if (result.status !== 0) {
    throw new Error(`command failed (${result.status ?? "signal"}): ${command.join(" ")}`);
  }
}

function capture(command: string[]): string {
  const result = spawnSync(command[0] ?? "", command.slice(1), {
    cwd: ROOT,
    encoding: "utf8",
  });
  if (result.status !== 0) {
    throw new Error(
      `command failed (${result.status ?? "signal"}): ${command.join(" ")}\n${result.stderr}`,
    );
  }
  return result.stdout.trim();
}

/** The `wasm-bindgen` version recorded in Cargo.lock — the CLI must match it exactly. */
function lockfileBindgenVersion(): string {
  const lock = readFileSync(join(ROOT, "Cargo.lock"), "utf8");
  const match = /name = "wasm-bindgen"\nversion = "([^"]+)"/.exec(lock);
  if (match?.[1] === undefined) {
    throw new Error("Cargo.lock: wasm-bindgen entry not found");
  }
  return match[1];
}

function ensureBindgen(version: string): string {
  const bin = join(CLI_ROOT, "bin/wasm-bindgen");
  if (!existsSync(bin)) {
    console.log(`installing wasm-bindgen-cli ${version} into target/wasm-bindgen-cli`);
    run([
      "cargo",
      "install",
      "wasm-bindgen-cli",
      "--version",
      version,
      "--locked",
      "--root",
      CLI_ROOT,
    ]);
  }
  const found = capture([bin, "--version"]).replace(/^wasm-bindgen\s+/, "");
  if (found !== version) {
    throw new Error(
      `wasm-bindgen CLI ${found} != Cargo.lock wasm-bindgen ${version}; ` +
        `rebuild target/wasm-bindgen-cli`,
    );
  }
  return bin;
}

type BuildSpec = {
  profile: string;
  features: string[];
  optLevel?: string;
};

function cargoBuild(spec: BuildSpec): string {
  const args = [
    "cargo",
    "build",
    "-p",
    "mk-wasm",
    "--target",
    WASM_TARGET,
    "--profile",
    spec.profile,
  ];
  if (spec.features.length > 0) {
    args.push("--features", spec.features.join(","));
  }
  const env =
    spec.optLevel === undefined
      ? undefined
      : { env: { CARGO_PROFILE_WASM_OPT_LEVEL: spec.optLevel } };
  run(args, env);
  return join(ROOT, `target/${WASM_TARGET}/${spec.profile}/marmot_wasm.wasm`);
}

function bindgen(cli: string, wasm: string, outDir: string): string {
  mkdirSync(outDir, { recursive: true });
  run([cli, "--target", "web", "--out-dir", outDir, wasm]);
  return join(outDir, "marmot_wasm_bg.wasm");
}

function wasmOpt(wasm: string): void {
  const bin = join(PACKAGE, "node_modules/.bin/wasm-opt");
  if (!existsSync(bin)) {
    throw new Error("wasm-opt not found: run `bun install` (binaryen devDependency)");
  }
  run([bin, "-Oz", ...WASM_OPT_FEATURES, wasm, "-o", wasm]);
}

function sizeLine(wasm: string): string {
  const bytes = readFileSync(wasm);
  const raw = bytes.byteLength;
  const gzip = gzipSync(bytes, { level: 9 }).byteLength;
  const brotli = brotliCompressSync(bytes, {
    params: { [constants.BROTLI_PARAM_QUALITY]: 11 },
  }).byteLength;
  return `raw=${raw} gzip-9=${gzip} brotli-11=${brotli}`;
}

function measure(cli: string): void {
  const log: string[] = [`wasm size matrix (${new Date().toISOString()})`, ""];
  for (const optLevel of ["z", "s"]) {
    const artifact = cargoBuild({ profile: "wasm", features: [], optLevel });
    const scratch = join(ROOT, `target/wasm-measure/${optLevel}`);
    const glued = bindgen(cli, artifact, scratch);
    log.push(`opt-level ${optLevel}, no wasm-opt: ${sizeLine(glued)}`);
    const optimized = join(scratch, "opt.wasm");
    writeFileSync(optimized, readFileSync(glued));
    wasmOpt(optimized);
    log.push(`opt-level ${optLevel}, wasm-opt -Oz: ${sizeLine(optimized)}`, "");
  }
  const path = join(ROOT, "target/wasm-sizes.log");
  writeFileSync(path, `${log.join("\n")}\n`);
  console.log(log.join("\n"));
  console.log(`wrote ${path}`);
}

const args = new Set(process.argv.slice(2));
const glueOnly = args.has("--glue-only");
const measureOnly = args.has("--measure");

const version = lockfileBindgenVersion();
const cli = ensureBindgen(version);

if (measureOnly) {
  measure(cli);
  process.exit(0);
}

// Glue-only: scratch output check:generated diffs against the committed glue —
// the release artifact under src/generated stays untouched.
if (glueOnly) {
  const artifact = cargoBuild({ profile: "wasm", features: [], optLevel: SHIP_OPT_LEVEL });
  bindgen(cli, artifact, GLUE_ONLY_DIR);
  console.log(`glue-only output in ${GLUE_ONLY_DIR}`);
  process.exit(0);
}

// Release variant: glue is committed, so it must be byte-identical every run.
const artifact = cargoBuild({ profile: "wasm", features: [], optLevel: SHIP_OPT_LEVEL });
const glued = bindgen(cli, artifact, GLUE);
if (SHIP_WASM_OPT) {
  wasmOpt(glued);
}
console.log(`release glue + wasm in ${GLUE}`);

// Test-panic variant: fast profile, separate output dir, never shipped.
const testArtifact = cargoBuild({
  profile: "wasm-test",
  features: ["test-panic"],
});
bindgen(cli, testArtifact, TEST_GLUE);
console.log(`test-panic glue + wasm in ${TEST_GLUE}`);
