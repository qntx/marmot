/// <reference types="node" />
import { spawnSync } from "node:child_process";
import { existsSync, readdirSync, readFileSync, writeFileSync } from "node:fs";
import { join } from "node:path";

import { excludedCrates } from "./check-version.ts";

// The repo does not depend on @types/bun; declare the used surface.
declare const Bun: {
  TOML: { parse: (text: string) => unknown };
};

type JsonObject = Record<string, unknown>;

function isRecord(value: unknown): value is JsonObject {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function asRecord(value: unknown): JsonObject {
  return isRecord(value) ? value : {};
}

/**
 * Rewrites a manifest's internal dependency ranges to `version`: `^<v>` in dependencies and
 * peerDependencies, `<v>` in devDependencies. Returns the (possibly mutated) manifest; bumpp
 * rewrites each package's own `version` field, this covers the ranges bumpp cannot express in
 * JSON.
 */
export function syncInternalDeps(
  pkg: JsonObject,
  version: string,
  internalNames: ReadonlySet<string>,
): JsonObject {
  for (const [section, prefix] of [
    ["dependencies", "^"],
    ["peerDependencies", "^"],
    ["devDependencies", ""],
  ] as const) {
    const deps = asRecord(pkg[section]);
    for (const name of Object.keys(deps)) {
      if (internalNames.has(name)) {
        deps[name] = `${prefix}${version}`;
      }
    }
  }
  return pkg;
}

/**
 * Rewrites `version = "=<v>"` pins on internal path dependencies — Cargo.toml lines carrying `path
 * = "../…"` (member and standalone manifests) or `path = "crates/…"` (the root workspace manifest)
 * — to `version`. bumpp cannot reach these files, and without this `cargo update` fails right after
 * a bump: member crates would still require the previous `=<v>`. A dep's own `version = "<v>"` (no
 * `=` pin) is untouched, so standalone projects keep their independent `0.0.0`.
 */
export function syncInternalPins(manifest: string, version: string): string {
  return manifest
    .split("\n")
    .map((line) =>
      /\bpath\s*=\s*"(?:\.\.|crates\/)/.test(line)
        ? line.replace(/\bversion(\s*=\s*)"=[^"]*"/, `version$1"=${version}"`)
        : line,
    )
    .join("\n");
}

if (import.meta.main) {
  const dirs = readdirSync("packages", { withFileTypes: true })
    .filter((entry) => entry.isDirectory())
    .map((entry) => join("packages", entry.name));

  const manifests = new Map<string, JsonObject>();
  const internalNames = new Set<string>();
  for (const dir of dirs) {
    const path = join(dir, "package.json");
    const pkg: unknown = JSON.parse(readFileSync(path, "utf8"));
    manifests.set(path, asRecord(pkg));
    const { name } = asRecord(pkg);
    if (typeof name === "string") {
      internalNames.add(name);
    }
  }

  const version = manifests.get("packages/marmot/package.json")?.["version"];
  if (typeof version === "string") {
    for (const [path, pkg] of manifests) {
      if (path === "packages/marmot/package.json") {
        continue;
      }
      syncInternalDeps(pkg, version, internalNames);
      writeFileSync(path, `${JSON.stringify(pkg, null, 2)}\n`);
    }

    // Internal `=<v>` pins on path deps live in crate manifests bumpp never
    // touches; they must move before `cargo update --workspace` can resolve.
    const tomls = ["Cargo.toml"];
    if (existsSync("crates")) {
      for (const entry of readdirSync("crates", { withFileTypes: true })) {
        if (entry.isDirectory()) {
          tomls.push(join("crates", entry.name, "Cargo.toml"));
        }
      }
    }
    for (const path of tomls) {
      if (!existsSync(path)) {
        continue;
      }
      const text = readFileSync(path, "utf8");
      const synced = syncInternalPins(text, version);
      if (synced !== text) {
        writeFileSync(path, synced);
      }
    }

    // bun.lock mirrors the workspace versions under `workspaces`; Cargo.lock
    // must record the new workspace version — but only once a lockfile
    // exists (no crates yet → `cargo update` has nothing to do). bumpp runs
    // the `execute` hook without a shell — `&&` would reach bun as
    // arguments — so the refresh is spawned here instead of chained in
    // bump.config.ts.
    if (existsSync("Cargo.lock")) {
      const update = spawnSync("cargo", ["update", "--workspace"], { stdio: "inherit" });
      if (update.status === 0) {
        // The workspace-excluded standalone projects (crates/interop,
        // crates/conformance, fuzz — dir names per root Cargo.toml `exclude`)
        // carry their own Cargo.lock recording the member versions.
        // `cargo metadata` re-resolves against the moved manifests and writes
        // only the changed entries — unlike `cargo update -p` it can never
        // upgrade an unrelated dependency.
        const excluded = excludedCrates(Bun.TOML.parse(readFileSync("Cargo.toml", "utf8")));
        for (const name of excluded) {
          const dir = existsSync(join("crates", name)) ? join("crates", name) : name;
          if (!existsSync(join(dir, "Cargo.toml")) || !existsSync(join(dir, "Cargo.lock"))) {
            continue;
          }
          const metadata = spawnSync("cargo", ["metadata", "--format-version", "1"], {
            cwd: dir,
            stdio: ["ignore", "ignore", "inherit"],
          });
          if (metadata.status !== 0) {
            const why =
              metadata.error === undefined
                ? `exit code ${String(metadata.status)}`
                : metadata.error.message;
            console.error(`sync-versions: cargo metadata failed in ${dir}: ${why}`);
            process.exitCode = 1;
          }
        }
      } else {
        const why =
          update.error === undefined ? `exit code ${String(update.status)}` : update.error.message;
        console.error(`sync-versions: cargo update --workspace failed: ${why}`);
        process.exitCode = 1;
      }
    }
    const lock = spawnSync("bun", ["install", "--lockfile-only"], { stdio: "inherit" });
    if (lock.status !== 0) {
      const why =
        lock.error === undefined ? `exit code ${String(lock.status)}` : lock.error.message;
      console.error(`sync-versions: bun install --lockfile-only failed: ${why}`);
      process.exitCode = 1;
    }
  } else {
    console.error('sync-versions: packages/marmot/package.json has no string "version"');
    process.exitCode = 1;
  }
}
