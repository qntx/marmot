#!/usr/bin/env bun
/**
 * Repin-mdk — point this repository at an arbitrary MDK commit (used by the advisory nightly CI job
 * tracking MDK master). Takes a local MDK checkout and rewrites:
 *
 * - Upstream.json `mdk`/`wn` pins: `tag` = `v<mdk workspace version>`, `rev` = the checkout's HEAD
 * - The git-dependency lines of ./Cargo.toml and crates/interop/Cargo.toml: every `rev`/`version` is
 *   taken from the MDK checkout's own Cargo.lock, so the pins stay coherent with each other.
 *
 * MDK workspace members (url = the MDK repository) take the new HEAD rev and the version of the
 * source-less `[[package]]` of the same name in MDK's Cargo.lock. Other git deps (OpenMLS, the
 * rust-nostr fork) take the rev after `#` and the version of the `[[package]]` whose `source`
 * matches `git+<url>`. A dependency the lock does not have is an error — nothing is written.
 */

/// <reference types="node" />
import { execFileSync } from "node:child_process";
import { readFileSync, writeFileSync } from "node:fs";
import { join } from "node:path";

const MDK_REPOSITORY = "https://github.com/marmot-protocol/mdk";
const MANIFESTS = ["Cargo.toml", "crates/interop/Cargo.toml"];

type JsonObject = Record<string, unknown>;

function isRecord(value: unknown): value is JsonObject {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

export type LockPackage = {
  name: string;
  version: string;
  source?: string;
};

/** Parse the flat `[[package]]` entries of a Cargo.lock. */
export function lockPackages(lockText: string): LockPackage[] {
  const packages: LockPackage[] = [];
  for (const block of lockText.split("[[package]]").slice(1)) {
    const name = /\bname\s*=\s*"([^"]*)"/.exec(block)?.[1];
    const version = /\bversion\s*=\s*"([^"]*)"/.exec(block)?.[1];
    if (name === undefined || version === undefined) {
      continue;
    }
    const source = /\bsource\s*=\s*"([^"]*)"/.exec(block)?.[1];
    packages.push(source === undefined ? { name, version } : { name, version, source });
  }
  return packages;
}

/** `[workspace.package].version` of a manifest. */
export function workspaceVersion(manifestText: string): string | undefined {
  const section = /\[workspace\.package\]([^[]*)/.exec(manifestText)?.[1] ?? "";
  return /\bversion\s*=\s*"([^"]*)"/.exec(section)?.[1];
}

/**
 * Resolve one git dependency line to its new pin. MDK members pin the new HEAD and the MDK
 * workspace version; other git deps keep following whatever rev/version the MDK lock records.
 * Throws naming the dependency when the lock has no matching package.
 */
export function resolveDep(
  name: string,
  url: string,
  packages: LockPackage[],
  mdkRev: string,
): { rev: string; version: string } {
  if (url === MDK_REPOSITORY) {
    const member = packages.find((pkg) => pkg.name === name && pkg.source === undefined);
    if (member === undefined) {
      throw new Error(`${name}: no workspace member in the MDK lockfile`);
    }
    return { rev: mdkRev, version: member.version };
  }
  const dep = packages.find(
    (pkg) => pkg.name === name && pkg.source?.startsWith(`git+${url}`) === true,
  );
  const commit = dep?.source?.split("#")[1];
  if (dep === undefined || commit === undefined) {
    throw new Error(`${name}: no git+${url} package in the MDK lockfile`);
  }
  return { rev: commit, version: dep.version };
}

const GIT_DEP = /^([^\s=]+)\s*=\s*\{\s*git\s*=\s*"([^"]+)"/;

/**
 * Rewrite every git-dependency line of a manifest to `resolveDep`'s pin. Only the `rev = "..."` and
 * `version = "=..."` values change; every other byte (alignment, features) is preserved. Returns
 * the new text plus one summary line per rewritten dependency.
 */
export function repinManifest(
  text: string,
  packages: LockPackage[],
  mdkRev: string,
): { text: string; summaries: string[] } {
  const summaries: string[] = [];
  const lines = text.split("\n").map((line) => {
    const dep = GIT_DEP.exec(line);
    if (dep === null) {
      return line;
    }
    const [, name = "", url = ""] = dep;
    const { rev, version } = resolveDep(name, url, packages, mdkRev);
    summaries.push(`${name}: rev ${rev}, version =${version}`);
    return line
      .replace(/\brev(\s*=\s*)"[^"]*"/, `rev$1"${rev}"`)
      .replace(/\bversion(\s*=\s*)"=[^"]*"/, `version$1"=${version}"`);
  });
  return { text: lines.join("\n"), summaries };
}

/** Upstream.json with the `mdk` and `wn` pins moved to `rev` under `v<mdkVersion>`. */
export function repinUpstream(text: string, mdkRev: string, mdkVersion: string): string {
  const pins: unknown = JSON.parse(text);
  if (!isRecord(pins)) {
    throw new Error("upstream.json: not a JSON object");
  }
  for (const name of ["mdk", "wn"]) {
    const pin = pins[name];
    if (!isRecord(pin)) {
      throw new Error(`upstream.json: no "${name}" pin`);
    }
    pin["tag"] = `v${mdkVersion}`;
    pin["rev"] = mdkRev;
  }
  return `${JSON.stringify(pins, null, 2)}\n`;
}

if (import.meta.main) {
  const [dir] = process.argv.slice(2);
  if (dir === undefined) {
    console.error("usage: bun scripts/repin-mdk.ts <mdk-checkout>");
    process.exit(1);
  }
  const rev = execFileSync("git", ["-C", dir, "rev-parse", "HEAD"], { encoding: "utf8" }).trim();
  const mdkVersion = workspaceVersion(readFileSync(join(dir, "Cargo.toml"), "utf8"));
  if (mdkVersion === undefined) {
    console.error(`repin-mdk: no [workspace.package].version in ${dir}/Cargo.toml`);
    process.exit(1);
  }
  const packages = lockPackages(readFileSync(join(dir, "Cargo.lock"), "utf8"));

  try {
    const rewritten = MANIFESTS.map((path) => {
      const { text, summaries } = repinManifest(readFileSync(path, "utf8"), packages, rev);
      return { path, text, summaries };
    });
    const upstream = repinUpstream(readFileSync("upstream.json", "utf8"), rev, mdkVersion);

    for (const { path, text, summaries } of rewritten) {
      writeFileSync(path, text);
      for (const summary of summaries) {
        console.log(`repin-mdk: ${path}: ${summary}`);
      }
    }
    writeFileSync("upstream.json", upstream);
    console.log(`repin-mdk: mdk/wn pins → v${mdkVersion} @ ${rev}`);
  } catch (error) {
    console.error(`repin-mdk: ${error instanceof Error ? error.message : String(error)}`);
    process.exit(1);
  }
}
