/// <reference types="node" />
import { execFileSync } from "node:child_process";
import { existsSync, readdirSync, readFileSync } from "node:fs";
import { join } from "node:path";

type JsonObject = Record<string, unknown>;

function isRecord(value: unknown): value is JsonObject {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function asRecord(value: unknown): JsonObject {
  return isRecord(value) ? value : {};
}

/**
 * The pin named `spec` is the Marmot spec checkout: `*.md` files under `vectors/spec/foundation`
 * are recorded byte-for-byte copies of `<checkout>/foundation/*.md`, compared only when the
 * checkout sits exactly at its pinned rev.
 */
const SPEC_PIN = "spec";
const SPEC_RECORDED = "vectors/spec/foundation";
const SPEC_SOURCE = "foundation";

export type PinProbe = {
  /** HEAD of a checkout directory, or undefined when the directory is absent. */
  checkoutHead: (checkout: string) => string | undefined;
  /** File names (not paths) directly under a directory; [] when absent. */
  listFiles: (dir: string) => string[];
  /** Raw bytes of a repo-relative path, or undefined when missing. */
  readBytes: (path: string) => Uint8Array | undefined;
};

export type PinsResult = {
  errors: string[];
  notes: string[];
};

function sameBytes(a: Uint8Array, b: Uint8Array): boolean {
  return a.length === b.length && a.every((byte, i) => byte === b[i]);
}

/**
 * Validates the upstream.json pin file: every pin needs an https repository, a full 40-hex rev and
 * a checkout path (optional `tag` for the pinned release name). For each pin whose checkout
 * directory exists locally, the checkout HEAD must equal the pinned rev; absent checkouts are
 * skipped with a note (CI has no 3rdparty). When the `spec` checkout is at the pin, every recorded
 * spec copy under `vectors/spec/foundation` must match its `foundation/` source byte-for-byte (we
 * record a subset of the spec files, not the whole directory).
 */
export function checkPins(upstream: unknown, probe: PinProbe): PinsResult {
  const errors: string[] = [];
  const notes: string[] = [];
  const pins = asRecord(upstream);
  const names = Object.keys(pins);
  if (names.length === 0) {
    errors.push("upstream.json: no pins declared");
  }

  for (const name of names) {
    const pin = asRecord(pins[name]);
    const { repository, rev, checkout, tag } = pin;
    if (typeof repository !== "string" || !repository.startsWith("https://")) {
      errors.push(`${name}: repository must be an https URL, got ${JSON.stringify(repository)}`);
    }
    if (tag !== undefined && typeof tag !== "string") {
      errors.push(`${name}: tag must be a string, got ${JSON.stringify(tag)}`);
    }
    if (typeof rev !== "string" || !/^[0-9a-f]{40}$/.test(rev)) {
      errors.push(`${name}: rev must be a full 40-hex commit, got ${JSON.stringify(rev)}`);
      continue;
    }
    if (typeof checkout !== "string" || checkout === "") {
      errors.push(`${name}: missing checkout directory`);
      continue;
    }

    const head = probe.checkoutHead(checkout);
    if (head === undefined) {
      notes.push(`${name}: ${checkout} not checked out locally — pin not verified`);
      continue;
    }
    if (head !== rev) {
      const suffix = typeof tag === "string" ? ` (${tag})` : "";
      errors.push(`${name}: ${checkout} is at ${head}, pinned rev is ${rev}${suffix}`);
      continue;
    }

    if (name !== SPEC_PIN) {
      continue;
    }
    // The spec checkout sits at the pinned rev: every recorded copy must be
    // byte-identical to its source under <checkout>/foundation/.
    for (const file of probe.listFiles(SPEC_RECORDED).filter((f) => f.endsWith(".md"))) {
      const copy = probe.readBytes(join(SPEC_RECORDED, file));
      const original = probe.readBytes(join(checkout, SPEC_SOURCE, file));
      if (original === undefined) {
        errors.push(
          `${name}: ${SPEC_RECORDED}/${file} has no source in ${checkout}/${SPEC_SOURCE}`,
        );
      } else if (copy === undefined || !sameBytes(copy, original)) {
        errors.push(
          `${name}: ${SPEC_RECORDED}/${file} differs from ${checkout}/${SPEC_SOURCE}/${file}`,
        );
      }
    }
  }

  return { errors, notes };
}

if (import.meta.main) {
  const upstream: unknown = JSON.parse(readFileSync("upstream.json", "utf8"));
  const probe: PinProbe = {
    checkoutHead(checkout) {
      if (!existsSync(checkout)) {
        return undefined;
      }
      try {
        return execFileSync("git", ["-C", checkout, "rev-parse", "HEAD"], {
          encoding: "utf8",
          stdio: ["ignore", "pipe", "ignore"],
        }).trim();
      } catch {
        return undefined;
      }
    },
    listFiles(dir) {
      return existsSync(dir)
        ? readdirSync(dir, { withFileTypes: true })
            .filter((entry) => entry.isFile() && !entry.name.startsWith("."))
            .map((entry) => entry.name)
        : [];
    },
    readBytes(path) {
      return existsSync(path) ? readFileSync(path) : undefined;
    },
  };
  const { errors, notes } = checkPins(upstream, probe);
  for (const note of notes) {
    console.log(`check-pins: ${note}`);
  }
  for (const error of errors) {
    console.error(`check-pins: ${error}`);
  }
  if (errors.length > 0) {
    process.exitCode = 1;
  }
}
