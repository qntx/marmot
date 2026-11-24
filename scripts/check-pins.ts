/// <reference types="node" />
import { execFileSync } from "node:child_process";
import { createHash } from "node:crypto";
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
 * Recorded-copy checks. Each pin may carry directories of byte-for-byte upstream copies under
 * `vectors/`; when that pin's checkout sits exactly at its pinned rev, every recorded file must
 * match its source. We record subsets of the source trees, not whole directories.
 */
type RecordedRoot = {
  /** Directory of recorded copies, repo-relative. */
  recorded: string;
  /** Source directory inside the upstream checkout. */
  source: string;
  /** Only files with this suffix are compared. */
  extension: string;
};

const RECORDED: Record<string, RecordedRoot[]> = {
  spec: [
    {
      recorded: "vectors/spec/foundation",
      source: "foundation",
      extension: ".md",
    },
  ],
  mdk: [
    {
      recorded: "vectors/mdk",
      source: "crates/cgka-conformance-simulator/vectors",
      extension: ".json",
    },
    {
      recorded: "vectors/mdk/incidents",
      source: "crates/cgka-conformance-simulator/vectors/incidents",
      extension: ".json",
    },
    {
      recorded: "vectors/mdk/byte-fixtures",
      source: "crates/cgka-conformance-simulator/vectors/byte-fixtures",
      extension: ".json",
    },
    {
      recorded: "vectors/mdk/encrypted-media",
      source: "fixtures/encrypted-media",
      extension: ".json",
    },
  ],
};

/**
 * Recorded-digest checks. Some pins carry `*_sha256` fields whose digest covers a file inside the
 * upstream checkout rather than a recorded copy under `vectors/`; when that pin's checkout sits at
 * its pinned rev, each mapped field must equal the SHA-256 of the file at the mapped checkout path.
 * Digests covering release artifacts (e.g. the nosfry tarball) are not listed here — they are not
 * part of the source checkout and the workflow verifies them at download time.
 */
const DIGESTS: Record<string, Record<string, string>> = {
  nosfry: { profile_sha256: "contrib/marmot/nosfry.toml" },
};

export type PinProbe = {
  /** HEAD of a checkout directory, or undefined when the directory is absent. */
  checkoutHead: (checkout: string) => string | undefined;
  /** Commit a tag resolves to in a checkout, or undefined when the tag is absent. */
  tagCommit: (checkout: string, tag: string) => string | undefined;
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
 * skipped with a note (CI has no 3rdparty). A pin carrying `tag` also requires that tag to resolve
 * to the pinned rev inside the checkout: a missing tag is a note (a shallow fetch may not carry
 * it), a tag pointing elsewhere is an error. Any field ending in `_sha256` must be a 64-hex digest,
 * checked for every pin whether or not its checkout is present. When a checkout is at the pin,
 * every recorded copy under the pin's `RECORDED` roots (spec `*.md`, mdk vector `*.json`) must
 * match its upstream source byte-for-byte (we record subsets of the source trees, not whole
 * directories), and each `DIGESTS` field must equal the SHA-256 of the mapped file inside the
 * checkout. Artifact digests (a release tarball's `*_sha256`) are not checkable offline — the
 * workflow verifies them when it downloads the artifact.
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
    for (const [key, value] of Object.entries(pin)) {
      if (key.endsWith("_sha256") && (typeof value !== "string" || !/^[0-9a-f]{64}$/.test(value))) {
        errors.push(`${name}: ${key} must be a 64-hex SHA-256, got ${JSON.stringify(value)}`);
      }
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

    if (typeof tag === "string") {
      const commit = probe.tagCommit(checkout, tag);
      if (commit === undefined) {
        notes.push(`${name}: tag ${tag} not present in ${checkout} — tag not verified`);
      } else if (commit !== rev) {
        errors.push(`${name}: ${tag} in ${checkout} is ${commit}, pinned rev is ${rev}`);
      }
    }

    // The checkout sits at the pinned rev: every recorded-digest field
    // must equal the SHA-256 of its file inside the checkout.
    for (const [field, path] of Object.entries(DIGESTS[name] ?? {})) {
      const expected = pin[field];
      if (typeof expected !== "string") {
        errors.push(`${name}: ${field} must be a 64-hex SHA-256, got ${JSON.stringify(expected)}`);
        continue;
      }
      const bytes = probe.readBytes(join(checkout, path));
      if (bytes === undefined) {
        errors.push(`${name}: ${checkout}/${path} missing`);
      } else {
        const actual = createHash("sha256").update(bytes).digest("hex");
        if (actual !== expected) {
          errors.push(
            `${name}: ${checkout}/${path} is sha256 ${actual}, pinned ${field} is ${expected}`,
          );
        }
      }
    }

    // Every recorded copy must be byte-identical to its source under
    // <checkout>/<source>/.
    for (const root of RECORDED[name] ?? []) {
      for (const file of probe.listFiles(root.recorded).filter((f) => f.endsWith(root.extension))) {
        const copy = probe.readBytes(join(root.recorded, file));
        const original = probe.readBytes(join(checkout, root.source, file));
        if (original === undefined) {
          errors.push(
            `${name}: ${root.recorded}/${file} has no source in ${checkout}/${root.source}`,
          );
        } else if (copy === undefined || !sameBytes(copy, original)) {
          errors.push(
            `${name}: ${root.recorded}/${file} differs from ${checkout}/${root.source}/${file}`,
          );
        }
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
    tagCommit(checkout, tag) {
      try {
        return execFileSync(
          "git",
          ["-C", checkout, "rev-parse", "--verify", "--quiet", `${tag}^{commit}`],
          { encoding: "utf8", stdio: ["ignore", "pipe", "ignore"] },
        ).trim();
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
