/// <reference types="node" />
import { describe, expect, test } from "vite-plus/test";

import { checkPins } from "./check-pins.ts";
import type { PinProbe } from "./check-pins.ts";

const REV_A = "f825024f7eb92157d12a0b8985bc6c5db5531d6d";
const REV_B = "a2e0558a634bbd20607cdd5afb93abc6220ee58d";

const PINS = {
  spec: {
    repository: "https://github.com/marmot-protocol/marmot",
    rev: REV_A,
    checkout: "3rdparty/marmot",
  },
  mdk: {
    repository: "https://github.com/marmot-protocol/mdk",
    tag: "v0.16.0",
    rev: REV_B,
    checkout: "3rdparty/mdk",
  },
};

const SPEC_FILES: Record<string, Uint8Array> = {
  "3rdparty/marmot/foundation/registries.md": new TextEncoder().encode("registries"),
  "3rdparty/marmot/foundation/errors.md": new TextEncoder().encode("errors"),
  "vectors/spec/foundation/registries.md": new TextEncoder().encode("registries"),
  "vectors/spec/foundation/errors.md": new TextEncoder().encode("errors"),
};

/** Fake probe: heads map checkout dirs to HEADs, files map paths to bytes. */
function probe(heads: Record<string, string>, files: Record<string, Uint8Array>): PinProbe {
  const dirs = new Map<string, string[]>();
  for (const path of Object.keys(files)) {
    const dir = path.slice(0, path.lastIndexOf("/"));
    const name = path.slice(path.lastIndexOf("/") + 1);
    dirs.set(dir, [...(dirs.get(dir) ?? []), name]);
  }
  return {
    checkoutHead: (checkout) => heads[checkout],
    listFiles: (dir) => dirs.get(dir) ?? [],
    readBytes: (path) => files[path],
  };
}

const BOTH_AT_PIN = probe({ "3rdparty/marmot": REV_A, "3rdparty/mdk": REV_B }, SPEC_FILES);

describe("checkPins", () => {
  test("accepts pins whose checkouts sit at the pinned revs", () => {
    expect(checkPins(PINS, BOTH_AT_PIN)).toStrictEqual({ errors: [], notes: [] });
  });

  test("rejects a malformed rev", () => {
    const { errors } = checkPins({ spec: { ...PINS.spec, rev: "f825024" } }, probe({}, {}));
    expect(errors).toStrictEqual(['spec: rev must be a full 40-hex commit, got "f825024"']);
  });

  test("rejects a non-https repository", () => {
    const { errors } = checkPins(
      { spec: { ...PINS.spec, repository: "git@github.com:marmot-protocol/marmot.git" } },
      probe({}, {}),
    );
    expect(errors).toStrictEqual([
      'spec: repository must be an https URL, got "git@github.com:marmot-protocol/marmot.git"',
    ]);
  });

  test("skips absent checkouts with a note", () => {
    const { errors, notes } = checkPins(PINS, probe({ "3rdparty/marmot": REV_A }, SPEC_FILES));
    expect(errors).toStrictEqual([]);
    expect(notes).toStrictEqual(["mdk: 3rdparty/mdk not checked out locally — pin not verified"]);
  });

  test("rejects a checkout at a different rev", () => {
    const { errors } = checkPins(
      PINS,
      probe(
        { "3rdparty/marmot": REV_A, "3rdparty/mdk": "9cf81ea674a8e6e63f9f899261ee7fe58b529681" },
        SPEC_FILES,
      ),
    );
    expect(errors).toStrictEqual([
      `mdk: 3rdparty/mdk is at 9cf81ea674a8e6e63f9f899261ee7fe58b529681, pinned rev is ${REV_B} (v0.16.0)`,
    ]);
  });

  test("rejects a recorded spec copy that differs from the checkout", () => {
    const files = {
      ...SPEC_FILES,
      "vectors/spec/foundation/errors.md": new TextEncoder().encode("stale copy"),
    };
    const { errors } = checkPins(
      PINS,
      probe({ "3rdparty/marmot": REV_A, "3rdparty/mdk": REV_B }, files),
    );
    expect(errors).toStrictEqual([
      "spec: vectors/spec/foundation/errors.md differs from 3rdparty/marmot/foundation/errors.md",
    ]);
  });

  test("rejects a recorded copy with no source, ignores unrecorded source files", () => {
    const files = { ...SPEC_FILES };
    delete files["vectors/spec/foundation/registries.md"];
    files["vectors/spec/foundation/extra.md"] = new TextEncoder().encode("extra");
    files["3rdparty/marmot/foundation/identity.md"] = new TextEncoder().encode("identity");
    const { errors } = checkPins(
      PINS,
      probe({ "3rdparty/marmot": REV_A, "3rdparty/mdk": REV_B }, files),
    );
    // We record a subset of foundation/, so the extra source file is fine;
    // a recorded file whose source is missing is not.
    expect(errors).toStrictEqual([
      "spec: vectors/spec/foundation/extra.md has no source in 3rdparty/marmot/foundation",
    ]);
  });

  test("does not compare recorded copies when the spec checkout is off the pin", () => {
    const files = {
      ...SPEC_FILES,
      "vectors/spec/foundation/errors.md": new TextEncoder().encode("stale copy"),
    };
    const { errors } = checkPins(
      PINS,
      probe({ "3rdparty/marmot": REV_B, "3rdparty/mdk": REV_B }, files),
    );
    expect(errors).toStrictEqual([`spec: 3rdparty/marmot is at ${REV_B}, pinned rev is ${REV_A}`]);
  });
});
