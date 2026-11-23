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

/**
 * Fake probe: heads map checkout dirs to HEADs, tags map checkout dirs to tag→commit, files map
 * paths to bytes.
 */
function probe(
  heads: Record<string, string>,
  files: Record<string, Uint8Array>,
  tags: Record<string, Record<string, string>> = {},
): PinProbe {
  const dirs = new Map<string, string[]>();
  for (const path of Object.keys(files)) {
    const dir = path.slice(0, path.lastIndexOf("/"));
    const name = path.slice(path.lastIndexOf("/") + 1);
    dirs.set(dir, [...(dirs.get(dir) ?? []), name]);
  }
  return {
    checkoutHead: (checkout) => heads[checkout],
    tagCommit: (checkout, tag) => tags[checkout]?.[tag],
    listFiles: (dir) => dirs.get(dir) ?? [],
    readBytes: (path) => files[path],
  };
}

const MDK_TAGS = { "3rdparty/mdk": { "v0.16.0": REV_B } };

const BOTH_AT_PIN = probe(
  { "3rdparty/marmot": REV_A, "3rdparty/mdk": REV_B },
  SPEC_FILES,
  MDK_TAGS,
);

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

  const MDK_FILES: Record<string, Uint8Array> = {
    "3rdparty/mdk/crates/cgka-conformance-simulator/vectors/a.v1.json": new TextEncoder().encode(
      "vector a",
    ),
    "3rdparty/mdk/crates/cgka-conformance-simulator/vectors/unrecorded.v1.json":
      new TextEncoder().encode("not recorded"),
    "3rdparty/mdk/crates/cgka-conformance-simulator/vectors/incidents/i.v1.json":
      new TextEncoder().encode("incident i"),
    "3rdparty/mdk/fixtures/encrypted-media/imeta-v1.json": new TextEncoder().encode("imeta v1"),
    "3rdparty/mdk/fixtures/encrypted-media/imeta-v2.json": new TextEncoder().encode("imeta v2"),
    "vectors/mdk/a.v1.json": new TextEncoder().encode("vector a"),
    "vectors/mdk/PROVENANCE.md": new TextEncoder().encode("provenance"),
    "vectors/mdk/incidents/i.v1.json": new TextEncoder().encode("incident i"),
    "vectors/mdk/encrypted-media/imeta-v1.json": new TextEncoder().encode("imeta v1"),
    "vectors/mdk/encrypted-media/imeta-v2.json": new TextEncoder().encode("imeta v2"),
  };

  test("compares recorded mdk vectors when the mdk checkout is at the pin", () => {
    const files = { ...SPEC_FILES, ...MDK_FILES };
    expect(
      checkPins(PINS, probe({ "3rdparty/marmot": REV_A, "3rdparty/mdk": REV_B }, files, MDK_TAGS)),
    ).toStrictEqual({ errors: [], notes: [] });
  });

  test("rejects a stale recorded mdk vector, including under incidents/", () => {
    const files = {
      ...SPEC_FILES,
      ...MDK_FILES,
      "vectors/mdk/a.v1.json": new TextEncoder().encode("stale vector"),
      "vectors/mdk/incidents/i.v1.json": new TextEncoder().encode("stale incident"),
    };
    const { errors } = checkPins(
      PINS,
      probe({ "3rdparty/marmot": REV_A, "3rdparty/mdk": REV_B }, files),
    );
    expect(errors).toStrictEqual([
      "mdk: vectors/mdk/a.v1.json differs from 3rdparty/mdk/crates/cgka-conformance-simulator/vectors/a.v1.json",
      "mdk: vectors/mdk/incidents/i.v1.json differs from 3rdparty/mdk/crates/cgka-conformance-simulator/vectors/incidents/i.v1.json",
    ]);
  });

  test("does not compare recorded mdk vectors when the mdk checkout is off the pin", () => {
    const files = {
      ...SPEC_FILES,
      ...MDK_FILES,
      "vectors/mdk/a.v1.json": new TextEncoder().encode("stale vector"),
    };
    const { errors } = checkPins(
      PINS,
      probe(
        { "3rdparty/marmot": REV_A, "3rdparty/mdk": "9cf81ea674a8e6e63f9f899261ee7fe58b529681" },
        files,
      ),
    );
    expect(errors).toStrictEqual([
      `mdk: 3rdparty/mdk is at 9cf81ea674a8e6e63f9f899261ee7fe58b529681, pinned rev is ${REV_B} (v0.16.0)`,
    ]);
  });

  test("rejects a recorded mdk vector with no source, ignores unrecorded source files", () => {
    const files = {
      ...SPEC_FILES,
      ...MDK_FILES,
      "vectors/mdk/missing.v1.json": new TextEncoder().encode("no source"),
    };
    const { errors } = checkPins(
      PINS,
      probe({ "3rdparty/marmot": REV_A, "3rdparty/mdk": REV_B }, files),
    );
    expect(errors).toStrictEqual([
      "mdk: vectors/mdk/missing.v1.json has no source in 3rdparty/mdk/crates/cgka-conformance-simulator/vectors",
    ]);
  });

  test("rejects a tag that resolves to a different commit", () => {
    const elsewhere = "9cf81ea674a8e6e63f9f899261ee7fe58b529681";
    const { errors } = checkPins(
      PINS,
      probe({ "3rdparty/marmot": REV_A, "3rdparty/mdk": REV_B }, SPEC_FILES, {
        "3rdparty/mdk": { "v0.16.0": elsewhere },
      }),
    );
    expect(errors).toStrictEqual([
      `mdk: v0.16.0 in 3rdparty/mdk is ${elsewhere}, pinned rev is ${REV_B}`,
    ]);
  });

  test("notes a tag that is absent from the checkout", () => {
    const { errors, notes } = checkPins(
      PINS,
      probe({ "3rdparty/marmot": REV_A, "3rdparty/mdk": REV_B }, SPEC_FILES),
    );
    expect(errors).toStrictEqual([]);
    expect(notes).toStrictEqual([
      "mdk: tag v0.16.0 not present in 3rdparty/mdk — tag not verified",
    ]);
  });

  test("verifies two pins that share one checkout", () => {
    const pins = {
      mdk: PINS.mdk,
      wn: { ...PINS.mdk },
    };
    const { errors, notes } = checkPins(
      pins,
      probe({ "3rdparty/mdk": REV_B }, SPEC_FILES, MDK_TAGS),
    );
    expect(errors).toStrictEqual([]);
    expect(notes).toStrictEqual([]);
  });

  const NOSFRY = {
    nosfry: {
      repository: "https://github.com/qntx/nosfry",
      tag: "v0.4.0",
      rev: REV_A,
      checkout: "3rdparty/nosfry",
      tarball_sha256: "2d711642b726b04401627ca9fbac32f5c8530fb1903cc4db02258717921a4881",
      profile_sha256: "2a616ec28a470ea3b71d9a29577cec8600a965ff5c9859d5f07ec0ded1980c5c",
    },
  };

  const NOSFRY_TAGS = { "3rdparty/nosfry": { "v0.4.0": REV_A } };

  const NOSFRY_FILES: Record<string, Uint8Array> = {
    "3rdparty/nosfry/contrib/marmot/nosfry.toml": new TextEncoder().encode("marmot profile"),
  };

  test("rejects a malformed _sha256 field, checkout present or not", () => {
    const { errors } = checkPins(
      { nosfry: { ...NOSFRY.nosfry, tarball_sha256: "not-a-digest" } },
      probe({}, {}),
    );
    expect(errors).toStrictEqual([
      'nosfry: tarball_sha256 must be a 64-hex SHA-256, got "not-a-digest"',
    ]);
  });

  test("verifies the nosfry profile digest when the checkout sits at the pin", () => {
    expect(
      checkPins(NOSFRY, probe({ "3rdparty/nosfry": REV_A }, NOSFRY_FILES, NOSFRY_TAGS)),
    ).toStrictEqual({ errors: [], notes: [] });
  });

  test("rejects a nosfry profile that does not match the pinned digest", () => {
    const files = {
      "3rdparty/nosfry/contrib/marmot/nosfry.toml": new TextEncoder().encode("stale profile"),
    };
    const { errors } = checkPins(NOSFRY, probe({ "3rdparty/nosfry": REV_A }, files, NOSFRY_TAGS));
    expect(errors).toStrictEqual([
      "nosfry: 3rdparty/nosfry/contrib/marmot/nosfry.toml is sha256 77f9d3d4a012803874f972ef760a158ac8725ffdde1ec4346daa90ed9679f471, pinned profile_sha256 is 2a616ec28a470ea3b71d9a29577cec8600a965ff5c9859d5f07ec0ded1980c5c",
    ]);
  });

  test("rejects a nosfry checkout without the profile file", () => {
    const { errors } = checkPins(NOSFRY, probe({ "3rdparty/nosfry": REV_A }, {}, NOSFRY_TAGS));
    expect(errors).toStrictEqual(["nosfry: 3rdparty/nosfry/contrib/marmot/nosfry.toml missing"]);
  });

  test("skips the nosfry digest check when the checkout is absent", () => {
    const { errors, notes } = checkPins(NOSFRY, probe({}, {}));
    expect(errors).toStrictEqual([]);
    expect(notes).toStrictEqual([
      "nosfry: 3rdparty/nosfry not checked out locally — pin not verified",
    ]);
  });
});
