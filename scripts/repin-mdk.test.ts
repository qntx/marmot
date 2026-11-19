import { describe, expect, test } from "vite-plus/test";

import {
  lockPackages,
  repinManifest,
  repinUpstream,
  resolveDep,
  workspaceVersion,
} from "./repin-mdk.ts";

const MDK_REV = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const OPENMLS_REV = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
const NOSTR_REV = "cccccccccccccccccccccccccccccccccccccccc";

const MDK_TOML = `[workspace]
members = ["crates/cgka-engine"]

[workspace.package]
version = "0.18.0"
edition = "2024"
`;

const LOCK = `version = 4

[[package]]
name = "cgka-engine"
version = "0.18.0"
dependencies = [
 "openmls",
]

[[package]]
name = "openmls"
version = "0.9.1"
source = "git+https://github.com/openmls/openmls.git?rev=${OPENMLS_REV}#${OPENMLS_REV}"

[[package]]
name = "nostr"
version = "0.44.8"
source = "registry+https://github.com/rust-lang/crates.io-index"

[[package]]
name = "nostr"
version = "0.45.6"
source = "git+https://github.com/erskingardner/rust-nostr?rev=${NOSTR_REV}#${NOSTR_REV}"
`;

const MANIFEST = `[workspace.dependencies]
cgka-engine = { git = "https://github.com/marmot-protocol/mdk", rev = "dddddddddddddddddddddddddddddddddddddddd", version = "=0.17.0" }
openmls     = { git = "https://github.com/openmls/openmls.git", rev = "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee", version = "=0.9.0", features = ["extensions-draft"] }
nostr       = { git = "https://github.com/erskingardner/rust-nostr", rev = "ffffffffffffffffffffffffffffffffffffffff", version = "=0.45.5", default-features = false }
serde       = { version = "1", features = ["derive"] }
`;

describe("workspaceVersion", () => {
  test("reads [workspace.package].version", () => {
    expect(workspaceVersion(MDK_TOML)).toBe("0.18.0");
  });
});

describe("lockPackages", () => {
  test("parses members and sourced packages", () => {
    expect(lockPackages(LOCK)).toStrictEqual([
      { name: "cgka-engine", version: "0.18.0" },
      {
        name: "openmls",
        version: "0.9.1",
        source: `git+https://github.com/openmls/openmls.git?rev=${OPENMLS_REV}#${OPENMLS_REV}`,
      },
      {
        name: "nostr",
        version: "0.44.8",
        source: "registry+https://github.com/rust-lang/crates.io-index",
      },
      {
        name: "nostr",
        version: "0.45.6",
        source: `git+https://github.com/erskingardner/rust-nostr?rev=${NOSTR_REV}#${NOSTR_REV}`,
      },
    ]);
  });
});

describe("resolveDep", () => {
  const packages = lockPackages(LOCK);

  test("an MDK member takes the checkout rev and the member version", () => {
    expect(
      resolveDep("cgka-engine", "https://github.com/marmot-protocol/mdk", packages, MDK_REV),
    ).toStrictEqual({ rev: MDK_REV, version: "0.18.0" });
  });

  test("another git dep takes the rev after # and the locked version", () => {
    expect(
      resolveDep("openmls", "https://github.com/openmls/openmls.git", packages, MDK_REV),
    ).toStrictEqual({ rev: OPENMLS_REV, version: "0.9.1" });
  });

  test("a registry package of the same name does not satisfy a git dep", () => {
    expect(
      resolveDep("nostr", "https://github.com/erskingardner/rust-nostr", packages, MDK_REV),
    ).toStrictEqual({ rev: NOSTR_REV, version: "0.45.6" });
  });

  test("a dependency the lock does not have is an error naming it", () => {
    expect(() =>
      resolveDep("storage-sqlite", "https://github.com/marmot-protocol/mdk", packages, MDK_REV),
    ).toThrow("storage-sqlite: no workspace member in the MDK lockfile");
    expect(() =>
      resolveDep("quinn", "https://github.com/quinn-rs/quinn", packages, MDK_REV),
    ).toThrow("quinn: no git+https://github.com/quinn-rs/quinn package in the MDK lockfile");
  });
});

describe("repinManifest", () => {
  test("rewrites only rev and version values, preserving every other byte", () => {
    const { text, summaries } = repinManifest(MANIFEST, lockPackages(LOCK), MDK_REV);
    expect(text).toBe(
      `[workspace.dependencies]
cgka-engine = { git = "https://github.com/marmot-protocol/mdk", rev = "${MDK_REV}", version = "=0.18.0" }
openmls     = { git = "https://github.com/openmls/openmls.git", rev = "${OPENMLS_REV}", version = "=0.9.1", features = ["extensions-draft"] }
nostr       = { git = "https://github.com/erskingardner/rust-nostr", rev = "${NOSTR_REV}", version = "=0.45.6", default-features = false }
serde       = { version = "1", features = ["derive"] }
`,
    );
    expect(summaries).toStrictEqual([
      `cgka-engine: rev ${MDK_REV}, version =0.18.0`,
      `openmls: rev ${OPENMLS_REV}, version =0.9.1`,
      `nostr: rev ${NOSTR_REV}, version =0.45.6`,
    ]);
  });

  test("leaves a manifest without git dependencies untouched", () => {
    const plain = `[workspace.dependencies]\nserde = "1"\n`;
    expect(repinManifest(plain, lockPackages(LOCK), MDK_REV)).toStrictEqual({
      text: plain,
      summaries: [],
    });
  });
});

describe("repinUpstream", () => {
  test("moves the mdk and wn pins to the new rev under v<version>", () => {
    const upstream = `{
  "spec": {
    "repository": "https://github.com/marmot-protocol/marmot",
    "rev": "1111111111111111111111111111111111111111",
    "checkout": "3rdparty/marmot"
  },
  "mdk": {
    "repository": "https://github.com/marmot-protocol/mdk",
    "tag": "v0.17.0",
    "rev": "dddddddddddddddddddddddddddddddddddddddd",
    "checkout": "3rdparty/mdk"
  },
  "wn": {
    "repository": "https://github.com/marmot-protocol/mdk",
    "tag": "v0.17.0",
    "rev": "dddddddddddddddddddddddddddddddddddddddd",
    "checkout": "3rdparty/mdk"
  }
}
`;
    const repinned = repinUpstream(upstream, MDK_REV, "0.18.0");
    expect(JSON.parse(repinned)).toStrictEqual({
      spec: {
        repository: "https://github.com/marmot-protocol/marmot",
        rev: "1111111111111111111111111111111111111111",
        checkout: "3rdparty/marmot",
      },
      mdk: {
        repository: "https://github.com/marmot-protocol/mdk",
        tag: "v0.18.0",
        rev: MDK_REV,
        checkout: "3rdparty/mdk",
      },
      wn: {
        repository: "https://github.com/marmot-protocol/mdk",
        tag: "v0.18.0",
        rev: MDK_REV,
        checkout: "3rdparty/mdk",
      },
    });
    // 2-space JSON with a trailing newline, key order preserved.
    expect(repinned).toContain('  "mdk": {\n    "repository"');
    expect(repinned.endsWith("}\n")).toBe(true);
  });
});
