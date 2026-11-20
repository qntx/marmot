import { describe, expect, test } from "vite-plus/test";

import {
  checkStandaloneApps,
  checkVersion,
  excludedCrates,
  STANDALONE_APPS,
} from "./check-version.ts";

const MARMOT = {
  path: "packages/marmot/package.json",
  pkg: { name: "@qntx/marmot", version: "0.9.0" },
};

const MARMOT_WASM = {
  path: "packages/marmot-wasm/package.json",
  pkg: {
    name: "@qntx/marmot-wasm",
    version: "0.9.0",
    dependencies: { "@qntx/marmot": "^0.9.0" } as Record<string, string>,
    peerDependencies: { "@qntx/marmot": "^0.9.0" } as Record<string, string>,
    devDependencies: { "@qntx/marmot": "0.9.0" } as Record<string, string>,
  },
};

const PACKAGES = [MARMOT, MARMOT_WASM];

const CARGO_TOML = `[workspace]
[workspace.package]
version = "0.9.0"
[workspace.dependencies]
mk-core = { path = "crates/mk-core", version = "=0.9.0", default-features = false }
sha2 = "0.9"
`;

const PARSED = {
  workspace: {
    package: { version: "0.9.0" },
    dependencies: {
      "mk-core": { version: "=0.9.0", path: "crates/mk-core", "default-features": false },
      sha2: "0.9",
    },
  },
};

const MEMBERS = {
  "crates/mk-core/Cargo.toml": { package: { version: { workspace: true } } },
};

const CARGO_TOML_11 = CARGO_TOML.replaceAll("0.9.0", "0.11.0");
const PARSED_11 = {
  workspace: {
    package: { version: "0.11.0" },
    dependencies: {
      "mk-core": { version: "=0.11.0", path: "crates/mk-core", "default-features": false },
      sha2: "0.9",
    },
  },
};
const LOCK_11 = {
  package: [
    { name: "mk-core", version: "0.11.0" },
    {
      name: "sha2",
      version: "0.10.0",
      source: "registry+https://github.com/rust-lang/crates.io-index",
    },
  ],
};

const LOCK = {
  package: [
    { name: "mk-core", version: "0.9.0" },
    {
      name: "sha2",
      version: "0.10.0",
      source: "registry+https://github.com/rust-lang/crates.io-index",
    },
  ],
};

// bun.lock mirrors the workspace manifests under `workspaces` keyed by
// directory; the root entry ("") has no version and is skipped.
const BUN_LOCK = {
  workspaces: {
    "": { name: "@qntx/marmot-monorepo" },
    "packages/marmot": { name: "@qntx/marmot", version: "0.9.0" },
    "packages/marmot-wasm": { name: "@qntx/marmot-wasm", version: "0.9.0" },
  },
};

const BUN_LOCK_11 = {
  workspaces: {
    "packages/marmot": { name: "@qntx/marmot", version: "0.11.0" },
    "packages/marmot-wasm": { name: "@qntx/marmot-wasm", version: "0.11.0" },
  },
};

const STALE_BUN_LOCK = {
  workspaces: {
    "packages/marmot": { name: "@qntx/marmot", version: "0.10.1" },
    "packages/marmot-wasm": { name: "@qntx/marmot-wasm", version: "0.10.1" },
  },
};

const PACKAGES_11 = [
  { path: "packages/marmot/package.json", pkg: { name: "@qntx/marmot", version: "0.11.0" } },
  {
    path: "packages/marmot-wasm/package.json",
    pkg: {
      name: "@qntx/marmot-wasm",
      version: "0.11.0",
      dependencies: { "@qntx/marmot": "^0.11.0" },
      peerDependencies: { "@qntx/marmot": "^0.11.0" },
      devDependencies: { "@qntx/marmot": "0.11.0" },
    },
  },
];

// The M1 workspace: members = [], no [workspace.dependencies], no Cargo.lock.
const EMPTY_CARGO_TOML = `[workspace]
members = []
resolver = "3"
[workspace.package]
version = "0.9.0"
`;
const EMPTY_PARSED = { workspace: { package: { version: "0.9.0" } } };

describe("check-version", () => {
  test("accepts synced versions", () => {
    expect(checkVersion(PACKAGES, PARSED, CARGO_TOML, MEMBERS, LOCK, BUN_LOCK)).toStrictEqual([]);
  });

  test("accepts an empty workspace with no members and no Cargo.lock", () => {
    expect(
      checkVersion(PACKAGES, EMPTY_PARSED, EMPTY_CARGO_TOML, {}, null, BUN_LOCK),
    ).toStrictEqual([]);
  });

  test("rejects a missing version", () => {
    const errors = checkVersion(
      [MARMOT, { ...MARMOT_WASM, pkg: { name: "@qntx/marmot-wasm" } }],
      PARSED,
      CARGO_TOML,
      MEMBERS,
      LOCK,
      BUN_LOCK,
    );
    expect(errors).toStrictEqual([
      'packages/marmot-wasm/package.json: missing string "version"',
      'bun.lock: workspaces["packages/marmot-wasm"] has version "0.9.0", expected undefined',
    ]);
  });

  test("rejects a dependency range without a caret", () => {
    const pkg = structuredClone(MARMOT_WASM.pkg);
    pkg.dependencies["@qntx/marmot"] = "0.9.0";
    const errors = checkVersion(
      [MARMOT, { ...MARMOT_WASM, pkg }],
      PARSED,
      CARGO_TOML,
      MEMBERS,
      LOCK,
      BUN_LOCK,
    );
    expect(errors).toStrictEqual([
      'packages/marmot-wasm/package.json: dependencies["@qntx/marmot"] must be "^0.9.0", got "0.9.0"',
    ]);
  });

  test("rejects a peer range without a caret", () => {
    const pkg = structuredClone(MARMOT_WASM.pkg);
    pkg.peerDependencies["@qntx/marmot"] = "0.9.0";
    const errors = checkVersion(
      [MARMOT, { ...MARMOT_WASM, pkg }],
      PARSED,
      CARGO_TOML,
      MEMBERS,
      LOCK,
      BUN_LOCK,
    );
    expect(errors).toStrictEqual([
      'packages/marmot-wasm/package.json: peerDependencies["@qntx/marmot"] must be "^0.9.0", got "0.9.0"',
    ]);
  });

  test("rejects a dev range with a caret", () => {
    const pkg = structuredClone(MARMOT_WASM.pkg);
    pkg.devDependencies["@qntx/marmot"] = "^0.9.0";
    const errors = checkVersion(
      [MARMOT, { ...MARMOT_WASM, pkg }],
      PARSED,
      CARGO_TOML,
      MEMBERS,
      LOCK,
      BUN_LOCK,
    );
    expect(errors).toStrictEqual([
      'packages/marmot-wasm/package.json: devDependencies["@qntx/marmot"] must be "0.9.0", got "^0.9.0"',
    ]);
  });

  test("ignores ranges on external packages", () => {
    const pkg = structuredClone(MARMOT_WASM.pkg);
    pkg.devDependencies["typescript"] = "^7.0.2";
    expect(
      checkVersion([MARMOT, { ...MARMOT_WASM, pkg }], PARSED, CARGO_TOML, MEMBERS, LOCK, BUN_LOCK),
    ).toStrictEqual([]);
  });

  test("rejects a stale bun.lock workspace version", () => {
    // The drift this check guards: manifests moved on while bun.lock still
    // records the old workspace versions.
    const errors = checkVersion(
      PACKAGES_11,
      PARSED_11,
      CARGO_TOML_11,
      MEMBERS,
      LOCK_11,
      STALE_BUN_LOCK,
    );
    expect(errors).toStrictEqual([
      'bun.lock: workspaces["packages/marmot"] has version "0.10.1", expected "0.11.0"',
      'bun.lock: workspaces["packages/marmot-wasm"] has version "0.10.1", expected "0.11.0"',
    ]);
  });

  test("rejects a bun.lock workspace entry that is missing", () => {
    const errors = checkVersion(PACKAGES, PARSED, CARGO_TOML, MEMBERS, LOCK, { workspaces: {} });
    expect(errors).toStrictEqual([
      'bun.lock: missing workspaces["packages/marmot"] entry',
      'bun.lock: missing workspaces["packages/marmot-wasm"] entry',
    ]);
  });

  test("accepts a bumped pair with a synced lock", () => {
    expect(
      checkVersion(PACKAGES_11, PARSED_11, CARGO_TOML_11, MEMBERS, LOCK_11, BUN_LOCK_11),
    ).toStrictEqual([]);
  });

  test("rejects a package.json/Cargo.toml version mismatch", () => {
    const errors = checkVersion(
      [MARMOT, { ...MARMOT_WASM, pkg: { ...MARMOT_WASM.pkg, version: "0.9.1" } }],
      PARSED,
      CARGO_TOML,
      MEMBERS,
      LOCK,
      BUN_LOCK,
    );
    expect(errors).toStrictEqual([
      "version mismatch: packages/marmot-wasm/package.json has 0.9.1, Cargo.toml has 0.9.0",
      'bun.lock: workspaces["packages/marmot-wasm"] has version "0.9.0", expected "0.9.1"',
    ]);
  });

  test("rejects a member path dependency without an exact pin", () => {
    const errors = checkVersion(
      PACKAGES,
      PARSED,
      CARGO_TOML,
      {
        "crates/mk-bridge/Cargo.toml": {
          package: { version: { workspace: true } },
          dependencies: { "mk-core": { path: "../mk-core", version: "=0.8.0" } },
        },
      },
      LOCK,
      BUN_LOCK,
    );
    expect(errors).toStrictEqual([
      'crates/mk-bridge/Cargo.toml: internal dependency "mk-core" must use version "=0.9.0", got "=0.8.0"',
    ]);
  });

  test("accepts a member path dependency pinned to the workspace version", () => {
    const errors = checkVersion(
      PACKAGES,
      PARSED,
      CARGO_TOML,
      {
        "crates/mk-bridge/Cargo.toml": {
          package: { version: { workspace: true } },
          dependencies: { "mk-core": { path: "../mk-core", version: "=0.9.0" } },
          "dev-dependencies": { "mk-core": { path: "../mk-core", version: "=0.9.0" } },
        },
      },
      LOCK,
      BUN_LOCK,
    );
    expect(errors).toStrictEqual([]);
  });

  test("rejects a member that does not inherit the workspace version", () => {
    const errors = checkVersion(
      PACKAGES,
      PARSED,
      CARGO_TOML,
      { "crates/mk-core/Cargo.toml": { package: { version: "0.9.0" } } },
      LOCK,
      BUN_LOCK,
    );
    expect(errors).toStrictEqual([
      "crates/mk-core/Cargo.toml: must inherit version via version.workspace = true",
    ]);
  });

  test("rejects an internal dependency without an exact pin", () => {
    const parsed = structuredClone(PARSED);
    parsed.workspace.dependencies["mk-core"].version = "^0.9.0";
    const errors = checkVersion(PACKAGES, parsed, CARGO_TOML, MEMBERS, LOCK, BUN_LOCK);
    expect(errors).toStrictEqual([
      'Cargo.toml: internal dependency "mk-core" must use version "=0.9.0", got "^0.9.0"',
    ]);
  });

  test("rejects a stray version string in Cargo.toml", () => {
    const errors = checkVersion(
      PACKAGES,
      PARSED,
      `${CARGO_TOML}\n# pinned at 0.9.0 for the release\n`,
      MEMBERS,
      LOCK,
      BUN_LOCK,
    );
    expect(errors).toStrictEqual([
      'Cargo.toml: "0.9.0" appears 3 times, expected 2 ([workspace.package] plus internal path dependencies)',
    ]);
  });

  test("rejects a stale Cargo.lock member version", () => {
    const lock = structuredClone(LOCK);
    lock.package[0] = { name: "mk-core", version: "0.9.1" };
    const errors = checkVersion(PACKAGES, PARSED, CARGO_TOML, MEMBERS, lock, BUN_LOCK);
    expect(errors).toStrictEqual([
      'Cargo.lock: workspace member "mk-core" has version "0.9.1", expected "0.9.0"',
    ]);
  });

  test("excludedCrateDirs strips the crates/ prefix and skips non-members", () => {
    expect(
      excludedCrates({
        workspace: { exclude: ["crates/interop", "spike", "fuzz", 3] },
      }),
    ).toStrictEqual(new Set(["interop", "spike", "fuzz"]));
    expect(excludedCrates({ workspace: {} })).toStrictEqual(new Set());
  });
});

describe("checkStandaloneApps", () => {
  test("native-smoke is the registered standalone app", () => {
    expect([...STANDALONE_APPS]).toStrictEqual(["native-smoke"]);
  });

  test("accepts a registered private app", () => {
    const errors = checkStandaloneApps([
      { path: "apps/native-smoke/package.json", pkg: { private: true, version: "0.0.0" } },
    ]);
    expect(errors).toStrictEqual([]);
  });

  test("rejects an unregistered app and a non-private one", () => {
    const errors = checkStandaloneApps([
      { path: "apps/other/package.json", pkg: { private: true } },
      { path: "apps/native-smoke/package.json", pkg: {} },
    ]);
    expect(errors).toStrictEqual([
      "apps/other/package.json: not registered in STANDALONE_APPS — apps/ projects are excluded from version checks and must be listed explicitly",
      'apps/native-smoke/package.json: standalone apps must declare "private": true',
    ]);
  });
});
