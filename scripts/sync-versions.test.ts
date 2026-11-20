import { describe, expect, test } from "vite-plus/test";

import { syncInternalDeps, syncInternalPins } from "./sync-versions.ts";

const INTERNAL = new Set(["@qntx/marmot", "@qntx/marmot-wasm"]);

describe("syncInternalDeps", () => {
  test("rewrites dependency and peer ranges to ^<version>, dev ranges to <version>", () => {
    const pkg = {
      name: "@qntx/marmot-wasm",
      dependencies: { "@qntx/marmot": "^0.9.0" },
      peerDependencies: { "@qntx/marmot": "^0.9.0" },
      devDependencies: { "@qntx/marmot": "0.9.0", typescript: "7.0.2" },
    };
    expect(syncInternalDeps(pkg, "0.9.1", INTERNAL)).toStrictEqual({
      name: "@qntx/marmot-wasm",
      dependencies: { "@qntx/marmot": "^0.9.1" },
      peerDependencies: { "@qntx/marmot": "^0.9.1" },
      devDependencies: { "@qntx/marmot": "0.9.1", typescript: "7.0.2" },
    });
  });

  test("leaves manifests without internal deps untouched", () => {
    const pkg = { name: "@qntx/marmot", dependencies: { "@noble/curves": "^2.4.0" } };
    expect(syncInternalDeps(pkg, "0.9.1", INTERNAL)).toStrictEqual(pkg);
  });
});

describe("syncInternalPins", () => {
  const MANIFEST = `[package]
name    = "interop"
version = "0.0.0"

[dependencies]
mk-core   = { path = "../mk-core", version = "=0.9.0", features = ["testing"] }
mk-bridge = { path = "../mk-bridge", version = "=0.9.0" }
nostr     = { git = "https://github.com/erskingardner/rust-nostr", rev = "e93cc06b5624f06c21ecd8111a124176eb0c7589", version = "=0.45.5" }
serde     = { version = "1" }
`;

  test("rewrites =<v> pins on internal path deps only", () => {
    expect(syncInternalPins(MANIFEST, "0.9.1")).toBe(`[package]
name    = "interop"
version = "0.0.0"

[dependencies]
mk-core   = { path = "../mk-core", version = "=0.9.1", features = ["testing"] }
mk-bridge = { path = "../mk-bridge", version = "=0.9.1" }
nostr     = { git = "https://github.com/erskingardner/rust-nostr", rev = "e93cc06b5624f06c21ecd8111a124176eb0c7589", version = "=0.45.5" }
serde     = { version = "1" }
`);
  });

  test("leaves a manifest without path-dep pins untouched", () => {
    const manifest = `[workspace.dependencies]\nserde = "1"\n`;
    expect(syncInternalPins(manifest, "0.9.1")).toBe(manifest);
  });
});
