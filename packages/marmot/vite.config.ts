import { defineConfig } from "vite-plus";
import type { UserConfig } from "vite-plus";

/** ESM-only sugar flattens to a string; keep types/import/default for pack entries. */
export function applyPackExports(pkgExports: Record<string, unknown>): Record<string, unknown> {
  for (const [key, value] of Object.entries(pkgExports)) {
    if (typeof value !== "string" || !value.endsWith(".mjs")) {
      continue;
    }
    pkgExports[key] = {
      types: value.replace(/\.mjs$/, ".d.mts"),
      import: value,
      default: value,
    };
  }
  return pkgExports;
}

const config: UserConfig = defineConfig({
  pack: {
    deps: { resolveDepSubpath: true },
    entry: {
      index: "src/index.ts",
      core: "src/core/index.ts",
      backend: "src/backend/index.ts",
      client: "src/client/index.ts",
      storage: "src/storage/index.ts",
      testing: "src/testing/index.ts",
    },
    dts: {
      generator: "tsgo",
    },
    sourcemap: true,
    exports: {
      customExports: applyPackExports,
    },
  },
  test: {
    // tests/bun runs under `bun test` (bun-only modules like bun:sqlite), not vp test.
    include: ["tests/*.test.ts"],
  },
});

export default config;
