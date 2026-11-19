import { fileURLToPath } from "node:url";

import { defineConfig } from "vite-plus";
import type { UserConfig } from "vite-plus";

import { applyPackExports } from "../marmot/vite.config.ts";

const config: UserConfig = defineConfig({
  pack: {
    tsconfig: "tsconfig.pack.json",
    deps: { resolveDepSubpath: true },
    entry: {
      index: "src/index.ts",
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
    include: ["tests/{unit,wasm}/**/*.test.ts"],
    alias: {
      // Tests resolve @qntx/marmot from source so the suite runs before any dist exists.
      "@qntx/marmot/backend": fileURLToPath(
        new URL("../marmot/src/backend/index.ts", import.meta.url),
      ),
      "@qntx/marmot/client": fileURLToPath(
        new URL("../marmot/src/client/index.ts", import.meta.url),
      ),
      "@qntx/marmot/storage": fileURLToPath(
        new URL("../marmot/src/storage/index.ts", import.meta.url),
      ),
      "@qntx/marmot/testing": fileURLToPath(
        new URL("../marmot/src/testing/index.ts", import.meta.url),
      ),
      "@qntx/marmot": fileURLToPath(new URL("../marmot/src/index.ts", import.meta.url)),
    },
  },
});

export default config;
