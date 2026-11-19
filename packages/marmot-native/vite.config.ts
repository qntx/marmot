import { fileURLToPath } from "node:url";

import { defineConfig } from "vite-plus";
import type { UserConfig } from "vite-plus";

const config: UserConfig = defineConfig({
  pack: {
    tsconfig: "tsconfig.pack.json",
    entry: {
      index: "src/index.ts",
    },
    dts: {
      generator: "tsgo",
    },
    deps: {
      neverBundle: ["@qntx/marmot", "react-native", "react-native-nitro-modules"],
    },
  },
  test: {
    include: ["tests/*.test.ts"],
    alias: {
      // Tests resolve @qntx/marmot from source so the suite runs before any dist exists.
      "@qntx/marmot/backend": fileURLToPath(
        new URL("../marmot/src/backend/index.ts", import.meta.url),
      ),
    },
  },
});

export default config;
