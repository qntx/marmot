import { writeFileSync } from "node:fs";
import { fileURLToPath } from "node:url";

import { describe, expect, test } from "vite-plus/test";

import { RELAY_1, RELAY_2 } from "../shared/client-e2e.ts";
import { scenarioModifiers } from "../shared/client-modifiers.ts";
import type { ModifierReport } from "../shared/client-modifiers.ts";

function report(name: string, value: ModifierReport): void {
  writeFileSync(
    fileURLToPath(new URL(`../../../../target/modifiers-${name}-node.json`, import.meta.url)),
    `${JSON.stringify(value, null, 2)}\n`,
  );
}

describe("modifier convergence e2e (node)", () => {
  test(
    "shuffle: out-of-order delivery converges chats, edits, deletes and reactions",
    { timeout: 300_000 },
    async () => {
      const result = await scenarioModifiers([
        [RELAY_1, "shuffle"],
        [RELAY_2, "shuffle"],
      ]);
      report("shuffle", result);
      expect(result.modifiersBeforeTargets).toBeGreaterThan(0);
      expect(Object.keys(result.delivered)).toStrictEqual(["a", "b", "c"]);
    },
  );

  test(
    "drop: a dropping relay still converges the modifier state",
    { timeout: 300_000 },
    async () => {
      const result = await scenarioModifiers([[RELAY_2, "drop"]]);
      report("drop", result);
      expect(Object.keys(result.delivered)).toStrictEqual(["a", "b", "c"]);
    },
  );
});
