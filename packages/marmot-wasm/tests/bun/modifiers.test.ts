import { describe, expect, test } from "bun:test";
import { writeFileSync } from "node:fs";
import { fileURLToPath } from "node:url";

import { RELAY_1, RELAY_2 } from "../shared/client-e2e.ts";
import { scenarioModifiers } from "../shared/client-modifiers.ts";
import type { ModifierReport } from "../shared/client-modifiers.ts";

function report(name: string, value: ModifierReport): void {
  writeFileSync(
    fileURLToPath(new URL(`../../../../target/modifiers-${name}-bun.json`, import.meta.url)),
    `${JSON.stringify(value, null, 2)}\n`,
  );
}

describe("modifier convergence e2e (bun)", () => {
  test("shuffle: out-of-order delivery converges chats, edits, deletes and reactions", async () => {
    const result = await scenarioModifiers([
      [RELAY_1, "shuffle"],
      [RELAY_2, "shuffle"],
    ]);
    report("shuffle", result);
    expect(result.modifiersBeforeTargets).toBeGreaterThan(0);
    expect(Object.keys(result.delivered)).toStrictEqual(["a", "b", "c"]);
  }, 300_000);

  test("drop: a dropping relay still converges the modifier state", async () => {
    const result = await scenarioModifiers([[RELAY_2, "drop"]]);
    report("drop", result);
    expect(Object.keys(result.delivered)).toStrictEqual(["a", "b", "c"]);
  }, 300_000);
});
