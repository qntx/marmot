import { writeFileSync } from "node:fs";
import { fileURLToPath } from "node:url";

import { describe, expect, test } from "vite-plus/test";

import { scenarioAppSurface } from "../shared/client-app-surface.ts";
import type { AppSurfaceReport } from "../shared/client-app-surface.ts";
import { RELAY_1, RELAY_2 } from "../shared/client-e2e.ts";

function report(name: string, value: AppSurfaceReport): void {
  writeFileSync(
    fileURLToPath(new URL(`../../../../target/app-surface-${name}-node.json`, import.meta.url)),
    `${JSON.stringify(value, null, 2)}\n`,
  );
}

describe("app surface e2e (node)", () => {
  test(
    "typed events, rowIds and snapshots converge over a normal relay",
    { timeout: 300_000 },
    async () => {
      const result = await scenarioAppSurface([]);
      report("normal", result);
      expect(Object.keys(result.delivered)).toStrictEqual(["a", "b", "c"]);
    },
  );

  test(
    "drop: a dropping relay still converges the typed surface",
    { timeout: 300_000 },
    async () => {
      const result = await scenarioAppSurface([[RELAY_2, "drop"]]);
      report("drop", result);
      expect(Object.keys(result.delivered)).toStrictEqual(["a", "b", "c"]);
    },
  );

  test(
    "reject: a rejecting relay still converges the typed surface",
    { timeout: 300_000 },
    async () => {
      const result = await scenarioAppSurface([[RELAY_2, "reject"]]);
      report("reject", result);
      expect(Object.keys(result.delivered)).toStrictEqual(["a", "b", "c"]);
    },
  );

  test(
    "shuffle: out-of-order duplicate delivery still converges",
    { timeout: 300_000 },
    async () => {
      const result = await scenarioAppSurface([
        [RELAY_1, "shuffle"],
        [RELAY_2, "shuffle"],
      ]);
      report("shuffle", result);
      expect(Object.keys(result.delivered)).toStrictEqual(["a", "b", "c"]);
    },
  );
});
