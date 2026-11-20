import { describe, expect, test } from "bun:test";
import { writeFileSync } from "node:fs";
import { fileURLToPath } from "node:url";

import { scenarioAppSurface } from "../shared/client-app-surface.ts";
import type { AppSurfaceReport } from "../shared/client-app-surface.ts";
import { RELAY_1, RELAY_2 } from "../shared/client-e2e.ts";

function report(name: string, value: AppSurfaceReport): void {
  writeFileSync(
    fileURLToPath(new URL(`../../../../target/app-surface-${name}-bun.json`, import.meta.url)),
    `${JSON.stringify(value, null, 2)}\n`,
  );
}

describe("app surface e2e (bun)", () => {
  test("typed events, rowIds and snapshots converge over a normal relay", async () => {
    const result = await scenarioAppSurface([]);
    report("normal", result);
    expect(Object.keys(result.delivered)).toStrictEqual(["a", "b", "c"]);
  }, 300_000);

  test("drop: a dropping relay still converges the typed surface", async () => {
    const result = await scenarioAppSurface([[RELAY_2, "drop"]]);
    report("drop", result);
    expect(Object.keys(result.delivered)).toStrictEqual(["a", "b", "c"]);
  }, 300_000);

  test("reject: a rejecting relay still converges the typed surface", async () => {
    const result = await scenarioAppSurface([[RELAY_2, "reject"]]);
    report("reject", result);
    expect(Object.keys(result.delivered)).toStrictEqual(["a", "b", "c"]);
  }, 300_000);

  test("shuffle: out-of-order duplicate delivery still converges", async () => {
    const result = await scenarioAppSurface([
      [RELAY_1, "shuffle"],
      [RELAY_2, "shuffle"],
    ]);
    report("shuffle", result);
    expect(Object.keys(result.delivered)).toStrictEqual(["a", "b", "c"]);
  }, 300_000);
});
