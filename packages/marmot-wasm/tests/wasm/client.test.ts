import { writeFileSync } from "node:fs";
import { fileURLToPath } from "node:url";

import { describe, expect, test } from "vite-plus/test";

import {
  scenarioCatchup,
  scenarioClockSkew,
  scenarioMain,
  scenarioMode,
  RELAY_1,
  RELAY_2,
} from "../shared/client-e2e.ts";
import type { CatchupReport, ScenarioReport } from "../shared/client-e2e.ts";

function report(name: string, value: ScenarioReport | CatchupReport): void {
  writeFileSync(
    fileURLToPath(new URL(`../../../../target/client-e2e-${name}-node.json`, import.meta.url)),
    `${JSON.stringify(value, null, 2)}\n`,
  );
}

describe("client e2e (node)", () => {
  test(
    "main: 500 messages each way, mid-stream restarts, member churn",
    { timeout: 600_000 },
    async () => {
      const result = await scenarioMain();
      report("main", result);
      expect(result.delivered).toStrictEqual({ a: 500, b: 500 });
    },
  );

  test("drop: a dropping relay still converges", { timeout: 300_000 }, async () => {
    const result = await scenarioMode([[RELAY_2, "drop"]]);
    report("drop", result);
    expect(result.delivered).toStrictEqual({ a: 40, b: 40 });
  });

  test("reject: a rejecting relay still converges", { timeout: 300_000 }, async () => {
    const result = await scenarioMode([[RELAY_2, "reject"]]);
    report("reject", result);
    expect(result.delivered).toStrictEqual({ a: 40, b: 40 });
  });

  test(
    "shuffle: out-of-order duplicate delivery converges once",
    { timeout: 300_000 },
    async () => {
      const result = await scenarioMode([
        [RELAY_1, "shuffle"],
        [RELAY_2, "shuffle"],
      ]);
      report("shuffle", result);
      expect(result.delivered).toStrictEqual({ a: 40, b: 40 });
    },
  );

  test("catch-up: 500 offline messages, no refetch from zero", { timeout: 300_000 }, async () => {
    const result = await scenarioCatchup();
    writeFileSync(
      fileURLToPath(new URL("../../../../target/client-catchup-node.json", import.meta.url)),
      `${JSON.stringify({ messages: result.messages, ms: result.catchupMs }, null, 2)}\n`,
    );
    report("catchup", result);
    expect(result.servedDelta).toBeGreaterThanOrEqual(500);
    expect(result.servedDelta).toBeLessThan(result.storedTotal);
  });

  test(
    "clock skew: ±300 s inside the overlap window still converges",
    { timeout: 300_000 },
    async () => {
      const result = await scenarioClockSkew();
      report("skew", result);
      expect(result.delivered).toStrictEqual({ a: 40, b: 40, c: 80 });
    },
  );
});
