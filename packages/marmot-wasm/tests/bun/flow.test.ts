import { describe, expect, test } from "bun:test";
import { writeFileSync } from "node:fs";
import { fileURLToPath } from "node:url";

import { runReleaseFlow } from "../shared/env.ts";
import { panicFlow } from "../shared/panic.ts";

describe("wasm flow (bun)", () => {
  test("runFlow completes over the release artifact", async () => {
    const timings = await runReleaseFlow(500);
    writeFileSync(
      fileURLToPath(new URL("../../../../target/flow-timings-bun.json", import.meta.url)),
      `${JSON.stringify(timings, null, 2)}\n`,
    );
    expect(timings.open).toBeGreaterThanOrEqual(0);
    expect(timings.send).toBeGreaterThan(0);
    expect(timings.ingest).toBeGreaterThan(0);
  }, 300_000);
});

describe("panic (bun)", () => {
  test("a trap poisons the backend and the module", async () => {
    const steps = panicFlow((body) => {
      expect(body).toThrow(/unreachable|panic|trap/i);
    });
    await steps.step1();
    steps.step2();
    steps.step3();
  });
});
