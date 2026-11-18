import { writeFileSync } from "node:fs";
import { fileURLToPath } from "node:url";

import { describe, expect, test } from "vite-plus/test";

import { runReleaseFlow } from "../shared/env.ts";
import { panicFlow } from "../shared/panic.ts";

describe("wasm flow (node)", () => {
  test("runFlow completes over the release artifact", { timeout: 300_000 }, async () => {
    const timings = await runReleaseFlow(500);
    writeFileSync(
      fileURLToPath(new URL("../../../../target/flow-timings-node.json", import.meta.url)),
      `${JSON.stringify(timings, null, 2)}\n`,
    );
    expect(timings.open).toBeGreaterThanOrEqual(0);
    expect(timings.send).toBeGreaterThan(0);
    expect(timings.ingest).toBeGreaterThan(0);
  });
});

describe("panic (node)", () => {
  test("a trap poisons the backend and the module", async () => {
    const steps = panicFlow((body) => {
      expect(body).toThrow(/unreachable|panic|trap/i);
    });
    await steps.step1();
    steps.step2();
    steps.step3();
  });
});
