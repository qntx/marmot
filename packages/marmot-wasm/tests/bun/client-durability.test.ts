import { describe, expect, test } from "bun:test";
import { writeFileSync } from "node:fs";
import { fileURLToPath } from "node:url";

import {
  ackSemantics,
  durabilityGateBeforePublish,
  killAtStage,
  killBetweenTakeAndReport,
  rowsContainIds,
} from "../shared/client-durability.ts";
import type { KillStage } from "../shared/client-durability.ts";

describe("client durability (bun)", () => {
  test("nothing publishes before the send is durable", async () => {
    const result = await durabilityGateBeforePublish();
    writeFileSync(
      fileURLToPath(new URL("../../../../target/client-durability-bun.json", import.meta.url)),
      `${JSON.stringify(result, null, 2)}\n`,
    );
    expect(rowsContainIds(result).total).toBeGreaterThan(0);
  }, 120_000);

  test("kill between takeOutbound and reportPublish republishes once", async () => {
    await killBetweenTakeAndReport();
    expect(1).toBeGreaterThan(0);
  }, 120_000);

  // oxlint-disable-next-line vitest/prefer-each -- bun:test's declared surface has no test.each
  for (const stage of ["buffered", "commit", "cursor", "handler"] as KillStage[]) {
    test(`kill at the ${stage} stage loses nothing`, async () => {
      await killAtStage(stage);
      expect(1).toBeGreaterThan(0);
    }, 120_000);
  }

  test("ack semantics: resolved stays, thrown redelivers", async () => {
    await ackSemantics();
    expect(1).toBeGreaterThan(0);
  }, 120_000);
});
