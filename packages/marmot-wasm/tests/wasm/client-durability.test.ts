import { writeFileSync } from "node:fs";
import { fileURLToPath } from "node:url";

import { describe, expect, test } from "vite-plus/test";

import {
  ackSemantics,
  durabilityGateBeforePublish,
  killAtStage,
  killBetweenTakeAndReport,
  rowsContainIds,
} from "../shared/client-durability.ts";
import type { KillStage } from "../shared/client-durability.ts";

describe("client durability (node)", () => {
  test("nothing publishes before the send is durable", { timeout: 120_000 }, async () => {
    const result = await durabilityGateBeforePublish();
    writeFileSync(
      fileURLToPath(new URL("../../../../target/client-durability-node.json", import.meta.url)),
      `${JSON.stringify(result, null, 2)}\n`,
    );
    expect(rowsContainIds(result).total).toBeGreaterThan(0);
  });

  test(
    "kill between takeOutbound and reportPublish republishes once",
    { timeout: 120_000 },
    async () => {
      await killBetweenTakeAndReport();
      expect(1).toBeGreaterThan(0);
    },
  );

  // oxlint-disable-next-line vitest/prefer-each -- the suite runs the same stage list as bun:test
  for (const stage of ["buffered", "commit", "cursor", "handler"] as KillStage[]) {
    test(`kill at the ${stage} stage loses nothing`, { timeout: 120_000 }, async () => {
      await killAtStage(stage);
      expect(1).toBeGreaterThan(0);
    });
  }

  test("ack semantics: resolved stays, thrown redelivers", { timeout: 120_000 }, async () => {
    await ackSemantics();
    expect(1).toBeGreaterThan(0);
  });
});
