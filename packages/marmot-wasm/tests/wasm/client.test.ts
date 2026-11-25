import { writeFileSync } from "node:fs";
import { fileURLToPath } from "node:url";

import { describe, expect, test } from "vite-plus/test";

import {
  scenarioCatchup,
  scenarioClockSkew,
  scenarioInviteRetry,
  scenarioMain,
  scenarioMode,
  scenarioOfflineCommitRollback,
  scenarioOfflineSend,
  scenarioPreJoinCursor,
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

  test(
    "invite retry: a refused welcome surfaces pending and retryInvites joins",
    { timeout: 300_000 },
    async () => {
      const result = await scenarioInviteRetry();
      report("invite-retry", result);
      expect(result.delivered).toStrictEqual({ a: 1, b: 2, c: 1 });
    },
  );

  test(
    "offline send: not_sent pacing holds the message and the commit until the dial returns",
    { timeout: 300_000 },
    async () => {
      const result = await scenarioOfflineSend();
      report("offline-send", result);
      expect(result.delivered).toStrictEqual({ a: 0, b: 1 });
    },
  );

  test(
    "offline commit rollback: a refused commit rolls back and the group stays usable",
    { timeout: 300_000 },
    async () => {
      const result = await scenarioOfflineCommitRollback();
      report("offline-commit-rollback", result);
      expect(result.delivered).toStrictEqual({ a: 0, b: 1 });
    },
  );

  test(
    "pre-join cursor: a joiner's restart `since` clears the pre-join range",
    { timeout: 300_000 },
    async () => {
      const result = await scenarioPreJoinCursor();
      report("pre-join-cursor", result);
      // A received B's five pre-join chats; C delivered nothing pre-join and nothing after.
      expect(result.delivered).toStrictEqual({ a: 5, b: 0, c: 0 });
    },
  );
});
