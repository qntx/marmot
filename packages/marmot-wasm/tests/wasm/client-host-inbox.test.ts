import { writeFileSync } from "node:fs";
import { fileURLToPath } from "node:url";

import { describe, expect, test } from "vite-plus/test";

import {
  scenarioCrashRefeed,
  scenarioDecryptAndRefeed,
  scenarioDifferential,
  scenarioSignerRefused,
} from "../shared/client-host-inbox.ts";

describe("client host-owned inbox (node)", () => {
  test(
    "a host-owned copy joins from a forwarded wrap and converges with the client-owned copy",
    { timeout: 120_000 },
    async () => {
      const result = await scenarioDifferential();
      expect(result.ms).toBeGreaterThan(0);
    },
  );

  test(
    "only forwarded wraps cost decrypts; re-feeds dedup, non-Welcomes never join",
    { timeout: 120_000 },
    async () => {
      const result = await scenarioDecryptAndRefeed();
      writeFileSync(
        fileURLToPath(new URL("../../../../target/client-host-inbox-node.json", import.meta.url)),
        `${JSON.stringify(result, null, 2)}\n`,
      );
      expect(result.welcomeDecrypts).toBe(2);
      expect(result.duplicateDecrypts).toBe(0);
    },
  );

  test(
    "a crash during the held ingest drops the wrap; re-feeding joins then dedups",
    { timeout: 120_000 },
    async () => {
      const result = await scenarioCrashRefeed();
      expect(result.ms).toBeGreaterThan(0);
    },
  );

  test(
    "a refused unwrap is pending and re-delivery is the host re-forwarding",
    { timeout: 120_000 },
    async () => {
      const result = await scenarioSignerRefused();
      expect(result.ms).toBeGreaterThan(0);
    },
  );
});
