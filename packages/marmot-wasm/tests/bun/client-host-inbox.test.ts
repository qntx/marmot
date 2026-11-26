import { describe, expect, test } from "bun:test";
import { writeFileSync } from "node:fs";
import { fileURLToPath } from "node:url";

import {
  scenarioCrashRefeed,
  scenarioDecryptAndRefeed,
  scenarioDifferential,
  scenarioSignerRefused,
} from "../shared/client-host-inbox.ts";

describe("client host-owned inbox (bun)", () => {
  test("a host-owned copy joins from a forwarded wrap and converges with the client-owned copy", async () => {
    const result = await scenarioDifferential();
    expect(result.ms).toBeGreaterThan(0);
  }, 120_000);

  test("only forwarded wraps cost decrypts; re-feeds dedup, non-Welcomes never join", async () => {
    const result = await scenarioDecryptAndRefeed();
    writeFileSync(
      fileURLToPath(new URL("../../../../target/client-host-inbox-bun.json", import.meta.url)),
      `${JSON.stringify(result, null, 2)}\n`,
    );
    expect(result.welcomeDecrypts).toStrictEqual(2);
    expect(result.duplicateDecrypts).toStrictEqual(0);
  }, 120_000);

  test("a crash during the held ingest drops the wrap; re-feeding joins then dedups", async () => {
    const result = await scenarioCrashRefeed();
    expect(result.ms).toBeGreaterThan(0);
  }, 120_000);

  test("a refused unwrap is pending and re-delivery is the host re-forwarding", async () => {
    const result = await scenarioSignerRefused();
    expect(result.ms).toBeGreaterThan(0);
  }, 120_000);
});
