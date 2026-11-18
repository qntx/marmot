/**
 * The platform-independent smoke driver: `flow` once, then the kill/verify loop. A
 * `PlatformDriver` supplies launch/kill/primitives; this module owns the journal waits, the
 * seeded kill delays (same mulberry32 default as kill.spec.ts), the per-iteration checks, and
 * the JSON summaries under `target/`.
 */

import { generateSecretKey } from "nostr-tools";
import { randomBytes } from "node:crypto";
import { mkdirSync, writeFileSync } from "node:fs";
import { join } from "node:path";

import { checkIteration, mulberry32 } from "./checks.ts";
import type { IterationReport } from "./checks.ts";
import { Collector } from "./collector.ts";

export type PlatformDriver = {
  name: string;
  /**
   * Launch the app fresh with these run params. iOS writes `Documents/run.json` +
   * `simctl launch`; Android builds the `marmotsmoke://run?...` deep link + `am start -d`.
   */
  launch: (params: Record<string, string>) => Promise<void>;
  /** SIGKILL the running app process; no-op if it already died. */
  kill: () => Promise<void>;
};

const TARGET = new URL("../target/", import.meta.url).pathname;

export const sleep = async (ms: number): Promise<void> =>
  new Promise((resolve) => {
    setTimeout(resolve, ms);
  });

function hex(bytes: Uint8Array): string {
  return Buffer.from(bytes).toString("hex");
}

/** `flow` mode: launch, await the result record, return it. */
export async function driveFlow(
  driver: PlatformDriver,
  collector: Collector,
  messages = 500,
): Promise<Record<string, unknown>> {
  const run = `${driver.name}-flow-${Date.now()}`;
  await driver.launch({ mode: "flow", run, messages: String(messages) });
  const result = await collector.waitFor(
    run,
    (j) => j.results.at(-1),
    15 * 60 * 1000,
    "flow result",
  );
  mkdirSync(TARGET, { recursive: true });
  const out = join(TARGET, `flow-${driver.name}.json`);
  writeFileSync(out, JSON.stringify({ run, result }, null, 2));
  console.log(`flow ${driver.name}: ok=${String(result.ok)} -> ${out}`);
  return result;
}

export type KillSummary = {
  platform: string;
  iterations: number;
  kills: number;
  gaps: { min: number; max: number };
  reports: IterationReport[];
};

/** The kill/verify loop: >=1 journal batch, seeded delay, SIGKILL, relaunch + verify + checks. */
export async function driveKillLoop(
  driver: PlatformDriver,
  collector: Collector,
  { iterations = 30, seed = 20260209 }: { iterations?: number; seed?: number } = {},
): Promise<KillSummary> {
  const prng = mulberry32(seed);
  const reports: IterationReport[] = [];
  mkdirSync(TARGET, { recursive: true });

  for (let i = 0; i < iterations; i += 1) {
    const db = `kill-${seed}-${i}`;
    const key = hex(randomBytes(32));
    const secret = hex(generateSecretKey());
    const killRun = `${driver.name}-k${i}`;
    const verifyRun = `${driver.name}-v${i}`;

    // oxlint-disable-next-line no-await-in-loop -- iterations are strictly sequential
    await driver.launch({ mode: "kill", run: killRun, db, key, secret });
    // oxlint-disable-next-line no-await-in-loop -- iterations are strictly sequential
    await collector.waitFor(killRun, (j) => (j.batches.length > 0 ? true : undefined), 60_000, "first batch");
    const delayMs = 20 + Math.floor(prng() * 400);
    // oxlint-disable-next-line no-await-in-loop -- iterations are strictly sequential
    await sleep(delayMs);
    // oxlint-disable-next-line no-await-in-loop -- iterations are strictly sequential
    await driver.kill();

    // oxlint-disable-next-line no-await-in-loop -- iterations are strictly sequential
    await driver.launch({ mode: "verify", run: verifyRun, db, key, secret });
    // oxlint-disable-next-line no-await-in-loop -- iterations are strictly sequential
    await collector.waitFor(verifyRun, (j) => j.dumps.at(-1), 60_000, "verify dump");
    // oxlint-disable-next-line no-await-in-loop -- iterations are strictly sequential
    const verify = await collector.waitFor(
      verifyRun,
      (j) => j.results.at(-1),
      60_000,
      "verify result",
    );
    if (verify.ok !== true) {
      throw new Error(`iteration ${i}: verify failed: ${JSON.stringify(verify)}`);
    }
    const journal = collector.journal(killRun);
    const verifyJournal = collector.journal(verifyRun);
    const violations = [...journal.violations, ...verifyJournal.violations];
    if (violations.length > 0) {
      throw new Error(`iteration ${i}: backend violations: ${violations.join("; ")}`);
    }
    const rows = verifyJournal.dumps.at(-1);
    if (rows === undefined) {
      throw new Error(`iteration ${i}: verify run recorded no dump`);
    }
    const report = checkIteration(i, journal, rows);
    reports.push(report);
    console.log(
      `kill ${driver.name} #${i}: batches=${report.batches} prefix=${report.durablePrefix} ` +
        `lastAcked=${report.lastAcked} published=${report.published} gap=${report.gap}`,
    );
    writeFileSync(
      join(TARGET, `kill-${driver.name}-${i}.json`),
      JSON.stringify({ report, journal, rows }, null, 2),
    );
    await driver.kill();
  }

  const gaps = reports.map((report) => report.gap);
  const summary: KillSummary = {
    platform: driver.name,
    iterations: reports.length,
    kills: reports.length,
    gaps: { min: Math.min(...gaps), max: Math.max(...gaps) },
    reports,
  };
  const out = join(TARGET, `kill-${driver.name}.json`);
  writeFileSync(out, JSON.stringify({ seed, summary }, null, 2));
  console.log(
    `kill ${driver.name}: ${summary.iterations} iterations, gap min=${summary.gaps.min} max=${summary.gaps.max} -> ${out}`,
  );
  return summary;
}
