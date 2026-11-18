/**
 * iOS smoke driver: dedicated `marmot-m4` simulator under a shared-machine slot, Release build
 * (Hermes bytecode, no Metro), install, `flow`, then the kill/verify loop (`kill -9` the sim app
 * process — simulator apps are host processes). Cleans up device, slot, and collector on exit.
 *
 * Usage: bun scripts/ios.ts [--flow] [--kill] [--iterations=N] [--seed=N]
 *        bun scripts/ios.ts --perf [--cases=a,b] [--keep-device]
 *               [--source=R --db=D --key=K --secret=S]
 */

import { spawnSync } from "node:child_process";
import { existsSync, mkdirSync, rmSync, writeFileSync } from "node:fs";
import { join } from "node:path";

import { startCollector } from "./collector.ts";
import { driveFlow, driveKillLoop, drivePerf } from "./drive.ts";
import type { PlatformDriver } from "./drive.ts";
import { syncLocalPackages } from "./sync-local.ts";

const APP_DIR = new URL("..", import.meta.url).pathname;
const SIM_NAME = "marmot-m4";
const BUNDLE_ID = "xyz.qntx.marmotsmoke";
const SLOTS_ROOT = "/Users/xu/Desktop/qntx/meowl.social/.cache/sim-slots";
const DERIVED = join(APP_DIR, "target/dd");
// Expo/pod/xcodebuild under a Homebrew PATH pick an invalid SDK sysroot; the JS bundle step
// still needs node (fnm), so it sits after /usr/bin.
const NODE_BIN = "/Users/xu/.local/share/fnm/node-versions/v24.15.0/installation/bin";
const ENV = {
  ...process.env,
  PATH: `/usr/bin:${NODE_BIN}:${process.env["PATH"] ?? ""}`,
};

function sh(command: string, args: string[], cwd = APP_DIR): string {
  const result = spawnSync(command, args, {
    cwd,
    env: ENV,
    encoding: "utf8",
    maxBuffer: 512 * 1024 * 1024,
  });
  if (result.status !== 0) {
    throw new Error(
      `${command} ${args.join(" ")} failed (${String(result.status)}):\n${result.stderr}\n${result.stdout}`,
    );
  }
  return result.stdout ?? "";
}

function acquireSlot(): string {
  mkdirSync(SLOTS_ROOT, { recursive: true });
  for (let n = 1; n <= 3; n += 1) {
    const dir = join(SLOTS_ROOT, `slot-${n}`);
    try {
      mkdirSync(dir);
    } catch {
      continue; // taken by another lane
    }
    writeFileSync(join(dir, "owner"), "marmot-m4");
    return dir;
  }
  throw new Error("all iOS simulator slots are taken — retry later");
}

type SimDevice = { udid: string; name: string; state?: string };

function ensureDevice(): string {
  const list = JSON.parse(
    sh("xcrun", ["simctl", "list", "devices", "-j", "available"]),
  ) as { devices: Record<string, SimDevice[]> };
  for (const devices of Object.values(list.devices)) {
    for (const device of devices) {
      if (device.name === SIM_NAME) {
        return device.udid;
      }
    }
  }
  const runtimes = (
    JSON.parse(sh("xcrun", ["simctl", "list", "runtimes", "-j"])) as {
      runtimes: { identifier: string; name: string; version?: string; isAvailable?: boolean }[];
    }
  ).runtimes.filter((r) => r.identifier.includes("SimRuntime.iOS") && r.isAvailable !== false);
  const runtime = [...runtimes].sort((a, b) =>
    (a.version ?? "").localeCompare(b.version ?? "", undefined, { numeric: true }),
  ).at(-1);
  if (runtime === undefined) {
    throw new Error("no iOS runtime available");
  }
  // simctl's min/maxRuntimeVersion is the semver triple packed (major<<16|minor<<8|patch).
  const [major = 0, minor = 0, patch = 0] = (runtime.version ?? "0").split(".").map(Number);
  const runtimeVersion = (major << 16) | (minor << 8) | patch;
  const deviceType = (
    JSON.parse(sh("xcrun", ["simctl", "list", "devicetypes", "-j"])) as {
      devicetypes: {
        identifier: string;
        name: string;
        productFamily?: string;
        minRuntimeVersion?: number;
        maxRuntimeVersion?: number;
      }[];
    }
  ).devicetypes.find(
    (d) =>
      d.productFamily === "iPhone" &&
      (d.minRuntimeVersion ?? 0) <= runtimeVersion &&
      runtimeVersion <= (d.maxRuntimeVersion ?? Number.MAX_SAFE_INTEGER),
  );
  if (deviceType === undefined) {
    throw new Error(`no iPhone device type compatible with ${runtime.name}`);
  }
  const udid = sh("xcrun", [
    "simctl",
    "create",
    SIM_NAME,
    deviceType.identifier,
    runtime.identifier,
  ]).trim();
  console.log(`created simulator ${SIM_NAME} (${deviceType.name}, ${runtime.name}) = ${udid}`);
  return udid;
}

function appPid(udid: string): number | undefined {
  const out = sh("xcrun", ["simctl", "spawn", udid, "launchctl", "list"]);
  for (const line of out.split("\n")) {
    if (line.includes(BUNDLE_ID)) {
      const pid = Number(line.trim().split(/\s+/)[0]);
      return Number.isFinite(pid) && pid > 0 ? pid : undefined;
    }
  }
  return undefined;
}

function describeDevice(udid: string): Record<string, unknown> {
  const list = JSON.parse(
    sh("xcrun", ["simctl", "list", "devices", "-j", "available"]),
  ) as { devices: Record<string, { udid: string; deviceTypeIdentifier?: string }[]> };
  for (const [runtime, devices] of Object.entries(list.devices)) {
    for (const device of devices) {
      if (device.udid === udid) {
        const types = (
          JSON.parse(sh("xcrun", ["simctl", "list", "devicetypes", "-j"])) as {
            devicetypes: { identifier: string; name: string }[];
          }
        ).devicetypes;
        return {
          udid,
          deviceType: types.find((t) => t.identifier === device.deviceTypeIdentifier)?.name ??
            device.deviceTypeIdentifier,
          // Key form: com.apple.CoreSimulator.SimRuntime.iOS-27-2 → "iOS 27.2".
          runtime: runtime
            .replace(/^.*SimRuntime\./, "")
            .replace(/-(\d+)-(\d+)$/, " $1.$2"),
        };
      }
    }
  }
  return { udid };
}

async function main(): Promise<void> {
  const argv = process.argv.slice(2);
  const flag = (name: string): string | undefined =>
    argv.find((a) => a.startsWith(`--${name}=`))?.split("=")[1];
  const iterations = Number(flag("iterations") ?? "30");
  const seed = Number(flag("seed") ?? "20260209");
  const onlyFlow = argv.includes("--flow");
  const onlyKill = argv.includes("--kill");
  const perf = argv.includes("--perf");
  const perfCases = flag("cases")?.split(",");
  const perfMembers = flag("members") === undefined ? undefined : Number(flag("members"));
  const keepDevice = argv.includes("--keep-device");
  const memrecv =
    flag("source") === undefined
      ? undefined
      : {
          source: flag("source") as string,
          db: flag("db") ?? "",
          key: flag("key") ?? "",
          secret: flag("secret") ?? "",
        };

  const { collector, stop } = startCollector();
  const slot = acquireSlot();
  let udid: string | undefined;
  try {
    udid = ensureDevice();
    const boot = spawnSync("xcrun", ["simctl", "boot", udid], { env: ENV });
    if (boot.status !== 0 && !String(boot.stderr).includes("Booted")) {
      throw new Error(`simctl boot failed: ${String(boot.stderr)}`);
    }
    sh("xcrun", ["simctl", "bootstatus", udid, "-b"], APP_DIR);
    console.log(`simulator ${udid} booted (slot ${slot})`);

    if (!existsSync(join(APP_DIR, "node_modules"))) {
      sh("bun", ["install"]);
    }
    syncLocalPackages();
    if (!existsSync(join(APP_DIR, "ios"))) {
      sh("bunx", ["expo", "prebuild", "--platform", "ios"]);
    }
    sh("xcodebuild", [
      "-workspace", "ios/MarmotSmoke.xcworkspace",
      "-scheme", "MarmotSmoke",
      "-configuration", "Release",
      "-destination", `id=${udid}`,
      "-derivedDataPath", DERIVED,
      "CODE_SIGNING_ALLOWED=NO",
      // MarmotFfi.xcframework is arm64-only (device + Apple-Silicon simulator).
      "ARCHS=arm64",
      "build",
    ]);
    const app = join(DERIVED, "Build/Products/Release-iphonesimulator/MarmotSmoke.app");
    sh("xcrun", ["simctl", "install", udid, app]);
    console.log(`installed ${app}`);
    // Deep links raise a per-open consent alert a headless run cannot dismiss — the app reads
    // its run params from Documents/run.json instead (written before `simctl launch`).
    const dataContainer = sh("xcrun", [
      "simctl", "get_app_container", udid, BUNDLE_ID, "data",
    ]).trim();
    mkdirSync(join(dataContainer, "Documents"), { recursive: true });
    const runFile = join(dataContainer, "Documents", "run.json");

    const driver: PlatformDriver = {
      name: "ios",
      launch: async (params) => {
        await Promise.resolve();
        writeFileSync(runFile, JSON.stringify(params));
        sh("xcrun", [
          "simctl", "launch", "--terminate-running-process", udid as string, BUNDLE_ID,
        ]);
      },
      kill: async () => {
        await Promise.resolve();
        const pid = appPid(udid as string);
        if (pid !== undefined) {
          spawnSync("kill", ["-9", String(pid)]);
        }
      },
      sampleMemory: async () => {
        await Promise.resolve();
        const pid = appPid(udid as string);
        if (pid === undefined) {
          return { pid: null };
        }
        const rssKb = Number(sh("ps", ["-o", "rss=", "-p", String(pid)]).trim());
        const footprint = sh("footprint", [String(pid)]);
        const phys = /phys_footprint:\s*([\d.]+)\s*(\w+)/.exec(footprint);
        return {
          pid,
          rssKb,
          physFootprint: phys === null ? undefined : `${phys[1]} ${phys[2]}`,
        };
      },
      device: async () => {
        await Promise.resolve();
        return describeDevice(udid as string);
      },
    };

    if (perf) {
      await drivePerf(driver, collector, {
        cases: perfCases,
        memTimeLimitSec: 1500,
        ...(perfMembers === undefined ? {} : { members: perfMembers }),
        ...(memrecv === undefined ? {} : { memrecv }),
      });
    } else {
      if (!onlyKill) {
        const flow = await driveFlow(driver, collector);
        if (flow.ok !== true) {
          throw new Error(`flow failed: ${JSON.stringify(flow)}`);
        }
        await driver.kill();
      }
      if (!onlyFlow) {
        await driveKillLoop(driver, collector, { iterations, seed });
      }
    }
  } finally {
    stop();
    if (udid !== undefined) {
      spawnSync("xcrun", ["simctl", "shutdown", udid], { env: ENV });
      if (!keepDevice) {
        spawnSync("xcrun", ["simctl", "delete", udid], { env: ENV });
      }
    }
    rmSync(slot, { recursive: true, force: true });
    console.log("simulator shut down, slot released, collector stopped");
  }
}

await main();
