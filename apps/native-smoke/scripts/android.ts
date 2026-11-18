/**
 * Android smoke driver: dedicated 16 KB-page AVD `marmot_m4` on console port 5600, Release APK
 * (Hermes bytecode, no Metro), `adb reverse` to the collector, `flow`, then the kill/verify loop
 * (`am force-stop`). Shuts the emulator down on exit; only ever touches its own serial.
 *
 * Usage: bun scripts/android.ts [--flow] [--kill] [--iterations=N] [--seed=N]
 *        bun scripts/android.ts --perf [--cases=a,b] [--source=R --db=D --key=K --secret=S]
 */

import { spawnSync } from "node:child_process";
import { existsSync, mkdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { homedir } from "node:os";
import { join } from "node:path";

import { startCollector } from "./collector.ts";
import { driveFlow, driveKillLoop, drivePerf } from "./drive.ts";
import type { PlatformDriver } from "./drive.ts";
import { syncLocalPackages } from "./sync-local.ts";

const APP_DIR = new URL("..", import.meta.url).pathname;
const PKG = "xyz.qntx.marmotsmoke";
const AVD_NAME = "marmot_m4";
const SERIAL = "emulator-5600";
const SDK = join(homedir(), "Library/Android/sdk");
const ADB = join(SDK, "platform-tools/adb");
const EMULATOR = join(SDK, "emulator/emulator");
const IMAGE = "system-images;android-37.0;google_apis_playstore_ps16k;arm64-v8a";
const JAVA_HOME = "/Applications/Android Studio.app/Contents/jbr/Contents/Home";
const NODE_BIN = "/Users/xu/.local/share/fnm/node-versions/v24.15.0/installation/bin";
const ENV = {
  ...process.env,
  PATH: `/usr/bin:${NODE_BIN}:${process.env["PATH"] ?? ""}`,
  JAVA_HOME,
  ANDROID_HOME: SDK,
  ANDROID_SDK_ROOT: SDK,
};

function sh(command: string, args: string[], cwd = APP_DIR, env: NodeJS.ProcessEnv = ENV): string {
  const result = spawnSync(command, args, {
    cwd,
    env,
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

const adb = (args: string[]): string => sh(ADB, ["-s", SERIAL, ...args]);

function ensureAvd(): void {
  const avds = sh(EMULATOR, ["-list-avds"]).split("\n").map((l) => l.trim());
  if (avds.includes(AVD_NAME)) {
    return;
  }
  // No avdmanager on PATH; assemble the AVD from the installed 16 KB Pixel template.
  const home = homedir();
  const templateDir = join(home, ".android/avd/Pixel_10_Pro_XL.avd");
  const avdDir = join(home, `.android/avd/${AVD_NAME}.avd`);
  const ini = join(home, `.android/avd/${AVD_NAME}.ini`);
  const config = readFileSync(join(templateDir, "config.ini"), "utf8")
    .replace(/^AvdId=.*$/m, `AvdId=${AVD_NAME}`)
    .replace(/^avd\.ini\.displayname=.*$/m, `avd.ini.displayname=${AVD_NAME}`)
    .replace(/^hw\.ramSize=.*$/m, "hw.ramSize=4096")
    .replace(/^image\.sysdir\.1=.*$/m, `image.sysdir.1=${IMAGE}/`);
  if (!config.includes(`image.sysdir.1=${IMAGE}/`)) {
    throw new Error(`16 KB image ${IMAGE} missing from SDK`);
  }
  mkdirSync(avdDir, { recursive: true });
  writeFileSync(join(avdDir, "config.ini"), config);
  writeFileSync(ini, `avd.ini.encoding=UTF-8\npath=${avdDir}\npath.rel=avd/${AVD_NAME}.avd\ntarget=android-37.0\n`);
  console.log(`created AVD ${AVD_NAME} from ${templateDir}`);
}

async function waitForBoot(): Promise<void> {
  sh(ADB, ["-s", SERIAL, "wait-for-device"]);
  const deadline = Date.now() + 240_000;
  for (;;) {
    const booted = spawnSync(ADB, ["-s", SERIAL, "shell", "getprop", "sys.boot_completed"], {
      env: ENV,
      encoding: "utf8",
    });
    if (String(booted.stdout).trim() === "1") {
      return;
    }
    if (Date.now() > deadline) {
      throw new Error("emulator did not finish booting in 240s");
    }
    await new Promise((resolve) => setTimeout(resolve, 2000));
  }
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
  const keepEmulator = argv.includes("--keep-emulator");
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
  let emulatorUp = false;
  try {
    ensureAvd();
    const serials = sh(ADB, ["devices"]).split("\n");
    if (!serials.some((line) => line.startsWith(SERIAL))) {
      const emu = Bun.spawn(
        [
          EMULATOR, "-avd", AVD_NAME, "-port", "5600",
          "-no-window", "-no-snapshot-save", "-no-audio", "-no-boot-anim", "-no-metrics",
        ],
        { env: ENV, stdout: "ignore", stderr: "ignore" },
      );
      emulatorUp = true;
      emu.unref();
      await waitForBoot();
      console.log(`emulator ${SERIAL} booted`);
    } else {
      console.log(`emulator ${SERIAL} already running (ours — reusing)`);
    }
    adb(["reverse", "tcp:8100", "tcp:8100"]);

    if (!existsSync(join(APP_DIR, "node_modules"))) {
      sh("bun", ["install"]);
    }
    syncLocalPackages();
    if (!existsSync(join(APP_DIR, "android"))) {
      sh("bunx", ["expo", "prebuild", "--platform", "android"]);
    }
    // Release rebuilds reuse a stale JS bundle — clear the release caches first.
    for (const dir of [
      "android/app/build/generated/assets/react/release",
      "android/app/build/intermediates/assets/release",
      "android/app/build/outputs/apk/release",
    ]) {
      rmSync(join(APP_DIR, dir), { recursive: true, force: true });
    }
    sh("./gradlew", [":app:assembleRelease"], join(APP_DIR, "android"));
    adb(["install", "-r", "android/app/build/outputs/apk/release/app-release.apk"]);
    console.log("installed app-release.apk");

    const driver: PlatformDriver = {
      name: "android",
      launch: async (params) => {
        await Promise.resolve();
        // A warm `am start` only delivers onNewIntent, and the app ignores a second
        // run in the same process — force a cold start so getInitialURL picks it up.
        adb(["shell", "am", "force-stop", PKG]);
        const query = Object.entries(params)
          .map(([key, value]) => `${key}=${encodeURIComponent(value)}`)
          .join("&");
        adb([
          "shell", "am", "start",
          "-a", "android.intent.action.VIEW",
          // adb shell re-parses through /system/bin/sh — quote or `&` splits the URL.
          "-d", `'marmotsmoke://run?${query}'`,
        ]);
      },
      kill: async () => {
        await Promise.resolve();
        adb(["shell", "am", "force-stop", PKG]);
      },
      sampleMemory: async () => {
        await Promise.resolve();
        const out = adb(["shell", "dumpsys", "meminfo", PKG]);
        const kb = (pattern: RegExp): number | undefined => {
          const hit = pattern.exec(out);
          return hit === null ? undefined : Number((hit[1] ?? "").replaceAll(",", ""));
        };
        return {
          totalPssKb: kb(/TOTAL PSS:\s+([\d,]+)/),
          totalRssKb: kb(/TOTAL RSS:\s+([\d,]+)/),
          nativeHeapPssKb: kb(/Native Heap:\s+([\d,]+)/),
        };
      },
      device: async () => {
        await Promise.resolve();
        const prop = (key: string): string => adb(["shell", "getprop", key]).trim();
        return {
          model: prop("ro.product.model"),
          osRelease: prop("ro.build.version.release"),
          sdk: prop("ro.build.version.sdk"),
          pageSizeKb: Number(adb(["shell", "getconf", "PAGE_SIZE"]).trim()) / 1024,
          memTotalKb: Number(
            /MemTotal:\s+(\d+)/.exec(adb(["shell", "cat", "/proc/meminfo"]))?.[1] ?? "0",
          ),
          cores: Number(adb(["shell", "nproc"]).trim()),
        };
      },
    };

    if (perf) {
      await drivePerf(driver, collector, {
        cases: perfCases,
        memTimeLimitSec: 3600,
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
    if (emulatorUp && !keepEmulator) {
      spawnSync(ADB, ["-s", SERIAL, "emu", "kill"], { env: ENV });
      console.log("emulator shut down, collector stopped");
    } else {
      console.log("collector stopped");
    }
  }
}

await main();
