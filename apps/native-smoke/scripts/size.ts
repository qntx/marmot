/**
 * App-size delta: build the Release app with and without `@qntx/marmot-native` and report
 * sizes. The "without" variant needs two switches that only engage together:
 * `MARMOT_SMOKE_NO_NATIVE=1` (metro → `src/native-stub.ts`) and a temporary
 * `expo.autolinking.exclude` entry in package.json (expo-modules-autolinking reads package.json
 * only — no env/config-file path exists). package.json is always restored in `finally`.
 *
 * Usage: bun scripts/size.ts [--ios] [--android]   (no flag = both)
 * Output: target/appsize.json
 */

import { spawnSync } from "node:child_process";
import { existsSync, mkdirSync, readFileSync, rmSync, statSync, writeFileSync } from "node:fs";
import { join } from "node:path";

const APP_DIR = new URL("..", import.meta.url).pathname;
const PKG_JSON = join(APP_DIR, "package.json");
const NATIVE_PKG = "@qntx/marmot-native";
const NODE_BIN = "/Users/xu/.local/share/fnm/node-versions/v24.15.0/installation/bin";
const ENV = {
  ...process.env,
  PATH: `/usr/bin:${NODE_BIN}:${process.env["PATH"] ?? ""}`,
  JAVA_HOME: "/Applications/Android Studio.app/Contents/jbr/Contents/Home",
  ANDROID_HOME: `${process.env["HOME"] ?? ""}/Library/Android/sdk`,
};

const sh = (command: string, args: string[], cwd = APP_DIR, env = ENV): string => {
  const result = spawnSync(command, args, {
    cwd, env, encoding: "utf8", maxBuffer: 512 * 1024 * 1024,
  });
  if (result.status !== 0) {
    throw new Error(`${command} ${args.join(" ")} failed (${String(result.status)}):\n${result.stderr}\n${result.stdout}`);
  }
  return result.stdout ?? "";
};

const STUB_ENV = { ...ENV, MARMOT_SMOKE_NO_NATIVE: "1" };

/** Add `expo.autolinking.exclude` to package.json while `body` runs; always restored. */
async function withoutNativePackage(body: () => Promise<void>): Promise<void> {
  const original = readFileSync(PKG_JSON, "utf8");
  const pkg = JSON.parse(original) as Record<string, unknown>;
  const expo = (pkg["expo"] ?? {}) as Record<string, unknown>;
  const autolinking = (expo["autolinking"] ?? {}) as Record<string, unknown>;
  autolinking["exclude"] = [...((autolinking["exclude"] as string[] | undefined) ?? []), NATIVE_PKG];
  expo["autolinking"] = autolinking;
  pkg["expo"] = expo;
  writeFileSync(PKG_JSON, JSON.stringify(pkg, null, 2) + "\n");
  try {
    await body();
  } finally {
    writeFileSync(PKG_JSON, original);
  }
}

const zipSize = (path: string, out: string): number => {
  rmSync(out, { force: true });
  sh("zip", ["-qr", out, path]);
  return statSync(out).size;
};

const duKb = (path: string): number => Number(sh("du", ["-sk", path]).split("\t")[0]);

function iosSizes(): Record<string, unknown> {
  const derived = join(APP_DIR, "target/dd-size");
  const build = (env: typeof ENV): void => {
    sh("xcodebuild", [
      "-workspace", "ios/MarmotSmoke.xcworkspace",
      "-scheme", "MarmotSmoke",
      "-configuration", "Release",
      "-destination", "generic/platform=iOS",
      "-derivedDataPath", derived,
      "CODE_SIGNING_ALLOWED=NO",
      "ARCHS=arm64",
      "build",
    ], APP_DIR, env);
  };
  const appPath = join(derived, "Build/Products/Release-iphoneos/MarmotSmoke.app");
  const measure = (): Record<string, number> => ({
    appBytes: duKb(appPath) * 1024,
    zippedBytes: zipSize(appPath, join(APP_DIR, "target/MarmotSmoke.zip")),
  });

  sh("pod", ["install"], join(APP_DIR, "ios"));
  build(ENV);
  const withNative = measure();
  return { withNative };
}

async function ios(): Promise<Record<string, unknown>> {
  const sizes = iosSizes();
  let without: Record<string, number> = {};
  await withoutNativePackage(async () => {
    await Promise.resolve();
    sh("pod", ["install"], join(APP_DIR, "ios"), STUB_ENV);
    const lock = readFileSync(join(APP_DIR, "ios/Podfile.lock"), "utf8");
    if (lock.includes("MarmotNative")) {
      throw new Error("MarmotNative still in Podfile.lock under the exclude — size build is invalid");
    }
    const derived = join(APP_DIR, "target/dd-size");
    sh("xcodebuild", [
      "-workspace", "ios/MarmotSmoke.xcworkspace",
      "-scheme", "MarmotSmoke",
      "-configuration", "Release",
      "-destination", "generic/platform=iOS",
      "-derivedDataPath", derived,
      "CODE_SIGNING_ALLOWED=NO",
      "ARCHS=arm64",
      "build",
    ], APP_DIR, STUB_ENV);
    const appPath = join(derived, "Build/Products/Release-iphoneos/MarmotSmoke.app");
    without = {
      appBytes: duKb(appPath) * 1024,
      zippedBytes: zipSize(appPath, join(APP_DIR, "target/MarmotSmoke.zip")),
    };
  });
  // Restore the normal pods so the next real build starts clean.
  sh("pod", ["install"], join(APP_DIR, "ios"));
  return { ...sizes, withoutNative: without };
}

function apkSize(abi: string, stub: boolean): number {
  const android = join(APP_DIR, "android");
  for (const dir of [
    "app/build/generated/assets/react/release",
    "app/build/intermediates/assets/release",
    "app/build/outputs/apk/release",
  ]) {
    rmSync(join(android, dir), { recursive: true, force: true });
  }
  sh("./gradlew", [":app:assembleRelease", `-PreactNativeArchitectures=${abi}`], android, stub ? STUB_ENV : ENV);
  return statSync(join(android, "app/build/outputs/apk/release/app-release.apk")).size;
}

async function android(): Promise<Record<string, unknown>> {
  const abis = ["arm64-v8a", "x86_64"];
  const sizes: Record<string, Record<string, number>> = {};
  await withoutNativePackage(async () => {
    await Promise.resolve();
    for (const abi of abis) {
      // oxlint-disable-next-line no-await-in-loop -- builds are sequential
      sizes[`${abi}-without`] = { apkBytes: apkSize(abi, true) };
    }
  });
  for (const abi of abis) {
    // oxlint-disable-next-line no-await-in-loop -- builds are sequential
    sizes[`${abi}-with`] = { apkBytes: apkSize(abi, false) };
  }
  return sizes;
}

/** Raw + gzip-9 sizes of the shipped native artifacts (already llvm-strip'ed by build:native). */
function librarySizes(): Record<string, unknown> {
  const out: Record<string, Record<string, number>> = {};
  const pkg = join(APP_DIR, "node_modules/@qntx/marmot-native");
  const gz = (path: string): number => {
    const result = spawnSync("gzip", ["-9c", path], { env: ENV, maxBuffer: 512 * 1024 * 1024 });
    if (result.status !== 0) {
      throw new Error(`gzip -9c ${path} failed`);
    }
    return (result.stdout as Buffer).length;
  };
  for (const abi of ["arm64-v8a", "x86_64"]) {
    const so = join(pkg, `android/src/main/jniLibs/${abi}/libmarmot_ffi.so`);
    if (existsSync(so)) {
      out[`android-${abi}`] = { bytes: statSync(so).size, gzipped: gz(so) };
    }
  }
  for (const slice of ["ios-arm64", "ios-arm64-simulator"]) {
    const fw = join(pkg, `ios/MarmotFfi.xcframework/${slice}/MarmotFfi.framework/MarmotFfi`);
    if (existsSync(fw)) {
      out[slice] = { bytes: statSync(fw).size, gzipped: gz(fw) };
    }
  }
  return out;
}

async function main(): Promise<void> {
  const argv = process.argv.slice(2);
  const doIos = !argv.includes("--android");
  const doAndroid = !argv.includes("--ios");
  const report: Record<string, unknown> = { libraries: librarySizes() };
  if (doAndroid && existsSync(join(APP_DIR, "android"))) {
    report["android"] = await android();
  }
  if (doIos && existsSync(join(APP_DIR, "ios"))) {
    report["ios"] = await ios();
  }
  mkdirSync(join(APP_DIR, "target"), { recursive: true });
  const out = join(APP_DIR, "target/appsize.json");
  writeFileSync(out, JSON.stringify(report, null, 2));
  console.log(JSON.stringify(report, null, 2));
  console.log(`-> ${out}`);
}

await main();
