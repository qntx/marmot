#!/usr/bin/env bun
/// <reference types="node" />
import { spawnSync } from "node:child_process";
import { createHash } from "node:crypto";
import {
  appendFileSync,
  copyFileSync,
  existsSync,
  mkdirSync,
  readFileSync,
  rmSync,
  statSync,
  writeFileSync,
} from "node:fs";
import { homedir, platform } from "node:os";
import { dirname, join, resolve } from "node:path";

// Builds the prebuilt `marmot_ffi` libraries that @qntx/marmot-native ships:
// an iOS `MarmotFfi.xcframework` and per-ABI Android `libmarmot_ffi.so`
// jniLibs, plus a copy of `marmot_ffi.h`. All outputs are gitignored build
// products consumed by the podspec and CMake.
//
// Each artifact is RELINKED from the mk-ffi staticlib: rustc links a cdylib
// with its own export list (every `#[no_mangle]` item in the dep graph, which
// leaks secp256k1-sys symbols) and both ld64 and GNU ld union multiple export
// lists — rustc's can never be subtracted from. Linking the static archive
// with -force_load/--whole-archive plus our own export list is the only way
// the dynamic table equals exactly the mk_* ABI.
//
//   bun run build:native            # iOS + Android
//   bun run build:native --ios
//   bun run build:native --android
//   bun run build:native --host     # host cdylib only (the CI export gate)
//
// Every artifact builds with `--profile native` (panic=unwind for the FFI
// boundary's catch_unwind, fat LTO) — never `--release`, which is panic=abort.
// The NDK is located via $ANDROID_NDK_HOME, then $ANDROID_HOME/ndk/<pinned>,
// then ~/Library/Android/sdk/ndk/<pinned>; no cargo-ndk — the NDK's own clang
// is wired per target through CARGO_TARGET_*_LINKER.

const REPO = resolve(import.meta.dirname, "..");
const NATIVE = join(REPO, "packages/marmot-native");
const HEADER_SRC = join(REPO, "crates/mk-ffi/include/marmot_ffi.h");
const HEADER_DST = join(NATIVE, "include/marmot_ffi.h");

// Matches packages/marmot-native/android/gradle.properties.
const NDK_VERSION = "27.1.12297006";
const ANDROID_API = 24;
// React Native's minimum deployment target (min_ios_version_supported, RN 0.88).
const IOS_DEPLOYMENT_TARGET = "15.1";
const INSTALL_NAME = "@rpath/MarmotFfi.framework/MarmotFfi";

const ANDROID_TARGETS = [
  ["aarch64-linux-android", "arm64-v8a", "aarch64-linux-android"],
  ["x86_64-linux-android", "x86_64", "x86_64-linux-android"],
] as const;

const IOS_TARGETS = ["aarch64-apple-ios", "aarch64-apple-ios-sim"] as const;

// The whole mk-ffi surface the Nitro C++ links; the exact-set gate also proves
// `mk_bridge_test_panic` never ships.
const EXPECTED_EXPORTS = new Set([
  "mk_bridge_new",
  "mk_bridge_open",
  "mk_bridge_call",
  "mk_bridge_drain",
  "mk_bridge_close",
  "mk_bridge_free",
  "mk_bytes_free",
]);

// Dynamic symbols a stripped shared library legitimately carries besides the
// ABI: ELF section-boundary markers emitted by the linker.
const PLATFORM_SYMBOLS = new Set([
  "__bss_start",
  "__end__",
  "_bss_end__",
  "__executable_start",
  "edata",
  "end",
]);

function fail(message: string): never {
  console.error(`build-native: ${message}`);
  process.exit(1);
}

function sh(cmd: string, args: ReadonlyArray<string>, env?: Record<string, string>): string {
  const result = spawnSync(cmd, [...args], {
    encoding: "utf8",
    env: { ...process.env, ...env },
  });
  if (result.status !== 0) {
    process.stderr.write(result.stderr ?? "");
    fail(`${cmd} ${args.join(" ")} exited ${result.status ?? "?"}\n${result.stderr ?? ""}`);
  }
  return result.stdout ?? "";
}

function sha256(path: string): string {
  return createHash("sha256").update(readFileSync(path)).digest("hex");
}

// Measurement lines go to the job summary on CI so release runs keep the
// artifact sizes and hashes next to the build logs.
function report(line: string): void {
  console.log(line);
  const summary = process.env["GITHUB_STEP_SUMMARY"];
  if (summary !== undefined) {
    appendFileSync(summary, `${line}\n`);
  }
}

/** Parses `nm`-style output into a flat symbol-name list. */
function listExports(tool: string, args: ReadonlyArray<string>, binary: string): string[] {
  return sh(tool, [...args, binary])
    .split("\n")
    .map((line) => line.trim())
    .filter((line) => line.length > 0)
    .map((line) => line.split(/\s+/).at(-1) ?? "")
    .filter((name) => name.length > 0);
}

/** The exported dynamic symbols must be exactly the mk_* ABI (Mach-O prefixes symbols with `_`). */
function checkExports(names: string[], label: string): void {
  const normalized = new Set(
    names
      .map((name) => (name.startsWith("_") && !name.startsWith("__") ? name.slice(1) : name))
      .filter((name) => !PLATFORM_SYMBOLS.has(name)),
  );
  const unexpected = [...normalized].filter((name) => !EXPECTED_EXPORTS.has(name));
  if (unexpected.length > 0) {
    fail(`${label}: unexpected exported symbols: ${unexpected.join(", ")}`);
  }
  const missing = [...EXPECTED_EXPORTS].filter((name) => !normalized.has(name));
  if (missing.length > 0) {
    fail(`${label}: missing exported symbols: ${missing.join(", ")}`);
  }
}

function copyHeader(): void {
  // include/ is gitignored (it only holds this copy) — recreate it in clean checkouts.
  mkdirSync(dirname(HEADER_DST), { recursive: true });
  copyFileSync(HEADER_SRC, HEADER_DST);
  report(`header: packages/marmot-native/include/marmot_ffi.h sha256=${sha256(HEADER_DST)}`);
}

// The cdylib would otherwise also export every C symbol its deps carry
// (secp256k1-sys): restrict the dynamic table to the mk_* ABI, matching the
// exact-set gate below. Mach-O takes -exported_symbols_list, ELF a
// --version-script; the file is generated per build so it can never drift
// from EXPECTED_EXPORTS.
function exportsFile(format: "macho" | "elf"): string {
  const dir = join(REPO, "target", "marmot-native");
  mkdirSync(dir, { recursive: true });
  if (format === "macho") {
    const path = join(dir, "exports.txt");
    writeFileSync(path, `${[...EXPECTED_EXPORTS].map((name) => `_${name}`).join("\n")}\n`);
    return path;
  }
  const path = join(dir, "exports.map");
  const globals = [...EXPECTED_EXPORTS].map((name) => `      ${name};`).join("\n");
  writeFileSync(path, `{\n  global:\n${globals}\n  local:\n      *;\n};\n`);
  return path;
}

const SCRATCH = join(REPO, "target", "marmot-native");

/**
 * Compiles the mk-ffi staticlib only (`--crate-type staticlib` skips the cdylib link, so no target
 * C linker is needed for the cargo step — the shared object is produced by the platform link below,
 * under our control).
 */
function cargoStaticlib(target: string | undefined, env?: Record<string, string>): string {
  const args = ["rustc", "--profile", "native", "-p", "mk-ffi", "--crate-type", "staticlib"];
  if (target !== undefined) {
    args.push("--target", target);
  }
  sh("cargo", args, env);
  const dir =
    target === undefined ? join(REPO, "target", "native") : join(REPO, "target", target, "native");
  const lib = join(dir, "libmarmot_ffi.a");
  if (!existsSync(lib)) {
    fail(`missing ${lib}`);
  }
  return lib;
}

// --- iOS -------------------------------------------------------------------

function frameworkPlist(platforms: string[]): string {
  const platformEntries = platforms.map((name) => `    <string>${name}</string>`).join("\n");
  return `<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>CFBundleDevelopmentRegion</key>
    <string>en</string>
    <key>CFBundleExecutable</key>
    <string>MarmotFfi</string>
    <key>CFBundleIdentifier</key>
    <string>org.qntx.marmot</string>
    <key>CFBundleInfoDictionaryVersion</key>
    <string>6.0</string>
    <key>CFBundleName</key>
    <string>MarmotFfi</string>
    <key>CFBundlePackageType</key>
    <string>FMWK</string>
    <key>CFBundleShortVersionString</key>
    <string>0.0.0</string>
    <key>CFBundleVersion</key>
    <string>1</string>
    <key>CFBundleSupportedPlatforms</key>
    <array>
${platformEntries}
    </array>
    <key>MinimumOSVersion</key>
    <string>${IOS_DEPLOYMENT_TARGET}</string>
</dict>
</plist>
`;
}

function writeFramework(dir: string, binary: string, platforms: string[]): string {
  const framework = join(dir, "MarmotFfi.framework");
  mkdirSync(join(framework, "Headers"), { recursive: true });
  mkdirSync(join(framework, "Modules"), { recursive: true });
  copyFileSync(binary, join(framework, "MarmotFfi"));
  copyFileSync(HEADER_SRC, join(framework, "Headers/marmot_ffi.h"));
  writeFileSync(
    join(framework, "Modules/module.modulemap"),
    'framework module MarmotFfi {\n  header "marmot_ffi.h"\n  export *\n}\n',
  );
  writeFileSync(join(framework, "Info.plist"), frameworkPlist(platforms));
  return framework;
}

const IOS_LINK: Record<string, { sdk: string; clangTarget: string }> = {
  "aarch64-apple-ios": {
    sdk: "iphoneos",
    clangTarget: `arm64-apple-ios${IOS_DEPLOYMENT_TARGET}`,
  },
  "aarch64-apple-ios-sim": {
    sdk: "iphonesimulator",
    clangTarget: `arm64-apple-ios${IOS_DEPLOYMENT_TARGET}-simulator`,
  },
};

function buildIos(): void {
  if (platform() !== "darwin") {
    fail("--ios requires macOS (xcodebuild)");
  }
  const env = { IPHONEOS_DEPLOYMENT_TARGET: IOS_DEPLOYMENT_TARGET };
  const exports = exportsFile("macho");

  const dylibs = new Map<string, string>();
  for (const target of IOS_TARGETS) {
    const staticlib = cargoStaticlib(target, env);
    const link = IOS_LINK[target];
    if (link === undefined) {
      fail(`no link config for ${target}`);
    }
    const out = join(SCRATCH, target, "libmarmot_ffi.dylib");
    mkdirSync(dirname(out), { recursive: true });
    sh("xcrun", [
      "-sdk",
      link.sdk,
      "clang",
      "-target",
      link.clangTarget,
      "-dynamiclib",
      "-o",
      out,
      `-Wl,-force_load,${staticlib}`,
      `-Wl,-exported_symbols_list,${exports}`,
      `-Wl,-install_name,${INSTALL_NAME}`,
      "-Wl,-x,-dead_strip",
      "-liconv",
      "-lSystem",
    ]);
    sh("strip", ["-Sx", out]);
    dylibs.set(target, out);
  }

  const staging = join(NATIVE, "ios/staging");
  rmSync(staging, { recursive: true, force: true });
  mkdirSync(staging, { recursive: true });

  // arm64 device + arm64 simulator only — one slice each, no lipo needed.
  const deviceFramework = writeFramework(
    join(staging, "ios-arm64"),
    dylibs.get("aarch64-apple-ios") ?? "",
    ["iPhoneOS"],
  );
  const simFramework = writeFramework(
    join(staging, "ios-arm64-simulator"),
    dylibs.get("aarch64-apple-ios-sim") ?? "",
    ["iPhoneSimulator"],
  );

  const output = join(NATIVE, "ios/MarmotFfi.xcframework");
  rmSync(output, { recursive: true, force: true });
  mkdirSync(dirname(output), { recursive: true });
  sh("xcodebuild", [
    "-create-xcframework",
    "-framework",
    deviceFramework,
    "-framework",
    simFramework,
    "-output",
    output,
  ]);

  const installName = (
    sh("otool", ["-D", join(deviceFramework, "MarmotFfi")])
      .trim()
      .split("\n")
      .at(-1) ?? ""
  ).trim();
  if (installName !== INSTALL_NAME) {
    fail(`install_name is ${installName}, expected ${INSTALL_NAME}`);
  }

  for (const [label, binary] of [
    ["ios-arm64", join(deviceFramework, "MarmotFfi")],
    ["ios-simulator", join(simFramework, "MarmotFfi")],
  ] as const) {
    const exports = listExports("nm", ["-gU"], binary);
    checkExports(exports, label);
    report(
      `ios ${label}: ${statSync(binary).size} B sha256=${sha256(binary)} exports=${exports.length}`,
    );
  }
  rmSync(staging, { recursive: true, force: true });
  report(`xcframework: packages/marmot-native/ios/MarmotFfi.xcframework`);
}

// --- Android -----------------------------------------------------------------

function ndkHome(): string {
  const candidates = [
    process.env["ANDROID_NDK_HOME"],
    process.env["ANDROID_NDK_ROOT"],
    process.env["ANDROID_HOME"] === undefined
      ? undefined
      : join(process.env["ANDROID_HOME"], "ndk", NDK_VERSION),
    join(homedir(), "Library/Android/sdk/ndk", NDK_VERSION),
  ];
  for (const candidate of candidates) {
    if (candidate !== undefined && existsSync(join(candidate, "toolchains"))) {
      return candidate;
    }
  }
  return fail(`Android NDK ${NDK_VERSION} not found (set ANDROID_NDK_HOME)`);
}

function ndkHostTag(): string {
  switch (platform()) {
    case "darwin":
      return "darwin-x86_64";
    case "linux":
      return "linux-x86_64";
    case "win32":
      return "windows-x86_64";
    default:
      return fail(`unsupported host platform ${platform()}`);
  }
}

/** Every LOAD segment must be 16 KB aligned (Android 15 page-size requirement). */
function checkPageSize(readelf: string, dest: string, label: string): void {
  const misaligned = sh(readelf, ["-lW", dest])
    .split("\n")
    .map((line) => line.trim())
    .filter((line) => line.startsWith("LOAD"))
    .map((line) => line.split(/\s+/).at(-1) ?? "")
    .filter((align) => Number.parseInt(align, 16) < 0x4000);
  if (misaligned.length > 0) {
    fail(`android ${label}: LOAD segments below 16 KB alignment: ${misaligned.join(", ")}`);
  }
}

function buildAndroid(): void {
  const ndk = ndkHome();
  const bin = join(ndk, "toolchains/llvm/prebuilt", ndkHostTag(), "bin");
  const llvmNm = join(bin, "llvm-nm");
  const llvmStrip = join(bin, "llvm-strip");
  const llvmReadelf = join(bin, "llvm-readelf");
  if (!existsSync(llvmNm)) {
    fail(`no llvm-nm at ${bin} — wrong NDK layout?`);
  }

  const jniLibs = join(NATIVE, "android/src/main/jniLibs");
  for (const [target, abi, clangTriple] of ANDROID_TARGETS) {
    const linker = join(bin, `${clangTriple}${ANDROID_API}-clang`);
    if (!existsSync(linker)) {
      fail(`missing linker ${linker}`);
    }
    // secp256k1-sys compiles C for the target: the NDK clang arrives through
    // CC_<triple>/AR_<triple>. The final link is ours: --version-script keeps
    // only the mk_* exports, and the soname lands in DT_SONAME so consumers
    // linking the prebuilt record "libmarmot_ffi.so" in DT_NEEDED — without
    // it they record the build path and the loader fails at runtime.
    const envKey = target.replaceAll("-", "_");
    const staticlib = cargoStaticlib(target, {
      [`CC_${envKey}`]: linker,
      [`AR_${envKey}`]: join(bin, "llvm-ar"),
    });
    const destDir = join(jniLibs, abi);
    mkdirSync(destDir, { recursive: true });
    const dest = join(destDir, "libmarmot_ffi.so");
    sh(linker, [
      "-shared",
      "-o",
      dest,
      `-Wl,--whole-archive,${staticlib},--no-whole-archive`,
      `-Wl,--version-script,${exportsFile("elf")}`,
      "-Wl,-z,max-page-size=16384",
      "-Wl,-soname,libmarmot_ffi.so",
      "-Wl,-z,noexecstack",
      "-ldl",
      "-lm",
      "-llog",
    ]);
    sh(llvmStrip, ["--strip-unneeded", dest]);
    const sonames = sh(llvmReadelf, ["-d", dest]);
    if (!sonames.includes("Library soname: [libmarmot_ffi.so]")) {
      fail(`android ${abi}: missing SONAME in ${dest}:\n${sonames}`);
    }
    checkPageSize(llvmReadelf, dest, abi);
    const exports = listExports(llvmNm, ["-D", "--defined-only"], dest);
    checkExports(exports, `android ${abi}`);
    report(
      `android ${abi}: ${statSync(dest).size} B sha256=${sha256(dest)} exports=${exports.length}`,
    );
  }
}

// --- Host --------------------------------------------------------------------

function buildHost(): void {
  const isMac = platform() === "darwin";
  if (!isMac && platform() !== "linux") {
    fail(`--host is only implemented for macOS and Linux, not ${platform()}`);
  }
  const staticlib = cargoStaticlib(undefined);
  const built = join(REPO, "target", "native", isMac ? "libmarmot_ffi.dylib" : "libmarmot_ffi.so");
  if (isMac) {
    sh("cc", [
      "-dynamiclib",
      "-o",
      built,
      `-Wl,-force_load,${staticlib}`,
      `-Wl,-exported_symbols_list,${exportsFile("macho")}`,
      "-Wl,-x,-dead_strip",
      "-liconv",
      "-lSystem",
    ]);
  } else {
    sh("cc", [
      "-shared",
      "-o",
      built,
      `-Wl,--whole-archive,${staticlib},--no-whole-archive`,
      `-Wl,--version-script,${exportsFile("elf")}`,
      "-Wl,-z,noexecstack",
      "-lpthread",
      "-ldl",
      "-lm",
    ]);
  }
  if (isMac) {
    sh("strip", ["-Sx", built]);
  } else {
    sh("strip", ["--strip-unneeded", built]);
  }
  const exports = isMac
    ? listExports("nm", ["-gU"], built)
    : listExports("nm", ["-D", "--defined-only"], built);
  checkExports(exports, "host");
  report(`host: ${statSync(built).size} B sha256=${sha256(built)} exports=${exports.length}`);
}

const flags = new Set(process.argv.slice(2));
const unknown = [...flags].filter(
  (flag) => flag !== "--ios" && flag !== "--android" && flag !== "--host",
);
if (unknown.length > 0) {
  fail(`unknown flags: ${unknown.join(" ")} (usage: build-native.ts [--ios] [--android] [--host])`);
}

copyHeader();
if (flags.size === 0 || flags.has("--ios")) {
  buildIos();
}
if (flags.size === 0 || flags.has("--android")) {
  buildAndroid();
}
if (flags.has("--host")) {
  buildHost();
}
