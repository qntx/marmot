import { describe, expect, test } from "vite-plus/test";

import {
  fileFindings,
  nativeLogFindings,
  rustLogFindings,
  tsConsoleFindings,
  unsafeFindings,
} from "./check-source-rules.ts";

const MK_CORE = "crates/mk-core/src/session.rs";
const ABI = "crates/mk-ffi/src/abi.rs";
const PKG = "packages/marmot/src/client/x.ts";
const GENERATED = "packages/marmot-wasm/src/generated/glue.ts";
const CPP = "packages/marmot-native/cpp/Hybrid.cpp";

describe("unsafe rules", () => {
  test("unsafe outside abi.rs is flagged; a safe file is clean", () => {
    const bad = "fn f() {\n    unsafe { g() }\n}\n";
    expect(unsafeFindings(MK_CORE, bad)).toStrictEqual([
      { path: MK_CORE, line: 2, rule: "unsafe outside crates/mk-ffi/src/abi.rs" },
    ]);
    expect(unsafeFindings(MK_CORE, "fn f() {\n    g()\n}\n")).toStrictEqual([]);
  });

  test("comments and lint names are not unsafe forms", () => {
    const source =
      "#![forbid(unsafe_code)]\n// never write unsafe { here\n#![deny(unsafe_op_in_unsafe_fn)]\n";
    expect(unsafeFindings(MK_CORE, source)).toStrictEqual([]);
  });

  test("abi.rs requires a contiguous SAFETY comment", () => {
    const documented =
      "// SAFETY: the caller keeps `ptr` valid for `len` bytes.\nlet v = unsafe { read(ptr) };\n";
    expect(unsafeFindings(ABI, documented)).toStrictEqual([]);
    const bare = "fn f() {\n    unsafe { g() };\n}\n";
    expect(unsafeFindings(ABI, bare)).toStrictEqual([
      { path: ABI, line: 2, rule: "unsafe without preceding // SAFETY:" },
    ]);
  });
});

describe("rust logging rule", () => {
  test("printing in library source is flagged; test modules are exempt", () => {
    const bad = 'fn f() {\n    println!("x");\n}\n';
    expect(rustLogFindings(MK_CORE, bad)).toStrictEqual([
      { path: MK_CORE, line: 2, rule: "log/print in library source" },
    ]);
    const tested =
      "fn f() {}\n\n#[cfg(test)]\nmod tests {\n    fn t() {\n        dbg!(1);\n    }\n}\n";
    expect(rustLogFindings(MK_CORE, tested)).toStrictEqual([]);
  });

  test("other crates and testing.rs follow their own gates", () => {
    const bad = 'fn f() {\n    eprintln!("x");\n}\n';
    expect(rustLogFindings("crates/mk-bridge/src/bridge.rs", bad)).toHaveLength(1);
    expect(rustLogFindings("crates/mk-core/src/testing.rs", bad)).toStrictEqual([]);
    expect(rustLogFindings("crates/interop/src/main.rs", bad)).toStrictEqual([]);
  });
});

describe("console rule", () => {
  test("console in package source is flagged; generated glue is exempt", () => {
    const bad = "export const f = () => {\n    console.log('x');\n};\n";
    expect(tsConsoleFindings(PKG, bad)).toStrictEqual([
      { path: PKG, line: 2, rule: "console.* in package source" },
    ]);
    expect(tsConsoleFindings(GENERATED, bad)).toStrictEqual([]);
    expect(tsConsoleFindings(PKG, "// console.log('x');\n")).toStrictEqual([]);
  });
});

describe("native logging rule", () => {
  test("native log calls are flagged; bounded names and comments are not", () => {
    const bad = 'void f() {\n    printf("%d", x);\n}\n';
    expect(nativeLogFindings(CPP, bad)).toStrictEqual([
      { path: CPP, line: 2, rule: "native log call" },
    ]);
    const clean = 'void f() {\n    snprintf(buf, n, "%d", x); // printf is banned\n}\n';
    expect(nativeLogFindings(CPP, clean)).toStrictEqual([]);
  });
});

describe("fileFindings dispatch", () => {
  test("a file outside every gate is clean", () => {
    expect(fileFindings("README.md", "println! unsafe { console.log")).toStrictEqual([]);
  });
});
