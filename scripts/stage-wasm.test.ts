import { describe, expect, test } from "vite-plus/test";

import { unpairedNames } from "./stage-wasm.ts";

const GLUE = `
function open() {
  return wasm.engine_open();
}
const close = () => wasm.engine_close();
function __wbg_log_aaa111(arg0) {
  console.log(arg0);
}
`;

describe("unpairedNames", () => {
  test("returns nothing when glue and module are from one build", () => {
    expect(
      unpairedNames(
        GLUE,
        new Set(["memory", "engine_open", "engine_close"]),
        new Set(["__wbg_log_aaa111"]),
      ),
    ).toStrictEqual([]);
  });

  test("lists a wasm.<name> reference the module does not export", () => {
    expect(
      unpairedNames(GLUE, new Set(["memory", "engine_open"]), new Set(["__wbg_log_aaa111"])),
    ).toStrictEqual(["engine_close"]);
  });

  test("lists a module import the glue never defines", () => {
    expect(
      unpairedNames(
        GLUE,
        new Set(["memory", "engine_open", "engine_close"]),
        new Set(["__wbg_log_aaa111", "__wbg_missing_bbb222"]),
      ),
    ).toStrictEqual(["__wbg_missing_bbb222"]);
  });

  test("does not match wasm inside a longer identifier like marmot_wasm.js", () => {
    const glue = `// src/generated/marmot_wasm.js\nconst f = () => wasm.ok();\n`;
    expect(unpairedNames(glue, new Set(["ok"]), new Set<string>())).toStrictEqual([]);
  });
});
