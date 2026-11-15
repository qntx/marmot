import { describe, expect, test } from "vite-plus/test";

import { DISPOSITIONS, INPUT_CATEGORIES, NAMED_OUTCOMES } from "../src/core/errors.ts";
import { bulletNames, specDoc, tableRows } from "./spec-docs.ts";

const ERRORS = specDoc("errors.md");

describe("error vocabulary mirrors the recorded spec", () => {
  test("input categories", () => {
    const spec = bulletNames(ERRORS, "Input categories");
    expect([...INPUT_CATEGORIES].toSorted()).toStrictEqual(spec.toSorted());
  });

  test("dispositions", () => {
    const spec = bulletNames(ERRORS, "Dispositions");
    expect([...DISPOSITIONS].toSorted()).toStrictEqual(spec.toSorted());
  });

  test("named outcomes map to one disposition and one category", () => {
    const spec = tableRows(ERRORS, "Named convergence outcomes").map(
      ([outcome, disposition, category]) => `${outcome}|${disposition}|${category}`,
    );
    const code = Object.entries(NAMED_OUTCOMES).map(
      ([outcome, entry]) => `${outcome}|${entry.disposition}|${entry.category}`,
    );
    expect(code.toSorted()).toStrictEqual(spec.toSorted());
  });
});
