import { expect, test } from "bun:test";

import { startCollector } from "./collector.ts";

// Regression: the app posts to `/<run>/journal` and `/<run>/result`. An earlier
// version parsed the path as `[kind, run]`, 404'd every record, and left the
// driver waiting for a result that never arrived.
test("records land under the run id, journal then result", async () => {
  const { collector, stop } = startCollector(18100);
  try {
    const journal = await fetch("http://127.0.0.1:18100/run-1/journal", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ kind: "batch", seq: "1", bytes: "AA==" }),
    });
    expect(journal.status).toBe(200);
    const result = await fetch("http://127.0.0.1:18100/run-1/result", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ mode: "flow", ok: true }),
    });
    expect(result.status).toBe(200);
    const other = await fetch("http://127.0.0.1:18100/run-2/result", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ mode: "verify", ok: true }),
    });
    expect(other.status).toBe(200);

    const run1 = collector.journal("run-1");
    expect(run1.batches).toStrictEqual([{ seq: "1", bytes: "AA==" }]);
    expect(run1.results).toStrictEqual([{ mode: "flow", ok: true }]);
    expect(collector.journal("run-2").results).toStrictEqual([{ mode: "verify", ok: true }]);
  } finally {
    stop();
  }
});

test("GET /<run> returns the run journal", async () => {
  const { stop } = startCollector(18101);
  try {
    await fetch("http://127.0.0.1:18101/run-1/journal", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ kind: "violation", text: "nope" }),
    });
    const response = await fetch("http://127.0.0.1:18101/run-1");
    expect(response.status).toBe(200);
    const journal = (await response.json()) as { violations: string[] };
    expect(journal.violations).toStrictEqual(["nope"]);
  } finally {
    stop();
  }
});
