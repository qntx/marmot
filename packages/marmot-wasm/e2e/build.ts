/** Bundle the browser harness into `e2e/dist/harness.js` — run before Playwright. */

const result = await Bun.build({
  entrypoints: [new URL("harness/index.ts", import.meta.url).pathname],
  outdir: new URL("dist", import.meta.url).pathname,
  naming: "harness.js",
  target: "browser",
  format: "esm",
  minify: false,
  sourcemap: "linked",
});

if (!result.success) {
  for (const log of result.logs) {
    // oxlint-disable-next-line no-console
    console.error(log);
  }
  throw new Error("harness build failed");
}
const [output] = result.outputs;
if (output === undefined) {
  throw new Error("harness build produced no output");
}
// oxlint-disable-next-line no-console
console.log(`harness: ${output.path}`);
