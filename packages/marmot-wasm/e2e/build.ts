/** Bundle the browser harness and the packed wasm loader into `e2e/dist/` — run before Playwright. */

async function bundle(entrypoint: string, name: string): Promise<string> {
  const result = await Bun.build({
    entrypoints: [new URL(entrypoint, import.meta.url).pathname],
    outdir: new URL("dist", import.meta.url).pathname,
    naming: name,
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
    throw new Error(`harness build failed for ${entrypoint}`);
  }
  const [output] = result.outputs;
  if (output === undefined) {
    throw new Error(`harness build produced no output for ${entrypoint}`);
  }
  return output.path;
}

// The harness imports the loader at runtime from /marmot.mjs: the loader must be the packed
// dist/index.mjs (its glue is a build-matched pair with the shipped .wasm), rebundled so its
// bare @qntx/marmot imports resolve for the browser.
const harness = await bundle("harness/index.ts", "harness.js");
const clientHarness = await bundle("harness/client.ts", "client-harness.js");
const module_ = await bundle("../dist/index.mjs", "marmot.mjs");

// oxlint-disable-next-line no-console
console.log(`harness: ${harness}`);
// oxlint-disable-next-line no-console
console.log(`client harness: ${clientHarness}`);
// oxlint-disable-next-line no-console
console.log(`loader: ${module_}`);
