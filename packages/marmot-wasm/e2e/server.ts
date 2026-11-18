/** The Playwright webServer: serves the harness bundle, the release wasm, and a shell page. */

const PORT = Number(process.env["E2E_PORT"] ?? "8710");
const root = new URL(".", import.meta.url).pathname;

/** Cross-origin isolation unlocks `performance.measureUserAgentSpecificMemory`. */
const ISOLATION = {
  "cross-origin-opener-policy": "same-origin",
  "cross-origin-embedder-policy": "require-corp",
} as const;

const PAGE = `<!doctype html>
<html><head><meta charset="utf-8"><title>marmot e2e</title></head>
<body><script type="module" src="/harness.js"></script></body></html>`;

const CLIENT_PAGE = `<!doctype html>
<html><head><meta charset="utf-8"><title>marmot client e2e</title></head>
<body><script type="module" src="/client-harness.js"></script></body></html>`;

const paths: Record<string, { file: string; type: string }> = {
  "/harness.js": { file: `${root}dist/harness.js`, type: "text/javascript" },
  "/client-harness.js": { file: `${root}dist/client-harness.js`, type: "text/javascript" },
  "/client-harness.js.map": { file: `${root}dist/client-harness.js.map`, type: "application/json" },
  "/harness.js.map": { file: `${root}dist/harness.js.map`, type: "application/json" },
  "/marmot.mjs": { file: `${root}dist/marmot.mjs`, type: "text/javascript" },
  "/marmot_wasm_bg.wasm": {
    file: `${root}../dist/marmot_wasm_bg.wasm`,
    type: "application/wasm",
  },
};

Bun.serve({
  port: PORT,
  fetch(request) {
    const url = new URL(request.url);
    if (url.pathname === "/") {
      return new Response(PAGE, {
        headers: { "content-type": "text/html", ...ISOLATION },
      });
    }
    if (url.pathname === "/client") {
      return new Response(CLIENT_PAGE, {
        headers: { "content-type": "text/html", ...ISOLATION },
      });
    }
    const route = paths[url.pathname];
    if (route !== undefined) {
      const file = Bun.file(route.file);
      return new Response(file, {
        headers: { "content-type": route.type, ...ISOLATION },
      });
    }
    return new Response("not found", { status: 404 });
  },
});
// oxlint-disable-next-line no-console
console.log(`e2e server on http://127.0.0.1:${PORT}`);
