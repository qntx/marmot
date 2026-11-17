import { defineConfig } from "@playwright/test";
import type { PlaywrightTestConfig } from "@playwright/test";

const PORT = Number(process.env["E2E_PORT"] ?? "8710");

const config: PlaywrightTestConfig = defineConfig({
  testDir: "./e2e",
  testMatch: "*.spec.ts",
  timeout: 120_000,
  fullyParallel: false,
  workers: 1,
  retries: 0,
  reporter: "line",
  use: {
    baseURL: `http://127.0.0.1:${PORT}`,
    browserName: "chromium",
    headless: true,
  },
  webServer: {
    command: "bun e2e/server.ts",
    url: `http://127.0.0.1:${PORT}`,
    reuseExistingServer: process.env["CI"] === undefined,
    timeout: 15_000,
  },
});

export default config;
