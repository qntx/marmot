/**
 * `file:` deps are *copies* — Expo autolinking and Metro both need real directories under
 * node_modules (glob-based discovery does not follow symlinks). Re-sync the two local packages
 * into their copies so builds always see the current dist/native artifacts.
 */

import { cpSync, rmSync } from "node:fs";
import { join } from "node:path";

const APP_DIR = new URL("..", import.meta.url).pathname;

export function syncLocalPackages(): void {
  for (const name of ["marmot", "marmot-native"]) {
    const src = join(APP_DIR, "../../packages", name);
    const dst = join(APP_DIR, "node_modules/@qntx", name);
    rmSync(dst, { recursive: true, force: true });
    cpSync(src, dst, {
      recursive: true,
      filter: (source) => !/\/(node_modules|target|\.expo)(\/|$)/.test(source),
    });
    console.log(`synced @qntx/${name} -> node_modules`);
  }
}

if (import.meta.main) {
  syncLocalPackages();
}
