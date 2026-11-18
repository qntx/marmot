/**
 * @qntx/marmot — public facade.
 *
 * Prefer the `@qntx/marmot/backend` / `/storage` / `/core` subpaths for
 * tree-shaking; `testing` is only exported as a subpath.
 */

export * from "./core/index.ts";
export * from "./backend/index.ts";
export * from "./storage/index.ts";
