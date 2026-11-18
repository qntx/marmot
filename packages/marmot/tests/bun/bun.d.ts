/* The `bun:test` and `bun:sqlite` surfaces the Bun suite uses, declared locally because the
 * repo does not take a dependency on Bun's published typings package. */

declare module "bun:test" {
  export function describe(name: string, body: () => void): void;
  export function test(name: string, body: () => void | Promise<void>): void;
  export function expect(actual: unknown): {
    toStrictEqual: (expected: unknown) => void;
    toBe: (expected: unknown) => void;
    toBeDefined: () => void;
    toBeUndefined: () => void;
    toHaveLength: (expected: number) => void;
  };
}

declare module "bun:sqlite" {
  export class Database {
    constructor(path: string);
    query(sql: string): { all: (...params: unknown[]) => unknown[] };
    run(sql: string, ...params: unknown[]): void;
    exec(sql: string): void;
  }
}
