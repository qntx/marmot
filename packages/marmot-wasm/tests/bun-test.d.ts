/** The `bun:test` surface the Bun suite uses — the repo does not depend on @types/bun. */
declare module "bun:test" {
  export function describe(name: string, body: () => void): void;
  export function test(name: string, body: () => void | Promise<void>, timeout?: number): void;
  export function expect(actual: unknown): {
    toThrow: (message?: string | RegExp) => void;
    toBeGreaterThan: (expected: number) => void;
    toBeGreaterThanOrEqual: (expected: number) => void;
    toBeLessThan: (expected: number) => void;
    toStrictEqual: (expected: unknown) => void;
  };
}
