/** The `Bun` surface the e2e tooling uses — the repo does not depend on @types/bun. */
declare const Bun: {
  build: (options: {
    entrypoints: string[];
    outdir: string;
    naming?: string;
    target?: string;
    format?: string;
    minify?: boolean;
    sourcemap?: string;
  }) => Promise<{ success: boolean; outputs: Array<{ path: string }>; logs: unknown[] }>;
  serve: (options: { port: number; fetch: (request: Request) => Response }) => unknown;
  file: (path: string) => Blob;
};
