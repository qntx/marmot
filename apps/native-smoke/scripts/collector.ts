/**
 * The out-of-process journal the app POSTs to (cleartext localhost; Android reaches it through
 * `adb reverse`). Records are scoped by run id in the URL path — `/journal/<run>` and
 * `/result/<run>` — so late in-flight posts from a killed process can never bleed into the next
 * iteration. Importable (`startCollector`) or standalone (`bun scripts/collector.ts`, logs).
 */

export type BatchRecord = { seq: string; bytes: string };
export type PublishRecord = { id: string; eventId: string; watermark: string };
export type DumpRecord = { key: string; value: string }[];
export type ResultRecord = Record<string, unknown> & { mode?: string; ok?: boolean };

export type RunJournal = {
  batches: BatchRecord[];
  acked: string[];
  published: PublishRecord[];
  dumps: DumpRecord[];
  results: ResultRecord[];
  ops: Record<string, unknown>[];
  violations: string[];
  milestones: Record<string, unknown>[];
};

function freshJournal(): RunJournal {
  return {
    batches: [], acked: [], published: [], dumps: [], results: [], ops: [], violations: [],
    milestones: [],
  };
}

export class Collector {
  private readonly runs = new Map<string, RunJournal>();
  private readonly listeners = new Map<string, Set<() => void>>();

  journal(run: string): RunJournal {
    let journal = this.runs.get(run);
    if (journal === undefined) {
      journal = freshJournal();
      this.runs.set(run, journal);
    }
    return journal;
  }

  record(run: string, kind: "journal" | "result", body: unknown): void {
    const journal = this.journal(run);
    const record = body as Record<string, unknown>;
    if (kind === "result") {
      journal.results.push(record as ResultRecord);
    } else if (record["kind"] === "batch") {
      journal.batches.push({ seq: String(record["seq"]), bytes: String(record["bytes"]) });
    } else if (record["kind"] === "ack") {
      journal.acked.push(String(record["seq"]));
    } else if (record["kind"] === "publish") {
      journal.published.push({
        id: String(record["id"]),
        eventId: String(record["eventId"]),
        watermark: String(record["watermark"]),
      });
    } else if (record["kind"] === "dump") {
      journal.dumps.push(record["rows"] as DumpRecord);
    } else if (record["kind"] === "backend") {
      journal.ops.push(record);
    } else if (record["kind"] === "violation") {
      journal.violations.push(String(record["text"]));
    } else if (record["kind"] === "milestone") {
      journal.milestones.push(record);
    }
    for (const wake of this.listeners.get(run) ?? []) {
      wake();
    }
  }

  /** Resolve once `pick` returns a value, or throw after `timeoutMs`. */
  async waitFor<T>(
    run: string,
    pick: (journal: RunJournal) => T | undefined,
    timeoutMs: number,
    label: string,
  ): Promise<T> {
    const deadline = Date.now() + timeoutMs;
    for (;;) {
      const hit = pick(this.journal(run));
      if (hit !== undefined) {
        return hit;
      }
      const left = deadline - Date.now();
      if (left <= 0) {
        throw new Error(`timeout waiting for ${label} (run ${run})`);
      }
      await new Promise<void>((resolve) => {
        let set = this.listeners.get(run);
        if (set === undefined) {
          set = new Set();
          this.listeners.set(run, set);
        }
        const timer = setTimeout(resolve, Math.min(left, 250));
        const wake = (): void => {
          clearTimeout(timer);
          set.delete(wake);
          resolve();
        };
        set.add(wake);
      });
    }
  }
}

export function startCollector(port = 8100): { collector: Collector; stop: () => void } {
  const collector = new Collector();
  const server = Bun.serve({
    hostname: "127.0.0.1",
    port,
    async fetch(request) {
      const url = new URL(request.url);
      const parts = url.pathname.split("/").filter((part) => part.length > 0);
      if (request.method === "GET" && parts.length === 1) {
        return Response.json(collector.journal(parts[0] ?? ""));
      }
      if (request.method === "POST" && parts.length === 2) {
        const [run, kind] = parts as [string, "journal" | "result"];
        if (kind === "journal" || kind === "result") {
          collector.record(run, kind, await request.json());
          if (import.meta.main) {
            console.log(`collector ${kind} ${run}`);
          }
          return new Response("ok");
        }
      }
      return new Response("not found", { status: 404 });
    },
  });
  return { collector, stop: () => server.stop(true) };
}

if (import.meta.main) {
  const { stop } = startCollector();
  console.log("collector listening on 127.0.0.1:8100");
  process.on("SIGINT", () => {
    stop();
    process.exit(0);
  });
}
