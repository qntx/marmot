/**
 * `manualTimer` — a deterministic `Timer` for tests: `set` schedules a task `ms` from `now`,
 * `advance` runs every task due inside the window in time order (insertion order on ties), stepping
 * the linked clock to each due time and yielding a macrotask after each callback so async work it
 * started progresses. `tick`/`next`/`pending` introspect and drive the schedule without the
 * event-loop yield. `countingTimer` — a real-`setTimeout` `Timer` with a pending count, for e2e
 * timer-leak checks.
 */

import type { Timer } from "../transport/port.ts";
import type { ManualClock } from "./clock.ts";

export type ManualTimer = Timer & {
  /** Advance time by `ms`, firing every due callback in order; resolves when the window closes. */
  advance: (ms?: number) => Promise<void>;
  /** Advance time by `ms` and run every task due, in schedule order (synchronous; no yield). */
  tick: (ms?: number) => void;
  /** Number of scheduled-but-not-run tasks. */
  pending: () => number;
  /** Milliseconds until the earliest task; `undefined` when none is scheduled. */
  next: () => number | undefined;
};

type Task = { at: number; order: number; run: () => void };

export function manualTimer(clock?: ManualClock): ManualTimer {
  let local = 0;
  let order = 0;
  const tasks: Task[] = [];

  const now = (): number => (clock === undefined ? local : Number(clock.now().monotonicMs));

  const stepTo = (ms: number): void => {
    if (clock === undefined) {
      local = ms;
    } else {
      clock.advance(ms - now());
    }
  };

  const earliest = (): Task | undefined => {
    let first: Task | undefined;
    for (const task of tasks) {
      if (
        first === undefined ||
        task.at < first.at ||
        (task.at === first.at && task.order < first.order)
      ) {
        first = task;
      }
    }
    return first;
  };

  const drop = (task: Task): void => {
    const index = tasks.indexOf(task);
    if (index !== -1) {
      tasks.splice(index, 1);
    }
  };

  const due = (): Task[] => {
    const at = now();
    const ready = tasks.filter((task) => task.at <= at);
    for (const task of ready) {
      drop(task);
    }
    ready.sort((a, b) => a.at - b.at || a.order - b.order);
    return ready;
  };

  return {
    set(ms, task) {
      const entry: Task = { at: now() + ms, order: order++, run: task };
      tasks.push(entry);
      return () => {
        drop(entry);
      };
    },

    async advance(ms = 0) {
      const end = now() + ms;
      for (;;) {
        const task = earliest();
        if (task === undefined || task.at > end) {
          break;
        }
        drop(task);
        stepTo(task.at);
        task.run();
        // Let the event loop turn so async work the callback started can schedule inside the window.
        // oxlint-disable-next-line no-await-in-loop -- due callbacks run one turn at a time
        await new Promise<void>((resolve) => {
          globalThis.setTimeout(resolve);
        });
      }
      stepTo(end);
    },

    tick(ms = 0) {
      stepTo(now() + ms);
      for (const task of due()) {
        task.run();
      }
    },

    pending() {
      return tasks.length;
    },

    next() {
      const task = earliest();
      return task === undefined ? undefined : task.at - now();
    },
  };
}

/** A `Timer` on real `setTimeout` with a pending count — for runtime e2e leak checks. */
export function countingTimer(): Timer & { pending: () => number } {
  const live = new Set<ReturnType<typeof setTimeout>>();
  return {
    set(ms, run) {
      const id = globalThis.setTimeout(() => {
        live.delete(id);
        run();
      }, ms);
      live.add(id);
      return () => {
        if (live.delete(id)) {
          globalThis.clearTimeout(id);
        }
      };
    },
    pending() {
      return live.size;
    },
  };
}
