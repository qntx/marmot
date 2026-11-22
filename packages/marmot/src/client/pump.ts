/**
 * `Pump` — the one serialized loop that owns the `Engine`: every op runs through `run`, which
 * drains and dispatches session events, reconciles subscriptions when flagged, and publishes
 * whatever `takeOutbound` released after `whenDurable`. Inbound events are buffered per event id
 * with the cursor keys that delivered them, ingested in bounded batches, and cursors advance only
 * after `whenDurable`: an event delivered after its subscription's EOSE counts immediately; an
 * event from the stored-event backlog folds in only when that EOSE arrives, so a subscription lost
 * mid-replay cannot persist a cursor past events never received.
 */

import type { Clock, Engine } from "../backend/engine.ts";
import type {
  InputReport,
  NostrEvent,
  Outbound,
  PublishResolution,
  DropReason,
  ReceivedMessage,
  RelayResult,
  SessionEvent,
} from "../backend/protocol.ts";
import { pendingInvites } from "../invites/index.ts";
import type { InviteEvent } from "../invites/index.ts";
import type { CursorStore } from "../storage/driver.ts";
import type { PlannedSubscription, RelayPort, Timer } from "../transport/index.ts";
import {
  GIFT_WRAP_KIND,
  GROUP_EVENT_KIND,
  groupCursorKey,
  inboxCursorKey,
  planSubscriptions,
  safeMediaUrl,
  safeRelayUrl,
  wireEventBytes,
} from "../transport/index.ts";
import { verifyInbound } from "../transport/verify.ts";
import { MarmotError, toMarmotError } from "./errors.ts";
import type { ClientState } from "./state.ts";
import { encodeClientState } from "./state.ts";

/** One delivered application message. */
export type ClientMessage = {
  group: string;
  message: ReceivedMessage;
};

/** Session events surfaced to group-change listeners. */
export type GroupEvent = Exclude<
  SessionEvent,
  | { type: "message_received" }
  | { type: "publish_resolved" }
  | { type: "app_event_dropped" }
  | { type: "key_package_rotated" }
  | { type: "unmapped" }
>;

/** A dropped application event surfaced to `onDropped` listeners. */
export type DropEvent = { group: string; reason: DropReason };

/** A resolved outbound item for delivery listeners. */
export type DeliveryEvent = { outbound: string; resolution: PublishResolution };

/** Flush the inbound buffer inline once this many events are buffered. */
export const INGEST_BATCH_MAX = 256;
/**
 * The most distinct inbound events held at once — buffered plus already flushed to the engine but
 * not yet settled. Past it the delivering subscription is closed and reopened from the durable
 * cursor after `RESUBSCRIBE_DELAY_MS`.
 */
export const MAX_PENDING_INBOUND: number = 4 * INGEST_BATCH_MAX;
/** Inbound events whose wire encoding exceeds this are dropped before buffering. */
export const MAX_INBOUND_EVENT_BYTES: number = 1024 * 1024;
/** Commit ids tracked for post-confirmation refusal warnings before the oldest entry is evicted. */
export const MAX_TRACKED_COMMITS = 256;
/** Delay after the first buffered inbound event before a flush runs. */
export const INGEST_DELAY_MS = 25;
/** Delay before reconciling after a relay-initiated subscription close. */
export const RESUBSCRIBE_DELAY_MS = 5000;
/**
 * Kind-1059 gift wraps handed to `engine.ingest` per `GIFT_WRAP_WINDOW_MS` — every wrap costs a
 * signer `nip44_decrypt` (a remote round trip, possibly an extension prompt), so a wrap flood must
 * not stampede the signer. Kind-445 group events are never budgeted.
 */
export const GIFT_WRAP_BUDGET = 32;
/** The fixed window `GIFT_WRAP_BUDGET` applies to, on the injected clock. */
export const GIFT_WRAP_WINDOW_MS = 10_000;
/**
 * The largest commit sent to a relay whose event limit is unknown — MDK `COMMIT_SIZE_FLOOR_BYTES`
 * (mdk#2332). The client knows no relay limits, so every relay counts as unknown-limit.
 */
export const COMMIT_SIZE_FLOOR_BYTES: number = 64 * 1024;
/** What the pump needs to drive one client session. */
export type PumpOptions = {
  engine: Engine;
  relays: RelayPort;
  timer: Timer;
  clock: Clock;
  cursors: CursorStore;
  state: ClientState;
  account: string;
  allowLoopback: boolean;
};

type ActiveSub = {
  key: string;
  relay: string;
  kind: 445 | 1059;
  routes: string[];
  /** The relay signalled end of stored events; later deliveries count live. */
  eose: boolean;
  /** Per cursor key the pre-EOSE aggregate, folded into `state.cursors` at EOSE. */
  backlog: Map<string, { max?: number; cap?: number }>;
  /** An ingest covering this sub's backlog failed — its EOSE job applies nothing. */
  tainted: boolean;
  close: () => void;
};

/** An event plus, per cursor key, the sub that delivered it pre-EOSE (`undefined` when live). */
type Buffered = { event: NostrEvent; keys: Map<string, ActiveSub | undefined> };

/** Drop the oldest insertion once a commit-bookkeeping collection exceeds `MAX_TRACKED_COMMITS`. */
function capTracked<K, V>(entries: Map<K, V> | Set<K>): void {
  if (entries.size <= MAX_TRACKED_COMMITS) {
    return;
  }
  const oldest = entries.keys().next().value;
  if (oldest !== undefined) {
    entries.delete(oldest);
  }
}

export class Pump {
  private readonly engine: Engine;
  private readonly relays: RelayPort;
  private readonly timer: Timer;
  private readonly clock: Clock;
  private readonly cursorStore: CursorStore;
  private readonly state: ClientState;
  private readonly account: string;
  private readonly allowLoopback: boolean;

  private readonly listeners = {
    messages: new Set<(batch: ClientMessage[]) => void | Promise<void>>(),
    groupChange: new Set<(event: GroupEvent) => void>(),
    invite: new Set<(event: InviteEvent) => void>(),
    delivery: new Set<(event: DeliveryEvent) => void>(),
    dropped: new Set<(event: DropEvent) => void>(),
    error: new Set<(error: MarmotError) => void>(),
  };

  private queue: Promise<void> = Promise.resolve();
  private deliveryChain: Promise<void> = Promise.resolve();
  private saveChain: Promise<void> = Promise.resolve();
  private readonly inflight = new Set<Promise<void>>();
  private readonly buffer = new Map<string, Buffered>();
  private readonly active = new Map<string, ActiveSub>();
  /** Distinct events buffered or inside an unsettled ingest batch. */
  private pendingInbound = 0;
  private wake: (() => void) | undefined;
  /**
   * Confirmed-commit refusal tracking (mdk#2333): `confirmed` holds outbound ids whose report
   * resolved `confirmed`; `earlyRefusals` holds the relays that reported `rejected` while the
   * commit was still `pending` — an `accepted` from the same relay on a later attempt reverses that
   * refusal. Kept in memory only: a restart between the confirmation and a later refusal loses the
   * warning (MDK persists refusals until acknowledged; we surface them per occurrence). Each
   * collection evicts its oldest entry past `MAX_TRACKED_COMMITS`; eviction loses only a warning.
   */
  private readonly confirmedCommits = new Set<string>();
  private readonly earlyRefusals = new Map<string, Set<string>>();

  private flushTimer: (() => void) | undefined;
  /** Re-flush armed for the wrap budget window reset while wraps stay deferred in `buffer`. */
  private wrapTimer: (() => void) | undefined;
  /** Start of the current wrap budget window, ms on the injected clock. */
  private wrapWindowStart: number | undefined;
  /** Kind-1059 events `flushNow` may still take in the current window. */
  private wrapBudgetLeft = 0;
  private reconcileTimer: (() => void) | undefined;
  private reconcileFlag = false;
  private saveScheduled = false;
  private closed = false;
  private closePromise: Promise<void> | undefined;

  constructor(options: PumpOptions) {
    this.engine = options.engine;
    this.relays = options.relays;
    this.timer = options.timer;
    this.clock = options.clock;
    this.cursorStore = options.cursors;
    this.state = options.state;
    this.account = options.account;
    this.allowLoopback = options.allowLoopback;
  }

  /** Reconcile subscriptions and run the first tick, so early listeners see the first drain. */
  start(): void {
    this.reconcileFlag = true;
    this.scheduleWake(0);
  }

  onMessages(listener: (batch: ClientMessage[]) => void | Promise<void>): () => void {
    this.listeners.messages.add(listener);
    return () => {
      this.listeners.messages.delete(listener);
    };
  }

  onGroupChange(listener: (event: GroupEvent) => void): () => void {
    this.listeners.groupChange.add(listener);
    return () => {
      this.listeners.groupChange.delete(listener);
    };
  }

  onInvite(listener: (event: InviteEvent) => void): () => void {
    this.listeners.invite.add(listener);
    return () => {
      this.listeners.invite.delete(listener);
    };
  }

  onDelivery(listener: (event: DeliveryEvent) => void): () => void {
    this.listeners.delivery.add(listener);
    return () => {
      this.listeners.delivery.delete(listener);
    };
  }

  onDropped(listener: (event: DropEvent) => void): () => void {
    this.listeners.dropped.add(listener);
    return () => {
      this.listeners.dropped.delete(listener);
    };
  }

  onError(listener: (error: MarmotError) => void): () => void {
    this.listeners.error.add(listener);
    return () => {
      this.listeners.error.delete(listener);
    };
  }

  /**
   * Run `job` serialized; afterwards drain+dispatch, reconcile when flagged, and — when `take` is
   * true — publish whatever `takeOutbound` releases. The engine itself withholds paced items from
   * `takeOutbound`, so the take always runs.
   */
  async run<T>(job: () => Promise<T>, take = true): Promise<T> {
    const prior = this.queue;
    const run = (async () => {
      await prior;
      const result = await job();
      await this.afterOp(take);
      return result;
    })();
    this.queue = (async () => {
      try {
        await run;
      } catch {
        // The caller of `run` holds the rejection; the queue link only serializes.
      }
    })();
    return run;
  }

  /** Set the persisted inbox relay list: persist, then reconcile on the next op. */
  setInbox(relays: string[]): void {
    this.state.inbox = [...relays];
    this.scheduleSave();
    this.reconcileFlag = true;
  }

  /** Mark subscriptions for re-planning (a group-change listener observed churn). */
  flagReconcile(): void {
    this.reconcileFlag = true;
  }

  /**
   * Close every active kind-1059 subscription and reconcile, so the inbox subscriptions reopen with
   * `since` taken from the capped cursor (an `unavailable` ingest report caps its key at the
   * earliest `createdAt − 1`) and the relays replay the withheld wraps. Stores no wraps.
   */
  async retryInvites(): Promise<void> {
    await this.run(async () => {
      for (const [key, sub] of this.active) {
        if (sub.kind === GIFT_WRAP_KIND) {
          sub.close();
          this.active.delete(key);
        }
      }
      this.reconcileFlag = true;
      await Promise.resolve();
    }, false);
  }

  /**
   * Close the pump: stop timers and subscriptions, drop buffered inbound (unadvanced cursors are
   * refetched later), then await queued ops, in-flight publishes and their reports, the delivery
   * chain, `whenDurable` and the last cursor save — before `engine.close()`.
   */
  async close(): Promise<void> {
    if (this.closePromise !== undefined) {
      return this.closePromise;
    }
    this.closed = true;
    this.wake?.();
    this.wake = undefined;
    this.flushTimer?.();
    this.flushTimer = undefined;
    this.wrapTimer?.();
    this.wrapTimer = undefined;
    this.reconcileTimer?.();
    this.reconcileTimer = undefined;
    for (const sub of this.active.values()) {
      sub.close();
    }
    this.active.clear();
    this.buffer.clear();
    this.closePromise = (async () => {
      await this.queue;
      while (this.inflight.size > 0) {
        // oxlint-disable-next-line no-await-in-loop -- drain until publishers settle
        await Promise.allSettled(this.inflight);
        // oxlint-disable-next-line no-await-in-loop -- reports enqueue behind the queue
        await this.queue;
      }
      await this.deliveryChain;
      await this.queue;
      try {
        await this.engine.whenDurable();
      } catch {
        // Durability failures surface through onError elsewhere; close proceeds.
      }
      try {
        await this.saveChain;
      } catch {
        // Same: cursor save failures are already reported.
      }
      await this.engine.close();
    })();
    return this.closePromise;
  }

  private async afterOp(take: boolean): Promise<void> {
    try {
      const events = await this.engine.drain();
      this.dispatch(events);
      if (this.reconcileFlag) {
        this.reconcileFlag = false;
        await this.reconcile();
      }
      if (take) {
        // Tracked, not awaited: the caller resolves now; the publish still waits for
        // `whenDurable` inside `publishOutbox` (the durability gate), then runs in order.
        this.track(this.publishOutbox());
      }
    } catch (error) {
      this.emitError(toMarmotError(error));
    }
  }

  /**
   * Queue a tick + publish pass for an op that armed deferred engine work (a pending disband or the
   * delayed self-remove commit). Serialized after the op's own pass, so the promoted outbound still
   * publishes in order; the wake is re-armed from the engine's answer.
   */
  requestTick(): void {
    void (async () => {
      try {
        const next = await this.run(async () => this.engine.tick());
        if (!this.closed) {
          this.scheduleWake(next === undefined ? undefined : Number(next));
        }
      } catch (error) {
        this.emitError(toMarmotError(error));
      }
    })();
  }

  private async publishOutbox(): Promise<void> {
    if (this.closed) {
      return;
    }
    await this.engine.whenDurable();
    const items = await this.run(async () => this.engine.takeOutbound(), false);
    if (items.length === 0) {
      return;
    }
    for (const item of items) {
      this.track(this.publishItem(item));
    }
  }

  private track(work: Promise<void>): void {
    const tracked = (async () => {
      try {
        await work;
      } catch (error) {
        this.emitError(toMarmotError(error));
      }
    })();
    void (async () => {
      await tracked;
      this.inflight.delete(tracked);
    })();
    this.inflight.add(tracked);
  }

  /**
   * Publish one outbound item to every listed relay exactly once, concurrently, then report the
   * verdicts in `item.relays` order. A commit over `COMMIT_SIZE_FLOOR_BYTES` is never transmitted:
   * each safe relay is probed with `connect` — a reachable one gets a local `rejected` (and one
   * `commit_too_large` error for the item), an unreachable one a `not_sent`, so an offline device
   * never makes a commit look too large. After the report the take always runs — the engine
   * withholds paced items — and a `pending` resolution schedules a tick that recomputes the wake
   * from the engine.
   */
  private async publishItem(item: Outbound): Promise<void> {
    const overFloor =
      item.purpose === "commit" && wireEventBytes(item.event) > COMMIT_SIZE_FLOOR_BYTES;
    let floorRefused = false;
    const verdicts = await Promise.all(
      item.relays.map(async (relay): Promise<{ result: RelayResult; refused: boolean }> => {
        if (!safeRelayUrl(relay, this.allowLoopback)) {
          // Local policy prohibits the destination — no contact, terminal.
          return { result: { relay, status: "rejected" }, refused: false };
        }
        if (overFloor) {
          const reachable = await this.relays.connect(relay);
          floorRefused ||= reachable;
          return {
            result: { relay, status: reachable ? "rejected" : "not_sent" },
            refused: false,
          };
        }
        try {
          const status = await this.relays.publish(relay, item.event);
          return { result: { relay, status }, refused: status === "rejected" };
        } catch {
          return { result: { relay, status: "unavailable" }, refused: false };
        }
      }),
    );
    if (floorRefused) {
      this.emitError(new MarmotError("commit_too_large"));
    }
    let resolution: PublishResolution;
    try {
      resolution = await this.run(async () =>
        this.engine.reportPublish(
          item.id,
          verdicts.map((verdict) => verdict.result),
        ),
      );
    } catch (error) {
      // A row deleted mid-publish — sign-out retires every live KeyPackage
      // row in one transaction — answers `unknown_outbound`: nothing is left
      // to resolve, the queue is unwedged and no verdict is reportable.
      const mapped = toMarmotError(error);
      if (mapped.kind === "engine" && mapped.code === "unknown_outbound") {
        return;
      }
      throw mapped;
    }
    this.trackCommitRefusal(item, verdicts, resolution);
    // `pending` re-offers immediately; a `confirmed` commit merge can arm a new
    // convergence/lifecycle deadline that only a tick can observe — without the
    // wake the deferred work (e.g. an own disband) never advances.
    if (resolution === "pending" || (resolution === "confirmed" && item.purpose === "commit")) {
      this.scheduleWake(0);
    }
  }

  /**
   * Track one commit item's report for `commit_refused` warnings: a relay `rejected` verdict on a
   * commit that is — or becomes — `confirmed` warns; locally synthesized rejections (unsafe
   * destinations, floor refusals) never do.
   */
  private trackCommitRefusal(
    item: Outbound,
    verdicts: ReadonlyArray<{ result: RelayResult; refused: boolean }>,
    resolution: PublishResolution,
  ): void {
    if (item.purpose !== "commit") {
      return;
    }
    const early = this.earlyRefusals.get(item.id);
    if (early !== undefined) {
      for (const verdict of verdicts) {
        if (verdict.result.status === "accepted") {
          early.delete(verdict.result.relay);
        }
      }
      if (early.size === 0) {
        this.earlyRefusals.delete(item.id);
      }
    }
    const refused = verdicts.filter((verdict) => verdict.refused).map((v) => v.result.relay);
    const warn =
      (resolution === "confirmed" && (refused.length > 0 || this.earlyRefusals.has(item.id))) ||
      (this.confirmedCommits.has(item.id) && refused.length > 0);
    if (resolution === "pending" && refused.length > 0) {
      const set = this.earlyRefusals.get(item.id) ?? new Set<string>();
      for (const relay of refused) {
        set.add(relay);
      }
      this.earlyRefusals.set(item.id, set);
      capTracked(this.earlyRefusals);
    }
    if (resolution === "confirmed") {
      this.confirmedCommits.add(item.id);
      capTracked(this.confirmedCommits);
      this.earlyRefusals.delete(item.id);
    }
    if (resolution === "rolled_back" || resolution === "complete") {
      this.confirmedCommits.delete(item.id);
      this.earlyRefusals.delete(item.id);
    }
    if (warn) {
      this.emitError(new MarmotError("commit_refused"));
    }
  }

  private scheduleWake(ms: number | undefined): void {
    if (this.wake !== undefined) {
      this.wake();
      this.wake = undefined;
    }
    if (ms === undefined || this.closed) {
      return;
    }
    // setTimeout wraps at 2^31−1 ms (~24.8 days); an engine wake further out would clamp to 1 ms
    // and spin — cap it instead; any earlier op reschedules anyway.
    this.wake = this.timer.set(Math.min(ms, 0x7fffffff), () => {
      this.wake = undefined;
      this.runTick();
    });
  }

  /** One engine tick, then the publish pass; schedules the next wake from its answer. */
  private runTick(): void {
    void (async () => {
      let next: bigint | undefined;
      try {
        next = await this.run(async () => this.engine.tick());
      } catch (error) {
        this.emitError(toMarmotError(error));
        return;
      }
      if (this.closed) {
        return;
      }
      if (next === undefined) {
        this.scheduleWake(undefined);
      } else {
        this.scheduleWake(Number(next));
      }
    })();
  }

  private scheduleFlush(ms: number = INGEST_DELAY_MS): void {
    if (this.flushTimer !== undefined) {
      this.flushTimer();
      this.flushTimer = undefined;
    }
    this.flushTimer = this.timer.set(ms, () => {
      this.flushTimer = undefined;
      this.flushNow();
    });
  }

  /**
   * Arm the wrap-deferral re-flush for the current window's reset; a no-op while one is armed.
   * `flushNow` re-arms it whenever deferred wraps stay in the buffer.
   */
  private scheduleWrapFlush(ms: number): void {
    if (this.wrapTimer !== undefined) {
      return;
    }
    this.wrapTimer = this.timer.set(ms, () => {
      this.wrapTimer = undefined;
      this.flushNow();
    });
  }

  private onInbound(sub: ActiveSub, event: NostrEvent): void {
    if (this.closed) {
      return;
    }
    if (
      event.content.length > MAX_INBOUND_EVENT_BYTES ||
      wireEventBytes(event) > MAX_INBOUND_EVENT_BYTES
    ) {
      return;
    }
    // The port is not obliged to verify: a forged copy must never enter the dedupe buffer or
    // credit a cursor key, or a same-id forgery could shadow the genuine event.
    if (!verifyInbound(event)) {
      return;
    }
    let key: string;
    if (sub.kind === GIFT_WRAP_KIND) {
      if (event.kind !== GIFT_WRAP_KIND) {
        return;
      }
      key = inboxCursorKey(sub.relay);
    } else {
      if (event.kind !== GROUP_EVENT_KIND) {
        return;
      }
      const route = event.tags.find((tag) => tag[0] === "h")?.[1];
      if (route === undefined || !sub.routes.includes(route)) {
        return;
      }
      key = groupCursorKey(route, sub.relay);
    }
    const existing = this.buffer.get(event.id);
    if (existing === undefined) {
      if (this.pendingInbound >= MAX_PENDING_INBOUND) {
        // The relay outruns the engine: drop the event, close its subscription and let the
        // reconcile reopen it from the durable cursor after RESUBSCRIBE_DELAY_MS.
        sub.close();
        if (this.active.get(sub.key) === sub) {
          this.active.delete(sub.key);
        }
        this.scheduleReconcile();
        return;
      }
      this.pendingInbound += 1;
      this.buffer.set(event.id, {
        event,
        keys: new Map([[key, sub.eose ? undefined : sub]]),
      });
    } else {
      existing.keys.set(key, sub.eose ? undefined : sub);
    }
    if (this.buffer.size >= INGEST_BATCH_MAX) {
      this.flushNow();
    } else if (this.flushTimer === undefined) {
      this.scheduleFlush();
    }
  }

  private flushNow(): void {
    if (this.flushTimer !== undefined) {
      this.flushTimer();
      this.flushTimer = undefined;
    }
    if (this.buffer.size === 0 || this.closed) {
      return;
    }
    const nowMs = Number(this.clock.now().unixMs);
    let windowStart = this.wrapWindowStart;
    if (windowStart === undefined || nowMs - windowStart >= GIFT_WRAP_WINDOW_MS) {
      windowStart = nowMs;
      this.wrapBudgetLeft = GIFT_WRAP_BUDGET;
    }
    this.wrapWindowStart = windowStart;
    // Every group event flushes; kind-1059s flush only within the window's remaining budget —
    // deferred wraps stay buffered (still counted in `pendingInbound`) until the window resets.
    const batch: Buffered[] = [];
    let wraps = 0;
    for (const [id, entry] of this.buffer) {
      if (entry.event.kind === GIFT_WRAP_KIND) {
        if (wraps >= this.wrapBudgetLeft) {
          continue;
        }
        wraps += 1;
      }
      batch.push(entry);
      this.buffer.delete(id);
    }
    this.wrapBudgetLeft -= wraps;
    if (this.buffer.size > 0) {
      this.scheduleWrapFlush(windowStart + GIFT_WRAP_WINDOW_MS - nowMs);
    }
    if (batch.length === 0) {
      return;
    }
    const work = this.run(async () => {
      const events = batch.map((entry) => entry.event);
      let reports: InputReport[];
      try {
        reports = await this.engine.ingest(events);
      } catch (error) {
        // The ingest never landed: every sub this batch carried backlog for is tainted — its
        // EOSE job applies nothing and the next reopen refetches the range. Marked inside the
        // job so it is visible to the fold link queued behind this run; a detached `work`
        // catch would race it.
        for (const entry of batch) {
          for (const sub of entry.keys.values()) {
            if (sub !== undefined) {
              sub.tainted = true;
            }
          }
        }
        throw error;
      }
      for (const invite of pendingInvites(reports)) {
        this.emitInvite(invite);
      }
      await this.engine.whenDurable();
      this.advanceCursors(batch, reports);
      this.scheduleSave();
    });
    void (async () => {
      try {
        await work;
      } catch (error) {
        this.emitError(toMarmotError(error));
        return;
      } finally {
        this.pendingInbound -= batch.length;
      }
      // Deferred peels are bounded by real time — keep ticking until the engine is quiet, and
      // re-flush immediately whatever arrived while this batch was ingesting.
      this.scheduleWake(0);
      if (this.buffer.size > 0) {
        this.scheduleFlush(0);
      }
    })();
  }

  /**
   * Cursor advance, per key: `max(old, min(createdAt, nowSecs))` over events whose outcome is not
   * `unavailable`; any `unavailable` event caps the key at its earliest `createdAt` − 1 — the range
   * is not synchronized, so the cursor may move back. Keys an event reached after its
   * subscription's EOSE apply to `state.cursors` here; keys it reached from the stored-event
   * backlog fold into the delivering sub's `backlog` instead and land only when that sub's EOSE job
   * runs — a sub closed or tainted before then drops its aggregate and the next `since` refetches
   * the range.
   */
  private advanceCursors(batch: Buffered[], reports: InputReport[]): void {
    const outcomes = new Map(reports.map((report) => [report.event, report.outcome] as const));
    const nowSecs = Number(this.clock.now().unixMs / 1000n);
    const perKey = new Map<string, Array<{ createdAt: bigint; unavailable: boolean }>>();
    for (const entry of batch) {
      const outcome = outcomes.get(entry.event.id);
      if (outcome === undefined) {
        continue;
      }
      for (const [key, sub] of entry.keys) {
        if (sub !== undefined) {
          let aggregate = sub.backlog.get(key);
          if (aggregate === undefined) {
            aggregate = {};
            sub.backlog.set(key, aggregate);
          }
          if (outcome.type === "unavailable") {
            const at = Number(entry.event.createdAt) - 1;
            aggregate.cap = aggregate.cap === undefined ? at : Math.min(aggregate.cap, at);
          } else {
            const secs = Math.min(Number(entry.event.createdAt), nowSecs);
            aggregate.max = aggregate.max === undefined ? secs : Math.max(aggregate.max, secs);
          }
          continue;
        }
        let list = perKey.get(key);
        if (list === undefined) {
          list = [];
          perKey.set(key, list);
        }
        list.push({
          createdAt: entry.event.createdAt,
          unavailable: outcome.type === "unavailable",
        });
      }
    }
    for (const [key, entries] of perKey) {
      const old = this.state.cursors[key] ?? 0;
      let value = old;
      for (const entry of entries) {
        if (entry.unavailable) {
          continue;
        }
        const secs = Math.min(Number(entry.createdAt), nowSecs);
        if (secs > value) {
          value = secs;
        }
      }
      const buffered = this.bufferedCap(key);
      if (buffered !== undefined) {
        value = Math.max(old, Math.min(value, buffered));
      }
      let cap: number | undefined;
      for (const entry of entries) {
        if (entry.unavailable) {
          const at = Number(entry.createdAt) - 1;
          cap = cap === undefined ? at : Math.min(cap, at);
        }
      }
      if (cap !== undefined) {
        value = Math.max(0, Math.min(value, cap));
      }
      this.state.cursors[key] = value;
    }
  }

  /**
   * `min(createdAt) − 1` over still-buffered (not yet ingested) events carrying `key`, or
   * `undefined` when none remain. A cursor must never advance past an event still buffered for it:
   * a budget-deferred wrap sitting on the far side of a persisted `since` would never be refetched
   * after reconnect. It may only stall advancement, never pull a cursor below its previous value —
   * every buffered event arrived through a subscription whose `since` already covers it, so the old
   * cursor still refetches it. Applies to live keys (`advanceCursors`) and the EOSE fold
   * (`applyBacklog`) alike — a live wrap deferred beside a folded backlog is just as lost.
   */
  private bufferedCap(key: string): number | undefined {
    let cap: number | undefined;
    for (const entry of this.buffer.values()) {
      if (!entry.keys.has(key)) {
        continue;
      }
      const at = Number(entry.event.createdAt) - 1;
      cap = cap === undefined ? at : Math.min(cap, at);
    }
    return cap;
  }

  /** Apply a settled backlog aggregate (`sub.backlog` entries) at EOSE; used only by the EOSE job. */
  private applyBacklog(sub: ActiveSub): void {
    if (this.active.get(sub.key) !== sub || sub.tainted || sub.backlog.size === 0) {
      return;
    }
    for (const [key, aggregate] of sub.backlog) {
      const old = this.state.cursors[key] ?? 0;
      let value = old;
      if (aggregate.max !== undefined && aggregate.max > value) {
        value = aggregate.max;
      }
      const buffered = this.bufferedCap(key);
      if (buffered !== undefined) {
        value = Math.max(old, Math.min(value, buffered));
      }
      const { cap } = aggregate;
      if (cap !== undefined) {
        value = Math.max(0, Math.min(value, cap));
      }
      this.state.cursors[key] = value;
    }
    sub.backlog.clear();
    this.scheduleSave();
  }

  private scheduleSave(): void {
    if (this.saveScheduled) {
      return;
    }
    this.saveScheduled = true;
    this.saveChain = (async () => {
      await this.saveChain;
      this.saveScheduled = false;
      try {
        await this.cursorStore.save(encodeClientState(this.state));
      } catch (error) {
        this.emitError(toMarmotError(error));
      }
    })();
  }

  private dispatch(events: SessionEvent[]): void {
    const batch: ClientMessage[] = [];
    for (const event of events) {
      switch (event.type) {
        case "message_received":
          batch.push({
            group: event.group,
            message: this.filterAttachmentLocators(event.message),
          });
          break;
        case "publish_resolved":
          this.emitDelivery({ outbound: event.outbound, resolution: event.resolution });
          break;
        case "group_joined":
          this.reconcileFlag = true;
          this.emitInvite({ type: "joined", group: event.group });
          this.emitGroupChange(event);
          break;
        case "app_event_dropped":
          this.emitDropped({ group: event.group, reason: event.reason });
          break;
        case "key_package_rotated":
        case "unmapped":
          break;
        case "group_created":
        case "local_copy_terminated":
        case "local_copy_restored":
        case "group_state_changed":
        case "epoch_changed":
        case "group_unrecoverable":
        case "group_state_invalidated":
        case "group_state_revalidated":
          this.reconcileFlag = true;
          this.emitGroupChange(event);
          break;
        default:
          this.emitGroupChange(event);
          break;
      }
    }
    if (batch.length > 0) {
      this.enqueueDelivery(batch);
    }
  }

  /**
   * Deliver a message batch through the serialized delivery chain; after every listener's promise
   * resolves, `acknowledgeMessages` runs in the op queue. No listeners or a rejecting listener → no
   * ack (the engine redelivers after reopen; a rejection also emits `handler_failed`).
   */
  private enqueueDelivery(batch: ClientMessage[]): void {
    this.deliveryChain = (async () => {
      await this.deliveryChain;
      const listeners = [...this.listeners.messages];
      if (listeners.length === 0) {
        return;
      }
      const results = await Promise.allSettled(listeners.map(async (listener) => listener(batch)));
      if (results.some((result) => result.status === "rejected")) {
        this.emitError(new MarmotError("handler_failed"));
        return;
      }
      const ids = batch.map((entry) => entry.message.transportId);
      try {
        await this.run(async () => this.engine.acknowledgeMessages(ids), false);
      } catch (error) {
        this.emitError(toMarmotError(error));
      }
    })();
  }

  private async reconcile(): Promise<void> {
    if (this.closed) {
      return;
    }
    const snapshots = await this.engine.groups();
    const live = snapshots.filter(
      (snapshot) => snapshot.state !== "removed" && snapshot.state !== "disbanded",
    );
    const plan = planSubscriptions({
      account: this.account,
      inboxRelays: this.state.inbox.filter((relay) => safeRelayUrl(relay, this.allowLoopback)),
      groups: live.map((snapshot) => ({
        route: snapshot.nostrGroupId,
        relays: snapshot.relays.filter((relay) => safeRelayUrl(relay, this.allowLoopback)),
      })),
      cursor: (key) => this.state.cursors[key],
    });
    const planned = new Map(plan.subscriptions.map((sub) => [sub.key, sub] as const));
    for (const [key, sub] of this.active) {
      if (!planned.has(key)) {
        sub.close();
        this.active.delete(key);
      }
    }
    for (const [key, sub] of planned) {
      if (!this.active.has(key)) {
        this.active.set(key, this.openSub(sub));
      }
    }
    if (plan.uncovered.length > 0) {
      this.emitError(new MarmotError("subscription_limit"));
    }
  }

  private openSub(planned: PlannedSubscription): ActiveSub {
    const sub: ActiveSub = {
      key: planned.key,
      relay: planned.relay,
      kind: planned.kind,
      routes: planned.routes,
      eose: false,
      backlog: new Map(),
      tainted: false,
      close: () => undefined,
    };
    sub.close = this.relays.subscribe(planned.relay, planned.filters, {
      event: (event) => {
        this.onInbound(sub, event);
      },
      eose: () => {
        if (sub.eose) {
          return;
        }
        sub.eose = true;
        // Flush so every pre-EOSE event's ingest is queued ahead of the fold; the link runs
        // after every still-parked pre-EOSE batch settled, so the aggregate is complete — and
        // tainted — by the time it lands. A bare queue link: the fold needs serialization,
        // not another drain/dispatch cycle.
        this.flushNow();
        const prior = this.queue;
        this.queue = (async () => {
          try {
            await prior;
            this.applyBacklog(sub);
          } catch (error) {
            this.emitError(toMarmotError(error));
          }
        })();
      },
      closed: () => {
        this.active.delete(sub.key);
        this.scheduleReconcile();
      },
    });
    return sub;
  }

  private scheduleReconcile(): void {
    if (this.reconcileTimer !== undefined || this.closed) {
      return;
    }
    this.reconcileTimer = this.timer.set(RESUBSCRIBE_DELAY_MS, () => {
      this.reconcileTimer = undefined;
      this.reconcileFlag = true;
      void (async () => {
        try {
          await this.run(async () => {
            await Promise.resolve();
          }, false);
        } catch (error) {
          this.emitError(toMarmotError(error));
        }
      })();
    });
  }

  /**
   * Peer-supplied locators reach `onMessages` listeners verbatim: keep only the ones `safeMediaUrl`
   * accepts. The attachment itself is never dropped — `openAttachment` authenticates against the
   * reference hashes, which do not include the locators.
   */
  private filterAttachmentLocators(message: ReceivedMessage): ReceivedMessage {
    return {
      ...message,
      attachments: message.attachments.map((attachment) =>
        attachment.type === "ok"
          ? {
              ...attachment,
              reference: {
                ...attachment.reference,
                locators: attachment.reference.locators.filter((locator) =>
                  safeMediaUrl(locator.value, this.allowLoopback),
                ),
              },
            }
          : attachment,
      ),
    };
  }

  private emitGroupChange(event: GroupEvent): void {
    for (const listener of this.listeners.groupChange) {
      try {
        listener(event);
      } catch {
        this.emitError(new MarmotError("handler_failed"));
      }
    }
  }

  private emitDropped(event: DropEvent): void {
    for (const listener of this.listeners.dropped) {
      try {
        listener(event);
      } catch {
        this.emitError(new MarmotError("handler_failed"));
      }
    }
  }

  private emitInvite(event: InviteEvent): void {
    for (const listener of this.listeners.invite) {
      try {
        listener(event);
      } catch {
        this.emitError(new MarmotError("handler_failed"));
      }
    }
  }

  private emitDelivery(event: DeliveryEvent): void {
    for (const listener of this.listeners.delivery) {
      try {
        listener(event);
      } catch {
        this.emitError(new MarmotError("handler_failed"));
      }
    }
  }

  emitError(error: MarmotError): void {
    for (const listener of this.listeners.error) {
      try {
        listener(error);
      } catch {
        // Errors from onError listeners are swallowed.
      }
    }
  }
}
