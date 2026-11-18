/**
 * `Pump` — the one serialized loop that owns the `Engine`: every op runs through `run`, which
 * drains and dispatches session events, reconciles subscriptions when flagged, and publishes
 * whatever `takeOutbound` released after `whenDurable`. Inbound events are buffered per event id
 * with the cursor keys that delivered them, ingested in bounded batches, and cursors advance only
 * after `whenDurable`.
 */

import type { Clock, Engine } from "../backend/engine.ts";
import type {
  InputReport,
  NostrEvent,
  Outbound,
  PublishResolution,
  ReceivedMessage,
  RelayResult,
  SessionEvent,
} from "../backend/protocol.ts";
import { blockedInvites } from "../invites/index.ts";
import type { InviteEvent } from "../invites/index.ts";
import type { CursorStore } from "../storage/driver.ts";
import type { PlannedSubscription, RelayPort, Timer } from "../transport/index.ts";
import {
  GIFT_WRAP_KIND,
  GROUP_EVENT_KIND,
  groupCursorKey,
  inboxCursorKey,
  planSubscriptions,
  safeRelayUrl,
} from "../transport/index.ts";
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
  { type: "message_received" } | { type: "publish_resolved" } | { type: "unmapped" }
>;

/** A resolved outbound item for delivery listeners. */
export type DeliveryEvent = { outbound: string; resolution: PublishResolution };

/** Flush the inbound buffer inline once this many events are buffered. */
export const INGEST_BATCH_MAX = 256;
/** Delay after the first buffered inbound event before a flush runs. */
export const INGEST_DELAY_MS = 25;
/** Dedicated outbox retry cadence after a publish reported `unavailable`. */
export const OUTBOX_RETRY_TICK_MS = 5000;
/** Delay before reconciling after a relay-initiated subscription close. */
export const RESUBSCRIBE_DELAY_MS = 5000;

type SignerVerdict = "refused" | "failed" | undefined;

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
  /** `observeSigner`'s `take`: the first refused/failed signer call since the last take. */
  takeVerdict: () => SignerVerdict;
};

type ActiveSub = {
  key: string;
  relay: string;
  kind: 445 | 1059;
  routes: string[];
  close: () => void;
};

type Buffered = { event: NostrEvent; keys: Set<string> };

export class Pump {
  private readonly engine: Engine;
  private readonly relays: RelayPort;
  private readonly timer: Timer;
  private readonly clock: Clock;
  private readonly cursorStore: CursorStore;
  private readonly state: ClientState;
  private readonly account: string;
  private readonly allowLoopback: boolean;
  private readonly takeVerdict: () => SignerVerdict;

  private readonly listeners = {
    messages: new Set<(batch: ClientMessage[]) => void | Promise<void>>(),
    groupChange: new Set<(event: GroupEvent) => void>(),
    invite: new Set<(event: InviteEvent) => void>(),
    delivery: new Set<(event: DeliveryEvent) => void>(),
    error: new Set<(error: MarmotError) => void>(),
  };

  private queue: Promise<void> = Promise.resolve();
  private deliveryChain: Promise<void> = Promise.resolve();
  private saveChain: Promise<void> = Promise.resolve();
  private readonly inflight = new Set<Promise<void>>();
  private readonly buffer = new Map<string, Buffered>();
  private readonly active = new Map<string, ActiveSub>();
  private wake: (() => void) | undefined;
  private flushTimer: (() => void) | undefined;
  private reconcileTimer: (() => void) | undefined;
  private reconcileFlag = false;
  private retryTimer: (() => void) | undefined;
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
    this.takeVerdict = options.takeVerdict;
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

  onError(listener: (error: MarmotError) => void): () => void {
    this.listeners.error.add(listener);
    return () => {
      this.listeners.error.delete(listener);
    };
  }

  /**
   * Run `job` serialized; afterwards drain+dispatch, reconcile when flagged, and — when `take`
   * resolves true — publish whatever `takeOutbound` releases. `take` is a function of the job's
   * result for `reportPublish` (a `pending` resolution must not re-take immediately).
   */
  async run<T>(job: () => Promise<T>, take: boolean | ((result: T) => boolean) = true): Promise<T> {
    const prior = this.queue;
    const run = (async () => {
      await prior;
      const result = await job();
      const doTake = typeof take === "function" ? take(result) : take;
      await this.afterOp(doTake);
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
    this.reconcileTimer?.();
    this.reconcileTimer = undefined;
    this.retryTimer?.();
    this.retryTimer = undefined;
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

  /** After a report that contained `unavailable`: one retry tick, whatever the engine wake says. */
  private scheduleRetry(): void {
    if (this.retryTimer !== undefined || this.closed) {
      return;
    }
    this.retryTimer = this.timer.set(OUTBOX_RETRY_TICK_MS, () => {
      this.retryTimer = undefined;
      this.runTick();
    });
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
   * verdicts in `item.relays` order. An `unavailable` verdict arms the dedicated retry timer.
   */
  private async publishItem(item: Outbound): Promise<void> {
    const results = await Promise.all(
      item.relays.map(async (relay): Promise<RelayResult> => {
        if (!safeRelayUrl(relay, this.allowLoopback)) {
          // Local policy prohibits the destination — no contact, terminal.
          return { relay, status: "rejected" };
        }
        try {
          return { relay, status: await this.relays.publish(relay, item.event) };
        } catch {
          return { relay, status: "unavailable" };
        }
      }),
    );
    await this.run(
      async () => this.engine.reportPublish(item.id, results),
      (resolution) => resolution !== "pending",
    );
    if (results.some((result) => result.status === "unavailable")) {
      this.scheduleRetry();
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

  private onInbound(sub: ActiveSub, event: NostrEvent): void {
    if (this.closed) {
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
      this.buffer.set(event.id, { event, keys: new Set([key]) });
    } else {
      existing.keys.add(key);
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
    const batch = [...this.buffer.values()];
    this.buffer.clear();
    const work = this.run(async () => {
      this.takeVerdict();
      const events = batch.map((entry) => entry.event);
      const reports = await this.engine.ingest(events);
      const verdict = this.takeVerdict();
      const wraps = new Set(
        events.filter((event) => event.kind === GIFT_WRAP_KIND).map((event) => event.id),
      );
      for (const invite of blockedInvites(reports, wraps, verdict)) {
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
   * is not synchronized, so the cursor may move back.
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
      for (const key of entry.keys) {
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
      let value = this.state.cursors[key] ?? 0;
      for (const entry of entries) {
        if (entry.unavailable) {
          continue;
        }
        const secs = Math.min(Number(entry.createdAt), nowSecs);
        if (secs > value) {
          value = secs;
        }
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
          batch.push({ group: event.group, message: event.message });
          break;
        case "publish_resolved":
          this.emitDelivery({ outbound: event.outbound, resolution: event.resolution });
          break;
        case "group_joined":
          this.reconcileFlag = true;
          this.emitInvite({ type: "joined", group: event.group });
          this.emitGroupChange(event);
          break;
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
      close: () => undefined,
    };
    sub.close = this.relays.subscribe(planned.relay, planned.filters, {
      event: (event) => {
        this.onInbound(sub, event);
      },
      eose: () => undefined,
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

  private emitGroupChange(event: GroupEvent): void {
    for (const listener of this.listeners.groupChange) {
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

  private emitError(error: MarmotError): void {
    for (const listener of this.listeners.error) {
      try {
        listener(error);
      } catch {
        // Errors from onError listeners are swallowed.
      }
    }
  }
}
