/**
 * `runFlow` — the shared end-to-end scenario every runtime (Bun, Node, browser) drives through the
 * public `Engine` API only: A creates a group inviting B → B ingests the gift wrap and joins →
 * `messages` messages A→B ingested in one batch (order and content asserted) → both members close
 * and reopen from their KvDrivers → A adds C → C joins → A removes C → one publish cycle that fails
 * on every relay, then retries to a terminal resolution.
 *
 * Every clock is a `ManualClock` the flow advances exactly the way `full_flow` does in
 * `mk-bridge`'s test suite — no real sleeping. Any mismatch throws; on success the per-stage
 * wall-clock timings are returned.
 */

import { Engine } from "../backend/engine.ts";
import type { Signer } from "../backend/engine.ts";
import { BridgeError } from "../backend/errors.ts";
import type { Backend } from "../backend/port.ts";
import type {
  NostrEvent,
  PublishResolution,
  ReceivedMessage,
  SessionEvent,
} from "../backend/protocol.ts";
import type { KvDriver } from "../storage/driver.ts";
import { manualClock } from "./clock.ts";
import type { ManualClock } from "./clock.ts";
import { fakeRelay } from "./relay.ts";
import type { FakeRelay } from "./relay.ts";

const RELAY = "wss://relay.example";

/** The injections `runFlow` needs. */
export type FlowEnv = {
  backend: Backend;
  /** The member's persistent store; reopening calls `kv` again with the same name. */
  kv: (name: string) => Promise<KvDriver> | KvDriver;
  /** A fresh `Signer` (fresh account key) per call — one per member. */
  signer: () => Signer;
  /**
   * A fresh clock per member; `runFlow` owns advancing it (send steps +1 s, sync rounds +2 s,
   * matching `full_flow`). Defaults to `manualClock`.
   */
  clock?: () => ManualClock;
  messages: number;
};

/** Wall-clock milliseconds per flow stage. */
export type FlowTimings = {
  open: number;
  create: number;
  join: number;
  send: number;
  ingest: number;
  reopen: number;
  add: number;
  remove: number;
  failureRetry: number;
};

type Member = {
  name: string;
  engine: Engine;
  clock: ManualClock;
  signer: Signer;
};

/** Acknowledge every delivered message, exactly the host's receive contract. */
async function ingestAndDrain(member: Member, events: NostrEvent[]): Promise<SessionEvent[]> {
  await member.engine.ingest(events);
  const drained = await member.engine.drain();
  const ids = drained
    .map((event) => (event.type === "message_received" ? event.message.transportId : undefined))
    .filter((id): id is string => id !== undefined);
  if (ids.length > 0) {
    await member.engine.acknowledgeMessages(ids);
  }
  return drained;
}

/** TakeOutbound → relay → reportPublish until nothing is gated, like the host delivery loop. */
async function publish(member: Member, relay: FakeRelay): Promise<PublishResolution[]> {
  const resolutions: PublishResolution[] = [];
  for (let round = 0; round < 64; round += 1) {
    // oxlint-disable-next-line no-await-in-loop -- the flow is sequential by design
    await member.engine.whenDurable();
    // oxlint-disable-next-line no-await-in-loop -- the flow is sequential by design
    const items = await member.engine.takeOutbound();
    if (items.length === 0) {
      break;
    }
    for (const item of items) {
      const results = relay.publish(item);
      // oxlint-disable-next-line no-await-in-loop -- the flow is sequential by design
      resolutions.push(await member.engine.reportPublish(item.id, results));
    }
  }
  return resolutions;
}

/** The group's `#h` route hex, read through `group`. */
async function routeHex(member: Member, group: string): Promise<string> {
  const snapshot = await member.engine.group(group);
  if (snapshot === undefined) {
    throw new Error("group snapshot missing");
  }
  return snapshot.nostrGroupId;
}

/**
 * Deliver what the relay holds for `member`, tick, advance 2 s, publish — repeated until quiescent:
 * no fresh events, nothing published, and `tick` schedules no wake inside the window. A hard cap
 * turns divergence into a failure instead of a hang.
 */
async function sync(member: Member, relay: FakeRelay, group: string): Promise<void> {
  const route = await routeHex(member, group);
  const windowMs = 2000n;
  for (let round = 0; round < 64; round += 1) {
    const fresh = relay.groupEvents(route, member.name);
    if (fresh.length > 0) {
      // oxlint-disable-next-line no-await-in-loop -- the flow is sequential by design
      await ingestAndDrain(member, fresh);
    }
    // oxlint-disable-next-line no-await-in-loop -- the flow is sequential by design
    const nextWakeMs = await member.engine.tick();
    member.clock.advance(Number(windowMs));
    // oxlint-disable-next-line no-await-in-loop -- the flow is sequential by design
    const resolutions = await publish(member, relay);
    const woke = nextWakeMs !== undefined && nextWakeMs <= windowMs;
    if (fresh.length === 0 && resolutions.length === 0 && !woke) {
      return;
    }
  }
  throw new Error(`runFlow: sync for ${member.name} did not quiesce in 64 rounds`);
}

async function publishIdentity(member: Member, relay: FakeRelay): Promise<void> {
  await member.engine.publishKeyPackage([RELAY]);
  await publish(member, relay);
  await member.engine.publishInboxRelays([RELAY]);
  await publish(member, relay);
}

function messageContents(events: SessionEvent[]): string[] {
  return events
    .map((event) => (event.type === "message_received" ? event.message : undefined))
    .filter((message): message is ReceivedMessage => message !== undefined)
    .map((message) => message.content);
}

function assert(condition: boolean, site: string): void {
  if (!condition) {
    throw new Error(`runFlow: ${site}`);
  }
}

/** Run the shared flow; throws on any mismatch, returns per-stage timings. */
export async function runFlow(env: FlowEnv): Promise<FlowTimings> {
  const relay = fakeRelay();
  const clockFor: () => ManualClock = env.clock ?? manualClock;
  // oxlint-disable-next-line no-unsafe-type-assertion -- each stage stamps its key before return
  const timings = {} as FlowTimings;

  const openMember = async (name: string, signer: Signer): Promise<Member> => {
    const clock = clockFor();
    const engine = await Engine.open({
      backend: env.backend,
      kv: await env.kv(name),
      clock,
      signer,
      allowLoopback: true,
    });
    return { name, engine, clock, signer };
  };

  let cursor = Date.now();
  const stamp = (stage: keyof FlowTimings): void => {
    const now = Date.now();
    timings[stage] = now - cursor;
    cursor = now;
  };

  // ---- open: A and B exist and publish their identities ----
  let a = await openMember("a", env.signer());
  let b = await openMember("b", env.signer());
  await publishIdentity(a, relay);
  await publishIdentity(b, relay);
  stamp("open");

  // ---- create: A creates the group, inviting B via its published KeyPackage ----
  const group = await a.engine.createGroup({
    name: "flow",
    description: "",
    relays: [RELAY],
    invitees: [{ keyPackage: relay.keyPackageOf(b.signer.pubkey), inboxRelays: [RELAY] }],
  });
  await publish(a, relay);
  stamp("create");

  // ---- join: B ingests its gift wraps and both sides converge ----
  await b.engine.ingest(relay.welcomesFor(b.signer.pubkey));
  await sync(b, relay, group.id);
  await sync(a, relay, group.id);
  stamp("join");

  // ---- send N: A messages B, publishing as it goes ----
  for (let i = 0; i < env.messages; i += 1) {
    a.clock.advance(1000);
    // `group_busy` is a legitimate transient verdict while the engine settles
    // convergence or maintenance work; a host settles and retries the send.
    for (let attempt = 0; attempt < 64; attempt += 1) {
      try {
        // oxlint-disable-next-line no-await-in-loop -- the flow is sequential by design
        await a.engine.sendMessage(group.id, { kind: 9, tags: [], content: `m${i}` });
        break;
      } catch (error) {
        if (attempt === 63 || !(error instanceof BridgeError) || error.kind !== "group_busy") {
          throw error;
        }
        // oxlint-disable-next-line no-await-in-loop -- the flow is sequential by design
        await sync(a, relay, group.id);
      }
    }
    // oxlint-disable-next-line no-await-in-loop -- the flow is sequential by design
    await publish(a, relay);
  }
  stamp("send");

  // ---- ingest N: one ingest call feeds the whole backlog; delivery drains in order ----
  // The engine bounds deferred peels by a real-time foreground budget, so a
  // slower runtime completes the drain over tick rounds — collect to quiescent.
  const route = await routeHex(b, group.id);
  const expected = Array.from({ length: env.messages }, (_, i) => `m${i}`);
  const contents = messageContents(await ingestAndDrain(b, relay.groupEvents(route, b.name)));
  for (let round = 0; contents.length < expected.length && round < 64; round += 1) {
    // oxlint-disable-next-line no-await-in-loop -- the flow is sequential by design
    await b.engine.tick();
    b.clock.advance(2000);
    const fresh = relay.groupEvents(route, b.name);
    // oxlint-disable-next-line no-await-in-loop -- the flow is sequential by design
    const drained = await ingestAndDrain(b, fresh);
    contents.push(...messageContents(drained));
    // oxlint-disable-next-line no-await-in-loop -- the flow is sequential by design
    await publish(b, relay);
  }
  assert(
    contents.length === expected.length && contents.every((text, i) => text === expected[i]),
    `ingest delivered ${contents.length}/${expected.length} messages`,
  );
  await b.engine.tick();
  b.clock.advance(2000);
  await publish(b, relay);
  stamp("ingest");

  // ---- reopen: both members restart from their durable rows ----
  await a.engine.close();
  await b.engine.close();
  a = await openMember("a", a.signer);
  b = await openMember("b", b.signer);
  await sync(a, relay, group.id);
  await sync(b, relay, group.id);
  stamp("reopen");

  // ---- add: A invites C through its published KeyPackage; C joins ----
  const c = await openMember("c", env.signer());
  await publishIdentity(c, relay);
  await a.engine.invite(group.id, [
    { keyPackage: relay.keyPackageOf(c.signer.pubkey), inboxRelays: [RELAY] },
  ]);
  await publish(a, relay);
  await c.engine.ingest(relay.welcomesFor(c.signer.pubkey));
  await sync(c, relay, group.id);
  await sync(a, relay, group.id);
  stamp("add");

  // ---- remove: A removes C ----
  await a.engine.removeMembers(group.id, [c.signer.pubkey]);
  await publish(a, relay);
  await sync(c, relay, group.id);
  await sync(a, relay, group.id);
  stamp("remove");

  // ---- failure + retry: every relay down, then back up ----
  a.clock.advance(1000);
  await a.engine.updateGroupProfile(group.id, "after", undefined);
  relay.down = true;
  const failed = await publish(a, relay);
  assert(
    failed.length > 0 && failed.every((resolution) => resolution === "pending"),
    "all-unavailable verdicts must stay pending",
  );
  relay.down = false;
  await a.engine.tick();
  const retried = await publish(a, relay);
  assert(
    retried.some((resolution) => resolution !== "pending"),
    "retry must reach a terminal resolution",
  );
  stamp("failureRetry");

  await a.engine.close();
  await b.engine.close();
  await c.engine.close();
  return timings;
}
