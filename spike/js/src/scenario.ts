// The shared M0 scenario — identical to spike/engine/tests/flow.rs.
// Driven over MemberDriver so the same code runs in Bun (local wasm) and in
// Node driving Chromium pages (remote wasm over page.evaluate).

import {
  FakeRelay,
  KIND_GIFT_WRAP,
  KIND_GROUP_MESSAGE,
  type RelayEvent,
} from "./relay.js";
import {
  IngestCounts,
  flushOutbound,
  publishResult,
  type MemberDriver,
} from "./member.js";

export interface ScenarioEnv {
  relay: FakeRelay;
  makeMember(seed: string): Promise<MemberDriver>;
  /** A second reader opened on a persisted-state entries snapshot. */
  spawnReader(seed: string, entries: [string, string][]): Promise<MemberDriver>;
  snapshot(member: MemberDriver): Promise<[string, string][]>;
  assert(cond: unknown, msg: string): void;
  row(step: string, detail: string): void;
  time(): number;
  /** Optional perf hooks for MEASUREMENTS.md. */
  measure?<T>(name: string, f: () => Promise<T>): Promise<T>;
  pubKeyOf?(member: MemberDriver): Promise<string>;
}

const MSG_PER_SENDER = 250;
const HALF = MSG_PER_SENDER / 2;
const BASE = 1_700_000_000;

async function sendChat(
  member: MemberDriver,
  relay: FakeRelay,
  gid: string,
  content: string,
  createdAt: number,
): Promise<string | undefined> {
  const result = await member.send(gid, {
    type: "appMessage",
    content,
    createdAt,
  });
  const id = result.type === "applicationMessage" ? result.events[0]?.id : undefined;
  await publishResult(member, result, (ev) => relay.tryPublish(ev));
  return id;
}

async function ingestAll(
  member: MemberDriver,
  relay: FakeRelay,
  gid: string,
  events: RelayEvent[],
): Promise<IngestCounts> {
  const counts = new IngestCounts();
  for (const ev of events) counts.add(await member.ingest(ev));
  await flushOutbound(member, gid, (ev) => relay.tryPublish(ev));
  return counts;
}

async function drainContents(member: MemberDriver): Promise<Set<string>> {
  const out = new Set<string>();
  for (const ev of await member.drainEvents()) {
    if (ev.type === "messageReceived" && ev.decoded) out.add(ev.decoded.content);
  }
  return out;
}

export async function runScenario(env: ScenarioEnv): Promise<void> {
  const { relay, assert } = env;
  const publish = (ev: RelayEvent) => relay.tryPublish(ev);
  let lastT = env.time();
  const row = (s: string, d: string) => {
    const now = env.time();
    env.row(s, `${d} (+${(now - lastT).toFixed(0)}ms)`);
    lastT = now;
  };

  // Step 1: three accounts, all publish KeyPackages (kind 30443).
  const a = await env.makeMember("alice");
  const b = await env.makeMember("bob");
  const c = await env.makeMember("carol");
  const aPk = await env.pubKeyOf!(a);
  const bPk = await env.pubKeyOf!(b);
  const cPk = await env.pubKeyOf!(c);

  const aKp = await a.publishKeyPackage();
  const bKp = await b.publishKeyPackage();
  publish(aKp);
  publish(bKp);
  publish(await c.publishKeyPackage());
  assert(aKp.kind === 30443, "KP kind");
  row("1. key packages published (kind 30443)", "A B C");

  // Step 2: A creates the group inviting B; B joins via the gift wrap.
  const created = await a.createGroup("m0-spike-wasm", ["ws://spike-relay.local:7777"], [bKp]);
  const gid = created.groupId;
  await publishResult(a, created.result, publish);
  const bWelcomes = relay.fetchAsc(KIND_GIFT_WRAP, undefined, bPk);
  assert(bWelcomes.length === 1, `B welcome count ${bWelcomes.length}`);
  const bJoin = await b.ingest(bWelcomes[0]);
  assert(bJoin.outcome === "processed", `B join: ${JSON.stringify(bJoin)}`);
  assert((await a.members(gid)).length === 2, "A members=2");
  assert((await b.members(gid)).length === 2, "B members=2");
  assert((await a.epoch(gid)) === (await b.epoch(gid)), "epoch match");
  await a.drainEvents();
  await b.drainEvents();
  row("2. group created, B joined via welcome", `epoch=${await a.epoch(gid)} members=2`);

  // Step 3: 500 application messages, all published before the read.
  const aIds = new Set<string>();
  for (let i = 0; i < MSG_PER_SENDER; i++) {
    const id = await sendChat(a, relay, gid, `a-${String(i).padStart(4, "0")}`, BASE + i);
    assert(id, `A send ${i} produced an event`);
    aIds.add(id!);
  }
  for (let i = 0; i < MSG_PER_SENDER; i++) {
    await sendChat(b, relay, gid, `b-${String(i).padStart(4, "0")}`, BASE + 1000 + i);
  }
  const asc = relay.fetchAsc(KIND_GROUP_MESSAGE);
  assert(asc.length === 500, `relay holds 500 (${asc.length})`);
  row("3. published 500 kind-445 messages", "500");

  // B's persisted snapshot BEFORE ingest — the newest-first reader uses it.
  const bPre = await env.snapshot(b);

  // 3a: a fresh reader on that state, unsorted newest-first — the finding.
  const bNewest = await env.spawnReader("bob", bPre);
  const newestCounts = await ingestAll(
    bNewest,
    relay,
    gid,
    relay.fetchNewest(KIND_GROUP_MESSAGE),
  );
  const newestDelivered = await drainContents(bNewest);
  row(
    "3a. reader on pre-ingest state, NEWEST-FIRST",
    `${newestCounts.line()} delivered=${newestDelivered.size}`,
  );

  // Step 4: true mid-ingest restart — live B ingests 125 of A's messages,
  // restarts from persisted KV, ingests the rest. Every A message exactly once.
  const aEvents = asc.filter((e) => aIds.has(e.id));
  assert(aEvents.length === MSG_PER_SENDER, "A event split");
  const bCounts = new IngestCounts();
  const bSeen = new Set<string>();
  let bDups = 0;
  for (const ev of aEvents.slice(0, HALF)) bCounts.add(await b.ingest(ev));
  await flushOutbound(b, gid, publish);
  for (const t of await drainContents(b)) if (!bSeen.add(t)) bDups++;

  await b.restart();

  for (const ev of aEvents.slice(HALF)) bCounts.add(await b.ingest(ev));
  await flushOutbound(b, gid, publish);
  for (const t of await drainContents(b)) if (!bSeen.add(t)) bDups++;
  assert(bSeen.size === MSG_PER_SENDER, `B delivered ${bSeen.size}/250`);
  assert(bDups === 0, `re-delivered ${bDups}`);
  row(
    "4. mid-ingest restart of B (125 + 125 ascending)",
    `${bCounts.line()} delivered=${bSeen.size} dups=${bDups}`,
  );

  const expectedA = new Set(
    [...Array(MSG_PER_SENDER)].map((_, i) => `a-${String(i).padStart(4, "0")}`),
  );
  let missing = [...expectedA].filter((t) => !bSeen.has(t));
  assert(missing.length === 0, `B missing ${missing.length}`);
  const newestLoss = MSG_PER_SENDER - newestDelivered.size;
  if (newestLoss > 0) {
    console.log(`!! FINDING: newest-first backlog lost ${newestLoss} of A's messages`);
  }

  // 4b: A reads the backlog too (its own are echoes).
  const aCounts = new IngestCounts();
  for (const ev of asc) aCounts.add(await a.ingest(ev));
  const aDelivered = await drainContents(a);
  assert(
    [...Array(MSG_PER_SENDER)].every((_, i) =>
      aDelivered.has(`b-${String(i).padStart(4, "0")}`),
    ),
    "A reads all of B",
  );
  row("4b. A reads the same backlog ascending", `${aCounts.line()} delivered=${aDelivered.size}`);

  // Step 5: A adds C via Welcome; three-way messages readable by all.
  const cKp = relay.fetchKeyPackage(cPk)!;
  const invite = await a.send(gid, { type: "invite", keyPackageEvents: [cKp] });
  await publishResult(a, invite, publish);
  const cWelcomes = relay.fetchAsc(KIND_GIFT_WRAP, undefined, cPk);
  assert(cWelcomes.length > 0, "C welcome");
  const cJoin = await c.ingest(cWelcomes[cWelcomes.length - 1]);
  assert(cJoin.outcome === "processed", `C join: ${JSON.stringify(cJoin)}`);
  await c.drainEvents();

  const hTag = asc[0].tags.find((t) => t[0] === "h")![1];
  const hEvents = relay.fetchAsc(KIND_GROUP_MESSAGE, hTag);
  for (const ev of hEvents) await b.ingest(ev);
  await b.drainEvents();
  await a.drainEvents();

  await sendChat(a, relay, gid, "post-c-a-0001", BASE + 2001);
  await sendChat(b, relay, gid, "post-c-b-0002", BASE + 2002);
  await sendChat(c, relay, gid, "post-c-c-0003", BASE + 2003);
  for (const m of [a, b, c]) await flushOutbound(m, gid, publish);

  const hAll = relay.fetchAsc(KIND_GROUP_MESSAGE, hTag);
  const delivered = await Promise.all(
    [a, b, c].map(async (m) => {
      await ingestAll(m, relay, gid, hAll);
      return drainContents(m);
    }),
  );
  const want = [
    ["post-c-b-0002", "post-c-c-0003"],
    ["post-c-a-0001", "post-c-c-0003"],
    ["post-c-a-0001", "post-c-b-0002"],
  ];
  for (let i = 0; i < 3; i++) {
    const miss = want[i].filter((t) => !delivered[i].has(t));
    assert(miss.length === 0, `member ${i} missing ${miss}`);
  }
  assert((await c.members(gid)).length === 3, "C sees 3 members");
  row("5. A added C; three-way messages readable", `epoch=${await c.epoch(gid)} members=3`);

  // Step 6: A removes B; post-removal message unreadable by B, readable by C.
  const bId = await b.memberId();
  const remove = await a.send(gid, {
    type: "removeMembers",
    memberIds: [bId],
  });
  await publishResult(a, remove, publish);
  await flushOutbound(a, gid, publish);
  for (const m of [a, b, c]) {
    for (const ev of relay.fetchAsc(KIND_GROUP_MESSAGE, hTag)) await m.ingest(ev);
    await flushOutbound(m, gid, publish);
  }
  const bDrained = await b.drainEvents();
  let bMemberCount = 0;
  try {
    bMemberCount = (await b.members(gid)).length;
  } catch {
    bMemberCount = 0;
  }
  const bRemoved = bDrained.length > 0 || bMemberCount === 0;
  assert(bRemoved, "B observes removal");
  assert((await a.members(gid)).length === 2, "A sees 2");
  assert((await c.members(gid)).length === 2, "C sees 2");

  await sendChat(a, relay, gid, "after-removal-0009", BASE + 2009);
  await flushOutbound(a, gid, publish);
  const post = relay.fetchAsc(KIND_GROUP_MESSAGE, hTag);
  for (const ev of post) {
    await b.ingest(ev);
    await c.ingest(ev);
  }
  await flushOutbound(b, gid, publish);
  await flushOutbound(c, gid, publish);
  assert(
    !(await drainContents(b)).has("after-removal-0009"),
    "post-removal readable by B",
  );
  assert(
    (await drainContents(c)).has("after-removal-0009"),
    "post-removal unreadable by C",
  );
  row("6. A removed B; post-removal msg unreadable by B", "members=2");

  // Step 7: publish failure → publish_failed → retry keeps group consistent.
  relay.failNext();
  const failed = await a.send(gid, { type: "selfUpdate" });
  const failRep = await publishResult(a, failed, publish);
  assert(!failRep.confirmed && failRep.pending !== undefined, "publish_failed ran");
  const retry = await a.send(gid, { type: "selfUpdate" });
  const retryRep = await publishResult(a, retry, publish);
  assert(retryRep.confirmed, "retry confirmed");
  const epochAfter = await a.epoch(gid);
  for (const ev of relay.fetchAsc(KIND_GROUP_MESSAGE, hTag)) await c.ingest(ev);
  await flushOutbound(c, gid, publish);
  assert((await c.epoch(gid)) === epochAfter, "C converged to retried epoch");
  row("7. publish_failed + retry keeps group consistent", `epoch=${epochAfter}`);

  row("relay events stored", `${relay.size}`);
}
