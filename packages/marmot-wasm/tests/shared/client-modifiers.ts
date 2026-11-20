/**
 * `client-modifiers` — three-client modifier convergence e2e over the release wasm backend. A
 * (admin), B and C exchange chats, edits, deletions, reactions and an unreact plus one unauthorized
 * edit under lossy relay modes; each client folds what it received plus its own sends into a
 * projection and all three must converge on the scripted final state. B and C restart after the
 * script so a `shuffle` relay replays the backlog out of order — the durable delivered index +
 * holds must still surface every modifier against its target.
 */

import type { NostrEvent, ReceivedMessage, Semantics } from "@qntx/marmot/backend";
import type { DropEvent, RelayPort } from "@qntx/marmot/client";
import { fakeRelay } from "@qntx/marmot/testing";
import type { FakeRelayMode, FakeRelayNetwork } from "@qntx/marmot/testing";

import {
  assertConverged,
  assertPeaks,
  assertQuiet,
  closeAll,
  identity,
  LOOKUP,
  RELAY_1,
  RELAY_2,
  silence,
  sleep,
  startClient,
  waitFor,
  wake,
} from "./client-e2e.ts";
import type { ClientHandle } from "./client-e2e.ts";

export type ModifierReport = {
  ms: number;
  delivered: Record<string, number>;
  /**
   * Wire-level out-of-order deliveries: a surfaced modifier's transport event reached a client
   * before the transport event of the target it modifies — the modifier necessarily rode a hold.
   */
  modifiersBeforeTargets: number;
  /** `unauthorized` drops per client port. */
  unauthorized: Record<string, number>;
  peaks: Record<string, { subs: number; relays: number }>;
};

/** What the scenario feeds a projection: a received message or a scripted own send. */
type Fold = {
  id: string;
  sender: string;
  createdAt: bigint;
  content: string;
  semantics: Semantics;
};

/** One client's folded view of the group's mutable content. */
type View = {
  /** Display rows by inner id: base messages only. */
  rows: Map<string, { author: string; content: string }>;
  /** Author of every seen inner event (needed to authorize later modifiers). */
  authors: Map<string, string>;
  /** Target -> surviving authorized edits, ordered by (createdAt, id) — the spec's latest-wins. */
  edits: Map<string, Array<{ createdAt: bigint; id: string; content: string }>>;
  /** Deletion targets and admin removal targets. */
  deleted: Set<string>;
  /** Reaction id -> (target, sender, emoji), minus unreacted and deleted reactions. */
  reactions: Map<string, { target: string; sender: string; emoji: string }>;
};

function emptyView(): View {
  return {
    rows: new Map(),
    authors: new Map(),
    edits: new Map(),
    deleted: new Set(),
    reactions: new Map(),
  };
}

function applyFold(view: View, fold: Fold): void {
  view.authors.set(fold.id, fold.sender);
  const { semantics } = fold;
  switch (semantics.type) {
    case "edit": {
      if (view.authors.get(semantics.target) !== fold.sender) {
        return;
      }
      const list = view.edits.get(semantics.target) ?? [];
      list.push({ createdAt: fold.createdAt, id: fold.id, content: semantics.content });
      view.edits.set(semantics.target, list);
      return;
    }
    case "deletion":
      for (const target of semantics.targets) {
        if (view.authors.get(target) !== fold.sender) {
          continue;
        }
        view.deleted.add(target);
        for (const [edited, list] of view.edits) {
          view.edits.set(
            edited,
            list.filter((edit) => edit.id !== target),
          );
        }
        view.reactions.delete(target);
      }
      return;

    case "unreact":
      for (const id of semantics.reactions) {
        view.reactions.delete(id);
      }
      return;

    case "removal":
      view.deleted.add(semantics.target);
      view.reactions.delete(semantics.target);
      return;

    case "reaction":
      view.reactions.set(fold.id, {
        target: semantics.target,
        sender: fold.sender,
        emoji: semantics.emoji,
      });
      return;

    default:
      view.rows.set(fold.id, { author: fold.sender, content: fold.content });
  }
}

/** The comparable final state: surviving rows carry the latest surviving authorized edit. */
function project(view: View): string {
  const rows = [...view.rows.entries()]
    .map(([id, row]) => {
      const edits = (view.edits.get(id) ?? [])
        .filter((edit) => !view.deleted.has(edit.id))
        .toSorted((x, y) =>
          x.createdAt === y.createdAt
            ? x.id.localeCompare(y.id)
            : Number(x.createdAt - y.createdAt),
        );
      const content = view.deleted.has(id) ? "<deleted>" : (edits.at(-1)?.content ?? row.content);
      return [id, row.author, content] as const;
    })
    .toSorted(([x], [y]) => x.localeCompare(y));
  const deleted = [...view.deleted].toSorted();
  const reactions = [...view.reactions.entries()]
    .map(([id, reaction]) => [id, reaction.target, reaction.sender, reaction.emoji] as const)
    .toSorted(([x], [y]) => x.localeCompare(y));
  return JSON.stringify({ rows, deleted, reactions });
}

/** A scripted send recorded as a fold the sender applies to its own projection. */
type Op = { handle: string; id: string; createdAt: bigint; content: string; semantics: Semantics };

function record(
  ops: Op[],
  handle: ClientHandle,
  sent: { message: { id: string; createdAt: bigint; content: string } },
  semantics: Semantics,
): void {
  ops.push({
    handle: handle.name,
    id: sent.message.id,
    createdAt: sent.message.createdAt,
    content: sent.message.content,
    semantics,
  });
}

/** A `RelayPort` that records the order kind-445 events are delivered to the client. */
function recordingPort(port: RelayPort, seen: string[]): RelayPort {
  return {
    publish: async (relay, event) => port.publish(relay, event),
    connect: async (relay) => port.connect(relay),
    subscribe: (relay, filters, handlers) =>
      port.subscribe(relay, filters, {
        event: (event: NostrEvent) => {
          if (event.kind === 445) {
            seen.push(event.id);
          }
          handlers.event(event);
        },
        eose: handlers.eose,
        closed: handlers.closed,
      }),
  };
}

function foldOf(handle: ClientHandle, group: string, ops: Op[]): View {
  const view = emptyView();
  for (const entry of handle.received) {
    if (entry.group !== group) {
      continue;
    }
    const message: ReceivedMessage = entry.message;
    applyFold(view, {
      id: message.id,
      sender: message.sender,
      createdAt: message.createdAt,
      content: message.content,
      semantics: message.semantics,
    });
  }
  for (const op of ops) {
    if (op.handle !== handle.name) {
      continue;
    }
    applyFold(view, {
      id: op.id,
      sender: handle.signer.pubkey,
      createdAt: op.createdAt,
      content: op.content,
      semantics: op.semantics,
    });
  }
  return view;
}

/** The modifier/target outer-event-id pairs the script produced (edit, deletion, unreact). */
function modifierPairs(
  ops: Op[],
  outerOf: Map<string, string>,
): Array<{ modifier: string; target: string }> {
  const pairs: Array<{ modifier: string; target: string }> = [];
  for (const op of ops) {
    const modifier = outerOf.get(op.id);
    if (modifier === undefined) {
      continue;
    }
    const { semantics } = op;
    const targets =
      semantics.type === "edit"
        ? [semantics.target]
        : semantics.type === "deletion"
          ? semantics.targets
          : semantics.type === "unreact"
            ? semantics.reactions
            : [];
    for (const target of targets) {
      const targetOuter = outerOf.get(target);
      if (targetOuter !== undefined) {
        pairs.push({ modifier, target: targetOuter });
      }
    }
  }
  return pairs;
}

/**
 * Wire-level out-of-order evidence: `seen` holds one client's kind-445 delivery order; the last
 * index of each id is its position in the most recent delivery (the post-silence replay for a
 * silenced client). A modifier whose event was delivered before its target's event had to ride a
 * hold before it could surface.
 */
function wireOutOfOrder(
  seen: string[],
  pairs: Array<{ modifier: string; target: string }>,
): number {
  const lastIndex = new Map<string, number>();
  for (const [index, id] of seen.entries()) {
    lastIndex.set(id, index);
  }
  let count = 0;
  for (const pair of pairs) {
    const modifierAt = lastIndex.get(pair.modifier);
    const targetAt = lastIndex.get(pair.target);
    if (modifierAt !== undefined && targetAt !== undefined && modifierAt < targetAt) {
      count += 1;
    }
  }
  return count;
}

/** A client's deduped kind-445 publish order — the last `count` are its scripted ops in order. */
function outerIds(network: FakeRelayNetwork, name: string, count: number): string[] {
  const deduped: string[] = [];
  for (const entry of network.published()) {
    if (entry.client === name && entry.event.kind === 445 && !deduped.includes(entry.event.id)) {
      deduped.push(entry.event.id);
    }
  }
  return deduped.slice(-count);
}

export async function scenarioModifiers(
  modes: Array<[string, FakeRelayMode]>,
): Promise<ModifierReport> {
  const start = Date.now();
  const network = fakeRelay(37);
  for (const [relay, mode] of modes) {
    network.mode(relay, mode);
  }
  const seen: Record<string, string[]> = { a: [], b: [], c: [] };
  const drops: Record<string, DropEvent[]> = { a: [], b: [], c: [] };
  // A wrapped network so every (re)opened port records its kind-445 delivery order, whatever
  // port name the lifetime uses.
  const wired = (name: string): FakeRelayNetwork => ({
    ...network,
    port: (client) => recordingPort(network.port(client), seen[name] ?? []),
  });
  const a = await startClient({ name: "a", network: wired("a") });
  const b = await startClient({ name: "b", network: wired("b") });
  const c = await startClient({ name: "c", network: wired("c") });
  const watchDrops = (handle: ClientHandle): void => {
    handle.client.onDropped((event) => {
      drops[handle.name]?.push(event);
    });
  };
  for (const handle of [a, b, c]) {
    watchDrops(handle);
    // oxlint-disable-next-line no-await-in-loop -- identities are sequential in every scenario
    await identity(handle);
  }

  const invitees = [
    ...(await a.client.lookupInvitees(b.signer.pubkey, [LOOKUP])),
    ...(await a.client.lookupInvitees(c.signer.pubkey, [LOOKUP])),
  ];
  const created = await a.client.createGroup({
    name: "modifiers",
    description: "modifier convergence",
    relays: [RELAY_1, RELAY_2],
    invitees,
  });
  const group = created.id;
  await waitFor(
    () =>
      b.invites.some((event) => event.type === "joined" && event.group === group) &&
      c.invites.some((event) => event.type === "joined" && event.group === group),
    "b and c joined",
  );
  await waitFor(async () => {
    const snapshots = await Promise.all(
      [a, b, c].map(async (handle) => handle.client.group(group)),
    );
    return snapshots.every((snapshot) => snapshot !== undefined && snapshot.state === "stable");
  }, "all members stable");

  // The scripted traffic; every op is recorded so the sender can fold its own send. B's own
  // ops must land before it is silenced — a dead port cannot publish.
  const ops: Op[] = [];
  const m2 = await b.client.sendChat(group, "m2 b note");
  record(ops, b, m2, { type: "chat", replyTo: undefined });
  const d1 = await b.client.deleteMessage(group, m2.message.id);
  record(ops, b, d1, { type: "deletion", targets: [m2.message.id] });
  // B's delete must actually reach the relays before its port dies.
  await waitFor(
    () =>
      a.received.some(
        (entry) => entry.group === group && entry.message.semantics.type === "deletion",
      ),
    "a received b's deletion",
  );

  // B goes dark: every op below misses it live and arrives on the reopen replay — under
  // `shuffle` that replay is out of order, exercising held modifiers against their targets.
  silence(b);
  const m1 = await a.client.sendChat(group, "m1 root");
  record(ops, a, m1, { type: "chat", replyTo: undefined });
  const m3 = await c.client.sendChat(group, "m3 c note");
  record(ops, c, m3, { type: "chat", replyTo: undefined });
  const c2 = await c.client.sendChat(group, "c second");
  record(ops, c, c2, { type: "chat", replyTo: undefined });

  // Three A edits of m1; the sleeps keep their created_at distinct so latest-wins is
  // deterministic (equal seconds would fall to the (created_at, id) tie-break).
  const e1 = await a.client.editMessage(group, m1.message.id, "m1 edit one");
  record(ops, a, e1, { type: "edit", target: m1.message.id, content: "m1 edit one" });
  await sleep(1100);
  const e2 = await a.client.editMessage(group, m1.message.id, "m1 edit two");
  record(ops, a, e2, { type: "edit", target: m1.message.id, content: "m1 edit two" });
  await sleep(1100);
  const e3 = await a.client.editMessage(group, m1.message.id, "m1 edit three");
  record(ops, a, e3, { type: "edit", target: m1.message.id, content: "m1 edit three" });

  // Author delete of an edit (A's e3 — m1 reverts to "m1 edit two").
  const d2 = await a.client.deleteMessage(group, e3.message.id);
  record(ops, a, d2, { type: "deletion", targets: [e3.message.id] });

  // Reactions from two senders, then A withdraws its own.
  const r1 = await a.client.react(group, m1.message.id, "👍");
  record(ops, a, r1, { type: "reaction", target: m1.message.id, emoji: "👍" });
  const r2 = await c.client.react(group, m1.message.id, "🔥");
  record(ops, c, r2, { type: "reaction", target: m1.message.id, emoji: "🔥" });
  const u1 = await a.client.unreact(group, [r1.message.id]);
  record(ops, a, u1, { type: "unreact", reactions: [r1.message.id] });

  // C edits A's m1 — unauthorized; every receiver must drop it `unauthorized`.
  const bad = await c.client.editMessage(group, m1.message.id, "c hostile edit");
  record(ops, c, bad, { type: "edit", target: m1.message.id, content: "c hostile edit" });

  // Admin removal of C's m3 feeds the deleted set through `removal`.
  const rm = await a.client.removeMessage(group, m3.message.id);
  record(ops, a, rm, { type: "removal", target: m3.message.id });

  // B reopens over its durable rows: the whole missed backlog replays — shuffled when the relay
  // mode asks for it.
  await wake(b);
  watchDrops(b);

  await assertConverged([a, b, c], group);
  await waitFor(() => {
    const projections = [a, b, c].map((handle) => project(foldOf(handle, group, ops)));
    return projections[0] !== undefined && projections.every((entry) => entry === projections[0]);
  }, "projections converge");

  // The scripted expected state: fold every op in order — the fold rejects the unauthorized
  // edit the same way the engine does.
  const expected = emptyView();
  const senders: Record<string, ClientHandle> = { a, b, c };
  for (const op of ops) {
    const handle = senders[op.handle];
    if (handle === undefined) {
      throw new Error(`op for unknown handle ${op.handle}`);
    }
    applyFold(expected, {
      id: op.id,
      sender: handle.signer.pubkey,
      createdAt: op.createdAt,
      content: op.content,
      semantics: op.semantics,
    });
  }
  const expectedProjection = project(expected);
  for (const handle of [a, b, c]) {
    const got = project(foldOf(handle, group, ops));
    if (got !== expectedProjection) {
      throw new Error(`${handle.name}: projection diverged\n${got}\n${expectedProjection}`);
    }
  }

  const unauthorized: Record<string, number> = {};
  for (const handle of [a, b, c]) {
    unauthorized[handle.portName] = (drops[handle.name] ?? []).filter(
      (event) => event.group === group && event.reason === "unauthorized",
    ).length;
  }
  for (const receiver of [a, b]) {
    if ((unauthorized[receiver.portName] ?? 0) === 0) {
      throw new Error(`${receiver.name}: no unauthorized drop observed for C's edit`);
    }
  }
  // Map each scripted op to its outer kind-445 id: each client's last N deduped publishes are
  // exactly its ops in order (retries republish the same id, which dedup preserves).
  const outerOf = new Map<string, string>();
  for (const handle of [a, b, c]) {
    const mine = ops.filter((op) => op.handle === handle.name);
    const ids = outerIds(network, handle.name, mine.length);
    for (const [index, op] of mine.entries()) {
      const outer = ids[index];
      if (outer !== undefined) {
        outerOf.set(op.id, outer);
      }
    }
  }
  const pairs = modifierPairs(ops, outerOf);
  const modifiersBeforeTargets = [a, b, c].reduce(
    (total, handle) => total + wireOutOfOrder(seen[handle.name] ?? [], pairs),
    0,
  );
  // Under `shuffle`, B's post-silence replay serves the whole backlog out of order — seven
  // modifier/target pairs make at least one inversion near-certain.
  if (modes.some(([, mode]) => mode === "shuffle") && modifiersBeforeTargets === 0) {
    throw new Error("shuffled replay never delivered a modifier before its target");
  }

  assertQuiet([a, b, c], modes.length > 0 ? ["commit_refused"] : []);
  assertPeaks([a, b, c], network);
  const delivered: Record<string, number> = {};
  for (const handle of [a, b, c]) {
    delivered[handle.name] = handle.received.filter((entry) => entry.group === group).length;
  }
  await closeAll([a, b, c], network);
  const peaks: Record<string, { subs: number; relays: number }> = {};
  for (const handle of [a, b, c]) {
    peaks[handle.name] = {
      subs: network.peakSubscriptions(handle.portName),
      relays: network.peakRelays(handle.portName),
    };
  }
  return { ms: Date.now() - start, delivered, modifiersBeforeTargets, unauthorized, peaks };
}
