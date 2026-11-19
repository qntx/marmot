/**
 * `client-app-surface` — three-client typed-application-surface e2e over the release wasm backend.
 * A (admin), B and C converge on the same typed messages, the same `rowId`s and the same snapshots
 * through reply, reaction, admin removal, rename/avatar/retention, promote/demote, C's leave and
 * A's disband — under each lossy relay mode, and across a restart of B between A's send and B's
 * ingest.
 */

import type { GroupChange, GroupSnapshot, ReceivedMessage } from "@qntx/marmot/backend";
import { fakeRelay } from "@qntx/marmot/testing";
import type { FakeRelayMode } from "@qntx/marmot/testing";

import {
  assertConverged,
  assertPeaks,
  assertQuiet,
  closeAll,
  duplicates,
  identity,
  LOOKUP,
  RELAY_1,
  RELAY_2,
  restart,
  startClient,
  waitFor,
} from "./client-e2e.ts";
import type { ClientHandle } from "./client-e2e.ts";

export type AppSurfaceReport = {
  ms: number;
  delivered: Record<string, number>;
  peaks: Record<string, { subs: number; relays: number }>;
};

/** A received message's typed shape, for cross-member comparison. */
function typed(message: ReceivedMessage): string {
  return JSON.stringify({ id: message.id, semantics: message.semantics });
}

/** `group_state_changed` rows seen by a handle, keyed by the change's identity. */
function stateRows(handle: ClientHandle, group: string): Map<string, { rowId: string }> {
  const rows = new Map<string, { rowId: string }>();
  for (const event of handle.groupChanges) {
    if (event.type !== "group_state_changed" || event.group !== group) {
      continue;
    }
    const key = rowKey(event.change);
    if (key !== undefined) {
      rows.set(key, { rowId: event.rowId });
    }
  }
  return rows;
}

function rowKey(change: GroupChange): string | undefined {
  switch (change.type) {
    case "member_added":
    case "member_removed":
    case "member_left":
    case "admin_added":
    case "admin_removed":
      return `${change.type}:${change.account}`;
    case "renamed":
      return `renamed:${change.newName}`;
    case "avatar_changed":
    case "disbanded":
      return change.type;
    case "retention_changed":
      return `retention_changed:${change.newSeconds.toString()}`;
    default:
      return undefined;
  }
}

async function receivedOne(
  handle: ClientHandle,
  group: string,
  id: string,
  what: string,
): Promise<ReceivedMessage> {
  await waitFor(
    () => handle.received.some((entry) => entry.group === group && entry.message.id === id),
    `${handle.name} received ${what}`,
  );
  const found = handle.received.filter((entry) => entry.group === group && entry.message.id === id);
  if (found.length !== 1) {
    throw new Error(`${handle.name}: ${what} delivered ${found.length} times`);
  }
  const [message] = found;
  if (message === undefined) {
    throw new Error(`${handle.name}: ${what} missing`);
  }
  return message.message;
}

/** `group(id)` that must exist; throws otherwise. */
async function snapshotOf(handle: ClientHandle, group: string): Promise<GroupSnapshot> {
  const snapshot = await handle.client.group(group);
  if (snapshot === undefined) {
    throw new Error(`${handle.name}: group ${group} missing`);
  }
  return snapshot;
}

export async function scenarioAppSurface(
  modes: Array<[string, FakeRelayMode]>,
): Promise<AppSurfaceReport> {
  const start = Date.now();
  const network = fakeRelay(31);
  for (const [relay, mode] of modes) {
    network.mode(relay, mode);
  }
  const a = await startClient({ name: "a", network });
  const b = await startClient({ name: "b", network });
  const c = await startClient({ name: "c", network });
  await identity(a);
  await identity(b);
  await identity(c);

  const invitees = [
    ...(await a.client.lookupInvitees(b.signer.pubkey, [LOOKUP])),
    ...(await a.client.lookupInvitees(c.signer.pubkey, [LOOKUP])),
  ];
  const created = await a.client.createGroup({
    name: "surface",
    description: "typed surface",
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
  // The creator's own send is still refused while the group has not settled.
  await waitFor(async () => {
    const snapshots = await Promise.all(
      [a, b, c].map(async (handle) => handle.client.group(group)),
    );
    return snapshots.every((snapshot) => snapshot !== undefined && snapshot.state === "stable");
  }, "all members stable");

  // A's send races B's close+reopen: B ingests `m1` only after the restart, exactly once.
  const sent = a.client.sendChat(group, "m1 hello");
  await restart(b);
  const m1 = await sent;
  await waitFor(
    () => handle_received(b, group).includes("m1 hello"),
    "b received m1 after restart",
  );
  const m1AtB = await receivedOne(b, group, m1.message.id, "m1");
  const m1AtC = await receivedOne(c, group, m1.message.id, "m1");
  for (const message of [m1AtB, m1AtC]) {
    if (message.semantics.type !== "chat" || message.semantics.replyTo !== undefined) {
      throw new Error(`m1 semantics: ${JSON.stringify(message.semantics)}`);
    }
  }

  // Reply: B replies to m1; A and C get chat{replyTo = m1.id}.
  const r1 = await b.client.sendChat(group, "r1 reply", m1.message.id);
  const r1s = await Promise.all(
    [a, c].map(
      async (handle) => [handle, await receivedOne(handle, group, r1.message.id, "r1")] as const,
    ),
  );
  for (const [handle, message] of r1s) {
    if (message.semantics.type !== "chat" || message.semantics.replyTo !== m1.message.id) {
      throw new Error(`${handle.name} r1 semantics: ${JSON.stringify(message.semantics)}`);
    }
  }

  // Reaction: A reacts to m1; B and C get reaction{target, emoji}.
  const reaction = await a.client.react(group, m1.message.id, "👍");
  const reactions = await Promise.all(
    [b, c].map(
      async (handle) =>
        [handle, await receivedOne(handle, group, reaction.message.id, "reaction")] as const,
    ),
  );
  for (const [handle, message] of reactions) {
    if (
      message.semantics.type !== "reaction" ||
      message.semantics.target !== m1.message.id ||
      message.semantics.emoji !== "👍"
    ) {
      throw new Error(`${handle.name} reaction semantics: ${JSON.stringify(message.semantics)}`);
    }
  }

  // Admin removal: A removes B's reply; B and C get removal{target = r1}.
  const removal = await a.client.removeMessage(group, r1.message.id);
  const removals = await Promise.all(
    [b, c].map(
      async (handle) =>
        [handle, await receivedOne(handle, group, removal.message.id, "removal")] as const,
    ),
  );
  for (const [handle, message] of removals) {
    if (message.semantics.type !== "removal" || message.semantics.target !== r1.message.id) {
      throw new Error(`${handle.name} removal semantics: ${JSON.stringify(message.semantics)}`);
    }
  }

  // Metadata: rename, avatar, retention; snapshots converge carrying them. The engine refuses a
  // second app-data update while one is pending, so each lands on B before the next stages.
  await a.client.updateGroupProfile(group, { name: "surfaced" });
  await waitFor(async () => {
    const snap = await b.client.group(group);
    return snap?.name === "surfaced";
  }, "rename lands on b");
  await a.client.setGroupAvatar(group, {
    url: "https://example.com/a.png",
    dim: "64x64",
    thumbhash: undefined,
  });
  await waitFor(async () => {
    const snap = await b.client.group(group);
    return snap?.avatar?.url === "https://example.com/a.png";
  }, "avatar lands on b");
  await a.client.setRetention(group, 60n);
  await waitFor(async () => {
    const snapshots = await Promise.all(
      [a, b, c].map(async (handle) => handle.client.group(group)),
    );
    return snapshots.every(
      (snapshot) =>
        snapshot !== undefined &&
        snapshot.name === "surfaced" &&
        snapshot.avatar?.url === "https://example.com/a.png" &&
        snapshot.retentionSeconds === 60n,
    );
  }, "snapshots carry name, avatar and retention");
  await assertConverged([a, b, c], group);

  // A message under retention arrives with expiresAt = createdAt + 60.
  const expiring = await a.client.sendChat(group, "expiring soon");
  const expiringAtB = await receivedOne(b, group, expiring.message.id, "expiring");
  if (expiringAtB.expiresAt !== expiringAtB.createdAt + 60n) {
    throw new Error(`expiresAt ${String(expiringAtB.expiresAt)} != createdAt+60`);
  }

  // Promote then demote B: rows admin_added/admin_removed, B's flag follows.
  await a.client.setAdmin(group, b.signer.pubkey, true);
  await waitFor(async () => {
    const snap = await snapshotOf(b, group);
    return snap.ownAccountAdmin;
  }, "b ownAccountAdmin true");
  await a.client.setAdmin(group, b.signer.pubkey, false);
  await waitFor(async () => {
    const snap = await snapshotOf(b, group);
    return !snap.ownAccountAdmin;
  }, "b ownAccountAdmin false");

  // C (non-admin) leaves: A and B see member_left subject C; C's snapshot is removed.
  await c.client.leave(group);
  await waitFor(
    () =>
      stateRows(a, group).has(`member_left:${c.signer.pubkey}`) &&
      stateRows(b, group).has(`member_left:${c.signer.pubkey}`),
    "a and b saw member_left c",
  );
  await waitFor(async () => {
    const snap = await snapshotOf(c, group);
    return snap.state === "removed";
  }, "c snapshot removed");

  // A disbands: A and B converge on `disbanded` (snapshot + row, actor = A).
  await a.client.disband(group);
  await waitFor(async () => {
    const sa = await snapshotOf(a, group);
    const sb = await snapshotOf(b, group);
    return sa.state === "disbanded" && sb.state === "disbanded";
  }, "a and b snapshots disbanded");
  const disbandA = stateRows(a, group).get("disbanded");
  const disbandB = stateRows(b, group).get("disbanded");
  if (disbandA === undefined || disbandB === undefined) {
    throw new Error("disbanded row missing on a or b");
  }
  if (disbandA.rowId !== disbandB.rowId) {
    throw new Error(`disbanded rowId diverged: ${disbandA.rowId} vs ${disbandB.rowId}`);
  }

  // Cross-member convergence: B and C received the same typed messages; A received every
  // message it did not author (the sender's own sends do not echo back to its listener).
  const ids = (handle: ClientHandle): Set<string> =>
    new Set(
      handle.received.filter((entry) => entry.group === group).map((entry) => typed(entry.message)),
    );
  const expectedB = new Set([
    typed(m1AtB),
    typed(await receivedOne(b, group, reaction.message.id, "reaction on b")),
    typed(await receivedOne(b, group, removal.message.id, "removal on b")),
    typed(await receivedOne(b, group, expiring.message.id, "expiring on b")),
  ]);
  const expectedC = new Set([
    ...expectedB,
    typed(await receivedOne(c, group, r1.message.id, "r1 on c")),
  ]);
  for (const [label, set, expected] of [
    ["b", ids(b), expectedB],
    ["c", ids(c), expectedC],
  ] as const) {
    const missing = [...expected].filter((id) => !set.has(id));
    if (missing.length > 0) {
      throw new Error(`${label}: missing typed messages ${missing.join(",")}`);
    }
  }
  const aTyped = ids(a);
  const r1AtA = await receivedOne(a, group, r1.message.id, "r1 on a");
  if (!aTyped.has(typed(r1AtA))) {
    throw new Error("a missing b's reply");
  }
  const rowMaps = [a, b, c].map((handle) => stateRows(handle, group));
  const [head] = rowMaps;
  if (head === undefined) {
    throw new Error("no row maps");
  }
  for (const [key, row] of head) {
    for (const [index, map] of rowMaps.entries()) {
      const other = map.get(key);
      if (other !== undefined && other.rowId !== row.rowId) {
        throw new Error(`rowId diverged for ${key}: ${row.rowId} vs ${other.rowId} (${index})`);
      }
    }
  }
  for (const handle of [a, b, c]) {
    const dupes = duplicates(handle, group);
    if (dupes.length > 0) {
      throw new Error(`${handle.name}: duplicate deliveries ${dupes.join(",")}`);
    }
  }
  // Under a lossy relay a refused commit may still resolve `confirmed` — that warning is
  // expected noise, not a defect.
  assertQuiet([a, b, c], modes.length > 0 ? ["commit_refused"] : []);
  assertPeaks([a, b, c], network);
  const delivered: Record<string, number> = {};
  for (const handle of [a, b, c]) {
    delivered[handle.name] = handle.received.filter((entry) => entry.group === group).length;
  }
  await closeAll([a, b, c], network);
  return { ms: Date.now() - start, delivered, peaks: peaksReport([a, b, c], network) };
}

function handle_received(handle: ClientHandle, group: string): string[] {
  return handle.received
    .filter((entry) => entry.group === group)
    .map((entry) => entry.message.content);
}

function peaksReport(
  handles: ClientHandle[],
  network: ReturnType<typeof fakeRelay>,
): Record<string, { subs: number; relays: number }> {
  const peaks: Record<string, { subs: number; relays: number }> = {};
  for (const handle of handles) {
    peaks[handle.name] = {
      subs: network.peakSubscriptions(handle.portName),
      relays: network.peakRelays(handle.portName),
    };
  }
  return peaks;
}
