/**
 * `client-host-inbox` — the host-owned inbox over the release wasm backend, driven by both the Node
 * and Bun runners. `inbox: "host"` opens no kind-1059 subscription; the host unwraps gift wraps
 * itself and forwards Welcome wraps to `ingestWrap`, which peels them through the serialized ingest
 * path and records the engine's own `InputReport`.
 */

import { generateSecretKey, nip59 } from "nostr-tools";

import { SignerRefusedError } from "@qntx/marmot/backend";
import type { InputReport, NostrEvent, Signer } from "@qntx/marmot/backend";
import { decodeClientState } from "@qntx/marmot/client";
import type { RelayFilter, RelayPort } from "@qntx/marmot/client";
import { fakeRelay, memoryCursorStore, memoryKv } from "@qntx/marmot/testing";
import type { FakeRelayNetwork } from "@qntx/marmot/testing";

import { testSigner } from "../signer.ts";
import {
  assertConverged,
  assertQuiet,
  contents,
  identity,
  INBOX,
  LOOKUP,
  RELAY_1,
  RELAY_2,
  send,
  silence,
  sleep,
  startClient,
  wake,
  waitFor,
} from "./client-e2e.ts";
import type { ClientHandle } from "./client-e2e.ts";

const GIFT_WRAP_KIND = 1059;

/** A `RelayPort` recording every subscribe's filters (the no-1059 assertions). */
function recordingPort(
  inner: RelayPort,
  subs: Array<{ relay: string; filters: RelayFilter[] }>,
): RelayPort {
  return {
    publish: async (relay, event) => inner.publish(relay, event),
    connect: async (relay) => inner.connect(relay),
    subscribe: (relay, filters, handlers) => {
      subs.push({ relay, filters });
      return inner.subscribe(relay, filters, handlers);
    },
  };
}

function sawWrapSub(subs: Array<{ relay: string; filters: RelayFilter[] }>): boolean {
  return subs.some((sub) =>
    sub.filters.some((filter) => (filter.kinds ?? []).includes(GIFT_WRAP_KIND)),
  );
}

/** A `Signer` counting `nip44Decrypt` calls; `reset` marks a phase boundary. */
function countingSigner(inner: Signer): {
  signer: Signer;
  state: { decrypts: number; peers: string[] };
} {
  const state = { decrypts: 0, peers: [] as string[] };
  return {
    state,
    signer: {
      pubkey: inner.pubkey,
      signEvent: async (unsigned) => inner.signEvent(unsigned),
      nip44Encrypt: async (peer, plaintext) => inner.nip44Encrypt(peer, plaintext),
      nip44Decrypt: async (peer, ciphertext) => {
        state.decrypts += 1;
        state.peers.push(`${peer.slice(0, 8)}:${ciphertext.length}`);
        return inner.nip44Decrypt(peer, ciphertext);
      },
    },
  };
}

/** A `Signer` refusing every `nip44Decrypt` while `state.refuse` holds. */
function refusingSigner(inner: Signer): { signer: Signer; state: { refuse: boolean } } {
  const state = { refuse: false };
  return {
    state,
    signer: {
      pubkey: inner.pubkey,
      signEvent: async (unsigned) => inner.signEvent(unsigned),
      nip44Encrypt: async (peer, plaintext) => inner.nip44Encrypt(peer, plaintext),
      nip44Decrypt: async (peer, ciphertext) => {
        if (state.refuse) {
          throw new SignerRefusedError();
        }
        return inner.nip44Decrypt(peer, ciphertext);
      },
    },
  };
}

/** A kind-1059 NIP-17 DM (a kind-14 rumor, not a Welcome) gift-wrapped to `recipient`. */
function dmWrap(recipient: string): NostrEvent {
  const sender = generateSecretKey();
  const wire = nip59.wrapEvent(
    {
      kind: 14,
      content: "unrelated dm",
      tags: [["p", recipient]],
      created_at: Math.floor(Date.now() / 1000),
    },
    sender,
    recipient,
  );
  return {
    id: wire.id,
    pubkey: wire.pubkey,
    createdAt: BigInt(wire.created_at),
    kind: wire.kind,
    tags: wire.tags,
    content: wire.content,
    sig: wire.sig,
  };
}

export type HostSetup = {
  a: ClientHandle;
  b: ClientHandle;
  group: string;
  /** The Welcome wrap A published for B — fetched from the relay, not a subscription. */
  wrap: NostrEvent;
  /** Every subscribe B's port saw, newest last. */
  subs: Array<{ relay: string; filters: RelayFilter[] }>;
};

/**
 * One host-owned invitee: a client-mode seed lifetime publishes the KeyPackage + inbox relays and
 * closes; the host copy opens over its durable rows with a fresh cursor store (`inbox: "host"`,
 * `lookupRelays: []` — no background ensure divergence) behind a recording port. A then creates a
 * group inviting it; resolves with the published Welcome wrap once the relay stores it.
 */
async function hostSetup(
  network: FakeRelayNetwork,
  options?: { signer?: Signer },
): Promise<HostSetup> {
  const signer = options?.signer ?? testSigner();
  const seed = await startClient({ name: "b-seed", network, signer });
  await identity(seed);
  const rows = seed.stores.kv.rows();
  await seed.client.close();

  const subs: HostSetup["subs"] = [];
  const b = await startClient({
    name: "b",
    network,
    signer,
    stores: { kv: memoryKv(rows), cursors: memoryCursorStore() },
    port: recordingPort(network.port("b"), subs),
    inbox: "host",
    lookupRelays: [],
  });
  const a = await startClient({ name: "a", network });
  await identity(a);
  const invitees = await a.client.lookupInvitees(signer.pubkey, [LOOKUP]);
  const snapshot = await a.client.createGroup({
    name: "host-inbox",
    description: "",
    relays: [RELAY_1, RELAY_2],
    invitees,
  });
  await waitFor(
    () => network.events(INBOX).some((event) => event.kind === GIFT_WRAP_KIND),
    "welcome wrap stored",
  );
  const wraps = network.welcomesFor(signer.pubkey);
  const [wrap] = wraps;
  if (wraps.length !== 1 || wrap === undefined) {
    throw new Error(`expected one welcome wrap, got ${wraps.length}`);
  }
  return { a, b, group: snapshot.id, wrap, subs };
}

async function requireJoined(handle: ClientHandle, group: string): Promise<void> {
  return waitFor(
    () => handle.invites.some((event) => event.type === "joined" && event.group === group),
    `${handle.name} joined`,
  );
}

/** A `type:group` sequence string, for comparing invite/group-change streams across copies. */
function shape(events: Array<{ type: string; group?: string }>): string {
  return events.map((event) => `${event.type}:${event.group ?? ""}`).join(",");
}

/** `JSON.stringify` with bigints as decimals — snapshots carry `epoch`/`retentionSeconds`. */
function json(value: unknown): string {
  return JSON.stringify(value, (_key, field: unknown) =>
    typeof field === "bigint" ? field.toString() : field,
  );
}

/**
 * Scenario 1 — differential: B's identity is seeded by a client-mode lifetime; copy `b1` opens
 * client-owned over the seeded cursor blob (its persisted inbox relays drive a kind-1059 sub), copy
 * `b2` opens host-owned over a fresh cursor store. `b1` joins from its own subscription, `b2` from
 * `ingestWrap` of the relay's copy — both report the same group snapshot, invite and group-change
 * sequences, and a later message reaches both.
 */
export async function scenarioDifferential(): Promise<{ ms: number }> {
  const start = Date.now();
  const network = fakeRelay(201);
  const signer = testSigner();
  const seed = await startClient({ name: "b-seed", network, signer });
  await identity(seed);
  const rows = seed.stores.kv.rows();
  const cursorBytes = seed.stores.cursors.bytes();
  await seed.client.close();

  const subs1: Array<{ relay: string; filters: RelayFilter[] }> = [];
  const subs2: Array<{ relay: string; filters: RelayFilter[] }> = [];
  const b1 = await startClient({
    name: "b1",
    network,
    signer,
    stores: { kv: memoryKv(rows), cursors: memoryCursorStore(cursorBytes) },
    port: recordingPort(network.port("b1"), subs1),
    inbox: "client",
    lookupRelays: [],
  });
  const b2 = await startClient({
    name: "b2",
    network,
    signer,
    stores: { kv: memoryKv(rows), cursors: memoryCursorStore() },
    port: recordingPort(network.port("b2"), subs2),
    inbox: "host",
    lookupRelays: [],
  });
  const a = await startClient({ name: "a", network });
  await identity(a);
  const invitees = await a.client.lookupInvitees(signer.pubkey, [LOOKUP]);
  const snapshot = await a.client.createGroup({
    name: "host-diff",
    description: "",
    relays: [RELAY_1, RELAY_2],
    invitees,
  });
  const group = snapshot.id;
  await requireJoined(b1, group);
  const wraps = network.welcomesFor(signer.pubkey);
  const [wrap] = wraps;
  if (wraps.length !== 1 || wrap === undefined) {
    throw new Error(`expected one welcome wrap, got ${wraps.length}`);
  }
  const wrapReport = await b2.client.ingestWrap(wrap);
  if (wrapReport.outcome.type !== "accepted") {
    throw new Error(`host ingestWrap outcome ${JSON.stringify(wrapReport.outcome)}`);
  }
  await requireJoined(b2, group);
  await send(a, group, "A>both");
  await waitFor(
    () => contents(b1, group).includes("A>both") && contents(b2, group).includes("A>both"),
    "both copies received",
  );
  await assertConverged([a, b1, b2], group, new Set(["A>both"]));
  // Same account, same device state: the snapshots must be identical, not just converged.
  const [groups1, groups2] = await Promise.all([b1.client.groups(), b2.client.groups()]);
  if (json(groups1) !== json(groups2)) {
    throw new Error(`groups() differ between copies: ${json(groups1)} vs ${json(groups2)}`);
  }
  // Invites and group changes carry the same `{type, group}` sequence on both copies — the only
  // difference is how the wrap arrived, which is invisible to the engine.
  if (shape(b1.invites) !== shape(b2.invites)) {
    throw new Error(`invite sequences differ: ${shape(b1.invites)} vs ${shape(b2.invites)}`);
  }
  if (shape(b1.groupChanges) !== shape(b2.groupChanges)) {
    throw new Error(
      `group-change sequences differ: ${shape(b1.groupChanges)} vs ${shape(b2.groupChanges)}`,
    );
  }
  // The client-owned copy subscribed for gift wraps; the host-owned copy never did.
  if (!sawWrapSub(subs1)) {
    throw new Error("b1 never opened a kind-1059 subscription");
  }
  if (sawWrapSub(subs2)) {
    throw new Error("b2 opened a kind-1059 subscription in host mode");
  }
  assertQuiet([a, b1, b2]);
  const report = { ms: Date.now() - start };
  await b1.client.close();
  await b2.client.close();
  await a.client.close();
  return report;
}

export type RefeedReport = {
  ms: number;
  /** Decrypts `ingestWrap(welcome)` cost — the second NIP-59 peel. */
  welcomeDecrypts: number;
  /** Decrypts the re-fed duplicate cost (0) and the non-Welcome forward (the peel: 2). */
  duplicateDecrypts: number;
  nonWelcomeDecrypts: number;
  /** `JSON.stringify` of the re-feed and non-Welcome outcomes — the documented values. */
  duplicateOutcome: string;
  nonWelcomeOutcome: string;
  welcomePeers: string[];
  wrapPubkey: string;
  aPubkey: string;
};

/**
 * Scenario 2/3 — decrypt accounting on a host-owned copy: while nothing is forwarded the pump
 * spends zero decrypts (it never sees the wraps the relay holds — Welcome or unrelated DM);
 * `ingestWrap` of the Welcome costs exactly two NIP-44 decrypts. A re-feed answers
 * `rejected{duplicate}` with no decrypt and no second join; a forwarded non-Welcome wrap peels (two
 * decrypts), reports `rejected{invalid_encoding}` with no invite and no group change, and is itself
 * dedup-marked — a re-feed of it answers `rejected{duplicate}` at zero decrypts.
 */
export async function scenarioDecryptAndRefeed(): Promise<RefeedReport> {
  const start = Date.now();
  const network = fakeRelay(203);
  const probe = countingSigner(testSigner());
  const { a, b, group, wrap, subs } = await hostSetup(network, { signer: probe.signer });
  // The fresh cursor store ran the open-time NIP-44 probe; reset so only forwarded wraps count.
  probe.state.decrypts = 0;
  probe.state.peers = [];

  // The relay also holds an unrelated NIP-17 DM to B — the host would not forward it, and even
  // sitting on the relay it must cost the pump nothing: no subscription ever delivers it.
  const dm = dmWrap(probe.signer.pubkey);
  await network.port("host").publish(INBOX, dm);
  await sleep(300);
  if (probe.state.decrypts !== 0) {
    throw new Error(`pump spent ${probe.state.decrypts} decrypts without a forwarded wrap`);
  }
  if (sawWrapSub(subs)) {
    throw new Error("b opened a kind-1059 subscription in host mode");
  }

  const welcome = await b.client.ingestWrap(wrap);
  if (welcome.outcome.type !== "accepted") {
    throw new Error(`welcome ingestWrap outcome ${JSON.stringify(welcome.outcome)}`);
  }
  const welcomeDecrypts = probe.state.decrypts;
  await requireJoined(b, group);

  const duplicate = await b.client.ingestWrap(wrap);
  const duplicateDecrypts = probe.state.decrypts - welcomeDecrypts;
  if (duplicateDecrypts !== 0) {
    throw new Error(`re-feed cost ${duplicateDecrypts} decrypts`);
  }
  const nonWelcome = await b.client.ingestWrap(dm);
  const nonWelcomeDecrypts = probe.state.decrypts - welcomeDecrypts - duplicateDecrypts;
  // The rejection is remembered by the dedup ring: a re-feed is `rejected{duplicate}` and costs
  // nothing — the signer never sees it again.
  const nonWelcomeRefeed = await b.client.ingestWrap(dm);
  const nonWelcomeRefeedDecrypts =
    probe.state.decrypts - welcomeDecrypts - duplicateDecrypts - nonWelcomeDecrypts;
  if (nonWelcomeRefeedDecrypts !== 0) {
    throw new Error(`non-Welcome re-feed cost ${nonWelcomeRefeedDecrypts} decrypts`);
  }
  if (
    nonWelcomeRefeed.outcome.type !== "rejected" ||
    nonWelcomeRefeed.outcome.category !== "duplicate"
  ) {
    throw new Error(`non-Welcome re-feed outcome ${JSON.stringify(nonWelcomeRefeed.outcome)}`);
  }
  await sleep(300);
  const groups = await b.client.groups();
  if (groups.length !== 1) {
    throw new Error(`b holds ${groups.length} groups after the non-Welcome forward`);
  }
  if (b.invites.length !== 1 || b.invites[0]?.type !== "joined") {
    throw new Error(`invite events changed: ${JSON.stringify(b.invites)}`);
  }
  assertQuiet([a, b]);
  const report: RefeedReport = {
    ms: Date.now() - start,
    welcomeDecrypts,
    duplicateDecrypts,
    nonWelcomeDecrypts,
    duplicateOutcome: JSON.stringify(duplicate.outcome),
    nonWelcomeOutcome: JSON.stringify(nonWelcome.outcome),
    welcomePeers: probe.state.peers,
    wrapPubkey: wrap.pubkey,
    aPubkey: a.signer.pubkey,
  };
  await a.client.close();
  await b.client.close();
  return report;
}

/**
 * Scenario 4 — crash/restart: `ingestWrap` cannot resolve while its commits are held; a crash (the
 * held commit never reached durable rows) drops the in-flight ingest, so a re-feed after the reopen
 * joins once. Crashing after the resolved ingest keeps the durable dedup marker, so a third feed
 * answers `rejected{duplicate}` and installs no second group. `joined` itself is at-least-once: its
 * durable pending-application-event row replays on the first drain after every reopen, so a
 * post-crash drain legitimately re-emits the same `joined` — what must not appear is a join of a
 * different group.
 */
export async function scenarioCrashRefeed(): Promise<{ ms: number }> {
  const start = Date.now();
  const network = fakeRelay(205);
  const { a, b, group, wrap } = await hostSetup(network);

  b.stores.kv.hold();
  let settled = false;
  const probe = async (): Promise<void> => {
    try {
      await b.client.ingestWrap(wrap);
    } catch {
      settled = true;
      return;
    }
    settled = true;
  };
  void probe();
  await sleep(300);
  if (settled) {
    throw new Error("ingestWrap resolved while its commits were held");
  }
  silence(b);
  await wake(b);

  const first = await b.client.ingestWrap(wrap);
  if (first.outcome.type !== "accepted") {
    throw new Error(`post-crash ingestWrap outcome ${JSON.stringify(first.outcome)}`);
  }
  await requireJoined(b, group);

  silence(b);
  await wake(b);
  const second = await b.client.ingestWrap(wrap);
  if (second.outcome.type !== "rejected" || second.outcome.category !== "duplicate") {
    throw new Error(`re-feed after reopen outcome ${JSON.stringify(second.outcome)}`);
  }
  const groups = await b.client.groups();
  if (groups.length !== 1) {
    throw new Error(`b holds ${groups.length} groups`);
  }
  // The drain after this op replays the durable `joined` (at-least-once) — allowed; a join of a
  // different group is not.
  if (b.invites.some((event) => event.type === "joined" && event.group !== group)) {
    throw new Error(`a joined invite names another group: ${JSON.stringify(b.invites)}`);
  }
  assertQuiet([a, b]);
  const report = { ms: Date.now() - start };
  await a.client.close();
  await b.client.close();
  return report;
}

/**
 * Scenario 5 — signer refused then re-fed: a refusing signer makes the forwarded wrap
 * `unavailable{signer_refused}` with a `pending` invite and no floor or `inbox|` cursor key in the
 * persisted blob (host mode keeps no retry state — re-delivery is the host re-forwarding); a
 * working signer re-feeds the same wrap to `accepted` + `joined`.
 */
export async function scenarioSignerRefused(): Promise<{ ms: number }> {
  const start = Date.now();
  const network = fakeRelay(207);
  const probe = refusingSigner(testSigner());
  const { a, b, group, wrap } = await hostSetup(network, { signer: probe.signer });

  probe.state.refuse = true;
  const first: InputReport = await b.client.ingestWrap(wrap);
  if (first.outcome.type !== "unavailable" || first.outcome.category !== "signer_refused") {
    throw new Error(`refused ingestWrap outcome ${JSON.stringify(first.outcome)}`);
  }
  await waitFor(
    () => b.invites.some((event) => event.type === "pending" && event.reason === "signer_refused"),
    "pending signer_refused invite",
  );
  await waitFor(() => b.stores.cursors.bytes() !== undefined, "cursor blob persisted");
  const state = decodeClientState(b.stores.cursors.bytes());
  if (state === undefined || state.inbox !== "host") {
    throw new Error("persisted state did not decode as a host-owned v4 blob");
  }
  if (Object.keys(state.floors).length > 0) {
    throw new Error(`host mode persisted floors ${JSON.stringify(state.floors)}`);
  }
  if (Object.keys(state.cursors).some((key) => key.startsWith("inbox|"))) {
    throw new Error("host mode persisted an inbox cursor key");
  }
  const refusedGroups = await b.client.groups();
  if (refusedGroups.length > 0) {
    throw new Error("a refused welcome created a group");
  }

  probe.state.refuse = false;
  const second = await b.client.ingestWrap(wrap);
  if (second.outcome.type !== "accepted") {
    throw new Error(`re-fed ingestWrap outcome ${JSON.stringify(second.outcome)}`);
  }
  await requireJoined(b, group);
  assertQuiet([a, b]);
  const report = { ms: Date.now() - start };
  await a.client.close();
  await b.client.close();
  return report;
}
