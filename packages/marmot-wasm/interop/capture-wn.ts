/**
 * White Noise 0.17.0 ground-truth capture — two real `wnd` daemons (A, B) plus our `MarmotClient`
 * (C) on the release wasm backend over one in-process websocket relay (`serveFakeRelay`). The
 * script drives a fixed sequence of `wn` commands (identity, group create/accept, messages,
 * replies, reactions, edits, deletes, metadata/admin changes, disband, leave, remove) and C-side
 * raw inner app events via `client.sendMessage`, recording per step: the command, wn's JSON reply,
 * every inner app event C received during the step (group/id/sender/kind/tags/content/created_at/
 * epoch), every C group-change/invite event, and wn's own views (`groups show|admins|members`,
 * `messages list`, `messages timeline list`). A failed step is recorded and the run continues.
 *
 * Output: `vectors/wn/capture-0.17.0.json` — an ordered list of step records, sanitized (relay URL
 * becomes `ws://relay.invalid`, home paths become `<wn-home>`, no nsecs). Full run log:
 * `<repo>/target/m5a-capture.log`. Needs local wn/wnd 0.17.0 binaries (WN_BIN/WND_BIN, default
 * `<repo>/spike/target/wn/bin/{wn,wnd}`); not part of `test:wasm` or CI.
 */

import { spawn, spawnSync } from "node:child_process";
import type { ChildProcess } from "node:child_process";
import {
  appendFileSync,
  closeSync,
  existsSync,
  mkdirSync,
  mkdtempSync,
  openSync,
  readFileSync,
  rmSync,
  writeFileSync,
} from "node:fs";
import { join, resolve } from "node:path";

import { finalizeEvent, generateSecretKey } from "nostr-tools";

import type { AppMessage, Backend } from "@qntx/marmot/backend";
import { MarmotClient, nostrRelayPort } from "@qntx/marmot/client";
import type {
  ClientMessage,
  GroupEvent,
  InviteEvent,
  MarmotError,
  Timer,
} from "@qntx/marmot/client";
import { memoryCursorStore, memoryKv } from "@qntx/marmot/testing";
import { Pool } from "@qntx/nostr";
import { decode, npubEncode } from "@qntx/nostr/nips/nip19";
import { serveFakeRelay } from "@qntx/nostr/testing";

import { releaseBackend } from "../tests/shared/env.ts";
import { testSigner } from "../tests/signer.ts";

const REPO_ROOT = resolve(import.meta.dirname, "../../..");
const WN_BIN = process.env["WN_BIN"] ?? join(REPO_ROOT, "spike/target/wn/bin/wn");
const WND_BIN = process.env["WND_BIN"] ?? join(REPO_ROOT, "spike/target/wn/bin/wnd");
const LOG_PATH = join(REPO_ROOT, "target", "m5a-capture.log");
const FIXTURE_PATH = join(REPO_ROOT, "vectors", "wn", "capture-0.17.0.json");

const WND_START_DEADLINE_MS = 60_000;
const STEP_DEADLINE_MS = 90_000;
const STEP_POLL_MS = 250;
/** Passive observation window for steps where C may or may not see anything. */
const OBSERVE_MS = 12_000;
/** Short window for pure wn queries (no group traffic expected). */
const QUERY_MS = 400;
const RELAY_LIST_KIND = 10002;
const KEY_PACKAGE_KIND = 30443;
const INBOX_RELAYS_KIND = 10050;
const WN_MESSAGE_LIMIT = 600;
const SANITIZED_RELAY = "ws://relay.invalid";

const startMs = Date.now();

function log(line: string): void {
  const stamped = `[${new Date().toISOString().slice(11, 23)}] ${line}`;
  appendFileSync(LOG_PATH, `${stamped}\n`);
  // oxlint-disable-next-line no-console -- the script is a CLI; stdout is the live log
  console.log(stamped);
}

function secs(since: number): string {
  return `${((Date.now() - since) / 1000).toFixed(1)}s`;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

/** Minimal JSON-pointer walk (`/result/group_id` style) for the `wn --json` payloads. */
function ptr(value: unknown, path: string): unknown {
  let cur: unknown = value;
  for (const seg of path.split("/").filter((part) => part.length > 0)) {
    if (Array.isArray(cur)) {
      cur = cur[Number(seg)];
    } else if (isRecord(cur)) {
      cur = cur[seg];
    } else {
      return undefined;
    }
  }
  return cur;
}

function ptrStr(value: unknown, path: string): string | undefined {
  const found = ptr(value, path);
  return typeof found === "string" ? found : undefined;
}

async function sleep(ms: number): Promise<void> {
  return new Promise<void>((resolve) => {
    setTimeout(resolve, ms);
  });
}

/** Bounded polling: the probe reports a miss with `undefined`. */
async function waitFor<T>(
  probe: () => Promise<T | undefined> | T | undefined,
  what: string,
  deadlineMs: number = STEP_DEADLINE_MS,
): Promise<T> {
  const deadline = Date.now() + deadlineMs;
  for (;;) {
    // oxlint-disable-next-line no-await-in-loop -- bounded polling by design
    const hit = await probe();
    if (hit !== undefined) {
      return hit;
    }
    if (Date.now() >= deadline) {
      throw new Error(`timed out waiting for ${what}`);
    }
    // oxlint-disable-next-line no-await-in-loop -- bounded polling by design
    await sleep(STEP_POLL_MS);
  }
}

/** Deep JSON conversion: bigints become numbers, keeping the fixture serializable. */
function jsonable(value: unknown): unknown {
  if (typeof value === "bigint") {
    return Number(value);
  }
  if (Array.isArray(value)) {
    return value.map(jsonable);
  }
  if (isRecord(value)) {
    const out: Record<string, unknown> = {};
    for (const [key, entry] of Object.entries(value)) {
      out[key] = jsonable(entry);
    }
    return out;
  }
  return value;
}

let relayUrl = "";

/** Scrub the relay URL/port, homes and secrets out of anything headed for the fixture. */
function sanitize(value: unknown): unknown {
  if (typeof value === "string") {
    let out = value;
    if (relayUrl.length > 0) {
      out = out.split(relayUrl).join(SANITIZED_RELAY);
      out = out.split(`${relayUrl}/`).join(SANITIZED_RELAY);
    }
    out = out.replaceAll(/wss?:\/\/127\.0\.0\.1:\d+\/?/g, SANITIZED_RELAY);
    out = out.replaceAll(/wss?:\/\/localhost:\d+\/?/g, SANITIZED_RELAY);
    out = out.replaceAll(/127\.0\.0\.1:\d+/g, "relay.invalid");
    out = out.replaceAll(/localhost:\d+/g, "relay.invalid");
    out = out.split(REPO_ROOT).join("<repo>");
    out = out.replaceAll(/wn-[ab]-[A-Za-z0-9]+/g, "<wn-home>");
    out = out.replaceAll(/nsec1[023456789acdefghjklmnpqrstuvwxyz]+/g, "<nsec>");
    return out;
  }
  if (Array.isArray(value)) {
    return value.map(sanitize);
  }
  if (isRecord(value)) {
    const out: Record<string, unknown> = {};
    for (const [key, entry] of Object.entries(value)) {
      out[key] = sanitize(entry);
    }
    return out;
  }
  return value;
}

/** `wn` daemon rig: short-path home (macOS ~104-char socket limit) plus the log directory. */
type Rig = {
  name: string;
  home: string;
  socket: string;
  logsDir: string;
  wnd: ChildProcess;
  outFd: number;
  errFd: number;
  npub: string;
  pub: string;
};

function wnEnv(home: string): NodeJS.ProcessEnv {
  return {
    ...process.env,
    WN_HOME: home,
    WN_SECRET_STORE: "file",
    WN_ALLOW_LOOPBACK_RELAYS: "1",
    WN_ALLOW_LOOPBACK_BLOB_ENDPOINTS: "1",
  };
}

/** `wn` invocation tolerating refusals: `ok` is the JSON envelope's `ok` flag. */
async function wnTry(
  rig: { home: string; socket: string },
  args: string[],
  account?: string,
): Promise<{ ok: boolean; parsed: unknown; raw: string }> {
  const argv = ["--json", "--home", rig.home, "--socket", rig.socket];
  if (account !== undefined) {
    argv.push("--account", account);
  }
  argv.push(...args);
  const out = await new Promise<{ stdout: string; stderr: string }>((resolve, reject) => {
    const child = spawn(WN_BIN, argv, { env: wnEnv(rig.home) });
    const chunks = { stdout: "", stderr: "" };
    child.stdout.setEncoding("utf8").on("data", (data: string) => {
      chunks.stdout += data;
    });
    child.stderr.setEncoding("utf8").on("data", (data: string) => {
      chunks.stderr += data;
    });
    child.on("error", reject);
    child.on("close", () => {
      resolve(chunks);
    });
  });
  const text = out.stdout.trim().length === 0 ? out.stderr : out.stdout;
  let parsed: unknown;
  try {
    parsed = JSON.parse(text);
  } catch {
    parsed = null;
  }
  return { ok: isRecord(parsed) && parsed["ok"] === true, parsed, raw: text };
}

/** `wn` invocation that throws on refusal. */
async function wn(
  rig: { home: string; socket: string },
  args: string[],
  account?: string,
): Promise<unknown> {
  const { ok, parsed, raw } = await wnTry(rig, args, account);
  if (!ok) {
    throw new Error(`wn ${args.join(" ")} refused: ${raw}`);
  }
  return parsed;
}

/** `wn` invocation for the fixture: never throws, records the raw envelope (or stderr text). */
async function wnCap(
  rig: { home: string; socket: string },
  args: string[],
  account?: string,
): Promise<unknown> {
  const { ok, parsed, raw } = await wnTry(rig, args, account);
  if (parsed === null) {
    return { ok, raw };
  }
  return parsed;
}

/** Wn view helper: unwrap `/result` when present so the fixture stays compact. */
async function wnView(rig: Rig, args: string[]): Promise<unknown> {
  const reply = await wnCap(rig, args, rig.npub);
  const result = ptr(reply, "/result");
  return result ?? reply;
}

function wnGroupId(created: unknown): string {
  const gid = ptrStr(created, "/result/group_id") ?? ptrStr(created, "/result/id");
  if (gid === undefined) {
    throw new Error(`wn group create carried no group_id: ${JSON.stringify(created)}`);
  }
  return gid;
}

function startRig(name: string, url: string): Rig {
  const home = mkdtempSync(join(REPO_ROOT, "target", `wn-${name}-`));
  const socket = join(home, "w.sock");
  const logsDir = join(REPO_ROOT, "target", "wn-logs", `capture-${process.pid}-${name}`);
  mkdirSync(logsDir, { recursive: true });
  const outFd = openSync(join(logsDir, "wnd.out.log"), "w");
  const errFd = openSync(join(logsDir, "wnd.err.log"), "w");
  const wnd = spawn(
    WND_BIN,
    [
      "--home",
      home,
      "--socket",
      socket,
      "--logs-dir",
      logsDir,
      "--discovery-relays",
      url,
      "--default-account-relays",
      url,
    ],
    { env: wnEnv(home), stdio: ["ignore", outFd, errFd] },
  );
  wnd.on("error", (error) => {
    log(`wnd-${name} spawn error: ${error.message}`);
  });
  log(`wnd-${name} pid ${wnd.pid ?? -1} home ${home} logs ${logsDir}`);
  return { name, home, socket, logsDir, wnd, outFd, errFd, npub: "", pub: "" };
}

type ObservedMessage = {
  group: string;
  id: string;
  sender: string;
  kind: number;
  tags: string[][];
  content: string;
  created_at: number;
  epoch: number;
};

type StepRecord = {
  step: string;
  actor: string;
  command: unknown;
  wn_reply?: unknown;
  error?: string;
  wait?: string;
  observed: {
    messages: ObservedMessage[];
    group_changes: unknown[];
    invites: unknown[];
  };
  wn_views?: Record<string, unknown>;
  elapsed_ms: number;
};

const received: ClientMessage[] = [];
const invites: InviteEvent[] = [];
const groupChanges: GroupEvent[] = [];
const clientErrors: MarmotError[] = [];
const steps: StepRecord[] = [];

type Mark = { m: number; g: number; i: number };

/** `command` accepts a lazy thunk so ids resolved mid-run serialize correctly. */
function isThunk(value: unknown): value is () => unknown {
  return typeof value === "function";
}

function mapMessage(entry: ClientMessage): ObservedMessage {
  const { message } = entry;
  return {
    group: entry.group,
    id: message.id,
    sender: message.sender,
    kind: Number(message.kind),
    tags: message.tags,
    content: message.content,
    created_at: Number(message.createdAt),
    epoch: Number(message.epoch),
  };
}

/** Probe factory: any new C-side event since `mark`. */
function anyEvent(mark: Mark): boolean | undefined {
  return (
    received.length > mark.m || groupChanges.length > mark.g || invites.length > mark.i || undefined
  );
}

function newMessage(mark: Mark, content: string): boolean | undefined {
  return received.slice(mark.m).some((entry) => entry.message.content === content) || undefined;
}

function newGroupEvent(mark: Mark, group: string): boolean | undefined {
  return groupChanges.slice(mark.g).some((event) => event.group === group) || undefined;
}

/**
 * Run one capture step: snapshot C's event buffers, run the action (recording the wn reply or the
 * thrown error), wait for the step's observation probe (90 s) or a passive window, then collect
 * everything C observed plus optional wn views. A failing step lands in `steps` regardless.
 */
async function step(opts: {
  name: string;
  actor: string;
  command: unknown;
  run?: () => Promise<unknown>;
  expect?: (mark: Mark) => unknown;
  observeMs?: number;
  views?: () => Promise<Record<string, unknown>>;
}): Promise<void> {
  const start = Date.now();
  const mark: Mark = { m: received.length, g: groupChanges.length, i: invites.length };
  const rec: StepRecord = {
    step: opts.name,
    actor: opts.actor,
    command: isThunk(opts.command) ? opts.command() : opts.command,
    observed: { messages: [], group_changes: [], invites: [] },
    elapsed_ms: 0,
  };
  if (opts.run !== undefined) {
    try {
      rec.wn_reply = await opts.run();
    } catch (error) {
      rec.error = error instanceof Error ? error.message : String(error);
    }
  }
  if (opts.expect === undefined) {
    const deadline = Date.now() + (opts.observeMs ?? OBSERVE_MS);
    while (Date.now() < deadline && anyEvent(mark) === undefined) {
      // oxlint-disable-next-line no-await-in-loop -- bounded observation window by design
      await sleep(STEP_POLL_MS);
    }
  } else {
    try {
      await waitFor(() => opts.expect?.(mark), `${opts.name} observation`);
    } catch (error) {
      rec.wait = error instanceof Error ? error.message : String(error);
    }
  }
  if (opts.views !== undefined) {
    try {
      rec.wn_views = await opts.views();
    } catch (error) {
      rec.error = `${rec.error ?? ""} views: ${error instanceof Error ? error.message : String(error)}`;
    }
  }
  rec.observed.messages = received.slice(mark.m).map(mapMessage);
  rec.observed.group_changes = groupChanges.slice(mark.g).map((event) => jsonable(event));
  rec.observed.invites = invites.slice(mark.i).map((event) => jsonable(event));
  rec.elapsed_ms = Date.now() - start;
  steps.push(rec);
  const note = rec.error ?? rec.wait ?? "ok";
  log(
    `STEP ${opts.name}: ${rec.observed.messages.length} msg, ` +
      `${rec.observed.group_changes.length} change, ${rec.observed.invites.length} invite ` +
      `(${secs(start)}) ${note}`,
  );
}

/** Shared `groups admins|members|show` views from one rig. */
function rosterViews(
  rig: Rig,
  gid: string | (() => string),
): () => Promise<Record<string, unknown>> {
  return async () => {
    const id = typeof gid === "function" ? gid() : gid;
    return {
      admins: await wnView(rig, ["groups", "admins", id]),
      members: await wnView(rig, ["groups", "members", id]),
      show: await wnView(rig, ["groups", "show", id]),
    };
  };
}

/** Wn `messages list` + `timeline list` views from one rig. */
async function messageViews(rig: Rig, gid: string): Promise<Record<string, unknown>> {
  return {
    messages_list: await wnView(rig, [
      "messages",
      "list",
      gid,
      "--limit",
      String(WN_MESSAGE_LIMIT),
    ]),
    timeline_list: await wnView(rig, ["messages", "timeline", "list", gid]),
  };
}

async function run(): Promise<void> {
  mkdirSync(join(REPO_ROOT, "target"), { recursive: true });
  mkdirSync(join(REPO_ROOT, "vectors", "wn"), { recursive: true });
  writeFileSync(LOG_PATH, "");
  log("m5a capture: White Noise 0.17.0 ground truth (wn/wnd A+B vs MarmotClient C)");

  const version = spawnSync(WN_BIN, ["--version"], { encoding: "utf8" });
  const versionText = `${version.stdout ?? ""}${version.stderr ?? ""}`.trim();
  log(`wn --version: ${versionText}`);
  if (!versionText.includes("wn 0.17.0")) {
    throw new Error(`expected "wn 0.17.0", got: ${versionText || "(no output)"}`);
  }
  const upstream: unknown = JSON.parse(readFileSync(join(REPO_ROOT, "upstream.json"), "utf8"));
  log(`upstream.json mdk tag: ${JSON.stringify(ptr(upstream, "/mdk/tag"))}`);

  const served = await serveFakeRelay({ host: "127.0.0.1", port: 0 });
  const url = served.url.replace(/\/$/, "");
  relayUrl = url;
  log(`relay: ${served.url}`);

  const rigA = startRig("a", url);
  const rigB = startRig("b", url);

  let poolRef: Pool | undefined;
  let clientRef: MarmotClient | undefined;
  const state = { gid1: "", gid2: "", m1: "", m2: "", r1: "", cMsg: "", cReact: "", bMsg2: "" };
  try {
    for (const rig of [rigA, rigB]) {
      // oxlint-disable-next-line no-await-in-loop -- daemons share the one relay; serialize startup
      await waitFor(
        async () => {
          if (!existsSync(rig.socket)) {
            return undefined;
          }
          const probe = await wnTry(rig, ["whoami"]);
          return probe.ok || undefined;
        },
        `wnd-${rig.name} socket + whoami`,
        WND_START_DEADLINE_MS,
      );
      log(`wnd-${rig.name} up`);
    }

    // Step 1a/1b — A and B identities + key packages.
    for (const rig of [rigA, rigB]) {
      const label = rig.name.toUpperCase();
      // oxlint-disable-next-line no-await-in-loop -- identity steps are sequential by design
      await step({
        name: `${rig.name}.create-identity`,
        actor: label,
        command: { argv: ["create-identity"] },
        run: async () => {
          const created = await wn(rig, ["create-identity"]);
          const npub = ptrStr(created, "/result/npub");
          if (npub === undefined) {
            throw new Error(`create-identity carried no npub: ${JSON.stringify(created)}`);
          }
          rig.npub = npub;
          const decoded = decode(npub);
          if (decoded.type !== "npub") {
            throw new Error(`npub did not decode to a pubkey: ${npub}`);
          }
          rig.pub = decoded.data;
          return created;
        },
        observeMs: QUERY_MS,
      });
      // oxlint-disable-next-line no-await-in-loop -- identity steps are sequential by design
      await step({
        name: `${rig.name}.keys-publish`,
        actor: label,
        command: () => ({ account: rig.npub, argv: ["keys", "publish"] }),
        run: async () => wn(rig, ["keys", "publish"], rig.npub),
        observeMs: QUERY_MS,
      });
    }
    if (rigA.npub.length === 0 || rigB.npub.length === 0) {
      throw new Error("identity setup failed; aborting capture");
    }
    log(`A npub ${rigA.npub}`);
    log(`B npub ${rigB.npub}`);

    // Step 1c — C publishes 10002 + KeyPackage + inbox relays (same shape as wn.ts).
    const secret = generateSecretKey();
    const signer = testSigner(secret);
    const ourNpub = npubEncode(signer.pubkey);
    log(`C npub ${ourNpub}`);

    const startMonotonic = Date.now();
    const backend: Backend = await releaseBackend();
    const timer: Timer = {
      set: (ms, runTimer) => {
        const id = setTimeout(runTimer, ms);
        return () => {
          clearTimeout(id);
        };
      },
    };
    poolRef = new Pool({
      websocketImplementation: WebSocket,
      trustedInsecureOrigins: [url],
    });
    const pool = poolRef;
    const client = await MarmotClient.open({
      backend,
      kv: memoryKv(),
      cursors: memoryCursorStore(),
      signer,
      clock: {
        now: () => ({
          unixMs: BigInt(Date.now()),
          monotonicMs: BigInt(Math.max(0, Date.now() - startMonotonic)),
        }),
      },
      timer,
      relays: nostrRelayPort(pool),
      allowLoopback: true,
    });
    clientRef = client;
    client.onMessages((batch) => {
      received.push(...batch);
    });
    client.onInvite((event) => {
      invites.push(event);
    });
    client.onGroupChange((event) => {
      groupChanges.push(event);
    });
    client.onError((error) => {
      clientErrors.push(error);
      log(`client onError ${error.kind}: ${error.message}`);
    });

    await step({
      name: "c.publish-identity",
      actor: "C",
      command: "finalizeEvent(10002) + client.publishKeyPackage + client.publishInboxRelays",
      run: async () => {
        const relayList = finalizeEvent(
          {
            kind: RELAY_LIST_KIND,
            created_at: Math.floor(Date.now() / 1000),
            tags: [["r", url]],
            content: "",
          },
          secret,
        );
        await pool.publish([url], relayList);
        await client.publishKeyPackage([url]);
        await client.publishInboxRelays([url]);
        return { npub: ourNpub, published: [RELAY_LIST_KIND, KEY_PACKAGE_KIND, INBOX_RELAYS_KIND] };
      },
      expect: () =>
        (served.relay
          .events()
          .some((stored) => stored.kind === KEY_PACKAGE_KIND && stored.pubkey === signer.pubkey) &&
          served.relay
            .events()
            .some(
              (stored) => stored.kind === INBOX_RELAYS_KIND && stored.pubkey === signer.pubkey,
            )) ||
        undefined,
    });
    log("C identity published (10002 + KeyPackage + inbox relays)");

    // Step 2 — A creates gt-1 with B and C; B accepts; C joins via onInvite.
    await step({
      name: "a.groups-create-gt-1",
      actor: "A",
      command: () => ({
        account: rigA.npub,
        argv: ["groups", "create", "gt-1", rigB.npub, ourNpub],
      }),
      run: async () => {
        const created = await wn(rigA, ["groups", "create", "gt-1", rigB.npub, ourNpub], rigA.npub);
        state.gid1 = wnGroupId(created);
        return created;
      },
      expect: (mark) =>
        invites
          .slice(mark.i)
          .some((event) => event.type === "joined" && event.group === state.gid1) || undefined,
      views: rosterViews(rigA, () => state.gid1),
    });
    log(`gt-1 group id ${state.gid1}`);

    await step({
      name: "b.groups-accept-gt-1",
      actor: "B",
      command: () => ({ account: rigB.npub, argv: ["groups", "accept", state.gid1] }),
      run: async () => wn(rigB, ["groups", "accept", state.gid1], rigB.npub),
      expect: async () => {
        const roster = await client.group(state.gid1);
        if (roster === undefined) {
          return undefined;
        }
        return roster.members.some((member) => member.account === rigB.pub) || undefined;
      },
      views: async () => ({
        a: await rosterViews(rigA, state.gid1)(),
        b_invites_after: await wnView(rigB, ["groups", "invites"]),
      }),
    });

    // Step 3 — A sends m1 and m2.
    await step({
      name: "a.send-m1",
      actor: "A",
      command: () => ({ account: rigA.npub, argv: ["messages", "send", state.gid1, "m1 hello"] }),
      run: async () => wn(rigA, ["messages", "send", state.gid1, "m1 hello"], rigA.npub),
      expect: (mark) => newMessage(mark, "m1 hello"),
    });
    state.m1 = received.find((entry) => entry.message.content === "m1 hello")?.message.id ?? "";
    log(`m1 inner id ${state.m1}`);

    await step({
      name: "a.send-m2",
      actor: "A",
      command: () => ({ account: rigA.npub, argv: ["messages", "send", state.gid1, "m2 second"] }),
      run: async () => wn(rigA, ["messages", "send", state.gid1, "m2 second"], rigA.npub),
      expect: (mark) => newMessage(mark, "m2 second"),
    });
    state.m2 = received.find((entry) => entry.message.content === "m2 second")?.message.id ?? "";
    log(`m2 inner id ${state.m2}`);

    // Step 4 — B replies to m1.
    await step({
      name: "b.reply-r1-to-m1",
      actor: "B",
      command: () => ({
        account: rigB.npub,
        argv: ["messages", "send", "--group", state.gid1, "--reply-to", state.m1, "r1 reply"],
      }),
      run: async () =>
        wn(
          rigB,
          ["messages", "send", "--group", state.gid1, "--reply-to", state.m1, "r1 reply"],
          rigB.npub,
        ),
      expect: (mark) => newMessage(mark, "r1 reply"),
    });
    state.r1 = received.find((entry) => entry.message.content === "r1 reply")?.message.id ?? "";
    log(`r1 inner id ${state.r1}`);

    // Step 5 — B reacts 👍, 🔥, 👍 again, then unreacts.
    await step({
      name: "b.react-thumbsup",
      actor: "B",
      command: () => ({
        account: rigB.npub,
        argv: ["messages", "react", state.gid1, state.m1, "👍"],
      }),
      run: async () => wn(rigB, ["messages", "react", state.gid1, state.m1, "👍"], rigB.npub),
      expect: (mark) => anyEvent(mark),
    });
    await step({
      name: "b.react-fire",
      actor: "B",
      command: () => ({
        account: rigB.npub,
        argv: ["messages", "react", state.gid1, state.m1, "🔥"],
      }),
      run: async () => wn(rigB, ["messages", "react", state.gid1, state.m1, "🔥"], rigB.npub),
      expect: (mark) => anyEvent(mark),
    });
    await step({
      name: "b.react-thumbsup-again",
      actor: "B",
      command: () => ({
        account: rigB.npub,
        argv: ["messages", "react", state.gid1, state.m1, "👍"],
      }),
      run: async () => wn(rigB, ["messages", "react", state.gid1, state.m1, "👍"], rigB.npub),
      expect: (mark) => anyEvent(mark),
    });
    await step({
      name: "b.unreact-m1",
      actor: "B",
      command: () => ({ account: rigB.npub, argv: ["messages", "unreact", state.gid1, state.m1] }),
      run: async () => wn(rigB, ["messages", "unreact", state.gid1, state.m1], rigB.npub),
      expect: (mark) => anyEvent(mark),
    });

    // Step 6 — A edits m1 twice, deletes m2.
    await step({
      name: "a.edit-m1-first",
      actor: "A",
      command: () => ({
        account: rigA.npub,
        argv: ["messages", "edit", state.gid1, state.m1, "m1 edited"],
      }),
      run: async () => wn(rigA, ["messages", "edit", state.gid1, state.m1, "m1 edited"], rigA.npub),
      expect: (mark) => anyEvent(mark),
    });
    await step({
      name: "a.edit-m1-second",
      actor: "A",
      command: () => ({
        account: rigA.npub,
        argv: ["messages", "edit", state.gid1, state.m1, "m1 edited twice"],
      }),
      run: async () =>
        wn(rigA, ["messages", "edit", state.gid1, state.m1, "m1 edited twice"], rigA.npub),
      expect: (mark) => anyEvent(mark),
    });
    await step({
      name: "a.delete-m2",
      actor: "A",
      command: () => ({
        account: rigA.npub,
        argv: ["messages", "delete", state.gid1, state.m2],
      }),
      run: async () => wn(rigA, ["messages", "delete", state.gid1, state.m2], rigA.npub),
      expect: (mark) => anyEvent(mark),
    });

    // Step 7 — B tries to edit and delete A's m1 (record refusals / what C sees).
    await step({
      name: "b.edit-m1-unauthorized",
      actor: "B",
      command: () => ({
        account: rigB.npub,
        argv: ["messages", "edit", state.gid1, state.m1, "B edits A"],
      }),
      run: async () =>
        wnCap(rigB, ["messages", "edit", state.gid1, state.m1, "B edits A"], rigB.npub),
      observeMs: OBSERVE_MS,
    });
    await step({
      name: "b.delete-m1-unauthorized",
      actor: "B",
      command: () => ({
        account: rigB.npub,
        argv: ["messages", "delete", state.gid1, state.m1],
      }),
      run: async () => wnCap(rigB, ["messages", "delete", state.gid1, state.m1], rigB.npub),
      observeMs: OBSERVE_MS,
    });

    // Step 8 — group metadata + admin changes; admins/members/show after each.
    const metaSteps: Array<{ name: string; argv: () => string[] }> = [
      {
        name: "a.groups-update",
        argv: () => ["groups", "update", "--name", "gt-1b", "--description", "desc 1", state.gid1],
      },
      {
        name: "a.set-avatar-url",
        argv: () => [
          "groups",
          "set-avatar-url",
          state.gid1,
          "--url",
          "https://example.com/a.png",
          "--dim",
          "64x64",
        ],
      },
      {
        name: "a.clear-avatar-url",
        argv: () => ["groups", "set-avatar-url", state.gid1, "--clear"],
      },
      {
        name: "a.retention-show",
        argv: () => ["groups", "retention", state.gid1],
      },
      {
        name: "a.retention-set-3600",
        argv: () => ["groups", "retention", state.gid1, "--set", "3600"],
      },
      {
        name: "a.promote-c",
        argv: () => ["groups", "promote", state.gid1, ourNpub],
      },
      {
        name: "a.demote-c",
        argv: () => ["groups", "demote", state.gid1, ourNpub],
      },
      {
        name: "a.promote-b",
        argv: () => ["groups", "promote", state.gid1, rigB.npub],
      },
      {
        name: "a.self-demote",
        argv: () => ["groups", "self-demote", state.gid1],
      },
    ];
    for (const meta of metaSteps) {
      const argv = meta.argv();
      const isQuery = meta.name === "a.retention-show";
      const observation = isQuery
        ? { observeMs: QUERY_MS }
        : { expect: (mark: Mark) => anyEvent(mark), observeMs: OBSERVE_MS };
      // oxlint-disable-next-line no-await-in-loop -- ordered capture; each step waits its turn
      await step({
        name: meta.name,
        actor: "A",
        command: { account: rigA.npub, argv },
        run: async () => wnCap(rigA, argv, rigA.npub),
        ...observation,
        views: rosterViews(rigA, state.gid1),
      });
    }
    await step({
      name: "a.groups-management",
      actor: "A",
      command: () => ({ account: rigA.npub, argv: ["groups", "management", state.gid1] }),
      run: async () => wnCap(rigA, ["groups", "management", state.gid1], rigA.npub),
      observeMs: QUERY_MS,
    });
    await step({
      name: "a.groups-disband-status-gt-1",
      actor: "A",
      command: () => ({ account: rigA.npub, argv: ["groups", "disband-status", state.gid1] }),
      run: async () => wnCap(rigA, ["groups", "disband-status", state.gid1], rigA.npub),
      observeMs: QUERY_MS,
    });

    // Step 9 — C sends raw inner events in wn's shapes; wn's list/timeline show how they render.
    const cSend = async (message: AppMessage): Promise<unknown> => {
      const outcome = await client.sendMessage(state.gid1, message);
      return jsonable(outcome);
    };
    const cViews = async (): Promise<Record<string, unknown>> => ({
      a: await messageViews(rigA, state.gid1),
      b: await messageViews(rigB, state.gid1),
    });

    let cReplyId = "";
    await step({
      name: "c.reply-kind9",
      actor: "C",
      command: () => ({
        call: "client.sendMessage",
        group: state.gid1,
        message: {
          kind: 9,
          tags: [
            ["e", state.m1],
            ["q", state.m1],
          ],
          content: "c reply to m1",
        },
      }),
      run: async () => {
        const reply = await cSend({
          kind: 9,
          tags: [
            ["e", state.m1],
            ["q", state.m1],
          ],
          content: "c reply to m1",
        });
        if (isRecord(reply) && reply["type"] === "staged" && Array.isArray(reply["ids"])) {
          cReplyId = String(reply["ids"][0] ?? "");
        }
        return reply;
      },
      observeMs: OBSERVE_MS,
      views: cViews,
    });
    log(`c reply inner id ${cReplyId}`);

    await step({
      name: "c.react-kind7",
      actor: "C",
      command: () => ({
        call: "client.sendMessage",
        group: state.gid1,
        message: { kind: 7, tags: [["e", state.m1]], content: "👍" },
      }),
      run: async () => {
        const reply = await cSend({ kind: 7, tags: [["e", state.m1]], content: "👍" });
        if (isRecord(reply) && reply["type"] === "staged" && Array.isArray(reply["ids"])) {
          state.cReact = String(reply["ids"][0] ?? "");
        }
        return reply;
      },
      observeMs: OBSERVE_MS,
      views: cViews,
    });
    log(`c reaction inner id ${state.cReact}`);

    await step({
      name: "c.unreact-kind5",
      actor: "C",
      command: () => ({
        call: "client.sendMessage",
        group: state.gid1,
        message: { kind: 5, tags: [["e", state.cReact]], content: "" },
      }),
      run: async () => cSend({ kind: 5, tags: [["e", state.cReact]], content: "" }),
      observeMs: OBSERVE_MS,
      views: cViews,
    });

    await step({
      name: "c.edit-own-kind1009",
      actor: "C",
      command: () => ({
        call: "client.sendMessage",
        group: state.gid1,
        message: { kind: 1009, tags: [["e", cReplyId]], content: "c reply edited" },
      }),
      run: async () => cSend({ kind: 1009, tags: [["e", cReplyId]], content: "c reply edited" }),
      observeMs: OBSERVE_MS,
      views: cViews,
    });

    await step({
      name: "c.delete-own-kind5",
      actor: "C",
      command: () => ({
        call: "client.sendMessage",
        group: state.gid1,
        message: { kind: 5, tags: [["e", cReplyId]], content: "" },
      }),
      run: async () => cSend({ kind: 5, tags: [["e", cReplyId]], content: "" }),
      observeMs: OBSERVE_MS,
      views: cViews,
    });

    await step({
      name: "c.edit-m1-unauthorized-kind1009",
      actor: "C",
      command: () => ({
        call: "client.sendMessage",
        group: state.gid1,
        message: { kind: 1009, tags: [["e", state.m1]], content: "C edits A" },
      }),
      run: async () => cSend({ kind: 1009, tags: [["e", state.m1]], content: "C edits A" }),
      observeMs: OBSERVE_MS,
      views: cViews,
    });

    // Admin removal kind 4891 while C is NOT admin (target: B's r1).
    await step({
      name: "c.remove-r1-nonadmin-kind4891",
      actor: "C",
      command: () => ({
        call: "client.sendMessage",
        group: state.gid1,
        message: {
          kind: 4891,
          tags: [["e", state.r1]],
          content: JSON.stringify({ v: 1, action: "remove" }),
        },
      }),
      run: async () =>
        cSend({
          kind: 4891,
          tags: [["e", state.r1]],
          content: JSON.stringify({ v: 1, action: "remove" }),
        }),
      observeMs: OBSERVE_MS,
      views: cViews,
    });

    // B is the remaining admin: promote C, then retry the admin removal.
    await step({
      name: "b.promote-c",
      actor: "B",
      command: () => ({ account: rigB.npub, argv: ["groups", "promote", state.gid1, ourNpub] }),
      run: async () => wnCap(rigB, ["groups", "promote", state.gid1, ourNpub], rigB.npub),
      expect: (mark) => anyEvent(mark),
      views: rosterViews(rigB, state.gid1),
    });
    await step({
      name: "b.send-b-msg-2",
      actor: "B",
      command: () => ({
        account: rigB.npub,
        argv: ["messages", "send", state.gid1, "b-msg-2 removal target"],
      }),
      run: async () =>
        wn(rigB, ["messages", "send", state.gid1, "b-msg-2 removal target"], rigB.npub),
      expect: (mark) => newMessage(mark, "b-msg-2 removal target"),
    });
    state.bMsg2 =
      received.find((entry) => entry.message.content === "b-msg-2 removal target")?.message.id ??
      "";
    log(`b-msg-2 inner id ${state.bMsg2}`);

    await step({
      name: "c.remove-b-msg-2-admin-kind4891",
      actor: "C",
      command: () => ({
        call: "client.sendMessage",
        group: state.gid1,
        message: {
          kind: 4891,
          tags: [["e", state.bMsg2]],
          content: JSON.stringify({ v: 1, action: "remove" }),
        },
      }),
      run: async () =>
        cSend({
          kind: 4891,
          tags: [["e", state.bMsg2]],
          content: JSON.stringify({ v: 1, action: "remove" }),
        }),
      observeMs: OBSERVE_MS,
      views: cViews,
    });

    // Step 10 — lifecycle: gt-2 disband, then B leaves gt-1, then A removes C from gt-1.
    await step({
      name: "a.groups-create-gt-2",
      actor: "A",
      command: () => ({
        account: rigA.npub,
        argv: ["groups", "create", "gt-2", rigB.npub, ourNpub],
      }),
      run: async () => {
        const created = await wn(rigA, ["groups", "create", "gt-2", rigB.npub, ourNpub], rigA.npub);
        state.gid2 = wnGroupId(created);
        return created;
      },
      expect: (mark) =>
        invites
          .slice(mark.i)
          .some((event) => event.type === "joined" && event.group === state.gid2) || undefined,
    });
    log(`gt-2 group id ${state.gid2}`);

    await step({
      name: "b.groups-accept-gt-2",
      actor: "B",
      command: () => ({ account: rigB.npub, argv: ["groups", "accept", state.gid2] }),
      run: async () => wnCap(rigB, ["groups", "accept", state.gid2], rigB.npub),
      expect: (mark) => newGroupEvent(mark, state.gid2),
    });

    await step({
      name: "a.groups-management-gt-2",
      actor: "A",
      command: () => ({ account: rigA.npub, argv: ["groups", "management", state.gid2] }),
      run: async () => wnCap(rigA, ["groups", "management", state.gid2], rigA.npub),
      observeMs: QUERY_MS,
    });
    await step({
      name: "a.disband-status-gt-2-before",
      actor: "A",
      command: () => ({ account: rigA.npub, argv: ["groups", "disband-status", state.gid2] }),
      run: async () => wnCap(rigA, ["groups", "disband-status", state.gid2], rigA.npub),
      observeMs: QUERY_MS,
    });
    await step({
      name: "a.disband-gt-2-first-try",
      actor: "A",
      command: () => ({
        account: rigA.npub,
        argv: ["groups", "disband", "--confirm", state.gid2],
      }),
      run: async () => wnCap(rigA, ["groups", "disband", "--confirm", state.gid2], rigA.npub),
      observeMs: OBSERVE_MS,
    });
    const firstDisbandReply = steps.at(-1)?.wn_reply;
    if (isRecord(firstDisbandReply) && firstDisbandReply["ok"] !== true) {
      await step({
        name: "a.enable-disbanding-gt-2",
        actor: "A",
        command: () => ({
          account: rigA.npub,
          argv: ["groups", "enable-disbanding", state.gid2],
        }),
        run: async () => wnCap(rigA, ["groups", "enable-disbanding", state.gid2], rigA.npub),
        expect: (mark) => newGroupEvent(mark, state.gid2),
      });
      await step({
        name: "a.disband-gt-2-retry",
        actor: "A",
        command: () => ({
          account: rigA.npub,
          argv: ["groups", "disband", "--confirm", state.gid2],
        }),
        run: async () => wnCap(rigA, ["groups", "disband", "--confirm", state.gid2], rigA.npub),
        expect: (mark) => newGroupEvent(mark, state.gid2),
      });
    }
    await step({
      name: "a.disband-status-gt-2-after",
      actor: "A",
      command: () => ({ account: rigA.npub, argv: ["groups", "disband-status", state.gid2] }),
      run: async () => wnCap(rigA, ["groups", "disband-status", state.gid2], rigA.npub),
      observeMs: QUERY_MS,
      views: async () => ({
        b_disband_status: await wnView(rigB, ["groups", "disband-status", state.gid2]),
        a_show: await wnView(rigA, ["groups", "show", state.gid2]),
      }),
    });

    await step({
      name: "b.leave-gt-1",
      actor: "B",
      command: () => ({ account: rigB.npub, argv: ["groups", "leave", state.gid1] }),
      run: async () => wnCap(rigB, ["groups", "leave", state.gid1], rigB.npub),
      expect: (mark) => newGroupEvent(mark, state.gid1),
      views: async () => ({ a: await rosterViews(rigA, state.gid1)() }),
    });

    await step({
      name: "a.remove-c-gt-1",
      actor: "A",
      command: () => ({
        account: rigA.npub,
        argv: ["groups", "remove-members", state.gid1, ourNpub],
      }),
      run: async () => wnCap(rigA, ["groups", "remove-members", state.gid1, ourNpub], rigA.npub),
      observeMs: OBSERVE_MS,
    });
    await step({
      name: "b.remove-c-gt-1",
      actor: "B",
      command: () => ({
        account: rigB.npub,
        argv: ["groups", "remove-members", state.gid1, ourNpub],
      }),
      run: async () => wnCap(rigB, ["groups", "remove-members", state.gid1, ourNpub], rigB.npub),
      observeMs: OBSERVE_MS,
    });
    // Whoever is still an admin removes C; if nobody can, C leaves voluntarily to close out.
    const roster = await client.group(state.gid1);
    const stillMember = roster?.members.some((member) => member.account === signer.pubkey) === true;
    if (stillMember) {
      await step({
        name: "c.leave-gt-1-voluntary",
        actor: "C",
        command: () => ({ call: "client.leave", group: state.gid1 }),
        run: async () => jsonable(await client.leave(state.gid1)),
        expect: (mark) => newGroupEvent(mark, state.gid1),
      });
    }

    // Write the sanitized fixture.
    const fixture = sanitize(steps);
    writeFileSync(FIXTURE_PATH, `${JSON.stringify(fixture, null, 2)}\n`);
    log(`fixture written: ${FIXTURE_PATH} (${steps.length} steps)`);
    log(`done (${secs(startMs)} total); client errors: ${clientErrors.length}`);
  } finally {
    try {
      await clientRef?.close();
    } catch (error) {
      log(`client close error: ${error instanceof Error ? error.message : String(error)}`);
    }
    poolRef?.close();
    for (const rig of [rigA, rigB]) {
      if (rig.wnd.exitCode === null && !rig.wnd.killed) {
        rig.wnd.kill("SIGKILL");
      }
      // oxlint-disable-next-line no-await-in-loop -- sequential cleanup
      await waitFor(
        () => rig.wnd.exitCode !== null || rig.wnd.signalCode !== null || undefined,
        `wnd-${rig.name} exit`,
        10_000,
      ).catch(() => {
        log(`wnd-${rig.name} did not exit within 10s of SIGKILL`);
      });
      closeSync(rig.outFd);
      closeSync(rig.errFd);
      rmSync(rig.home, { recursive: true, force: true });
    }
    await served.close();
    log("cleanup done (wnds stopped, homes removed)");
  }
}

try {
  await run();
} catch (error) {
  const detail = error instanceof Error ? (error.stack ?? error.message) : String(error);
  log(`FATAL: ${detail}`);
  process.exitCode = 1;
}
