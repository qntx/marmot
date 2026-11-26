/**
 * Phase D interop — real White Noise `wn`/`wnd` against `MarmotClient` on the release wasm backend
 * over one in-process websocket relay (`serveFakeRelay`). `WN_BIN`/`WND_BIN` name the binaries
 * built from the `wn` pin in `upstream.json`; `wn --version` must report that pin's tag without the
 * leading `v` (currently `wn 0.18.0`). Mirrors `crates/interop/tests/support/mod.rs` (CLI flags,
 * daemon env, JSON pointers) and the scenario flow of `crates/interop/tests/wn.rs`. Every step,
 * timing and PASS/FAIL line is appended to `<repo>/target/interop-m3b.log`; the script exits
 * non-zero on any failure.
 *
 * `INTEROP_RELAY` picks the relay shape. `nip11` (default) answers NIP-11 information requests with
 * a `limitation.max_limit`, as production relays do, in front of the fake relay. `bare` is the fake
 * relay alone: no NIP-11 document, which makes wnd's post-join history sweep degenerate into
 * one-second slices and drop a message that arrives meanwhile (S2; see the CI job
 * `interop-known-upstream`).
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
} from "node:fs";
import { createServer } from "node:http";
import { connect } from "node:net";
import { join, resolve } from "node:path";

import { finalizeEvent, generateSecretKey, getPublicKey, nip59 } from "nostr-tools";

import type { Backend, InputReport } from "@qntx/marmot/backend";
import { MarmotClient, nostrRelayPort, MarmotError } from "@qntx/marmot/client";
import type {
  ClientMessage,
  GroupEvent,
  InviteEvent,
  RelayFilter,
  RelayPort,
  Timer,
} from "@qntx/marmot/client";
import { memoryCursorStore, memoryKv } from "@qntx/marmot/testing";
import { Pool } from "@qntx/nostr";
import { decode, npubEncode, nsecEncode } from "@qntx/nostr/nips/nip19";
import { serveFakeRelay } from "@qntx/nostr/testing";

import { releaseBackend } from "../tests/shared/env.ts";
import { testSigner } from "../tests/signer.ts";

function requiredEnv(name: string): string {
  const value = process.env[name];
  if (value === undefined || value === "") {
    throw new Error(`${name} must point at the binary built from the upstream.json wn pin`);
  }
  return value;
}

const WN_BIN = requiredEnv("WN_BIN");
const WND_BIN = requiredEnv("WND_BIN");
const RELAY_SHAPE = process.env["INTEROP_RELAY"] ?? "nip11";
if (RELAY_SHAPE !== "nip11" && RELAY_SHAPE !== "bare") {
  throw new Error(`INTEROP_RELAY must be nip11 or bare, got ${RELAY_SHAPE}`);
}
/** The NIP-11 document a production relay serves (`max_limit` 500 is strfry's default). */
const RELAY_INFORMATION = {
  name: "marmot-interop",
  supported_nips: [1, 11],
  limitation: { max_limit: 500 },
};
const REPO_ROOT = resolve(import.meta.dirname, "../../..");
const LOG_PATH = join(REPO_ROOT, "target", "interop-m5a.log");

const WND_START_DEADLINE_MS = 60_000;
const STEP_DEADLINE_MS = 90_000;
const STEP_POLL_MS = 250;
const RELAY_LIST_KIND = 10002;
const KEY_PACKAGE_KIND = 30443;
const INBOX_RELAYS_KIND = 10050;
const GIFT_WRAP_KIND = 1059;
const WELCOME_RUMOR_KIND = 444;
const CHAT_KIND = 9;
const WN_MESSAGE_LIMIT = 600;

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

function ptrArr(value: unknown, path: string): unknown[] {
  const found = ptr(value, path);
  return Array.isArray(found) ? found : [];
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
    await new Promise<void>((resolve) => {
      setTimeout(resolve, STEP_POLL_MS);
    });
  }
}

/** `wn` daemon rig: short-path home (macOS ~104-char socket limit) plus the log directory. */
type Rig = {
  home: string;
  socket: string;
  logsDir: string;
  wnd: ChildProcess;
  wnNpub: string;
  wnPub: string;
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

/**
 * `wn` invocation tolerating refusals: `ok` is the JSON envelope's `ok` flag. Asynchronous by
 * necessity — a synchronous spawn would freeze this process's event loop, and the in-process ws
 * relay could not answer wnd's handshake, producing "connect relay timed out" failures.
 */
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

/** `wn` invocation that throws on refusal — the harness's `wn` (assert-ok) form. */
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

/** One extra `wnd` home of the run — registered for cleanup. */
type WnHome = { home: string; socket: string; wnd: ChildProcess };

const extraWns: WnHome[] = [];

/** Spawn a second `wnd` against the run's relay and wait for its socket. */
async function spawnWn(tag: string, relayUrl: string): Promise<WnHome> {
  const home = mkdtempSync(join(REPO_ROOT, "target", `wn-home-${tag}-`));
  const socket = join(home, "w.sock");
  const logsDir = join(REPO_ROOT, "target", "wn-logs", `${tag}-${process.pid}`);
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
      relayUrl,
      "--default-account-relays",
      relayUrl,
    ],
    { env: wnEnv(home), stdio: ["ignore", outFd, errFd] },
  );
  const base = { home, socket };
  await waitFor(
    async () => {
      if (!existsSync(socket)) {
        return undefined;
      }
      const probe = await wnTry(base, ["whoami"]);
      return probe.ok || undefined;
    },
    `extra wnd (${tag}) socket`,
    WND_START_DEADLINE_MS,
  );
  const entry = { home, socket, wnd };
  extraWns.push(entry);
  return entry;
}

/**
 * Reap a scenario-spawned `wnd`: SIGKILL, then drop the home. A live child process holds the event
 * loop open, so every spawn path — success or failure — must pass through here.
 */
async function killWn(entry: WnHome): Promise<void> {
  if (entry.wnd.exitCode === null && !entry.wnd.killed) {
    entry.wnd.kill("SIGKILL");
  }
  await waitFor(
    () => entry.wnd.exitCode !== null || entry.wnd.signalCode !== null || undefined,
    "extra wnd exit",
    10_000,
  ).catch(() => undefined);
  rmSync(entry.home, { recursive: true, force: true });
}

/** `wn` invocation with `input` piped to stdin — `login --nsec-stdin`. */
async function wnStdin(
  rig: { home: string; socket: string },
  args: string[],
  input: string,
): Promise<unknown> {
  const argv = ["--json", "--home", rig.home, "--socket", rig.socket, ...args];
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
    child.stdin.write(input);
    child.stdin.end();
  });
  const text = out.stdout.trim().length === 0 ? out.stderr : out.stdout;
  const parsed = JSON.parse(text) as unknown;
  if (!isRecord(parsed) || parsed["ok"] !== true) {
    throw new Error(`wn ${args.join(" ")} refused: ${text}`);
  }
  return parsed;
}

/** The selected account's npub of a `wn` home (`whoami` account 0). */
async function wnAccountNpub(rig: { home: string; socket: string }): Promise<string> {
  const who = await wn(rig, ["whoami"]);
  const npub = ptrStr(who, "/result/accounts/0/npub");
  if (npub === undefined) {
    throw new Error(`whoami carried no npub: ${JSON.stringify(who)}`);
  }
  return npub;
}

function wnGroupId(created: unknown): string {
  const gid = ptrStr(created, "/result/group_id") ?? ptrStr(created, "/result/id");
  if (gid === undefined) {
    throw new Error(`wn group create carried no group_id: ${JSON.stringify(created)}`);
  }
  return gid;
}

async function wnInviteGids(rig: Rig): Promise<string[]> {
  const { ok, parsed } = await wnTry(rig, ["groups", "invites"], rig.wnNpub);
  if (!ok) {
    return [];
  }
  const invites = [...ptrArr(parsed, "/result/invites"), ...ptrArr(parsed, "/result")];
  return invites.flatMap((invite) => {
    const gid = ptrStr(invite, "/group_id") ?? ptrStr(invite, "/id");
    return gid === undefined ? [] : [gid];
  });
}

async function wnMessages(rig: Rig, gid: string): Promise<string[]> {
  const { ok, parsed } = await wnTry(
    rig,
    ["messages", "list", gid, "--limit", String(WN_MESSAGE_LIMIT)],
    rig.wnNpub,
  );
  if (!ok) {
    return [];
  }
  const items = [
    ...ptrArr(parsed, "/result/messages"),
    ...ptrArr(parsed, "/result/items"),
    ...ptrArr(parsed, "/result"),
  ];
  return items.map((item) => JSON.stringify(item));
}

async function sleep(ms: number): Promise<void> {
  await new Promise<void>((resolve) => {
    setTimeout(resolve, ms);
  });
}

function stamp(): string {
  return `-${Math.floor(Date.now() / 1000)}-${Math.floor(Math.random() * 1e4)}`;
}

type ServedFakeRelay = Awaited<ReturnType<typeof serveFakeRelay>>;

type InteropRelay = {
  url: string;
  relay: ServedFakeRelay["relay"];
  /** NIP-11 documents served so far (always 0 for `bare`). */
  documentsServed: () => number;
  close: () => Promise<void>;
};

/**
 * The fake relay behind an HTTP front that answers NIP-11 requests itself and splices every
 * WebSocket upgrade, byte for byte, onto the fake relay's own port.
 */
async function serveNip11Relay(served: ServedFakeRelay): Promise<InteropRelay> {
  const backend = new URL(served.url);
  const spliced = new Set<{ destroy: () => void }>();
  let documents = 0;
  const front = createServer((request, response) => {
    if (request.headers.accept?.includes("application/nostr+json") === true) {
      documents += 1;
      response.writeHead(200, {
        "content-type": "application/nostr+json",
        "access-control-allow-origin": "*",
      });
      response.end(JSON.stringify(RELAY_INFORMATION));
      return;
    }
    response.writeHead(404).end();
  });
  front.on("upgrade", (request, socket, head) => {
    const upstream = connect(Number(backend.port), backend.hostname, () => {
      const lines = [`${request.method ?? "GET"} ${request.url ?? "/"} HTTP/1.1`];
      const raw = request.rawHeaders;
      for (let i = 0; i + 1 < raw.length; i += 2) {
        lines.push(`${raw[i] ?? ""}: ${raw[i + 1] ?? ""}`);
      }
      upstream.write(`${lines.join("\r\n")}\r\n\r\n`);
      upstream.write(head);
      socket.pipe(upstream).pipe(socket);
    });
    spliced.add(socket).add(upstream);
    socket.on("close", () => {
      spliced.delete(socket);
      upstream.destroy();
    });
    upstream.on("close", () => {
      spliced.delete(upstream);
      socket.destroy();
    });
    upstream.on("error", () => {
      socket.destroy();
    });
    socket.on("error", () => {
      upstream.destroy();
    });
  });
  await new Promise<void>((resolve, reject) => {
    front.once("error", reject);
    front.listen(0, "127.0.0.1", () => {
      resolve();
    });
  });
  const address = front.address();
  if (address === null || typeof address === "string") {
    throw new Error("NIP-11 front has no TCP address");
  }
  const { port } = address;
  return {
    url: `ws://127.0.0.1:${port}`,
    relay: served.relay,
    documentsServed: () => documents,
    close: async () => {
      for (const stream of spliced) {
        stream.destroy();
      }
      await new Promise<void>((resolve) => {
        front.close(() => {
          resolve();
        });
      });
      await served.close();
    },
  };
}

async function serveRelay(): Promise<InteropRelay> {
  const served = await serveFakeRelay({ host: "127.0.0.1", port: 0 });
  if (RELAY_SHAPE === "nip11") {
    return serveNip11Relay(served);
  }
  return {
    url: served.url.replace(/\/$/, ""),
    relay: served.relay,
    documentsServed: () => 0,
    close: served.close,
  };
}

async function run(): Promise<void> {
  mkdirSync(join(REPO_ROOT, "target"), { recursive: true });
  log("m5a interop: wn/wnd vs MarmotClient (release wasm, app surface)");

  // The wn pin's tag (upstream.json `wn.tag`, minus the leading `v`) is the release we
  // interoperate with — the check fails loudly when the binary comes from another build.
  const upstream: unknown = JSON.parse(readFileSync(join(REPO_ROOT, "upstream.json"), "utf8"));
  if (
    typeof upstream !== "object" ||
    upstream === null ||
    !("wn" in upstream) ||
    typeof upstream.wn !== "object" ||
    upstream.wn === null ||
    !("tag" in upstream.wn) ||
    typeof upstream.wn.tag !== "string" ||
    !upstream.wn.tag.startsWith("v")
  ) {
    throw new Error("upstream.json: wn.tag must be a v-prefixed release tag");
  }
  const expectedWn = `wn ${upstream.wn.tag.slice(1)}`;
  const version = spawnSync(WN_BIN, ["--version"], { encoding: "utf8" });
  const versionText = `${version.stdout ?? ""}${version.stderr ?? ""}`.trim();
  log(`wn --version: ${versionText}`);
  if (!versionText.includes(expectedWn)) {
    throw new Error(`expected "${expectedWn}", got: ${versionText || "(no output)"}`);
  }

  const served = await serveRelay();
  const { url } = served;
  log(`relay: ${url} (${RELAY_SHAPE})`);

  // wnd rejects a home under world-writable /tmp ("untrusted directory alias"); the repo's
  // target/ is user-owned and keeps the socket path short (macOS ~104-char limit).
  const home = mkdtempSync(join(REPO_ROOT, "target", "wn-home-"));
  const socket = join(home, "w.sock");
  const logsDir = join(REPO_ROOT, "target", "wn-logs", `m5a-${process.pid}`);
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
    log(`wnd spawn error: ${error.message}`);
  });
  log(`wnd pid ${wnd.pid ?? -1} home ${home} logs ${logsDir}`);

  const failures: string[] = [];
  let poolRef: Pool | undefined;
  let clientRef: MarmotClient | undefined;
  let client2Ref: MarmotClient | undefined;
  try {
    const rigBase = { home, socket };
    await waitFor(
      async () => {
        if (!existsSync(socket)) {
          return undefined;
        }
        const probe = await wnTry(rigBase, ["whoami"]);
        return probe.ok || undefined;
      },
      "wnd socket + whoami",
      WND_START_DEADLINE_MS,
    );
    log("wnd up");

    const who = await wnTry(rigBase, ["whoami"]);
    let wnNpub = who.ok ? ptrStr(who.parsed, "/result/accounts/0/npub") : undefined;
    if (wnNpub === undefined) {
      const created = await wn(rigBase, ["create-identity"]);
      wnNpub = ptrStr(created, "/result/npub");
      if (wnNpub === undefined) {
        throw new Error(`create-identity carried no npub: ${JSON.stringify(created)}`);
      }
    }
    await wn(rigBase, ["keys", "publish"], wnNpub);
    const wnPubDecoded = decode(wnNpub);
    if (wnPubDecoded.type !== "npub") {
      throw new Error(`wn npub did not decode to a pubkey: ${wnNpub}`);
    }
    const rig: Rig = { home, socket, logsDir, wnd, wnNpub, wnPub: wnPubDecoded.data };
    log(`wn account ${wnNpub}`);

    const secret = generateSecretKey();
    const signer = testSigner(secret);
    const ourNpub = npubEncode(signer.pubkey);
    log(`our npub ${ourNpub}`);

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
      inbox: "client",
      lookupRelays: [url],
      allowLoopback: true,
    });
    clientRef = client;

    const received: ClientMessage[] = [];
    const invites: InviteEvent[] = [];
    const groupChanges: GroupEvent[] = [];
    const clientErrors: MarmotError[] = [];
    const drops: Array<{ group: string; reason: string }> = [];
    client.onMessages((batch) => {
      received.push(...batch);
    });
    client.onInvite((event) => {
      invites.push(event);
    });
    client.onGroupChange((event) => {
      groupChanges.push(event);
    });
    client.onDropped((event) => {
      drops.push({ group: event.group, reason: event.reason });
      log(`onDropped group=${event.group} reason=${event.reason}`);
    });
    client.onError((error) => {
      clientErrors.push(error);
      log(`client onError ${error.kind}: ${error.message}`);
    });

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
    await client.ensureKeyPackage();
    await client.publishInboxRelays([url]);
    await waitFor(
      () =>
        (served.relay
          .events()
          .some((stored) => stored.kind === KEY_PACKAGE_KIND && stored.pubkey === signer.pubkey) &&
          served.relay
            .events()
            .some(
              (stored) => stored.kind === INBOX_RELAYS_KIND && stored.pubkey === signer.pubkey,
            )) ||
        undefined,
      "our key package + inbox relays stored on the relay",
    );
    log("identity published (10002 + KeyPackage + inbox relays)");

    const s1 = { wnGid: "", ourGid: "" };

    async function scenario(name: string, runScenario: () => Promise<void>): Promise<void> {
      // WN_ONLY is a comma-separated list of scenario ids ("S1,S2,S3").
      const only = process.env["WN_ONLY"]?.split(",");
      if (only !== undefined && !only.includes(name.slice(0, name.indexOf(" ")))) {
        return;
      }
      const start = Date.now();
      try {
        await runScenario();
        log(`PASS ${name} (${secs(start)})`);
      } catch (error) {
        failures.push(name);
        const detail = error instanceof Error ? (error.stack ?? error.message) : String(error);
        const cause =
          error instanceof Error && error.cause !== undefined
            ? ` | cause: ${error.cause instanceof Error ? error.cause.message : JSON.stringify(error.cause)}`
            : "";
        log(`FAIL ${name} (${secs(start)}): ${detail}${cause}`);
      }
    }

    // S1 — wn invites us.
    await scenario("S1 wn creates m3b-1 inviting us", async () => {
      let at = Date.now();
      const created = await wn(rig, ["groups", "create", "m3b-1", ourNpub], rig.wnNpub);
      s1.wnGid = wnGroupId(created);
      log(`wn groups create m3b-1 -> ${s1.wnGid} (${secs(at)})`);

      at = Date.now();
      s1.ourGid = await waitFor(
        () => invites.flatMap((event) => (event.type === "joined" ? [event.group] : []))[0],
        "our onInvite joined",
      );
      log(`onInvite joined group ${s1.ourGid} (${secs(at)})`);
      if (s1.ourGid !== s1.wnGid) {
        throw new Error(`group id mismatch: ours ${s1.ourGid} wn ${s1.wnGid}`);
      }

      at = Date.now();
      const wnText = `s1-wn-${Math.floor(Date.now() / 1000)}`;
      await wn(rig, ["messages", "send", s1.wnGid, wnText], rig.wnNpub);
      await waitFor(
        () =>
          received.some((entry) => entry.group === s1.ourGid && entry.message.content === wnText) ||
          undefined,
        `we receive wn message ${wnText}`,
      );
      log(`received wn message ${wnText} (${secs(at)})`);

      at = Date.now();
      const ourText = `s1-us-${Math.floor(Date.now() / 1000)}`;
      await client.sendMessage(s1.ourGid, { kind: CHAT_KIND, tags: [], content: ourText });
      await waitFor(async () => {
        const entries = await wnMessages(rig, s1.wnGid);
        return entries.some((entry) => entry.includes(ourText)) || undefined;
      }, `wn messages list shows ${ourText}`);
      log(`wn messages list shows ${ourText} (${secs(at)})`);
    });

    // S2 — we invite wn.
    await scenario("S2 we create m3b-2 inviting wn", async () => {
      let at = Date.now();
      const invitees = await client.lookupInvitees(rig.wnPub, [url]);
      if (invitees.length === 0) {
        throw new Error(`lookupInvitees(${rig.wnNpub}) returned no key packages`);
      }
      log(`lookupInvitees -> ${invitees.length} invitee(s) (${secs(at)})`);

      at = Date.now();
      const snapshot = await client.createGroup({
        name: "m3b-2",
        description: "",
        relays: [url],
        invitees,
      });
      log(`createGroup m3b-2 -> ${snapshot.id} (${secs(at)})`);

      at = Date.now();
      await waitFor(async () => {
        const gids = await wnInviteGids(rig);
        return gids.includes(snapshot.id) || undefined;
      }, `wn groups invites shows ${snapshot.id}`);
      log(`wn groups invites shows ${snapshot.id} (${secs(at)})`);

      at = Date.now();
      await wn(rig, ["groups", "accept", snapshot.id], rig.wnNpub);
      await waitFor(async () => {
        const roster = await client.group(snapshot.id);
        if (roster === undefined) {
          return undefined;
        }
        return roster.members.some((member) => member.account === rig.wnPub) || undefined;
      }, `wn member ${rig.wnNpub} in m3b-2 roster`);
      log(`wn accepted; roster includes wn (${secs(at)})`);

      at = Date.now();
      const wnText = `s2-wn-${Math.floor(Date.now() / 1000)}`;
      await wn(rig, ["messages", "send", snapshot.id, wnText], rig.wnNpub);
      await waitFor(
        () =>
          received.some(
            (entry) => entry.group === snapshot.id && entry.message.content === wnText,
          ) || undefined,
        `we receive wn message ${wnText}`,
      );
      log(`received wn message ${wnText} (${secs(at)})`);

      at = Date.now();
      const ourText = `s2-us-${Math.floor(Date.now() / 1000)}`;
      await client.sendMessage(snapshot.id, { kind: CHAT_KIND, tags: [], content: ourText });
      await waitFor(async () => {
        const entries = await wnMessages(rig, snapshot.id);
        return entries.some((entry) => entry.includes(ourText)) || undefined;
      }, `wn messages list shows ${ourText}`);
      log(`wn messages list shows ${ourText} (${secs(at)})`);
    });

    // S3 — wn removes us from S1's group.
    await scenario("S3 wn removes us from m3b-1", async () => {
      if (s1.wnGid.length === 0) {
        throw new Error("S1 never created a group; nothing to remove us from");
      }
      let at = Date.now();
      const before = await client.group(s1.ourGid);
      if (
        before === undefined ||
        before.access === "removed" ||
        before.access === "removed_device" ||
        !before.members.some((member) => member.account === signer.pubkey)
      ) {
        const detail =
          before === undefined
            ? "snapshot absent"
            : `access=${before.access}, self in roster=${String(
                before.members.some((member) => member.account === signer.pubkey),
              )}`;
        throw new Error(`S3 precondition failed: ${detail}`);
      }
      log(`precondition: group access=${before.access}, self in roster (${secs(at)})`);

      at = Date.now();
      await wn(rig, ["groups", "remove-members", s1.wnGid, ourNpub], rig.wnNpub);
      log(`wn groups remove-members ${s1.wnGid} ${ourNpub} (${secs(at)})`);

      at = Date.now();
      const observation = await waitFor(async () => {
        const evidence: string[] = [];
        const snapshot = await client.group(s1.ourGid);
        if (
          snapshot === undefined ||
          snapshot.access === "removed" ||
          snapshot.access === "removed_device"
        ) {
          evidence.push("snapshot absent/removed");
        } else if (!snapshot.members.some((member) => member.account === signer.pubkey)) {
          evidence.push("self absent from roster");
        }
        if (
          groupChanges.some(
            (event) => event.group === s1.ourGid && event.type === "local_copy_terminated",
          )
        ) {
          evidence.push("local_copy_terminated event");
        }
        return evidence.length > 0 ? evidence.join(" + ") : undefined;
      }, "removal observation (state removed / no self member / terminate event)");
      log(`removal observed via ${observation} (${secs(at)})`);

      at = Date.now();
      const terminated = await waitFor(
        () =>
          groupChanges.find(
            (event) => event.group === s1.ourGid && event.type === "local_copy_terminated",
          ),
        "local_copy_terminated event",
        10_000,
      )
        .then((event) => (event?.type === "local_copy_terminated" ? event : undefined))
        .catch(() => undefined);
      log(
        terminated === undefined
          ? `no local_copy_terminated event within 10s (${secs(at)})`
          : `local_copy_terminated voluntary=${String(terminated.voluntary)} (${secs(at)})`,
      );
    });

    const g = { w: "", u: "", w2: "", u2: "", u3: "", w3: "" };

    /** Wn creates a group inviting us; resolves once our client joined, plus the 2 s settle. */
    async function wnGroup(name: string): Promise<string> {
      const created = await wn(rig, ["groups", "create", name, ourNpub], rig.wnNpub);
      const gid = wnGroupId(created);
      const joined = await waitFor(
        () =>
          invites.flatMap((event) =>
            event.type === "joined" && event.group === gid ? [event.group] : [],
          )[0],
        `our onInvite joined ${name}`,
      );
      if (joined !== gid) {
        throw new Error(`group id mismatch: ours ${joined} wn ${gid}`);
      }
      await sleep(2000);
      return gid;
    }

    /** We create a group inviting wn; resolves once wn accepted, plus the 2 s settle. */
    async function ourGroup(name: string): Promise<string> {
      const invitees = await client.lookupInvitees(rig.wnPub, [url]);
      if (invitees.length === 0) {
        throw new Error(`lookupInvitees returned no key packages for ${name}`);
      }
      const snapshot = await client.createGroup({
        name,
        description: "",
        relays: [url],
        invitees,
      });
      await waitFor(async () => {
        const gids = await wnInviteGids(rig);
        return gids.includes(snapshot.id) || undefined;
      }, `wn groups invites shows ${snapshot.id}`);
      await wn(rig, ["groups", "accept", snapshot.id], rig.wnNpub);
      await waitFor(async () => {
        const roster = await client.group(snapshot.id);
        if (roster === undefined) {
          return undefined;
        }
        return roster.members.some((member) => member.account === rig.wnPub) || undefined;
      }, `wn member ${rig.wnNpub} in ${name} roster`);
      await sleep(2000);
      return snapshot.id;
    }

    /** Parsed `wn --json messages timeline list` rows (reply/reaction/edit/delete fields). */
    async function wnList(gid: string): Promise<Array<Record<string, unknown>>> {
      const reply = await wn(rig, ["messages", "timeline", "list", gid], rig.wnNpub);
      return ptrArr(reply, "/result/messages").filter(isRecord);
    }

    /** `wn --json groups <sub> <gid> <rest>` -> /result object. */
    async function wnGroupResult(
      gid: string,
      sub: string,
      extra: string[] = [],
    ): Promise<Record<string, unknown>> {
      const reply = await wn(rig, ["groups", sub, gid, ...extra], rig.wnNpub);
      const result = ptr(reply, "/result");
      if (!isRecord(result)) {
        throw new Error(`wn groups ${sub} carried no result: ${JSON.stringify(reply)}`);
      }
      return result;
    }

    async function wnRetentionSecs(gid: string): Promise<number> {
      const result = await wnGroupResult(gid, "retention");
      const secs = result["disappearing_message_secs"];
      if (typeof secs !== "number") {
        throw new TypeError(
          `retention lacked disappearing_message_secs: ${JSON.stringify(result)}`,
        );
      }
      return secs;
    }

    /** Whether `wn groups <sub>` lists `who` (npub or hex pubkey inside one entry). */
    async function wnHas(gid: string, sub: string, npub: string, hex: string): Promise<boolean> {
      const reply = await wn(rig, ["groups", sub, gid], rig.wnNpub);
      return ptrArr(reply, `/result/${sub}`).some((item) => {
        const text = JSON.stringify(item);
        return text.includes(npub) || text.includes(hex);
      });
    }

    const findMsg = (gid: string, content: string): ClientMessage | undefined =>
      received.find((entry) => entry.group === gid && entry.message.content === content);
    const findRow = (
      gid: string,
      type: string,
      account?: string,
    ): Extract<GroupEvent, { type: "group_state_changed" }> | undefined => {
      for (const event of groupChanges) {
        if (event.group !== gid || event.type !== "group_state_changed") {
          continue;
        }
        const change = event.change as { type: string; account?: string };
        if (change.type === type && (account === undefined || change.account === account)) {
          return event;
        }
      }
      return undefined;
    };

    /** `unauthorized`/`unverifiable` drops collected for `gid` since `since` (a prior count). */
    const badDrops = (gid: string): number =>
      drops.filter(
        (d) => d.group === gid && (d.reason === "unauthorized" || d.reason === "unverifiable"),
      ).length;

    /** After `action` + settle, `gid` must show no new unauthorized/unverifiable drop. */
    async function expectClean(gid: string, action: () => Promise<unknown>): Promise<void> {
      const before = badDrops(gid);
      await action();
      await sleep(2000);
      if (badDrops(gid) !== before) {
        throw new Error(
          `unexpected unauthorized/unverifiable drop(s) in ${gid.slice(0, 8)}: ${JSON.stringify(drops.filter((d) => d.group === gid))}`,
        );
      }
    }

    /**
     * Commit-producing ops refuse with `group_busy` while a previous commit's publish resolution is
     * still in flight — retry the staging call until the group settles.
     */
    async function whenTransitionClears<T>(op: () => Promise<T>): Promise<T> {
      const deadline = Date.now() + STEP_DEADLINE_MS;
      for (;;) {
        try {
          // oxlint-disable-next-line no-await-in-loop -- retries are inherently sequential
          return await op();
        } catch (error) {
          if (
            !(error instanceof MarmotError && error.kind === "group_busy") ||
            Date.now() > deadline
          ) {
            throw error;
          }
          // oxlint-disable-next-line no-await-in-loop -- backoff between retries
          await sleep(STEP_POLL_MS);
        }
      }
    }

    const needGroups = async (): Promise<void> => {
      if (g.w === "") {
        g.w = await wnGroup("m5a-w");
        log(`Gw (wn-admin) ${g.w}`);
      }
      if (g.u === "") {
        g.u = await ourGroup("m5a-u");
        log(`Gu (our-admin) ${g.u}`);
      }
    };

    // S4 — replies both directions.
    await scenario("S4 reply both directions", async () => {
      await needGroups();
      const wnText = `s4-wn${stamp()}`;
      await wn(rig, ["messages", "send", g.w, wnText], rig.wnNpub);
      const wnMsg = await waitFor(() => findMsg(g.w, wnText), `we receive ${wnText}`);

      const ourText = `s4-us${stamp()}`;
      const sent = await client.sendChat(g.w, ourText, wnMsg.message.id);
      const row = await waitFor(async () => {
        const rows = await wnList(g.w);
        return rows.find((entry) => entry["plaintext"] === ourText);
      }, "wn list shows our reply");
      if (row["reply_to_message_id"] !== wnMsg.message.id) {
        throw new Error(
          `reply_to_message_id ${JSON.stringify(row["reply_to_message_id"])} != ${wnMsg.message.id}; tags=${JSON.stringify(row["tags"])}`,
        );
      }

      const wnReplyText = `s4-wn-r${stamp()}`;
      await wn(
        rig,
        ["messages", "send", "--group", g.w, "--reply-to", sent.message.id, wnReplyText],
        rig.wnNpub,
      );
      const replyMsg = await waitFor(() => findMsg(g.w, wnReplyText), `we receive ${wnReplyText}`);
      const sem = replyMsg.message.semantics;
      if (sem.type !== "chat" || sem.replyTo !== sent.message.id) {
        throw new Error(`reply semantics ${JSON.stringify(sem)} != replyTo ${sent.message.id}`);
      }
    });

    // S5 — reactions both directions.
    await scenario("S5 reaction both directions", async () => {
      await needGroups();
      const wnText = `s5-wn${stamp()}`;
      await wn(rig, ["messages", "send", g.w, wnText], rig.wnNpub);
      const wnMsg = await waitFor(() => findMsg(g.w, wnText), `we receive ${wnText}`);

      await client.react(g.w, wnMsg.message.id, "👍");
      await waitFor(async () => {
        const rows = await wnList(g.w);
        const row = rows.find((entry) => entry["message_id"] === wnMsg.message.id);
        return JSON.stringify(row?.["reactions"] ?? {}).includes(signer.pubkey) || undefined;
      }, "wn lists our reaction on the target");

      const ourText = `s5-us${stamp()}`;
      const ourMsg = await client.sendChat(g.w, ourText);
      await wn(rig, ["messages", "react", g.w, ourMsg.message.id, "🔥"], rig.wnNpub);
      const wnReaction = await waitFor(
        () =>
          received.find(
            (entry) =>
              entry.group === g.w &&
              entry.message.semantics.type === "reaction" &&
              (entry.message.semantics as { target?: string }).target === ourMsg.message.id,
          ),
        "we receive wn reaction on our message",
      );
      const sem = wnReaction.message.semantics;
      if (sem.type !== "reaction" || sem.emoji !== "🔥") {
        throw new Error(`reaction semantics ${JSON.stringify(sem)} != fire`);
      }
    });

    // S6 — unreact: ours applies on wn's side; wn's arrives as unreact{reactions:[its own id]}.
    await scenario("S6 unreact both directions", async () => {
      await needGroups();
      const wnText = `s6-wn${stamp()}`;
      await wn(rig, ["messages", "send", g.w, wnText], rig.wnNpub);
      const wnMsg = await waitFor(() => findMsg(g.w, wnText), `we receive ${wnText}`);
      const reaction = await client.react(g.w, wnMsg.message.id, "🎉");
      await waitFor(async () => {
        const rows = await wnList(g.w);
        const row = rows.find((entry) => entry["message_id"] === wnMsg.message.id);
        return JSON.stringify(row?.["reactions"] ?? {}).includes(signer.pubkey) || undefined;
      }, "wn lists our reaction");

      await client.unreact(g.w, [reaction.message.id]);
      await waitFor(async () => {
        const rows = await wnList(g.w);
        const row = rows.find((entry) => entry["message_id"] === wnMsg.message.id);
        return !JSON.stringify(row?.["reactions"] ?? {}).includes(signer.pubkey) || undefined;
      }, "wn no longer lists our reaction");

      // wn can only retract a live reaction of its own: give it one first.
      await wn(rig, ["messages", "react", g.w, wnMsg.message.id, "🔥"], rig.wnNpub);
      const wnReaction = await waitFor(
        () =>
          received.find(
            (entry) =>
              entry.group === g.w &&
              entry.message.semantics.type === "reaction" &&
              (entry.message.semantics as { target?: string }).target === wnMsg.message.id,
          ),
        "we receive wn's reaction to retract",
      );
      let unreact: ClientMessage | undefined;
      await expectClean(g.w, async () => {
        await wn(rig, ["messages", "unreact", g.w, wnMsg.message.id], rig.wnNpub);
        unreact = await waitFor(
          () =>
            received.find(
              (entry) => entry.group === g.w && entry.message.semantics.type === "unreact",
            ),
          "we receive wn's unreact",
        );
      });
      if (unreact === undefined) {
        throw new Error("wn unreact was not received");
      }
      const unreactSem = unreact.message.semantics;
      if (
        unreactSem.type !== "unreact" ||
        unreactSem.reactions.length !== 1 ||
        unreactSem.reactions[0] !== wnReaction.message.id
      ) {
        throw new Error(
          `unreact semantics ${JSON.stringify(unreactSem)} != [${wnReaction.message.id}]`,
        );
      }
    });

    // S7 — edit: ours applies on wn's side; wn's arrives as edit{target, content}.
    await scenario("S7 edit both directions", async () => {
      await needGroups();
      const ourText = `s7-us${stamp()}`;
      const ourMsg = await client.sendChat(g.w, ourText);
      const edited = `s7-edited${stamp()}`;
      await client.editMessage(g.w, ourMsg.message.id, edited);
      await waitFor(async () => {
        const rows = await wnList(g.w);
        const row = rows.find((entry) => entry["message_id"] === ourMsg.message.id);
        const edit = row === undefined ? undefined : row["edit"];
        return (
          (row?.["plaintext"] === edited &&
            isRecord(edit) &&
            typeof edit["edit_count"] === "number" &&
            edit["edit_count"] >= 1) ||
          undefined
        );
      }, "wn list shows edited content + edit_count");

      const wnText = `s7-wn${stamp()}`;
      await wn(rig, ["messages", "send", g.w, wnText], rig.wnNpub);
      const wnMsg = await waitFor(() => findMsg(g.w, wnText), `we receive ${wnText}`);
      const wnEdited = `s7-wn-ed${stamp()}`;
      let wnEdit: ClientMessage | undefined;
      await expectClean(g.w, async () => {
        await wn(rig, ["messages", "edit", g.w, wnMsg.message.id, wnEdited], rig.wnNpub);
        wnEdit = await waitFor(
          () =>
            received.find(
              (entry) =>
                entry.group === g.w &&
                entry.message.semantics.type === "edit" &&
                (entry.message.semantics as { target?: string }).target === wnMsg.message.id,
            ),
          "we receive wn's edit",
        );
      });
      if (wnEdit === undefined) {
        throw new Error("wn edit was not received");
      }
      const editSem = wnEdit.message.semantics;
      if (editSem.type !== "edit" || editSem.target !== wnMsg.message.id) {
        throw new Error(`edit semantics ${JSON.stringify(editSem)} != target ${wnMsg.message.id}`);
      }
      if (editSem.content !== wnEdited) {
        throw new Error(`edit content ${JSON.stringify(editSem.content)} != ${wnEdited}`);
      }
    });

    // S8 — deletes/removals in every authority class.
    await scenario("S8 deletes and removals", async () => {
      await needGroups();
      // Non-admin author delete (kind 5) of our own message in wn's group.
      const ownText = `s8-own${stamp()}`;
      const ownMsg = await client.sendChat(g.w, ownText);
      await client.deleteMessage(g.w, ownMsg.message.id);
      await waitFor(async () => {
        const rows = await wnList(g.w);
        const row = rows.find((entry) => entry["message_id"] === ownMsg.message.id);
        return (row?.["deleted"] === true && row["deletion_source"] === "author") || undefined;
      }, "wn shows our non-admin delete (author)");

      // Admin removals in our group: own message + a wn message.
      const ourText2 = `s8-adm${stamp()}`;
      const ourMsg2 = await client.sendChat(g.u, ourText2);
      const wnText2 = `s8-wn${stamp()}`;
      await wn(rig, ["messages", "send", g.u, wnText2], rig.wnNpub);
      const wnMsg2 = await waitFor(() => findMsg(g.u, wnText2), `we receive ${wnText2}`);
      await client.deleteMessage(g.u, ourMsg2.message.id);
      await client.removeMessage(g.u, wnMsg2.message.id);
      await waitFor(async () => {
        const rows = await wnList(g.u);
        const own = rows.find((entry) => entry["message_id"] === ourMsg2.message.id);
        const wnrow = rows.find((entry) => entry["message_id"] === wnMsg2.message.id);
        return (
          (own?.["deleted"] === true &&
            own["deletion_source"] === "admin" &&
            wnrow?.["deleted"] === true &&
            wnrow["deletion_source"] === "admin") ||
          undefined
        );
      }, "wn shows admin removals (admin)");

      // wn admin delete of our message in wn's group -> typed removal on our side.
      const targetText = `s8-rm${stamp()}`;
      const target = await client.sendChat(g.w, targetText);
      await wn(rig, ["messages", "delete", g.w, target.message.id], rig.wnNpub);
      const removal = await waitFor(
        () =>
          received.find(
            (entry) =>
              entry.group === g.w &&
              entry.message.semantics.type === "removal" &&
              (entry.message.semantics as { target?: string }).target === target.message.id,
          ),
        "we receive wn admin removal",
      );
      const sem = removal.message.semantics;
      if (sem.type !== "removal" || sem.target !== target.message.id) {
        throw new Error(`removal semantics ${JSON.stringify(sem)} != target ${target.message.id}`);
      }

      // wn non-admin kind-5 delete of its own message in our group -> deletion{targets:[id]}.
      const wnOwn = `s8-wnown${stamp()}`;
      await wn(rig, ["messages", "send", g.u, wnOwn], rig.wnNpub);
      const wnOwnMsg = await waitFor(() => findMsg(g.u, wnOwn), `we receive ${wnOwn}`);
      let wnDelete: ClientMessage | undefined;
      await expectClean(g.u, async () => {
        const reply = await wnTry(
          rig,
          ["messages", "delete", g.u, wnOwnMsg.message.id],
          rig.wnNpub,
        );
        if (!reply.ok) {
          throw new Error(`wn refused its own non-admin delete locally: ${reply.raw}`);
        }
        wnDelete = await waitFor(
          () =>
            received.find(
              (entry) =>
                entry.group === g.u &&
                entry.message.semantics.type === "deletion" &&
                (entry.message.semantics as { targets?: string[] }).targets?.includes(
                  wnOwnMsg.message.id,
                ) === true,
            ),
          "we receive wn's author deletion",
        );
      });
      if (wnDelete === undefined) {
        throw new Error("wn delete was not received");
      }
      const deleteSem = wnDelete.message.semantics;
      if (deleteSem.type !== "deletion" || !deleteSem.targets.includes(wnOwnMsg.message.id)) {
        throw new Error(
          `deletion semantics ${JSON.stringify(deleteSem)} != [${wnOwnMsg.message.id}]`,
        );
      }
    });

    // S9 — metadata both directions.
    await scenario("S9 metadata both directions", async () => {
      await needGroups();
      const name = `m5a-u-renamed${stamp()}`;
      await whenTransitionClears(async () =>
        client.updateGroupProfile(g.u, { name, description: "desc m5a" }),
      );
      // The engine refuses a second app-data update while one is pending — wait for our
      // own snapshot to reflect the rename before staging the avatar.
      await waitFor(async () => {
        const snap = await client.group(g.u);
        return snap?.name === name || undefined;
      }, "our snapshot shows the rename");
      await whenTransitionClears(async () =>
        client.setGroupAvatar(g.u, {
          url: "https://example.com/m5a.png",
          dim: "64x64",
          thumbhash: undefined,
        }),
      );
      await waitFor(async () => {
        const show = await wnGroupResult(g.u, "show");
        const text = JSON.stringify(show);
        return (text.includes(name) && text.includes("https://example.com/m5a.png")) || undefined;
      }, "wn groups show reflects our name+avatar");
      const snap1 = await client.group(g.u);
      if (snap1?.live?.avatar?.url !== "https://example.com/m5a.png") {
        throw new Error(`our avatar snapshot ${JSON.stringify(snap1?.live?.avatar)}`);
      }

      await wn(
        rig,
        ["groups", "update", "--name", `m5a-w-renamed${stamp()}`, "--description", "wn desc", g.w],
        rig.wnNpub,
      );
      await wn(
        rig,
        ["groups", "set-avatar-url", g.w, "--url", "https://example.com/wn.png", "--dim", "32x32"],
        rig.wnNpub,
      );
      await waitFor(
        () => findRow(g.w, "avatar_changed") ?? undefined,
        "avatar_changed row for wn's update",
      );
      await waitFor(() => findRow(g.w, "renamed") ?? undefined, "renamed row for wn's update");
      const renamed = findRow(g.w, "renamed");
      const avatarRow = findRow(g.w, "avatar_changed");
      if (renamed?.actor !== rig.wnPub || avatarRow?.actor !== rig.wnPub) {
        throw new Error(
          `row actors renamed=${String(renamed?.actor)} avatar=${String(avatarRow?.actor)} != wn`,
        );
      }
      const snap2 = await client.group(g.w);
      if (snap2?.live?.avatar?.url !== "https://example.com/wn.png") {
        throw new Error(`snapshot avatar ${JSON.stringify(snap2?.live?.avatar)}`);
      }
    });

    // S10 — admin changes both directions + local non-admin refusal.
    await scenario("S10 admin changes + local refusal", async () => {
      await needGroups();
      await whenTransitionClears(async () => client.setAdmin(g.u, rig.wnPub, true));
      await waitFor(
        async () => (await wnHas(g.u, "admins", rig.wnNpub, rig.wnPub)) || undefined,
        "wn groups admins lists wn after promote",
      );
      await waitFor(async () => {
        const snap = await client.group(g.u);
        return (
          snap?.members.some(
            (member) =>
              member.account === rig.wnPub && snap.live?.admins.includes(member.account) === true,
          ) === true || undefined
        );
      }, "our snapshot shows wn admin");
      await whenTransitionClears(async () => client.setAdmin(g.u, rig.wnPub, false));
      await waitFor(
        async () => !(await wnHas(g.u, "admins", rig.wnNpub, rig.wnPub)) || undefined,
        "wn groups admins drops wn after demote",
      );

      // Our non-admin removal attempt fails locally and publishes nothing.
      const own = await client.sendChat(g.w, `s10-own${stamp()}`);
      await sleep(1000);
      const relayBefore = served.relay.events().length;
      let refusal = "";
      try {
        await client.removeMessage(g.w, own.message.id);
      } catch (error) {
        refusal =
          error !== null && typeof error === "object" && "kind" in error
            ? String(error.kind)
            : String(error);
      }
      if (refusal !== "not_group_admin") {
        throw new Error(`non-admin removeMessage -> ${refusal || "no error"}`);
      }
      await sleep(1000);
      if (served.relay.events().length !== relayBefore) {
        throw new Error("refused non-admin op still published relay events");
      }

      await wn(rig, ["groups", "promote", g.w, ourNpub], rig.wnNpub);
      const added = await waitFor(
        () => findRow(g.w, "admin_added", signer.pubkey) ?? undefined,
        "admin_added row for our account",
      );
      if (added.actor !== rig.wnPub) {
        throw new Error(`admin_added actor ${String(added.actor)} != wn`);
      }
      await waitFor(async () => {
        const snap = await client.group(g.w);
        return snap?.live?.ownAccountAdmin === true || undefined;
      }, "snapshot ownAccountAdmin true");
      await wn(rig, ["groups", "demote", g.w, ourNpub], rig.wnNpub);
      const removed = await waitFor(
        () => findRow(g.w, "admin_removed", signer.pubkey) ?? undefined,
        "admin_removed row for our account",
      );
      if (removed.actor !== rig.wnPub) {
        throw new Error(`admin_removed actor ${String(removed.actor)} != wn`);
      }
      await waitFor(async () => {
        const snap = await client.group(g.w);
        return snap?.live?.ownAccountAdmin === false || undefined;
      }, "snapshot ownAccountAdmin false");
    });

    // S11 — retention both directions.
    await scenario("S11 retention both directions", async () => {
      await needGroups();
      await whenTransitionClears(async () => client.setRetention(g.u, 3600n));
      await waitFor(
        async () => (await wnRetentionSecs(g.u)) === 3600 || undefined,
        "wn groups retention shows 3600",
      );

      await wn(rig, ["groups", "retention", g.w, "--set", "1800"], rig.wnNpub);
      const row = await waitFor(
        () => findRow(g.w, "retention_changed") ?? undefined,
        "retention_changed row",
      );
      if (row.actor !== rig.wnPub || row.change.type !== "retention_changed") {
        throw new Error(`retention_changed row malformed: ${JSON.stringify(row)}`);
      }
      const wnText = `s11-wn${stamp()}`;
      await wn(rig, ["messages", "send", g.w, wnText], rig.wnNpub);
      const wnMsg = await waitFor(() => findMsg(g.w, wnText), `we receive ${wnText}`);
      const expected = wnMsg.message.createdAt + 1800n;
      if (wnMsg.message.expiresAt !== expected) {
        throw new Error(
          `expiresAt ${String(wnMsg.message.expiresAt)} != createdAt+1800 ${String(expected)}`,
        );
      }
      // Restore Gw to no retention so later scenarios are unaffected.
      await wn(rig, ["groups", "retention", g.w, "--set", "0"], rig.wnNpub);
      await waitFor(
        () =>
          groupChanges.some(
            (event) =>
              event.group === g.w &&
              event.type === "group_state_changed" &&
              event.change.type === "retention_changed" &&
              event.change.newSeconds === 0n,
          ) || undefined,
        "retention restored to 0",
      );
    });

    // S12 — leaves both directions.
    await scenario("S12 leave both directions", async () => {
      g.w2 = await wnGroup("m5a-w2");
      g.u2 = await ourGroup("m5a-u2");

      await client.leave(g.w2);
      await waitFor(
        async () => !(await wnHas(g.w2, "members", ourNpub, signer.pubkey)) || undefined,
        "wn groups members drops us",
      );
      await waitFor(async () => {
        const snap = await client.group(g.w2);
        return (
          snap === undefined ||
          snap.access === "removed" ||
          snap.access === "removed_device" ||
          undefined
        );
      }, "our snapshot removed after leave");

      await wn(rig, ["groups", "leave", g.u2], rig.wnNpub);
      const row = await waitFor(
        () => findRow(g.u2, "member_left", rig.wnPub) ?? undefined,
        "member_left row for wn",
      );
      if (!("account" in row.change) || row.change.account !== rig.wnPub) {
        throw new Error(`member_left subject ${JSON.stringify(row.change)} != wn`);
      }
    });

    // S13 — disband both directions.
    await scenario("S13 disband both directions", async () => {
      g.u3 = await ourGroup("m5a-u3");
      g.w3 = await wnGroup("m5a-w3");

      const outcome = await client.disband(g.u3);
      if (outcome.type !== "disband_requested") {
        throw new Error(`disband outcome ${outcome.type} != disband_requested`);
      }
      await waitFor(async () => {
        const status = await wnGroupResult(g.u3, "disband-status");
        return (
          status["disbanded"] === true || status["lifecycle_state"] === "terminal" || undefined
        );
      }, "wn disband-status reaches terminal");

      await wn(rig, ["groups", "disband", "--confirm", g.w3], rig.wnNpub);
      const row = await waitFor(
        () => findRow(g.w3, "disbanded") ?? undefined,
        "disbanded row for wn's disband",
      );
      if (row.actor !== rig.wnPub) {
        throw new Error(`disbanded actor ${String(row.actor)} != wn`);
      }
      await waitFor(async () => {
        const snap = await client.group(g.w3);
        return snap?.access === "disbanded" || undefined;
      }, "our snapshot disbanded");
    });

    // S17 — a second device of our account signs out: its SelfRemove
    // commits, wn shows one leaf left, the signed-out device ends removed.
    await scenario("S17 second device signs out", async () => {
      const backend2: Backend = await releaseBackend();
      const client2 = await MarmotClient.open({
        backend: backend2,
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
        inbox: "client",
        lookupRelays: [url],
        allowLoopback: true,
      });
      client2Ref = client2;
      const invites2: InviteEvent[] = [];
      const changes2: GroupEvent[] = [];
      client2.onInvite((event) => {
        invites2.push(event);
      });
      client2.onGroupChange((event) => {
        changes2.push(event);
      });
      // Relative count: wait for one NEW package, so the scenario does
      // not depend on how many devices published before it.
      const knownKps = new Set(
        served.relay
          .events()
          .filter((stored) => stored.kind === KEY_PACKAGE_KIND && stored.pubkey === signer.pubkey)
          .map((stored) => stored.id),
      );
      await client2.ensureKeyPackage();
      await client2.publishInboxRelays([url]);
      await waitFor(
        () =>
          served.relay
            .events()
            .some(
              (stored) =>
                stored.kind === KEY_PACKAGE_KIND &&
                stored.pubkey === signer.pubkey &&
                !knownKps.has(stored.id),
            ) || undefined,
        "second device's KeyPackage on the relay",
      );

      let at = Date.now();
      const created = await wn(rig, ["groups", "create", "m3b-17", ourNpub], rig.wnNpub);
      const wnGid = wnGroupId(created);
      const [gid1, gid2] = await Promise.all([
        waitFor(
          () =>
            invites.flatMap((event) =>
              event.type === "joined" && event.group === wnGid ? [event.group] : [],
            )[0],
          "device 1 onInvite joined",
        ),
        waitFor(
          () =>
            invites2.flatMap((event) =>
              event.type === "joined" && event.group === wnGid ? [event.group] : [],
            )[0],
          "device 2 onInvite joined",
        ),
      ]);
      if (gid1 !== wnGid || gid2 !== wnGid) {
        throw new Error(`group id mismatch: device1 ${gid1} device2 ${gid2} wn ${wnGid}`);
      }
      log(`both devices joined ${wnGid} (${secs(at)})`);

      at = Date.now();
      // Removing device 2's leaf keeps the account a member through
      // device 1's leaf: the commit surfaces as `epoch_changed`, not a
      // member_left/member_removed account change.
      const epochChangesBefore = groupChanges.filter(
        (event) => event.group === wnGid && event.type === "epoch_changed",
      ).length;
      const report = await client2.signOut();
      if (!report.left.includes(gid2)) {
        throw new Error(`signOut left ${JSON.stringify(report.left)}, expected ${gid2}`);
      }
      await waitFor(async () => {
        const reply = await wn(rig, ["groups", "members", wnGid], rig.wnNpub);
        const leaves = ptrArr(reply, "/result/members").filter((item) => {
          const text = JSON.stringify(item);
          return text.includes(ourNpub) || text.includes(signer.pubkey);
        });
        return leaves.length === 1 || undefined;
      }, "wn members shows one leaf for our account");
      await waitFor(
        () =>
          changes2.some(
            (event) =>
              event.group === gid2 && event.type === "local_copy_terminated" && event.voluntary,
          ) || undefined,
        "device 2 observes its own removal",
      );
      // The signed-out device lost its leaf; the account lives on through
      // device 1 — `access` is removed_device, not removed. A removed
      // copy is no longer live, so the snapshot comes from `group(id)`,
      // not `groups()`.
      await waitFor(async () => {
        const snapshot = await client2.group(gid2);
        return snapshot?.access === "removed_device" || undefined;
      }, "device 2 access removed_device");
      log(`device 2 signed out; wn keeps one leaf (${secs(at)})`);

      // Device 1 applies wn's removal commit too: the shared leaf update
      // surfaces as `epoch_changed`, not a member change for the account.
      await waitFor(
        () =>
          groupChanges.filter((event) => event.group === gid1 && event.type === "epoch_changed")
            .length > epochChangesBefore || undefined,
        "device 1 applies the removal commit",
      );
    });

    // S19 — our device consumed KeyPackage → rotation; wn re-invites.
    await scenario("S19 consumed KeyPackage rotates; wn re-invites", async () => {
      const [before] = served.relay
        .events()
        .filter((stored) => stored.kind === KEY_PACKAGE_KIND && stored.pubkey === signer.pubkey)
        .toSorted((a, b) => b.created_at - a.created_at);
      if (before === undefined) {
        throw new Error("no KeyPackage on the relay");
      }
      const dTag = before.tags.find((tag) => tag[0] === "d")?.[1] ?? "";
      const iTag = before.tags.find((tag) => tag[0] === "i")?.[1];

      // Consume the publication heading the slot: wn invites us into a
      // fresh group, which marks the current bundle consumed.
      let at = Date.now();
      const createdA = await wn(rig, ["groups", "create", "m3b-19a", ourNpub], rig.wnNpub);
      const wnGidA = wnGroupId(createdA);
      const gidA = await waitFor(
        () =>
          invites.flatMap((event) =>
            event.type === "joined" && event.group === wnGidA ? [event.group] : [],
          )[0],
        "our onInvite joined (s19 consumer group)",
      );
      if (gidA !== wnGidA) {
        throw new Error(`group id mismatch: ours ${gidA} wn ${wnGidA}`);
      }
      log(`joined ${gidA} — current KeyPackage consumed (${secs(at)})`);

      // Within one driven pass the lifecycle stages and publishes a
      // replacement to the same slot: same `d`, newer `created_at`,
      // different `i`.
      const rotated = await waitFor(async () => {
        await client.ensureKeyPackage();
        return (
          served.relay
            .events()
            .filter(
              (stored) =>
                stored.kind === KEY_PACKAGE_KIND &&
                stored.pubkey === signer.pubkey &&
                (stored.tags.find((tag) => tag[0] === "d")?.[1] ?? "") === dTag &&
                stored.created_at > before.created_at &&
                (stored.tags.find((tag) => tag[0] === "i")?.[1] ?? undefined) !== iTag,
            )
            .toSorted((a, b) => b.created_at - a.created_at)[0] ?? undefined
        );
      }, "same-slot KeyPackage replacement on the relay");
      log(`rotation: kind-30443 ${rotated.id} in slot ${dTag} (${secs(at)})`);

      // wn's invite must select the replacement heading the slot.
      at = Date.now();
      const created = await wn(rig, ["groups", "create", "m3b-19", ourNpub], rig.wnNpub);
      const wnGid = wnGroupId(created);
      const ourGid = await waitFor(
        () =>
          invites.flatMap((event) =>
            event.type === "joined" && event.group === wnGid ? [event.group] : [],
          )[0],
        "our onInvite joined (second group)",
      );
      if (ourGid !== wnGid) {
        throw new Error(`group id mismatch: ours ${ourGid} wn ${wnGid}`);
      }
      log(`joined ${ourGid} on the rotated KeyPackage (${secs(at)})`);

      const wnText = `s19-wn-${Math.floor(Date.now() / 1000)}`;
      await wn(rig, ["messages", "send", wnGid, wnText], rig.wnNpub);
      await waitFor(
        () =>
          received.some((entry) => entry.group === ourGid && entry.message.content === wnText) ||
          undefined,
        `we receive wn message ${wnText}`,
      );
      const ourText = `s19-us-${Math.floor(Date.now() / 1000)}`;
      await client.sendMessage(ourGid, { kind: CHAT_KIND, tags: [], content: ourText });
      await waitFor(async () => {
        const entries = await wnMessages(rig, wnGid);
        return entries.some((entry) => entry.includes(ourText)) || undefined;
      }, `wn messages list shows ${ourText}`);
    });

    // S20 (D2) — two devices of our account, wn admin. A censoring
    // RelayPort withholds wn's rename commit from device 1 only: three
    // chats defer, the session suspects once, the Pump's one unfloored
    // refetch still lacks the commit, and the copy reports
    // `access: stalled` while device 2 stays active. wn then removes and
    // re-invites the account: device 2's removed copy reinstalls from
    // its new Welcome without consent, device 1's stalled copy gets a
    // consent-required `rejoin_offered`, and after `confirmRejoin` both
    // devices exchange messages with wn again.
    //
    // Device 1 creates the group and seats wn as an admin rather than
    // joining a wn-created one: a Welcome-installed copy floors
    // `local_copy_welcome_created_at`, and the five-minute post-Welcome
    // skew window swallows everything wn could emit on wall-clock time —
    // a self-created copy has no such floor, so the withheld wn commit
    // defers the chats for real.
    await scenario("S20 stall detection + rejoin repair", async () => {
      const inner = nostrRelayPort(pool);
      // Two-stage drop: while `armed` every kind-445 event is withheld
      // and its id recorded — the rename's commit hits the live
      // subscription before the relay store can be polled for it, so
      // arming must precede the rename. After disarm, only the recorded
      // ids stay censored, refetch replay included.
      const drop = { armed: false, ids: new Set<string>() };
      const droppingPort: RelayPort = {
        publish: async (relay, event) => inner.publish(relay, event),
        connect: async (relay) => inner.connect(relay),
        subscribe: (relay, filters, handlers) =>
          inner.subscribe(relay, filters, {
            ...handlers,
            event: (event) => {
              if (event.kind === 445) {
                if (drop.armed) {
                  drop.ids.add(event.id);
                  return;
                }
                if (drop.ids.has(event.id)) {
                  return;
                }
              }
              handlers.event(event);
            },
          }),
      };
      const backend3: Backend = await releaseBackend();
      const client3 = await MarmotClient.open({
        backend: backend3,
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
        relays: droppingPort,
        inbox: "client",
        lookupRelays: [url],
        allowLoopback: true,
      });
      let client4: MarmotClient | undefined;
      try {
        const backend4: Backend = await releaseBackend();
        client4 = await MarmotClient.open({
          backend: backend4,
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
          inbox: "client",
          lookupRelays: [url],
          allowLoopback: true,
        });
        const invites3: InviteEvent[] = [];
        const changes3: GroupEvent[] = [];
        const received3: ClientMessage[] = [];
        client3.onInvite((event) => {
          invites3.push(event);
        });
        client3.onGroupChange((event) => {
          changes3.push(event);
        });
        client3.onMessages((batch) => {
          received3.push(...batch);
        });
        const invites4: InviteEvent[] = [];
        const changes4: GroupEvent[] = [];
        const received4: ClientMessage[] = [];
        client4.onInvite((event) => {
          invites4.push(event);
        });
        client4.onGroupChange((event) => {
          changes4.push(event);
        });
        client4.onMessages((batch) => {
          received4.push(...batch);
        });
        // Relative counts: earlier scenarios may or may not have run under
        // WN_ONLY filtering, so wait for NEW packages, not totals.
        const knownKps = new Set(
          served.relay
            .events()
            .filter((stored) => stored.kind === KEY_PACKAGE_KIND && stored.pubkey === signer.pubkey)
            .map((stored) => stored.id),
        );
        await client3.ensureKeyPackage();
        await client3.publishInboxRelays([url]);
        await client4.ensureKeyPackage();
        await client4.publishInboxRelays([url]);
        const ourKps = await waitFor(() => {
          const fresh = served.relay
            .events()
            .filter(
              (stored) =>
                stored.kind === KEY_PACKAGE_KIND &&
                stored.pubkey === signer.pubkey &&
                !knownKps.has(stored.id),
            );
          return fresh.length >= 2 ? fresh : undefined;
        }, "both devices' KeyPackages on the relay");

        let at = Date.now();
        const wnInvitees = await client3.lookupInvitees(rig.wnPub, [url]);
        if (wnInvitees.length === 0) {
          throw new Error("lookupInvitees returned no key packages for m3b-20");
        }
        // The creator's own leaf is already a member, so only the second
        // device's KeyPackage is an invitee — a Commit cannot add a leaf
        // whose signature key duplicates the creator's (`d` slot tag).
        const ownStatus = await client3.keyPackageStatus();
        const ownSlot = ownStatus.slot;
        const siblingKps = ourKps.filter(
          (stored) => stored.tags.find((tag) => tag[0] === "d")?.[1] !== ownSlot,
        );
        if (siblingKps.length === 0) {
          throw new Error("no sibling-device KeyPackage for m3b-20");
        }
        const snapshot3 = await client3.createGroup({
          name: "m3b-20",
          description: "",
          relays: [url],
          invitees: wnInvitees,
          includeOwnDevices: false,
          ownDevices: siblingKps.map((stored) => ({
            keyPackage: {
              id: stored.id,
              pubkey: stored.pubkey,
              createdAt: BigInt(stored.created_at),
              kind: stored.kind,
              tags: stored.tags.map((tag) => [...tag]),
              content: stored.content,
              sig: stored.sig,
            },
            inboxRelays: [url],
          })),
          admins: [rig.wnPub],
        });
        const gid = snapshot3.id;
        const wnGid = gid;
        // Device 2 installs its copy from its own founding wrap.
        await waitFor(
          () =>
            invites4.some((event) => event.type === "joined" && event.group === gid) || undefined,
          "device 4 joins via its founding wrap",
        );
        await waitFor(async () => {
          const gids = await wnInviteGids(rig);
          return gids.includes(gid) || undefined;
        }, `wn groups invites shows ${gid}`);
        await wn(rig, ["groups", "accept", wnGid], rig.wnNpub);
        for (const [label, member] of [
          ["device 3", client3],
          ["device 4", client4],
        ] as const) {
          // oxlint-disable-next-line no-await-in-loop -- sequential convergence waits
          await waitFor(async () => {
            const roster = await member.group(gid);
            if (roster === undefined) {
              return undefined;
            }
            return roster.members.some((entry) => entry.account === rig.wnPub) || undefined;
          }, `${label} roster includes wn`);
        }
        await sleep(2000);
        const route = snapshot3.live?.nostrGroupId;
        if (route === undefined) {
          throw new Error("device 3 route missing");
        }
        log(`device 3 created ${wnGid} with wn admin; device 4 joined (${secs(at)})`);

        // Arm the drop, then rename: the commit must never reach device 1's
        // live subscription. The relay-store read below only CONFIRMS the
        // set — it stays nonempty and unchanged across two polls before
        // the port is disarmed, so a slow publish cannot slip one through.
        at = Date.now();
        const seenIds = new Set(
          served.relay
            .events()
            .filter(
              (stored) =>
                stored.kind === 445 &&
                stored.tags.some((tag) => tag[0] === "h" && tag[1] === route),
            )
            .map((stored) => stored.id),
        );
        drop.armed = true;
        await wn(rig, ["groups", "rename", wnGid, "m3b-20-renamed"], rig.wnNpub);
        let lastIds = "";
        const newIds = await waitFor(() => {
          const ids = served.relay
            .events()
            .filter(
              (stored) =>
                stored.kind === 445 &&
                !seenIds.has(stored.id) &&
                stored.tags.some((tag) => tag[0] === "h" && tag[1] === route),
            )
            .map((stored) => stored.id)
            .toSorted()
            .join(",");
          if (ids !== "" && ids === lastIds) {
            return new Set(ids.split(","));
          }
          lastIds = ids;
          return undefined;
        }, "wn's rename commit on the route");
        for (const id of newIds) {
          drop.ids.add(id);
        }
        drop.armed = false;
        if (drop.ids.size === 0) {
          throw new Error("the censoring port captured no commit to withhold");
        }
        for (const text of ["s20-m0", "s20-m1", "s20-m2"]) {
          // oxlint-disable-next-line no-await-in-loop -- sends are sequential by design
          await wn(rig, ["messages", "send", wnGid, text], rig.wnNpub);
        }

        // Three deferred events suspect once; the Pump's unfloored replay
        // still lacks the dropped commit, so `history_fetched` flags the
        // copy stalled — exactly one refetch deep.
        await waitFor(
          () =>
            changes3.some((event) => event.group === gid && event.type === "group_stalled") ||
            undefined,
          "device 3 stalls after one refetch",
        );
        const stalled3 = await client3.group(gid);
        if (stalled3?.access !== "stalled") {
          throw new Error(`device 3 access ${String(stalled3?.access)}, expected stalled`);
        }
        const healthy4 = await client4.group(gid);
        if (healthy4?.access !== "active") {
          throw new Error(`device 4 access ${String(healthy4?.access)}, expected active`);
        }
        log(`device 3 stalled after one refetch; device 4 active (${secs(at)})`);

        // Repair: wn removes the whole account, then re-invites it. Device
        // 4 must apply the removal before the re-add wraps reach it —
        // arriving while it still held a leaf would park a consent offer.
        at = Date.now();
        await wn(rig, ["groups", "remove-members", wnGid, ourNpub], rig.wnNpub);
        await waitFor(async () => {
          const snapshot = await client4?.group(gid);
          return (
            snapshot?.access === "removed" || snapshot?.access === "removed_device" || undefined
          );
        }, "device 4 removed by wn");
        await wn(rig, ["groups", "add-members", wnGid, ourNpub], rig.wnNpub);

        // Device 4's removed copy reinstalls from its new Welcome on its
        // own — no consent, no `rejoin_offered` across the whole repair.
        await waitFor(async () => {
          const snapshot = await client4?.group(gid);
          return snapshot?.access === "active" || undefined;
        }, "device 4 auto-installs the re-add welcome");
        if (changes4.some((event) => event.type === "rejoin_offered" && event.group === gid)) {
          throw new Error("the healthy sibling saw a consent offer");
        }
        log(`device 4 reinstalled without consent (${secs(at)})`);

        // Device 3's stalled copy cannot apply the removal commit, so the
        // re-add wrap parks a consent-required offer on it.
        await waitFor(
          () =>
            changes3.some((event) => event.type === "rejoin_offered" && event.group === gid) ||
            undefined,
          "device 3 rejoin offer",
        );
        const offers = await client3.rejoinOffers(gid);
        if (offers.length !== 1) {
          throw new Error(`device 3 holds ${offers.length} offers, expected 1`);
        }
        const [offer] = offers;
        if (offer === undefined || offer.author !== rig.wnPub) {
          throw new Error(`offer author ${String(offer?.author)}, expected ${rig.wnPub}`);
        }
        await client3.confirmRejoin(gid, offer.offer, offer.token);
        await waitFor(async () => {
          const snapshot = await client3.group(gid);
          return snapshot?.access === "active" || undefined;
        }, "device 3 active after confirmRejoin");
        log(`device 3 confirmed the rejoin (${secs(at)})`);

        // wn's next message reaches both devices at the repaired epoch; a
        // message from each device reaches wn.
        const wnText = `s20-wn-${Math.floor(Date.now() / 1000)}`;
        await wn(rig, ["messages", "send", wnGid, wnText], rig.wnNpub);
        await waitFor(
          () =>
            received3.some((entry) => entry.group === gid && entry.message.content === wnText) ||
            undefined,
          "device 3 reads wn after rejoin",
        );
        await waitFor(
          () =>
            received4.some((entry) => entry.group === gid && entry.message.content === wnText) ||
            undefined,
          "device 4 reads wn after rejoin",
        );
        for (const [label, member, text] of [
          ["device 3", client3, `s20-d3-${Math.floor(Date.now() / 1000)}`],
          ["device 4", client4, `s20-d4-${Math.floor(Date.now() / 1000)}`],
        ] as const) {
          // oxlint-disable-next-line no-await-in-loop -- sends are sequential by design
          await member.sendMessage(gid, { kind: CHAT_KIND, tags: [], content: text });
          // oxlint-disable-next-line no-await-in-loop -- sequential wn polls
          await waitFor(async () => {
            const entries = await wnMessages(rig, wnGid);
            return entries.some((entry) => entry.includes(text)) || undefined;
          }, `wn reads ${label}`);
        }
      } finally {
        await client3.close();
        await client4?.close();
      }
    });

    // S22 (D2) — we are the admin of a group holding a wn member. A
    // publish-censoring RelayPort withholds one of our commits from the
    // relay while still answering `accepted` — our epoch moves, wn never
    // learns it, and a chat rides the withheld epoch that wn cannot
    // read. `readdMember` stages the removal; its confirmation stages
    // exactly one Invite; wn's stalled copy surfaces a consent-required
    // offer through `groups recovery-status`; `confirm-rejoin` installs
    // the replacement copy and the round trip recovers.
    await scenario("S22 we re-add a stuck wn member", async () => {
      let at = Date.now();
      const inner = nostrRelayPort(pool);
      // While `armed`, the FIRST kind-445 publish is reported `accepted`
      // but never forwarded — the session's epoch moves on a commit the
      // relay (and wn) never sees.
      const drop = { armed: false, id: "" };
      const censoringPort: RelayPort = {
        publish: async (relay, event) => {
          if (event.kind === 445 && drop.armed && drop.id === "") {
            drop.id = event.id;
            return "accepted";
          }
          return inner.publish(relay, event);
        },
        connect: async (relay) => inner.connect(relay),
        subscribe: (relay, filters, handlers) => inner.subscribe(relay, filters, handlers),
      };
      const backend5: Backend = await releaseBackend();
      const client5 = await MarmotClient.open({
        backend: backend5,
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
        relays: censoringPort,
        inbox: "client",
        lookupRelays: [url],
        allowLoopback: true,
      });
      try {
        const invites5: InviteEvent[] = [];
        const received5: ClientMessage[] = [];
        client5.onInvite((event) => {
          invites5.push(event);
        });
        client5.onMessages((batch) => {
          received5.push(...batch);
        });
        const knownKps = new Set(
          served.relay
            .events()
            .filter((stored) => stored.kind === KEY_PACKAGE_KIND && stored.pubkey === signer.pubkey)
            .map((stored) => stored.id),
        );
        await client5.ensureKeyPackage();
        await client5.publishInboxRelays([url]);
        await waitFor(
          () =>
            served.relay
              .events()
              .some(
                (stored) =>
                  stored.kind === KEY_PACKAGE_KIND &&
                  stored.pubkey === signer.pubkey &&
                  !knownKps.has(stored.id),
              ) || undefined,
          "admin device's KeyPackage on the relay",
        );
        const wnInvitees = await client5.lookupInvitees(rig.wnPub, [url]);
        if (wnInvitees.length === 0) {
          throw new Error("lookupInvitees returned no key packages for m3b-22");
        }
        const snapshot5 = await client5.createGroup({
          name: "m3b-22",
          description: "",
          relays: [url],
          invitees: wnInvitees,
        });
        const gid = snapshot5.id;
        const wnGid = gid;
        await waitFor(async () => {
          const gids = await wnInviteGids(rig);
          return gids.includes(gid) || undefined;
        }, `wn groups invites shows ${gid}`);
        await wn(rig, ["groups", "accept", wnGid], rig.wnNpub);
        await waitFor(async () => {
          const roster = await client5.group(gid);
          if (roster === undefined) {
            return undefined;
          }
          return roster.members.some((entry) => entry.account === rig.wnPub) || undefined;
        }, "wn member in m3b-22 roster");
        await sleep(2000);
        log(`created ${wnGid} with wn member (${secs(at)})`);

        // Withhold the rename's commit: armed just before the call, the
        // port reports the publish accepted but never forwards the event.
        at = Date.now();
        drop.armed = true;
        await client5.updateGroupProfile(gid, { name: "m3b-22-moved" });
        await waitFor(() => drop.id !== "" || undefined, "rename commit withheld");
        await client5.sendMessage(gid, { kind: CHAT_KIND, tags: [], content: "s22-unreadable" });
        await sleep(4000);
        log(`withheld one commit; wn is stuck (${secs(at)})`);

        // Whatever wn does about being stuck (its kind-1159 recovery
        // requests, if it issues them), our side stays inert: the route
        // must gain no further events authored by our session.
        const route5 = snapshot5.live?.nostrGroupId;
        if (route5 === undefined) {
          throw new Error("device 5 route missing");
        }
        const routeIds = () =>
          new Set(
            served.relay
              .events()
              .filter(
                (stored) =>
                  stored.kind === 445 &&
                  stored.tags.some((tag) => tag[0] === "h" && tag[1] === route5),
              )
              .map((stored) => stored.id),
          );
        const baselineIds = routeIds();
        await sleep(6000);
        const gained = [...routeIds()].filter((id) => !baselineIds.has(id));
        if (gained.length > 0) {
          throw new Error(`a recovery request staged ${gained.length} event(s)`);
        }

        // Re-add the stuck account: `removed` — the Invite follows once
        // the removal commit confirms.
        const outcome = await client5.readdMember(gid, rig.wnPub, [url]);
        if (outcome !== "removed") {
          throw new Error(`readdMember outcome ${outcome}, expected removed`);
        }

        // wn's stalled copy holds the offer: `recovery-status` lists it
        // with the token `confirm-rejoin` needs.
        const offerInfo = await waitFor(async () => {
          const reply = await wnTry(rig, ["groups", "recovery-status", wnGid], rig.wnNpub);
          const offers = ptrArr(reply.parsed, "/result/rejoin_invitations");
          const [first] = offers;
          if (first === undefined) {
            return undefined;
          }
          const welcomeId = ptrStr(first, "/welcome_id_hex");
          const token = ptrStr(first, "/local_state_token");
          return welcomeId !== undefined && token !== undefined ? { welcomeId, token } : undefined;
        }, "wn rejoin offer in recovery-status");
        await wn(
          rig,
          [
            "groups",
            "confirm-rejoin",
            offerInfo.welcomeId,
            "--local-state-token",
            offerInfo.token,
            "--confirm",
          ],
          rig.wnNpub,
        );
        log(`wn confirmed the rejoin (${secs(at)})`);

        // The round trip recovers both ways.
        const ourText = `s22-us-${Math.floor(Date.now() / 1000)}`;
        await client5.sendMessage(gid, { kind: CHAT_KIND, tags: [], content: ourText });
        await waitFor(async () => {
          const entries = await wnMessages(rig, wnGid);
          return entries.some((entry) => entry.includes(ourText)) || undefined;
        }, "wn reads us after re-add");
        const wnText = `s22-wn-${Math.floor(Date.now() / 1000)}`;
        await wn(rig, ["messages", "send", wnGid, wnText], rig.wnNpub);
        await waitFor(
          () =>
            received5.some((entry) => entry.group === gid && entry.message.content === wnText) ||
            undefined,
          "we read wn after re-add",
        );
      } finally {
        await client5.close();
      }
    });

    // S21 — post-join self-update: the Pump reports history_fetched at
    // EOSE; quiet + jitter gate one commit wn accepts both ways.
    await scenario("S21 post-join self-update", async () => {
      let at = Date.now();
      const created = await wn(rig, ["groups", "create", "m3b-21", ourNpub], rig.wnNpub);
      const wnGid = wnGroupId(created);
      const ourGid = await waitFor(
        () =>
          invites.flatMap((event) =>
            event.type === "joined" && event.group === wnGid ? [event.group] : [],
          )[0],
        "our onInvite joined (s21 group)",
      );
      if (ourGid !== wnGid) {
        throw new Error(`group id mismatch: ours ${ourGid} wn ${wnGid}`);
      }
      log(`joined ${ourGid}; waiting for the maintenance self-update (${secs(at)})`);

      at = Date.now();
      await waitFor(
        () =>
          groupChanges.flatMap((event) =>
            event.type === "self_updated" && event.group === ourGid ? [event] : [],
          )[0] ?? undefined,
        "self_updated group-change event",
        240_000,
      );
      log(`self_updated for ${ourGid} (${secs(at)})`);

      const wnText = `s21-wn-${Math.floor(Date.now() / 1000)}`;
      await wn(rig, ["messages", "send", wnGid, wnText], rig.wnNpub);
      await waitFor(
        () =>
          received.some((entry) => entry.group === ourGid && entry.message.content === wnText) ||
          undefined,
        `we receive wn message ${wnText}`,
      );
      const ourText = `s21-us-${Math.floor(Date.now() / 1000)}`;
      await client.sendMessage(ourGid, { kind: CHAT_KIND, tags: [], content: ourText });
      await waitFor(async () => {
        const entries = await wnMessages(rig, wnGid);
        return entries.some((entry) => entry.includes(ourText)) || undefined;
      }, `wn messages list shows ${ourText}`);
    });

    /** A sibling mk device of our account (same signer, fresh stores). */
    const openSibling = async (
      deviceSigner = signer,
    ): Promise<{
      mk: MarmotClient;
      invites: InviteEvent[];
      changes: GroupEvent[];
      received2: ClientMessage[];
    }> => {
      const backendN: Backend = await releaseBackend();
      const mk = await MarmotClient.open({
        backend: backendN,
        kv: memoryKv(),
        cursors: memoryCursorStore(),
        signer: deviceSigner,
        clock: {
          now: () => ({
            unixMs: BigInt(Date.now()),
            monotonicMs: BigInt(Math.max(0, Date.now() - startMonotonic)),
          }),
        },
        timer,
        relays: nostrRelayPort(pool),
        inbox: "client",
        lookupRelays: [url],
        allowLoopback: true,
      });
      const invitesN: InviteEvent[] = [];
      const changesN: GroupEvent[] = [];
      const receivedN: ClientMessage[] = [];
      mk.onInvite((event) => {
        invitesN.push(event);
      });
      mk.onGroupChange((event) => {
        changesN.push(event);
      });
      mk.onMessages((batch) => {
        receivedN.push(...batch);
      });
      return { mk, invites: invitesN, changes: changesN, received2: receivedN };
    };

    // S14 — wn invites an account holding one wn device (a second wnd
    // home on the same nsec) and one mk device; both join, the account
    // shows devices 2, messages flow both ways from both devices.
    // A fresh secret keeps the leaf count exact: the shared signer's
    // older slots would be invited too and inflate `devices`.
    await scenario("S14 wn invites our wn sibling + mk device", async () => {
      const secret14 = generateSecretKey();
      const signer14 = testSigner(secret14);
      const npub14 = npubEncode(signer14.pubkey);
      const wn2 = await spawnWn("s14", url);
      const device = await openSibling(signer14);
      try {
        await pool.publish(
          [url],
          finalizeEvent(
            {
              kind: RELAY_LIST_KIND,
              created_at: Math.floor(Date.now() / 1000),
              tags: [["r", url]],
              content: "",
            },
            secret14,
          ),
        );
        await wnStdin(wn2, ["login", "--nsec-stdin"], nsecEncode(secret14));
        const a2npub = await wnAccountNpub(wn2);
        if (a2npub !== npub14) {
          throw new Error(`wn sibling npub ${a2npub} != ours ${npub14}`);
        }
        await wn(wn2, ["keys", "publish"], a2npub);
        await device.mk.ensureKeyPackage();
        await device.mk.publishInboxRelays([url]);

        const created = await wn(rig, ["groups", "create", "m3b-14", npub14], rig.wnNpub);
        const wnGid = wnGroupId(created);
        const gid = await waitFor(
          () =>
            device.invites.flatMap((event) =>
              event.type === "joined" && event.group === wnGid ? [event.group] : [],
            )[0],
          "mk device joined",
        );
        await waitFor(async () => {
          const reply = await wnTry(wn2, ["groups", "invites"], a2npub);
          if (reply.ok && JSON.stringify(reply.parsed).includes(wnGid)) {
            await wn(wn2, ["groups", "accept", wnGid], a2npub);
            return true;
          }
          return undefined;
        }, "wn sibling accepts the invite");

        await waitFor(async () => {
          const snap = await device.mk.group(gid);
          const member = snap?.members.find((entry) => entry.account === signer14.pubkey);
          return member?.devices === 2 || undefined;
        }, "our account shows devices == 2");

        await device.mk.sendMessage(gid, { kind: CHAT_KIND, tags: [], content: "s14-mk-to-wn" });
        await waitFor(async () => {
          const list = await wnTry(
            wn2,
            ["messages", "list", wnGid, "--limit", String(WN_MESSAGE_LIMIT)],
            a2npub,
          );
          return (list.ok && JSON.stringify(list.parsed).includes("s14-mk-to-wn")) || undefined;
        }, "wn sibling lists our message");
        await wn(wn2, ["messages", "send", wnGid, "s14-sibling-to-mk"], a2npub);
        await waitFor(
          () =>
            device.received2.some(
              (entry) => entry.group === gid && entry.message.content === "s14-sibling-to-mk",
            ) || undefined,
          "mk decodes the sibling's message",
        );
      } finally {
        await device.mk.close();
        await killWn(wn2);
      }
    });

    // S15 — we invite an account holding two wn devices (two wnd homes
    // on one nsec; `wn devices link` when the CLI flow completes, else
    // unlinked). Both get Welcomes and list the chat; round trip.
    await scenario("S15 we invite two wn devices of one account", async () => {
      const nsec15 = generateSecretKey();
      const npub15 = npubEncode(getPublicKey(nsec15));
      const wn1 = await spawnWn("s15-d1", url);
      const wn2 = await spawnWn("s15-d2", url);
      try {
        await wnStdin(wn1, ["login", "--nsec-stdin"], nsecEncode(nsec15));
        const npub1 = await wnAccountNpub(wn1);
        if (npub1 !== npub15) {
          throw new Error(`wn device 1 npub ${npub1} != ${npub15}`);
        }
        await wn(wn1, ["keys", "publish"], npub15);

        await wnStdin(wn2, ["login", "--nsec-stdin"], nsecEncode(nsec15));
        await wn(wn2, ["keys", "publish"], npub15);
        // The link flow is a SHOULD in the design: run it when the CLI
        // cooperates; an unlinked second installation also counts.
        let linked = false;
        const start = await wnTry(wn2, ["devices", "link", "start"], npub15);
        if (start.ok) {
          let prompt: string | undefined;
          try {
            prompt = await waitFor(
              async () => {
                const status = await wnTry(wn1, ["devices", "link", "status"], npub15);
                if (!status.ok) {
                  return undefined;
                }
                const text = JSON.stringify(status.parsed);
                const found = /"prompt_?id"\s*:\s*"([^"]+)"/.exec(text);
                return found?.[1];
              },
              "link prompt on the existing device",
              30_000,
            );
          } catch (error) {
            log(
              `link prompt never appeared: ${error instanceof Error ? error.message : String(error)}`,
            );
          }
          const matched =
            prompt === undefined
              ? { ok: false, raw: "no prompt" }
              : await wnTry(wn2, ["devices", "link", "confirm-match"], npub15);
          if (matched.ok && prompt !== undefined) {
            const approved = await wnTry(
              wn1,
              ["devices", "link", "approve", prompt, "--label", "second"],
              npub15,
            );
            linked = approved.ok;
            if (!approved.ok) {
              log(`link approve refused: ${approved.raw}`);
            }
          } else {
            log(`link confirm-match refused: ${matched.raw}`);
          }
        } else {
          log(`link start refused: ${start.raw}`);
        }
        log(`S15 second wn device ${linked ? "linked" : "unlinked"}`);

        // Both devices' KeyPackages land as invitees; both accept and
        // list the chat.
        const kps = await client.lookupInvitees(getPublicKey(nsec15), [url]);
        log(
          `S15 invitees: ${kps.length} (${kps.map((i) => i.keyPackage.id.slice(0, 8)).join(",")})`,
        );
        // Let both wnd workers open the account's inbox subscription
        // before the Welcomes publish — a wrap older than the `since`
        // cursor is never replayed to them.
        await sleep(5000);
        const created = await client.createGroup({
          name: "m3b-15",
          description: "",
          relays: [url],
          invitees: kps,
          admins: [],
          components: [],
        });
        const gid = created.id;
        // A Welcome lands as a pending invite; when the devices are
        // linked, the first device's accept fans the joined chat out to
        // its sibling through the device group, so the sibling may go
        // straight to `chats list` without an invite of its own.
        for (const home of [wn1, wn2]) {
          // oxlint-disable-next-line no-await-in-loop -- device 2's fan-out follows device 1's join
          await waitFor(
            async () => {
              const chats = await wnTry(home, ["chats", "list"], npub15);
              if (chats.ok && JSON.stringify(chats.parsed).includes(gid)) {
                return true;
              }
              const reply = await wnTry(home, ["groups", "invites"], npub15);
              if (reply.ok && JSON.stringify(reply.parsed).includes(gid)) {
                await wn(home, ["groups", "accept", gid], npub15);
                await wnTry(home, ["sync"], npub15);
              }
              return undefined;
            },
            `wn device joins and lists ${gid.slice(0, 8)}`,
          );
        }
        await client.sendMessage(gid, { kind: CHAT_KIND, tags: [], content: "s15-mk-to-wn" });
        for (const home of [wn1, wn2]) {
          // oxlint-disable-next-line no-await-in-loop -- sequential wn polls
          await waitFor(async () => {
            const list = await wnTry(
              home,
              ["messages", "list", gid, "--limit", String(WN_MESSAGE_LIMIT)],
              npub15,
            );
            return (list.ok && JSON.stringify(list.parsed).includes("s15-mk-to-wn")) || undefined;
          }, "wn device lists our message");
        }
        await wn(wn1, ["messages", "send", gid, "s15-d1-reply"], npub15);
        await wn(wn2, ["messages", "send", gid, "s15-d2-reply"], npub15);
        for (const wanted of ["s15-d1-reply", "s15-d2-reply"]) {
          // oxlint-disable-next-line no-await-in-loop -- sequential wn polls
          await waitFor(
            () =>
              received.some((entry) => entry.group === gid && entry.message.content === wanted) ||
              undefined,
            `mk decodes ${wanted}`,
          );
        }
      } finally {
        await killWn(wn1);
        await killWn(wn2);
      }
    });

    // S16 — includeOwnDevices founders our second mk device; it joins
    // from its own Welcome and the account shows devices 2. A fresh
    // secret keeps the leaf count exact.
    await scenario("S16 createGroup includes our own device", async () => {
      const secret16 = generateSecretKey();
      const signer16 = testSigner(secret16);
      const primary = await openSibling(signer16);
      const sibling = await openSibling(signer16);
      try {
        await pool.publish(
          [url],
          finalizeEvent(
            {
              kind: RELAY_LIST_KIND,
              created_at: Math.floor(Date.now() / 1000),
              tags: [["r", url]],
              content: "",
            },
            secret16,
          ),
        );
        await primary.mk.ensureKeyPackage();
        await primary.mk.publishInboxRelays([url]);
        await sibling.mk.ensureKeyPackage();
        await sibling.mk.publishInboxRelays([url]);
        await waitFor(
          () =>
            new Set(
              served.relay
                .events()
                .filter(
                  (stored) => stored.kind === KEY_PACKAGE_KIND && stored.pubkey === signer16.pubkey,
                )
                .map((stored) => stored.id),
            ).size >= 2 || undefined,
          "both devices' KeyPackages on the relay",
        );
        const wnKps = await primary.mk.lookupInvitees(rig.wnPub, [url]);
        const created = await primary.mk.createGroup({
          name: "m3b-16",
          description: "",
          relays: [url],
          invitees: wnKps,
          admins: [],
          components: [],
        });
        const gid = created.id;
        const joined = await waitFor(
          () =>
            sibling.invites.flatMap((event) =>
              event.type === "joined" && event.group === gid ? [event.group] : [],
            )[0],
          "sibling joined from own_devices",
        );
        if (joined !== gid) {
          throw new Error(`sibling joined ${joined}, expected ${gid}`);
        }
        await waitFor(async () => {
          const snap = await primary.mk.group(gid);
          const member = snap?.members.find((entry) => entry.account === signer16.pubkey);
          return member?.devices === 2 || undefined;
        }, "our account shows devices == 2");
        await waitFor(async () => {
          const reply = await wn(rig, ["groups", "members", gid], rig.wnNpub);
          const entries = ptrArr(reply, "/result/members").filter((item) =>
            JSON.stringify(item).includes(signer16.pubkey),
          );
          return entries.length === 1 || undefined;
        }, "wn members view shows the account once");
        await sibling.mk.sendMessage(gid, {
          kind: CHAT_KIND,
          tags: [],
          content: "s16-a2-to-wn",
        });
        await waitFor(
          () =>
            primary.changes.some(
              (event) => event.group === gid && event.type === "epoch_changed",
            ) || undefined,
          "primary applies the sibling's message",
        );
      } finally {
        await sibling.mk.close();
        await primary.mk.close();
      }
    });

    // S18 — wn admin removes our account: every leaf goes in one
    // Commit and both mk devices end access == "removed".
    await scenario("S18 wn removes our whole account", async () => {
      const sibling = await openSibling();
      try {
        const knownKps = new Set(
          served.relay
            .events()
            .filter((stored) => stored.kind === KEY_PACKAGE_KIND && stored.pubkey === signer.pubkey)
            .map((stored) => stored.id),
        );
        await sibling.mk.ensureKeyPackage();
        await sibling.mk.publishInboxRelays([url]);
        await waitFor(
          () =>
            served.relay
              .events()
              .some(
                (stored) =>
                  stored.kind === KEY_PACKAGE_KIND &&
                  stored.pubkey === signer.pubkey &&
                  !knownKps.has(stored.id),
              ) || undefined,
          "sibling's KeyPackage on the relay",
        );
        const created = await wn(rig, ["groups", "create", "m3b-18", ourNpub], rig.wnNpub);
        const wnGid = wnGroupId(created);
        const gid1 = await waitFor(
          () =>
            invites.flatMap((event) =>
              event.type === "joined" && event.group === wnGid ? [event.group] : [],
            )[0],
          "device 1 joined",
        );
        const gid2 = await waitFor(
          () =>
            sibling.invites.flatMap((event) =>
              event.type === "joined" && event.group === wnGid ? [event.group] : [],
            )[0],
          "device 2 joined",
        );
        await wn(rig, ["groups", "remove-members", wnGid, ourNpub], rig.wnNpub);
        await waitFor(async () => {
          const snap = await client.group(gid1);
          return snap?.access === "removed" || undefined;
        }, "device 1 access == removed");
        await waitFor(async () => {
          const snap = await sibling.mk.group(gid2);
          return snap?.access === "removed" || undefined;
        }, "device 2 access == removed");
      } finally {
        await sibling.mk.close();
      }
    });

    // S23 — a host-owned inbox end to end: a fresh account whose client never
    // subscribes kind 1059. The test host runs the NIP-17 inbox instead — it
    // publishes its own 10050, unwraps gift wraps itself, and forwards the
    // ones carrying a kind-444 rumor through `ingestWrap`.
    await scenario("S23 wn invites a host-owned-inbox device", async () => {
      let at: number;
      const hostSecret = generateSecretKey();
      const hostSigner = testSigner(hostSecret);
      const hostNpub = npubEncode(hostSigner.pubkey);
      log(`host-owned npub ${hostNpub}`);

      const hostSubs: Array<{ relay: string; filters: RelayFilter[] }> = [];
      const innerPort = nostrRelayPort(pool);
      const hostPort: RelayPort = {
        publish: async (relay, event) => innerPort.publish(relay, event),
        connect: async (relay) => innerPort.connect(relay),
        subscribe: (relay, filters, handlers) => {
          hostSubs.push({ relay, filters });
          return innerPort.subscribe(relay, filters, handlers);
        },
      };
      const hostClient = await MarmotClient.open({
        backend: await releaseBackend(),
        kv: memoryKv(),
        cursors: memoryCursorStore(),
        signer: hostSigner,
        clock: {
          now: () => ({
            unixMs: BigInt(Date.now()),
            monotonicMs: BigInt(Math.max(0, Date.now() - startMonotonic)),
          }),
        },
        timer,
        relays: hostPort,
        inbox: "host",
        lookupRelays: [url],
        allowLoopback: true,
      });
      const hostReceived: ClientMessage[] = [];
      const hostInvites: InviteEvent[] = [];
      hostClient.onMessages((batch) => {
        hostReceived.push(...batch);
      });
      hostClient.onInvite((event) => {
        hostInvites.push(event);
      });
      hostClient.onError((error) => {
        log(`host-owned client onError ${error.kind}: ${error.message}`);
      });
      try {
        // Identity: 10002 and the KeyPackage as usual; the 10050 is the host's
        // own — `publishInboxRelays` is `wrong_inbox_owner` in host mode.
        const relayList = finalizeEvent(
          {
            kind: RELAY_LIST_KIND,
            created_at: Math.floor(Date.now() / 1000),
            tags: [["r", url]],
            content: "",
          },
          hostSecret,
        );
        await pool.publish([url], relayList);
        await hostClient.ensureKeyPackage();
        const inboxRelays = finalizeEvent(
          {
            kind: INBOX_RELAYS_KIND,
            created_at: Math.floor(Date.now() / 1000),
            tags: [["relay", url]],
            content: "",
          },
          hostSecret,
        );
        await pool.publish([url], inboxRelays);
        await waitFor(
          () =>
            served.relay
              .events()
              .some(
                (stored) => stored.kind === KEY_PACKAGE_KIND && stored.pubkey === hostSigner.pubkey,
              ) || undefined,
          "host-owned account key package stored",
        );

        // The host's NIP-17 inbox: its own kind-1059 #p subscription, unwrapped
        // with nostr-tools; only kind-444 rumors are forwarded — untouched —
        // to `ingestWrap`.
        const reports: InputReport[] = [];
        const closeInbox = pool.subscribe(
          [url],
          [{ kinds: [GIFT_WRAP_KIND], "#p": [hostSigner.pubkey] }],
          {
            onEvent: (event) => {
              let rumor: ReturnType<typeof nip59.unwrapEvent>;
              try {
                rumor = nip59.unwrapEvent(
                  {
                    id: event.id,
                    pubkey: event.pubkey,
                    created_at: event.created_at,
                    kind: event.kind,
                    tags: event.tags.map((tag) => [...tag]),
                    content: event.content,
                    sig: event.sig,
                  },
                  hostSecret,
                );
              } catch {
                return;
              }
              if (rumor.kind !== WELCOME_RUMOR_KIND) {
                return;
              }
              void (async () => {
                try {
                  reports.push(
                    await hostClient.ingestWrap({
                      id: event.id,
                      pubkey: event.pubkey,
                      createdAt: BigInt(event.created_at),
                      kind: event.kind,
                      tags: event.tags.map((tag) => [...tag]),
                      content: event.content,
                      sig: event.sig,
                    }),
                  );
                } catch (error) {
                  log(
                    `host ingestWrap threw: ${error instanceof Error ? error.message : String(error)}`,
                  );
                }
              })();
            },
          },
        );
        try {
          at = Date.now();
          const created = await wn(rig, ["groups", "create", "m3b-23", hostNpub], rig.wnNpub);
          const wnGid = wnGroupId(created);
          log(`wn groups create m3b-23 -> ${wnGid} (${secs(at)})`);

          await waitFor(() => reports[0], "host ingestWrap report");
          const [report] = reports;
          if (report === undefined || report.outcome.type !== "accepted") {
            throw new Error(`ingestWrap outcome ${JSON.stringify(report?.outcome)}`);
          }
          log(`ingestWrap accepted the forwarded wrap (${secs(at)})`);

          at = Date.now();
          await waitFor(
            () => hostInvites.some((event) => event.type === "joined" && event.group === wnGid),
            "host-owned joined invite",
          );
          log(`host-owned joined ${wnGid} (${secs(at)})`);

          at = Date.now();
          const wnText = `s23-wn-${Math.floor(Date.now() / 1000)}`;
          await wn(rig, ["messages", "send", wnGid, wnText], rig.wnNpub);
          await waitFor(
            () =>
              hostReceived.some(
                (entry) => entry.group === wnGid && entry.message.content === wnText,
              ) || undefined,
            `host-owned client receives wn message ${wnText}`,
          );
          log(`host-owned received wn message ${wnText} (${secs(at)})`);

          at = Date.now();
          const ourText = `s23-us-${Math.floor(Date.now() / 1000)}`;
          await hostClient.sendMessage(wnGid, { kind: CHAT_KIND, tags: [], content: ourText });
          await waitFor(async () => {
            const entries = await wnMessages(rig, wnGid);
            return entries.some((entry) => entry.includes(ourText)) || undefined;
          }, `wn messages list shows ${ourText}`);
          log(`wn messages list shows ${ourText} (${secs(at)})`);

          if (
            hostSubs.some((sub) =>
              sub.filters.some((filter) => (filter.kinds ?? []).includes(GIFT_WRAP_KIND)),
            )
          ) {
            throw new Error("host-owned client's port subscribed kind 1059");
          }
          log("host-owned client's port never subscribed kind 1059");
        } finally {
          closeInbox.close();
        }
      } finally {
        await hostClient.close();
      }
    });

    log(`NIP-11 documents served: ${served.documentsServed()}`);
    if (RELAY_SHAPE === "nip11" && served.documentsServed() === 0) {
      failures.push("relay shape: wnd never asked for the NIP-11 document");
    }
    log(
      `${failures.length === 0 ? "ALL PASS" : `FAILURES: ${failures.join(", ")}`} ` +
        `(${secs(startMs)} total)`,
    );
    if (clientErrors.length > 0) {
      log(`note: ${clientErrors.length} client error(s) during the run`);
    }
    if (failures.length > 0) {
      throw new Error(`${failures.length} scenario(s) failed`);
    }
  } finally {
    try {
      await client2Ref?.close();
    } catch (error) {
      log(`client2 close error: ${error instanceof Error ? error.message : String(error)}`);
    }
    try {
      await clientRef?.close();
    } catch (error) {
      log(`client close error: ${error instanceof Error ? error.message : String(error)}`);
    }
    poolRef?.close();
    for (const entry of extraWns) {
      try {
        // oxlint-disable-next-line no-await-in-loop -- sequential reap
        await killWn(entry);
      } catch (error) {
        log(`extra wnd cleanup error: ${error instanceof Error ? error.message : String(error)}`);
      }
    }
    if (wnd.exitCode === null && !wnd.killed) {
      wnd.kill("SIGKILL");
    }
    await waitFor(
      () => wnd.exitCode !== null || wnd.signalCode !== null || undefined,
      "wnd exit",
      10_000,
    ).catch(() => {
      log("wnd did not exit within 10s of SIGKILL");
    });
    closeSync(outFd);
    closeSync(errFd);
    rmSync(home, { recursive: true, force: true });
    await served.close();
    log(`cleanup done (wnd stopped, home ${home} removed)`);
  }
}

try {
  await run();
} catch (error) {
  const detail = error instanceof Error ? (error.stack ?? error.message) : String(error);
  log(`FATAL: ${detail}`);
  process.exitCode = 1;
}
