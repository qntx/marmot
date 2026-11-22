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

import { finalizeEvent, generateSecretKey } from "nostr-tools";

import type { Backend } from "@qntx/marmot/backend";
import { MarmotClient, nostrRelayPort, MarmotError } from "@qntx/marmot/client";
import type { ClientMessage, GroupEvent, InviteEvent, Timer } from "@qntx/marmot/client";
import { memoryCursorStore, memoryKv } from "@qntx/marmot/testing";
import { Pool } from "@qntx/nostr";
import { decode, npubEncode } from "@qntx/nostr/nips/nip19";
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
        before.state === "removed" ||
        !before.members.some((member) => member.account === signer.pubkey)
      ) {
        const detail =
          before === undefined
            ? "snapshot absent"
            : `state=${before.state}, self in roster=${String(
                before.members.some((member) => member.account === signer.pubkey),
              )}`;
        throw new Error(`S3 precondition failed: ${detail}`);
      }
      log(`precondition: group state=${before.state}, self in roster (${secs(at)})`);

      at = Date.now();
      await wn(rig, ["groups", "remove-members", s1.wnGid, ourNpub], rig.wnNpub);
      log(`wn groups remove-members ${s1.wnGid} ${ourNpub} (${secs(at)})`);

      at = Date.now();
      const observation = await waitFor(async () => {
        const evidence: string[] = [];
        const snapshot = await client.group(s1.ourGid);
        if (snapshot === undefined || snapshot.state === "removed") {
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
      ).then(
        (event) => (event?.type === "local_copy_terminated" ? event : undefined),
        () => undefined,
      );
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
      if (snap1?.avatar?.url !== "https://example.com/m5a.png") {
        throw new Error(`our avatar snapshot ${JSON.stringify(snap1?.avatar)}`);
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
      if (snap2?.avatar?.url !== "https://example.com/wn.png") {
        throw new Error(`snapshot avatar ${JSON.stringify(snap2?.avatar)}`);
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
          snap?.members.some((member) => member.account === rig.wnPub && member.admin) === true ||
          undefined
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
        return snap?.ownAccountAdmin === true || undefined;
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
        return snap?.ownAccountAdmin === false || undefined;
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
        return snap === undefined || snap.state === "removed" || undefined;
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
        return snap?.state === "disbanded" || undefined;
      }, "our snapshot disbanded");
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
      await clientRef?.close();
    } catch (error) {
      log(`client close error: ${error instanceof Error ? error.message : String(error)}`);
    }
    poolRef?.close();
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
