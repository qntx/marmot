/**
 * Phase D interop — real White Noise `wn`/`wnd` against `MarmotClient` on the release wasm backend
 * over one in-process websocket relay (`serveFakeRelay`). The expected wn version is `mdk.tag` from
 * `upstream.json` without the leading `v` (currently `wn 0.17.0`). Mirrors
 * `crates/interop/tests/support/mod.rs` (CLI flags, daemon env, JSON pointers) and the scenario
 * flow of `crates/interop/tests/wn.rs`. Every step, timing and PASS/FAIL line is appended to
 * `<repo>/target/interop-m3b.log`; the script exits non-zero on any failure. It needs local wn/wnd
 * binaries, so it is not part of `test:wasm` or CI.
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
import { join, resolve } from "node:path";

import { finalizeEvent, generateSecretKey } from "nostr-tools";

import type { Backend } from "@qntx/marmot/backend";
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

const WN_BIN = process.env["WN_BIN"] ?? "/Users/xu/Desktop/qntx/marmot/spike/target/wn/bin/wn";
const WND_BIN = process.env["WND_BIN"] ?? "/Users/xu/Desktop/qntx/marmot/spike/target/wn/bin/wnd";
const REPO_ROOT = resolve(import.meta.dirname, "../../..");
const LOG_PATH = join(REPO_ROOT, "target", "interop-m3b.log");

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

async function run(): Promise<void> {
  mkdirSync(join(REPO_ROOT, "target"), { recursive: true });
  log("m3b phase D interop: wn/wnd vs MarmotClient (release wasm)");

  // The pinned MDK tag (upstream.json `mdk.tag`, minus the leading `v`) is the wn release we
  // interoperate with — the check fails loudly when the local binary is a different cohort.
  const upstream: unknown = JSON.parse(readFileSync(join(REPO_ROOT, "upstream.json"), "utf8"));
  if (
    typeof upstream !== "object" ||
    upstream === null ||
    !("mdk" in upstream) ||
    typeof upstream.mdk !== "object" ||
    upstream.mdk === null ||
    !("tag" in upstream.mdk) ||
    typeof upstream.mdk.tag !== "string" ||
    !upstream.mdk.tag.startsWith("v")
  ) {
    throw new Error("upstream.json: mdk.tag must be a v-prefixed release tag");
  }
  const expectedWn = `wn ${upstream.mdk.tag.slice(1)}`;
  const version = spawnSync(WN_BIN, ["--version"], { encoding: "utf8" });
  const versionText = `${version.stdout ?? ""}${version.stderr ?? ""}`.trim();
  log(`wn --version: ${versionText}`);
  if (!versionText.includes(expectedWn)) {
    throw new Error(`expected "${expectedWn}", got: ${versionText || "(no output)"}`);
  }

  const served = await serveFakeRelay({ host: "127.0.0.1", port: 0 });
  const url = served.url.replace(/\/$/, "");
  log(`relay: ${served.url}`);

  // wnd rejects a home under world-writable /tmp ("untrusted directory alias"); the repo's
  // target/ is user-owned and keeps the socket path short (macOS ~104-char limit).
  const home = mkdtempSync(join(REPO_ROOT, "target", "wn-home-"));
  const socket = join(home, "w.sock");
  const logsDir = join(REPO_ROOT, "target", "wn-logs", `m3b-${process.pid}`);
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
      allowLoopback: true,
    });
    clientRef = client;

    const received: ClientMessage[] = [];
    const invites: InviteEvent[] = [];
    const groupChanges: GroupEvent[] = [];
    const clientErrors: MarmotError[] = [];
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
        log(`FAIL ${name} (${secs(start)}): ${detail}`);
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
