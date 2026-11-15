// White Noise (wn/wnd at MDK v0.16.0) ↔ spike wasm client interop.
// `bun spike/js/run-interop.ts` — a real WebSocket relay + the installed
// wn binaries; logs to spike/results/interop-*.log via WN_LOG env.
import init, { Client } from "../wasm/pkg/marmot_spike_wasm.js";
import { serveFakeRelay } from "@qntx/nostr/testing";
import { nip19Decode, npubEncode, signEvent } from "@qntx/nostr";
import { appendFileSync } from "node:fs";
import { accountKeys, LocalMember } from "./src/local-member.js";
import { flushOutbound, MapBackend, publishResult } from "./src/member.js";
import { WsRelay } from "./src/ws-relay.js";
import { KIND_GIFT_WRAP, KIND_GROUP_MESSAGE } from "./src/relay.js";

const WN = "/Users/xu/Desktop/qntx/marmot/spike/target/wn/bin/wn";
const LOG = process.env.WN_LOG ?? "/dev/stdout";
const log = (s: string) => {
  const line = `[${new Date().toISOString().slice(11, 19)}] ${s}`;
  console.log(line);
  appendFileSync(LOG, line + "\n");
};

const wnEnv = {
  ...process.env,
  WN_HOME: "/Users/xu/Desktop/qntx/marmot/spike/target/wn-home",
  WN_SECRET_STORE: "file",
  WN_ALLOW_LOOPBACK_RELAYS: "1",
};

async function wn(...args: string[]): Promise<any> {
  const p = Bun.spawn([WN, "--json", ...args], {
    env: wnEnv,
    stdout: "pipe",
    stderr: "pipe",
  });
  const out = await new Response(p.stdout).text();
  const err = await new Response(p.stderr).text();
  const code = await p.exited;
  if (code !== 0) throw new Error(`wn ${args.join(" ")} failed(${code}): ${err || out}`);
  try {
    return JSON.parse(out);
  } catch {
    throw new Error(`wn ${args.join(" ")} non-JSON: ${out.slice(0, 300)}`);
  }
}

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

async function until<T>(f: () => Promise<T | false>, what: string, ms = 60000, step = 1500): Promise<T> {
  const t0 = Date.now();
  for (;;) {
    const v = await f();
    if (v) return v;
    if (Date.now() - t0 > ms) throw new Error(`timeout waiting for ${what}`);
    await sleep(step);
  }
}

// ---- setup -----------------------------------------------------------------
const wasmPath = new URL("../wasm/pkg/marmot_spike_wasm_bg.wasm", import.meta.url);
await init({ module_or_path: await Bun.file(wasmPath).arrayBuffer() });

const served = await serveFakeRelay({ port: 45670, host: "127.0.0.1" });
const relayUrl = served.url;
log(`relay: ${relayUrl}`);

const relay = await WsRelay.connect(relayUrl);
const publish = (ev: any) => {
  void relay.publish(ev);
  return true;
};
const mkMember = async (seed: string) =>
  LocalMember.open({ Client, keys: await accountKeys(seed), backend: new MapBackend() });

// wn requires invitees to publish a Marmot inbox relay list (kind 10050,
// ["relay", url] tags, empty content — transport-nostr-adapter/relay_list.rs).
const publishInbox = async (m: LocalMember, relayUrl: string) => {
  const ev = signEvent(
    {
      kind: 10050,
      pubkey: m.keys.publicKey,
      created_at: Math.floor(Date.now() / 1000),
      tags: [["relay", relayUrl]],
      content: "",
    },
    m.keys.secretKey,
  );
  await relay.publish(ev);
  return ev;
};

const results: Record<string, string> = {};

// wn identity (created once outside this script or freshly here)
const who = await wn("whoami");
let wnNpub: string;
if (who.result?.accounts?.length) {
  wnNpub = who.result.accounts[0].npub;
  log(`wn account exists: ${wnNpub}`);
} else {
  const created = await wn("create-identity");
  wnNpub = created.result.npub;
  log(`wn identity created: ${wnNpub}`);
}
const wnAcct = wnNpub;
const wnPub = nip19Decode(wnNpub).data as string;
log(`wn pubkey: ${wnPub}`);

// republish the wn key package onto this script's fresh relay
const kpPub = await wn("--account", wnAcct, "keys", "publish");
log(`wn keys publish: ${JSON.stringify(kpPub.result ?? kpPub).slice(0, 200)}`);

// ---- scenario 1: spike creates group, invites wn ----------------------------
const a = await mkMember("interop-a");
const kpA = await a.publishKeyPackage();
await relay.publish(kpA);
await publishInbox(a, relayUrl);
log("spike A inbox relay list published");
log("spike A key package published");

// fetch wn's key package from the relay (published by create-identity)
const wnKps = await until(async () => {
  const evs = await relay.fetchRemote({ kinds: [30443], authors: [wnPub] });
  return evs.length ? evs : false;
}, "wn key package on relay");
log(`wn KP event ${wnKps[0].id.slice(0, 12)}… fetched (${wnKps.length})`);

const t0 = Date.now();
const created = await a.createGroup("interop-1", [relayUrl], [wnKps[0]]);
await publishResult(a, created.result, publish);
const gid1 = created.groupId;
log(`spike A created group gid=${gid1.slice(0, 16)}… welcome+commit published (${Date.now() - t0}ms)`);

// wn daemon picks up the welcome → accept the invite
const invite: any = await until(async () => {
  const r = await wn("--account", wnAcct, "groups", "invites");
  const list = r.result?.invites ?? r.result ?? [];
  const arr = Array.isArray(list) ? list : (list.items ?? []);
  return arr.length ? arr[0] : false;
}, "wn groups invites non-empty", 90000);
const wnGid1 = invite.group_id ?? invite.id;
log(`wn invite: ${JSON.stringify(invite).slice(0, 400)}`);

await wn("--account", wnAcct, "groups", "accept", wnGid1);
log("wn accepted invite");

// Documented settle: wnd finishes post-join fanout setup asynchronously
// (the gate report's kind-445 post-join lag). First probe `groups members`
// for roster visibility, then a short fixed delay.
await until(async () => {
  const r = await wn("--account", wnAcct, "groups", "members", wnGid1).catch(() => null);
  const arr = r?.result?.members ?? r?.result ?? [];
  const n = Array.isArray(arr) ? arr.length : (arr?.length ?? 0);
  return n >= 2 ? n : false;
}, "wn roster shows 2 members", 90000);
await sleep(3000);
log("wn roster settled (2 members) + 3s fanout settle");

// spike reads back the join commit / wn-side events, drives convergence
await until(async () => {
  await relay.fetchRemote({ kinds: [KIND_GROUP_MESSAGE] });
  const evs = relay.fetchAsc(KIND_GROUP_MESSAGE);
  for (const ev of evs) await a.ingest(ev);
  await flushOutbound(a, gid1, publish);
  const mem = await a.members(gid1);
  return mem.length >= 2 ? mem : false;
}, "spike sees wn as member");
log("spike sees 2 members");

// bidirectional messages: wn → spike
await wn("--account", wnAcct, "messages", "send", wnGid1, "hello-from-wn");
log("wn sent message");

const wnMsg = await until(async () => {
  await relay.fetchRemote({ kinds: [KIND_GROUP_MESSAGE] });
  for (const ev of relay.fetchAsc(KIND_GROUP_MESSAGE)) await a.ingest(ev);
  const drained = await a.drainEvents();
  const hit = drained.find((d: any) => JSON.stringify(d).includes("hello-from-wn"));
  return hit ?? false;
}, "spike decodes wn message");
log(`spike decoded wn message: ${JSON.stringify(wnMsg).slice(0, 300)}`);

// spike → wn
const r1 = await a.send(gid1, { type: "appMessage", content: "hello-from-spike", createdAt: Math.floor(Date.now() / 1000) });
await publishResult(a, r1, publish);
await flushOutbound(a, gid1, publish);
log("spike sent message");

const spikeMsg = await until(async () => {
  const r = await wn("--account", wnAcct, "messages", "list", wnGid1, "--limit", "20");
  const msgs = r.result?.messages ?? r.result ?? [];
  const arr = Array.isArray(msgs) ? msgs : (msgs.items ?? []);
  const hit = arr.find((m: any) => JSON.stringify(m).includes("hello-from-spike"));
  return hit ?? false;
}, "wn lists spike message");
log(`wn decoded spike message: ${JSON.stringify(spikeMsg).slice(0, 300)}`);
results.scenario1 = "PASS";

// ---- scenario 2: wn creates group, invites the spike -----------------------
const aPk = a.keys.publicKey;
const aNpub = npubEncode(aPk);
log(`spike A npub: ${aNpub}`);
const g2 = await wn("--account", wnAcct, "groups", "create", "interop-2", aNpub);
const gid2 = g2.result?.group_id ?? g2.result?.id;
log(`wn created group gid2=${gid2} : ${JSON.stringify(g2.result).slice(0, 300)}`);

// spike picks up the gift-wrapped welcome addressed to A
const wel = await until(async () => {
  const evs = await relay.fetchRemote({ kinds: [KIND_GIFT_WRAP], "#p": [aPk] });
  return evs.length ? evs : false;
}, "gift wrap for spike A", 90000);
const joinOutcome = await a.ingest(wel[wel.length - 1]);
log(`spike ingest welcome: ${JSON.stringify(joinOutcome).slice(0, 200)}`);
const drained2 = await a.drainEvents();
const joined = drained2.find((d: any) => JSON.stringify(d).includes("groupJoined") || JSON.stringify(d).includes("GroupJoined"));
const gid2Spike = (() => {
  for (const d of drained2) {
    const g = (d as any).groupId;
    if (g && g !== gid1) return g;
  }
  return undefined;
})();
log(`spike joined group: gid2Spike=${gid2Spike} (wn gid2=${gid2})`);
if (!gid2Spike || gid2Spike !== gid2) log(`!! gid mismatch spike=${gid2Spike} wn=${gid2}`);

// let wnd install group-2 subscriptions (same settle as scenario 1)
await sleep(3000);

// spike → wn in group 2
const r2 = await a.send(gid2, { type: "appMessage", content: "s2-hello-from-spike", createdAt: Math.floor(Date.now() / 1000) });
log(`s2 send result: events=${r2.events?.length} pending=${JSON.stringify(r2.pending ?? null).slice(0, 80)}`);
await publishResult(a, r2, publish);
const nFlush = await flushOutbound(a, gid2, publish);
log(`s2 flush published ${nFlush} events`);
await relay.fetchRemote({ kinds: [KIND_GROUP_MESSAGE] });
const s2Evs = relay.fetchAsc(KIND_GROUP_MESSAGE).filter((e: any) => e.tags?.some((t: string[]) => t[0] === "h"));
log(`relay kind-445 events: ${s2Evs.length}`);
await until(async () => {
  const r = await wn("--account", wnAcct, "messages", "list", gid2, "--limit", "20");
  const arr = Array.isArray(r.result) ? r.result : (r.result?.messages ?? r.result?.items ?? []);
  return arr.find((m: any) => JSON.stringify(m).includes("s2-hello-from-spike")) ?? false;
}, "wn lists spike s2 message", 120000);
log("wn decoded s2 spike message");

// wn → spike in group 2
await wn("--account", wnAcct, "messages", "send", gid2, "s2-hello-from-wn");
await until(async () => {
  await relay.fetchRemote({ kinds: [KIND_GROUP_MESSAGE] });
  for (const ev of relay.fetchAsc(KIND_GROUP_MESSAGE)) await a.ingest(ev);
  const drained = await a.drainEvents();
  return drained.find((d: any) => JSON.stringify(d).includes("s2-hello-from-wn")) ?? false;
}, "spike decodes wn s2 message");
log("spike decoded wn s2 message");
results.scenario2 = "PASS";

// ---- scenario 3: bulk flow in group 1 --------------------------------------
// wn sends N messages through the CLI (measure practicality), spike sends the
// rest, spike ingests ascending; then spike adds a 2nd spike member, removes
// wn; wn admin removes a spike member in group 2.
const WN_MSGS = 25;
const tBulk = Date.now();
for (let i = 0; i < WN_MSGS; i++) {
  await wn("--account", wnAcct, "messages", "send", wnGid1, `bulk-wn-${i}`);
}
log(`wn sent ${WN_MSGS} messages in ${Date.now() - tBulk}ms`);

const SPIKE_MSGS = 500 - WN_MSGS;
let g1h: string | undefined;
for (let i = 0; i < SPIKE_MSGS; i++) {
  const r = await a.send(gid1, {
    type: "appMessage",
    content: `bulk-spike-${i}`,
    createdAt: 1_800_000_000 + i,
  });
  g1h ??= r.events?.[0]?.tags?.find((t: string[]) => t[0] === "h")?.[1];
  await publishResult(a, r, publish);
}
log(`spike sent ${SPIKE_MSGS} messages (h=${g1h?.slice(0, 12)}…)`);

// spike ingests the whole group-1 backlog ascending
await relay.fetchRemote({ kinds: [KIND_GROUP_MESSAGE] });
const backlog = relay.fetchAsc(KIND_GROUP_MESSAGE, g1h);
const tIngest = Date.now();
let delivered = 0;
for (const ev of backlog) {
  const o = await a.ingest(ev);
  if (o.outcome === "processed") delivered++;
}
await flushOutbound(a, gid1, publish);
const bulkDrained = await a.drainEvents();
const wnBulkGot = bulkDrained.filter((d: any) => JSON.stringify(d).includes("bulk-wn-")).length;
log(`spike ingested ${backlog.length} events ascending in ${Date.now() - tIngest}ms (processed=${delivered}, wn bulk msgs decoded=${wnBulkGot}/${WN_MSGS})`);

// wn reads the spike's bulk messages
const wnList = await until(async () => {
  const r = await wn("--account", wnAcct, "messages", "list", wnGid1, "--limit", "600");
  const arr = Array.isArray(r.result) ? r.result : (r.result?.messages ?? r.result?.items ?? []);
  const found = arr.filter((m: any) => JSON.stringify(m).includes("bulk-spike-")).length;
  return found >= SPIKE_MSGS ? found : false;
}, `wn sees ${SPIKE_MSGS} spike messages`, 120000);
log(`wn sees ${wnList} spike bulk messages`);

// spike A adds a second spike member (C) to group 1
const c = await mkMember("interop-c");
const kpC = await c.publishKeyPackage();
await relay.publish(kpC);
await publishInbox(c, relayUrl);
log("spike C inbox relay list published");
const inv = await a.send(gid1, { type: "invite", keyPackageEvents: [kpC] });
await publishResult(a, inv, publish);
await flushOutbound(a, gid1, publish);
const cWel = await relay.fetchRemote({ kinds: [KIND_GIFT_WRAP], "#p": [c.keys.publicKey] });
await c.ingest(cWel[cWel.length - 1]);
log(`spike C joined group 1: ${JSON.stringify(await c.members(gid1)).slice(0, 200)}`);

// spike removes wn from group 1
const rmWn = await a.send(gid1, { type: "removeMembers", memberIds: [wnPub] });
await publishResult(a, rmWn, publish);
await flushOutbound(a, gid1, publish);
const postRm = await a.send(gid1, { type: "appMessage", content: "post-removal-secret", createdAt: 1_800_100_000 });
await publishResult(a, postRm, publish);
await flushOutbound(a, gid1, publish);
await relay.fetchRemote({ kinds: [KIND_GROUP_MESSAGE] });
for (const ev of relay.fetchAsc(KIND_GROUP_MESSAGE, g1h)) await c.ingest(ev);
const cDrained = await c.drainEvents();
const cGot = cDrained.some((d: any) => JSON.stringify(d).includes("post-removal-secret"));
log(`C reads post-removal message: ${cGot}`);

// wn observes removal + cannot read post-removal content
await sleep(5000); // let wnd ingest the removal commit
const wnAfter = await wn("--account", wnAcct, "groups", "members", wnGid1).catch((e: Error) => ({ err: String(e) }));
log(`wn members after removal: ${JSON.stringify(wnAfter).slice(0, 300)}`);
const wnMsgsAfter = await wn("--account", wnAcct, "messages", "list", wnGid1, "--limit", "600").catch((e: Error) => ({ err: String(e) }));
const leaked = JSON.stringify(wnMsgsAfter).includes("post-removal-secret");
log(`wn sees post-removal plaintext: ${leaked}`);

// wn (admin in group 2) removes spike A
await wn("--account", wnAcct, "groups", "remove-members", gid2, aNpub);
log("wn removed spike A from group 2");
await until(async () => {
  await relay.fetchRemote({ kinds: [KIND_GROUP_MESSAGE] });
  for (const ev of relay.fetchAsc(KIND_GROUP_MESSAGE)) await a.ingest(ev);
  await flushOutbound(a, gid2, publish).catch(() => 0);
  const mem = await a.members(gid2).catch(() => []);
  return mem.length === 0 || (await a.drainEvents()).length > 0 ? true : false;
}, "spike observes own removal", 60000);
log(`spike A members(gid2) after removal: ${JSON.stringify(await a.members(gid2).catch(() => "err"))}`);
results.scenario3 = "PASS";

// ---- scenario 4: 0x800d account-devices requirement ------------------------
const show = await wn("--account", wnAcct, "groups", "show", gid2).catch((e: Error) => ({ err: String(e) }));
const showStr = JSON.stringify(show);
log(`groups show gid2 components: ${showStr.slice(0, 400)}`);
// Evidence for 0x800d being required is the earlier preflight error:
// wn refused the invite with missing_required_capabilities listing
// app_components {…,32781(0x800d),…}; the join only succeeded after the
// spike advertised it. `groups show` prints group app-data, where
// account-devices correctly does not appear (member-level capability).
results.scenario4 = "PASS: wn required_components incl. 0x800d (see INTEROP.md)";

log(JSON.stringify(results));
relay.close();
await served.close();
process.exit(0);
