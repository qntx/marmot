/// <reference lib="dom" />
/// <reference lib="dom.iterable" />
/**
 * Browser harness for `e2e/client.spec.ts`: one `MarmotClient` per page over the real websocket
 * relay the spec process serves (`serveFakeRelay`), with `indexedDbKv` + `webLocks` + a WebCrypto
 * AES-GCM cipher and `indexedDbCursorStore` for the cursor blob. Driven through
 * `window.__marmotClient`. The transcript the spec asserts on is persisted in `localStorage` so a
 * page reload mid-conversation does not lose it.
 */

import type { Backend, GroupSnapshot } from "@qntx/marmot/backend";
import { MarmotClient, nostrRelayPort } from "@qntx/marmot/client";
import type { Timer } from "@qntx/marmot/client";
import { indexedDbCursorStore, indexedDbKv, webLocks } from "@qntx/marmot/storage";
import type { Cipher, IdbFactoryLike, LockManagerLike } from "@qntx/marmot/storage";
import { Pool } from "@qntx/nostr";

import type { load } from "../../src/index.ts";
import { testSigner } from "../../tests/signer.ts";
import type { ClientHarness } from "../types.ts";

type MarmotWasmModule = { load: typeof load };

let loader: Promise<MarmotWasmModule> | undefined;
async function wasmModule(): Promise<MarmotWasmModule> {
  const { href } = new URL("marmot.mjs", location.href);
  loader ??=
    // oxlint-disable-next-line no-unsafe-type-assertion -- the served module is packed src/index.ts
    import(href) as Promise<MarmotWasmModule>;
  return loader;
}

function b64decode(text: string): Uint8Array {
  const raw = atob(text);
  const out = new Uint8Array(raw.length);
  for (let i = 0; i < raw.length; i += 1) {
    out[i] = raw.codePointAt(i) ?? 0;
  }
  return out;
}

/** Test cipher: AES-GCM, fresh 96-bit IV per value, row key as `additionalData`, IV ‖ ciphertext. */
function aesGcmCipher(keyBytes: Uint8Array): Cipher {
  const key = crypto.subtle.importKey("raw", new Uint8Array(keyBytes), { name: "AES-GCM" }, false, [
    "encrypt",
    "decrypt",
  ]);
  return {
    seal: async (rowKey, plain) => {
      const iv = crypto.getRandomValues(new Uint8Array(12));
      const sealed = new Uint8Array(
        await crypto.subtle.encrypt(
          { name: "AES-GCM", iv, additionalData: new Uint8Array(rowKey) },
          await key,
          new Uint8Array(plain),
        ),
      );
      const out = new Uint8Array(iv.length + sealed.length);
      out.set(iv);
      out.set(sealed, iv.length);
      return out;
    },
    open: async (rowKey, sealed) => {
      const iv = new Uint8Array(sealed.subarray(0, 12));
      return new Uint8Array(
        await crypto.subtle.decrypt(
          { name: "AES-GCM", iv, additionalData: new Uint8Array(rowKey) },
          await key,
          new Uint8Array(sealed.subarray(12)),
        ),
      );
    },
  };
}

/* The DOM typings for `indexedDB`/`navigator.locks` are wider than the structural ports the
 * driver declares — narrow them explicitly at this one boundary. */
// oxlint-disable-next-line no-unsafe-type-assertion
const IDB = indexedDB as unknown as IdbFactoryLike;
// oxlint-disable-next-line no-unsafe-type-assertion
const LOCKS = navigator.locks as unknown as LockManagerLike;

/** `setTimeout`-backed `Timer` — the browser runs on the real clock. */
const timer: Timer = {
  set(ms, run) {
    const id = setTimeout(() => {
      run();
    }, ms);
    return () => {
      clearTimeout(id);
    };
  },
};

const clockStart = performance.now();
const clock = {
  now: () => ({
    unixMs: BigInt(Date.now()),
    monotonicMs: BigInt(Math.max(0, Math.floor(performance.now() - clockStart))),
  }),
};

/** Outbound ids the delivery listener resolved `complete`/`confirmed` — the pre-reload boundary. */
const delivered = new Set<string>();

let backend: Backend | undefined;
let aesKey: Uint8Array | undefined;
let pageSigner = testSigner();
let pool: Pool | undefined;
let client: MarmotClient | undefined;
let relayUrl = "";
const errors: string[] = [];
const invites: string[] = [];

function readStored(key: string): string[] {
  const parsed: unknown = JSON.parse(localStorage.getItem(key) ?? "[]");
  if (!Array.isArray(parsed)) {
    return [];
  }
  return parsed.filter((entry): entry is string => typeof entry === "string");
}

function appendStored(key: string, entries: string[]): void {
  localStorage.setItem(key, JSON.stringify([...readStored(key), ...entries]));
}

function sentMap(): Map<string, string[]> {
  const parsed: unknown = JSON.parse(localStorage.getItem("mc:sentmap") ?? "[]");
  if (!Array.isArray(parsed)) {
    return new Map();
  }
  const entries = parsed.filter(
    (entry): entry is [string, string[]] =>
      Array.isArray(entry) &&
      typeof entry[0] === "string" &&
      Array.isArray(entry[1]) &&
      entry[1].every((id) => typeof id === "string"),
  );
  return new Map(entries);
}

function requireClient(): MarmotClient {
  if (client === undefined) {
    throw new Error("client.open first");
  }
  return client;
}

function snapshotShape(snapshot: GroupSnapshot): unknown {
  return {
    id: snapshot.id,
    epoch: snapshot.epoch.toString(),
    members: snapshot.members
      .map((member) => `${member.account}:${member.admin ? 1 : 0}`)
      .toSorted(),
    name: snapshot.name,
    relays: [...snapshot.relays].toSorted(),
    nostrGroupId: snapshot.nostrGroupId,
    state: snapshot.state,
    access: snapshot.access,
  };
}

const api: ClientHarness = {
  /** Fetch + instantiate the wasm; installs the injected AES key and account secret. */
  async init(keyBase64: string, secretBase64: string): Promise<void> {
    aesKey = b64decode(keyBase64);
    pageSigner = testSigner(b64decode(secretBase64));
    const response = await fetch("/marmot_wasm_bg.wasm");
    const bytes = new Uint8Array(await response.arrayBuffer());
    const module_ = await wasmModule();
    backend = await module_.load(bytes);
  },

  /** Open the client: pool + nostrRelayPort + indexedDbKv + indexedDbCursorStore. */
  async open(url: string): Promise<void> {
    if (backend === undefined || aesKey === undefined) {
      throw new Error("client.init first");
    }
    relayUrl = url;
    pool = new Pool({
      websocketImplementation: WebSocket,
      trustedInsecureOrigins: [url],
    });
    const kv = await indexedDbKv(IDB, "mc-kv", {
      lock: webLocks(LOCKS),
      cipher: aesGcmCipher(aesKey),
    });
    const cursors = await indexedDbCursorStore(IDB, "mc-cursors", {
      cipher: aesGcmCipher(aesKey),
    });
    client = await MarmotClient.open({
      backend,
      kv,
      cursors,
      signer: pageSigner,
      clock,
      timer,
      relays: nostrRelayPort(pool),
      lookupRelays: [relayUrl],
      allowLoopback: true,
    });
    client.onDelivery((event) => {
      if (event.resolution === "complete" || event.resolution === "confirmed") {
        delivered.add(event.outbound);
      }
    });
    client.onMessages((batch) => {
      appendStored(
        "mc:received",
        batch.map((entry) => entry.message.content),
      );
    });
    client.onInvite((event) => {
      invites.push(event.type);
    });
    client.onError((error) => {
      errors.push(error.kind);
    });
  },

  /** Kind-10002 + KeyPackage + kind-10050 on the served relay. */
  async identity(): Promise<void> {
    const event = await pageSigner.signEvent({
      pubkey: pageSigner.pubkey,
      createdAt: BigInt(Math.floor(Date.now() / 1000)),
      kind: 10002,
      tags: [["r", relayUrl]],
      content: "",
    });
    if (pool === undefined) {
      throw new Error("client.open first");
    }
    await nostrRelayPort(pool).publish(relayUrl, event);
    await requireClient().ensureKeyPackage();
    await requireClient().publishInboxRelays([relayUrl]);
  },

  /** Create a group on `relays` inviting `inviteePubkey` (looked up via `lookupRelays`). */
  async createGroup(
    name: string,
    relays: string[],
    inviteePubkey: string,
    lookupRelays: string[],
  ): Promise<string> {
    const invitees = await requireClient().lookupInvitees(inviteePubkey, lookupRelays);
    const snapshot = await requireClient().createGroup({
      name,
      description: "",
      relays,
      invitees,
    });
    return snapshot.id;
  },

  async send(group: string, text: string): Promise<void> {
    for (let attempt = 0; attempt < 200; attempt += 1) {
      try {
        // oxlint-disable-next-line no-await-in-loop -- a send is sequential by design
        const sent = await requireClient().sendMessage(group, {
          kind: 9,
          tags: [],
          content: text,
        });
        appendStored("mc:sent", [text]);
        if (sent.outcome.type === "staged") {
          const map = sentMap();
          map.set(text, sent.outcome.ids);
          localStorage.setItem("mc:sentmap", JSON.stringify([...map.entries()]));
        }
        return;
      } catch (error) {
        if (error instanceof Error && "kind" in error && error.kind === "group_busy") {
          // oxlint-disable-next-line no-await-in-loop -- bounded app-level retry
          await new Promise((resolve) => {
            setTimeout(resolve, 20);
          });
          continue;
        }
        throw error;
      }
    }
    throw new Error("send stayed group_busy");
  },

  /** Sent ∪ received contents, sent first — the member transcript for convergence. */
  transcript(): string[] {
    return [...readStored("mc:sent"), ...readStored("mc:received")];
  },

  received(): string[] {
    return readStored("mc:received");
  },

  sentMapEntries(): Array<[string, string[]]> {
    return [...sentMap().entries()];
  },

  invitesSeen(): string[] {
    return [...invites];
  },

  errorsSeen(): string[] {
    return [...errors];
  },

  async snapshot(group: string): Promise<unknown> {
    const found = await requireClient().group(group);
    return found === undefined ? undefined : snapshotShape(found);
  },

  deliveredIds(): string[] {
    return [...delivered];
  },

  async close(): Promise<void> {
    await client?.close();
    client = undefined;
    pool?.close();
    pool = undefined;
  },
};

globalThis.__marmotClient = api;
