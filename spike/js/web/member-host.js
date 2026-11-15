// Page-side member host: the same SDK layer as src/local-member.ts, with an
// IndexedDB KV backend. Exposes window.__member.call(name, method, arg) for
// the Playwright driver; deterministic keys mean a page reload re-opens the
// same account-device from persisted IDB state.

import init, { Client } from "/pkg/marmot_spike_wasm.js";
import {
  SecretKey,
  Keys,
  signEvent,
  finalizeEvent,
  hexToBytes,
} from "@qntx/nostr";
import { encryptToPubkey, decryptFromPubkey } from "@qntx/nostr/nips/nip44";

const KIND_KEY_PACKAGE = 30443;

// One IndexedDB row per KV entry; every onCommit batch becomes ONE
// transaction — the durability boundary the publish-before-apply contract
// needs.
class KvBackend {
  constructor(db) {
    this.db = db;
  }

  static async open(dbName) {
    const db = await new Promise((resolve, reject) => {
      const req = indexedDB.open(dbName, 1);
      req.onupgradeneeded = () => req.result.createObjectStore("kv");
      req.onsuccess = () => resolve(req.result);
      req.onerror = () => reject(req.error);
    });
    return new KvBackend(db);
  }

  tx(mode, fn) {
    return new Promise((resolve, reject) => {
      const t = this.db.transaction("kv", mode);
      const req = fn(t.objectStore("kv"));
      t.oncomplete = () => resolve(req?.result);
      t.onerror = () => reject(t.error);
    });
  }

  async loadAll() {
    const rows = await this.tx("readonly", (s) => s.getAll());
    return rows.map((r) => [r.k, r.v]);
  }

  async commit(batch) {
    await this.tx("readwrite", (s) => {
      for (const op of batch) {
        if (op.op === "put") s.put({ k: op.key, v: op.value }, op.key);
        else s.delete(op.key);
      }
    });
  }
}

async function accountKeys(seed) {
  const digest = await crypto.subtle.digest(
    "SHA-256",
    new TextEncoder().encode(`marmot-spike-account:${seed}`),
  );
  return Keys.fromSecretKey(new Uint8Array(digest));
}

function makeJsSigner(keys) {
  const sk = keys.secretKey.bytes;
  return {
    publicKey: () => keys.publicKey,
    signAccountIdentityProof(bytes) {
      const unsigned = JSON.parse(new TextDecoder().decode(bytes));
      return hexToBytes(signEvent(unsigned, keys.secretKey).sig);
    },
    async signEvent(unsignedJson) {
      return JSON.stringify(signEvent(JSON.parse(unsignedJson), keys.secretKey));
    },
    // encryptToPubkey/decryptFromPubkey are SYNC in @qntx/nostr — wrap in
    // Promise.resolve: wasm awaits a real Promise via JsFuture.
    nip44Encrypt: (peer, plaintext) =>
      Promise.resolve(encryptToPubkey(plaintext, sk, peer)),
    nip44Decrypt: (peer, payload) =>
      Promise.resolve(decryptFromPubkey(payload, sk, peer)),
  };
}

const members = new Map();
const SLOT = new URLSearchParams(location.search).get("slot") ?? "default";

// An account-device opened on the IDB backend is recorded so a page reload
// (the real-world app restart) re-opens it from persisted state.
function markOpened(name, seed) {
  const key = `spike-opened:${SLOT}`;
  const list = JSON.parse(localStorage.getItem(key) ?? "[]");
  if (!list.some((e) => e.name === name)) {
    list.push({ name, seed });
    localStorage.setItem(key, JSON.stringify(list));
  }
}

async function openMember(name, seed, opts = {}) {
  const keys = await accountKeys(seed);
  const backend = opts.entries
    ? {
        map: new Map(opts.entries),
        async loadAll() { return [...this.map.entries()]; },
        async commit(batch) {
          for (const op of batch) {
            if (op.op === "put") this.map.set(op.key, op.value);
            else this.map.delete(op.key);
          }
        },
      }
    : await KvBackend.open(`marmot-spike-${seed}`);
  let tail = Promise.resolve();
  const client = Client.open({
    entries: await backend.loadAll(),
    onCommit: (batch) => {
      tail = tail.then(() => backend.commit(batch));
    },
    signer: makeJsSigner(keys),
  });
  await tail;
  const member = {
    client,
    keys,
    backend,
    flush: () => tail,
  };
  members.set(name, member);
  if (!opts.entries) markOpened(name, seed);
  return { ok: true };
}

const ops = {
  async open(name, { seed, entries }) {
    return openMember(name, seed, { entries });
  },
  async pubkey(name) {
    return members.get(name).keys.publicKey;
  },
  async entries(name) {
    return members.get(name).backend.loadAll();
  },
  async publishKeyPackage(name) {
    const m = members.get(name);
    const fields = await m.client.publishKeyPackage();
    await m.flush();
    return finalizeEvent(
      {
        kind: fields.kind,
        created_at: fields.created_at,
        tags: fields.tags,
        content: fields.content,
      },
      m.keys.secretKey,
    );
  },
  async createGroup(name, { groupName, relays, keyPackageEvents }) {
    const m = members.get(name);
    const out = await m.client.createGroup({
      name: groupName,
      relays,
      keyPackageEvents,
    });
    await m.flush();
    const { groupId, ...result } = out;
    return { groupId, result };
  },
  async send(name, { groupId, intent }) {
    const m = members.get(name);
    const out = await m.client.send({ groupId, intent });
    await m.flush();
    return out;
  },
  async ingest(name, ev) {
    const m = members.get(name);
    const out = await m.client.ingest(ev);
    await m.flush();
    return out;
  },
  async advance(name, groupId) {
    const m = members.get(name);
    const out = await m.client.advanceConvergence(groupId);
    await m.flush();
    return out;
  },
  async confirmPublished(name, pending) {
    const m = members.get(name);
    await m.client.confirmPublished(pending);
    await m.flush();
  },
  async publishFailed(name, pending) {
    const m = members.get(name);
    await m.client.publishFailed(pending);
    await m.flush();
  },
  async drainEvents(name) {
    const m = members.get(name);
    const out = m.client.drainEvents();
    await m.flush();
    return out;
  },
  async members(name, groupId) {
    return members.get(name).client.members(groupId);
  },
  async epoch(name, groupId) {
    return members.get(name).client.epoch(groupId);
  },
  async memberId(name) {
    return members.get(name).client.memberId();
  },
  async wasmMemoryBytes(name) {
    return members.get(name).client.wasmMemoryBytes();
  },
};

window.__member = {
  ready: false,
  async call(name, method, arg) {
    return ops[method](name, arg);
  },
};

init({ module_or_path: "/pkg/marmot_spike_wasm_bg.wasm" })
  .then(async () => {
    const opened = JSON.parse(
      localStorage.getItem(`spike-opened:${SLOT}`) ?? "[]",
    );
    for (const { name, seed } of opened) {
      await openMember(name, seed);
    }
    window.__member.ready = true;
    console.log(`member-host ready (${opened.length} persisted members)`);
  })
  .catch((e) => console.error("wasm init failed:", e));
