// WebSocket relay adapter: same driver-facing surface as FakeRelay
// (tryPublish / fetchAsc / fetchNewest / size) but over a real NIP-01
// relay socket. The served relay already returns backlog newest-first;
// fetchAsc re-sorts ascending like the SDK path.
import WebSocket from "ws";
import type { RelayEvent } from "./relay.js";

export class WsRelay {
  private ws!: WebSocket;
  private seq = 0;
  private pending = new Map<string, { resolve: (v: unknown) => void; kind: string }>();
  private store: RelayEvent[] = [];
  private seen = new Set<string>();
  private collect = new Map<string, RelayEvent[]>();

  static async connect(url: string): Promise<WsRelay> {
    const r = new WsRelay();
    r.ws = new WebSocket(url);
    await new Promise<void>((res, rej) => {
      r.ws.once("open", () => res());
      r.ws.once("error", rej);
    });
    r.ws.on("message", (data: WebSocket.RawData) => {
      const msg = JSON.parse(data.toString());
      const [type, sub] = msg;
      if (type === "EVENT" && (r.pending.has(`req:${sub}`) || r.collect.has(sub))) {
        const ev = msg[2] as RelayEvent;
        if (!r.seen.has(ev.id)) {
          r.seen.add(ev.id);
          r.store.push(ev);
        }
        r.collect.get(sub)?.push(ev);
      } else if (type === "EOSE" && r.pending.has(`req:${sub}`)) {
        r.pending.get(`req:${sub}`)!.resolve(undefined);
        r.pending.delete(`req:${sub}`);
      } else if (type === "OK" && r.pending.has(`ok:${msg[1]}`)) {
        r.pending.get(`ok:${msg[1]}`)!.resolve(msg[2] === true);
        r.pending.delete(`ok:${msg[1]}`);
      } else if (type === "CLOSED" && r.pending.has(`req:${sub}`)) {
        r.pending.get(`req:${sub}`)!.resolve(undefined);
        r.pending.delete(`req:${sub}`);
      }
    });
    return r;
  }

  private ask(key: string, msg: unknown, timeoutMs = 15000): Promise<unknown> {
    return new Promise((resolve, reject) => {
      const t = setTimeout(() => {
        this.pending.delete(key);
        reject(new Error(`relay timeout ${key}`));
      }, timeoutMs);
      this.pending.set(key, {
        kind: key.split(":")[0],
        resolve: (v) => {
          clearTimeout(t);
          resolve(v);
        },
      });
      this.ws.send(JSON.stringify(msg));
    });
  }

  /** Publish one signed event; true on OK. */
  async publish(ev: RelayEvent): Promise<boolean> {
    if (!this.seen.has(ev.id)) {
      this.seen.add(ev.id);
      this.store.push(ev);
    }
    return (await this.ask(`ok:${ev.id}`, ["EVENT", ev])) as boolean;
  }

  /** Driver surface parity with FakeRelay.tryPublish (sync there, async here). */
  tryPublish(ev: RelayEvent): boolean {
    void this.publish(ev);
    return true;
  }

  async publishSync(ev: RelayEvent): Promise<boolean> {
    return this.publish(ev);
  }

  /** REQ once, wait for EOSE, return newest-first like a real relay. */
  async fetchRemote(filter: Record<string, unknown>): Promise<RelayEvent[]> {
    const sub = `s${++this.seq}`;
    const collected: RelayEvent[] = [];
    this.collect.set(sub, collected);
    await this.ask(`req:${sub}`, ["REQ", sub, filter]);
    this.collect.delete(sub);
    this.ws.send(JSON.stringify(["CLOSE", sub]));
    return collected.sort((a, b) => b.created_at - a.created_at || (a.id < b.id ? 1 : -1));
  }

  fetchAsc(kind: number, h?: string, p?: string): RelayEvent[] {
    return this.store
      .filter((e) => e.kind === kind)
      .filter((e) => h === undefined || e.tags.some((t) => t[0] === "h" && t[1] === h))
      .filter((e) => p === undefined || e.tags.some((t) => t[0] === "p" && t[1] === p))
      .sort((a, b) => a.created_at - b.created_at || (a.id < b.id ? -1 : 1));
  }

  async fetchRemoteAsc(kind: number, h?: string, p?: string): Promise<RelayEvent[]> {
    const filter: Record<string, unknown> = { kinds: [kind] };
    if (h) filter["#h"] = [h];
    if (p) filter["#p"] = [p];
    await this.fetchRemote(filter);
    return this.fetchAsc(kind, h, p);
  }

  get size(): number {
    return this.store.length;
  }

  close() {
    this.ws.close();
  }
}
