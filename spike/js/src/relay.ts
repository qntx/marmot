// In-process fake relay: stores signed events, answers kind/#h/#p filters,
// returns backlog NEWEST-FIRST like real relays. The scenario driver sorts
// ascending by (created_at, id) before ingest — the SDK path.

export interface RelayEvent {
  id: string;
  pubkey: string;
  created_at: number;
  kind: number;
  tags: string[][];
  content: string;
  sig?: string;
}

export const KIND_GIFT_WRAP = 1059;
export const KIND_GROUP_MESSAGE = 445;
export const KIND_KEY_PACKAGE = 30443;

function tagValues(ev: RelayEvent, name: string): string[] {
  return ev.tags.filter((t) => t[0] === name).map((t) => t[1]);
}

export class FakeRelay {
  events: RelayEvent[] = [];
  private failNextFlag = false;

  /** Make the next publish return false (simulated relay failure). */
  failNext() {
    this.failNextFlag = true;
  }

  /** Publish; returns false if the fail flag was armed. */
  tryPublish(ev: RelayEvent): boolean {
    if (this.failNextFlag) {
      this.failNextFlag = false;
      return false;
    }
    this.events.push(ev);
    return true;
  }

  private filtered(kind: number, h?: string, p?: string): RelayEvent[] {
    return this.events.filter(
      (e) =>
        e.kind === kind &&
        (h === undefined || tagValues(e, "h").includes(h)) &&
        (p === undefined || tagValues(e, "p").includes(p)),
    );
  }

  /** Backlog as a real relay emits it: newest first. */
  fetchNewest(kind: number, h?: string, p?: string): RelayEvent[] {
    return this.filtered(kind, h, p).sort(
      (a, b) => b.created_at - a.created_at || b.id.localeCompare(a.id),
    );
  }

  /** Same filter, ascending (created_at, id) — the SDK ingest order. */
  fetchAsc(kind: number, h?: string, p?: string): RelayEvent[] {
    return this.filtered(kind, h, p).sort(
      (a, b) => a.created_at - b.created_at || a.id.localeCompare(b.id),
    );
  }

  /** Newest KeyPackage event by an author pubkey. */
  fetchKeyPackage(authorHex: string): RelayEvent | undefined {
    return this.events
      .filter((e) => e.kind === KIND_KEY_PACKAGE && e.pubkey === authorHex)
      .sort(
        (a, b) => b.created_at - a.created_at || b.id.localeCompare(a.id),
      )[0];
  }

  get size(): number {
    return this.events.length;
  }
}
