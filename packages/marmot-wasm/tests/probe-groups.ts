/** Does send_message cost grow per-group or per-session? bun tests/probe-groups.ts */
import { Engine } from "@qntx/marmot/backend";
import { fakeRelay, manualClock, memoryKv } from "@qntx/marmot/testing";

import { releaseBackend } from "./shared/env.ts";
import { testSigner } from "./signer.ts";

const RELAY = "wss://relay.example";
const backend = await releaseBackend();
const relay = fakeRelay();
const signer = testSigner();
const engine = await Engine.open({
  backend,
  kv: memoryKv(),
  clock: manualClock(),
  signer,
  allowLoopback: true,
});

async function publish(): Promise<void> {
  for (let round = 0; round < 64; round += 1) {
    // oxlint-disable-next-line no-await-in-loop
    await engine.whenDurable();
    // oxlint-disable-next-line no-await-in-loop
    const items = await engine.takeOutbound();
    if (items.length === 0) {
      return;
    }
    for (const item of items) {
      const results = relay.publish(item);
      // oxlint-disable-next-line no-await-in-loop
      await engine.reportPublish(item.id, results);
    }
  }
}

const groups: string[] = [];
for (let g = 0; g < 2; g += 1) {
  // oxlint-disable-next-line no-await-in-loop
  const group = await engine.createGroup({
    name: `g${g}`,
    description: "",
    relays: [RELAY],
    invitees: [],
  });
  groups.push(group.id);
  // oxlint-disable-next-line no-await-in-loop
  await publish();
}

const time = async (g: number, i: number): Promise<number> => {
  const t0 = performance.now();
  await engine.sendMessage(groups[g] ?? "", { kind: 9, tags: [], content: `g${g}m${i}` });
  const ms = performance.now() - t0;
  await publish();
  return ms;
};

for (let i = 0; i < 100; i += 1) {
  // oxlint-disable-next-line no-await-in-loop
  await time(0, i);
}
const original = await time(0, 100);
// oxlint-disable-next-line no-console
console.log(`send to g0 #101: ${original.toFixed(1)} ms`);
const fresh = await time(1, 0);
// oxlint-disable-next-line no-console
console.log(`send to g1 (fresh group, session has 101+ msgs): ${fresh.toFixed(1)} ms`);
await engine.close();
