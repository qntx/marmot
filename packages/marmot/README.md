# @qntx/marmot

Marmot (MLS over Nostr) SDK for web, iOS and Android.

**Status: pre-alpha.** The package ships the typed engine surface plus its
storage and testing support under dedicated subpaths:

- `@qntx/marmot` / `@qntx/marmot/core` — spec registries and the shared
  result vocabulary.
- `@qntx/marmot/backend` — the `Backend` port (byte-oriented bridge calls),
  wire codecs, and `Engine`, the typed facade over it:

  ```ts
  import { Engine } from "@qntx/marmot/backend";

  const engine = await Engine.open({ backend, kv, signer, clock });
  ```

- `@qntx/marmot/storage` — the durable KV drivers `indexedDbKv`
  (browsers) and `sqliteKv` (an injected `SqlitePort`; op-sqlite's `DB`
  or a `node:sqlite`/`bun:sqlite` adapter satisfies it). Both **require**
  an injected `cipher` (encrypts every stored value, authenticating the
  row key as associated data so sealed values cannot be swapped between
  rows) and `lock` (`webLocks(navigator.locks)` or the process-local
  `localLocks()`) — a driver that cannot take the exclusive lock reports
  `StorageError` kind `locked`, and a driver is terminal after a failed
  `commit`. `sqliteKv` never closes the host-owned database.
- `@qntx/marmot/testing` — `memoryKv`, `manualClock`, `fakeRelay` and
  `runFlow` for engine-level suites without a browser.

All `src/` code is platform-neutral and Hermes V1-safe (no Node- or
browser-only globals, no `node:*` imports, no ES2023+ array methods);
platform capabilities arrive through the injected ports.

## Multi-device

Each Marmot installation is a device with its own `KeyPackage` slot (the
`d` tag), and an account's membership is per leaf: inviters — this SDK
and White Noise alike — add every fresh slot they are shown. A new
device sees a group only after an admin invites the account again, or
when the group is founded with `createGroup`'s default
`includeOwnDevices`, which adds the account's sibling slots as
`own_devices` (a MAY — lookup failures found without them).

`GroupSnapshot.access` names how this device's copy stands — it alone
decides send-permission and row state: `active`; `stalled` when a stall
suspicion survives one full refetch; `unrecoverable` when the copy lost
its key schedule; `removed_device` when this device left or was dropped
while the account stays on its other leaves; `removed` for a
whole-account removal; `disbanded` for a terminal disband;
`quarantined` when hydration refused the copy at open. `id` through
`members` are durable record facts; `live` carries the facts only a
loaded MLS state answers and is `undefined` for a disbanded or
quarantined copy — a live snapshot's `nostrGroupId` is `undefined` when
the group has no routing component, never a placeholder. `members`
carries one entry per account with `devices`, its leaf count — a
sibling leaf leaving fires no member event, so hosts re-read the
snapshot on `epoch_changed`. A stalled, removed-device or unrecoverable copy is
replaced by an admin re-invite: the client surfaces `rejoin_offered`,
the host shows it, and `confirmRejoin` installs the new local copy while
delivered history stays; messages between the stall and the rejoin
epoch are not recoverable. `declineRejoin` drops the offer. An admin
heals a stuck member with `readdMember`, which removes and re-invites
the account's leaves exactly once. `open` also audits the device's own
`KeyPackage` slot daily, outranking any foreign publication of the same
slot.

## Invites

`onInvite` reports `joined` when a Welcome installs and `pending` when the
signer refused or failed to open a gift wrap. The client stores no wraps,
only a per-inbox-relay floor in its cursor state, so a refused invite
re-surfaces as `pending` after any restart or `retryInvites()` until it is
opened, permanently rejected, or its `createdAt` is 30 days old — after
that it is given up on this device. Nothing retries on its own: call
`retryInvites()` once the signer is available again (an unlocked
extension, an approved remote signer).

A White Noise device of the same account lists each Meowl device as an
unrecognized sign-in; tell users to answer "Not now" there — "This
wasn't me" makes White Noise warn that the account's key may be
compromised. The protocol cannot fix this (a draft device-group
limitation).

## License

Licensed under either of Apache-2.0 or MIT, at your option.
