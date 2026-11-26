# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

A `vX.Y.Z` tag runs `publish-npm.yml`, which publishes `@qntx/marmot`,
`@qntx/marmot-wasm` and `@qntx/marmot-native` at one lockstep version through
npm trusted publishing (OIDC).

## [Unreleased]

## [0.4.0] - 2026-11-27

### Added

- Host-owned inbox (`inbox: "host"`): for a host that already runs the
  account's NIP-17 inbox, the client opens no kind-1059 subscription and
  keeps no inbox cursors or floors; the host unwraps wraps itself and
  forwards each one whose rumor is kind 444 to `MarmotClient.ingestWrap`,
  which verifies and peels the untouched wrap through the same ingest
  path a subscription-delivered wrap takes.

### Changed (BREAKING)

- `MarmotClient.open` requires `inbox: "client" | "host"`.
- Client state blob v4 records the inbox owner (`inbox`) and renames the
  relay-list field `inbox` to `inboxRelays`; a v3 or otherwise
  undecodable blob decodes as the empty state — one full refetch and one
  signer probe at the next open. Reopening in the other mode rejects at
  open with the new `MarmotErrorKind` `wrong_inbox_owner`, which also
  answers `retryInvites()`, `publishInboxRelays()` and `ingestWrap()`
  calls made in the wrong mode.

### Fixed

- `signer_pending` no longer re-lists requests whose replies the
  suspended operation already consumed: each batch now carries only
  requests still waiting on the host. Previously every later batch
  re-listed all earlier ones, so hosts re-ran signer calls — one extra
  NIP-44 decrypt per gift-wrap unwrap and repeated signs on multi-sign
  operations.

## [0.3.0] - 2026-11-26

### Added

- Emptied-slot refill: a write relay that accepted this device's current
  `KeyPackage` but now serves nothing in the slot gets a new `KeyPackage`
  once the publication is a day old — at most one per day, never one per
  audit or restart.

### Changed (BREAKING)

- `groups()` now returns every local copy including removed, disbanded and
  quarantined ones (`access` alone tells them apart), in a stable order,
  until the host calls `forgetGroup`; hosts that want only live chats
  filter on `access`. A copy whose stored record row does not decode is
  the one exception: it stays unlisted.
- Bridge protocol 10: `GroupSnapshot` carries durable record facts (`id`,
  `access`, profile, `epoch`, `members`) plus `live` — the facts only a
  loaded MLS state answers (`state`, `admins`, `ownAccountAdmin`,
  `relays`, `nostrGroupId`, the media/stream/avatar/retention/disband
  policies). `access` gains `disbanded` and `quarantined`; `GroupState`
  drops `removed` and `disbanded`; `MemberSnapshot` drops `admin` (a live
  fact now read from `live.admins`). `live` is `undefined`/`null` for a
  disbanded or quarantined copy — a disbanded copy no longer reports
  `access: active` with placeholder routes, and a live snapshot's
  `nostrGroupId` is `undefined`/`null` (never all-zero) when the group
  has no routing component.
- Bridge protocol 8: the `audit_key_package_slot` request carries
  `emptyRelays` — the write relays whose read of the slot completed
  (eose) and returned nothing.
- `fetchEvents` returns `{ events, emptyRelays }` instead of the bare
  event array; empty-finished reads are distinguished from closed or
  timed-out ones.
- Client cursor blob v3: it gains `floors`, so a v2 blob decodes as the
  empty state (one full refetch at the next open, no migration).
- Bridge protocol 9: ingest reports can carry the new availability cause
  `predates_local_copy` — a deferred kind-445 stamped before this copy's
  Welcome. The pump credits it like an accepted event; every other
  `unavailable` report still caps the cursor at `createdAt − 1`.

### Fixed

- `auditKeyPackageSlot`'s `emptyRelays` matching no longer depends on URL
  spelling: entries and accepted publication targets compare as parsed
  relay URLs (`nostr::types::RelayUrl`), so a host that reports
  `wss://Relay.Example/` where the write set said `wss://relay.example`
  still refills the emptied slot; entries that do not parse are ignored.
- A gift wrap the signer refused or failed to open is no longer lost when
  a later invite or message wrap is opened more than two days after it:
  its inbox cursor stays below the unopened wrap (a sticky floor
  persisted with the cursors), so it re-surfaces as `pending` after any
  restart or `retryInvites()` until it is opened, permanently rejected,
  or its `createdAt` is 30 days old. A signer that keeps refusing causes
  no resubscription loop — replays happen only on `retryInvites()`, a
  reconnect or a restart.
- The pump now reconciles on `hydration_quarantined` and
  `hydration_recovered`: a quarantined copy's subscription is dropped and
  a recovered group's kind-445 subscription opens without waiting for an
  unrelated reconcile.
- `group(id)` on a forgotten copy answers `undefined` instead of failing.
- A newly joined device no longer re-fetches the group's pre-join history
  on every reconnect: those deferred kind-445s report
  `predates_local_copy`, which credits the group cursor instead of
  capping it under the pre-join range.

## [0.2.0] - 2026-11-24

### Changed (BREAKING)

- Storage schema 9 (was 8): a store written by 0.1.0 is rejected at open
  and must be recreated. In-flight outbound artifacts now carry the queued
  intent they were drained from, which the old rows lack.

### Fixed

- A message or group change that was queued behind an in-flight commit
  could be sent twice. The engine could not recognise the already staged
  artifact as in flight after a restart, after a publish that was not yet
  reported, or after a retryable publish failure, and prepared a second
  one. Staged artifacts now name their queued intent, so the same artifact
  is re-offered instead (#49).
- A crash between the engine recording a self-update as preparing and
  staging its commit left the maintenance obligation stuck and the group
  epoch frozen for that device (#49).

### Added

- Advisory `interop-nosfry` workflow: the White Noise interop suite runs
  against a pinned nosfry release with its Marmot profile (#43).

### Dependencies

- `@qntx/nostr` ^0.16.0 and the tooling group; `@qntx/oxlint` and
  `@qntx/oxfmt` 2.0.0; axum 0.8.9 in the interop harness (#44, #46, #47).

## [0.1.0] - 2026-11-23

First release. Marmot group messaging (MLS over Nostr) for web and React
Native, built on the upstream MDK engine pinned at `v0.18.0` (`34646a5`) and
the Marmot spec at `04fbdf8`, and interoperable with White Noise 0.18.0.

### Added

- `@qntx/marmot`: the platform-neutral TypeScript client. `MarmotClient`
  opens an account against an injected signer, relays, storage and timers;
  a serialized pump owns the engine, publishes outbound only after the
  covering write is durable, verifies every inbound event, bounds gift-wrap
  intake and persists sync cursors. Groups (create, invite, remove, leave,
  metadata, admins, retention, disband), typed messages (text, reply,
  reaction, edit, delete), invites with retry, relay lists and KeyPackages.
  Encrypted IndexedDB and SQLite key-value drivers.
- `@qntx/marmot-wasm`: the engine for browsers. The `.wasm` is built twice
  from the tagged source and shipped only when both builds are
  byte-identical; its gzip size is gated at 3,000,000 bytes.
- `@qntx/marmot-native`: the same engine for React Native through Nitro
  modules, as an iOS xcframework and Android libraries for arm64-v8a,
  armeabi-v7a and x86_64.
- Multi-device accounts: invitee KeyPackage selection, a KeyPackage
  lifecycle with one slot per device (rotation, refresh, consumption,
  own-slot audit), sign-out, post-join and periodic self-updates, a
  per-group access state (`active`, `removed_device`, `removed`,
  `unrecoverable`, `stalled`) with a durable stall detector, consented
  rejoin, admin re-add, and one member entry per account with its device
  count.
- Bridge protocol version 7 between hosts and the Rust core, with a
  canonical vector for every message under `vectors/bridge`; store schema
  version 8.
- Verification: interop scenarios against White Noise `wn`/`wnd` in CI,
  conformance replay of the recorded MDK vectors, crash-consistency and
  model tests for the store, and fourteen fuzz targets over every decoder
  the project owns.

### Security

- Every relay and media URL taken from group state passes a
  destination-safety check before any connection; loopback is admitted
  only on request, never private or link-local ranges.
- Signer replies are checked against the request and signature-verified;
  a signer must pass the NIP-44 extended-length round trip to open a
  client.
- Stored values are sealed by an injected cipher that authenticates the
  row key, and nothing identifying (accounts, groups, messages, relays,
  keys, payloads) is written to logs or diagnostics.

### Known issues

- A member that receives another member's commit of a SelfRemove proposal
  before the proposal itself can end on a different branch of the group
  and stop decrypting new messages
  ([mdk#3617](https://github.com/marmot-protocol/mdk/issues/3617)). Chats
  between an account's remaining devices after one device signs out are
  affected.
- A new member can miss a message sent right after its join until the
  client restarts, on relays that publish no NIP-11 `max_limit`
  ([mdk#3584](https://github.com/marmot-protocol/mdk/issues/3584)).
