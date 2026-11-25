# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

A `vX.Y.Z` tag runs `publish-npm.yml`, which publishes `@qntx/marmot`,
`@qntx/marmot-wasm` and `@qntx/marmot-native` at one lockstep version through
npm trusted publishing (OIDC).

## [Unreleased]

### Added

- Emptied-slot refill: a write relay that accepted this device's current
  `KeyPackage` but now serves nothing in the slot gets a new `KeyPackage`
  once the publication is a day old — at most one per day, never one per
  audit or restart.

### Changed (BREAKING)

- `groups()` now returns every local copy including removed and disbanded
  ones (with `access`/`state` telling them apart), in a stable order, until
  the host calls `forgetGroup`; hosts that want only live chats filter on
  `state`/`access`.
- Bridge protocol 8: the `audit_key_package_slot` request carries
  `emptyRelays` — the write relays whose read of the slot completed
  (eose) and returned nothing.
- `fetchEvents` returns `{ events, emptyRelays }` instead of the bare
  event array; empty-finished reads are distinguished from closed or
  timed-out ones.
- Bridge protocol 9: ingest reports can carry the new availability cause
  `predates_local_copy` — a deferred kind-445 stamped before this copy's
  Welcome. The pump credits it like an accepted event; every other
  `unavailable` report still caps the cursor at `createdAt − 1`.

### Fixed

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
