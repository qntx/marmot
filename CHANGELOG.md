# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

A `vX.Y.Z` tag runs `publish-npm.yml`, which publishes `@qntx/marmot`,
`@qntx/marmot-wasm` and `@qntx/marmot-native` at one lockstep version through
npm trusted publishing (OIDC).

## [Unreleased]

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
