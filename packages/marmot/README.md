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

## License

Licensed under either of Apache-2.0 or MIT, at your option.
