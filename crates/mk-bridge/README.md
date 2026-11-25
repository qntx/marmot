# mk-bridge

Marmot kit bridge: a pure-Rust, byte-oriented host protocol that surfaces the
`mk-core` session API to wasm and JavaScript hosts.

```text
mk-core -> mk-bridge -> mk-wasm -> @qntx/marmot-wasm
```

A host owns storage, the clock and the account signer; the bridge owns a
`mk_core::Session`. Every entry point on `Bridge` is bytes-in/bytes-out JSON
and never returns `Result` or panics:

- `open(config)` — decode the open config (`protocol_version`, `account`,
  `now`, `allow_loopback`, `client_name`), load durable rows through
  `Host::load`, and answer `ready`, `needs_account_proof` or `error`.
- `call(request)` — `{now, request}` envelopes; one request variant per
  `Session` surface. Engine work that awaits an `AccountSigner` operation
  suspends mid-call: the response is `signer_pending` listing numbered
  `SignerRequest`s, and the host resumes with `signer_reply`.
- `drain()` — the queued session events (`message_received`,
  `group_state_changed`, `publish_resolved`, ...).
- `close()` — release the session.

## Host contract

```rust
pub trait Host {
    fn load(&mut self) -> Result<Vec<u8>, HostError>;
    fn commit(&mut self, batch: &[u8]) -> Result<(), HostError>;
}
```

`load` returns every durable row encoded by `encode_rows`/`decode_rows`
(version byte, `u32` count, then `[key len][key][value len][value]` per row).
`commit` receives one `KvBatch::encode` payload at a time, strictly in
sequence order, after every session poll and before every response. The host
acknowledges durability through `ack_durable { seq }`; staged outbound items
only appear in `take_outbound` once their gating batch is acknowledged.

A `HostError` from `load` or `commit` poisons the bridge (`host_failed` on
the failing call, `poisoned` after): the session is over and must be
reopened from durable rows.

## Protocol

`PROTOCOL_VERSION = 8`. Requests over `MAX_REQUEST_BYTES` (64 MiB) are
refused with `too_large`. Encoding rules: UTF-8 JSON; bytes as lowercase hex;
`u64` as decimal strings; `u8`/`u16`/`u32` as JSON numbers; options always
emitted and decoding to `null`; internally-tagged enums with snake_case
variant names; `deny_unknown_fields`. Serde error messages are never
surfaced — decode failures map to `invalid_request` with a fixed reason
label. Any change to an existing message's shape bumps `PROTOCOL_VERSION`;
every protocol variant has a canonical encoding in `vectors/bridge` (see
`vectors/bridge/README.md`), replayed by `tests/vectors.rs`.

`monotonic_ms` may never regress across calls (`clock_regressed`). Signer
operations are numbered per bridge and answered exactly once each
(`unknown_signer_request` otherwise; a result kind that does not fit the
operation is `invalid_request { reason: "value" }`).

Result payloads that carry more than their request implies:

| Request        | Result          | Note                                                                                                                        |
| -------------- | --------------- | --------------------------------------------------------------------------------------------------------------------------- |
| `create_group` | `group_created` | The full `GroupSnapshot`, not just the group id — hosts need `nostr_group_id`, the 32-byte transport route publishes carry. |

## Timing

Engine futures register timers through the surrounding async runtime: on
native, calls that drive engine work must run inside a runtime context with
a time driver (the same requirement `mk-core` documents, exercised here via
`#[tokio::test]`); on wasm the engine uses gloo timers. The crate itself
takes no runtime dependency.
