# vectors/wn — White Noise ground-truth captures

Recorded behavior of the real White Noise `wn`/`wnd` binaries interopping
with `MarmotClient` over an in-process relay. These are observation
fixtures, not conformance vectors: they document what wn 0.17.0 emits on
the wire (inner app-event kinds/tags/content), which commands it refuses
locally, and how it materializes raw events sent by our client
(`messages list` / `messages timeline list` views).

- `capture-0.17.0.json` — ordered list of step records
  `{ step, actor, command, wn_reply, observed, wn_views, elapsed_ms }`.
  `observed.messages` is every inner app event `MarmotClient` received in
  the step window (`group`, `id`, `sender`, `kind`, `tags`, `content`,
  `created_at`, `epoch`); `observed.group_changes`/`invites` are the
  client's group-change and invite events. Pubkeys/ids are throwaway test
  identities. The relay URL is sanitized to `ws://relay.invalid`.

Regenerate (needs local wn/wnd 0.17.0 binaries and a current release wasm
in `packages/marmot-wasm/dist`, i.e. `bun run --cwd packages/marmot-wasm
build:wasm` first if stale):

```sh
bun packages/marmot-wasm/interop/capture-wn.ts
```

`WN_BIN`/`WND_BIN` default to `<repo>/spike/target/wn/bin/{wn,wnd}`. The
full run log lands at `target/m5a-capture.log`; daemon logs under
`target/wn-logs/`.
