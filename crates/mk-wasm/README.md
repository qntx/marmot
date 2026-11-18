# mk-wasm

`mk-wasm` is the wasm-bindgen shell over [`mk-bridge`](../mk-bridge), the
artifact behind the `@qntx/marmot-wasm` npm package. It exports one class:

```text
MarmotBridge {
  constructor(load: () => Uint8Array, commit: (batch: Uint8Array) => void);
  open(Uint8Array): Uint8Array;   // encoded OpenResponse
  call(Uint8Array): Uint8Array;   // encoded Response
  drain(): Uint8Array;            // encoded DrainResponse
  close(): void;
}
```

Every method is bytes-in/bytes-out, matching the `mk-bridge` envelopes
exactly. `load` returns the encoded durable rows and `commit` accepts one
encoded batch; a throw out of either is a `HostError` and poisons the
bridge.

A panic hook installed at module init flips a static flag: after a trap
escapes to JavaScript, every method on every instance answers the encoded
`{"type":"error","error":{"kind":"poisoned"}}` envelope instead of running
again. The crate feature `test-panic` adds a `triggerPanic()` export used
to exercise that path; it is never enabled in shipped builds.

On `wasm32-unknown-unknown` the nostr fork's `universal-time` facade needs
a time provider defined in the final artifact; `mk-wasm` supplies one over
`web-time`. It is the only ambient clock read in the artifact — `mk-bridge`
itself takes time through the host.
