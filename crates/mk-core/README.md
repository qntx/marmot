# mk-core

Marmot kit core: a pure-Rust persistent key-value storage provider for the MDK
engine, plus the session API built on it.

The crate exposes `KvStorageProvider`, an implementation of the MDK
`cgka_traits::storage::StorageProvider` aggregate and the OpenMLS storage
trait whose observable behavior matches MDK's `storage-sqlite`. The host
persists an ordered sequence of atomic `KvBatch` commits through the `KvHost`
trait; reopening replays the durable rows.

See the crate-level rustdoc for the host contract, the key schema, and the
memory-growth inventory.

## White Noise parity

The session advertises exactly the capabilities it implements: app
components `{0x8001, 0x8003, 0x8004, 0x8006, 0x8009, 0x800b, 0x800c,
0x800d}` and extension `0xf2d1` (agent text stream QUIC receive,
`Optional`). Stream send (`0xf2d2`), fanout (`0xf2d4`) and encrypted media
v1 (`0x8008`) are never advertised.

Two protocol surfaces live in pure modules:

- `media`: strict encrypted-media-v2 `imeta` parse/serialize
  (`parse_imeta`, `MediaReference::to_imeta`), the file key schedule
  (HKDF-SHA256 + ChaCha20-Poly1305 over `encrypt_file`/`decrypt_file`),
  `EncryptedMediaPolicy` <-> `0x800b` component codec, `BlobEndpoint`
  destination classification, and the shared relay/blob
  `DestinationPolicy` (SSRF) guard.
- `agent_stream`: the `0x8006` policy codec (`AgentStreamPolicy`,
  `StreamRoles`) and the stream start/final tag anchors
  (`parse_stream_start`, `parse_stream_final`).

`Session` projects these into host-facing shapes: kind-9 messages carry
`attachments` (one `Result<MediaReference, AttachmentRejection>` per
`imeta` tag — a rejected attachment never drops the message) and
`stream_final`; kind-1200 stream starts arrive as
`SessionEvent::AgentStreamStarted`. `seal_attachment`/`send_media`/
`open_attachment` cover encrypt/upload/send/decrypt with per-epoch
exporter secrets persisted under `(group, transport id)` and cleared on
message invalidation and group deletion.
