# `vectors/bridge/` — mk-bridge wire vectors

Canonical JSON encodings for the `mk-bridge` host protocol (crate
`crates/mk-bridge`). One file per wire enum: a JSON object mapping each
variant tag to one canonical encoding. `config.json` is one open config and
`call.json` one call envelope.

Encoding rules: UTF-8 JSON; byte arrays are lowercase hex strings; `u64`
values are decimal strings; `u8`/`u16`/`u32` are JSON numbers; options are
always present (`null` when absent). Data-carrying enums are internally
tagged `"type"` (the error enum uses `"kind"`), fieldless enums are plain
`snake_case` strings.

| File | Type |
| --- | --- |
| `request.json` | `Request` |
| `result.json` | `OpResult` |
| `response.json` | `Response` |
| `open_response.json` | `OpenResponse` |
| `event.json` | `SessionEvent` |
| `error.json` | `BridgeError` |
| `signer_op.json` | `SignerOp` |
| `signer_result.json` | `SignerResult` |
| `send_outcome.json` | `SendOutcome` |
| `message_intent.json` | `MessageIntent` |
| `semantics.json` | `Semantics` |
| `drop_reason.json` | `DropReason` |
| `publish_resolution.json` | `PublishResolution` |
| `input_outcome.json` | `InputOutcome` |
| `availability_category.json` | `AvailabilityCategory` |
| `group_change.json` | `GroupChange` |
| `relay_status.json` | `RelayStatus` |
| `outbound_purpose.json` | `OutboundPurpose` |
| `group_state.json` | `GroupState` |
| `local_state.json` | `LocalState` |
| `attachment_result.json` | `AttachmentResult` |
| `config.json` | `Config` |
| `call.json` | `CallEnvelope` |

`crates/mk-bridge/tests/vectors.rs` decodes every entry, re-encodes it,
asserts `serde_json::Value` equality, checks the key equals the decoded
variant's tag, and checks the key set equals the enum's full serde-derived
variant set.
