# Spike storage provider — `KvStorageProvider`

Phase-A deliverable: a non-SQLite `cgka_traits::storage::StorageProvider`
over a synchronous host key/value boundary, mirroring `storage-sqlite`
semantics (MDK `v0.16.0`, rev `a2e0558a`).

## Host boundary

```rust
pub trait KvHost: Send + Sync {
    fn load_all(&self) -> Vec<(Vec<u8>, Vec<u8>)>;  // once, at open
    fn commit(&self, batch: Vec<KvOp>);              // one atomic batch
}
pub enum KvOp { Put(Vec<u8>, Vec<u8>), Delete(Vec<u8>) }
```

`KvStore` keeps a committed `BTreeMap` mirror plus a stack of overlay
maps. `with_transaction` pushes a layer; the outermost merge applies every
op to the mirror and issues exactly one `KvHost::commit(batch)`.
`with_group_savepoint` pushes another layer (dropping it on `Err` undoes
every write the closure made — a superset of the contract's group-scoped
undo). `InMemoryHost` (`Mutex<map>` + batch log) implements `dump()` /
`load()` for restart simulation; step 4 of the flow test dumps and
rebuilds member B mid-stream.

All reads hit the in-memory mirror; the host is only ever written, one
batch per committed unit.

## Trait inventory

`cgka_traits::StorageProvider` (storage.rs) aggregates 19 Marmot
sub-traits plus `type Mls: openmls_traits::storage::StorageProvider`.
Numbers are `implemented / declared in trait` (trait-provided defaults
cover the rest and need no override):

| Trait | impl / declared |
| --- | --- |
| GroupStorage | 16 / 16 |
| MessageStorage | 23 / 30 (7 use trait defaults: `has_messages_in_states`, `list_messages_in_states`, `list_openmls_messages_in_states`, `visit_messages_in_states`, `list_deferred_message_metadata`, `release_message_for_replay`, one more default) |
| OutboundIntentStorage | 7 / 7 |
| OutboundFanoutStorage | 5 / 5 |
| LeaveRequestStorage | 3 / 3 |
| DisbandRequestStorage | 3 / 3 |
| DisbandCandidateStorage | 4 / 4 |
| DisbandTombstoneStorage | 4 / 4 |
| WelcomeStorage | 3 / 3 |
| CapabilityStorage | 2 / 2 |
| ConvergencePolicyStorage | 2 / 2 |
| ConvergencePassStorage | 4 / 4 |
| DeferredPeelGenerationStorage | 3 / 3 |
| MemberValidationCacheStorage | 2 / 2 |
| AccountDeviceSignerStorage | 2 / 2 |
| AccountDeviceHistoryKeyStorage | 2 / 2 |
| DeviceGroupWelcomeStorage | 4 / 5 |
| KeyPackageBundleStorage | 2 / 2 |
| MaintenanceStorage | 21 / 21 |
| `StorageProvider` aggregate methods | 8 (incl. `with_transaction`, `with_group_savepoint`, `after_commit`, `mls_write_generation`, `pending_ref_high_water`, `backend`) |
| `openmls_traits::storage::StorageProvider` (`KvOpenMlsStorage`) | 56 |

**Stubs: 0.** Every method above is fully implemented. No
`unimplemented!`, no `Ok(default)` placeholders, no distinct-error stubs.

**Optional accessor stores not provided** (trait default `None`, by
design — mdk#2628/2761 device-linking features the flow does not reach):
`private_key_package_storage()`, `roster_tag_key_storage()`,
`sibling_add_storage()`. `maintenance_storage()` IS provided (the engine
calls it for key-material maintenance).

**`backend()`** returns `Backend::Sqlite` — the enum has no other variant
in v0.16.0; there is no honest value to report for a KV backend. Flagged
as an upstream gap.

## Serialization

Values are `serde_json`. Postcard/bincode are unusable: several OpenMLS
entities carry hand-written `Deserialize` impls that only work against a
self-describing format (storage-sqlite makes the same choice). Three
record families are not `DeserializeOwned` and use explicit tuple codecs:
`InvitationAdmissionTuple`, `TransportGroupRouteTuple`,
`AccountDeviceHistoryKeyTuple`. `CapabilityRequirement` contains
`description: &'static str`; it is persisted as
`(Capability, RequirementLevel, String)` and the description is
reconstructed by leaking a bounded, feature-registry-sized string
(`Box::leak` — at most a few hundred bytes per restore, once per
capability row).

## Layout

Prefix-namespaced keys, mirroring storage-sqlite's tables:

```
grp/ iad/ msg/ midx/ papp/ pauth/ ddm/ pti/ int/ oci/ fan/
lvr/ dbr/ dbc/ dbt/ wel/ cap/ cpo/ mvc/ ads/ ahk/ dgw/ mnt/ cps/ dpg/
rt/ for/ snap/ chki/ chkd/ mls/ gml/ meta/seq
```

`msg/<gid>/<seq>/<id>` carries a monotonically increasing sequence so
`list_messages` replays in insertion order; `midx/<id>` is the message-id
index that makes re-`put_message` idempotent on the original sequence.

OpenMLS rows use `build_key(label, entity_key)` identical to
storage-sqlite's `value_store.rs`, under `mls/<label>/<storage-key>`, and
each group-scoped write registers the storage key under
`gml/<group_key>/<storage_key>` so group delete, snapshots and
checkpoints can enumerate the group's rows.

### The `group_key` subtlety (found the hard way)

`group_key` must byte-match `serde_json::to_vec` of the **OpenMLS**
`GroupId` — which serializes as the struct `{"value":{"vec":[…]}}`, not a
bare byte array. An earlier version encoded `serde_json(&gid.to_vec())`
in the `gml/` prefix scan while `mls.rs` registered index rows under the
struct form. Every `capture_group`/`clear_group` scan then matched zero
rows: OpenMLS writes inside `SnapshotRollbackGuard` windows (candidate
replays) were never rolled back, the live `MlsGroup` stayed one epoch
ahead of the Marmot record, and every later convergence apply died in a
`WrongEpoch` livelock. Fixed by deriving the key from
`openmls::group::GroupId::from_slice`. This is exactly the class of bug a
SQLite backend hides because its `group_key` column is generated by one
code path — a KV provider must derive the index key and the scan prefix
from the same function.

## Atomicity contract

Engine-relevant multi-row units:

- `with_transaction` — convergence apply, ingest bookkeeping, publish
  confirm/fail, group create/join. One host batch on success, discard on
  `Err`/panic.
- `with_group_savepoint(gid, fallback, f)` — `apply_openmls_canonicalization_result`'s
  anchor refresh + rewind + selected-branch apply; `SnapshotRollbackGuard`
  windows (candidate replay, retained-anchor rewinds, peel probes). Our
  layered overlay makes the rollback total.
- `rollback_group_state_keeping_live_records` / `create_group_state_snapshot` /
  `rollback_group_state_to_snapshot` / `release_group_snapshot` /
  `list_group_snapshots` — named snapshot rows under `snap/` covering the
  group's canonical rows (group record + `gml`-indexed OpenMLS state +
  convergence pass/policy/maintenance rows), plus the message ledger,
  outbound intents/fanouts, leave/disband rows, pending app events and
  routes for full snapshots. "Canonical only" excludes the ledger and
  outbound rows — the guard contract requires live records to stay.
- `create/restore/list/release_group_state_checkpoint` — `chki/`+`chkd/`
  commit-addressed canonical-state images used by own-commit replay.
- `queue_proposal`, `put_message` + `midx`, `put_pending_application_event`
  + `pauth` request, `put_message` + `put_processed_transport_id` —
  multi-row writes the engine wraps in `with_transaction`; each is also
  correct as a single-op unit when the engine calls it bare.
- `after_commit` — callbacks run immediately outside a unit, queue until
  the outermost commit inside one, and are dropped on rollback.
- `mls_write_generation` — bumped on every committed batch (including
  checkpoint restores and snapshot rollbacks), which is what the engine's
  `MlsGroup` reuse cache needs; `None` would disable reuse and would also
  be defensible.

## Sync API vs async host

The engine's storage surface is fully synchronous. IndexedDB is not.
That forces the mirror-in-memory design: `load_all()` hydrates the
`BTreeMap` once at open, every storage call is served from memory, and
durability work happens only at `commit(batch)` boundaries. On IndexedDB
the `commit` call itself must enqueue the batch and return before the
write lands — the provider cannot await.

**Publish-before-apply consequence.** The engine's contract is that a
staged commit is durable *before* the transport publishes it
(`confirm_published`/`publish_failed` resolve the publish afterwards).
With a lagging async host the batch for the *staged* write is already
committed to the mirror but possibly not yet flushed to IndexedDB. If
the process dies in that window, the publish succeeded while the staged
intent was lost — the peer applies the commit, we never did. The host
boundary must therefore treat `commit` as "accepted and guaranteed
durable"; an async host either (a) flushes synchronously enough that the
engine's next read of the mirror is safe and orders its own write queue,
or (b) keeps an outbox: the transport layer must not publish until the
host acknowledges the batch that staged it. `KvHost::commit` returning
before durability is the documented risk; a production host should make
`commit` hand off to a write-ahead queue and let the transport publish
hook wait for its ack.

**Restart.** `InMemoryHost::dump()` serializes the committed map (the
host's authoritative state); `load()` rebuilds a member at a snapshot.
Step 4 proves mid-group restart recovery: 252 batches at dump time, all
later messages readable, no earlier message redelivered.
