# API drift: cgka-traits / cgka-engine / transport-nostr-peeler across v0.14.0 → v0.16.0

Tag → commit: `v0.14.0` = `3fb4b4759ac676b0c21c4c3527074866c9fa4c89`,
`v0.15.0` = `827687ba7fdbb05ec1b8f17667ad919fedde7266`,
`v0.16.0` = `a2e0558a634bbd20607cdd5afb93abc6220ee58d` (pinned in `upstream.json`).

Method: syntactic extraction of `pub trait` method names+return types and `pub enum`
variant shapes from `crates/traits/src`, `crates/cgka-engine/src`,
`crates/transport-nostr-peeler/src` after stripping comments/attributes
(nightly rustdoc JSON was not available locally). Verified spot-checks against
source. Sources: `spike/target/mdk-tags/{v0.14.0,v0.15.0,v0.16.0}` (gitignored).

## v0.14.0 → v0.15.0 (large: device-group feature landed)

### `CgkaEngine` trait (30 → 35 methods)
Added: `epoch_pin`, `expect_device_group_welcome`, `group_recovery_source`,
`own_signature_key`, `withdraw_device_group_welcome`. No removals.

### `SendIntent` enum (9 → 21 variants)
- `AppMessage.expected_epoch`: `Option<EpochId>` → `Option<EpochPin>` (breaking
  for callers that pinned epochs).
- Added: `RecoverMembers`, `AddOwnAccountDevices`, `RemoveOwnAccountDevices`,
  `LinkDevice`, `UnlinkDevice`, `RecordRemovedSlot`, `ImportDeviceGroupRejections`,
  `MarkDeviceGroupMoved`, `StepDownAndLeave`, `ReplaceDeviceEntry`,
  `ReplaceAccountEntry`, `EnableAccountDevices`.
- `Invite`, `RemoveMembers`, `Leave`, `SelfUpdate`, `UpdateAppComponents`,
  `UpdateGroupData`, `EnableDisbanding`, `Disband` unchanged.

### `GroupEvent` enum (17 → 18)
- `MessageReceived` gained `sender_leaf_index: Option<u32>`.
- Added `QueuedIntentRefused { group_id, intent_id, intent_kind, reason }`.

### `IngestOutcome` enum (8 → 9)
- Added `GroupRecovery { recovery }`.

### `TransportPeeler` trait (6 → 7)
- Added `wrap_group_recovery(...) -> Result<TransportMessage, PeelerError>`.

### Storage surface (18 sub-traits / 122 methods → 23 / 144)
New sub-traits a provider may implement (all behind `Option` accessors on
`StorageProvider`, default `None` — so an old provider still compiles):
- `PrivateKeyPackageStorage` (4 methods)
- `DeviceGroupWelcomeStorage` (5)
- `RosterTagKeyStorage` (3)
- `SiblingAddStorage` (3)
- `AccountDeviceHistoryKeyStorage` (2)

Changed:
- `StorageProvider` itself: + `after_commit`, `pending_ref_high_water`,
  `private_key_package_storage`, `roster_tag_key_storage`, `sibling_add_storage` (9 → 14).
- `GroupStorage`: + `invitation_admission`, `put_invitation_admission` (14 → 16).
- `CapabilityStorage`: `member_capabilities` / `save_member_capabilities`
  removed (4 → 2).

### Provider impact
Of the ~145 storage methods our provider implements at v0.16.0, the
v0.14→v0.15 bump would have required: 5 new optional sub-traits (17 methods —
skippable by returning `None` from the accessors, with feature loss: no private
KP lifecycle, no device-group welcomes, no roster tags, no sibling adds, no
account-device history), 2 new `GroupStorage` methods, 5 new `StorageProvider`
methods, and deletion of the 2 removed `CapabilityStorage` methods. Engine API
changes were additive except the `EpochPin` rename and `MessageReceived`'s new
field.

## v0.15.0 → v0.16.0 (small)

- `StorageProvider`: + `list_snapshots_with_prefixes` (14 → 15). One new method
  our provider implements (snapshot listing for forensics/repair).
- `GroupEvent::QueuedIntentRefused`: gained `artifact:
  Option<OversizedArtifactRef>` field.
- `SendIntent`, `IngestOutcome`, `CgkaEngine`, `TransportPeeler`,
  `AccountIdentityProofSigner`, `MarmotNostrSigner`, `EngineBuilder`: no
  detected changes.

### Provider impact
v0.15→v0.16 needed exactly **1 new method** (`list_snapshots_with_prefixes`)
and one new field matched on `QueuedIntentRefused`. The storage trait family
was otherwise stable (23 sub-traits, 144 → 145 methods).

## Summary

| bump        | storage traits | storage methods | provider methods to change | engine API break           |
|-------------|----------------|-----------------|----------------------------|----------------------------|
| 0.14 → 0.15 | 18 → 23        | 122 → 144       | ~7 required + 17 optional  | `EpochPin`, `+sender_leaf_index` |
| 0.15 → 0.16 | 23 → 23        | 144 → 145       | 1                          | none (additive field)      |

The trait surface is clearly converging: the device-group feature wave (0.15)
was the last large churn; 0.16 was nearly free for a storage-provider
implementation.
