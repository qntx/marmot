# MDK conformance vectors — provenance

Byte-for-byte copies of the cgka-conformance-simulator vector fixtures,
frozen at the MDK pin in `upstream.json` (`mdk`). Do not edit them; they
move only when the pin moves. `bun run check:pins` verifies them against a
local `3rdparty/mdk` checkout when present.

- repository: https://github.com/marmot-protocol/mdk
- tag: v0.18.0
- rev: 34646a577807f2b112ee031f3c164ec58aa18b64

| recorded file | source path (repo root) | sha256 |
| --- | --- | --- |
| `account-devices-activation-loses-to-unsupported-invite.v1.json` | `crates/cgka-conformance-simulator/vectors/account-devices-activation-loses-to-unsupported-invite.v1.json` | `b5672045994927be57c8bbb36c6c1e01f7665f9454478c794d3f4911bcfe3466` |
| `account-devices-activation-needs-support.v1.json` | `crates/cgka-conformance-simulator/vectors/account-devices-activation-needs-support.v1.json` | `2d1af4a54025c2ea58840868afac91a1e1af0997d759d0c5234d4b5362a224ed` |
| `account-devices-activation-wins-over-unsupported-invite.v1.json` | `crates/cgka-conformance-simulator/vectors/account-devices-activation-wins-over-unsupported-invite.v1.json` | `0529453446014606fbb3d4a12fdc986074b2b4abf61ede0a2d77245bc212e2e9` |
| `account-devices-activation.v1.json` | `crates/cgka-conformance-simulator/vectors/account-devices-activation.v1.json` | `3ef0450f50572dda1cbc809c2c8179b4cf91699ce271b6896d43d279d16167eb` |
| `account-devices-founding.v1.json` | `crates/cgka-conformance-simulator/vectors/account-devices-founding.v1.json` | `06743ec466e1626b09ad345fdb7eade02d680cd41fda75eaacbdd51829ab92e8` |
| `account-devices-own-device-removal-loses-to-admin-remove.v1.json` | `crates/cgka-conformance-simulator/vectors/account-devices-own-device-removal-loses-to-admin-remove.v1.json` | `5ad5f276ff213d138152013081f6653a541ee0158fc83dd25479ecdef9727826` |
| `admin-handover.v1.json` | `crates/cgka-conformance-simulator/vectors/admin-handover.v1.json` | `333f1131b18e4ec65544c7e4eef34bc728047b3e13f3059f5d10ff8632d58f7e` |
| `admin-policy-update.v1.json` | `crates/cgka-conformance-simulator/vectors/admin-policy-update.v1.json` | `51c7480cfb4909e7249ff4439ecd5206579908e089df540e2c2550c39b0351e4` |
| `concurrent-invite-fork-recovery.v1.json` | `crates/cgka-conformance-simulator/vectors/concurrent-invite-fork-recovery.v1.json` | `4db698960e895f68a9565c8df36e2d886e29927a549e9ed6dfceb1438bfa61c7` |
| `convergence-committer-selected.v1.json` | `crates/cgka-conformance-simulator/vectors/convergence-committer-selected.v1.json` | `f7a571313ff5ce97993ab26028f3bab695bddc8a46bd8560c05f6c370bb02b02` |
| `convergence-witness-selected.v1.json` | `crates/cgka-conformance-simulator/vectors/convergence-witness-selected.v1.json` | `e49356712a228c6489ad3c6b4e83d55a4650183eb48c64c0e9646546ec7bfe85` |
| `conversation.v1.json` | `crates/cgka-conformance-simulator/vectors/conversation.v1.json` | `61227e09779302aa00ebc029aa5d1fdfa311696aec186062f69203c8d5a92738` |
| `cross-route-own-commit-recovery.v1.json` | `crates/cgka-conformance-simulator/vectors/cross-route-own-commit-recovery.v1.json` | `68c590f7fe21bdd006c3ffad337cb30c3fe79ba5b8e1c3b38104cc7458799be8` |
| `current-profile-required-set.v1.json` | `crates/cgka-conformance-simulator/vectors/current-profile-required-set.v1.json` | `abc88f401dd7a36033d09e9226f34f9dbd2a33567e4c6b752026b306a84f9f3e` |
| `deferred-tick-catchup.v1.json` | `crates/cgka-conformance-simulator/vectors/deferred-tick-catchup.v1.json` | `0fc46e488969df760ea05f39c28f7cc3f0fd4ed7cf0baeaa3bf4298e8cad2650` |
| `delayed-past-epoch-app-message.v1.json` | `crates/cgka-conformance-simulator/vectors/delayed-past-epoch-app-message.v1.json` | `8015f343a469ae0d864e5ea72f164741511c208da932d566e74ef9ac889fa33d` |
| `drop-queued.v1.json` | `crates/cgka-conformance-simulator/vectors/drop-queued.v1.json` | `4b59e3163c7808acb8538f3bfdc07645e43999a8ea3ea19c3946403a6c12287a` |
| `group-data-fork-recovery.v1.json` | `crates/cgka-conformance-simulator/vectors/group-data-fork-recovery.v1.json` | `5ae843fbcfccb8858e36d2b8b99c5f1a77900978c80e79cafd5b796b4b5b6e15` |
| `group-data-update.v1.json` | `crates/cgka-conformance-simulator/vectors/group-data-update.v1.json` | `8dd630acb1a90e3804e2a7b6b9d2984db474cdc8b6193fa2dd743ede85cbaff0` |
| `group-profile-update.v1.json` | `crates/cgka-conformance-simulator/vectors/group-profile-update.v1.json` | `11384a693edf2839de9bef65824df02d646a82cb14ab2cb2e6743b8c40ddfa23` |
| `incremental-growth.v1.json` | `crates/cgka-conformance-simulator/vectors/incremental-growth.v1.json` | `11464f4a4da71c4baf2c34129f68acae0a07ad814607fa656733cbf89e90ddf0` |
| `invite-member.v1.json` | `crates/cgka-conformance-simulator/vectors/invite-member.v1.json` | `4e1d65841aa444aa5a95c17b41cc9dc0cfb2bb9df4cc97504733ed0c26899140` |
| `invite-publish-fail.v1.json` | `crates/cgka-conformance-simulator/vectors/invite-publish-fail.v1.json` | `ddfcb136e027a012f17d92440a4424c452d61053ca11ef83a0050b81e726103e` |
| `late-welcome-backfill.v1.json` | `crates/cgka-conformance-simulator/vectors/late-welcome-backfill.v1.json` | `8ab7180162437076ca520572a35b0728681716b7b664ec17bfe634934720c89a` |
| `latecomer-forward-secrecy.v1.json` | `crates/cgka-conformance-simulator/vectors/latecomer-forward-secrecy.v1.json` | `7bce909035e711b21fdae09279aafa69e7d4616718205829ff8f8c169e3e0ac7` |
| `leaver-removal-secrecy.v1.json` | `crates/cgka-conformance-simulator/vectors/leaver-removal-secrecy.v1.json` | `a8ff7f41cdb45137b16e49fe2f200592ee8c51797d7036d71c75c699c819c83e` |
| `manifest.v1.json` | `crates/cgka-conformance-simulator/vectors/manifest.v1.json` | `104d1d026eb0b41557d9e018fe12fb87b7c9bd149406379251368b7e6a509f05` |
| `multigroup-isolation.v1.json` | `crates/cgka-conformance-simulator/vectors/multigroup-isolation.v1.json` | `75b91380be3f5e123b8b7eb62e252678d5ebc70cf618bdcef1240d5ca45852bc` |
| `partition-clear-leave.v1.json` | `crates/cgka-conformance-simulator/vectors/partition-clear-leave.v1.json` | `ef2818b3c4c13950ac2f52fbc1e75604f40e2dc6bd24eccce4558e0dd9d8f126` |
| `publish-fail.v1.json` | `crates/cgka-conformance-simulator/vectors/publish-fail.v1.json` | `58b5245d0b99eae7a71351892307f7075a0fd303b037f97cfa74df120b6bf29b` |
| `queue-faults.v1.json` | `crates/cgka-conformance-simulator/vectors/queue-faults.v1.json` | `c9a2944e5bd42824059cce952a7deecce9856133281386e1dc45b5381d8de769` |
| `readd-after-eviction.v1.json` | `crates/cgka-conformance-simulator/vectors/readd-after-eviction.v1.json` | `e9dcd1476603288287aa7823477e3be556d3195d0388486d19a03df40b3c581f` |
| `restart-delivery-faults.v1.json` | `crates/cgka-conformance-simulator/vectors/restart-delivery-faults.v1.json` | `6c65d32395233a9765b5174acc560331f00de15946c3b398ae0e92b277bb0755` |
| `route-equivalence.v1.json` | `crates/cgka-conformance-simulator/vectors/route-equivalence.v1.json` | `465d394b5c6cba5ed1e3a6506a0f1386c921e87cc0f0c098bd15e5c796a18875` |
| `three-client-message-exchange.v1.json` | `crates/cgka-conformance-simulator/vectors/three-client-message-exchange.v1.json` | `9a2a876db10cbc6ee530170374bacc6af93b49cb5c1538e8decb17732326459d` |
| `incidents/convergence-incident.v1.json` | `crates/cgka-conformance-simulator/vectors/incidents/convergence-incident.v1.json` | `d5cb19524e901a11e335238f80f103b952977ff90c16c87221fb42b803bd25cb` |
| `incidents/fork-recovery-incident.v1.json` | `crates/cgka-conformance-simulator/vectors/incidents/fork-recovery-incident.v1.json` | `12d80e917f8168a21a60b3761ef40f9a7379321782a8fb6a6aa7ef21b2d82365` |
| `incidents/membership-fork-recovery-incident.v1.json` | `crates/cgka-conformance-simulator/vectors/incidents/membership-fork-recovery-incident.v1.json` | `222eb69bb7936b409781755ad42f21ee259ab7b5c6437faa4af268b2a346243c` |
| `encrypted-media/imeta-v1.json` | `fixtures/encrypted-media/imeta-v1.json` | `24adea4c86f4c9bae04f69a1f5a183c2dc88b78fb9fd9bbb2355d584a7be82ee` |
| `encrypted-media/imeta-v2.json` | `fixtures/encrypted-media/imeta-v2.json` | `fbab0c8dbdd60efa6439ab97c0cd2c14ae9e6a9423dd67ad32d440eda152a578` |
