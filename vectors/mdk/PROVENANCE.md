# MDK conformance vectors — provenance

Byte-for-byte copies of the cgka-conformance-simulator vector fixtures,
frozen at the MDK pin in `upstream.json` (`mdk`). Do not edit them; they
move only when the pin moves. `bun run check:pins` verifies them against a
local `3rdparty/mdk` checkout when present.

- repository: https://github.com/marmot-protocol/mdk
- tag: v0.17.0
- rev: 4df811aea99269b437a6023be9446a5141e93cbe

| recorded file | source path (repo root) | sha256 |
| --- | --- | --- |
| `account-devices-activation-loses-to-unsupported-invite.v1.json` | `crates/cgka-conformance-simulator/vectors/account-devices-activation-loses-to-unsupported-invite.v1.json` | `04ac9521c1c0f5d02365eda0162fd7a4f6f976467ca2943f3249eb1318a28f1f` |
| `account-devices-activation-needs-support.v1.json` | `crates/cgka-conformance-simulator/vectors/account-devices-activation-needs-support.v1.json` | `fad7c6164bf97d89840028b52ac89e9cbb39bd157a3a6c69694aff1abce708c3` |
| `account-devices-activation-wins-over-unsupported-invite.v1.json` | `crates/cgka-conformance-simulator/vectors/account-devices-activation-wins-over-unsupported-invite.v1.json` | `dfb18dc82cbadeb287d7eabdd693f27aa07e531e8357be6b1a7af4e852917d97` |
| `account-devices-activation.v1.json` | `crates/cgka-conformance-simulator/vectors/account-devices-activation.v1.json` | `96ca92f5a34df27ad8f7a5c60116581fa87b0173a25a91fec1cfc2b80138d8e3` |
| `account-devices-founding.v1.json` | `crates/cgka-conformance-simulator/vectors/account-devices-founding.v1.json` | `149b84ed7d1aba65cc787cddcbd841613182718080fabe959e0b434209d8f908` |
| `account-devices-own-device-removal-loses-to-admin-remove.v1.json` | `crates/cgka-conformance-simulator/vectors/account-devices-own-device-removal-loses-to-admin-remove.v1.json` | `4db355bae1e3ade2bb7ec9c51966b71cfc50e967008efc6347481cd33ffca77a` |
| `admin-handover.v1.json` | `crates/cgka-conformance-simulator/vectors/admin-handover.v1.json` | `4d0dc9642d7c2e6011a36211e0d59e6afe5b66d8240e994cecc818320f16e150` |
| `admin-policy-update.v1.json` | `crates/cgka-conformance-simulator/vectors/admin-policy-update.v1.json` | `fd032eab42d032b8200b68b14e402731ca8fcd0c75226961c8e0d9f81727c3a9` |
| `concurrent-invite-fork-recovery.v1.json` | `crates/cgka-conformance-simulator/vectors/concurrent-invite-fork-recovery.v1.json` | `5e1d5b26e72e62e0cd37666ad364bd8cf579d936a3683219d83803a20d285b43` |
| `convergence-committer-selected.v1.json` | `crates/cgka-conformance-simulator/vectors/convergence-committer-selected.v1.json` | `40dc6702108d7c40e97a89a1f121e8990865cecd1cc3e548c2d4e1480aae8d85` |
| `convergence-witness-selected.v1.json` | `crates/cgka-conformance-simulator/vectors/convergence-witness-selected.v1.json` | `00440d2ef8080ff31ed10e4cf1aeea1cb8272e61a3d04fb2f1b9a60dbe95c1ae` |
| `conversation.v1.json` | `crates/cgka-conformance-simulator/vectors/conversation.v1.json` | `9e4282eb925e144b26a47796090e6580aac48c0047a9e0d61cedfb149239c895` |
| `cross-route-own-commit-recovery.v1.json` | `crates/cgka-conformance-simulator/vectors/cross-route-own-commit-recovery.v1.json` | `f173b906093db034e1f4e69513c27d1f779e9be891d37f4588b9fb3795855da7` |
| `current-profile-required-set.v1.json` | `crates/cgka-conformance-simulator/vectors/current-profile-required-set.v1.json` | `a27eb9601cd40e439bb51b9ec4be61f070417a12d166ca56db35f5e7f7a0d903` |
| `deferred-tick-catchup.v1.json` | `crates/cgka-conformance-simulator/vectors/deferred-tick-catchup.v1.json` | `bac47c3c7f5e52c86841e7d1ffb65a25991a39056962a257f6837a47c4044221` |
| `delayed-past-epoch-app-message.v1.json` | `crates/cgka-conformance-simulator/vectors/delayed-past-epoch-app-message.v1.json` | `22e5c8f1959b75e0b3521f987f97a825a297882dd8abf824d012a5d807f65afc` |
| `drop-queued.v1.json` | `crates/cgka-conformance-simulator/vectors/drop-queued.v1.json` | `07a8c418e10a3087c4386519f086cf0bce425fe0a1bd4d24311e9d9ba742d711` |
| `group-data-fork-recovery.v1.json` | `crates/cgka-conformance-simulator/vectors/group-data-fork-recovery.v1.json` | `5e14c9ea884cc2da0bd045d331dbb05c6a8212833a6e45d74cb0975fe2063fe1` |
| `group-data-update.v1.json` | `crates/cgka-conformance-simulator/vectors/group-data-update.v1.json` | `84992b3154c366eb50156f6d9f42019a43d658522d68863ed00de9bc06772b7c` |
| `group-profile-update.v1.json` | `crates/cgka-conformance-simulator/vectors/group-profile-update.v1.json` | `8f6dcdb6de61ffc07b7d9ea9d894e9521d707eb9afe0dbae50eac406dd3fdb23` |
| `incremental-growth.v1.json` | `crates/cgka-conformance-simulator/vectors/incremental-growth.v1.json` | `f63e7b3bcc3420901176a63a2ed9dc92f4d342c7f6b6703c6f539edc06e9b1d5` |
| `invite-member.v1.json` | `crates/cgka-conformance-simulator/vectors/invite-member.v1.json` | `056c29304682619adc01ecd45d3a9a96f49c04903fc15672c14cf7838ff3add4` |
| `invite-publish-fail.v1.json` | `crates/cgka-conformance-simulator/vectors/invite-publish-fail.v1.json` | `e5121f3cbbf51958954f5156808582b2c09496791f6c469671ca1280b5c89b20` |
| `late-welcome-backfill.v1.json` | `crates/cgka-conformance-simulator/vectors/late-welcome-backfill.v1.json` | `4480af5d04ed1ac6d0f233f61fa5abce75d61fffa98bc350442fe10d0131ebb6` |
| `latecomer-forward-secrecy.v1.json` | `crates/cgka-conformance-simulator/vectors/latecomer-forward-secrecy.v1.json` | `373785cdcdceaf017e2f167c47541fea88bb4528d28e8851028591ae820ec976` |
| `leaver-removal-secrecy.v1.json` | `crates/cgka-conformance-simulator/vectors/leaver-removal-secrecy.v1.json` | `6361b5abf42540baad92c0218d9851b3f4ac5b8da9764357973ec9c5f3ba05e3` |
| `manifest.v1.json` | `crates/cgka-conformance-simulator/vectors/manifest.v1.json` | `104d1d026eb0b41557d9e018fe12fb87b7c9bd149406379251368b7e6a509f05` |
| `multigroup-isolation.v1.json` | `crates/cgka-conformance-simulator/vectors/multigroup-isolation.v1.json` | `c63028a47d7dc259f8a2eb5a9db28d9e8eb97a447965a84237b7dab5ab0aa6cc` |
| `partition-clear-leave.v1.json` | `crates/cgka-conformance-simulator/vectors/partition-clear-leave.v1.json` | `8b4bab875c37162f269c728ee286a85ed8f325b5c191bd77e7a486e8ca025ecb` |
| `publish-fail.v1.json` | `crates/cgka-conformance-simulator/vectors/publish-fail.v1.json` | `4e3b794fb8a4ecd5ae17c572777d99eaa5373a14be7d421ca82a9f8ec2117e4e` |
| `queue-faults.v1.json` | `crates/cgka-conformance-simulator/vectors/queue-faults.v1.json` | `c2e43a149e6753fe2a6755bee5519f7b986ab13da6daaa7fb101a6d6c3cd8ddf` |
| `readd-after-eviction.v1.json` | `crates/cgka-conformance-simulator/vectors/readd-after-eviction.v1.json` | `177c9ee62d45df2294066fd5b02b982103a22fd8479e7d8f02f5f29da89ce724` |
| `restart-delivery-faults.v1.json` | `crates/cgka-conformance-simulator/vectors/restart-delivery-faults.v1.json` | `bdb08bce2d2184679059e0b6d4f0a6efe946099b7407aac88214b1150869082e` |
| `route-equivalence.v1.json` | `crates/cgka-conformance-simulator/vectors/route-equivalence.v1.json` | `104e610eb691e3d1d129989c2989e28745e6ce3fffc036361076cf6fd1988b4f` |
| `three-client-message-exchange.v1.json` | `crates/cgka-conformance-simulator/vectors/three-client-message-exchange.v1.json` | `04873ecb124f592eae7a1ecc754aac8758e5928d3ee0364a01511bc6a10276ef` |
| `incidents/convergence-incident.v1.json` | `crates/cgka-conformance-simulator/vectors/incidents/convergence-incident.v1.json` | `302a4901c8e41ff7aeea9edc3fdab4939e3121c109ebbd69f65f47b025865ec9` |
| `incidents/fork-recovery-incident.v1.json` | `crates/cgka-conformance-simulator/vectors/incidents/fork-recovery-incident.v1.json` | `811e4959603671787e8c28000eb48b12d452ea41e9e93e2a78f6ca0bcbd0d0d3` |
| `incidents/membership-fork-recovery-incident.v1.json` | `crates/cgka-conformance-simulator/vectors/incidents/membership-fork-recovery-incident.v1.json` | `5d5e3b4cfe0d83770d7524172d50588ce54b220392dcf8cfbf7d80d22b105f44` |
| `encrypted-media/imeta-v1.json` | `fixtures/encrypted-media/imeta-v1.json` | `24adea4c86f4c9bae04f69a1f5a183c2dc88b78fb9fd9bbb2355d584a7be82ee` |
| `encrypted-media/imeta-v2.json` | `fixtures/encrypted-media/imeta-v2.json` | `fbab0c8dbdd60efa6439ab97c0cd2c14ae9e6a9423dd67ad32d440eda152a578` |
