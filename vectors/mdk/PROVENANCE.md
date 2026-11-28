# MDK conformance vectors — provenance

Byte-for-byte copies of the cgka-conformance-simulator vector fixtures,
frozen at the MDK pin in `upstream.json` (`mdk`). Do not edit them; they
move only when the pin moves. `bun run check:pins` verifies them against a
local `3rdparty/mdk` checkout when present.

- repository: https://github.com/marmot-protocol/mdk
- tag: v0.19.0
- rev: 3fce95465cdea1edc8456dbd1c6691d1c8dc1f2e

| recorded file | source path (repo root) | sha256 |
| --- | --- | --- |
| `account-devices-activation-loses-to-unsupported-invite.v1.json` | `crates/cgka-conformance-simulator/vectors/account-devices-activation-loses-to-unsupported-invite.v1.json` | `4b210a235a2ff2b1e12205d993a0a9ccf7d1365c0b839c2002cfb414b5e45144` |
| `account-devices-activation-needs-support.v1.json` | `crates/cgka-conformance-simulator/vectors/account-devices-activation-needs-support.v1.json` | `e1ad5363a4c885cac986b60e94c65514437ac996e53a0ad1aa6e3ac68d4675db` |
| `account-devices-activation-wins-over-unsupported-invite.v1.json` | `crates/cgka-conformance-simulator/vectors/account-devices-activation-wins-over-unsupported-invite.v1.json` | `5c8ad0cd176edc06f011865d7736be6fafa80fd3f05127e1a122479cc0a751e2` |
| `account-devices-activation.v1.json` | `crates/cgka-conformance-simulator/vectors/account-devices-activation.v1.json` | `2d8f8a29592e01a50022a5d76c6bfa3e1257bcccd3d6b1425bc24cc6eaccbc4d` |
| `account-devices-founding.v1.json` | `crates/cgka-conformance-simulator/vectors/account-devices-founding.v1.json` | `ba4defc51402071b132528240f31eca7caa1fad13883454d044faeb1c0d56195` |
| `account-devices-own-device-removal-loses-to-admin-remove.v1.json` | `crates/cgka-conformance-simulator/vectors/account-devices-own-device-removal-loses-to-admin-remove.v1.json` | `7dd8b1fbee0396f8d1100eac542dfdb44a6c512d35dfd25f56595b3f6f82a2d0` |
| `admin-handover.v1.json` | `crates/cgka-conformance-simulator/vectors/admin-handover.v1.json` | `4763c40617e7fbd6be1c7e839e5a4963d0b573e5c3e043f65b7e79a10a1db583` |
| `admin-policy-update.v1.json` | `crates/cgka-conformance-simulator/vectors/admin-policy-update.v1.json` | `f39d34d3295bb4ff93ce6e94c1a729028d4008635b49701332306cf910ab1473` |
| `concurrent-invite-fork-recovery.v1.json` | `crates/cgka-conformance-simulator/vectors/concurrent-invite-fork-recovery.v1.json` | `a3e10d448781bbe3ebfeab65039b1c83a169f467073eb5dacee9555b8418a88d` |
| `convergence-committer-selected.v1.json` | `crates/cgka-conformance-simulator/vectors/convergence-committer-selected.v1.json` | `30689e83b9f7fc308a5c59c36d0429e94140604208cd255af6e9ee6531d03a31` |
| `convergence-witness-selected.v1.json` | `crates/cgka-conformance-simulator/vectors/convergence-witness-selected.v1.json` | `80285bf36100f10cd3c4b5a4a0a75a77446d284a46bb7f530ff4af5037ea20c6` |
| `conversation.v1.json` | `crates/cgka-conformance-simulator/vectors/conversation.v1.json` | `3ee4d473bbbe3b385fc3b90df84575dacb669f09d6b313f24f81adecfc41128d` |
| `cross-route-own-commit-recovery.v1.json` | `crates/cgka-conformance-simulator/vectors/cross-route-own-commit-recovery.v1.json` | `022ba217a3460dab60e0b40f0f57b540eec8275053adaf800712d8ab5185fe7e` |
| `current-profile-required-set.v1.json` | `crates/cgka-conformance-simulator/vectors/current-profile-required-set.v1.json` | `330c465c017ed7dcf7f059a24e8c40ec4a7c9cc3f6905405dd4bdf6f4c6c25b4` |
| `deferred-tick-catchup.v1.json` | `crates/cgka-conformance-simulator/vectors/deferred-tick-catchup.v1.json` | `68c1afe4d5b3f06c9f726dd71898e8def2fe9fc6c6db351a3ff8f237fc9d0e26` |
| `delayed-past-epoch-app-message.v1.json` | `crates/cgka-conformance-simulator/vectors/delayed-past-epoch-app-message.v1.json` | `96e7f73e8e010415c3cf99306074f9de25c8b3d42dcfce2c92dd64335aabbf4e` |
| `drop-queued.v1.json` | `crates/cgka-conformance-simulator/vectors/drop-queued.v1.json` | `11a16689466743f63e86bda18f29b8f684c527c93b7e698e015e9ce31281201a` |
| `group-data-fork-recovery.v1.json` | `crates/cgka-conformance-simulator/vectors/group-data-fork-recovery.v1.json` | `b5bf20f0970549567f22fb99490b5c2b816ef0ea4a28a613aff1385938922fbe` |
| `group-data-update.v1.json` | `crates/cgka-conformance-simulator/vectors/group-data-update.v1.json` | `fcfb4f58239b50d64ad006044180741fbde2ef2f7f958fecaab13331b4727adf` |
| `group-profile-update.v1.json` | `crates/cgka-conformance-simulator/vectors/group-profile-update.v1.json` | `cf3e3eca491d30f8ae06793fb74dfbf89fb85035545c15344924ba35a5ca9a82` |
| `incremental-growth.v1.json` | `crates/cgka-conformance-simulator/vectors/incremental-growth.v1.json` | `c5895c93a033a35eb4ac44ae1da2fcf24fadf7a599c74027b8374525baa10484` |
| `invite-member.v1.json` | `crates/cgka-conformance-simulator/vectors/invite-member.v1.json` | `cc333e96a2ea19d17457519012b102c9ebe01eab8b901ef82633a2de2db32631` |
| `invite-publish-fail.v1.json` | `crates/cgka-conformance-simulator/vectors/invite-publish-fail.v1.json` | `2c8a4c2a12bdcd34fcd4815e6302f0e3fb7038c14905df4945b5e132195b0c49` |
| `late-welcome-backfill.v1.json` | `crates/cgka-conformance-simulator/vectors/late-welcome-backfill.v1.json` | `bd1569872857e77725fac26e3249252798b34b58748950fa02aedd36d36c42e9` |
| `latecomer-forward-secrecy.v1.json` | `crates/cgka-conformance-simulator/vectors/latecomer-forward-secrecy.v1.json` | `8d647710d77cb2757623dfa2e8532fb1a2cb3c712318c7f69fc54940a40a98fa` |
| `leaver-removal-secrecy.v1.json` | `crates/cgka-conformance-simulator/vectors/leaver-removal-secrecy.v1.json` | `637f0dd250e74d871081cddf064c0c1d84d361133415d567a48c3f17ad09c8ad` |
| `manifest.v1.json` | `crates/cgka-conformance-simulator/vectors/manifest.v1.json` | `104d1d026eb0b41557d9e018fe12fb87b7c9bd149406379251368b7e6a509f05` |
| `multigroup-isolation.v1.json` | `crates/cgka-conformance-simulator/vectors/multigroup-isolation.v1.json` | `c61937f27d9a0951d7c3c12dcb530422383d40fa436b052689ab444851c170c4` |
| `partition-clear-leave.v1.json` | `crates/cgka-conformance-simulator/vectors/partition-clear-leave.v1.json` | `35002fa6389576238d74f88e321340fa6a0040de6daa4ca940362bbde5116448` |
| `publish-fail.v1.json` | `crates/cgka-conformance-simulator/vectors/publish-fail.v1.json` | `67ac4fd340af74675f428910b074cb02cbf5afa9f42dd628e9ddd496aa5b5181` |
| `queue-faults.v1.json` | `crates/cgka-conformance-simulator/vectors/queue-faults.v1.json` | `a5bb6f171e42ec6ee32c3f4c57aa4af62e256a72d8476e315b2e4fee44f9ad88` |
| `readd-after-eviction.v1.json` | `crates/cgka-conformance-simulator/vectors/readd-after-eviction.v1.json` | `bd442d6ab8ff926951db463ad9af0a22e3d37eefe1ba3f5c91a85861d94cd86b` |
| `restart-delivery-faults.v1.json` | `crates/cgka-conformance-simulator/vectors/restart-delivery-faults.v1.json` | `91651e8bdff34e54e7b74bdf61c216770be0b1d8ee101dc5027725616955989f` |
| `route-equivalence.v1.json` | `crates/cgka-conformance-simulator/vectors/route-equivalence.v1.json` | `679efb0e4df9917e98ec512cf1d8791767698922b6378f88108654f347b6cfc2` |
| `three-client-message-exchange.v1.json` | `crates/cgka-conformance-simulator/vectors/three-client-message-exchange.v1.json` | `65fab1955fe94f29263305bad5770266559e319281acc1190408d58a56b55ef8` |
| `incidents/convergence-incident.v1.json` | `crates/cgka-conformance-simulator/vectors/incidents/convergence-incident.v1.json` | `691aca35c767f1225edc43d8d47fa23b373042fe3ee5319501a3074fa2f2e643` |
| `incidents/fork-recovery-incident.v1.json` | `crates/cgka-conformance-simulator/vectors/incidents/fork-recovery-incident.v1.json` | `9b88c6fb7a9ee8a2fb50cba59eb3754ead110be79e9f297db70ebad352d675bc` |
| `incidents/membership-fork-recovery-incident.v1.json` | `crates/cgka-conformance-simulator/vectors/incidents/membership-fork-recovery-incident.v1.json` | `70ae6ce51930fadc47c707c0fedc3e605c077f290a435ff88d5b53c22090088d` |
| `encrypted-media/imeta-v1.json` | `fixtures/encrypted-media/imeta-v1.json` | `24adea4c86f4c9bae04f69a1f5a183c2dc88b78fb9fd9bbb2355d584a7be82ee` |
| `encrypted-media/imeta-v2.json` | `fixtures/encrypted-media/imeta-v2.json` | `fbab0c8dbdd60efa6439ab97c0cd2c14ae9e6a9423dd67ad32d440eda152a578` |
