# MDK conformance vectors — provenance

Byte-for-byte copies of the cgka-conformance-simulator vector fixtures,
frozen at the MDK pin in `upstream.json` (`mdk`). Do not edit them; they
move only when the pin moves. `bun run check:pins` verifies them against a
local `3rdparty/mdk` checkout when present.

- repository: https://github.com/marmot-protocol/mdk
- tag: v0.16.0
- rev: a2e0558a634bbd20607cdd5afb93abc6220ee58d

| recorded file | source path (repo root) | sha256 |
| --- | --- | --- |
| `account-devices-activation-loses-to-unsupported-invite.v1.json` | `crates/cgka-conformance-simulator/vectors/account-devices-activation-loses-to-unsupported-invite.v1.json` | `2d6424ca12a77cefb2d1d4be4b863db31f61e05315ae175e5c29f4245c3a8cb1` |
| `account-devices-activation-needs-support.v1.json` | `crates/cgka-conformance-simulator/vectors/account-devices-activation-needs-support.v1.json` | `4434d1ec274cb9128fe046434434f7e73ef62878dace305c737b24efb6034c24` |
| `account-devices-activation-wins-over-unsupported-invite.v1.json` | `crates/cgka-conformance-simulator/vectors/account-devices-activation-wins-over-unsupported-invite.v1.json` | `8301f5deac96b2ab92a960a9c6b9d865c1d0befef8adc336ba8ae60740b01994` |
| `account-devices-activation.v1.json` | `crates/cgka-conformance-simulator/vectors/account-devices-activation.v1.json` | `879178f4ed8f54592c0cc75b187fe8eec700bb8bcd44172f5a4ce8f5f3e7249d` |
| `account-devices-founding.v1.json` | `crates/cgka-conformance-simulator/vectors/account-devices-founding.v1.json` | `231c5621e5ae6ad3e990aeac6ab98dccc6764d478df2848b70513e4cd44b0308` |
| `account-devices-own-device-removal-loses-to-admin-remove.v1.json` | `crates/cgka-conformance-simulator/vectors/account-devices-own-device-removal-loses-to-admin-remove.v1.json` | `a9b7f39a1e3de322140c9c8fb14ce22849accd24a024fb2b73cd6432e7d703cf` |
| `admin-handover.v1.json` | `crates/cgka-conformance-simulator/vectors/admin-handover.v1.json` | `f8736103edd3df9dca790d9c06568666f0cde8f4b0dd2fd6e4d8e6a0196bde01` |
| `admin-policy-update.v1.json` | `crates/cgka-conformance-simulator/vectors/admin-policy-update.v1.json` | `9718ff89c9ae7ee2091099ead452dbca96a121443869d86b13f0065b687e5e1b` |
| `concurrent-invite-fork-recovery.v1.json` | `crates/cgka-conformance-simulator/vectors/concurrent-invite-fork-recovery.v1.json` | `5e0df95dbcd0860703fbf873f5a9727aac81de36162567b2e4b4f046724d4c5f` |
| `convergence-committer-selected.v1.json` | `crates/cgka-conformance-simulator/vectors/convergence-committer-selected.v1.json` | `0abd4b55dc2675dc8dfe4011bd06b51bd767c722f80bdaed4ce42f783e7d8e60` |
| `convergence-witness-selected.v1.json` | `crates/cgka-conformance-simulator/vectors/convergence-witness-selected.v1.json` | `bf81b86b1de8d854c63407c0ead6db05ce7a087eccdbddd3b7a80bb3d9b0ec5e` |
| `conversation.v1.json` | `crates/cgka-conformance-simulator/vectors/conversation.v1.json` | `c9b92ce6ab4fe0c9d7fc80df648df722f8f8b39a662ddeb7fb1926eb097421a4` |
| `cross-route-own-commit-recovery.v1.json` | `crates/cgka-conformance-simulator/vectors/cross-route-own-commit-recovery.v1.json` | `c555820e87d4a9ab277f5d2b8f74c3a130b46b8cd9aec53c7779912b4f97e584` |
| `current-profile-required-set.v1.json` | `crates/cgka-conformance-simulator/vectors/current-profile-required-set.v1.json` | `56ad3879622fe3701d18e8049904948f1d4db9626ba58f70f09d38ea4394dee2` |
| `deferred-tick-catchup.v1.json` | `crates/cgka-conformance-simulator/vectors/deferred-tick-catchup.v1.json` | `81fc3b2bf84b6f2504769ec062137ed71be8edd7b3e7c711d30d85b4b57c3420` |
| `delayed-past-epoch-app-message.v1.json` | `crates/cgka-conformance-simulator/vectors/delayed-past-epoch-app-message.v1.json` | `8f0a1e9eef6160a2bb715e8ed399d5c336ddd01d6a5e07dec75967193429d04e` |
| `drop-queued.v1.json` | `crates/cgka-conformance-simulator/vectors/drop-queued.v1.json` | `1e9539a8285a6c89f257931aaf0d76bc1a4c43704d5ce3978a398a318195a670` |
| `group-data-fork-recovery.v1.json` | `crates/cgka-conformance-simulator/vectors/group-data-fork-recovery.v1.json` | `259829d45dba17de48b29bc361cf25369c51a789390063018382b23f768ec63b` |
| `group-data-update.v1.json` | `crates/cgka-conformance-simulator/vectors/group-data-update.v1.json` | `87c2ecad7cfa6a91d48714b22d185899ef8392b0bd7949b362c3713306a65bea` |
| `group-profile-update.v1.json` | `crates/cgka-conformance-simulator/vectors/group-profile-update.v1.json` | `310e085c4aaf70d6687c23fe6fa7cfcd1a08a7bbc97eb88abb37389d94673f91` |
| `incremental-growth.v1.json` | `crates/cgka-conformance-simulator/vectors/incremental-growth.v1.json` | `1630da623a30ed0cdeb7e6d805f51d8025f6028d9f95e449779c6bc14ece15ba` |
| `invite-member.v1.json` | `crates/cgka-conformance-simulator/vectors/invite-member.v1.json` | `cfeda65a9c3eed9d9be684e98de8b410b923c12364abc986101071ac71a18199` |
| `invite-publish-fail.v1.json` | `crates/cgka-conformance-simulator/vectors/invite-publish-fail.v1.json` | `001ece2864535d5124c9c2cf9526862ce5640f4f79efaf6ab03ae56064addb57` |
| `late-welcome-backfill.v1.json` | `crates/cgka-conformance-simulator/vectors/late-welcome-backfill.v1.json` | `fc8dee86076ec1465e723ac0bc6547419b9246391d5f718db3847c9010d2c52a` |
| `latecomer-forward-secrecy.v1.json` | `crates/cgka-conformance-simulator/vectors/latecomer-forward-secrecy.v1.json` | `362dfa33a4a38b62f6f7dc8b04fa3fba83e6949c46ee98f54eb8ee34a12d23d2` |
| `leaver-removal-secrecy.v1.json` | `crates/cgka-conformance-simulator/vectors/leaver-removal-secrecy.v1.json` | `61601608a842e49776c7852f94f49974ce89105afaba9a0ee7f9c409dd661387` |
| `manifest.v1.json` | `crates/cgka-conformance-simulator/vectors/manifest.v1.json` | `418d4bd6eaf8a9537cc56b43d14318c4458d3c7124b41a9a2c3f33127941e19b` |
| `multigroup-isolation.v1.json` | `crates/cgka-conformance-simulator/vectors/multigroup-isolation.v1.json` | `ce01920fd8e6a4d303a79b80728172d6437c80388d01a87c844d17a218624a2a` |
| `partition-clear-leave.v1.json` | `crates/cgka-conformance-simulator/vectors/partition-clear-leave.v1.json` | `0d1d1657bbaadddd0a87dde6f1419f3032d68103dd6b72c2b8ebe648267f790e` |
| `publish-fail.v1.json` | `crates/cgka-conformance-simulator/vectors/publish-fail.v1.json` | `0f923d81ab2e13ef0d6f35a4915cac63661f7627fef2fde431141c69d995c842` |
| `queue-faults.v1.json` | `crates/cgka-conformance-simulator/vectors/queue-faults.v1.json` | `fe2512e96b49cff7cba516f9d3856d4f01a8f2124c7df1a3fb668890d248181b` |
| `readd-after-eviction.v1.json` | `crates/cgka-conformance-simulator/vectors/readd-after-eviction.v1.json` | `1a3b188faf2573f0047c9520396a5284a26696a7bedc857b24171a04c16b48e8` |
| `restart-delivery-faults.v1.json` | `crates/cgka-conformance-simulator/vectors/restart-delivery-faults.v1.json` | `3395ba38382f6b85afbdc9c7ebcc3ba39138c68b1e2c7b18d7a99fbd387bfa1c` |
| `route-equivalence.v1.json` | `crates/cgka-conformance-simulator/vectors/route-equivalence.v1.json` | `734272a60f0a12795d5ae0f4fe37180257e03f9cd1bcb7691396e0e802ff7ab9` |
| `three-client-message-exchange.v1.json` | `crates/cgka-conformance-simulator/vectors/three-client-message-exchange.v1.json` | `a5d7df81364245a51e74d339178a74622737c2cb87c7bcf92ae797d3fc1c2ac1` |
| `incidents/convergence-incident.v1.json` | `crates/cgka-conformance-simulator/vectors/incidents/convergence-incident.v1.json` | `d37012ef176172a943cb1779a3f4d8560eb04b368769588105d090c25337e3f0` |
| `incidents/fork-recovery-incident.v1.json` | `crates/cgka-conformance-simulator/vectors/incidents/fork-recovery-incident.v1.json` | `95d02d1b4e13efb65267e2db4c37832f3e46c87620b653dc2b85af66f750e70b` |
| `incidents/membership-fork-recovery-incident.v1.json` | `crates/cgka-conformance-simulator/vectors/incidents/membership-fork-recovery-incident.v1.json` | `823ce91d09e78fba1d0a22210ed001a9699679b8a33656de8cebe034c5f5a64e` |
