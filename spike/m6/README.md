# m6 spike — MDK v0.18.0 cgka-engine round trip

Standalone workspace (own lockfile, `publish = false`, edition 2024, no
`mk-core`) exercising the pinned MDK `cgka-engine` public API end to end on
its real SQLCipher storage (`storage-sqlite`, in-memory via `test-support`),
plus real `wn`/`wnd` v0.18.0 interop scenarios over an in-process Nostr
relay.

## Pinned inputs

- MDK `https://github.com/marmot-protocol/mdk` rev
  `34646a577807f2b112ee031f3c164ec58aa18b64` (v0.18.0):
  `cgka-traits`, `cgka-engine`, `storage-sqlite` (`test-support`),
  `transport-nostr-peeler` (W4 only).
- OpenMLS `6100df3fb1d8c3eeab53e349fcbd621f98c44fe2` (`extensions-draft`),
  exactly as MDK's workspace pins it.
- `nostr` / `nostr-sdk` (`local-relay`) from the erskingardner fork rev
  `e93cc06b5624f06c21ecd8111a124176eb0c7589`, same declaration as MDK.
- MDK's workspace `[patch.crates-io]` entries were **not** needed: the
  crates.io `libsqlite3-sys` and `hax-lib-macros` resolve fine without them.

## Tools built alongside (same `target/` tree)

- Odin `dev-2026-10` rev `84bc3fc` → `target/odin-bin/odin`
  (`3rdparty/mdk/scripts/install-odin.sh`; marmot-markdown build-time dep).
  `odin version` → `dev-2026-10-nightly:84bc3fc`.
- `wn` / `wnd` → `target/wn/bin/` (`ODIN=<…> cargo install --locked --path
  3rdparty/mdk/crates/cli --root target/wn --target-dir target`, package
  `wn-cli`). `wn --version` → `wn 0.18.0`. `wnd` has no `--version` flag
  (unsupported-argument error); `wnd --help` identifies it as the "White
  Noise background runtime daemon".

## Engine setup (per device, `tests/support/mod.rs`)

- Storage: `SqliteAccountStorage::in_memory_for_test()` (test-support hook:
  migrated template). `device_on(storage, …)` rebuilds an engine over a
  file-backed DB for the restart/restore scenario.
- Identity: deterministic spec-valid x-only secp256k1 public key per account
  label (`pad32`); devices of one account share it and differ only in their
  MLS signing keys.
- Proof signer: `AccountIdentityProofSigner` over the same key
  (`sign_account_identity_proof`, verifies `request.account_identity`).
- Builder chain (`device_on`):

  ```rust
  EngineBuilder::new(storage.clone())
      .identity(pad32(account))
      .account_identity_proof_signer(proof_signer(account))
      .protocol_profile(ProtocolProfile::Current)
      .feature_registry(registry())              // self-remove → Proposal(10)
      .supported_app_components(supported)       // defaults + extras (e.g. 0x800d)
      .peeler(peeler)                            // MockPeeler or NostrMlsPeeler
      .build()
  ```

  No `.legacy_compatibility_profile()` exists on this dependency surface;
  Current profile is used everywhere. `ManualConvergenceClock` is injectable
  via `.convergence_clock(..)` (`clocked_device`).
- `MockPeeler` is the tests' pass-through `TransportPeeler` (wrap copies
  ciphertext into `TransportMessage.payload`, peel hands it back). It does
  **not** populate `TransportMessage.timestamp` — outer `created_at` is
  transport-owned, the engine/peeler never sets it from the wall clock.

## Smoke call sequence (`tests/smoke.rs`)

1. `bob.fresh_key_package().await` → `cgka_traits::engine::KeyPackage`.
2. `alice.create_group(CreateGroupRequest { members: vec![bob_kp],
   required_features: vec![Feature("self-remove")], .. })` → under Current
   profile `SendResult::FoundingGroupCreated { welcomes }` (no `pending`;
   the publish-before-apply `GroupCreated { pending }` shape belongs to the
   legacy profile).
3. `bob.ingest(welcome).await` → `IngestOutcome::Processed`;
   `bob.members(&group_id)` lists 2.
4. `alice.send(SendIntent::AppMessage { group_id, payload, expected_epoch:
   None })` → `SendResult::ApplicationMessage { msg, .. }`; the test routes
   the transport envelope's `h` tag to the group id.
5. `bob.ingest(msg)` → `IngestOutcome::Processed`; `bob.drain_events()`
   yields `GroupEvent::MessageReceived` whose `MarmotAppEvent` payload
   round-trips (`"hello bob"`).

There is no explicit batch-ingest API: consecutive `ingest` calls form a
batch when no convergence pass runs between them;
`converge_stored_openmls_messages_at(group_id, monotonic_ms)` forces a pass.

## Part A — engine scenarios (`cargo test --release`)

All pass; full `--nocapture --test-threads=1` output in
`evidence/engine-run.txt`.

- `tests/devices.rs` D1–D4: founding add of two KeyPackages of one account
  → 3 leaves / 2 accounts, `0x800d` required; sibling messages delivered as
  normal messages; founding own-device add works (`welcomer: None`);
  non-admin `Invite`/`AddOwnAccountDevices` refusals; later device join sees
  `PreMembership`-style outcomes for old traffic; own-account device removal
  and self-remove paths.
- `tests/key_packages.rs` K1–K5: `fresh_key_package()` is last-resort with a
  bounded lifetime and stays in `durably_owned_key_packages()`; last-resort
  reuse across two groups works; retirement/rotation makes an old-KP
  Welcome fail while a new-KP Welcome joins; a failed Welcome keeps the
  KeyPackage owned (corrupt copy needs a *different* transport id, as a
  real relay would); deleted KeyPackage Welcomes are ignored.
- `tests/recovery.rs` R1–R5 + R2b/R2c + R3b/R3c + R4b + R5b: offline
  catch-up in order (N ∈ {3,5,6,10}); commits-first delivery — one
  `converge_stored_openmls_messages_at` pass applies at most ~6 commits
  and then arms a `settlement_quiescence_ms` (1 s) deadline, so repeat
  calls on a frozen monotonic clock apply nothing more and a naive
  fixpoint stalls mid-backlog (R2: tip 7 of 11). The mk-core drive
  (`drive_like_mk_core`: `advance_convergence_inputs_until_settled` +
  `advance_convergence` + deferred-peel cutoff on the injected clock)
  clears the same 10-commit backlog in two passes (R2b: epoch 1→7→11);
  message retention is **5 past epochs** (`tip − E > 5` →
  `Stale { BeyondAppRetention }`, pinned by R2c k ∈ {4,5,6,7}). A
  missing commit buffers later traffic until it arrives; under the
  epoch-sealed peeler (R3b/R3c) the gap shows as `TransportDeferred`,
  deferred rows are **not** auto-retried when the epoch advances (the
  ~30-day `deferred_peel_cutoff` is a residence deadline — hitting it
  emits `TransportObjectResourceRefused` and drops the ciphertext), so
  recovery needs transport redelivery of every deferred object; a
  restored stale DB (R4b) drops its own fork commit as
  `InvalidAgainstCandidateState` and stays `Stable` at the old epoch;
  removal + rejoin (R5/R5b) needs a fresh device leaf — a stale copy
  that still believes it is active gets `LocalState
  { RejoinConfirmationRequired }` from the Welcome,
  `confirm_group_rejoin` rejects a wrong token with `InvalidTransition`
  and accepts `rejoin.local_state_token`, and the still-healthy copy
  ignores the sibling Welcome as `MissingWelcomeKeyPackage`.
- `tests/gift_wraps.rs` G1: a NIP-59 wrap whose rumor is a kind-452
  device-pairing request ingests as `Ignored { InvalidEncoding }` on a
  non-device-group client; a kind-1159 rumor with
  `["mdk_group_recovery","1"]` ingests as
  `IngestOutcome::GroupRecovery { AuthenticatedGroupRecovery { … } }`
  and is surfaced to the host.
- `tests/self_update.rs` S1–S4 + S1b: post-join self-update while the peer
  still sends old-epoch traffic decrypts everything once the commit lands;
  **S1b** — a new-epoch app message delivered *before* its commit is
  `Buffered { epoch: EpochId(2) }` and is accepted by one convergence pass
  after the commit (`EpochId(1)`) is applied — deferred, not lost — in both
  separate-ingest and same-batch orderings; outer `created_at` on both
  envelopes is `Timestamp(0)` under the mock transport, confirming the
  engine does not stamp transport time; concurrent commits converge to the
  same epoch; the PostJoin maintenance obligation stays in `CatchUp` —
  the engine does not complete it, the embedder's runtime must.

## Part B — `wn`/`wnd` interop (`tests/wnd.rs`, all `#[ignore]`)

Harness: per-device `wnd` under a fresh `WN_HOME`
(`WN_SECRET_STORE=file`, `WN_ALLOW_LOOPBACK_RELAYS=1`), in-process
`LocalRelay` behind a TCP NIP-11 front (the daemon history sweep requires
the relay info document), `wn --json` with nsec only ever via
`--nsec-stdin`. Every daemon/relay is reaped on drop; raw JSON per step in
`evidence/wnd/<scenario>.jsonl`.

```sh
export WN_BIN=$PWD/target/wn/bin/wn WND_BIN=$PWD/target/wn/bin/wnd
# serial — parallel scenarios starve the daemons
cargo test --release --test wnd -- --ignored --nocapture --test-threads=1 w1 w2 w3 w4 w5 w6
```

Observed (this build of wn/wnd v0.18.0):

- **W1** — PASS: both A devices `wn login --nsec-stdin` on separate homes;
  B's invite selected a KeyPackage per device slot
  (`device_count=[1,2]` on `groups members`); three-way message round trip.
- **W2** — PASS with lag: `devices link start` → emoji `comparing` →
  `confirm-match` (new device) + `devices link approve <prompt_id>` (old
  device) → `linked`; ~9 s later B sees A's `device_count` go 1→2 (A1
  commits the same-account Add), and the sibling Welcome reaches A2 a few
  seconds after that (`groups list` non-empty, send works).
- **W3** — the CLI exposes no spec sign-out: `wn logout` is documented
  as destructive local-account removal, not the shared-runtime sibling
  sign-out/wipe protocol. The wnd socket refuses it with
  `daemon_forbidden` (must run directly without `--socket`); direct
  logout returns `logged_out: true`. The device's leaf stays in the
  shared chat for all 180 s of observation (`groups members` stayed
  `[1,2]`) — expected here: the two devices were never linked (no
  device group), so only the device's own SelfRemove could remove its
  leaf, and logout does not send one.
- **W4** — PASS: an engine device built exactly like mk-core
  (proposals `{0x0008,0x000a}`, extensions `{0x0006,0xf2d1}`, private-use
  components `{0x8001,0x8003,0x8004,0x8006,0x8009,0x800b,0x800c,0x800d}`,
  `d` = 64 lowercase hex of 32 random bytes) publishes kind-30443 in its
  own slot → B's invite selects it
  (`member_outcomes[].selected_device_count = 2`), A3 unwraps the gift
  wrap (`Processed` → `GroupJoined`) and its kind-445 app message reaches
  `wn` B. Note: whether A1 notices the foreign slot is **not observable
  via the CLI** — no `wn` command lists sign-ins
  (`devices link status.prompts` are pairing prompts; marmot-app's
  `unrecognized_sign_ins` has no CLI exposure), so empty prompts across
  180 s plus one wnd restart prove nothing either way.
- **W5** — PASS: wn rotates the KeyPackage once a Welcome consumes it.
  Pre-accept `current_key_package_ref` was consumed; at the first poll
  (~15 s after `groups accept`) `authored_event_id`/`created_at` had
  advanced to a new kind-30443 event and `current_key_package_ref` to a
  new ref, the consumed ref moved into `retained_private_material`
  (`consumed: true`, `replaced_at` set, `awaiting_confirmed_replacement:
  false`), and `refresh_at` resampled (~6 days). `pending_replacement`
  is null because staging, publication and promotion all complete
  between polls — rotation is detected by comparing
  `current_key_package_ref`/`authored_event_id` to the pre-accept
  baseline, not by `pending_replacement`.
- **W6** — observed: `groups maintenance-status` shows the `post_join`
  obligation in `grace` until ~11 s after accept, then `quiet` for ~60 s
  (`quiet_since` ≈ accept+11 s), then `not_before` (jitter) at ~88 s; the
  `self_update` evolution publishes `confirmed` at `source_epoch 1 →
  target_epoch 2` ~89 s after accept, and `last_own_leaf_rotation_at` is
  ~93 s after enrollment.

## Run

```sh
cargo test --release -- --nocapture --test-threads=1   # Part A + smoke
cargo clippy --all-targets                           # warnings only
```

Release builds use the repo's heavy LTO profile: the `wn` install takes
10+ min and each test-binary relink 1–6 min. Everything lives in this
project's `target/`; wnd logs go to `target/wn-logs/`.
