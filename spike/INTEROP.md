# White Noise ↔ spike wasm interop (MDK v0.16.0, rev a2e0558)

`wn`/`wnd` built from MDK `v0.16.0` with `cargo install --locked
--git https://github.com/marmot-protocol/mdk --rev
a2e0558a634bbd20607cdd5afb93abc6220ee58d --root spike/target/wn wn-cli`
(~12 min; required pinned Odin `dev-2026-10` rev `84bc3fc` via `ODIN=` for
`marmot-markdown`). `wn --version` reports the v0.16.0 build.

- Loopback relays are gated by **`WN_ALLOW_LOOPBACK_RELAYS=1`** (cli env flag;
  blob endpoints have a sibling `WN_ALLOW_LOOPBACK_BLOB_ENDPOINTS`).
- `WN_HOME=spike/target/wn-home`, `WN_SECRET_STORE=file`.
- `wnd` runs with `--discovery-relays` and `--default-account-relays` both
  `ws://127.0.0.1:45670`.
- Relay: `serveFakeRelay` from `@qntx/nostr@0.15.4/testing` (real WebSocket
  NIP-01 relay; backlog newest-first). The spike side is the wasm `Client`
  in Bun (`js/run-interop.ts`), sorting ascending before ingest.
- **Documented settle: 3 s** after roster visibility. Post-join catch-up is a
  long-lived obligation with no completion signal; probing `groups members`
  ≥ 2 then waiting a fixed 3 s covers the wnd fanout/subscription install.
  Sending before the settle made wnd miss a spike kind-445 once (group-2
  subscription not yet installed); 3 s was sufficient in every run. Catch-up
  via backlog fetch still delivers missed events later.

## Prerequisites discovered

1. **Capability negotiation** — `wn groups create <member>` preflights the
   invitee KeyPackage against the group's RequiredCapabilities. wn groups
   require `proposals {8,10}`, `extensions {6,62161(0xF2D1)}`,
   `app_components {0x8001,0x8003,0x8004,0x8006,0x8009,0x800B,0x800C,0x800D}`
   — marmot-app's `app_feature_registry` (`crates/marmot-app/src/lib.rs`)
   plus White Noise product components. A bare `EngineBuilder` advertises
   neither proposal 10 (MIP-03 SelfRemove — registered by the *app*, not the
   engine) nor 0xF2D1 / 0x8006 / 0x800B / 0x800D. Fix in the spike: mirror
   `app_feature_registry` (`self-remove` Required, three QUIC features
   Optional — leaf_capabilities advertises every registered capability
   regardless of level) and widen `supported_app_components` with the
   engine-advertisable ids 0x8006, 0x800B, 0x800D
   (`ADVERTISABLE_PROTOCOL_APP_COMPONENTS`,
   `cgka-engine/src/app_components.rs`). Engine-level `feature_registry` +
   `supported_app_components` builder seams, no MDK patch needed.
2. **Inbox relay list** — wn refuses to invite a member with
   `missing_member_inbox_route` unless the member has published a Marmot
   inbox relay list: kind **10050**, `["relay", <url>]` tags, empty content
   (`transport-nostr-adapter/src/relay_list.rs`). The spike publishes one
   per member alongside its kind-30443 KeyPackage.
3. **Routing component** — the group's `nostr_group_id` must be unique per
   group; an early spike build derived it deterministically from the
   creator, which made every group by one account share the routing id and
   `wnd` answer `application-message fanout group does not match publish
   target`. Fixed: random 32-byte id per group (`harness.rs`).

## Results (`results/interop-all.log`, keys are throwaway, fresh `WN_HOME`)

Every claim below is an `assert` in `run-interop.ts`; the script exits
non-zero on any failure.

| # | scenario | result | asserted evidence |
|---|---|---|---|
| 1 | spike creates group, invites wn; both directions | **PASS** | `wnGid1 === gid1` (same MLS group id on both sides); spike roster = {A, wn}; `hello-from-wn` decoded by spike; `hello-from-spike` in `wn messages list` |
| 2 | wn creates group, invites the spike; both directions | **PASS** | welcome ingest `processed`; `members(gid2)` = {wn, A} — resolving wn's group id in the spike engine proves same group id; `s2-hello-from-spike` in `messages list`; `s2-hello-from-wn` decoded by spike |
| 3 | bulk + membership changes | **PASS** | 500 msgs total: wn sent 25 via CLI (~35 ms/msg — CLI-bound, so 250 was not run), spike 475. Asserted: spike decoded 25/25 wn messages (502 events ingested ascending in ~30 ms); wn listed 475/475 spike messages. Spike added spike-C (roster 3). Spike removed wn: asserted on both spike members (roster 2, no wn), **C decoded `post-removal-secret`**, `wn groups members` no longer lists wn, `wn messages list` has no `post-removal-secret`. wn admin removed spike-A from group 2: asserted `members(gid2)` drops A's member id (roster = [wn] only). |
| 4 | is 0x800d (account-devices) required? | **CONDITIONAL — yes when founding KPs advertise it** | In-run proof: member E advertising the full required set minus 0x800d was **accepted** into a fresh group (requirement dropped), but the same E was **refused** (`missing_required_capabilities`, `had` shows every required component except 32781) when added to group 2, which was founded by 0x800d-advertising members. Matches `do_create_group`: account-devices is required exactly when all founding KeyPackages advertise it (mdk#2628). |

### C post-removal read — root cause

The first committed log showed `C reads post-removal message: false`.
Root cause was a **harness bug, not the engine/provider**: after C's join,
the script ingested the group-1 backlog into C but never drove C's
convergence (`flushOutbound`), so the add-C and remove-wn commits stayed
`Buffered` and the post-removal application message stayed deferred —
the same class of bug the shared `scenario.ts` avoids by flushing every
member after every ingest pass. Adding `flushOutbound(c, gid1, publish)`
after C's backlog ingest made C decode `post-removal-secret` (1 delivery)
in every subsequent run. The same missing flush hid A's own-removal
observation in group 2 (the old check also passed on *any* drained event —
now it asserts A's member id actually leaves the roster).

Timings: full script ≈ 21 s of scenario time per run (dominated by wn CLI
calls and the fixed settles). Spike-side costs: create 26–47 ms, welcome
ingest ~1 ms, 502-event ingest ~30 ms, bulk send ≈ 1.9 ms/msg.

## Commands

```sh
# start relay + run all four scenarios (WN_LOG picks the evidence file)
cd spike
WN_LOG=$PWD/results/interop-all.log bun js/run-interop.ts

# wnd was started once with:
target/wn/bin/wnd --home spike/target/wn-home \
  --socket spike/target/wn-home/dev/wnd.sock \
  --discovery-relays ws://127.0.0.1:45670 \
  --default-account-relays ws://127.0.0.1:45670
# with WN_ALLOW_LOOPBACK_RELAYS=1 WN_SECRET_STORE=file
```
