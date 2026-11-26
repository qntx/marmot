//! MDK constants the TypeScript client mirrors as literals (it cannot import
//! them): each test reads the literal from the TS source and asserts it
//! equals the pinned MDK constant, so an MDK pin bump that moves one fails
//! here instead of drifting silently.

#![allow(
    unused_crate_dependencies,
    reason = "every integration-test binary links the full dev-dependency set"
)]
#![allow(
    clippy::missing_docs_in_private_items,
    clippy::expect_used,
    clippy::tests_outside_test_module,
    reason = "test code"
)]

use cgka_engine::message_processor::MAX_DEFERRED_PEEL_RESIDENCE_MS;

const PUMP_TS: &str = include_str!("../../../packages/marmot/src/client/pump.ts");

/// The integer literal of `export const <name> = <digits>;` in `source`.
fn ts_integer(source: &str, name: &str) -> u64 {
    let prefix = format!("export const {name} = ");
    let line = source
        .lines()
        .find_map(|line| line.strip_prefix(prefix.as_str()))
        .expect("the TS constant is declared");
    line.trim_end_matches(';')
        .replace('_', "")
        .parse()
        .expect("the TS constant is an integer literal")
}

#[test]
fn pump_floor_horizon_is_mdk_deferred_peel_residence() {
    assert_eq!(
        ts_integer(PUMP_TS, "DEFERRED_PEEL_RESIDENCE_SECS") * 1_000,
        MAX_DEFERRED_PEEL_RESIDENCE_MS,
        "DEFERRED_PEEL_RESIDENCE_SECS mirrors MDK MAX_DEFERRED_PEEL_RESIDENCE_MS"
    );
}
