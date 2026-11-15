//! The injected time source and the adapters that feed it to the engine.
//!
//! The session owns no ambient clock: [`Clock::now`] supplies both the
//! monotonic reading used for convergence scheduling and the Unix time used
//! for every signed event (`created_at`) the account produces.

use core::fmt;
use std::sync::Arc;

use cgka_engine::ConvergenceClock;
use cgka_traits::{MaintenanceRandom, Timestamp, WallClock};

/// One reading of the injected clock.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Time {
    /// Milliseconds on a monotonic scale. Only differences are meaningful;
    /// used for convergence deadlines, never persisted as protocol time.
    pub monotonic_ms: u64,
    /// Milliseconds since the Unix epoch. This is the wall-clock source for
    /// Nostr `created_at` and persisted deadlines.
    pub unix_ms: u64,
}

/// Host-provided time source.
pub trait Clock: Send + Sync + fmt::Debug {
    /// Current time. Both fields come from the same reading.
    fn now(&self) -> Time;
}

/// Engine [`WallClock`] fed by [`Clock::now`]'s `unix_ms`.
pub(crate) struct EngineWallClock {
    clock: Arc<dyn Clock>,
}

impl EngineWallClock {
    pub(crate) fn new(clock: Arc<dyn Clock>) -> Self {
        Self { clock }
    }
}

impl fmt::Debug for EngineWallClock {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EngineWallClock").finish_non_exhaustive()
    }
}

impl WallClock for EngineWallClock {
    fn now(&self) -> Timestamp {
        Timestamp(self.clock.now().unix_ms / 1_000)
    }

    fn now_ms(&self) -> u64 {
        self.clock.now().unix_ms
    }
}

/// Engine [`MaintenanceRandom`]. The engine's own default (`OsMaintenanceRandom`,
/// `cgka-engine/src/engine.rs`) samples `rand::rngs::OsRng`; this adapter draws
/// the same OS entropy through `getrandom` so the session needs no extra RNG
/// crate.
pub(crate) struct EngineMaintenanceRandom;

impl fmt::Debug for EngineMaintenanceRandom {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EngineMaintenanceRandom").finish()
    }
}

impl MaintenanceRandom for EngineMaintenanceRandom {
    fn next_u64(&self) -> u64 {
        getrandom::u64().unwrap_or(0)
    }
}

/// Engine [`ConvergenceClock`] fed by [`Clock::now`] (both fields).
pub(crate) struct EngineConvergenceClock {
    clock: Arc<dyn Clock>,
}

impl EngineConvergenceClock {
    pub(crate) fn new(clock: Arc<dyn Clock>) -> Self {
        Self { clock }
    }
}

impl fmt::Debug for EngineConvergenceClock {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EngineConvergenceClock")
            .finish_non_exhaustive()
    }
}

impl ConvergenceClock for EngineConvergenceClock {
    fn now(&self) -> cgka_engine::ConvergenceTime {
        let now = self.clock.now();
        cgka_engine::ConvergenceTime {
            monotonic_ms: now.monotonic_ms,
            wall_ms: now.unix_ms,
        }
    }
}
