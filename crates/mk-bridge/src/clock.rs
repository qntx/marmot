//! The injected clock cell: the host supplies `now` on every call and the
//! bridge never reads ambient time.

use std::sync::{Arc, Mutex};

use mk_core::{Clock, Time};

use crate::host::lock;

/// A [`Clock`] reading the value the host last supplied on `open`/`call`.
#[derive(Debug, Clone)]
pub(crate) struct CellClock {
    cell: Arc<Mutex<Time>>,
}

impl CellClock {
    pub(crate) const fn new(cell: Arc<Mutex<Time>>) -> Self {
        Self { cell }
    }
}

impl Clock for CellClock {
    fn now(&self) -> Time {
        *lock(&self.cell)
    }
}
