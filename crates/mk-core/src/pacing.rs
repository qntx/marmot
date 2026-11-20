//! Per-group send pacing: the `created_at` second allocator.
//!
//! Every application message the session sends shares its `created_at`
//! second with at most [`MAX_APP_MESSAGES_PER_SECOND`] siblings; when the
//! cap is hit the allocator spills into the next second rather than
//! producing a burst relays or ingest reordering would lose. The allocated
//! second may run ahead of the session clock only up to
//! [`MAX_SEND_SKEW_SECONDS`]; past that the send refuses
//! [`SessionError::SendRateExceeded`] — typed, retryable, with nothing
//! staged. Because `second` never decreases, a clock stepping backward
//! cannot move `created_at` backward either.
//!
//! [`crate::delivered::PacingRow`] persists only `second` (a restarted
//! session treats that second as full — never reuse, occasionally waste);
//! `used` is memory-only.

use std::collections::HashMap;

use cgka_traits::GroupId as EngineGroupId;

use crate::delivered::PacingRow;
use crate::session::{Session, SessionError};

/// The most application messages of one group the session gives one
/// `created_at` second.
pub(crate) const MAX_APP_MESSAGES_PER_SECOND: u64 = 50;
/// How far the allocated second may run ahead of the session clock before
/// sends refuse [`SessionError::SendRateExceeded`].
pub(crate) const MAX_SEND_SKEW_SECONDS: u64 = 30;

/// Per-group allocator state: the last issued `created_at` second and how
/// many sends used it.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct PacingState {
    /// The `created_at` second currently being filled.
    pub second: u64,
    /// Sends already issued at `second`.
    pub used: u64,
}

/// Advance `state` by one allocation under `now_secs`; `None` when the
/// allocation would run further than [`MAX_SEND_SKEW_SECONDS`] ahead of
/// the clock. The output `second` is never less than the input's, so
/// `created_at` is monotonic even when the clock steps back.
pub(crate) const fn allocate(state: PacingState, now_secs: u64) -> Option<PacingState> {
    let mut next = state;
    if now_secs > next.second {
        next.second = now_secs;
        next.used = 0;
    }
    if next.used >= MAX_APP_MESSAGES_PER_SECOND {
        next.second = next.second.saturating_add(1);
        next.used = 0;
    }
    if next.second.saturating_sub(now_secs) > MAX_SEND_SKEW_SECONDS {
        return None;
    }
    next.used += 1;
    Some(next)
}

/// Per-group allocator state, keyed by the group's raw bytes. Memory-only:
/// the durable [`PacingRow`] alone defines post-restart behavior.
pub(crate) type PacingMap = HashMap<Vec<u8>, PacingState>;

impl Session {
    /// Allocate the `created_at` second for the next application send into
    /// `group_id`. When the allocation moves to a new second, its
    /// [`PacingRow`] lands in its own storage unit before the caller
    /// stages the outbound — a crash between the two never reuses the
    /// second. [`SessionError::SendRateExceeded`] changes no in-memory or
    /// durable state.
    pub(crate) fn allocate_send_second(
        &mut self,
        group_id: &EngineGroupId,
    ) -> Result<u64, SessionError> {
        let gid = group_id.as_slice();
        let state = match self.pacing.get(gid) {
            Some(state) => *state,
            None => self
                .provider
                .pacing_row(group_id)
                .map_err(SessionError::Storage)?
                .map_or_else(PacingState::default, |row| PacingState {
                    second: row.second,
                    used: MAX_APP_MESSAGES_PER_SECOND,
                }),
        };
        let next = allocate(state, self.now_ms() / 1_000).ok_or(SessionError::SendRateExceeded)?;
        if next.second != state.second {
            self.provider
                .store()
                .transact(|| {
                    self.provider.put_pacing_row(
                        group_id,
                        &PacingRow {
                            second: next.second,
                        },
                    )
                })
                .map_err(SessionError::Storage)?;
        }
        self.pacing.insert(gid.to_vec(), next);
        Ok(next.second)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, reason = "test code")]
mod tests {
    use super::*;

    /// Cap, spill and skew: 50 sends share the clock's second, the 51st
    /// spills into the next, and allocation refuses once the issued second
    /// runs more than [`MAX_SEND_SKEW_SECONDS`] ahead of the clock.
    #[test]
    fn cap_spill_and_skew() {
        let now = 1_700_000_000;
        let mut state = PacingState::default();
        // First allocation adopts the clock's second.
        state = allocate(state, now).unwrap();
        assert_eq!(
            state,
            PacingState {
                second: now,
                used: 1
            }
        );
        // Up to MAX_APP_MESSAGES_PER_SECOND share the second.
        for expected_used in 2..=MAX_APP_MESSAGES_PER_SECOND {
            state = allocate(state, now).unwrap();
            assert_eq!(
                state,
                PacingState {
                    second: now,
                    used: expected_used
                }
            );
        }
        // The next send spills into the following second.
        state = allocate(state, now).unwrap();
        assert_eq!(
            state,
            PacingState {
                second: now + 1,
                used: 1
            }
        );
        // A frozen clock keeps spilling until the skew bound: the second
        // may lead the clock by exactly MAX_SEND_SKEW_SECONDS.
        state.second = now + MAX_SEND_SKEW_SECONDS - 1;
        state.used = MAX_APP_MESSAGES_PER_SECOND;
        state = allocate(state, now).unwrap();
        assert_eq!(state.second, now + MAX_SEND_SKEW_SECONDS);
        state.used = MAX_APP_MESSAGES_PER_SECOND;
        assert_eq!(allocate(state, now), None, "skew past the bound refuses");
    }

    /// The issued second never decreases: a clock stepping back keeps the
    /// allocator on its last second (and a large step back refuses until
    /// the clock catches up).
    #[test]
    fn clock_step_back_stays_monotonic() {
        let now = 1_700_000_000;
        let mut state = PacingState {
            second: now + 5,
            used: 3,
        };
        // Clock moved backward: the second holds, the send still counts.
        state = allocate(state, now).unwrap();
        assert_eq!(state.second, now + 5);
        // A step back beyond the skew bound refuses rather than issuing
        // an earlier created_at.
        assert_eq!(
            allocate(
                PacingState {
                    second: now + MAX_SEND_SKEW_SECONDS,
                    used: MAX_APP_MESSAGES_PER_SECOND,
                },
                now
            ),
            None
        );
    }

    /// The reload rule: a persisted `second` reads back as full, so a
    /// restarted session's next allocation lands on the second after.
    #[test]
    fn persisted_second_reads_as_full() {
        let now = 1_700_000_000;
        let reloaded = PacingState {
            second: now,
            used: MAX_APP_MESSAGES_PER_SECOND,
        };
        assert_eq!(
            allocate(reloaded, now),
            Some(PacingState {
                second: now + 1,
                used: 1,
            })
        );
    }
}
