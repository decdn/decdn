//! The public gateway's spend bound: the bytes it delivers per UTC day from
//! its pool, over every request.
//!
//! The bound is the only thing between an anonymous `GET` and the operator's
//! wallet. It is metered on the bytes the gateway delivers, in memory: the
//! pool's balance is the hard stop, so a restart that forgives at most one
//! day's allowance costs nothing worth a store. A request whose size is known
//! and does not fit is refused before any pull; a stream whose size nobody
//! knew is cut once the bound is reached, so the gateway never delivers more
//! than the allowance plus the frames in flight. The next request is refused
//! until the day turns.
//!
//! The one bucket takes every request, whatever namespace hint it carries:
//! the hint is the client's to set (ADR 002), so it buys no bucket of its
//! own.

use std::sync::{Mutex, PoisonError};

/// Seconds in the UTC day the allowance counts over.
const DAY_SECS: u64 = 86_400;

/// Bytes delivered in one day.
struct Used {
    day: u64,
    bytes: u64,
}

/// The bytes the gateway may deliver per day, and what it delivered today.
pub(super) struct Allowance {
    /// The bytes per day. `u64::MAX` is no bound.
    per_day: u64,
    used: Mutex<Used>,
}

impl Allowance {
    /// An allowance of `per_day` bytes per UTC day.
    pub(super) const fn new(per_day: u64) -> Self {
        Self {
            per_day,
            used: Mutex::new(Used { day: 0, bytes: 0 }),
        }
    }

    /// The bytes the gateway may still deliver today, or, when none, the
    /// seconds until the UTC day turns and it may again.
    pub(super) fn remaining(&self, now_secs: u64) -> Result<u64, u64> {
        if self.per_day == u64::MAX {
            return Ok(u64::MAX);
        }
        let day = now_secs / DAY_SECS;
        let used = self.used.lock().unwrap_or_else(PoisonError::into_inner);
        let spent = if used.day == day { used.bytes } else { 0 };
        let left = self.per_day.saturating_sub(spent);
        if left > 0 {
            Ok(left)
        } else {
            Err((day + 1).saturating_mul(DAY_SECS).saturating_sub(now_secs))
        }
    }

    /// Record `bytes` delivered now. `false` once the day's allowance is
    /// reached: the caller stops delivering.
    pub(super) fn charge(&self, bytes: u64, now_secs: u64) -> bool {
        if self.per_day == u64::MAX {
            return true;
        }
        let day = now_secs / DAY_SECS;
        let mut used = self.used.lock().unwrap_or_else(PoisonError::into_inner);
        if used.day != day {
            *used = Used { day, bytes: 0 };
        }
        used.bytes = used.bytes.saturating_add(bytes);
        used.bytes < self.per_day
    }
}

#[cfg(test)]
mod tests;
