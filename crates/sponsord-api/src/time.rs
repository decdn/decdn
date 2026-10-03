//! Wall-clock time, behind a trait so servers can be tested at a fixed time.

use std::sync::atomic::{AtomicU64, Ordering};

/// Unix time in seconds.
pub trait Clock: Send + Sync {
    fn now_unix(&self) -> u64;
}

/// The system clock. A clock before 1970 reads as 0.
#[derive(Clone, Copy, Debug, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now_unix(&self) -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0)
    }
}

/// A clock that reads whatever it was last set to.
#[derive(Debug, Default)]
pub struct FixedClock(AtomicU64);

impl FixedClock {
    #[must_use]
    pub fn new(now_unix: u64) -> Self {
        Self(AtomicU64::new(now_unix))
    }

    pub fn set(&self, now_unix: u64) {
        self.0.store(now_unix, Ordering::SeqCst);
    }

    pub fn advance(&self, secs: u64) {
        self.0.fetch_add(secs, Ordering::SeqCst);
    }
}

impl Clock for FixedClock {
    fn now_unix(&self) -> u64 {
        self.0.load(Ordering::SeqCst)
    }
}
