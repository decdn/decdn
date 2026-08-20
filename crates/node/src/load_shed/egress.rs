//! A lock-free rolling egress meter. Delivery loops call [`EgressMeter::record`]
//! with each delivered chunk's byte length (the bao-stream content plus
//! interleaved proof — the same quantity billed as `delivered`, not the full
//! QUIC frame); a periodic task calls [`EgressMeter::sample`] once per fixed
//! interval to fold the bytes delivered since the last sample into an
//! exponentially-weighted bytes/sec rate that the load-shed policy and the
//! metrics gauge read.

use std::sync::atomic::{AtomicU64, Ordering};

/// Cumulative delivered bytes plus the last computed EWMA rate. All state
/// is atomic, so `record` stays off any lock on the hot delivery path.
#[derive(Debug, Default)]
pub struct EgressMeter {
    /// Monotonic total delivered bytes.
    cumulative: AtomicU64,
    /// `cumulative` as of the last `sample` — the baseline for the next delta.
    last_sample_cumulative: AtomicU64,
    /// Whether `sample` has run at least once (seeds the EWMA on the first run).
    seeded: AtomicU64,
    /// Last computed bytes/sec EWMA.
    bps: AtomicU64,
}

impl EgressMeter {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Add `bytes` of delivered content to the running total. Called per chunk
    /// with the same length billed as `delivered` (bao content plus proof).
    pub fn record(&self, bytes: u64) {
        self.cumulative.fetch_add(bytes, Ordering::Relaxed);
    }

    /// Fold the bytes delivered since the last call into the EWMA and return the
    /// new bytes/sec. `interval_secs` is the fixed tick period; `0` is a no-op
    /// that returns the prior rate.
    pub fn sample(&self, interval_secs: u64) -> u64 {
        if interval_secs == 0 {
            return self.bps.load(Ordering::Relaxed);
        }
        let now = self.cumulative.load(Ordering::Relaxed);
        let prev = self.last_sample_cumulative.swap(now, Ordering::Relaxed);
        let delta = now.saturating_sub(prev);
        let instant = delta / interval_secs;
        // Seed on the first sample; afterwards halve toward the instant rate
        // (EWMA, alpha = 0.5) using integer math to stay off floats.
        let next = if self.seeded.swap(1, Ordering::Relaxed) == 0 {
            instant
        } else {
            let last = self.bps.load(Ordering::Relaxed);
            (instant.saturating_add(last)) / 2
        };
        self.bps.store(next, Ordering::Relaxed);
        next
    }

    /// The most recently computed bytes/sec, read on the admission hot path.
    #[must_use]
    pub fn current_bps(&self) -> u64 {
        self.bps.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn first_sample_is_instant_rate() {
        let m = EgressMeter::new();
        m.record(2_000);
        // 2000 bytes over a 1s interval, EWMA seeded from the first instant rate.
        assert_eq!(m.sample(1), 2_000);
        assert_eq!(m.current_bps(), 2_000);
    }

    #[test]
    fn ewma_halves_toward_new_rate() {
        let m = EgressMeter::new();
        m.record(2_000);
        assert_eq!(m.sample(1), 2_000);
        // Next interval delivers 0: instant rate 0, EWMA = (2000 + 0)/2 = 1000.
        assert_eq!(m.sample(1), 1_000);
    }

    #[test]
    fn rate_uses_bytes_since_last_sample_over_interval() {
        let m = EgressMeter::new();
        m.record(8_000);
        // 8000 bytes over a 2s interval = 4000 B/s instant; first sample = instant.
        assert_eq!(m.sample(2), 4_000);
    }

    #[test]
    fn zero_interval_is_ignored() {
        let m = EgressMeter::new();
        m.record(1_000);
        // A zero interval must not divide-by-zero; it returns the prior rate.
        assert_eq!(m.sample(0), 0);
    }
}
