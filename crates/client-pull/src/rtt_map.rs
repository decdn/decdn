//! Per-peer RTT map for latency-driven proxy warming (#1174, ADR 037 § Client
//! RTT map and latency discovery).
//!
//! # Status: staged, not yet wired
//!
//! **Nothing constructs an `RttMap` today.** The shipped proxy-warming decision
//! (`crate::discovery::proxy_warming_order`, called from the CLI's
//! `probe_and_rank`) ranks candidates by the RTT of the live `cdn/probe/v1`
//! probe issued for the current request, *not* by this longitudinal map. This
//! module is the ADR-conformant structure the map-backed path will use once the
//! client grows a peer table and a background latency sweep; it is kept (and
//! tested) rather than deleted so that work starts from a verified base.
//!
//! Everything below therefore describes the **intended** design, not current
//! runtime behaviour:
//!
//! A bounded, EWMA-smoothed `NodeId → rtt` structure kept **separate** from the
//! 15-second hash-keyed probe cache (ADR 001): keyed by node rather than hash,
//! and a *longitudinal* record rather than a per-request one. Every QUIC
//! interaction with a bonded node would contribute a sample (completed
//! `cdn/client/v1` streams, `cdn/probe/v1` responses, the background latency
//! sweep). Entries older than `staleness` read as absent, and the map is
//! capacity-bounded so a long-running client cannot grow it without bound.
//!
//! Eviction is by **least-recently-sampled**, not least-recently-*used*: only
//! [`RttMap::record`] refreshes an entry's timestamp, so a peer that is read on
//! every ranking decision but never re-sampled is still evicted first. That is
//! the intended policy — the entry whose measurement is oldest is the one worth
//! dropping — but it is not LRU, despite the shape.
//!
//! Time is passed in explicitly (`now: Instant`) so the staleness and eviction
//! logic is deterministically testable rather than wall-clock dependent.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use iroh::PublicKey;

/// Recommended entry cap (ADR 037 § Per-peer RTT map).
pub const DEFAULT_RTT_MAP_CAPACITY: usize = 4096;

/// EWMA weight on the newest sample. A moderate 0.3 tracks genuine latency
/// shifts within a few samples while damping single-sample jitter — proxy
/// selection wants a stable estimate, not the last raw measurement.
const EWMA_ALPHA: f64 = 0.3;

#[derive(Debug, Clone, Copy)]
struct Entry {
    /// EWMA-smoothed round-trip estimate, microseconds.
    rtt_us: f64,
    /// When this entry was last updated — drives LRU eviction and staleness.
    last_sampled: Instant,
}

/// Bounded, EWMA-smoothed per-peer RTT map.
#[derive(Debug)]
pub struct RttMap {
    entries: HashMap<PublicKey, Entry>,
    capacity: usize,
    staleness: Duration,
}

impl RttMap {
    /// Create a map holding at most `capacity` entries, treating any entry older
    /// than `staleness` as absent. `capacity` is clamped to at least 1.
    #[must_use]
    pub fn new(capacity: usize, staleness: Duration) -> Self {
        Self {
            entries: HashMap::new(),
            capacity: capacity.max(1),
            staleness,
        }
    }

    /// Number of entries currently held (including any not-yet-swept stale ones).
    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether the map holds no entries.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Fold a fresh RTT sample (milliseconds) for `node` into its EWMA estimate,
    /// stamping it `now`. A first sample seeds the estimate directly. When the
    /// insert would exceed capacity, the least-recently-sampled entry is evicted
    /// first (LRU) — but never the entry being updated.
    pub fn record(&mut self, node: PublicKey, rtt_ms: f64, now: Instant) {
        // Ignore non-finite / negative samples defensively; a bad probe reading
        // must not poison the estimate.
        if !rtt_ms.is_finite() || rtt_ms < 0.0 {
            return;
        }
        let sample_us = rtt_ms * 1000.0;
        if let Some(entry) = self.entries.get_mut(&node) {
            entry.rtt_us = EWMA_ALPHA.mul_add(sample_us, (1.0 - EWMA_ALPHA) * entry.rtt_us);
            entry.last_sampled = now;
        } else {
            if self.entries.len() >= self.capacity {
                self.evict_oldest();
            }
            self.entries.insert(
                node,
                Entry {
                    rtt_us: sample_us,
                    last_sampled: now,
                },
            );
        }
    }

    /// Current smoothed RTT estimate for `node` in milliseconds, or `None` when
    /// the entry is absent or older than `staleness` at `now`. A stale entry
    /// reads as absent so proxy warming never ranks on a latency the client can
    /// no longer trust.
    #[must_use]
    pub fn rtt_ms(&self, node: &PublicKey, now: Instant) -> Option<f64> {
        let entry = self.entries.get(node)?;
        if now.duration_since(entry.last_sampled) > self.staleness {
            return None;
        }
        Some(entry.rtt_us / 1000.0)
    }

    /// Whether `node` needs a (re)sample: absent, or older than `staleness`. The
    /// background latency sweep drives itself off this — breadth-first over
    /// un-sampled peers, then refreshing stale ones.
    #[must_use]
    pub fn needs_sample(&self, node: &PublicKey, now: Instant) -> bool {
        self.rtt_ms(node, now).is_none()
    }

    /// Drop entries older than `staleness` at `now`. Optional housekeeping —
    /// `rtt_ms` already ignores stale entries — but keeps the map from holding
    /// dead weight against the capacity bound between sweeps.
    pub fn evict_stale(&mut self, now: Instant) {
        let staleness = self.staleness;
        self.entries
            .retain(|_, e| now.duration_since(e.last_sampled) <= staleness);
    }

    /// Evict the single least-recently-sampled entry. Called on an over-capacity
    /// insert; a no-op on an empty map.
    fn evict_oldest(&mut self) {
        if let Some(oldest) = self
            .entries
            .iter()
            .min_by_key(|(_, e)| e.last_sampled)
            .map(|(k, _)| *k)
        {
            self.entries.remove(&oldest);
        }
    }
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::float_cmp,
    // Test durations read fine as seconds; from_mins adds no clarity here.
    clippy::duration_suboptimal_units
)]
mod tests {
    use super::*;

    fn key(b: u8) -> PublicKey {
        // Derive a valid ed25519 public key from a seed — a raw byte array is
        // not guaranteed to be a curve point.
        iroh::SecretKey::from_bytes(&[b; 32]).public()
    }

    #[test]
    fn first_sample_seeds_estimate_directly() {
        let now = Instant::now();
        let mut m = RttMap::new(8, Duration::from_secs(60));
        m.record(key(1), 40.0, now);
        assert_eq!(m.rtt_ms(&key(1), now), Some(40.0));
    }

    #[test]
    fn ewma_pulls_toward_new_samples() {
        let now = Instant::now();
        let mut m = RttMap::new(8, Duration::from_secs(60));
        m.record(key(1), 100.0, now);
        m.record(key(1), 0.0, now);
        // 0.3*0 + 0.7*100 = 70
        assert!((m.rtt_ms(&key(1), now).unwrap() - 70.0).abs() < 1e-9);
    }

    #[test]
    fn stale_entries_read_as_absent() {
        let t0 = Instant::now();
        let mut m = RttMap::new(8, Duration::from_secs(30));
        m.record(key(1), 40.0, t0);
        let later = t0 + Duration::from_secs(31);
        assert_eq!(m.rtt_ms(&key(1), later), None);
        assert!(m.needs_sample(&key(1), later));
    }

    #[test]
    fn lru_eviction_drops_oldest_on_overflow() {
        let t0 = Instant::now();
        let mut m = RttMap::new(2, Duration::from_secs(600));
        m.record(key(1), 10.0, t0);
        m.record(key(2), 20.0, t0 + Duration::from_secs(1));
        // key(3) overflows cap=2; key(1) is oldest → evicted.
        m.record(key(3), 30.0, t0 + Duration::from_secs(2));
        assert_eq!(m.len(), 2);
        assert_eq!(m.rtt_ms(&key(1), t0 + Duration::from_secs(2)), None);
        assert!(m.rtt_ms(&key(2), t0 + Duration::from_secs(2)).is_some());
        assert!(m.rtt_ms(&key(3), t0 + Duration::from_secs(2)).is_some());
    }

    #[test]
    fn non_finite_samples_are_ignored() {
        let now = Instant::now();
        let mut m = RttMap::new(8, Duration::from_secs(60));
        m.record(key(1), f64::NAN, now);
        m.record(key(1), -5.0, now);
        assert!(m.is_empty());
    }

    #[test]
    fn evict_stale_prunes_old_entries() {
        let t0 = Instant::now();
        let mut m = RttMap::new(8, Duration::from_secs(30));
        m.record(key(1), 10.0, t0);
        m.record(key(2), 20.0, t0 + Duration::from_secs(40));
        m.evict_stale(t0 + Duration::from_secs(40));
        assert_eq!(m.len(), 1);
        assert!(m.rtt_ms(&key(2), t0 + Duration::from_secs(40)).is_some());
    }
}
