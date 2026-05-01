//! Per-source connection rate limiting for all deCDN QUIC protocol handlers.
//!
//! # Problem
//!
//! iroh's `Router` spawns one task per accepted QUIC connection with no
//! concurrency cap and no per-source throttle. A single attacker can exhaust
//! the node's task capacity by flooding connections faster than the idle
//! timeout reaps them.
//!
//! # Solution
//!
//! `ConnectionLimiter` enforces three independent layers at the top of every
//! `ProtocolHandler::accept` implementation:
//!
//! 1. **Global semaphore** — hard cap on total in-flight handler tasks.
//! 2. **Per-NodeID token bucket** — bounds rate from any cryptographic identity.
//! 3. **Per-IP token bucket** — bounds rate from a source address (stronger
//!    defense because `NodeIDs` are free to mint; IPs are scarce).
//!
//! Relay connections (no direct IP) skip the per-IP check and are still
//! subject to the global and per-NodeID limits.

use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use iroh::TransportAddr;
use iroh::Watcher as _;
use iroh::endpoint::Connection;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::config::ResolvedSecurity;
use crate::metrics::Metrics;

/// Reason a connection was rejected.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RejectReason {
    /// Global concurrency semaphore exhausted.
    GlobalFull,
    /// Per-NodeID token bucket exhausted.
    PerNodeId,
    /// Per-IP token bucket exhausted.
    PerIp,
}

/// RAII permit that holds a semaphore slot for the lifetime of a handler task.
///
/// Dropping this value releases the slot. The metrics gauge is updated in
/// `ConnectionLimiter::acquire` (increment) and here (decrement) so it tracks
/// actual in-flight count.
#[allow(missing_debug_implementations)] // OwnedSemaphorePermit doesn't impl Debug.
pub struct Permit {
    _sem: OwnedSemaphorePermit,
    metrics: Arc<Metrics>,
}

impl Drop for Permit {
    fn drop(&mut self) {
        self.metrics.dispatch_permit_released();
    }
}

/// Lazy-refill token bucket for per-source rate limiting.
///
/// Tokens accumulate at `rate` per second up to `burst`. One token is consumed
/// per connection attempt. Refill is computed on demand from elapsed wall time
/// so no background task is required.
#[derive(Debug)]
struct TokenBucket {
    tokens: f64,
    rate: f64,
    burst: f64,
    last_refill: Instant,
}

impl TokenBucket {
    fn new(rate: f64, burst: f64) -> Self {
        Self {
            tokens: burst,
            rate,
            burst,
            last_refill: Instant::now(),
        }
    }

    /// Attempt to consume one token. Returns `true` if a token was available.
    fn try_consume(&mut self, now: Instant) -> bool {
        let elapsed = now.duration_since(self.last_refill).as_secs_f64();
        self.tokens = (self.tokens + elapsed * self.rate).min(self.burst);
        self.last_refill = now;
        if self.tokens >= 1.0 {
            self.tokens -= 1.0;
            true
        } else {
            false
        }
    }
}

/// `HashMap<K, TokenBucket>` with a hard cap on the number of tracked entries.
///
/// When the map is full and a new key arrives, the entry with the oldest
/// `last_refill` timestamp is evicted (O(n) linear scan over the values —
/// acceptable at `PoC` scale with `max_tracked_sources ≤ 4096`).
struct BoundedRateMap<K> {
    inner: HashMap<K, TokenBucket>,
    cap: usize,
    rate: f64,
    burst: f64,
}

impl<K: std::hash::Hash + Eq + Clone> BoundedRateMap<K> {
    fn new(cap: usize, rate: f64, burst: f64) -> Self {
        Self {
            inner: HashMap::new(),
            cap,
            rate,
            burst,
        }
    }

    /// Consume one token for `key`. Returns `true` if the connection is allowed.
    fn try_consume(&mut self, key: &K, now: Instant) -> bool {
        if !self.inner.contains_key(key) {
            if self.inner.len() >= self.cap {
                self.evict_oldest();
            }
            self.inner
                .insert(key.clone(), TokenBucket::new(self.rate, self.burst));
        }
        self.inner.get_mut(key).is_some_and(|b| b.try_consume(now))
    }

    /// Evict the entry with the smallest `last_refill` (least recently touched).
    fn evict_oldest(&mut self) {
        let oldest_key = self
            .inner
            .iter()
            .min_by_key(|(_, b)| b.last_refill)
            .map(|(k, _)| k.clone());
        if let Some(k) = oldest_key {
            self.inner.remove(&k);
        }
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.inner.len()
    }
}

/// Shared rate limiter used by all deCDN-authored `ProtocolHandler` implementations.
#[allow(missing_debug_implementations)] // Mutex<BoundedRateMap<_>> contains HashMap which is Debug, but the overall type is complex enough to skip.
pub struct ConnectionLimiter {
    semaphore: Arc<Semaphore>,
    by_node: Mutex<BoundedRateMap<[u8; 32]>>,
    by_ip: Mutex<BoundedRateMap<IpAddr>>,
    metrics: Arc<Metrics>,
}

impl ConnectionLimiter {
    /// Construct a limiter from the resolved security configuration.
    pub fn new(cfg: &ResolvedSecurity, metrics: Arc<Metrics>) -> Self {
        let semaphore = Arc::new(Semaphore::new(
            usize::try_from(cfg.max_concurrent_handlers).unwrap_or(usize::MAX),
        ));
        let by_node = Mutex::new(BoundedRateMap::new(
            cfg.max_tracked_sources,
            cfg.per_node_rate_per_sec,
            f64::from(cfg.per_node_burst),
        ));
        let by_ip = Mutex::new(BoundedRateMap::new(
            cfg.max_tracked_sources,
            cfg.per_ip_rate_per_sec,
            f64::from(cfg.per_ip_burst),
        ));
        Self {
            semaphore,
            by_node,
            by_ip,
            metrics,
        }
    }

    /// Try to acquire a permit for `conn`.
    ///
    /// On success returns a [`Permit`] that holds the semaphore slot and
    /// updates the in-flight gauge. On failure returns the [`RejectReason`]
    /// and records the appropriate metric counter.
    ///
    /// This method is synchronous and completes in microseconds — it never
    /// awaits I/O.
    pub fn acquire(&self, conn: &Connection) -> Result<Permit, RejectReason> {
        // --- global semaphore --------------------------------------------------
        let sem_permit = Arc::clone(&self.semaphore)
            .try_acquire_owned()
            .map_err(|_| {
                self.metrics.dispatch_rejected_global();
                RejectReason::GlobalFull
            })?;

        let now = Instant::now();
        let node_key = *conn.remote_id().as_bytes();

        // --- per-NodeID bucket -------------------------------------------------
        {
            let mut map = self
                .by_node
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if !map.try_consume(&node_key, now) {
                self.metrics.dispatch_rejected_per_node();
                return Err(RejectReason::PerNodeId);
            }
        }

        // --- per-IP bucket (skip for relay-only connections) -------------------
        if let Some(ip) = peer_ip(conn) {
            let mut map = self
                .by_ip
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if !map.try_consume(&ip, now) {
                self.metrics.dispatch_rejected_per_ip();
                return Err(RejectReason::PerIp);
            }
        }

        self.metrics.dispatch_permit_acquired();
        Ok(Permit {
            _sem: sem_permit,
            metrics: Arc::clone(&self.metrics),
        })
    }
}

/// Extract the remote IP from a connection's currently-selected network path.
///
/// Returns `None` for relay-only connections that have no direct IP path.
fn peer_ip(conn: &Connection) -> Option<IpAddr> {
    let paths = conn.paths().peek().clone();
    paths
        .iter()
        .find(|p| p.is_selected())
        .and_then(|p| match p.remote_addr() {
            TransportAddr::Ip(addr) => Some(addr.ip()),
            _ => None,
        })
        // Fall back to any IP path if no selected path has a direct IP yet.
        .or_else(|| {
            paths.iter().find_map(|p| match p.remote_addr() {
                TransportAddr::Ip(addr) => Some(addr.ip()),
                _ => None,
            })
        })
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use std::time::{Duration, Instant};

    use super::{BoundedRateMap, TokenBucket};

    // --- TokenBucket -----------------------------------------------------------

    #[test]
    fn token_bucket_starts_full() {
        let mut b = TokenBucket::new(10.0, 5.0);
        // Should allow 5 consecutive consumes before blocking.
        let now = Instant::now();
        for _ in 0..5 {
            assert!(b.try_consume(now), "bucket should have tokens");
        }
        assert!(!b.try_consume(now), "bucket should be empty");
    }

    #[test]
    fn token_bucket_refills_at_rate() {
        let mut b = TokenBucket::new(10.0, 5.0);
        let now = Instant::now();
        // Drain completely.
        for _ in 0..5 {
            b.try_consume(now);
        }
        assert!(!b.try_consume(now));
        // After 0.2 s at 10 tok/s => 2 tokens refilled.
        let later = now + Duration::from_millis(200);
        assert!(b.try_consume(later), "should have 2 tokens after 0.2 s");
        assert!(b.try_consume(later));
        assert!(!b.try_consume(later));
    }

    #[test]
    fn token_bucket_caps_at_burst() {
        let mut b = TokenBucket::new(10.0, 5.0);
        let now = Instant::now();
        // Long idle: 60 s at 10 tok/s would be 600, but burst caps at 5.
        let later = now + Duration::from_secs(60);
        // First consume refills lazily up to burst.
        assert!(b.try_consume(later));
        // Should have burst - 1 = 4 more.
        for _ in 0..4 {
            assert!(b.try_consume(later));
        }
        assert!(!b.try_consume(later));
    }

    // --- BoundedRateMap --------------------------------------------------------

    #[test]
    fn bounded_map_allows_under_rate() {
        let mut map: BoundedRateMap<u32> = BoundedRateMap::new(16, 5.0, 3.0);
        let now = Instant::now();
        assert!(map.try_consume(&1, now));
        assert!(map.try_consume(&1, now));
        assert!(map.try_consume(&1, now));
        assert!(!map.try_consume(&1, now), "burst exhausted");
    }

    #[test]
    fn bounded_map_evicts_oldest_when_full() {
        let cap = 4_usize;
        let mut map: BoundedRateMap<u32> = BoundedRateMap::new(cap, 100.0, 1.0);
        let now = Instant::now();
        // Fill to cap. Key 0 is inserted first (oldest).
        for k in 0..u32::try_from(cap).unwrap() {
            map.try_consume(&k, now);
        }
        assert_eq!(map.len(), cap);
        // Insert a fifth key — key 0 should be evicted (oldest last_refill).
        let later = now + Duration::from_millis(1);
        let cap32 = u32::try_from(cap).unwrap();
        map.try_consume(&cap32, later);
        assert_eq!(map.len(), cap, "len stays at cap after eviction");
        // Key 0 is gone; a fresh consume on key 0 starts a new bucket.
        // We can't observe this directly, but try_consume must not panic.
        map.try_consume(&0, later);
    }

    #[test]
    fn bounded_map_independent_keys() {
        let mut map: BoundedRateMap<u8> = BoundedRateMap::new(16, 1.0, 1.0);
        let now = Instant::now();
        // Each key starts with a full burst of 1 token.
        assert!(map.try_consume(&1, now));
        assert!(map.try_consume(&2, now));
        assert!(!map.try_consume(&1, now), "key 1 exhausted");
        assert!(!map.try_consume(&2, now), "key 2 exhausted");
    }
}
