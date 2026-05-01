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
//! [`ConnectionLimiter`] enforces three independent layers at the top of every
//! deCDN-authored `ProtocolHandler::accept` implementation:
//!
//! 1. **Global semaphore** — hard cap on total in-flight handler tasks.
//! 2. **Per-IP token bucket** — bounds rate from a source address. Checked
//!    before per-NodeID because IPs are scarce (`NodeIDs` are free to mint, so
//!    the per-IP layer is the more meaningful defense and rejecting it first
//!    avoids charging a per-NodeID token for a connection we're about to drop).
//! 3. **Per-NodeID token bucket** — bounds rate from any cryptographic identity.
//!
//! Relay connections (no direct IP) skip the per-IP check and are still
//! subject to the global and per-NodeID limits.

use std::collections::HashMap;
use std::net::IpAddr;
use std::num::NonZeroUsize;
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

impl RejectReason {
    /// Short, stable label suitable for log fields and the QUIC close-frame
    /// reason bytes. Peers receiving the close can disambiguate the layer
    /// (and pick an appropriate backoff strategy) without parsing free text.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::GlobalFull => "global-full",
            Self::PerNodeId => "per-node-id",
            Self::PerIp => "per-ip",
        }
    }
}

/// RAII permit that holds a semaphore slot for the lifetime of a handler task.
///
/// Construction (inside [`ConnectionLimiter::acquire`]) increments the
/// `dispatch_in_flight` gauge; `Drop` decrements it. The pair must remain
/// symmetric — if you split the construction site, keep the gauge bookkeeping
/// adjacent so the invariant is locally checkable.
pub struct Permit {
    _sem: OwnedSemaphorePermit,
    metrics: Arc<Metrics>,
}

impl std::fmt::Debug for Permit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Permit").finish_non_exhaustive()
    }
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
///
/// `rate` and `burst` are required to be finite-positive at construction. The
/// caller (always [`ConnectionLimiter::new`]) gets validated values from
/// `resolve_security`; the debug-assert documents the contract for any future
/// caller and turns a config-validation skip into a test-time panic.
#[derive(Debug)]
struct TokenBucket {
    tokens: f64,
    rate: f64,
    burst: f64,
    last_refill: Instant,
}

impl TokenBucket {
    fn new(rate: f64, burst: f64) -> Self {
        debug_assert!(
            rate.is_finite() && rate > 0.0,
            "rate must be finite-positive"
        );
        debug_assert!(
            burst.is_finite() && burst >= 1.0,
            "burst must be finite and at least 1"
        );
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
/// acceptable at `PoC` scale with `cap ≤ a few thousand`).
///
/// `cap` is `NonZeroUsize` so the type itself rules out the `cap == 0`
/// degenerate case that would otherwise silently exceed the bound.
struct BoundedRateMap<K> {
    inner: HashMap<K, TokenBucket>,
    cap: NonZeroUsize,
    rate: f64,
    burst: f64,
}

impl<K: std::hash::Hash + Eq + Clone> BoundedRateMap<K> {
    fn new(cap: NonZeroUsize, rate: f64, burst: f64) -> Self {
        Self {
            inner: HashMap::new(),
            cap,
            rate,
            burst,
        }
    }

    /// Consume one token for `key`. Returns `true` if the connection is allowed.
    fn try_consume(&mut self, key: &K, now: Instant) -> bool {
        if let Some(bucket) = self.inner.get_mut(key) {
            return bucket.try_consume(now);
        }
        if self.inner.len() >= self.cap.get() {
            self.evict_oldest();
        }
        self.inner
            .entry(key.clone())
            .or_insert_with(|| TokenBucket::new(self.rate, self.burst))
            .try_consume(now)
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

    #[cfg(test)]
    fn contains_key(&self, key: &K) -> bool {
        self.inner.contains_key(key)
    }
}

/// Shared rate limiter used by all deCDN-authored `ProtocolHandler` implementations.
#[allow(missing_debug_implementations)]
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
        // `resolve_security` guarantees `max_tracked_sources > 0`. The
        // `unwrap_or(MIN)` is defense-in-depth at the type boundary so a
        // future caller bypassing validation still gets a usable map.
        let cap = NonZeroUsize::new(cfg.max_tracked_sources).unwrap_or(NonZeroUsize::MIN);
        let by_node = Mutex::new(BoundedRateMap::new(
            cap,
            cfg.per_node_rate_per_sec,
            f64::from(cfg.per_node_burst),
        ));
        let by_ip = Mutex::new(BoundedRateMap::new(
            cap,
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
    /// updates the in-flight gauge. On failure returns the [`RejectReason`],
    /// records the appropriate metric counter, and emits a `tracing::debug!`
    /// breadcrumb with `reason`/`node_id`/`ip` so operators investigating a
    /// counter spike have something to grep for.
    ///
    /// **Cross-layer charge invariant:** layers are checked in order
    /// global → per-IP → per-NodeID. The first layer to reject short-circuits;
    /// later layers never see the connection, so no token is wasted on a
    /// connection we already decided to drop. A token consumed by an earlier
    /// layer (e.g. per-IP allows, per-NodeID rejects) is *not* refunded — the
    /// per-IP rate then reflects "attempts" rather than "served" by design,
    /// matching the per-source-budget intent.
    ///
    /// This method is synchronous and completes in microseconds — it never
    /// awaits I/O.
    pub fn acquire(&self, conn: &Connection) -> Result<Permit, RejectReason> {
        self.acquire_inner(*conn.remote_id().as_bytes(), peer_ip(conn))
    }

    /// Plumbing-free variant exposed for unit tests that don't have a real
    /// iroh `Connection`. Identical to [`Self::acquire`] in behavior.
    fn acquire_inner(
        &self,
        node_key: [u8; 32],
        peer_ip: Option<IpAddr>,
    ) -> Result<Permit, RejectReason> {
        let sem_permit = Arc::clone(&self.semaphore)
            .try_acquire_owned()
            .map_err(|_| {
                self.metrics.dispatch_rejected_global();
                tracing::debug!(
                    reason = RejectReason::GlobalFull.as_str(),
                    node_id = %hex_short(&node_key),
                    ip = ?peer_ip,
                    "dispatch rejected: global semaphore exhausted"
                );
                RejectReason::GlobalFull
            })?;

        let now = Instant::now();

        // Per-IP first (skip for relay-only connections). Checking the
        // scarcer-resource layer first means a per-IP rejection never
        // charges a per-NodeID token.
        if let Some(ip) = peer_ip {
            let mut map = lock_recover(&self.by_ip, "per-ip");
            if !map.try_consume(&ip, now) {
                self.metrics.dispatch_rejected_per_ip();
                tracing::debug!(
                    reason = RejectReason::PerIp.as_str(),
                    node_id = %hex_short(&node_key),
                    %ip,
                    "dispatch rejected: per-IP token bucket exhausted"
                );
                return Err(RejectReason::PerIp);
            }
        }

        {
            let mut map = lock_recover(&self.by_node, "per-node");
            if !map.try_consume(&node_key, now) {
                self.metrics.dispatch_rejected_per_node();
                tracing::debug!(
                    reason = RejectReason::PerNodeId.as_str(),
                    node_id = %hex_short(&node_key),
                    ip = ?peer_ip,
                    "dispatch rejected: per-NodeID token bucket exhausted"
                );
                return Err(RejectReason::PerNodeId);
            }
        }

        self.metrics.dispatch_permit_acquired();
        Ok(Permit {
            _sem: sem_permit,
            metrics: Arc::clone(&self.metrics),
        })
    }
}

/// Lock a mutex, recovering from poisoning. A poisoned lock here means a
/// previous caller panicked while holding the guard — the bounded-map state
/// (a `HashMap` of independent token buckets) survives a poison without
/// inconsistency, so silent recovery is correct. We emit `tracing::error!`
/// once per observation so the recovery is at least grep-able.
fn lock_recover<'a, T>(m: &'a Mutex<T>, label: &'static str) -> std::sync::MutexGuard<'a, T> {
    m.lock().unwrap_or_else(|poisoned| {
        tracing::error!(
            map = label,
            "dispatch rate-limit mutex poisoned; recovering inner state"
        );
        poisoned.into_inner()
    })
}

/// First 8 hex chars of a 32-byte node id, for log-line readability.
fn hex_short(key: &[u8; 32]) -> String {
    use std::fmt::Write as _;
    let mut s = String::with_capacity(8);
    for b in &key[..4] {
        let _ = write!(s, "{b:02x}");
    }
    s
}

/// Extract the remote IP from a connection's currently-selected network path.
///
/// Returns `None` for relay-only connections that have no direct IP path.
/// Path selection may not have completed at the moment `accept` returns;
/// without the fallback to *any* IP path, the per-IP limit would silently
/// no-op on freshly-accepted connections and an attacker churning identities
/// could bypass the per-IP layer in that race window.
fn peer_ip(conn: &Connection) -> Option<IpAddr> {
    let paths = conn.paths().peek().clone();
    paths
        .iter()
        .find(|p| p.is_selected() && !p.is_closed())
        .and_then(|p| match p.remote_addr() {
            TransportAddr::Ip(addr) => Some(addr.ip()),
            _ => None,
        })
        .or_else(|| {
            paths
                .iter()
                .filter(|p| !p.is_closed())
                .find_map(|p| match p.remote_addr() {
                    TransportAddr::Ip(addr) => Some(addr.ip()),
                    _ => None,
                })
        })
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use std::net::{IpAddr, Ipv4Addr};
    use std::num::NonZeroUsize;
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use super::{BoundedRateMap, ConnectionLimiter, RejectReason, TokenBucket};
    use crate::config::ResolvedSecurity;
    use crate::metrics::Metrics;

    fn nz(n: usize) -> NonZeroUsize {
        NonZeroUsize::new(n).unwrap()
    }

    // --- TokenBucket -----------------------------------------------------------

    #[test]
    fn token_bucket_starts_full() {
        let mut b = TokenBucket::new(10.0, 5.0);
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
        for _ in 0..5 {
            b.try_consume(now);
        }
        assert!(!b.try_consume(now));
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
        assert!(b.try_consume(later));
        for _ in 0..4 {
            assert!(b.try_consume(later));
        }
        assert!(!b.try_consume(later));
    }

    #[test]
    fn token_bucket_handles_backward_clock_skew() {
        // Instant is monotonic, but `duration_since` saturates if `now` is
        // earlier than `last_refill`. Belt-and-braces: passing an earlier
        // instant must not panic and must not credit phantom tokens.
        let mut b = TokenBucket::new(10.0, 5.0);
        let now = Instant::now();
        for _ in 0..5 {
            b.try_consume(now);
        }
        let earlier = now.checked_sub(Duration::from_secs(1)).unwrap_or(now);
        assert!(
            !b.try_consume(earlier),
            "earlier-time consume must not credit"
        );
    }

    // --- BoundedRateMap --------------------------------------------------------

    #[test]
    fn bounded_map_allows_under_rate() {
        let mut map: BoundedRateMap<u32> = BoundedRateMap::new(nz(16), 5.0, 3.0);
        let now = Instant::now();
        assert!(map.try_consume(&1, now));
        assert!(map.try_consume(&1, now));
        assert!(map.try_consume(&1, now));
        assert!(!map.try_consume(&1, now), "burst exhausted");
    }

    #[test]
    fn bounded_map_evicts_oldest_when_full() {
        let cap = 4_usize;
        let mut map: BoundedRateMap<u32> = BoundedRateMap::new(nz(cap), 100.0, 1.0);
        let now = Instant::now();
        // Insert keys 0..3, each at successively later instants so key 0 is
        // unambiguously the oldest by `last_refill`.
        for k in 0..u32::try_from(cap).unwrap() {
            let t = now + Duration::from_millis(u64::from(k));
            map.try_consume(&k, t);
        }
        assert_eq!(map.len(), cap);
        assert!(map.contains_key(&0), "key 0 present before eviction");

        // Insert a fifth key — key 0 (oldest last_refill) must be evicted.
        let later = now + Duration::from_secs(1);
        let cap32 = u32::try_from(cap).unwrap();
        map.try_consume(&cap32, later);
        assert_eq!(map.len(), cap, "len stays at cap after eviction");
        assert!(
            !map.contains_key(&0),
            "key 0 (oldest last_refill) must have been evicted"
        );
        assert!(map.contains_key(&cap32), "newly inserted key present");
    }

    #[test]
    fn bounded_map_independent_keys() {
        let mut map: BoundedRateMap<u8> = BoundedRateMap::new(nz(16), 1.0, 1.0);
        let now = Instant::now();
        assert!(map.try_consume(&1, now));
        assert!(map.try_consume(&2, now));
        assert!(!map.try_consume(&1, now), "key 1 exhausted");
        assert!(!map.try_consume(&2, now), "key 2 exhausted");
    }

    // --- ConnectionLimiter -----------------------------------------------------

    fn strict_security(max: u32) -> ResolvedSecurity {
        ResolvedSecurity {
            max_concurrent_handlers: max,
            per_node_rate_per_sec: 1.0,
            per_node_burst: 1,
            per_ip_rate_per_sec: 1.0,
            per_ip_burst: 1,
            max_tracked_sources: 16,
        }
    }

    fn permissive_security() -> ResolvedSecurity {
        ResolvedSecurity {
            max_concurrent_handlers: u32::MAX,
            per_node_rate_per_sec: 1e9,
            per_node_burst: u32::MAX,
            per_ip_rate_per_sec: 1e9,
            per_ip_burst: u32::MAX,
            max_tracked_sources: 4096,
        }
    }

    fn ip(a: u8, b: u8, c: u8, d: u8) -> IpAddr {
        IpAddr::V4(Ipv4Addr::new(a, b, c, d))
    }

    #[test]
    fn acquire_global_full_rejects_when_semaphore_exhausted() {
        let metrics = Arc::new(Metrics::new());
        let limiter = ConnectionLimiter::new(&strict_security(1), Arc::clone(&metrics));
        let permit = limiter
            .acquire_inner([1u8; 32], Some(ip(127, 0, 0, 1)))
            .expect("first acquire");
        // Second acquire from a *different* node id and IP must still fail
        // the global cap before the per-source layers get a chance.
        let err = limiter
            .acquire_inner([2u8; 32], Some(ip(10, 0, 0, 1)))
            .expect_err("second acquire should be rejected");
        assert_eq!(err, RejectReason::GlobalFull);
        // After dropping the first permit the slot frees and a third acquire
        // succeeds — proves the OwnedSemaphorePermit drop releases the slot.
        drop(permit);
        limiter
            .acquire_inner([3u8; 32], Some(ip(10, 0, 0, 2)))
            .expect("acquire after drop should succeed");
    }

    #[test]
    fn acquire_per_ip_rejects_after_burst_exhausted() {
        let metrics = Arc::new(Metrics::new());
        let limiter = ConnectionLimiter::new(&strict_security(u32::MAX), Arc::clone(&metrics));
        let same_ip = Some(ip(192, 0, 2, 1));
        // First (different node id, same IP) succeeds.
        let _p1 = limiter
            .acquire_inner([1u8; 32], same_ip)
            .expect("first acquire");
        // Second (also different node id, same IP) hits the per-IP burst-1 limit.
        let err = limiter
            .acquire_inner([2u8; 32], same_ip)
            .expect_err("second per-IP acquire should reject");
        assert_eq!(err, RejectReason::PerIp);
    }

    #[test]
    fn acquire_per_node_rejects_after_burst_exhausted() {
        let metrics = Arc::new(Metrics::new());
        let limiter = ConnectionLimiter::new(&strict_security(u32::MAX), Arc::clone(&metrics));
        let same_node = [7u8; 32];
        // First (same node id, different IPs) succeeds.
        let _p1 = limiter
            .acquire_inner(same_node, Some(ip(10, 0, 0, 1)))
            .expect("first acquire");
        // Second from same node id but different IP hits per-NodeID burst=1.
        let err = limiter
            .acquire_inner(same_node, Some(ip(10, 0, 0, 2)))
            .expect_err("second per-node acquire should reject");
        assert_eq!(err, RejectReason::PerNodeId);
    }

    #[test]
    fn acquire_per_ip_rejection_does_not_charge_per_node_token() {
        // Per-IP is checked first, so a per-IP rejection must leave the
        // per-NodeID bucket untouched. Subsequent acquire from the same
        // node id but a fresh IP must succeed.
        let metrics = Arc::new(Metrics::new());
        let limiter = ConnectionLimiter::new(&strict_security(u32::MAX), Arc::clone(&metrics));
        let same_ip = Some(ip(192, 0, 2, 99));
        let attacker_node = [9u8; 32];
        let _p1 = limiter
            .acquire_inner([0u8; 32], same_ip)
            .expect("p1: drains the per-IP bucket for that IP");
        let err = limiter
            .acquire_inner(attacker_node, same_ip)
            .expect_err("p2: per-IP bucket empty for that IP");
        assert_eq!(err, RejectReason::PerIp);
        // Same node id, fresh IP — must succeed if the per-NodeID token
        // wasn't burned by the rejected p2.
        limiter
            .acquire_inner(attacker_node, Some(ip(10, 0, 0, 1)))
            .expect("per-node bucket should still have its first token");
    }

    #[test]
    fn acquire_relay_connection_skips_per_ip() {
        // peer_ip = None simulates a relay-only connection. Per-IP layer is
        // skipped; only global + per-NodeID apply.
        let metrics = Arc::new(Metrics::new());
        let limiter = ConnectionLimiter::new(&strict_security(u32::MAX), Arc::clone(&metrics));
        let _p1 = limiter
            .acquire_inner([1u8; 32], None)
            .expect("relay acquire 1");
        // Different node id, also relay — must succeed (per-IP doesn't apply).
        let _p2 = limiter
            .acquire_inner([2u8; 32], None)
            .expect("relay acquire 2 from different node id");
    }

    #[test]
    fn permit_drop_decrements_in_flight_metric() {
        // We can't read the gauge value directly without scraping, so the
        // contract is verified indirectly: permits must be released so that
        // a strictly-bounded global semaphore can be re-acquired after Drop.
        let metrics = Arc::new(Metrics::new());
        let limiter = ConnectionLimiter::new(&strict_security(2), Arc::clone(&metrics));
        let p1 = limiter
            .acquire_inner([1u8; 32], Some(ip(10, 0, 0, 1)))
            .unwrap();
        let p2 = limiter
            .acquire_inner([2u8; 32], Some(ip(10, 0, 0, 2)))
            .unwrap();
        // Global is full. Drop one and re-acquire.
        drop(p1);
        let _p3 = limiter
            .acquire_inner([3u8; 32], Some(ip(10, 0, 0, 3)))
            .expect("slot should be free after dropping p1");
        drop(p2);
    }

    #[test]
    fn concurrent_acquires_under_strict_burst_serialize_correctly() {
        // 32 threads racing on a burst=1 limiter must all see exactly one
        // success and 31 PerIp rejections. The std::sync::Mutex around the
        // bounded maps serialises the consume step.
        use std::sync::Barrier;
        let metrics = Arc::new(Metrics::new());
        let limiter = Arc::new(ConnectionLimiter::new(&strict_security(u32::MAX), metrics));
        let n = 32;
        let barrier = Arc::new(Barrier::new(n));
        let single_ip = Some(ip(192, 0, 2, 250));
        let mut handles = Vec::with_capacity(n);
        for i in 0..n {
            let lim = Arc::clone(&limiter);
            let bar = Arc::clone(&barrier);
            handles.push(std::thread::spawn(move || {
                bar.wait();
                let mut node = [0u8; 32];
                node[0] = u8::try_from(i).unwrap_or(0);
                lim.acquire_inner(node, single_ip).is_ok()
            }));
        }
        let successes: usize = handles
            .into_iter()
            .map(|h| usize::from(h.join().unwrap()))
            .sum();
        assert_eq!(successes, 1, "exactly one acquire should succeed");
    }

    #[test]
    fn poisoned_mutex_recovers_inner_state() {
        let metrics = Arc::new(Metrics::new());
        let limiter = Arc::new(ConnectionLimiter::new(&permissive_security(), metrics));
        let lim_for_thread = Arc::clone(&limiter);
        // Poison by_ip from a panicking thread holding the lock.
        let join = std::thread::spawn(move || {
            let _g = lim_for_thread.by_ip.lock().unwrap();
            panic!("intentional");
        });
        let _ = join.join();
        assert!(limiter.by_ip.is_poisoned());
        // acquire must still work — recovery branch returns the inner guard.
        limiter
            .acquire_inner([1u8; 32], Some(ip(10, 0, 0, 1)))
            .expect("acquire after by_ip poison");
    }
}
