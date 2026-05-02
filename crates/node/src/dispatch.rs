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
use std::net::{IpAddr, Ipv6Addr};
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use iroh::TransportAddr;
use iroh::Watcher as _;
use iroh::endpoint::Connection;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::config::ResolvedSecurity;
use crate::metrics::Metrics;

/// Bucket key for the per-IP rate limit. IPv4 addresses are used as-is; IPv6
/// addresses are masked to their `/64` prefix.
///
/// Without the mask the per-IP layer is trivially defeated: a customer-grade
/// IPv6 allocation is typically `/64` (or larger), giving an attacker `2^64`
/// distinct `IpAddr`s inside one allocation. Each unique address would be a
/// separate map key, both bypassing the rate limit and churning the eviction
/// path so legitimate IPv4 victims' buckets get flushed.
fn ip_bucket_key(ip: IpAddr) -> IpAddr {
    match ip {
        IpAddr::V4(_) => ip,
        IpAddr::V6(v6) => {
            let mut octets = v6.octets();
            for b in &mut octets[8..] {
                *b = 0;
            }
            IpAddr::V6(Ipv6Addr::from(octets))
        }
    }
}

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
/// On the success path of [`ConnectionLimiter::acquire`] (`acquire_inner`),
/// the limiter calls `Metrics::dispatch_permit_acquired()` immediately before
/// constructing this struct; `Drop` calls `Metrics::dispatch_permit_released()`
/// unconditionally. The two calls must remain paired: every successful
/// construction is matched by exactly one drop, and the gauge stays balanced.
///
/// `_sem` is `Option` so the `max_concurrent_handlers = 0` disabled-cap mode
/// can return a `Permit` without holding a semaphore slot — the in-flight
/// gauge still tracks the handler, and `Drop` releases nothing (the
/// `OwnedSemaphorePermit` is `None`).
///
/// `#[must_use]`: dropping the permit immediately (`let _ = limiter.acquire(&conn);`)
/// would defeat the purpose of acquiring it — the slot is freed before the
/// handler runs, so the cap fails to bound concurrency.
#[must_use = "drop the Permit at end of handler scope; otherwise the in-flight slot is released immediately"]
pub struct Permit {
    _sem: Option<OwnedSemaphorePermit>,
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

    /// Replace `rate`/`burst` in place across a hot-reload.
    ///
    /// `last_refill` is preserved so a lowered rate doesn't retroactively
    /// debit phantom tokens and a raised rate doesn't credit them — the
    /// next `try_consume` computes refill from the elapsed-since-`last_refill`
    /// window using the new `rate`, which is the natural fairness boundary
    /// across the swap. Accumulated `tokens` are clamped to the new
    /// `burst` so a lowered burst can't leave the bucket above its
    /// ceiling.
    fn set_rate_burst(&mut self, rate: f64, burst: f64) {
        debug_assert!(
            rate.is_finite() && rate > 0.0,
            "rate must be finite-positive (callers gate on rate > 0 before reaching here)"
        );
        debug_assert!(
            burst.is_finite() && burst >= 1.0,
            "burst must be finite and at least 1"
        );
        self.rate = rate;
        self.burst = burst;
        if self.tokens > burst {
            self.tokens = burst;
        }
    }
}

/// `HashMap<K, TokenBucket>` with an optional cap on the number of tracked
/// entries.
///
/// When the map is full and a new key arrives, the entry with the oldest
/// `last_refill` timestamp is evicted (O(n) linear scan over the values —
/// acceptable at `PoC` scale with `cap ≤ a few thousand`).
///
/// `cap == 0` makes the map unbounded — operator opt-in for nodes that want
/// to disable the source-bookkeeping limit. An attacker churning identities
/// can grow the map without bound in that mode; `resolve_security` warns
/// about this at startup.
///
/// `rate <= 0.0` disables the layer entirely: `try_consume` short-circuits
/// to `true` without touching the map. Existing buckets are left in place
/// so a subsequent enable transition can resume from them.
struct BoundedRateMap<K> {
    inner: HashMap<K, TokenBucket>,
    /// `0` = unbounded.
    cap: usize,
    /// `0.0` = layer disabled.
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
    ///
    /// Disabled-layer fast path: if `self.rate <= 0.0` the layer is
    /// administratively disabled and every consume returns `true` without
    /// touching the map. Without this short-circuit a `rate == 0` config
    /// would silently deny every request after the first burst — the
    /// opposite of "disabled."
    fn try_consume(&mut self, key: &K, now: Instant) -> bool {
        if self.rate <= 0.0 {
            return true;
        }
        if let Some(bucket) = self.inner.get_mut(key) {
            return bucket.try_consume(now);
        }
        // `cap == 0` means "no cap" — skip the eviction path entirely so
        // an unbounded map doesn't waste a linear scan finding nothing to
        // evict each insert.
        if self.cap > 0 && self.inner.len() >= self.cap {
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

    /// Replace `rate`/`burst` and propagate the new parameters to every
    /// existing bucket. Used by `ConnectionLimiter::reload`.
    ///
    /// Walks `self.inner.values_mut()` — O(n) with n ≤ `cap`. The map's
    /// surrounding `Mutex` is held for the duration; at the default
    /// `max_tracked_sources = 4096` this blocks acquires for microseconds.
    ///
    /// Transitioning rate `>0 → 0` (disabling the layer) leaves existing
    /// buckets in place — they're cheap, and a subsequent re-enable just
    /// resumes from where they left off rather than discarding fairness
    /// state. A reload that disables and never re-enables will eventually
    /// drain via the cap-eviction path the next time a new key arrives.
    ///
    /// Safety across dramatic rate drops (e.g. `1e9 → 1.0`) depends on
    /// `TokenBucket::try_consume` clamping with `.min(self.burst)`:
    /// preserved `last_refill` would otherwise let `elapsed * new_rate`
    /// credit phantom tokens during a long idle window. The clamp keeps
    /// post-reload behavior bounded by the new `burst` regardless of
    /// how stale `last_refill` is.
    fn set_rate_burst(&mut self, rate: f64, burst: f64) {
        self.rate = rate;
        self.burst = burst;
        if rate > 0.0 {
            for bucket in self.inner.values_mut() {
                bucket.set_rate_burst(rate, burst);
            }
        }
    }

    /// Replace the cap. `0` switches the map to unbounded mode (no
    /// eviction). Shrinking evicts oldest entries until `len() <= cap`.
    /// Growing is a no-op for existing entries.
    ///
    /// Bulk shrink path: collect the keys with their `last_refill`,
    /// `select_nth_unstable_by_key` to partition the oldest cohort, then
    /// drop them. `select_nth_unstable_by_key` is O(n) (introselect, since
    /// stdlib 1.49). Calling `evict_oldest` in a loop here would be
    /// O(n²) — at the default `max_tracked_sources = 4096`, a shrink to
    /// 10 entries is ~16 M comparisons under the per-map `Mutex` held by
    /// every live acquire. The bulk path keeps the worst-case shrink in
    /// microseconds.
    ///
    /// Tie-breaking: `select_nth_unstable_by_key` does not promise stable
    /// ordering for entries with equal `last_refill`. With `Instant::now()`
    /// resolution being typically nanosecond-grained on Linux, ties are
    /// vanishingly rare in production traffic — but tests using tight
    /// `Instant`-arithmetic loops can produce them, so test assertions
    /// about *which* tied entry survives a shrink are unstable and should
    /// not be written. Functionally it's a wash: any of the `cap` newest
    /// entries are equally valid to retain.
    fn set_cap(&mut self, cap: usize) {
        self.cap = cap;
        if cap == 0 || self.inner.len() <= cap {
            return;
        }
        // Snapshot (key, last_refill). The clone of K is unavoidable
        // because we need both the keys to remove and the timestamps to
        // partition by — `HashMap::remove` consumes a `&K` and we don't
        // hold mutable iterators across removes.
        let mut entries: Vec<(K, Instant)> = self
            .inner
            .iter()
            .map(|(k, b)| (k.clone(), b.last_refill))
            .collect();
        // Partition so the `cap` newest (largest `last_refill`) entries
        // end up in `entries[entries.len() - cap..]`. The first
        // `entries.len() - cap` slots are then guaranteed to be the
        // *oldest* — exactly what we want to evict. `select_nth_unstable_by_key`
        // is O(n) average; the `_by_key` variant takes a closure
        // returning the sort key (timestamp here).
        let drop_count = entries.len() - cap;
        let _ = entries.select_nth_unstable_by_key(drop_count, |(_, t)| *t);
        for (k, _) in entries.into_iter().take(drop_count) {
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
///
/// `reload(&self, &ResolvedSecurity)` applies a new resolved-security
/// snapshot in place: token-bucket maps are mutated under their existing
/// `Mutex` (preserving `last_refill` and accumulated `tokens`, clamped to
/// the new `burst`), and the `Arc<Semaphore>` identity is preserved across
/// cap changes via `add_permits` / `acquire_many_owned(...).forget()` —
/// every outstanding `OwnedSemaphorePermit` continues to drain into the
/// same semaphore on `Drop`.
///
/// `target_max_concurrent` records the operator-asked-for cap; `0` means
/// "global cap disabled" and the acquire path skips the semaphore
/// entirely. `live_semaphore_size` tracks the actual live size of the
/// semaphore so a disable→re-enable transition resizes from the live
/// value rather than the (stale-while-disabled) target.
///
/// Two reloads racing (N→N+1→N) compute their permit deltas from
/// `live_semaphore_size`, not from the live `available_permits()` —
/// which would lag behind an in-flight shrink and produce drift. The
/// `target_lock` `Mutex<()>` serialises the recording-and-resize step so
/// concurrent reloads see each other's effects.
#[allow(missing_debug_implementations)]
pub struct ConnectionLimiter {
    semaphore: Arc<Semaphore>,
    /// Operator-requested cap. `0` = disabled (acquire path skips the
    /// semaphore entirely). Source of truth for racing reloads.
    target_max_concurrent: AtomicU32,
    /// Actual live size of `semaphore`. Diverges from `target` only
    /// while disabled (`target == 0` doesn't touch permits). Re-enable
    /// transitions resize from this value to the new target.
    live_semaphore_size: AtomicU32,
    /// Serialises reload-vs-reload for the semaphore resize.
    target_lock: Mutex<()>,
    by_node: Mutex<BoundedRateMap<[u8; 32]>>,
    by_ip: Mutex<BoundedRateMap<IpAddr>>,
    /// Mirrors `by_ip.rate > 0.0`. Read on the relay-only-connection
    /// fast path so we can record `dispatch_per_ip_skipped_no_addr_total`
    /// without taking the `by_ip` mutex on every relay accept. Written
    /// only under `target_lock` during reload.
    per_ip_enabled: AtomicBool,
    /// One-shot poison-log gates. Set on the first observation of a
    /// poisoned `by_ip` / `by_node` mutex so `tracing::error!` fires
    /// once per process rather than once per acquire — under sustained
    /// traffic on a poisoned mutex the unguarded form would emit
    /// megabytes of identical log lines per second.
    by_ip_poison_logged: AtomicBool,
    by_node_poison_logged: AtomicBool,
    metrics: Arc<Metrics>,
}

impl ConnectionLimiter {
    /// Construct a limiter from the resolved security configuration.
    pub fn new(cfg: &ResolvedSecurity, metrics: Arc<Metrics>) -> Self {
        // `cfg.max_concurrent_handlers == 0` → disabled. Build the
        // semaphore at size 0 (it will never be acquired; the acquire
        // path branches on `target_max_concurrent`). Sizing it to anything
        // else would just be wasted work.
        let initial_size = cfg.max_concurrent_handlers;
        let semaphore = Arc::new(Semaphore::new(
            usize::try_from(initial_size).unwrap_or(usize::MAX),
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
        let per_ip_enabled = AtomicBool::new(cfg.per_ip_rate_per_sec > 0.0);
        Self {
            semaphore,
            target_max_concurrent: AtomicU32::new(initial_size),
            live_semaphore_size: AtomicU32::new(initial_size),
            target_lock: Mutex::new(()),
            by_node,
            by_ip,
            per_ip_enabled,
            by_ip_poison_logged: AtomicBool::new(false),
            by_node_poison_logged: AtomicBool::new(false),
            metrics,
        }
    }

    /// Apply a new `ResolvedSecurity` to the live limiter.
    ///
    /// Field-by-field strategy:
    /// - `per_{node,ip}_rate_per_sec`, `per_{node,ip}_burst`,
    ///   `max_tracked_sources`: in-place under the existing per-map
    ///   `Mutex` (see the private `BoundedRateMap::set_rate_burst` and
    ///   `BoundedRateMap::set_cap` mutators). Concurrent acquires briefly
    ///   serialise behind the reload — bounded by an O(n ≤ cap) walk of
    ///   the bucket map.
    /// - `max_concurrent_handlers`: `Arc<Semaphore>` identity is
    ///   preserved (every in-flight `OwnedSemaphorePermit` holds a
    ///   clone). Grow via `add_permits(delta)` synchronously; shrink by
    ///   spawning a detached task that `acquire_many_owned(delta).await
    ///   .unwrap().forget()`s — live handlers naturally drain the
    ///   surplus. `0` records the disabled state without touching
    ///   permits; a later non-zero value resizes from
    ///   `live_semaphore_size` (the size left over from the last
    ///   enabled period).
    ///
    /// Worked example (cap=4 startup → disable → re-enable smaller → grow):
    /// ```text
    /// reload(target=0): target=0, live stays 4; early return (acquire skips semaphore).
    /// reload(target=3): live=4, store live=3, shrink by 1 (spawn forget task).
    /// reload(target=5): live=3, store live=5, add_permits(2).
    /// ```
    ///
    /// Caveats: during a shrink the live cap is `>= new_target` until
    /// enough handlers drain. Under racing reloads (N→N+1→N) the
    /// post-task permit count may briefly land anywhere in `[N, N+1]`,
    /// and a parked shrink task that fires after a subsequent grow can
    /// "forget" permits the operator just re-granted — leaving
    /// `live_semaphore_size` (the recorded value) ahead of the actual
    /// permit count by however many the parked task ate. Subsequent
    /// reloads compute deltas from the recorded value, so the
    /// discrepancy stays bounded by the in-flight shrink count and
    /// converges as the operator stops reloading. A disable→enable→
    /// smaller-cap sequence spawns a shrink task on the re-enable.
    /// All acceptable at `PoC` scale.
    ///
    /// Infallible: token-bucket mutation can't fail, `add_permits` can't
    /// fail, and the shrink path is `tokio::spawn` (which only fails by
    /// panicking — not via this return). Caller (`RuntimeReloadState
    /// ::reload`) has already validated the values via
    /// `resolve_security`, so this method takes a `&ResolvedSecurity`
    /// rather than re-parsing.
    pub fn reload(&self, cfg: &ResolvedSecurity) {
        // 1. Token-bucket maps: per-map mutex, in-place mutation.
        //    rate=0 → layer disabled; cap=0 → unbounded map.
        {
            let mut g = lock_recover(&self.by_node, "per-node", &self.by_node_poison_logged);
            g.set_rate_burst(cfg.per_node_rate_per_sec, f64::from(cfg.per_node_burst));
            g.set_cap(cfg.max_tracked_sources);
        }
        {
            let mut g = lock_recover(&self.by_ip, "per-ip", &self.by_ip_poison_logged);
            g.set_rate_burst(cfg.per_ip_rate_per_sec, f64::from(cfg.per_ip_burst));
            g.set_cap(cfg.max_tracked_sources);
        }
        // Mirror the per-IP enabled state for the relay-only fast path.
        // Stored under Relaxed because there's no happens-before
        // relationship to the bucket's own state — the worst case across
        // a swap is one accept observing the prior generation, which is
        // operationally indistinguishable from arriving microseconds
        // earlier.
        self.per_ip_enabled
            .store(cfg.per_ip_rate_per_sec > 0.0, Ordering::Relaxed);

        // 2. Semaphore: serialise reloads through `target_lock`. Resize
        //    math operates on `live_semaphore_size`, not `target`, so
        //    disable→enable→resize transitions stay correct (target was
        //    0 during disabled period; live retains the last enabled size).
        let _g = self
            .target_lock
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let next_target = cfg.max_concurrent_handlers;
        self.target_max_concurrent
            .store(next_target, Ordering::Relaxed);

        // No structural change needed when disabling or staying disabled
        // — `acquire_inner` branches on `target_max_concurrent`, and
        // leaving `live_semaphore_size` at its last-enabled value lets a
        // future re-enable resize from a sensible baseline.
        if next_target == 0 {
            return;
        }

        let live = self.live_semaphore_size.load(Ordering::Relaxed);
        self.live_semaphore_size
            .store(next_target, Ordering::Relaxed);

        if next_target > live {
            // Grow: add permits synchronously.
            let delta = next_target - live;
            self.semaphore.add_permits(delta as usize);
        } else if next_target < live {
            // Shrink: spawn a detached task that waits for `delta`
            // permits to become free, then forgets them. The acquire
            // is best-effort — if a third reload arrives growing the
            // semaphore back up while this task is parked, the task
            // may forget permits the operator just re-granted. The
            // next reload will reconcile from the recorded
            // `live_semaphore_size`.
            let delta = live - next_target;
            let sem = Arc::clone(&self.semaphore);
            let metrics = Arc::clone(&self.metrics);
            tokio::spawn(async move {
                match sem.acquire_many_owned(delta).await {
                    Ok(p) => p.forget(),
                    Err(err) => {
                        // `live_semaphore_size` already moved to the new
                        // target, but the actual permit count did not —
                        // accounting drifts permanently for the rest of
                        // the process lifetime. Surface at `error!` and
                        // bump a counter so dashboards can fire on it;
                        // otherwise the discrepancy is invisible until
                        // the operator notices the cap stopped behaving.
                        // The only documented `AcquireError` today is
                        // "semaphore closed" (shutdown only), but bind
                        // `err` so a future `#[non_exhaustive]` variant
                        // surfaces.
                        metrics.dispatch_shrink_skipped();
                        tracing::error!(
                            %err,
                            delta,
                            "dispatch limiter shrink task failed; live_semaphore_size now drifts from actual permit count by `delta`"
                        );
                    }
                }
            });
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

    /// Cross-module test hook with the same behavior as `acquire`,
    /// minus the iroh `Connection` argument. Exposed as `pub` so unit
    /// tests in other modules and integration tests under `tests/` can
    /// drive the limiter directly. Production code must not call this —
    /// `Self::acquire` is the only supported entry point.
    /// `#[doc(hidden)]` keeps it out of the rendered public API surface.
    #[doc(hidden)]
    pub fn acquire_for_test(
        &self,
        node_key: [u8; 32],
        peer_ip: Option<IpAddr>,
    ) -> Result<Permit, RejectReason> {
        self.acquire_inner(node_key, peer_ip)
    }

    /// Shared implementation behind [`Self::acquire`] and
    /// [`Self::acquire_for_test`].
    fn acquire_inner(
        &self,
        node_key: [u8; 32],
        peer_ip: Option<IpAddr>,
    ) -> Result<Permit, RejectReason> {
        // target == 0 disables the global cap; skip the semaphore.
        let sem_permit = if self.target_max_concurrent.load(Ordering::Relaxed) > 0 {
            Some(
                Arc::clone(&self.semaphore)
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
                    })?,
            )
        } else {
            None
        };

        let now = Instant::now();

        // Per-IP first (skip for relay-only connections). Checking the
        // scarcer-resource layer first means a per-IP rejection never
        // charges a per-NodeID token.
        match peer_ip {
            Some(ip) => {
                let mut map = lock_recover(&self.by_ip, "per-ip", &self.by_ip_poison_logged);
                if !map.try_consume(&ip_bucket_key(ip), now) {
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
            None => {
                // Relay-only connection: no IP to charge. If the per-IP
                // layer is enabled, the operator's intent ("bound rate
                // from each source address") cannot be enforced for
                // this connection — surface a counter so dashboards
                // distinguish "legitimate relay-only peer" from
                // "attacker exploiting the path-not-yet-selected race
                // window described in `peer_ip`".
                if self.per_ip_enabled.load(Ordering::Relaxed) {
                    self.metrics.dispatch_per_ip_skipped_no_addr();
                }
            }
        }

        {
            let mut map = lock_recover(&self.by_node, "per-node", &self.by_node_poison_logged);
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

        // Increment the in-flight gauge before constructing the Permit
        // so the "incremented on success" / "Drop decrements
        // unconditionally" pair stays balanced even if the constructor
        // is split or the assignment is reordered.
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
/// inconsistency, so silent recovery is correct.
///
/// `logged` is a per-mutex one-shot gate: the first poison observation
/// emits `tracing::error!`, every subsequent recovery is silent. Without
/// the gate, sustained traffic on a poisoned mutex would emit one error
/// line per acquire — megabytes per second under flood.
fn lock_recover<'a, T>(
    m: &'a Mutex<T>,
    label: &'static str,
    logged: &AtomicBool,
) -> std::sync::MutexGuard<'a, T> {
    m.lock().unwrap_or_else(|poisoned| {
        if logged
            .compare_exchange(false, true, Ordering::Relaxed, Ordering::Relaxed)
            .is_ok()
        {
            tracing::error!(
                map = label,
                "dispatch rate-limit mutex poisoned; recovering inner state (further occurrences suppressed)"
            );
        }
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
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use super::{BoundedRateMap, ConnectionLimiter, RejectReason, TokenBucket};
    use crate::config::ResolvedSecurity;
    use crate::metrics::Metrics;

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
        let mut map: BoundedRateMap<u8> = BoundedRateMap::new(16, 1.0, 1.0);
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
        let _p = limiter
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
        let _p = limiter
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
        let _p = limiter
            .acquire_inner([1u8; 32], Some(ip(10, 0, 0, 1)))
            .expect("acquire after by_ip poison");
    }

    // --- BoundedRateMap reload mutators ---------------------------------------

    #[test]
    fn bounded_map_set_rate_burst_clamps_existing_tokens() {
        // Drain to 0 tokens, refill to ~3 via elapsed time, then lower the
        // burst to 1. The clamp must drop the bucket's `tokens` to 1.
        let mut map: BoundedRateMap<u8> = BoundedRateMap::new(16, 10.0, 5.0);
        let t0 = Instant::now();
        for _ in 0..5 {
            map.try_consume(&1, t0);
        }
        assert!(!map.try_consume(&1, t0), "drained");
        let t1 = t0 + Duration::from_millis(300); // ~3 tokens credited
        // Lower burst before consuming again. set_rate_burst clamps tokens
        // to the new burst on the *bucket*, but the bucket's tokens are
        // recomputed on next consume; clamp must still be observable as
        // "at most one extra token after lowering burst to 1".
        map.set_rate_burst(10.0, 1.0);
        // Existing bucket: tokens were 0 at t0, accrued ~3 at t1, now
        // clamped to burst=1 at the moment of consume.
        assert!(map.try_consume(&1, t1), "1 token after clamp");
        assert!(
            !map.try_consume(&1, t1),
            "burst=1 clamp must prevent immediate second consume"
        );
    }

    #[test]
    fn bounded_map_set_rate_burst_preserves_last_refill() {
        // last_refill stays at the original consume time, so the next
        // consume credits proportionally to (new rate × elapsed-since-
        // original-last-refill) — not (new rate × time-since-set-call).
        let mut map: BoundedRateMap<u8> = BoundedRateMap::new(16, 1.0, 1.0);
        let t0 = Instant::now();
        assert!(map.try_consume(&7, t0)); // burst=1 consumed; last_refill=t0
        // Raise the rate dramatically. last_refill must stay at t0.
        map.set_rate_burst(100.0, 1.0);
        // 100ms after t0: at rate=100 tok/s, 10 tokens accrued (capped at
        // burst=1). One consume should succeed.
        let t1 = t0 + Duration::from_millis(100);
        assert!(map.try_consume(&7, t1), "rate=100 credited from t0");
    }

    #[test]
    fn bounded_map_set_cap_evicts_when_shrinking() {
        // Fill cap=4 with successively-later last_refill, then shrink to
        // cap=2. The two oldest must be evicted, surviving keys = newest 2.
        let mut map: BoundedRateMap<u32> = BoundedRateMap::new(4, 100.0, 1.0);
        let t0 = Instant::now();
        for k in 0..4u32 {
            map.try_consume(&k, t0 + Duration::from_millis(u64::from(k)));
        }
        assert_eq!(map.len(), 4);
        map.set_cap(2);
        assert_eq!(map.len(), 2, "shrink to cap evicts oldest");
        assert!(!map.contains_key(&0), "oldest evicted");
        assert!(!map.contains_key(&1), "second-oldest evicted");
        assert!(map.contains_key(&2), "newer survives");
        assert!(map.contains_key(&3), "newest survives");
    }

    #[test]
    fn bounded_map_set_cap_bulk_shrink_drops_oldest_correctly() {
        // Larger drop than the basic eviction test: 4096 → 10 must
        // retain the 10 newest entries (by last_refill) and drop the
        // 4086 oldest. Catches a regression where the bulk-shrink path
        // selects wrong-side entries (e.g. partitions newest-first and
        // drops the wrong half).
        let mut map: BoundedRateMap<u32> = BoundedRateMap::new(4096, 100.0, 1.0);
        let t0 = Instant::now();
        for k in 0..4096u32 {
            map.try_consume(&k, t0 + Duration::from_micros(u64::from(k)));
        }
        assert_eq!(map.len(), 4096);
        map.set_cap(10);
        assert_eq!(map.len(), 10, "shrink to 10 leaves exactly 10");
        // Surviving keys are 4086..=4095 (the 10 newest by last_refill).
        for k in 4086..4096u32 {
            assert!(map.contains_key(&k), "newest key {k} survives");
        }
        for k in [0u32, 1, 100, 1000, 4085] {
            assert!(!map.contains_key(&k), "old key {k} evicted");
        }
    }

    #[test]
    fn bounded_map_set_cap_grow_is_noop_for_existing_entries() {
        let mut map: BoundedRateMap<u32> = BoundedRateMap::new(4, 100.0, 1.0);
        let t0 = Instant::now();
        for k in 0..4u32 {
            map.try_consume(&k, t0);
        }
        map.set_cap(16);
        assert_eq!(map.len(), 4, "grow doesn't touch existing entries");
        // 5th insert succeeds without eviction (cap > len).
        let t1 = t0 + Duration::from_millis(1);
        map.try_consume(&99, t1);
        assert_eq!(map.len(), 5, "grow allowed a new key without eviction");
    }

    // --- Disabled-layer (0 = unlimited) ---------------------------------------

    #[test]
    fn bounded_map_disabled_rate_passes_all_consumes() {
        // rate=0 short-circuits to true; the map stays empty (no buckets
        // inserted on the disabled-layer fast path).
        let mut map: BoundedRateMap<u32> = BoundedRateMap::new(16, 0.0, 1.0);
        let now = Instant::now();
        for k in 0..1000u32 {
            assert!(map.try_consume(&k, now), "rate=0 must accept");
        }
        assert_eq!(map.len(), 0, "disabled-layer must not allocate buckets");
    }

    #[test]
    fn bounded_map_unbounded_cap_skips_eviction() {
        // cap=0 means unbounded — keys keep accumulating without eviction.
        let mut map: BoundedRateMap<u32> = BoundedRateMap::new(0, 100.0, 1.0);
        let t0 = Instant::now();
        for k in 0..50u32 {
            map.try_consume(&k, t0 + Duration::from_micros(u64::from(k)));
        }
        assert_eq!(map.len(), 50, "no eviction when cap=0");
        for k in 0..50u32 {
            assert!(map.contains_key(&k), "all keys retained");
        }
    }

    fn disabled_global_security() -> ResolvedSecurity {
        ResolvedSecurity {
            max_concurrent_handlers: 0,
            per_node_rate_per_sec: 1e9,
            per_node_burst: u32::MAX,
            per_ip_rate_per_sec: 1e9,
            per_ip_burst: u32::MAX,
            max_tracked_sources: 4096,
        }
    }

    #[test]
    fn connection_limiter_disabled_global_skips_semaphore() {
        // max_concurrent_handlers=0 disables the global cap. We can hold
        // far more permits than the per-source layers would normally
        // allow at startup. Drop-test verifies all permits live until the
        // explicit drop at the end.
        let metrics = Arc::new(Metrics::new());
        let limiter = ConnectionLimiter::new(&disabled_global_security(), Arc::clone(&metrics));
        let mut held = Vec::with_capacity(256);
        for i in 0..256 {
            let mut node = [0u8; 32];
            node[0] = u8::try_from(i % 251).unwrap_or(0);
            node[1] = u8::try_from(i / 251).unwrap_or(0);
            let octet = u8::try_from(i % 200).unwrap_or(0);
            let p = limiter
                .acquire_inner(node, Some(ip(10, 0, 0, octet)))
                .expect("disabled global cap must accept all acquires");
            held.push(p);
        }
        assert_eq!(held.len(), 256);
        drop(held);
    }

    // --- ConnectionLimiter::reload --------------------------------------------

    #[tokio::test]
    async fn connection_limiter_reload_grows_semaphore() {
        let metrics = Arc::new(Metrics::new());
        let limiter = ConnectionLimiter::new(&strict_security(2), Arc::clone(&metrics));
        let _p1 = limiter
            .acquire_inner([1u8; 32], Some(ip(10, 0, 0, 1)))
            .unwrap();
        let _p2 = limiter
            .acquire_inner([2u8; 32], Some(ip(10, 0, 0, 2)))
            .unwrap();
        // Cap exhausted at 2.
        assert!(
            limiter
                .acquire_inner([3u8; 32], Some(ip(10, 0, 0, 3)))
                .is_err()
        );

        // Reload to cap=5. Same per-source layers (permissive enough for
        // these acquires).
        let mut new_cfg = permissive_security();
        new_cfg.max_concurrent_handlers = 5;
        limiter.reload(&new_cfg);

        // Three more acquires from fresh (node, IP) pairs — global cap
        // grew from 2 to 5, so all three succeed.
        let _p3 = limiter
            .acquire_inner([3u8; 32], Some(ip(10, 0, 0, 3)))
            .expect("after grow");
        let _p4 = limiter
            .acquire_inner([4u8; 32], Some(ip(10, 0, 0, 4)))
            .expect("after grow");
        let _p5 = limiter
            .acquire_inner([5u8; 32], Some(ip(10, 0, 0, 5)))
            .expect("after grow");
    }

    #[tokio::test]
    async fn connection_limiter_reload_shrinks_semaphore_eventually() {
        // Start at cap=4, hold 2 permits, shrink to cap=3 (forget 1
        // available permit), drop 1 held permit. The shrink task should
        // forget exactly the available delta; the drop should re-add 1
        // permit. Net free = 0; a fresh acquire must fail GlobalFull.
        let metrics = Arc::new(Metrics::new());
        let limiter = ConnectionLimiter::new(&strict_security(4), Arc::clone(&metrics));
        let _p1 = limiter
            .acquire_inner([1u8; 32], Some(ip(10, 0, 0, 1)))
            .unwrap();
        let p2 = limiter
            .acquire_inner([2u8; 32], Some(ip(10, 0, 0, 2)))
            .unwrap();

        let mut new_cfg = permissive_security();
        new_cfg.max_concurrent_handlers = 3;
        limiter.reload(&new_cfg);

        // Yield to let the spawned shrink task forget the available permit.
        for _ in 0..32 {
            tokio::task::yield_now().await;
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
        // 4 (live) → 3 (target). Held = 2. Forget = 4 - 3 = 1. Available = 1.
        // Drop p2: available = 2; held = 1. New acquires can succeed.
        drop(p2);
        let _p3 = limiter
            .acquire_inner([3u8; 32], Some(ip(10, 0, 0, 3)))
            .expect("after drop, slot freed");
        let _p4 = limiter
            .acquire_inner([4u8; 32], Some(ip(10, 0, 0, 4)))
            .expect("after drop, second slot freed");
        // Now held = 3 (p1, p3, p4), cap = 3, no more free.
        assert!(
            limiter
                .acquire_inner([5u8; 32], Some(ip(10, 0, 0, 5)))
                .is_err(),
            "shrink to 3 must reject the 4th acquire"
        );
    }

    #[tokio::test]
    async fn connection_limiter_reload_updates_per_node_rate_in_place() {
        // Start with strict per-node burst=1; exhaust it from a single
        // node; reload with raised burst=10 and confirm the *existing*
        // bucket sees the new burst (next consume succeeds without
        // dropping or re-creating the bucket).
        let metrics = Arc::new(Metrics::new());
        let limiter = ConnectionLimiter::new(&strict_security(u32::MAX), Arc::clone(&metrics));
        let attacker = [42u8; 32];
        let _p1 = limiter
            .acquire_inner(attacker, Some(ip(10, 0, 0, 1)))
            .unwrap();
        // Per-node burst=1 exhausted. (The per-IP burst=1 also blocks
        // re-use of 10.0.0.1; switch IPs to isolate the per-node check.)
        assert!(
            limiter
                .acquire_inner(attacker, Some(ip(10, 0, 0, 2)))
                .is_err()
        );
        // Reload: raise per-node burst (and rate) so the in-place mutator
        // refreshes the bucket. Per-IP also raised so it doesn't shadow.
        let mut new_cfg = permissive_security();
        new_cfg.max_concurrent_handlers = u32::MAX;
        limiter.reload(&new_cfg);
        let _p2 = limiter
            .acquire_inner(attacker, Some(ip(10, 0, 0, 3)))
            .expect("raised burst should make this consume succeed");
    }

    #[tokio::test]
    async fn connection_limiter_reload_target_tracking_handles_repeated_grow() {
        // Three reloads N=1, N=5, N=2. Then a fourth to N=3. The grow
        // step is observable: we should be able to hold exactly 3 permits
        // after the fourth reload (held grows to 3, 4th is rejected).
        let metrics = Arc::new(Metrics::new());
        let limiter = ConnectionLimiter::new(&strict_security(1), Arc::clone(&metrics));
        let mut c = permissive_security();
        c.max_concurrent_handlers = 5;
        limiter.reload(&c);
        c.max_concurrent_handlers = 2;
        limiter.reload(&c);
        // Yield so the shrink (5→2) task forgets 3 permits.
        for _ in 0..32 {
            tokio::task::yield_now().await;
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
        c.max_concurrent_handlers = 3;
        limiter.reload(&c);
        let _p1 = limiter
            .acquire_inner([1u8; 32], Some(ip(10, 0, 0, 1)))
            .unwrap();
        let _p2 = limiter
            .acquire_inner([2u8; 32], Some(ip(10, 0, 0, 2)))
            .unwrap();
        let _p3 = limiter
            .acquire_inner([3u8; 32], Some(ip(10, 0, 0, 3)))
            .unwrap();
        assert!(
            limiter
                .acquire_inner([4u8; 32], Some(ip(10, 0, 0, 4)))
                .is_err(),
            "4th must be rejected at cap=3"
        );
    }

    #[tokio::test]
    async fn connection_limiter_reload_disable_then_re_enable_resizes_correctly() {
        // Start cap=4, disable (target=0), acquire freely (semaphore
        // skipped), re-enable to cap=3, drop everyone, hold 3 — the 4th
        // must reject at the new cap=3.
        let metrics = Arc::new(Metrics::new());
        let limiter = ConnectionLimiter::new(&strict_security(4), Arc::clone(&metrics));

        // Disable the global cap.
        let mut c = permissive_security();
        c.max_concurrent_handlers = 0;
        limiter.reload(&c);

        // While disabled, hold many permits — the acquire path skips the
        // semaphore so live_semaphore_size is irrelevant.
        let bulk: Vec<_> = (0..16u8)
            .map(|i| {
                limiter
                    .acquire_inner([i; 32], Some(ip(10, 0, 0, i)))
                    .expect("disabled cap accepts all")
            })
            .collect();
        drop(bulk);

        // Re-enable to cap=3. This resizes from live_semaphore_size=4
        // (last enabled value) to 3 — spawn a shrink task forgetting 1
        // permit.
        c.max_concurrent_handlers = 3;
        limiter.reload(&c);
        for _ in 0..32 {
            tokio::task::yield_now().await;
            tokio::time::sleep(Duration::from_millis(2)).await;
        }

        let _p1 = limiter
            .acquire_inner([1u8; 32], Some(ip(10, 0, 0, 1)))
            .unwrap();
        let _p2 = limiter
            .acquire_inner([2u8; 32], Some(ip(10, 0, 0, 2)))
            .unwrap();
        let _p3 = limiter
            .acquire_inner([3u8; 32], Some(ip(10, 0, 0, 3)))
            .unwrap();
        assert!(
            limiter
                .acquire_inner([4u8; 32], Some(ip(10, 0, 0, 4)))
                .is_err(),
            "after re-enable to 3, 4th must reject"
        );
    }

    #[test]
    fn connection_limiter_reload_per_layer_disable_independent() {
        // Per-IP disabled, per-NodeID still enforces its burst. Then
        // disable per-NodeID instead and verify per-IP is the gate.
        let metrics = Arc::new(Metrics::new());
        let limiter = ConnectionLimiter::new(&strict_security(u32::MAX), Arc::clone(&metrics));

        // Disable per-IP only.
        let mut c = strict_security(u32::MAX);
        c.per_ip_rate_per_sec = 0.0;
        c.per_ip_burst = 0;
        limiter.reload(&c);

        let same_node = [9u8; 32];
        // Same per-node burst is still 1; first acquire from that node
        // succeeds, second from same node (different IPs to bypass per-IP
        // — which is anyway disabled) fails on per-NodeID.
        let _p1 = limiter
            .acquire_inner(same_node, Some(ip(10, 0, 0, 1)))
            .expect("first per-node acquire");
        let err = limiter
            .acquire_inner(same_node, Some(ip(10, 0, 0, 2)))
            .expect_err("second from same node must reject on per-NodeID");
        assert_eq!(err, RejectReason::PerNodeId);

        // Now disable per-NodeID *and* re-enable per-IP. Per-IP burst=1
        // is now the gate.
        let mut c = strict_security(u32::MAX);
        c.per_node_rate_per_sec = 0.0;
        c.per_node_burst = 0;
        limiter.reload(&c);

        let same_ip = Some(ip(192, 0, 2, 99));
        let _q1 = limiter
            .acquire_inner([1u8; 32], same_ip)
            .expect("first per-IP acquire");
        let err = limiter
            .acquire_inner([2u8; 32], same_ip)
            .expect_err("second from same IP must reject on per-IP");
        assert_eq!(err, RejectReason::PerIp);
    }

    // --- Reload regression: concurrent reloads / disabled-baseline / metrics-handle ---

    /// `target_lock` exists specifically to serialise concurrent reloads
    /// so racing N→…→M operations converge. Run 8 concurrent reloads
    /// against the same limiter and assert the post-race `live_semaphore_size`
    /// matches the *last* reload to commit (whichever wins the lock race),
    /// then a follow-up reload to a known cap produces exactly that cap's
    /// behaviour. A regression that removed `target_lock` would let two
    /// concurrent reloads' resize math step on each other.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_reloads_converge() {
        let metrics = Arc::new(Metrics::new());
        let limiter = Arc::new(ConnectionLimiter::new(&strict_security(8), metrics));
        let barrier = Arc::new(tokio::sync::Barrier::new(8));
        let mut handles = Vec::new();
        for i in 0..8u32 {
            let l = Arc::clone(&limiter);
            let b = Arc::clone(&barrier);
            handles.push(tokio::spawn(async move {
                b.wait().await;
                let mut cfg = permissive_security();
                cfg.max_concurrent_handlers = 4 + i;
                l.reload(&cfg);
            }));
        }
        for h in handles {
            h.await.unwrap();
        }
        // Drain any pending shrink tasks before the final reconcile.
        for _ in 0..64 {
            tokio::task::yield_now().await;
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
        // Reload to a known cap = 11. From whatever the racing reloads
        // left `live_semaphore_size` at, this should resize correctly.
        let mut cfg = permissive_security();
        cfg.max_concurrent_handlers = 11;
        limiter.reload(&cfg);
        for _ in 0..64 {
            tokio::task::yield_now().await;
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
        // Hold 11 permits; the 12th must reject. This is the operator-
        // observable convergence proof.
        let mut held = Vec::with_capacity(11);
        for i in 0..11u8 {
            held.push(
                limiter
                    .acquire_inner([i; 32], Some(ip(10, 0, 0, i)))
                    .expect("11 acquires under cap=11"),
            );
        }
        assert!(
            limiter
                .acquire_inner([99u8; 32], Some(ip(10, 0, 0, 99)))
                .is_err(),
            "12th acquire must reject at cap=11 after concurrent reloads converge"
        );
    }

    /// Disable → re-enable on `BoundedRateMap` must preserve existing
    /// bucket state, not grant fresh burst on re-enable. The docstring
    /// claims this; the test pins it. Drain a bucket, disable, re-enable
    /// with same parameters at the *same* `Instant`, and assert the next
    /// consume still fails — the bucket carried its drained state across.
    #[test]
    fn bounded_map_disable_then_reenable_preserves_existing_buckets() {
        let mut map: BoundedRateMap<u8> = BoundedRateMap::new(16, 1.0, 1.0);
        let t0 = Instant::now();
        assert!(map.try_consume(&7, t0), "first consume drains burst=1");
        assert!(!map.try_consume(&7, t0), "exhausted at the same instant");

        map.set_rate_burst(0.0, 1.0); // disable layer
        assert!(map.try_consume(&7, t0), "disabled-layer fast path accepts");

        // Re-enable with identical params at the same instant. If the
        // bucket was preserved (per docstring), tokens are still 0.
        map.set_rate_burst(1.0, 1.0);
        assert!(
            !map.try_consume(&7, t0),
            "re-enable must resume bucket state, not grant fresh burst"
        );
    }

    /// Disabled→still-disabled reload must leave `live_semaphore_size`
    /// untouched so a future re-enable resizes from the right baseline.
    /// A regression that flipped the `next_target == 0` early-return to
    /// also reset `live` would silently break the next re-enable.
    #[tokio::test]
    async fn reload_disabled_to_disabled_preserves_live_baseline() {
        let metrics = Arc::new(Metrics::new());
        let limiter = ConnectionLimiter::new(&strict_security(4), Arc::clone(&metrics));
        // First reload: disable.
        let mut c = permissive_security();
        c.max_concurrent_handlers = 0;
        limiter.reload(&c);
        // Second reload: still disabled. Must not touch live.
        limiter.reload(&c);
        // Re-enable to 4. With baseline preserved, this is a no-op resize
        // (live=4 → 4); without it, the resize would be wrong.
        c.max_concurrent_handlers = 4;
        limiter.reload(&c);
        // Hold 4 permits; the 5th must reject.
        let _h: Vec<_> = (0..4u8)
            .map(|i| {
                limiter
                    .acquire_inner([i; 32], Some(ip(10, 0, 0, i)))
                    .expect("acquire under cap=4")
            })
            .collect();
        assert!(
            limiter
                .acquire_inner([99u8; 32], Some(ip(10, 0, 0, 99)))
                .is_err(),
            "5th must reject; if live baseline drifted, this would have over-allocated"
        );
    }

    /// `set_cap(0)` on a populated bounded map transitions the map to
    /// unbounded mode. It must NOT clear existing entries — that would
    /// silently lose fairness state across the cap change.
    #[test]
    fn bounded_map_set_cap_zero_makes_unbounded_and_retains_entries() {
        let mut map: BoundedRateMap<u32> = BoundedRateMap::new(4, 100.0, 1.0);
        let t0 = Instant::now();
        for k in 0..4u32 {
            map.try_consume(&k, t0);
        }
        assert_eq!(map.len(), 4);
        map.set_cap(0);
        assert_eq!(
            map.len(),
            4,
            "transition to unbounded must retain existing entries"
        );
        // And a 5th insert must succeed without eviction.
        map.try_consume(&99, t0 + Duration::from_millis(1));
        assert_eq!(map.len(), 5, "unbounded mode allows growth past old cap");
        for k in 0..4u32 {
            assert!(map.contains_key(&k), "key {k} retained");
        }
    }

    /// Metrics counters must continue to flow after a hot reload — the
    /// `Arc<Metrics>` handle is shared in by the `ConnectionLimiter`
    /// constructor and reused via in-place mutation. A regression that
    /// rebuilt the limiter on reload (and lost the `Arc<Metrics>`) would
    /// leave reject counters frozen at zero post-reload.
    #[tokio::test]
    async fn reload_preserves_metrics_handle_for_post_reload_rejects() {
        let metrics = Arc::new(Metrics::new());
        let limiter = ConnectionLimiter::new(&permissive_security(), Arc::clone(&metrics));

        // Tighten per-NodeID via reload to burst=1.
        let mut cfg = strict_security(u32::MAX);
        cfg.per_node_burst = 1;
        cfg.per_node_rate_per_sec = 0.001; // effectively no refill
        cfg.per_ip_rate_per_sec = 1e9;
        cfg.per_ip_burst = u32::MAX;
        limiter.reload(&cfg);

        let attacker = [42u8; 32];
        let _ok = limiter
            .acquire_inner(attacker, Some(ip(10, 0, 0, 1)))
            .expect("first acquire under tightened limit");
        // Second from same node, fresh IP — per-node burst exhausted post-reload.
        let _err = limiter
            .acquire_inner(attacker, Some(ip(10, 0, 0, 2)))
            .expect_err("post-reload reject");

        let text = metrics.encode().unwrap();
        // OpenMetrics auto-appends `_total` to counter field names, so
        // the field `dispatch_rejected_per_node` (under the `decdn`
        // group) is exposed as `decdn_dispatch_rejected_per_node_total`.
        assert!(
            text.contains("decdn_dispatch_rejected_per_node_total 1"),
            "post-reload reject must increment counter; got:\n{text}"
        );
    }

    /// Sibling of `reload_preserves_metrics_handle_for_post_reload_rejects`
    /// covering the *global-cap* reject path. A regression that swapped the
    /// `dispatch_rejected_global` and `dispatch_rejected_per_node` counter
    /// calls would still pass `RejectReason`-equality assertions in other
    /// tests; only an encoded-scrape assertion catches the typo.
    #[tokio::test]
    async fn rejection_counters_global_visible_in_scrape() {
        let metrics = Arc::new(Metrics::new());
        let limiter = ConnectionLimiter::new(&strict_security(1), Arc::clone(&metrics));
        let _p = limiter
            .acquire_inner([1u8; 32], Some(ip(10, 0, 0, 1)))
            .unwrap();
        let _err = limiter
            .acquire_inner([2u8; 32], Some(ip(10, 0, 0, 2)))
            .expect_err("global cap exhausted");
        let text = metrics.encode().unwrap();
        assert!(
            text.contains("decdn_dispatch_rejected_global_total 1"),
            "global reject must increment its own counter; got:\n{text}"
        );
        assert!(
            !text.contains("decdn_dispatch_rejected_per_ip_total 1"),
            "per-IP counter must not move on a global rejection"
        );
        assert!(
            !text.contains("decdn_dispatch_rejected_per_node_total 1"),
            "per-NodeID counter must not move on a global rejection"
        );
    }

    /// Sibling covering the *per-IP* reject path against the encoded scrape.
    #[tokio::test]
    async fn rejection_counters_per_ip_visible_in_scrape() {
        let metrics = Arc::new(Metrics::new());
        let limiter = ConnectionLimiter::new(&strict_security(u32::MAX), Arc::clone(&metrics));
        let same_ip = Some(ip(192, 0, 2, 7));
        let _p = limiter
            .acquire_inner([1u8; 32], same_ip)
            .expect("first per-IP acquire");
        let _err = limiter
            .acquire_inner([2u8; 32], same_ip)
            .expect_err("second per-IP acquire rejects");
        let text = metrics.encode().unwrap();
        assert!(
            text.contains("decdn_dispatch_rejected_per_ip_total 1"),
            "per-IP reject must increment its own counter; got:\n{text}"
        );
        assert!(
            !text.contains("decdn_dispatch_rejected_global_total 1"),
            "global counter must not move on a per-IP rejection"
        );
        assert!(
            !text.contains("decdn_dispatch_rejected_per_node_total 1"),
            "per-NodeID counter must not move on a per-IP rejection"
        );
    }

    /// Per-IP layer enabled + relay-only connection (no peer IP):
    /// `dispatch_per_ip_skipped_no_addr_total` increments. When the layer
    /// is disabled, no skip is recorded (no enforcement intent => nothing
    /// to skip).
    #[tokio::test]
    async fn per_ip_skipped_when_relay_only_and_layer_enabled() {
        let metrics = Arc::new(Metrics::new());
        let limiter = ConnectionLimiter::new(&strict_security(u32::MAX), Arc::clone(&metrics));
        let _p = limiter
            .acquire_inner([1u8; 32], None)
            .expect("relay acquire under per-IP enabled");
        let text = metrics.encode().unwrap();
        assert!(
            text.contains("decdn_dispatch_per_ip_skipped_no_addr_total 1"),
            "relay-only acquire under enabled per-IP must increment skip counter; got:\n{text}"
        );

        // Disable per-IP via reload; a subsequent relay acquire must
        // *not* increment the counter (no enforcement intent).
        let mut c = strict_security(u32::MAX);
        c.per_ip_rate_per_sec = 0.0;
        c.per_ip_burst = 0;
        limiter.reload(&c);
        let _p2 = limiter
            .acquire_inner([2u8; 32], None)
            .expect("relay acquire under per-IP disabled");
        let text = metrics.encode().unwrap();
        assert!(
            text.contains("decdn_dispatch_per_ip_skipped_no_addr_total 1"),
            "disabled per-IP must not bump the skip counter past 1; got:\n{text}"
        );
    }

    /// IPv6 addresses in the same /64 share a per-IP bucket. Without
    /// `/64` grouping an attacker with a customer-grade IPv6 allocation
    /// can trivially defeat the per-IP layer.
    #[test]
    fn per_ip_buckets_ipv6_by_64_prefix() {
        use std::net::{IpAddr, Ipv6Addr};
        let metrics = Arc::new(Metrics::new());
        let limiter = ConnectionLimiter::new(&strict_security(u32::MAX), Arc::clone(&metrics));
        // Two distinct IPv6 addresses inside the same /64 prefix
        // (`2001:db8::1` and `2001:db8::ffff:ffff:ffff:ffff`). Without
        // /64 grouping these would use independent buckets.
        let v6_a = IpAddr::V6(Ipv6Addr::new(0x2001, 0xdb8, 0, 0, 0, 0, 0, 1));
        let v6_b = IpAddr::V6(Ipv6Addr::new(
            0x2001, 0xdb8, 0, 0, 0xffff, 0xffff, 0xffff, 0xffff,
        ));
        let _p1 = limiter
            .acquire_inner([1u8; 32], Some(v6_a))
            .expect("first acquire from /64");
        // burst=1 per-IP — second acquire from any address in the same
        // /64 must reject on per-IP.
        let err = limiter
            .acquire_inner([2u8; 32], Some(v6_b))
            .expect_err("second IPv6 from same /64 must reject on per-IP");
        assert_eq!(err, RejectReason::PerIp);

        // Address in a *different* /64 must succeed — proves the mask
        // isn't accidentally collapsing every IPv6 into one bucket.
        let v6_c = IpAddr::V6(Ipv6Addr::new(0x2001, 0xdb8, 0, 1, 0, 0, 0, 1));
        let _p2 = limiter
            .acquire_inner([3u8; 32], Some(v6_c))
            .expect("acquire from a different /64 must succeed");
    }

    /// Multi-thread runtime: races N acquire loops against a reload
    /// storm and asserts no panic + final permit count converges to one
    /// of the targets the reload storm wrote. Acquire-vs-reload was
    /// the untested seam: existing tests cover reload-vs-reload
    /// (`concurrent_reloads_converge`) and reload-with-held-permits
    /// (`connection_limiter_reload_*`), but never the cross pair.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn acquire_storm_during_reload_storm_does_not_panic() {
        let metrics = Arc::new(Metrics::new());
        let limiter = Arc::new(ConnectionLimiter::new(
            &strict_security(8),
            Arc::clone(&metrics),
        ));

        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let mut acquirers = Vec::new();
        for w in 0..4u8 {
            let l = Arc::clone(&limiter);
            let stop = Arc::clone(&stop);
            acquirers.push(tokio::spawn(async move {
                let mut node = [0u8; 32];
                node[0] = w;
                while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                    // Don't care about success/fail — only that we
                    // never panic and the limiter remains internally
                    // consistent across the race window.
                    let _ = l.acquire_inner(node, Some(ip(10, 0, 0, w)));
                    tokio::task::yield_now().await;
                }
            }));
        }

        let l = Arc::clone(&limiter);
        let reloader = tokio::spawn(async move {
            let targets = [4u32, 12, 6, 16, 2, 10];
            for _ in 0..6 {
                for &t in &targets {
                    let mut cfg = permissive_security();
                    cfg.max_concurrent_handlers = t;
                    l.reload(&cfg);
                    tokio::task::yield_now().await;
                }
            }
        });
        reloader.await.unwrap();
        stop.store(true, std::sync::atomic::Ordering::Relaxed);
        for h in acquirers {
            h.await.unwrap();
        }

        // Drain pending shrink tasks before the final convergence
        // reload — same pattern as `concurrent_reloads_converge`.
        for _ in 0..64 {
            tokio::task::yield_now().await;
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
        let mut cfg = permissive_security();
        cfg.max_concurrent_handlers = 5;
        limiter.reload(&cfg);
        for _ in 0..64 {
            tokio::task::yield_now().await;
            tokio::time::sleep(Duration::from_millis(2)).await;
        }

        // Operator-observable convergence proof: hold 5 permits; the
        // 6th must reject under cap=5. If the racing acquires had
        // poisoned the limiter's permit accounting, the 6th would
        // either succeed (over-allocated) or this test would panic.
        let mut held = Vec::with_capacity(5);
        for i in 0..5u8 {
            held.push(
                limiter
                    .acquire_inner([i; 32], Some(ip(192, 168, 1, i)))
                    .expect("5 acquires under cap=5"),
            );
        }
        assert!(
            limiter
                .acquire_inner([99u8; 32], Some(ip(192, 168, 1, 99)))
                .is_err(),
            "6th acquire must reject at cap=5 after acquire+reload storm converges"
        );
    }
}
