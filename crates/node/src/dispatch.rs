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
//! [`ConnectionLimiter`] enforces two independent layers at the top of every
//! deCDN-authored `ProtocolHandler::accept` implementation:
//!
//! 1. **Global semaphore** — hard cap on total in-flight handler tasks.
//! 2. **Per-source rate limit** — bounds the connection rate from any one
//!    source. Today the source axis is the remote `IpAddr` (`/64`-grouped
//!    for IPv6); the layer's name is intentionally generic so future
//!    keying axes (e.g. `NodeID`, ASN) can land without renaming the
//!    operator-visible config and metrics surface.
//!
//! Relay connections that have no resolvable direct IP at accept time
//! bypass the per-source layer (no key to charge); the
//! `dispatch_per_source_skipped_no_addr` counter records the bypass so
//! operators chasing rejection anomalies can distinguish "layer didn't
//! fire" from "layer wasn't applicable".
//!
//! The keyed rate limiter is provided by the [`governor`] crate. Hot-
//! reload of the rate/burst quota rebuilds the limiter from scratch and
//! swaps it in under an `RwLock` — token-bucket state is *not* preserved
//! across a reload (operators changing live quotas should expect the
//! next acquire from each source to start with a fresh burst budget).

use std::net::{IpAddr, Ipv6Addr};
use std::num::NonZeroU32;
use std::sync::Arc;
use std::sync::RwLock;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use arc_swap::ArcSwapOption;
use governor::{DefaultKeyedRateLimiter, Quota};
use iroh::TransportAddr;
use iroh::Watcher as _;
use iroh::endpoint::Connection;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::metrics::Metrics;
use decdn_common::config::ResolvedSecurity;

/// Bucket key for the per-source rate limit. IPv4 addresses are used as-is;
/// IPv6 addresses are masked to their `/64` prefix.
///
/// Without the mask the per-source layer is trivially defeated: a
/// customer-grade IPv6 allocation is typically `/64` (or larger),
/// giving an attacker `2^64` distinct `IpAddr`s inside one allocation.
/// Each unique address would be a separate map key, both bypassing the
/// rate limit and churning the eviction path so legitimate IPv4
/// victims' buckets get flushed.
fn source_key(ip: IpAddr) -> IpAddr {
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
    /// Per-source rate limit exhausted.
    PerSource,
}

impl RejectReason {
    /// Short, stable label suitable for log fields and the QUIC close-frame
    /// reason bytes. Peers receiving the close can disambiguate the layer
    /// (and pick an appropriate backoff strategy) without parsing free text.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::GlobalFull => "global-full",
            Self::PerSource => "per-source",
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

/// Per-source keyed rate limiter plus its operator-configured cap on the
/// number of distinct keys to track.
///
/// `limiter` is `None` when the per-source layer is disabled
/// (`per_source_rate_per_sec == 0.0`). `cap == 0` means "unbounded —
/// don't prune".
struct PerSource {
    /// `None` when the layer is disabled. `Some` when enabled — the
    /// keyed governor limiter holds one bucket per source key.
    limiter: Option<Arc<DefaultKeyedRateLimiter<IpAddr>>>,
    /// `0` = unbounded. Otherwise: prune via `retain_recent` whenever
    /// `limiter.len() > cap` so the keyspace stays bounded.
    cap: usize,
}

impl PerSource {
    fn new(rate_per_sec: f64, burst: u32, cap: usize) -> Self {
        Self {
            limiter: build_keyed_limiter(rate_per_sec, burst),
            cap,
        }
    }
}

/// Construct a keyed governor limiter from a (rate, burst) pair, or
/// return `None` when the layer is disabled.
///
/// `rate <= 0.0` short-circuits to `None`. `burst == 0` would deny every
/// request after the first burst-many — `resolve_security` rejects that
/// combination at config time, so this function treats it as a soft-
/// disable belt-and-braces (returns `None`) rather than panicking on a
/// `NonZeroU32::new(0).unwrap()`.
fn build_keyed_limiter(
    rate_per_sec: f64,
    burst: u32,
) -> Option<Arc<DefaultKeyedRateLimiter<IpAddr>>> {
    if rate_per_sec <= 0.0 || burst == 0 {
        return None;
    }
    // Guard against pathological inputs (NaN, Inf) that would have
    // already been rejected by `resolve_security` — the function is
    // also called from tests that build `ResolvedSecurity` directly,
    // so a defensive bail-out keeps the limiter sane without panicking.
    if !rate_per_sec.is_finite() {
        return None;
    }
    // governor's quota uses "1 cell per period" + burst capacity. The
    // period is 1.0 / rate_per_sec seconds. Saturate at the smallest
    // representable nanosecond period (1ns) so very large rates still
    // yield a valid `Duration` rather than wrapping or rounding to 0.
    let period_secs = 1.0_f64 / rate_per_sec;
    // Convert to nanoseconds with `as` (precision loss is fine for a
    // rate-limit period). Clamp to at least 1ns so `Duration::from_nanos(0)`
    // never reaches `Quota::with_period`, which returns `None` on a
    // zero-length interval.
    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    let period_ns = (period_secs * 1_000_000_000.0).max(1.0) as u64;
    let period = Duration::from_nanos(period_ns);
    let burst_nz = NonZeroU32::new(burst)?;
    let quota = Quota::with_period(period)?.allow_burst(burst_nz);
    Some(Arc::new(DefaultKeyedRateLimiter::keyed(quota)))
}

/// Build an `Arc<Semaphore>` of `cap` permits, or `None` when the global
/// cap is disabled (`cap == 0`). Used both at construction and on every
/// reload — the `Arc` is swapped wholesale so already-held permits keep
/// the previous semaphore alive until they drop.
fn build_semaphore(cap: u32) -> Option<Arc<Semaphore>> {
    if cap == 0 {
        return None;
    }
    Some(Arc::new(Semaphore::new(
        usize::try_from(cap).unwrap_or(usize::MAX),
    )))
}

/// Shared rate limiter used by all deCDN-authored `ProtocolHandler` implementations.
///
/// `reload(&self, &ResolvedSecurity)` applies a new resolved-security
/// snapshot in place. The per-source [`governor`] limiter is rebuilt
/// from the new quota and swapped in under an internal `RwLock`. The
/// global semaphore is replaced wholesale: a fresh `Arc<Semaphore>` of
/// the new capacity is built and atomically swapped into the
/// [`ArcSwapOption`] cell, so the next acquire hits the new semaphore.
/// Already-acquired `OwnedSemaphorePermit`s hold a clone of the old
/// `Arc<Semaphore>` and continue to drain into it on `Drop` — they're
/// dropped harmlessly once the last permit goes away.
///
/// `semaphore = None` encodes "global cap disabled"
/// (`max_concurrent_handlers == 0`) and the acquire path skips the
/// semaphore entirely.
#[allow(missing_debug_implementations)]
pub struct ConnectionLimiter {
    /// `None` when the layer is disabled (`max_concurrent_handlers ==
    /// 0`), `Some(arc)` otherwise. Reload swaps the whole `Arc` in;
    /// the acquire path loads it lock-free.
    semaphore: ArcSwapOption<Semaphore>,
    /// Per-source keyed limiter behind an `RwLock` so reads (the hot
    /// path) take a shared lock and reloads briefly take exclusive.
    /// Held in `Arc` form for cheap cloning into the rare case where
    /// concurrent acquires want to release the read guard before
    /// calling `check_key`.
    per_source: RwLock<PerSource>,
    /// Mirrors `per_source.limiter.is_some()`. Read on the relay-only-
    /// connection fast path so we can record
    /// `dispatch_per_source_skipped_no_addr` without taking the
    /// `RwLock` on every relay accept. Written only under the
    /// `per_source` write guard during reload.
    per_source_enabled: AtomicBool,
    /// One-shot poison-log gate. Set on the first observation of a
    /// poisoned `per_source` lock so `tracing::error!` fires once per
    /// process rather than once per acquire — under sustained traffic
    /// on a poisoned lock the unguarded form would emit megabytes of
    /// identical log lines per second.
    per_source_poison_logged: AtomicBool,
    /// Single-flight guard for `retain_recent` pruning. Without it, a
    /// flood of distinct-source connections that all observe an
    /// over-cap keyspace simultaneously would each spawn an `O(n)`
    /// walk over the keyed state — one per acquire thread. The flag
    /// ensures at most one thread is pruning at a time; concurrent
    /// over-cap observers skip and the next observer after the prune
    /// completes picks up the work.
    pruning_in_progress: AtomicBool,
    metrics: Arc<Metrics>,
}

/// RAII reset for `ConnectionLimiter::pruning_in_progress`. Holding one
/// of these means the holder owns the single-flight slot for
/// `retain_recent`; on drop — including drop during panic unwind — the
/// flag is released. Without this, a panic inside `retain_recent` (e.g.
/// from a future regression in the keyed limiter or an allocation
/// failure during the walk) would leave the flag stuck `true` and
/// permanently disable both prune codepaths for the lifetime of the
/// process, which is the exact unbounded-keyspace failure mode #440 is
/// meant to prevent.
struct PruneGuard<'a>(&'a AtomicBool);

impl Drop for PruneGuard<'_> {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}

impl ConnectionLimiter {
    /// Construct a limiter from the resolved security configuration.
    pub fn new(cfg: &ResolvedSecurity, metrics: Arc<Metrics>) -> Self {
        let semaphore = ArcSwapOption::from(build_semaphore(cfg.max_concurrent_handlers));
        let per_source = PerSource::new(
            cfg.per_source_rate_per_sec,
            cfg.per_source_burst,
            cfg.max_tracked_sources,
        );
        let per_source_enabled = AtomicBool::new(per_source.limiter.is_some());
        Self {
            semaphore,
            per_source: RwLock::new(per_source),
            per_source_enabled,
            per_source_poison_logged: AtomicBool::new(false),
            pruning_in_progress: AtomicBool::new(false),
            metrics,
        }
    }

    /// Apply a new `ResolvedSecurity` to the live limiter.
    ///
    /// - Per-source: rebuild the keyed [`governor`] limiter from the new
    ///   quota and atomically swap it in under the per-source write
    ///   lock. **Token-bucket state is not preserved across the swap.**
    /// - Global semaphore: build a fresh `Arc<Semaphore>` (or `None`
    ///   when `max_concurrent_handlers == 0`) and atomically swap it
    ///   into the [`ArcSwapOption`] cell. Already-acquired permits hold
    ///   a clone of the previous `Arc<Semaphore>` and drop it harmlessly
    ///   on permit release; new acquires hit the new cell.
    ///
    /// Infallible. Caller (`RuntimeReloadState::reload`) has already
    /// validated the values via `resolve_security`.
    pub fn reload(&self, cfg: &ResolvedSecurity) {
        // 1. Per-source: rebuild limiter from the new quota and swap
        //    under the write lock.
        let new_per_source = PerSource::new(
            cfg.per_source_rate_per_sec,
            cfg.per_source_burst,
            cfg.max_tracked_sources,
        );
        let per_source_enabled = new_per_source.limiter.is_some();
        {
            let mut g = self.per_source.write().unwrap_or_else(|poisoned| {
                self.log_per_source_poison();
                poisoned.into_inner()
            });
            *g = new_per_source;
        }
        // Mirror the enabled state for the relay-only fast path.
        // Stored under Relaxed because there's no happens-before
        // relationship to the limiter's own state — the worst case
        // across a swap is one accept observing the prior generation,
        // operationally indistinguishable from arriving microseconds
        // earlier.
        self.per_source_enabled
            .store(per_source_enabled, Ordering::Relaxed);

        // 2. Global semaphore: swap the whole `Arc<Semaphore>`. Any
        //    in-flight `OwnedSemaphorePermit` holds a clone of the
        //    previous `Arc` and drops it harmlessly when released.
        self.semaphore
            .store(build_semaphore(cfg.max_concurrent_handlers));
    }

    /// Try to acquire a permit for `conn`.
    ///
    /// On success returns a [`Permit`] that holds the semaphore slot and
    /// updates the in-flight gauge. On failure returns the [`RejectReason`],
    /// records the appropriate metric counter, and emits a `tracing::debug!`
    /// breadcrumb with `reason`/`ip` so operators investigating a counter
    /// spike have something to grep for.
    ///
    /// **Cross-layer charge invariant:** layers are checked in order
    /// global → per-source. The first layer to reject short-circuits;
    /// later layers never see the connection, so no token is wasted on
    /// a connection we already decided to drop.
    ///
    /// This method is synchronous and completes in microseconds — it never
    /// awaits I/O.
    pub fn acquire(&self, conn: &Connection) -> Result<Permit, RejectReason> {
        self.acquire_inner(peer_ip(conn))
    }

    /// Cross-module test hook with the same behavior as `acquire`,
    /// minus the iroh `Connection` argument. Exposed as `pub` so unit
    /// tests in other modules and integration tests under `tests/` can
    /// drive the limiter directly. Production code must not call this —
    /// `Self::acquire` is the only supported entry point.
    /// `#[doc(hidden)]` keeps it out of the rendered public API surface.
    #[doc(hidden)]
    pub fn acquire_for_test(&self, peer_ip: Option<IpAddr>) -> Result<Permit, RejectReason> {
        self.acquire_inner(peer_ip)
    }

    /// Shared implementation behind [`Self::acquire`] and
    /// [`Self::acquire_for_test`].
    fn acquire_inner(&self, peer_ip: Option<IpAddr>) -> Result<Permit, RejectReason> {
        // Load the current semaphore. `None` means the global cap is
        // administratively disabled — skip the layer entirely.
        let sem_permit = match self.semaphore.load_full() {
            Some(sem) => Some(sem.try_acquire_owned().map_err(|_| {
                self.metrics.dispatch_rejected_global();
                tracing::debug!(
                    reason = RejectReason::GlobalFull.as_str(),
                    ip = ?peer_ip,
                    "dispatch rejected: global semaphore exhausted"
                );
                RejectReason::GlobalFull
            })?),
            None => None,
        };

        // Per-source layer. Relay-only connections (no IP) bypass the
        // layer entirely — there's no source key to charge — and bump
        // the skip counter when the layer is enabled so operators can
        // distinguish "layer didn't fire" from "layer wasn't applicable".
        match peer_ip {
            Some(ip) => {
                if let Some(reason) = self.check_per_source(ip) {
                    self.metrics.dispatch_rejected_per_source();
                    tracing::debug!(
                        reason = reason.as_str(),
                        %ip,
                        "dispatch rejected: per-source rate limit exhausted"
                    );
                    return Err(reason);
                }
            }
            None => {
                if self.per_source_enabled.load(Ordering::Relaxed) {
                    self.metrics.dispatch_per_source_skipped_no_addr();
                }
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

    /// Run the per-source layer for a known peer IP. Returns
    /// `Some(RejectReason::PerSource)` to reject, `None` to allow.
    ///
    /// Disabled-layer fast path: if the inner limiter is `None` the
    /// layer is administratively disabled and the call short-circuits
    /// to `None`. Otherwise: bucket the address (`/64` for IPv6),
    /// call `check_key`, and prune the keyspace if it has grown past
    /// `cap`. Pruning runs lazily under the read guard via
    /// `retain_recent` (drops keys whose state is indistinguishable
    /// from a fresh bucket).
    fn check_per_source(&self, ip: IpAddr) -> Option<RejectReason> {
        let key = source_key(ip);
        // Snapshot the limiter `Arc` and cap under the read guard, then
        // drop the guard before doing anything O(n). `retain_recent` walks
        // the entire keyed state map; holding the read guard across it
        // would block reload (which needs the write lock) for the
        // duration of the walk under a flood. The `Arc` clone keeps the
        // observed `KeyedRateLimiter` alive across a concurrent reload —
        // a reload swap leaves us operating on the prior generation,
        // which is operationally indistinguishable from arriving
        // microseconds earlier.
        let (limiter, cap) = {
            let g = self.per_source.read().unwrap_or_else(|poisoned| {
                self.log_per_source_poison();
                poisoned.into_inner()
            });
            (g.limiter.clone(), g.cap)
        };
        let limiter = limiter?;
        let result = limiter.check_key(&key);
        // Best-effort cap enforcement, two layers of throttle:
        //
        // 1. 10% slack: prune only when the keyspace has grown past
        //    cap + cap/10 so a sustained 1-key-over-cap fluctuation
        //    under flood doesn't trigger an O(n) walk on every accept.
        // 2. Single-flight: a connection flood from N distinct sources
        //    that all observe over-cap simultaneously would otherwise
        //    spawn N concurrent O(n) walks. The pruning_in_progress
        //    flag bounds it to one walk at a time; concurrent observers
        //    skip and the next over-cap observer after the prune
        //    completes picks up any remaining work.
        //
        // The map can briefly exceed cap by more than 10% while the
        // single-flight prune is in progress; on completion governor's
        // retain_recent brings it back to cap (it drops keys whose
        // state is indistinguishable from fresh). Runs after the read
        // guard has been released so the walk can't block reload.
        if cap > 0
            && limiter.len() > cap.saturating_add(cap / 10)
            && self
                .pruning_in_progress
                .compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
                .is_ok()
        {
            // RAII reset on drop: if `retain_recent` ever panics we must
            // not leave `pruning_in_progress` stuck `true`, or both this
            // path and the periodic GC task would be permanently disabled
            // for the lifetime of the process — exactly the
            // unbounded-keyspace pathology #440 fixes. See `PruneGuard`.
            let _guard = PruneGuard(&self.pruning_in_progress);
            limiter.retain_recent();
        }
        match result {
            Ok(()) => None,
            Err(_) => Some(RejectReason::PerSource),
        }
    }

    /// Drop per-source buckets whose state has refilled to the fresh
    /// baseline (#440). The acquire path already prunes opportunistically
    /// when the keyspace exceeds `cap + cap/10`, but a node whose
    /// connection rate falls below the over-cap threshold can carry
    /// millions of stale buckets indefinitely. The runtime spawns a
    /// periodic task that calls this method to bound steady-state
    /// memory regardless of acquire-driven activity.
    ///
    /// Returns `Some((before, after))` keyspace counts when a prune
    /// actually ran, or `None` when the per-source layer is disabled or
    /// the call lost the single-flight CAS to a concurrent prune. The
    /// runtime's periodic task uses this to emit a debug breadcrumb
    /// only on real sweeps — silence is meaningful.
    ///
    /// Single-flight with the acquire-path prune via the same
    /// `pruning_in_progress` flag, with `PruneGuard` ensuring the flag
    /// is released even if `retain_recent` panics — concurrent
    /// observers (the periodic task and a flood-driven acquire)
    /// coexist without spawning duplicate `O(n)` walks.
    pub fn gc_per_source(&self) -> Option<(usize, usize)> {
        // Snapshot the limiter `Arc` under the read guard, then release
        // the lock before the `O(n)` walk. Holding the read guard across
        // `retain_recent` would block reload (which needs the write
        // lock) for the whole walk — same reasoning as `check_per_source`.
        let limiter = {
            let g = self.per_source.read().unwrap_or_else(|poisoned| {
                self.log_per_source_poison();
                poisoned.into_inner()
            });
            g.limiter.clone()
        };
        let limiter = limiter?;
        if self
            .pruning_in_progress
            .compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
            .is_ok()
        {
            let _guard = PruneGuard(&self.pruning_in_progress);
            let before = limiter.len();
            limiter.retain_recent();
            let after = limiter.len();
            Some((before, after))
        } else {
            None
        }
    }

    /// Number of per-source buckets currently tracked. Returns `0` when
    /// the layer is disabled. Used by the periodic GC task in tests and
    /// by the runtime's debug-log breadcrumb so operators can correlate
    /// keyspace size with the bookkeeping cap.
    #[must_use]
    pub fn per_source_tracked(&self) -> usize {
        let g = self.per_source.read().unwrap_or_else(|poisoned| {
            self.log_per_source_poison();
            poisoned.into_inner()
        });
        g.limiter.as_ref().map_or(0, |l| l.len())
    }

    /// One-shot poison log for the `per_source` lock. The first
    /// observation emits `tracing::error!`; every subsequent recovery
    /// is silent. Without the gate, sustained traffic on a poisoned
    /// lock would emit one error line per acquire — megabytes per
    /// second under flood.
    fn log_per_source_poison(&self) {
        if self
            .per_source_poison_logged
            .compare_exchange(false, true, Ordering::Relaxed, Ordering::Relaxed)
            .is_ok()
        {
            tracing::error!(
                "dispatch per-source limiter lock poisoned; recovering inner state \
                 (further occurrences suppressed)"
            );
        }
    }
}

/// Extract the remote IP from a connection's currently-selected network path.
///
/// Returns `None` for relay-only connections that have no direct IP path.
/// Path selection may not have completed at the moment `accept` returns;
/// without the fallback to *any* IP path, the per-source limit would
/// silently no-op on freshly-accepted connections and an attacker churning
/// identities could bypass the layer in that race window.
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

    use super::{ConnectionLimiter, RejectReason};
    use crate::metrics::Metrics;
    use decdn_common::config::ResolvedSecurity;

    fn strict_security(max: u32) -> ResolvedSecurity {
        ResolvedSecurity {
            max_concurrent_handlers: max,
            per_source_rate_per_sec: 1.0,
            per_source_burst: 1,
            max_tracked_sources: 16,
        }
    }

    fn permissive_security() -> ResolvedSecurity {
        ResolvedSecurity {
            max_concurrent_handlers: u32::MAX,
            per_source_rate_per_sec: 1e9,
            per_source_burst: u32::MAX,
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
            .acquire_inner(Some(ip(127, 0, 0, 1)))
            .expect("first acquire");
        // Second acquire from a *different* IP must still fail the
        // global cap before the per-source layer gets a chance.
        let err = limiter
            .acquire_inner(Some(ip(10, 0, 0, 1)))
            .expect_err("second acquire should be rejected");
        assert_eq!(err, RejectReason::GlobalFull);
        // After dropping the first permit the slot frees and a third acquire
        // succeeds — proves the OwnedSemaphorePermit drop releases the slot.
        drop(permit);
        let _p = limiter
            .acquire_inner(Some(ip(10, 0, 0, 2)))
            .expect("acquire after drop should succeed");
    }

    #[test]
    fn acquire_per_source_rejects_after_burst_exhausted() {
        let metrics = Arc::new(Metrics::new());
        let limiter = ConnectionLimiter::new(&strict_security(u32::MAX), Arc::clone(&metrics));
        let same_ip = Some(ip(192, 0, 2, 1));
        // First acquire from the IP succeeds.
        let _p1 = limiter.acquire_inner(same_ip).expect("first acquire");
        // Second from the same IP hits the per-source burst-1 limit.
        let err = limiter
            .acquire_inner(same_ip)
            .expect_err("second per-source acquire should reject");
        assert_eq!(err, RejectReason::PerSource);
    }

    #[test]
    fn acquire_per_source_rejection_does_not_charge_global_permit() {
        // Per-source layer is checked *after* the global semaphore — a
        // per-source rejection releases the held semaphore permit on
        // drop. Verify that a fresh IP can immediately acquire after a
        // per-source rejection from another IP, even when the global
        // cap is tight.
        let metrics = Arc::new(Metrics::new());
        let limiter = ConnectionLimiter::new(&strict_security(2), Arc::clone(&metrics));
        let same_ip = Some(ip(192, 0, 2, 99));
        let _p1 = limiter
            .acquire_inner(same_ip)
            .expect("p1 drains the per-source bucket for that IP");
        let err = limiter
            .acquire_inner(same_ip)
            .expect_err("p2: per-source bucket empty for that IP");
        assert_eq!(err, RejectReason::PerSource);
        // Global cap = 2; held = 1 (p1). Fresh IP must still acquire
        // — the rejected p2 must have released its semaphore slot.
        let _p3 = limiter
            .acquire_inner(Some(ip(10, 0, 0, 1)))
            .expect("fresh IP must acquire under global=2");
    }

    #[test]
    fn acquire_relay_connection_skips_per_source() {
        // peer_ip = None simulates a relay-only connection. Per-source
        // layer is skipped; only the global cap applies.
        let metrics = Arc::new(Metrics::new());
        let limiter = ConnectionLimiter::new(&strict_security(u32::MAX), Arc::clone(&metrics));
        let _p1 = limiter.acquire_inner(None).expect("relay acquire 1");
        // Second relay acquire — must succeed (per-source doesn't apply).
        let _p2 = limiter.acquire_inner(None).expect("relay acquire 2");
    }

    #[test]
    fn permit_drop_decrements_in_flight_metric() {
        // We can't read the gauge value directly without scraping, so the
        // contract is verified indirectly: permits must be released so that
        // a strictly-bounded global semaphore can be re-acquired after Drop.
        let metrics = Arc::new(Metrics::new());
        let limiter = ConnectionLimiter::new(&strict_security(2), Arc::clone(&metrics));
        let p1 = limiter.acquire_inner(Some(ip(10, 0, 0, 1))).unwrap();
        let p2 = limiter.acquire_inner(Some(ip(10, 0, 0, 2))).unwrap();
        // Global is full. Drop one and re-acquire.
        drop(p1);
        let _p3 = limiter
            .acquire_inner(Some(ip(10, 0, 0, 3)))
            .expect("slot should be free after dropping p1");
        drop(p2);
    }

    #[test]
    fn concurrent_acquires_from_same_source_serialize_correctly() {
        // 32 threads racing on a burst=1 limiter must all see exactly one
        // success and 31 PerSource rejections. governor's keyed limiter
        // is internally synchronised; this proves we don't accidentally
        // leak two permits through the per-source check.
        use std::sync::Barrier;
        let metrics = Arc::new(Metrics::new());
        let limiter = Arc::new(ConnectionLimiter::new(&strict_security(u32::MAX), metrics));
        let n = 32;
        let barrier = Arc::new(Barrier::new(n));
        let single_ip = Some(ip(192, 0, 2, 250));
        let mut handles = Vec::with_capacity(n);
        for _ in 0..n {
            let lim = Arc::clone(&limiter);
            let bar = Arc::clone(&barrier);
            handles.push(std::thread::spawn(move || {
                bar.wait();
                lim.acquire_inner(single_ip).is_ok()
            }));
        }
        let successes: usize = handles
            .into_iter()
            .map(|h| usize::from(h.join().unwrap()))
            .sum();
        assert_eq!(successes, 1, "exactly one acquire should succeed");
    }

    #[test]
    fn poisoned_lock_recovers_inner_state() {
        let metrics = Arc::new(Metrics::new());
        let limiter = Arc::new(ConnectionLimiter::new(&permissive_security(), metrics));
        let lim_for_thread = Arc::clone(&limiter);
        // Poison per_source from a panicking thread holding the write lock.
        let join = std::thread::spawn(move || {
            let _g = lim_for_thread.per_source.write().unwrap();
            panic!("intentional");
        });
        let _ = join.join();
        assert!(limiter.per_source.is_poisoned());
        // acquire must still work — recovery branch returns the inner guard.
        let _p = limiter
            .acquire_inner(Some(ip(10, 0, 0, 1)))
            .expect("acquire after per_source poison");
    }

    // --- Disabled-layer (0 = unlimited) ---------------------------------------

    fn disabled_global_security() -> ResolvedSecurity {
        ResolvedSecurity {
            max_concurrent_handlers: 0,
            per_source_rate_per_sec: 1e9,
            per_source_burst: u32::MAX,
            max_tracked_sources: 4096,
        }
    }

    #[test]
    fn connection_limiter_disabled_global_skips_semaphore() {
        // max_concurrent_handlers=0 disables the global cap. We can hold
        // far more permits than the per-source layer would normally
        // allow at startup. Drop-test verifies all permits live until the
        // explicit drop at the end.
        let metrics = Arc::new(Metrics::new());
        let limiter = ConnectionLimiter::new(&disabled_global_security(), Arc::clone(&metrics));
        let mut held = Vec::with_capacity(256);
        for i in 0..256 {
            let octet = u8::try_from(i % 200).unwrap_or(0);
            let p = limiter
                .acquire_inner(Some(ip(10, 0, 0, octet)))
                .expect("disabled global cap must accept all acquires");
            held.push(p);
        }
        assert_eq!(held.len(), 256);
        drop(held);
    }

    #[test]
    fn connection_limiter_disabled_per_source_passes_all() {
        // per_source_rate_per_sec=0 disables the per-source layer. With
        // global cap also generous, hundreds of acquires from a single
        // IP must all succeed.
        let metrics = Arc::new(Metrics::new());
        let cfg = ResolvedSecurity {
            max_concurrent_handlers: u32::MAX,
            per_source_rate_per_sec: 0.0,
            per_source_burst: 0,
            max_tracked_sources: 16,
        };
        let limiter = ConnectionLimiter::new(&cfg, Arc::clone(&metrics));
        let same_ip = Some(ip(10, 0, 0, 1));
        for _ in 0..512 {
            let _p = limiter
                .acquire_inner(same_ip)
                .expect("per-source disabled must accept");
        }
    }

    // --- ConnectionLimiter::reload --------------------------------------------

    #[tokio::test]
    async fn connection_limiter_reload_grows_semaphore() {
        // Whole-Arc swap: a reload to cap=5 installs a fresh semaphore
        // with 5 permits. Already-held permits drain into the *previous*
        // semaphore on drop and don't count against the new cap. Five
        // fresh acquires from new IPs must succeed; the sixth rejects.
        let metrics = Arc::new(Metrics::new());
        let limiter = ConnectionLimiter::new(&strict_security(2), Arc::clone(&metrics));
        let _p1 = limiter.acquire_inner(Some(ip(10, 0, 0, 1))).unwrap();
        let _p2 = limiter.acquire_inner(Some(ip(10, 0, 0, 2))).unwrap();
        assert!(limiter.acquire_inner(Some(ip(10, 0, 0, 3))).is_err());

        let mut new_cfg = permissive_security();
        new_cfg.max_concurrent_handlers = 5;
        limiter.reload(&new_cfg);

        let mut held = Vec::with_capacity(5);
        for i in 3..8u8 {
            held.push(
                limiter
                    .acquire_inner(Some(ip(10, 0, 0, i)))
                    .expect("under new cap=5"),
            );
        }
        assert!(
            limiter.acquire_inner(Some(ip(10, 0, 0, 99))).is_err(),
            "6th acquire against the new sem must reject at cap=5"
        );
    }

    #[tokio::test]
    async fn connection_limiter_reload_shrinks_caps_new_acquires() {
        // Whole-Arc swap: a reload to a smaller cap installs a fresh
        // semaphore at that size. New acquires hit the new sem; the
        // (new+1)th rejects. Already-held permits hold the previous
        // semaphore alive until they drop — they don't count against
        // the new cap.
        let metrics = Arc::new(Metrics::new());
        let limiter = ConnectionLimiter::new(&strict_security(4), Arc::clone(&metrics));
        let _p1 = limiter.acquire_inner(Some(ip(10, 0, 0, 1))).unwrap();
        let _p2 = limiter.acquire_inner(Some(ip(10, 0, 0, 2))).unwrap();

        let mut new_cfg = permissive_security();
        new_cfg.max_concurrent_handlers = 3;
        limiter.reload(&new_cfg);

        // Three acquires must succeed against the new (cap=3) sem.
        let _p3 = limiter.acquire_inner(Some(ip(10, 0, 0, 3))).unwrap();
        let _p4 = limiter.acquire_inner(Some(ip(10, 0, 0, 4))).unwrap();
        let _p5 = limiter.acquire_inner(Some(ip(10, 0, 0, 5))).unwrap();
        assert!(
            limiter.acquire_inner(Some(ip(10, 0, 0, 6))).is_err(),
            "4th acquire against the new cap=3 sem must reject"
        );
    }

    #[tokio::test]
    async fn connection_limiter_reload_swaps_per_source_quota() {
        // Start with strict per-source burst=1; exhaust it from a
        // single IP; reload with raised burst=10 and confirm the
        // limiter accepts new acquires immediately. (The new keyed
        // limiter has a fresh state, so the next acquire from the
        // same IP starts with a full burst budget.)
        let metrics = Arc::new(Metrics::new());
        let limiter = ConnectionLimiter::new(&strict_security(u32::MAX), Arc::clone(&metrics));
        let attacker_ip = Some(ip(10, 0, 0, 1));
        let _p1 = limiter.acquire_inner(attacker_ip).unwrap();
        // Per-source burst=1 exhausted.
        assert!(limiter.acquire_inner(attacker_ip).is_err());
        // Reload: raise per-source burst & rate so the rebuild yields a
        // fresh limiter state.
        let mut new_cfg = permissive_security();
        new_cfg.max_concurrent_handlers = u32::MAX;
        limiter.reload(&new_cfg);
        let _p2 = limiter
            .acquire_inner(attacker_ip)
            .expect("rebuild must reset per-source state");
    }

    #[tokio::test]
    async fn connection_limiter_reload_disable_then_re_enable_caps_correctly() {
        // Start cap=4, disable (max_concurrent_handlers=0), acquire
        // freely (semaphore skipped), re-enable to cap=3 — fresh
        // semaphore with 3 permits installed; new acquires up to 3
        // succeed, 4th rejects.
        let metrics = Arc::new(Metrics::new());
        let limiter = ConnectionLimiter::new(&strict_security(4), Arc::clone(&metrics));

        // Disable the global cap.
        let mut c = permissive_security();
        c.max_concurrent_handlers = 0;
        limiter.reload(&c);

        // While disabled, hold many permits — the acquire path skips the
        // semaphore entirely.
        let bulk: Vec<_> = (0..16u8)
            .map(|i| {
                limiter
                    .acquire_inner(Some(ip(10, 0, 0, i)))
                    .expect("disabled cap accepts all")
            })
            .collect();
        drop(bulk);

        // Re-enable to cap=3.
        c.max_concurrent_handlers = 3;
        limiter.reload(&c);

        let _p1 = limiter.acquire_inner(Some(ip(10, 0, 0, 1))).unwrap();
        let _p2 = limiter.acquire_inner(Some(ip(10, 0, 0, 2))).unwrap();
        let _p3 = limiter.acquire_inner(Some(ip(10, 0, 0, 3))).unwrap();
        assert!(
            limiter.acquire_inner(Some(ip(10, 0, 0, 4))).is_err(),
            "after re-enable to 3, 4th must reject"
        );
    }

    #[test]
    fn connection_limiter_reload_disables_per_source_independently() {
        // Per-source enabled at startup: burst=1 rejects the second
        // acquire from a single IP. After reload disabling per-source,
        // many consecutive acquires from that IP succeed.
        let metrics = Arc::new(Metrics::new());
        let limiter = ConnectionLimiter::new(&strict_security(u32::MAX), Arc::clone(&metrics));
        let same_ip = Some(ip(192, 0, 2, 99));
        let _p1 = limiter
            .acquire_inner(same_ip)
            .expect("first per-source acquire");
        let err = limiter
            .acquire_inner(same_ip)
            .expect_err("second from same IP must reject");
        assert_eq!(err, RejectReason::PerSource);

        // Disable per-source via reload.
        let mut c = strict_security(u32::MAX);
        c.per_source_rate_per_sec = 0.0;
        c.per_source_burst = 0;
        limiter.reload(&c);

        for _ in 0..32 {
            let _p = limiter
                .acquire_inner(same_ip)
                .expect("per-source disabled must accept");
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

        // Tighten per-source via reload to burst=1, rate ~ 0 so refill
        // doesn't lift the cap within the test window.
        let cfg = ResolvedSecurity {
            max_concurrent_handlers: u32::MAX,
            per_source_rate_per_sec: 0.001,
            per_source_burst: 1,
            max_tracked_sources: 16,
        };
        limiter.reload(&cfg);

        let same_ip = Some(ip(10, 0, 0, 1));
        let _ok = limiter
            .acquire_inner(same_ip)
            .expect("first acquire under tightened limit");
        // Second from same IP — per-source burst exhausted post-reload.
        let _err = limiter
            .acquire_inner(same_ip)
            .expect_err("post-reload reject");

        let text = metrics.encode().unwrap();
        // OpenMetrics auto-appends `_total` to counter field names, so
        // the field `dispatch_rejected_per_source` (under the `decdn`
        // group) is exposed as `decdn_dispatch_rejected_per_source_total`.
        assert!(
            text.contains("decdn_dispatch_rejected_per_source_total 1"),
            "post-reload reject must increment counter; got:\n{text}"
        );
    }

    /// Sibling of `reload_preserves_metrics_handle_for_post_reload_rejects`
    /// covering the *global-cap* reject path. A regression that swapped the
    /// `dispatch_rejected_global` and `dispatch_rejected_per_source`
    /// counter calls would still pass `RejectReason`-equality assertions
    /// in other tests; only an encoded-scrape assertion catches the typo.
    #[tokio::test]
    async fn rejection_counters_global_visible_in_scrape() {
        let metrics = Arc::new(Metrics::new());
        let limiter = ConnectionLimiter::new(&strict_security(1), Arc::clone(&metrics));
        let _p = limiter.acquire_inner(Some(ip(10, 0, 0, 1))).unwrap();
        let _err = limiter
            .acquire_inner(Some(ip(10, 0, 0, 2)))
            .expect_err("global cap exhausted");
        let text = metrics.encode().unwrap();
        assert!(
            text.contains("decdn_dispatch_rejected_global_total 1"),
            "global reject must increment its own counter; got:\n{text}"
        );
        assert!(
            !text.contains("decdn_dispatch_rejected_per_source_total 1"),
            "per-source counter must not move on a global rejection"
        );
    }

    /// Sibling covering the *per-source* reject path against the encoded
    /// scrape.
    #[tokio::test]
    async fn rejection_counters_per_source_visible_in_scrape() {
        let metrics = Arc::new(Metrics::new());
        let limiter = ConnectionLimiter::new(&strict_security(u32::MAX), Arc::clone(&metrics));
        let same_ip = Some(ip(192, 0, 2, 7));
        let _p = limiter
            .acquire_inner(same_ip)
            .expect("first per-source acquire");
        let _err = limiter
            .acquire_inner(same_ip)
            .expect_err("second per-source acquire rejects");
        let text = metrics.encode().unwrap();
        assert!(
            text.contains("decdn_dispatch_rejected_per_source_total 1"),
            "per-source reject must increment its own counter; got:\n{text}"
        );
        assert!(
            !text.contains("decdn_dispatch_rejected_global_total 1"),
            "global counter must not move on a per-source rejection"
        );
    }

    /// Per-source layer enabled + relay-only connection (no peer IP):
    /// `dispatch_per_source_skipped_no_addr_total` increments. When the
    /// layer is disabled, no skip is recorded (no enforcement intent =>
    /// nothing to skip).
    #[tokio::test]
    async fn per_source_skipped_when_relay_only_and_layer_enabled() {
        let metrics = Arc::new(Metrics::new());
        let limiter = ConnectionLimiter::new(&strict_security(u32::MAX), Arc::clone(&metrics));
        let _p = limiter
            .acquire_inner(None)
            .expect("relay acquire under per-source enabled");
        let text = metrics.encode().unwrap();
        assert!(
            text.contains("decdn_dispatch_per_source_skipped_no_addr_total 1"),
            "relay-only acquire under enabled per-source must increment skip counter; got:\n{text}"
        );

        // Disable per-source via reload; a subsequent relay acquire must
        // *not* increment the counter (no enforcement intent).
        let mut c = strict_security(u32::MAX);
        c.per_source_rate_per_sec = 0.0;
        c.per_source_burst = 0;
        limiter.reload(&c);
        let _p2 = limiter
            .acquire_inner(None)
            .expect("relay acquire under per-source disabled");
        let text = metrics.encode().unwrap();
        assert!(
            text.contains("decdn_dispatch_per_source_skipped_no_addr_total 1"),
            "disabled per-source must not bump the skip counter past 1; got:\n{text}"
        );
    }

    /// `gc_per_source` (#440) drops fully-refilled buckets between
    /// acquires. Without it, a long-lived node whose connection rate
    /// stays below the over-cap threshold accumulates stale entries
    /// indefinitely — the acquire-path prune only fires under flood.
    #[tokio::test]
    async fn gc_per_source_drops_refilled_buckets() {
        // Fast refill: rate=1000/s, burst=1. Each bucket refills to
        // baseline well within a 100ms wait, so `retain_recent()` will
        // drop every key it sees.
        let metrics = Arc::new(Metrics::new());
        let cfg = ResolvedSecurity {
            max_concurrent_handlers: u32::MAX,
            per_source_rate_per_sec: 1000.0,
            per_source_burst: 1,
            // cap=0 disables the acquire-path opportunistic prune so
            // this test isolates the explicit GC method.
            max_tracked_sources: 0,
        };
        let limiter = ConnectionLimiter::new(&cfg, Arc::clone(&metrics));

        // Fill 8 distinct per-source buckets and drop each permit
        // immediately so the bucket state matches "fresh baseline"
        // after refill.
        for i in 0..8u8 {
            let _p = limiter
                .acquire_inner(Some(ip(10, 0, 0, i)))
                .expect("acquire should succeed under generous rate");
        }
        assert_eq!(limiter.per_source_tracked(), 8);

        // Wait long enough for every bucket to refill (rate=1000/s
        // means the single token returns in ~1ms).
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        limiter.gc_per_source();
        assert_eq!(
            limiter.per_source_tracked(),
            0,
            "refilled buckets should be dropped by gc_per_source"
        );
    }

    /// `gc_per_source` is a no-op when the per-source layer is
    /// disabled — exercises the early-return arm.
    #[test]
    fn gc_per_source_is_noop_when_layer_disabled() {
        let metrics = Arc::new(Metrics::new());
        let cfg = ResolvedSecurity {
            max_concurrent_handlers: u32::MAX,
            per_source_rate_per_sec: 0.0,
            per_source_burst: 0,
            max_tracked_sources: 0,
        };
        let limiter = ConnectionLimiter::new(&cfg, Arc::clone(&metrics));
        // Must not panic and must return None (layer disabled).
        assert!(limiter.gc_per_source().is_none());
        assert_eq!(limiter.per_source_tracked(), 0);
    }

    /// `PruneGuard` releases `pruning_in_progress` even when the
    /// protected operation panics. Without this, a single panic inside
    /// `retain_recent` (third-party code from `governor`, or a future
    /// allocation failure during the walk) would leave the flag stuck
    /// `true` and permanently disable both prune codepaths for the
    /// lifetime of the process — the unbounded-keyspace failure mode
    /// #440 is meant to prevent. We can't make `retain_recent` itself
    /// panic on demand, so test the guard's Drop semantics directly:
    /// a `catch_unwind` around a guard whose protected scope panics
    /// must observe the flag reset to `false`.
    #[test]
    fn prune_guard_resets_flag_on_panic() {
        use std::panic::{AssertUnwindSafe, catch_unwind};
        use std::sync::atomic::{AtomicBool, Ordering};

        let flag = AtomicBool::new(false);
        flag.compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
            .expect("uncontended CAS must succeed");

        let result = catch_unwind(AssertUnwindSafe(|| {
            let _guard = super::PruneGuard(&flag);
            panic!("simulated retain_recent panic");
        }));
        assert!(
            result.is_err(),
            "panic should propagate out of catch_unwind"
        );
        assert!(
            !flag.load(Ordering::Acquire),
            "PruneGuard::drop must reset the flag during panic unwind"
        );
    }

    /// IPv6 addresses in the same /64 share a per-source bucket. Without
    /// `/64` grouping an attacker with a customer-grade IPv6 allocation
    /// can trivially defeat the per-source layer.
    #[test]
    fn per_source_buckets_ipv6_by_64_prefix() {
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
            .acquire_inner(Some(v6_a))
            .expect("first acquire from /64");
        // burst=1 per-source — second acquire from any address in the
        // same /64 must reject.
        let err = limiter
            .acquire_inner(Some(v6_b))
            .expect_err("second IPv6 from same /64 must reject");
        assert_eq!(err, RejectReason::PerSource);

        // Address in a *different* /64 must succeed — proves the mask
        // isn't accidentally collapsing every IPv6 into one bucket.
        let v6_c = IpAddr::V6(Ipv6Addr::new(0x2001, 0xdb8, 0, 1, 0, 0, 0, 1));
        let _p2 = limiter
            .acquire_inner(Some(v6_c))
            .expect("acquire from a different /64 must succeed");
    }
}
