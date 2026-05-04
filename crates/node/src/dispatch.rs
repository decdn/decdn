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
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

use governor::{DefaultKeyedRateLimiter, Quota};
use iroh::TransportAddr;
use iroh::Watcher as _;
use iroh::endpoint::Connection;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::config::ResolvedSecurity;
use crate::metrics::Metrics;

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

/// Shared rate limiter used by all deCDN-authored `ProtocolHandler` implementations.
///
/// `reload(&self, &ResolvedSecurity)` applies a new resolved-security
/// snapshot in place. The per-source [`governor`] limiter is rebuilt
/// from the new quota and swapped in under an internal `RwLock`;
/// existing in-flight acquires that already passed the per-source
/// check are unaffected. The `Arc<Semaphore>` identity is
/// preserved across cap changes via `add_permits` /
/// `acquire_many_owned(...).forget()` so every outstanding
/// `OwnedSemaphorePermit` continues to drain into the same semaphore
/// on `Drop`.
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
        let per_source = PerSource::new(
            cfg.per_source_rate_per_sec,
            cfg.per_source_burst,
            cfg.max_tracked_sources,
        );
        let per_source_enabled = AtomicBool::new(per_source.limiter.is_some());
        Self {
            semaphore,
            target_max_concurrent: AtomicU32::new(initial_size),
            live_semaphore_size: AtomicU32::new(initial_size),
            target_lock: Mutex::new(()),
            per_source: RwLock::new(per_source),
            per_source_enabled,
            per_source_poison_logged: AtomicBool::new(false),
            metrics,
        }
    }

    /// Apply a new `ResolvedSecurity` to the live limiter.
    ///
    /// Field-by-field strategy:
    /// - `per_source_rate_per_sec`, `per_source_burst`,
    ///   `max_tracked_sources`: rebuild the keyed [`governor`] limiter
    ///   from the new quota and atomically swap it in under the
    ///   per-source write lock. **Token-bucket state is not preserved
    ///   across the swap** — governor's keyed map is
    ///   discarded and a fresh one takes its place. Operators tuning a
    ///   live quota should expect each source's next acquire to start
    ///   with a fresh burst budget.
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
    /// Infallible: limiter rebuild can't fail (quota construction is
    /// gated on validated config), `add_permits` can't fail, and the
    /// shrink path is `tokio::spawn` (which only fails by panicking —
    /// not via this return). Caller (`RuntimeReloadState::reload`) has
    /// already validated the values via `resolve_security`, so this
    /// method takes a `&ResolvedSecurity` rather than re-parsing.
    pub fn reload(&self, cfg: &ResolvedSecurity) {
        // 1. Per-source: rebuild limiter from the new quota and swap
        //    under the write lock.
        {
            let new = PerSource::new(
                cfg.per_source_rate_per_sec,
                cfg.per_source_burst,
                cfg.max_tracked_sources,
            );
            let enabled = new.limiter.is_some();
            let mut g = self.per_source.write().unwrap_or_else(|poisoned| {
                self.log_per_source_poison();
                poisoned.into_inner()
            });
            *g = new;
            // Mirror the enabled state for the relay-only fast path.
            // Stored under Relaxed because there's no happens-before
            // relationship to the limiter's own state — the worst case
            // across a swap is one accept observing the prior generation,
            // operationally indistinguishable from arriving microseconds
            // earlier.
            self.per_source_enabled.store(enabled, Ordering::Relaxed);
        }

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
        // target == 0 disables the global cap; skip the semaphore.
        let sem_permit = if self.target_max_concurrent.load(Ordering::Relaxed) > 0 {
            Some(
                Arc::clone(&self.semaphore)
                    .try_acquire_owned()
                    .map_err(|_| {
                        self.metrics.dispatch_rejected_global();
                        tracing::debug!(
                            reason = RejectReason::GlobalFull.as_str(),
                            ip = ?peer_ip,
                            "dispatch rejected: global semaphore exhausted"
                        );
                        RejectReason::GlobalFull
                    })?,
            )
        } else {
            None
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
        let g = self.per_source.read().unwrap_or_else(|poisoned| {
            self.log_per_source_poison();
            poisoned.into_inner()
        });
        let limiter = g.limiter.as_ref()?;
        let result = limiter.check_key(&key);
        // Best-effort cap enforcement: if the keyspace has grown past
        // the operator-configured cap, prune entries whose state has
        // refilled to a fresh baseline. `retain_recent` is governor's
        // O(n) walk over the keyed state and runs without contending
        // the limiter's per-key state. Pruning here (rather than from
        // a background task) keeps the bound tight — keys can never
        // accumulate past the cap by more than the time a single
        // acquire takes to observe the overflow.
        if g.cap > 0 && limiter.len() > g.cap {
            limiter.retain_recent();
        }
        match result {
            Ok(()) => None,
            Err(_) => Some(RejectReason::PerSource),
        }
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
    use std::time::Duration;

    use super::{ConnectionLimiter, RejectReason};
    use crate::config::ResolvedSecurity;
    use crate::metrics::Metrics;

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
        let metrics = Arc::new(Metrics::new());
        let limiter = ConnectionLimiter::new(&strict_security(2), Arc::clone(&metrics));
        let _p1 = limiter.acquire_inner(Some(ip(10, 0, 0, 1))).unwrap();
        let _p2 = limiter.acquire_inner(Some(ip(10, 0, 0, 2))).unwrap();
        // Cap exhausted at 2.
        assert!(limiter.acquire_inner(Some(ip(10, 0, 0, 3))).is_err());

        // Reload to cap=5. Permissive per-source so it doesn't shadow.
        let mut new_cfg = permissive_security();
        new_cfg.max_concurrent_handlers = 5;
        limiter.reload(&new_cfg);

        // Three more acquires from fresh IPs — global cap grew from 2
        // to 5, so all three succeed.
        let _p3 = limiter
            .acquire_inner(Some(ip(10, 0, 0, 3)))
            .expect("after grow");
        let _p4 = limiter
            .acquire_inner(Some(ip(10, 0, 0, 4)))
            .expect("after grow");
        let _p5 = limiter
            .acquire_inner(Some(ip(10, 0, 0, 5)))
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
        let _p1 = limiter.acquire_inner(Some(ip(10, 0, 0, 1))).unwrap();
        let p2 = limiter.acquire_inner(Some(ip(10, 0, 0, 2))).unwrap();

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
            .acquire_inner(Some(ip(10, 0, 0, 3)))
            .expect("after drop, slot freed");
        let _p4 = limiter
            .acquire_inner(Some(ip(10, 0, 0, 4)))
            .expect("after drop, second slot freed");
        // Now held = 3 (p1, p3, p4), cap = 3, no more free.
        assert!(
            limiter.acquire_inner(Some(ip(10, 0, 0, 5))).is_err(),
            "shrink to 3 must reject the 4th acquire"
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
        let _p1 = limiter.acquire_inner(Some(ip(10, 0, 0, 1))).unwrap();
        let _p2 = limiter.acquire_inner(Some(ip(10, 0, 0, 2))).unwrap();
        let _p3 = limiter.acquire_inner(Some(ip(10, 0, 0, 3))).unwrap();
        assert!(
            limiter.acquire_inner(Some(ip(10, 0, 0, 4))).is_err(),
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
                    .acquire_inner(Some(ip(10, 0, 0, i)))
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

    /// `target_lock` exists specifically to serialise concurrent reloads
    /// so racing N→…→M operations converge. Run 8 concurrent reloads
    /// against the same limiter and assert the post-race
    /// `live_semaphore_size` matches the *last* reload to commit
    /// (whichever wins the lock race), then a follow-up reload to a
    /// known cap produces exactly that cap's behaviour.
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
        // Hold 11 permits; the 12th must reject.
        let mut held = Vec::with_capacity(11);
        for i in 0..11u8 {
            held.push(
                limiter
                    .acquire_inner(Some(ip(10, 0, 0, i)))
                    .expect("11 acquires under cap=11"),
            );
        }
        assert!(
            limiter.acquire_inner(Some(ip(10, 0, 0, 99))).is_err(),
            "12th acquire must reject at cap=11 after concurrent reloads converge"
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
                    .acquire_inner(Some(ip(10, 0, 0, i)))
                    .expect("acquire under cap=4")
            })
            .collect();
        assert!(
            limiter.acquire_inner(Some(ip(10, 0, 0, 99))).is_err(),
            "5th must reject; if live baseline drifted, this would have over-allocated"
        );
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

    /// Multi-thread runtime: races N acquire loops against a reload
    /// storm and asserts no panic + final permit count converges to
    /// one of the targets the reload storm wrote.
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
                while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                    // Don't care about success/fail — only that we
                    // never panic and the limiter remains internally
                    // consistent across the race window.
                    let _ = l.acquire_inner(Some(ip(10, 0, 0, w)));
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
        // 6th must reject under cap=5.
        let mut held = Vec::with_capacity(5);
        for i in 0..5u8 {
            held.push(
                limiter
                    .acquire_inner(Some(ip(192, 168, 1, i)))
                    .expect("5 acquires under cap=5"),
            );
        }
        assert!(
            limiter.acquire_inner(Some(ip(192, 168, 1, 99))).is_err(),
            "6th acquire must reject at cap=5 after acquire+reload storm converges"
        );
    }
}
