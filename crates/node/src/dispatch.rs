//! Per-source connection rate limiting for all deCDN QUIC protocol handlers.
//!
//! # Problem
//!
//! iroh's `Router` spawns one task per accepted QUIC connection with no
//! concurrency cap. Its `incoming_filter` hook can refuse a connection before
//! the handshake, but it carries no rate state of its own. A single attacker
//! can exhaust the node's task capacity by flooding connections faster than
//! the idle timeout reaps them.
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
//! swaps the whole `PerSource` in through an [`ArcSwap`] — the acquire
//! read path is lock-free, matching the rest of the reload surface
//! (`semaphore` is an [`ArcSwapOption`]; the reload-fed serve structures
//! are `ArcSwap`/atomic). Token-bucket state is *not* preserved across a
//! reload (operators changing live quotas should expect the next acquire
//! from each source to start with a fresh burst budget).
//!
//! The keyspace prune (`retain_recent`, an `O(n)` walk over the keyed map)
//! never runs on the accept path. An accept does only the constant-time
//! bucket check plus an `O(1)` keyspace-length comparison; when the
//! keyspace grows past `cap + cap/10` the accept offloads one single-
//! flighted `retain_recent` sweep to a background task, so connection
//! admission stays constant-time regardless of keyspace size.

use std::net::{IpAddr, Ipv6Addr};
use std::num::NonZeroU32;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use arc_swap::{ArcSwap, ArcSwapOption};
use governor::{DefaultKeyedRateLimiter, Quota};
use iroh::endpoint::Connection;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::metrics::Metrics;
use crate::prune_guard::PruneGuard;
use crate::rate_limit::peer_ip;
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
///
/// Shared with the DHT per-IP limiter (`dht::rate_limit`), which had the
/// same bypass (#841).
pub(crate) fn source_key(ip: IpAddr) -> IpAddr {
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
///
/// Metrics-wise this is a **sibling-counter** split, per the convention settled
/// in #1475: each variant increments its own unlabeled counter
/// (`decdn_dispatch_rejected_{global,per_source}_total`) rather than a `reason`
/// label. `decdn_probe_hold_unavailable_total` is the one labeled *reason
/// split* — see [`crate::metrics::ProbeHoldUnavailableReason`].
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
/// from the new quota and swapped in wholesale through an internal
/// [`ArcSwap`]. The
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
    /// Per-source keyed limiter published through an [`ArcSwap`] so the
    /// acquire read path is lock-free: it loads the current [`PerSource`]
    /// and clones the inner limiter `Arc` out. Reload swaps a fresh
    /// `Arc<PerSource>` in wholesale; in-flight acquires keep operating on
    /// the generation they loaded, which is indistinguishable from
    /// arriving microseconds before the swap.
    per_source: ArcSwap<PerSource>,
    /// Mirrors `per_source.limiter.is_some()`. Read on the relay-only-
    /// connection fast path so we can record
    /// `dispatch_per_source_skipped_no_addr` without loading the whole
    /// `PerSource` on every relay accept. Written on reload.
    per_source_enabled: AtomicBool,
    /// Single-flight guard for `retain_recent` pruning. Without it, a
    /// flood of distinct-source connections that all observe an
    /// over-cap keyspace simultaneously would each dispatch an `O(n)`
    /// walk over the keyed state — one per acquire thread. The flag
    /// ensures at most one sweep is in flight at a time; concurrent
    /// over-cap observers skip and the next observer after the sweep
    /// completes dispatches any remaining work. Held in an `Arc` so the
    /// background sweep task owns a `'static` handle to reset it.
    pruning_in_progress: Arc<AtomicBool>,
    metrics: Arc<Metrics>,
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
            per_source: ArcSwap::from_pointee(per_source),
            per_source_enabled,
            pruning_in_progress: Arc::new(AtomicBool::new(false)),
            metrics,
        }
    }

    /// Apply a new `ResolvedSecurity` to the live limiter.
    ///
    /// - Per-source: rebuild the keyed [`governor`] limiter from the new
    ///   quota and atomically swap the whole `PerSource` into the
    ///   [`ArcSwap`] cell. **Token-bucket state is not preserved across the
    ///   swap.**
    /// - Global semaphore: build a fresh `Arc<Semaphore>` (or `None`
    ///   when `max_concurrent_handlers == 0`) and atomically swap it
    ///   into the [`ArcSwapOption`] cell. Already-acquired permits hold
    ///   a clone of the previous `Arc<Semaphore>` and drop it harmlessly
    ///   on permit release; new acquires hit the new cell.
    ///
    /// Infallible. Caller (`RuntimeReloadState::reload`) has already
    /// validated the values via `resolve_security`.
    pub fn reload(&self, cfg: &ResolvedSecurity) {
        // 1. Per-source: rebuild the limiter from the new quota and swap the
        //    whole `Arc<PerSource>` into the `ArcSwap` cell. In-flight acquires
        //    keep the generation they loaded until they drop it.
        let new_per_source = PerSource::new(
            cfg.per_source_rate_per_sec,
            cfg.per_source_burst,
            cfg.max_tracked_sources,
        );
        let per_source_enabled = new_per_source.limiter.is_some();
        self.per_source.store(Arc::new(new_per_source));
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
    /// drive the limiter directly. Non-test callers must not invoke this
    /// — `Self::acquire` is the only supported entry point.
    /// `#[doc(hidden)]` keeps it out of the rendered public API surface.
    #[doc(hidden)]
    pub fn acquire_for_test(&self, peer_ip: Option<IpAddr>) -> Result<Permit, RejectReason> {
        self.acquire_inner(peer_ip)
    }

    /// Shared implementation behind [`Self::acquire`] and
    /// [`Self::acquire_for_test`].
    fn acquire_inner(&self, peer_ip: Option<IpAddr>) -> Result<Permit, RejectReason> {
        // Every inbound connection passes here once, admitted or not, so this
        // is where its arrival path is counted (ADR 001 § Node Discovery).
        if peer_ip.is_some() {
            self.metrics.inbound_connection_direct();
        } else {
            self.metrics.inbound_connection_relayed();
        }

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
    /// to `None`. Otherwise: bucket the address (`/64` for IPv6) and
    /// call `check_key`. The `O(n)` keyspace prune never runs here —
    /// when the keyspace has grown past `cap + cap/10` the accept
    /// offloads one single-flighted `retain_recent` sweep to a
    /// background task (#1788) so admission stays constant-time.
    fn check_per_source(&self, ip: IpAddr) -> Option<RejectReason> {
        let key = source_key(ip);
        // Load the current `PerSource` lock-free and clone the limiter `Arc`
        // out. The clone keeps the observed `KeyedRateLimiter` alive across a
        // concurrent reload swap — operating on the prior generation is
        // indistinguishable from arriving microseconds earlier.
        let per_source = self.per_source.load();
        let limiter = per_source.limiter.clone()?;
        let cap = per_source.cap;
        let result = limiter.check_key(&key);
        // Best-effort cap enforcement, kept OFF the accept path (#1788 item 2).
        // The accept does only two constant-time reads here — `check_key` above
        // and `len()` below — and, when the keyspace has grown past `cap + cap/10`,
        // dispatches ONE single-flighted `retain_recent` sweep to a background
        // task rather than walking the `O(n)` keyed map inline:
        //
        // 1. 10% slack: dispatch only past cap + cap/10 so a sustained
        //    1-key-over-cap fluctuation under flood doesn't dispatch a sweep on
        //    every accept.
        // 2. Single-flight: a flood from N distinct sources that all observe
        //    over-cap simultaneously would otherwise dispatch N concurrent
        //    `O(n)` sweeps. The `pruning_in_progress` flag bounds it to one in
        //    flight at a time; concurrent observers skip and the next over-cap
        //    observer after the sweep completes dispatches any remaining work.
        //
        // The map can briefly exceed cap by more than 10% while a sweep is in
        // flight; on completion governor's `retain_recent` brings it back to cap
        // (it drops keys whose state is indistinguishable from fresh). `len()` is
        // `O(1)` in the keyspace size; only the `retain_recent` walk it gates is
        // load-dependent, which is why the walk — and only the walk — runs off
        // the accept path.
        if cap > 0 && limiter.len() > cap.saturating_add(cap / 10) {
            self.dispatch_prune(limiter);
        }
        match result {
            Ok(()) => None,
            Err(_) => Some(RejectReason::PerSource),
        }
    }

    /// Run one single-flighted `retain_recent` sweep off the accept path.
    ///
    /// Wins the `false -> true` CAS or returns immediately (another sweep is in
    /// flight). The winner offloads the `O(n)` walk to a `spawn_blocking` task so
    /// the accepting task never blocks on it; the [`PruneGuard`] resets the
    /// single-flight flag when the walk finishes, even on panic. When no tokio
    /// runtime is present — synchronous unit tests drive `acquire` directly — the
    /// sweep runs inline, since there is no accept task to keep responsive.
    fn dispatch_prune(&self, limiter: Arc<DefaultKeyedRateLimiter<IpAddr>>) {
        if self
            .pruning_in_progress
            .compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
            .is_err()
        {
            return;
        }
        let flag = Arc::clone(&self.pruning_in_progress);
        let sweep = move || {
            // RAII reset on drop: a panic inside `retain_recent` (third-party
            // `governor` code, or an allocation failure during the walk) must not
            // leave the flag stuck `true`, or both this path and the periodic GC
            // task would be permanently disabled for the process lifetime — the
            // unbounded-keyspace pathology #440 prevents. See `PruneGuard`.
            let _guard = PruneGuard(&flag);
            limiter.retain_recent();
        };
        match tokio::runtime::Handle::try_current() {
            Ok(handle) => {
                handle.spawn_blocking(sweep);
            }
            Err(_) => sweep(),
        }
    }

    /// Drop per-source buckets whose state has refilled to the fresh
    /// baseline (#440). The acquire path dispatches an opportunistic
    /// background sweep when the keyspace exceeds `cap + cap/10`, but a node
    /// whose connection rate falls below the over-cap threshold can carry
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
        // Load the current `PerSource` lock-free and clone the limiter `Arc`
        // out before the `O(n)` walk, so the walk operates on a stable handle
        // even across a concurrent reload swap.
        let limiter = self.per_source.load().limiter.clone()?;
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
        self.per_source
            .load()
            .limiter
            .as_ref()
            .map_or(0, |l| l.len())
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests;
