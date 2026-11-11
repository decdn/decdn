//! Connection concurrency cap for all deCDN QUIC protocol handlers.
//!
//! # Problem
//!
//! iroh's `Router` spawns one task per accepted QUIC connection with no
//! concurrency cap. A single attacker can exhaust the node's task capacity by
//! flooding connections faster than the idle timeout reaps them.
//!
//! # Solution
//!
//! [`ConnectionLimiter`] holds a **global semaphore** at the top of every
//! deCDN-authored `ProtocolHandler::accept` implementation: a hard cap on total
//! in-flight handler tasks.
//!
//! The limiter keeps no per-source state. A source-address key binds no
//! attacker: a relay-routed connection carries no client address, so an
//! attacker that dials through a relay skips it for free, while honest peers
//! behind one NAT share its bucket (#2505).

use std::net::IpAddr;
use std::sync::Arc;

use arc_swap::ArcSwapOption;
use iroh::TransportAddr;
use iroh::endpoint::Connection;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::metrics::Metrics;
use decdn_common::config::ResolvedSecurity;

/// Reason a connection was rejected.
///
/// The rejection increments the unlabeled counter
/// `decdn_dispatch_rejected_global_total`, per the sibling-counter convention
/// settled in #1475.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RejectReason {
    /// Global concurrency semaphore exhausted.
    GlobalFull,
}

impl RejectReason {
    /// Short, stable label suitable for log fields and the QUIC close-frame
    /// reason bytes. Peers receiving the close can disambiguate the layer
    /// (and pick an appropriate backoff strategy) without parsing free text.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::GlobalFull => "global-full",
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

/// Shared connection limiter used by all deCDN-authored `ProtocolHandler`
/// implementations.
///
/// `reload(&self, &ResolvedSecurity)` applies a new resolved-security
/// snapshot in place: a fresh `Arc<Semaphore>` of the new capacity is built
/// and atomically swapped into the [`ArcSwapOption`] cell, so the next acquire
/// hits the new semaphore. Already-acquired `OwnedSemaphorePermit`s hold a
/// clone of the old `Arc<Semaphore>` and continue to drain into it on `Drop` —
/// they're dropped harmlessly once the last permit goes away.
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
    metrics: Arc<Metrics>,
}

impl ConnectionLimiter {
    /// Construct a limiter from the resolved security configuration.
    pub fn new(cfg: &ResolvedSecurity, metrics: Arc<Metrics>) -> Self {
        Self {
            semaphore: ArcSwapOption::from(build_semaphore(cfg.max_concurrent_handlers)),
            metrics,
        }
    }

    /// Apply a new `ResolvedSecurity` to the live limiter: build a fresh
    /// `Arc<Semaphore>` (or `None` when `max_concurrent_handlers == 0`) and
    /// atomically swap it into the [`ArcSwapOption`] cell. Already-acquired
    /// permits hold a clone of the previous `Arc<Semaphore>` and drop it
    /// harmlessly on permit release; new acquires hit the new cell.
    ///
    /// Infallible. Caller (`RuntimeReloadState::reload`) has already
    /// validated the values via `resolve_security`.
    pub fn reload(&self, cfg: &ResolvedSecurity) {
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

/// Extract the remote IP from a connection's currently-selected network path.
///
/// Returns `None` for relay-only connections that have no direct IP path.
/// Path selection may not have completed at the moment `accept` returns, so
/// the lookup falls back to *any* IP path. The dispatch limiter reads it to
/// count each inbound connection as direct or relayed, and to name the source
/// in its debug breadcrumb.
fn peer_ip(conn: &Connection) -> Option<IpAddr> {
    // `paths()` returns a snapshot of the *currently open* paths (closed paths
    // are not retained), so no explicit open-path filter is needed.
    let paths = conn.paths();
    paths
        .iter()
        .find(iroh::endpoint::Path::is_selected)
        .and_then(|p| match p.remote_addr() {
            TransportAddr::Ip(addr) => Some(addr.ip()),
            _ => None,
        })
        .or_else(|| {
            paths.iter().find_map(|p| match p.remote_addr() {
                TransportAddr::Ip(addr) => Some(addr.ip()),
                _ => None,
            })
        })
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests;
