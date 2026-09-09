//! Liveness of the node's chain-derived compliance state (ADR 011 § Serving
//! while chain-stale).
//!
//! Every serve-path guard that reads on-chain state — the blacklist deny-set,
//! the pool-solvency recheck, the per-signer capability cap — answers from a
//! projection that only advances while the chain watchers can reach the RPC.
//! During an RPC outage each of those guards silently passes on stale data: a
//! takedown, a drained pool, or a spent capability that landed on-chain during
//! the blind window is invisible, so the node keeps signing serve and probe
//! responses it can no longer vouch for. Serving a hash past its compliance
//! window is slashable (ADR 011 § Slashing), and the offense is exactly a signed
//! `StreamResponse{ok:true}` or `ProbeResponse{has_blob:true}` — the node
//! produces its own evidence.
//!
//! [`ChainFreshness`] is the single backstop over that whole family: it holds
//! the wall-clock time of the blacklist watcher's last successful poll tick and
//! answers one question — has the node been unable to reach the chain for longer
//! than the operator's grace window? While it has, the serve and probe paths
//! refuse rather than sign. The signal is the *successful tick*, not the last
//! event: a quiet chain produces no blacklist events for hours legitimately, and
//! keying on events would take every node dark on any quiet stretch. The tick
//! stamps on every successful poll including idle ones (the same liveness the
//! `decdn_blacklist_watcher_last_tick_timestamp_seconds` gauge exports), and the
//! boot enumeration stamps it too, so a freshly-booted node that has just
//! vetted its deny-set is never treated as stale.
//!
//! **Grace, not a switch.** There is one knob — the grace window in seconds — and
//! it is always evaluated. An operator who wants the old fail-open behavior sets
//! the window large (a year); there is no separate enable flag, so there is no
//! "enabled but zero-grace" state to reason about.
//!
//! **Unwired = fail-open.** A node with no chain configured (dev/test) has no
//! blacklist watcher to stamp the cell, so the runtime wires no
//! [`ChainFreshness`] at all and the guards read `None` — the same fail-open
//! shape the pool-view guards use when no chain is present.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

/// Live wall clock in seconds since the Unix epoch, or `0` if the system clock
/// is before the epoch. Both the stamp and the staleness read use this, so a
/// backward clock step affects the difference only by the step size — well
/// within a grace window measured in minutes.
fn live_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_secs())
}

/// Shared liveness of the node's chain reads. Cloned into the blacklist watcher
/// (which stamps it) and the serve and probe handlers (which read it); every
/// clone shares one cell.
#[derive(Clone, Debug)]
pub struct ChainFreshness {
    /// Unix seconds of the last successful blacklist poll tick (or boot
    /// enumeration). `0` means "never stamped", which reads as stale.
    last_ok_secs: Arc<AtomicU64>,
    /// How long the node may go without a successful chain read before the serve
    /// and probe paths refuse. Always evaluated; a large value is how an operator
    /// opts out.
    grace_secs: u64,
}

impl ChainFreshness {
    /// A freshness handle with the given grace window, not yet stamped (so it
    /// reads stale until the boot enumeration or the first successful tick stamps
    /// it).
    #[must_use]
    pub fn new(grace: Duration) -> Self {
        Self {
            last_ok_secs: Arc::new(AtomicU64::new(0)),
            grace_secs: grace.as_secs(),
        }
    }

    /// Record that a chain read just succeeded (a successful blacklist poll tick,
    /// or the clean boot enumeration). Stamps the current wall clock.
    pub fn stamp(&self) {
        self.last_ok_secs.store(live_secs(), Ordering::Relaxed);
    }

    /// Has the node been unable to reach the chain for longer than the grace
    /// window? A never-stamped cell (`0`) is stale by definition — the node has
    /// no confirmed-fresh compliance state to serve against.
    #[must_use]
    pub fn is_stale(&self) -> bool {
        let last = self.last_ok_secs.load(Ordering::Relaxed);
        if last == 0 {
            return true;
        }
        live_secs().saturating_sub(last) > self.grace_secs
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A fresh handle has never been stamped, so it reads stale — a node that has
    /// not yet confirmed its deny-set must not serve.
    #[test]
    fn unstamped_reads_stale() {
        let f = ChainFreshness::new(Duration::from_mins(30));
        assert!(f.is_stale(), "a never-stamped handle is stale");
    }

    /// A just-stamped handle is fresh, and a clone shares the same cell — the
    /// watcher's stamp is visible to the handler's read.
    #[test]
    fn stamped_reads_fresh_across_clones() {
        let f = ChainFreshness::new(Duration::from_mins(30));
        let reader = f.clone();
        f.stamp();
        assert!(
            !reader.is_stale(),
            "a clone sees the stamp through the shared cell"
        );
    }

    /// A stamp older than the grace window reads stale; the same stamp under a
    /// larger window reads fresh. This is the relativity a large window relies on
    /// to opt out — set it well past any real outage and no gap ever trips it.
    #[test]
    fn staleness_is_relative_to_the_grace_window() {
        let ten_min_ago = live_secs().saturating_sub(600);

        let tight = ChainFreshness::new(Duration::from_mins(1));
        tight.last_ok_secs.store(ten_min_ago, Ordering::Relaxed);
        assert!(tight.is_stale(), "a 10 min gap exceeds a 1 min grace");

        let generous = ChainFreshness::new(Duration::from_mins(30));
        generous.last_ok_secs.store(ten_min_ago, Ordering::Relaxed);
        assert!(
            !generous.is_stale(),
            "the same 10 min gap is fresh under a 30 min window"
        );
    }
}
