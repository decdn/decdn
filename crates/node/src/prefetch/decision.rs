//! Speculative-prefetch decision engine (ADR 022 §Prefetch Decision). Given a
//! hash whose `FIND_VALUE` demand crossed the trigger threshold, decide whether
//! to acquire it, applying the operator-policy gates in order:
//! enabled → demand-quality throttle → authorized-origin → budget.
//!
//! Pure: the caller injects the clock (`now`, seconds) and the
//! [`OriginDirectory`]. Ledger state lives behind a `std::sync::Mutex`; lock
//! poisoning fails closed (the decision becomes a skip).

use std::collections::VecDeque;
use std::sync::Mutex;

use decdn_common::config::ResolvedPrefetch;

use crate::dht::origin::{Hash, OriginDirectory};

/// Fixed rolling-window length for the spend budget: 1 hour, in seconds. ADR
/// 022 names the cap as `budget_usdc_per_hour`, so the window is not operator-
/// tunable (only the cap value is).
const BUDGET_WINDOW_SECS: u64 = 3600;

/// Outcome of a prefetch decision.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PrefetchDecision {
    /// Proceed to acquire the hash (the live acquisition is a #650 follow-up).
    Acquire,
    /// Do not acquire; carries the first gate that rejected.
    Skip(SkipReason),
}

/// Why a prefetch was skipped (gate-ordered).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SkipReason {
    /// `prefetch.enabled == false`.
    Disabled,
    /// Demand-quality auto-throttle is latched active.
    Throttled,
    /// `require_authorized_origin` and no authorized origin for the hash.
    Unauthorized,
    /// Rolling-1h spend has reached `budget_usdc_per_hour`.
    BudgetExhausted,
}

/// Rolling-window ledgers + throttle latch.
#[derive(Debug, Default)]
struct Ledgers {
    /// (`timestamp_secs`, `micro_usdc`) prefetch spends, oldest-first.
    spend: VecDeque<(u64, u64)>,
    /// (`timestamp_secs`, bytes) acquired via prefetch, oldest-first.
    acquired: VecDeque<(u64, u64)>,
    /// (`timestamp_secs`, bytes) served from prefetched content, oldest-first.
    served: VecDeque<(u64, u64)>,
    /// Whether the demand-quality auto-throttle is currently latched active.
    throttled: bool,
}

/// Operator-policy prefetch decision engine.
#[derive(Debug)]
pub struct PrefetchPolicy {
    cfg: ResolvedPrefetch,
    ledgers: Mutex<Ledgers>,
}

impl PrefetchPolicy {
    /// Construct from resolved config.
    #[must_use]
    pub fn new(cfg: ResolvedPrefetch) -> Self {
        Self {
            cfg,
            ledgers: Mutex::new(Ledgers::default()),
        }
    }

    /// Decide whether to prefetch `hash` at `now` (seconds), consulting `dir`
    /// for the authorized-origin gate. Gates apply in order; the first to
    /// reject wins. Fails closed (`Skip(Throttled)` as a conservative
    /// stand-in) if the ledger lock is poisoned.
    #[must_use]
    pub fn decide(&self, hash: &Hash, dir: &dyn OriginDirectory, now: u64) -> PrefetchDecision {
        // Gate 1: master switch.
        if !self.cfg.enabled {
            return PrefetchDecision::Skip(SkipReason::Disabled);
        }

        // Gate 2: demand-quality throttle (also prunes ledgers for gate 4).
        let Ok(mut led) = self.ledgers.lock() else {
            tracing::error!("prefetch decide: ledger mutex poisoned; skipping");
            return PrefetchDecision::Skip(SkipReason::Throttled);
        };
        Self::prune(&mut led.spend, BUDGET_WINDOW_SECS, now);
        Self::prune(&mut led.acquired, self.cfg.demand_quality_window_secs, now);
        Self::prune(&mut led.served, self.cfg.demand_quality_window_secs, now);
        led.throttled = Self::compute_throttle(&led, self.cfg.demand_quality_min_ratio);
        if led.throttled {
            return PrefetchDecision::Skip(SkipReason::Throttled);
        }

        // Gate 3: authorized origin.
        if self.cfg.require_authorized_origin && dir.lookup_origins(hash).is_empty() {
            return PrefetchDecision::Skip(SkipReason::Unauthorized);
        }

        // Gate 4: rolling-1h budget.
        let spent: u64 = led.spend.iter().map(|(_, v)| *v).sum();
        if spent >= self.cfg.budget_usdc_per_hour {
            return PrefetchDecision::Skip(SkipReason::BudgetExhausted);
        }

        PrefetchDecision::Acquire
    }

    /// Record a completed prefetch acquisition (drives budget + demand-quality
    /// denominator). Exercised by unit tests this slice; wired to the live
    /// acquisition path in the #650 follow-up.
    pub fn record_acquisition(&self, micro_usdc: u64, bytes: u64, now: u64) {
        if let Ok(mut led) = self.ledgers.lock() {
            led.spend.push_back((now, micro_usdc));
            led.acquired.push_back((now, bytes));
        } else {
            tracing::error!("prefetch record_acquisition: ledger mutex poisoned");
        }
    }

    /// Record bytes served from prefetched content (demand-quality numerator).
    pub fn record_served(&self, bytes: u64, now: u64) {
        if let Ok(mut led) = self.ledgers.lock() {
            led.served.push_back((now, bytes));
        } else {
            tracing::error!("prefetch record_served: ledger mutex poisoned");
        }
    }

    /// Current rolling-window `served / acquired` ratio at `now`. Returns
    /// `1.0` (healthy) when nothing has been acquired yet.
    #[must_use]
    pub fn demand_quality_ratio(&self, now: u64) -> f64 {
        let Ok(mut led) = self.ledgers.lock() else {
            return 1.0;
        };
        Self::prune(&mut led.acquired, self.cfg.demand_quality_window_secs, now);
        Self::prune(&mut led.served, self.cfg.demand_quality_window_secs, now);
        Self::ratio(&led)
    }

    /// Whether the demand-quality throttle is latched active at `now`.
    #[must_use]
    pub fn throttle_active(&self, now: u64) -> bool {
        let Ok(mut led) = self.ledgers.lock() else {
            return true; // fail closed
        };
        Self::prune(&mut led.acquired, self.cfg.demand_quality_window_secs, now);
        Self::prune(&mut led.served, self.cfg.demand_quality_window_secs, now);
        Self::compute_throttle(&led, self.cfg.demand_quality_min_ratio)
    }

    /// `served / acquired` over the current ledger; `1.0` when `acquired == 0`.
    #[allow(clippy::cast_precision_loss)] // byte counts < 2^53 in any real window
    fn ratio(led: &Ledgers) -> f64 {
        let acquired: u64 = led.acquired.iter().map(|(_, v)| *v).sum();
        if acquired == 0 {
            return 1.0;
        }
        let served: u64 = led.served.iter().map(|(_, v)| *v).sum();
        served as f64 / acquired as f64
    }

    /// Throttle active iff at least one acquisition exists and the ratio is
    /// below the floor.
    fn compute_throttle(led: &Ledgers, min_ratio: f64) -> bool {
        let acquired: u64 = led.acquired.iter().map(|(_, v)| *v).sum();
        acquired > 0 && Self::ratio(led) < min_ratio
    }

    /// Drop `(ts, _)` entries strictly older than `window_secs` (`age >= window`).
    fn prune(q: &mut VecDeque<(u64, u64)>, window_secs: u64, now: u64) {
        while let Some((ts, _)) = q.front() {
            if now.saturating_sub(*ts) >= window_secs {
                q.pop_front();
            } else {
                break;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use decdn_common::config::ResolvedPrefetch;

    use super::{PrefetchDecision, PrefetchPolicy, SkipReason};
    use crate::dht::origin::{ConfigOriginDirectory, Hash, OriginDirectory};
    use crate::dht::routing::NodeId;

    fn hash() -> Hash {
        Hash::from_bytes([7u8; 32])
    }

    /// Directory that authorizes `hash()` -> one origin.
    fn authorized_dir() -> Arc<dyn OriginDirectory> {
        let mut m = std::collections::HashMap::new();
        m.insert(hash(), vec![NodeId::from_bytes([1u8; 32])]);
        Arc::new(ConfigOriginDirectory::new(m))
    }

    /// Directory that authorizes nothing.
    fn empty_dir() -> Arc<dyn OriginDirectory> {
        Arc::new(ConfigOriginDirectory::new(std::collections::HashMap::new()))
    }

    fn cfg(enabled: bool) -> ResolvedPrefetch {
        ResolvedPrefetch {
            enabled,
            require_authorized_origin: true,
            budget_usdc_per_hour: 1_000_000,
            find_value_threshold: 5,
            threshold_window_secs: 300,
            demand_quality_min_ratio: 0.1,
            demand_quality_window_secs: 3600,
        }
    }

    #[test]
    fn disabled_skips_before_origin_lookup() {
        let p = PrefetchPolicy::new(cfg(false));
        assert_eq!(
            p.decide(&hash(), &*empty_dir(), 0),
            PrefetchDecision::Skip(SkipReason::Disabled)
        );
    }

    #[test]
    fn unauthorized_when_no_origin() {
        let p = PrefetchPolicy::new(cfg(true));
        assert_eq!(
            p.decide(&hash(), &*empty_dir(), 0),
            PrefetchDecision::Skip(SkipReason::Unauthorized)
        );
    }

    #[test]
    fn acquires_when_authorized_and_in_budget() {
        let p = PrefetchPolicy::new(cfg(true));
        assert_eq!(
            p.decide(&hash(), &*authorized_dir(), 0),
            PrefetchDecision::Acquire
        );
    }

    #[test]
    fn origin_gate_bypassed_when_disabled() {
        let mut c = cfg(true);
        c.require_authorized_origin = false;
        let p = PrefetchPolicy::new(c);
        // No authorized origin, but the gate is off => proceeds.
        assert_eq!(
            p.decide(&hash(), &*empty_dir(), 0),
            PrefetchDecision::Acquire
        );
    }

    #[test]
    fn budget_exhaustion_blocks_until_window_advances() {
        let mut c = cfg(true);
        c.budget_usdc_per_hour = 100;
        let p = PrefetchPolicy::new(c);
        p.record_acquisition(100, 1_000, 0); // spend hits the cap at t=0
        // Serve the acquired bytes so the demand-quality gate stays healthy and
        // we isolate the budget gate (throttle is checked before budget).
        p.record_served(1_000, 0);
        assert_eq!(
            p.decide(&hash(), &*authorized_dir(), 10),
            PrefetchDecision::Skip(SkipReason::BudgetExhausted)
        );
        // 1h (3600s) later the spend has aged out of the rolling window.
        assert_eq!(
            p.decide(&hash(), &*authorized_dir(), 3700),
            PrefetchDecision::Acquire
        );
    }

    #[test]
    fn zero_budget_always_exhausted() {
        let mut c = cfg(true);
        c.budget_usdc_per_hour = 0;
        let p = PrefetchPolicy::new(c);
        assert_eq!(
            p.decide(&hash(), &*authorized_dir(), 0),
            PrefetchDecision::Skip(SkipReason::BudgetExhausted)
        );
    }

    #[test]
    fn throttle_latches_below_ratio_and_clears_on_recovery() {
        let p = PrefetchPolicy::new(cfg(true));
        // Acquire 1000 bytes, serve only 50 => ratio 0.05 < 0.1 => throttle.
        p.record_acquisition(10, 1_000, 0);
        p.record_served(50, 0);
        assert_eq!(
            p.decide(&hash(), &*authorized_dir(), 1),
            PrefetchDecision::Skip(SkipReason::Throttled)
        );
        // Serve 200 more => 250/1000 = 0.25 >= 0.1 => recovers.
        p.record_served(200, 2);
        assert_eq!(
            p.decide(&hash(), &*authorized_dir(), 3),
            PrefetchDecision::Acquire
        );
    }

    #[test]
    fn no_acquisitions_means_no_throttle() {
        let p = PrefetchPolicy::new(cfg(true));
        // acquired == 0 => ratio undefined => not throttled.
        assert_eq!(
            p.decide(&hash(), &*authorized_dir(), 0),
            PrefetchDecision::Acquire
        );
        assert!(!p.throttle_active(0));
    }
}
