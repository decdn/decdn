//! `LoadShedController` ties the swappable policy, the live counters, and the
//! egress meter into one handle the serve path holds. It mirrors
//! `ConnectionLimiter`: the handler owns an `Arc<LoadShedController>`, and a
//! hot-reload swaps the policy in place under an `ArcSwap`.

use std::sync::Arc;

use alloy::primitives::B256;
use arc_swap::ArcSwap;
use decdn_common::config::{LoadShedPolicyKind, ResolvedLoadShed};

use super::{
    AlwaysAdmit, EgressMeter, LoadShedPolicy, PressureSnapshot, RequestClass, ResourcePressure,
    ResourcePressureParams, ShedDecision, ShedReason, ShedSlot, ShedState,
};

/// Bytes/sec for a megabit/sec budget: `Mbps * 1_000_000 / 8`.
#[must_use]
pub(super) const fn egress_budget_bps(mbps: u64) -> u64 {
    mbps.saturating_mul(125_000)
}

/// Build the policy object a resolved config selects.
fn build_policy(cfg: &ResolvedLoadShed) -> Arc<dyn LoadShedPolicy> {
    match cfg.policy {
        LoadShedPolicyKind::AlwaysAdmit => Arc::new(AlwaysAdmit),
        LoadShedPolicyKind::ResourcePressure => {
            Arc::new(ResourcePressure::new(ResourcePressureParams {
                egress_budget_bps: egress_budget_bps(cfg.egress_budget_mbps),
                max_serves_high: cfg.max_concurrent_serves_high,
                max_serves_low: cfg.max_concurrent_serves_low,
                per_client_cap: cfg.per_client_serve_cap,
            }))
        }
    }
}

/// The serve path's load-shed handle: a swappable policy over live meters.
#[allow(missing_debug_implementations)]
pub struct LoadShedController {
    // `ArcSwap<Arc<dyn LoadShedPolicy>>` (not `ArcSwap<dyn LoadShedPolicy>`): arc-swap's
    // `RefCnt` blanket impl `impl<T> RefCnt for Arc<T>` bounds `T: Sized`, so
    // `Arc<dyn LoadShedPolicy>` does not implement `RefCnt`. Storing the trait object
    // as the sized `Arc<dyn ...>` pointee is the supported form; `.load()` derefs
    // through both `Arc`s to the trait object.
    policy: ArcSwap<Arc<dyn LoadShedPolicy>>,
    state: Arc<ShedState>,
    egress: Arc<EgressMeter>,
}

impl LoadShedController {
    #[must_use]
    pub fn from_config(cfg: &ResolvedLoadShed) -> Arc<Self> {
        Arc::new(Self {
            policy: ArcSwap::from_pointee(build_policy(cfg)),
            state: ShedState::new(),
            egress: Arc::new(EgressMeter::new()),
        })
    }

    /// Decide and, on admit, take a slot. On shed returns the reason; the caller
    /// maps it to the `NotFound`-shaped refusal.
    pub fn try_admit(&self, class: RequestClass, client: B256) -> Result<ShedSlot, ShedReason> {
        let (node, per) = self.state.counts(client);
        let snap = PressureSnapshot {
            streams_in_flight: node,
            client_streams_in_flight: per,
            egress_bps: self.egress.current_bps(),
        };
        let policy = self.policy.load();
        match policy.decide(class, &snap) {
            ShedDecision::Admit => Ok(self.state.acquire(client)),
            ShedDecision::Shed(reason) => Err(reason),
        }
    }

    /// Feed delivered bytes into the egress meter (delivery hot path). `bytes`
    /// is the per-chunk length also added to the billed `delivered` counter —
    /// the bao-stream content plus interleaved proof, not the full QUIC frame.
    pub fn record_egress(&self, bytes: u64) {
        self.egress.record(bytes);
    }

    /// Fold the interval's bytes into the egress EWMA; returns the new bytes/sec
    /// (for the gauge).
    pub fn sample_egress(&self, interval_secs: u64) -> u64 {
        self.egress.sample(interval_secs)
    }

    /// Whether the active policy currently considers the node pressured (gauge).
    #[must_use]
    pub fn pressure_active(&self) -> bool {
        self.policy.load().pressure_active()
    }

    /// Swap in a policy rebuilt from a new resolved config. Live counters and
    /// the egress meter are preserved; only the decision policy changes.
    pub fn reload(&self, cfg: &ResolvedLoadShed) {
        self.policy.store(Arc::new(build_policy(cfg)));
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;
    use crate::load_shed::RequestClass;
    use alloy::primitives::B256;
    use decdn_common::config::{LoadShedPolicyKind, ResolvedLoadShed};

    fn cfg(kind: LoadShedPolicyKind) -> ResolvedLoadShed {
        ResolvedLoadShed {
            policy: kind,
            egress_budget_mbps: 0,
            max_concurrent_serves_high: 2,
            max_concurrent_serves_low: 1,
            per_client_serve_cap: 0,
        }
    }
    fn client(n: u8) -> B256 {
        B256::from([n; 32])
    }

    #[test]
    fn resource_pressure_sheds_miss_when_node_full() {
        let c = LoadShedController::from_config(&cfg(LoadShedPolicyKind::ResourcePressure));
        // Hold two slots to reach the high-water mark of 2.
        let _s1 = c
            .try_admit(RequestClass::CacheHit, client(1))
            .expect("first admit");
        let _s2 = c
            .try_admit(RequestClass::CacheHit, client(1))
            .expect("second admit");
        // A new miss is shed; a new hit is still admitted.
        assert_eq!(
            c.try_admit(RequestClass::CacheMiss, client(2)).err(),
            Some(ShedReason::NodeAtCapacity)
        );
        assert!(c.try_admit(RequestClass::CacheHit, client(2)).is_ok());
        assert!(c.pressure_active());
    }

    #[test]
    fn always_admit_never_sheds() {
        let c = LoadShedController::from_config(&cfg(LoadShedPolicyKind::AlwaysAdmit));
        let mut held = Vec::new();
        for _ in 0..50 {
            held.push(
                c.try_admit(RequestClass::CacheMiss, client(1))
                    .expect("always admit"),
            );
        }
        assert!(!c.pressure_active());
    }

    #[test]
    fn reload_swaps_policy_live() {
        let c = LoadShedController::from_config(&cfg(LoadShedPolicyKind::ResourcePressure));
        c.reload(&cfg(LoadShedPolicyKind::AlwaysAdmit));
        let _a = c
            .try_admit(RequestClass::CacheMiss, client(1))
            .expect("admit under swapped policy");
        let _b = c
            .try_admit(RequestClass::CacheMiss, client(1))
            .expect("admit 2");
        let _d = c
            .try_admit(RequestClass::CacheMiss, client(1))
            .expect("admit 3 — always-admit ignores caps");
    }

    #[test]
    fn egress_meter_feeds_snapshot() {
        let c = LoadShedController::from_config(&ResolvedLoadShed {
            policy: LoadShedPolicyKind::ResourcePressure,
            egress_budget_mbps: 1, // 1 Mbps = 125_000 B/s ceiling
            max_concurrent_serves_high: 1_000,
            max_concurrent_serves_low: 900,
            per_client_serve_cap: 0,
        });
        c.record_egress(200_000);
        c.sample_egress(1); // instant 200_000 B/s > 125_000 ceiling
        assert_eq!(
            c.try_admit(RequestClass::CacheHit, client(1)).err(),
            Some(ShedReason::EgressSaturated)
        );
    }
}
