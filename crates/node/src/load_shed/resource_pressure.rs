//! The `ResourcePressure` load-shed policy: hysteresis over node-wide
//! concurrency, a hard egress ceiling, hit-before-miss priority, and a
//! per-client fair-share cap that engages only under pressure.

use std::sync::atomic::{AtomicBool, Ordering};

use super::{LoadShedPolicy, PressureSnapshot, RequestClass, ShedDecision, ShedReason};

/// Tunable thresholds, resolved from node config (see `ResolvedLoadShed`).
#[derive(Debug, Clone, Copy)]
pub struct Params {
    /// Measured egress ceiling in bytes/sec. `0` disables the egress gate.
    pub egress_budget_bps: u64,
    /// Concurrency high-water mark: at or above it, the node becomes pressured.
    pub max_serves_high: u32,
    /// Concurrency low-water mark: at or below it, pressure clears (hysteresis).
    pub max_serves_low: u32,
    /// Per-client concurrent-serve ceiling, enforced ONLY while pressured.
    /// `0` disables per-client fairness.
    pub per_client_cap: u32,
}

/// Sheds the miss tier under concurrency pressure and any class under egress
/// saturation, while capping a single client's share only during real
/// contention. Holds one bit of hysteresis state.
#[derive(Debug)]
pub struct ResourcePressure {
    params: Params,
    /// Latched pressure state: set at the high-water mark, cleared at the
    /// low-water mark, so the node does not flap at a single boundary.
    pressured: AtomicBool,
}

impl ResourcePressure {
    /// A policy with these thresholds, starting unpressured.
    #[must_use]
    pub const fn new(params: Params) -> Self {
        Self {
            params,
            pressured: AtomicBool::new(false),
        }
    }

    /// Update and read the hysteresis latch from the live concurrency count.
    fn update_pressure(&self, streams_in_flight: u32) -> bool {
        if streams_in_flight >= self.params.max_serves_high {
            self.pressured.store(true, Ordering::Relaxed);
        } else if streams_in_flight <= self.params.max_serves_low {
            self.pressured.store(false, Ordering::Relaxed);
        }
        self.pressured.load(Ordering::Relaxed)
    }
}

impl LoadShedPolicy for ResourcePressure {
    fn decide(&self, class: RequestClass, snap: &PressureSnapshot) -> ShedDecision {
        let pressured = self.update_pressure(snap.streams_in_flight);

        // Per-client fairness engages ONLY under pressure (fairness invariant):
        // a client is capped for holding more than its share during real
        // contention, never to reserve a slot for a client that might arrive.
        if pressured
            && self.params.per_client_cap > 0
            && snap.client_streams_in_flight >= self.params.per_client_cap
        {
            return ShedDecision::Shed(ShedReason::ClientAtCapacity);
        }

        // Hard egress ceiling: when the pipe is full, admitting another stream
        // — hit or miss — creates no bandwidth, so shed it. `0` disables.
        if self.params.egress_budget_bps > 0 && snap.egress_bps >= self.params.egress_budget_bps {
            return ShedDecision::Shed(ShedReason::EgressSaturated);
        }

        // Concurrency pressure sheds the miss tier first (it fronts upstream
        // cost and stresses disk/CPU); hits ride until egress is the constraint.
        if pressured && class == RequestClass::CacheMiss {
            return ShedDecision::Shed(ShedReason::NodeAtCapacity);
        }

        ShedDecision::Admit
    }

    fn pressure_active(&self) -> bool {
        self.pressured.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests;
