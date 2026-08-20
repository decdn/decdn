//! Load-shedding: content-agnostic overload protection for the paid serve path.
//!
//! When the node is saturated it refuses NEW serves — regardless of blob or
//! price — so in-flight streams stay fast. A refusal reuses the delivery
//! path's `NotFound`-shaped reject, so the client re-routes (ADR 037 / #1174)
//! at only a latency cost; it is not slashable and carries no reputation
//! penalty. The decision is a pluggable [`LoadShedPolicy`]; the node wiring
//! layer selects the implementation from config, mirroring the ADR 040
//! eviction/admission policy split.

mod controller;
mod egress;
mod resource_pressure;
mod state;

/// Which serve tier a request falls into, resolved where `cache.has` is known.
/// A hit is local — zero upstream cost, pure margin — so it is shed last; a
/// miss fronts origin egress / upstream USDC and stresses disk and CPU, so it
/// is shed first.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RequestClass {
    CacheHit,
    CacheMiss,
}

/// One immutable read of live node pressure, sampled per admission. New
/// signals are added as fields; a policy ignores fields it does not read, so
/// the seam is additive (spec §5).
#[derive(Debug, Clone, Copy)]
pub struct PressureSnapshot {
    /// Serve streams in flight across all clients.
    pub streams_in_flight: u32,
    /// Serve streams in flight for THIS client (fairness axis).
    pub client_streams_in_flight: u32,
    /// Rolling measured egress, bytes/sec.
    pub egress_bps: u64,
}

/// Why a request was shed. Metrics/observability granularity only — every
/// reason is `NotFound` on the wire.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShedReason {
    /// Node-wide concurrency is above the high-water mark (sheds the miss tier).
    NodeAtCapacity,
    /// Measured egress has reached the configured budget (sheds any class).
    EgressSaturated,
    /// This client already holds its fair share while the node is pressured.
    ClientAtCapacity,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShedDecision {
    Admit,
    Shed(ShedReason),
}

/// A pluggable overload-protection policy. `decide` is pure over the snapshot
/// so it is unit-testable without a running node; the node wiring layer selects
/// the implementation from config.
pub trait LoadShedPolicy: Send + Sync {
    fn decide(&self, class: RequestClass, snap: &PressureSnapshot) -> ShedDecision;

    /// Whether the policy currently considers the node pressured. Drives the
    /// `load_shed_pressure_active` gauge; defaults to `false` for policies with
    /// no pressure notion (e.g. [`AlwaysAdmit`]).
    fn pressure_active(&self) -> bool {
        false
    }
}

/// The `NoOp` policy: never sheds. A valid operator choice for a node that wants
/// no heuristic shedding. It is UNWISE under a real flood — the OS OOM-killer
/// becomes the only backstop — but physical limits bind regardless, and the
/// `ConnectionLimiter` task caps still apply beneath it, so the choice stays
/// available.
#[derive(Debug, Default, Clone, Copy)]
pub struct AlwaysAdmit;

impl LoadShedPolicy for AlwaysAdmit {
    fn decide(&self, _class: RequestClass, _snap: &PressureSnapshot) -> ShedDecision {
        ShedDecision::Admit
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn snap(n: u32, c: u32, bps: u64) -> PressureSnapshot {
        PressureSnapshot {
            streams_in_flight: n,
            client_streams_in_flight: c,
            egress_bps: bps,
        }
    }

    #[test]
    fn always_admit_admits_every_class_under_any_pressure() {
        let p = AlwaysAdmit;
        assert_eq!(
            p.decide(RequestClass::CacheHit, &snap(10_000, 9_999, u64::MAX)),
            ShedDecision::Admit
        );
        assert_eq!(
            p.decide(RequestClass::CacheMiss, &snap(10_000, 9_999, u64::MAX)),
            ShedDecision::Admit
        );
        assert!(!p.pressure_active());
    }
}
