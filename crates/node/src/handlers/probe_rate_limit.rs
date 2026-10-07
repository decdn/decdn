//! Three-layer token-bucket rate limiter for `cdn/probe/v1` inbound traffic
//! (ADR 005 §Probe rate limiting).
//!
//! A thin probe-flavoured wrapper over the shared
//! [`crate::rate_limit::ThreeLayerRateLimiter`] engine: it pins the
//! probe-specific operator-visible metric names (via a private metrics sink)
//! and re-exports the config/reject types under probe names. The layering and
//! cheapest-first ordering (global → per-IP → per-peer) live in the shared
//! engine — see its module docs.
//!
//! ## Relationship to [`crate::dispatch::ConnectionLimiter`]
//!
//! The probe handler runs **both** limiters: it still acquires a
//! `ConnectionLimiter` permit (the global concurrency semaphore + the shared
//! per-source IP bucket that gates every ALPN) and then calls
//! [`ProbeRateLimiter::check`] for the ADR 005 three-layer token buckets. The
//! per-source IP layer and this limiter's per-IP layer therefore both charge a
//! token per probe — a benign overlap: the ADR 005 probe per-IP default
//! (50/s) is stricter than the `security.per_source_*` default (100/s), so the
//! probe layer dominates, and the per-peer (`NodeId`) layer ADR 005 mandates
//! exists **only** here. `cdn/probe/v1` is one connection / one stream / one
//! probe, so a single `check` per accepted connection is the whole budget.

use std::sync::Arc;

use crate::metrics::Metrics;
use crate::rate_limit::{
    RateLimitConfig, RateLimitMetricsSink, RejectLayer, ThreeLayerRateLimiter,
};

/// Resolved probe rate-limit configuration. See ADR 005 §Probe rate limiting.
/// The ADR 005 defaults (per-peer 5/5, per-IP 50/200, global 1000/2000) are
/// supplied by the config resolver, not by [`Default`] (which returns the DHT
/// values shared across the engine).
pub type ProbeRateLimitConfig = RateLimitConfig;

/// Layer that triggered a `cdn/probe/v1` rejection. Alias of the shared
/// [`RejectLayer`]; the `per_peer`/`per_ip`/`global` label strings are stable.
pub type ProbeRejectLayer = RejectLayer;

/// Routes shared-limiter metric events to the probe operator-visible counters
/// (`decdn_probe_rate_limit_*`): one unlabeled Counter per layer, per the
/// sibling-counter convention settled in #1475 and recorded in
/// `adr/appendix-observability.md` § Reason splits. Operators recover the
/// rolled-up rate with
/// `sum(rate({__name__=~"decdn_probe_rate_limit_rejected_(per_peer|per_ip|global)_total"}[1m]))`.
///
/// **This is a choice, not a backend limitation.** `iroh_metrics` supports
/// per-field labels:
/// `DecdnMetrics::probe_hold_unavailable` and `DecdnMetrics::streams_active` are
/// both `Family<L, M>` (private fields; see
/// [`crate::metrics::Metrics::probe_hold_unavailable`] for the accessor that
/// uses one). Each layer here has an unrelated remedy (one abusive
/// peer, one abusive host, aggregate load), so no alert spans the family and a
/// shared label would buy nothing. The appendix registry carries this trio of
/// unlabelled counters (ADR 005 § Observability).
struct ProbeRateLimitMetrics(Arc<Metrics>);

impl RateLimitMetricsSink for ProbeRateLimitMetrics {
    fn rejected(&self, layer: RejectLayer) {
        match layer {
            RejectLayer::Global => self.0.probe_rate_limit_rejected_global(),
            RejectLayer::PerIp => self.0.probe_rate_limit_rejected_per_ip(),
            RejectLayer::PerPeer => self.0.probe_rate_limit_rejected_per_peer(),
        }
    }

    fn prune_sweep_per_ip(&self) {
        self.0.probe_rate_limit_prune_sweep_per_ip();
    }

    fn prune_sweep_per_peer(&self) {
        self.0.probe_rate_limit_prune_sweep_per_peer();
    }

    fn set_tracked_per_ip(&self, n: usize) {
        self.0.probe_rate_limit_tracked_per_ip_set(n);
    }

    fn set_tracked_per_peer(&self, n: usize) {
        self.0.probe_rate_limit_tracked_per_peer_set(n);
    }
}

/// Three-layer rate limiter for inbound `cdn/probe/v1` requests. Newtype over
/// the shared [`ThreeLayerRateLimiter`] that injects the probe metrics sink.
#[allow(missing_debug_implementations)]
pub struct ProbeRateLimiter(ThreeLayerRateLimiter);

impl ProbeRateLimiter {
    /// Build a limiter from the resolved probe config snapshot.
    #[must_use]
    pub fn new(cfg: &ProbeRateLimitConfig, metrics: Arc<Metrics>) -> Self {
        Self(ThreeLayerRateLimiter::new(
            cfg,
            Arc::new(ProbeRateLimitMetrics(metrics)),
        ))
    }

    /// Build a limiter straight from the resolved `[probe.rate_limit]` section.
    ///
    /// Prefer this over [`Self::new`] at wiring sites. `ProbeRateLimitConfig`
    /// is an alias of the shared [`RateLimitConfig`], so `new` accepts a config
    /// mapped from *either* protocol's resolved section — building the probe
    /// limiter from `[dht.rate_limit]` type-checks fine and would quietly
    /// quadruple the per-peer probe cap (ADR 022's 20/sec in place of ADR 005's
    /// 5/sec). Taking `&ResolvedProbe` makes that a type error instead (#1457).
    #[must_use]
    pub fn from_resolved(
        resolved: &decdn_common::config::ResolvedProbe,
        metrics: Arc<Metrics>,
    ) -> Self {
        Self::new(&RateLimitConfig::from(resolved), metrics)
    }

    /// Try to admit one inbound probe. See [`ThreeLayerRateLimiter::check`].
    pub fn check(
        &self,
        peer_node_id: &crate::dht::routing::NodeId,
        peer_ip: Option<std::net::IpAddr>,
    ) -> Result<(), ProbeRejectLayer> {
        self.0.check(peer_node_id, peer_ip)
    }

    /// Periodic GC sweep for the per-IP keyed map (#645).
    #[must_use]
    pub fn gc_per_ip(&self) -> Option<(usize, usize)> {
        self.0.gc_per_ip()
    }

    /// Periodic GC sweep for the per-peer keyed map (#645).
    #[must_use]
    pub fn gc_per_peer(&self) -> Option<(usize, usize)> {
        self.0.gc_per_peer()
    }

    /// Current tracked-key count for the per-IP keyed map (#645).
    #[must_use]
    pub fn per_ip_tracked(&self) -> usize {
        self.0.per_ip_tracked()
    }

    /// Current tracked-key count for the per-peer keyed map (#645).
    #[must_use]
    pub fn per_peer_tracked(&self) -> usize {
        self.0.per_peer_tracked()
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests;
