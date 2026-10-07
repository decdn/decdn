//! Three-layer token-bucket rate limiter for `cdn/dht/v1` inbound traffic
//! (ADR 022 §DHT Rate Limiting).
//!
//! This is a thin DHT-flavoured wrapper over the shared
//! [`crate::rate_limit::ThreeLayerRateLimiter`] engine: it pins the
//! DHT-specific operator-visible metric names (via a private metrics sink)
//! and re-exports the config/reject types under their historical DHT names.
//! The layering, cheapest-first ordering, keyspace bound, and batch-token
//! accounting all live in the shared engine — see its
//! module docs.

use std::sync::Arc;

use crate::metrics::Metrics;
use crate::rate_limit::{
    RateLimitConfig, RateLimitMetricsSink, RejectLayer, ThreeLayerRateLimiter,
};

#[cfg(test)]
use std::net::IpAddr;
#[cfg(test)]
use std::time::Duration;

#[cfg(test)]
use crate::dht::routing::NodeId;

/// Resolved DHT rate-limit configuration. See ADR 022 §DHT Rate Limiting.
/// [`Default`] returns the ADR 022 values (per-peer 20/40, per-IP 100/200,
/// global 1000/2000).
pub type DhtRateLimitConfig = RateLimitConfig;

/// Layer that triggered a `cdn/dht/v1` rejection. Alias of the shared
/// [`RejectLayer`]; the `per_peer`/`per_ip`/`global` label strings are stable.
pub type DhtRejectLayer = RejectLayer;

/// Routes shared-limiter metric events to the DHT operator-visible counters
/// (`decdn_dht_rate_limit_*`): one unlabeled Counter per layer, per the
/// sibling-counter convention recorded in
/// `adr/appendix-observability.md` § Reason splits. Operators recover the
/// rolled-up rate with
/// `sum(rate({__name__=~"decdn_dht_rate_limit_rejected_(per_peer|per_ip|global)_total"}[1m]))`.
///
/// **This is a choice, not a backend limitation** — `iroh_metrics` supports
/// per-field labels: `DecdnMetrics::probe_hold_unavailable` and
/// `DecdnMetrics::streams_active` are both `Family<L, M>` (private fields; see
/// [`crate::metrics::Metrics::probe_hold_unavailable`] for the accessor that
/// uses one). Each layer here has an unrelated remedy (one abusive
/// peer, one abusive host, aggregate load), so no alert spans the family and a
/// shared label would buy nothing. The appendix registry carries this trio of
/// unlabelled per-layer counters rather than a single labelled
/// `decdn_dht_rate_limit_rejections_total`.
struct DhtRateLimitMetrics(Arc<Metrics>);

impl RateLimitMetricsSink for DhtRateLimitMetrics {
    fn rejected(&self, layer: RejectLayer) {
        match layer {
            RejectLayer::Global => self.0.dht_rate_limit_rejected_global(),
            RejectLayer::PerIp => self.0.dht_rate_limit_rejected_per_ip(),
            RejectLayer::PerPeer => self.0.dht_rate_limit_rejected_per_peer(),
        }
    }

    fn prune_sweep_per_ip(&self) {
        self.0.dht_rate_limit_prune_sweep_per_ip();
    }

    fn prune_sweep_per_peer(&self) {
        self.0.dht_rate_limit_prune_sweep_per_peer();
    }

    fn set_tracked_per_ip(&self, n: usize) {
        self.0.dht_rate_limit_tracked_per_ip_set(n);
    }

    fn set_tracked_per_peer(&self, n: usize) {
        self.0.dht_rate_limit_tracked_per_peer_set(n);
    }
}

/// Three-layer rate limiter for inbound `cdn/dht/v1` requests. Newtype over the
/// shared [`ThreeLayerRateLimiter`] that injects the DHT metrics sink.
#[allow(missing_debug_implementations)]
pub struct DhtRateLimiter(ThreeLayerRateLimiter);

impl DhtRateLimiter {
    /// Build a limiter from the resolved DHT config snapshot.
    #[must_use]
    pub fn new(cfg: &DhtRateLimitConfig, metrics: Arc<Metrics>) -> Self {
        Self(ThreeLayerRateLimiter::new(
            cfg,
            Arc::new(DhtRateLimitMetrics(metrics)),
        ))
    }

    /// Build a limiter straight from the resolved `[dht.rate_limit]` section.
    ///
    /// Prefer this over [`Self::new`] at wiring sites: `DhtRateLimitConfig` is
    /// an alias of the shared [`RateLimitConfig`], so `new` cannot tell a config
    /// mapped from `[dht.rate_limit]` from one mapped out of
    /// `[probe.rate_limit]`. Taking `&ResolvedDht` makes the wrong pairing a
    /// type error (#1457); see
    /// [`crate::handlers::probe_rate_limit::ProbeRateLimiter::from_resolved`]
    /// for the direction that actually loosens a cap.
    #[must_use]
    pub fn from_resolved(
        resolved: &decdn_common::config::ResolvedDht,
        metrics: Arc<Metrics>,
    ) -> Self {
        Self::new(&RateLimitConfig::from(resolved), metrics)
    }

    /// Try to admit one inbound DHT request. See
    /// [`ThreeLayerRateLimiter::check`].
    pub fn check(
        &self,
        peer_node_id: &crate::dht::routing::NodeId,
        peer_ip: Option<std::net::IpAddr>,
    ) -> Result<(), DhtRejectLayer> {
        self.0.check(peer_node_id, peer_ip)
    }

    /// Stage 2 of batch admission. See
    /// [`ThreeLayerRateLimiter::admit_batch_extra`].
    #[must_use]
    pub fn admit_batch_extra(
        &self,
        peer_node_id: &crate::dht::routing::NodeId,
        peer_ip: Option<std::net::IpAddr>,
        extra: usize,
    ) -> usize {
        self.0.admit_batch_extra(peer_node_id, peer_ip, extra)
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
