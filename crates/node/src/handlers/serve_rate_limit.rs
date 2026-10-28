//! Three-layer token-bucket rate limiter for inbound `cdn/client/v1` requests
//! (ADR 005 §Request rate limiting).
//!
//! A thin serve-flavoured wrapper over the shared
//! [`crate::rate_limit::ThreeLayerRateLimiter`] engine, like
//! [`super::probe_rate_limit`]: it pins the serve-specific operator-visible
//! metric names (via a private metrics sink). The layering and cheapest-first
//! ordering (global → per-IP → per-peer) live in the shared engine.
//!
//! The client handler charges one token per stream, before it reads the
//! request. A refused stream is reset with `RATE_LIMITED` and gets no signed
//! response, so a flood of unpaid requests reaches neither a signature, an
//! origin `HEAD` nor a chain read.

use std::sync::Arc;

use crate::metrics::Metrics;
use crate::rate_limit::{
    RateLimitConfig, RateLimitMetricsSink, RejectLayer, ThreeLayerRateLimiter,
};

/// Routes shared-limiter metric events to the serve operator-visible counters
/// (`decdn_serve_rate_limit_*`), like the probe and DHT sinks, with one
/// difference: a refusal is not counted here. A refused stream is an inbound
/// failure, and the client handler's dispatch sink counts every failed end on
/// exactly one reason ([`crate::metrics::INBOUND_FAILURE_REASONS`]), so it
/// counts the refusal on the layer's `rejected_*` counter when the stream ends.
struct ServeRateLimitMetrics(Arc<Metrics>);

impl RateLimitMetricsSink for ServeRateLimitMetrics {
    fn rejected(&self, _layer: RejectLayer) {}

    fn prune_sweep_per_ip(&self) {
        self.0.serve_rate_limit_prune_sweep_per_ip();
    }

    fn prune_sweep_per_peer(&self) {
        self.0.serve_rate_limit_prune_sweep_per_peer();
    }

    fn set_tracked_per_ip(&self, n: usize) {
        self.0.serve_rate_limit_tracked_per_ip_set(n);
    }

    fn set_tracked_per_peer(&self, n: usize) {
        self.0.serve_rate_limit_tracked_per_peer_set(n);
    }
}

/// Three-layer rate limiter for inbound `cdn/client/v1` requests. Newtype over
/// the shared [`ThreeLayerRateLimiter`] that injects the serve metrics sink.
#[allow(missing_debug_implementations)]
pub struct ServeRateLimiter(ThreeLayerRateLimiter);

impl ServeRateLimiter {
    /// Build a limiter straight from the resolved `[serve.rate_limit]` section.
    /// Taking `&ResolvedServe` keeps another protocol's section from being
    /// passed in by mistake (#1457).
    #[must_use]
    pub fn from_resolved(
        resolved: &decdn_common::config::ResolvedServe,
        metrics: Arc<Metrics>,
    ) -> Self {
        Self(ThreeLayerRateLimiter::new(
            &RateLimitConfig::from(resolved),
            Arc::new(ServeRateLimitMetrics(metrics)),
        ))
    }

    /// Try to admit one inbound request. See [`ThreeLayerRateLimiter::check`].
    ///
    /// # Errors
    ///
    /// Returns the layer that refused the request.
    pub fn check(
        &self,
        peer_node_id: &crate::dht::routing::NodeId,
        peer_ip: Option<std::net::IpAddr>,
    ) -> Result<(), RejectLayer> {
        self.0.check(peer_node_id, peer_ip)
    }

    /// Periodic GC sweep for the per-IP keyed map.
    #[must_use]
    pub fn gc_per_ip(&self) -> Option<(usize, usize)> {
        self.0.gc_per_ip()
    }

    /// Periodic GC sweep for the per-peer keyed map.
    #[must_use]
    pub fn gc_per_peer(&self) -> Option<(usize, usize)> {
        self.0.gc_per_peer()
    }
}
