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
/// **This is a choice, not a backend limitation** — an earlier version of this
/// comment claimed `iroh_metrics` has no per-field labels, which is false:
/// `DecdnMetrics::probe_hold_unavailable` and `DecdnMetrics::streams_active` are
/// both `Family<L, M>` (private fields; see
/// [`crate::metrics::Metrics::probe_hold_unavailable`] for the accessor that
/// uses one). Each layer here has an unrelated remedy (one abusive
/// peer, one abusive host, aggregate load), so no alert spans the family and a
/// shared label would buy nothing. ADR 005 § Observability originally specified
/// a single labelled `decdn_probe_rate_limit_rejections_total`; that name was
/// never exported and the appendix registry now carries this trio instead.
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
mod tests {
    use super::*;
    use crate::dht::routing::NodeId;
    use std::net::{IpAddr, Ipv4Addr};

    fn metrics() -> Arc<Metrics> {
        Arc::new(Metrics::new())
    }

    fn peer(byte: u8) -> NodeId {
        NodeId::from_bytes([byte; 32])
    }

    fn ip(a: u8, b: u8, c: u8, d: u8) -> IpAddr {
        IpAddr::V4(Ipv4Addr::new(a, b, c, d))
    }

    /// ADR 005 §Probe rate limiting defaults: per-peer 5/5, per-IP 50/200,
    /// global 1000/2000. The shared engine logic is covered exhaustively by
    /// the DHT wrapper's suite; here we pin the probe-specific behaviour — that
    /// the per-peer burst of 5 fires on the 6th same-peer probe — to catch a
    /// regression in how the probe defaults are wired through this newtype.
    ///
    /// Routed through `ResolvedProbe::default()` and the real `From` mapping
    /// rather than restating the eight literals, so these behavioural tests
    /// exercise the same path production takes. A default that drifted out of
    /// ADR 005 shows up as a burst assertion failing here, not as a fixture
    /// that quietly disagrees with the resolver.
    fn adr005_default_cfg() -> ProbeRateLimitConfig {
        ProbeRateLimitConfig::from(&decdn_common::config::ResolvedProbe::default())
    }

    #[test]
    fn per_peer_burst_of_five_admits_five_then_rejects() {
        let lim = ProbeRateLimiter::new(&adr005_default_cfg(), metrics());
        let p = peer(1);
        let i = Some(ip(10, 0, 0, 1));
        // Five probes fit the per-peer burst of 5.
        for n in 0..5 {
            assert_eq!(lim.check(&p, i), Ok(()), "probe {n} within burst");
        }
        // The sixth from the same NodeId exhausts the per-peer bucket. Per-IP
        // (burst 200) and global (burst 2000) still have headroom, so per-peer
        // is unambiguously the layer that fires.
        assert_eq!(lim.check(&p, i), Err(ProbeRejectLayer::PerPeer));
    }

    /// The probe sink must drive the `decdn_probe_rate_limit_*` counters — a
    /// regression that wired the probe newtype to the DHT sink would bump
    /// `decdn_dht_rate_limit_*` instead and leave the probe scrape silent.
    #[test]
    fn rejection_increments_probe_scrape_counter_not_dht() {
        let mut cfg = adr005_default_cfg();
        // Strict per-peer so the second same-peer probe rejects; pin refill to
        // ~zero so a slow scheduler can't refill mid-test.
        cfg.per_peer_burst = 1;
        cfg.per_peer_rate_per_sec = 1e-9;
        let metrics = metrics();
        let lim = ProbeRateLimiter::new(&cfg, Arc::clone(&metrics));
        let p = peer(1);
        let i = Some(ip(10, 0, 0, 1));
        assert_eq!(lim.check(&p, i), Ok(()));
        assert_eq!(lim.check(&p, i), Err(ProbeRejectLayer::PerPeer));
        let text = metrics.encode().unwrap();
        assert!(
            text.contains("decdn_probe_rate_limit_rejected_per_peer_total 1"),
            "probe per-peer rejection must appear in /metrics; got:\n{text}"
        );
        assert!(
            text.contains("decdn_dht_rate_limit_rejected_per_peer_total 0"),
            "probe rejection must NOT bump the DHT counter; got:\n{text}"
        );
    }

    /// Pins the per-IP scrape counter: the `ProbeRateLimitMetrics::rejected`
    /// match is hand-written, so a copy-paste swap of its `PerIp` arm would
    /// otherwise go uncaught (the per-peer arm is pinned by the test above,
    /// the global arm by the test below).
    #[test]
    fn per_ip_rejection_drives_probe_per_ip_counter() {
        let mut cfg = adr005_default_cfg();
        // Per-IP burst=1 with ~zero refill: the second distinct peer from the
        // same IP can only fail the per-IP layer.
        cfg.per_ip_burst = 1;
        cfg.per_ip_rate_per_sec = 1e-9;
        let metrics = metrics();
        let lim = ProbeRateLimiter::new(&cfg, Arc::clone(&metrics));
        let i = Some(ip(10, 0, 0, 2));
        assert_eq!(lim.check(&peer(3), i), Ok(()));
        assert_eq!(lim.check(&peer(4), i), Err(ProbeRejectLayer::PerIp));
        let text = metrics.encode().unwrap();
        assert!(
            text.contains("decdn_probe_rate_limit_rejected_per_ip_total 1"),
            "probe per-IP rejection must drive the per-IP counter; got:\n{text}"
        );
    }

    /// A global-cap rejection drives the probe global counter — the third arm
    /// of the hand-written `ProbeRateLimitMetrics::rejected` match. With global
    /// burst=1 (no refill) and the other layers loose, the second probe from a
    /// distinct peer+IP can only fail the global layer.
    #[test]
    fn global_rejection_drives_probe_global_counter() {
        let mut cfg = adr005_default_cfg();
        cfg.global_burst = 1;
        cfg.global_rate_per_sec = 1e-9;
        let metrics = metrics();
        let lim = ProbeRateLimiter::new(&cfg, Arc::clone(&metrics));
        assert_eq!(lim.check(&peer(1), Some(ip(10, 0, 0, 1))), Ok(()));
        assert_eq!(
            lim.check(&peer(2), Some(ip(10, 0, 0, 2))),
            Err(ProbeRejectLayer::Global)
        );
        let text = metrics.encode().unwrap();
        assert!(
            text.contains("decdn_probe_rate_limit_rejected_global_total 1"),
            "probe global rejection must drive the global counter; got:\n{text}"
        );
    }
}
