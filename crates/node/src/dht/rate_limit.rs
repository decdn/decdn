//! Three-layer token-bucket rate limiter for `cdn/dht/v1` inbound traffic
//! (ADR 022 §DHT Rate Limiting).
//!
//! Distinct from the connection-level [`crate::dispatch::ConnectionLimiter`]:
//! that limiter caps how many connections we accept; this one caps the
//! per-request work done on each accepted connection. Both run — the
//! connection limiter rejects floods at handshake, this layer rejects
//! per-request floods on a successfully accepted connection.
//!
//! # Layered checks
//!
//! Layers fire **global → per-IP → per-peer** (cheapest-first ordering, ADR
//! 022 §DHT Rate Limiting). A request rejected by the global cap never
//! pays the per-IP map lookup; a per-IP rejection never pays the per-peer
//! map lookup. The first rejection short-circuits and is the only one
//! counted in one of
//! `decdn_dht_rate_limit_rejected_{per_peer,per_ip,global}_total` — one
//! Counter per layer (the iroh-metrics backend does not support per-field
//! labels, so we use distinct counters; see `Metrics::dht_rate_limit_rejected_*`
//! for the deviation rationale from ADR 022 §Observability's labeled-counter
//! shape and the rolled-up Prometheus query operators can use).
//!
//! # Trusted-IP exemption
//!
//! Operators MAY exempt source IPs from the per-IP layer only — peer
//! operators with predictable cross-peer DHT traffic, in-cluster
//! monitoring, etc. The exemption explicitly does NOT bypass per-peer or
//! global; a single misbehaving `NodeId` at a trusted IP is still rate
//! limited, and a global flood from many trusted IPs still hits the
//! global cap.

use std::collections::HashSet;
use std::net::IpAddr;
use std::num::NonZeroU32;
use std::sync::Arc;
use std::time::Duration;

use governor::{DefaultDirectRateLimiter, DefaultKeyedRateLimiter, Quota};

use crate::dht::routing::NodeId;
use crate::metrics::Metrics;

/// Resolved DHT rate-limit configuration. See ADR 022 §DHT Rate Limiting.
///
/// Each `*_rate_per_sec == 0.0` and the corresponding `*_burst == 0`
/// disables that layer (operator opt-out). `max_concurrent_handlers`-style
/// disable semantics, but per-layer.
#[derive(Debug, Clone)]
pub struct DhtRateLimitConfig {
    /// Per-peer (source `NodeId`) sustained rate (requests/second). ADR 022
    /// default: 20.
    pub per_peer_rate_per_sec: f64,
    /// Per-peer burst capacity. ADR 022 default: 40.
    pub per_peer_burst: u32,
    /// Per-IP sustained rate (requests/second). ADR 022 default: 100.
    pub per_ip_rate_per_sec: f64,
    /// Per-IP burst capacity. ADR 022 default: 200.
    pub per_ip_burst: u32,
    /// Global inbound DHT sustained rate (requests/second). ADR 022
    /// default: 1000.
    pub global_rate_per_sec: f64,
    /// Global inbound DHT burst capacity. ADR 022 default: 2000.
    pub global_burst: u32,
    /// IPs that bypass the per-IP layer only (per-peer + global still
    /// apply). Per ADR 022 §Trusted-IP exemption.
    pub trusted_ips: HashSet<IpAddr>,
}

impl Default for DhtRateLimitConfig {
    fn default() -> Self {
        Self {
            per_peer_rate_per_sec: 20.0,
            per_peer_burst: 40,
            per_ip_rate_per_sec: 100.0,
            per_ip_burst: 200,
            global_rate_per_sec: 1000.0,
            global_burst: 2000,
            trusted_ips: HashSet::new(),
        }
    }
}

/// Layer that triggered a rejection. Used as a log/trace field and to
/// pick the right per-layer Counter to bump
/// (`decdn_dht_rate_limit_rejected_{per_peer,per_ip,global}_total`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DhtRejectLayer {
    /// Per-peer (`NodeId`) bucket exhausted.
    PerPeer,
    /// Per-IP bucket exhausted.
    PerIp,
    /// Global cap exhausted.
    Global,
}

impl DhtRejectLayer {
    /// Label string for log/metric fields. Stable across releases.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::PerPeer => "per_peer",
            Self::PerIp => "per_ip",
            Self::Global => "global",
        }
    }
}

/// Three-layer rate limiter for inbound `cdn/dht/v1` requests.
///
/// Methods are `&self` — concurrent admission decisions don't take a write
/// lock. Each layer uses its own `governor` limiter and the
/// `Result<(), DhtRejectLayer>` short-circuits at the first rejection.
#[allow(missing_debug_implementations)]
pub struct DhtRateLimiter {
    /// `None` when the layer is disabled. Built once at construction; not
    /// hot-reloadable in this PR (operators can restart). When made
    /// reloadable, follow the `ConnectionLimiter` pattern of `ArcSwap` on
    /// the inner limiter.
    global: Option<Arc<DefaultDirectRateLimiter>>,
    /// Keyed by source IP. `None` when disabled.
    per_ip: Option<Arc<DefaultKeyedRateLimiter<IpAddr>>>,
    /// Keyed by source `NodeId`. `None` when disabled.
    per_peer: Option<Arc<DefaultKeyedRateLimiter<NodeId>>>,
    trusted_ips: HashSet<IpAddr>,
    metrics: Arc<Metrics>,
}

impl DhtRateLimiter {
    /// Build a limiter from the resolved config snapshot.
    #[must_use]
    pub fn new(cfg: &DhtRateLimitConfig, metrics: Arc<Metrics>) -> Self {
        Self {
            global: build_direct_limiter(cfg.global_rate_per_sec, cfg.global_burst),
            per_ip: build_keyed_limiter(cfg.per_ip_rate_per_sec, cfg.per_ip_burst),
            per_peer: build_keyed_limiter(cfg.per_peer_rate_per_sec, cfg.per_peer_burst),
            trusted_ips: cfg.trusted_ips.clone(),
            metrics,
        }
    }

    /// Try to admit one inbound DHT request. `peer_ip == None` for
    /// relay-only connections — the per-IP layer is skipped (no key to
    /// charge), matching the `ConnectionLimiter` precedent at
    /// [`crate::dispatch::ConnectionLimiter::acquire`].
    ///
    /// On rejection, increments the appropriate per-layer counter
    /// (`decdn_dht_rate_limit_rejected_{per_peer,per_ip,global}_total`)
    /// and returns the layer that fired. No counter increment on the
    /// success path.
    ///
    /// Cheapest-first ordering (global → per-IP → per-peer): the first
    /// layer to reject short-circuits, so a per-peer-flooded request that
    /// would also have failed the global cap counts as `global` (the
    /// layer that actually drained the budget).
    pub fn check(
        &self,
        peer_node_id: &NodeId,
        peer_ip: Option<IpAddr>,
    ) -> Result<(), DhtRejectLayer> {
        // Layer 1 — global.
        if let Some(g) = self.global.as_ref()
            && g.check().is_err()
        {
            self.metrics.dht_rate_limit_rejected_global();
            return Err(DhtRejectLayer::Global);
        }

        // Layer 2 — per-IP. Skipped for relay-only connections and for
        // trusted IPs.
        if let (Some(ip), Some(limiter)) = (peer_ip, self.per_ip.as_ref())
            && !self.trusted_ips.contains(&ip)
            && limiter.check_key(&ip).is_err()
        {
            self.metrics.dht_rate_limit_rejected_per_ip();
            return Err(DhtRejectLayer::PerIp);
        }

        // Layer 3 — per-peer (NodeId).
        if let Some(limiter) = self.per_peer.as_ref()
            && limiter.check_key(peer_node_id).is_err()
        {
            self.metrics.dht_rate_limit_rejected_per_peer();
            return Err(DhtRejectLayer::PerPeer);
        }

        Ok(())
    }
}

/// Build a non-keyed (global) governor limiter from a (rate, burst) pair,
/// or `None` when the layer is disabled. Mirrors the private
/// `build_keyed_limiter` helper in [`crate::dispatch`] — same `0.0` /
/// non-finite-rate / zero-burst guards.
fn build_direct_limiter(rate_per_sec: f64, burst: u32) -> Option<Arc<DefaultDirectRateLimiter>> {
    let quota = make_quota(rate_per_sec, burst)?;
    Some(Arc::new(governor::RateLimiter::direct(quota)))
}

fn build_keyed_limiter<K>(rate_per_sec: f64, burst: u32) -> Option<Arc<DefaultKeyedRateLimiter<K>>>
where
    K: std::hash::Hash + Eq + Clone + Send + Sync + 'static,
{
    let quota = make_quota(rate_per_sec, burst)?;
    Some(Arc::new(DefaultKeyedRateLimiter::keyed(quota)))
}

fn make_quota(rate_per_sec: f64, burst: u32) -> Option<Quota> {
    if rate_per_sec <= 0.0 || burst == 0 || !rate_per_sec.is_finite() {
        return None;
    }
    let period_secs = 1.0_f64 / rate_per_sec;
    // Saturate to at least 1ns: a positive but extremely high rate would
    // otherwise round to 0ns and `Quota::with_period(0)` returns None.
    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    let period_ns = (period_secs * 1_000_000_000.0).max(1.0) as u64;
    let period = Duration::from_nanos(period_ns);
    let burst_nz = NonZeroU32::new(burst)?;
    Some(Quota::with_period(period)?.allow_burst(burst_nz))
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use std::net::Ipv4Addr;

    fn metrics() -> Arc<Metrics> {
        Arc::new(Metrics::new())
    }

    fn peer(byte: u8) -> NodeId {
        [byte; 32]
    }

    fn ip(a: u8, b: u8, c: u8, d: u8) -> IpAddr {
        IpAddr::V4(Ipv4Addr::new(a, b, c, d))
    }

    fn strict_cfg() -> DhtRateLimitConfig {
        DhtRateLimitConfig {
            per_peer_rate_per_sec: 1.0,
            per_peer_burst: 1,
            per_ip_rate_per_sec: 1.0,
            per_ip_burst: 1,
            global_rate_per_sec: 1.0,
            global_burst: 1,
            trusted_ips: HashSet::new(),
        }
    }

    #[test]
    fn admits_first_request_under_default_caps() {
        let lim = DhtRateLimiter::new(&DhtRateLimitConfig::default(), metrics());
        assert_eq!(lim.check(&peer(1), Some(ip(10, 0, 0, 1))), Ok(()));
    }

    #[test]
    fn per_peer_layer_rejects_after_burst() {
        let mut cfg = strict_cfg();
        // Loosen the global + per-IP layers so we're testing per-peer alone.
        cfg.global_burst = u32::MAX;
        cfg.global_rate_per_sec = 1e9;
        cfg.per_ip_burst = u32::MAX;
        cfg.per_ip_rate_per_sec = 1e9;
        let lim = DhtRateLimiter::new(&cfg, metrics());
        let p = peer(1);
        let i = Some(ip(10, 0, 0, 1));
        assert_eq!(lim.check(&p, i), Ok(()));
        assert_eq!(lim.check(&p, i), Err(DhtRejectLayer::PerPeer));
    }

    #[test]
    fn per_ip_layer_rejects_after_burst() {
        let mut cfg = strict_cfg();
        cfg.global_burst = u32::MAX;
        cfg.global_rate_per_sec = 1e9;
        cfg.per_peer_burst = u32::MAX;
        cfg.per_peer_rate_per_sec = 1e9;
        let lim = DhtRateLimiter::new(&cfg, metrics());
        let i = Some(ip(10, 0, 0, 1));
        // Different peers from the same IP: first admits, second hits per-IP.
        assert_eq!(lim.check(&peer(1), i), Ok(()));
        assert_eq!(lim.check(&peer(2), i), Err(DhtRejectLayer::PerIp));
    }

    #[test]
    fn global_layer_rejects_after_burst() {
        let mut cfg = strict_cfg();
        cfg.per_peer_burst = u32::MAX;
        cfg.per_peer_rate_per_sec = 1e9;
        cfg.per_ip_burst = u32::MAX;
        cfg.per_ip_rate_per_sec = 1e9;
        let lim = DhtRateLimiter::new(&cfg, metrics());
        // Different peer AND IP every time — only the global cap can fire.
        assert_eq!(lim.check(&peer(1), Some(ip(10, 0, 0, 1))), Ok(()));
        assert_eq!(
            lim.check(&peer(2), Some(ip(10, 0, 0, 2))),
            Err(DhtRejectLayer::Global)
        );
    }

    #[test]
    fn cheapest_first_global_short_circuits_per_peer() {
        // burst=1 everywhere. The first request consumes the global bucket;
        // the second from the same peer+IP must be reported as global
        // (cheapest layer), not per-peer or per-ip.
        let lim = DhtRateLimiter::new(&strict_cfg(), metrics());
        assert_eq!(lim.check(&peer(1), Some(ip(10, 0, 0, 1))), Ok(()));
        assert_eq!(
            lim.check(&peer(1), Some(ip(10, 0, 0, 1))),
            Err(DhtRejectLayer::Global)
        );
    }

    #[test]
    fn relay_only_connection_skips_per_ip_layer() {
        let mut cfg = strict_cfg();
        // Set per-IP very strict but everything else loose: a relay-only
        // peer (peer_ip = None) must still be admitted because per-IP is
        // skipped when no key is available.
        cfg.global_burst = u32::MAX;
        cfg.global_rate_per_sec = 1e9;
        cfg.per_peer_burst = u32::MAX;
        cfg.per_peer_rate_per_sec = 1e9;
        let lim = DhtRateLimiter::new(&cfg, metrics());
        for _ in 0..16 {
            assert_eq!(lim.check(&peer(1), None), Ok(()));
        }
    }

    #[test]
    fn trusted_ip_bypasses_per_ip_layer_only() {
        // Strict per-IP. Different peers from the same IP normally would
        // hit per-IP on the second call; if the IP is trusted they pass.
        let mut cfg = strict_cfg();
        cfg.global_burst = u32::MAX;
        cfg.global_rate_per_sec = 1e9;
        cfg.per_peer_burst = u32::MAX;
        cfg.per_peer_rate_per_sec = 1e9;
        cfg.trusted_ips.insert(ip(10, 0, 0, 1));
        let lim = DhtRateLimiter::new(&cfg, metrics());
        assert_eq!(lim.check(&peer(1), Some(ip(10, 0, 0, 1))), Ok(()));
        assert_eq!(lim.check(&peer(2), Some(ip(10, 0, 0, 1))), Ok(()));
        // Untrusted IP still rejects.
        assert_eq!(lim.check(&peer(3), Some(ip(10, 0, 0, 2))), Ok(()));
        assert_eq!(
            lim.check(&peer(4), Some(ip(10, 0, 0, 2))),
            Err(DhtRejectLayer::PerIp)
        );
    }

    #[test]
    fn trusted_ip_does_not_bypass_per_peer_or_global() {
        let mut cfg = strict_cfg();
        cfg.trusted_ips.insert(ip(10, 0, 0, 1));
        let lim = DhtRateLimiter::new(&cfg, metrics());
        assert_eq!(lim.check(&peer(1), Some(ip(10, 0, 0, 1))), Ok(()));
        // The trusted IP burned global=1; the second request fails on global.
        assert_eq!(
            lim.check(&peer(2), Some(ip(10, 0, 0, 1))),
            Err(DhtRejectLayer::Global)
        );
    }

    #[test]
    fn rejection_increments_layer_metric_in_scrape() {
        // ADR 022 §Observability specifies a single labeled counter
        // (`decdn_dht_rate_limit_rejections_total{layer=...}`), but the
        // codebase's metrics backend (`iroh_metrics::MetricsGroup`) does
        // not support per-field labels. We follow the existing convention
        // from the dispatch counters (`dispatch_rejected_global` /
        // `dispatch_rejected_per_source`) and surface one Counter per
        // layer. Operators with the ADR-022 alert form do
        // `sum(rate(decdn_dht_rate_limit_rejected_{per_peer,per_ip,global}_total[1m]))`
        // to recover the rolled-up rate; per-layer alerts are unaffected.
        let metrics = metrics();
        let lim = DhtRateLimiter::new(&strict_cfg(), Arc::clone(&metrics));
        lim.check(&peer(1), Some(ip(10, 0, 0, 1)))
            .expect("first ok");
        let _ = lim.check(&peer(1), Some(ip(10, 0, 0, 1)));
        let text = metrics.encode().unwrap();
        assert!(
            text.contains("decdn_dht_rate_limit_rejected_global_total 1"),
            "global rejection must appear in /metrics; got:\n{text}"
        );
    }

    #[test]
    fn disabled_layer_admits_all() {
        let cfg = DhtRateLimitConfig {
            per_peer_rate_per_sec: 0.0,
            per_peer_burst: 0,
            per_ip_rate_per_sec: 0.0,
            per_ip_burst: 0,
            global_rate_per_sec: 0.0,
            global_burst: 0,
            trusted_ips: HashSet::new(),
        };
        let lim = DhtRateLimiter::new(&cfg, metrics());
        for _ in 0..1000 {
            assert_eq!(lim.check(&peer(1), Some(ip(10, 0, 0, 1))), Ok(()));
        }
    }

    #[test]
    fn layer_label_strings_are_stable() {
        // Pinned because operators alert on these label values.
        assert_eq!(DhtRejectLayer::PerPeer.as_str(), "per_peer");
        assert_eq!(DhtRejectLayer::PerIp.as_str(), "per_ip");
        assert_eq!(DhtRejectLayer::Global.as_str(), "global");
    }
}
