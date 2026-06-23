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
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use governor::{DefaultDirectRateLimiter, DefaultKeyedRateLimiter, Quota};

use crate::dht::routing::NodeId;
use crate::dispatch::source_key;
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
    /// Hard cap on the per-IP keyed-limiter map (#645). `0` => unbounded
    /// (operator opt-in, the resolver warns).
    pub max_tracked_per_ip: usize,
    /// Hard cap on the per-peer keyed-limiter map (#645). `0` => unbounded.
    pub max_tracked_per_peer: usize,
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
            max_tracked_per_ip: 4096,
            max_tracked_per_peer: 4096,
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
///
/// **Keyspace bound (#645).** The two keyed layers carry per-layer
/// `cap_per_{ip,peer}` and `pruning_per_{ip,peer}` fields. The flags are
/// deliberately split (not one shared) so a slow per-IP `retain_recent`
/// sweep does not block a concurrent per-peer sweep — and vice versa.
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
    /// Hard cap on the per-IP keyed-limiter map (#645). `0` => unbounded
    /// (no lazy prune from `check`; periodic `gc_per_ip` still runs but
    /// only releases buckets that have refilled to their baseline).
    cap_per_ip: usize,
    /// Hard cap on the per-peer keyed-limiter map (#645).
    cap_per_peer: usize,
    /// Single-flight guard for `retain_recent` on the per-IP keyed map.
    pruning_per_ip: AtomicBool,
    /// Single-flight guard for `retain_recent` on the per-peer keyed map.
    pruning_per_peer: AtomicBool,
    trusted_ips: HashSet<IpAddr>,
    metrics: Arc<Metrics>,
}

/// RAII reset for [`DhtRateLimiter::pruning_per_ip`] /
/// [`DhtRateLimiter::pruning_per_peer`] — see [`crate::dispatch`]'s
/// `PruneGuard` for the full rationale. Briefly: holding one means the
/// holder owns the single-flight slot for `retain_recent`; on drop
/// (including drop during panic unwind) the flag is released with
/// `Release` ordering, so a panic in `retain_recent` cannot permanently
/// stall the prune codepath.
struct PruneGuard<'a>(&'a AtomicBool);

impl Drop for PruneGuard<'_> {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}

impl DhtRateLimiter {
    /// Build a limiter from the resolved config snapshot.
    #[must_use]
    pub fn new(cfg: &DhtRateLimitConfig, metrics: Arc<Metrics>) -> Self {
        Self {
            global: build_direct_limiter(cfg.global_rate_per_sec, cfg.global_burst),
            per_ip: build_keyed_limiter(cfg.per_ip_rate_per_sec, cfg.per_ip_burst),
            per_peer: build_keyed_limiter(cfg.per_peer_rate_per_sec, cfg.per_peer_burst),
            cap_per_ip: cfg.max_tracked_per_ip,
            cap_per_peer: cfg.max_tracked_per_peer,
            pruning_per_ip: AtomicBool::new(false),
            pruning_per_peer: AtomicBool::new(false),
            // Store trusted entries under the same /64 mask the lookup applies
            // (#841), so a configured IPv6 trusted address still matches a
            // masked inbound key. IPv4 entries are unchanged.
            trusted_ips: cfg.trusted_ips.iter().copied().map(source_key).collect(),
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
        // Charge one unit cheapest-first; bump the per-layer rejection
        // counter for the layer that drained the budget. The accounting
        // itself lives in `try_admit_one` so the batch stage-2 path
        // (`admit_batch_extra`) can reuse it without the metric bump — a
        // partial-admit boundary is not a request rejection.
        // `prune = true`: a fresh inbound frame may have inserted a new
        // keyed-map entry, so run the opportunistic keyspace prune.
        match self.try_admit_one(peer_node_id, peer_ip, true) {
            Ok(()) => Ok(()),
            Err(layer) => {
                match layer {
                    DhtRejectLayer::Global => self.metrics.dht_rate_limit_rejected_global(),
                    DhtRejectLayer::PerIp => self.metrics.dht_rate_limit_rejected_per_ip(),
                    DhtRejectLayer::PerPeer => self.metrics.dht_rate_limit_rejected_per_peer(),
                }
                Err(layer)
            }
        }
    }

    /// Charge exactly one unit from each enabled layer, cheapest-first
    /// (global → per-IP → per-peer) with short-circuit on the first
    /// exhausted layer. Does **not** touch any metric — callers decide
    /// whether a failure is a rejection ([`Self::check`]) or a
    /// partial-admit boundary ([`Self::admit_batch_extra`]).
    ///
    /// `prune` gates the opportunistic keyspace prune (#645). Stage-1
    /// [`Self::check`] passes `true`; the stage-2 batch loop passes
    /// `false` because every extra unit it charges keys on the *same*
    /// `(peer, ip)` stage-1 already inserted — so it can never grow the
    /// keyed maps, making a per-unit prune check pure overhead (the
    /// keyspace bound is upheld by stage-1 + the periodic GC sweep).
    fn try_admit_one(
        &self,
        peer_node_id: &NodeId,
        peer_ip: Option<IpAddr>,
        prune: bool,
    ) -> Result<(), DhtRejectLayer> {
        // Layer 1 — global.
        if let Some(g) = self.global.as_ref()
            && g.check().is_err()
        {
            return Err(DhtRejectLayer::Global);
        }

        // Layer 2 — per-IP. Skipped for relay-only connections and for
        // trusted IPs. The key is masked to its /64 prefix for IPv6 (#841) —
        // the same mask the dispatch limiter applies — so an attacker rotating
        // within one IPv6 allocation can't mint a fresh bucket per request.
        if let (Some(ip), Some(limiter)) = (peer_ip, self.per_ip.as_ref()) {
            let ip = source_key(ip);
            if !self.trusted_ips.contains(&ip) {
                let result = limiter.check_key(&ip);
                if prune {
                    self.maybe_prune_per_ip(limiter);
                }
                if result.is_err() {
                    return Err(DhtRejectLayer::PerIp);
                }
            }
        }

        // Layer 3 — per-peer (NodeId).
        if let Some(limiter) = self.per_peer.as_ref() {
            let result = limiter.check_key(peer_node_id);
            if prune {
                self.maybe_prune_per_peer(limiter);
            }
            if result.is_err() {
                return Err(DhtRejectLayer::PerPeer);
            }
        }

        Ok(())
    }

    /// Stage 2 of the two-stage batch admission (ADR 022 §Batch token
    /// accounting). After [`Self::check`] has charged the stage-1 frame
    /// token, this consumes up to `extra` (= `n - 1` for a batch of `n`
    /// hashes) more units cheapest-first, each unit identical to what one
    /// separate `StoreRequest` admission would charge — so the per-second
    /// work ceiling is the same whether the publisher sends `n` separate
    /// `Store`s or one `BatchStore` of size `n` (ADR 022 AC 17).
    ///
    /// Returns the number of extra units granted; the first
    /// `1 + returned` hashes of the batch pass through per-hash
    /// processing, the remainder are acked `false`. Returns `0` when
    /// `extra == 0` or the budget is already exhausted.
    ///
    /// No rejection counter is bumped: the inbound frame was already
    /// admitted at stage 1, and the over-budget tail is deferred work the
    /// publisher retries, not a rejected request. Because the per-unit
    /// charge short-circuits cheapest-first, the terminating unit may
    /// consume a global (and per-IP) token without a per-peer token —
    /// byte-for-byte what the `(k+1)`-th separate `Store` would have done.
    #[must_use]
    pub fn admit_batch_extra(
        &self,
        peer_node_id: &NodeId,
        peer_ip: Option<IpAddr>,
        extra: usize,
    ) -> usize {
        let mut granted = 0usize;
        while granted < extra {
            // `prune = false`: see `try_admit_one` — stage-2 units never
            // add keyed-map entries, so the prune check is redundant here.
            if self.try_admit_one(peer_node_id, peer_ip, false).is_err() {
                break;
            }
            granted = granted.saturating_add(1);
        }
        granted
    }

    /// Opportunistic prune of the per-IP keyed map when it exceeds
    /// `cap + cap/10`. Single-flighted via `pruning_per_ip`; a contended
    /// observer skips and the next over-cap observer picks up the work
    /// once the prior sweep releases the guard. Hot-path cost on the
    /// no-flood path: one `usize` compare, one relaxed CAS.
    fn maybe_prune_per_ip(&self, limiter: &Arc<DefaultKeyedRateLimiter<IpAddr>>) {
        if self.cap_per_ip > 0
            && limiter.len() > self.cap_per_ip.saturating_add(self.cap_per_ip / 10)
            && self
                .pruning_per_ip
                .compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
                .is_ok()
        {
            let _guard = PruneGuard(&self.pruning_per_ip);
            limiter.retain_recent();
            self.metrics.dht_rate_limit_prune_sweep_per_ip();
            self.metrics
                .dht_rate_limit_tracked_per_ip_set(limiter.len());
        }
    }

    fn maybe_prune_per_peer(&self, limiter: &Arc<DefaultKeyedRateLimiter<NodeId>>) {
        if self.cap_per_peer > 0
            && limiter.len() > self.cap_per_peer.saturating_add(self.cap_per_peer / 10)
            && self
                .pruning_per_peer
                .compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
                .is_ok()
        {
            let _guard = PruneGuard(&self.pruning_per_peer);
            limiter.retain_recent();
            self.metrics.dht_rate_limit_prune_sweep_per_peer();
            self.metrics
                .dht_rate_limit_tracked_per_peer_set(limiter.len());
        }
    }

    /// Periodic GC sweep for the per-IP keyed map (#645). Returns
    /// `Some((before, after))` on a sweep that actually ran, or `None`
    /// when the per-IP layer is disabled or the single-flight CAS was
    /// lost to a concurrent prune (lazy or another GC tick). Mirrors
    /// [`crate::dispatch::ConnectionLimiter::gc_per_source`].
    #[must_use]
    pub fn gc_per_ip(&self) -> Option<(usize, usize)> {
        let limiter = self.per_ip.as_ref()?.clone();
        if self
            .pruning_per_ip
            .compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
            .is_ok()
        {
            let _guard = PruneGuard(&self.pruning_per_ip);
            let before = limiter.len();
            limiter.retain_recent();
            let after = limiter.len();
            self.metrics.dht_rate_limit_prune_sweep_per_ip();
            self.metrics.dht_rate_limit_tracked_per_ip_set(after);
            Some((before, after))
        } else {
            None
        }
    }

    /// Periodic GC sweep for the per-peer keyed map (#645). Sibling of
    /// [`Self::gc_per_ip`].
    #[must_use]
    pub fn gc_per_peer(&self) -> Option<(usize, usize)> {
        let limiter = self.per_peer.as_ref()?.clone();
        if self
            .pruning_per_peer
            .compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
            .is_ok()
        {
            let _guard = PruneGuard(&self.pruning_per_peer);
            let before = limiter.len();
            limiter.retain_recent();
            let after = limiter.len();
            self.metrics.dht_rate_limit_prune_sweep_per_peer();
            self.metrics.dht_rate_limit_tracked_per_peer_set(after);
            Some((before, after))
        } else {
            None
        }
    }

    /// Current tracked-key count for the per-IP keyed map (#645). `0`
    /// when the layer is disabled.
    #[must_use]
    pub fn per_ip_tracked(&self) -> usize {
        self.per_ip.as_ref().map_or(0, |l| l.len())
    }

    /// Current tracked-key count for the per-peer keyed map (#645).
    #[must_use]
    pub fn per_peer_tracked(&self) -> usize {
        self.per_peer.as_ref().map_or(0, |l| l.len())
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
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;
    use std::net::Ipv4Addr;

    fn metrics() -> Arc<Metrics> {
        Arc::new(Metrics::new())
    }

    fn peer(byte: u8) -> NodeId {
        NodeId::from_bytes([byte; 32])
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
            max_tracked_per_ip: 4096,
            max_tracked_per_peer: 4096,
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

    // ---- #969: explicit evaluation-order + trusted-IP-scope coverage. ADR
    // 005 §Probe rate limiting (and the mirroring ADR 022 §DHT Rate Limiting
    // this limiter implements) specify cheapest-first ordering
    // (global → per-IP → per-peer) and a trusted-IP list that exempts ONLY
    // the per-IP layer. ----

    /// The global cap is the cheapest (first-checked) layer, so when both the
    /// global bucket and the per-IP bucket are exhausted, the rejection must
    /// be attributed to `Global` — never `PerIp`. This pins the
    /// global-before-per-IP edge of the cheapest-first order specifically
    /// (the existing `cheapest_first_global_short_circuits_per_peer` pins the
    /// global-before-per-peer edge).
    ///
    /// Construction: global burst=1 and per-IP burst=1 from the *same* IP but
    /// *distinct* peers (so the per-peer layer never fires and can't be the
    /// reported layer). The first request drains both global and per-IP; the
    /// second would fail both, and global — checked first — must win.
    #[test]
    fn cheapest_first_global_fires_before_per_ip() {
        let mut cfg = strict_cfg();
        // global burst=1, per-IP burst=1 (both from strict_cfg); loosen
        // per-peer so it can never be the layer that fires.
        cfg.per_peer_burst = u32::MAX;
        cfg.per_peer_rate_per_sec = 1e9;
        let lim = DhtRateLimiter::new(&cfg, metrics());
        let i = Some(ip(10, 0, 0, 1));
        // First request drains both the global and the per-IP bucket.
        assert_eq!(lim.check(&peer(1), i), Ok(()));
        // Second request from the SAME IP but a DIFFERENT peer would be
        // rejected by BOTH the global and the per-IP layer. Cheapest-first
        // ordering means global is consulted first and short-circuits, so
        // the reported layer must be `Global`, not `PerIp`.
        assert_eq!(lim.check(&peer(2), i), Err(DhtRejectLayer::Global));
    }

    /// The global rejection in `cheapest_first_global_fires_before_per_ip`
    /// must increment the *global* counter only — a regression that consulted
    /// per-IP first (or mis-attributed the counter) would bump
    /// `decdn_dht_rate_limit_rejected_per_ip_total` instead. The
    /// `Err(DhtRejectLayer::Global)` assertion alone can't catch a counter
    /// mix-up, so confirm the attribution via the encoded scrape too.
    #[test]
    fn global_before_per_ip_attributes_rejection_to_global_counter() {
        let mut cfg = strict_cfg();
        cfg.per_peer_burst = u32::MAX;
        cfg.per_peer_rate_per_sec = 1e9;
        let metrics = metrics();
        let lim = DhtRateLimiter::new(&cfg, Arc::clone(&metrics));
        let i = Some(ip(10, 0, 0, 1));
        assert_eq!(lim.check(&peer(1), i), Ok(()));
        assert_eq!(lim.check(&peer(2), i), Err(DhtRejectLayer::Global));
        let text = metrics.encode().unwrap();
        assert!(
            text.contains("decdn_dht_rate_limit_rejected_global_total 1"),
            "global must be the counted layer when both global+per-IP would \
             reject; got:\n{text}"
        );
        assert!(
            text.contains("decdn_dht_rate_limit_rejected_per_ip_total 0"),
            "per-IP counter must NOT move — global short-circuits first; \
             got:\n{text}"
        );
    }

    /// A trusted IP bypasses the per-IP layer but remains fully subject to the
    /// per-peer layer. With per-IP and global loosened so neither can fire,
    /// repeated requests from a trusted IP using the SAME `NodeId` must still
    /// be rejected by the per-peer bucket — proving the exemption is scoped to
    /// per-IP only and does not leak into per-peer.
    #[test]
    fn trusted_ip_still_subject_to_per_peer_layer() {
        let mut cfg = strict_cfg();
        // Per-peer burst=1 (from strict_cfg) is the only layer that can fire.
        cfg.per_ip_burst = u32::MAX;
        cfg.per_ip_rate_per_sec = 1e9;
        cfg.global_burst = u32::MAX;
        cfg.global_rate_per_sec = 1e9;
        cfg.trusted_ips.insert(ip(10, 0, 0, 1));
        let lim = DhtRateLimiter::new(&cfg, metrics());
        let trusted = Some(ip(10, 0, 0, 1));
        let p = peer(1);
        // First request from the trusted IP + peer admits.
        assert_eq!(lim.check(&p, trusted), Ok(()));
        // Second request — same trusted IP, same peer — must reject on the
        // per-peer layer. If the trust exemption wrongly bypassed per-peer
        // this would erroneously return `Ok(())`.
        assert_eq!(lim.check(&p, trusted), Err(DhtRejectLayer::PerPeer));
        // A different peer from the same trusted IP is admitted: the per-peer
        // bucket is keyed by NodeId, and the per-IP layer that *would* have
        // limited a second distinct peer from one IP is the one the trust
        // exemption legitimately bypasses.
        assert_eq!(lim.check(&peer(2), trusted), Ok(()));
    }

    fn ip6(segments: [u16; 8]) -> IpAddr {
        IpAddr::V6(std::net::Ipv6Addr::new(
            segments[0],
            segments[1],
            segments[2],
            segments[3],
            segments[4],
            segments[5],
            segments[6],
            segments[7],
        ))
    }

    #[test]
    fn per_ip_layer_masks_ipv6_to_slash_64() {
        // #841: two distinct IPv6 addresses inside the same /64 must share one
        // per-IP bucket — otherwise an attacker rotating within a /64 mints a
        // fresh bucket per request and defeats the per-IP tier entirely.
        let mut cfg = strict_cfg();
        cfg.global_burst = u32::MAX;
        cfg.global_rate_per_sec = 1e9;
        cfg.per_peer_burst = u32::MAX;
        cfg.per_peer_rate_per_sec = 1e9;
        let lim = DhtRateLimiter::new(&cfg, metrics());
        let a = Some(ip6([0x2001, 0xdb8, 0, 0, 0, 0, 0, 1]));
        let b = Some(ip6([0x2001, 0xdb8, 0, 0, 0xffff, 0xffff, 0xffff, 0xfffe]));
        // Same /64 (only the host bits differ): second request hits per-IP.
        assert_eq!(lim.check(&peer(1), a), Ok(()));
        assert_eq!(lim.check(&peer(2), b), Err(DhtRejectLayer::PerIp));
        // A different /64 lands in a fresh bucket and is admitted.
        let c = Some(ip6([0x2001, 0xdb8, 0, 1, 0, 0, 0, 1]));
        assert_eq!(lim.check(&peer(3), c), Ok(()));
    }

    #[test]
    fn trusted_ipv6_matches_masked_inbound_key() {
        // #841: a trusted IPv6 address must exempt any address in its /64, since
        // the lookup key is masked — store the trusted entry under the same mask.
        let mut cfg = strict_cfg();
        cfg.global_burst = u32::MAX;
        cfg.global_rate_per_sec = 1e9;
        cfg.per_peer_burst = u32::MAX;
        cfg.per_peer_rate_per_sec = 1e9;
        cfg.trusted_ips
            .insert(ip6([0x2001, 0xdb8, 0, 0, 0, 0, 0, 1]));
        let lim = DhtRateLimiter::new(&cfg, metrics());
        // A different host in the trusted /64 is still exempt.
        let other = Some(ip6([0x2001, 0xdb8, 0, 0, 0xaaaa, 0, 0, 9]));
        assert_eq!(lim.check(&peer(1), other), Ok(()));
        assert_eq!(lim.check(&peer(2), other), Ok(()));
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
            max_tracked_per_ip: 0,
            max_tracked_per_peer: 0,
        };
        let lim = DhtRateLimiter::new(&cfg, metrics());
        for _ in 0..1000 {
            assert_eq!(lim.check(&peer(1), Some(ip(10, 0, 0, 1))), Ok(()));
        }
    }

    // ---- #648: two-stage batch token accounting (ADR 022 §Batch token
    // accounting). `admit_batch_extra` is stage 2: after `check` has
    // charged the stage-1 frame token, it consumes up to `extra` (= n-1)
    // more units cheapest-first with short-circuit, exactly as `extra`
    // separate `StoreRequest` admissions would. ----

    #[test]
    fn admit_batch_extra_grants_full_budget_when_permissive() {
        let lim = DhtRateLimiter::new(&DhtRateLimitConfig::default(), metrics());
        let p = peer(1);
        let i = Some(ip(10, 0, 0, 1));
        // Stage 1: the frame token (matches `serve`).
        assert_eq!(lim.check(&p, i), Ok(()));
        // Stage 2: a batch of n=10 wants n-1=9 extra; all granted under
        // the default burst caps (per-peer burst 40).
        assert_eq!(lim.admit_batch_extra(&p, i, 9), 9);
    }

    #[test]
    fn admit_batch_extra_returns_zero_for_zero_extra() {
        // n=1 batch: extra = 0, nothing more to charge.
        let lim = DhtRateLimiter::new(&DhtRateLimitConfig::default(), metrics());
        assert_eq!(lim.admit_batch_extra(&peer(1), Some(ip(10, 0, 0, 1)), 0), 0);
    }

    #[test]
    fn admit_batch_extra_bounded_by_per_peer_remaining() {
        // per-peer burst 5, everything else loose. Stage 1 burns 1
        // (4 left); a batch wanting 10 extra gets only the 4 remaining.
        let mut cfg = strict_cfg();
        cfg.per_peer_rate_per_sec = 1.0;
        cfg.per_peer_burst = 5;
        cfg.per_ip_burst = u32::MAX;
        cfg.per_ip_rate_per_sec = 1e9;
        cfg.global_burst = u32::MAX;
        cfg.global_rate_per_sec = 1e9;
        let lim = DhtRateLimiter::new(&cfg, metrics());
        let p = peer(1);
        let i = Some(ip(10, 0, 0, 1));
        assert_eq!(lim.check(&p, i), Ok(()));
        assert_eq!(lim.admit_batch_extra(&p, i, 10), 4);
        // Bucket now empty — a second batch gets nothing more.
        assert_eq!(lim.admit_batch_extra(&p, i, 10), 0);
    }

    #[test]
    fn admit_batch_extra_bounded_by_min_across_layers() {
        // per-IP is the tightest layer (burst 3). Stage 1 burns 1 from
        // each (per-IP 2 left); the batch gets min remaining = 2.
        let mut cfg = strict_cfg();
        cfg.per_peer_rate_per_sec = 1e9;
        cfg.per_peer_burst = u32::MAX;
        cfg.per_ip_rate_per_sec = 1.0;
        cfg.per_ip_burst = 3;
        cfg.global_rate_per_sec = 1e9;
        cfg.global_burst = u32::MAX;
        let lim = DhtRateLimiter::new(&cfg, metrics());
        let p = peer(1);
        let i = Some(ip(10, 0, 0, 1));
        assert_eq!(lim.check(&p, i), Ok(()));
        assert_eq!(lim.admit_batch_extra(&p, i, 8), 2);
    }

    #[test]
    fn admit_batch_extra_does_not_bump_rejection_counters() {
        // The partial-admit boundary is NOT a request rejection — the
        // frame was already admitted at stage 1. Draining the budget in
        // stage 2 must leave the rate-limit rejection counters at 0 so
        // operators don't see phantom rejections for a partially-admitted
        // batch.
        let mut cfg = strict_cfg();
        cfg.per_peer_rate_per_sec = 1.0;
        cfg.per_peer_burst = 2;
        cfg.per_ip_burst = u32::MAX;
        cfg.per_ip_rate_per_sec = 1e9;
        cfg.global_burst = u32::MAX;
        cfg.global_rate_per_sec = 1e9;
        let metrics = metrics();
        let lim = DhtRateLimiter::new(&cfg, Arc::clone(&metrics));
        let p = peer(1);
        let i = Some(ip(10, 0, 0, 1));
        assert_eq!(lim.check(&p, i), Ok(()));
        // extra=5 but only 1 token left after stage 1 → grants 1, then
        // the budget is exhausted for the rest (no counter bump).
        assert_eq!(lim.admit_batch_extra(&p, i, 5), 1);
        let text = metrics.encode().unwrap();
        for layer in ["per_peer", "per_ip", "global"] {
            assert!(
                text.contains(&format!("decdn_dht_rate_limit_rejected_{layer}_total 0")),
                "stage-2 partial admit must not bump the {layer} rejection counter:\n{text}"
            );
        }
    }

    #[test]
    fn layer_label_strings_are_stable() {
        // Pinned because operators alert on these label values.
        assert_eq!(DhtRejectLayer::PerPeer.as_str(), "per_peer");
        assert_eq!(DhtRejectLayer::PerIp.as_str(), "per_ip");
        assert_eq!(DhtRejectLayer::Global.as_str(), "global");
    }

    // ---- #645: keyspace bound — periodic GC + lazy prune ----

    #[test]
    fn gc_per_ip_is_noop_when_layer_disabled() {
        // Disabled per-IP layer => no keyed map to prune. `gc_per_ip` must
        // return `None` (matches the dispatch-limiter
        // `gc_per_source_is_noop_when_layer_disabled` precedent).
        let mut cfg = strict_cfg();
        cfg.per_ip_rate_per_sec = 0.0;
        cfg.per_ip_burst = 0;
        let lim = DhtRateLimiter::new(&cfg, metrics());
        assert!(lim.gc_per_ip().is_none());
        assert_eq!(lim.per_ip_tracked(), 0);
    }

    #[test]
    fn gc_per_peer_is_noop_when_layer_disabled() {
        let mut cfg = strict_cfg();
        cfg.per_peer_rate_per_sec = 0.0;
        cfg.per_peer_burst = 0;
        let lim = DhtRateLimiter::new(&cfg, metrics());
        assert!(lim.gc_per_peer().is_none());
        assert_eq!(lim.per_peer_tracked(), 0);
    }

    /// Fast refill (`rate=1000.0, burst=1`) so a 100ms sleep is enough
    /// for `retain_recent` to drop every populated bucket. Mirrors
    /// `dispatch.rs::gc_per_source_drops_refilled_buckets`.
    #[tokio::test]
    async fn gc_per_ip_drops_refilled_buckets() {
        let cfg = DhtRateLimitConfig {
            per_peer_rate_per_sec: 1e6,
            per_peer_burst: u32::MAX,
            per_ip_rate_per_sec: 1000.0,
            per_ip_burst: 1,
            global_rate_per_sec: 1e6,
            global_burst: u32::MAX,
            trusted_ips: HashSet::new(),
            // 0 so lazy-prune in `check` is out of scope here — the GC
            // method is the only path that can drop refilled buckets.
            max_tracked_per_ip: 0,
            max_tracked_per_peer: 0,
        };
        let lim = DhtRateLimiter::new(&cfg, metrics());
        for i in 0..8 {
            let _ = lim.check(&peer(1), Some(ip(10, 0, 0, i)));
        }
        assert_eq!(lim.per_ip_tracked(), 8, "8 distinct IPs populated");
        tokio::time::sleep(Duration::from_millis(100)).await;
        let (before, after) = lim
            .gc_per_ip()
            .expect("per-IP layer enabled and CAS uncontended");
        assert_eq!(before, 8);
        assert_eq!(after, 0, "all buckets refilled and were dropped");
        assert_eq!(lim.per_ip_tracked(), 0);
    }

    #[tokio::test]
    async fn gc_per_peer_drops_refilled_buckets() {
        let cfg = DhtRateLimitConfig {
            per_peer_rate_per_sec: 1000.0,
            per_peer_burst: 1,
            per_ip_rate_per_sec: 1e6,
            per_ip_burst: u32::MAX,
            global_rate_per_sec: 1e6,
            global_burst: u32::MAX,
            trusted_ips: HashSet::new(),
            max_tracked_per_ip: 0,
            max_tracked_per_peer: 0,
        };
        let lim = DhtRateLimiter::new(&cfg, metrics());
        for byte in 0..8u8 {
            let _ = lim.check(&peer(byte), Some(ip(10, 0, 0, 1)));
        }
        assert_eq!(lim.per_peer_tracked(), 8);
        tokio::time::sleep(Duration::from_millis(100)).await;
        let (before, after) = lim
            .gc_per_peer()
            .expect("per-peer layer enabled and CAS uncontended");
        assert_eq!(before, 8);
        assert_eq!(after, 0);
        assert_eq!(lim.per_peer_tracked(), 0);
    }

    /// `cap_per_ip` bounds the per-IP map. 200 distinct IPs with a `cap`
    /// of 16 should not exceed `cap + cap/10 + 1` (the `+1` covers
    /// floor-division slack — the lazy prune fires at `len() > cap +
    /// cap/10`, so the largest size we can transiently observe is `cap +
    /// cap/10 + 1`). Fast refill so `retain_recent` can drop refilled
    /// buckets between bursts.
    #[tokio::test]
    async fn lazy_prune_bounds_per_ip_under_flood() {
        let cap: usize = 16;
        let cfg = DhtRateLimitConfig {
            per_peer_rate_per_sec: 1e6,
            per_peer_burst: u32::MAX,
            per_ip_rate_per_sec: 1000.0,
            per_ip_burst: 1,
            global_rate_per_sec: 1e6,
            global_burst: u32::MAX,
            trusted_ips: HashSet::new(),
            max_tracked_per_ip: cap,
            max_tracked_per_peer: 0,
        };
        let metrics_handle = metrics();
        let lim = DhtRateLimiter::new(&cfg, Arc::clone(&metrics_handle));
        for i in 0..200u16 {
            let a = u8::try_from((i >> 8) & 0xff).unwrap_or(0);
            let b = u8::try_from(i & 0xff).unwrap_or(0);
            let _ = lim.check(&peer(1), Some(ip(10, 0, a, b)));
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
        let final_size = lim.per_ip_tracked();
        let bound = cap.saturating_add(cap / 10).saturating_add(1);
        assert!(
            final_size <= bound,
            "expected per_ip_tracked <= {bound}, got {final_size}"
        );
        // The lazy-prune path in `check` must also bump the prune-sweep
        // counter — a regression that dropped the metric call from
        // `maybe_prune_per_ip` (but kept it on `gc_per_ip`) would not be
        // caught by `gauge_and_sweep_counter_emit_in_scrape` since that
        // test only exercises the GC path.
        let text = metrics_handle.encode().unwrap();
        assert!(
            text.contains("decdn_dht_rate_limit_prune_sweeps_per_ip_total"),
            "per-IP prune-sweep counter missing from scrape:\n{text}"
        );
        assert!(
            !text.contains("decdn_dht_rate_limit_prune_sweeps_per_ip_total 0"),
            "per-IP prune-sweep counter must have fired at least once during the flood:\n{text}"
        );
    }

    /// Pins the `0 = unbounded` contract: 1000 distinct keys with cap=0
    /// stay in the keyed map (no prune fires). Operators opting in to
    /// the unbounded mode see the `tracing::warn!` from the resolver
    /// (covered by the `decdn-common` config tests, not here).
    #[test]
    fn unbounded_when_cap_is_zero_per_ip() {
        let cfg = DhtRateLimitConfig {
            per_peer_rate_per_sec: 1e6,
            per_peer_burst: u32::MAX,
            per_ip_rate_per_sec: 1e6,
            per_ip_burst: u32::MAX,
            global_rate_per_sec: 1e6,
            global_burst: u32::MAX,
            trusted_ips: HashSet::new(),
            max_tracked_per_ip: 0,
            max_tracked_per_peer: 0,
        };
        let lim = DhtRateLimiter::new(&cfg, metrics());
        for i in 0..1000u16 {
            let a = u8::try_from((i >> 8) & 0xff).unwrap_or(0);
            let b = u8::try_from(i & 0xff).unwrap_or(0);
            assert!(lim.check(&peer(1), Some(ip(10, 0, a, b))).is_ok());
        }
        assert_eq!(lim.per_ip_tracked(), 1000);
    }

    #[test]
    fn unbounded_when_cap_is_zero_per_peer() {
        let cfg = DhtRateLimitConfig {
            per_peer_rate_per_sec: 1e6,
            per_peer_burst: u32::MAX,
            per_ip_rate_per_sec: 1e6,
            per_ip_burst: u32::MAX,
            global_rate_per_sec: 1e6,
            global_burst: u32::MAX,
            trusted_ips: HashSet::new(),
            max_tracked_per_ip: 0,
            max_tracked_per_peer: 0,
        };
        let lim = DhtRateLimiter::new(&cfg, metrics());
        for i in 0..1000u16 {
            let mut id = [0u8; 32];
            id[0] = u8::try_from((i >> 8) & 0xff).unwrap_or(0);
            id[1] = u8::try_from(i & 0xff).unwrap_or(0);
            assert!(
                lim.check(&NodeId::from_bytes(id), Some(ip(10, 0, 0, 1)))
                    .is_ok()
            );
        }
        assert_eq!(lim.per_peer_tracked(), 1000);
    }

    /// Mirrors `dispatch.rs::prune_guard_resets_flag_on_panic`:
    /// a panic inside the guarded section must still release the
    /// single-flight flag via `Drop`. Without this, a single panic
    /// inside `retain_recent` would permanently disable the prune
    /// codepath for the lifetime of the process — the unbounded-keyspace
    /// failure #645 exists to prevent.
    #[test]
    fn prune_guard_resets_flag_on_panic() {
        let flag = AtomicBool::new(false);
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            assert!(
                flag.compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
                    .is_ok()
            );
            let _guard = PruneGuard(&flag);
            panic!("simulated panic inside retain_recent");
        }));
        assert!(result.is_err(), "panic was caught");
        assert!(
            !flag.load(Ordering::Acquire),
            "PruneGuard::drop must release the flag during unwind"
        );
    }

    #[tokio::test]
    async fn lazy_prune_bounds_per_peer_under_flood() {
        let cap: usize = 16;
        let cfg = DhtRateLimitConfig {
            per_peer_rate_per_sec: 1000.0,
            per_peer_burst: 1,
            per_ip_rate_per_sec: 1e6,
            per_ip_burst: u32::MAX,
            global_rate_per_sec: 1e6,
            global_burst: u32::MAX,
            trusted_ips: HashSet::new(),
            max_tracked_per_ip: 0,
            max_tracked_per_peer: cap,
        };
        let metrics_handle = metrics();
        let lim = DhtRateLimiter::new(&cfg, Arc::clone(&metrics_handle));
        for i in 0..200u16 {
            // Distinct NodeId per iteration — pack `i` into bytes 0..2.
            let mut id = [0u8; 32];
            id[0] = u8::try_from((i >> 8) & 0xff).unwrap_or(0);
            id[1] = u8::try_from(i & 0xff).unwrap_or(0);
            let _ = lim.check(&NodeId::from_bytes(id), Some(ip(10, 0, 0, 1)));
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
        let final_size = lim.per_peer_tracked();
        let bound = cap.saturating_add(cap / 10).saturating_add(1);
        assert!(
            final_size <= bound,
            "expected per_peer_tracked <= {bound}, got {final_size}"
        );
        let text = metrics_handle.encode().unwrap();
        assert!(
            text.contains("decdn_dht_rate_limit_prune_sweeps_per_peer_total"),
            "per-peer prune-sweep counter missing from scrape:\n{text}"
        );
        assert!(
            !text.contains("decdn_dht_rate_limit_prune_sweeps_per_peer_total 0"),
            "per-peer prune-sweep counter must have fired at least once during the flood:\n{text}"
        );
    }

    /// Both sweep counters and gauges must appear in the encoded scrape
    /// after `gc_per_ip` + `gc_per_peer` run on an enabled limiter. Uses
    /// the same `encode().contains(...)` pattern as
    /// `rejection_increments_layer_metric_in_scrape`.
    #[tokio::test]
    async fn gauge_and_sweep_counter_emit_in_scrape() {
        let metrics_handle = metrics();
        let cfg = DhtRateLimitConfig {
            per_peer_rate_per_sec: 1000.0,
            per_peer_burst: 1,
            per_ip_rate_per_sec: 1000.0,
            per_ip_burst: 1,
            global_rate_per_sec: 1e6,
            global_burst: u32::MAX,
            trusted_ips: HashSet::new(),
            max_tracked_per_ip: 0,
            max_tracked_per_peer: 0,
        };
        let lim = DhtRateLimiter::new(&cfg, Arc::clone(&metrics_handle));
        for i in 0..4u8 {
            let mut id = [0u8; 32];
            id[0] = i;
            let _ = lim.check(&NodeId::from_bytes(id), Some(ip(10, 0, 0, i)));
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(lim.gc_per_ip().is_some());
        assert!(lim.gc_per_peer().is_some());
        let text = metrics_handle.encode().unwrap();
        assert!(
            text.contains("decdn_dht_rate_limit_prune_sweeps_per_ip_total 1"),
            "per-IP sweep counter missing from scrape:\n{text}"
        );
        assert!(
            text.contains("decdn_dht_rate_limit_prune_sweeps_per_peer_total 1"),
            "per-peer sweep counter missing from scrape:\n{text}"
        );
        assert!(
            text.contains("decdn_dht_rate_limit_tracked_per_ip 0"),
            "per-IP tracked gauge missing from scrape:\n{text}"
        );
        assert!(
            text.contains("decdn_dht_rate_limit_tracked_per_peer 0"),
            "per-peer tracked gauge missing from scrape:\n{text}"
        );
    }
}
