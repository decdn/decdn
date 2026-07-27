//! Shared three-layer token-bucket rate limiter for per-request inbound
//! traffic on authenticated QUIC handlers.
//!
//! This is the engine behind both the `cdn/dht/v1` limiter (ADR 022 §DHT
//! Rate Limiting, via [`crate::dht::rate_limit::DhtRateLimiter`]) and the
//! `cdn/probe/v1` limiter (ADR 005 §Probe rate limiting, via
//! [`crate::handlers::probe_rate_limit::ProbeRateLimiter`]). The two ADRs
//! specify the identical three-layer shape — the differences are the default
//! parameters, the operator-visible metric names, and that the DHT path
//! additionally drives the engine's batch-token accounting
//! ([`ThreeLayerRateLimiter::admit_batch_extra`]) while the probe path issues
//! exactly one [`ThreeLayerRateLimiter::check`] per connection. So the
//! mechanism lives here once and each path supplies its own
//! [`RateLimitMetricsSink`].
//!
//! Distinct from the connection-level [`crate::dispatch::ConnectionLimiter`]:
//! that limiter caps how many connections we accept; this one caps the
//! per-request work done on each accepted connection (for DHT, many requests
//! per connection; the probe path is one probe per connection). Both run — the
//! connection limiter rejects floods at handshake, this layer rejects
//! per-request floods on a successfully accepted connection.
//!
//! # Layered checks
//!
//! Layers fire **global → per-IP → per-peer** (cheapest-first ordering). A
//! request rejected by the global cap never pays the per-IP map lookup; a
//! per-IP rejection never pays the per-peer map lookup. The first rejection
//! short-circuits and is the only one reported to the sink's
//! [`RateLimitMetricsSink::rejected`] — one Counter per layer (the
//! iroh-metrics backend does not support per-field labels, so we use distinct
//! counters; see the per-path `Metrics` methods for the deviation rationale
//! from each ADR's labeled-counter shape and the rolled-up Prometheus query
//! operators can use).

use std::net::IpAddr;
use std::num::NonZeroU32;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use governor::{DefaultDirectRateLimiter, DefaultKeyedRateLimiter, Quota};
use iroh::TransportAddr;
use iroh::endpoint::Connection;

use crate::dht::routing::NodeId;
use crate::dispatch::source_key;
use crate::prune_guard::PruneGuard;

/// Resolved three-layer rate-limit configuration.
///
/// A layer is disabled (its `Option<_>` limiter is `None`) when its quota is
/// not buildable — see `make_quota`: that is `*_rate_per_sec <= 0.0`, a
/// non-finite rate, **or** `*_burst == 0`. The intended operator opt-out sets
/// **both** the rate to `0.0` and the burst to `0`; the config resolver
/// (`resolve_{dht,probe}_into`) rejects the asymmetric `rate > 0, burst == 0`
/// combination up front, so in production a layer is only ever disabled via the
/// both-zero opt-out. A `RateLimitConfig` built directly (bypassing the
/// resolver) with `rate > 0, burst == 0` will silently disable that layer
/// rather than error — construct via the `From<&Resolved{Dht,Probe}>` impls or
/// the resolver to keep that invariant.
///
/// [`Default`] returns the ADR 022 DHT-layer values (the historical default of
/// this struct). The probe path supplies its own ADR 005 defaults through the
/// config resolver and never relies on `Default` here.
#[derive(Debug, Clone)]
pub struct RateLimitConfig {
    /// Per-peer (source `NodeId`) sustained rate (requests/second).
    pub per_peer_rate_per_sec: f64,
    /// Per-peer burst capacity.
    pub per_peer_burst: u32,
    /// Per-IP sustained rate (requests/second).
    pub per_ip_rate_per_sec: f64,
    /// Per-IP burst capacity.
    pub per_ip_burst: u32,
    /// Global inbound sustained rate (requests/second).
    pub global_rate_per_sec: f64,
    /// Global inbound burst capacity.
    pub global_burst: u32,
    /// Hard cap on the per-IP keyed-limiter map (#645). `0` => unbounded
    /// (operator opt-in, the resolver warns).
    pub max_tracked_per_ip: usize,
    /// Hard cap on the per-peer keyed-limiter map (#645). `0` => unbounded.
    pub max_tracked_per_peer: usize,
}

impl Default for RateLimitConfig {
    fn default() -> Self {
        Self {
            per_peer_rate_per_sec: 20.0,
            per_peer_burst: 40,
            per_ip_rate_per_sec: 100.0,
            per_ip_burst: 200,
            global_rate_per_sec: 1000.0,
            global_burst: 2000,
            max_tracked_per_ip: 4096,
            max_tracked_per_peer: 4096,
        }
    }
}

// The resolved DHT/probe config (in `decdn-common`) and this engine config
// hold the identical eight fields, but `common` can't depend on `node`, so the
// engine config lives here. These `From` impls are the single mapping point
// the runtime uses to build a limiter from resolved config, so a field added
// on one side has exactly one place to be threaded through on the other.
//
// Three failure modes, three different guards (#1457):
//
// - **Arity** — the compiler, but from two places, not one. The literals below
//   are exhaustive (no `..Default::default()`), which covers three of the four
//   quadrants: a field added to or dropped from `RateLimitConfig` fails here,
//   and so does a field dropped from a `Resolved*`. A field *added* to a
//   `Resolved*` does not — reading `r.field` carries no exhaustiveness
//   requirement, so these impls simply never mention it. That quadrant is
//   caught by the exhaustive `Resolved*` literals in the two tests below
//   (and by each type's own `Default` impl over in `common`), which is why
//   those fixtures must never be relaxed to `..Default::default()`.
// - **Transposition** — the `*_mapping_is_not_transposed` tests below. The
//   compiler cannot help: all eight fields are `f64`/`u32`/`usize`, so
//   swapping `per_peer_burst` with `per_ip_burst` still compiles. The tests
//   feed eight pairwise-distinct sentinels through each impl and pin every
//   landing slot. Review-by-diff cannot substitute — the two `Self { .. }`
//   literals are byte-identical, so a swap in one reads as plausible next to
//   the other.
// - **Wiring** — `{Probe,Dht}RateLimiter::from_resolved`, which take the
//   `Resolved*` type rather than the mapped config. Both limiter configs are
//   aliases of `RateLimitConfig`, so a limiter built from the *other*
//   protocol's section type-checks fine; going through `from_resolved` at the
//   wiring sites makes that pairing a type error instead.
//
// One more duplication sits outside the mapping itself:
// `RateLimitConfig::default()` above restates `ResolvedDht::default()`
// value-for-value across a crate boundary, with no shared constant.
// `dht_default_matches_engine_default` below is the only thing holding those
// two in step.
impl From<&decdn_common::config::ResolvedDht> for RateLimitConfig {
    fn from(r: &decdn_common::config::ResolvedDht) -> Self {
        Self {
            per_peer_rate_per_sec: r.per_peer_rate_per_sec,
            per_peer_burst: r.per_peer_burst,
            per_ip_rate_per_sec: r.per_ip_rate_per_sec,
            per_ip_burst: r.per_ip_burst,
            global_rate_per_sec: r.global_rate_per_sec,
            global_burst: r.global_burst,
            max_tracked_per_ip: r.max_tracked_per_ip,
            max_tracked_per_peer: r.max_tracked_per_peer,
        }
    }
}

impl From<&decdn_common::config::ResolvedProbe> for RateLimitConfig {
    fn from(r: &decdn_common::config::ResolvedProbe) -> Self {
        Self {
            per_peer_rate_per_sec: r.per_peer_rate_per_sec,
            per_peer_burst: r.per_peer_burst,
            per_ip_rate_per_sec: r.per_ip_rate_per_sec,
            per_ip_burst: r.per_ip_burst,
            global_rate_per_sec: r.global_rate_per_sec,
            global_burst: r.global_burst,
            max_tracked_per_ip: r.max_tracked_per_ip,
            max_tracked_per_peer: r.max_tracked_per_peer,
        }
    }
}

/// Layer that triggered a rejection. Used as a log/trace field and to pick
/// the right per-layer Counter via [`RateLimitMetricsSink::rejected`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RejectLayer {
    /// Per-peer (`NodeId`) bucket exhausted.
    PerPeer,
    /// Per-IP bucket exhausted.
    PerIp,
    /// Global cap exhausted.
    Global,
}

impl RejectLayer {
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

/// Metrics fan-out for a [`ThreeLayerRateLimiter`]. Each protocol path
/// (`cdn/dht/v1`, `cdn/probe/v1`) supplies an implementation that routes to
/// its own operator-visible counters/gauges, so the limiter core stays
/// backend-agnostic. Implementations must be cheap and lock-free on the hot
/// `rejected` path.
pub trait RateLimitMetricsSink: Send + Sync {
    /// Record a request rejected at `layer` (the cheapest-first layer that
    /// drained the budget). Called at most once per rejected request.
    fn rejected(&self, layer: RejectLayer);
    /// Record a `retain_recent` sweep of the per-IP keyed-limiter map (#645).
    fn prune_sweep_per_ip(&self);
    /// Record a `retain_recent` sweep of the per-peer keyed-limiter map (#645).
    fn prune_sweep_per_peer(&self);
    /// Publish the per-IP keyed-limiter map size after a prune (#645).
    fn set_tracked_per_ip(&self, n: usize);
    /// Publish the per-peer keyed-limiter map size after a prune (#645).
    fn set_tracked_per_peer(&self, n: usize);
}

/// Three-layer rate limiter for inbound authenticated per-request traffic.
///
/// Methods are `&self` — concurrent admission decisions don't take a write
/// lock. Each layer uses its own `governor` limiter and the
/// `Result<(), RejectLayer>` short-circuits at the first rejection.
///
/// **Keyspace bound (#645).** The two keyed layers carry per-layer
/// `cap_per_{ip,peer}` and `pruning_per_{ip,peer}` fields. The flags are
/// deliberately split (not one shared) so a slow per-IP `retain_recent` sweep
/// does not block a concurrent per-peer sweep — and vice versa.
#[allow(missing_debug_implementations)]
pub struct ThreeLayerRateLimiter {
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
    metrics: Arc<dyn RateLimitMetricsSink>,
}

impl ThreeLayerRateLimiter {
    /// Build a limiter from the resolved config snapshot and a metrics sink.
    #[must_use]
    pub fn new(cfg: &RateLimitConfig, metrics: Arc<dyn RateLimitMetricsSink>) -> Self {
        Self {
            global: build_direct_limiter(cfg.global_rate_per_sec, cfg.global_burst),
            per_ip: build_keyed_limiter(cfg.per_ip_rate_per_sec, cfg.per_ip_burst),
            per_peer: build_keyed_limiter(cfg.per_peer_rate_per_sec, cfg.per_peer_burst),
            cap_per_ip: cfg.max_tracked_per_ip,
            cap_per_peer: cfg.max_tracked_per_peer,
            pruning_per_ip: AtomicBool::new(false),
            pruning_per_peer: AtomicBool::new(false),
            metrics,
        }
    }

    /// Try to admit one inbound request. `peer_ip == None` for relay-only
    /// connections — the per-IP layer is skipped (no key to charge), matching
    /// the `ConnectionLimiter` precedent at
    /// [`crate::dispatch::ConnectionLimiter::acquire`].
    ///
    /// On rejection, calls [`RateLimitMetricsSink::rejected`] with the layer
    /// that fired and returns it. No sink call on the success path.
    ///
    /// Cheapest-first ordering (global → per-IP → per-peer): the first layer
    /// to reject short-circuits, so a per-peer-flooded request that would also
    /// have failed the global cap counts as `global` (the layer that actually
    /// drained the budget).
    pub fn check(&self, peer_node_id: &NodeId, peer_ip: Option<IpAddr>) -> Result<(), RejectLayer> {
        // Charge one unit cheapest-first; report the layer that drained the
        // budget to the sink. The accounting itself lives in `try_admit_one`
        // so the batch stage-2 path (`admit_batch_extra`) can reuse it without
        // the metric bump — a partial-admit boundary is not a request
        // rejection. `prune = true`: a fresh inbound frame may have inserted a
        // new keyed-map entry, so run the opportunistic keyspace prune.
        match self.try_admit_one(peer_node_id, peer_ip, true) {
            Ok(()) => Ok(()),
            Err(layer) => {
                self.metrics.rejected(layer);
                Err(layer)
            }
        }
    }

    /// Charge exactly one unit from each enabled layer, cheapest-first
    /// (global → per-IP → per-peer) with short-circuit on the first exhausted
    /// layer. Does **not** touch any metric — callers decide whether a failure
    /// is a rejection ([`Self::check`]) or a partial-admit boundary
    /// ([`Self::admit_batch_extra`]).
    ///
    /// `prune` gates the opportunistic keyspace prune (#645). Stage-1
    /// [`Self::check`] passes `true`; the stage-2 batch loop passes `false`
    /// because every extra unit it charges keys on the *same* `(peer, ip)`
    /// stage-1 already inserted — so it can never grow the keyed maps, making a
    /// per-unit prune check pure overhead (the keyspace bound is upheld by
    /// stage-1 + the periodic GC sweep).
    fn try_admit_one(
        &self,
        peer_node_id: &NodeId,
        peer_ip: Option<IpAddr>,
        prune: bool,
    ) -> Result<(), RejectLayer> {
        // Layer 1 — global.
        if let Some(g) = self.global.as_ref()
            && g.check().is_err()
        {
            return Err(RejectLayer::Global);
        }

        // Layer 2 — per-IP. Skipped for relay-only connections. The key is
        // masked to its /64 prefix for IPv6 (#841) — the same mask the dispatch
        // limiter applies — so an attacker rotating within one IPv6 allocation
        // can't mint a fresh bucket per request.
        if let (Some(ip), Some(limiter)) = (peer_ip, self.per_ip.as_ref()) {
            let ip = source_key(ip);
            let result = limiter.check_key(&ip);
            if prune {
                self.maybe_prune_per_ip(limiter);
            }
            if result.is_err() {
                return Err(RejectLayer::PerIp);
            }
        }

        // Layer 3 — per-peer (NodeId).
        if let Some(limiter) = self.per_peer.as_ref() {
            let result = limiter.check_key(peer_node_id);
            if prune {
                self.maybe_prune_per_peer(limiter);
            }
            if result.is_err() {
                return Err(RejectLayer::PerPeer);
            }
        }

        Ok(())
    }

    /// Stage 2 of the two-stage batch admission (ADR 022 §Batch token
    /// accounting). After [`Self::check`] has charged the stage-1 frame token,
    /// this consumes up to `extra` (= `n - 1` for a batch of `n` hashes) more
    /// units cheapest-first, each unit identical to what one separate
    /// `StoreRequest` admission would charge — so the per-second work ceiling
    /// is the same whether the publisher sends `n` separate `Store`s or one
    /// `BatchStore` of size `n` (ADR 022 AC 17).
    ///
    /// Returns the number of extra units granted; the first `1 + returned`
    /// hashes of the batch pass through per-hash processing, the remainder are
    /// acked `false`. Returns `0` when `extra == 0` or the budget is already
    /// exhausted.
    ///
    /// No rejection counter is bumped: the inbound frame was already admitted
    /// at stage 1, and the over-budget tail is deferred work the publisher
    /// retries, not a rejected request. Because the per-unit charge
    /// short-circuits cheapest-first, the terminating unit may consume a global
    /// (and per-IP) token without a per-peer token — byte-for-byte what the
    /// `(k+1)`-th separate `Store` would have done.
    #[must_use]
    pub fn admit_batch_extra(
        &self,
        peer_node_id: &NodeId,
        peer_ip: Option<IpAddr>,
        extra: usize,
    ) -> usize {
        let mut granted = 0usize;
        while granted < extra {
            // `prune = false`: see `try_admit_one` — stage-2 units never add
            // keyed-map entries, so the prune check is redundant here.
            if self.try_admit_one(peer_node_id, peer_ip, false).is_err() {
                break;
            }
            granted = granted.saturating_add(1);
        }
        granted
    }

    /// Opportunistic prune of the per-IP keyed map when it exceeds
    /// `cap + cap/10`. Single-flighted via `pruning_per_ip`; a contended
    /// observer skips and the next over-cap observer picks up the work once the
    /// prior sweep releases the guard. Hot-path cost on the no-flood path: one
    /// `usize` compare, one relaxed CAS.
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
            self.metrics.prune_sweep_per_ip();
            self.metrics.set_tracked_per_ip(limiter.len());
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
            self.metrics.prune_sweep_per_peer();
            self.metrics.set_tracked_per_peer(limiter.len());
        }
    }

    /// Periodic GC sweep for the per-IP keyed map (#645). Returns
    /// `Some((before, after))` on a sweep that actually ran, or `None` when the
    /// per-IP layer is disabled or the single-flight CAS was lost to a
    /// concurrent prune (lazy or another GC tick). Mirrors
    /// [`crate::dispatch::ConnectionLimiter::gc_per_source`].
    #[must_use]
    pub fn gc_per_ip(&self) -> Option<(usize, usize)> {
        // Borrow, don't clone: `len`/`retain_recent` take `&self`, and the
        // `pruning_per_ip`/`metrics` accesses below touch disjoint fields, so a
        // shared borrow of `self.per_ip` is sound (matches `maybe_prune_per_ip`).
        let limiter = self.per_ip.as_ref()?;
        if self
            .pruning_per_ip
            .compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
            .is_ok()
        {
            let _guard = PruneGuard(&self.pruning_per_ip);
            let before = limiter.len();
            limiter.retain_recent();
            let after = limiter.len();
            self.metrics.prune_sweep_per_ip();
            self.metrics.set_tracked_per_ip(after);
            Some((before, after))
        } else {
            None
        }
    }

    /// Periodic GC sweep for the per-peer keyed map (#645). Sibling of
    /// [`Self::gc_per_ip`].
    #[must_use]
    pub fn gc_per_peer(&self) -> Option<(usize, usize)> {
        // Borrow, don't clone (see `gc_per_ip`).
        let limiter = self.per_peer.as_ref()?;
        if self
            .pruning_per_peer
            .compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
            .is_ok()
        {
            let _guard = PruneGuard(&self.pruning_per_peer);
            let before = limiter.len();
            limiter.retain_recent();
            let after = limiter.len();
            self.metrics.prune_sweep_per_peer();
            self.metrics.set_tracked_per_peer(after);
            Some((before, after))
        } else {
            None
        }
    }

    /// Current tracked-key count for the per-IP keyed map (#645). `0` when the
    /// layer is disabled.
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

/// Extract the remote IP from a connection's currently-selected network path.
///
/// Returns `None` for relay-only connections that have no direct IP path.
/// Path selection may not have completed at the moment `accept` returns;
/// without the fallback to *any* IP path, the per-IP/per-source limits would
/// silently no-op on freshly-accepted connections and an attacker churning
/// identities could bypass the layer in that race window.
///
/// Shared by [`crate::dispatch`], the DHT handler, and the probe handler so
/// "the connection's source IP" means one thing across every rate-limit layer.
pub(crate) fn peer_ip(conn: &Connection) -> Option<IpAddr> {
    // `paths()` returns a snapshot of the *currently open* paths (closed paths
    // are not retained), so no explicit open-path filter is needed.
    let paths = conn.paths();
    paths
        .iter()
        .find(iroh::endpoint::Path::is_selected)
        .and_then(|p| match p.remote_addr() {
            TransportAddr::Ip(addr) => Some(addr.ip()),
            _ => None,
        })
        .or_else(|| {
            paths.iter().find_map(|p| match p.remote_addr() {
                TransportAddr::Ip(addr) => Some(addr.ip()),
                _ => None,
            })
        })
}

/// Build a non-keyed (global) governor limiter from a (rate, burst) pair, or
/// `None` when the layer is disabled. Mirrors the private `build_keyed_limiter`
/// helper in [`crate::dispatch`] — same `0.0` / non-finite-rate / zero-burst
/// guards.
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
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    // float_cmp: the mapped `f64` slots are copied literals, never computed, so
    // exact equality is the property being pinned — an epsilon compare here
    // would be strictly laxer than the mapping guarantee.
    clippy::float_cmp
)]
mod tests {
    use super::*;

    // Eight sentinels, one per mapped field, so no two fields can be confused
    // for each other by a transposed assignment. Shared by both `From` tests:
    // the sentinel identifies the *slot*, and the two `Resolved*` types are
    // what select the impl under test.
    //
    // Two properties make them work, and both are pinned rather than left to
    // chance: pairwise distinctness (the `const _` asserts just below, so a swap
    // always changes an asserted value) and no collision with either `Default`
    // impl (`sentinels_never_collide_with_defaults`, so a slot that silently
    // fell back to a default cannot pass for a correct map).
    const S_PER_PEER_RATE: f64 = 11.0;
    const S_PER_PEER_BURST: u32 = 22;
    const S_PER_IP_RATE: f64 = 33.0;
    const S_PER_IP_BURST: u32 = 44;
    const S_GLOBAL_RATE: f64 = 55.0;
    const S_GLOBAL_BURST: u32 = 66;
    const S_MAX_TRACKED_PER_IP: usize = 77;
    const S_MAX_TRACKED_PER_PEER: usize = 88;

    // Cross-type swaps cannot compile (`f64`/`u32`/`usize` do not coerce in a
    // struct literal), so distinctness only has to hold *within* each type
    // group. A future edit that made two same-type sentinels equal would leave
    // both `*_mapping_is_not_transposed` tests passing while silently
    // disarming the guard for that pair — the compiler catches it here first.
    const _: () = assert!(
        S_PER_PEER_RATE != S_PER_IP_RATE
            && S_PER_IP_RATE != S_GLOBAL_RATE
            && S_PER_PEER_RATE != S_GLOBAL_RATE,
        "the three f64 sentinels must stay pairwise distinct"
    );
    const _: () = assert!(
        S_PER_PEER_BURST != S_PER_IP_BURST
            && S_PER_IP_BURST != S_GLOBAL_BURST
            && S_PER_PEER_BURST != S_GLOBAL_BURST,
        "the three u32 sentinels must stay pairwise distinct"
    );
    const _: () = assert!(
        S_MAX_TRACKED_PER_IP != S_MAX_TRACKED_PER_PEER,
        "the two usize sentinels must stay pairwise distinct"
    );

    /// Assert the sentinels landed in their matching `RateLimitConfig` slots.
    ///
    /// The destructure is exhaustive (no `..`) so this helper joins the arity
    /// guard rather than opting out of it. Without it the impl literals would
    /// force a ninth field to be *mapped* while leaving it unpinned — a slot
    /// that is mapped but unasserted, which is where #1457 started.
    ///
    /// The enforcement is two links, not one: a ninth field hard-errors here
    /// (`E0027`, pattern does not mention field) until the pattern names it,
    /// and CI's `cargo clippy --workspace --all-targets -- -D warnings` then
    /// rejects the named-but-unread binding as `unused_variables`. Note that
    /// two of the fixes rustc suggests for `E0027` — `field: _` and `..` —
    /// silence both links with no diagnostic at all; adding an `assert_eq!` is
    /// the only response that keeps the guard.
    fn assert_sentinels_in_place(cfg: &RateLimitConfig) {
        let RateLimitConfig {
            per_peer_rate_per_sec,
            per_peer_burst,
            per_ip_rate_per_sec,
            per_ip_burst,
            global_rate_per_sec,
            global_burst,
            max_tracked_per_ip,
            max_tracked_per_peer,
        } = cfg;
        assert_eq!(
            *per_peer_rate_per_sec, S_PER_PEER_RATE,
            "per_peer_rate_per_sec"
        );
        assert_eq!(*per_peer_burst, S_PER_PEER_BURST, "per_peer_burst");
        assert_eq!(*per_ip_rate_per_sec, S_PER_IP_RATE, "per_ip_rate_per_sec");
        assert_eq!(*per_ip_burst, S_PER_IP_BURST, "per_ip_burst");
        assert_eq!(*global_rate_per_sec, S_GLOBAL_RATE, "global_rate_per_sec");
        assert_eq!(*global_burst, S_GLOBAL_BURST, "global_burst");
        assert_eq!(
            *max_tracked_per_ip, S_MAX_TRACKED_PER_IP,
            "max_tracked_per_ip"
        );
        assert_eq!(
            *max_tracked_per_peer, S_MAX_TRACKED_PER_PEER,
            "max_tracked_per_peer"
        );
    }

    /// `From<&ResolvedDht>` must not transpose same-typed siblings. The
    /// compiler catches arity but not order: every one of the eight fields is
    /// `f64`/`u32`/`usize`, so any swap within a type group still compiles
    /// (#1457). The exhaustive `ResolvedDht` literal below is also what carries
    /// the source-side arity guard — see the impl comment above.
    #[test]
    fn dht_mapping_is_not_transposed() {
        let resolved = decdn_common::config::ResolvedDht {
            per_peer_rate_per_sec: S_PER_PEER_RATE,
            per_peer_burst: S_PER_PEER_BURST,
            per_ip_rate_per_sec: S_PER_IP_RATE,
            per_ip_burst: S_PER_IP_BURST,
            global_rate_per_sec: S_GLOBAL_RATE,
            global_burst: S_GLOBAL_BURST,
            max_tracked_per_ip: S_MAX_TRACKED_PER_IP,
            max_tracked_per_peer: S_MAX_TRACKED_PER_PEER,
        };
        assert_sentinels_in_place(&RateLimitConfig::from(&resolved));
    }

    /// Sibling of `dht_mapping_is_not_transposed` for `From<&ResolvedProbe>`.
    /// Load-bearing independently of the DHT test: the two `Self { .. }`
    /// literals are byte-identical, so a transposition in one is invisible to
    /// review, and the probe defaults are deliberately tighter per-peer
    /// (ADR 005 §Probe rate limiting: 5 probes/sec against the per-IP 50). A
    /// `per_peer`/`per_ip` swap here raises the per-peer cap to that 50 and
    /// the per-peer burst from 5 to 200 — and the probe path is where that
    /// hurts most, since probers are unstaked and can rotate `NodeId` for free
    /// (ADR 005 §Why three layers), so the per-peer bucket is the only layer
    /// charging them at all.
    #[test]
    fn probe_mapping_is_not_transposed() {
        let resolved = decdn_common::config::ResolvedProbe {
            per_peer_rate_per_sec: S_PER_PEER_RATE,
            per_peer_burst: S_PER_PEER_BURST,
            per_ip_rate_per_sec: S_PER_IP_RATE,
            per_ip_burst: S_PER_IP_BURST,
            global_rate_per_sec: S_GLOBAL_RATE,
            global_burst: S_GLOBAL_BURST,
            max_tracked_per_ip: S_MAX_TRACKED_PER_IP,
            max_tracked_per_peer: S_MAX_TRACKED_PER_PEER,
        };
        assert_sentinels_in_place(&RateLimitConfig::from(&resolved));
    }

    /// No sentinel may equal a value either `Default` impl can supply.
    ///
    /// Pairwise distinctness (the `const _` asserts above) makes a *swap*
    /// detectable; this makes a *fallback* detectable. If a mapping row were
    /// ever rewritten to reach for a default instead of the resolved field, the
    /// slot would hold a default value — which the sentinel assertions only
    /// catch because no sentinel can be mistaken for one.
    #[test]
    fn sentinels_never_collide_with_defaults() {
        let sentinel_rates = [S_PER_PEER_RATE, S_PER_IP_RATE, S_GLOBAL_RATE];
        let sentinel_bursts = [S_PER_PEER_BURST, S_PER_IP_BURST, S_GLOBAL_BURST];
        let sentinel_tracked = [S_MAX_TRACKED_PER_IP, S_MAX_TRACKED_PER_PEER];

        // Both `Resolved*` defaults plus this engine's own, since a fallback
        // could plausibly reach for any of the three.
        let dht = RateLimitConfig::from(&decdn_common::config::ResolvedDht::default());
        let probe = RateLimitConfig::from(&decdn_common::config::ResolvedProbe::default());
        for cfg in [&RateLimitConfig::default(), &dht, &probe] {
            for rate in [
                cfg.per_peer_rate_per_sec,
                cfg.per_ip_rate_per_sec,
                cfg.global_rate_per_sec,
            ] {
                assert!(
                    !sentinel_rates.contains(&rate),
                    "default rate {rate} collides with an f64 sentinel"
                );
            }
            for burst in [cfg.per_peer_burst, cfg.per_ip_burst, cfg.global_burst] {
                assert!(
                    !sentinel_bursts.contains(&burst),
                    "default burst {burst} collides with a u32 sentinel"
                );
            }
            for tracked in [cfg.max_tracked_per_ip, cfg.max_tracked_per_peer] {
                assert!(
                    !sentinel_tracked.contains(&tracked),
                    "default cap {tracked} collides with a usize sentinel"
                );
            }
        }
    }

    /// `RateLimitConfig::default()` must stay equal to the DHT resolved
    /// defaults, as this module's docs claim (the engine `Default` is
    /// documented as "the ADR 022 DHT-layer values").
    ///
    /// The two live in different crates with no shared constant, so nothing but
    /// this test ties them together. Drift would mean every fixture that builds
    /// a limiter from `RateLimitConfig::default()` silently exercises a
    /// configuration the resolver never produces.
    #[test]
    fn dht_default_matches_engine_default() {
        let from_resolved = RateLimitConfig::from(&decdn_common::config::ResolvedDht::default());
        let engine = RateLimitConfig::default();
        assert_eq!(
            engine.per_peer_rate_per_sec,
            from_resolved.per_peer_rate_per_sec
        );
        assert_eq!(engine.per_peer_burst, from_resolved.per_peer_burst);
        assert_eq!(
            engine.per_ip_rate_per_sec,
            from_resolved.per_ip_rate_per_sec
        );
        assert_eq!(engine.per_ip_burst, from_resolved.per_ip_burst);
        assert_eq!(
            engine.global_rate_per_sec,
            from_resolved.global_rate_per_sec
        );
        assert_eq!(engine.global_burst, from_resolved.global_burst);
        assert_eq!(engine.max_tracked_per_ip, from_resolved.max_tracked_per_ip);
        assert_eq!(
            engine.max_tracked_per_peer,
            from_resolved.max_tracked_per_peer
        );
    }

    /// Mirrors `dispatch.rs::prune_guard_resets_flag_on_panic`: a panic inside
    /// the guarded section must still release the single-flight flag via
    /// `Drop`. Without this, a single panic inside `retain_recent` would
    /// permanently disable the prune codepath for the lifetime of the process
    /// — the unbounded-keyspace failure #645 exists to prevent.
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

    /// Layer label strings are pinned because operators alert on these values.
    #[test]
    fn layer_label_strings_are_stable() {
        assert_eq!(RejectLayer::PerPeer.as_str(), "per_peer");
        assert_eq!(RejectLayer::PerIp.as_str(), "per_ip");
        assert_eq!(RejectLayer::Global.as_str(), "global");
    }
}
