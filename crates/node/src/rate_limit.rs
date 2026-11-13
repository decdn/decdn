//! Shared two-layer token-bucket rate limiter for per-request inbound
//! traffic on authenticated QUIC handlers.
//!
//! This is the engine behind the `cdn/dht/v1` limiter (ADR 022 §DHT Rate
//! Limiting, via [`crate::dht::rate_limit::DhtRateLimiter`]), the
//! `cdn/probe/v1` limiter (ADR 005 §Probe rate limiting, via
//! [`crate::handlers::probe_rate_limit::ProbeRateLimiter`]) and the
//! `cdn/client/v1` limiter (ADR 005 §Request rate limiting, via
//! [`crate::handlers::serve_rate_limit::ServeRateLimiter`]). The ADRs specify
//! the identical two-layer shape — the differences are the default
//! parameters, the operator-visible metric names, and that the DHT path
//! additionally drives the engine's batch-token accounting
//! ([`LayeredRateLimiter::admit_batch_extra`]) while the probe path issues
//! exactly one [`LayeredRateLimiter::check`] per connection and the serve
//! path one per stream. So the
//! mechanism lives here once and each path supplies its own
//! [`RateLimitMetricsSink`].
//!
//! Distinct from the connection-level [`crate::dispatch::ConnectionLimiter`]:
//! that limiter caps how many connection handlers run at once; this one caps
//! the per-request work done on each accepted connection (for DHT, many
//! requests per connection; the probe path is one probe per connection).
//!
//! # Layered checks
//!
//! Layers fire **per-peer → global** (narrowest-first ordering). The global
//! token is charged only after the per-peer layer admitted the request, so a
//! request one peer's own bucket refuses never spends a token of the shared
//! global budget: one `NodeId` that floods at the global rate drains only its
//! own bucket, and honest peers keep the global one. The first rejection
//! short-circuits and is the only one reported to the sink's
//! [`RateLimitMetricsSink::rejected`] — one unlabeled Counter per layer, per
//! the sibling-counter convention settled in #1475 (see [`RejectLayer`] below
//! and `adr/appendix-observability.md` § Reason splits). Each layer has an
//! unrelated remedy, so no alert spans the family. Operators recover the
//! rolled-up rate with
//! `sum(rate({__name__=~"decdn_(probe|dht|serve)_rate_limit_rejected_(per_peer|global)_total"}[1m]))`.
//!
//! The layers key on the `NodeId` and on nothing else. A source-address key
//! binds no attacker: a relay-routed connection carries no client address, so
//! an attacker that dials through a relay skips it for free, while honest peers
//! behind one NAT share its bucket (#2505).
//!
//! The trade-off of the narrowest-first order: a request the global cap
//! refuses has still touched its peer's bucket, so a flood of fresh `NodeId`s
//! grows the keyed map even while the global cap refuses it.
//! `max_tracked_per_peer` and the prune sweeps bound that growth.

use std::num::NonZeroU32;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use governor::{DefaultDirectRateLimiter, DefaultKeyedRateLimiter, Quota};

use crate::dht::routing::NodeId;
use crate::prune_guard::PruneGuard;

/// Resolved two-layer rate-limit configuration.
///
/// A layer is disabled (its `Option<_>` limiter is `None`) when its quota is
/// not buildable — see `make_quota`: that is `*_rate_per_sec <= 0.0`, a
/// non-finite rate, **or** `*_burst == 0`. The config resolver
/// (`resolve_{dht,probe,serve}_into`) rejects `rate > 0, burst == 0` as a
/// deny-all, so in production a layer is disabled by a `0.0` rate, whatever its
/// burst. A `RateLimitConfig` built directly (bypassing the resolver) with
/// `rate > 0, burst == 0` will silently disable that layer rather than error —
/// construct via the `From<&Resolved{Dht,Probe,Serve}>` impls or the resolver
/// to keep that invariant.
///
/// [`Default`] returns the ADR 022 DHT-layer values. The probe path supplies
/// its own ADR 005 defaults through the config resolver and never relies on
/// `Default` here.
#[derive(Debug, Clone)]
pub struct RateLimitConfig {
    /// Per-peer (source `NodeId`) sustained rate (requests/second).
    pub per_peer_rate_per_sec: f64,
    /// Per-peer burst capacity.
    pub per_peer_burst: u32,
    /// Global inbound sustained rate (requests/second).
    pub global_rate_per_sec: f64,
    /// Global inbound burst capacity.
    pub global_burst: u32,
    /// Hard cap on the per-peer keyed-limiter map (#645). `0` => unbounded
    /// (operator opt-in, the resolver warns).
    pub max_tracked_per_peer: usize,
}

impl Default for RateLimitConfig {
    fn default() -> Self {
        Self {
            per_peer_rate_per_sec: 20.0,
            per_peer_burst: 40,
            global_rate_per_sec: 1000.0,
            global_burst: 2000,
            max_tracked_per_peer: 4096,
        }
    }
}

// The resolved DHT/probe/serve config (in `decdn-common`) and this engine config
// hold the identical five fields, but `common` can't depend on `node`, so the
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
//   caught by the exhaustive `Resolved*` literals in the tests (and by each
//   type's own `Default` impl over in `common`), which is why those fixtures
//   must never be relaxed to `..Default::default()`.
// - **Transposition** — the `*_mapping_is_not_transposed` tests. The compiler
//   cannot help: the fields are `f64`/`u32`/`usize`, so swapping
//   `per_peer_burst` with `global_burst` still compiles. The tests feed
//   pairwise-distinct sentinels through each impl and pin every landing slot.
// - **Wiring** — `{Probe,Dht}RateLimiter::from_resolved`, which take the
//   `Resolved*` type rather than the mapped config. Both limiter configs are
//   aliases of `RateLimitConfig`, so a limiter built from the *other*
//   protocol's section type-checks fine; going through `from_resolved` at the
//   wiring sites makes that pairing a type error instead.
//
// One more duplication sits outside the mapping itself:
// `RateLimitConfig::default()` above restates `ResolvedDht::default()`
// value-for-value across a crate boundary, with no shared constant.
// `dht_default_matches_engine_default` in the tests is the only thing holding
// those two in step.
impl From<&decdn_common::config::ResolvedDht> for RateLimitConfig {
    fn from(r: &decdn_common::config::ResolvedDht) -> Self {
        Self {
            per_peer_rate_per_sec: r.per_peer_rate_per_sec,
            per_peer_burst: r.per_peer_burst,
            global_rate_per_sec: r.global_rate_per_sec,
            global_burst: r.global_burst,
            max_tracked_per_peer: r.max_tracked_per_peer,
        }
    }
}

impl From<&decdn_common::config::ResolvedProbe> for RateLimitConfig {
    fn from(r: &decdn_common::config::ResolvedProbe) -> Self {
        Self {
            per_peer_rate_per_sec: r.per_peer_rate_per_sec,
            per_peer_burst: r.per_peer_burst,
            global_rate_per_sec: r.global_rate_per_sec,
            global_burst: r.global_burst,
            max_tracked_per_peer: r.max_tracked_per_peer,
        }
    }
}

impl From<&decdn_common::config::ResolvedServe> for RateLimitConfig {
    fn from(r: &decdn_common::config::ResolvedServe) -> Self {
        Self {
            per_peer_rate_per_sec: r.per_peer_rate_per_sec,
            per_peer_burst: r.per_peer_burst,
            global_rate_per_sec: r.global_rate_per_sec,
            global_burst: r.global_burst,
            max_tracked_per_peer: r.max_tracked_per_peer,
        }
    }
}

/// Layer that triggered a rejection. Used as a log/trace field and to pick
/// the right per-layer Counter via [`RateLimitMetricsSink::rejected`].
///
/// A **sibling-counter** split, per the convention settled in #1475 — one
/// unlabeled counter per layer (`decdn_probe_rate_limit_rejected_{per_peer,
/// global}_total` on the probe path, and the matching pairs on the DHT and
/// serve paths — this enum is shared by all three), not a `layer` label. The
/// labeled `decdn_probe_hold_unavailable_total` is the one deliberate
/// reason-split exception.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RejectLayer {
    /// Per-peer (`NodeId`) bucket exhausted.
    PerPeer,
    /// Global cap exhausted.
    Global,
}

impl RejectLayer {
    /// Label string for log/metric fields. Stable across releases.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::PerPeer => "per_peer",
            Self::Global => "global",
        }
    }
}

/// Metrics fan-out for a [`LayeredRateLimiter`]. Each protocol path
/// (`cdn/dht/v1`, `cdn/probe/v1`, `cdn/client/v1`) supplies an implementation
/// that routes to its own operator-visible counters/gauges, so the limiter core
/// stays backend-agnostic. Implementations must be cheap and lock-free on the
/// hot `rejected` path.
pub trait RateLimitMetricsSink: Send + Sync {
    /// Record a request rejected at `layer` (the narrowest-first layer that
    /// drained the budget). Called at most once per rejected request.
    fn rejected(&self, layer: RejectLayer);
    /// Record a `retain_recent` sweep of the per-peer keyed-limiter map (#645).
    fn prune_sweep_per_peer(&self);
    /// Publish the per-peer keyed-limiter map size after a prune (#645).
    fn set_tracked_per_peer(&self, n: usize);
}

/// Two-layer rate limiter for inbound authenticated per-request traffic.
///
/// Methods are `&self` — concurrent admission decisions don't take a write
/// lock. Each layer uses its own `governor` limiter and the
/// `Result<(), RejectLayer>` short-circuits at the first rejection.
///
/// **Keyspace bound (#645).** The per-peer keyed layer carries a
/// `cap_per_peer` and a single-flight `pruning_per_peer` flag.
#[allow(missing_debug_implementations)]
pub struct LayeredRateLimiter {
    /// `None` when the layer is disabled. Built once at construction; not
    /// hot-reloadable (operators restart to change it).
    global: Option<Arc<DefaultDirectRateLimiter>>,
    /// Keyed by source `NodeId`. `None` when disabled.
    per_peer: Option<Arc<DefaultKeyedRateLimiter<NodeId>>>,
    /// Hard cap on the per-peer keyed-limiter map (#645). `0` => unbounded
    /// (no lazy prune from `check`; periodic `gc_per_peer` still runs but
    /// only releases buckets that have refilled to their baseline).
    cap_per_peer: usize,
    /// Single-flight guard for `retain_recent` on the per-peer keyed map.
    pruning_per_peer: AtomicBool,
    metrics: Arc<dyn RateLimitMetricsSink>,
}

impl LayeredRateLimiter {
    /// Build a limiter from the resolved config snapshot and a metrics sink.
    #[must_use]
    pub fn new(cfg: &RateLimitConfig, metrics: Arc<dyn RateLimitMetricsSink>) -> Self {
        Self {
            global: build_direct_limiter(cfg.global_rate_per_sec, cfg.global_burst),
            per_peer: build_keyed_limiter(cfg.per_peer_rate_per_sec, cfg.per_peer_burst),
            cap_per_peer: cfg.max_tracked_per_peer,
            pruning_per_peer: AtomicBool::new(false),
            metrics,
        }
    }

    /// Try to admit one inbound request from `peer_node_id`.
    ///
    /// On rejection, calls [`RateLimitMetricsSink::rejected`] with the layer
    /// that fired and returns it. No sink call on the success path.
    ///
    /// Narrowest-first ordering (per-peer → global): the first layer to reject
    /// short-circuits, so a request that would fail both its peer's bucket and
    /// the global cap counts as `per_peer`, and spends no global token.
    pub fn check(&self, peer_node_id: &NodeId) -> Result<(), RejectLayer> {
        // Charge one unit narrowest-first; report the layer that drained the
        // budget to the sink. The accounting itself lives in `try_admit_one`
        // so the batch stage-2 path (`admit_batch_extra`) can reuse it without
        // the metric bump — a partial-admit boundary is not a request
        // rejection. `prune = true`: a fresh inbound frame may have inserted a
        // new keyed-map entry, so run the opportunistic keyspace prune.
        match self.try_admit_one(peer_node_id, true) {
            Ok(()) => Ok(()),
            Err(layer) => {
                self.metrics.rejected(layer);
                Err(layer)
            }
        }
    }

    /// Charge exactly one unit from each enabled layer, narrowest-first
    /// (per-peer → global) with short-circuit on the first exhausted layer.
    /// Does **not** touch any metric — callers decide whether a failure is a
    /// rejection ([`Self::check`]) or a partial-admit boundary
    /// ([`Self::admit_batch_extra`]).
    ///
    /// `prune` gates the opportunistic keyspace prune (#645). Stage-1
    /// [`Self::check`] passes `true`; the stage-2 batch loop passes `false`
    /// because every extra unit it charges keys on the *same* peer stage-1
    /// already inserted — so it can never grow the keyed map, making a
    /// per-unit prune check pure overhead (the keyspace bound is upheld by
    /// stage-1 + the periodic GC sweep).
    fn try_admit_one(&self, peer_node_id: &NodeId, prune: bool) -> Result<(), RejectLayer> {
        // Layer 1 — per-peer (NodeId).
        if let Some(limiter) = self.per_peer.as_ref() {
            let result = limiter.check_key(peer_node_id);
            if prune {
                self.maybe_prune_per_peer(limiter);
            }
            if result.is_err() {
                return Err(RejectLayer::PerPeer);
            }
        }

        // Layer 2 — global. Charged last, so a request the per-peer layer
        // refused spends none of the shared budget.
        if let Some(g) = self.global.as_ref()
            && g.check().is_err()
        {
            return Err(RejectLayer::Global);
        }

        Ok(())
    }

    /// Stage 2 of the two-stage batch admission (ADR 022 §Batch token
    /// accounting). After [`Self::check`] has charged the stage-1 frame token,
    /// this consumes up to `extra` (= `n - 1` for a batch of `n` hashes) more
    /// units narrowest-first, each unit identical to what one separate
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
    /// short-circuits narrowest-first, the terminating unit may consume a
    /// per-peer token without a global token — byte-for-byte what the
    /// `(k+1)`-th separate `Store` would have done.
    #[must_use]
    pub fn admit_batch_extra(&self, peer_node_id: &NodeId, extra: usize) -> usize {
        let mut granted = 0usize;
        while granted < extra {
            // `prune = false`: see `try_admit_one` — stage-2 units never add
            // keyed-map entries, so the prune check is redundant here.
            if self.try_admit_one(peer_node_id, false).is_err() {
                break;
            }
            granted = granted.saturating_add(1);
        }
        granted
    }

    /// Opportunistic prune of the per-peer keyed map when it exceeds
    /// `cap + cap/10`. Single-flighted via `pruning_per_peer`; a contended
    /// observer skips and the next over-cap observer picks up the work once the
    /// prior sweep releases the guard. Hot-path cost on the no-flood path: one
    /// `usize` compare, one relaxed CAS.
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

    /// Periodic GC sweep for the per-peer keyed map (#645). Returns
    /// `Some((before, after))` on a sweep that actually ran, or `None` when the
    /// per-peer layer is disabled or the single-flight CAS was lost to a
    /// concurrent prune (lazy or another GC tick).
    #[must_use]
    pub fn gc_per_peer(&self) -> Option<(usize, usize)> {
        // Borrow, don't clone: `len`/`retain_recent` take `&self`, and the
        // `pruning_per_peer`/`metrics` accesses below touch disjoint fields.
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

    /// Current tracked-key count for the per-peer keyed map (#645).
    #[must_use]
    pub fn per_peer_tracked(&self) -> usize {
        self.per_peer.as_ref().map_or(0, |l| l.len())
    }
}

/// Build a non-keyed (global) governor limiter from a (rate, burst) pair, or
/// `None` when the layer is disabled.
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
mod tests;
