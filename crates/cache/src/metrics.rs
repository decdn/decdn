//! Cache-engine `OpenMetrics` counters.
//!
//! Monotonic counters exposed under the `decdn_cache_*`
//! family per ADR `appendix-observability`. The exported metric names
//! all carry the OpenMetrics-encoder-appended `_total` suffix even
//! though the corresponding Rust struct fields here do not — that's
//! `iroh-metrics 1.0.1 encoding.rs:109` writing `_total` for every
//! `Counter`. The `PromQL` fragments below use the exported names so
//! operators can paste them verbatim into a Prometheus query.
//!
//! Operators reason about cache health from four ratios:
//!
//! - **Hit rate** — `decdn_cache_hits_total / (decdn_cache_hits_total +
//!   decdn_cache_misses_total)`.
//! - **Origin egress amplification** —
//!   `decdn_cache_pull_through_bytes_total / decdn_cache_bytes_returned_total`.
//!   Equal to 1.0 when the cache is acting as pure pass-through; trends
//!   toward 0 as cached content gets re-served.
//! - **Origin retry health** —
//!   `decdn_cache_origin_retry_exhausted_total / decdn_cache_origin_fetches_total`.
//!   Sustained nonzero rate = user-visible origin failures the retry
//!   budget couldn't save (#285).
//! - **GC reclaim ratio** —
//!   `rate(decdn_cache_gc_bytes_reclaimed_total) /
//!   rate(decdn_cache_pull_through_bytes_total)` over a recent
//!   observation window. `pull_through_bytes` counts every origin byte
//!   we receive, success or failure (#418), so this ratio approaches
//!   `1.0` in the hostile-origin amplification scenario where almost
//!   everything we pull is later reclaimed by GC. Healthy nodes sit
//!   near `0.0`. Use a window at least `2 * cache.gc_interval_sec`
//!   wide: byte attribution lags one sweep cycle (see
//!   `gc_bytes_reclaimed`).
//!
//! Hit/miss accounting (#418): on `Ok` and on the cache-domain error
//! returns (`NoOrigin`, evicted `NotFound`, origin `NotFound`,
//! `HashMismatch`, `BlobTooLarge`, `OriginError`), `CacheEngine::get`
//! bumps exactly one of `hits` or `misses`.
//!
//! Store I/O error carve-out (`CacheError::Store`): these surface via
//! `tracing::error!` and split by phase.
//!
//! - **Local-lookup phase** (`has`, `read_local` before pull-through is
//!   initiated): a Store error here means the cache can't *determine*
//!   whether the blob is present, so neither counter is bumped. A
//!   degraded local store does not silently classify reads as misses.
//! - **Pull-through phase** (after the miss is already determined —
//!   e.g., `add_bytes` fails to persist verified origin bytes): the
//!   `misses` and `pull_through_bytes` bumps that fired earlier in
//!   `pull_through` stay bumped. The miss genuinely happened, the
//!   origin egress was genuinely paid, and the failure surfaces both
//!   via `tracing::error!` and as the get-call's `Err`.
//!
//! The cache crate owns this group rather than re-exporting node-side
//! state so the cache stays self-describing. Node wires `Arc<CacheMetrics>`
//! into its registry under the `decdn_cache` prefix.

use iroh_metrics::{Counter, MetricsGroup};
use serde::{Deserialize, Serialize};

/// `OpenMetrics` counters for the cache engine. Register via
/// `Registry::register(Arc::clone(&cache_metrics) as Arc<dyn MetricsGroup>)`
/// (or under a sub-registry prefix); the same `Arc` is handed to
/// [`crate::CacheEngine::open_full`] so engine-side bumps land in the
/// same encoder output.
#[derive(Debug, Default, Serialize, Deserialize, MetricsGroup)]
#[metrics(name = "cache")]
pub struct CacheMetrics {
    /// Origin pull-through fetches attempted (one per `pull_through`
    /// call, success or failure). Denominator for any retry-exhaustion
    /// alert.
    pub origin_fetches: Counter,
    /// Origin fetches that gave up after exhausting `max_retries`
    /// (#285). Operator-actionable: any nonzero rate = user-visible
    /// origin failures the retry budget couldn't save.
    pub origin_retry_exhausted: Counter,
    /// Times `pull_through` advanced from one origin to the next in
    /// the operator-configured fallback chain (#284). One bump per
    /// chain-walk step, *not* per per-origin retry — the latter is
    /// covered by `origin_retry_exhausted`. Bumped only when there is
    /// a next entry to try, so a single-origin chain (the pre-#284
    /// common case) keeps this counter flat at zero. The denominator
    /// for a "what fraction of misses needed fallback" alert is
    /// `decdn_cache_origin_fetches_total`. Sustained nonzero against
    /// a healthy primary backend suggests the chain is masking an
    /// outage that should be paged on instead.
    pub origin_fallback: Counter,
    /// `get()` calls served from the local store (#418). Includes both
    /// first-attempt hits and waiter retries that find the blob present
    /// after a coalesced concurrent pull-through completes (the
    /// engine's coalescing is between in-process tasks fronting the
    /// same hash, not between network peers). Paired with `misses`;
    /// see the module-level doc for the hits-XOR-misses contract and
    /// the store-error carve-out.
    pub hits: Counter,
    /// `get()` calls that did not find the blob locally (#418). Bumped
    /// once on every operator-evicted `NotFound` return and once at
    /// the entry of every pull-through — the latter covers all
    /// pull-through outcomes (`Ok` and any later error, including a
    /// `Store` error from `add_bytes`) without enumerating each
    /// variant. See the module-level doc for the contract with `hits`
    /// and the per-phase store-error carve-out.
    pub misses: Counter,
    /// Total bytes returned to the caller of `get()` on success (#418).
    /// Counts both cache-hit and pull-through-success paths.
    pub bytes_returned: Counter,
    /// Bytes fetched from origin during pull-through (#418). Counted
    /// the moment bytes are received from origin, regardless of whether
    /// they pass BLAKE3 verification or land in the store — origin
    /// egress is paid either way. Distinguishes 'serving from cache'
    /// vs. 'paying origin egress' when paired with `bytes_returned`.
    pub pull_through_bytes: Counter,
    /// iroh-blobs GC sweep cycles observed (#518). Bumped once per
    /// `add_protected` callback fire, after the pre-sweep snapshot
    /// succeeds. Iroh-blobs spawns the sweep loop internally when
    /// `cache.gc_interval_sec > 0`; the cb is our hook into each cycle.
    /// On-demand reclamation is tracked under #520.
    ///
    /// Operator-actionable: a flat-line at zero against a nonzero
    /// `cache.gc_interval_sec` means the periodic loop never spawned,
    /// or the snapshot has been failing on every cycle (the engine
    /// emits a `tracing::warn!` when that happens).
    ///
    /// Field name omits `_total`: the `OpenMetrics` encoder appends it
    /// automatically (`iroh-metrics` 1.0.1 `encoding.rs:109`), so the
    /// emitted name is `decdn_cache_gc_runs_total`.
    pub gc_runs: Counter,
    /// Bytes reclaimed by the iroh-blobs GC, attributed across cycles
    /// (#518). Computed as the pre-sweep blob-set diff between the
    /// previous cycle and the current cycle: hashes that vanish across
    /// that window are exactly what the previous sweep deleted (the
    /// cache crate has no other public delete path). Sustained nonzero
    /// rate against zero `rate(pull_through_bytes)` is a red flag —
    /// either the pull path is failing to promote named tags on
    /// success, or a hostile origin is amplifying disk usage via
    /// repeated mid-stream errors.
    ///
    /// **Attribution lags one sweep cycle.** The first cycle records a
    /// baseline and bumps zero bytes; from cycle two onward the bump
    /// reflects the previous sweep's reclaim. For an alert window
    /// shorter than `2 * cache.gc_interval_sec` the reading will be
    /// noisy.
    ///
    /// Field name omits `_total`: the `OpenMetrics` encoder appends it
    /// automatically, so the emitted name is
    /// `decdn_cache_gc_bytes_reclaimed_total`.
    pub gc_bytes_reclaimed: Counter,
    /// Best-effort named-tag deletions that failed (#860/#837). Bumped when
    /// `evict()` (DMCA/corruption takedown) or the drain-path hash-mismatch
    /// arm cannot delete a blob's protecting named tag. Serving is unaffected
    /// — an evict's logical-evicted set still blocks `get`/`has`, and a
    /// mismatch still returns `HashMismatch` — but the bytes stay GC-protected
    /// on disk because the tag survives, and nothing re-attempts the delete.
    ///
    /// Operator-actionable: any sustained nonzero rate means disk reclaim is
    /// stuck. For an `evict()` failure that is a DMCA/compliance concern (the
    /// takedown stopped serving but did not reclaim the bytes); for the drain
    /// path it is the #837 unbounded-disk-growth leak under a hostile origin.
    /// A persistently nonzero rate warrants investigating the iroh-blobs tag
    /// store (a wedged store actor, I/O errors); the engine also emits a
    /// `tracing::warn!` per failure.
    ///
    /// Field name omits `_total`: the `OpenMetrics` encoder appends it
    /// automatically, so the emitted name is
    /// `decdn_cache_tag_drop_failures_total`.
    pub tag_drop_failures: Counter,
    /// Per-origin circuit-breaker trips from CLOSED/HALF-OPEN to OPEN
    /// (#963). Bumped once each time a breaker opens — on crossing the
    /// `failure_threshold` from CLOSED, or on a failed HALF-OPEN trial.
    /// Sustained nonzero rate against a configured origin means that
    /// origin is having a sustained outage and the cache is shedding the
    /// retry-backoff storm by fast-failing its misses.
    ///
    /// Operator-actionable: a breaker that keeps opening points at an
    /// unhealthy origin (or a backend the chain is masking). Pair with
    /// `circuit_breaker_short_circuits` to see how much retry-backoff
    /// work the breaker saved while open.
    ///
    /// Field name omits `_total`: the `OpenMetrics` encoder appends it
    /// automatically, so the emitted name is
    /// `decdn_cache_circuit_breaker_trips_total`.
    pub circuit_breaker_trips: Counter,
    /// Per-origin circuit-breaker recoveries from HALF-OPEN to CLOSED
    /// (#963). Bumped once each time a half-open trial succeeds and the
    /// breaker closes. A trip followed by a recovery is the healthy
    /// outage→recovery cycle; trips without matching recoveries means an
    /// origin that keeps failing its half-open probes.
    ///
    /// Field name omits `_total`: the emitted name is
    /// `decdn_cache_circuit_breaker_recoveries_total`.
    pub circuit_breaker_recoveries: Counter,
    /// Cache misses fast-failed by an OPEN circuit-breaker before the
    /// retry/backoff loop ran (#963). Each bump is one miss that would
    /// otherwise have incurred up to the full
    /// `max_retries`-worth of exponential backoff against a dead origin;
    /// the breaker shed that cost. The whole point of the feature is to
    /// drive this counter up during an outage so the retry-exhaustion
    /// and origin-egress counters stay flat.
    ///
    /// Field name omits `_total`: the emitted name is
    /// `decdn_cache_circuit_breaker_short_circuits_total`.
    pub circuit_breaker_short_circuits: Counter,
}
