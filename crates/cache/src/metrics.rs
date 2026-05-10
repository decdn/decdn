//! Cache-engine `OpenMetrics` counters.
//!
//! Eight counters, all monotonic, exposed under the `decdn_cache_*`
//! family per ADR `appendix-observability`. Operators reason about
//! cache health from four ratios:
//!
//! - **Hit rate** — `hits / (hits + misses)`.
//! - **Origin egress amplification** — `pull_through_bytes / bytes_returned`.
//!   Equal to 1.0 when the cache is acting as pure pass-through; trends
//!   toward 0 as cached content gets re-served.
//! - **Origin retry health** — `origin_retry_exhausted / origin_fetches`.
//!   Sustained nonzero rate = user-visible origin failures the retry
//!   budget couldn't save (#285).
//! - **GC orphan rate** — `rate(gc_bytes_reclaimed_total) /
//!   rate(pull_through_bytes)` over a recent observation window.
//!   Sustained nonzero numerator against zero denominator means bytes
//!   are being orphaned faster than the pull path promotes named tags
//!   — typically a hostile-origin signal (#518). Use a window at
//!   least `2 * cache.gc_interval_sec` wide: byte attribution lags one
//!   sweep cycle (see `gc_bytes_reclaimed_total`).
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
    pub gc_runs_total: Counter,
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
    pub gc_bytes_reclaimed_total: Counter,
}
