//! Cache-engine `OpenMetrics` counters.
//!
//! Six counters, all monotonic, exposed under the `decdn_cache_*`
//! family per ADR `appendix-observability`. Operators reason about
//! cache health from three ratios:
//!
//! - **Hit rate** — `hits / (hits + misses)`.
//! - **Origin egress amplification** — `pull_through_bytes / bytes_returned`.
//!   Equal to 1.0 when the cache is acting as pure pass-through; trends
//!   toward 0 as cached content gets re-served.
//! - **Origin retry health** — `origin_retry_exhausted / origin_fetches`.
//!   Sustained nonzero rate = user-visible origin failures the retry
//!   budget couldn't save (#285).
//!
//! Hit/miss accounting (#418): on `Ok` and on the cache-domain error
//! returns (`NoOrigin`, evicted `NotFound`, origin `NotFound`,
//! `HashMismatch`, `BlobTooLarge`, `OriginError`), `CacheEngine::get`
//! bumps exactly one of `hits` or `misses`. Store I/O errors
//! (`CacheError::Store`) are infrastructure failures distinct from
//! cache outcomes — they surface via `tracing::error!` and propagate
//! without bumping either counter, so a degraded local store does not
//! pollute hit-rate dashboards.
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
    /// after a coalesced peer pull-through completes. Paired with
    /// `misses`; see the module-level doc for the hits-XOR-misses
    /// contract and the store-error carve-out.
    pub hits: Counter,
    /// `get()` calls that did not find the blob locally (#418). Bumped
    /// once on every operator-evicted `NotFound` return and once on
    /// every pull-through entry — the latter covers all pull-through
    /// outcomes (`Ok`, no-origin, origin `NotFound`, hash mismatch,
    /// size cap, transport failure) without enumerating each error
    /// variant by name. See the module-level doc for the contract with
    /// `hits`.
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
}
