//! Cache-engine `OpenMetrics` counters (#285).
//!
//! Two counters, both monotonic. Operators alert on the *rate* of
//! `origin_retry_exhausted_total / origin_fetches_total` to catch
//! sustained origin failures the retry budget couldn't save.
//!
//! - `origin_fetches_total` — every pull-through call (success or fail).
//!   Bumped once per cache-miss origin pull from [`crate::CacheEngine`].
//! - `origin_retry_exhausted_total` — per-fetch terminal exhaustion of
//!   the retry budget. Only bumped when at least one retry actually
//!   fired; a single failure under `max_retries = 0` is just a failure,
//!   not an exhausted budget.
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
    /// after a peer pull-through completes.
    pub hits: Counter,
    /// `get()` calls that did not find the blob locally (#418). Bumped
    /// on every pull-through attempt (success, `NoOrigin`, origin
    /// `NotFound`, hash mismatch, `BlobTooLarge`, transport failure) and
    /// once for every operator-evicted (#279) `NotFound` return. Every
    /// call to `get()` increments exactly one of `hits` or `misses`.
    pub misses: Counter,
    /// Total bytes returned to the caller of `get()` on success (#418).
    /// Counts both cache-hit and pull-through-success paths. Useful as
    /// the numerator for "cache-served bytes per second" panels.
    pub bytes_returned: Counter,
    /// Bytes fetched from origin during pull-through (#418). Counted
    /// whenever the origin returns `OriginFetch::Found(b)`, regardless
    /// of whether the bytes pass BLAKE3 verification or land in the
    /// store — the egress is paid either way. Distinguishes
    /// 'serving from cache' vs. 'paying origin egress' when paired
    /// with `bytes_returned`.
    pub pull_through_bytes: Counter,
}
