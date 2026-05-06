//! Cache-engine `OpenMetrics` counters (#285).
//!
//! Two counters, both monotonic. Operators alert on the *rate* of
//! `origin_retry_exhausted_total / origin_fetches_total` to catch
//! sustained origin failures the retry budget couldn't save.
//!
//! - `origin_fetches_total` — every pull-through call (success or fail).
//!   Bumped once per [`crate::CacheEngine::pull_through`] entry.
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
}
