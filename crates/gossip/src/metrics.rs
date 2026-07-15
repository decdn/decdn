//! Metrics trait surfaced by the gossip crate.
//!
//! The gossip crate intentionally doesn't depend on any concrete metrics
//! backend (Prometheus or otherwise). Consumers (e.g. `decdn-node`) implement
//! this trait against their own registry.

/// Counters the gossip runtime emits. All methods take `&self` and must be
/// cheap and non-blocking — they are called on hot paths.
pub trait GossipMetrics: Send + Sync + 'static {
    /// Called once per successful publish, labelled by topic.
    fn inc_published(&self, topic: &str);
    /// Called once per inbound envelope that passes framing, labelled by topic.
    fn inc_received(&self, topic: &str);
    /// Called once per rejected envelope. `reason` is a stable static label
    /// (one of the [`crate::AnnounceReject`] variant names).
    fn inc_rejected(&self, reason: &'static str);
    /// Gauge set to the current peer-table size after each mutation.
    fn set_peer_table_size(&self, n: i64);
    /// Called each time a subscriber successfully reconnects after a stream drop.
    fn inc_reconnected(&self, topic: &str);
    /// Called once per TTL sweep that evicted at least one entry, with the
    /// number of entries the sweeper removed. Backs the unlabeled
    /// `decdn_peer_table_evicted_ttl_total` counter
    /// (appendix-peer-table-eviction § Observability).
    fn add_evicted_ttl(&self, n: u64);
}

/// A no-op implementation convenient for tests.
#[derive(Debug, Default, Clone, Copy)]
pub struct NoopMetrics;

impl GossipMetrics for NoopMetrics {
    fn inc_published(&self, _topic: &str) {}
    fn inc_received(&self, _topic: &str) {}
    fn inc_rejected(&self, _reason: &'static str) {}
    fn set_peer_table_size(&self, _n: i64) {}
    fn inc_reconnected(&self, _topic: &str) {}
    fn add_evicted_ttl(&self, _n: u64) {}
}
