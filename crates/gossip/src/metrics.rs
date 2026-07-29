//! Metrics trait surfaced by the gossip crate.
//!
//! The gossip crate intentionally doesn't depend on any concrete metrics
//! backend (Prometheus or otherwise). Consumers (e.g. `decdn-node`) implement
//! this trait against their own registry.

/// Counters the gossip runtime emits. All methods take `&self` and must be
/// cheap and non-blocking — they are called on hot paths.
///
/// # `topic` is not a metrics label
///
/// Three methods accept a `topic`, but no implementor exports it as an
/// `OpenMetrics` label: the node backs each with a single unlabeled counter and
/// discards the argument.
///
/// This is a choice, not a backend limitation. `iroh-metrics` 1.x does support
/// labels via `Family<L, M>` — `decdn-node` uses one for `streams_active`,
/// keyed on a `StreamDirection` enum. What a `Family` requires is a statically
/// typed `EncodeLabelSet`: a closed label set fixed at compile time. A `&str`
/// topic is open-ended, so exporting it directly would put an unbounded string
/// in the label position, and unbounded label cardinality is the classic way to
/// blow up a metrics backend.
///
/// So a per-topic breakdown here would mean either enumerating the topics into
/// an `EncodeLabelSet` type or adding one sibling counter per topic — the
/// latter being the established convention for small closed splits in
/// `decdn-node`'s `metrics` module (`dispatch_rejected_{global,per_source}`,
/// `gossip_messages_rejected_clock_skew`). Neither is wired today.
///
/// That convention is settled, not provisional (#1475): sibling counters are the
/// default for a closed reason split, and `decdn_probe_hold_unavailable_total`
/// is the one deliberate labeled exception — earned because its values share a
/// single alert and remedy. A new split here should follow the siblings.
///
/// The argument is kept because it is meaningful at the call site and is what
/// such a breakdown would key on. Callers should not assume the resulting
/// series distinguish topics — in practice they do not, and the
/// publish/receive/reconnect counters merge the per-`NodeAnnounce` topic names
/// (global + region).
pub trait GossipMetrics: Send + Sync + 'static {
    /// Called once per successful publish. Backs the unlabeled
    /// `decdn_gossip_announces_published_total` counter; `topic` is accepted
    /// for call-site clarity and discarded (see the trait docs).
    fn inc_published(&self, topic: &str);
    /// Called once per inbound envelope that passes framing. Backs the
    /// unlabeled `decdn_gossip_announces_received_total` counter; `topic` is
    /// accepted for call-site clarity and discarded (see the trait docs).
    fn inc_received(&self, topic: &str);
    /// Called once per rejected envelope. `reason` is a stable static label
    /// (one of the [`crate::AnnounceReject`] variant names). Unlike `topic`,
    /// this one is load-bearing: the node branches on it to pick a sibling
    /// counter.
    fn inc_rejected(&self, reason: &'static str);
    /// Gauge set to the current peer-table size after each mutation.
    fn set_peer_table_size(&self, n: i64);
    /// Called each time a subscriber successfully reconnects after a stream
    /// drop. Backs the unlabeled `decdn_gossip_subscriber_reconnections_total`
    /// counter; `topic` is accepted for call-site clarity and discarded (see
    /// the trait docs).
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
