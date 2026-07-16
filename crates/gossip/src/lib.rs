//! Gossip transport, peer table, and `NodeAnnounce` validation for deCDN.
//!
//! Implements the publish/subscribe side of ADR 001 `NodeAnnounce` over
//! iroh-gossip, keeping the wire types themselves in `decdn-protocol`. This
//! crate is consumable by both `decdn-node` (which runs its own `NodeAnnounce`
//! publisher) and future client libraries (which want the peer table and
//! validation without pulling in the node binary).

pub mod metrics;
pub mod peer_table;
pub mod reputation;
pub mod service;
pub mod validation;

pub use metrics::GossipMetrics;
pub use peer_table::{InsertOutcome, PeerEntry, PeerTable, StaleTimestamp};
pub use reputation::{
    ALLOWED_CLOCK_SKEW_SECS, MAX_REPORT_AGE_SECS, MAX_REPORTS_PER_REPORTER_PER_HR, ReportDrain,
    ReputationRateLimiter, ReputationReject, ReputationSink, StakedNodeSet, ValidatedReport,
    validate_reputation_envelope,
};
pub use service::{
    AnnounceTrigger, GossipHandles, GossipRuntimeConfig, GossipService, GossipSpawnError,
    ReputationPublishTrigger, ReputationWiring, build_gossip,
};
pub use validation::{AnnounceReject, GOSSIP_MAX_FRAME, validate_envelope};
