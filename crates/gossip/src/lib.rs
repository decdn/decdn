//! Gossip transport, peer table, and `NodeAnnounce` validation for deCDN.
//!
//! Implements the publish/subscribe side of ADR 001 `NodeAnnounce` over
//! iroh-gossip, keeping the wire types themselves in `decdn-protocol`. This
//! crate is consumable by both `decdn-node` (which runs its own `NodeAnnounce`
//! publisher) and future client libraries (which want the peer table and
//! validation without pulling in the node binary).

pub mod metrics;
pub mod peer_table;
pub mod service;
pub mod validation;

pub use metrics::GossipMetrics;
pub use peer_table::{InsertOutcome, PeerEntry, PeerTable, StaleTimestamp};
pub use service::{AnnounceTrigger, GossipHandles, GossipRuntimeConfig, GossipService};
pub use validation::{AnnounceReject, validate_envelope};
