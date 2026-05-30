//! Kademlia content DHT (`cdn/dht/v1`) — runtime-side modules.
//!
//! See [ADR 022](../../adr/022-content-discovery.md) for the canonical design.
//! Wire types live in [`decdn_protocol::dht`]; this module owns the routing
//! table, rate limiter, record store, republish scheduler, and the
//! requester-side iterative lookup that surround the handler.

pub mod auth;
pub mod batch_fallback;
pub mod bootstrap;
pub mod bucket_refresh;
pub mod chain_staker_set;
pub mod client;
pub mod lookup;
pub mod negative_cache;
pub mod origin;
pub mod publish;
pub mod rate_limit;
pub mod records;
pub mod routing;
pub mod staker_set;

pub use auth::AuthenticatedNodeId;
pub use batch_fallback::{BatchStoreFallback, DEFAULT_BATCH_UNSUPPORTED_TTL};
pub use bootstrap::{BootstrapOutcome, bootstrap};
pub use chain_staker_set::ChainStakerSet;
pub use lookup::{LookupConfig, find_providers};
pub use negative_cache::NegativeProbeCache;
pub use origin::{ConfigOriginDirectory, OriginDirectory};
pub use publish::RepublishScheduler;
pub use rate_limit::{DhtRateLimiter, DhtRejectLayer};
pub use records::{InsertOutcome, RecordStore, RecordStoreConfig};
pub use routing::{NODE_ID_LEN, NodeId, RoutingTable, xor_distance};
pub use staker_set::{ConfigStakerSet, StakerChange, StakerSet};
