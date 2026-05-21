//! Kademlia content DHT (`cdn/dht/v1`) — runtime-side modules.
//!
//! See [ADR 022](../../adr/022-content-discovery.md) for the canonical design.
//! Wire types live in [`decdn_protocol::dht`]; this module owns the routing
//! table, rate limiter, record store, scheduler, and iterative lookup logic
//! that surround the handler.
//!
//! PR slice (#320): this module is being built incrementally. Current
//! state: routing table, three-layer rate limiter, handler that answers
//! `FindNode` plus real `Store`/`FindValue` against an in-memory record
//! store with the ADR 022 admission rules (per-publisher quota, global
//! LRU, receiver-anchored TTL, active-staker filter). Iterative
//! requester-side `FindValue` lookup and the republish scheduler land
//! in PR 4 of #320.

pub mod rate_limit;
pub mod records;
pub mod routing;
pub mod staker_set;

pub use rate_limit::{DhtRateLimiter, DhtRejectLayer};
pub use records::{InsertOutcome, RecordStore, RecordStoreConfig};
pub use routing::{NODE_ID_LEN, NodeId, RoutingTable, xor_distance};
pub use staker_set::{ConfigStakerSet, StakerSet};
