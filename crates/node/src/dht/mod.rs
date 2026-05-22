//! Kademlia content DHT (`cdn/dht/v1`) — runtime-side modules.
//!
//! See [ADR 022](../../adr/022-content-discovery.md) for the canonical design.
//! Wire types live in [`decdn_protocol::dht`]; this module owns the routing
//! table, rate limiter, record store, scheduler, and iterative lookup logic
//! that surround the handler.
//!
//! PR slice (#320): this module is being built incrementally. The current PR
//! introduces the routing table, rate limiter, and a handler skeleton that
//! answers `FindNode`. Record storage, `Store` / `FindValue`, iterative
//! lookup, and the republish scheduler land in follow-up PRs.

pub mod rate_limit;
pub mod routing;

pub use rate_limit::{DhtRateLimiter, DhtRejectLayer};
pub use routing::{NODE_ID_LEN, NodeId, RoutingTable, xor_distance};
