//! Wire protocol types, ALPN identifiers, and message definitions for deCDN.
//!
//! This is the leaf crate in the dependency graph — it has minimal dependencies
//! and defines the shared vocabulary used by all other deCDN crates.

/// ALPN protocol identifier for latency and availability probing.
pub const ALPN_PROBE: &[u8] = b"cdn/probe/v1";

/// ALPN protocol identifier for paid blob delivery (client-to-node and node-to-node).
pub const ALPN_CLIENT: &[u8] = b"cdn/client/v1";

/// ALPN protocol identifier for watchtower channel-dispute monitoring.
pub const ALPN_WATCHTOWER: &[u8] = b"cdn/watchtower/v1";

/// ALPN protocol identifier for Kademlia-based content discovery (ADR 022).
pub const ALPN_DHT: &[u8] = b"cdn/dht/v1";

/// Gossip topic for global node announcements and rate changes.
pub const TOPIC_GLOBAL: &str = "cdn/global/v1";

/// Gossip topic prefix for regional node announcements.
pub const TOPIC_REGION_PREFIX: &str = "cdn/region/";

/// Gossip topic for reputation reports.
pub const TOPIC_REPUTATION: &str = "cdn/reputation/v1";
