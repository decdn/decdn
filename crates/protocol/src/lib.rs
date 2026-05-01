//! Wire protocol types, ALPN identifiers, and message definitions for deCDN.
//!
//! This is the leaf crate in the dependency graph — it has minimal dependencies
//! and defines the shared vocabulary used by all other deCDN crates.

pub mod framing;
pub mod gossip;
pub mod message;

pub use framing::{
    FrameError, MAX_MESSAGE_SIZE, decode_message, encode_message, read_frame, write_frame,
};
pub use gossip::{
    GOSSIP_VERSION, GossipEnvelope, GossipPayload, LoadHint, NodeAnnounce, NodeAnnounceBody,
    POPULAR_HASHES_MAX, SIGNATURE_LEN,
};
pub use message::ProbeMessage;

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

/// Connection rejected by the per-source or global rate limiter (ADR 013 §0x10).
///
/// Delivered via `CONNECTION_CLOSE` immediately on accept so the peer sees a
/// deterministic close code. Peers that receive this code SHOULD back off before
/// reconnecting; they MUST NOT treat it as a protocol error (the server is
/// functioning normally — the client is overloading it).
pub const APP_ERR_RATE_LIMITED: u32 = 0x10;
