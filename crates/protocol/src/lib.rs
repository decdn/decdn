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
    SIGNATURE_LEN,
};
pub use message::{
    MAX_RATE_PER_MB, MessageValidationError, ProbeMessage, ProbeRequest, ProbeResponse,
    ProbeResponseBody, SLASH_SIG_LEN,
};

/// ALPN protocol identifier for latency and availability probing.
pub const ALPN_PROBE: &[u8] = b"cdn/probe/v1";

/// ALPN protocol identifier for paid blob delivery (client-to-node and node-to-node).
pub const ALPN_CLIENT: &[u8] = b"cdn/client/v1";

/// ALPN protocol identifier for Kademlia-based content discovery (ADR 022).
pub const ALPN_DHT: &[u8] = b"cdn/dht/v1";

/// QUIC 0-RTT session-ticket budget (ADR 015 §Session Ticket
/// Management). Passed to `Endpoint::builder().max_tls_tickets(..)`,
/// which in iroh sizes only the **client-side** rustls
/// `ClientSessionMemoryCache` (the server-side ticket store is
/// rustls-internal at its own default and is *not* sized by this knob).
/// Reused as the saturation ceiling for the approximate
/// `decdn_quic_session_ticket_cache_size` gauge so the node's whole
/// 0-RTT memory budget is one number — not because the gauge mirrors a
/// rustls cache of this size.
pub const SESSION_TICKET_CACHE_SIZE: usize = 1000;

/// Gossip topic for global node announcements and rate changes.
pub const TOPIC_GLOBAL: &str = "cdn/global/v1";

/// Gossip topic prefix for regional node announcements.
pub const TOPIC_REGION_PREFIX: &str = "cdn/region/";

/// Gossip topic for reputation reports.
pub const TOPIC_REPUTATION: &str = "cdn/reputation/v1";

/// Connection rejected by the per-source or global rate limiter (ADR 013 §0x10).
///
/// Unlike `0x01`–`0x03`, this code is delivered via `CONNECTION_CLOSE` rather
/// than `RESET_STREAM` because the rejection happens before any application
/// stream is opened. The close-frame reason bytes carry a short layer label
/// (e.g. `global-full`, `per-source`) so the peer can pick an appropriate
/// backoff. Peers that receive this code SHOULD back off before
/// reconnecting; they MUST NOT treat it as a protocol error (the server is
/// functioning normally — the client is overloading it).
pub const APP_ERR_RATE_LIMITED: u32 = 0x10;
