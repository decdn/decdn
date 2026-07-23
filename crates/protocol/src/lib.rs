//! Wire protocol types, ALPN identifiers, and message definitions for deCDN.
//!
//! This is the leaf crate in the dependency graph — it has minimal dependencies
//! and defines the shared vocabulary used by all other deCDN crates.

pub mod client;
pub mod dht;
pub mod framing;
pub mod gossip;
pub mod identity;
pub mod message;
pub mod region;

pub use client::{
    BINDING_SIG_LEN, CHUNK_SIZE, ChunkData, ClientBinding, ClientMessage,
    DEFAULT_VOUCHER_INTERVAL_MB, MAX_VOUCHER_INTERVAL_MB, MB_BYTES, StreamError, StreamRequest,
    StreamRequestExt, StreamResponse, StreamResponseBody, VOUCHER_SIG_LEN, Voucher,
    VoucherRejectReason, encode_stream_request, parse_stream_request_ext,
};
pub use dht::{
    BatchStoreAck, BatchStoreRequest, CloserNodes, CloserNodesError, DhtMessage, FindNodeRequest,
    FindNodeResponse, FindValueRequest, FindValueResponse, MAX_BATCH_STORE_HASHES,
    MAX_CLOSER_NODES, MAX_PROVIDERS_PER_HASH, StoreAck, StoreRequest,
};
pub use framing::{
    FrameError, MAX_MESSAGE_SIZE, TopLevelEnum, decode_message, encode_message, is_unknown_variant,
    read_frame, write_frame,
};
pub use gossip::{
    GOSSIP_VERSION, GossipEnvelope, GossipPayload, NodeAnnounce, NodeAnnounceBody, SIGNATURE_LEN,
};
pub use identity::{ContentHash, ID_LEN, NodeId};
pub use message::{
    MAX_RATE_PER_MB, MessageValidationError, ProbeMessage, ProbeRequest, ProbeResponse,
    ProbeResponseBody, SLASH_SIG_LEN,
};
pub use region::{InvalidRegion, Region, is_valid_region};

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
