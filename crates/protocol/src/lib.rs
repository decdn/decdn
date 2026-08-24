//! Wire protocol types, ALPN identifiers, and message definitions for deCDN.
//!
//! This is the leaf crate in the dependency graph — it has minimal dependencies
//! and defines the shared vocabulary used by all other deCDN crates.

pub mod client;
pub mod dht;
pub mod framing;
pub mod identity;
pub mod message;
pub mod region;

pub use client::{
    BINDING_SIG_LEN, CHUNK_BYTES, ChunkData, ChunkPreimage, ClientBinding, ClientMessage,
    MAX_CHAIN_LENGTH, MB_BYTES, StreamError, StreamRequest, StreamRequestExt, StreamResponse,
    StreamResponseBody, VOUCHER_SIG_LEN, Voucher, VoucherRejectReason, encode_chunk_frame,
    encode_stream_request, parse_stream_request_ext,
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
