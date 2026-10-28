//! The deCDN wire protocol: the ALPN identifiers, the message types each ALPN
//! carries, and the framing that puts them on a QUIC stream.
//!
//! deCDN speaks three ALPNs ([ADR 005]):
//!
//! | ALPN | Constant | Messages | Purpose |
//! |---|---|---|---|
//! | `cdn/probe/v1` | [`ALPN_PROBE`] | [`ProbeMessage`] | Latency and availability probing |
//! | `cdn/client/v1` | [`ALPN_CLIENT`] | [`ClientMessage`] | All paid delivery, client to node and node to node |
//! | `cdn/dht/v1` | [`ALPN_DHT`] | [`DhtMessage`] | Content discovery over a Kademlia DHT ([ADR 022]) |
//!
//! # Modules
//!
//! | Module | Contents |
//! |---|---|
//! | [`framing`] | The length-prefixed frame, [`encode_message`] / [`decode_message`], and the async [`read_frame`] / [`write_frame`] |
//! | [`message`] | `cdn/probe/v1`: [`ProbeRequest`], [`ProbeResponse`] |
//! | [`client`] | `cdn/client/v1`: [`StreamRequest`], [`StreamResponse`], [`ChunkData`], the wire [`Voucher`], [`StreamError`] |
//! | [`dht`] | `cdn/dht/v1`: store and find requests, provider records, closer-node lists |
//! | [`coverage`] | The block coverage bitmap a partial holder advertises |
//! | [`identity`] | The 32-byte [`NodeId`] and [`ContentHash`] newtypes |
//! | [`region`] | ISO 3166-1 alpha-2 [`Region`] codes |
//!
//! # Frames and extensions
//!
//! Every message is a postcard encoding behind a varint length prefix. Each read
//! caps the prefix at its frame ceiling — [`MAX_PROBE_FRAME`],
//! [`MAX_CLIENT_CONTROL_FRAME`], [`MAX_CHUNK_FRAME`] or [`MAX_DHT_FRAME`]
//! ([ADR 013 § Wire Framing]). A signed message has a frozen
//! base: the fields its signature covers. Unsigned fields travel as trailing
//! bytes after the base. [`decode_message`] returns them as a remainder, and the
//! `parse_*_ext` functions read them ([ADR 013 § Three Tiers of Evolution]).
//!
//! ```
//! use decdn_protocol::{ProbeMessage, ProbeRequest, decode_message, encode_message};
//!
//! let request = ProbeMessage::Request(ProbeRequest {
//!     hash: [7; 32],
//!     timestamp_us: 1_700_000_000_000_000,
//! });
//! let frame = encode_message(&request)?;
//! let (decoded, remainder) = decode_message::<ProbeMessage>(&frame)?;
//! assert_eq!(decoded, request);
//! assert!(remainder.is_empty());
//! # Ok::<(), decdn_protocol::FrameError>(())
//! ```
//!
//! This crate holds no cryptography. The signatures carried in these messages
//! are made and checked by [`decdn-incentive`]. It has few dependencies, so
//! code that only speaks the protocol links no blob store, Ethereum client, or
//! cloud SDK.
//!
//! [ADR 005]: https://github.com/decdn/decdn/blob/main/adr/005-protocol.md
//! [ADR 022]: https://github.com/decdn/decdn/blob/main/adr/022-content-discovery.md
//! [ADR 013 § Wire Framing]: https://github.com/decdn/decdn/blob/main/adr/013-schema-evolution.md#wire-framing
//! [ADR 013 § Three Tiers of Evolution]: https://github.com/decdn/decdn/blob/main/adr/013-schema-evolution.md#three-tiers-of-evolution
//! [`decdn-incentive`]: https://docs.rs/decdn-incentive

pub mod client;
pub mod coverage;
pub mod dht;
pub mod framing;
pub mod identity;
pub mod message;
pub mod region;

pub use client::{
    BINDING_SIG_LEN, CHUNK_BYTES, ChunkData, ChunkPreimage, ClientBinding, ClientMessage,
    MAX_CHAIN_LENGTH, MAX_CHUNK_FRAME, MAX_CLIENT_CONTROL_FRAME, MB_BYTES, StreamError,
    StreamRequest, StreamRequestExt, StreamResponse, StreamResponseBody, StreamResponseExt,
    VOUCHER_SIG_LEN, Voucher, VoucherRejectReason, encode_chunk_frame, encode_stream_request,
    encode_stream_response, parse_stream_request_ext, parse_stream_response_ext,
};
pub use coverage::{Coverage, DISCOVERY_BLOCK_BYTES, discovery_block_bytes, num_blocks};
#[cfg(feature = "test-support")]
pub use coverage::{TestBlockSizeGuard, override_discovery_block_bytes_for_test};
pub use dht::{
    BatchStoreAck, BatchStoreRequest, CloserNodes, CloserNodesError, DhtMessage, FindNodeRequest,
    FindNodeResponse, FindValueRequest, FindValueResponse, FindValueResponseExt,
    MAX_BATCH_STORE_HASHES, MAX_CLOSER_NODES, MAX_DHT_FRAME, MAX_PROVIDERS_PER_HASH, StoreAck,
    StoreRequest, StoreRequestExt, encode_find_value_response, encode_store_request,
    parse_find_value_response_ext, parse_store_request_ext,
};
pub use framing::{
    FrameError, TopLevelEnum, decode_message, encode_message, is_unknown_variant, read_frame,
    write_frame,
};
pub use identity::{ContentHash, ID_LEN, NodeId};
pub use message::{
    EMPTY_ROOT, MAX_PROBE_FRAME, MAX_RATE_PER_MB, MessageValidationError, ProbeMessage,
    ProbeRequest, ProbeResponse, ProbeResponseBody, ProbeResponseExt, SLASH_SIG_LEN,
    encode_probe_response, parse_probe_response_ext,
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
/// functioning normally — the client is overloading it). The requester side
/// (`decdn-client`) types this code as `UpstreamRateLimited` so the pull
/// orchestrator suppresses the peer briefly instead of scoring it unreachable.
pub const APP_ERR_RATE_LIMITED: u32 = 0x10;

/// Request refused because its `timestamp_us` differs from the node's clock
/// by more than 30 seconds (ADR 013 §Application Error Codes; ADR 005
/// §`cdn/probe/v1` and §`cdn/client/v1`). The signed response bodies echo the
/// requester's timestamp into the slash-evidence set, so a node refuses such a
/// request with no signed response.
///
/// Delivered as a `RESET_STREAM` on `cdn/client/v1`. On `cdn/probe/v1` it
/// travels in the `CONNECTION_CLOSE` frame only. The usual cause is the
/// requester's clock. If only one node refuses, that node's clock is wrong.
/// The requester side (`decdn-client`) types this code as
/// `UpstreamTimestampRejected` so the failure names the clock instead of
/// reading as an unreachable peer. A requester that has no mapping for this
/// code treats it as a generic transport error, which is safe: the request
/// fails.
pub const APP_ERR_TIMESTAMP_SKEW: u32 = 0x04;
