//! The deCDN wire protocol: the ALPN identifiers, the message types each ALPN
//! carries, and the framing that puts them on a QUIC stream.
//!
//! deCDN speaks two ALPNs ([ADR 005]):
//!
//! | ALPN | Constant | Messages | Purpose |
//! |---|---|---|---|
//! | `cdn/client/v1` | [`ALPN_CLIENT`] | [`ClientMessage`] | All paid delivery, client to node and node to node |
//! | `cdn/dht/v1` | [`ALPN_DHT`] | [`DhtMessage`] | Content discovery over a Kademlia DHT ([ADR 022]) |
//!
//! # Modules
//!
//! | Module | Contents |
//! |---|---|
//! | [`framing`] | The length-prefixed frame, [`encode_message`] / [`decode_message`], and the async [`read_frame`] / [`write_frame`] |
//! | [`client`] | `cdn/client/v1`: [`StreamRequest`], [`StreamResponse`], [`ChunkData`], the wire [`Voucher`], [`StreamError`] |
//! | [`dht`] | `cdn/dht/v1`: store and find requests, provider records, closer-node lists |
//! | [`coverage`] | The block coverage bitmap a partial holder advertises |
//! | [`size_proof`] | The [`SizeProof`] that proves a stated blob size |
//! | [`identity`] | The 32-byte [`NodeId`] and [`ContentHash`] newtypes |
//! | [`chain_id`] | The CAIP-2 [`ChainId`] of a chain |
//! | [`pool_account`] | The CAIP-10 [`PoolAccount`] that names a `PaymentPool` contract |
//! | [`region`] | ISO 3166-1 alpha-2 [`Region`] codes |
//! | [`message`] | Items every signed message shares: [`MAX_RATE_PER_MB`], [`OPERATOR_SIG_LEN`], [`MessageValidationError`], [`EMPTY_ROOT`] |
//!
//! # Frames and extensions
//!
//! Every message is a postcard encoding behind a varint length prefix. Each read
//! caps the prefix at its frame ceiling — [`MAX_CLIENT_CONTROL_FRAME`],
//! [`MAX_STREAM_RESPONSE_FRAME`], [`MAX_CHUNK_FRAME`] or [`MAX_DHT_FRAME`]
//! ([ADR 013 § Wire Framing]). A signed message has a frozen
//! base: the fields its signature covers. Unsigned fields travel as trailing
//! bytes after the base. [`decode_message`] returns them as a remainder, and the
//! `parse_*_ext` functions read them ([ADR 013 § Three Tiers of Evolution]).
//!
//! ```
//! use decdn_protocol::{ClientMessage, StreamRequest, decode_message, encode_message};
//!
//! let request = ClientMessage::StreamRequest(StreamRequest {
//!     hash: [7; 32],
//!     namespace_id: [0; 32],
//!     payment_pool: "eip155:421614:0x0000000000000000000000000000000000000001".parse()?,
//!     pool_id: [9; 32],
//!     byte_offset: 0,
//!     byte_len: 0,
//!     timestamp_us: 1_700_000_000_000_000,
//! });
//! let frame = encode_message(&request)?;
//! let (decoded, remainder) = decode_message::<ClientMessage>(&frame)?;
//! assert_eq!(decoded, request);
//! assert!(remainder.is_empty());
//! # Ok::<(), Box<dyn std::error::Error>>(())
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

pub mod chain_id;
pub mod client;
pub mod coverage;
pub mod dht;
pub mod framing;
pub mod identity;
pub mod message;
pub mod pool_account;
pub mod region;
pub mod size_proof;

pub use chain_id::{ChainId, ChainIdError, MAX_CHAIN_ID_LEN};
pub use client::{
    BINDING_SIG_LEN, CHUNK_BYTES, ChunkData, ChunkPreimage, ClientBinding, ClientMessage,
    MAX_CHAIN_LENGTH, MAX_CHUNK_FRAME, MAX_CLIENT_CONTROL_FRAME, MAX_STREAM_RESPONSE_FRAME,
    MB_BYTES, StreamError, StreamRequest, StreamRequestExt, StreamResponse, StreamResponseBody,
    StreamResponseExt, VOUCHER_SIG_LEN, Voucher, VoucherRejectReason, encode_chunk_frame,
    encode_stream_request, encode_stream_response, parse_stream_request_ext,
    parse_stream_response_ext,
};
pub use coverage::{
    Coverage, DISCOVERY_BLOCK_BYTES, byte_range_blocks, discovery_block_bytes, num_blocks,
};
#[cfg(feature = "test-support")]
pub use coverage::{TestBlockSizeGuard, override_discovery_block_bytes_for_test};
pub use dht::{
    BatchStoreAck, BatchStoreRequest, CloserNodes, CloserNodesError, DEFAULT_ATTESTATION_TTL_SEC,
    DhtMessage, FindNodeRequest, FindNodeResponse, FindValueRequest, FindValueResponse,
    FindValueResponseExt, MAX_ATTESTATION_FUTURE_SKEW_US, MAX_ATTESTATION_TTL_SEC,
    MAX_BATCH_STORE_HASHES, MAX_CLOSER_NODES, MAX_DHT_FRAME, MAX_PROVIDERS_PER_HASH,
    MIN_ATTESTATION_TTL_SEC, OriginAttestation, Provider, StoreAck, StoreRequest, StoreRequestExt,
    encode_find_value_response, encode_store_request, parse_find_value_response_ext,
    parse_store_request_ext,
};
pub use framing::{
    FrameError, TopLevelEnum, decode_message, encode_message, is_unknown_variant, read_frame,
    write_frame,
};
pub use identity::{ContentHash, ID_LEN, NodeId};
pub use message::{EMPTY_ROOT, MAX_RATE_PER_MB, MessageValidationError, OPERATOR_SIG_LEN};
pub use pool_account::{MAX_PAYMENT_POOLS, MAX_POOL_ACCOUNT_LEN, PoolAccount, PoolAccountError};
pub use region::{InvalidRegion, Region, is_valid_region};
pub use size_proof::{MAX_SIZE_PROOF_BYTES, SizeProof, SizeProofTooLong};

/// ALPN protocol identifier for paid blob delivery (client-to-node and node-to-node).
pub const ALPN_CLIENT: &[u8] = b"cdn/client/v1";

/// ALPN protocol identifier for Kademlia-based content discovery (ADR 022).
pub const ALPN_DHT: &[u8] = b"cdn/dht/v1";

/// Connection or stream rejected by a node's concurrency cap or rate limiter
/// (ADR 013 §0x10).
///
/// A node that sheds a whole connection (the connection cap or a per-connection
/// limiter, before any application stream exists) sends it as
/// `CONNECTION_CLOSE`, and the close-frame reason bytes carry a short layer
/// label (e.g. `global-full`, `per_peer`) so the peer can pick an appropriate
/// backoff. A node that sheds one stream on an open connection (the
/// `cdn/client/v1` stream cap or request limiter) sends it as `RESET_STREAM`,
/// which carries no reason bytes. Peers that receive this code SHOULD back off before
/// reconnecting; they MUST NOT treat it as a protocol error (the server is
/// functioning normally — the client is overloading it). The requester side
/// (`decdn-client`) types this code as `UpstreamRateLimited` so the pull
/// orchestrator suppresses the peer briefly instead of scoring it unreachable.
pub const APP_ERR_RATE_LIMITED: u32 = 0x10;

/// Request refused because its `timestamp_us` differs from the node's clock
/// by more than 30 seconds (ADR 013 §Application Error Codes; ADR 005
/// §`cdn/client/v1`). The signed response body echoes the requester's
/// timestamp into the EIP-712 signed set, so a node refuses such a request
/// with no signed response.
///
/// Delivered as a `RESET_STREAM` on `cdn/client/v1`. The usual cause is the
/// requester's clock. If only one node refuses, that node's clock is wrong.
/// The requester side (`decdn-client`) types this code as
/// `UpstreamTimestampRejected` so the failure names the clock instead of
/// reading as an unreachable peer. A requester that has no mapping for this
/// code treats it as a generic transport error, which is safe: the request
/// fails.
pub const APP_ERR_TIMESTAMP_SKEW: u32 = 0x04;
