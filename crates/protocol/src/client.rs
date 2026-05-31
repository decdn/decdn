//! Wire message payload types for the `cdn/client/v1` paid-delivery protocol.
//!
//! `cdn/client/v1` is the paid byte-transfer path (ADR 005 §`cdn/client/v1`):
//! a payer opens a bidirectional QUIC stream, sends a [`StreamRequest`], and
//! the delivering node answers with a [`StreamResponse`] followed by a loop of
//! [`ChunkData`] interleaved with cumulative payment [`Voucher`]s and
//! [`ClientMessage::VoucherAck`]s, terminating in [`ClientMessage::StreamEnd`].
//! A delivery or payment fault is signalled by [`StreamError`].
//!
//! Like [`crate::message`] this is a leaf crate with **no crypto dependency**.
//! The two signed artifacts on this protocol are produced/verified by
//! `decdn_incentive`:
//!   - `StreamResponse.slash_sig` — an EIP-712 secp256k1 signature over the
//!     signed body fields `{hash, ok, rate_per_mb, total_bytes, channel_id,
//!     timestamp_us, redirect}` (ADR 014 §1, `decdn_incentive::StreamSlashData`).
//!     `error` and `voucher_interval_mb` are unsigned (ADR 005 §Voucher interval
//!     negotiation).
//!   - `Voucher.signature` — an EIP-712 secp256k1 voucher signature; the wire
//!     carries `{signature, amount, nonce}` and the receiver reconstructs the
//!     full typed data `{channelId, amount, nonce, bytesDelivered, token}` from
//!     stream context (ADR 005 §Voucher wire format, `decdn_incentive::Voucher`).
//!
//! # Signed-field freezing (ADR 013 §Signed Field Freezing)
//!
//! The signed set on [`StreamResponseBody`] is the `cdn/client/v1` v1 baseline.
//! Any change to it is a Tier-3 ALPN bump. New unsigned fields go on
//! [`StreamResponse`] as optional fields, following the `total_bytes` exemplar
//! on [`crate::message::ProbeResponse`].
//!
//! # Variant order
//!
//! [`ClientMessage`], [`StreamError`], and [`VoucherRejectReason`] all have
//! **frozen** variant order — postcard encodes each variant as its
//! declaration-order index. Reordering is a wire-breaking change; the
//! `*_discriminant_is_*` / `*_variant_order` tests lock the assignments.

use serde::{Deserialize, Serialize};

use crate::identity::NodeId;
use crate::message::{MAX_RATE_PER_MB, MessageValidationError, SLASH_SIG_LEN};

/// Exact byte length of a `ChunkData` payload, except the final chunk which MAY
/// be smaller (ADR 005 §`cdn/client/v1`, §Partial final chunk). Matches
/// iroh-blobs' internal 1024-byte chunk granularity; the voucher cadence
/// (`voucher_interval_mb`, default 1 MiB) is coarser, so a buffering layer sits
/// between the payment and transfer tick rates (ADR 005 §Tradeoffs).
pub const CHUNK_SIZE: usize = 1024;

/// One megabyte in bytes, the unit of `voucher_interval_mb` and `rate_per_mb`
/// (ADR 003: 1 MB = 1,048,576 bytes, exactly). The node pauses delivery when
/// outstanding unvouchered bytes exceed `voucher_interval_mb * MB_BYTES`.
pub const MB_BYTES: u64 = 1_048_576;

/// Default voucher cadence when neither peer proposes one (ADR 005 §Voucher
/// interval negotiation).
pub const DEFAULT_VOUCHER_INTERVAL_MB: u64 = 1;

/// Governable upper bound on a proposed `voucher_interval_mb` (ADR 003 §Voucher
/// Interval Negotiation: range 1..=1024 MB).
pub const MAX_VOUCHER_INTERVAL_MB: u64 = 1024;

/// Exact byte length of an EOA secp256k1 voucher signature (`r‖s‖v`, 32+32+1).
/// Mirrors [`SLASH_SIG_LEN`]; both are the EOA off-chain signing form (ADR 024
/// §18). Carried as a `Vec<u8>` on the wire (serde derives array impls only up
/// to `[T; 32]`), with the length pinned by [`Voucher::validate`].
pub const VOUCHER_SIG_LEN: usize = 65;

/// Top-level protocol enum for `cdn/client/v1`. Variant order is frozen per
/// ADR 013 — new variants MUST be appended at the end.
///
/// ⚠️ **VARIANT ORDER FROZEN — ADR 013 §Protocol Enums**
/// Postcard encodes each variant as its declaration-order index. Reordering,
/// inserting, or removing a variant is a wire-breaking change requiring an ALPN
/// version bump (`cdn/client/v2`). The discriminant assignments are locked by
/// the `client_message_*_discriminant_is_*` tests.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ClientMessage {
    /// discriminant 0 — payer → node, opens a delivery.
    StreamRequest(StreamRequest),
    /// discriminant 1 — node → payer, answers a [`StreamRequest`].
    StreamResponse(StreamResponse),
    /// discriminant 2 — node → payer, one sequential blob chunk.
    ChunkData(ChunkData),
    /// discriminant 3 — payer → node, cumulative payment voucher.
    Voucher(Voucher),
    /// discriminant 4 — node → payer, acknowledges an accepted [`Voucher`].
    VoucherAck,
    /// discriminant 5 — payer → node, signals the payer received the full blob.
    StreamEnd,
    /// discriminant 6 — node → payer, mid-stream failure (carries
    /// [`StreamError::VoucherRejected`]); delivery-side errors instead ride in
    /// [`StreamResponse::error`].
    StreamError(StreamError),
}

/// Payer → node request opening a paid delivery (ADR 005 §`cdn/client/v1`).
///
/// `timestamp_us` is a requester-generated microsecond timestamp echoed back in
/// [`StreamResponseBody::timestamp_us`]; it both correlates the response and
/// drives rate-manipulation slashing (ADR 005, ADR 014). `byte_offset` supports
/// seek/resume on failover — the requester reconnects to another node and
/// resumes from the last BLAKE3-verified byte.
///
/// This struct holds **only the frozen base fields** (ADR 013 §Tier 1). The
/// optional [`StreamRequestExt`] is carried as **separate trailing bytes** after
/// this message in the frame, parsed via [`parse_stream_request_ext`] — *not* as
/// an embedded field. This is the ADR 005 §Client identity binding two-phase
/// deserialization pattern: keeping the base frozen and parsing extension bytes
/// separately means a future field added to [`StreamRequestExt`] stays
/// backward-compatible (old senders simply emit fewer trailing bytes; new
/// optional fields are appended and old receivers skip them). Embedding `ext` as
/// a nested struct field instead would break old `Some(...)` senders the moment
/// a field is added (postcard would hit EOF inside the nested struct). Use
/// [`encode_stream_request`] to build the wire payload with an optional `ext`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamRequest {
    /// BLAKE3 hash of the requested blob (iroh `Hash`, 32 bytes).
    pub hash: [u8; 32],
    /// `channelId = keccak256(client, provider, channelNonce)` (ADR 003).
    pub channel_id: [u8; 32],
    /// Resume position in bytes; `0` for a full-blob fetch.
    pub byte_offset: u64,
    /// Requester-generated microseconds since the Unix epoch, echoed back.
    pub timestamp_us: u64,
}

/// Optional [`StreamRequest`] extension fields (ADR 005 §Client identity
/// binding), carried as trailing bytes after the `StreamRequest` message via the
/// two-phase pattern (see [`encode_stream_request`] / [`parse_stream_request_ext`]).
/// All inner fields are independently optional.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct StreamRequestExt {
    /// Proposed voucher cadence for this stream in MB; the node answers with an
    /// equal-or-smaller value in [`StreamResponse::voucher_interval_mb`]. Absent
    /// ⇒ both sides default to [`DEFAULT_VOUCHER_INTERVAL_MB`].
    pub voucher_interval_mb: Option<u64>,
    /// Ephemeral client Ethereum address (20 bytes) for vouchers issued by an
    /// off-chain (unregistered) client. Attested by `binding_signature`.
    pub ethereum_address: Option<[u8; 20]>,
    /// EIP-712 `BindNodeId(nodeId, nonce=0)` signature attesting the
    /// NodeId↔Ethereum-address mapping for the connection's lifetime (ADR 003
    /// §Off-Chain Ephemeral Binding). Verified by `decdn_incentive` via
    /// `ecrecover`.
    pub binding_signature: Option<Vec<u8>>,
}

/// Encode a `cdn/client/v1` `StreamRequest` frame payload with an optional
/// trailing [`StreamRequestExt`] (ADR 005 §Client identity binding, two-phase).
///
/// The result is the full frame payload to pass to [`crate::write_frame`]: the
/// postcard-encoded `ClientMessage::StreamRequest(req)` followed, when `ext` is
/// `Some`, by the postcard-encoded extension. The receiver recovers `req` from
/// [`crate::decode_message`] and the extension from that call's returned
/// remainder via [`parse_stream_request_ext`].
///
/// # Errors
///
/// Propagates a [`postcard::Error`] if serialization fails.
pub fn encode_stream_request(
    req: &StreamRequest,
    ext: Option<&StreamRequestExt>,
) -> Result<Vec<u8>, postcard::Error> {
    let mut buf = postcard::to_allocvec(&ClientMessage::StreamRequest(req.clone()))?;
    if let Some(ext) = ext {
        buf.extend_from_slice(&postcard::to_allocvec(ext)?);
    }
    Ok(buf)
}

/// Parse the trailing [`StreamRequestExt`] bytes returned as the remainder by
/// [`crate::decode_message`] after a `ClientMessage::StreamRequest`.
///
/// An empty remainder ⇒ [`StreamRequestExt::default`] (no client binding,
/// default cadence). Trailing bytes beyond the known fields are tolerated for
/// forward compatibility (ADR 013 §Tier 1): a future optional field appended to
/// [`StreamRequestExt`] is read by new receivers and skipped by old ones.
///
/// # Errors
///
/// Returns a [`postcard::Error`] if a non-empty remainder is not a valid
/// `StreamRequestExt` prefix.
pub fn parse_stream_request_ext(remainder: &[u8]) -> Result<StreamRequestExt, postcard::Error> {
    if remainder.is_empty() {
        Ok(StreamRequestExt::default())
    } else {
        Ok(postcard::take_from_bytes::<StreamRequestExt>(remainder)?.0)
    }
}

/// Node → payer response to a [`StreamRequest`] (ADR 005 §`cdn/client/v1`).
///
/// The signed [`StreamResponseBody`] is covered by `slash_sig`; `error` and
/// `voucher_interval_mb` are unsigned. `slash_sig` is mandatory and non-empty
/// (exactly [`SLASH_SIG_LEN`] bytes); requesters MUST reject missing/zero-length
/// or zero-`rate_per_mb` responses (enforced via [`StreamResponse::validate`]).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamResponse {
    /// Signed body. Its wire layout is frozen per ADR 013.
    pub body: StreamResponseBody,
    /// Delivery-side failure code when `body.ok == false` (`NotFound`,
    /// `Overloaded`, `BlobTooLarge`, `InternalError`, `EvictedSinceProbe`).
    /// Unsigned and informational. `VoucherRejected` never rides here — it is
    /// delivered mid-stream via [`ClientMessage::StreamError`].
    pub error: Option<StreamError>,
    /// The node's accepted voucher cadence in MB (≤ the proposed value).
    /// Unsigned; absent ⇒ [`DEFAULT_VOUCHER_INTERVAL_MB`] (ADR 005 §Voucher
    /// interval negotiation).
    pub voucher_interval_mb: Option<u64>,
    /// EIP-712 secp256k1 signature over `body`'s signed fields (ADR 014 §1; see
    /// `decdn_incentive::StreamSlashData`). Always exactly [`SLASH_SIG_LEN`]
    /// bytes — *not* a signature over postcard bytes.
    pub slash_sig: Vec<u8>,
}

/// Signed fields of a [`StreamResponse`] (ADR 014 §1). Layout is frozen per
/// ADR 013 — future additions go on [`StreamResponse`] as optional unsigned
/// fields, not here.
///
/// `rate_per_mb` is bounded by [`MAX_RATE_PER_MB`] at the wire boundary via the
/// shared `deserialize_rate_per_mb` hook (#378), keeping the bound bilateral
/// with the node's config layer.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamResponseBody {
    /// BLAKE3 hash this response answers for, echoed from the request.
    pub hash: [u8; 32],
    /// Whether the node will serve the blob. A node MUST NOT sign `true` unless
    /// it can deliver within the slashing window.
    pub ok: bool,
    /// Quoted rate in token base units per MB. Bounded by [`MAX_RATE_PER_MB`].
    #[serde(deserialize_with = "crate::message::deserialize_rate_per_mb")]
    pub rate_per_mb: u64,
    /// Total blob size in bytes (used for `BlobTooLarge` enforcement on
    /// cache-miss pulls — ADR 005 §`BlobTooLarge` enforcement).
    pub total_bytes: u64,
    /// `channel_id` echoed from the [`StreamRequest`] (signed, so a node cannot
    /// silently re-bind the response to a different channel).
    pub channel_id: [u8; 32],
    /// Requester-generated microsecond timestamp from the [`StreamRequest`],
    /// echoed back unchanged.
    pub timestamp_us: u64,
    /// Alternate provider to retry when this node cannot serve — a `NodeId`,
    /// never an external URL (ADR 005). Signed as `bytes32(0)` when `None`
    /// (`decdn_incentive::StreamSlashData`).
    pub redirect: Option<NodeId>,
}

impl StreamResponse {
    /// Validate requester-side invariants (ADR 005, ADR 014 §1, #252):
    /// `rate_per_mb` within `(0, MAX_RATE_PER_MB]` and `slash_sig` exactly
    /// [`SLASH_SIG_LEN`] bytes. The wire-decode path already enforces the upper
    /// `rate_per_mb` bound via `deserialize_rate_per_mb`; this method adds the
    /// zero-rate and `slash_sig`-length obligations and is exposed so
    /// requesters re-check on receive and construction sites assert validity
    /// before signing.
    pub const fn validate(&self) -> Result<(), MessageValidationError> {
        if self.body.rate_per_mb > MAX_RATE_PER_MB {
            return Err(MessageValidationError::RateTooLarge {
                rate: self.body.rate_per_mb,
            });
        }
        // #252: requesters MUST reject a zero rate on receive — same rule as
        // ProbeResponse. Shared `RateIsZero` so both paths fail identically.
        if self.body.rate_per_mb == 0 {
            return Err(MessageValidationError::RateIsZero);
        }
        if self.slash_sig.len() != SLASH_SIG_LEN {
            return Err(MessageValidationError::InvalidSlashSigLen {
                len: self.slash_sig.len(),
            });
        }
        Ok(())
    }
}

/// Node → payer chunk of blob bytes. Payload is at most [`CHUNK_SIZE`] bytes;
/// the final chunk before [`ClientMessage::StreamEnd`] MAY be smaller and
/// receivers MUST accept it (ADR 005 §Partial final chunk).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChunkData {
    /// Sequential blob bytes (≤ [`CHUNK_SIZE`]).
    pub bytes: Vec<u8>,
}

/// Payer → node cumulative payment voucher (ADR 005 §Voucher wire format).
///
/// Only `{signature, amount, nonce}` travel on the wire; the receiver
/// reconstructs the full EIP-712 typed data `{channelId, amount, nonce,
/// bytesDelivered, token}` from stream context (`channel_id` from the
/// [`StreamRequest`], `token` fixed at channel open, `bytesDelivered` the node's
/// per-channel cumulative counter). `amount` and `nonce` are 256-bit values in
/// big-endian bytes — the protocol crate has no `U256`, and truncating to
/// `u64` would break channels whose on-chain nonce exceeds `u64::MAX`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Voucher {
    /// EOA secp256k1 EIP-712 signature (`r‖s‖v`, exactly [`VOUCHER_SIG_LEN`]).
    pub signature: Vec<u8>,
    /// Cumulative payment in token base units, big-endian `uint256`.
    pub amount: [u8; 32],
    /// Voucher sequence number within the channel (starts at 1), big-endian
    /// `uint256`.
    pub nonce: [u8; 32],
}

impl Voucher {
    /// Validate the wire-level `signature` length ([`VOUCHER_SIG_LEN`]). The
    /// cryptographic checks (recovery, monotonicity, deposit bound) happen in
    /// `decdn_incentive` once the typed data is reconstructed.
    pub const fn validate(&self) -> Result<(), MessageValidationError> {
        if self.signature.len() != VOUCHER_SIG_LEN {
            return Err(MessageValidationError::InvalidVoucherSigLen {
                len: self.signature.len(),
            });
        }
        Ok(())
    }
}

/// A stream failure code (ADR 005 §Stream errors). Variant order is frozen.
///
/// The delivery-side variants (`NotFound`..`EvictedSinceProbe`) ride in
/// [`StreamResponse::error`] alongside `ok: false`; `VoucherRejected` is the
/// only variant delivered mid-stream, inside a [`ClientMessage::StreamError`].
/// All codes are unsigned and informational — never on-chain evidence.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum StreamError {
    /// Node lacks the blob and cannot reach a provider, or declines to pull
    /// through (ADR 037 seed-leech caps).
    NotFound,
    /// Node is at capacity; try another node.
    Overloaded,
    /// Blob exceeds the node's configured `max_blob_size`; do not retry it.
    BlobTooLarge,
    /// Unexpected failure; do not retry this node.
    InternalError,
    /// Blob was evicted between probe and stream request. WARNING: still
    /// slashable after a signed `has_blob: true` probe (ADR 005).
    EvictedSinceProbe,
    /// Mid-stream payment-voucher rejection carried in a
    /// [`ClientMessage::StreamError`] message (never in the initial
    /// [`StreamResponse`]).
    VoucherRejected {
        /// The specific validation failure.
        reason: VoucherRejectReason,
    },
}

/// Why a [`Voucher`] was rejected (ADR 005 §`VoucherRejected` semantics).
///
/// Mirrors `decdn_incentive::ChannelError` ∪ `VoucherError` one-to-one (minus
/// the transient `Store` failure, which is not a rejection — the client retries
/// the same voucher). Variant order is frozen; the handler-side conversion
/// `voucher_reject_reason` matches exhaustively so a new `ChannelError` variant
/// fails to compile until this enum is extended (ADR 005 §Mirror obligation).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum VoucherRejectReason {
    /// Signature malformed (corrupted bytes, non-canonical `s`, invalid
    /// recovery id). `VoucherError::InvalidSignature`.
    BadSignature,
    /// Signature well-formed but recovers to the wrong signer.
    /// `VoucherError::WrongSigner`.
    WrongSigner,
    /// `voucher.channel_id` mismatch (also: unknown channel).
    /// `ChannelError::WrongChannel`.
    WrongChannel,
    /// `voucher.token` mismatch — cross-token replay defense.
    /// `ChannelError::WrongToken`.
    WrongToken,
    /// Nonce did not strictly increase. `ChannelError::NonceNotIncreasing`.
    StaleNonce,
    /// Cumulative amount regressed. `ChannelError::AmountDecreasing`.
    AmountRegression,
    /// Cumulative bytes delivered regressed. `ChannelError::BytesDecreasing`.
    BytesRegression,
    /// Voucher amount exceeds the channel deposit.
    /// `ChannelError::AmountExceedsDeposit`.
    InsufficientDeposit,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::framing::{decode_message, encode_message, read_frame, write_frame};

    fn sample_body() -> StreamResponseBody {
        StreamResponseBody {
            hash: [7u8; 32],
            ok: true,
            rate_per_mb: 10,
            total_bytes: 4096,
            channel_id: [9u8; 32],
            timestamp_us: 1_700_000_000_000_000,
            redirect: None,
        }
    }

    fn sample_response() -> StreamResponse {
        StreamResponse {
            body: sample_body(),
            error: None,
            voucher_interval_mb: Some(1),
            slash_sig: vec![0xABu8; SLASH_SIG_LEN],
        }
    }

    fn sample_request() -> StreamRequest {
        StreamRequest {
            hash: [1u8; 32],
            channel_id: [2u8; 32],
            byte_offset: 0,
            timestamp_us: 0xdead_beef,
        }
    }

    fn sample_ext() -> StreamRequestExt {
        StreamRequestExt {
            voucher_interval_mb: Some(8),
            ethereum_address: Some([0xEEu8; 20]),
            binding_signature: Some(vec![0x01u8; VOUCHER_SIG_LEN]),
        }
    }

    fn sample_voucher() -> Voucher {
        Voucher {
            signature: vec![0xCDu8; VOUCHER_SIG_LEN],
            amount: [0x11u8; 32],
            nonce: [0x22u8; 32],
        }
    }

    // --- Roundtrips ----------------------------------------------------------

    #[test]
    fn stream_request_roundtrip() -> Result<(), postcard::Error> {
        let req = sample_request();
        let bytes = postcard::to_allocvec(&req)?;
        let decoded: StreamRequest = postcard::from_bytes(&bytes)?;
        assert_eq!(req, decoded);
        Ok(())
    }

    /// Two-phase: a request with no ext encodes to just the `ClientMessage`,
    /// and `decode_message` yields an empty remainder → `Ext::default()`.
    #[test]
    fn stream_request_two_phase_no_ext() -> Result<(), crate::framing::FrameError> {
        let req = sample_request();
        let payload = encode_stream_request(&req, None)?;
        let (msg, remainder) = decode_message::<ClientMessage>(&payload)?;
        assert_eq!(msg, ClientMessage::StreamRequest(req));
        assert!(remainder.is_empty(), "no ext ⇒ no trailing bytes");
        assert_eq!(
            parse_stream_request_ext(remainder)?,
            StreamRequestExt::default()
        );
        Ok(())
    }

    /// Two-phase: a request with ext appends the extension bytes after the
    /// message; `decode_message` returns them as the remainder and
    /// `parse_stream_request_ext` recovers the ext.
    #[test]
    fn stream_request_two_phase_with_ext() -> Result<(), crate::framing::FrameError> {
        let req = sample_request();
        let ext = sample_ext();
        let payload = encode_stream_request(&req, Some(&ext))?;
        let (msg, remainder) = decode_message::<ClientMessage>(&payload)?;
        assert_eq!(msg, ClientMessage::StreamRequest(req));
        assert!(!remainder.is_empty(), "ext present ⇒ trailing bytes");
        assert_eq!(parse_stream_request_ext(remainder)?, ext);
        Ok(())
    }

    /// Forward compatibility (ADR 013 §Tier 1): a future field appended to
    /// `StreamRequestExt` shows up as extra trailing bytes; an old parser reads
    /// the known fields and ignores the rest rather than failing on EOF. This is
    /// exactly what embedding `ext` as a struct field would have broken.
    #[test]
    fn stream_request_ext_tolerates_future_trailing_bytes() -> Result<(), postcard::Error> {
        let ext = sample_ext();
        let mut bytes = postcard::to_allocvec(&ext)?;
        bytes.extend_from_slice(&[0xAAu8, 0xBB, 0xCC]); // simulated future field
        assert_eq!(parse_stream_request_ext(&bytes)?, ext);
        Ok(())
    }

    #[test]
    fn stream_response_roundtrip() -> Result<(), postcard::Error> {
        let resp = sample_response();
        let bytes = postcard::to_allocvec(&resp)?;
        let decoded: StreamResponse = postcard::from_bytes(&bytes)?;
        assert_eq!(resp, decoded);
        Ok(())
    }

    #[test]
    fn stream_response_redirect_some_roundtrip() -> Result<(), postcard::Error> {
        let resp = StreamResponse {
            body: StreamResponseBody {
                redirect: Some(NodeId::from_bytes([0x5Au8; 32])),
                ..sample_body()
            },
            ..sample_response()
        };
        let bytes = postcard::to_allocvec(&resp)?;
        let decoded: StreamResponse = postcard::from_bytes(&bytes)?;
        assert_eq!(resp, decoded);
        Ok(())
    }

    #[test]
    fn stream_response_error_roundtrip() -> Result<(), postcard::Error> {
        let resp = StreamResponse {
            body: StreamResponseBody {
                ok: false,
                ..sample_body()
            },
            error: Some(StreamError::BlobTooLarge),
            ..sample_response()
        };
        let bytes = postcard::to_allocvec(&resp)?;
        let decoded: StreamResponse = postcard::from_bytes(&bytes)?;
        assert_eq!(resp, decoded);
        Ok(())
    }

    #[test]
    fn chunk_data_roundtrip() -> Result<(), postcard::Error> {
        let chunk = ChunkData {
            bytes: vec![0x42u8; CHUNK_SIZE],
        };
        let bytes = postcard::to_allocvec(&chunk)?;
        let decoded: ChunkData = postcard::from_bytes(&bytes)?;
        assert_eq!(chunk, decoded);
        Ok(())
    }

    #[test]
    fn voucher_roundtrip() -> Result<(), postcard::Error> {
        let v = sample_voucher();
        let bytes = postcard::to_allocvec(&v)?;
        let decoded: Voucher = postcard::from_bytes(&bytes)?;
        assert_eq!(v, decoded);
        Ok(())
    }

    #[test]
    fn stream_error_voucher_rejected_roundtrip() -> Result<(), postcard::Error> {
        let e = StreamError::VoucherRejected {
            reason: VoucherRejectReason::StaleNonce,
        };
        let bytes = postcard::to_allocvec(&e)?;
        let decoded: StreamError = postcard::from_bytes(&bytes)?;
        assert_eq!(e, decoded);
        Ok(())
    }

    // --- Discriminant pins (FROZEN order) ------------------------------------

    fn first_byte<T: Serialize>(v: &T) -> Result<u8, postcard::Error> {
        Ok(postcard::to_allocvec(v)?.first().copied().unwrap_or(0xFF))
    }

    #[test]
    fn client_message_discriminants_are_frozen() -> Result<(), postcard::Error> {
        assert_eq!(
            first_byte(&ClientMessage::StreamRequest(sample_request()))?,
            0
        );
        assert_eq!(
            first_byte(&ClientMessage::StreamResponse(sample_response()))?,
            1
        );
        assert_eq!(
            first_byte(&ClientMessage::ChunkData(ChunkData { bytes: vec![] }))?,
            2
        );
        assert_eq!(first_byte(&ClientMessage::Voucher(sample_voucher()))?, 3);
        assert_eq!(first_byte(&ClientMessage::VoucherAck)?, 4);
        assert_eq!(first_byte(&ClientMessage::StreamEnd)?, 5);
        assert_eq!(
            first_byte(&ClientMessage::StreamError(StreamError::NotFound))?,
            6
        );
        Ok(())
    }

    #[test]
    fn stream_error_variant_order_is_frozen() -> Result<(), postcard::Error> {
        for (i, e) in [
            StreamError::NotFound,
            StreamError::Overloaded,
            StreamError::BlobTooLarge,
            StreamError::InternalError,
            StreamError::EvictedSinceProbe,
            StreamError::VoucherRejected {
                reason: VoucherRejectReason::BadSignature,
            },
        ]
        .into_iter()
        .enumerate()
        {
            assert_eq!(first_byte(&e)?, u8::try_from(i).unwrap_or(0xFF));
        }
        Ok(())
    }

    #[test]
    fn voucher_reject_reason_variant_order_is_frozen() -> Result<(), postcard::Error> {
        for (i, r) in [
            VoucherRejectReason::BadSignature,
            VoucherRejectReason::WrongSigner,
            VoucherRejectReason::WrongChannel,
            VoucherRejectReason::WrongToken,
            VoucherRejectReason::StaleNonce,
            VoucherRejectReason::AmountRegression,
            VoucherRejectReason::BytesRegression,
            VoucherRejectReason::InsufficientDeposit,
        ]
        .into_iter()
        .enumerate()
        {
            assert_eq!(first_byte(&r)?, u8::try_from(i).unwrap_or(0xFF));
        }
        Ok(())
    }

    #[test]
    fn client_message_rejects_unknown_discriminant() {
        let bytes = [99u8, 0, 0, 0, 0];
        let r: Result<ClientMessage, _> = postcard::from_bytes(&bytes);
        assert!(r.is_err());
    }

    // --- Wire-format stability (fixed bytes) ---------------------------------

    #[allow(clippy::cast_possible_truncation)]
    #[test]
    fn stream_response_wire_format_is_stable() -> Result<(), postcard::Error> {
        let resp = StreamResponse {
            body: StreamResponseBody {
                hash: [3u8; 32],
                ok: true,
                rate_per_mb: 4,
                total_bytes: 5,
                channel_id: [6u8; 32],
                timestamp_us: 7,
                redirect: None,
            },
            error: None,
            voucher_interval_mb: None,
            slash_sig: vec![0xABu8; SLASH_SIG_LEN],
        };
        let bytes = postcard::to_allocvec(&resp)?;
        let mut expected = Vec::new();
        expected.extend_from_slice(&[3u8; 32]); // body.hash
        expected.push(1u8); // body.ok = true
        expected.push(4u8); // body.rate_per_mb varint
        expected.push(5u8); // body.total_bytes varint
        expected.extend_from_slice(&[6u8; 32]); // body.channel_id
        expected.push(7u8); // body.timestamp_us varint
        expected.push(0u8); // body.redirect = None
        expected.push(0u8); // error = None
        expected.push(0u8); // voucher_interval_mb = None
        expected.push(SLASH_SIG_LEN as u8); // slash_sig length prefix (65)
        expected.extend_from_slice(&[0xABu8; SLASH_SIG_LEN]); // slash_sig bytes
        assert_eq!(bytes, expected);
        Ok(())
    }

    #[allow(clippy::cast_possible_truncation)]
    #[test]
    fn voucher_wire_format_is_stable() -> Result<(), postcard::Error> {
        let v = Voucher {
            signature: vec![0xCDu8; VOUCHER_SIG_LEN],
            amount: [0x01u8; 32],
            nonce: [0x02u8; 32],
        };
        let bytes = postcard::to_allocvec(&v)?;
        let mut expected = Vec::new();
        expected.push(VOUCHER_SIG_LEN as u8); // signature length prefix (65)
        expected.extend_from_slice(&[0xCDu8; VOUCHER_SIG_LEN]); // signature
        expected.extend_from_slice(&[0x01u8; 32]); // amount (no length prefix)
        expected.extend_from_slice(&[0x02u8; 32]); // nonce (no length prefix)
        assert_eq!(bytes, expected);
        Ok(())
    }

    // --- Validation ----------------------------------------------------------

    #[test]
    fn stream_response_validate_accepts_sample() {
        assert_eq!(sample_response().validate(), Ok(()));
    }

    #[test]
    fn stream_response_validate_rejects_zero_rate() {
        let resp = StreamResponse {
            body: StreamResponseBody {
                rate_per_mb: 0,
                ..sample_body()
            },
            ..sample_response()
        };
        assert_eq!(resp.validate(), Err(MessageValidationError::RateIsZero));
    }

    #[test]
    fn stream_response_validate_rejects_empty_slash_sig() {
        let resp = StreamResponse {
            slash_sig: Vec::new(),
            ..sample_response()
        };
        assert_eq!(
            resp.validate(),
            Err(MessageValidationError::InvalidSlashSigLen { len: 0 })
        );
    }

    #[test]
    fn stream_response_validate_rejects_wrong_len_slash_sig() {
        let resp = StreamResponse {
            slash_sig: vec![0xAB; SLASH_SIG_LEN - 1],
            ..sample_response()
        };
        assert_eq!(
            resp.validate(),
            Err(MessageValidationError::InvalidSlashSigLen {
                len: SLASH_SIG_LEN - 1
            })
        );
    }

    #[test]
    fn stream_response_decode_rejects_oversize_rate() -> Result<(), postcard::Error> {
        let resp = StreamResponse {
            body: StreamResponseBody {
                rate_per_mb: MAX_RATE_PER_MB + 1,
                ..sample_body()
            },
            ..sample_response()
        };
        let bytes = postcard::to_allocvec(&resp)?;
        let decoded: Result<StreamResponse, _> = postcard::from_bytes(&bytes);
        assert!(decoded.is_err(), "decode must reject rate above MAX");
        Ok(())
    }

    #[test]
    fn voucher_validate_rejects_wrong_len_signature() {
        let v = Voucher {
            signature: vec![0xCD; VOUCHER_SIG_LEN - 1],
            ..sample_voucher()
        };
        assert_eq!(
            v.validate(),
            Err(MessageValidationError::InvalidVoucherSigLen {
                len: VOUCHER_SIG_LEN - 1
            })
        );
        assert_eq!(sample_voucher().validate(), Ok(()));
    }

    // --- Full framing stack --------------------------------------------------

    #[tokio::test]
    async fn client_message_full_stack_roundtrip() -> Result<(), crate::framing::FrameError> {
        let messages = [
            ClientMessage::StreamRequest(sample_request()),
            ClientMessage::StreamResponse(sample_response()),
            ClientMessage::ChunkData(ChunkData {
                bytes: vec![0x7u8; 1000],
            }),
            ClientMessage::Voucher(sample_voucher()),
            ClientMessage::VoucherAck,
            ClientMessage::StreamEnd,
            ClientMessage::StreamError(StreamError::VoucherRejected {
                reason: VoucherRejectReason::InsufficientDeposit,
            }),
        ];
        for msg in messages {
            let payload = encode_message(&msg)?;
            let mut buf = Vec::new();
            write_frame(&mut buf, &payload).await?;
            let mut cursor = std::io::Cursor::new(buf);
            let frame = read_frame(&mut cursor).await?;
            let (decoded, tail) = decode_message::<ClientMessage>(&frame)?;
            assert_eq!(decoded, msg);
            assert!(tail.is_empty(), "no extension bytes expected");
        }
        Ok(())
    }
}
