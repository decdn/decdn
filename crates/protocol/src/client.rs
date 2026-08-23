//! Wire message payload types for the `cdn/client/v1` paid-delivery protocol.
//!
//! `cdn/client/v1` is the paid byte-transfer path (ADR 005 §`cdn/client/v1`):
//! a payer opens a bidirectional QUIC stream, sends a [`StreamRequest`], and
//! the delivering node answers with a [`StreamResponse`] followed by a loop of
//! [`ChunkData`] interleaved with cumulative payment [`Voucher`]s, terminating
//! in [`ClientMessage::StreamEnd`]. Acceptance of a voucher is implicit —
//! delivery simply continues; only rejection is signalled, via
//! [`ClientMessage::StreamError`]. A delivery or payment fault is signalled by
//! [`StreamError`].
//!
//! Like [`crate::message`] this is a leaf crate with **no crypto dependency**.
//! The two signed artifacts on this protocol are produced/verified by
//! `decdn_incentive`:
//!   - `StreamResponse.slash_sig` — an EIP-712 secp256k1 signature over the
//!     signed body fields `{hash, ok, rate_per_mb, total_bytes, pool_id,
//!     timestamp_us, redirect}` (ADR 014 §1), produced by the stream-response
//!     slash signer in `decdn_incentive` (analogous to its `ProbeSlashData`).
//!     `error` is unsigned.
//!   - `Voucher.signature` — an EIP-712 secp256k1 voucher signature; the wire
//!     carries `{signature, amount}` and the receiver reconstructs the full typed
//!     data `{poolId, signer, provider, amount, bytesDelivered}` from stream
//!     context (ADR 005 §Voucher wire format, `decdn_incentive::Voucher`).
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
/// iroh-blobs' internal 1024-byte chunk granularity; the payment quantum
/// ([`CHUNK_BYTES`]) is coarser, so a buffering layer sits between the payment
/// and transfer tick rates (ADR 005 §Tradeoffs).
pub const CHUNK_SIZE: usize = 1024;

/// One megabyte in bytes (ADR 003: 1 MB = 1,048,576 bytes, exactly). The unit
/// of `rate_per_mb` and, by the identity below, of [`CHUNK_BYTES`].
pub const MB_BYTES: u64 = 1_048_576;

/// The payment quantum: one chunk of delivery, 1 MiB (ADR 003 §Chunk Cadence).
///
/// A protocol constant, never negotiated. No message carries it, no node
/// advertises it, and governance does not move it — a chunk is the unit one
/// hash-chain tick pays for, so payer and node disagreeing on it would make one
/// released preimage worth two different amounts.
///
/// `CHUNK_BYTES == MB_BYTES` by identity, which is what makes a chunk cost
/// exactly the advertised `rate_per_mb` with no rounding at any rate.
pub const CHUNK_BYTES: u64 = MB_BYTES;

/// The highest chain index: the hash chain's incremental range over its anchor
/// (ADR 003 §Chain length and rollover).
///
/// The index space is the `u8` domain `0..=255` — 256 slots, exactly one byte.
/// Index 0 names `chain_root` and resolves to the voucher's own `amount`, so
/// the base voucher is payable with no chain at all; it adds no increment,
/// which is why it never travels the wire. Indices `1..=255` each release one
/// preimage and add one `chunk_price` over that anchor, so a chain adds up to
/// 255 MiB at [`CHUNK_BYTES`]. 256 slots = one payable base + 255 increments.
pub const MAX_CHAIN_LENGTH: u8 = 255;

/// Exact byte length of an EOA secp256k1 voucher signature (`r‖s‖v`, 32+32+1).
/// Mirrors [`SLASH_SIG_LEN`]; both are the EOA off-chain signing form (ADR 024
/// §Off-Chain ERC-1271 Verification). Carried as a `Vec<u8>` on the wire (serde
/// derives array impls only up to `[T; 32]`), with the length pinned by
/// [`Voucher::validate`].
pub const VOUCHER_SIG_LEN: usize = 65;

/// Exact byte length of a `BindNodeId` client-binding signature (`r‖s‖v`,
/// 32+32+1). The same EOA off-chain EIP-712 signing form as [`SLASH_SIG_LEN`] /
/// [`VOUCHER_SIG_LEN`] (ADR 024 §Off-Chain ERC-1271 Verification); pinned by
/// [`ClientBinding::validate`].
pub const BINDING_SIG_LEN: usize = 65;

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
    /// discriminant 3 — payer → node, cumulative payment voucher. Acceptance
    /// is implicit — delivery simply continues; only rejection is signalled,
    /// via [`Self::StreamError`].
    Voucher(Voucher),
    /// discriminant 4 — payer → node, one released hash-chain preimage,
    /// advancing the lane's claim by one chunk without a signature (ADR 003
    /// §Hash-chain metering (`PayWord`)). Acceptance is implicit, exactly as for
    /// [`Self::Voucher`].
    ChunkPreimage(ChunkPreimage),
    /// discriminant 5 — payer → node, signals the payer received the full blob.
    StreamEnd,
    /// discriminant 6 — node → payer, mid-stream failure (carries
    /// [`StreamError::VoucherRejected`]); delivery-side errors instead ride in
    /// [`StreamResponse::error`].
    StreamError(StreamError),
}

impl crate::framing::TopLevelEnum for ClientMessage {
    /// `StreamRequest` (0) … `StreamError` (6). Pinned by
    /// `client_message_variant_count_matches_discriminants`.
    const VARIANT_COUNT: u32 = 7;
}

impl ClientMessage {
    /// Validate the payload's requester-side invariants, dispatching to the
    /// per-variant `validate()`. This is the single decode-then-validate seam a
    /// receive-path handler should call after [`crate::decode_message`] so the
    /// zero-rate / signature-length / `ok`-`error` checks can't be silently
    /// skipped (they are not enforced at decode time — see
    /// [`StreamResponse::validate`]).
    ///
    /// `StreamRequest`'s optional [`StreamRequestExt`] travels as separate
    /// trailing bytes (two-phase), so it is *not* reachable from here; validate
    /// it via [`StreamRequestExt::validate`] after [`parse_stream_request_ext`].
    /// Variants with no value invariants (`StreamEnd`, `StreamError`) return
    /// `Ok(())`.
    ///
    /// [`ChunkData`] is dispatched here for TOTALITY over the enum, and for nothing more
    /// (#1145 review). Two facts about this method are easy to misread, and either mistake
    /// leads the next reader to rely on a check that is not here:
    ///
    /// - No receive loop calls this. `read_client_message` decodes and returns; only
    ///   `StreamResponse::validate` runs on the receive path, explicitly, via
    ///   `verify_response`. This aggregate is a convenience, not a load-bearing seam.
    /// - The `ChunkData` arm cannot fail. Its field is private behind `#[serde(try_from)]`,
    ///   which makes `ChunkData::validate` total — its own doc says so ("always `Ok` for a
    ///   frame that exists").
    ///
    /// The empty-frame floor #1088 needs is enforced by the DECODE GATE, not by this
    /// dispatch: an empty frame cannot be constructed *or* deserialized, so no receive loop
    /// has to remember anything. That is what makes the floor structural rather than a
    /// convention.
    ///
    /// # Errors
    ///
    /// Propagates the [`MessageValidationError`] from the wrapped payload's
    /// `validate()`.
    pub const fn validate(&self) -> Result<(), MessageValidationError> {
        match self {
            Self::StreamResponse(resp) => resp.validate(),
            Self::Voucher(voucher) => voucher.validate(),
            Self::ChunkData(chunk) => chunk.validate(),
            Self::ChunkPreimage(preimage) => preimage.validate(),
            Self::StreamRequest(_) | Self::StreamEnd | Self::StreamError(_) => Ok(()),
        }
    }
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
    /// Namespace the content is published under, as a big-endian `uint256`
    /// (ADR 002 §Retrieval by namespace, ADR 005 §Namespace routing). All-zero
    /// = no namespace: the node serves best-effort from local cache / DHT only,
    /// with no authorized origins. A non-zero value routes cache-miss origin
    /// pulls to that namespace's `OriginAssignment` set. It is a **routing hint,
    /// not a trust anchor** — returned bytes are verified against `hash`
    /// independently, so a wrong/hostile value can only fail the fetch, never
    /// corrupt delivery. The node layer converts this to an alloy `U256`;
    /// keeping it `[u8; 32]` here (like `hash`/`pool_id`) leaves `protocol`
    /// alloy-free. Billing-agnostic but load-bearing for routing, so it lives in
    /// the frozen base — every node reads it for origin routing (a node with no
    /// chain origin directory configured resolves nothing from it).
    pub namespace_id: [u8; 32],
    /// `poolId = keccak256(owner, poolNonce)` (ADR 005).
    pub pool_id: [u8; 32],
    /// Resume position in bytes; `0` for a full-blob fetch.
    pub byte_offset: u64,
    /// Upper bound on the requested range: the request covers the half-open span
    /// `[byte_offset, byte_offset + byte_len)`. `0` means "to end-of-blob" (the
    /// whole-tail default, so an unset value preserves the prior behavior). A
    /// non-zero `byte_len` lets a node scope a cache-miss origin fetch to exactly
    /// the requested bytes and meters payment over the range (ADR 005 §Bounded
    /// byte ranges, ADR 037 §Origin-tier pull-through). Billing-relevant, so it
    /// lives in the frozen base — every node must understand it.
    pub byte_len: u64,
    /// Requester-generated microseconds since the Unix epoch, echoed back.
    pub timestamp_us: u64,
}

/// The reserved "no namespace" [`StreamRequest::namespace_id`] value (all-zero
/// big-endian `uint256`): content published without a namespace, served
/// best-effort from cache / DHT with no authorized origins (ADR 002 §Namespace 0).
pub const NO_NAMESPACE: [u8; 32] = [0u8; 32];

/// Optional [`StreamRequest`] extension fields (ADR 005 §Client identity
/// binding), carried as trailing bytes after the `StreamRequest` message via the
/// two-phase pattern (see [`encode_stream_request`] / [`parse_stream_request_ext`]).
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct StreamRequestExt {
    /// Off-chain client identity binding (address + attesting signature). Grouped
    /// so a half-populated state (address without signature, or vice versa) is
    /// unrepresentable; absent ⇒ a registered/on-chain client.
    pub binding: Option<ClientBinding>,
    /// The pool owner's spending capability for this stream's `signer`,
    /// attached at session start so the delivering node can register the
    /// signer on that signer's first on-chain redemption (ADR 003 §Capability
    /// delegation). Node-agnostic: the same capability is valid at every node
    /// the client streams from, since it grants spend against the pool, not
    /// against a specific delivering node. Absent ⇒ the node already has the
    /// signer registered (or the client is relying on a capability it sent on
    /// an earlier stream to a different node this session).
    pub capability: Option<WireCapability>,
}

/// Off-chain ephemeral client identity binding (ADR 003 §Off-Chain Ephemeral
/// Binding), carried inside [`StreamRequestExt`]. The address and its attesting
/// signature are paired so neither can appear without the other.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClientBinding {
    /// Ephemeral client Ethereum address (20 bytes) issuing vouchers for an
    /// off-chain (unregistered) client.
    pub ethereum_address: [u8; 20],
    /// EIP-712 `BindNodeId(nodeId, nonce=0)` signature attesting the
    /// `NodeId`↔`ethereum_address` mapping for the connection's lifetime. Exactly
    /// [`BINDING_SIG_LEN`] bytes; verified by `decdn_incentive` via `ecrecover`.
    pub binding_signature: Vec<u8>,
}

impl ClientBinding {
    /// Validate the wire-level `binding_signature` length ([`BINDING_SIG_LEN`]).
    /// The cryptographic attestation check (`ecrecover` over the `BindNodeId`
    /// typed data) happens in `decdn_incentive`.
    ///
    /// # Errors
    ///
    /// [`MessageValidationError::InvalidBindingSigLen`] if the signature is not
    /// exactly [`BINDING_SIG_LEN`] bytes.
    pub const fn validate(&self) -> Result<(), MessageValidationError> {
        if self.binding_signature.len() != BINDING_SIG_LEN {
            return Err(MessageValidationError::InvalidBindingSigLen {
                len: self.binding_signature.len(),
            });
        }
        Ok(())
    }
}

/// A pool owner's spending capability, carried inside [`StreamRequestExt`]
/// (ADR 003 §Capability delegation). `signer` and `pool_id` are not on the
/// wire — they are derived from stream context, exactly like a [`Voucher`]'s
/// implicit fields: `signer` is the request's bound Ethereum address
/// ([`ClientBinding::ethereum_address`], or the registered on-chain signer
/// when `binding` is absent) and `pool_id` is [`StreamRequest::pool_id`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WireCapability {
    /// Maximum cumulative amount the signer may spend against the pool under
    /// this grant, big-endian `uint256` (mirrors [`Voucher::amount`] — no
    /// `U256` in the protocol crate).
    pub spending_cap: [u8; 32],
    /// Unix-seconds expiry. After it, vouchers under this capability are no
    /// longer redeemable.
    pub expiry: u64,
    /// EIP-712 owner signature over the capability (`r‖s‖v`, 65 bytes for an
    /// EOA owner; may be longer for an ERC-1271 contract owner, so unlike
    /// [`VOUCHER_SIG_LEN`]-pinned signatures this is only checked non-empty at
    /// the wire boundary — [`WireCapability::validate`]). The cryptographic
    /// recovery/verification happens in `decdn_incentive`.
    pub owner_signature: Vec<u8>,
}

impl WireCapability {
    /// Validate the wire-level `owner_signature` non-emptiness. Unlike
    /// [`ClientBinding::validate`] / [`Voucher::validate`] this cannot pin an
    /// exact length — an ERC-1271 contract signature may be longer than the
    /// 65-byte EOA form — so this is a floor, not a full shape check.
    ///
    /// # Errors
    ///
    /// [`MessageValidationError::EmptyCapabilitySignature`] if `owner_signature`
    /// is empty.
    pub const fn validate(&self) -> Result<(), MessageValidationError> {
        if self.owner_signature.is_empty() {
            return Err(MessageValidationError::EmptyCapabilitySignature);
        }
        Ok(())
    }
}

impl StreamRequestExt {
    /// Validate the (if present) client binding and capability. Called by the
    /// node on the receive path after [`parse_stream_request_ext`]; kept
    /// separate from parsing so forward-compatible trailing bytes don't couple
    /// to value checks.
    ///
    /// # Errors
    ///
    /// [`MessageValidationError::InvalidBindingSigLen`] if a present `binding`
    /// has a wrong-length signature; [`MessageValidationError::EmptyCapabilitySignature`]
    /// if a present `capability` has an empty `owner_signature`.
    pub const fn validate(&self) -> Result<(), MessageValidationError> {
        if let Some(binding) = &self.binding {
            // `?` is not yet stable in `const fn`; match-return instead.
            match binding.validate() {
                Ok(()) => {}
                Err(e) => return Err(e),
            }
        }
        if let Some(capability) = &self.capability {
            match capability.validate() {
                Ok(()) => {}
                Err(e) => return Err(e),
            }
        }
        Ok(())
    }
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
/// An empty remainder ⇒ [`StreamRequestExt::default`] (no client binding, no
/// capability). Trailing bytes beyond the known fields are tolerated for
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
/// The signed [`StreamResponseBody`] is covered by `slash_sig`; `error` is
/// unsigned. `slash_sig` is mandatory and non-empty (exactly
/// [`SLASH_SIG_LEN`] bytes); requesters MUST reject missing/zero-length or
/// zero-`rate_per_mb` responses (enforced via [`StreamResponse::validate`]).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamResponse {
    /// Signed body. Its wire layout is frozen per ADR 013.
    pub body: StreamResponseBody,
    /// Delivery-side failure code when `body.ok == false` (`NotFound`,
    /// `Overloaded`, `BlobTooLarge`, `InternalError`, `EvictedSinceProbe`).
    /// Unsigned and informational. `VoucherRejected` never rides here — it is
    /// delivered mid-stream via [`ClientMessage::StreamError`]. The
    /// `ok`/`error` consistency rules and the mid-stream-only exclusion are
    /// enforced by [`StreamResponse::validate`], not just documented.
    pub error: Option<StreamError>,
    /// EIP-712 secp256k1 signature over `body`'s signed fields (ADR 014 §1;
    /// produced by the `decdn_incentive` stream slash signer, analogous to its
    /// `ProbeSlashData`). Always exactly [`SLASH_SIG_LEN`] bytes — *not* a
    /// signature over postcard bytes.
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
    /// `pool_id` echoed from the [`StreamRequest`] (signed, so a node cannot
    /// silently re-bind the response to a different pool).
    pub pool_id: [u8; 32],
    /// Requester-generated microsecond timestamp from the [`StreamRequest`],
    /// echoed back unchanged.
    pub timestamp_us: u64,
    /// Alternate provider to retry when this node cannot serve — a `NodeId`,
    /// never an external URL (ADR 005). Signed as `bytes32(0)` when `None` by the
    /// `decdn_incentive` stream slash signer (analogous to its `ProbeSlashData`).
    pub redirect: Option<NodeId>,
}

impl StreamResponse {
    /// Validate requester-side invariants (ADR 005, ADR 014 §1, #252):
    ///
    /// - `rate_per_mb` within `(0, MAX_RATE_PER_MB]` (the decode path already
    ///   enforces the upper bound via `deserialize_rate_per_mb`; this adds the
    ///   zero-rate rule),
    /// - `slash_sig` exactly [`SLASH_SIG_LEN`] bytes,
    /// - `ok`/`error` consistency: `ok == true` ⇒ no `error`; `ok == false` ⇒
    ///   exactly one delivery-side `error` (never the mid-stream-only
    ///   [`StreamError::VoucherRejected`]).
    ///
    /// Exposed so requesters re-check on receive and construction sites assert
    /// validity before signing.
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
        // ADR 005 §`cdn/client/v1`: the signed `ok` flag and the unsigned
        // `error` code must agree, and `VoucherRejected` is mid-stream-only.
        match (self.body.ok, &self.error) {
            (true, Some(_)) => return Err(MessageValidationError::StreamErrorWithOk),
            (false, None) => return Err(MessageValidationError::MissingStreamError),
            (false, Some(StreamError::VoucherRejected { .. })) => {
                return Err(MessageValidationError::VoucherRejectedInResponse);
            }
            _ => {}
        }
        Ok(())
    }
}

/// Node → payer chunk of blob bytes. Payload is at least 1 and at most
/// [`CHUNK_SIZE`] bytes; the final chunk before [`ClientMessage::StreamEnd`] MAY
/// be smaller and receivers MUST accept it (ADR 005 §Partial final chunk).
///
/// The lower bound is load-bearing, not cosmetic (#1088). "Partial final chunk"
/// permits a *smaller* frame, never an *empty* one: an empty frame carries no
/// payload, so it advances neither the receiver's cumulative byte count nor its
/// voucher accounting. An unbounded run of them therefore drives the receive
/// loops without making application-level progress, and the `cumulative >
/// expected_wire` overrun guard — which only ever trips on bytes — never fires.
/// An empty frame cannot be obtained at all — [`ChunkData::new`] and the `try_from` decode
/// gate both reject one, and they are the only two doors. That is the invariant the pull
/// paths' inactivity deadline rests on: with empty frames banned, "a frame arrived" and
/// "bytes made progress" are the same statement, so a peer cannot refresh the deadline
/// with padding.
///
/// # The bounds are enforced by construction
///
/// The field is private and [`ChunkData::new`] is the only constructor, so an invalid
/// frame cannot be built — and `#[serde(try_from)]` routes decoding through the same
/// check, so it cannot be *decoded* either. A receive loop therefore holds a valid frame
/// by having one at all, and a serve path cannot emit an empty frame even by accident.
///
/// The invariant lives on the type, not in a convention, because a convention here is
/// silently skippable. Were the floor an advisory `validate()`, it would ride on every
/// receive loop remembering to call it and every emitter avoiding an empty frame — and
/// the serve side avoids one only *incidentally*, differently on each path:
///
/// - The **buffered** path chunks its payload with `slice::chunks`, which yields no
///   items for an empty slice. So even the empty blob (whose bao encoding is zero
///   bytes — see `decdn_bao_range::align_range`) goes straight to
///   [`ClientMessage::StreamEnd`] rather than sending an empty frame first (#1054).
/// - The **window-paced** path (#856) forwards upstream frames verbatim and does no
///   re-chunking, so it inherits the guarantee rather than establishing it.
///
/// Both facts are true, both are about unrelated code, and either could change without
/// anyone noticing which invariant they had just removed — while the reputation system
/// depends on it, since a false `PullStalled` scores an honest peer as unreachable. That
/// is too much weight for a convention, so the type carries the floor instead: the field
/// is private and both doors reject an empty payload, so it cannot be skipped.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "ChunkDataWire")]
pub struct ChunkData {
    /// Sequential blob bytes (1..=[`CHUNK_SIZE`]). Private: see the type's docs — this
    /// is the invariant the inactivity deadline and, through it, `PullStalled` rest on.
    bytes: Vec<u8>,
}

/// Decode shape for [`ChunkData`], which cannot deserialize directly without giving up
/// its private field and with it the invariant. Structurally identical, so the wire
/// format is unchanged; it exists only to be validated on the way in.
#[derive(Deserialize)]
struct ChunkDataWire {
    bytes: Vec<u8>,
}

impl TryFrom<ChunkDataWire> for ChunkData {
    type Error = MessageValidationError;

    fn try_from(wire: ChunkDataWire) -> Result<Self, Self::Error> {
        Self::new(wire.bytes)
    }
}

impl ChunkData {
    /// The only constructor. Enforces the payload bounds: non-empty and within
    /// [`CHUNK_SIZE`].
    ///
    /// # Errors
    ///
    /// [`MessageValidationError::EmptyChunk`] for a zero-length payload;
    /// [`MessageValidationError::ChunkTooLarge`] above the ceiling.
    pub fn new(bytes: Vec<u8>) -> Result<Self, MessageValidationError> {
        if bytes.is_empty() {
            return Err(MessageValidationError::EmptyChunk);
        }
        if bytes.len() > CHUNK_SIZE {
            return Err(MessageValidationError::ChunkTooLarge { len: bytes.len() });
        }
        Ok(Self { bytes })
    }

    /// The payload. Non-empty and within [`CHUNK_SIZE`] by construction.
    #[must_use]
    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    /// Consume the frame for its payload, avoiding a copy on the receive path.
    #[must_use]
    pub fn into_bytes(self) -> Vec<u8> {
        self.bytes
    }

    /// Re-check the payload bounds.
    ///
    /// Always `Ok` for a frame that exists — [`Self::new`] and the `try_from` decode gate
    /// are the only ways to obtain one. Retained because [`ClientMessage::validate`]
    /// dispatches to it, so the aggregate validator stays total over the message enum
    /// rather than silently skipping this variant.
    ///
    /// `pub(crate)`, deliberately: it is total by construction, and a public function that
    /// cannot fail is a check the API advertises and does not have (#1145 review). Leaving it
    /// on the committed surface would invite a caller to depend on it and make removing it a
    /// breaking change — for a validator whose only honest answer is `Ok`. The dispatcher is
    /// in this crate, so `pub(crate)` costs nothing and says the truth.
    ///
    /// # Errors
    ///
    /// [`MessageValidationError::EmptyChunk`] / [`MessageValidationError::ChunkTooLarge`],
    /// neither of which a constructed frame can produce.
    pub(crate) const fn validate(&self) -> Result<(), MessageValidationError> {
        if self.bytes.is_empty() {
            return Err(MessageValidationError::EmptyChunk);
        }
        if self.bytes.len() > CHUNK_SIZE {
            return Err(MessageValidationError::ChunkTooLarge {
                len: self.bytes.len(),
            });
        }
        Ok(())
    }
}

/// Payer → node cumulative payment voucher (ADR 005 §Voucher wire format).
///
/// The wire carries `{signature, amount, bytes_delivered, chain_root,
/// chunk_price}`; the receiver reconstructs the full EIP-712 typed data
/// `{poolId, signer, provider, amount, bytesDelivered, chainRoot, chunkPrice}`
/// from stream context (`poolId`/`signer`/`provider` fixed for the stream) plus
/// the self-described fields. There is no nonce: `amount` is the sole ordering
/// and replay key — a voucher whose `amount` is no greater than the highest
/// accepted is stale. `bytes_delivered` is an additional signed, monotone
/// cumulative that must not regress below the highest accepted; it is what the
/// node verifies against (rather than reconstructing) so same-lane vouchers
/// settle independent of arrival order. Both are `u64` cumulative totals
/// matching the contract's on-chain `uint64` `Lane`/`LaneVoucher` storage; the
/// node zero-extends them to `uint256` to reconstruct the EIP-712 signature.
///
/// `amount` is the **settlement anchor**, and `chain_root` heads the optional
/// hash chain that advances it between signatures (ADR 003 §Hash-chain metering
/// (`PayWord`)). Redemption resolves both with one formula:
/// `claimed = amount + chain_index × chunk_price`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Voucher {
    /// EOA secp256k1 EIP-712 signature (`r‖s‖v`, exactly [`VOUCHER_SIG_LEN`]).
    pub signature: Vec<u8>,
    /// Cumulative payment in token base units.
    pub amount: u64,
    /// Cumulative bytes delivered. Signed by the client and transmitted so
    /// the node verifies against exactly what was signed, independent of
    /// same-lane stream ordering.
    pub bytes_delivered: u64,
    /// Head of the hash chain this voucher opens: `keccak^MAX_CHAIN_LENGTH` of
    /// the payer's per-lane seed. All-zero **seals** the voucher at exactly
    /// `amount` — no value that hashes to zero is findable, so no index above 0
    /// can redeem against it (ADR 003 §The sealed voucher (zero-hash root)).
    pub chain_root: [u8; 32],
    /// The price one chunk of delivery adds over `amount`, in token base units.
    /// Signed so the claim arithmetic is fixed at signing time. A metering
    /// voucher MUST carry the node's quoted `rate_per_mb` (`CHUNK_BYTES ==
    /// MB_BYTES`, so a chunk costs exactly one MB); a sealed voucher meters no
    /// chunk and MUST carry `0`. The node rejects otherwise with
    /// [`VoucherRejectReason::ChunkPriceMismatch`] (ADR 003 §Chunk Cadence).
    pub chunk_price: u64,
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

/// Payer → node released hash-chain preimage, advancing the lane's claim by one
/// chunk with no signature (ADR 003 §Hash-chain metering (`PayWord`), ADR 005
/// §Payment quantum and credit window).
///
/// Released after the payer has received and verified the chunk it pays for, so
/// the payer's exposure stays at zero. Preimage resistance makes the value
/// self-proving: nobody derives `keccak^(N−k−1)(s)` from `keccak^(N−k)(s)`
/// without the seed, so a deeper preimage **is** the receipt for every chunk
/// below it.
///
/// # Wire encoding
///
/// `preimage ‖ index`, 33 bytes flat — postcard writes a `[u8; 32]` raw and a
/// `u8` as one byte, so there is no length prefix and no varint. The byte IS
/// the index, with no offset on send and no increment on receipt.
///
/// # Index domain
///
/// Releasable indices are `1..=`[`MAX_CHAIN_LENGTH`]. `index == 0` is a
/// protocol error — index 0 names `chain_root` and is the settlement case at
/// redemption, so it proves nothing the voucher does not already say. It is
/// rejected **in band** by the delivery handler as
/// [`VoucherRejectReason::ChainIndexZero`] rather than at decode, so the
/// payer learns why instead of seeing an opaque stream close. An index above
/// [`MAX_CHAIN_LENGTH`] cannot be encoded at all: the walk is bounded at 255
/// hashes by the type, which is stronger than a runtime comparison.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChunkPreimage {
    /// The released value, `keccak^(MAX_CHAIN_LENGTH − index)` of the payer's
    /// per-lane seed.
    pub preimage: [u8; 32],
    /// Depth of `preimage` in the chain. Verified as
    /// `keccak^(index − verified)(preimage) == tip` against the receiving
    /// stream's anchor.
    pub index: u8,
}

impl ChunkPreimage {
    /// Always `Ok`. Both fields are fixed-width, so a decoded `ChunkPreimage`
    /// has no shape to check — and the one value invariant (`index != 0`) is
    /// deliberately **not** enforced here: it must surface as an in-band
    /// [`VoucherRejectReason::ChainIndexZero`] from the delivery handler,
    /// which holds the stream it has to answer on. Validating it here would
    /// collapse that reason into a decode failure and close the stream mute.
    ///
    /// Present so [`ClientMessage::validate`] stays total over the enum, for
    /// the same reason `ChunkData::validate` is.
    ///
    /// # Errors
    ///
    /// Never.
    pub const fn validate(&self) -> Result<(), MessageValidationError> {
        Ok(())
    }
}

/// The node's true watermark for a pool capability, echoed back on a gated
/// [`StreamError::VoucherRejected`] so a wallet-less client can self-heal
/// (issue #1481). `amount`/`bytes_delivered` mirror the seller-side
/// `PoolState::last_*` fields as `u64` cumulative totals, for the same reason
/// as [`Voucher`] — matching the contract's on-chain `uint64` storage.
/// `last_signature` is the node's stored last-accepted **client** signature
/// (`r‖s‖v`, exactly [`VOUCHER_SIG_LEN`]) — not a node signature over this
/// bundle — so the client can confirm which of its own vouchers the node
/// holds.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WatermarkBundle {
    /// Cumulative amount of the node's last-accepted voucher.
    pub amount: u64,
    /// Cumulative bytes delivered as of the node's last-accepted voucher.
    pub bytes_delivered: u64,
    /// Root the lane is currently metering against; all-zero when the lane
    /// holds no live chain.
    pub chain_root: [u8; 32],
    /// Deepest chain index the node has verified under `chain_root`.
    pub verified_index: u8,
    /// The preimage bytes at `verified_index`; all-zero when the node has
    /// verified none. Echoed so a re-seeding signer folds
    /// `verified_index × chunk_price` back into the amount it re-signs rather
    /// than dropping the frontier (ADR 005 §Watermark bundle).
    pub tip: [u8; 32],
    /// The `chunk_price` the node's last-accepted voucher was signed at.
    pub chunk_price: u64,
    /// The client's own signature (`r‖s‖v`, exactly [`VOUCHER_SIG_LEN`]) on the
    /// node's last-accepted voucher. `Vec<u8>` rather than a fixed array,
    /// mirroring [`Voucher::signature`] — postcard/serde signature fields on
    /// this wire are length-prefixed `Vec<u8>`, not `[u8; N]` (serde's array
    /// impls top out at N=32).
    pub last_signature: Vec<u8>,
}

impl WatermarkBundle {
    /// Validate the wire-level `last_signature` length ([`VOUCHER_SIG_LEN`]) —
    /// the same client-voucher-signature length [`Voucher::validate`] checks,
    /// since this IS a client voucher signature (issue #1481). A caller MUST
    /// call this before trusting a bundle enough to `reseed` a ledger from it:
    /// the bundle rides inside an application-level `StreamError`, not signed
    /// itself, so this is a shape check, not an authentication check — it stops
    /// a malformed/truncated bundle from reaching a fixed-length conversion
    /// downstream, nothing more.
    pub const fn validate(&self) -> Result<(), MessageValidationError> {
        if self.last_signature.len() != VOUCHER_SIG_LEN {
            return Err(MessageValidationError::InvalidVoucherSigLen {
                len: self.last_signature.len(),
            });
        }
        Ok(())
    }
}

/// A stream failure code (ADR 005 §Stream errors). Variant order is frozen.
///
/// Every variant except `VoucherRejected` is delivery-side and rides in
/// [`StreamResponse::error`] alongside `ok: false`; `VoucherRejected` is the
/// only variant delivered mid-stream, inside a [`ClientMessage::StreamError`].
/// All codes are unsigned and informational — never on-chain evidence.
///
/// New variants are appended at the end, never inserted: the postcard
/// discriminant is the declaration index, so `OriginBlacklisted` and
/// `HashBlacklisted` sit after `VoucherRejected` even though they read as
/// delivery-side neighbours of `EvictedSinceProbe`. Moving them would silently
/// renumber `VoucherRejected` on the wire — an ADR 013 Tier-3 break. Use
/// [`StreamError::is_delivery_side`], not variant position, to reason about the
/// domain split.
///
/// The same append-only discipline applies WITHIN a struct-variant's fields:
/// postcard encodes a struct variant's payload positionally, in declaration
/// order, so `VoucherRejected`'s `bundle` field sits after `reason` and any
/// future field must append after `bundle`, never insert before it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum StreamError {
    /// Node lacks the blob and cannot reach a provider, or declines to pull
    /// through (ADR 037 ramped credit window).
    NotFound,
    /// Node is at capacity; try another node.
    Overloaded,
    /// Blob exceeds this node's configured `max_blob_size`; try another node,
    /// but do not retry this one for the same blob.
    BlobTooLarge,
    /// Unexpected failure; do not retry this node.
    InternalError,
    /// Blob was evicted between probe and stream request. WARNING: still
    /// slashable after a signed `has_blob: true` probe (ADR 005).
    ///
    /// An eviction with no blacklist entry behind it — corruption recovery, a
    /// manual `decdn node evict`. A takedown answers [`Self::HashBlacklisted`]
    /// whichever list it came from; routing governance takedowns here instead
    /// would leave `HashBlacklisted` uniquely identifying an operator's *private*
    /// denylist (ADR 011 §`StreamRequest` Response).
    EvictedSinceProbe,
    /// Mid-stream payment-voucher rejection carried in a
    /// [`ClientMessage::StreamError`] message (never in the initial
    /// [`StreamResponse`]).
    VoucherRejected {
        /// The specific validation failure.
        reason: VoucherRejectReason,
        /// The node's true watermark plus the client's own last-accepted
        /// signature, attached ONLY on the regression/exhaustion reasons
        /// (`AmountRegression`, `BytesRegression`, `SpendingCapExhausted`) and ONLY
        /// when the rejected voucher's signature recovers to the
        /// capability's pinned `voucher_signer` (issue #1481 §5 security
        /// property — otherwise anyone who guessed the chain-derivable
        /// `pool_id` could pull the node's watermark). A wallet-less client
        /// cannot reconstruct its watermark from chain (the claim watermark
        /// is `0` until settlement), so this lets it self-heal: re-seed the
        /// ledger's PAYMENT BASELINE to `bytes_delivered` (a pool-cumulative
        /// counter, NOT a blob `byte_offset`) and re-sign from the new
        /// baseline. `None` for every handler-direct reason (`CapabilityExpired`,
        /// `PoolExhausted`) and whenever the signer does not recover to
        /// `voucher_signer`.
        bundle: Option<WatermarkBundle>,
    },
    /// The pool funding this request is owned by a blacklisted origin
    /// operator (ADR 011 §`StreamRequest` Response). Permanent for this pool:
    /// opening a new one under the same address will be refused identically, so
    /// a requester should not retry here or elsewhere with this funder.
    OriginBlacklisted,
    /// The blob is on the governance blacklist or this operator's local
    /// denylist (ADR 011 §`StreamRequest` Response). Deliberately does not
    /// distinguish the two — a local denylist entry is nobody else's business,
    /// and a client that could tell them apart could map an operator's private
    /// legal exposure. Retry on a different node: a local entry binds only this
    /// one, and a governance entry will be refused everywhere.
    HashBlacklisted,
}

impl StreamError {
    /// `true` for the delivery-side codes that ride in [`StreamResponse::error`]
    /// alongside `ok: false` — everything except `VoucherRejected`. Expresses
    /// the enum's domain split in code rather than only in prose, and backs the
    /// [`StreamResponse::validate`] mid-stream-only exclusion.
    pub const fn is_delivery_side(&self) -> bool {
        !self.is_mid_stream()
    }

    /// `true` for the mid-stream-only [`StreamError::VoucherRejected`], which
    /// rides exclusively in [`ClientMessage::StreamError`].
    pub const fn is_mid_stream(&self) -> bool {
        matches!(self, Self::VoucherRejected { .. })
    }
}

/// Why a [`Voucher`] was rejected (ADR 005 §`VoucherRejected` semantics).
///
/// The first seven variants mirror `decdn_incentive::PoolError` ∪
/// `VoucherError` one-to-one; the handler-side conversion `voucher_reject_reason`
/// matches those exhaustively so a new `PoolError` variant fails to compile
/// until this enum is extended (ADR 005 §Mirror obligation). The remaining
/// variants have no validation-enum counterpart and are emitted directly by the
/// `cdn/client/v1` handler: [`Self::CapabilityExpired`] fires when the signer's
/// capability has passed its expiry; [`Self::PoolExhausted`] fires when the
/// pool's remaining deposit can no longer fund further credit; and the four
/// hash-chain reasons ([`Self::BadPreimage`], [`Self::ChainIndexZero`],
/// [`Self::UnanchoredPreimage`], [`Self::ChunkPriceMismatch`]) are raised where
/// the handler holds the per-stream chain anchor a validation enum cannot see.
/// Variant order is frozen — new handler-direct reasons append at the end.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum VoucherRejectReason {
    /// Signature malformed (corrupted bytes, non-canonical `s`, invalid
    /// recovery id). `VoucherError::InvalidSignature`.
    BadSignature,
    /// Signature well-formed but recovers to the wrong signer.
    /// `VoucherError::WrongSigner`.
    WrongSigner,
    /// `voucher.pool_id` mismatch (also: unknown pool). `PoolError::WrongPool`.
    WrongPool,
    /// The voucher names a provider node other than the one receiving it —
    /// a capability voucher scoped to one node redeemed against another.
    /// `PoolError::WrongProvider`.
    WrongProvider,
    /// Cumulative amount regressed. `PoolError::AmountDecreasing`.
    AmountRegression,
    /// Cumulative bytes delivered regressed. `PoolError::BytesDecreasing`.
    BytesRegression,
    /// The signer's remaining spending cap is exhausted — the voucher amount
    /// exceeds what the capability has left to spend (`cap − spent`).
    /// Recovery: the pool **owner** raises this signer's cap or delegates a new
    /// capability. `PoolError::CapExceeded`.
    SpendingCapExhausted,
    /// The signer's capability has passed its `expiry`. The node's serve-side check
    /// compares its LOCAL wall clock (`unix_now`, operator-settable) against `expiry`
    /// and stops serving the lane; on-chain settlement separately gates redemption on
    /// `block.timestamp`. Already-earned vouchers stay redeemable until expiry at
    /// settlement. Recovery: the owner mints a **fresh capability** with a new expiry —
    /// a watermark resync or top-up does not help. Emitted directly by the
    /// `cdn/client/v1` handler (the node holds the clock), not via the `PoolError`
    /// bridge.
    CapabilityExpired,
    /// The pool's on-chain remaining deposit, minus the refundable floor `M` and
    /// the pool's already-committed concurrent floor credit, can no longer fund
    /// further credit for this stream (ADR 003 §Pool solvency). A pool-wide
    /// condition, not this signer's. Recovery: the pool **owner tops up the
    /// deposit**. Emitted mid-stream by the serve loop after the client has proved
    /// capability ownership; the open-time equivalent stays wire-`NotFound`
    /// (anti-enumeration). Not watermark-gated.
    PoolExhausted,
    /// A released [`ChunkPreimage`] does not hash to the stream's deepest
    /// verified preimage in `index − verified` steps (ADR 003 §Concurrent
    /// Streams, Rule 2). A payer bug — a wrong seed, a wrong chain, or a
    /// mis-derived index. On-chain the same mismatch reverts `BadPreimage`,
    /// which is caller error and not transient state, so this is terminal and
    /// carries **no** watermark bundle: a preimage has no signature of its own,
    /// and a payment watermark cannot repair a hash-chain mismatch.
    BadPreimage,
    /// A [`ChunkPreimage`] arrived with `index == 0`, which never travels the
    /// wire — index 0 names `chain_root` and is the settlement case at
    /// redemption. A payer bug; do not retry.
    ///
    /// Zero is the whole of it. There is no over-large index to report: the wire
    /// index is a `u8` and [`MAX_CHAIN_LENGTH`] is 255, so an index past the end
    /// of the chain cannot be encoded. A chain that has run out of indices is
    /// not this reason either — the payer rolls to a fresh `chain_root` first.
    ChainIndexZero,
    /// A [`ChunkPreimage`] arrived on a stream that holds no chain anchor, so
    /// the node cannot name the chain the reveal belongs to (ADR 003
    /// §Concurrent Streams, Rule 1). Per-stream and therefore decidable, which
    /// a lane-wide reading would not be. **Not fatal**: the payer sends the
    /// current epoch's `chain_root` voucher on this stream and resends the
    /// preimage. The resend is free — an at-or-below-watermark voucher is
    /// already-satisfied rather than rejected.
    UnanchoredPreimage,
    /// The voucher's `chunk_price` is not the node's quoted `rate_per_mb` on a
    /// metering voucher, or is non-zero on a sealed one (`chain_root == 0`).
    ///
    /// `chunk_price` is signed by the *payer* and a preimage carries no price
    /// of its own, so a voucher signed at the governance floor against a node
    /// quoting ten times that would meter every later chunk at a tenth of the
    /// quote, with no per-tick moment revealing it. The node therefore checks
    /// the price it is being paid before it meters against it (ADR 003 §Chunk
    /// Cadence). A payer bug: re-read `rate_per_mb` from the `StreamResponse`
    /// and re-sign; do not retry with the same price.
    ChunkPriceMismatch,
}

impl VoucherRejectReason {
    /// Whether a [`StreamError::VoucherRejected`] carrying this reason is
    /// eligible for a [`WatermarkBundle`] (issue #1481 §5): exactly the three
    /// regression/exhaustion reasons a wallet-less client cannot distinguish
    /// from chain, since its local watermark is the only thing that could be
    /// wrong. Every handler-direct reason (`CapabilityExpired` / `PoolExhausted`,
    /// plus the signer/pool/provider mismatches) is never eligible — a bundle
    /// would not help there, since the fix is not "resync the watermark".
    ///
    /// Single source of truth for the gate: the node checks this before
    /// attaching a bundle (`crates/node/src/handlers/client/voucher.rs`) and
    /// the client checks it again before trusting one enough to self-heal
    /// (`crates/client-pull/src/lib.rs`) — both call this rather than each
    /// keeping their own copy of the three-way match.
    #[must_use]
    pub const fn is_watermark_gated(self) -> bool {
        matches!(
            self,
            Self::SpendingCapExhausted | Self::AmountRegression | Self::BytesRegression
        )
    }
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
            pool_id: [9u8; 32],
            timestamp_us: 1_700_000_000_000_000,
            redirect: None,
        }
    }

    fn sample_response() -> StreamResponse {
        StreamResponse {
            body: sample_body(),
            error: None,
            slash_sig: vec![0xABu8; SLASH_SIG_LEN],
        }
    }

    fn sample_request() -> StreamRequest {
        StreamRequest {
            hash: [1u8; 32],
            namespace_id: [3u8; 32],
            pool_id: [2u8; 32],
            byte_offset: 0,
            byte_len: 0,
            timestamp_us: 0xdead_beef,
        }
    }

    fn sample_binding() -> ClientBinding {
        ClientBinding {
            ethereum_address: [0xEEu8; 20],
            binding_signature: vec![0x01u8; BINDING_SIG_LEN],
        }
    }

    fn sample_ext() -> StreamRequestExt {
        StreamRequestExt {
            binding: Some(sample_binding()),
            capability: Some(sample_capability()),
        }
    }

    fn sample_capability() -> WireCapability {
        WireCapability {
            spending_cap: [0x22u8; 32],
            expiry: 1_800_000_000,
            owner_signature: vec![0x03u8; VOUCHER_SIG_LEN],
        }
    }

    fn sample_voucher() -> Voucher {
        Voucher {
            signature: vec![0xCDu8; VOUCHER_SIG_LEN],
            amount: 0x1111_1111_1111_1111u64,
            bytes_delivered: 0x2222_2222_2222_2222u64,
            chain_root: [0x55u8; 32],
            chunk_price: 0x6666_6666_6666_6666u64,
        }
    }

    fn sample_preimage() -> ChunkPreimage {
        ChunkPreimage {
            preimage: [0x77u8; 32],
            index: 42,
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

    /// `capability` round-trips both present and absent, independent of
    /// `binding` (the wire fields are orthogonal — a registered on-chain
    /// client can still carry a capability, and vice versa).
    #[test]
    fn stream_request_ext_capability_roundtrip() -> Result<(), postcard::Error> {
        let with_cap = StreamRequestExt {
            binding: None,
            capability: Some(sample_capability()),
        };
        let bytes = postcard::to_allocvec(&with_cap)?;
        let decoded: StreamRequestExt = postcard::from_bytes(&bytes)?;
        assert_eq!(with_cap, decoded);

        let without_cap = StreamRequestExt {
            capability: None,
            ..with_cap
        };
        let bytes = postcard::to_allocvec(&without_cap)?;
        let decoded: StreamRequestExt = postcard::from_bytes(&bytes)?;
        assert_eq!(without_cap, decoded);
        Ok(())
    }

    #[test]
    fn wire_capability_rejects_empty_signature() {
        let cap = WireCapability {
            spending_cap: [0u8; 32],
            expiry: 0,
            owner_signature: Vec::new(),
        };
        assert_eq!(
            cap.validate(),
            Err(MessageValidationError::EmptyCapabilitySignature)
        );
    }

    #[test]
    fn stream_request_ext_validate_rejects_empty_capability_signature() {
        let ext = StreamRequestExt {
            binding: None,
            capability: Some(WireCapability {
                spending_cap: [0u8; 32],
                expiry: 0,
                owner_signature: Vec::new(),
            }),
        };
        assert_eq!(
            ext.validate(),
            Err(MessageValidationError::EmptyCapabilitySignature)
        );
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
    fn chunk_data_roundtrip() -> Result<(), Box<dyn std::error::Error>> {
        let chunk = ChunkData::new(vec![0x42u8; CHUNK_SIZE])?;
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
    fn chunk_preimage_roundtrip() -> Result<(), postcard::Error> {
        let p = sample_preimage();
        let bytes = postcard::to_allocvec(&p)?;
        let decoded: ChunkPreimage = postcard::from_bytes(&bytes)?;
        assert_eq!(p, decoded);
        Ok(())
    }

    /// ADR 005 §Payment quantum: `preimage ‖ index`, **33 bytes flat** — no
    /// length prefix and no varint. The byte IS the index. This is the property
    /// that lets the wire index and the on-chain packed `chainMeter` low byte
    /// agree with no offset on send and no increment on receipt, so pin the
    /// exact bytes rather than only the round-trip.
    #[test]
    fn chunk_preimage_wire_format_is_stable() -> Result<(), postcard::Error> {
        let p = ChunkPreimage {
            preimage: [0xA5u8; 32],
            index: MAX_CHAIN_LENGTH,
        };
        let bytes = postcard::to_allocvec(&p)?;
        let mut expected = Vec::new();
        expected.extend_from_slice(&[0xA5u8; 32]); // preimage (raw, no prefix)
        expected.push(255u8); // index (one byte, as itself)
        assert_eq!(bytes, expected);
        assert_eq!(bytes.len(), 33);
        Ok(())
    }

    /// `index == 0` decodes cleanly and passes `validate()`. The rejection is
    /// the delivery handler's, in band as `ChainIndexZero` — if this ever
    /// starts failing at decode, the payer loses the reason and sees only a
    /// closed stream.
    #[test]
    fn chunk_preimage_index_zero_decodes_and_validates() -> Result<(), postcard::Error> {
        let p = ChunkPreimage {
            preimage: [0u8; 32],
            index: 0,
        };
        let bytes = postcard::to_allocvec(&ClientMessage::ChunkPreimage(p))?;
        let decoded: ClientMessage = postcard::from_bytes(&bytes)?;
        assert_eq!(decoded.validate(), Ok(()));
        Ok(())
    }

    #[test]
    fn stream_error_voucher_rejected_roundtrip() -> Result<(), postcard::Error> {
        let e = StreamError::VoucherRejected {
            reason: VoucherRejectReason::SpendingCapExhausted,
            bundle: None,
        };
        let bytes = postcard::to_allocvec(&e)?;
        let decoded: StreamError = postcard::from_bytes(&bytes)?;
        assert_eq!(e, decoded);
        Ok(())
    }

    /// The gated case: a `WatermarkBundle` rides alongside the reject reason
    /// (issue #1481). Round-trips `Some` distinctly from the `None` case above
    /// — this is the shape a wallet-less client actually receives on a gated
    /// regression/exhaustion reject.
    #[test]
    fn stream_error_voucher_rejected_with_bundle_roundtrip() -> Result<(), postcard::Error> {
        let e = StreamError::VoucherRejected {
            reason: VoucherRejectReason::SpendingCapExhausted,
            bundle: Some(WatermarkBundle {
                amount: 0x1111_1111_1111_1111u64,
                bytes_delivered: 0x3333_3333_3333_3333u64,
                chain_root: [0x55u8; 32],
                verified_index: 7,
                tip: [0x66u8; 32],
                chunk_price: 0x7777_7777_7777_7777u64,
                last_signature: vec![0x44u8; VOUCHER_SIG_LEN],
            }),
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
    fn client_message_discriminants_are_frozen() -> Result<(), Box<dyn std::error::Error>> {
        assert_eq!(
            first_byte(&ClientMessage::StreamRequest(sample_request()))?,
            0
        );
        assert_eq!(
            first_byte(&ClientMessage::StreamResponse(sample_response()))?,
            1
        );
        assert_eq!(
            first_byte(&ClientMessage::ChunkData(ChunkData::new(vec![0x1u8])?))?,
            2
        );
        assert_eq!(first_byte(&ClientMessage::Voucher(sample_voucher()))?, 3);
        assert_eq!(
            first_byte(&ClientMessage::ChunkPreimage(sample_preimage()))?,
            4
        );
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
                bundle: None,
            },
            StreamError::OriginBlacklisted,
            StreamError::HashBlacklisted,
        ]
        .into_iter()
        .enumerate()
        {
            // Pin the discriminant AND decode-roundtrip each variant's payload —
            // a first-byte-only check would miss a payload-shape regression.
            let bytes = postcard::to_allocvec(&e)?;
            assert_eq!(
                bytes.first().copied(),
                Some(u8::try_from(i).unwrap_or(0xFF))
            );
            let decoded: StreamError = postcard::from_bytes(&bytes)?;
            assert_eq!(decoded, e);
        }
        Ok(())
    }

    #[test]
    fn voucher_reject_reason_variant_order_is_frozen() -> Result<(), postcard::Error> {
        for (i, r) in [
            VoucherRejectReason::BadSignature,
            VoucherRejectReason::WrongSigner,
            VoucherRejectReason::WrongPool,
            VoucherRejectReason::WrongProvider,
            VoucherRejectReason::AmountRegression,
            VoucherRejectReason::BytesRegression,
            VoucherRejectReason::SpendingCapExhausted,
            VoucherRejectReason::CapabilityExpired,
            VoucherRejectReason::PoolExhausted,
            VoucherRejectReason::BadPreimage,
            VoucherRejectReason::ChainIndexZero,
            VoucherRejectReason::UnanchoredPreimage,
            VoucherRejectReason::ChunkPriceMismatch,
        ]
        .into_iter()
        .enumerate()
        {
            let bytes = postcard::to_allocvec(&r)?;
            assert_eq!(
                bytes.first().copied(),
                Some(u8::try_from(i).unwrap_or(0xFF))
            );
            let decoded: VoucherRejectReason = postcard::from_bytes(&bytes)?;
            assert_eq!(decoded, r);
        }
        Ok(())
    }

    #[test]
    fn client_message_rejects_unknown_discriminant() {
        let bytes = [99u8, 0, 0, 0, 0];
        let r: Result<ClientMessage, _> = postcard::from_bytes(&bytes);
        assert!(r.is_err());
    }

    // Pins `TopLevelEnum::VARIANT_COUNT` to the highest discriminant so a future
    // variant addition must update the count the ADR 013 unknown/known
    // classifier relies on.
    #[test]
    fn client_message_variant_count_matches_discriminants() -> Result<(), postcard::Error> {
        use crate::framing::TopLevelEnum;
        assert_eq!(ClientMessage::VARIANT_COUNT, 7);
        // The last declared variant (`StreamError`) must encode to discriminant
        // VARIANT_COUNT - 1. Compare against postcard's own varint encoding of
        // that index (not `first_byte`/`bytes.first()`) so the pin survives a
        // future multi-byte discriminant (> 127 variants).
        let last = ClientMessage::StreamError(StreamError::NotFound);
        let bytes = postcard::to_allocvec(&last)?;
        let expected_disc = postcard::to_allocvec(&(ClientMessage::VARIANT_COUNT - 1))?;
        assert!(bytes.starts_with(&expected_disc));
        Ok(())
    }

    #[test]
    fn client_message_unknown_discriminant_is_flagged_unsupported() {
        // Discriminant 7 is the first index past the known set → UNSUPPORTED.
        assert!(crate::is_unknown_variant::<ClientMessage>(&[7u8, 0, 0]));
        // A known in-range discriminant (1 = StreamResponse) with a bad payload
        // stays MALFORMED.
        assert!(!crate::is_unknown_variant::<ClientMessage>(&[1u8, 0xFF]));
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
                pool_id: [6u8; 32],
                timestamp_us: 7,
                redirect: None,
            },
            error: None,
            slash_sig: vec![0xABu8; SLASH_SIG_LEN],
        };
        let bytes = postcard::to_allocvec(&resp)?;
        let mut expected = Vec::new();
        expected.extend_from_slice(&[3u8; 32]); // body.hash
        expected.push(1u8); // body.ok = true
        expected.push(4u8); // body.rate_per_mb varint
        expected.push(5u8); // body.total_bytes varint
        expected.extend_from_slice(&[6u8; 32]); // body.pool_id
        expected.push(7u8); // body.timestamp_us varint
        expected.push(0u8); // body.redirect = None
        expected.push(0u8); // error = None
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
            amount: 1u64,
            bytes_delivered: 2u64,
            chain_root: [0xABu8; 32],
            chunk_price: 3u64,
        };
        let bytes = postcard::to_allocvec(&v)?;
        let mut expected = Vec::new();
        expected.push(VOUCHER_SIG_LEN as u8); // signature length prefix (65)
        expected.extend_from_slice(&[0xCDu8; VOUCHER_SIG_LEN]); // signature
        expected.push(1u8); // amount (varint)
        expected.push(2u8); // bytes_delivered (varint)
        expected.extend_from_slice(&[0xABu8; 32]); // chain_root (raw, no prefix)
        expected.push(3u8); // chunk_price (varint)
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

    /// A malformed/truncated bundle must fail its own shape check before any
    /// caller trusts it enough to reseed a ledger (issue #1481 review).
    #[test]
    fn watermark_bundle_validate_rejects_wrong_len_signature() {
        let b = WatermarkBundle {
            amount: 0u64,
            bytes_delivered: 0u64,
            chain_root: [0u8; 32],
            verified_index: 0,
            tip: [0u8; 32],
            chunk_price: 0u64,
            last_signature: vec![0xCDu8; VOUCHER_SIG_LEN - 1],
        };
        assert_eq!(
            b.validate(),
            Err(MessageValidationError::InvalidVoucherSigLen {
                len: VOUCHER_SIG_LEN - 1
            })
        );
        let ok = WatermarkBundle {
            last_signature: vec![0xCDu8; VOUCHER_SIG_LEN],
            ..b
        };
        assert_eq!(ok.validate(), Ok(()));
    }

    // --- Edge-case roundtrips ------------------------------------------------

    #[test]
    fn stream_request_nonzero_byte_offset_roundtrip() -> Result<(), postcard::Error> {
        // byte_offset != 0 exercises the multi-byte varint path (resume/seek);
        // the shared sample uses 0, a single byte.
        let req = StreamRequest {
            byte_offset: 1_048_576,
            ..sample_request()
        };
        let bytes = postcard::to_allocvec(&req)?;
        let decoded: StreamRequest = postcard::from_bytes(&bytes)?;
        assert_eq!(req, decoded);
        assert_eq!(decoded.byte_offset, 1_048_576);
        Ok(())
    }

    #[test]
    fn stream_request_bounded_byte_len_roundtrip() -> Result<(), postcard::Error> {
        // A bounded range [byte_offset, byte_offset + byte_len) (ADR 005 §Bounded
        // byte ranges) — byte_len != 0 distinguishes a scoped origin range pull
        // from the whole-tail default (byte_len == 0).
        let req = StreamRequest {
            byte_offset: 1_048_576,
            byte_len: 262_144,
            ..sample_request()
        };
        let bytes = postcard::to_allocvec(&req)?;
        let decoded: StreamRequest = postcard::from_bytes(&bytes)?;
        assert_eq!(req, decoded);
        assert_eq!(decoded.byte_len, 262_144);
        Ok(())
    }

    #[test]
    fn an_empty_chunk_cannot_be_decoded_at_all() -> Result<(), Box<dyn std::error::Error>> {
        // An empty chunk is not a legal frame (#1088), and since the #1145 review it is
        // not a REPRESENTABLE one: rejecting it is the decoder's job, not a receive
        // loop's obligation to remember.
        //
        // Decode-time rejection is what closes the hole. If an empty frame decoded cleanly
        // and only a later `validate()` call refused it, a receive loop that forgot to call
        // `validate()` would spin on empty frames forever — each empty frame advances neither
        // the cumulative byte count nor the stall deadline, so nothing ever breaks the loop.
        // Making the decoder refuse the frame lifts that standing obligation off every loop.
        //
        // So: forge the bytes an adversary would send (a length-prefix of 0, which no
        // constructor will produce) and require the decoder to refuse them.
        // postcard encodes `Vec<u8>` as a varint length then the bytes, so `[0x00]` is a
        // length of 0 and nothing else.
        assert!(
            postcard::from_bytes::<ChunkData>(&[0x00]).is_err(),
            "an empty ChunkData must not decode — the receive loops' stall detection, and \
             through it the reputation system, rest on every frame carrying bytes"
        );
        // And the same through the message enum, which is what the wire actually carries.
        let mut msg = encode_message(&ClientMessage::ChunkData(ChunkData::new(vec![0x00])?))?;
        assert_eq!(msg.pop(), Some(0x00), "payload byte");
        assert_eq!(msg.pop(), Some(0x01), "length prefix of 1");
        msg.push(0x00); // rewrite the length to 0
        assert!(
            decode_message::<ClientMessage>(&msg).is_err(),
            "an empty ChunkData must not decode inside a ClientMessage either"
        );
        Ok(())
    }

    #[test]
    fn chunk_data_new_is_the_only_way_in_and_it_enforces_both_bounds() {
        // The floor is what makes every frame a unit of progress (#1088); the ceiling
        // bounds per-frame allocation. A 1-byte and a full-size chunk are both legal —
        // "partial final chunk" means smaller, not empty.
        assert_eq!(
            ChunkData::new(Vec::new()),
            Err(MessageValidationError::EmptyChunk)
        );
        assert_eq!(
            ChunkData::new(vec![0u8; CHUNK_SIZE + 1]),
            Err(MessageValidationError::ChunkTooLarge {
                len: CHUNK_SIZE + 1
            })
        );
        assert!(ChunkData::new(vec![0u8]).is_ok());
        assert!(ChunkData::new(vec![0u8; CHUNK_SIZE]).is_ok());
    }

    #[test]
    fn chunk_data_validate_agrees_with_the_constructor() -> Result<(), MessageValidationError> {
        // `validate` survives only as the arm `ClientMessage::validate` dispatches to, so
        // the aggregate validator stays total over the enum. It cannot FAIL — a
        // `ChunkData` that exists came through `new` or the decode gate — and that is the
        // assertion worth making: if this ever returns `Err`, some construction path has
        // gone around the constructor.
        assert_eq!(ChunkData::new(vec![0u8])?.validate(), Ok(()));
        assert_eq!(ChunkData::new(vec![0u8; CHUNK_SIZE])?.validate(), Ok(()));
        Ok(())
    }

    #[test]
    fn client_binding_roundtrip() -> Result<(), postcard::Error> {
        let b = sample_binding();
        let bytes = postcard::to_allocvec(&b)?;
        let decoded: ClientBinding = postcard::from_bytes(&bytes)?;
        assert_eq!(b, decoded);
        Ok(())
    }

    /// A non-empty-but-malformed remainder MUST error, not silently degrade to
    /// `default()`. `take_from_bytes` rejects a truncated encoding on EOF. This
    /// pins the contract a future `unwrap_or_default()` refactor would break.
    #[test]
    fn parse_stream_request_ext_rejects_malformed_remainder() {
        assert!(parse_stream_request_ext(&[0x01]).is_err());
    }

    // --- Extension / binding validation --------------------------------------

    #[test]
    fn stream_request_ext_validate_accepts_sample_and_default() {
        assert_eq!(sample_ext().validate(), Ok(()));
        assert_eq!(StreamRequestExt::default().validate(), Ok(()));
    }

    #[test]
    fn stream_request_ext_validate_rejects_wrong_len_binding_sig() {
        let ext = StreamRequestExt {
            binding: Some(ClientBinding {
                ethereum_address: [0u8; 20],
                binding_signature: vec![0x01; BINDING_SIG_LEN - 1],
            }),
            capability: None,
        };
        assert_eq!(
            ext.validate(),
            Err(MessageValidationError::InvalidBindingSigLen {
                len: BINDING_SIG_LEN - 1
            })
        );
    }

    #[test]
    fn client_binding_validate_rejects_wrong_len() {
        let b = ClientBinding {
            ethereum_address: [0u8; 20],
            binding_signature: Vec::new(),
        };
        assert_eq!(
            b.validate(),
            Err(MessageValidationError::InvalidBindingSigLen { len: 0 })
        );
        assert_eq!(sample_binding().validate(), Ok(()));
    }

    // --- StreamResponse ok/error consistency ---------------------------------

    #[test]
    fn stream_response_validate_rejects_error_with_ok() {
        // sample_response has body.ok = true; an error must not accompany it.
        let resp = StreamResponse {
            error: Some(StreamError::NotFound),
            ..sample_response()
        };
        assert_eq!(
            resp.validate(),
            Err(MessageValidationError::StreamErrorWithOk)
        );
    }

    #[test]
    fn stream_response_validate_rejects_failure_without_error() {
        let resp = StreamResponse {
            body: StreamResponseBody {
                ok: false,
                ..sample_body()
            },
            error: None,
            ..sample_response()
        };
        assert_eq!(
            resp.validate(),
            Err(MessageValidationError::MissingStreamError)
        );
    }

    #[test]
    fn stream_response_validate_accepts_failure_with_delivery_error() {
        let resp = StreamResponse {
            body: StreamResponseBody {
                ok: false,
                ..sample_body()
            },
            error: Some(StreamError::Overloaded),
            ..sample_response()
        };
        assert_eq!(resp.validate(), Ok(()));
    }

    #[test]
    fn stream_response_validate_rejects_voucher_rejected_in_error() {
        // VoucherRejected is mid-stream-only; it must never ride in the response.
        let resp = StreamResponse {
            body: StreamResponseBody {
                ok: false,
                ..sample_body()
            },
            error: Some(StreamError::VoucherRejected {
                reason: VoucherRejectReason::SpendingCapExhausted,
                bundle: None,
            }),
            ..sample_response()
        };
        assert_eq!(
            resp.validate(),
            Err(MessageValidationError::VoucherRejectedInResponse)
        );
    }

    // --- ClientMessage dispatcher + StreamError domain split -----------------

    #[test]
    fn client_message_validate_dispatches_to_payload() -> Result<(), MessageValidationError> {
        // Valid payloads pass through.
        assert_eq!(
            ClientMessage::StreamResponse(sample_response()).validate(),
            Ok(())
        );
        assert_eq!(ClientMessage::Voucher(sample_voucher()).validate(), Ok(()));
        // Invalid payloads propagate their own error through the dispatcher.
        let bad_resp = StreamResponse {
            slash_sig: Vec::new(),
            ..sample_response()
        };
        assert_eq!(
            ClientMessage::StreamResponse(bad_resp).validate(),
            Err(MessageValidationError::InvalidSlashSigLen { len: 0 })
        );
        let bad_voucher = Voucher {
            signature: Vec::new(),
            ..sample_voucher()
        };
        assert_eq!(
            ClientMessage::Voucher(bad_voucher).validate(),
            Err(MessageValidationError::InvalidVoucherSigLen { len: 0 })
        );
        // The aggregate seam dispatches to `ChunkData::validate` rather than waving the
        // variant through (#1088). It cannot catch an empty frame — none can be
        // built to hand it — but the arm must stay, so the validator remains total over
        // the enum and a future payload invariant on this variant is not silently skipped.
        assert_eq!(
            ClientMessage::ChunkData(ChunkData::new(vec![7])?).validate(),
            Ok(())
        );
        // Variants carrying no value invariants are unconditionally Ok.
        assert_eq!(
            ClientMessage::StreamRequest(sample_request()).validate(),
            Ok(())
        );
        assert_eq!(ClientMessage::StreamEnd.validate(), Ok(()));
        assert_eq!(
            ClientMessage::StreamError(StreamError::NotFound).validate(),
            Ok(())
        );
        Ok(())
    }

    #[test]
    fn stream_error_domain_split_matches_variants() {
        for e in [
            StreamError::NotFound,
            StreamError::Overloaded,
            StreamError::BlobTooLarge,
            StreamError::InternalError,
            StreamError::EvictedSinceProbe,
            // Appended after `VoucherRejected` for wire-order reasons, but
            // delivery-side all the same — this is the assertion that keeps the
            // "everything except VoucherRejected" rule honest as the enum grows.
            StreamError::OriginBlacklisted,
            StreamError::HashBlacklisted,
        ] {
            assert!(e.is_delivery_side(), "{e:?} is delivery-side");
            assert!(!e.is_mid_stream(), "{e:?} is not mid-stream");
        }
        let v = StreamError::VoucherRejected {
            reason: VoucherRejectReason::WrongSigner,
            bundle: None,
        };
        assert!(v.is_mid_stream());
        assert!(!v.is_delivery_side());
    }

    // --- Full framing stack --------------------------------------------------

    #[tokio::test]
    async fn client_message_full_stack_roundtrip() -> Result<(), Box<dyn std::error::Error>> {
        let messages = [
            ClientMessage::StreamRequest(sample_request()),
            ClientMessage::StreamResponse(sample_response()),
            ClientMessage::ChunkData(ChunkData::new(vec![0x7u8; 1000])?),
            ClientMessage::Voucher(sample_voucher()),
            ClientMessage::ChunkPreimage(sample_preimage()),
            ClientMessage::StreamEnd,
            ClientMessage::StreamError(StreamError::VoucherRejected {
                reason: VoucherRejectReason::SpendingCapExhausted,
                bundle: Some(WatermarkBundle {
                    amount: 0x0101_0101_0101_0101u64,
                    bytes_delivered: 0x0303_0303_0303_0303u64,
                    chain_root: [0x05u8; 32],
                    verified_index: 9,
                    tip: [0x06u8; 32],
                    chunk_price: 0x0707_0707_0707_0707u64,
                    last_signature: vec![0x04u8; VOUCHER_SIG_LEN],
                }),
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
