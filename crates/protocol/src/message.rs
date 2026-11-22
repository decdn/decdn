//! Items every signed wire message shares: the rate bound, the `operator_sig`
//! length, the validation errors, and the empty-blob root.
//!
//! The per-ALPN message types live in [`crate::client`] and [`crate::dht`].
//! This crate holds no cryptography: the caller (`decdn_incentive`) computes
//! and verifies every EIP-712 `operator_sig`.

use serde::Deserialize;
use serde::de::{self, Deserializer};

/// Maximum permitted value for a signed `rate_per_mb`, such as
/// [`crate::StreamResponseBody::rate_per_mb`] (issue #378).
///
/// 1000 µUSDC/MB (USDC at 6 decimals: ~$1/GB), a realistic ceiling about 100×
/// market price (~10 µUSDC/MB ≈ $0.01/GB). It is the largest `rate_per_mb` the
/// wire decodes. The governance-chain `FeeRouter` bounds its `deliveryFloor`
/// at the same value (ADR 003 §Rate-floor enforcement, ADR 009). That floor
/// is the net-USDC price of one MB of vote weight, not a rate a node must
/// quote: the `FeeRouter` credits `min(bytes, net × 1 MiB / deliveryFloor)`
/// per sweep. A floor far above real prices would drive credited bytes toward
/// zero and suppress ADR-036 vote-weight accrual, so the bound stays close to
/// real prices.
///
/// The value also participates in the client selection score
/// `rate_per_mb × rtt_ms × scale / reputation²` (issue #322, ADR 001 §Node
/// Selection Algorithm); a peer-controlled `u64::MAX` would overflow the
/// multiplication and could silently let a malicious node win the selection.
/// Selection-formula callers SHOULD still use saturating arithmetic as
/// defense-in-depth — the bound is the protocol-boundary check, not a
/// substitute for safe math.
pub const MAX_RATE_PER_MB: u64 = 1000;

/// Length in bytes of an EOA secp256k1 EIP-712 signature (`r‖s‖v`, 32+32+1).
///
/// ADR 005 § Signer binding mandates `operator_sig` is **non-empty** and that
/// requesters MUST reject missing/zero-length signatures — it does not itself
/// fix a byte length. Off-chain `operator_sig` producers are EOA-only — ADR 024
/// §Off-Chain ERC-1271 Verification: "Every signature deCDN produces or
/// verifies off-chain … is the fixed 65-byte secp256k1 `r‖s‖v` form" — which
/// is always exactly this length. Enforcing exactly this length off-chain is
/// the correct, intentionally-strict bound for the EOA producer set. When
/// off-chain ERC-1271 handling lands this constant becomes a lower bound.
pub const OPERATOR_SIG_LEN: usize = 65;

/// Errors produced when validating wire-decoded protocol messages.
///
/// `RateTooLarge` surfaces from the [`Deserialize`] impl on
/// [`crate::StreamResponseBody`] (postcard wraps it as [`postcard::Error`]
/// which the framing layer maps to [`crate::FrameError::Decode`]).
/// `InvalidOperatorSigLen` is a requester-side obligation enforced via
/// [`crate::StreamResponse::validate`] — it is intentionally NOT enforced at
/// decode time so the handler can build the response before signing it.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum MessageValidationError {
    /// `rate_per_mb` exceeds [`MAX_RATE_PER_MB`].
    #[error("rate_per_mb {rate} exceeds MAX_RATE_PER_MB ({max})", max = MAX_RATE_PER_MB)]
    RateTooLarge {
        /// The rate that was offered.
        rate: u64,
    },
    /// `rate_per_mb` is zero. A zero rate trivially wins client selection
    /// (score `rate × rtt × scale / reputation²` → 0, sorting to the top)
    /// while earning the node nothing — an obvious misconfiguration. Nodes
    /// reject it at config resolution; requesters MUST also reject it on
    /// receive (#252, ADR 005 §Rate bounds validation). Enforced via
    /// [`crate::StreamResponse::validate`], not at decode time, so a handler
    /// can still build a response before signing it.
    #[error("rate_per_mb is zero (requesters must reject; #252)")]
    RateIsZero,
    /// `operator_sig` is missing or not [`OPERATOR_SIG_LEN`] bytes. ADR 005
    /// § Signer binding mandates a non-empty signature on every signed response; the
    /// EOA-only off-chain signing path (ADR 024 §Off-Chain Signature
    /// Verification — EOA Recovery Only) makes that exactly [`OPERATOR_SIG_LEN`],
    /// which requesters MUST reject deviations from.
    #[error(
        "operator_sig has invalid length {len} \
         (ADR 005 § Signer binding: mandatory non-empty; EOA form is {expected} bytes)",
        expected = OPERATOR_SIG_LEN
    )]
    InvalidOperatorSigLen {
        /// The signature length that was offered.
        len: usize,
    },
    /// A wire [`crate::client::Voucher`]'s `signature` is not
    /// [`crate::client::VOUCHER_SIG_LEN`] bytes. The EOA off-chain voucher
    /// signing form (ADR 024 §Off-Chain Signature Verification — EOA Recovery Only) is exactly that
    /// length; receivers reject deviations before reconstructing the EIP-712
    /// typed data.
    #[error("Voucher.signature has invalid length {len} (EOA form is 65 bytes)")]
    InvalidVoucherSigLen {
        /// The signature length that was offered.
        len: usize,
    },
    /// A wire [`crate::client::ClientBinding`]'s `binding_signature` is not
    /// [`crate::client::BINDING_SIG_LEN`] bytes. The `BindNodeId` attestation
    /// is the same EOA off-chain signing form (ADR 024 §Off-Chain ERC-1271
    /// Verification); receivers reject deviations before `decdn_incentive`
    /// recovers the bound address.
    #[error("ClientBinding.binding_signature has invalid length {len} (EOA form is 65 bytes)")]
    InvalidBindingSigLen {
        /// The signature length that was offered.
        len: usize,
    },
    /// A [`crate::client::StreamResponse`] carries `body.ok == true` but a
    /// refused outcome, so it states no size proof. A node MUST NOT promise to
    /// serve without the size it serves under (ADR 005 §`cdn/client/v1`).
    /// Enforced via [`crate::client::StreamResponseExt::validate`].
    #[error("StreamResponse has ok=true but a refused outcome with no size proof")]
    StreamErrorWithOk,
    /// A [`crate::client::StreamResponse`] carries `body.ok == false` but a
    /// served outcome. A refusal MUST name its reason (ADR 005
    /// §`cdn/client/v1`). Enforced via [`crate::client::StreamResponseExt::validate`].
    #[error("StreamResponse has ok=false but no error code")]
    MissingStreamError,
    /// A [`crate::client::StreamResponse`] carries a mid-stream-only
    /// [`crate::client::StreamError::VoucherRejected`] in its `error` field.
    /// That variant rides exclusively in [`crate::client::ClientMessage::StreamError`]
    /// (ADR 005 §`VoucherRejected` semantics). Enforced via
    /// [`crate::client::StreamResponseExt::validate`].
    #[error("StreamResponse.error carries VoucherRejected (a mid-stream-only code)")]
    VoucherRejectedInResponse,
    /// A [`crate::client::ChunkData`] carries a zero-length payload. ADR 005
    /// §Frame size permits a *shorter* final frame, never an *empty*
    /// one: an empty frame advances neither the receiver's cumulative byte count
    /// nor its voucher accounting, so an unbounded run of them drives the receive
    /// loop without application-level progress (#1088). Enforced by
    /// [`crate::client::ChunkData::new`], the `try_from` decode gate, and every
    /// `encode_chunk_*` helper ([`crate::client::encode_chunk_frame`],
    /// [`crate::client::encode_chunk_frame_headers`], and the crate-private one the
    /// latter builds on) — every route to a frame body.
    #[error(
        "ChunkData carries a zero-length payload (ADR 005: a chunk must carry at least 1 byte)"
    )]
    EmptyChunk,
    /// The postcard message body of a [`crate::client::ChunkData`] frame —
    /// discriminant + length prefix + payload, which is the length the framing prefix
    /// announces — exceeds [`crate::client::MAX_CHUNK_FRAME`] (ADR 013 §Wire
    /// Framing).
    ///
    /// The field is named for what it holds: this is always larger than the
    /// `payload_len` the caller passed in, so comparing it against that is a category
    /// error.
    #[error("ChunkData frame length {frame_len} exceeds MAX_CHUNK_FRAME ({max})", max = crate::client::MAX_CHUNK_FRAME)]
    ChunkTooLarge {
        /// The refused frame-body length, in bytes.
        frame_len: usize,
    },
    /// A wire [`crate::client::WireCapability`]'s `owner_signature` is empty.
    /// The EOA form is exactly [`crate::client::VOUCHER_SIG_LEN`] bytes but an
    /// ERC-1271 contract-signer form may be longer, so only the non-empty
    /// floor is a wire-level check; the rest is `decdn_incentive`'s job
    /// (ADR 024 §Off-Chain Signature Verification — EOA Recovery Only).
    #[error("WireCapability.owner_signature is empty")]
    EmptyCapabilitySignature,
}

/// The BLAKE3 hash of zero bytes: the root of the empty blob.
///
/// The empty blob spans zero discovery blocks, so it is the one hash whose
/// coverage is empty on every honest holder. For it,
/// [`crate::StreamResponseExt::consistent_with`] only requires an empty
/// coverage, and the requester reads `ok` alone to learn whether the node
/// serves it.
pub const EMPTY_ROOT: [u8; 32] = [
    0xaf, 0x13, 0x49, 0xb9, 0xf5, 0xf9, 0xa1, 0xa6, 0xa0, 0x40, 0x4d, 0xea, 0x36, 0xdc, 0xc9, 0x49,
    0x9b, 0xcb, 0x25, 0xc9, 0xad, 0xc1, 0x12, 0xb7, 0xcc, 0x9a, 0x93, 0xca, 0xe4, 0x1f, 0x32, 0x62,
];

// Field-level deserialize hook so the protocol boundary rejects oversize
// `rate_per_mb` (issue #378). Using `#[serde(deserialize_with)]` rather
// than a hand-written `impl Deserialize for StreamResponseBody` keeps the
// struct's field order and codec in lockstep with the derive — there is
// no mirror struct to drift out of sync. The wire bytes are byte-identical
// to what a fully-derived `Deserialize` would have read, so postcard's
// positional layout is preserved.
pub(crate) fn deserialize_rate_per_mb<'de, D>(deserializer: D) -> Result<u64, D::Error>
where
    D: Deserializer<'de>,
{
    let rate = u64::deserialize(deserializer)?;
    if rate > MAX_RATE_PER_MB {
        return Err(de::Error::custom(MessageValidationError::RateTooLarge {
            rate,
        }));
    }
    Ok(rate)
}

#[cfg(test)]
mod tests;
