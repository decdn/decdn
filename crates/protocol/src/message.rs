//! Wire message payload types for deCDN ALPN protocols.
//!
//! Every ALPN defines a top-level enum (e.g. [`ProbeMessage`]) whose variants
//! wrap the per-message structs. The enum is serialized as the outermost
//! postcard value inside a length-prefixed frame (see [`crate::framing`]).
//!
//! # Signed-field freezing (ADR 013 §Signed Field Freezing)
//!
//! [`ProbeResponse`] is split into a signed [`ProbeResponseBody`] plus the
//! outer unsigned fields `total_bytes` and `slash_sig`. Unlike
//! [`crate::gossip::NodeAnnounce`] (Ed25519 over postcard bytes), `slash_sig`
//! is **not** computed over a postcard serialization: it is an EIP-712
//! secp256k1 signature over the *typed-data hash of the body fields*
//! `{hash, has_blob, rate_per_mb, timestamp_us}`, exactly as defined in ADR
//! 014 §EIP-712 Type Definitions and implemented in
//! `decdn_incentive::ProbeSlashData`. There is intentionally no
//! `signing_bytes()` helper here — postcard bytes are *not* the signing
//! input, and exposing one would invite incompatible verifier
//! implementations. The `#[serde(deserialize_with)]` validation on
//! `rate_per_mb` lives on the body field so the protocol-boundary bound on
//! [`MAX_RATE_PER_MB`] continues to apply (issue #378).
//!
//! This establishes the `cdn/probe/v1` signed baseline — it is **not** an
//! ALPN bump. ADR 013 freezes a signed field set at the protocol version that
//! introduces it; `cdn/probe/v1` had no prior signed `ProbeResponse`, so the
//! set `{hash, has_blob, rate_per_mb, timestamp_us}` is the v1 baseline (ADR
//! 005 §`cdn/probe/v1`, ADR 014 §1). `total_bytes` is the ADR 013 Tier-1
//! unsigned-evolution exemplar (ADR 005 §`cdn/probe/v1`) and is deliberately
//! NOT covered by `slash_sig`. The `ProbeMessage` variant order is unchanged.
//!
//! This crate deliberately does not depend on any crypto library. The caller
//! (`decdn_incentive::ProbeSlashData` and the probe handler/CLI) computes and
//! verifies the EIP-712 `slash_sig`.

use serde::de::{self, Deserializer};
use serde::{Deserialize, Serialize};

/// Maximum permitted value for [`ProbeResponseBody::rate_per_mb`] (issue #378).
///
/// 1 trillion (10^12) base units. The value participates in the client
/// selection score `rate_per_mb × rtt_ms × scale / reputation²` (issue #322,
/// ADR 001 §Node Selection Algorithm); a peer-controlled `u64::MAX` would
/// cause the multiplication to overflow and could silently let a malicious
/// node win the selection — exactly the inverse of what an honestly-priced
/// rate would do.
///
/// The bound sits above any plausible CDN rate (USDC at 6 decimals: $1M per
/// MB) while leaving 7+ orders of magnitude of headroom from `u64::MAX`
/// (~1.8 × 10^19) for the downstream selection arithmetic. Selection-formula
/// callers SHOULD still use saturating arithmetic as defense-in-depth — the
/// bound is the protocol-boundary check, not a substitute for safe math.
pub const MAX_RATE_PER_MB: u64 = 1_000_000_000_000;

/// Length in bytes of an EOA secp256k1 EIP-712 signature (`r‖s‖v`, 32+32+1).
///
/// ADR 014 §1 mandates `slash_sig` is **non-empty** and that requesters MUST
/// reject missing/zero-length signatures — it does not itself fix a byte
/// length. Off-chain `slash_sig` producers are EOA-only — ADR 024 §Off-Chain
/// ERC-1271 Verification: "Every signature deCDN produces or verifies
/// off-chain … is the fixed 65-byte secp256k1 `r‖s‖v` form" — which is always
/// exactly this length. Variable-length
/// **ERC-1271** smart-account signatures are verified on-chain by
/// `SlashJudge` via `SignatureChecker.isValidSignatureNow` (ADR 014
/// §On-Chain Verification); enforcing exactly this length off-chain is the
/// correct, intentionally-strict bound for the EOA producer set. When
/// off-chain ERC-1271 handling lands this constant becomes a lower bound.
pub const SLASH_SIG_LEN: usize = 65;

/// Errors produced when validating wire-decoded protocol messages.
///
/// `RateTooLarge` surfaces from the [`Deserialize`] impl on
/// [`ProbeResponseBody`] (postcard wraps it as [`postcard::Error`] which the
/// framing layer maps to [`crate::FrameError::Decode`]).
/// `InvalidSlashSigLen` is a requester-side obligation enforced via
/// [`ProbeResponse::validate`] — it is intentionally NOT enforced at decode
/// time so the handler can build the response before signing it.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum MessageValidationError {
    /// `rate_per_mb` exceeds [`MAX_RATE_PER_MB`].
    #[error("rate_per_mb {rate} exceeds MAX_RATE_PER_MB ({max})", max = MAX_RATE_PER_MB)]
    RateTooLarge { rate: u64 },
    /// `rate_per_mb` is zero. A zero rate trivially wins client selection
    /// (score `rate × rtt × scale / reputation²` → 0, sorting to the top)
    /// while earning the node nothing — an obvious misconfiguration. Nodes
    /// reject it at config resolution; requesters MUST also reject it on
    /// receive (#252, ADR 005 §Rate bounds validation). Enforced via
    /// [`ProbeResponse::validate`] / `StreamResponse::validate`, not at decode
    /// time, so a handler can still build a response before signing it.
    #[error("rate_per_mb is zero (requesters must reject; #252)")]
    RateIsZero,
    /// `slash_sig` is missing or not [`SLASH_SIG_LEN`] bytes. ADR 014 §1
    /// mandates a non-empty signature on every `ProbeResponse`; the
    /// EOA-only off-chain signing path (ADR 024 §Off-Chain ERC-1271
    /// Verification) makes that exactly [`SLASH_SIG_LEN`], which requesters
    /// MUST reject deviations from.
    #[error(
        "slash_sig has invalid length {len} \
         (ADR 014 §1: mandatory non-empty; EOA form is {expected} bytes)",
        expected = SLASH_SIG_LEN
    )]
    InvalidSlashSigLen { len: usize },
    /// A wire [`crate::client::Voucher`]'s `signature` is not
    /// [`crate::client::VOUCHER_SIG_LEN`] bytes. The EOA off-chain voucher
    /// signing form (ADR 024 §Off-Chain ERC-1271 Verification) is exactly that
    /// length; receivers reject deviations before reconstructing the EIP-712
    /// typed data.
    #[error("Voucher.signature has invalid length {len} (EOA form is 65 bytes)")]
    InvalidVoucherSigLen { len: usize },
    /// A wire [`crate::client::ClientBinding`]'s `binding_signature` is not
    /// [`crate::client::BINDING_SIG_LEN`] bytes. The `BindNodeId` attestation
    /// is the same EOA off-chain signing form (ADR 024 §Off-Chain ERC-1271
    /// Verification); receivers reject deviations before `decdn_incentive`
    /// recovers the bound address.
    #[error("ClientBinding.binding_signature has invalid length {len} (EOA form is 65 bytes)")]
    InvalidBindingSigLen { len: usize },
    /// A negotiated `voucher_interval_mb` is outside `1..=MAX_VOUCHER_INTERVAL_MB`
    /// (ADR 003 §Voucher Interval Negotiation). Zero would never require a
    /// voucher; an oversized value opens an unbounded unvouchered-byte window
    /// (and `interval * MB_BYTES` can overflow `u64`) — the inverse of #378 for
    /// the rate field. Enforced via [`crate::client::StreamResponse::validate`]
    /// / [`crate::client::StreamRequestExt::validate`].
    #[error(
        "voucher_interval_mb {interval} out of range (1..={max})",
        max = crate::client::MAX_VOUCHER_INTERVAL_MB
    )]
    VoucherIntervalOutOfRange { interval: u64 },
    /// A [`crate::client::StreamResponse`] carries `body.ok == true` yet also an
    /// `error`. A node MUST NOT both promise to serve and report a failure
    /// (ADR 005 §`cdn/client/v1`). Enforced via
    /// [`crate::client::StreamResponse::validate`].
    #[error("StreamResponse has ok=true but also carries an error")]
    StreamErrorWithOk,
    /// A [`crate::client::StreamResponse`] carries `body.ok == false` but no
    /// `error` code. A refusal MUST name its reason (ADR 005
    /// §`cdn/client/v1`). Enforced via [`crate::client::StreamResponse::validate`].
    #[error("StreamResponse has ok=false but no error code")]
    MissingStreamError,
    /// A [`crate::client::StreamResponse`] carries a mid-stream-only
    /// [`crate::client::StreamError::VoucherRejected`] in its `error` field.
    /// That variant rides exclusively in [`crate::client::ClientMessage::StreamError`]
    /// (ADR 005 §`VoucherRejected` semantics). Enforced via
    /// [`crate::client::StreamResponse::validate`].
    #[error("StreamResponse.error carries VoucherRejected (a mid-stream-only code)")]
    VoucherRejectedInResponse,
    /// A [`crate::client::ChunkData`] carries a zero-length payload. ADR 005
    /// §Partial final chunk permits a *smaller* final frame, never an *empty*
    /// one: an empty frame advances neither the receiver's cumulative byte count
    /// nor its voucher accounting, so an unbounded run of them drives the receive
    /// loop without application-level progress (#1088). Enforced by
    /// [`crate::client::ChunkData::new`] and the `try_from` decode gate — the only
    /// two ways to obtain a frame.
    #[error(
        "ChunkData carries a zero-length payload (ADR 005: a chunk must carry at least 1 byte)"
    )]
    EmptyChunk,
    /// A [`crate::client::ChunkData`] payload exceeds [`crate::CHUNK_SIZE`].
    /// The ceiling bounds receiver allocation per frame (ADR 005
    /// §`cdn/client/v1`). Enforced by [`crate::client::ChunkData::new`] and the
    /// `try_from` decode gate, as above.
    #[error("ChunkData payload of {len} bytes exceeds CHUNK_SIZE ({max})", max = crate::CHUNK_SIZE)]
    ChunkTooLarge { len: usize },
    /// A wire [`crate::client::WireCapability`]'s `owner_signature` is empty.
    /// The EOA form is exactly [`crate::client::VOUCHER_SIG_LEN`] bytes but an
    /// ERC-1271 contract-signer form may be longer, so only the non-empty
    /// floor is a wire-level check; the rest is `decdn_incentive`'s job
    /// (ADR 024 §Off-Chain ERC-1271 Verification).
    #[error("WireCapability.owner_signature is empty")]
    EmptyCapabilitySignature,
}

/// Top-level protocol enum for `cdn/probe/v1`. Variant order is frozen per
/// ADR 013 — new variants MUST be appended at the end.
///
/// ⚠️ **VARIANT ORDER FROZEN — ADR 013 §Protocol Enums**
/// Postcard encodes each variant as its declaration-order index. Reordering,
/// inserting, or removing a variant is a wire-breaking change requiring an
/// ALPN version bump (`cdn/probe/v2`). The discriminant assignments are
/// locked in by the tests `probe_message_request_discriminant_is_zero` and
/// `probe_message_response_discriminant_is_one` — if you change this enum,
/// those tests will fail and tell you why.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ProbeMessage {
    /// discriminant 0 — asserted by `probe_message_request_discriminant_is_zero`
    Request(ProbeRequest),
    /// discriminant 1 — asserted by `probe_message_response_discriminant_is_one`
    Response(ProbeResponse),
}

impl crate::framing::TopLevelEnum for ProbeMessage {
    /// `Request` (0), `Response` (1). Pinned by
    /// `probe_message_variant_count_matches_discriminants`.
    const VARIANT_COUNT: u32 = 2;
}

/// Client → node request on `cdn/probe/v1` (ADR 005 §`cdn/probe/v1`).
///
/// A probe is unauthenticated and unpaid: it asks a candidate node whether it
/// holds `hash` and at what rate it will serve it, so the client can rank
/// candidates on both availability and price in a single round-trip.
/// `timestamp_us` is a requester-generated microsecond timestamp echoed back
/// in [`ProbeResponseBody::timestamp_us`]; it serves both response
/// correlation and RTT measurement (RTT = `receive_time` − `timestamp_us`)
/// and is
/// part of the EIP-712 signed set so the slashing window can be computed
/// on-chain from a single requester clock (ADR 014 §1).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProbeRequest {
    /// BLAKE3 hash of the blob being queried (iroh `Hash`, 32 bytes).
    pub hash: [u8; 32],
    /// Requester-generated microseconds since Unix epoch, echoed back.
    pub timestamp_us: u64,
}

/// Node → client response on `cdn/probe/v1` (ADR 005 §`cdn/probe/v1`).
///
/// The signed [`ProbeResponseBody`] is covered by `slash_sig` (EIP-712
/// secp256k1, ADR 014 §1). `total_bytes` is an optional unsigned field added
/// per the ADR 013 Tier-1 minor-evolution pattern and is deliberately NOT
/// covered by `slash_sig`. `slash_sig` is mandatory and non-empty on the
/// wire; requesters MUST reject missing/zero-length signatures (enforced via
/// [`ProbeResponse::validate`] and the requester's [`SLASH_SIG_LEN`] check).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProbeResponse {
    /// Signed body. Its wire layout is frozen per ADR 013.
    pub body: ProbeResponseBody,
    /// Blob size in bytes when known. Optional, unsigned, NOT covered by
    /// `slash_sig` (ADR 005 §`cdn/probe/v1`, ADR 013 Tier-1). Nodes SHOULD
    /// include it when the size is known so requesters can estimate cost.
    pub total_bytes: Option<u64>,
    /// EIP-712 secp256k1 signature over the typed-data hash of `body`'s
    /// fields `{hash, has_blob, rate_per_mb, timestamp_us}` (ADR 014 §1; see
    /// `decdn_incentive::ProbeSlashData`). Always exactly [`SLASH_SIG_LEN`]
    /// bytes — *not* a signature over postcard bytes. The verify path
    /// rejects any other length.
    pub slash_sig: Vec<u8>,
}

/// Signed fields of a [`ProbeResponse`]. Layout is frozen per ADR 013 — future
/// additions go on [`ProbeResponse`] as optional unsigned fields, not here.
///
/// `rate_per_mb` is bounded by [`MAX_RATE_PER_MB`] at the wire boundary — the
/// field-level `deserialize_rate_per_mb` hook rejects oversize values so a
/// malicious node cannot poison the client selection score with an
/// overflow-inducing rate (issue #378). Server-side construction is
/// unconstrained at the type level; the node's config layer
/// (`resolve_payment`) enforces the same ceiling at startup and on hot
/// reload, keeping the bound bilateral.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProbeResponseBody {
    /// BLAKE3 hash this response answers for, echoed from the request.
    pub hash: [u8; 32],
    /// Whether the node currently holds the blob and will serve it. A node
    /// MUST NOT sign `true` unless it can guarantee delivery within the
    /// slashing window (ADR 005 §Probe-triggered eviction hold).
    pub has_blob: bool,
    /// The node's current quoted rate in payment-token base units per MB. The
    /// token is USDC, fixed at contract deployment (ADR 003), so the wire
    /// carries no token identifier and the units need no normalization.
    /// Bounded by [`MAX_RATE_PER_MB`].
    #[serde(deserialize_with = "deserialize_rate_per_mb")]
    pub rate_per_mb: u64,
    /// The requester-generated microsecond timestamp from the corresponding
    /// [`ProbeRequest`], echoed back unchanged.
    pub timestamp_us: u64,
}

impl ProbeResponse {
    /// Validate field invariants. The wire-decode path enforces the
    /// `rate_per_mb` bound automatically via `deserialize_rate_per_mb`; this
    /// method additionally enforces the requester-side mandatory-`slash_sig`
    /// rule (ADR 014 §1) and is exposed so construction sites can re-check
    /// before sending and tests can assert validity without a full
    /// encode/decode roundtrip.
    pub const fn validate(&self) -> Result<(), MessageValidationError> {
        if self.body.rate_per_mb > MAX_RATE_PER_MB {
            return Err(MessageValidationError::RateTooLarge {
                rate: self.body.rate_per_mb,
            });
        }
        // #252: requesters MUST reject a zero rate on receive. The decode hook
        // only bounds the upper end (a peer-controlled overflow source, #378);
        // zero is a legitimate `u64` the wire accepts but a requester must not.
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

// Field-level deserialize hook so the protocol boundary rejects oversize
// `rate_per_mb` (issue #378). Using `#[serde(deserialize_with)]` rather
// than a hand-written `impl Deserialize for ProbeResponseBody` keeps the
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
mod tests {
    use super::*;

    fn sample_body() -> ProbeResponseBody {
        ProbeResponseBody {
            hash: [7u8; 32],
            has_blob: true,
            rate_per_mb: 10,
            timestamp_us: 1_700_000_000_000_000,
        }
    }

    fn sample_response() -> ProbeResponse {
        ProbeResponse {
            body: sample_body(),
            total_bytes: Some(4096),
            slash_sig: vec![0xABu8; SLASH_SIG_LEN],
        }
    }

    #[test]
    fn probe_request_roundtrip() -> Result<(), postcard::Error> {
        let req = ProbeRequest {
            hash: [9u8; 32],
            timestamp_us: 0xdead_beef,
        };
        let bytes = postcard::to_allocvec(&req)?;
        let decoded: ProbeRequest = postcard::from_bytes(&bytes)?;
        assert_eq!(req, decoded);
        Ok(())
    }

    #[test]
    fn probe_response_roundtrip() -> Result<(), postcard::Error> {
        let resp = sample_response();
        let bytes = postcard::to_allocvec(&resp)?;
        let decoded: ProbeResponse = postcard::from_bytes(&bytes)?;
        assert_eq!(resp, decoded);
        Ok(())
    }

    #[test]
    fn probe_response_total_bytes_none_roundtrip() -> Result<(), postcard::Error> {
        let resp = ProbeResponse {
            total_bytes: None,
            ..sample_response()
        };
        let bytes = postcard::to_allocvec(&resp)?;
        let decoded: ProbeResponse = postcard::from_bytes(&bytes)?;
        assert_eq!(resp, decoded);
        assert_eq!(decoded.total_bytes, None);
        Ok(())
    }

    #[test]
    fn probe_message_request_discriminant_is_zero() -> Result<(), postcard::Error> {
        let msg = ProbeMessage::Request(ProbeRequest {
            hash: [0u8; 32],
            timestamp_us: 1,
        });
        let bytes = postcard::to_allocvec(&msg)?;
        assert_eq!(bytes.first().copied(), Some(0u8));
        let decoded: ProbeMessage = postcard::from_bytes(&bytes)?;
        assert_eq!(decoded, msg);
        Ok(())
    }

    #[test]
    fn probe_message_response_discriminant_is_one() -> Result<(), postcard::Error> {
        let msg = ProbeMessage::Response(sample_response());
        let bytes = postcard::to_allocvec(&msg)?;
        assert_eq!(bytes.first().copied(), Some(1u8));
        let decoded: ProbeMessage = postcard::from_bytes(&bytes)?;
        assert_eq!(decoded, msg);
        Ok(())
    }

    #[test]
    fn probe_message_rejects_unknown_discriminant() {
        let bytes = [99u8, 0, 0, 0, 0];
        let r: Result<ProbeMessage, _> = postcard::from_bytes(&bytes);
        assert!(r.is_err());
    }

    // Pins `TopLevelEnum::VARIANT_COUNT` to the actual highest discriminant so a
    // future variant addition (which shifts the unknown/known boundary the ADR
    // 013 classifier relies on) must update the count in lockstep.
    #[test]
    fn probe_message_variant_count_matches_discriminants() -> Result<(), postcard::Error> {
        use crate::framing::TopLevelEnum;
        assert_eq!(ProbeMessage::VARIANT_COUNT, 2);
        // The last declared variant (`Response`) must encode to discriminant
        // VARIANT_COUNT - 1. Compare against postcard's own varint encoding of
        // that index (not `bytes.first()`) so the pin stays correct even if the
        // enum ever grows a multi-byte discriminant (> 127 variants).
        let last = ProbeMessage::Response(sample_response());
        let bytes = postcard::to_allocvec(&last)?;
        let expected_disc = postcard::to_allocvec(&(ProbeMessage::VARIANT_COUNT - 1))?;
        assert!(bytes.starts_with(&expected_disc));
        Ok(())
    }

    #[test]
    fn probe_message_unknown_discriminant_is_flagged_unsupported() {
        // Discriminant 2 is the first index past the known set → UNSUPPORTED.
        assert!(crate::is_unknown_variant::<ProbeMessage>(&[2u8, 0, 0]));
        // Discriminant 1 (Response) is known — an over-cap-rate decode failure
        // on it must stay MALFORMED, not flip to UNSUPPORTED.
        assert!(!crate::is_unknown_variant::<ProbeMessage>(&[1u8, 0xFF]));
    }

    // Issue #378: the wire boundary MUST reject `rate_per_mb` above
    // MAX_RATE_PER_MB so a malicious peer cannot feed an overflow-inducing
    // value into the client selection score. The hook now lives on the
    // signed body field.
    #[test]
    fn probe_response_body_decode_rejects_rate_above_max() -> Result<(), postcard::Error> {
        let body = ProbeResponseBody {
            rate_per_mb: MAX_RATE_PER_MB + 1,
            ..sample_body()
        };
        let bytes = postcard::to_allocvec(&body)?;
        let decoded: Result<ProbeResponseBody, _> = postcard::from_bytes(&bytes);
        assert!(decoded.is_err(), "expected decode rejection");
        Ok(())
    }

    #[test]
    fn probe_response_decode_rejects_u64_max_rate() -> Result<(), postcard::Error> {
        let resp = ProbeResponse {
            body: ProbeResponseBody {
                rate_per_mb: u64::MAX,
                ..sample_body()
            },
            ..sample_response()
        };
        let bytes = postcard::to_allocvec(&resp)?;
        let decoded: Result<ProbeResponse, _> = postcard::from_bytes(&bytes);
        assert!(decoded.is_err(), "expected decode rejection for u64::MAX");
        Ok(())
    }

    // Boundary holds: exactly MAX_RATE_PER_MB must round-trip cleanly,
    // confirming the rejection above is on `>`, not `>=`.
    #[test]
    fn probe_response_decode_accepts_rate_at_max() -> Result<(), postcard::Error> {
        let resp = ProbeResponse {
            body: ProbeResponseBody {
                rate_per_mb: MAX_RATE_PER_MB,
                ..sample_body()
            },
            ..sample_response()
        };
        let bytes = postcard::to_allocvec(&resp)?;
        let decoded: ProbeResponse = postcard::from_bytes(&bytes)?;
        assert_eq!(decoded.body.rate_per_mb, MAX_RATE_PER_MB);
        Ok(())
    }

    #[test]
    fn probe_message_response_decode_rejects_oversize_rate() -> Result<(), postcard::Error> {
        let resp = ProbeResponse {
            body: ProbeResponseBody {
                rate_per_mb: MAX_RATE_PER_MB + 1,
                ..sample_body()
            },
            ..sample_response()
        };
        let msg = ProbeMessage::Response(resp);
        let bytes = postcard::to_allocvec(&msg)?;
        let decoded: Result<ProbeMessage, _> = postcard::from_bytes(&bytes);
        assert!(
            decoded.is_err(),
            "ProbeMessage decode must propagate ProbeResponseBody validation"
        );
        Ok(())
    }

    #[test]
    fn probe_response_validate_is_consistent_with_decode() {
        let bad = ProbeResponse {
            body: ProbeResponseBody {
                rate_per_mb: MAX_RATE_PER_MB + 1,
                ..sample_body()
            },
            ..sample_response()
        };
        assert_eq!(
            bad.validate(),
            Err(MessageValidationError::RateTooLarge {
                rate: MAX_RATE_PER_MB + 1,
            })
        );

        let good = sample_response();
        assert_eq!(good.validate(), Ok(()));
    }

    // #252: a requester calling `validate()` must reject a zero rate. The
    // decode path accepts it (zero is a valid u64 ≤ MAX), so the obligation
    // lives in the requester-side `validate()` — pin it here.
    #[test]
    fn probe_response_validate_rejects_zero_rate() {
        let resp = ProbeResponse {
            body: ProbeResponseBody {
                rate_per_mb: 0,
                ..sample_body()
            },
            ..sample_response()
        };
        assert_eq!(resp.validate(), Err(MessageValidationError::RateIsZero));
    }

    #[test]
    fn probe_response_validate_rejects_empty_slash_sig() {
        let resp = ProbeResponse {
            slash_sig: Vec::new(),
            ..sample_response()
        };
        assert_eq!(
            resp.validate(),
            Err(MessageValidationError::InvalidSlashSigLen { len: 0 })
        );
    }

    #[test]
    fn probe_response_validate_rejects_wrong_length_slash_sig() {
        // A non-empty but too-short signature must also be rejected — the
        // public helper has to be as strict as the wire invariant
        // (SLASH_SIG_LEN), not merely "non-empty".
        let resp = ProbeResponse {
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
    fn probe_response_trailing_bytes_tolerated() -> Result<(), postcard::Error> {
        // ADR 013: `take_from_bytes` silently ignores trailing bytes so future
        // unknown unsigned fields don't break old decoders.
        let resp = sample_response();
        let mut bytes = postcard::to_allocvec(&resp)?;
        bytes.extend_from_slice(&[0xAAu8, 0xBB, 0xCC]);
        let (decoded, tail) = postcard::take_from_bytes::<ProbeResponse>(&bytes)?;
        assert_eq!(decoded, resp);
        assert_eq!(tail, &[0xAAu8, 0xBB, 0xCC]);
        Ok(())
    }

    // Wire-format guard: the signed-body split must not silently change the
    // on-wire layout. If postcard's bytes for a ProbeResponse change, this
    // fixed-byte assertion catches it before the change ships.
    // SLASH_SIG_LEN (65) fits a u8 and a single postcard varint byte; the
    // cast is exact and asserted by this very test.
    #[allow(clippy::cast_possible_truncation)]
    #[test]
    fn probe_response_wire_format_is_stable() -> Result<(), postcard::Error> {
        let resp = ProbeResponse {
            body: ProbeResponseBody {
                hash: [3u8; 32],
                has_blob: true,
                rate_per_mb: 4,
                timestamp_us: 5,
            },
            total_bytes: None,
            slash_sig: vec![0xABu8; SLASH_SIG_LEN],
        };
        let bytes = postcard::to_allocvec(&resp)?;
        // postcard layout: body{ hash=32 raw, has_blob=1 byte (0x01),
        // rate_per_mb=4 (1-byte varint), timestamp_us=5 (1-byte varint) },
        // total_bytes=None (1-byte Option tag 0x00), slash_sig Vec
        // (len varint 65=0x41, then 65 bytes).
        let mut expected = Vec::with_capacity(32 + 1 + 1 + 1 + 1 + 1 + SLASH_SIG_LEN);
        expected.extend_from_slice(&[3u8; 32]); // body.hash
        expected.push(1u8); // body.has_blob = true
        expected.push(4u8); // body.rate_per_mb varint
        expected.push(5u8); // body.timestamp_us varint
        expected.push(0u8); // total_bytes = None
        expected.push(SLASH_SIG_LEN as u8); // slash_sig length prefix (65)
        expected.extend_from_slice(&[0xABu8; SLASH_SIG_LEN]); // slash_sig bytes
        assert_eq!(bytes, expected);
        Ok(())
    }
}
