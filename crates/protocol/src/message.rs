//! Wire message payload types for deCDN ALPN protocols.
//!
//! Every ALPN defines a top-level enum (e.g. [`ProbeMessage`]) whose variants
//! wrap the per-message structs. The enum is serialized as the outermost
//! postcard value inside a length-prefixed frame (see [`crate::framing`]).
//!
//! Signed-field freezing (ADR 013 §Signed Field Freezing): `ProbeResponse` is
//! currently unsigned; when it gains a signature, its signed fields will be
//! split into a frozen `ProbeResponseBody` per ADR 013. The
//! `#[serde(deserialize_with)]` validation on `rate_per_mb` must move with
//! the signed body so the protocol-boundary bound on [`MAX_RATE_PER_MB`]
//! continues to apply.

use serde::de::{self, Deserializer};
use serde::{Deserialize, Serialize};

/// Maximum permitted value for [`ProbeResponse::rate_per_mb`] (issue #378).
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

/// Errors produced when validating wire-decoded protocol messages.
///
/// These surface from the [`Deserialize`] impl on validated message types
/// (see [`ProbeResponse`]); postcard wraps them as [`postcard::Error`] which
/// the framing layer in turn maps to [`crate::FrameError::Decode`].
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum MessageValidationError {
    /// `rate_per_mb` exceeds [`MAX_RATE_PER_MB`].
    #[error(
        "ProbeResponse.rate_per_mb {rate} exceeds MAX_RATE_PER_MB ({max})",
        max = MAX_RATE_PER_MB
    )]
    RateTooLarge { rate: u64 },
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

/// Client → node request on `cdn/probe/v1`.
///
/// A probe is unauthenticated and unpaid: it asks a candidate node to identify
/// itself and report its current per-MB rate so the client can rank it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProbeRequest {
    /// Client-chosen nonce echoed back in the response. Lets clients correlate
    /// concurrent probes and match responses to the originating request.
    pub nonce: u64,
}

/// Node → client response on `cdn/probe/v1`.
///
/// `rate_per_mb` is bounded by [`MAX_RATE_PER_MB`] at the wire boundary —
/// the field-level [`deserialize_rate_per_mb`] hook rejects oversize values
/// so a malicious node cannot poison the client selection score with an
/// overflow-inducing rate (issue #378). Server-side construction is
/// unconstrained at the type level; the node's config layer
/// (`resolve_payment`) enforces the same ceiling at startup and on hot
/// reload, keeping the bound bilateral.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProbeResponse {
    /// The nonce from the corresponding [`ProbeRequest`].
    pub nonce: u64,
    /// Node-side timestamp when the response was generated, as Unix epoch ms.
    pub measured_at_unix_ms: u64,
    /// The responding node's ed25519 public key (iroh `NodeId`), 32 bytes.
    pub node_id: [u8; 32],
    /// The node's current quoted rate in token base units per MB. The specific
    /// token is a deployment concern (see ADR 010) — the protocol itself does
    /// not normalize units across tokens. Bounded by [`MAX_RATE_PER_MB`].
    #[serde(deserialize_with = "deserialize_rate_per_mb")]
    pub rate_per_mb: u64,
}

impl ProbeResponse {
    /// Validate field invariants. The wire-decode path enforces the same
    /// bound automatically via [`deserialize_rate_per_mb`]; this method is
    /// exposed so server-side construction sites can re-check before
    /// sending and so tests can assert validity without going through a
    /// full encode/decode roundtrip.
    pub const fn validate(&self) -> Result<(), MessageValidationError> {
        if self.rate_per_mb > MAX_RATE_PER_MB {
            return Err(MessageValidationError::RateTooLarge {
                rate: self.rate_per_mb,
            });
        }
        Ok(())
    }
}

// Field-level deserialize hook so the protocol boundary rejects oversize
// `rate_per_mb` (issue #378). Using `#[serde(deserialize_with)]` rather
// than a hand-written `impl Deserialize for ProbeResponse` keeps the
// struct's field order and codec in lockstep with the derive — there is
// no mirror struct to drift out of sync. The wire bytes are byte-identical
// to what a fully-derived `Deserialize` would have read, so postcard's
// positional layout is preserved.
fn deserialize_rate_per_mb<'de, D>(deserializer: D) -> Result<u64, D::Error>
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

    #[test]
    fn probe_request_roundtrip() -> Result<(), postcard::Error> {
        let req = ProbeRequest { nonce: 0xdead_beef };
        let bytes = postcard::to_allocvec(&req)?;
        let decoded: ProbeRequest = postcard::from_bytes(&bytes)?;
        assert_eq!(req, decoded);
        Ok(())
    }

    #[test]
    fn probe_response_roundtrip() -> Result<(), postcard::Error> {
        let resp = ProbeResponse {
            nonce: 42,
            measured_at_unix_ms: 1_700_000_000_000,
            node_id: [7u8; 32],
            rate_per_mb: 10,
        };
        let bytes = postcard::to_allocvec(&resp)?;
        let decoded: ProbeResponse = postcard::from_bytes(&bytes)?;
        assert_eq!(resp, decoded);
        Ok(())
    }

    #[test]
    fn probe_message_request_discriminant_is_zero() -> Result<(), postcard::Error> {
        let msg = ProbeMessage::Request(ProbeRequest { nonce: 1 });
        let bytes = postcard::to_allocvec(&msg)?;
        assert_eq!(bytes.first().copied(), Some(0u8));
        let decoded: ProbeMessage = postcard::from_bytes(&bytes)?;
        assert_eq!(decoded, msg);
        Ok(())
    }

    #[test]
    fn probe_message_response_discriminant_is_one() -> Result<(), postcard::Error> {
        let resp = ProbeResponse {
            nonce: 9,
            measured_at_unix_ms: 1,
            node_id: [0u8; 32],
            rate_per_mb: 2,
        };
        let msg = ProbeMessage::Response(resp);
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

    // Issue #378: the wire boundary MUST reject `rate_per_mb` above
    // MAX_RATE_PER_MB so a malicious peer cannot feed an overflow-inducing
    // value into the client selection score.
    #[test]
    fn probe_response_decode_rejects_rate_above_max() -> Result<(), postcard::Error> {
        let resp = ProbeResponse {
            nonce: 1,
            measured_at_unix_ms: 0,
            node_id: [0u8; 32],
            rate_per_mb: MAX_RATE_PER_MB + 1,
        };
        let bytes = postcard::to_allocvec(&resp)?;
        let decoded: Result<ProbeResponse, _> = postcard::from_bytes(&bytes);
        assert!(decoded.is_err(), "expected decode rejection");
        Ok(())
    }

    #[test]
    fn probe_response_decode_rejects_u64_max_rate() -> Result<(), postcard::Error> {
        let resp = ProbeResponse {
            nonce: 1,
            measured_at_unix_ms: 0,
            node_id: [0u8; 32],
            rate_per_mb: u64::MAX,
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
            nonce: 1,
            measured_at_unix_ms: 0,
            node_id: [0u8; 32],
            rate_per_mb: MAX_RATE_PER_MB,
        };
        let bytes = postcard::to_allocvec(&resp)?;
        let decoded: ProbeResponse = postcard::from_bytes(&bytes)?;
        assert_eq!(decoded.rate_per_mb, MAX_RATE_PER_MB);
        Ok(())
    }

    #[test]
    fn probe_message_response_decode_rejects_oversize_rate() -> Result<(), postcard::Error> {
        let resp = ProbeResponse {
            nonce: 1,
            measured_at_unix_ms: 0,
            node_id: [0u8; 32],
            rate_per_mb: MAX_RATE_PER_MB + 1,
        };
        let msg = ProbeMessage::Response(resp);
        let bytes = postcard::to_allocvec(&msg)?;
        let decoded: Result<ProbeMessage, _> = postcard::from_bytes(&bytes);
        assert!(
            decoded.is_err(),
            "ProbeMessage decode must propagate ProbeResponse validation"
        );
        Ok(())
    }

    #[test]
    fn probe_response_validate_is_consistent_with_decode() {
        let bad = ProbeResponse {
            nonce: 1,
            measured_at_unix_ms: 0,
            node_id: [0u8; 32],
            rate_per_mb: MAX_RATE_PER_MB + 1,
        };
        assert_eq!(
            bad.validate(),
            Err(MessageValidationError::RateTooLarge {
                rate: MAX_RATE_PER_MB + 1,
            })
        );

        let good = ProbeResponse {
            rate_per_mb: MAX_RATE_PER_MB,
            ..bad
        };
        assert_eq!(good.validate(), Ok(()));
    }

    // Wire-format guard: switching to manual `Deserialize` for
    // `ProbeResponse` must not change the on-wire layout. If postcard's
    // bytes for a ProbeResponse change, this fixed-byte assertion catches
    // it before the change ships.
    #[test]
    fn probe_response_wire_format_is_stable() -> Result<(), postcard::Error> {
        let resp = ProbeResponse {
            nonce: 1,
            measured_at_unix_ms: 2,
            node_id: [3u8; 32],
            rate_per_mb: 4,
        };
        let bytes = postcard::to_allocvec(&resp)?;
        // postcard varint encoding: nonce=1 (1 byte), measured_at=2 (1 byte),
        // node_id=32 raw bytes, rate_per_mb=4 (1 byte) → 35 bytes.
        let mut expected = Vec::with_capacity(35);
        expected.push(1u8); // nonce varint
        expected.push(2u8); // measured_at_unix_ms varint
        expected.extend_from_slice(&[3u8; 32]); // node_id
        expected.push(4u8); // rate_per_mb varint
        assert_eq!(bytes, expected);
        Ok(())
    }
}
