//! Wire message payload types for deCDN ALPN protocols.
//!
//! Every ALPN defines a top-level enum (e.g. [`ProbeMessage`]) whose variants
//! wrap the per-message structs. The enum is serialized as the outermost
//! postcard value inside a length-prefixed frame (see [`crate::framing`]).
//!
//! Signed-field freezing (ADR 013 §Signed Field Freezing): `ProbeResponse` is
//! currently unsigned; when it gains a signature, its signed fields will be
//! split into a frozen `ProbeResponseBody` per ADR 013.

use serde::{Deserialize, Serialize};

/// Top-level protocol enum for `cdn/probe/v1`. Variant order is frozen per
/// ADR 013 — new variants MUST be appended at the end.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ProbeMessage {
    /// discriminant 0
    Request(ProbeRequest),
    /// discriminant 1
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
    /// not normalize units across tokens.
    pub rate_per_mb: u64,
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
}
