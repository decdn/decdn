//! Wire message payload types for deCDN ALPN protocols.
//!
//! This module defines the postcard-serializable request/response payloads used
//! by deCDN protocols. It does not itself define or implement stream framing.
//!
//! ADR 013 specifies varint-length-prefixed framing plus top-level protocol
//! enums for all ALPNs; that framing is not yet implemented. Current handlers
//! read a single message per stream delimited by QUIC FIN. Implementing the
//! ADR 013 framing + enum wrappers is tracked as a follow-up.

use serde::{Deserialize, Serialize};

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
}
