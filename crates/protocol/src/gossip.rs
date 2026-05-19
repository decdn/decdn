//! Gossip message types for deCDN.
//!
//! Every gossip message on every topic is wrapped in a [`GossipEnvelope`],
//! which carries a version byte and a payload enum. The envelope is serialized
//! as the outermost postcard value inside a length-prefixed frame (see
//! [`crate::framing`]). Unknown envelope versions and unknown [`GossipPayload`]
//! variants MUST be silently dropped by receivers per ADR 013.
//!
//! # Signed-field freezing (ADR 013)
//!
//! [`NodeAnnounce`] is split into a signed [`NodeAnnounceBody`] plus an outer
//! `signature`. The signing input is `postcard::to_allocvec(&body)` — use
//! [`NodeAnnounceBody::signing_bytes`]. Keeping the split explicit lets future
//! unsigned fields be added via `Option<T>` / `#[serde(default)]` on
//! `NodeAnnounce` without invalidating existing signatures.
//!
//! This crate deliberately does not depend on any crypto library. The caller
//! (the `decdn-gossip` crate, which does depend on iroh) is responsible for
//! computing and verifying the Ed25519 signature over `signing_bytes()`.

use serde::{Deserialize, Serialize};

/// Current `GossipEnvelope::version` emitted by this protocol version.
pub const GOSSIP_VERSION: u8 = 1;

/// Length in bytes of an Ed25519 signature.
pub const SIGNATURE_LEN: usize = 64;

/// Outer envelope for every gossip message.
///
/// Unknown `version` values MUST be silently dropped by receivers (ADR 013).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GossipEnvelope {
    /// Envelope schema version. Currently [`GOSSIP_VERSION`] == 1.
    pub version: u8,
    /// The wrapped gossip payload.
    pub payload: GossipPayload,
}

/// Top-level gossip payload enum. Variant order is frozen per ADR 013 — new
/// variants MUST be appended at the end.
///
/// ⚠️ **VARIANT ORDER FROZEN — ADR 013 §Protocol Enums**
/// Postcard encodes each variant as its declaration-order index. Reordering,
/// inserting, or removing a variant is a wire-breaking change. Discriminants
/// are pinned by the tests in this module.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum GossipPayload {
    /// discriminant 0 — asserted by `gossip_payload_node_announce_is_zero`
    NodeAnnounce(NodeAnnounce),
    // Reserved for later steps (append only):
    //   ReputationReport(ReputationReport)   // discriminant 1
}

/// Signed `NodeAnnounce` gossip message (ADR 001).
///
/// `signature` is Ed25519 over `body.signing_bytes()` using the key identified
/// by `body.node_id`. Verification is performed outside this crate.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeAnnounce {
    /// Signed body. Its wire layout is frozen per ADR 013.
    pub body: NodeAnnounceBody,
    /// Ed25519 signature over `postcard::to_allocvec(&body)`. Always
    /// [`SIGNATURE_LEN`] bytes; the verify path rejects other lengths.
    pub signature: Vec<u8>,
}

/// Signed fields of a [`NodeAnnounce`]. Layout is frozen per ADR 013 — future
/// additions go on [`NodeAnnounce`] as optional unsigned fields, not here.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeAnnounceBody {
    /// Announcing node's Ed25519 public key (iroh `NodeId`), 32 bytes.
    pub node_id: [u8; 32],
    /// ISO 3166-1 alpha-2 region code, 2 ASCII uppercase letters.
    pub region: String,
    /// Approximate current utilization.
    pub load: LoadHint,
    /// Microseconds since Unix epoch.
    pub timestamp_us: u64,
}

impl NodeAnnounceBody {
    /// Canonical bytes to sign/verify: `postcard::to_allocvec(self)`.
    pub fn signing_bytes(&self) -> Result<Vec<u8>, postcard::Error> {
        postcard::to_allocvec(self)
    }
}

/// Coarse load hint carried in every [`NodeAnnounce`] (ADR 001).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct LoadHint {
    /// Current concurrent delivery streams.
    pub active_streams: u32,
    /// 0..=100 percentage of self-reported capacity.
    pub bandwidth_utilization: u8,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_body() -> NodeAnnounceBody {
        NodeAnnounceBody {
            node_id: [1u8; 32],
            region: "US".to_string(),
            load: LoadHint {
                active_streams: 3,
                bandwidth_utilization: 42,
            },
            timestamp_us: 1_700_000_000_000_000,
        }
    }

    #[test]
    fn node_announce_body_roundtrip() -> Result<(), postcard::Error> {
        let body = sample_body();
        let bytes = postcard::to_allocvec(&body)?;
        let decoded: NodeAnnounceBody = postcard::from_bytes(&bytes)?;
        assert_eq!(body, decoded);
        Ok(())
    }

    #[test]
    fn envelope_roundtrip() -> Result<(), postcard::Error> {
        let env = GossipEnvelope {
            version: GOSSIP_VERSION,
            payload: GossipPayload::NodeAnnounce(NodeAnnounce {
                body: sample_body(),
                signature: vec![7u8; 64],
            }),
        };
        let bytes = postcard::to_allocvec(&env)?;
        let decoded: GossipEnvelope = postcard::from_bytes(&bytes)?;
        assert_eq!(env, decoded);
        Ok(())
    }

    #[test]
    fn gossip_payload_node_announce_is_zero() -> Result<(), postcard::Error> {
        let env = GossipEnvelope {
            version: GOSSIP_VERSION,
            payload: GossipPayload::NodeAnnounce(NodeAnnounce {
                body: sample_body(),
                signature: vec![0u8; 64],
            }),
        };
        let bytes = postcard::to_allocvec(&env)?;
        // First byte is the version (1), second byte is the payload discriminant.
        assert_eq!(bytes.first().copied(), Some(GOSSIP_VERSION));
        assert_eq!(bytes.get(1).copied(), Some(0u8));
        Ok(())
    }

    #[test]
    fn signing_bytes_are_postcard_of_body() -> Result<(), postcard::Error> {
        let body = sample_body();
        let via_helper = body.signing_bytes()?;
        let via_postcard = postcard::to_allocvec(&body)?;
        assert_eq!(via_helper, via_postcard);
        Ok(())
    }

    #[test]
    fn envelope_trailing_bytes_tolerated() -> Result<(), postcard::Error> {
        // ADR 013: `take_from_bytes` silently ignores trailing bytes so future
        // unknown unsigned fields don't break old decoders.
        let env = GossipEnvelope {
            version: GOSSIP_VERSION,
            payload: GossipPayload::NodeAnnounce(NodeAnnounce {
                body: sample_body(),
                signature: vec![0u8; 64],
            }),
        };
        let mut bytes = postcard::to_allocvec(&env)?;
        bytes.extend_from_slice(&[0xAAu8, 0xBB, 0xCC]);
        let (decoded, rest) = postcard::take_from_bytes::<GossipEnvelope>(&bytes)?;
        assert_eq!(decoded, env);
        assert_eq!(rest, &[0xAAu8, 0xBB, 0xCC]);
        Ok(())
    }
}
