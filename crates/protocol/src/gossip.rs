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
    /// discriminant 1 — asserted by `gossip_payload_reputation_report_is_one`
    ReputationReport(ReputationReport),
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
    /// Acceptance is the allowlist in [`crate::region::is_valid_region`]
    /// (assigned codes + the user-reserved ranges); receive-side and
    /// publish-side validators both call it so the wire and config
    /// layers cannot drift.
    pub region: String,
    /// Microseconds since Unix epoch.
    pub timestamp_us: u64,
}

impl NodeAnnounceBody {
    /// Canonical bytes to sign/verify: `postcard::to_allocvec(self)`.
    pub fn signing_bytes(&self) -> Result<Vec<u8>, postcard::Error> {
        postcard::to_allocvec(self)
    }
}

/// Signed `ReputationReport` gossip message (ADR 008 §Gossip Protocol).
///
/// A node broadcasts one of these about a `provider` it has interacted with.
/// `signature` is Ed25519 over `body.signing_bytes()` using the key identified
/// by `body.reporter`. Verification is performed outside this crate (in the
/// `decdn-gossip` crate). Mirrors the [`NodeAnnounce`] signed-split so future
/// unsigned fields can be added to [`ReputationReport`] without invalidating
/// existing signatures.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReputationReport {
    /// Signed body. Its wire layout is frozen per ADR 013.
    pub body: ReputationReportBody,
    /// Ed25519 signature over `postcard::to_allocvec(&body)`. Always
    /// [`SIGNATURE_LEN`] bytes; the verify path rejects other lengths.
    pub signature: Vec<u8>,
}

/// Signed fields of a [`ReputationReport`]. Layout is frozen per ADR 013 —
/// future additions go on [`ReputationReport`] as optional unsigned fields.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReputationReportBody {
    /// The node being rated (its Ed25519 public key / iroh `NodeId`), 32 bytes.
    pub provider: [u8; 32],
    /// The reporting node's Ed25519 public key, 32 bytes. Must equal the key
    /// that produced `signature`.
    pub reporter: [u8; 32],
    /// Observed quality metrics for `provider`.
    pub metrics: ReportMetrics,
    /// Seconds since Unix epoch when the report was generated. Receivers reject
    /// reports outside the `max_report_age + clock_skew` window (ADR 008).
    pub timestamp_secs: u64,
}

/// Quality metrics carried by a [`ReputationReport`] (ADR 008 §Gossip Protocol).
///
/// All fields are optional so a reporter can omit signals it did not observe;
/// a missing or negative signal scores its component as `0.0` at the receiver.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReportMetrics {
    /// Observed delivery rate in bytes/sec, or `None` if not measured.
    pub delivery_speed: Option<u32>,
    /// Whether the provider was reachable, or `None` if not observed.
    pub uptime_observed: Option<bool>,
    /// Whether delivered bytes passed BLAKE3 verification, or `None`.
    pub data_correct: Option<bool>,
}

impl ReputationReportBody {
    /// Canonical bytes to sign/verify: `postcard::to_allocvec(self)`.
    pub fn signing_bytes(&self) -> Result<Vec<u8>, postcard::Error> {
        postcard::to_allocvec(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_body() -> NodeAnnounceBody {
        NodeAnnounceBody {
            node_id: [1u8; 32],
            region: "US".to_string(),
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
        // First byte is the version, second byte is the payload discriminant.
        assert_eq!(bytes.first().copied(), Some(GOSSIP_VERSION));
        assert_eq!(bytes.get(1).copied(), Some(0u8));
        Ok(())
    }

    fn sample_report_body() -> ReputationReportBody {
        ReputationReportBody {
            provider: [2u8; 32],
            reporter: [3u8; 32],
            metrics: ReportMetrics {
                delivery_speed: Some(1_048_576),
                uptime_observed: Some(true),
                data_correct: Some(true),
            },
            timestamp_secs: 1_700_000_000,
        }
    }

    #[test]
    fn reputation_report_body_roundtrip() -> Result<(), postcard::Error> {
        let body = sample_report_body();
        let bytes = postcard::to_allocvec(&body)?;
        let decoded: ReputationReportBody = postcard::from_bytes(&bytes)?;
        assert_eq!(body, decoded);
        Ok(())
    }

    #[test]
    fn gossip_payload_reputation_report_is_one() -> Result<(), postcard::Error> {
        let env = GossipEnvelope {
            version: GOSSIP_VERSION,
            payload: GossipPayload::ReputationReport(ReputationReport {
                body: sample_report_body(),
                signature: vec![0u8; 64],
            }),
        };
        let bytes = postcard::to_allocvec(&env)?;
        // First byte is the version, second byte is the payload discriminant.
        assert_eq!(bytes.first().copied(), Some(GOSSIP_VERSION));
        assert_eq!(bytes.get(1).copied(), Some(1u8));
        Ok(())
    }

    #[test]
    fn reputation_report_signing_bytes_are_postcard_of_body() -> Result<(), postcard::Error> {
        let body = sample_report_body();
        assert_eq!(body.signing_bytes()?, postcard::to_allocvec(&body)?);
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
