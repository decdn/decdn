//! Pure, no-IO validation of inbound gossip envelopes.
//!
//! Separating validation from the subscriber loop keeps the rules unit-testable
//! and makes the rejection-reason taxonomy explicit. Each `AnnounceReject`
//! variant maps one-to-one to a stable metric label via
//! [`AnnounceReject::label`].

use std::collections::HashSet;

use decdn_protocol::{
    GOSSIP_VERSION, GossipEnvelope, GossipPayload, NodeAnnounce, POPULAR_HASHES_MAX, SIGNATURE_LEN,
};
use thiserror::Error;

/// Every reason an incoming envelope can be rejected. Each variant's
/// [`Self::label`] is stable and suitable as a Prometheus label value.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum AnnounceReject {
    #[error("envelope postcard decode failed")]
    DecodeFailed,
    #[error("unknown envelope version")]
    UnknownVersion,
    /// Reserved for the day [`GossipPayload`] grows a second variant. With
    /// only `NodeAnnounce` defined, postcard decode of an unknown
    /// discriminant surfaces as [`Self::DecodeFailed`] (no reachable path
    /// emits this variant today). Kept in the enum + `label()` so adding a
    /// manual discriminant peek later is a pure code change that doesn't
    /// rename any Prometheus label.
    #[error("unknown gossip payload variant")]
    UnknownVariant,
    #[error("signature length != {SIGNATURE_LEN}")]
    BadSignatureLen,
    #[error("invalid public key")]
    InvalidPublicKey,
    #[error("body postcard encode failed")]
    BodyEncodeFailed,
    #[error("invalid signature")]
    InvalidSignature,
    #[error("clock skew > 60s from receiver")]
    ClockSkew,
    #[error("timestamp not strictly greater than existing entry")]
    StaleTimestamp,
    #[error("region must be 2 ASCII uppercase letters")]
    BadRegion,
    #[error("popular_hashes contains duplicates")]
    DuplicateHashes,
    #[error("popular_hashes exceeds {POPULAR_HASHES_MAX} entries")]
    TooManyHashes,
    #[error("announcer not in allowlist")]
    NotAllowlisted,
}

impl AnnounceReject {
    /// Stable metric label for this rejection reason.
    pub const fn label(&self) -> &'static str {
        match self {
            Self::DecodeFailed => "decode_failed",
            Self::UnknownVersion => "unknown_version",
            Self::UnknownVariant => "unknown_variant",
            Self::BadSignatureLen => "bad_signature_len",
            Self::InvalidPublicKey => "invalid_public_key",
            Self::BodyEncodeFailed => "body_encode_failed",
            Self::InvalidSignature => "invalid_signature",
            Self::ClockSkew => "clock_skew",
            Self::StaleTimestamp => "stale_timestamp",
            Self::BadRegion => "bad_region",
            Self::DuplicateHashes => "duplicate_hashes",
            Self::TooManyHashes => "too_many_hashes",
            Self::NotAllowlisted => "not_allowlisted",
        }
    }
}

/// Maximum tolerated difference between `timestamp_us` and receiver clock
/// (ADR 001 rule 3). 60 seconds in microseconds.
pub const CLOCK_SKEW_TOLERANCE_US: u64 = 60 * 1_000_000;

/// Validate a postcard-encoded [`GossipEnvelope`]. On success, returns the
/// contained [`NodeAnnounce`]. The caller is responsible for threading the
/// result through the peer table, where the final monotonic/stale check
/// (rule 4) is enforced against the latest stored entry.
///
/// `allowlist` is checked only if non-empty: empty allowlist means accept
/// any signature-valid announce. This is the `PoC` stand-in for ADR 001
/// rule 2 (on-chain staking registry check) until the registry contract
/// lands; it is not an implementation of the rule itself.
pub fn validate_envelope<S: std::hash::BuildHasher>(
    bytes: &[u8],
    now_us: u64,
    allowlist: &HashSet<[u8; 32], S>,
) -> Result<NodeAnnounce, AnnounceReject> {
    // ADR 013: trailing bytes are tolerated so future unsigned extensions on
    // the envelope don't break old decoders. Use `take_from_bytes` and drop
    // the remainder rather than `from_bytes`, which errors on trailing input.
    let (env, _rest): (GossipEnvelope, &[u8]) =
        postcard::take_from_bytes(bytes).map_err(|_| AnnounceReject::DecodeFailed)?;

    if env.version != GOSSIP_VERSION {
        return Err(AnnounceReject::UnknownVersion);
    }

    // Only one variant today; future variants will need their own handling.
    #[allow(irrefutable_let_patterns)]
    let GossipPayload::NodeAnnounce(announce) = env.payload else {
        return Err(AnnounceReject::UnknownVariant);
    };

    validate_announce_fields(&announce, now_us)?;
    verify_signature(&announce)?;

    if !allowlist.is_empty() && !allowlist.contains(&announce.body.node_id) {
        return Err(AnnounceReject::NotAllowlisted);
    }

    Ok(announce)
}

fn validate_announce_fields(a: &NodeAnnounce, now_us: u64) -> Result<(), AnnounceReject> {
    let b = &a.body;

    // Region: exactly 2 ASCII uppercase letters (ISO 3166-1 alpha-2).
    if b.region.len() != 2 || !b.region.bytes().all(|c| c.is_ascii_uppercase()) {
        return Err(AnnounceReject::BadRegion);
    }

    if b.popular_hashes.len() > POPULAR_HASHES_MAX {
        return Err(AnnounceReject::TooManyHashes);
    }
    let unique: HashSet<&[u8; 32]> = b.popular_hashes.iter().collect();
    if unique.len() != b.popular_hashes.len() {
        return Err(AnnounceReject::DuplicateHashes);
    }

    // Clock skew: |now - ts| <= 60s. Use signed subtraction in i128 to dodge
    // wrap-around when ts is far in the future or the past.
    let diff = i128::from(now_us) - i128::from(b.timestamp_us);
    let abs_diff = u64::try_from(diff.unsigned_abs()).unwrap_or(u64::MAX);
    if abs_diff > CLOCK_SKEW_TOLERANCE_US {
        return Err(AnnounceReject::ClockSkew);
    }

    Ok(())
}

fn verify_signature(a: &NodeAnnounce) -> Result<(), AnnounceReject> {
    let sig_bytes: &[u8; SIGNATURE_LEN] = a
        .signature
        .as_slice()
        .try_into()
        .map_err(|_| AnnounceReject::BadSignatureLen)?;
    let pk = iroh::PublicKey::from_bytes(&a.body.node_id)
        .map_err(|_| AnnounceReject::InvalidPublicKey)?;
    let signing_bytes = a
        .body
        .signing_bytes()
        .map_err(|_| AnnounceReject::BodyEncodeFailed)?;
    let sig = iroh::Signature::from_bytes(sig_bytes);
    pk.verify(&signing_bytes, &sig)
        .map_err(|_| AnnounceReject::InvalidSignature)
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;
    use decdn_protocol::{
        GOSSIP_VERSION, GossipEnvelope, GossipPayload, LoadHint, NodeAnnounce, NodeAnnounceBody,
    };
    use iroh::SecretKey;

    /// Freeze the Prometheus label strings: a rename without updating this
    /// test would break operator dashboards silently. Every variant must be
    /// listed here; a new variant that forgets its label will fail to
    /// compile thanks to the exhaustive match.
    #[test]
    fn reject_labels_are_stable() {
        fn assert_label(r: &AnnounceReject, expected: &'static str) {
            assert_eq!(r.label(), expected, "label changed for {r:?}");
        }
        assert_label(&AnnounceReject::DecodeFailed, "decode_failed");
        assert_label(&AnnounceReject::UnknownVersion, "unknown_version");
        assert_label(&AnnounceReject::UnknownVariant, "unknown_variant");
        assert_label(&AnnounceReject::BadSignatureLen, "bad_signature_len");
        assert_label(&AnnounceReject::InvalidPublicKey, "invalid_public_key");
        assert_label(&AnnounceReject::BodyEncodeFailed, "body_encode_failed");
        assert_label(&AnnounceReject::InvalidSignature, "invalid_signature");
        assert_label(&AnnounceReject::ClockSkew, "clock_skew");
        assert_label(&AnnounceReject::StaleTimestamp, "stale_timestamp");
        assert_label(&AnnounceReject::BadRegion, "bad_region");
        assert_label(&AnnounceReject::DuplicateHashes, "duplicate_hashes");
        assert_label(&AnnounceReject::TooManyHashes, "too_many_hashes");
        assert_label(&AnnounceReject::NotAllowlisted, "not_allowlisted");
    }

    fn sample_body(sk: &SecretKey, ts_us: u64) -> NodeAnnounceBody {
        NodeAnnounceBody {
            node_id: *sk.public().as_bytes(),
            region: "US".to_string(),
            load: LoadHint {
                active_streams: 0,
                bandwidth_utilization: 0,
            },
            popular_hashes: vec![],
            timestamp_us: ts_us,
        }
    }

    fn sign(sk: &SecretKey, body: &NodeAnnounceBody) -> Vec<u8> {
        let bytes = body.signing_bytes().expect("body encode");
        sk.sign(&bytes).to_bytes().to_vec()
    }

    fn encode(env: &GossipEnvelope) -> Vec<u8> {
        postcard::to_allocvec(env).expect("envelope encode")
    }

    fn mk_envelope(sk: &SecretKey, tweak: impl FnOnce(&mut NodeAnnounceBody)) -> Vec<u8> {
        let mut body = sample_body(sk, 1_700_000_000_000_000);
        tweak(&mut body);
        let signature = sign(sk, &body);
        encode(&GossipEnvelope {
            version: GOSSIP_VERSION,
            payload: GossipPayload::NodeAnnounce(NodeAnnounce { body, signature }),
        })
    }

    fn no_list() -> HashSet<[u8; 32]> {
        HashSet::new()
    }

    #[test]
    fn happy_path() {
        let sk = SecretKey::generate(&mut rand::rng());
        let bytes = mk_envelope(&sk, |_| {});
        let a = validate_envelope(&bytes, 1_700_000_000_000_000, &no_list()).expect("valid");
        assert_eq!(a.body.node_id, *sk.public().as_bytes());
    }

    #[test]
    fn unknown_version_rejected() {
        let sk = SecretKey::generate(&mut rand::rng());
        let body = sample_body(&sk, 1_700_000_000_000_000);
        let signature = sign(&sk, &body);
        let bytes = encode(&GossipEnvelope {
            version: 99,
            payload: GossipPayload::NodeAnnounce(NodeAnnounce { body, signature }),
        });
        assert_eq!(
            validate_envelope(&bytes, 1_700_000_000_000_000, &no_list()),
            Err(AnnounceReject::UnknownVersion)
        );
    }

    #[test]
    fn bad_signature_rejected() {
        let sk = SecretKey::generate(&mut rand::rng());
        let body = sample_body(&sk, 1_700_000_000_000_000);
        let bytes = encode(&GossipEnvelope {
            version: GOSSIP_VERSION,
            payload: GossipPayload::NodeAnnounce(NodeAnnounce {
                body,
                signature: vec![7u8; SIGNATURE_LEN],
            }),
        });
        assert_eq!(
            validate_envelope(&bytes, 1_700_000_000_000_000, &no_list()),
            Err(AnnounceReject::InvalidSignature)
        );
    }

    #[test]
    fn bad_signature_length_rejected() {
        let sk = SecretKey::generate(&mut rand::rng());
        let body = sample_body(&sk, 1_700_000_000_000_000);
        let bytes = encode(&GossipEnvelope {
            version: GOSSIP_VERSION,
            payload: GossipPayload::NodeAnnounce(NodeAnnounce {
                body,
                signature: vec![0u8; 63],
            }),
        });
        assert_eq!(
            validate_envelope(&bytes, 1_700_000_000_000_000, &no_list()),
            Err(AnnounceReject::BadSignatureLen)
        );
    }

    #[test]
    fn clock_skew_future_rejected() {
        let sk = SecretKey::generate(&mut rand::rng());
        let bytes = mk_envelope(&sk, |b| {
            b.timestamp_us = 2_000_000_000_000_000;
        });
        // now 61s earlier than ts
        let now = 2_000_000_000_000_000 - (61 * 1_000_000);
        assert_eq!(
            validate_envelope(&bytes, now, &no_list()),
            Err(AnnounceReject::ClockSkew)
        );
    }

    #[test]
    fn clock_skew_past_rejected() {
        let sk = SecretKey::generate(&mut rand::rng());
        let bytes = mk_envelope(&sk, |b| {
            b.timestamp_us = 2_000_000_000_000_000;
        });
        let now = 2_000_000_000_000_000 + (61 * 1_000_000);
        assert_eq!(
            validate_envelope(&bytes, now, &no_list()),
            Err(AnnounceReject::ClockSkew)
        );
    }

    #[test]
    fn bad_region_rejected() {
        let sk = SecretKey::generate(&mut rand::rng());
        for bad in ["us", "USA", "", "U1", "U"] {
            let bytes = mk_envelope(&sk, |b| b.region = bad.to_string());
            assert_eq!(
                validate_envelope(&bytes, 1_700_000_000_000_000, &no_list()),
                Err(AnnounceReject::BadRegion),
                "region {bad:?} should be rejected"
            );
        }
    }

    #[test]
    fn duplicate_hashes_rejected() {
        let sk = SecretKey::generate(&mut rand::rng());
        let bytes = mk_envelope(&sk, |b| b.popular_hashes = vec![[1u8; 32], [1u8; 32]]);
        assert_eq!(
            validate_envelope(&bytes, 1_700_000_000_000_000, &no_list()),
            Err(AnnounceReject::DuplicateHashes)
        );
    }

    #[test]
    fn too_many_hashes_rejected() {
        let sk = SecretKey::generate(&mut rand::rng());
        let bytes = mk_envelope(&sk, |b| {
            b.popular_hashes = (0..=POPULAR_HASHES_MAX)
                .map(|i| {
                    let mut h = [0u8; 32];
                    h[0] = u8::try_from(i & 0xff).unwrap_or(0);
                    h
                })
                .collect();
        });
        assert_eq!(
            validate_envelope(&bytes, 1_700_000_000_000_000, &no_list()),
            Err(AnnounceReject::TooManyHashes)
        );
    }

    #[test]
    fn allowlist_enforced_when_non_empty() {
        let sk = SecretKey::generate(&mut rand::rng());
        let bytes = mk_envelope(&sk, |_| {});
        let mut allow = HashSet::new();
        allow.insert([42u8; 32]);
        assert_eq!(
            validate_envelope(&bytes, 1_700_000_000_000_000, &allow),
            Err(AnnounceReject::NotAllowlisted)
        );
        allow.insert(*sk.public().as_bytes());
        assert!(validate_envelope(&bytes, 1_700_000_000_000_000, &allow).is_ok());
    }

    #[test]
    fn garbage_bytes_rejected() {
        let allow = no_list();
        assert_eq!(
            validate_envelope(&[0xFFu8, 0xFF], 1_700_000_000_000_000, &allow),
            Err(AnnounceReject::DecodeFailed)
        );
    }
}
