//! Pure, no-IO validation of inbound gossip envelopes.
//!
//! Separating validation from the subscriber loop keeps the rules unit-testable
//! and makes the rejection-reason taxonomy explicit. Each `AnnounceReject`
//! variant maps one-to-one to a stable metric label via
//! [`AnnounceReject::label`].

use std::collections::HashSet;

use decdn_protocol::{
    GOSSIP_VERSION, GossipEnvelope, GossipPayload, NodeAnnounce, SIGNATURE_LEN, is_valid_region,
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
    #[error("region must be an ISO 3166-1 alpha-2 code (assigned or user-reserved)")]
    BadRegion,
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
    // Check the version byte *before* deserializing the payload. Postcard
    // encodes a u8 as a single byte, so the first byte is always the
    // envelope version. This ensures unknown versions produce
    // `UnknownVersion` (silent drop per ADR 013) rather than `DecodeFailed`
    // when a future v2 payload schema is incompatible with our types.
    let version = bytes.first().ok_or(AnnounceReject::DecodeFailed)?;
    if *version != GOSSIP_VERSION {
        return Err(AnnounceReject::UnknownVersion);
    }

    // ADR 013: trailing bytes are tolerated so future unsigned extensions on
    // the envelope don't break old decoders. Use `take_from_bytes` and drop
    // the remainder rather than `from_bytes`, which errors on trailing input.
    let (env, _rest): (GossipEnvelope, &[u8]) =
        postcard::take_from_bytes(bytes).map_err(|_| AnnounceReject::DecodeFailed)?;

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

    // Region: must be in the ISO 3166-1 alpha-2 allowlist (assigned codes
    // + the user-reserved ranges). The strict allowlist closes the topic-name
    // injection surface (`/`, `\0`, non-ASCII) and also rejects unassigned
    // codes like `XX` or `OO` that the bare "2 ASCII uppercase" check let
    // through before.
    if !is_valid_region(&b.region) {
        return Err(AnnounceReject::BadRegion);
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

    fn fresh_key() -> SecretKey {
        SecretKey::generate()
    }

    /// Freeze the Prometheus label strings: a rename without updating this
    /// test would break operator dashboards silently. The exhaustive `match`
    /// inside `expected_label` is the compile-time guard — adding a new
    /// `AnnounceReject` variant without a label mapping here fails to
    /// compile, so the contract cannot silently drift.
    #[test]
    fn reject_labels_are_stable() {
        const fn expected_label(r: &AnnounceReject) -> &'static str {
            match r {
                AnnounceReject::DecodeFailed => "decode_failed",
                AnnounceReject::UnknownVersion => "unknown_version",
                AnnounceReject::UnknownVariant => "unknown_variant",
                AnnounceReject::BadSignatureLen => "bad_signature_len",
                AnnounceReject::InvalidPublicKey => "invalid_public_key",
                AnnounceReject::BodyEncodeFailed => "body_encode_failed",
                AnnounceReject::InvalidSignature => "invalid_signature",
                AnnounceReject::ClockSkew => "clock_skew",
                AnnounceReject::StaleTimestamp => "stale_timestamp",
                AnnounceReject::BadRegion => "bad_region",
                AnnounceReject::NotAllowlisted => "not_allowlisted",
            }
        }
        // One representative value per variant; the match above is the real
        // contract, this just forces it to be exercised.
        for r in [
            AnnounceReject::DecodeFailed,
            AnnounceReject::UnknownVersion,
            AnnounceReject::UnknownVariant,
            AnnounceReject::BadSignatureLen,
            AnnounceReject::InvalidPublicKey,
            AnnounceReject::BodyEncodeFailed,
            AnnounceReject::InvalidSignature,
            AnnounceReject::ClockSkew,
            AnnounceReject::StaleTimestamp,
            AnnounceReject::BadRegion,
            AnnounceReject::NotAllowlisted,
        ] {
            assert_eq!(r.label(), expected_label(&r), "label drift for {r:?}");
        }

        // Free-form labels passed directly to `inc_rejected` aren't covered
        // by `AnnounceReject::label`; keep them pinned here too.
        assert_eq!(crate::service::SUBSCRIBE_FAILED_LABEL, "subscribe_failed");
    }

    fn sample_body(sk: &SecretKey, ts_us: u64) -> NodeAnnounceBody {
        NodeAnnounceBody {
            node_id: *sk.public().as_bytes(),
            region: "US".to_string(),
            load: LoadHint {
                active_streams: 0,
                bandwidth_utilization: 0,
            },
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
        let sk = fresh_key();
        let bytes = mk_envelope(&sk, |_| {});
        let a = validate_envelope(&bytes, 1_700_000_000_000_000, &no_list()).expect("valid");
        assert_eq!(a.body.node_id, *sk.public().as_bytes());
    }

    #[test]
    fn unknown_version_rejected() {
        let sk = fresh_key();
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
        let sk = fresh_key();
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
        let sk = fresh_key();
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
        let sk = fresh_key();
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
        let sk = fresh_key();
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
        let sk = fresh_key();
        // Length / case malformations, unassigned codes, and the
        // security-critical adversarial chars (slash, NUL, newline,
        // non-ASCII) that would otherwise reach the topic-name builder.
        let bad_inputs = [
            "us", "USA", "", "U1", "U", "OO", "JJ", "BX", "U/", "/U", "U\0", "U\n", "Ü1",
        ];
        for bad in bad_inputs {
            let bytes = mk_envelope(&sk, |b| b.region = bad.to_string());
            assert_eq!(
                validate_envelope(&bytes, 1_700_000_000_000_000, &no_list()),
                Err(AnnounceReject::BadRegion),
                "region {bad:?} should be rejected"
            );
        }
    }

    #[test]
    fn reserved_region_accepted() {
        // Reserved-for-user-assignment ranges (AA, QM–QZ, XA–XZ, ZZ) are
        // accepted so air-gapped / testnet operators can pick a private
        // code. Spot-check each range boundary.
        let sk = fresh_key();
        for good in ["AA", "QM", "QZ", "XA", "XK", "XZ", "ZZ"] {
            let bytes = mk_envelope(&sk, |b| b.region = good.to_string());
            assert!(
                validate_envelope(&bytes, 1_700_000_000_000_000, &no_list()).is_ok(),
                "region {good:?} should be accepted"
            );
        }
    }

    #[test]
    fn allowlist_enforced_when_non_empty() {
        let sk = fresh_key();
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
        // First byte 0xFF != GOSSIP_VERSION, so the version-first check
        // rejects before attempting deserialization.
        assert_eq!(
            validate_envelope(&[0xFFu8, 0xFF], 1_700_000_000_000_000, &allow),
            Err(AnnounceReject::UnknownVersion)
        );
        // Empty input has no version byte at all → DecodeFailed.
        assert_eq!(
            validate_envelope(&[], 1_700_000_000_000_000, &allow),
            Err(AnnounceReject::DecodeFailed)
        );
    }

    // Closes #281. Existing tests cover the "random signature bytes" path
    // (`bad_signature_rejected`) and the length check
    // (`bad_signature_length_rejected`). The remaining security-critical
    // paths — "wrong key signed this" (impersonation) and "body mutated
    // after signing" (wire tampering) — go here.
    #[test]
    fn signature_from_different_key_rejected() {
        // Impersonation attempt: announce claims to be key A, signature
        // was actually produced by key B. Ed25519 verification keys off
        // the `body.node_id` bytes as the public key, so B's signature
        // cannot verify against A's public key. `bad_signature_rejected`
        // covers junk-bytes-as-signature; this one is the adversarial
        // case where the attacker has a valid keypair but announces
        // under someone else's identity.
        let sk_a = fresh_key();
        let sk_b = fresh_key();

        let body = sample_body(&sk_a, 1_700_000_000_000_000); // claims A
        let signature = sign(&sk_b, &body); // signed by B
        let bytes = encode(&GossipEnvelope {
            version: GOSSIP_VERSION,
            payload: GossipPayload::NodeAnnounce(NodeAnnounce { body, signature }),
        });

        assert_eq!(
            validate_envelope(&bytes, 1_700_000_000_000_000, &no_list()),
            Err(AnnounceReject::InvalidSignature)
        );
    }

    #[test]
    fn signature_does_not_verify_after_body_mutation() {
        // A relay / on-path attacker alters a field after a legitimate
        // signature was produced. ADR 001 requires per-field signing: any
        // edit to `region`, `load`, `timestamp_us`, or `node_id` must
        // invalidate the signature. Mutating `region`
        // stands in for the whole class — postcard's canonical encoding
        // means any body-byte difference changes the signed bytes.
        let sk = fresh_key();
        let original = sample_body(&sk, 1_700_000_000_000_000);
        let signature = sign(&sk, &original); // sign the pre-mutation bytes

        let mut mutated = original.clone();
        mutated.region = "DE".to_string(); // was "US"

        let bytes = encode(&GossipEnvelope {
            version: GOSSIP_VERSION,
            payload: GossipPayload::NodeAnnounce(NodeAnnounce {
                body: mutated,
                signature, // stale — matches the pre-mutation body
            }),
        });

        assert_eq!(
            validate_envelope(&bytes, 1_700_000_000_000_000, &no_list()),
            Err(AnnounceReject::InvalidSignature)
        );
    }
}
