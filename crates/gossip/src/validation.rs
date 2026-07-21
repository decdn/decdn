//! Pure, no-IO validation of inbound gossip envelopes.
//!
//! Separating validation from the subscriber loop keeps the rules unit-testable
//! and makes the rejection-reason taxonomy explicit. Each `AnnounceReject`
//! variant maps one-to-one to a stable metric label via
//! [`AnnounceReject::label`].

use std::sync::Arc;

use decdn_protocol::{
    GOSSIP_VERSION, GossipEnvelope, GossipPayload, NodeAnnounce, SIGNATURE_LEN, is_valid_region,
};
use thiserror::Error;

use crate::reputation::StakedNodeSet;

/// ADR 001 rule-2 admission gate for inbound `NodeAnnounce`, naming its two
/// safety-opposite states so neither is the silent default: [`Self::Enforce`]
/// gates on staked membership, `Disabled` (test-only) fails open. The generic `S` is
/// the membership-set handle — `&dyn StakedNodeSet` on the borrowed
/// [`validate_envelope`] path, `Arc<dyn StakedNodeSet>` on the owned spawn path
/// (see [`OwnedAnnounceGate`]).
///
/// **`AnnounceGate::Disabled` fails OPEN.** The inverse pole still exists —
/// `ReputationWiring::Disabled` is fail-CLOSED — so the polarity
/// inversion #1338 warned about has not gone away; always check which type you
/// are holding. What #1342 removed is the *shape* collision: the fail-closed
/// side used to be a second two-variant `*Gate` enum over this same
/// [`StakedNodeSet`] seam, so the two could be swapped at a call site by
/// mistaking one for the other. It is now a differently-shaped wiring enum with
/// a different name, which is what makes them hard to confuse — not the absence
/// of an inverted twin.
// `Copy` is conditional: it applies only where `S: Copy`, i.e. the borrowed
// `AnnounceGate<&dyn StakedNodeSet>`. The owned `OwnedAnnounceGate` (an `Arc`)
// is `Clone`-only, so `gate.clone()` at each subscriber is a refcount bump, not
// an `Arc` copy — do not read `Copy` as copying the set.
#[derive(Clone, Copy)]
pub enum AnnounceGate<S> {
    /// Enforce rule 2: accept an announce only when its author `node_id` is a
    /// currently-staked node in this set. Note the empty-set semantics — an
    /// `Enforce` set with no members rejects every announce, which is correct:
    /// an empty active registry has no staked peers to learn.
    Enforce(S),
    /// FAIL-OPEN: skip the staked-membership check *only*. Every other
    /// [`validate_envelope`] rule still applies — **signature verification
    /// included**, since it runs before this gate is consulted — as do the
    /// version byte, trailing bytes, payload variant, region allowlist and
    /// clock skew.
    ///
    /// **`#[cfg(test)]`: this variant does not exist in a production build.**
    /// It previously relied on a doc comment plus a unit test on
    /// `announce_staked_gate` to stay out of the runtime — but nothing stopped a
    /// future edit passing `Disabled` straight to
    /// [`crate::GossipService::spawn`], which would reopen the hole #1170 closed
    /// with every existing test still green (`announce_staked_gate` would keep
    /// returning `Enforce`; it would just have no callers). Gating the variant
    /// moves that invariant from prose to the compiler. Note the asymmetry with
    /// `ReputationWiring::Disabled`, which is *not* gated: that one fails
    /// CLOSED, so it is a legitimate runtime choice.
    #[cfg(test)]
    Disabled,
}

impl<S> std::fmt::Debug for AnnounceGate<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // `S` (a `StakedNodeSet` handle) isn't `Debug`; report the variant only,
        // which is all that identifies the gate's safety state.
        match self {
            Self::Enforce(_) => f.write_str("AnnounceGate::Enforce(..)"),
            #[cfg(test)]
            Self::Disabled => f.write_str("AnnounceGate::Disabled"),
        }
    }
}

/// Owned announce gate handed to [`crate::GossipService::spawn`] and threaded
/// into each subscriber task; borrowed as an [`AnnounceGate<&dyn StakedNodeSet>`]
/// for [`validate_envelope`] via [`Self::as_gate`].
pub type OwnedAnnounceGate = AnnounceGate<Arc<dyn StakedNodeSet>>;

impl OwnedAnnounceGate {
    /// Borrow-project this owned gate into the borrowed form
    /// [`validate_envelope`] takes, preserving the variant.
    pub fn as_gate(&self) -> AnnounceGate<&dyn StakedNodeSet> {
        match self {
            AnnounceGate::Enforce(s) => AnnounceGate::Enforce(s.as_ref()),
            #[cfg(test)]
            AnnounceGate::Disabled => AnnounceGate::Disabled,
        }
    }
}

/// Every reason an incoming envelope can be rejected. Each variant's
/// [`Self::label`] is stable and suitable as a Prometheus label value.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum AnnounceReject {
    #[error("envelope postcard decode failed")]
    DecodeFailed,
    #[error("unknown envelope version")]
    UnknownVersion,
    /// The envelope decoded to a [`GossipPayload`] variant other than
    /// `NodeAnnounce` (e.g. a `ReputationReport`, which has its own
    /// [`crate::reputation::validate_reputation_envelope`] path). Reached now
    /// that the enum has more than one variant.
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
    #[error("announcer is not a currently-staked node")]
    NotStaked,
    /// Trailing bytes after the postcard envelope exceed
    /// [`MAX_TRAILING_BYTES`] (4 KiB, #577 M3). ADR 013 §Tier 1
    /// permits trailing bytes for forward-compat; this defense-in-
    /// depth cap catches shape violations that slip under iroh-
    /// gossip's per-frame [`GOSSIP_MAX_FRAME`] allocation ceiling.
    #[error("trailing bytes after envelope exceed {MAX_TRAILING_BYTES} byte allowance")]
    OversizeTrailingBytes,
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
            Self::NotStaked => "not_staked",
            Self::OversizeTrailingBytes => "oversize_trailing_bytes",
        }
    }
}

/// Maximum tolerated difference between `timestamp_us` and receiver clock
/// (ADR 001 rule 3). 60 seconds in microseconds.
pub const CLOCK_SKEW_TOLERANCE_US: u64 = 60 * 1_000_000;

/// Maximum trailing bytes tolerated after the postcard envelope in
/// [`validate_envelope`] (#577 M3). ADR 013 §Tier 1 documents trailing
/// bytes as the forward-compat extension mechanism; a sane envelope
/// today is ~150 bytes and any plausible future Tier-1 extension is
/// expected to be well under 4 KiB. Defense-in-depth behind iroh-
/// gossip's per-actor [`GOSSIP_MAX_FRAME`] ceiling (enforced before
/// allocation in `read_lp`, ADR 013 §Gossip Framing): even under that
/// ceiling, an attacker padding every ~150-byte announce up to
/// `GOSSIP_MAX_FRAME` blows each inbound allocation ~100× larger,
/// and Plumtree fan-out repeats that cost at every eager peer the
/// message reaches. Rejecting at the validation layer caps the per-
/// frame size and surfaces the attack via the
/// `oversize_trailing_bytes` metric, while leaving legitimate Tier-1
/// extensions room to grow.
pub const MAX_TRAILING_BYTES: usize = 4 * 1024;

/// Maximum size in bytes of a single iroh-gossip wire frame on the
/// deCDN gossip actor (ADR 013 §Gossip Framing). Passed to
/// `iroh_gossip::net::Gossip::builder().max_message_size(...)` and
/// applies symmetrically to every frame the actor sends or receives:
/// `NodeAnnounce` traffic on `cdn/global/v1` and the region topic,
/// plus iroh-gossip's `HyParView` control frames (which iroh-gossip
/// maintains per-topic alongside the Plumtree data traffic, so
/// tightening this constant breaks legitimate non-payload frames
/// too). The cap bounds allocation inside iroh-gossip's
/// `read_lp` *before* the frame ever reaches [`validate_envelope`].
/// Must accommodate (a) a maximally-extended
/// [`decdn_protocol::GossipEnvelope`] — envelope (~256 B) +
/// [`MAX_TRAILING_BYTES`] padding — plus plumtree/topic message-wrapper
/// overhead (~64 B), and (b) `HyParView` control frames carrying peer-
/// info lists (analytical upper bound from iroh-gossip 0.98 source:
/// Shuffle/ShuffleReply at default fanout = ~1.7 KiB for 7 `PeerInfo`
/// entries with two-relay `AddrInfo`). 16 KiB gives ~3.7× headroom
/// over the ~4.4 KiB data-frame floor.
///
/// **Network-coordination invariant:** this value must agree across
/// all deCDN nodes on the network. iroh-gossip enforces the cap on
/// both send and receive; tightening it asymmetrically silently
/// partitions the gossip swarm for legitimate `HyParView` control
/// frames. Treat changes as wire-compatibility events.
pub const GOSSIP_MAX_FRAME: usize = 16 * 1024;

/// Compile-time invariant: the validation-layer trailing-bytes cap
/// must fit inside a single iroh-gossip frame with room for the
/// envelope itself and plumtree/topic wrappers. The numeric padding
/// reflects: ~256 B envelope (`NodeAnnounce` body + 64 B signature +
/// version byte + postcard overhead) + ~64 B plumtree/topic message
/// wrappers (variant tags, `MessageId`, `DeliveryScope`/`Round`).
const _: () = assert!(
    MAX_TRAILING_BYTES + 256 + 64 <= GOSSIP_MAX_FRAME,
    "GOSSIP_MAX_FRAME must fit a maximally-padded GossipEnvelope plus plumtree/topic wrappers"
);

/// Validate a postcard-encoded [`GossipEnvelope`]. On success, returns the
/// contained [`NodeAnnounce`]. The caller is responsible for threading the
/// result through the peer table, where the final monotonic/stale check
/// (rule 4) is enforced against the latest stored entry.
///
/// `gate` enforces ADR 001 rule 2: [`AnnounceGate::Enforce`] accepts an announce
/// only when its author `node_id` is a currently-staked node in the on-chain
/// registry (queried through the live [`StakedNodeSet`] cache, kept fresh by the
/// registry event tail). The test-only `AnnounceGate::Disabled` skips *that
/// check alone* — every other rule below still applies, signature verification
/// included (it runs before the gate is consulted). It is `#[cfg(test)]`, so the
/// runtime cannot pass it. See [`AnnounceGate::Enforce`] for the empty-set
/// semantics.
pub fn validate_envelope(
    bytes: &[u8],
    now_us: u64,
    gate: AnnounceGate<&dyn StakedNodeSet>,
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
    // the envelope don't break old decoders. Use `take_from_bytes` and inspect
    // the remainder rather than `from_bytes`, which errors on trailing input.
    let (env, rest): (GossipEnvelope, &[u8]) =
        postcard::take_from_bytes(bytes).map_err(|_| AnnounceReject::DecodeFailed)?;

    // #577 M3: bound the trailing-bytes allowance. Reject before signature
    // verify so an attacker can't burn ed25519 cycles on a shape we'll drop
    // regardless. Defense-in-depth behind [`GOSSIP_MAX_FRAME`], which
    // already capped the iroh-gossip allocation; this surfaces shape
    // violations under that ceiling via the rejection metric and keeps
    // them out of the peer table.
    if rest.len() > MAX_TRAILING_BYTES {
        return Err(AnnounceReject::OversizeTrailingBytes);
    }

    // This validator handles only `NodeAnnounce`; a `ReputationReport`
    // envelope on this path is the wrong variant (the reputation topic has its
    // own `validate_reputation_envelope`).
    let GossipPayload::NodeAnnounce(announce) = env.payload else {
        return Err(AnnounceReject::UnknownVariant);
    };

    validate_announce_fields(&announce, now_us)?;
    verify_signature(&announce)?;

    if let AnnounceGate::Enforce(staked) = gate
        && !staked.contains(&announce.body.node_id)
    {
        return Err(AnnounceReject::NotStaked);
    }

    Ok(announce)
}

fn validate_announce_fields(a: &NodeAnnounce, now_us: u64) -> Result<(), AnnounceReject> {
    let b = &a.body;

    // Region: must be in the ISO 3166-1 alpha-2 allowlist (assigned codes
    // + the user-reserved ranges, see `decdn_protocol::region`). The
    // strict allowlist closes the topic-name injection surface (`/`,
    // `\0`, non-ASCII) and also rejects unassigned codes like `OO` or
    // `JJ` that the bare "2 ASCII uppercase" check let through before.
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
        GOSSIP_VERSION, GossipEnvelope, GossipPayload, NodeAnnounce, NodeAnnounceBody,
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
                AnnounceReject::NotStaked => "not_staked",
                AnnounceReject::OversizeTrailingBytes => "oversize_trailing_bytes",
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
            AnnounceReject::NotStaked,
            AnnounceReject::OversizeTrailingBytes,
        ] {
            assert_eq!(r.label(), expected_label(&r), "label drift for {r:?}");
        }

        // Free-form labels passed directly to `inc_rejected` aren't covered
        // by `AnnounceReject::label`; keep them pinned here too.
        assert_eq!(crate::service::SUBSCRIBE_FAILED_LABEL, "subscribe_failed");
        assert_eq!(crate::service::PEER_TABLE_FULL_LABEL, "table_full");
        assert_eq!(
            crate::service::RESUBSCRIBE_FAILED_LABEL,
            "resubscribe_failed"
        );
    }

    fn sample_body(sk: &SecretKey, ts_us: u64) -> NodeAnnounceBody {
        NodeAnnounceBody {
            node_id: *sk.public().as_bytes(),
            region: "US".to_string(),
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

    /// Membership stub: staked iff the node ID is in the list. An empty list
    /// therefore rejects every announce — the strict rule-2 default.
    struct StakedSet(Vec<[u8; 32]>);
    impl StakedNodeSet for StakedSet {
        fn contains(&self, node_id: &[u8; 32]) -> bool {
            self.0.contains(node_id)
        }
    }

    #[test]
    fn happy_path() {
        let sk = fresh_key();
        let bytes = mk_envelope(&sk, |_| {});
        let a = validate_envelope(&bytes, 1_700_000_000_000_000, AnnounceGate::Disabled)
            .expect("valid");
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
            validate_envelope(&bytes, 1_700_000_000_000_000, AnnounceGate::Disabled),
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
            validate_envelope(&bytes, 1_700_000_000_000_000, AnnounceGate::Disabled),
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
            validate_envelope(&bytes, 1_700_000_000_000_000, AnnounceGate::Disabled),
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
            validate_envelope(&bytes, now, AnnounceGate::Disabled),
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
            validate_envelope(&bytes, now, AnnounceGate::Disabled),
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
                validate_envelope(&bytes, 1_700_000_000_000_000, AnnounceGate::Disabled),
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
                validate_envelope(&bytes, 1_700_000_000_000_000, AnnounceGate::Disabled).is_ok(),
                "region {good:?} should be accepted"
            );
        }
    }

    /// ADR 001 rule 2: with an `Enforce` staked set, an announce from a
    /// non-member is rejected (`NotStaked`) and one from a member is accepted.
    #[test]
    fn staked_node_gate_enforced() {
        let sk = fresh_key();
        let bytes = mk_envelope(&sk, |_| {});
        // A set that does not contain the announcer rejects it — including the
        // empty set (no staked peers to learn).
        let others = StakedSet(vec![[42u8; 32]]);
        assert_eq!(
            validate_envelope(
                &bytes,
                1_700_000_000_000_000,
                AnnounceGate::Enforce(&others)
            ),
            Err(AnnounceReject::NotStaked)
        );
        let with_announcer = StakedSet(vec![[42u8; 32], *sk.public().as_bytes()]);
        assert!(
            validate_envelope(
                &bytes,
                1_700_000_000_000_000,
                AnnounceGate::Enforce(&with_announcer)
            )
            .is_ok()
        );
    }

    /// The empty staked set rejects every announce — the strict rule-2 default
    /// (an empty active registry has no staked peers, so nothing is learnable).
    #[test]
    fn empty_staked_set_rejects_all() {
        let sk = fresh_key();
        let bytes = mk_envelope(&sk, |_| {});
        assert_eq!(
            validate_envelope(
                &bytes,
                1_700_000_000_000_000,
                AnnounceGate::Enforce(&StakedSet(Vec::new()))
            ),
            Err(AnnounceReject::NotStaked)
        );
    }

    /// [`AnnounceGate::Disabled`] fails open (tests / unstaked modes): an
    /// otherwise-valid announce from an unstaked author is accepted. Only the
    /// membership check is skipped — the many sibling tests in this module that
    /// assert a rejection while passing `Disabled` are what cover the rules that
    /// still apply.
    #[test]
    fn disabled_gate_accepts_any() {
        let sk = fresh_key();
        let bytes = mk_envelope(&sk, |_| {});
        assert!(validate_envelope(&bytes, 1_700_000_000_000_000, AnnounceGate::Disabled).is_ok());
    }

    /// [`OwnedAnnounceGate::as_gate`] must preserve the variant when projecting
    /// the owned gate into the borrowed form [`validate_envelope`] takes: an
    /// `Enforce` set must still gate on membership, a `Disabled` gate must still
    /// fail open. This is the owned→borrowed step the runtime runs at every
    /// subscriber (`subscriber_task` calls `gate.as_gate()`); the multi-node
    /// integration tests only cover the `Enforce` arm, so pin both here.
    #[test]
    fn as_gate_preserves_variant() {
        let sk = fresh_key();
        let bytes = mk_envelope(&sk, |_| {});

        // Enforce(non-member) → still rejects after projection.
        let enforce_miss: OwnedAnnounceGate =
            AnnounceGate::Enforce(Arc::new(StakedSet(vec![[42u8; 32]])));
        assert_eq!(
            validate_envelope(&bytes, 1_700_000_000_000_000, enforce_miss.as_gate()),
            Err(AnnounceReject::NotStaked)
        );

        // Enforce(member) → still accepts after projection.
        let enforce_hit: OwnedAnnounceGate =
            AnnounceGate::Enforce(Arc::new(StakedSet(vec![*sk.public().as_bytes()])));
        assert!(validate_envelope(&bytes, 1_700_000_000_000_000, enforce_hit.as_gate()).is_ok());

        // Disabled → still fails open after projection.
        let disabled: OwnedAnnounceGate = AnnounceGate::Disabled;
        assert!(validate_envelope(&bytes, 1_700_000_000_000_000, disabled.as_gate()).is_ok());
    }

    /// The hand-written [`AnnounceGate`] `Debug` (needed because the `S` handle
    /// isn't `Debug`) reports the variant only — never the set contents, which
    /// would leak staked membership into logs. Pins the format strings and, by
    /// construction, that it compiles with no `S: Debug` bound.
    #[test]
    fn debug_reports_variant_only() {
        let enforce: OwnedAnnounceGate = AnnounceGate::Enforce(Arc::new(StakedSet(Vec::new())));
        assert_eq!(format!("{enforce:?}"), "AnnounceGate::Enforce(..)");
        let disabled: OwnedAnnounceGate = AnnounceGate::Disabled;
        assert_eq!(format!("{disabled:?}"), "AnnounceGate::Disabled");
    }

    /// Now that `GossipPayload` has a second variant, a `ReputationReport`
    /// envelope fed to the `NodeAnnounce` validator must hit the (newly
    /// reachable) `UnknownVariant` arm rather than being mis-accepted. Guards
    /// the two validators against cross-accepting each other's payloads.
    #[test]
    fn reputation_report_envelope_rejected_as_unknown_variant() {
        use decdn_protocol::{ReportMetrics, ReputationReport, ReputationReportBody};
        let sk = fresh_key();
        let body = ReputationReportBody {
            provider: [2u8; 32],
            reporter: *sk.public().as_bytes(),
            metrics: ReportMetrics {
                delivery_speed: None,
                uptime_observed: Some(true),
                data_correct: Some(true),
            },
            timestamp_secs: 1_700_000_000,
        };
        let signature = sk
            .sign(&body.signing_bytes().expect("body encode"))
            .to_bytes()
            .to_vec();
        let bytes = encode(&GossipEnvelope {
            version: GOSSIP_VERSION,
            payload: GossipPayload::ReputationReport(ReputationReport { body, signature }),
        });
        assert_eq!(
            validate_envelope(&bytes, 1_700_000_000_000_000, AnnounceGate::Disabled),
            Err(AnnounceReject::UnknownVariant)
        );
    }

    #[test]
    fn garbage_bytes_rejected() {
        // First byte 0xFF != GOSSIP_VERSION, so the version-first check
        // rejects before attempting deserialization.
        assert_eq!(
            validate_envelope(
                &[0xFFu8, 0xFF],
                1_700_000_000_000_000,
                AnnounceGate::Disabled
            ),
            Err(AnnounceReject::UnknownVersion)
        );
        // Empty input has no version byte at all → DecodeFailed.
        assert_eq!(
            validate_envelope(&[], 1_700_000_000_000_000, AnnounceGate::Disabled),
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
            validate_envelope(&bytes, 1_700_000_000_000_000, AnnounceGate::Disabled),
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
            validate_envelope(&bytes, 1_700_000_000_000_000, AnnounceGate::Disabled),
            Err(AnnounceReject::InvalidSignature)
        );
    }

    /// #577 M3 — a peer padding a valid envelope past
    /// [`MAX_TRAILING_BYTES`] is rejected before signature verification.
    /// The reject must come from the size check, not from a downstream
    /// signature mismatch, so the test appends garbage *after* the
    /// signed envelope completes (trailing bytes are outside postcard's
    /// consumed range and don't affect signature verification).
    #[test]
    fn rejects_oversize_trailing_bytes() {
        let sk = fresh_key();
        let mut bytes = mk_envelope(&sk, |_| {});
        bytes.extend(std::iter::repeat_n(0xAAu8, MAX_TRAILING_BYTES + 1));
        assert_eq!(
            validate_envelope(&bytes, 1_700_000_000_000_000, AnnounceGate::Disabled),
            Err(AnnounceReject::OversizeTrailingBytes)
        );
    }

    /// Boundary partner for [`rejects_oversize_trailing_bytes`]: exactly
    /// [`MAX_TRAILING_BYTES`] of trailing bytes is accepted (the `>` in
    /// the size gate). A regression that flipped `>` to `>=` (or
    /// vice-versa) is the failure mode this pair catches. Mirrors the
    /// existing `evict_at_exact_cutoff_keeps_entry` /
    /// `evict_one_microsecond_past_cutoff_removes_entry` boundary
    /// discipline in `peer_table::tests`.
    #[test]
    fn accepts_trailing_bytes_at_threshold() {
        let sk = fresh_key();
        let mut bytes = mk_envelope(&sk, |_| {});
        bytes.extend(std::iter::repeat_n(0xAAu8, MAX_TRAILING_BYTES));
        assert!(
            validate_envelope(&bytes, 1_700_000_000_000_000, AnnounceGate::Disabled).is_ok(),
            "exactly MAX_TRAILING_BYTES of padding must be accepted"
        );
    }

    /// #577 M3 — pins the *ordering* of size-check vs signature-verify.
    /// The variant's doc-comment promises: "Place the check before
    /// signature verification — no point spending ed25519 cycles on an
    /// envelope that's going to be rejected for shape." A regression
    /// that swapped the order (verify-first) would still pass
    /// [`rejects_oversize_trailing_bytes`] because the inner envelope's
    /// signature is valid; this test corrupts the signature *and*
    /// over-pads so verify-first would surface `InvalidSignature`
    /// instead of `OversizeTrailingBytes`. The assertion locks the
    /// ed25519-cycle-saving guarantee at the test layer.
    #[test]
    fn rejects_oversize_before_checking_signature() {
        let sk = fresh_key();
        let body = sample_body(&sk, 1_700_000_000_000_000);
        // Garbage signature — verify would fail with InvalidSignature.
        let mut bytes = encode(&GossipEnvelope {
            version: GOSSIP_VERSION,
            payload: GossipPayload::NodeAnnounce(NodeAnnounce {
                body,
                signature: vec![7u8; SIGNATURE_LEN],
            }),
        });
        bytes.extend(std::iter::repeat_n(0xAAu8, MAX_TRAILING_BYTES + 1));
        assert_eq!(
            validate_envelope(&bytes, 1_700_000_000_000_000, AnnounceGate::Disabled),
            Err(AnnounceReject::OversizeTrailingBytes),
            "size check must run BEFORE signature verify"
        );
    }
}
