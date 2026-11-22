use std::sync::{Arc, LazyLock};

use alloy::dyn_abi::Eip712Domain;
use alloy::primitives::{B256, U256, address};
use alloy::signers::local::PrivateKeySigner;
use decdn_protocol::{DEFAULT_ATTESTATION_TTL_SEC, OriginAttestation};

use super::{AttestationVerifier, sign_attestation};
use crate::content_deny::ContentDenylist;
use crate::dht::seated_origins::SeatedOriginSet;

/// The fixture origin's key. Fixed so every test signs alike.
pub static ORIGIN: LazyLock<PrivateKeySigner> = LazyLock::new(|| {
    PrivateKeySigner::from_bytes(&B256::repeat_byte(0x42))
        .unwrap_or_else(|_| PrivateKeySigner::random())
});

/// The namespace [`ORIGIN`] holds its seat in.
pub const NAMESPACE: U256 = U256::from_limbs([1, 0, 0, 0]);

/// The `SlashJudge` domain the fixtures sign under.
#[must_use]
pub fn domain() -> Eip712Domain {
    decdn_incentive::slash_judge_domain(
        421_614,
        address!("0000000000000000000000000000000000001234"),
    )
}

/// A verifier that admits [`ORIGIN`] in [`NAMESPACE`] and nobody else.
#[must_use]
pub fn verifier() -> AttestationVerifier {
    AttestationVerifier::new(
        Arc::new(SeatedOriginSet::from_seats([(NAMESPACE, ORIGIN.address())])),
        Arc::new(ContentDenylist::empty()),
        domain(),
    )
}

/// An attestation store whose node signs as [`ORIGIN`], so it signs its own
/// attestation for every hash its own backend holds. It holds none for any
/// other hash until a test keeps one.
#[must_use]
pub fn self_signing() -> Arc<crate::attestations::Attestations> {
    Arc::new(crate::attestations::Attestations::in_memory(
        verifier(),
        Some(crate::attestations::SelfOrigin {
            signer: Arc::new(ORIGIN.clone()),
            ttl_sec: DEFAULT_ATTESTATION_TTL_SEC,
        }),
    ))
}

/// An attestation for `hash` signed by [`ORIGIN`] in [`NAMESPACE`] at
/// `timestamp_us`, with the default lifetime.
#[must_use]
pub fn attest_at(hash: [u8; 32], timestamp_us: u64) -> OriginAttestation {
    sign_attestation(
        &*ORIGIN,
        &domain(),
        hash,
        NAMESPACE,
        timestamp_us,
        DEFAULT_ATTESTATION_TTL_SEC,
    )
    .unwrap_or_else(|_| OriginAttestation {
        namespace_id: NAMESPACE.to_be_bytes(),
        timestamp_us,
        ttl_sec: DEFAULT_ATTESTATION_TTL_SEC,
        sig: vec![0; decdn_protocol::message::SLASH_SIG_LEN],
    })
}

/// The timestamp [`attest`] signs: the clock at its first use in this
/// process, so every call for one hash returns the same attestation, and it
/// stays live for the default lifetime.
static STAMP_US: LazyLock<u64> = LazyLock::new(crate::handlers::timestamp_skew::now_us);

/// A live attestation for `hash`, signed by [`ORIGIN`]. Deterministic within
/// a process.
#[must_use]
pub fn attest(hash: [u8; 32]) -> OriginAttestation {
    attest_at(hash, *STAMP_US)
}
