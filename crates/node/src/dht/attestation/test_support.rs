use std::sync::{Arc, LazyLock};

use alloy::dyn_abi::Eip712Domain;
use alloy::primitives::{B256, U256, address};
use alloy::signers::local::PrivateKeySigner;
use decdn_protocol::OriginAttestation;

use super::{AttestationVerifier, sign_attestation};
use crate::content_deny::ContentDenylist;
use crate::dht::seated_origins::SeatedOriginSet;

/// The fixture origin's key. Fixed so every test signs alike.
pub static ORIGIN: LazyLock<PrivateKeySigner> = LazyLock::new(|| {
    PrivateKeySigner::from_bytes(&B256::repeat_byte(0x42))
        .unwrap_or_else(|_| PrivateKeySigner::random())
});

/// The `SlashJudge` domain the fixtures sign under.
#[must_use]
pub fn domain() -> Eip712Domain {
    decdn_incentive::slash_judge_domain(
        421_614,
        address!("0000000000000000000000000000000000001234"),
    )
}

/// A verifier that admits [`ORIGIN`] and nobody else.
#[must_use]
pub fn verifier() -> AttestationVerifier {
    AttestationVerifier::new(
        Arc::new(SeatedOriginSet::from_seats([(
            U256::from(1),
            ORIGIN.address(),
        )])),
        Arc::new(ContentDenylist::empty()),
        domain(),
    )
}

/// An attestation store whose node signs as [`ORIGIN`], so it publishes a
/// valid record for every hash it holds.
#[must_use]
pub fn self_signing() -> Arc<crate::attestations::Attestations> {
    Arc::new(crate::attestations::Attestations::in_memory(
        verifier(),
        Some(crate::attestations::SelfOrigin {
            signer: Arc::new(ORIGIN.clone()),
            rate_per_mb: 1,
        }),
    ))
}

/// A valid attestation for `hash`, signed by [`ORIGIN`].
#[must_use]
pub fn attest(hash: [u8; 32]) -> OriginAttestation {
    sign_attestation(&*ORIGIN, &domain(), hash, 1, 1).unwrap_or_else(|_| OriginAttestation {
        rate_per_mb: 1,
        timestamp_us: 1,
        slash_sig: vec![0; decdn_protocol::message::SLASH_SIG_LEN],
    })
}
