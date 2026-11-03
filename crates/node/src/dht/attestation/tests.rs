use std::sync::Arc;

use alloy::primitives::U256;
use alloy::signers::local::PrivateKeySigner;

use super::test_support::{ORIGIN, attest, domain, verifier};
use super::*;
use crate::dht::seated_origins::SeatedOriginSet;

#[test]
fn a_seated_origins_attestation_is_valid_for_its_hash() {
    let hash = [7; 32];
    assert_eq!(
        verifier().verify(hash, &attest(hash)),
        Some(ORIGIN.address())
    );
}

#[test]
fn an_attestation_does_not_carry_to_another_hash() {
    let att = attest([7; 32]);
    assert_ne!(
        verifier().recover([8; 32], &att),
        Some(ORIGIN.address()),
        "the hash is part of the signed body"
    );
    assert_eq!(verifier().verify([8; 32], &att), None);
}

#[test]
fn an_unseated_signer_attests_nothing() {
    let other = PrivateKeySigner::random();
    let hash = [7; 32];
    let att = sign_attestation(&other, &domain(), hash, 1, 1, 1).unwrap();
    assert_eq!(verifier().recover(hash, &att), Some(other.address()));
    assert_eq!(verifier().verify(hash, &att), None);
}

#[test]
fn unseating_the_signer_cancels_its_attestation() {
    let seated = Arc::new(SeatedOriginSet::from_seats([(
        U256::from(1),
        ORIGIN.address(),
    )]));
    let v = AttestationVerifier::new(seated.clone(), Arc::new(ContentDenylist::empty()), domain());
    let hash = [7; 32];
    let att = attest(hash);
    assert!(v.verify(hash, &att).is_some());
    seated.unseat(U256::from(1), ORIGIN.address());
    assert!(v.verify(hash, &att).is_none());
}

#[test]
fn a_tampered_field_breaks_the_signature() {
    let hash = [7; 32];
    let mut att = attest(hash);
    att.total_bytes += 1;
    assert_eq!(verifier().verify(hash, &att), None);
}
