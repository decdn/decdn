use std::sync::Arc;

use alloy::primitives::U256;
use alloy::signers::local::PrivateKeySigner;
use decdn_protocol::{
    DEFAULT_ATTESTATION_TTL_SEC, MAX_ATTESTATION_FUTURE_SKEW_US, MAX_ATTESTATION_TTL_SEC,
    MIN_ATTESTATION_TTL_SEC,
};

use super::test_support::{NAMESPACE, ORIGIN, attest, attest_at, domain, registered, verifier};
use super::*;
use crate::dht::seated_origins::SeatedOriginSet;

const NOW: u64 = 1_800_000_000_000_000;

fn sign(ns: U256, timestamp_us: u64, ttl_sec: u32) -> OriginAttestation {
    sign_attestation(&*ORIGIN, &domain(), [7; 32], ns, timestamp_us, ttl_sec).unwrap()
}

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
    let att = sign_attestation(
        &other,
        &domain(),
        hash,
        NAMESPACE,
        NOW,
        DEFAULT_ATTESTATION_TTL_SEC,
    )
    .unwrap();
    assert_eq!(verifier().recover(hash, &att), Some(other.address()));
    assert_eq!(verifier().verify_at(hash, &att, NOW), None);
}

/// A seat outlives its operator's registration until someone prunes it, so
/// a seated signer outside the registered set attests nothing (#2851), and
/// one that leaves the registered set stops attesting at the next check.
#[test]
fn a_seated_signer_outside_the_registered_set_attests_nothing() {
    let seated = Arc::new(SeatedOriginSet::from_seats([(NAMESPACE, ORIGIN.address())]));
    let unregistered = AttestationVerifier::new(
        seated.clone(),
        registered([]),
        Arc::new(ContentDenylist::empty()),
        domain(),
    );
    let att = sign(NAMESPACE, NOW, DEFAULT_ATTESTATION_TTL_SEC);
    assert_eq!(unregistered.verify_at([7; 32], &att, NOW), None);
    assert_eq!(unregistered.attesting_namespace(&ORIGIN.address()), None);

    // Bound to a NodeId the staker set no longer reports active.
    let node = NodeId::from_bytes([9; 32]);
    let stakers = Arc::new(crate::dht::staker_set::ConfigStakerSet::empty());
    let inactive = AttestationVerifier::new(
        seated,
        RegisteredOperators::new(
            Arc::new(RwLock::new(HashMap::from([(ORIGIN.address(), node)]))),
            stakers,
        ),
        Arc::new(ContentDenylist::empty()),
        domain(),
    );
    assert_eq!(inactive.verify_at([7; 32], &att, NOW), None);
    assert_eq!(
        verifier().verify_at([7; 32], &att, NOW),
        Some(ORIGIN.address())
    );
}

#[test]
fn a_seat_in_another_namespace_attests_nothing_here() {
    let att = sign(U256::from(2), NOW, DEFAULT_ATTESTATION_TTL_SEC);
    assert_eq!(verifier().verify_at([7; 32], &att, NOW), None);
}

#[test]
fn namespace_zero_attests_nothing() {
    let v = AttestationVerifier::new(
        Arc::new(SeatedOriginSet::from_seats([(
            U256::ZERO,
            ORIGIN.address(),
        )])),
        registered([ORIGIN.address()]),
        Arc::new(ContentDenylist::empty()),
        domain(),
    );
    let att = sign(U256::ZERO, NOW, DEFAULT_ATTESTATION_TTL_SEC);
    assert_eq!(v.verify_at([7; 32], &att, NOW), None);
}

#[test]
fn unseating_the_signer_from_its_namespace_cancels_its_attestation() {
    let seated = Arc::new(SeatedOriginSet::from_seats([
        (NAMESPACE, ORIGIN.address()),
        (U256::from(2), ORIGIN.address()),
    ]));
    let v = AttestationVerifier::new(
        seated.clone(),
        registered([ORIGIN.address()]),
        Arc::new(ContentDenylist::empty()),
        domain(),
    );
    let att = sign(NAMESPACE, NOW, DEFAULT_ATTESTATION_TTL_SEC);
    assert!(v.verify_at([7; 32], &att, NOW).is_some());
    seated.unseat(NAMESPACE, ORIGIN.address());
    assert!(
        v.verify_at([7; 32], &att, NOW).is_none(),
        "a seat in namespace 2 does not keep a namespace-1 attestation alive"
    );
}

#[test]
fn an_attestation_expires_after_its_ttl() {
    let att = sign(NAMESPACE, NOW, MIN_ATTESTATION_TTL_SEC);
    let ttl_us = u64::from(MIN_ATTESTATION_TTL_SEC) * 1_000_000;
    assert!(
        verifier()
            .verify_at([7; 32], &att, NOW + ttl_us - 1)
            .is_some()
    );
    assert!(verifier().verify_at([7; 32], &att, NOW + ttl_us).is_none());
}

#[test]
fn a_future_timestamp_past_the_skew_bound_is_not_valid() {
    let att = sign(
        NAMESPACE,
        NOW + MAX_ATTESTATION_FUTURE_SKEW_US + 1,
        DEFAULT_ATTESTATION_TTL_SEC,
    );
    assert!(verifier().verify_at([7; 32], &att, NOW).is_none());
    assert!(verifier().verify_at([7; 32], &att, NOW + 1).is_some());
}

#[test]
fn a_ttl_out_of_bounds_is_not_valid() {
    for ttl in [MIN_ATTESTATION_TTL_SEC - 1, MAX_ATTESTATION_TTL_SEC + 1] {
        let att = sign(NAMESPACE, NOW, ttl);
        assert!(verifier().verify_at([7; 32], &att, NOW).is_none(), "{ttl}");
    }
}

#[test]
fn a_tampered_field_breaks_the_signature() {
    let mut att = attest_at([7; 32], NOW);
    att.timestamp_us += 1;
    assert_eq!(verifier().verify_at([7; 32], &att, NOW), None);
}

#[test]
fn the_attesting_namespace_is_the_lowest_seat() {
    let v = AttestationVerifier::new(
        Arc::new(SeatedOriginSet::from_seats([
            (U256::from(9), ORIGIN.address()),
            (U256::from(4), ORIGIN.address()),
        ])),
        registered([ORIGIN.address()]),
        Arc::new(ContentDenylist::empty()),
        domain(),
    );
    assert_eq!(
        v.attesting_namespace(&ORIGIN.address()),
        Some(U256::from(4))
    );
    assert_eq!(
        v.attesting_namespace(&PrivateKeySigner::random().address()),
        None
    );
}
