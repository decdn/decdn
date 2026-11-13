use std::sync::Arc;

use alloy::primitives::U256;
use alloy::signers::local::PrivateKeySigner;

use super::*;
use crate::content_deny::ContentDenylist;
use crate::dht::attestation::test_support::{ORIGIN, attest, domain, verifier};
use crate::dht::seated_origins::SeatedOriginSet;

#[test]
fn keep_refuses_an_attestation_no_seated_origin_signed() {
    let store = Attestations::in_memory(verifier(), None);
    let other = PrivateKeySigner::random();
    let att = crate::dht::attestation::sign_attestation(&other, &domain(), [1; 32], 1, 1).unwrap();
    assert!(!store.keep([1; 32], att));
    assert_eq!(store.held(&[1; 32]), None);
    assert_eq!(store.for_publish([1; 32]), None);
}

#[test]
fn a_kept_attestation_is_forwarded_and_published() {
    let store = Attestations::in_memory(verifier(), None);
    let att = attest([1; 32]);
    assert!(store.keep([1; 32], att.clone()));
    assert_eq!(store.held(&[1; 32]), Some(att.clone()));
    assert_eq!(store.for_publish([1; 32]), Some(att));
}

#[test]
fn a_held_attestation_stops_publishing_once_its_origin_is_unseated() {
    let seated = Arc::new(SeatedOriginSet::from_seats([(
        U256::from(1),
        ORIGIN.address(),
    )]));
    let v = AttestationVerifier::new(seated.clone(), Arc::new(ContentDenylist::empty()), domain());
    let store = Attestations::in_memory(v, None);
    assert!(store.keep([1; 32], attest([1; 32])));
    seated.unseat(U256::from(1), ORIGIN.address());
    assert_eq!(store.for_publish([1; 32]), None);
}

#[test]
fn a_seated_node_signs_its_own_attestation() {
    let store = Attestations::in_memory(
        verifier(),
        Some(SelfOrigin {
            signer: Arc::new(ORIGIN.clone()),
            rate_per_mb: 7,
        }),
    );
    let att = store.for_publish([2; 32]).expect("self-signed");
    assert_eq!(att.rate_per_mb, 7);
    assert_eq!(
        store.verifier().verify([2; 32], &att),
        Some(ORIGIN.address())
    );
    assert_eq!(store.held(&[2; 32]), Some(att));
}

#[test]
fn an_unseated_node_does_not_self_sign() {
    let store = Attestations::in_memory(
        verifier(),
        Some(SelfOrigin {
            signer: Arc::new(PrivateKeySigner::random()),
            rate_per_mb: 7,
        }),
    );
    assert_eq!(store.for_publish([2; 32]), None);
}

#[test]
fn attestations_survive_a_restart_and_forget_removes_them() {
    let dir = tempfile::tempdir().unwrap();
    let att = attest([3; 32]);
    {
        let store = Attestations::open(verifier(), dir.path(), None);
        assert!(store.keep([3; 32], att.clone()));
    }
    let store = Attestations::open(verifier(), dir.path(), None);
    assert_eq!(store.held(&[3; 32]), Some(att));
    store.forget(&[3; 32]);
    let store = Attestations::open(verifier(), dir.path(), None);
    assert_eq!(store.held(&[3; 32]), None);
}
