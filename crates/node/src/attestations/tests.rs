use std::sync::Arc;

use alloy::primitives::U256;
use alloy::signers::local::PrivateKeySigner;
use decdn_protocol::{DEFAULT_ATTESTATION_TTL_SEC, MIN_ATTESTATION_TTL_SEC};

use super::*;
use crate::content_deny::ContentDenylist;
use crate::dht::attestation::sign_attestation;
use crate::dht::attestation::test_support::{
    NAMESPACE, ORIGIN, attest, attest_at, domain, verifier,
};
use crate::dht::seated_origins::SeatedOriginSet;

const HOUR_US: u64 = 3_600_000_000;

fn origin(signer: PrivateKeySigner) -> SelfOrigin {
    SelfOrigin {
        signer: Arc::new(signer),
        ttl_sec: MIN_ATTESTATION_TTL_SEC,
    }
}

#[test]
fn keep_refuses_an_attestation_no_seated_origin_signed() {
    let store = Attestations::in_memory(verifier(), None);
    let other = PrivateKeySigner::random();
    let att = sign_attestation(
        &other,
        &domain(),
        [1; 32],
        NAMESPACE,
        now_us(),
        DEFAULT_ATTESTATION_TTL_SEC,
    )
    .unwrap();
    assert!(!store.keep([1; 32], att));
    assert_eq!(store.held(&[1; 32]), None);
    assert_eq!(store.live(&[1; 32]), None);
}

#[test]
fn keep_refuses_an_expired_attestation() {
    let store = Attestations::in_memory(verifier(), None);
    let stale = attest_at([1; 32], now_us() - 13 * HOUR_US);
    assert!(!store.keep([1; 32], stale));
    assert_eq!(store.live(&[1; 32]), None);
}

#[test]
fn a_kept_attestation_is_live() {
    let store = Attestations::in_memory(verifier(), None);
    let att = attest([1; 32]);
    assert!(store.keep([1; 32], att.clone()));
    assert_eq!(store.live(&[1; 32]), Some(att));
}

#[test]
fn keep_holds_the_attestation_that_expires_last() {
    let store = Attestations::in_memory(verifier(), None);
    let now = now_us();
    let older = attest_at([1; 32], now - HOUR_US);
    let newer = attest_at([1; 32], now);
    assert!(store.keep([1; 32], newer.clone()));
    assert!(store.keep([1; 32], older), "an older one is still valid");
    assert_eq!(store.held(&[1; 32]), Some(newer.clone()), "but not kept");
    let newest = attest_at([1; 32], now + 1);
    assert!(store.keep([1; 32], newest.clone()));
    assert_eq!(store.held(&[1; 32]), Some(newest));
}

#[test]
fn keep_offered_reports_whether_the_node_can_serve() {
    let store = Attestations::in_memory(verifier(), None);
    assert!(!store.keep_offered([1; 32], None));
    let stranger = sign_attestation(
        &PrivateKeySigner::random(),
        &domain(),
        [1; 32],
        NAMESPACE,
        now_us(),
        DEFAULT_ATTESTATION_TTL_SEC,
    )
    .unwrap();
    assert!(!store.keep_offered([1; 32], Some(&stranger)));
    assert!(store.keep_offered([1; 32], Some(&attest([1; 32]))));
    assert!(
        store.keep_offered([1; 32], None),
        "an upstream that offers none leaves the held one live"
    );
}

#[test]
fn a_held_attestation_stops_being_live_once_its_origin_is_unseated() {
    let seated = Arc::new(SeatedOriginSet::from_seats([(NAMESPACE, ORIGIN.address())]));
    let v = AttestationVerifier::new(seated.clone(), Arc::new(ContentDenylist::empty()), domain());
    let store = Attestations::in_memory(v, None);
    assert!(store.keep([1; 32], attest([1; 32])));
    seated.unseat(NAMESPACE, ORIGIN.address());
    assert_eq!(store.live(&[1; 32]), None);
}

#[test]
fn a_seated_node_signs_its_own_attestation_in_its_seat_namespace() {
    let store = Attestations::in_memory(verifier(), Some(origin(ORIGIN.clone())));
    assert!(store.can_attest_own());
    let att = store.attest_own([2; 32]).expect("self-signed");
    assert_eq!(att.namespace_id, NAMESPACE.to_be_bytes());
    assert_eq!(att.ttl_sec, MIN_ATTESTATION_TTL_SEC);
    assert_eq!(
        store.verifier().verify([2; 32], &att),
        Some(ORIGIN.address())
    );
    assert_eq!(store.live(&[2; 32]), Some(att.clone()));
    assert_eq!(
        store.attest_own([2; 32]),
        Some(att),
        "a fresh one is not re-signed"
    );
}

#[test]
fn a_seated_node_re_signs_past_half_life() {
    let store = Attestations::in_memory(
        verifier(),
        Some(SelfOrigin {
            signer: Arc::new(ORIGIN.clone()),
            ttl_sec: DEFAULT_ATTESTATION_TTL_SEC,
        }),
    );
    let aged = attest_at([2; 32], now_us() - 7 * HOUR_US);
    assert!(store.keep([2; 32], aged.clone()));
    let fresh = store.attest_own([2; 32]).expect("re-signed");
    assert!(fresh.timestamp_us > aged.timestamp_us);
    assert_eq!(store.held(&[2; 32]), Some(fresh));
}

#[test]
fn an_unseated_node_does_not_self_sign() {
    let store = Attestations::in_memory(verifier(), Some(origin(PrivateKeySigner::random())));
    assert!(!store.can_attest_own());
    assert_eq!(store.attest_own([2; 32]), None);
    assert_eq!(store.live(&[2; 32]), None);
}

#[test]
fn refresh_is_due_between_half_life_and_three_quarters() {
    let store = Attestations::in_memory(verifier(), None);
    let now = now_us();
    let att = attest_at([4; 32], now);
    let ttl_us = u64::from(att.ttl_sec) * 1_000_000;
    assert!(store.keep([4; 32], att.clone()));
    assert!(store.refresh_due(now).is_empty());
    assert!(store.refresh_due(now + ttl_us / 2 - 1).is_empty());
    let due = store.refresh_due(now + ttl_us * 3 / 4);
    assert_eq!(
        due,
        vec![RefreshDue {
            hash: [4; 32],
            attestation: att,
        }]
    );
}

#[test]
fn a_deferred_refresh_waits_for_its_retry() {
    let store = Attestations::in_memory(verifier(), None);
    let now = now_us();
    assert!(store.keep([4; 32], attest_at([4; 32], now)));
    let late = now + u64::from(DEFAULT_ATTESTATION_TTL_SEC) * 1_000_000 * 3 / 4;
    store.defer_refresh([4; 32], late);
    assert!(store.refresh_due(late).is_empty());
    assert_eq!(store.refresh_due(late + REFRESH_RETRY_US).len(), 1);
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
    assert_eq!(store.live(&[3; 32]), Some(att));
    store.forget(&[3; 32]);
    let store = Attestations::open(verifier(), dir.path(), None);
    assert_eq!(store.held(&[3; 32]), None);
}

#[test]
fn another_namespaces_seat_does_not_self_sign_here() {
    let v = AttestationVerifier::new(
        Arc::new(SeatedOriginSet::from_seats([(
            U256::from(5),
            ORIGIN.address(),
        )])),
        Arc::new(ContentDenylist::empty()),
        domain(),
    );
    let store = Attestations::in_memory(v, Some(origin(ORIGIN.clone())));
    let att = store.attest_own([6; 32]).expect("seated in 5");
    assert_eq!(att.namespace_id, U256::from(5).to_be_bytes());
}
