use super::*;

use alloy::primitives::B256;

/// The expiry margin the tests pay under.
const MARGIN: u64 = 420;

/// A capability from pool `pool` (every byte `pool`) to `signer`, expiring
/// at `expiry`.
fn grant(signer: Address, pool: u8, expiry: u64) -> CapabilityGrant {
    CapabilityGrant {
        pool_id: B256::repeat_byte(pool),
        signer,
        spending_cap: 1,
        expiry,
        owner_signature: vec![0; 65],
    }
}

/// Register a capability from `pool` to the pending key, and return the key.
fn claim(delegates: &mut Delegates, pool: u8, expiry: u64) -> Address {
    let signer = delegates.pending();
    delegates.register(grant(signer, pool, expiry)).unwrap();
    signer
}

#[test]
fn the_pending_key_is_one_until_a_capability_claims_it() {
    let mut delegates = Delegates::default();
    let pending = delegates.pending();
    assert_eq!(delegates.pending(), pending);
    delegates.register(grant(pending, 1, u64::MAX)).unwrap();
    let next = delegates.pending();
    assert_ne!(next, pending);
    assert_eq!(delegates.pending(), next);
}

#[test]
fn a_key_holds_one_capability_per_pool() {
    let mut delegates = Delegates::default();
    let held = claim(&mut delegates, 1, u64::MAX);
    assert_eq!(
        delegates.register(grant(held, 1, u64::MAX)),
        Err(Refusal::PoolTaken)
    );
    delegates.register(grant(held, 2, u64::MAX)).unwrap();
    assert_eq!(delegates.list().len(), 2);
    // Registering a held key leaves the pending key pending.
    let pending = delegates.pending();
    delegates.register(grant(held, 3, u64::MAX)).unwrap();
    assert_eq!(delegates.pending(), pending);
}

#[test]
fn a_key_the_gateway_does_not_hold_is_refused() {
    let mut delegates = Delegates::default();
    let stranger = Address::repeat_byte(9);
    assert_eq!(
        delegates.admits(&grant(stranger, 1, u64::MAX)),
        Err(Refusal::UnknownKey)
    );
    assert_eq!(
        delegates.register(grant(stranger, 1, u64::MAX)),
        Err(Refusal::UnknownKey)
    );
    assert!(delegates.list().is_empty());
}

#[test]
fn the_earliest_registered_capability_pays_first() {
    let mut delegates = Delegates::default();
    let first = claim(&mut delegates, 1, u64::MAX);
    let second = claim(&mut delegates, 2, u64::MAX);
    let paying = delegates.paying(0, MARGIN).unwrap();
    assert_eq!(paying.signer.address(), first);
    // A funding stop retires it, and the next one pays.
    delegates.retire(first, B256::repeat_byte(1));
    let paying = delegates.paying(0, MARGIN).unwrap();
    assert_eq!(paying.signer.address(), second);
    assert_eq!(paying.grant.pool_id, B256::repeat_byte(2));
    delegates.retire(second, B256::repeat_byte(2));
    assert!(delegates.paying(0, MARGIN).is_none());
    // Both stay listed, as spent.
    assert!(delegates.list().iter().all(|c| c.spent));
}

#[test]
fn a_capability_inside_the_expiry_margin_pays_no_more() {
    let mut delegates = Delegates::default();
    let soon = claim(&mut delegates, 1, 1_000);
    let later = claim(&mut delegates, 2, 10_000);
    assert_eq!(
        delegates
            .paying(1_000 - MARGIN - 1, MARGIN)
            .unwrap()
            .signer
            .address(),
        soon
    );
    assert_eq!(
        delegates
            .paying(1_000 - MARGIN, MARGIN)
            .unwrap()
            .signer
            .address(),
        later
    );
    assert!(delegates.paying(10_000 - MARGIN, MARGIN).is_none());
}

#[test]
fn prune_drops_expired_capabilities_and_keys_left_with_none() {
    let mut delegates = Delegates::default();
    let key = claim(&mut delegates, 1, 100);
    delegates.register(grant(key, 2, 200)).unwrap();
    let pending = delegates.pending();
    delegates.prune(150);
    assert_eq!(delegates.list().len(), 1);
    delegates.prune(200);
    assert!(delegates.list().is_empty());
    assert_eq!(
        delegates.admits(&grant(key, 3, u64::MAX)),
        Err(Refusal::UnknownKey),
        "a key with no capability left is dropped"
    );
    assert_eq!(delegates.pending(), pending, "the pending key stays");
}

#[test]
fn the_list_and_the_lanes_follow_registration_order() {
    let mut delegates = Delegates::default();
    let a = claim(&mut delegates, 1, u64::MAX);
    let b = claim(&mut delegates, 2, u64::MAX);
    delegates.register(grant(a, 3, u64::MAX)).unwrap();
    let listed: Vec<_> = delegates
        .list()
        .into_iter()
        .map(|c| (c.signer, c.pool))
        .collect();
    assert_eq!(
        listed,
        [
            (a.to_string(), B256::repeat_byte(1).to_string()),
            (b.to_string(), B256::repeat_byte(2).to_string()),
            (a.to_string(), B256::repeat_byte(3).to_string()),
        ]
    );
    let mut lanes = delegates.lanes();
    lanes.sort();
    let mut expected = vec![
        (B256::repeat_byte(1), a),
        (B256::repeat_byte(2), b),
        (B256::repeat_byte(3), a),
    ];
    expected.sort();
    assert_eq!(lanes, expected);
}
