use super::*;

use alloy::primitives::B256;

/// The expiry margin the tests pay under.
const MARGIN: u64 = 420;

/// The `PaymentPool` contract the tests' pools are on.
const HERE: Deployment = Deployment {
    chain_id: 421_614,
    payment_pool: Address::repeat_byte(0x9c),
};

/// A second `PaymentPool` contract, on another payment chain.
const THERE: Deployment = Deployment {
    chain_id: 84_532,
    payment_pool: Address::repeat_byte(0x9d),
};

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

/// Register an unscoped capability from `pool` to the pending key, and
/// return the key.
fn claim(delegates: &mut Delegates, pool: u8, expiry: u64) -> Address {
    claim_scoped(delegates, pool, expiry, None)
}

/// Register a capability from `pool` to the pending key, scoped to
/// `namespaces`, and return the key.
fn claim_scoped(
    delegates: &mut Delegates,
    pool: u8,
    expiry: u64,
    namespaces: Option<Vec<u64>>,
) -> Address {
    let signer = delegates.pending();
    delegates
        .register(grant(signer, pool, expiry), HERE, namespaces)
        .unwrap();
    signer
}

/// The key the next pull pays with at `now`, for a request on `namespace`.
fn payer(delegates: &Delegates, now: u64, namespace: Option<u64>) -> Option<Address> {
    delegates
        .paying(now, MARGIN, namespace)
        .map(|c| c.signer.address())
}

#[test]
fn the_pending_key_is_one_until_a_capability_claims_it() {
    let mut delegates = Delegates::default();
    let pending = delegates.pending();
    assert_eq!(delegates.pending(), pending);
    delegates
        .register(grant(pending, 1, u64::MAX), HERE, None)
        .unwrap();
    let next = delegates.pending();
    assert_ne!(next, pending);
    assert_eq!(delegates.pending(), next);
}

#[test]
fn a_key_holds_one_capability_per_pool() {
    let mut delegates = Delegates::default();
    let held = claim(&mut delegates, 1, u64::MAX);
    assert_eq!(
        delegates.register(grant(held, 1, u64::MAX), HERE, None),
        Err(Refusal::PoolTaken)
    );
    delegates
        .register(grant(held, 2, u64::MAX), HERE, None)
        .unwrap();
    assert_eq!(delegates.list().len(), 2);
    // Registering a held key leaves the pending key pending.
    let pending = delegates.pending();
    delegates
        .register(grant(held, 3, u64::MAX), HERE, None)
        .unwrap();
    assert_eq!(delegates.pending(), pending);
}

#[test]
fn one_pool_id_on_two_contracts_is_two_pools() {
    let mut delegates = Delegates::default();
    let held = claim(&mut delegates, 1, u64::MAX);
    // Before the chain read, a pool id taken on one contract alone still
    // admits: the read may name the other.
    assert_eq!(
        delegates.admits(&grant(held, 1, u64::MAX), &[HERE, THERE]),
        Ok(())
    );
    delegates
        .register(grant(held, 1, u64::MAX), THERE, None)
        .unwrap();
    assert_eq!(
        delegates.admits(&grant(held, 1, u64::MAX), &[HERE, THERE]),
        Err(Refusal::PoolTaken)
    );
    assert_eq!(
        delegates.register(grant(held, 1, u64::MAX), THERE, None),
        Err(Refusal::PoolTaken)
    );
    // A funding stop on one contract retires that capability alone, and each
    // pays through its own contract.
    delegates.retire(held, HERE, B256::repeat_byte(1));
    let paying = delegates.paying(0, MARGIN, None).unwrap();
    assert_eq!(paying.pool, THERE);
    let lanes = delegates.lanes();
    assert_eq!(lanes.len(), 2);
    assert!(lanes.contains(&(HERE, B256::repeat_byte(1), held)));
    assert!(lanes.contains(&(THERE, B256::repeat_byte(1), held)));
}

#[test]
fn a_key_the_gateway_does_not_hold_is_refused() {
    let mut delegates = Delegates::default();
    let stranger = Address::repeat_byte(9);
    assert_eq!(
        delegates.admits(&grant(stranger, 1, u64::MAX), &[HERE]),
        Err(Refusal::UnknownKey)
    );
    assert_eq!(
        delegates.register(grant(stranger, 1, u64::MAX), HERE, None),
        Err(Refusal::UnknownKey)
    );
    assert!(delegates.list().is_empty());
}

#[test]
fn the_earliest_registered_capability_pays_first() {
    let mut delegates = Delegates::default();
    let first = claim(&mut delegates, 1, u64::MAX);
    let second = claim(&mut delegates, 2, u64::MAX);
    assert_eq!(payer(&delegates, 0, None), Some(first));
    // A funding stop retires it, and the next one pays.
    delegates.retire(first, HERE, B256::repeat_byte(1));
    let paying = delegates.paying(0, MARGIN, None).unwrap();
    assert_eq!(paying.signer.address(), second);
    assert_eq!(paying.grant.pool_id, B256::repeat_byte(2));
    delegates.retire(second, HERE, B256::repeat_byte(2));
    assert!(delegates.paying(0, MARGIN, None).is_none());
    // Both stay listed, as spent.
    assert!(delegates.list().iter().all(|c| c.spent));
}

#[test]
fn a_capability_inside_the_expiry_margin_pays_no_more() {
    let mut delegates = Delegates::default();
    let soon = claim(&mut delegates, 1, 1_000);
    let later = claim(&mut delegates, 2, 10_000);
    assert_eq!(payer(&delegates, 1_000 - MARGIN - 1, None), Some(soon));
    assert_eq!(payer(&delegates, 1_000 - MARGIN, None), Some(later));
    assert_eq!(payer(&delegates, 10_000 - MARGIN, None), None);
}

#[test]
fn a_scoped_capability_pays_only_for_its_namespaces() {
    let mut delegates = Delegates::default();
    let scoped = claim_scoped(&mut delegates, 1, u64::MAX, Some(vec![7, 9]));
    assert_eq!(payer(&delegates, 0, Some(7)), Some(scoped));
    assert_eq!(payer(&delegates, 0, Some(9)), Some(scoped));
    assert_eq!(payer(&delegates, 0, Some(8)), None, "another namespace");
    assert_eq!(payer(&delegates, 0, None), None, "no pointer at all");
}

#[test]
fn a_matching_scoped_capability_pays_before_an_unscoped_one() {
    let mut delegates = Delegates::default();
    let unscoped = claim(&mut delegates, 1, u64::MAX);
    let scoped = claim_scoped(&mut delegates, 2, u64::MAX, Some(vec![7]));
    assert_eq!(payer(&delegates, 0, Some(7)), Some(scoped));
    assert_eq!(payer(&delegates, 0, Some(8)), Some(unscoped));
    assert_eq!(payer(&delegates, 0, None), Some(unscoped));
    // Once the scoped one runs out, the unscoped one pays for its namespace.
    delegates.retire(scoped, HERE, B256::repeat_byte(2));
    assert_eq!(payer(&delegates, 0, Some(7)), Some(unscoped));
}

#[test]
fn the_list_names_a_scoped_capabilitys_namespaces() {
    let mut delegates = Delegates::default();
    claim(&mut delegates, 1, u64::MAX);
    claim_scoped(&mut delegates, 2, u64::MAX, Some(vec![7]));
    let listed: Vec<_> = delegates.list().into_iter().map(|c| c.namespaces).collect();
    assert_eq!(listed, [None, Some(vec![7])]);
}

#[test]
fn prune_drops_expired_capabilities_and_keys_left_with_none() {
    let mut delegates = Delegates::default();
    let key = claim(&mut delegates, 1, 100);
    delegates.register(grant(key, 2, 200), HERE, None).unwrap();
    let pending = delegates.pending();
    delegates.prune(150);
    assert_eq!(delegates.list().len(), 1);
    delegates.prune(200);
    assert!(delegates.list().is_empty());
    assert_eq!(
        delegates.admits(&grant(key, 3, u64::MAX), &[HERE]),
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
    delegates
        .register(grant(a, 3, u64::MAX), HERE, None)
        .unwrap();
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
        (HERE, B256::repeat_byte(1), a),
        (HERE, B256::repeat_byte(2), b),
        (HERE, B256::repeat_byte(3), a),
    ];
    expected.sort();
    assert_eq!(lanes, expected);
}
