use alloy::signers::local::PrivateKeySigner;

use super::*;

#[test]
fn a_capability_must_be_signed_by_its_pools_on_chain_owner() {
    let owner = PrivateKeySigner::random().address();
    let other = PrivateKeySigner::random().address();
    let pool = alloy::primitives::B256::ZERO;
    assert!(owner_matches(owner, owner, pool).is_ok());
    assert!(owner_matches(owner, other, pool).is_err());
    assert!(owner_matches(Address::ZERO, owner, pool).is_err());
}

/// Two configured pools on two payment chains, the capability's pool id, and
/// an owner key.
fn two_pools() -> (Vec<PaymentPoolChain>, PoolId, PrivateKeySigner) {
    let pool = |chain_id, byte| PaymentPoolChain {
        rpc_url: "http://127.0.0.1:1".to_owned(),
        deployment: Deployment {
            chain_id,
            payment_pool: Address::repeat_byte(byte),
        },
    };
    (
        vec![pool(421_614, 0xA1), pool(84_532, 0xB2)],
        PoolId::repeat_byte(0x42),
        PrivateKeySigner::random(),
    )
}

/// A capability for `pool_id` signed by `owner` under `deployment`'s domain.
fn grant_on(owner: &PrivateKeySigner, pool_id: PoolId, deployment: Deployment) -> CapabilityGrant {
    let signed = decdn_incentive::Capability {
        signer: Address::repeat_byte(0x55),
        spending_cap: 1_000,
        pool_id,
        expiry: u64::MAX,
    }
    .sign(owner, &deployment.voucher_domain())
    .unwrap();
    CapabilityGrant::from_signed_capability(&signed)
}

/// A capability signed for the second configured pool resolves to that pool:
/// its signature recovers to the owner only under that pool's domain, even
/// where the same pool id exists on the first pool under another owner.
#[tokio::test]
async fn a_capability_resolves_to_the_pool_whose_domain_it_was_signed_under() {
    let (pools, pool_id, owner) = two_pools();
    let second = pools[1].deployment;
    let grant = grant_on(&owner, pool_id, second);
    let first_owner = PrivateKeySigner::random().address();
    let owner_address = owner.address();
    let resolved = capability_pool(&grant, &pools, |pool, id| {
        assert_eq!(id, pool_id);
        let on_chain = if pool.deployment == second {
            owner_address
        } else {
            first_owner
        };
        async move { Ok(on_chain) }
    })
    .await;
    assert_eq!(resolved, Ok(second));
}

/// A capability no configured pool's owner signed resolves to no pool,
/// naming a pool that exists as not signed by its owner; one whose pool
/// exists on no configured contract is named as missing.
#[tokio::test]
async fn a_capability_no_configured_pool_owner_signed_is_refused() {
    let (pools, pool_id, owner) = two_pools();
    let elsewhere = Deployment {
        chain_id: 1,
        payment_pool: Address::repeat_byte(0xC3),
    };
    let grant = grant_on(&owner, pool_id, elsewhere);
    let owner_address = owner.address();
    let err = capability_pool(&grant, &pools, |_, _| async move { Ok(owner_address) })
        .await
        .unwrap_err();
    assert!(
        matches!(&err, CapabilityPoolError::NoMatch(text) if text.contains("not signed by the owner")),
        "{err}"
    );
    let err = capability_pool(&grant, &pools, |_, _| async { Ok(Address::ZERO) })
        .await
        .unwrap_err();
    assert!(
        matches!(&err, CapabilityPoolError::NoMatch(text) if text.contains("does not exist")),
        "{err}"
    );
}

/// A read that fails while no other pool matches is an unread pool; a pool
/// that matches resolves even when another pool's read fails.
#[tokio::test]
async fn an_unreadable_pool_fails_the_resolution_only_when_no_pool_matches() {
    let (pools, pool_id, owner) = two_pools();
    let second = pools[1].deployment;
    let grant = grant_on(&owner, pool_id, second);
    let owner_address = owner.address();
    let resolved = capability_pool(&grant, &pools, |pool, _| {
        let read = if pool.deployment == second {
            Ok(owner_address)
        } else {
            Err("connection refused".to_owned())
        };
        async move { read }
    })
    .await;
    assert_eq!(resolved, Ok(second));
    let err = capability_pool(&grant, &pools, |_, _| async {
        Err("connection refused".to_owned())
    })
    .await
    .unwrap_err();
    assert!(matches!(err, CapabilityPoolError::Unread(_)), "{err}");
}
