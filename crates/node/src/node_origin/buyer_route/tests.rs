use alloy::primitives::Address;
use anyhow::Context as _;

use super::*;

fn dep(chain_id: u64, last: u8) -> Deployment {
    Deployment {
        chain_id,
        payment_pool: Address::with_last_byte(last),
    }
}

fn hint(deployments: &[Deployment]) -> Vec<PoolAccount> {
    deployments.iter().map(|d| d.pool_account()).collect()
}

#[test]
fn the_buyer_pays_through_a_held_pool_first() {
    let (a, b) = (dep(1, 0xa), dep(2, 0xb));
    let held_b = BuyerPool::choose(&[a, b], |d| d == b);
    assert_eq!(held_b.map(|p| p.deployment()), Some(b));
    let none_held = BuyerPool::choose(&[a, b], |_| false);
    assert_eq!(none_held.map(|p| p.deployment()), Some(a));
    assert!(BuyerPool::choose(&[], |_| true).is_none());
}

#[test]
fn an_upstream_is_paid_only_on_a_pool_it_accepts() -> anyhow::Result<()> {
    let (a, b) = (dep(1, 0xa), dep(2, 0xb));
    let buyer = BuyerPool::choose(&[a, b], |_| false).context("a buyer pool")?;
    assert_eq!(buyer.deployment(), a);
    // A hint that lists the buyer pool.
    assert!(buyer.pays(Some(&hint(&[b, a]))));
    // A hint that lists only the other accepted pool: the route sends the
    // upstream there, and this buyer pool pays it nothing.
    assert!(!buyer.pays(Some(&hint(&[b]))));
    // No common pool.
    assert!(!buyer.pays(Some(&hint(&[dep(3, 0xc)]))));
    // No hint: the upstream accepts the first configured pool.
    assert!(buyer.pays(None));
    Ok(())
}

#[test]
fn a_buyer_on_a_held_later_pool_skips_upstreams_with_no_hint() -> anyhow::Result<()> {
    let (a, b) = (dep(1, 0xa), dep(2, 0xb));
    let buyer = BuyerPool::choose(&[a, b], |d| d == b).context("a buyer pool")?;
    assert!(buyer.pays(Some(&hint(&[a, b]))));
    // An upstream with no hint takes the first configured pool, not this
    // one.
    assert!(!buyer.pays(None));
    Ok(())
}
