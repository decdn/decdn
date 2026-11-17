use alloy::primitives::Address;
use decdn_incentive::Deployment;

use super::{ChainConfig, GovernanceChain, PaymentPoolChain};

/// The first payment pool of `chain`, and passes that pay every node through
/// it.
pub(crate) fn first_pass(
    chain: &ChainConfig,
) -> anyhow::Result<(PaymentPoolChain, crate::pool_route::PoolPasses)> {
    let pool = chain.first_pool()?.clone();
    let passes =
        crate::pool_route::PoolPasses::new(crate::pool_route::PoolRoute::single(pool.deployment));
    passes.start(pool.deployment);
    Ok((pool, passes))
}

/// A chain config whose governance chain and one payment pool share
/// `rpc_url` and `chain_id`.
pub(crate) fn one_chain(
    rpc_url: String,
    chain_id: u64,
    payment_pool: Address,
    slash_judge: Address,
    capacity_bond: Option<Address>,
) -> ChainConfig {
    ChainConfig {
        governance: GovernanceChain {
            rpc_url: rpc_url.clone(),
            chain_id,
            slash_judge,
            capacity_bond,
        },
        payment_pools: vec![PaymentPoolChain {
            rpc_url,
            deployment: Deployment {
                chain_id,
                payment_pool,
            },
        }],
    }
}
