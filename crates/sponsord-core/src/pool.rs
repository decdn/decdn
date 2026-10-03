//! `PoolChain`: the on-chain `PaymentPool` operations the sponsor needs. It
//! reads the pool's remaining balance, tops the pool up from the hot wallet,
//! reads the pool's owner (a boot-time sanity check), and reads a signer's
//! registration. `ChainPool` backs it in production; tests use
//! `test_support::FakePool`.

use alloy::primitives::{Address, B256, U256};
use alloy::providers::Provider;
use alloy::signers::local::PrivateKeySigner;
use async_trait::async_trait;
use decdn_client::buyer_pool::{ensure_allowance, top_up};
use decdn_client::provider::build_provider;
use decdn_incentive::payment_pool::PaymentPool;

use crate::MicroUsdc;

/// A signer's registration under a pool: the terms fixed by the first
/// capability redeemed for it (`PaymentPool.authorized[poolId][signer]`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Authorization {
    pub spending_cap: u64,
    pub expiry: u64,
}

/// The contract's unregistered state is a zero cap and zero expiry
/// (`PaymentPool._registerCapability`); anything else is a registration.
#[must_use]
fn registration(cap: u64, expiry: u64) -> Option<Authorization> {
    (cap != 0 || expiry != 0).then_some(Authorization {
        spending_cap: cap,
        expiry,
    })
}

#[async_trait]
pub trait PoolChain: Send + Sync {
    /// The hot wallet address, which must be the pool's on-chain owner.
    fn owner_address(&self) -> Address;

    /// The pool's still-payable balance (`deposit - totalRedeemed`).
    async fn remaining(&self, pool_id: B256) -> anyhow::Result<MicroUsdc>;

    /// Add `additional` USDC to the pool from the hot wallet; returns the
    /// credited amount.
    async fn top_up(&self, pool_id: B256, additional: MicroUsdc) -> anyhow::Result<MicroUsdc>;

    /// The pool's on-chain `owner` (used once at boot to confirm this wallet
    /// owns the configured pool).
    async fn pool_owner(&self, pool_id: B256) -> anyhow::Result<Address>;

    /// `signer`'s registration under `pool_id`, or `None` if no capability
    /// has been redeemed for it yet.
    async fn authorization(
        &self,
        pool_id: B256,
        signer: Address,
    ) -> anyhow::Result<Option<Authorization>>;
}

#[derive(Clone, Debug)]
pub struct ChainPoolConfig {
    pub rpc_url: String,
    pub payment_pool: Address,
    pub chain_id: u64,
    pub signer: PrivateKeySigner,
}

pub struct ChainPool<P: Provider + Clone> {
    contract: PaymentPool::PaymentPoolInstance<P>,
    provider: P,
    self_address: Address,
    token: Address,
    payment_pool: Address,
}

/// Build a `ChainPool` from `cfg`: a wallet-filled provider signing as
/// `cfg.signer`, bound to the `PaymentPool` at `cfg.payment_pool`, with the
/// settlement token read from the contract's immutable `usdc()`.
pub async fn connect(cfg: &ChainPoolConfig) -> anyhow::Result<Box<dyn PoolChain>> {
    let signer = cfg.signer.clone();
    let self_address = signer.address();
    let provider = build_provider(&cfg.rpc_url, &signer)?;
    let contract = PaymentPool::new(cfg.payment_pool, provider.clone());
    let token = contract
        .usdc()
        .call()
        .await
        .map_err(|e| anyhow::anyhow!("read PaymentPool.usdc(): {e}"))?;
    Ok(Box::new(ChainPool {
        contract,
        provider,
        self_address,
        token,
        payment_pool: cfg.payment_pool,
    }))
}

#[async_trait]
impl<P: Provider + Clone + 'static> PoolChain for ChainPool<P> {
    fn owner_address(&self) -> Address {
        self.self_address
    }

    async fn remaining(&self, pool_id: B256) -> anyhow::Result<MicroUsdc> {
        let pool = self
            .contract
            .getPool(pool_id)
            .call()
            .await
            .map_err(|e| anyhow::anyhow!("getPool({pool_id}): {e}"))?;
        Ok(MicroUsdc(pool.deposit.saturating_sub(pool.totalRedeemed)))
    }

    async fn top_up(&self, pool_id: B256, additional: MicroUsdc) -> anyhow::Result<MicroUsdc> {
        let amount = U256::from(additional.0);
        ensure_allowance(
            &self.provider,
            self.token,
            self.self_address,
            self.payment_pool,
            Some(amount),
        )
        .await?;
        let topped = top_up(&self.contract, pool_id, amount).await?;
        Ok(MicroUsdc(
            u64::try_from(topped.credited).unwrap_or(u64::MAX),
        ))
    }

    async fn pool_owner(&self, pool_id: B256) -> anyhow::Result<Address> {
        let pool = self
            .contract
            .getPool(pool_id)
            .call()
            .await
            .map_err(|e| anyhow::anyhow!("getPool({pool_id}): {e}"))?;
        Ok(pool.owner)
    }

    async fn authorization(
        &self,
        pool_id: B256,
        signer: Address,
    ) -> anyhow::Result<Option<Authorization>> {
        let a = self
            .contract
            .getAuthorization(pool_id, signer)
            .call()
            .await
            .map_err(|e| anyhow::anyhow!("getAuthorization({pool_id}, {signer}): {e}"))?;
        Ok(registration(a.cap, a.expiry))
    }
}

#[cfg(test)]
mod tests {
    use super::{Authorization, registration};

    #[test]
    fn zero_cap_and_expiry_is_unregistered() {
        assert_eq!(registration(0, 0), None);
        assert_eq!(
            registration(5, 10),
            Some(Authorization {
                spending_cap: 5,
                expiry: 10
            })
        );
        assert_eq!(
            registration(0, 10),
            Some(Authorization {
                spending_cap: 0,
                expiry: 10
            })
        );
    }
}
