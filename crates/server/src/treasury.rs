//! `Treasury`: the on-chain `PaymentPool` operations the sponsor needs — read
//! the pool's remaining balance, top it up from the hot wallet, and read its
//! owner (a boot-time sanity check). Mocked by `FakeTreasury` in the HTTP
//! contract tests; backed by `DecdnTreasury` in production.

use alloy::primitives::{Address, B256, U256};
use alloy::providers::Provider;
use alloy::signers::local::PrivateKeySigner;
use async_trait::async_trait;
use decdn_client_pull::buyer_pool::{ensure_allowance, top_up};
use decdn_client_pull::provider::build_provider;
use decdn_incentive::payment_pool::PaymentPool;

use crate::money::MicroUsdc;

#[async_trait]
pub trait Treasury: Send + Sync {
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
}

#[derive(Clone, Debug)]
pub struct TreasuryConfig {
    pub rpc_url: String,
    pub payment_pool: Address,
    pub chain_id: u64,
    pub signer: PrivateKeySigner,
}

pub struct DecdnTreasury<P: Provider + Clone> {
    contract: PaymentPool::PaymentPoolInstance<P>,
    provider: P,
    self_address: Address,
    token: Address,
    payment_pool: Address,
}

/// Build a `DecdnTreasury` from `cfg`: a wallet-filled provider signing as
/// `cfg.signer`, bound to the `PaymentPool` at `cfg.payment_pool`, with the
/// settlement token read from the contract's immutable `usdc()`.
pub async fn connect(cfg: &TreasuryConfig) -> anyhow::Result<Box<dyn Treasury>> {
    let signer = cfg.signer.clone();
    let self_address = signer.address();
    let provider = build_provider(&cfg.rpc_url, &signer)?;
    let contract = PaymentPool::new(cfg.payment_pool, provider.clone());
    let token = contract
        .usdc()
        .call()
        .await
        .map_err(|e| anyhow::anyhow!("read PaymentPool.usdc(): {e}"))?;
    Ok(Box::new(DecdnTreasury {
        contract,
        provider,
        self_address,
        token,
        payment_pool: cfg.payment_pool,
    }))
}

#[async_trait]
impl<P: Provider + Clone + 'static> Treasury for DecdnTreasury<P> {
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
        let credited = top_up(&self.contract, pool_id, amount).await?;
        Ok(MicroUsdc(u64::try_from(credited).unwrap_or(u64::MAX)))
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
}
