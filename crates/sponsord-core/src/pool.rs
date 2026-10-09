//! `PoolChain`: the on-chain `PaymentPool` operations the sponsor needs. It
//! reads the pool's remaining balance, tops the pool up from the treasury
//! (the hot wallet that owns the pool), reads the pool's owner (a boot-time
//! sanity check), and reads a signer's registration. It also reads where a
//! broadcast transaction stands and the treasury's confirmed nonce, which
//! the keeper uses to settle a top-up it could not confirm. `ChainPool`
//! backs it in production; tests use `test_support::FakePool`.

use alloy::consensus::Transaction as _;
use alloy::network::{EthereumWallet, TxSigner};
use alloy::primitives::{Address, B256, Signature, TxHash, U256};
use alloy::providers::{DynProvider, Provider, ProviderBuilder};
use async_trait::async_trait;
use decdn_client::buyer_pool::{AllowanceShortfall, TopUpUnconfirmed, ensure_allowance, top_up};
use decdn_common::redact::strip_urls;
use decdn_incentive::payment_pool::PaymentPool;
use sponsord_api::MicroUsdc;

/// A signer's registration under a pool: the terms fixed by the first
/// capability redeemed for it (`PaymentPool.authorized[poolId][signer]`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Authorization {
    /// The most the signer may spend from the pool, in micro-USDC.
    pub spending_cap: MicroUsdc,
    /// Unix time, in seconds, at which the registration expires.
    pub expiry: u64,
}

/// Render a chain-call error and its causes as `a: b: c`, without the RPC URL
/// that reqwest's message names (#38). The causes carry the failure class
/// (connection refused, timeout, DNS, TLS) that the outer error's Display
/// leaves out. Each is stripped on its own, so a removed URL cannot take the
/// separator with it, and a cause that repeats the error before it (alloy's
/// transport error forwards reqwest's message) is skipped.
fn redacted(err: &(dyn std::error::Error + 'static)) -> String {
    redacted_parts(err).join(": ")
}

fn redacted_parts(err: &(dyn std::error::Error + 'static)) -> Vec<String> {
    let mut parts: Vec<String> = Vec::new();
    let mut next = Some(err);
    while let Some(e) = next {
        let part = strip_urls(&e.to_string()).into_owned();
        if parts.last() != Some(&part) {
            parts.push(part);
        }
        next = e.source();
    }
    parts
}

/// [`redacted`] for a `decdn_client` top-up error, keeping the typed markers
/// a caller downcasts on: [`TopUpUnconfirmed`] (the `topUp` was broadcast and
/// may have mined) and [`AllowanceShortfall`].
fn redacted_top_up(context: String, err: &anyhow::Error) -> anyhow::Error {
    let unconfirmed = err.downcast_ref::<TopUpUnconfirmed>().copied();
    let shortfall = err.downcast_ref::<AllowanceShortfall>().copied();
    // Each marker is re-attached below, so leave its text out of the cause.
    let markers = [
        unconfirmed.map(|m| m.to_string()),
        shortfall.map(|m| m.to_string()),
    ];
    let cause: Vec<String> = redacted_parts(err.as_ref())
        .into_iter()
        .filter(|part| !markers.iter().flatten().any(|m| m == part))
        .collect();
    let mut out = anyhow::Error::msg(cause.join(": "));
    if let Some(m) = shortfall {
        out = out.context(m);
    }
    if let Some(m) = unconfirmed {
        out = out.context(m);
    }
    out.context(context)
}

/// Where a broadcast transaction stands, as the RPC node sees it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TxState {
    /// It mined and succeeded.
    Mined,
    /// It mined and reverted, so it moved nothing.
    Reverted,
    /// The node knows the transaction but has no receipt for it yet.
    Pending {
        /// The nonce it was sent with.
        nonce: u64,
    },
    /// The node has neither the transaction nor a receipt: it was dropped,
    /// replaced, or never reached this node.
    Unknown,
}

/// A provider that sends as `signer` over `url`. Each transaction's nonce is
/// the node's pending count for the treasury, read when it is sent, rather
/// than alloy's default cached count, which never re-syncs: after a dropped
/// transaction it would leave every later one queued behind the unused
/// nonce, and after a transaction sent from elsewhere (the self-transfers
/// that clear a held top-up) it would reuse a spent nonce.
fn treasury_provider<S>(url: alloy::transports::http::reqwest::Url, signer: S) -> DynProvider
where
    S: TxSigner<Signature> + Send + Sync + 'static,
{
    ProviderBuilder::default()
        .with_gas_estimation()
        .with_simple_nonce_management()
        .fetch_chain_id()
        .wallet(EthereumWallet::new(signer))
        .connect_http(url)
        .erased()
}

/// The contract's unregistered state is a zero cap and zero expiry
/// (`PaymentPool._registerCapability`); anything else is a registration.
fn registration(cap: u64, expiry: u64) -> Option<Authorization> {
    (cap != 0 || expiry != 0).then_some(Authorization {
        spending_cap: MicroUsdc(cap),
        expiry,
    })
}

/// The `PaymentPool` reads and writes the sponsor and keeper make, as the
/// treasury. [`ChainPool`] is the on-chain implementation.
#[async_trait]
pub trait PoolChain: Send + Sync {
    /// The treasury wallet address, which must be the pool's on-chain owner.
    fn owner_address(&self) -> Address;

    /// The pool's still-payable balance (`deposit - totalRedeemed`).
    async fn remaining(&self, pool_id: B256) -> anyhow::Result<MicroUsdc>;

    /// Add `additional` USDC to the pool from the treasury; returns the
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

    /// Where the transaction `tx` stands.
    async fn transaction(&self, tx: TxHash) -> anyhow::Result<TxState>;

    /// The treasury's confirmed transaction count (its nonce at the latest
    /// block): every nonce below it has been used by a mined transaction.
    async fn confirmed_nonce(&self) -> anyhow::Result<u64>;
}

/// The `PaymentPool` contract, read through and paid into by the treasury
/// wallet.
pub struct ChainPool {
    contract: PaymentPool::PaymentPoolInstance<DynProvider>,
    provider: DynProvider,
    owner: Address,
    token: Address,
    payment_pool: Address,
}

// By hand: the provider holds the RPC URL, which can carry an API key.
impl std::fmt::Debug for ChainPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ChainPool")
            .field("owner", &self.owner)
            .field("token", &self.token)
            .field("payment_pool", &self.payment_pool)
            .finish_non_exhaustive()
    }
}

impl ChainPool {
    /// Connect to the `PaymentPool` at `payment_pool` over `rpc_url`, sending
    /// transactions as `signer`, and read its settlement token (the
    /// contract's immutable `usdc()`).
    ///
    /// # Errors
    ///
    /// A malformed `rpc_url`, or the `usdc()` read fails.
    pub async fn connect<S>(rpc_url: &str, payment_pool: Address, signer: S) -> anyhow::Result<Self>
    where
        S: TxSigner<Signature> + Send + Sync + 'static,
    {
        let owner = signer.address();
        // The URL stays out of every error `ChainPool` returns: RPC URLs often
        // carry an API key in their path or query. Here it is never formatted,
        // and chain-call errors go through `redacted` (#38).
        let url = rpc_url.parse().map_err(|e| {
            anyhow::anyhow!(
                "RPC URL ({} characters) is not a valid URL: {e}",
                rpc_url.len()
            )
        })?;
        let provider = treasury_provider(url, signer);
        let contract = PaymentPool::new(payment_pool, provider.clone());
        let token = contract
            .usdc()
            .call()
            .await
            .map_err(|e| anyhow::anyhow!("read PaymentPool.usdc(): {}", redacted(&e)))?;
        Ok(Self {
            contract,
            provider,
            owner,
            token,
            payment_pool,
        })
    }
}

#[async_trait]
impl PoolChain for ChainPool {
    fn owner_address(&self) -> Address {
        self.owner
    }

    async fn remaining(&self, pool_id: B256) -> anyhow::Result<MicroUsdc> {
        let pool = self
            .contract
            .getPool(pool_id)
            .call()
            .await
            .map_err(|e| anyhow::anyhow!("getPool({pool_id}): {}", redacted(&e)))?;
        Ok(MicroUsdc(pool.deposit.saturating_sub(pool.totalRedeemed)))
    }

    async fn top_up(&self, pool_id: B256, additional: MicroUsdc) -> anyhow::Result<MicroUsdc> {
        let amount = U256::from(additional.0);
        ensure_allowance(
            &self.provider,
            self.token,
            self.owner,
            self.payment_pool,
            Some(amount),
        )
        .await
        .map_err(|e| redacted_top_up("approve the top-up".to_owned(), &e))?;
        let topped = top_up(&self.contract, self.owner, pool_id, amount)
            .await
            .map_err(|e| redacted_top_up(format!("top up {pool_id}"), &e))?;
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
            .map_err(|e| anyhow::anyhow!("getPool({pool_id}): {}", redacted(&e)))?;
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
            .map_err(|e| {
                anyhow::anyhow!("getAuthorization({pool_id}, {signer}): {}", redacted(&e))
            })?;
        Ok(registration(a.cap, a.expiry))
    }

    async fn transaction(&self, tx: TxHash) -> anyhow::Result<TxState> {
        let receipt = self
            .provider
            .get_transaction_receipt(tx)
            .await
            .map_err(|e| anyhow::anyhow!("read receipt of {tx}: {}", redacted(&e)))?;
        if let Some(r) = receipt {
            return Ok(if r.status() {
                TxState::Mined
            } else {
                TxState::Reverted
            });
        }
        // A transaction with a block but no receipt yet is reported pending;
        // the next read sees its receipt.
        let found = self
            .provider
            .get_transaction_by_hash(tx)
            .await
            .map_err(|e| anyhow::anyhow!("read transaction {tx}: {}", redacted(&e)))?;
        Ok(found.map_or(TxState::Unknown, |t| TxState::Pending { nonce: t.nonce() }))
    }

    async fn confirmed_nonce(&self) -> anyhow::Result<u64> {
        self.provider
            .get_transaction_count(self.owner)
            .latest()
            .await
            .map_err(|e| anyhow::anyhow!("read the treasury's nonce: {}", redacted(&e)))
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests;
