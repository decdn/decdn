//! `Sponsor`: the embedding surface. Holds the owner key's issuer and the
//! pool, checks a signer's on-chain registration before signing, and hands
//! out the pool keeper for the caller to spawn.
//!
//! The first capability redeemed for a signer fixes its terms on-chain for
//! good (`PaymentPool._registerCapability` is a no-op afterwards). So for a
//! registered signer `issue` re-signs the registered terms, and an expired
//! registration is an error. Requested terms are validated on every call.

use std::future::Future;
use std::sync::Arc;

use alloy::network::TxSigner;
use alloy::primitives::{Address, B256, Signature};
use alloy::signers::Signer;
use decdn_incentive::voucher_domain;
use sponsord_api::daemon::{Info, IssueResponse};
use tokio_util::sync::CancellationToken;

use crate::issuer::{Issuer, Limits, TermsError, TermsRequest};
use crate::keeper::{self, KeeperConfig, KeeperStatus};
use crate::pool::{ChainPool, PoolChain};

/// The chain, contract and pool a sponsor signs for. `Debug` shows the RPC
/// URL's length, not the URL: it often carries an API key (#38).
#[derive(Clone, PartialEq, Eq)]
pub struct ChainConfig {
    /// JSON-RPC endpoint of the chain the pool is on.
    pub rpc_url: String,
    /// EIP-155 chain id, part of the capability's EIP-712 domain.
    pub chain_id: u64,
    /// The `PaymentPool` contract.
    pub payment_pool: Address,
    /// The sponsor's pool in it, opened out-of-band with `decdn pool open`.
    pub pool_id: B256,
}

/// Why [`Sponsor::issue`] gave no capability.
#[derive(Debug, thiserror::Error)]
pub enum SponsorError {
    /// The requested terms are zero or above the maximum.
    #[error(transparent)]
    Terms(#[from] TermsError),
    /// The signer is registered on-chain and its registration has expired.
    #[error("signer registration expired at {expiry}")]
    SignerExpired {
        /// Unix time, in seconds, the registration expired at.
        expiry: u64,
    },
    /// Reading the signer's registration from the chain failed.
    #[error("read signer authorization: {0:#}")]
    Chain(anyhow::Error),
    /// The signer failed to sign the capability.
    #[error("{0:#}")]
    Sign(anyhow::Error),
}

impl std::fmt::Debug for ChainConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ChainConfig")
            .field(
                "rpc_url",
                &format_args!("<{} characters>", self.rpc_url.len()),
            )
            .field("chain_id", &self.chain_id)
            .field("payment_pool", &self.payment_pool)
            .field("pool_id", &self.pool_id)
            .finish()
    }
}

/// Issues capabilities against one pool and runs its keeper. Built with
/// [`Sponsor::connect`], or [`Sponsor::new`] from parts.
pub struct Sponsor {
    issuer: Issuer,
    pool: Arc<dyn PoolChain>,
    chain_id: u64,
    payment_pool: Address,
    keeper: Arc<KeeperStatus>,
}

impl std::fmt::Debug for Sponsor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Sponsor")
            .field("issuer", &self.issuer)
            .field("chain_id", &self.chain_id)
            .field("payment_pool", &self.payment_pool)
            .finish_non_exhaustive()
    }
}

impl Sponsor {
    /// Connect to the pool with `signer` (the pool owner, which also funds
    /// top-ups) and run the boot owner check.
    ///
    /// `signer` is any cloneable alloy signer that signs raw hashes and
    /// transactions: a local key (`PrivateKeySigner`), or a remote one such
    /// as AWS or GCP KMS. Hardware wallets don't qualify yet: alloy's
    /// `LedgerSigner` signs only typed data and isn't `Clone`.
    ///
    /// # Errors
    ///
    /// RPC failure or owner-check failure.
    pub async fn connect<S>(signer: S, chain: ChainConfig, limits: Limits) -> anyhow::Result<Self>
    where
        S: Signer + TxSigner<Signature> + Clone + Send + Sync + 'static,
    {
        let pool = ChainPool::connect(&chain.rpc_url, chain.payment_pool, signer.clone()).await?;
        let issuer = Issuer::new(
            signer,
            voucher_domain(chain.chain_id, chain.payment_pool),
            chain.pool_id,
            limits,
        );
        Self::new(issuer, Arc::new(pool), chain.chain_id, chain.payment_pool).await
    }

    /// Assemble from built parts and run the boot owner check: the signing
    /// key must own the pool on-chain, else every capability it signs is
    /// worthless (the node recovers a non-owner).
    ///
    /// # Errors
    ///
    /// Owner read failure or owner mismatch.
    pub async fn new(
        issuer: Issuer,
        pool: Arc<dyn PoolChain>,
        chain_id: u64,
        payment_pool: Address,
    ) -> anyhow::Result<Self> {
        let owner = pool.pool_owner(issuer.pool_id()).await?;
        anyhow::ensure!(
            owner == issuer.owner_address(),
            "pool {} is owned on-chain by {owner}, not the capability-signing wallet {}: \
             wrong pool id, keystore, or contract",
            issuer.pool_id(),
            issuer.owner_address()
        );
        Ok(Self {
            issuer,
            pool,
            chain_id,
            payment_pool,
            keeper: Arc::default(),
        })
    }

    /// The chain, `PaymentPool` and maximum terms, as `GET /v1/info` reports
    /// them.
    #[must_use]
    pub const fn info(&self) -> Info {
        let limits = self.issuer.limits();
        Info {
            chain_id: self.chain_id,
            payment_pool: self.payment_pool,
            max_spending_cap: limits.max_spending_cap,
            max_ttl_secs: limits.max_ttl_secs,
        }
    }

    /// Hand `signer` a capability: the existing one if it is registered and
    /// unexpired, else a fresh one with the requested terms.
    ///
    /// # Errors
    ///
    /// `Terms` for out-of-bounds requested terms (checked first, on every
    /// call), `Chain` if the registration read fails, `SignerExpired` for an
    /// expired registration, `Sign` for a signer failure.
    pub async fn issue(
        &self,
        signer: Address,
        req: &TermsRequest,
        now_unix: u64,
    ) -> Result<IssueResponse, SponsorError> {
        let terms = self.issuer.terms(req)?;
        let registered = self
            .pool
            .authorization(self.issuer.pool_id(), signer)
            .await
            .map_err(SponsorError::Chain)?;
        let (cap, expiry, registered) = match registered {
            None => (
                terms.spending_cap,
                now_unix.saturating_add(terms.ttl_secs),
                false,
            ),
            Some(a) if a.expiry > now_unix => (a.spending_cap, a.expiry, true),
            Some(a) => return Err(SponsorError::SignerExpired { expiry: a.expiry }),
        };
        let capability = self
            .issuer
            .sign(signer, cap, expiry)
            .await
            .map_err(SponsorError::Sign)?;
        Ok(IssueResponse {
            capability,
            registered,
        })
    }

    /// The pool keeper, for the caller to spawn. It runs until `shutdown` is
    /// cancelled and publishes its progress in [`keeper_status`](Self::keeper_status).
    pub fn keeper(
        &self,
        cfg: KeeperConfig,
        shutdown: CancellationToken,
    ) -> impl Future<Output = ()> + Send + 'static {
        keeper::run(
            self.pool.clone(),
            self.issuer.pool_id(),
            cfg,
            self.keeper.clone(),
            shutdown,
        )
    }

    /// What the keeper has seen and done so far.
    #[must_use]
    pub fn keeper_status(&self) -> &KeeperStatus {
        &self.keeper
    }
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]
mod tests;
