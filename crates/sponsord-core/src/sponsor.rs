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
    pub rpc_url: String,
    pub chain_id: u64,
    /// The `PaymentPool` contract.
    pub payment_pool: Address,
    /// The sponsor's pool in it, opened out-of-band with `decdn pool open`.
    pub pool_id: B256,
}

#[derive(Debug, thiserror::Error)]
pub enum SponsorError {
    #[error(transparent)]
    Terms(#[from] TermsError),
    #[error("signer registration expired at {expiry}")]
    SignerExpired { expiry: u64 },
    #[error("read signer authorization: {0:#}")]
    Chain(anyhow::Error),
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

pub struct Sponsor {
    issuer: Issuer,
    pool: Arc<dyn PoolChain>,
    chain_id: u64,
    payment_pool: Address,
    keeper: Arc<KeeperStatus>,
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

    #[must_use]
    pub fn info(&self) -> Info {
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
mod tests {
    use alloy::signers::local::PrivateKeySigner;
    use decdn_incentive::CapabilityGrant;
    use sponsord_api::MicroUsdc;

    use super::*;
    use crate::pool::Authorization;
    use crate::test_support::{
        FakePool, TEST_CHAIN_ID, TEST_PAYMENT_POOL, TEST_POOL_ID, fake_sponsor,
    };

    const NOW: u64 = 1_769_904_000;
    const SIGNER: Address = Address::repeat_byte(0xaa);

    fn req(cap: Option<u64>, ttl: Option<u64>) -> TermsRequest {
        TermsRequest {
            spending_cap: cap.map(MicroUsdc),
            ttl_secs: ttl,
        }
    }

    fn auth(cap: u64, expiry: u64) -> Authorization {
        Authorization {
            spending_cap: MicroUsdc(cap),
            expiry,
        }
    }

    #[tokio::test]
    async fn new_rejects_a_pool_owned_by_another_key() {
        let issuer = Issuer::new(
            PrivateKeySigner::random(),
            voucher_domain(TEST_CHAIN_ID, TEST_PAYMENT_POOL),
            TEST_POOL_ID,
            Limits {
                max_spending_cap: MicroUsdc(5_000_000),
                max_ttl_secs: 172_800,
            },
        );
        let pool = Arc::new(FakePool::new(Address::repeat_byte(0x99), 0));
        let err = Sponsor::new(issuer, pool, TEST_CHAIN_ID, TEST_PAYMENT_POOL)
            .await
            .err()
            .unwrap();
        assert!(err.to_string().contains("owned on-chain by"), "{err}");
    }

    #[tokio::test]
    async fn unregistered_signer_gets_requested_terms() {
        let (sponsor, pool) = fake_sponsor(5_000_000, 172_800).await;
        let issued = sponsor
            .issue(SIGNER, &req(Some(1_000_000), Some(3_600)), NOW)
            .await
            .unwrap();
        assert!(!issued.registered);
        assert_eq!(issued.capability.spending_cap, MicroUsdc(1_000_000));
        assert_eq!(issued.capability.expiry, NOW + 3_600);
        let grant = CapabilityGrant::from_token(&issued.capability.token).unwrap();
        assert_eq!(grant.signer, SIGNER);
        assert_eq!(grant.spending_cap, 1_000_000);
        assert_eq!(
            grant
                .owner(&voucher_domain(TEST_CHAIN_ID, TEST_PAYMENT_POOL))
                .unwrap(),
            pool.owner_address()
        );
    }

    #[tokio::test]
    async fn omitted_terms_default_to_the_maximum() {
        let (sponsor, _) = fake_sponsor(5_000_000, 172_800).await;
        let issued = sponsor.issue(SIGNER, &req(None, None), NOW).await.unwrap();
        assert_eq!(issued.capability.spending_cap, MicroUsdc(5_000_000));
        assert_eq!(issued.capability.expiry, NOW + 172_800);
    }

    #[tokio::test]
    async fn registered_signer_gets_its_existing_capability_back() {
        let (sponsor, pool) = fake_sponsor(5_000_000, 172_800).await;
        let first = sponsor
            .issue(SIGNER, &req(Some(2_000_000), None), NOW)
            .await
            .unwrap()
            .capability;
        pool.register(SIGNER, auth(first.spending_cap.0, first.expiry));

        // Asking again, for other terms, later: the registered terms win and
        // the token is byte-identical to the one first issued.
        let again = sponsor
            .issue(SIGNER, &req(Some(4_000_000), Some(60)), NOW + 100)
            .await
            .unwrap();
        assert!(again.registered);
        assert_eq!(again.capability, first);
    }

    #[tokio::test]
    async fn expired_registration_is_an_error() {
        let (sponsor, pool) = fake_sponsor(5_000_000, 172_800).await;
        pool.register(SIGNER, auth(5_000_000, NOW));
        // expiry == now counts as expired.
        let err = sponsor
            .issue(SIGNER, &req(None, None), NOW)
            .await
            .err()
            .unwrap();
        assert!(matches!(err, SponsorError::SignerExpired { expiry } if expiry == NOW));
    }

    #[tokio::test]
    async fn bad_terms_are_rejected_even_for_a_registered_signer() {
        let (sponsor, pool) = fake_sponsor(5_000_000, 172_800).await;
        pool.register(SIGNER, auth(5_000_000, NOW + 1_000));
        let err = sponsor
            .issue(SIGNER, &req(Some(5_000_001), None), NOW)
            .await
            .err()
            .unwrap();
        assert!(matches!(
            err,
            SponsorError::Terms(TermsError::ExceedsMax { .. })
        ));
        let err = sponsor
            .issue(SIGNER, &req(Some(0), None), NOW)
            .await
            .err()
            .unwrap();
        assert!(matches!(err, SponsorError::Terms(TermsError::Zero { .. })));
    }

    #[tokio::test]
    async fn failed_registration_read_is_a_chain_error() {
        let (sponsor, pool) = fake_sponsor(5_000_000, 172_800).await;
        pool.fail_authorization_reads(true);
        let err = sponsor
            .issue(SIGNER, &req(None, None), NOW)
            .await
            .err()
            .unwrap();
        assert!(matches!(err, SponsorError::Chain(_)));
    }

    #[tokio::test]
    async fn info_reports_chain_and_maximums() {
        let (sponsor, _) = fake_sponsor(5_000_000, 172_800).await;
        assert_eq!(
            sponsor.info(),
            Info {
                chain_id: TEST_CHAIN_ID,
                payment_pool: TEST_PAYMENT_POOL,
                max_spending_cap: MicroUsdc(5_000_000),
                max_ttl_secs: 172_800,
            }
        );
    }
}
