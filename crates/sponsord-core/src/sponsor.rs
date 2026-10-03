//! `Sponsor`: the embedding surface. Holds the owner key's issuer and the
//! pool, checks a signer's on-chain registration before signing,
//! and hands out the pool keeper for the caller to spawn.
//!
//! The first capability redeemed for a signer fixes its terms on-chain for
//! good (`PaymentPool._registerCapability` is a no-op afterwards). So for a
//! registered signer `issue` re-signs the registered terms, which yields the
//! token first issued (signing is deterministic), and an expired
//! registration is an error. Requested terms are validated on every call.

use std::future::Future;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use alloy::primitives::{Address, B256};
use decdn_incentive::voucher_domain;

use crate::issuer::{Issuer, TermsError};
use crate::keeper;
use crate::money::MicroUsdc;
use crate::pool::{self, ChainPoolConfig, PoolChain};

/// Everything `Sponsor::connect` needs.
#[derive(Clone)]
pub struct SponsorConfig {
    pub rpc_url: String,
    pub chain_id: u64,
    pub payment_pool: Address,
    pub pool_id: B256,
    pub treasury_keystore: PathBuf,
    pub treasury_password: String,
    pub max_spending_cap: u64,
    pub max_ttl_secs: u64,
}

impl std::fmt::Debug for SponsorConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SponsorConfig")
            .field("rpc_url", &self.rpc_url)
            .field("chain_id", &self.chain_id)
            .field("payment_pool", &self.payment_pool)
            .field("pool_id", &self.pool_id)
            .field("treasury_keystore", &self.treasury_keystore)
            .field("treasury_password", &"<redacted>")
            .field("max_spending_cap", &self.max_spending_cap)
            .field("max_ttl_secs", &self.max_ttl_secs)
            .finish()
    }
}

/// What a caller needs to know about this sponsor.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Info {
    pub chain_id: u64,
    pub payment_pool: Address,
    pub max_spending_cap: u64,
    pub max_ttl_secs: u64,
}

/// A capability handed to a caller, and whether its terms come from an
/// existing on-chain registration.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Issued {
    pub token: String,
    pub spending_cap: u64,
    pub expiry: u64,
    pub registered: bool,
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

pub struct Sponsor {
    issuer: Issuer,
    pool: Arc<dyn PoolChain>,
    chain_id: u64,
    payment_pool: Address,
}

impl Sponsor {
    /// Load the owner keystore, connect to the pool, and run the boot owner
    /// check.
    ///
    /// # Errors
    ///
    /// Keystore, RPC, or owner-check failure.
    pub async fn connect(cfg: SponsorConfig) -> anyhow::Result<Self> {
        let ks = cfg.treasury_keystore.clone();
        let pw = cfg.treasury_password.clone();
        let signer = tokio::task::spawn_blocking(move || {
            decdn_incentive::eth_identity::load_signer(&ks, &pw)
        })
        .await??;
        let pool = pool::connect(&ChainPoolConfig {
            rpc_url: cfg.rpc_url.clone(),
            payment_pool: cfg.payment_pool,
            chain_id: cfg.chain_id,
            signer: signer.clone(),
        })
        .await?;
        let issuer = Issuer::new(
            signer,
            voucher_domain(cfg.chain_id, cfg.payment_pool),
            cfg.pool_id,
            cfg.max_spending_cap,
            cfg.max_ttl_secs,
        );
        Self::from_parts(issuer, Arc::from(pool), cfg.chain_id, cfg.payment_pool).await
    }

    /// Assemble from built parts and run the boot owner check: the signing
    /// key must own the pool on-chain, else every capability it signs is
    /// worthless (the node recovers a non-owner).
    ///
    /// # Errors
    ///
    /// Owner read failure or owner mismatch.
    pub async fn from_parts(
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
        })
    }

    #[must_use]
    pub fn info(&self) -> Info {
        Info {
            chain_id: self.chain_id,
            payment_pool: self.payment_pool,
            max_spending_cap: self.issuer.max_spending_cap(),
            max_ttl_secs: self.issuer.max_ttl_secs(),
        }
    }

    /// Hand `signer` a capability: the existing one if it is registered and
    /// unexpired, else a fresh one with the requested terms (omitted = the
    /// maximum).
    ///
    /// # Errors
    ///
    /// `Terms` for out-of-bounds requested terms (checked first, on every
    /// call), `Chain` if the registration read fails, `SignerExpired` for an
    /// expired registration, `Sign` for a signer failure.
    pub async fn issue(
        &self,
        signer: Address,
        spending_cap: Option<u64>,
        ttl_secs: Option<u64>,
        now_unix: u64,
    ) -> Result<Issued, SponsorError> {
        let (cap, ttl) = self.issuer.terms(spending_cap, ttl_secs)?;
        let registered = self
            .pool
            .authorization(self.issuer.pool_id(), signer)
            .await
            .map_err(SponsorError::Chain)?;
        let (cap, expiry, registered) = match registered {
            None => (cap, now_unix.saturating_add(ttl), false),
            Some(a) if a.expiry > now_unix => (a.spending_cap, a.expiry, true),
            Some(a) => return Err(SponsorError::SignerExpired { expiry: a.expiry }),
        };
        let signed = self
            .issuer
            .sign(signer, cap, expiry)
            .map_err(SponsorError::Sign)?;
        Ok(Issued {
            token: signed.token,
            spending_cap: signed.spending_cap,
            expiry: signed.expiry,
            registered,
        })
    }

    /// The pool top-up loop, for the caller to spawn.
    pub fn pool_keeper(
        &self,
        low_water: MicroUsdc,
        refill: MicroUsdc,
        interval: Duration,
    ) -> impl Future<Output = ()> + Send + 'static {
        keeper::run(
            self.pool.clone(),
            interval,
            low_water,
            refill,
            self.issuer.pool_id(),
        )
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
    use std::sync::Arc;

    use alloy::primitives::Address;
    use alloy::signers::local::PrivateKeySigner;
    use decdn_incentive::{CapabilityGrant, voucher_domain};

    use super::*;
    use crate::issuer::{Issuer, TermsError};
    use crate::pool::{Authorization, PoolChain};
    use crate::test_support::{
        FakePool, TEST_CHAIN_ID, TEST_PAYMENT_POOL, TEST_POOL_ID, fake_sponsor,
    };

    const NOW: u64 = 1_769_904_000;
    const SIGNER: Address = Address::repeat_byte(0xaa);

    #[test]
    fn debug_redacts_the_treasury_password() {
        let cfg = SponsorConfig {
            rpc_url: "http://localhost:8545".into(),
            chain_id: TEST_CHAIN_ID,
            payment_pool: TEST_PAYMENT_POOL,
            pool_id: TEST_POOL_ID,
            treasury_keystore: PathBuf::from("/tmp/ks.json"),
            treasury_password: "hunter2-secret".into(),
            max_spending_cap: 5_000_000,
            max_ttl_secs: 172_800,
        };
        let shown = format!("{cfg:?}");
        assert!(!shown.contains("hunter2-secret"));
        assert!(shown.contains("<redacted>"));
    }

    #[tokio::test]
    async fn from_parts_rejects_a_pool_owned_by_another_key() {
        let issuer = Issuer::new(
            PrivateKeySigner::random(),
            voucher_domain(TEST_CHAIN_ID, TEST_PAYMENT_POOL),
            TEST_POOL_ID,
            5_000_000,
            172_800,
        );
        let pool = Arc::new(FakePool::new(Address::repeat_byte(0x99), 0));
        let err = Sponsor::from_parts(
            issuer,
            pool as Arc<dyn PoolChain>,
            TEST_CHAIN_ID,
            TEST_PAYMENT_POOL,
        )
        .await
        .err()
        .unwrap();
        assert!(err.to_string().contains("owned on-chain by"), "{err}");
    }

    #[tokio::test]
    async fn unregistered_signer_gets_requested_terms() {
        let (sponsor, pool) = fake_sponsor(5_000_000, 172_800).await;
        let issued = sponsor
            .issue(SIGNER, Some(1_000_000), Some(3_600), NOW)
            .await
            .unwrap();
        assert!(!issued.registered);
        assert_eq!(issued.spending_cap, 1_000_000);
        assert_eq!(issued.expiry, NOW + 3_600);
        let grant = CapabilityGrant::from_token(&issued.token).unwrap();
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
        let issued = sponsor.issue(SIGNER, None, None, NOW).await.unwrap();
        assert_eq!(issued.spending_cap, 5_000_000);
        assert_eq!(issued.expiry, NOW + 172_800);
    }

    #[tokio::test]
    async fn registered_signer_gets_its_existing_capability_back() {
        let (sponsor, pool) = fake_sponsor(5_000_000, 172_800).await;
        let first = sponsor
            .issue(SIGNER, Some(2_000_000), None, NOW)
            .await
            .unwrap();
        pool.register(
            SIGNER,
            Authorization {
                spending_cap: first.spending_cap,
                expiry: first.expiry,
            },
        );

        // Asking again, for other terms, later: the registered terms win and
        // the token is byte-identical to the one first issued.
        let again = sponsor
            .issue(SIGNER, Some(4_000_000), Some(60), NOW + 100)
            .await
            .unwrap();
        assert!(again.registered);
        assert_eq!(again.token, first.token);
        assert_eq!(again.spending_cap, 2_000_000);
        assert_eq!(again.expiry, first.expiry);
    }

    #[tokio::test]
    async fn expired_registration_is_an_error() {
        let (sponsor, pool) = fake_sponsor(5_000_000, 172_800).await;
        pool.register(
            SIGNER,
            Authorization {
                spending_cap: 5_000_000,
                expiry: NOW,
            },
        );
        // expiry == now counts as expired.
        let err = sponsor.issue(SIGNER, None, None, NOW).await.err().unwrap();
        assert!(matches!(err, SponsorError::SignerExpired { expiry } if expiry == NOW));
    }

    #[tokio::test]
    async fn bad_terms_are_rejected_even_for_a_registered_signer() {
        let (sponsor, pool) = fake_sponsor(5_000_000, 172_800).await;
        pool.register(
            SIGNER,
            Authorization {
                spending_cap: 5_000_000,
                expiry: NOW + 1_000,
            },
        );
        let err = sponsor
            .issue(SIGNER, Some(5_000_001), None, NOW)
            .await
            .err()
            .unwrap();
        assert!(matches!(
            err,
            SponsorError::Terms(TermsError::ExceedsMax { .. })
        ));
        let err = sponsor
            .issue(SIGNER, Some(0), None, NOW)
            .await
            .err()
            .unwrap();
        assert!(matches!(err, SponsorError::Terms(TermsError::Zero { .. })));
    }

    #[tokio::test]
    async fn failed_registration_read_is_a_chain_error() {
        let (sponsor, pool) = fake_sponsor(5_000_000, 172_800).await;
        pool.fail_authorization_reads(true);
        let err = sponsor.issue(SIGNER, None, None, NOW).await.err().unwrap();
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
                max_spending_cap: 5_000_000,
                max_ttl_secs: 172_800,
            }
        );
    }
}
