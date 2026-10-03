//! The sponsor's signing and pool top-up logic: sign capped, expiring
//! capabilities against the sponsor's `PaymentPool` and keep that pool
//! funded from the treasury. Embeddable on its own; the `sponsord` daemon
//! serves it over HTTP.
//!
//! ```no_run
//! # async fn demo(signer: alloy::signers::local::PrivateKeySigner) -> anyhow::Result<()> {
//! use sponsord_core::{ChainConfig, KeeperConfig, Limits, MicroUsdc, Sponsor, TermsRequest};
//!
//! let sponsor = Sponsor::connect(
//!     signer,
//!     ChainConfig {
//!         rpc_url: "https://sepolia-rollup.arbitrum.io/rpc".into(),
//!         chain_id: 421_614,
//!         payment_pool: "0x0000000000000000000000000000000000000001".parse()?,
//!         pool_id: "0x1111111111111111111111111111111111111111111111111111111111111111".parse()?,
//!     },
//!     Limits { max_spending_cap: MicroUsdc(5_000_000), max_ttl_secs: 172_800 },
//! )
//! .await?;
//! let shutdown = tokio_util::sync::CancellationToken::new();
//! tokio::spawn(sponsor.keeper(
//!     KeeperConfig {
//!         low_water: MicroUsdc(20_000_000),
//!         refill: MicroUsdc(100_000_000),
//!         interval: std::time::Duration::from_secs(3600),
//!     },
//!     shutdown.clone(),
//! ));
//! let issued = sponsor
//!     .issue("0x00000000000000000000000000000000000000aa".parse()?, &TermsRequest::default(), 1_769_904_000)
//!     .await?;
//! println!("{}", issued.capability.token);
//! # Ok(())
//! # }
//! ```

pub mod issuer;
pub mod keeper;
pub mod pool;
pub mod sponsor;

pub use issuer::{Issuer, Limits, Terms, TermsError, TermsRequest};
pub use keeper::{KeeperConfig, KeeperSnapshot, KeeperStatus};
pub use sponsor::{ChainConfig, Sponsor, SponsorError};
pub use sponsord_api::{IssuedCapability, MicroUsdc};

#[cfg(any(test, feature = "test-support"))]
pub mod test_support;
