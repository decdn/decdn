//! The pending protocol share for `admin_v1_earnings`.
//!
//! Each `redeemMany` pays the operator share at once and leaves the rest of
//! the payout in the pool as the operator's pending protocol share, until a
//! `decdn node sweep` sends it to the governance chain. The earnings call
//! reads it per pool with `pending(operator)` and `minSweep()`.

use alloy::primitives::Address;
use alloy::providers::Provider;
use decdn_common::admin::ProtocolShare;
use decdn_incentive::payment_pool::PaymentPool;

use crate::chain_events::timed;

/// A chain source of one pool's pending protocol share. A trait so the
/// earnings call runs against a fake in tests, and so
/// [`crate::admin::AdminState`] stays non-generic over the provider type.
#[async_trait::async_trait]
pub trait ProtocolShareSource: Send + Sync {
    /// `operator`'s pending protocol share in the pool, and the pool's
    /// `minSweep`. `Err` when a read fails or times out.
    async fn share(&self, operator: Address) -> anyhow::Result<ProtocolShare>;
}

/// [`ProtocolShareSource`] over a `PaymentPool` binding: one bounded
/// `pending` and one bounded `minSweep` read per call.
pub struct ChainProtocolShare<P: Provider + Clone> {
    contract: PaymentPool::PaymentPoolInstance<P>,
}

impl<P: Provider + Clone> std::fmt::Debug for ChainProtocolShare<P> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ChainProtocolShare")
            .field("payment_pool", self.contract.address())
            .finish_non_exhaustive()
    }
}

impl<P: Provider + Clone> ChainProtocolShare<P> {
    /// Wrap a `PaymentPool` binding.
    pub const fn new(contract: PaymentPool::PaymentPoolInstance<P>) -> Self {
        Self { contract }
    }
}

#[async_trait::async_trait]
impl<P: Provider + Clone + 'static> ProtocolShareSource for ChainProtocolShare<P> {
    async fn share(&self, operator: Address) -> anyhow::Result<ProtocolShare> {
        let pending = timed(
            None,
            "admin pending(operator)",
            self.contract.pending(operator).call(),
        )
        .await?;
        let min_sweep = timed(None, "admin minSweep", self.contract.minSweep().call()).await?;
        Ok(ProtocolShare {
            pending_micro_usdc: u64::try_from(pending.usdcAmount).unwrap_or(u64::MAX),
            pending_bytes: u64::try_from(pending.servedBytes).unwrap_or(u64::MAX),
            min_sweep_micro_usdc: min_sweep,
        })
    }
}
