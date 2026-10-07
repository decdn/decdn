//! Live operator fee-share (basis points), read from `FeeRouter.getShares()[0]`.
//! A single `Arc<AtomicU16>` shared by all clones, seeded at startup and
//! refreshed by the multiplexed log poller (see `fee_shares_watcher.rs`).
//! `(1 - f)` numerator for the serve-economics margin.

use std::sync::{
    Arc,
    atomic::{AtomicU16, Ordering},
};

use alloy::primitives::{Address, U256};
use alloy::providers::Provider;
use anyhow::Context;
use decdn_common::redact::sanitize_err_chain;
use decdn_incentive::payment_pool::{FeeRouter, PaymentPool};

use crate::chain_events::boot_retry::BootRetry;
use crate::chain_events::timed;

const BPS_DENOMINATOR: u64 = 10_000;

/// Cloneable live operator fee share. All clones share one atomic cell.
#[derive(Clone, Debug)]
pub struct OperatorShares {
    bps: Arc<AtomicU16>,
}

impl OperatorShares {
    /// Seed the cell, typically from the startup `getShares()` read.
    #[must_use]
    pub fn new(bps: u16) -> Self {
        Self {
            bps: Arc::new(AtomicU16::new(bps)),
        }
    }

    /// Current operator fee share, in basis points.
    #[must_use]
    pub fn bps(&self) -> u16 {
        self.bps.load(Ordering::Relaxed)
    }

    /// Publish a new operator share and return the one it replaces — called by
    /// the fee-shares watcher on a `SharesUpdated` event and by the periodic
    /// authoritative re-read.
    pub fn store(&self, bps: u16) -> u16 {
        self.bps.swap(bps, Ordering::Relaxed)
    }
}

/// Narrow the on-chain `[operator, buyback, treasury]` shares to operator bps.
///
/// Fail-closed: an out-of-range or unnarrowable operator share is a hard
/// error, not a clamp, so a malformed read never silently understates the
/// skim.
pub fn operator_bps_from_shares(shares: [U256; 3], addr: &str) -> anyhow::Result<u16> {
    let operator = shares.first().copied().unwrap_or(U256::ZERO);
    let raw = u64::try_from(operator).map_err(|_| {
        anyhow::anyhow!("FeeRouter operator share {operator} at {addr} exceeds u64")
    })?;
    anyhow::ensure!(
        raw <= BPS_DENOMINATOR,
        "FeeRouter operator share {raw} at {addr} exceeds {BPS_DENOMINATOR} bps"
    );
    // raw <= BPS_DENOMINATOR (10_000), so this narrowing is always lossless.
    u16::try_from(raw)
        .map_err(|_| anyhow::anyhow!("FeeRouter operator share {raw} at {addr} exceeds u16"))
}

/// The startup fee-share state: the router the fee-shares watcher follows, and
/// the operator share that seeds [`OperatorShares`].
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct FeeShareSeed {
    /// `PaymentPool.feeRouter()`, or `None` when it cannot be read. With no
    /// router address there is nothing to watch, so the runtime registers the
    /// fee-shares route only when this is `Some`. When `None`, `operator_bps` is
    /// `floor_bps`.
    pub(crate) router: Option<Address>,
    /// The operator share, or `floor_bps` when it cannot be read or narrowed.
    pub(crate) operator_bps: u16,
}

/// Read the fee router and its operator share at startup.
///
/// Neither read is fatal to boot: the serve-economics margin is an economic
/// optimization, not a safety invariant, so a failure seeds `floor_bps` and the
/// node still comes up. Both reads still retry transient errors on `boot`'s
/// shared deadline, bounded by the per-call RPC timeout. `feeRouter` is
/// immutable, so a missed `feeRouter()` read leaves the fee-shares watcher
/// unregistered for the life of the process; a retry at boot is the only
/// recovery. A deterministic error falls back at once.
///
/// A failed `getShares()` read still returns the router: the watcher's periodic
/// authoritative re-read recovers the share from there.
pub(crate) async fn seed_from_chain<P: Provider + Clone>(
    provider: P,
    payment_pool_addr: Address,
    floor_bps: u16,
    boot: &BootRetry,
) -> FeeShareSeed {
    let pool = PaymentPool::new(payment_pool_addr, provider.clone());
    let router = match boot
        .run("PaymentPool.feeRouter()", || async {
            timed(None, "PaymentPool.feeRouter()", pool.feeRouter().call())
                .await
                .with_context(|| format!("PaymentPool.feeRouter() at {payment_pool_addr}"))
        })
        .await
    {
        Ok(router) => router,
        Err(err) => {
            tracing::warn!(
                error = %sanitize_err_chain(&err),
                fallback_bps = floor_bps,
                "PaymentPool.feeRouter() startup read failed; operator fee share seeded to the \
                 FeeRouter OPERATOR_BPS_FLOOR and the fee-shares watcher is not registered"
            );
            return FeeShareSeed {
                router: None,
                operator_bps: floor_bps,
            };
        }
    };

    let operator_bps = read_operator_bps(&FeeRouter::new(router, provider), floor_bps, boot).await;
    FeeShareSeed {
        router: Some(router),
        operator_bps,
    }
}

/// The `getShares()` leg of [`seed_from_chain`]: the operator share, or
/// `floor_bps` when the read fails or the split cannot be narrowed.
async fn read_operator_bps<P: Provider + Clone>(
    fee_router: &FeeRouter::FeeRouterInstance<P>,
    floor_bps: u16,
    boot: &BootRetry,
) -> u16 {
    let router = *fee_router.address();
    match boot
        .run("FeeRouter.getShares()", || async {
            timed(None, "FeeRouter.getShares()", fee_router.getShares().call())
                .await
                .with_context(|| format!("FeeRouter.getShares() at {router}"))
        })
        .await
    {
        Ok(shares) => match operator_bps_from_shares(shares, &router.to_string()) {
            Ok(bps) => bps,
            Err(err) => {
                tracing::warn!(
                    error = %sanitize_err_chain(&err),
                    fallback_bps = floor_bps,
                    "FeeRouter.getShares() startup read could not be narrowed to operator bps; \
                     falling back to OPERATOR_BPS_FLOOR"
                );
                floor_bps
            }
        },
        Err(err) => {
            tracing::warn!(
                error = %sanitize_err_chain(&err),
                fallback_bps = floor_bps,
                "FeeRouter.getShares() startup read failed; falling back to OPERATOR_BPS_FLOOR"
            );
            floor_bps
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests;
