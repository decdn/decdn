//! Operator fee share (basis points), read once at startup from the
//! immutable `PaymentPool.operatorBps()`: the share of each redemption the
//! redeemer receives in the same transaction. A single `Arc<AtomicU16>` shared
//! by all clones. `(1 - f)` numerator for the serve-economics margin.

use std::sync::{
    Arc,
    atomic::{AtomicU16, Ordering},
};

use alloy::primitives::{Address, U256};
use alloy::providers::Provider;
use anyhow::Context;
use decdn_common::redact::sanitize_err_chain;
use decdn_incentive::payment_pool::PaymentPool;

use crate::chain_events::boot_retry::BootRetry;
use crate::chain_events::timed;

const BPS_DENOMINATOR: u64 = 10_000;

/// Cloneable operator fee share. All clones share one atomic cell.
#[derive(Clone, Debug)]
pub struct OperatorShares {
    bps: Arc<AtomicU16>,
}

impl OperatorShares {
    /// Seed the cell, typically from the startup `operatorBps()` read.
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

    /// Publish a new operator share and return the one it replaces.
    pub fn store(&self, bps: u16) -> u16 {
        self.bps.swap(bps, Ordering::Relaxed)
    }
}

/// Narrow the on-chain `operatorBps` to `u16` basis points.
///
/// Fail-closed: a share above `10_000` is a hard error, not a clamp, so a
/// malformed read never silently overstates the operator's take.
///
/// # Errors
///
/// When `raw` exceeds `10_000`. `pool` names the contract in the message.
pub fn operator_bps_from_raw(raw: U256, pool: &str) -> anyhow::Result<u16> {
    let raw = u64::try_from(raw)
        .map_err(|_| anyhow::anyhow!("PaymentPool operatorBps {raw} at {pool} exceeds u64"))?;
    anyhow::ensure!(
        raw <= BPS_DENOMINATOR,
        "PaymentPool operatorBps {raw} at {pool} exceeds {BPS_DENOMINATOR} bps"
    );
    u16::try_from(raw)
        .map_err(|_| anyhow::anyhow!("PaymentPool operatorBps {raw} at {pool} exceeds u16"))
}

/// Read the pool's operator share at startup.
///
/// Not fatal to boot: the serve-economics margin is an economic optimization,
/// not a safety invariant, so a failure seeds `floor_bps` and the node still
/// comes up. The read retries transient errors on `boot`'s shared deadline,
/// bounded by the per-call RPC timeout. A deterministic error falls back at
/// once.
pub(crate) async fn seed_from_chain<P: Provider + Clone>(
    provider: P,
    payment_pool_addr: Address,
    floor_bps: u16,
    boot: &BootRetry,
) -> u16 {
    let pool = PaymentPool::new(payment_pool_addr, provider);
    let read = boot
        .run("PaymentPool.operatorBps()", || async {
            timed(None, "PaymentPool.operatorBps()", pool.operatorBps().call())
                .await
                .with_context(|| format!("PaymentPool.operatorBps() at {payment_pool_addr}"))
        })
        .await
        .and_then(|raw| operator_bps_from_raw(raw, &payment_pool_addr.to_string()));
    match read {
        Ok(bps) => bps,
        Err(err) => {
            tracing::warn!(
                error = %sanitize_err_chain(&err),
                fallback_bps = floor_bps,
                "PaymentPool.operatorBps() startup read failed; operator fee share seeded to \
                 the floor"
            );
            floor_bps
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests;
