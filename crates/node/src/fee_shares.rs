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

    /// Publish a new operator share — called by the fee-shares watcher on a
    /// `SharesUpdated` event and by the periodic authoritative re-read.
    pub fn store(&self, bps: u16) {
        self.bps.store(bps, Ordering::Relaxed);
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
mod tests {
    use alloy::primitives::{Bytes, U256};
    use alloy::providers::ProviderBuilder;
    use alloy::providers::mock::Asserter;
    use alloy::sol_types::SolValue;

    use super::*;
    use crate::chain_events::boot_retry::BOOT_CHAIN_RETRY_BUDGET;
    use crate::metrics::Metrics;

    const FLOOR: u16 = 4000;

    fn retries(metrics: &Metrics) -> u64 {
        let text = metrics.encode().unwrap();
        text.lines()
            .find_map(|l| l.strip_prefix("decdn_chain_boot_read_retries_total "))
            .and_then(|v| v.parse().ok())
            .unwrap()
    }

    fn outage() -> alloy_json_rpc::ErrorPayload {
        serde_json::from_value(serde_json::json!({
            "code": 19,
            "message": "Temporary internal error. Please retry",
        }))
        .unwrap()
    }

    async fn seed(asserter: &Asserter, boot: &BootRetry) -> FeeShareSeed {
        seed_from_chain(
            ProviderBuilder::new().connect_mocked_client(asserter.clone()),
            Address::repeat_byte(0x11),
            FLOOR,
            boot,
        )
        .await
    }

    /// A transient `feeRouter()` error is retried, so the router — and with it
    /// the fee-shares watcher — is not lost for the life of the process.
    #[tokio::test(start_paused = true)]
    async fn a_transient_fee_router_error_is_retried() {
        let router = Address::repeat_byte(0x22);
        let asserter = Asserter::new();
        asserter.push_failure(outage());
        asserter.push_success(&Bytes::from(router.abi_encode()));
        let shares = [
            U256::from(4500u64),
            U256::from(3500u64),
            U256::from(2000u64),
        ];
        asserter.push_success(&Bytes::from(shares.abi_encode()));
        let metrics = Arc::new(Metrics::new());

        let seed = seed(
            &asserter,
            &BootRetry::new(BOOT_CHAIN_RETRY_BUDGET, Arc::clone(&metrics)),
        )
        .await;

        assert_eq!(
            seed,
            FeeShareSeed {
                router: Some(router),
                operator_bps: 4500,
            }
        );
        assert_eq!(retries(&metrics), 1);
    }

    /// No contract at the pool address is deterministic: the seed falls back to
    /// the floor at once, with no router to watch.
    #[tokio::test(start_paused = true)]
    async fn a_deterministic_fee_router_error_falls_back_at_once() {
        let asserter = Asserter::new();
        asserter.push_success(&Bytes::new());
        let metrics = Arc::new(Metrics::new());
        let start = tokio::time::Instant::now();

        let seed = seed(
            &asserter,
            &BootRetry::new(BOOT_CHAIN_RETRY_BUDGET, Arc::clone(&metrics)),
        )
        .await;

        assert_eq!(
            seed,
            FeeShareSeed {
                router: None,
                operator_bps: FLOOR,
            }
        );
        assert_eq!(retries(&metrics), 0);
        assert_eq!(start.elapsed(), std::time::Duration::ZERO);
    }

    /// A stalled provider cannot wedge boot: the fee reads are bounded by the
    /// per-call timeout and fall back to the floor share.
    #[tokio::test(start_paused = true)]
    async fn a_stalled_provider_falls_back_to_the_floor() {
        use crate::chain_events::test_support::{bounded, hanging_provider};

        let metrics = Arc::new(Metrics::new());
        let seed = bounded(
            "fee-share seed",
            seed_from_chain(
                hanging_provider(),
                Address::repeat_byte(0x11),
                FLOOR,
                &BootRetry::single_attempt(Arc::clone(&metrics)),
            ),
        )
        .await;

        assert_eq!(
            seed,
            FeeShareSeed {
                router: None,
                operator_bps: FLOOR,
            }
        );
    }

    /// A `getShares()` read that never succeeds seeds the floor but keeps the
    /// router, so the watcher's periodic re-read can recover the share.
    #[tokio::test(start_paused = true)]
    async fn a_failed_shares_read_keeps_the_router() {
        let router = Address::repeat_byte(0x22);
        let asserter = Asserter::new();
        asserter.push_success(&Bytes::from(router.abi_encode()));
        asserter.push_failure(outage());
        let metrics = Arc::new(Metrics::new());

        let seed = seed(&asserter, &BootRetry::single_attempt(Arc::clone(&metrics))).await;

        assert_eq!(
            seed,
            FeeShareSeed {
                router: Some(router),
                operator_bps: FLOOR,
            }
        );
    }

    #[test]
    fn shares_narrow_to_operator_bps() {
        let shares = [
            U256::from(6000u64),
            U256::from(3000u64),
            U256::from(1000u64),
        ];
        assert_eq!(operator_bps_from_shares(shares, "0xrouter").unwrap(), 6000);
    }

    #[test]
    fn shares_above_denominator_are_rejected() {
        let shares = [U256::from(10_001u64), U256::ZERO, U256::ZERO];
        assert!(operator_bps_from_shares(shares, "0xrouter").is_err());
    }

    #[test]
    fn cell_round_trips_and_is_shared_across_clones() {
        let cell = OperatorShares::new(6000);
        let clone = cell.clone();
        cell.store(4000);
        assert_eq!(clone.bps(), 4000);
    }
}
