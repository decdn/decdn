//! `NodeFunder` — the node's [`Funder`] adapter over [`PoolOpener::top_up_pool`].
//!
//! [`client-pull`](decdn_client_pull)'s gap-driven `drive()` reactively tops up
//! the buyer deposit through the injected [`Funder`] seam (`source.rs`) rather
//! than naming a chain handle directly, so the node's upstream cache-miss pull
//! leg (B2) shares that driver instead of running its own copy of the top-up
//! loop. `NodeFunder` is the bridge.
//!
//! `Funder::top_up(additional)` means "add exactly `additional` to the deposit"
//! — the same contract the CLI's `CliFunder` honours by calling `topUp` with
//! `additional` directly. A `topUp` leaves the pool's committed spend untouched,
//! so adding `additional` to the deposit adds exactly `additional` to the
//! spendable headroom too. [`PoolOpener::top_up_pool`], though, takes a SPENDABLE
//! target (post-top-up spendable == its argument), so `NodeFunder` converts:
//! it reads the pool's current spendable — `deposit - committed`, off the shared
//! [`PoolContext`] the driver mutates and the shared [`PoolLedger`] the pull
//! commits through — and asks `top_up_pool` for `current_spendable + additional`.
//! That drives spendable up by exactly `additional`, matching `resume::fund`'s own
//! use of `top_up_pool` (which targets `working_deposit` of spendable directly).
//!
//! The pool has no expiry, so a `topUp` strands nothing time-bound — the node
//! funds its own pool freely, and there is no near-expiry refusal to derive.

use std::sync::{Arc, Mutex};

use alloy::primitives::U256;
use async_trait::async_trait;
use decdn_client_pull::source::SourceFuture;
use decdn_client_pull::{Funder, PoolContext, PoolLedger};
use decdn_incentive::DepositOutcome;

use super::resume::MAX_REACTIVE_TOPUPS;
use crate::buyer_channel::PoolOpener;

/// The node's [`Funder`]: reactively tops up ITS OWN pool through an injected
/// [`PoolOpener`].
///
/// # Fields
///
/// - `opener`: where the top-up lands. `Arc<dyn PoolOpener>` because the node
///   origin already stores its buyer service behind that same object-safe seam.
/// - `ctx`: the driver's live [`PoolContext`], shared (not copied) because its
///   `deposit` field grows across the fetch as earlier top-ups land — reading a
///   stale copy would under-shoot the target on a pool that already got topped up
///   once this fetch by a DIFFERENT path (the proactive low-water refill can fire
///   concurrently; see `top_up_pool`'s own join-or-spawn dedup for why that race
///   is expected). Locked only to copy `deposit` out; the guard is never held
///   across `.await` (`top_up_pool` is a network+chain round trip).
/// - `ledger`: the pull's shared [`PoolLedger`], read for the committed spend that
///   turns `deposit` into spendable headroom. It is the same ledger the driver
///   subtracts to compute the `additional` it passes here, so the two agree on
///   what "current spendable" is.
/// - `metrics`: the node's metrics handle. `top_up` records
///   `node_pull_reactive_topup` on a headroom-adding success and
///   `node_pull_reactive_topup_refused` on a no-headroom landing or a
///   `top_up_pool` error — the one seam both the window-paced serve leg and the
///   gap-driven pull leg fund through, so metering here covers both.
pub(crate) struct NodeFunder {
    opener: Arc<dyn PoolOpener>,
    ctx: Arc<Mutex<PoolContext>>,
    ledger: Arc<PoolLedger>,
    metrics: Arc<crate::metrics::Metrics>,
}

impl std::fmt::Debug for NodeFunder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NodeFunder").finish_non_exhaustive()
    }
}

impl NodeFunder {
    pub(crate) fn new(
        opener: Arc<dyn PoolOpener>,
        ctx: Arc<Mutex<PoolContext>>,
        ledger: Arc<PoolLedger>,
        metrics: Arc<crate::metrics::Metrics>,
    ) -> Self {
        Self {
            opener,
            ctx,
            ledger,
            metrics,
        }
    }
}

#[async_trait]
impl Funder for NodeFunder {
    fn max_topups(&self) -> u32 {
        MAX_REACTIVE_TOPUPS
    }

    fn top_up(&self, additional: U256) -> SourceFuture<'_, DepositOutcome> {
        Box::pin(async move {
            // Read the raw deposit once; derive spendable locally so the target
            // computation and the headroom classification share one baseline.
            let current_deposit = {
                let guard = self
                    .ctx
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                guard.deposit
            };
            let current_spendable = current_deposit.saturating_sub(self.ledger.committed().amount);
            // `top_up_pool` targets spendable, so raise the target by exactly
            // `additional` above the current spendable — that is what adds
            // `additional` to the deposit (a `topUp` never touches committed spend).
            let spendable_target = current_spendable.saturating_add(additional);
            match self.opener.top_up_pool(spendable_target).await {
                Ok(new_deposit) if new_deposit > current_deposit => {
                    self.metrics.node_pull_reactive_topup();
                    Ok(DepositOutcome::Added(new_deposit))
                }
                Ok(new_deposit) => {
                    // Landed but added no headroom (a concurrent proactive refill
                    // already held the slot). The driver treats a non-advancing
                    // deposit as an unmet top-up.
                    self.metrics.node_pull_reactive_topup_refused();
                    Ok(DepositOutcome::Added(new_deposit))
                }
                Err(err) => {
                    self.metrics.node_pull_reactive_topup_refused();
                    Err(err)
                }
            }
        })
    }
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    reason = "workspace anti-panic policy targets runtime code"
)]
mod tests {
    use std::sync::Mutex as StdMutex;
    use std::sync::atomic::{AtomicU32, Ordering};

    use alloy::dyn_abi::Eip712Domain;
    use alloy::primitives::{Address, B256};
    use alloy::signers::local::PrivateKeySigner;
    use anyhow::Result;
    use decdn_incentive::PoolId;

    use super::*;
    use crate::buyer_channel::PoolOpener;

    /// A configurable [`PoolOpener`] double: `top_up_pool` records the
    /// `target_deposit` it was called with (and how many times) and returns a
    /// fixed outcome. Only `top_up_pool` is exercised by `NodeFunder`; the rest of
    /// the trait is required by its signature but unreachable from these tests.
    #[derive(Debug)]
    struct MockOpener {
        top_up_result: Result<U256, String>,
        top_up_calls: AtomicU32,
        last_target: StdMutex<Option<U256>>,
    }

    impl MockOpener {
        fn new(top_up_result: Result<U256, String>) -> Self {
            Self {
                top_up_result,
                top_up_calls: AtomicU32::new(0),
                last_target: StdMutex::new(None),
            }
        }

        fn call_count(&self) -> u32 {
            self.top_up_calls.load(Ordering::SeqCst)
        }

        fn last_target(&self) -> Option<U256> {
            *self
                .last_target
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
        }
    }

    #[async_trait]
    impl PoolOpener for MockOpener {
        async fn open_or_reuse_pool(
            &self,
            _provider_addr: Address,
            _deposit_hint: U256,
            _budget: std::time::Duration,
        ) -> Result<PoolContext> {
            unreachable!("not exercised by NodeFunder tests")
        }

        fn record_progress(
            &self,
            _provider_addr: Address,
            _pool_id: PoolId,
            _bytes_delivered: U256,
            _amount: U256,
        ) -> Result<()> {
            unreachable!("not exercised by NodeFunder tests")
        }

        async fn top_up_pool(&self, target_deposit: U256) -> Result<U256> {
            self.top_up_calls.fetch_add(1, Ordering::SeqCst);
            *self
                .last_target
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(target_deposit);
            self.top_up_result.clone().map_err(|e| anyhow::anyhow!(e))
        }
    }

    fn test_ctx(deposit: U256) -> Arc<Mutex<PoolContext>> {
        let signer = PrivateKeySigner::random();
        Arc::new(Mutex::new(PoolContext {
            pool_id: B256::ZERO,
            provider: Address::repeat_byte(9),
            deposit,
            client_signer: Arc::new(signer),
            voucher_domain: Eip712Domain::default(),
            prior_bytes_delivered: U256::ZERO,
            prior_amount: U256::ZERO,
            client_binding: None,
            capability: None,
        }))
    }

    /// A lane ledger whose committed spend is `committed` — the amount already
    /// vouchered, which `deposit - committed` is the spendable headroom over.
    fn test_ledger(committed: U256) -> Arc<PoolLedger> {
        Arc::new(PoolLedger::new(decdn_client_pull::Cumulative {
            bytes: U256::ZERO,
            amount: committed,
        }))
    }

    /// With nothing committed, spendable == deposit, so the target `top_up_pool`
    /// receives is `deposit + additional`.
    #[tokio::test]
    async fn top_up_targets_current_spendable_plus_additional() {
        let deposit = U256::from(1_000u64);
        let additional = U256::from(250u64);
        let opener = Arc::new(MockOpener::new(Ok(U256::from(1_250u64))));
        let metrics = Arc::new(crate::metrics::Metrics::new());
        let f = NodeFunder::new(
            opener.clone(),
            test_ctx(deposit),
            test_ledger(U256::ZERO),
            metrics,
        );

        let outcome = f.top_up(additional).await.expect("top-up should succeed");

        assert_eq!(opener.last_target(), Some(deposit + additional));
        assert_eq!(outcome, DepositOutcome::Added(U256::from(1_250u64)));
    }

    /// The driver path's contract: `top_up(additional)` adds exactly `additional`
    /// to spendable, never over-escrowing by the committed spend. With `committed`
    /// already vouchered, spendable is `deposit - committed`, so the target must be
    /// `(deposit - committed) + additional` — NOT `deposit + additional`, which
    /// would over-target by `committed` and drive spendable to `working + committed`.
    #[tokio::test]
    async fn top_up_adds_exactly_additional_to_spendable() {
        let deposit = U256::from(1_000u64);
        let committed = U256::from(600u64);
        let additional = U256::from(250u64);
        let opener = Arc::new(MockOpener::new(Ok(U256::from(1_250u64))));
        let metrics = Arc::new(crate::metrics::Metrics::new());
        let f = NodeFunder::new(
            opener.clone(),
            test_ctx(deposit),
            test_ledger(committed),
            metrics,
        );

        let _ = f.top_up(additional).await.expect("top-up should succeed");

        // current spendable = 1000 - 600 = 400; target = 400 + 250 = 650.
        assert_eq!(opener.last_target(), Some(U256::from(650u64)));
    }

    #[tokio::test]
    async fn top_up_pool_error_propagates() {
        let opener = Arc::new(MockOpener::new(Err("chain rejected".to_string())));
        let metrics = Arc::new(crate::metrics::Metrics::new());
        let f = NodeFunder::new(
            opener.clone(),
            test_ctx(U256::from(100u64)),
            test_ledger(U256::ZERO),
            metrics,
        );

        let err = f.top_up(U256::from(50u64)).await.unwrap_err();

        assert!(err.to_string().contains("chain rejected"));
        assert_eq!(opener.call_count(), 1);
    }

    #[test]
    fn max_topups_reports_the_reactive_budget() {
        let opener = Arc::new(MockOpener::new(Ok(U256::ZERO)));
        let metrics = Arc::new(crate::metrics::Metrics::new());
        let f = NodeFunder::new(
            opener,
            test_ctx(U256::ZERO),
            test_ledger(U256::ZERO),
            metrics,
        );

        assert_eq!(f.max_topups(), MAX_REACTIVE_TOPUPS);
        assert_eq!(f.max_topups(), 1);
    }

    /// A headroom-adding top-up bumps `node_pull_reactive_topup_total`; a
    /// `top_up_pool` failure bumps `node_pull_reactive_topup_refused_total`
    /// instead (and still propagates the error) — the seam both pull paths
    /// now share for metering.
    #[tokio::test]
    async fn top_up_meters_success_and_refusal() {
        let metrics = Arc::new(crate::metrics::Metrics::new());

        let ok_opener = Arc::new(MockOpener::new(Ok(U256::from(1_250u64))));
        let f = NodeFunder::new(
            ok_opener,
            test_ctx(U256::from(1_000u64)),
            test_ledger(U256::ZERO),
            Arc::clone(&metrics),
        );
        let _ = f
            .top_up(U256::from(250u64))
            .await
            .expect("top-up should succeed");
        let text = metrics.encode().expect("metrics should encode");
        assert!(
            text.contains("decdn_node_pull_reactive_topup_total 1"),
            "expected a headroom-adding top-up to bump the success counter: {text}"
        );

        let err_opener = Arc::new(MockOpener::new(Err("chain rejected".to_string())));
        let f = NodeFunder::new(
            err_opener,
            test_ctx(U256::from(1_000u64)),
            test_ledger(U256::ZERO),
            Arc::clone(&metrics),
        );
        let _ = f.top_up(U256::from(250u64)).await;
        let text = metrics.encode().expect("metrics should encode");
        assert!(
            text.contains("decdn_node_pull_reactive_topup_refused_total 1"),
            "expected a failing top-up to bump the refused counter: {text}"
        );
    }

    /// Regression for the raw-deposit-vs-spendable baseline bug: with `committed`
    /// nonzero, `top_up_pool`'s no-op path returns the deposit UNCHANGED (a
    /// concurrent proactive refill already grabbed the slot). Classifying against
    /// the spendable baseline (`deposit - committed`) would wrongly read the
    /// unchanged raw deposit as `> current_spendable` and count it as a success;
    /// classifying against the raw deposit — the fix — correctly calls it refused.
    #[tokio::test]
    async fn top_up_no_headroom_landing_with_committed_spend_is_refused() {
        let deposit = U256::from(1_000u64);
        let committed = U256::from(600u64);
        // The no-op path: `top_up_pool` returns the pre-call raw deposit unchanged.
        let opener = Arc::new(MockOpener::new(Ok(deposit)));
        let metrics = Arc::new(crate::metrics::Metrics::new());
        let f = NodeFunder::new(
            opener,
            test_ctx(deposit),
            test_ledger(committed),
            Arc::clone(&metrics),
        );

        let outcome = f
            .top_up(U256::from(250u64))
            .await
            .expect("top-up should succeed (a no-op landing is not an error)");

        assert_eq!(outcome, DepositOutcome::Added(deposit));
        let text = metrics.encode().expect("metrics should encode");
        assert!(
            text.contains("decdn_node_pull_reactive_topup_refused_total 1"),
            "expected a no-headroom landing to bump the refused counter: {text}"
        );
        assert!(
            !text.contains("decdn_node_pull_reactive_topup_total 1"),
            "expected a no-headroom landing NOT to bump the success counter: {text}"
        );
    }
}
