//! `NodeFunder` — the node's [`Funder`] adapter over [`PoolOpener::top_up_pool`].
//!
//! [`client-pull`](decdn_client_pull)'s gap-driven `drive()` reactively tops up
//! the buyer deposit through the injected [`Funder`] seam (`source.rs`) rather
//! than naming a chain handle directly, so the node's upstream cache-miss pull
//! leg (B2) shares that driver instead of running its own copy of the top-up
//! loop. `NodeFunder` is the bridge: it maps `Funder::top_up`'s DELTA
//! (`additional`) onto `PoolOpener::top_up_pool`'s ABSOLUTE `target_deposit`,
//! reading the "current deposit" half of that sum off the shared [`PoolContext`]
//! the driver mutates as the pull progresses.
//!
//! The pool has no expiry, so a `topUp` strands nothing time-bound — the node
//! funds its own pool freely, and there is no near-expiry refusal to derive.

use std::sync::{Arc, Mutex};

use alloy::primitives::U256;
use async_trait::async_trait;
use decdn_client_pull::source::SourceFuture;
use decdn_client_pull::{Funder, PoolContext};
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
///   stale copy would under-shoot `target` on a pool that already got topped up
///   once this fetch by a DIFFERENT path (the proactive low-water refill can fire
///   concurrently; see `top_up_pool`'s own join-or-spawn dedup for why that race
///   is expected). Locked only to copy `deposit` out; the guard is never held
///   across `.await` (`top_up_pool` is a network+chain round trip).
pub(crate) struct NodeFunder {
    opener: Arc<dyn PoolOpener>,
    ctx: Arc<Mutex<PoolContext>>,
}

impl std::fmt::Debug for NodeFunder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NodeFunder").finish_non_exhaustive()
    }
}

impl NodeFunder {
    pub(crate) fn new(opener: Arc<dyn PoolOpener>, ctx: Arc<Mutex<PoolContext>>) -> Self {
        Self { opener, ctx }
    }

    /// Copy the shared context's current deposit. Locks, copies, drops — never
    /// held across an `.await`.
    fn current_deposit(&self) -> U256 {
        let guard = self
            .ctx
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        guard.deposit
    }
}

#[async_trait]
impl Funder for NodeFunder {
    fn max_topups(&self) -> u32 {
        MAX_REACTIVE_TOPUPS
    }

    fn top_up(&self, additional: U256) -> SourceFuture<'_, DepositOutcome> {
        Box::pin(async move {
            let target = self.current_deposit().saturating_add(additional);
            let new_deposit = self.opener.top_up_pool(target).await?;
            Ok(DepositOutcome::Added(new_deposit))
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

    #[tokio::test]
    async fn top_up_computes_target_from_current_deposit_plus_additional() {
        let deposit = U256::from(1_000u64);
        let additional = U256::from(250u64);
        let opener = Arc::new(MockOpener::new(Ok(U256::from(1_250u64))));
        let ctx = test_ctx(deposit);
        let f = NodeFunder::new(opener.clone(), ctx);

        let outcome = f.top_up(additional).await.expect("top-up should succeed");

        assert_eq!(opener.last_target(), Some(deposit + additional));
        assert_eq!(outcome, DepositOutcome::Added(U256::from(1_250u64)));
    }

    #[tokio::test]
    async fn top_up_pool_error_propagates() {
        let opener = Arc::new(MockOpener::new(Err("chain rejected".to_string())));
        let ctx = test_ctx(U256::from(100u64));
        let f = NodeFunder::new(opener.clone(), ctx);

        let err = f.top_up(U256::from(50u64)).await.unwrap_err();

        assert!(err.to_string().contains("chain rejected"));
        assert_eq!(opener.call_count(), 1);
    }

    #[test]
    fn max_topups_reports_the_reactive_budget() {
        let opener = Arc::new(MockOpener::new(Ok(U256::ZERO)));
        let ctx = test_ctx(U256::ZERO);
        let f = NodeFunder::new(opener, ctx);

        assert_eq!(f.max_topups(), MAX_REACTIVE_TOPUPS);
        assert_eq!(f.max_topups(), 1);
    }
}
