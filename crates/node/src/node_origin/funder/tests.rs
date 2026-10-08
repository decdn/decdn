use std::sync::Mutex as StdMutex;
use std::sync::atomic::{AtomicU32, Ordering};

use alloy::primitives::{Address, B256};
use anyhow::Result;
use async_trait::async_trait;
use decdn_client::{PoolContext, PoolReplaced};
use decdn_incentive::PoolId;

use super::*;
use crate::buyer_channel::PoolOpener;

/// A [`PoolOpener`] double whose `recover_pool` counts its calls and answers a
/// fixed outcome. Only `recover_pool` is exercised by `NodeFunder`; the rest of
/// the trait is required by its signature but unreachable from these tests.
#[derive(Debug)]
struct MockOpener {
    outcome: StdMutex<Option<Result<Recovery, String>>>,
    calls: AtomicU32,
    /// The deposit the last call said its fill saw.
    seen: StdMutex<Option<U256>>,
}

impl MockOpener {
    fn new(outcome: Result<Recovery, String>) -> Self {
        Self {
            outcome: StdMutex::new(Some(outcome)),
            calls: AtomicU32::new(0),
            seen: StdMutex::new(None),
        }
    }

    fn call_count(&self) -> u32 {
        self.calls.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl PoolOpener for MockOpener {
    async fn open_or_reuse_pool(
        &self,
        _provider_addr: Address,
        _budget: std::time::Duration,
    ) -> Result<PoolContext> {
        unreachable!("not exercised by NodeFunder tests")
    }

    fn record_progress(
        &self,
        _provider_addr: Address,
        _pool_id: PoolId,
        _write: decdn_client::buyer_pool::ProgressWrite,
    ) -> Result<()> {
        unreachable!("not exercised by NodeFunder tests")
    }

    async fn recover_pool(&self, seen_deposit: U256) -> Result<Recovery> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        *self
            .seen
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(seen_deposit);
        let outcome = self
            .outcome
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
            .unwrap_or(Ok(Recovery::Unavailable));
        outcome.map_err(|e| anyhow::anyhow!(e))
    }
}

/// A step that tops the pool up passes the new deposit through and meters a
/// funded step. The opener learns the deposit the fill saw.
#[tokio::test]
async fn a_topped_up_step_passes_the_deposit_through_and_meters_it() {
    let opener = Arc::new(MockOpener::new(Ok(Recovery::ToppedUp(U256::from(
        1_250u64,
    )))));
    let metrics = Arc::new(crate::metrics::Metrics::new());
    let f = NodeFunder::new(opener.clone(), Arc::clone(&metrics), U256::from(700u64));

    let outcome = f.recover(U256::ZERO).await.expect("the step lands");

    assert_eq!(outcome, Recovery::ToppedUp(U256::from(1_250u64)));
    assert_eq!(opener.call_count(), 1);
    assert_eq!(*opener.seen.lock().unwrap(), Some(U256::from(700u64)));
    let text = metrics.encode().expect("metrics should encode");
    assert!(
        text.contains("decdn_node_pull_reactive_topup_total 1"),
        "expected a funded step to bump the success counter: {text}"
    );
}

/// A pool that no longer accepts funds is replaced, and the replacement
/// reaches the fill unchanged.
#[tokio::test]
async fn a_replaced_pool_reaches_the_fill() {
    let replaced = PoolReplaced {
        closed: B256::repeat_byte(1),
        opened: B256::repeat_byte(2),
    };
    let opener = Arc::new(MockOpener::new(Ok(Recovery::Replaced(replaced))));
    let metrics = Arc::new(crate::metrics::Metrics::new());
    let f = NodeFunder::new(opener, metrics, U256::ZERO);

    assert_eq!(
        f.recover(U256::ZERO).await.expect("the step lands"),
        Recovery::Replaced(replaced)
    );
}

/// A step with nothing to add is refused, not funded.
#[tokio::test]
async fn a_step_with_no_funding_path_is_metered_as_refused() {
    let opener = Arc::new(MockOpener::new(Ok(Recovery::Unavailable)));
    let metrics = Arc::new(crate::metrics::Metrics::new());
    let f = NodeFunder::new(opener, Arc::clone(&metrics), U256::ZERO);

    assert_eq!(
        f.recover(U256::ZERO).await.expect("no error"),
        Recovery::Unavailable
    );
    let text = metrics.encode().expect("metrics should encode");
    assert!(
        text.contains("decdn_node_pull_reactive_topup_refused_total 1"),
        "expected a step with no funding path to bump the refused counter: {text}"
    );
}

/// A funding failure is the node's own fault, so it carries `LocalPullFault`
/// and the pull verdict does not score the upstream `Unreachable` for it.
#[tokio::test]
async fn a_failed_step_is_a_local_fault() {
    let opener = Arc::new(MockOpener::new(Err("chain rejected".to_string())));
    let metrics = Arc::new(crate::metrics::Metrics::new());
    let f = NodeFunder::new(opener, metrics, U256::ZERO);

    let err = f.recover(U256::ZERO).await.unwrap_err();

    assert!(format!("{err:#}").contains("chain rejected"), "{err:#}");
    assert!(err.downcast_ref::<LocalPullFault>().is_some());
    assert_eq!(
        super::super::pull_verdict(&err),
        super::super::PullVerdict::OurLocalFault
    );
}

/// Two poll intervals of 500 ms steps, so the upstream clears a full poll plus
/// its head cache — 28 steps (14 s) at the 7 s default.
#[test]
fn settle_budget_tracks_the_chain_poll_cadence() {
    assert_eq!(settle_wait_budget(Duration::from_secs(7)), 28);
    assert_eq!(
        settle_window(Duration::from_secs(7)),
        Duration::from_secs(14)
    );
    assert_eq!(settle_wait_budget(Duration::from_secs(1)), 4);
    // A chain lane tuned faster than one step still gets a real, if tiny, budget
    // rather than zero — a zero budget would make the top-up a coin flip.
    assert_eq!(settle_wait_budget(Duration::from_millis(250)), 1);
    // ...and a SLOW chain lane cannot buy unbounded foreground waiting.
    // `event_poll_interval_ms` has a config floor but no ceiling, so without
    // the cap a 60 s lane would hold a client's pull for two minutes.
    assert_eq!(
        settle_wait_budget(Duration::from_secs(60)),
        MAX_SETTLE_WAITS
    );
    assert_eq!(
        settle_wait_budget(Duration::from_secs(3600)),
        MAX_SETTLE_WAITS
    );
}
