use std::sync::Mutex as StdMutex;
use std::sync::atomic::{AtomicU32, Ordering};

use alloy::dyn_abi::Eip712Domain;
use alloy::primitives::{Address, B256};
use alloy::signers::local::PrivateKeySigner;
use anyhow::Result;
use decdn_incentive::PoolId;

use super::*;
use crate::buyer_channel::{PoolOpener, TopUpLanded};

/// A configurable [`PoolOpener`] double: `top_up_pool_by` records the
/// `additional` it was called with (and how many times) and returns a
/// fixed outcome. Only `top_up_pool_by` is exercised by `NodeFunder`; the rest of
/// the trait is required by its signature but unreachable from these tests.
#[derive(Debug)]
struct MockOpener {
    top_up_result: Result<TopUpLanded, String>,
    top_up_calls: AtomicU32,
    last_additional: StdMutex<Option<U256>>,
}

impl MockOpener {
    fn new(top_up_result: Result<TopUpLanded, String>) -> Self {
        Self {
            top_up_result,
            top_up_calls: AtomicU32::new(0),
            last_additional: StdMutex::new(None),
        }
    }

    /// An opener whose top-up lands with `new_deposit` and adds `added`.
    fn landing(new_deposit: u64, added: u64) -> Self {
        Self::new(Ok(TopUpLanded {
            new_deposit: U256::from(new_deposit),
            added: U256::from(added),
        }))
    }

    fn call_count(&self) -> u32 {
        self.top_up_calls.load(Ordering::SeqCst)
    }

    fn last_additional(&self) -> Option<U256> {
        *self
            .last_additional
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
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

    async fn top_up_pool_by(&self, additional: U256) -> Result<TopUpLanded> {
        self.top_up_calls.fetch_add(1, Ordering::SeqCst);
        *self
            .last_additional
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(additional);
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

fn test_funded() -> Arc<AtomicBool> {
    Arc::new(AtomicBool::new(false))
}

/// The driver path's contract: `top_up(additional)` asks the opener to add
/// exactly `additional`. The driver sizes it from the pull's live ledger; the
/// funder never re-sizes it from a deposit or committed-spend view of its own,
/// which would drift from what the driver saw (#1893).
#[tokio::test]
async fn top_up_asks_the_opener_for_exactly_additional() {
    let deposit = U256::from(1_000u64);
    let additional = U256::from(250u64);
    let opener = Arc::new(MockOpener::landing(1_250, 250));
    let metrics = Arc::new(crate::metrics::Metrics::new());
    let f = NodeFunder::new(opener.clone(), test_ctx(deposit), metrics, test_funded());

    let outcome = f.top_up(additional).await.expect("top-up should succeed");

    assert_eq!(opener.last_additional(), Some(additional));
    assert_eq!(outcome, DepositOutcome::Added(U256::from(1_250u64)));
}

/// A funding failure is the node's own fault, so it carries `LocalPullFault`
/// and the pull verdict does not score the upstream `Unreachable` for it.
#[tokio::test]
async fn top_up_pool_error_propagates_as_a_local_fault() {
    let opener = Arc::new(MockOpener::new(Err("chain rejected".to_string())));
    let metrics = Arc::new(crate::metrics::Metrics::new());
    let f = NodeFunder::new(
        opener.clone(),
        test_ctx(U256::from(100u64)),
        metrics,
        test_funded(),
    );

    let err = f.top_up(U256::from(50u64)).await.unwrap_err();

    assert!(format!("{err:#}").contains("chain rejected"), "{err:#}");
    assert!(err.downcast_ref::<LocalPullFault>().is_some());
    assert_eq!(
        super::super::pull_verdict(&err),
        super::super::PullVerdict::OurLocalFault
    );
    assert_eq!(opener.call_count(), 1);
}

#[test]
fn max_topups_reports_the_reactive_budget() {
    let opener = Arc::new(MockOpener::landing(0, 0));
    let metrics = Arc::new(crate::metrics::Metrics::new());
    let f = NodeFunder::new(opener, test_ctx(U256::ZERO), metrics, test_funded());

    assert_eq!(f.max_topups(), MAX_REACTIVE_TOPUPS);
    assert_eq!(f.max_topups(), 1);
}

/// A top-up that adds the full request bumps `node_pull_reactive_topup_total`
/// and marks the pull funded; a `top_up_pool_by` failure bumps
/// `node_pull_reactive_topup_refused_total` instead (and still propagates the
/// error) — the seam both pull paths share for metering.
#[tokio::test]
async fn top_up_meters_success_and_refusal() {
    let metrics = Arc::new(crate::metrics::Metrics::new());

    let funded = test_funded();
    let f = NodeFunder::new(
        Arc::new(MockOpener::landing(1_250, 250)),
        test_ctx(U256::from(1_000u64)),
        Arc::clone(&metrics),
        Arc::clone(&funded),
    );
    let _ = f
        .top_up(U256::from(250u64))
        .await
        .expect("top-up should succeed");
    assert!(funded.load(Ordering::Relaxed));
    let text = metrics.encode().expect("metrics should encode");
    assert!(
        text.contains("decdn_node_pull_reactive_topup_total 1"),
        "expected a full top-up to bump the success counter: {text}"
    );

    let err_opener = Arc::new(MockOpener::new(Err("chain rejected".to_string())));
    let funded = test_funded();
    let f = NodeFunder::new(
        err_opener,
        test_ctx(U256::from(1_000u64)),
        Arc::clone(&metrics),
        Arc::clone(&funded),
    );
    let _ = f.top_up(U256::from(250u64)).await;
    assert!(!funded.load(Ordering::Relaxed));
    let text = metrics.encode().expect("metrics should encode");
    assert!(
        text.contains("decdn_node_pull_reactive_topup_refused_total 1"),
        "expected a failing top-up to bump the refused counter: {text}"
    );
}

/// An opener that adds nothing is a refusal, not a success, and leaves the pull
/// unfunded. A zero `new_deposit` (the trait default) never lowers the deposit
/// the driver credits.
#[tokio::test]
async fn top_up_that_adds_nothing_is_refused() {
    let deposit = U256::from(1_000u64);
    let opener = Arc::new(MockOpener::landing(0, 0));
    let metrics = Arc::new(crate::metrics::Metrics::new());
    let funded = test_funded();
    let f = NodeFunder::new(
        opener,
        test_ctx(deposit),
        Arc::clone(&metrics),
        Arc::clone(&funded),
    );

    let outcome = f
        .top_up(U256::from(250u64))
        .await
        .expect("a landing that adds nothing is not an error");

    assert_eq!(outcome, DepositOutcome::Added(deposit));
    assert!(!funded.load(Ordering::Relaxed));
    let text = metrics.encode().expect("metrics should encode");
    assert!(
        text.contains("decdn_node_pull_reactive_topup_refused_total 1"),
        "expected an empty landing to bump the refused counter: {text}"
    );
    assert!(
        !text.contains("decdn_node_pull_reactive_topup_total 1"),
        "expected an empty landing NOT to bump the success counter: {text}"
    );
}

/// A landing that adds less than `additional` is metered as refused (#2012),
/// even when the pool's total deposit grew by more — another funder's escrow is
/// not this pull's headroom. It still marks the pull funded, because it escrowed
/// something, so the extortion metering does not count it a second time.
#[tokio::test]
async fn top_up_short_landing_is_refused() {
    let deposit = U256::from(1_000u64);
    let opener = Arc::new(MockOpener::landing(1_400, 100));
    let metrics = Arc::new(crate::metrics::Metrics::new());
    let funded = test_funded();
    let f = NodeFunder::new(
        opener,
        test_ctx(deposit),
        Arc::clone(&metrics),
        Arc::clone(&funded),
    );

    let outcome = f
        .top_up(U256::from(250u64))
        .await
        .expect("a short landing still credits the deposit");

    assert_eq!(outcome, DepositOutcome::Added(U256::from(1_400u64)));
    assert!(funded.load(Ordering::Relaxed));
    let text = metrics.encode().expect("metrics should encode");
    assert!(
        text.contains("decdn_node_pull_reactive_topup_refused_total 1"),
        "expected a short landing to bump the refused counter: {text}"
    );
    assert!(
        !text.contains("decdn_node_pull_reactive_topup_total 1"),
        "expected a short landing NOT to bump the success counter: {text}"
    );
}

/// Two poll intervals of 500 ms steps, so the upstream clears a full poll plus
/// its head cache — 28 steps (14 s) at the 7 s default.
#[test]
fn settle_budget_tracks_the_chain_poll_cadence() {
    assert_eq!(settle_wait_budget(Duration::from_secs(7)), 28);
    assert_eq!(settle_wait_budget(Duration::from_secs(1)), 4);
    // A chain lane tuned faster than one step still gets a real, if tiny, budget
    // rather than zero — a zero budget would make the top-up a coin flip.
    assert_eq!(settle_wait_budget(Duration::from_millis(250)), 1);
    // ...and a SLOW chain lane cannot buy unbounded foreground sleep.
    // `event_poll_interval_ms` has a config floor but no ceiling, so without
    // the cap a 60 s lane would sleep a client's pull for two minutes.
    assert_eq!(
        settle_wait_budget(Duration::from_secs(60)),
        MAX_SETTLE_WAITS
    );
    assert_eq!(
        settle_wait_budget(Duration::from_secs(3600)),
        MAX_SETTLE_WAITS
    );
}
