//! `NodeFunder` — the node's [`Funder`] adapter over
//! [`ChannelOpener::top_up_channel`], with the #1603 near-expiry guard.
//!
//! [`client-pull`](decdn_client_pull)'s gap-driven `drive()` reactively tops up
//! the buyer deposit through the injected [`Funder`] seam (`source.rs`) rather
//! than naming a chain handle directly, so the node's upstream cache-miss pull
//! leg (B2) can share that driver instead of running its own copy of the top-up
//! loop. `NodeFunder` is the bridge: it maps `Funder::top_up`'s DELTA
//! (`additional`) onto `ChannelOpener::top_up_channel`'s ABSOLUTE
//! `target_deposit`, using the shared [`ChannelContext`] the driver mutates as
//! the pull progresses for the "current deposit" half of that sum.
//!
//! The #1603 near-expiry rule — `topUp` cannot extend `expires_at`, so funding a
//! channel that is about to expire stands to strand capital — is NOT
//! re-derived here. It is `resume::near_expiry`, reused as the single source
//! of that policy; this module only calls it.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use alloy::primitives::{Address, U256};
use async_trait::async_trait;
use decdn_client_pull::source::SourceFuture;
use decdn_client_pull::{ChannelContext, Funder};
use decdn_incentive::DepositOutcome;

use super::resume::{MAX_REACTIVE_TOPUPS, near_expiry};
use crate::buyer_channel::ChannelOpener;

/// The node's [`Funder`]: reactively tops up ITS OWN upstream buyer channel to
/// `provider_addr` through an injected [`ChannelOpener`], refusing when the
/// channel is too close to its on-chain expiry (#1603).
///
/// # Fields
///
/// - `opener` / `provider_addr`: where the top-up lands. `Arc<dyn ChannelOpener>`
///   because the node origin already stores its buyer service behind that same
///   object-safe seam (`buyer_channel.rs:1439`) — no reason for this adapter to
///   be generic over the alloy `Provider` when its caller isn't.
/// - `ctx`: the driver's live [`ChannelContext`], shared (not copied) because its
///   `deposit` field grows across the fetch as earlier top-ups land — reading a
///   stale copy would under-shoot `target` on a channel that already got topped
///   up once this fetch by a DIFFERENT path (the proactive low-water refill can
///   fire concurrently; see `top_up_channel`'s own join-or-spawn dedup for why
///   that race is expected, not exotic). Locked only to copy `deposit` out; the
///   guard is never held across `.await` (`top_up_channel` is a network+chain
///   round trip).
/// - `min_ttl`: the #1603 margin, forwarded to `near_expiry` unchanged.
/// - `now_secs`: injected instead of reading `SystemTime` directly, so the
///   near-expiry branch is deterministic under test. Production wiring (Task 11)
///   passes `crate::payment_settlement::unix_now`.
#[allow(dead_code, reason = "wired by Task 11's driver construction")]
pub(crate) struct NodeFunder {
    opener: Arc<dyn ChannelOpener>,
    provider_addr: Address,
    ctx: Arc<Mutex<ChannelContext>>,
    min_ttl: Duration,
    now_secs: Arc<dyn Fn() -> u64 + Send + Sync>,
}

impl std::fmt::Debug for NodeFunder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NodeFunder")
            .field("provider_addr", &self.provider_addr)
            .field("min_ttl", &self.min_ttl)
            .finish_non_exhaustive()
    }
}

#[allow(dead_code, reason = "wired by Task 11's driver construction")]
impl NodeFunder {
    /// # Parameters
    ///
    /// `now_secs` yields the current Unix time in seconds — the same clock
    /// `resume::near_expiry`'s callers use — as a plain closure rather than a
    /// trait object tied to a monotonic epoch (unlike `decdn_cache`'s
    /// [`decdn_cache::circuit_breaker::Clock`], the #1603 margin is compared
    /// against an on-chain Unix timestamp, so it needs wall-clock seconds, not an
    /// arbitrary monotonic origin).
    pub(crate) fn new(
        opener: Arc<dyn ChannelOpener>,
        provider_addr: Address,
        ctx: Arc<Mutex<ChannelContext>>,
        min_ttl: Duration,
        now_secs: Arc<dyn Fn() -> u64 + Send + Sync>,
    ) -> Self {
        Self {
            opener,
            provider_addr,
            ctx,
            min_ttl,
            now_secs,
        }
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
            if let Some(expires_at) = self.opener.channel_expiry(self.provider_addr) {
                let now = (self.now_secs)();
                if near_expiry(expires_at, now, self.min_ttl) {
                    anyhow::bail!(
                        "refusing reactive top-up for provider {}: channel is within the \
                         #1603 near-expiry margin (expires_at={expires_at}, now={now}, \
                         min_ttl={:?}) — topUp cannot extend expires_at",
                        self.provider_addr,
                        self.min_ttl,
                    );
                }
            }

            let target = self.current_deposit().saturating_add(additional);
            let new_deposit = self
                .opener
                .top_up_channel(self.provider_addr, target)
                .await?;
            Ok(DepositOutcome::Added(new_deposit))
        })
    }
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::duration_suboptimal_units,
    reason = "workspace anti-panic policy targets runtime code"
)]
mod tests {
    use std::sync::atomic::{AtomicU32, Ordering};

    use alloy::dyn_abi::Eip712Domain;
    use alloy::primitives::B256;
    use alloy::signers::local::PrivateKeySigner;
    use anyhow::Result;
    use decdn_incentive::buyer_channel::NEVER_EXPIRES;

    use super::*;
    use decdn_incentive::ChannelId;

    use crate::buyer_channel::ChannelOpener;

    const NOW: u64 = 1_000_000;

    /// A configurable [`ChannelOpener`] double: `channel_expiry` returns a fixed
    /// value, `top_up_channel` records the `target_deposit` it was called with
    /// (and how many times) and returns a fixed outcome. Only `channel_expiry`
    /// and `top_up_channel` are exercised by `NodeFunder`; the rest of the trait
    /// is required by its signature but unreachable from these tests.
    #[derive(Debug)]
    struct MockOpener {
        expiry: Option<u64>,
        top_up_result: Result<U256, String>,
        top_up_calls: AtomicU32,
        last_target: Mutex<Option<U256>>,
    }

    impl MockOpener {
        fn new(expiry: Option<u64>, top_up_result: Result<U256, String>) -> Self {
            Self {
                expiry,
                top_up_result,
                top_up_calls: AtomicU32::new(0),
                last_target: Mutex::new(None),
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
    impl ChannelOpener for MockOpener {
        async fn open_or_reuse_channel(
            &self,
            _provider_addr: Address,
            _deposit_hint: U256,
            _budget: Duration,
        ) -> Result<ChannelContext> {
            unreachable!("not exercised by NodeFunder tests")
        }

        fn record_progress(
            &self,
            _provider_addr: Address,
            _channel_id: ChannelId,
            _nonce: U256,
            _bytes_delivered: U256,
            _amount: U256,
        ) -> Result<()> {
            unreachable!("not exercised by NodeFunder tests")
        }

        fn retire_channel(&self, _provider_addr: Address, _channel_id: ChannelId) -> Result<bool> {
            unreachable!("not exercised by NodeFunder tests")
        }

        fn channel_expiry(&self, _provider_addr: Address) -> Option<u64> {
            self.expiry
        }

        async fn top_up_channel(
            &self,
            _provider_addr: Address,
            target_deposit: U256,
        ) -> Result<U256> {
            self.top_up_calls.fetch_add(1, Ordering::SeqCst);
            *self
                .last_target
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(target_deposit);
            self.top_up_result.clone().map_err(|e| anyhow::anyhow!(e))
        }
    }

    fn test_ctx(deposit: U256) -> Arc<Mutex<ChannelContext>> {
        let signer = PrivateKeySigner::random();
        Arc::new(Mutex::new(ChannelContext {
            channel_id: B256::ZERO,
            token: Address::ZERO,
            deposit,
            client_signer: Arc::new(signer),
            voucher_domain: Eip712Domain::default(),
            prior_nonce: U256::ZERO,
            prior_bytes_delivered: U256::ZERO,
            prior_amount: U256::ZERO,
            client_binding: None,
        }))
    }

    fn funder(
        opener: Arc<MockOpener>,
        ctx: Arc<Mutex<ChannelContext>>,
        min_ttl: Duration,
        now: u64,
    ) -> NodeFunder {
        NodeFunder::new(opener, Address::ZERO, ctx, min_ttl, Arc::new(move || now))
    }

    #[tokio::test]
    async fn top_up_computes_target_from_current_deposit_plus_additional() {
        let deposit = U256::from(1_000u64);
        let additional = U256::from(250u64);
        let opener = Arc::new(MockOpener::new(None, Ok(U256::from(1_250u64))));
        let ctx = test_ctx(deposit);
        let f = funder(opener.clone(), ctx, Duration::from_secs(60), NOW);

        let outcome = f.top_up(additional).await.expect("top-up should succeed");

        assert_eq!(opener.last_target(), Some(deposit + additional));
        assert_eq!(outcome, DepositOutcome::Added(U256::from(1_250u64)));
    }

    #[tokio::test]
    async fn near_expiry_channel_refuses_topup() {
        let opener = Arc::new(MockOpener::new(Some(NOW + 10), Ok(U256::from(999u64))));
        let ctx = test_ctx(U256::from(100u64));
        let f = funder(opener.clone(), ctx, Duration::from_secs(60), NOW);

        let err = f.top_up(U256::from(50u64)).await.unwrap_err();

        assert!(err.to_string().contains("#1603"));
        assert_eq!(opener.call_count(), 0);
    }

    #[tokio::test]
    async fn never_expires_allows_topup() {
        let opener = Arc::new(MockOpener::new(Some(NEVER_EXPIRES), Ok(U256::from(999u64))));
        let ctx = test_ctx(U256::from(100u64));
        let f = funder(opener.clone(), ctx, Duration::from_secs(60), NOW);

        let outcome = f.top_up(U256::from(50u64)).await.expect("should proceed");

        assert_eq!(opener.call_count(), 1);
        assert_eq!(outcome, DepositOutcome::Added(U256::from(999u64)));
    }

    #[tokio::test]
    async fn zero_min_ttl_allows_topup() {
        // Expiry is imminent, but min_ttl == 0 disables the guard entirely.
        let opener = Arc::new(MockOpener::new(Some(NOW + 1), Ok(U256::from(999u64))));
        let ctx = test_ctx(U256::from(100u64));
        let f = funder(opener.clone(), ctx, Duration::from_secs(0), NOW);

        let outcome = f.top_up(U256::from(50u64)).await.expect("should proceed");

        assert_eq!(opener.call_count(), 1);
        assert_eq!(outcome, DepositOutcome::Added(U256::from(999u64)));
    }

    #[tokio::test]
    async fn top_up_channel_error_propagates() {
        let opener = Arc::new(MockOpener::new(None, Err("chain rejected".to_string())));
        let ctx = test_ctx(U256::from(100u64));
        let f = funder(opener.clone(), ctx, Duration::from_secs(60), NOW);

        let err = f.top_up(U256::from(50u64)).await.unwrap_err();

        assert!(err.to_string().contains("chain rejected"));
        assert_eq!(opener.call_count(), 1);
    }

    #[test]
    fn max_topups_is_one() {
        let opener = Arc::new(MockOpener::new(None, Ok(U256::ZERO)));
        let ctx = test_ctx(U256::ZERO);
        let f = funder(opener, ctx, Duration::from_secs(60), NOW);

        assert_eq!(f.max_topups(), MAX_REACTIVE_TOPUPS);
        assert_eq!(f.max_topups(), 1);
    }
}
