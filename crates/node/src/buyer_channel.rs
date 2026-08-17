//! On-chain `PaymentPool` buyer-side service (#744).
//!
//! The node is the *buyer* when it pulls content from an upstream provider on a
//! cache miss (ADR 003 §node→node). It owns one `PaymentPool` and signs vouchers
//! against per-`(signer, provider)` lanes of that single pool: the deposit fans
//! out across every upstream it pays. This service owns the on-chain half of that
//! path that the off-chain voucher signer in [`crate::client_requester`] leaves
//! open:
//!
//! - **One-time USDC approval.** `openPool` escrows the deposit via
//!   `safeTransferFrom`, so the node holds a standing ERC-20 allowance for the
//!   `PaymentPool` contract. At bootstrap it reads the allowance and, if
//!   insufficient, issues a single unlimited `approve` (ADR 003 § Deposit
//!   Economics one-time-approval design).
//! - **Lazy open + reuse.** [`BuyerPoolService::open_or_reuse_pool`] returns a
//!   [`PoolContext`] pinned to the upstream provider (`with_provider`) and
//!   carrying the node's own self-issued capability (`with_capability`), for the
//!   requester to sign vouchers against. It reuses the single pool the node owns,
//!   or opens one on the first miss (lazy-on-first-miss). One pool amortizes the
//!   deposit + gas across many pulls to many providers.
//! - **Abandonment reclaim.** A background sweep completes the grace-window
//!   `reclaim` of a pool the node has closed, refunding the residual and dropping
//!   the local record.
//!
//! Structurally this mirrors [`crate::payment_settlement::PoolSettlementService`]:
//! a generic-over-`Provider` struct owning an `AbortOnDrop` background task.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use alloy::dyn_abi::Eip712Domain;
use alloy::eips::BlockId;
use alloy::primitives::{Address, U256};
use alloy::providers::Provider;
use alloy::signers::local::PrivateKeySigner;
use anyhow::{Context, Result};
use decdn_incentive::payment_pool::PaymentPool;
use decdn_incentive::{
    AdvanceOutcome, BuyerPoolState, BuyerPoolStore, DepositOutcome, LaneKey, PoolId,
};
use futures_util::FutureExt;
use tracing::{debug, error, info, warn};

use crate::chain_events::AbortOnDrop;
use crate::client_requester::buyer_pool::{
    LOW_WATER_DIVISOR, ensure_allowance, issue_self_capability, open_pool, refill_amount,
    top_up as pool_top_up,
};
use crate::client_requester::{LocalPullFault, PoolContext};
use crate::metrics::Metrics;

/// How often the reclaim sweep scans the node's pool for a completed close.
/// Pool lifetimes are long, so an hourly scan is ample — it matches the seller
/// expiry-sweep cadence.
const RECLAIM_SWEEP_INTERVAL: Duration = Duration::from_hours(1);

/// Expiry stamped on the self-owned capability the buyer signs for its own key.
/// The single-user buyer owns both keys, so there is nothing to time-box —
/// `u64::MAX` means "never expires", and the pool's grace-window close is the
/// only lifecycle gate (there is no pool expiry, ADR 003). Matches the expiry
/// [`open_pool`] stamps, so the reuse-path capability recovers to the same grant.
const SELF_CAPABILITY_EXPIRY: u64 = u64::MAX;

/// Typed sentinel: the caller's `open_or_reuse_pool` budget elapsed while the
/// pool open is still in flight. The open **keeps going** in a detached task; the
/// caller retries the next candidate and a later miss reuses the pool once it
/// lands (#1143).
#[derive(Debug)]
pub struct PoolOpenPending {
    /// How long the caller waited before giving up on the answer.
    pub waited: Duration,
}

impl std::fmt::Display for PoolOpenPending {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "buyer pool open still in flight after {:?}; retry",
            self.waited
        )
    }
}

impl std::error::Error for PoolOpenPending {}

/// Typed marker: an open failure this layer has already logged and metered, so
/// the caller-side classification ladder does not restate it.
#[derive(Debug)]
pub struct OpenReported;

impl std::fmt::Display for OpenReported {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "buyer pool open failure (already reported)")
    }
}

impl std::error::Error for OpenReported {}

type OpenOutcome = Result<(), Arc<anyhow::Error>>;
type SharedOpen = futures_util::future::Shared<BoxFuture<'static, OpenOutcome>>;
type TopUpOutcome = Result<DepositOutcome, Arc<anyhow::Error>>;
type SharedTopUp = futures_util::future::Shared<BoxFuture<'static, TopUpOutcome>>;
type BoxFuture<'a, T> = std::pin::Pin<Box<dyn std::future::Future<Output = T> + Send + 'a>>;

/// Un-`Arc` a shared open error into a fresh chain for one waiter, preserving the
/// typed markers ([`OpenReported`], [`LocalPullFault`], `PoolOpenFailureReason`)
/// the classifier downcasts on. A `Shared` future hands every waiter the same
/// `Arc<anyhow::Error>`; a waiter that needs an owned `anyhow::Error` rebuilds one
/// that keeps the markers rather than the concrete source.
fn rehydrate_open_error(err: &Arc<anyhow::Error>) -> anyhow::Error {
    let mut rebuilt = anyhow::anyhow!("{err:#}");
    if err.downcast_ref::<OpenReported>().is_some() {
        rebuilt = rebuilt.context(OpenReported);
    }
    if err.downcast_ref::<LocalPullFault>().is_some() {
        rebuilt = rebuilt.context(LocalPullFault);
    }
    if let Some(reason) = err.downcast_ref::<decdn_incentive::PoolOpenFailureReason>() {
        rebuilt = rebuilt.context(*reason);
    }
    rebuilt
}

/// RAII claim on the single-pool open (or top-up) slot: clears it when the task
/// that holds it ends (any path, including a panic unwind), so a wedged task can
/// never pin the slot shut. A poisoned lock is recovered into its inner value —
/// releasing the slot always wins over propagating the poison.
struct SlotGuard<T> {
    slot: Arc<Mutex<Option<T>>>,
}

impl<T> Drop for SlotGuard<T> {
    fn drop(&mut self) {
        *self
            .slot
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = None;
    }
}

/// The `'static` slice of [`BuyerPoolService`] a detached funding task needs,
/// lifted off `&self` so the task can outlive any single caller.
struct FundingHandles<P: Provider + Clone + 'static> {
    contract: PaymentPool::PaymentPoolInstance<P>,
    store: Arc<dyn BuyerPoolStore>,
    rpc: P,
    token: Address,
    owner: Address,
    payment_pool_addr: Address,
    metrics: Arc<Metrics>,
}

/// Add `additional` USDC to the buyer pool `pool_id` on-chain, then credit the
/// returned amount into the persisted [`BuyerPoolState`]. The shared funding
/// kernel behind both the proactive low-water refill and the reactive mid-pull
/// top-up (#1146/#1530), so the allowance posture, the [`DepositOutcome`]
/// grading, and the metering cannot drift apart.
async fn fund_pool<P: Provider + Clone + 'static>(
    handles: &FundingHandles<P>,
    pool_id: PoolId,
    additional: U256,
) -> Result<DepositOutcome> {
    // Daemon posture: ensure the standing unlimited allowance (idempotent — skips
    // when already granted) so `topUp`'s `transferFrom` can pull the funds even if
    // the standing approval was revoked. Mirrors bootstrap.
    if let Err(err) = ensure_allowance(
        &handles.rpc,
        handles.token,
        handles.owner,
        handles.payment_pool_addr,
        None,
    )
    .await
    {
        warn!(
            %pool_id,
            %additional,
            error = %format!("{err:#}"),
            "buyer top-up: ensure_allowance failed; pool not topped up"
        );
        handles.metrics.buyer_topup_failure();
        return Err(err);
    }

    let credited = match pool_top_up(&handles.contract, pool_id, additional).await {
        Ok(credited) => credited,
        Err(err) => {
            warn!(
                %pool_id,
                %additional,
                error = %format!("{err:#}"),
                "buyer top-up: topUp failed; pool not topped up"
            );
            handles.metrics.buyer_topup_failure();
            return Err(err);
        }
    };

    // Credit the CHAIN-measured delta into the committed row inside one write txn,
    // so a concurrent settle cannot clobber the deposit or lose the top-up.
    match handles
        .store
        .add_deposit(handles.owner, pool_id, credited)
        .context("credit buyer pool top-up")?
    {
        outcome @ DepositOutcome::Added(_) => {
            handles.metrics.buyer_topup_ok();
            Ok(outcome)
        }
        // The topUp landed on-chain but the local row vanished or was replaced
        // during the RPC. Funds are escrowed-but-untracked — NOT a clean success,
        // so meter it as a failure rather than `buyer_topup_ok`, or an operator
        // watching the failure metric would miss stranded deposits (#1146 review).
        outcome => {
            error!(
                %pool_id,
                %credited,
                ?outcome,
                "buyer top-up: topUp landed on-chain but the local pool row could not be \
                 credited; the deposit is ESCROWED AND UNTRACKED — reconcile against the chain"
            );
            handles.metrics.buyer_topup_failure();
            Ok(outcome)
        }
    }
}

/// The proactive low-water top-up amount for a reused pool (#1146, #1103):
/// `U256::ZERO` when the remaining deposit still has headroom, else the amount
/// that restores it to the working `target`. `target = max(deposit_hint,
/// working_deposit)` — the graduation target a proven-good pool refills toward —
/// and the trigger is `target / LOW_WATER_DIVISOR` (20% remaining).
/// `committed` is the pool's cumulative vouchered amount across all its lanes, so
/// the remaining spendable is `deposit - committed`. Pure so the policy is
/// unit-testable; the shared [`refill_amount`] kernel is the same one the CLI
/// fetch auto-refill uses.
fn refill_decision(
    deposit: U256,
    committed: U256,
    deposit_hint: U256,
    working_deposit: U256,
) -> U256 {
    if working_deposit.is_zero() {
        // `0` disables top-up entirely (matches the CLI's auto-refill config
        // semantics) — a reused pool is never proactively refilled.
        return U256::ZERO;
    }
    let target = deposit_hint.max(working_deposit);
    let low_water = target / U256::from(LOW_WATER_DIVISOR);
    refill_amount(deposit, committed, target, low_water)
}

/// The deposit a FRESH open escrows (#1497): "open small, graduate on
/// proof". `max(deposit_hint, initial_deposit)` — deliberately the small
/// `initial_deposit`, not the larger `working_deposit` [`refill_decision`]
/// graduates a proven pool toward. `deposit_hint` still lets a caller ask for
/// more up front. Pure so the open path's sizing is unit-testable.
fn open_deposit(deposit_hint: U256, initial_deposit: U256) -> U256 {
    deposit_hint.max(initial_deposit)
}

/// The pool's cumulative vouchered amount across every `(signer, provider)` lane —
/// what has been spent against the single shared deposit. Remaining spendable is
/// `deposit - committed_amount(state)`.
fn committed_amount(state: &BuyerPoolState) -> U256 {
    state
        .lanes()
        .fold(U256::ZERO, |acc, (_, p)| acc.saturating_add(p.last_amount))
}

/// Build the [`PoolContext`] paying `provider_addr` from this pool's state: pin
/// the provider (`with_provider`) with the lane's prior cumulative totals, and
/// attach the node's self-issued capability (`with_capability`) so an upstream
/// registers the signer on its first on-chain redemption.
///
/// The provider pin is MANDATORY: a voucher signed with `provider == Address::ZERO`
/// hard-fails at signing, so a context left at `for_pool`'s ZERO provider cannot
/// pay a cache-miss pull.
///
/// # Errors
///
/// Propagates a signing error from the owner signer while issuing the capability.
fn pin_ctx(
    state: &BuyerPoolState,
    signer: &Arc<PrivateKeySigner>,
    voucher_domain: &Eip712Domain,
    provider_addr: Address,
) -> Result<PoolContext> {
    let lane = LaneKey {
        pool_id: state.pool_id,
        signer: signer.address(),
        provider: provider_addr,
    };
    let (prior_bytes, prior_amount) = state
        .lane_progress(lane)
        .map_or((U256::ZERO, U256::ZERO), |p| (p.last_bytes, p.last_amount));
    // Re-issue the self-owned capability from the owner key: it is node-agnostic
    // (valid at every provider this pool pays) and cheap to regenerate, so both
    // the fresh-open and the reused-pool paths present one without a stored copy.
    // Uncapped: the delegate IS the owner, so the pool deposit — not the
    // capability cap — is the real spending bound; a finite cap here would pin
    // the on-chain cap below a later `topUp` (`_registerCapability` is
    // idempotent past first redemption) and reject spend past it.
    let capability = issue_self_capability(
        signer.as_ref(),
        state.pool_id,
        U256::MAX,
        SELF_CAPABILITY_EXPIRY,
        voucher_domain,
    )?;
    Ok(
        PoolContext::for_pool(state, Arc::clone(signer), voucher_domain.clone())
            .with_provider(provider_addr, prior_bytes, prior_amount)
            .with_capability(capability),
    )
}

/// Buyer-side `PaymentPool` service. Generic over the alloy [`Provider`] (a
/// wallet-filled provider is required for the `approve` / `openPool` / `topUp` /
/// `closePool` / `reclaim` write paths). Cheap to construct; owns its background
/// reclaim task.
pub struct BuyerPoolService<P: Provider + Clone + 'static> {
    contract: PaymentPool::PaymentPoolInstance<P>,
    store: Arc<dyn BuyerPoolStore>,
    signer: Arc<PrivateKeySigner>,
    voucher_domain: Eip712Domain,
    token: Address,
    owner: Address,
    /// The small deposit a fresh `openPool` escrows (#1497): "open small,
    /// graduate on proof". Consumed by [`open_deposit`].
    initial_deposit: U256,
    /// The larger graduation target a reused pool's low-water refill tops up
    /// toward (see [`refill_decision`]).
    working_deposit: U256,
    /// The single in-flight `openPool`, if one is running (#1143). A concurrent
    /// [`Self::open_or_reuse_pool`] that arrives mid-open JOINS it rather than
    /// escrowing a second deposit. `None` when no open is running.
    open_in_flight: Arc<Mutex<Option<SharedOpen>>>,
    /// The single in-flight funding `topUp`, if one is running (#1146/#1530). Both
    /// the proactive low-water refill and the reactive mid-pull top-up dedup here,
    /// so the two legs racing on the one pool cannot double-escrow one shortfall.
    topup_in_flight: Arc<Mutex<Option<SharedTopUp>>>,
    metrics: Arc<Metrics>,
    _reclaimer: AbortOnDrop,
}

impl<P: Provider + Clone + 'static> BuyerPoolService<P> {
    /// Bootstrap the service: self-check the contract, read the immutable USDC
    /// token, issue the one-time USDC approval if requested, and spawn the
    /// reclaim sweep.
    ///
    /// `initial_deposit` is the small deposit a fresh open escrows;
    /// `working_deposit` is the larger target a reused pool's low-water refill
    /// graduates it toward (#1497).
    ///
    /// # Errors
    ///
    /// Returns an error if the `usdc()` self-check call fails (a bad
    /// `payment_pool_address` or unreachable RPC is fatal at bring-up), if the
    /// persisted buyer pools cannot be loaded, or if the one-time approval
    /// transaction fails.
    #[allow(clippy::too_many_arguments)]
    pub async fn bootstrap(
        provider: P,
        payment_pool_addr: Address,
        owner: Address,
        store: Arc<dyn BuyerPoolStore>,
        signer: Arc<PrivateKeySigner>,
        voucher_domain: Eip712Domain,
        initial_deposit: U256,
        working_deposit: U256,
        ensure_max_approval: bool,
        metrics: Arc<Metrics>,
    ) -> Result<Self> {
        let contract = PaymentPool::new(payment_pool_addr, provider.clone());

        let token = contract
            .usdc()
            .call()
            .await
            .with_context(|| format!("PaymentPool.usdc() self-check at {payment_pool_addr}"))?;

        if ensure_max_approval {
            // Daemon posture: an unlimited (`None`) standing approval for a
            // long-lived node, avoiding re-approve churn across many miss pulls.
            ensure_allowance(&provider, token, owner, payment_pool_addr, None).await?;
        }

        let load = store.load_all().context("hydrate persisted buyer pools")?;
        metrics.buyer_pool_store_skipped_undecodable_records(load.skipped.len());
        info!(
            %payment_pool_addr,
            %token,
            %owner,
            tracked = load.pools.len(),
            "BuyerPoolService bootstrap complete"
        );

        let reclaimer = tokio::spawn(reclaim_loop(
            contract.clone(),
            Arc::clone(&store),
            owner,
            Arc::clone(&metrics),
        ));

        Ok(Self {
            contract,
            store,
            signer,
            voucher_domain,
            token,
            owner,
            initial_deposit,
            working_deposit,
            open_in_flight: Arc::new(Mutex::new(None)),
            topup_in_flight: Arc::new(Mutex::new(None)),
            metrics,
            _reclaimer: AbortOnDrop(reclaimer),
        })
    }

    /// Load the node's pool, reporting a store fault at the severity it deserves —
    /// a node that cannot read its pool store can pay no one, so the fault is
    /// metered and marked [`OpenReported`] + [`LocalPullFault`] (node-wide, ours),
    /// not restated as an ordinary skipped candidate.
    fn reuse_or_report(&self) -> Result<Option<BuyerPoolState>> {
        self.store.get_by_owner(self.owner).map_err(|err| {
            self.metrics.node_pull_pool_open_failure();
            error!(
                error = %format!("{err:#}"),
                "buyer pool store read failed; this node can neither open nor reuse its \
                 payment pool until the store recovers"
            );
            anyhow::Error::new(err)
                .context("look up the node's buyer pool")
                .context(OpenReported)
                .context(LocalPullFault)
        })
    }

    /// Return a [`PoolContext`] for paying `provider_addr`, pinned to that
    /// provider and carrying the node's self-issued capability: reuse the single
    /// pool the node owns, or lazily open one.
    ///
    /// `deposit_hint` sizes a freshly-opened pool (`max(deposit_hint,
    /// initial_deposit)`); it is ignored on reuse (a low-water refill tops a live
    /// pool up instead).
    ///
    /// # Bounding (#1143)
    ///
    /// `budget` bounds how long the CALLER waits — not how long the open runs. When
    /// it expires this returns [`PoolOpenPending`] and the open **keeps going** in
    /// a detached task: a broadcast `openPool` tx that is dropped mid-flight would
    /// escrow a deposit against a pool nobody tracks, so the open is never
    /// cancelled, only stopped-waiting-on. The next miss reuses it once it lands.
    ///
    /// # Errors
    ///
    /// [`PoolOpenPending`] when `budget` expires with the open still running.
    /// Otherwise: store errors, any failure of the `openPool` transaction (submit,
    /// revert, or receipt — carrying a [`decdn_incentive::PoolOpenFailureReason`]
    /// the caller can `downcast_ref`), or a capability-signing fault.
    pub async fn open_or_reuse_pool(
        &self,
        provider_addr: Address,
        deposit_hint: U256,
        budget: Duration,
    ) -> Result<PoolContext> {
        // Fast path: reuse the live pool. A below-low-water pool kicks off a
        // detached refill (#1146) and hands THIS pull the current deposit
        // immediately — the refill must not sit in the hot path behind an on-chain
        // `topUp`.
        if let Some(state) = self.reuse_or_report()? {
            self.spawn_refill_if_low(&state, deposit_hint);
            return pin_ctx(&state, &self.signer, &self.voucher_domain, provider_addr);
        }

        let open = self.join_or_spawn_open(deposit_hint)?;

        match tokio::time::timeout(budget, open).await {
            // Still running. The task owns the tx; hand the caller a typed "not
            // yet" so it can move to the next candidate.
            Err(_) => Err(anyhow::Error::new(PoolOpenPending { waited: budget })),
            Ok(Err(err)) => Err(rehydrate_open_error(&err)),
            // The task persisted the pool before it signalled, so re-reading the
            // store is how we collect the result — the same read a later reuse does.
            Ok(Ok(())) => {
                let state = self.reuse_or_report()?.ok_or_else(|| {
                    self.metrics.node_pull_pool_open_failure();
                    error!(
                        "buyer pool opened on-chain but is not present in the store — a deposit \
                         is escrowed against a row we cannot see"
                    );
                    anyhow::anyhow!(
                        "buyer pool opened but is not present in the store (store/chain disagree?)"
                    )
                    .context(OpenReported)
                    .context(LocalPullFault)
                })?;
                pin_ctx(&state, &self.signer, &self.voucher_domain, provider_addr)
            }
        }
    }

    /// Best-effort background top-up of a reused pool that has run below its
    /// low-water mark (#1146). Spawns a detached task and returns immediately — it
    /// NEVER blocks the pull. Deduped via [`Self::topup_in_flight`], so many
    /// concurrent reuse pulls fire at most one `topUp`. Every leg is advisory: an
    /// in-flight refill, an allowance failure, or a reverted `topUp` all just skip
    /// it (logged / metered), leaving the pool un-topped-up — strictly no worse.
    fn spawn_refill_if_low(&self, state: &BuyerPoolState, deposit_hint: U256) {
        let additional = refill_decision(
            state.deposit,
            committed_amount(state),
            deposit_hint,
            self.working_deposit,
        );
        if additional.is_zero() {
            return; // still above the low-water mark
        }
        info!(
            pool_id = %state.pool_id,
            %additional,
            "buyer refill: reused pool below its low-water mark; funding toward the working \
             deposit (#1146)"
        );
        // Detached: the handle is DROPPED, not awaited — the task self-reports, and
        // a reactive top-up arriving while it runs JOINS the same future.
        drop(self.join_or_spawn_topup(state.pool_id, additional));
    }

    /// Everything the detached funding task needs, lifted off `&self` so the task
    /// can be `'static`.
    fn funding_handles(&self) -> FundingHandles<P> {
        FundingHandles {
            contract: self.contract.clone(),
            store: Arc::clone(&self.store),
            rpc: self.contract.provider().clone(),
            token: self.token,
            owner: self.owner,
            payment_pool_addr: *self.contract.address(),
            metrics: Arc::clone(&self.metrics),
        }
    }

    /// Join the in-flight funding `topUp`, or spawn one escrowing `additional`.
    /// Reservation and spawn are one indivisible step under the slot lock, so two
    /// callers cannot both decide to spawn; the task holds a [`SlotGuard`] that
    /// frees the slot when it ends (any path).
    ///
    /// # Errors
    ///
    /// A poisoned `topup_in_flight` lock — reported as this node's fault, since
    /// every future top-up would fail here.
    fn join_or_spawn_topup(&self, pool_id: PoolId, additional: U256) -> Result<SharedTopUp> {
        let mut slot = self.topup_in_flight.lock().map_err(|err| {
            self.metrics.buyer_topup_failure();
            error!(%err, "topup_in_flight mutex poisoned; this node must be restarted");
            anyhow::anyhow!("topup_in_flight mutex poisoned: {err}").context(LocalPullFault)
        })?;

        if let Some(existing) = slot.as_ref() {
            debug!(%pool_id, "joining a pool topUp already in flight");
            return Ok(existing.clone());
        }

        let handles = self.funding_handles();
        let guard = SlotGuard {
            slot: Arc::clone(&self.topup_in_flight),
        };
        let task = tokio::spawn(async move {
            // Frees the slot when the task ends (including a panic unwind), never
            // before — a wedged funding task cannot pin the slot shut.
            let _guard = guard;
            fund_pool(&handles, pool_id, additional)
                .await
                .map_err(Arc::new)
        });
        let fut: BoxFuture<'static, TopUpOutcome> = Box::pin(async move {
            task.await.unwrap_or_else(|join_err| {
                Err(Arc::new(anyhow::anyhow!(
                    "buyer pool topUp task failed: {join_err}"
                )))
            })
        });
        let shared = fut.shared();
        *slot = Some(shared.clone());
        Ok(shared)
    }

    /// Join the in-flight `openPool`, or spawn one. The one pool the node owns is
    /// opened at most once; concurrent misses join the single running open.
    ///
    /// # Errors
    ///
    /// A poisoned `open_in_flight` lock — reported as this node's fault
    /// ([`OpenReported`] + [`LocalPullFault`]), since every future open would fail.
    fn join_or_spawn_open(&self, deposit_hint: U256) -> Result<SharedOpen> {
        let mut slot = self.open_in_flight.lock().map_err(|err| {
            self.metrics.node_pull_pool_open_failure();
            error!(%err, "open_in_flight mutex poisoned; this node must be restarted");
            anyhow::anyhow!("open_in_flight mutex poisoned: {err}")
                .context(OpenReported)
                .context(LocalPullFault)
        })?;

        if let Some(existing) = slot.as_ref() {
            debug!("joining an openPool already in flight");
            return Ok(existing.clone());
        }

        let contract = self.contract.clone();
        let store = Arc::clone(&self.store);
        let signer = Arc::clone(&self.signer);
        let voucher_domain = self.voucher_domain.clone();
        let token = self.token;
        let owner = self.owner;
        // Open at the INITIAL deposit — "open small, graduate on proof".
        let deposit = open_deposit(deposit_hint, self.initial_deposit);
        let metrics = Arc::clone(&self.metrics);
        let guard = SlotGuard {
            slot: Arc::clone(&self.open_in_flight),
        };
        let task = tokio::spawn(async move {
            let _guard = guard;
            run_open(
                &contract,
                &store,
                signer,
                &voucher_domain,
                token,
                owner,
                deposit,
                &metrics,
            )
            .await
            .map_err(Arc::new)
        });
        let fut: BoxFuture<'static, OpenOutcome> = Box::pin(async move {
            task.await.unwrap_or_else(|join_err| {
                Err(Arc::new(
                    anyhow::anyhow!("buyer pool open task failed: {join_err}")
                        .context(OpenReported),
                ))
            })
        });
        let shared = fut.shared();
        *slot = Some(shared.clone());
        Ok(shared)
    }

    /// Persist the cumulative voucher totals after a delivery exchange so a later
    /// reuse (or a restart) resumes the `(signer, provider)` lane at the right
    /// cumulative `bytes` / `amount`. The buyer prices `cumulative = bytes × rate`
    /// from its own BLAKE3-verified bytes, so there is no nonce to track.
    ///
    /// # Errors
    ///
    /// Errors on a store write fault. A stale write (the owner's pool was replaced
    /// by a newer open) or a superseded watermark (a concurrent settle wrote a
    /// higher value first) is metered and returns `Ok(())` — neither loses a
    /// voucher and neither must fail the already-paid pull.
    pub fn record_progress(
        &self,
        provider_addr: Address,
        pool_id: PoolId,
        bytes_delivered: U256,
        amount: U256,
    ) -> Result<()> {
        let lane = LaneKey {
            pool_id,
            signer: self.signer.address(),
            provider: provider_addr,
        };
        match self
            .store
            .advance_progress(self.owner, pool_id, lane, bytes_delivered, amount)
            .context("advance buyer pool lane progress")?
        {
            AdvanceOutcome::Advanced => Ok(()),
            AdvanceOutcome::UnknownPool => {
                anyhow::bail!("record_progress for unknown pool {pool_id}")
            }
            // The owner's pool was replaced by a newer open between the delivery and
            // this write. Recording stale progress onto the new pool would be wrong;
            // real USDC was paid but its watermark is dropped, so it is METERED, not
            // just logged — a sustained rate would mean a pool-id plumbing bug rather
            // than the rare benign mid-pull replacement (#1145 review).
            AdvanceOutcome::PoolMismatch => {
                self.metrics.node_pull_progress_dropped();
                warn!(
                    provider = %provider_addr,
                    %pool_id,
                    "record_progress: the owner's pool was replaced by a newer open; \
                     skipping stale progress write"
                );
                Ok(())
            }
            // A CONCURRENT pull on this shared lane already persisted a higher
            // watermark, and the store is monotonic, so it kept the correct value and
            // rejected ours. No voucher is lost — the routine outcome of two settles
            // racing under `BuyerLedgers`, metered as benign (#1145 review).
            AdvanceOutcome::Regressed(_err) => {
                self.metrics.node_pull_progress_superseded();
                debug!(
                    provider = %provider_addr,
                    %pool_id,
                    "record_progress: a concurrent settle persisted a higher watermark first; \
                     ours superseded (benign under the shared ledger)"
                );
                Ok(())
            }
        }
    }

    /// Raise the node's pool toward `target_deposit` and return the pool's NEW
    /// total deposit (#1530). The reactive counterpart of the proactive low-water
    /// refill: the node-to-node pull loop calls this when an upstream rejects a
    /// voucher `SpendingCapExhausted` AND the buyer's own ledger corroborates it,
    /// then resumes on the larger deposit.
    ///
    /// `target_deposit` targets **spendable headroom**: the shortfall is computed
    /// against `deposit - committed_amount` (the amount already vouchered across
    /// every lane), so a pool whose deposit equals the target but is fully spent
    /// still gets the full amount.
    ///
    /// Routes through the detached, join-or-spawn funding task (never an inline
    /// `.await`): `topUp` waits on an unbounded `get_receipt`, and this runs inside
    /// the miss-pull future the foreground serve path DROPS on its deadline — an
    /// inline await cancelled mid-receipt would leave the deposit escrowed and
    /// untracked. On the spawned task, a dropped caller loses only the answer.
    ///
    /// # Errors
    ///
    /// Errors if no pool is tracked, if the allowance/`topUp` fails, or if the
    /// `topUp` mined but the local row could not be credited (terminal — the
    /// deposit is escrowed-and-untracked; a retry would escrow again).
    pub async fn top_up_pool(&self, target_deposit: U256) -> Result<U256> {
        let state = self
            .reuse_or_report()?
            .with_context(|| "no buyer pool tracked to top up")?;
        let additional = refill_amount(
            state.deposit,
            committed_amount(&state),
            target_deposit,
            target_deposit,
        );
        if additional.is_zero() {
            return Ok(state.deposit);
        }
        match self.join_or_spawn_topup(state.pool_id, additional)?.await {
            Ok(DepositOutcome::Added(new_deposit)) => {
                info!(
                    pool_id = %state.pool_id,
                    %additional,
                    %new_deposit,
                    "reactive top-up: pool exhausted mid-pull; raised toward the working deposit (#1530)"
                );
                Ok(new_deposit)
            }
            Ok(outcome @ (DepositOutcome::UnknownPool | DepositOutcome::PoolMismatch)) => {
                Err(anyhow::anyhow!(
                    "reactive top-up of {additional} µUSDC landed on-chain but the local record \
                     could not be credited ({outcome:?}): the deposit is ESCROWED AND UNTRACKED. \
                     Reconcile against the chain"
                ))
            }
            Err(err) => Err(anyhow::anyhow!("reactive top-up failed: {err:#}")),
        }
    }

    /// Run one reclaim-sweep pass synchronously: complete the grace-window
    /// `reclaim` of the node's pool if it has been closed and its dispute window
    /// has elapsed. Best-effort — errors are logged, never propagated. Exposed so
    /// the runtime (or a test) can trigger an immediate pass.
    pub async fn sweep_reclaimable_once(&self) {
        reclaim_once(&self.contract, &self.store, self.owner, &self.metrics).await;
    }
}

impl<P: Provider + Clone + 'static> std::fmt::Debug for BuyerPoolService<P> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BuyerPoolService")
            .field("address", self.contract.address())
            .field("owner", &self.owner)
            .finish_non_exhaustive()
    }
}

/// Object-safe seam over [`BuyerPoolService`] (#831).
///
/// `BuyerPoolService` is generic over the alloy [`Provider`], but the
/// node-to-node pull origin ([`crate::node_origin::NodeOrigin`]) is stored as an
/// `Arc<dyn Origin>` and cannot itself be generic. This trait erases the provider
/// type so the origin holds the bootstrapped service behind an `Arc<dyn
/// PoolOpener>` and opens (or reuses) the node's buyer pool on a cache-miss pull.
#[async_trait::async_trait]
pub trait PoolOpener: Send + Sync + std::fmt::Debug {
    /// Open or reuse the node's buyer pool and return a [`PoolContext`] pinned to
    /// `provider_addr` (with the self-issued capability attached). See
    /// [`BuyerPoolService::open_or_reuse_pool`] for the full contract.
    async fn open_or_reuse_pool(
        &self,
        provider_addr: Address,
        deposit_hint: U256,
        budget: Duration,
    ) -> Result<PoolContext>;

    /// Persist the cumulative voucher totals paid on the `(signer, provider)` lane
    /// of `pool_id`. See [`BuyerPoolService::record_progress`].
    ///
    /// # Errors
    ///
    /// On store write failure.
    fn record_progress(
        &self,
        provider_addr: Address,
        pool_id: PoolId,
        bytes_delivered: U256,
        amount: U256,
    ) -> Result<()>;

    /// Raise the node's pool toward `target_deposit`, returning its NEW total
    /// deposit. Defaults to "funding not supported" (`U256::ZERO`) so a read-only
    /// or test double need not override it.
    ///
    /// # Errors
    ///
    /// Implementations error when no pool is tracked, when the allowance or `topUp`
    /// fails, or when the tx lands but the local row can no longer be credited.
    async fn top_up_pool(&self, _target_deposit: U256) -> Result<U256> {
        Ok(U256::ZERO)
    }
}

#[async_trait::async_trait]
impl<P: Provider + Clone + 'static> PoolOpener for BuyerPoolService<P> {
    async fn open_or_reuse_pool(
        &self,
        provider_addr: Address,
        deposit_hint: U256,
        budget: Duration,
    ) -> Result<PoolContext> {
        BuyerPoolService::open_or_reuse_pool(self, provider_addr, deposit_hint, budget).await
    }

    fn record_progress(
        &self,
        provider_addr: Address,
        pool_id: PoolId,
        bytes_delivered: U256,
        amount: U256,
    ) -> Result<()> {
        BuyerPoolService::record_progress(self, provider_addr, pool_id, bytes_delivered, amount)
    }

    async fn top_up_pool(&self, target_deposit: U256) -> Result<U256> {
        BuyerPoolService::top_up_pool(self, target_deposit).await
    }
}

/// Open the node's buyer pool and persist it, unless a concurrent open already
/// landed one for this owner (the fast-path miss that spawned this task read the
/// store BEFORE the slot lock, so a previous open could have persisted in the
/// gap). Detached, so every failure leg reports for itself — by the time it fails
/// there may be nobody waiting to observe the `Err`.
#[allow(clippy::too_many_arguments)]
#[allow(clippy::cognitive_complexity)]
async fn run_open<P: Provider + Clone>(
    contract: &PaymentPool::PaymentPoolInstance<P>,
    store: &Arc<dyn BuyerPoolStore>,
    signer: Arc<PrivateKeySigner>,
    voucher_domain: &Eip712Domain,
    token: Address,
    owner: Address,
    deposit: U256,
    metrics: &Arc<Metrics>,
) -> Result<()> {
    // Re-check under the slot: a previous open for this owner may have persisted
    // between the caller's fast-path miss and our claiming the slot.
    match store.get_by_owner(owner) {
        Ok(Some(_)) => {
            debug!("a live buyer pool appeared while claiming the open slot; not opening a second");
            return Ok(());
        }
        Ok(None) => {}
        Err(err) => {
            error!(%err, "buyer pool store read failed under the open slot; cannot open a pool");
            metrics.node_pull_pool_open_failure();
            return Err(anyhow::Error::new(err))
                .context("look up the node's buyer pool under the open slot")
                .context(OpenReported)
                .context(LocalPullFault);
        }
    }

    let opened = open_pool(contract, signer, voucher_domain, token, owner, deposit)
        .await
        .inspect_err(|err| {
            error!(error = %format!("{err:#}"), "buyer pool open failed");
            metrics.node_pull_pool_open_failure();
        })?;

    if let Err(err) = store.record(&opened.state) {
        // The deposit is escrowed on-chain (`openPool` mined) but the row could not
        // be persisted — escrowed-but-untracked. Log the tx so an operator can
        // reconcile; the funds are safe on-chain, and re-persisting is idempotent.
        error!(
            tx = %opened.tx,
            pool_id = %opened.state.pool_id,
            %err,
            "buyer pool opened on-chain but its row could not be persisted; the deposit is \
             escrowed but UNTRACKED — reconcile against the tx"
        );
        metrics.node_pull_pool_open_failure();
        return Err(anyhow::Error::new(err))
            .context("persist opened buyer pool")
            .context(OpenReported)
            .context(LocalPullFault);
    }
    info!(pool_id = %opened.state.pool_id, %deposit, "opened buyer payment pool");
    Ok(())
}

/// Background reclaim sweep: complete the grace-window `reclaim` of the node's
/// pool once it has been closed and its dispute window has elapsed, refunding the
/// residual and dropping the local row. Best-effort — errors are logged, never
/// fatal. The node does not auto-close its pool (it is long-lived and reused); a
/// close is initiated on shutdown/operator action, and this sweep finishes the
/// refund permissionlessly (ADR 003 § grace-window close).
async fn reclaim_loop<P: Provider + Clone>(
    contract: PaymentPool::PaymentPoolInstance<P>,
    store: Arc<dyn BuyerPoolStore>,
    owner: Address,
    metrics: Arc<Metrics>,
) {
    let mut ticker = tokio::time::interval(RECLAIM_SWEEP_INTERVAL);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        ticker.tick().await;
        reclaim_once(&contract, &store, owner, &metrics).await;
    }
}

/// Chain head block timestamp. The grace-window pre-check compares against
/// `disputeDeadline`, which the contract set from `block.timestamp`, so it reads
/// the same clock the contract's `reclaim` gate uses rather than the node's wall
/// clock.
async fn head_timestamp<P: Provider>(provider: &P) -> Result<u64> {
    let block = provider
        .get_block(BlockId::latest())
        .await
        .context("read the latest block for the reclaim grace-window pre-check")?
        .ok_or_else(|| anyhow::anyhow!("no latest block"))?;
    Ok(block.header.timestamp)
}

/// One reclaim pass. Reads the node's tracked pool; if the on-chain pool has been
/// closed (`disputeDeadline > 0`) and its grace window has elapsed, submits
/// `reclaim` and forgets the row on success.
#[allow(clippy::cognitive_complexity)]
async fn reclaim_once<P: Provider + Clone>(
    contract: &PaymentPool::PaymentPoolInstance<P>,
    store: &Arc<dyn BuyerPoolStore>,
    owner: Address,
    metrics: &Arc<Metrics>,
) {
    let Some(state) = (match store.get_by_owner(owner) {
        Ok(state) => state,
        Err(err) => {
            warn!(%err, "reclaim sweep: buyer pool store read failed");
            metrics.buyer_reclaim_failure();
            return;
        }
    }) else {
        return; // no pool tracked
    };

    let pool = match contract.getPool(state.pool_id).call().await {
        Ok(pool) => pool,
        Err(err) => {
            warn!(pool_id = %state.pool_id, error = %format!("{err:#}"), "reclaim sweep: getPool failed");
            metrics.buyer_reclaim_failure();
            return;
        }
    };

    // Not closed: nothing to reclaim yet.
    if pool.disputeDeadline == 0 {
        return;
    }

    // Still inside the grace window: skip the doomed submit. The deadline is
    // chain time (`closePool` set it from `block.timestamp`), so this pre-check
    // reads the chain head — not the node's wall clock — to match the
    // `block.timestamp >= disputeDeadline` gate the contract enforces on
    // `reclaim`. A head-read failure leaves the check optimistic (attempt the
    // reclaim; the contract is the authoritative backstop).
    match head_timestamp(contract.provider()).await {
        Ok(now) if now < pool.disputeDeadline => return,
        Ok(_) => {}
        Err(err) => {
            debug!(pool_id = %state.pool_id, error = %format!("{err:#}"), "reclaim sweep: chain head read failed; attempting reclaim anyway");
        }
    }

    match contract.reclaim(state.pool_id).send().await {
        Ok(pending) => match pending.get_receipt().await {
            Ok(receipt) if receipt.status() => {
                if let Err(err) = store.forget_if_pool(owner, state.pool_id) {
                    warn!(pool_id = %state.pool_id, %err, "reclaim sweep: forget after reclaim failed");
                    return;
                }
                info!(pool_id = %state.pool_id, "reclaimed the buyer pool residual and dropped the row");
            }
            Ok(_) => {
                warn!(pool_id = %state.pool_id, "reclaim sweep: reclaim reverted");
                metrics.buyer_reclaim_failure();
            }
            Err(err) => {
                warn!(pool_id = %state.pool_id, error = %format!("{err:#}"), "reclaim sweep: reclaim receipt failed");
                metrics.buyer_reclaim_failure();
            }
        },
        Err(err) => {
            warn!(pool_id = %state.pool_id, error = %format!("{err:#}"), "reclaim sweep: reclaim submit failed");
            metrics.buyer_reclaim_failure();
        }
    }
}

#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    reason = "workspace anti-panic policy targets runtime code"
)]
#[cfg(test)]
mod tests {
    use decdn_incentive::{BuyerPoolState, LaneKey};

    use super::*;

    fn signer() -> Arc<PrivateKeySigner> {
        Arc::new(PrivateKeySigner::random())
    }

    fn pool_with_lane(
        signer_addr: Address,
        provider: Address,
        bytes: U256,
        amount: U256,
    ) -> BuyerPoolState {
        let pool_id = decdn_incentive::PoolId::from([7u8; 32]);
        let mut state = BuyerPoolState::new(
            pool_id,
            Address::repeat_byte(1),
            Address::repeat_byte(2),
            U256::from(10_000u64),
        );
        let lane = LaneKey {
            pool_id,
            signer: signer_addr,
            provider,
        };
        state.advance_lane(lane, bytes, amount).unwrap();
        state
    }

    #[test]
    fn refill_decision_no_topup_with_headroom() {
        // committed 100 of 10_000; working 10_000 → low-water 2_000; remaining huge.
        assert_eq!(
            refill_decision(
                U256::from(10_000u64),
                U256::from(100u64),
                U256::ZERO,
                U256::from(10_000u64)
            ),
            U256::ZERO
        );
    }

    #[test]
    fn refill_decision_tops_up_to_target_when_below_low_water() {
        // deposit 1_000, committed 900 → remaining 100 < low-water 2_000; refill to
        // target 10_000 restores remaining to 10_000 (adds 9_900).
        assert_eq!(
            refill_decision(
                U256::from(1_000u64),
                U256::from(900u64),
                U256::ZERO,
                U256::from(10_000u64)
            ),
            U256::from(9_900u64)
        );
    }

    #[test]
    fn refill_decision_zero_working_disables_topup() {
        assert_eq!(
            refill_decision(
                U256::from(1u64),
                U256::from(1u64),
                U256::from(10_000u64),
                U256::ZERO
            ),
            U256::ZERO
        );
    }

    #[test]
    fn open_deposit_uses_initial_not_working() {
        assert_eq!(
            open_deposit(U256::ZERO, U256::from(500u64)),
            U256::from(500u64)
        );
    }

    #[test]
    fn open_deposit_hint_wins_when_larger() {
        assert_eq!(
            open_deposit(U256::from(900u64), U256::from(500u64)),
            U256::from(900u64)
        );
    }

    #[test]
    fn committed_amount_sums_every_lane() {
        let s = signer();
        let mut state = pool_with_lane(
            s.address(),
            Address::repeat_byte(3),
            U256::from(10u64),
            U256::from(40u64),
        );
        let lane2 = LaneKey {
            pool_id: state.pool_id,
            signer: s.address(),
            provider: Address::repeat_byte(4),
        };
        state
            .advance_lane(lane2, U256::from(5u64), U256::from(60u64))
            .unwrap();
        assert_eq!(committed_amount(&state), U256::from(100u64));
    }

    #[test]
    fn pin_ctx_pins_provider_and_lane_priors_and_capability() {
        let s = signer();
        let provider = Address::repeat_byte(3);
        let state = pool_with_lane(s.address(), provider, U256::from(10u64), U256::from(40u64));
        let domain = Eip712Domain::default();

        let ctx = pin_ctx(&state, &s, &domain, provider).expect("pin");

        assert_eq!(
            ctx.provider, provider,
            "the delivering provider must be pinned (never ZERO)"
        );
        assert_eq!(ctx.pool_id, state.pool_id);
        assert_eq!(
            ctx.prior_bytes_delivered,
            U256::from(10u64),
            "lane priors seed the resume"
        );
        assert_eq!(ctx.prior_amount, U256::from(40u64));
        assert!(
            ctx.capability.is_some(),
            "the self-issued capability rides the request"
        );
    }

    #[test]
    fn pin_ctx_untouched_lane_starts_at_zero_priors() {
        let s = signer();
        let provider = Address::repeat_byte(9);
        let state = BuyerPoolState::new(
            decdn_incentive::PoolId::from([7u8; 32]),
            Address::repeat_byte(1),
            Address::repeat_byte(2),
            U256::from(10_000u64),
        );
        let domain = Eip712Domain::default();

        let ctx = pin_ctx(&state, &s, &domain, provider).expect("pin");

        assert_eq!(ctx.provider, provider);
        assert_eq!(ctx.prior_bytes_delivered, U256::ZERO);
        assert_eq!(ctx.prior_amount, U256::ZERO);
    }
}
