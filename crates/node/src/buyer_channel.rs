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
use decdn_incentive::erc20::Erc20;
use decdn_incentive::payment_pool::{PaymentPool, enumerate_owned_pools};
use decdn_incentive::{
    AdvanceOutcome, BuyerPoolState, BuyerPoolStore, LaneKey, PoolId, PoolOpenFailureReason,
};
use futures_util::FutureExt;
use tracing::{debug, error, info, warn};

use crate::chain_events::AbortOnDrop;
use crate::client_requester::buyer_pool::{
    LOW_WATER_DIVISOR, SELF_CAPABILITY_CAP, ToppedUpPool, ensure_allowance, grade_deposit_credit,
    issue_self_capability, open_pool, refill_amount, top_up as pool_top_up, topped_up_effect,
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
type TopUpOutcome = Result<TopUpLanded, Arc<anyhow::Error>>;
type SharedTopUp = futures_util::future::Shared<BoxFuture<'static, TopUpOutcome>>;
type BoxFuture<'a, T> = std::pin::Pin<Box<dyn std::future::Future<Output = T> + Send + 'a>>;

/// The result of a pool top-up: the pool's new total deposit, and how much of the
/// growth belongs to the caller.
///
/// `added` is the caller's own share. Another funder's concurrent `topUp` can raise
/// `new_deposit` by more than `added`, and that headroom is already spoken for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TopUpLanded {
    /// The pool's total deposit after the top-up, as the local pool row records it.
    pub new_deposit: U256,
    /// The part of the deposit growth that this call funded or claimed.
    pub added: U256,
}

/// Typed marker: a `topUp` mined but its deposit could not be credited to the local
/// pool row. The deposit is escrowed and untracked, so no caller funds again on top
/// of it — a second `topUp` would strand a second deposit the same way.
#[derive(Debug)]
struct EscrowedUntracked;

impl std::fmt::Display for EscrowedUntracked {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "topUp escrowed but not credited to the local pool row")
    }
}

impl std::error::Error for EscrowedUntracked {}

/// Who starts a funding `topUp`. The two differ in who may rely on its headroom.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TopUpFunder {
    /// The proactive low-water refill. No pull waits on its amount, so a reactive
    /// top-up that joins it can claim that amount as its own.
    Refill,
    /// A reactive mid-pull top-up. Its spawner relies on the whole amount, so no
    /// joiner can claim any of it.
    Reactive,
}

/// What a funding call gets from the `topUp` slot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TopUpClaim {
    /// The call started a `topUp` for its own amount.
    Spawned,
    /// The call joined a `topUp` another funder started, and claimed part of its
    /// unclaimed amount.
    Joined {
        /// How much of the joined `topUp` this call claimed. It can be zero.
        claimed: U256,
        /// The amount the joined `topUp` asked for. Claims are made against this
        /// amount before the `topUp` lands, so they hold only if all of it lands.
        requested: U256,
    },
}

/// The single in-flight funding `topUp` and the part of its amount no caller has
/// claimed yet.
struct InFlightTopUp {
    fut: SharedTopUp,
    /// The amount this `topUp` asks the chain for.
    requested: U256,
    /// Headroom this `topUp` adds that no pull relies on yet. A refill starts with
    /// its full amount; a reactive top-up starts with zero.
    unclaimed: U256,
}

impl InFlightTopUp {
    fn new(fut: SharedTopUp, requested: U256, funder: TopUpFunder) -> Self {
        let unclaimed = match funder {
            TopUpFunder::Refill => requested,
            TopUpFunder::Reactive => U256::ZERO,
        };
        Self {
            fut,
            requested,
            unclaimed,
        }
    }

    /// Join this `topUp`. A reactive joiner claims up to `want` of the unclaimed
    /// amount. A refill joiner claims nothing, because no pull waits on it.
    fn join(&mut self, want: U256, funder: TopUpFunder) -> (SharedTopUp, TopUpClaim) {
        let claimed = match funder {
            TopUpFunder::Refill => U256::ZERO,
            TopUpFunder::Reactive => self.unclaimed.min(want),
        };
        self.unclaimed = self.unclaimed.saturating_sub(claimed);
        let claim = TopUpClaim::Joined {
            claimed,
            requested: self.requested,
        };
        (self.fut.clone(), claim)
    }
}

/// The headroom a join counts once its `topUp` has landed. Claims are split out of
/// the requested amount before the chain answers. The contract credits the measured
/// transfer, which can be less than requested, and the claims can then add up to
/// more than landed. So a join counts its claim only when the whole requested amount
/// landed. Otherwise it counts nothing and funds its need with its own `topUp`.
fn joined_headroom(claimed: U256, requested: U256, landed: U256) -> U256 {
    if landed >= requested {
        claimed
    } else {
        U256::ZERO
    }
}

/// How many funding calls one reactive top-up makes before it gives up. A join that
/// covers less than the request, or that fails, is followed by a call for the
/// remainder. That call normally spawns, because the joined task frees the slot
/// before its result reaches any waiter. This bound stops a caller that keeps losing
/// the slot to other funders from looping without end.
const MAX_TOPUP_CALLS: u32 = 3;

/// Fund at least `additional` of new headroom for one reactive top-up on `pool_id`.
///
/// `join_or_spawn(amount)` joins the in-flight `topUp` or spawns one for `amount`.
///
/// - A spawned `topUp` escrows this caller's own amount, so its result is final.
///   A spawned `topUp` that credits less than requested is not retried: the
///   shortfall is not from a join, and another call escrows a second `topUp`.
/// - A joined `topUp` counts only for the amount the join claimed, and only when
///   the whole requested amount landed (see [`joined_headroom`]). When that is less
///   than the remainder, or when the joined `topUp` fails, this warns and asks
///   again for the rest.
///
/// When [`MAX_TOPUP_CALLS`] calls end short, this returns what landed and warns.
/// The caller compares [`TopUpLanded::added`] with its request.
///
/// # Errors
///
/// Propagates the error of a spawned `topUp`, and of any `topUp` whose deposit is
/// escrowed but untracked. Errors when every call joins a `topUp` that fails.
async fn top_up_at_least<F, Fut>(
    pool_id: PoolId,
    additional: U256,
    mut join_or_spawn: F,
) -> Result<TopUpLanded>
where
    F: FnMut(U256) -> (Fut, TopUpClaim),
    Fut: std::future::Future<Output = TopUpOutcome>,
{
    let mut added = U256::ZERO;
    let mut new_deposit = None;
    let mut last_err = None;
    for _ in 0..MAX_TOPUP_CALLS {
        let (fut, claim) = join_or_spawn(additional.saturating_sub(added));
        let landed = match (fut.await, claim) {
            (Ok(landed), TopUpClaim::Spawned) => {
                added = added.saturating_add(landed.added);
                let out = TopUpLanded {
                    new_deposit: landed.new_deposit,
                    added,
                };
                if added < additional {
                    warn_short_top_up(pool_id, additional, out, "our own topUp credited less");
                }
                return Ok(out);
            }
            (Ok(landed), TopUpClaim::Joined { claimed, requested }) => {
                added = added.saturating_add(joined_headroom(claimed, requested, landed.added));
                TopUpLanded {
                    new_deposit: landed.new_deposit,
                    added,
                }
            }
            (Err(err), claim) => {
                last_err = Some(retryable_join_error(pool_id, additional, err, claim)?);
                continue;
            }
        };
        if added >= additional {
            return Ok(landed);
        }
        warn_short_top_up(pool_id, additional, landed, "joined topUp covered less");
        new_deposit = Some(landed.new_deposit);
    }
    match (new_deposit, last_err) {
        (Some(new_deposit), _) => {
            let out = TopUpLanded { new_deposit, added };
            warn_short_top_up(pool_id, additional, out, "funding calls ran out");
            Ok(out)
        }
        (None, Some(err)) => Err(anyhow::anyhow!(
            "reactive top-up failed: every joined topUp failed, last: {err:#}"
        )),
        (None, None) => Err(anyhow::anyhow!(
            "reactive top-up made no funding call (MAX_TOPUP_CALLS is zero)"
        )),
    }
}

/// Warn that a reactive top-up on `pool_id` has less than `requested` so far.
fn warn_short_top_up(pool_id: PoolId, requested: U256, landed: TopUpLanded, why: &str) {
    warn!(
        %pool_id,
        %requested,
        landed = %landed.added,
        new_deposit = %landed.new_deposit,
        why,
        "reactive top-up is short of the requested amount"
    );
}

/// Decide whether a failed `topUp` lets the reactive top-up try again. A joined
/// `topUp` failure is another funder's, so the caller funds with its own `topUp`
/// and gets the error back to report if every call fails.
///
/// # Errors
///
/// A spawned `topUp` failure is this caller's own and is final. So is an
/// escrowed-but-untracked failure: a second `topUp` against a row that cannot be
/// credited strands a second deposit. `fund_pool` has already graded that case
/// into the error, tx and all, so there is nothing to re-diagnose.
fn retryable_join_error(
    pool_id: PoolId,
    requested: U256,
    err: Arc<anyhow::Error>,
    claim: TopUpClaim,
) -> Result<Arc<anyhow::Error>> {
    if claim == TopUpClaim::Spawned || err.downcast_ref::<EscrowedUntracked>().is_some() {
        return Err(anyhow::anyhow!("reactive top-up failed: {err:#}"));
    }
    warn!(
        %pool_id,
        %requested,
        error = %format_args!("{err:#}"),
        "reactive top-up: the joined topUp failed; funding with our own topUp"
    );
    Ok(err)
}

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

/// Run `attempt` (a `topUp`), and only if it fails with an
/// [`AllowanceShortfall`](crate::client_requester::buyer_pool::AllowanceShortfall)
/// run `recover_allowance` (a just-in-time `approve`) and retry `attempt`
/// exactly once. Any other error — and any error from the recovery or the
/// retry — returns as-is. Generic over the two async effects so the retry
/// control flow is unit-testable with plain closures and counters, no RPC
/// (matches the repo's extracted-control-flow posture). On the happy path
/// `attempt` runs once and
/// `recover_allowance` never runs, so the node issues zero allowance reads
/// per top-up.
///
/// `T` is whatever the top-up returns, carried through untouched — from the
/// retry on a recovered allowance, so it describes the second transaction.
/// `attempt` may run twice, so producing a `T` must be safe to repeat.
async fn top_up_recovering_allowance<T, A, AFut, R, RFut>(
    attempt: A,
    recover_allowance: R,
) -> Result<T>
where
    A: Fn() -> AFut,
    AFut: std::future::Future<Output = Result<T>>,
    R: FnOnce() -> RFut,
    RFut: std::future::Future<Output = Result<()>>,
{
    match attempt().await {
        Ok(out) => Ok(out),
        Err(err)
            if err
                .downcast_ref::<crate::client_requester::buyer_pool::AllowanceShortfall>()
                .is_some() =>
        {
            // The standing approval was revoked or never granted; do the
            // just-in-time `approve`, then retry the transfer exactly once.
            recover_allowance().await?;
            attempt().await
        }
        Err(err) => Err(err),
    }
}

/// Add `additional` USDC to the buyer pool `pool_id` on-chain, then credit the
/// returned amount into the persisted [`BuyerPoolState`] and return the new
/// deposit. The shared funding kernel behind both the proactive low-water refill
/// and the reactive mid-pull top-up (#1146/#1530), so the allowance posture, the
/// deposit-credit grading, and the metering cannot drift apart.
///
/// # Errors
///
/// Errors if the allowance or `topUp` fails — the funds did not move — and if a
/// mined `topUp` cannot be credited locally, in which case the deposit is
/// escrowed and the error names the tx.
async fn fund_pool<P: Provider + Clone + 'static>(
    handles: &FundingHandles<P>,
    pool_id: PoolId,
    additional: U256,
) -> Result<TopUpLanded> {
    // Daemon posture: attempt the transfer directly against the standing
    // unlimited allowance granted at bootstrap. Only when `topUp` reverts with
    // an allowance shortfall (the approval was revoked or never granted) do a
    // just-in-time `approve` and retry once — so the happy path issues zero
    // allowance reads per top-up.
    let ToppedUpPool { credited, tx } = match top_up_recovering_allowance(
        || pool_top_up(&handles.contract, pool_id, additional),
        || {
            ensure_allowance(
                &handles.rpc,
                handles.token,
                handles.owner,
                handles.payment_pool_addr,
                None,
            )
        },
    )
    .await
    {
        Ok(topped_up) => topped_up,
        Err(err) => {
            warn!(
                %pool_id,
                %additional,
                error = %format_args!("{err:#}"),
                "buyer top-up: topUp failed; pool not topped up"
            );
            handles.metrics.buyer_topup_failure();
            return Err(err);
        }
    };

    // Credit the CHAIN-measured delta into the committed row inside one write txn,
    // so a concurrent settle cannot clobber the deposit or lose the top-up.
    // `add_deposit` splits its failures across two channels — a backend fault is
    // the `Err`, a committed-row mismatch (the row vanished or was replaced
    // during the RPC) is a non-`Added` `Ok`. Both mean the topUp landed and the
    // funds are escrowed-but-untracked, so both meter as a failure rather than
    // `buyer_topup_ok`, or an operator watching the failure metric would miss
    // stranded deposits (#1146 review). Grading both here is also what puts the
    // tx in the propagated error: `DepositOutcome` has nowhere to carry it.
    let effect = topped_up_effect(pool_id, credited);
    match grade_deposit_credit(
        handles.store.add_deposit(handles.owner, pool_id, credited),
        &effect,
        tx,
    ) {
        Ok(new_deposit) => {
            handles.metrics.buyer_topup_ok();
            Ok(TopUpLanded {
                new_deposit,
                added: credited,
            })
        }
        Err(err) => {
            error!(
                %pool_id,
                %credited,
                %tx,
                error = %format_args!("{err:#}"),
                "buyer top-up: topUp landed on-chain but the local pool row could not be \
                 credited; the deposit is ESCROWED AND UNTRACKED — reconcile against the chain"
            );
            handles.metrics.buyer_topup_failure();
            Err(err.context(EscrowedUntracked))
        }
    }
}

/// The proactive low-water top-up amount for a reused pool (#1146, #1103):
/// `U256::ZERO` when the remaining deposit still has headroom, else the amount
/// that restores it to the `working_deposit` target — the deposit a proven-good
/// pool refills toward — with the trigger at `working_deposit / LOW_WATER_DIVISOR`
/// (20% remaining). `committed` is the pool's cumulative vouchered amount across
/// all its lanes, so the remaining spendable is `deposit - committed`. Pure so the
/// policy is unit-testable; the shared [`refill_amount`] kernel is the same one
/// the CLI fetch auto-refill uses.
fn refill_decision(deposit: U256, committed: U256, working_deposit: U256) -> U256 {
    let low_water = working_deposit / U256::from(LOW_WATER_DIVISOR);
    refill_amount(deposit, committed, working_deposit, low_water)
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
        SELF_CAPABILITY_CAP,
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
    /// The deposit a fresh `openPool` escrows, and the target a reused pool's
    /// low-water refill tops up toward (see [`refill_decision`]). The shared pool
    /// is fully withdrawable, so the open escrows the working deposit directly.
    working_deposit: U256,
    /// The single in-flight `openPool`, if one is running (#1143). A concurrent
    /// [`Self::open_or_reuse_pool`] that arrives mid-open JOINS it rather than
    /// escrowing a second deposit. `None` when no open is running.
    open_in_flight: Arc<Mutex<Option<SharedOpen>>>,
    /// The single in-flight funding `topUp`, if one is running (#1146/#1530). Both
    /// the proactive low-water refill and the reactive mid-pull top-up dedup here,
    /// so the two legs racing on the one pool cannot double-escrow one shortfall. The
    /// slot also tracks how much of the running `topUp` no pull has claimed, so two
    /// reactive top-ups cannot both count one escrow as their own.
    topup_in_flight: Arc<Mutex<Option<InFlightTopUp>>>,
    metrics: Arc<Metrics>,
    _reclaimer: AbortOnDrop,
}

impl<P: Provider + Clone + 'static> BuyerPoolService<P> {
    /// Bootstrap the service: self-check the contract, read the immutable USDC
    /// token, issue the one-time USDC approval if requested, and spawn the
    /// reclaim sweep.
    ///
    /// `working_deposit` is the deposit a fresh open escrows and the target a
    /// reused pool's low-water refill tops it up toward.
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

        let adopted = reconcile_owned_pool(&contract, &store, owner, token, &metrics).await;
        info!(
            %payment_pool_addr,
            %token,
            %owner,
            tracked = load.pools.len(),
            adopted,
            "BuyerPoolService bootstrap complete"
        );

        publish_buyer_wallet_usdc(&provider, token, owner, &metrics).await;

        let reclaimer = tokio::spawn(reclaim_loop(
            contract.clone(),
            Arc::clone(&store),
            owner,
            token,
            Arc::clone(&metrics),
        ));

        Ok(Self {
            contract,
            store,
            signer,
            voucher_domain,
            token,
            owner,
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
                error = %format_args!("{err:#}"),
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
    /// A freshly-opened pool escrows the service's `working_deposit`; on reuse
    /// that deposit is ignored (a low-water refill tops a live pool up instead).
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
        budget: Duration,
    ) -> Result<PoolContext> {
        // Fast path: reuse the live pool. A below-low-water pool kicks off a
        // detached refill (#1146) and hands THIS pull the current deposit
        // immediately — the refill must not sit in the hot path behind an on-chain
        // `topUp`.
        if let Some(state) = self.reuse_or_report()? {
            self.spawn_refill_if_low(&state);
            let state = self.reseed_lane_from_chain(state, provider_addr).await?;
            return pin_ctx(&state, &self.signer, &self.voucher_domain, provider_addr);
        }

        let open = self.join_or_spawn_open();

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

    /// Seed a lane's committed progress from the chain when this node has no
    /// local record of it, returning the state `pin_ctx` should pin.
    ///
    /// `PoolLedger` treats the pinned priors as an *offset*: it signs
    /// `prior + accrued`. A lane the node has paid on before therefore has to
    /// resume from what that lane was already paid, and the buyer store is the
    /// only thing that remembers it. Resuming a known lane from zero signs
    /// cumulatives at or below the contract's watermark, which `_applyVoucher`
    /// treats as transient-empty and pays nothing for — the node would stream
    /// real bytes and buy none of them.
    ///
    /// Reached whenever the local record is missing and the chain's is not: a
    /// pool adopted at bootstrap after a store reset, or a row lost under a
    /// partially-restored store. `advance_progress` is monotone, so a lane the
    /// node is already ahead of keeps its local watermark.
    ///
    /// **A failed read refuses the pull.** Resuming the lane from zero is not a
    /// recoverable degradation: the first pull persists its own progress on every
    /// exit path (`SettleOnDrop`), so a zero-resumed lane gains a local row, the
    /// `lane_progress` guard above then matches, and this reseed never runs for
    /// that provider again. One transient RPC blip would strand the lane below
    /// the chain watermark until it climbed back organically — one rejected
    /// voucher at a time. A node that cannot read a lane's watermark cannot price
    /// that lane, so it refuses rather than delivering bytes it cannot buy.
    ///
    /// The refusal is [`LocalPullFault`]: the failure is this node's chain lane,
    /// not the upstream's, so the classifier exonerates the peer and refuses
    /// instead of reporting the blob absent.
    ///
    /// # Errors
    ///
    /// The `getWatermark` read failed, or the seed could not be persisted.
    ///
    /// The chain's watermark counts *redeemed* vouchers only. A provider still
    /// holding an unredeemed voucher is ahead of it by at most its own
    /// redemption threshold, and rejects this node's first vouchers until the
    /// lane catches up. That window closes on the provider's next redemption.
    async fn reseed_lane_from_chain(
        &self,
        state: BuyerPoolState,
        provider_addr: Address,
    ) -> Result<BuyerPoolState> {
        let lane = LaneKey {
            pool_id: state.pool_id,
            signer: self.signer.address(),
            provider: provider_addr,
        };
        if state.lane_progress(lane).is_some() {
            return Ok(state);
        }
        let onchain = match self
            .contract
            .getWatermark(state.pool_id, lane.signer, provider_addr)
            .call()
            .await
        {
            Ok(onchain) => onchain,
            Err(err) => {
                self.metrics.buyer_lane_seed_failure();
                error!(
                    pool_id = %state.pool_id, %provider_addr, error = %err,
                    "could not read this lane's on-chain watermark; refusing the pull rather \
                     than resuming the lane from zero, which would strand it below the \
                     watermark permanently"
                );
                return Err(anyhow::Error::new(err)
                    .context("read the lane's on-chain watermark")
                    .context(OpenReported)
                    .context(LocalPullFault));
            }
        };
        if onchain.amount == 0 && onchain.bytesDelivered == 0 {
            // The chain has never paid this lane, so zero is the right resume
            // point. A provider holding vouchers below its own redemption
            // threshold also reads zero here; it rejects this node's first
            // vouchers until it redeems, which is self-limiting.
            debug!(
                pool_id = %state.pool_id, %provider_addr,
                "lane has no on-chain watermark; resuming it from zero"
            );
            return Ok(state);
        }
        self.persist_lane_seed(lane, &onchain)?;
        Ok(self.pin_seeded_state(state, lane, &onchain))
    }

    /// The state to pin once a lane seed is committed: the freshly-read row when
    /// the store can be read, and the snapshot with the seed applied when it
    /// cannot.
    ///
    /// Never the unseeded snapshot. The store has just committed the seed, so
    /// pinning zero priors against a committed watermark would sign cumulatives
    /// the contract pays nothing for — and then persist that regression on the
    /// way out, where it reads as ordinary shared-ledger race noise.
    fn pin_seeded_state(
        &self,
        state: BuyerPoolState,
        lane: LaneKey,
        onchain: &PaymentPool::Lane,
    ) -> BuyerPoolState {
        match self.store.get_by_owner(self.owner) {
            Ok(Some(seeded)) => seeded,
            Ok(None) => {
                warn!(
                    pool_id = %lane.pool_id,
                    "the buyer pool row vanished between persisting a lane seed and \
                     re-reading it; pinning the seed from memory"
                );
                Self::apply_seed_in_memory(state, lane, onchain)
            }
            Err(err) => {
                warn!(
                    pool_id = %lane.pool_id,
                    error = %format_args!("{err:#}"),
                    "could not re-read the buyer pool after seeding a lane; pinning the \
                     seed from memory"
                );
                Self::apply_seed_in_memory(state, lane, onchain)
            }
        }
    }

    /// Apply a committed lane seed to an in-memory snapshot, so a pinned context
    /// carries it even when the store cannot be re-read. Monotone via
    /// [`BuyerPoolState::advance_lane`]; a snapshot already ahead keeps its own
    /// watermark.
    fn apply_seed_in_memory(
        mut state: BuyerPoolState,
        lane: LaneKey,
        onchain: &PaymentPool::Lane,
    ) -> BuyerPoolState {
        if let Err(err) = state.advance_lane(
            lane,
            U256::from(onchain.bytesDelivered),
            U256::from(onchain.amount),
        ) {
            debug!(error = %err, "in-memory lane seed did not advance the snapshot");
        }
        state
    }

    /// Commit one lane seed read from the chain. Split from
    /// [`Self::reseed_lane_from_chain`] so the read, the decision and the write
    /// each stay legible on their own.
    ///
    /// An outcome other than `Advanced` is not an error: `advance_progress` is
    /// monotone, so a committed row already at or beyond the seed is the answer
    /// the caller wanted. Only a store fault fails, because that leaves the lane
    /// unpriced.
    ///
    /// # Errors
    ///
    /// The buyer store could not commit the seed.
    fn persist_lane_seed(&self, lane: LaneKey, onchain: &PaymentPool::Lane) -> Result<()> {
        match self.store.advance_progress(
            self.owner,
            lane.pool_id,
            lane,
            U256::from(onchain.bytesDelivered),
            U256::from(onchain.amount),
        ) {
            Ok(AdvanceOutcome::Advanced) => {
                info!(
                    pool_id = %lane.pool_id,
                    provider_addr = %lane.provider,
                    amount = onchain.amount,
                    bytes = onchain.bytesDelivered,
                    "seeded a lane from its on-chain watermark; this node had no local record \
                     of having paid on it"
                );
                Ok(())
            }
            Ok(other) => {
                debug!(
                    pool_id = %lane.pool_id, provider_addr = %lane.provider, ?other,
                    "lane seed from chain did not advance the committed row"
                );
                Ok(())
            }
            Err(err) => {
                self.metrics.buyer_lane_seed_failure();
                error!(
                    pool_id = %lane.pool_id, provider_addr = %lane.provider,
                    error = %format_args!("{err:#}"),
                    "could not persist a lane seed; refusing the pull rather than resuming \
                     the lane from zero"
                );
                Err(anyhow::Error::new(err)
                    .context("persist the lane's on-chain watermark")
                    .context(OpenReported)
                    .context(LocalPullFault))
            }
        }
    }

    /// Best-effort background top-up of a reused pool that has run below its
    /// low-water mark (#1146). Spawns a detached task and returns immediately — it
    /// NEVER blocks the pull. Deduped via [`Self::topup_in_flight`], so many
    /// concurrent reuse pulls fire at most one `topUp`. An in-flight refill, an
    /// allowance failure, or a reverted `topUp` all just skip it (logged /
    /// metered), leaving the pool un-topped-up — strictly no worse. The one leg
    /// that IS worse is a `topUp` that mines and cannot be credited locally: the
    /// deposit is then escrowed and untracked, and because this handle is
    /// dropped, `fund_pool`'s `error!` and `buyer_topup_failure()` are all the
    /// operator gets.
    fn spawn_refill_if_low(&self, state: &BuyerPoolState) {
        let additional =
            refill_decision(state.deposit, committed_amount(state), self.working_deposit);
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
        // a reactive top-up arriving while it runs JOINS the same future and can
        // claim its amount.
        drop(self.join_or_spawn_topup(state.pool_id, additional, TopUpFunder::Refill));
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
    /// frees the slot when it ends (any path). The returned [`TopUpClaim`] says
    /// which, and how much of a joined `topUp` this caller claimed (see
    /// [`InFlightTopUp::join`]).
    fn join_or_spawn_topup(
        &self,
        pool_id: PoolId,
        additional: U256,
        funder: TopUpFunder,
    ) -> (SharedTopUp, TopUpClaim) {
        // Recover the slot on poison rather than treat a prior holder's panic as
        // node-fatal (as [`SlotGuard`]'s own Drop does): the guarded value is a
        // single `Option<Shared…>` move that cannot tear, so the worst a panic
        // leaves is a stale slot this call overwrites. Refusing here would wedge
        // ALL funding node-wide for one unrelated panic.
        let mut slot = self
            .topup_in_flight
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);

        if let Some(existing) = slot.as_mut() {
            debug!(%pool_id, "joining a pool topUp already in flight");
            return existing.join(additional, funder);
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
        *slot = Some(InFlightTopUp::new(shared.clone(), additional, funder));
        (shared, TopUpClaim::Spawned)
    }

    /// Join the in-flight `openPool`, or spawn one. The one pool the node owns is
    /// opened at most once; concurrent misses join the single running open.
    fn join_or_spawn_open(&self) -> SharedOpen {
        // Recover on poison rather than treat one panic as node-fatal — see
        // [`Self::join_or_spawn_topup`] for why the slot is safe to recover.
        let mut slot = self
            .open_in_flight
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);

        if let Some(existing) = slot.as_ref() {
            debug!("joining an openPool already in flight");
            return existing.clone();
        }

        let contract = self.contract.clone();
        let store = Arc::clone(&self.store);
        let signer = Arc::clone(&self.signer);
        let voucher_domain = self.voucher_domain.clone();
        let token = self.token;
        let owner = self.owner;
        // The shared pool is fully withdrawable, so open at the working deposit.
        let deposit = self.working_deposit;
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
        let join_metrics = Arc::clone(&self.metrics);
        let fut: BoxFuture<'static, OpenOutcome> = Box::pin(async move {
            task.await.unwrap_or_else(|join_err| {
                // Meters here to hold the invariant the classifier relies on: a site
                // marks `OpenReported` if and only if it meters the total. Without
                // this the open task could panic and move no counter at all.
                //
                // `LocalPullFault` too, and for the same reason the store legs carry
                // it: the open task dying is this node's machinery failing, not the
                // upstream's, so the client is refused rather than told the blob does
                // not exist. A cancellation at shutdown takes the same path, where
                // "do not retry this node" is if anything the more useful answer.
                join_metrics.node_pull_pool_open_failure();
                error!(%join_err, "buyer pool open task did not run to completion");
                Err(Arc::new(
                    anyhow::anyhow!("buyer pool open task failed: {join_err}")
                        .context(OpenReported)
                        .context(LocalPullFault),
                ))
            })
        });
        let shared = fut.shared();
        *slot = Some(shared.clone());
        shared
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

    /// Fund `additional` of new headroom in the node's pool and return the pool's NEW
    /// total deposit with the amount this call added (#1530). The reactive
    /// counterpart of the proactive low-water refill:
    /// the node-to-node pull loop calls this when an upstream's cap rejection is
    /// backed by its own ledger, or when its deposit can no longer cover the next
    /// voucher, then resumes on the larger deposit.
    ///
    /// The caller sizes `additional` from its live pull ledger. This method does
    /// not re-derive a shortfall from the persisted lane progress: that progress is
    /// recorded when a pull ends, so mid-pull it omits the spend of the pull that
    /// asks, and a shortfall computed from it tops up too little.
    ///
    /// A `topUp` already in flight is JOINED, not duplicated. This call counts only
    /// the part of it that no other pull relies on: all of a refill's amount, none of
    /// another reactive top-up's. When that part is less than `additional`, this
    /// method funds the remainder with a further `topUp` (see `top_up_at_least`).
    /// `added` is below `additional` only when the funding calls run out, or when the
    /// chain credits less than a `topUp` asked for. The caller compares the two.
    ///
    /// Routes through the detached, join-or-spawn funding task (never an inline
    /// `.await`): `topUp` waits on an unbounded `get_receipt`, and this runs inside
    /// the miss-pull future the foreground serve path DROPS on its deadline — an
    /// inline await cancelled mid-receipt would leave the deposit escrowed and
    /// untracked. On the spawned task, a dropped caller loses only the answer.
    ///
    /// # Errors
    ///
    /// Errors if no pool is tracked, if our own allowance/`topUp` fails, if a
    /// `topUp` mined but the local row could not be credited (terminal — the
    /// deposit is escrowed-and-untracked; a retry would escrow again), or if every
    /// funding call joined a `topUp` that failed.
    pub async fn top_up_pool_by(&self, additional: U256) -> Result<TopUpLanded> {
        let state = self
            .reuse_or_report()?
            .with_context(|| "no buyer pool tracked to top up")?;
        if additional.is_zero() {
            return Ok(TopUpLanded {
                new_deposit: state.deposit,
                added: U256::ZERO,
            });
        }
        let landed = top_up_at_least(state.pool_id, additional, |amount| {
            self.join_or_spawn_topup(state.pool_id, amount, TopUpFunder::Reactive)
        })
        .await?;
        info!(
            pool_id = %state.pool_id,
            %additional,
            added = %landed.added,
            new_deposit = %landed.new_deposit,
            "reactive top-up: pool exhausted mid-pull; funded (#1530)"
        );
        Ok(landed)
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

    /// Fund `additional` of new headroom in the node's pool, returning its NEW total
    /// deposit and the amount this call added. See [`BuyerPoolService::top_up_pool_by`].
    /// An implementation reports what really landed in [`TopUpLanded::added`], which
    /// can be less than `additional`.
    ///
    /// Defaults to "funding not supported", so a read-only or test double need not
    /// override it: nothing is added, and `new_deposit` is `U256::ZERO`. Callers
    /// never lower their deposit view from a landing that added nothing.
    ///
    /// # Errors
    ///
    /// Implementations error when no pool is tracked, when the allowance or `topUp`
    /// fails, or when the tx lands but the local row can no longer be credited.
    async fn top_up_pool_by(&self, _additional: U256) -> Result<TopUpLanded> {
        Ok(TopUpLanded {
            new_deposit: U256::ZERO,
            added: U256::ZERO,
        })
    }
}

#[async_trait::async_trait]
impl<P: Provider + Clone + 'static> PoolOpener for BuyerPoolService<P> {
    async fn open_or_reuse_pool(
        &self,
        provider_addr: Address,
        budget: Duration,
    ) -> Result<PoolContext> {
        BuyerPoolService::open_or_reuse_pool(self, provider_addr, budget).await
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

    async fn top_up_pool_by(&self, additional: U256) -> Result<TopUpLanded> {
        BuyerPoolService::top_up_pool_by(self, additional).await
    }
}

/// Adopt the pool this `owner` already holds on-chain when the local store has
/// no row for it. Returns whether a pool was adopted.
///
/// The buyer store is the node's only record that it owns a pool, and ADR 003
/// §`node→node` is unambiguous about what that record is for: "A pool is opened
/// once and reused. There is no per-node, per-fetch, or per-client open", and
/// "Owner funds are therefore never stranded". A store that is reset — a moved
/// data dir, a redeployed host — breaks both. The node forgets a funded pool,
/// `openPool`s a second deposit beside it, forgets that one too, and once the
/// wallet is drained every pull fails `ERC20: transfer amount exceeds balance`
/// with its own escrow sitting idle on-chain (#2072).
///
/// `getPools` is the chain's answer to the question the store could not, so ask
/// it before opening anything. The newest `Open` pool wins: pools are
/// enumerated oldest-first and a later one is the one a previous adoption cycle
/// would have been using.
///
/// The enumeration runs whether or not anything is adopted, and that is the
/// second half of the job: every open pool beside the one in use is a deposit
/// this node is not spending, and [`report_stranded_pools`] is the only thing
/// that names them. Reporting them solely after an adoption would leave the
/// steady state — an intact store and a deposit stranded by an earlier build —
/// permanently silent.
///
/// Adopting is deliberately softer than the rest of bootstrap. A failure here
/// leaves the store untouched and returns `false`, so the first miss falls
/// through to the ordinary lazy open — an RPC blip while enumerating must not
/// disable buying for the life of the process the way a failed approval does.
async fn reconcile_owned_pool<P: Provider + Clone>(
    contract: &PaymentPool::PaymentPoolInstance<P>,
    store: &Arc<dyn BuyerPoolStore>,
    owner: Address,
    token: Address,
    metrics: &Arc<Metrics>,
) -> bool {
    // Resolve `Unknown` here, at the one point that can act on it, so the rest
    // of the function carries only the two states that survive it. Leaving the
    // third variant alive downstream would let a future edit turn "the store
    // could not be read" into "the store is empty, go open a pool" — the
    // duplicate-deposit bug — without a compile error.
    let tracked: Option<PoolId> = match adoption_applies(store, owner) {
        AdoptionCheck::Unknown => {
            metrics.buyer_pool_adoption_failure();
            return false;
        }
        AdoptionCheck::Applies => None,
        AdoptionCheck::AlreadyTracked(pool_id) => Some(pool_id),
    };

    let Some(pools) = enumerate_open_pools(contract, owner, tracked.is_none(), metrics).await
    else {
        return false;
    };

    let candidate = match tracked {
        Some(pool_id) => match reconcile_tracked(&pools, store, owner, pool_id) {
            TrackedOutcome::Keep => return false,
            TrackedOutcome::Replace(candidate) => candidate,
        },
        None => pools.newest_solvent(),
    };

    let Some((pool_id, pool)) = candidate else {
        return false;
    };
    let state = BuyerPoolState::new(pool_id, owner, token, U256::from(pool.deposit));
    if let Err(err) = store.record(&state) {
        metrics.buyer_pool_adoption_failure();
        warn!(
            %pool_id,
            error = %format_args!("{err:#}"),
            "could not persist the adopted pool; the next miss opens a fresh one"
        );
        return false;
    }
    report_stranded_pools(&pools.recoverable_beside(pool_id), pools.unreadable.len());
    info!(
        %pool_id,
        deposit = pool.deposit,
        remaining = pool.deposit.saturating_sub(pool.totalRedeemed),
        "adopted this node's existing on-chain payment pool; not opening a second one"
    );
    true
}

/// Every pool this `owner` holds on chain with its state, or `None` when the
/// chain could not be asked at all.
///
/// Runs on BOTH reconciliation paths, not only when adopting. A node whose
/// store is intact is the steady state, and it is exactly the state in which an
/// older build's stranded deposit sits unnoticed: nothing else ever asks the
/// chain what this owner holds, so nothing ever names it (#2078).
///
/// `adopting` decides how a failure is reported, and the distinction matters:
/// `decdn_buyer_pool_adoption_failures_total` means "could not tell whether it
/// already owned a pool and is about to open a second one", which is what the
/// runbook reads it as. A sweep that could not run beside an already-tracked
/// pool was never going to adopt anything, so it warns instead of counting.
async fn enumerate_open_pools<P: Provider + Clone>(
    contract: &PaymentPool::PaymentPoolInstance<P>,
    owner: Address,
    adopting: bool,
    metrics: &Arc<Metrics>,
) -> Option<OwnedPools> {
    let ids = match enumerate_owned_pools(contract, owner).await {
        Ok(ids) => ids,
        Err(err) if adopting => {
            metrics.buyer_pool_adoption_failure();
            warn!(
                error = %format_args!("{err:#}"),
                "could not enumerate this node's on-chain pools; a pool it already owns \
                 stays unadopted and the next miss opens a fresh one"
            );
            return None;
        }
        Err(err) => {
            warn!(
                error = %format_args!("{err:#}"),
                "could not enumerate this node's on-chain pools; a deposit stranded beside \
                 the pool it is using would go unreported this boot"
            );
            return None;
        }
    };
    Some(OwnedPools::walk(contract, ids).await)
}

/// What reconciliation should do about the pool the store already tracks.
enum TrackedOutcome {
    /// Keep the row and adopt nothing this boot.
    Keep,
    /// The row is gone; adopt this candidate, if any.
    Replace(Option<(PoolId, PaymentPool::Pool)>),
}

/// Decide the fate of the pool the store tracks, against what the chain said.
///
/// The three answers are deliberately not two. Only a pool read successfully
/// and found not `Open` justifies deleting the node's sole record that it owns
/// a funded deposit; a read that faulted leaves the row exactly where it is.
/// Collapsing those two would turn one transient RPC error at bootstrap into a
/// forgotten pool plus a second escrow — the #2072 failure, self-inflicted.
fn reconcile_tracked(
    pools: &OwnedPools,
    store: &Arc<dyn BuyerPoolStore>,
    owner: Address,
    pool_id: PoolId,
) -> TrackedOutcome {
    match pools.tracked_status(pool_id) {
        // In use. Everything else this owner holds open is stranded, including
        // pools this node never chose and would never have selected.
        TrackedStatus::Open => {
            report_stranded_pools(&pools.recoverable_beside(pool_id), pools.unreadable.len());
            TrackedOutcome::Keep
        }
        TrackedStatus::Unknown => {
            warn!(
                %pool_id,
                "could not read the tracked buyer pool's on-chain status; keeping the row. A \
                 pool this node owns must not be dropped because one read failed — the next \
                 bootstrap asks again"
            );
            TrackedOutcome::Keep
        }
        TrackedStatus::NotOpen => {
            if drop_stale_row(store, owner, pool_id) {
                TrackedOutcome::Replace(pools.newest_solvent())
            } else {
                TrackedOutcome::Keep
            }
        }
    }
}

/// Forget a tracked row whose pool the chain no longer lists as open, so the
/// caller may adopt or open a replacement. Returns whether the row is gone.
///
/// `redeemMany` rejects a voucher on a `Closed` pool outright, and on a
/// `Closing` one once `block.timestamp >= disputeDeadline`. Inside the dispute
/// window a `Closing` pool still redeems, so the wedge is not immediate — it
/// becomes total when the window elapses, and the residual is refunded to the
/// owner at `reclaim` regardless. [`BuyerPoolService::reuse_or_report`] reads
/// the tracked row without a status check, so a node that kept this row would
/// pin every pull to that pool and, past the deadline, stream bytes it cannot
/// pay for. This is the state `decdn pool close --pool` run from a node host
/// leaves behind: it lands the close on chain but cannot write the daemon's
/// store.
///
/// A failed forget does NOT count as an adoption failure. That counter means
/// "could not tell whether it already owns a pool, and is about to open a
/// second one", which the runbook reads it as; a node whose forget failed
/// keeps reusing the pool it already has and opens nothing. It is stuck, not
/// duplicating, so it warns instead.
fn drop_stale_row(store: &Arc<dyn BuyerPoolStore>, owner: Address, pool_id: PoolId) -> bool {
    warn!(
        %pool_id,
        "the tracked buyer pool is no longer open on chain; dropping the stale row so this \
         node stops pinning its pulls to a pool that can fund no voucher"
    );
    if let Err(err) = store.forget_if_pool(owner, pool_id) {
        warn!(
            %pool_id,
            error = %format_args!("{err:#}"),
            "could not drop the stale buyer pool row; this node keeps reusing a pool whose \
             vouchers stop redeeming at its dispute deadline, until the store recovers"
        );
        return false;
    }
    true
}

/// What bootstrap knows about whether `owner` already has a tracked pool.
///
/// Three states, not two, because "the store says there is no pool" and "the
/// store could not be read" want the same action for opposite reasons and must
/// not be confused at the call site: only the first justifies adopting, and only
/// the second is a fault worth counting.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AdoptionCheck {
    /// The store holds no pool for this owner, so adoption applies.
    Applies,
    /// The store already tracks this pool; there is nothing to adopt. The id
    /// rides along because the stranded set is "every other open pool", and
    /// the tracked pool is not necessarily the one an adoption would pick.
    AlreadyTracked(PoolId),
    /// The store could not be read, so what it holds is unknown.
    Unknown,
}

/// Decide whether adoption applies to `owner`.
///
/// An unreadable store answers [`AdoptionCheck::Unknown`], never `Applies`:
/// writing an adopted row into a store whose contents are unknown risks a second
/// row beside one already there, which is the failure adoption exists to prevent.
fn adoption_applies(store: &Arc<dyn BuyerPoolStore>, owner: Address) -> AdoptionCheck {
    match store.get_by_owner(owner) {
        Ok(None) => AdoptionCheck::Applies,
        Ok(Some(state)) => AdoptionCheck::AlreadyTracked(state.pool_id),
        Err(err) => {
            warn!(
                error = %format_args!("{err:#}"),
                "buyer pool store read failed before on-chain reconciliation; \
                 skipping adoption and leaving the open path to report it"
            );
            AdoptionCheck::Unknown
        }
    }
}

/// Name the open pools this node holds and is not using, so their deposits are
/// recoverable. `unreadable` is how many pools could not be read this boot;
/// a non-zero count means the list below may be short.
///
/// Nothing else reports which ids they are: adoption takes one pool and the rest
/// are invisible, which is how a deposit stays stranded indefinitely.
///
/// The remedy names each pool explicitly. `--all` is refused on a node's data
/// dir precisely because it enumerates from chain and would close the pool this
/// node is paying from (#2078).
fn report_stranded_pools(stranded: &[PoolId], unreadable: usize) {
    if stranded.is_empty() {
        if unreadable > 0 {
            warn!(
                unreadable,
                "could not read every pool this node owns, so this boot cannot say whether it \
                 holds a stranded deposit; the absence of a stranded-pool warning is not \
                 evidence there is none"
            );
        }
        return;
    }
    warn!(
        count = stranded.len(),
        pools = ?stranded,
        unreadable,
        "this node owns further open payment pools it is not using; their deposits are \
         recoverable with `decdn pool close --pool <id>` then, after the dispute window, \
         `decdn pool reclaim --pool <id>`. Name each id: `--all` would close the pool this \
         node is using, and is refused on a node's data dir. Neither command clears this \
         node's own record — restart it afterwards if you closed the pool it is using"
    );
}

/// What a walk of this owner's on-chain pools learned, newest first.
///
/// The two fields are kept apart because their absence means opposite things.
/// A pool missing from [`Self::read`] because its `getPool` call faulted is
/// **not** known to be closed, and reading it as closed is what would let one
/// transient RPC error delete a live pool's row.
struct OwnedPools {
    /// Every pool whose state was read successfully, with that state, newest
    /// first. Includes pools of every status, not only `Open`.
    read: Vec<(PoolId, PaymentPool::Pool)>,
    /// Ids whose `getPool` call faulted. Nothing is known about these.
    unreadable: Vec<PoolId>,
}

/// What the chain says about the pool the store tracks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TrackedStatus {
    /// Read successfully and `Open`: this is the pool in use.
    Open,
    /// Read successfully and not `Open`: the row may be dropped.
    NotOpen,
    /// Nothing is known — the read faulted, or `getPools` did not list it at
    /// all. Either way the row stays.
    Unknown,
}

impl OwnedPools {
    /// Walk `ids`, reading each pool's on-chain state.
    ///
    /// `ids` arrives oldest-first from `getPools`, and this walks it in
    /// reverse: a later pool is the one a previous adoption cycle would have
    /// been using, and the earlier ones are what the cycle stranded. A pool
    /// whose state cannot be read is recorded as unreadable rather than
    /// failing the walk — one unreadable pool must not hide a live one behind
    /// it, and must not be mistaken for a closed one either.
    async fn walk<P: Provider + Clone>(
        contract: &PaymentPool::PaymentPoolInstance<P>,
        ids: Vec<PoolId>,
    ) -> Self {
        let mut read = Vec::new();
        let mut unreadable = Vec::new();
        for pool_id in ids.into_iter().rev() {
            match contract.getPool(pool_id).call().await {
                Ok(pool) => read.push((pool_id, pool)),
                Err(err) => {
                    warn!(
                        %pool_id,
                        error = %err,
                        "could not read an owned pool's state; this boot cannot say whether it \
                         is open, so it is neither adopted nor reported as stranded"
                    );
                    unreadable.push(pool_id);
                }
            }
        }
        Self { read, unreadable }
    }

    /// What the chain says about `pool_id`.
    ///
    /// Answers [`TrackedStatus::Unknown`] for anything not read successfully,
    /// including an id `getPools` never listed. `getPools` is append-only —
    /// it derives ids from `ownerPoolNonce`, and `closePool`/`reclaim` only
    /// change a pool's status — so a tracked id it does not list is an
    /// anomaly, not evidence the pool is gone. Destroying a row on that
    /// evidence is exactly the mistake this type exists to prevent.
    fn tracked_status(&self, pool_id: PoolId) -> TrackedStatus {
        self.read.iter().find(|(id, _)| *id == pool_id).map_or(
            TrackedStatus::Unknown,
            |(_, pool)| {
                if matches!(pool.status, PaymentPool::Status::Open) {
                    TrackedStatus::Open
                } else {
                    TrackedStatus::NotOpen
                }
            },
        )
    }

    /// Every pool read as `Open`, newest first.
    fn open(&self) -> impl Iterator<Item = &(PoolId, PaymentPool::Pool)> {
        self.read
            .iter()
            .filter(|(_, pool)| matches!(pool.status, PaymentPool::Status::Open))
    }

    /// The newest `Open` pool that still has something to spend.
    ///
    /// A fully-redeemed pool is still `Open` on chain, and adopting one would
    /// wedge the node: `reuse_or_report` would answer `Some` forever, so no
    /// fresh pool would ever open, against a deposit that can fund no voucher.
    fn newest_solvent(&self) -> Option<(PoolId, PaymentPool::Pool)> {
        self.open()
            .find(|(_, pool)| pool.deposit > pool.totalRedeemed)
            .map(|(id, pool)| (*id, pool.clone()))
    }

    /// Every open pool other than `in_use` that still holds something to
    /// recover — the set [`report_stranded_pools`] names.
    ///
    /// Solvency is the filter because the warning promises the deposits are
    /// recoverable, and a fully-redeemed pool refunds nothing: `reclaim`
    /// returns `deposit - totalRedeemed`. It is the same test that decides
    /// adoptability, so a pool this node would refuse to adopt is also one it
    /// will not ask an operator to chase.
    ///
    /// Pools whose read faulted are absent, so this list can under-report. The
    /// per-pool warning in [`Self::walk`] is what says the answer is partial.
    fn recoverable_beside(&self, in_use: PoolId) -> Vec<PoolId> {
        self.open()
            .filter(|(id, pool)| *id != in_use && pool.deposit > pool.totalRedeemed)
            .map(|(id, _)| *id)
            .collect()
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
            error!(error = %err, "buyer pool store read failed under the open slot; cannot open a pool");
            metrics.node_pull_pool_open_failure();
            return Err(anyhow::Error::new(err))
                .context("look up the node's buyer pool under the open slot")
                .context(OpenReported)
                .context(LocalPullFault);
        }
    }

    // The open task is the reporter for every one of its legs, so this one meters
    // and logs here and marks the error `OpenReported` — a caller still waiting on
    // the shared open must not restate it. Metering the by-reason sibling here too
    // is what keeps the unlabeled total and the family reconcilable: the classifier's
    // residual arm never sees a leg that reports itself.
    //
    // `InsufficientDeposit` and `RpcError` are additionally `LocalPullFault`, because
    // neither can be answered by trying a different provider. `openPool(uint64 deposit)`
    // names no provider: every candidate re-runs the identical call against the identical
    // contract through the identical RPC. A wallet that cannot fund a deposit cannot pay
    // anyone, and a chain lane that cannot carry the transaction cannot carry it for
    // anyone — so walking the candidate list burns `MAX_PROVIDER_ATTEMPTS` futile opens
    // and then answers the client `NotFound`, which is a lie about this node's state
    // rather than a fact about the blob (#1560).
    //
    // `ContractRevert` stays unmarked: it is deterministic on-chain state (a paused
    // contract, a future revert reason), it is metered by reason, and it does not say
    // this node is unable to pay — another candidate may still deliver.
    let opened = match open_pool(contract, signer, voucher_domain, token, owner, deposit).await {
        Ok(opened) => opened,
        Err(err) => {
            error!(error = %format_args!("{err:#}"), "buyer pool open failed");
            metrics.node_pull_pool_open_failure();
            let reason = err.downcast_ref::<PoolOpenFailureReason>().copied();
            if let Some(reason) = reason {
                metrics.pool_open_failure_by_reason(reason);
            }
            let node_wide = matches!(
                reason,
                Some(PoolOpenFailureReason::InsufficientDeposit | PoolOpenFailureReason::RpcError)
            );
            let err = err.context(OpenReported);
            return Err(if node_wide {
                err.context(LocalPullFault)
            } else {
                err
            });
        }
    };

    if let Err(err) = store.record(&opened.state) {
        // The deposit is escrowed on-chain (`openPool` mined) but the row could not
        // be persisted — escrowed-but-untracked. Log the tx so an operator can
        // reconcile; the funds are safe on-chain, and re-persisting is idempotent.
        error!(
            tx = %opened.tx,
            pool_id = %opened.state.pool_id,
            error = %err,
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
    token: Address,
    metrics: Arc<Metrics>,
) {
    let mut ticker = tokio::time::interval(RECLAIM_SWEEP_INTERVAL);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        ticker.tick().await;
        reclaim_once(&contract, &store, owner, &metrics).await;
        publish_buyer_wallet_usdc(contract.provider(), token, owner, &metrics).await;
    }
}

/// Read the buyer wallet's USDC balance and publish `decdn_buyer_wallet_usdc`.
///
/// Rides the reclaim sweep rather than a task of its own: the balance moves
/// only when this node opens or tops up a pool, so a sweep-cadence read is
/// ample, and it costs one `eth_call` an hour. Published at bootstrap too, so
/// the gauge is true from the first scrape instead of reading zero until the
/// first sweep — which is the exact reading the gauge exists to distinguish.
///
/// Best-effort: a failed read leaves the previous value standing rather than
/// publishing a zero the operator would read as an empty wallet.
async fn publish_buyer_wallet_usdc<P: Provider>(
    provider: &P,
    token: Address,
    owner: Address,
    metrics: &Arc<Metrics>,
) {
    match Erc20::new(token, provider).balanceOf(owner).call().await {
        Ok(balance) => metrics.set_buyer_wallet_usdc(balance),
        Err(err) => {
            debug!(%token, %owner, error = %err, "could not read the buyer wallet's USDC balance");
        }
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
            warn!(error = %err, "reclaim sweep: buyer pool store read failed");
            metrics.buyer_reclaim_failure();
            return;
        }
    }) else {
        return; // no pool tracked
    };

    let pool = match contract.getPool(state.pool_id).call().await {
        Ok(pool) => pool,
        Err(err) => {
            warn!(pool_id = %state.pool_id, error = %format_args!("{err:#}"), "reclaim sweep: getPool failed");
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
            debug!(pool_id = %state.pool_id, error = %format_args!("{err:#}"), "reclaim sweep: chain head read failed; attempting reclaim anyway");
        }
    }

    match contract.reclaim(state.pool_id).send().await {
        Ok(pending) => match pending.get_receipt().await {
            Ok(receipt) if receipt.status() => {
                // Only the `Err` leaves the row's fate unknown — `Ok(false)`
                // means the compare-and-delete found no row for this pool, so
                // nothing maps the owner to it. A surviving row would send a
                // later reuse back to a `Closed` pool whose `deposit` field
                // still reads healthy, so meter it like the sibling arms
                // instead of dropping out of the sweep silently.
                if let Err(err) = store.forget_if_pool(owner, state.pool_id) {
                    warn!(pool_id = %state.pool_id, error = %err, "reclaim sweep: forget after reclaim failed");
                    metrics.buyer_reclaim_failure();
                    return;
                }
                info!(pool_id = %state.pool_id, "reclaimed the buyer pool residual and dropped the row");
            }
            Ok(_) => {
                warn!(pool_id = %state.pool_id, "reclaim sweep: reclaim reverted");
                metrics.buyer_reclaim_failure();
            }
            Err(err) => {
                warn!(pool_id = %state.pool_id, error = %format_args!("{err:#}"), "reclaim sweep: reclaim receipt failed");
                metrics.buyer_reclaim_failure();
            }
        },
        Err(err) => {
            warn!(pool_id = %state.pool_id, error = %format_args!("{err:#}"), "reclaim sweep: reclaim submit failed");
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
    use decdn_incentive::store::StoreError;
    use decdn_incentive::{BuyerPoolState, LaneKey, MemoryBuyerPoolStore};

    use super::*;

    fn signer() -> Arc<PrivateKeySigner> {
        Arc::new(PrivateKeySigner::random())
    }

    /// A fresh metrics handle for a test that only needs somewhere to count.
    fn metrics() -> Arc<Metrics> {
        Arc::new(Metrics::new())
    }

    /// An on-chain pool row in the state `getPool` returns it in.
    fn onchain_pool(
        owner: Address,
        status: PaymentPool::Status,
        deposit: u64,
    ) -> PaymentPool::Pool {
        PaymentPool::Pool {
            owner,
            status,
            disputeDeadline: 0,
            deposit,
            totalRedeemed: 0,
        }
    }

    /// A `PaymentPool` bound to a mocked transport whose `eth_call` queue is
    /// `responses`, in order. The first entry answers `getPools`, and each
    /// subsequent one answers the `getPool` the adoption walk makes.
    fn mocked_pool_contract(
        responses: Vec<alloy::primitives::Bytes>,
    ) -> PaymentPool::PaymentPoolInstance<impl Provider + Clone + 'static> {
        mocked_pool_contract_with(responses.into_iter().map(MockCall::Ok).collect()).0
    }

    /// One queued `eth_call` answer.
    enum MockCall {
        /// Decode this payload.
        Ok(alloy::primitives::Bytes),
        /// Fault the call, as a transient RPC error does.
        Err,
    }

    /// [`mocked_pool_contract`] with per-call failure injection, returning the
    /// [`Asserter`] so a test can assert the queue was fully consumed.
    ///
    /// `Asserter` only faults on OVER-calls — unspent responses are dropped
    /// silently — so "the code reached this call" is only provable by checking
    /// the queue is empty afterwards.
    fn mocked_pool_contract_with(
        calls: Vec<MockCall>,
    ) -> (
        PaymentPool::PaymentPoolInstance<impl Provider + Clone + 'static>,
        alloy::providers::mock::Asserter,
    ) {
        use alloy::providers::ProviderBuilder;
        use alloy::providers::mock::Asserter;

        let asserter = Asserter::new();
        for call in calls {
            match call {
                MockCall::Ok(response) => asserter.push_success(&response),
                MockCall::Err => asserter.push_failure_msg("transient rpc fault"),
            }
        }
        (
            PaymentPool::new(
                Address::ZERO,
                ProviderBuilder::new().connect_mocked_client(asserter.clone()),
            ),
            asserter,
        )
    }

    /// A node whose store was reset adopts the pool it already owns rather than
    /// escrowing a second deposit beside it (#2072).
    #[tokio::test]
    async fn reconcile_adopts_the_newest_open_pool() {
        use alloy::sol_types::SolValue;

        let owner = Address::repeat_byte(1);
        let token = Address::repeat_byte(2);
        let older = PoolId::from([0xAA; 32]);
        let newer = PoolId::from([0xBB; 32]);

        // `getPools` is oldest-first; the walk reads the newer one first.
        let contract = mocked_pool_contract(vec![
            vec![older, newer].abi_encode().into(),
            onchain_pool(owner, PaymentPool::Status::Open, 10_000_000)
                .abi_encode()
                .into(),
        ]);
        let store: Arc<dyn BuyerPoolStore> = Arc::new(MemoryBuyerPoolStore::new());

        assert!(reconcile_owned_pool(&contract, &store, owner, token, &metrics()).await);
        let adopted = store.get_by_owner(owner).unwrap().expect("row recorded");
        assert_eq!(adopted.pool_id, newer);
        assert_eq!(adopted.deposit, U256::from(10_000_000u64));
        assert_eq!(adopted.token, token);
    }

    /// A pool the owner closed is not a pool to resume on, so the walk keeps
    /// going and settles on the live one behind it.
    #[tokio::test]
    async fn reconcile_skips_a_closing_pool_for_the_open_one_behind_it() {
        use alloy::sol_types::SolValue;

        let owner = Address::repeat_byte(1);
        let open = PoolId::from([0xAA; 32]);
        let closing = PoolId::from([0xBB; 32]);

        let contract = mocked_pool_contract(vec![
            vec![open, closing].abi_encode().into(),
            onchain_pool(owner, PaymentPool::Status::Closing, 10_000_000)
                .abi_encode()
                .into(),
            onchain_pool(owner, PaymentPool::Status::Open, 9_000_000)
                .abi_encode()
                .into(),
        ]);
        let store: Arc<dyn BuyerPoolStore> = Arc::new(MemoryBuyerPoolStore::new());

        assert!(
            reconcile_owned_pool(
                &contract,
                &store,
                owner,
                Address::repeat_byte(2),
                &metrics()
            )
            .await
        );
        assert_eq!(store.get_by_owner(owner).unwrap().unwrap().pool_id, open);
    }

    /// An owner with no pools on chain has nothing to adopt, and the first miss
    /// opens one the ordinary way.
    #[tokio::test]
    async fn reconcile_adopts_nothing_when_the_owner_holds_no_pool() {
        use alloy::sol_types::SolValue;

        let owner = Address::repeat_byte(1);
        let contract = mocked_pool_contract(vec![Vec::<PoolId>::new().abi_encode().into()]);
        let store: Arc<dyn BuyerPoolStore> = Arc::new(MemoryBuyerPoolStore::new());

        assert!(
            !reconcile_owned_pool(
                &contract,
                &store,
                owner,
                Address::repeat_byte(2),
                &metrics()
            )
            .await
        );
        assert!(store.get_by_owner(owner).unwrap().is_none());
    }

    /// A store that already tracks a pool it still owns is left alone, while
    /// the boot-time sweep for stranded deposits runs around it.
    ///
    /// The chain is stocked with a DIFFERENT adoptable pool, so a build that
    /// dropped the already-tracked check would adopt it and fail the assertion.
    /// The queue holds exactly the `getPools` + `getPool` pair that sweep
    /// consumes; an empty one would not prove the same thing, because the
    /// mocked transport errors an unexpected call and the error path also
    /// returns `false`.
    #[tokio::test]
    async fn reconcile_is_a_no_op_when_the_store_already_tracks_a_pool() {
        use alloy::sol_types::SolValue;

        let owner = Address::repeat_byte(1);
        let token = Address::repeat_byte(2);
        let existing = PoolId::from([9u8; 32]);
        let other = PoolId::from([0xCC; 32]);

        // Both are open on chain. `existing` must be listed: a tracked pool the
        // chain does NOT list as open is stale, and reconciliation replaces it
        // (see `a_tracked_pool_the_chain_has_closed_is_dropped_and_replaced`).
        // The walk is newest-first, so `other` is read before `existing` — and
        // `other` is what a build that dropped the already-tracked check would
        // adopt, which is what the row assertion below catches.
        let contract = mocked_pool_contract(vec![
            vec![existing, other].abi_encode().into(),
            onchain_pool(owner, PaymentPool::Status::Open, 10_000_000)
                .abi_encode()
                .into(),
            onchain_pool(owner, PaymentPool::Status::Open, 5_000)
                .abi_encode()
                .into(),
        ]);
        let store: Arc<dyn BuyerPoolStore> = Arc::new(MemoryBuyerPoolStore::new());
        store
            .record(&BuyerPoolState::new(
                existing,
                owner,
                token,
                U256::from(5_000u64),
            ))
            .unwrap();

        assert!(!reconcile_owned_pool(&contract, &store, owner, token, &metrics()).await);
        assert_eq!(
            store.get_by_owner(owner).unwrap().unwrap().pool_id,
            existing
        );
    }

    /// The stranded set on the already-tracked path is measured against the
    /// pool the STORE names, not the one an adoption would have picked (#2078).
    ///
    /// The chain holds two open pools, neither of them the tracked one. A
    /// build that reused the adoption selector would call the newer of the two
    /// "in use" and report only the older; both are stranded.
    #[test]
    fn stranded_set_excludes_only_the_pool_actually_in_use() {
        let owner = Address::repeat_byte(1);
        let in_use = PoolId::from([9u8; 32]);
        let other_a = PoolId::from([0xAA; 32]);
        let other_b = PoolId::from([0xBB; 32]);
        let pools = OwnedPools {
            read: vec![
                (
                    other_b,
                    onchain_pool(owner, PaymentPool::Status::Open, 8_000),
                ),
                (
                    in_use,
                    onchain_pool(owner, PaymentPool::Status::Open, 5_000),
                ),
                (
                    other_a,
                    onchain_pool(owner, PaymentPool::Status::Open, 9_000),
                ),
            ],
            unreadable: Vec::new(),
        };
        assert_eq!(pools.recoverable_beside(in_use), vec![other_b, other_a]);
    }

    /// A fully-redeemed pool refunds nothing, so it is not reported as a
    /// recoverable deposit — the warning promises recoverability, and chasing
    /// a zero residual is noise on every boot.
    #[test]
    fn stranded_set_skips_a_fully_redeemed_pool() {
        let owner = Address::repeat_byte(1);
        let in_use = PoolId::from([9u8; 32]);
        let spent = PoolId::from([0xAA; 32]);
        let mut pool = onchain_pool(owner, PaymentPool::Status::Open, 9_000);
        pool.totalRedeemed = pool.deposit;
        let pools = OwnedPools {
            read: vec![(spent, pool)],
            unreadable: Vec::new(),
        };
        assert!(pools.recoverable_beside(in_use).is_empty());
    }

    /// The already-tracked path enumerates and reaches the stranded sweep
    /// rather than returning early (#2078). Proven by mock consumption: the
    /// queue holds exactly `getPools` + three `getPool`s, and the transport
    /// errors on an unexpected call, so a build that returned early would
    /// leave responses unspent and a build that over-called would fault.
    #[tokio::test]
    async fn reconcile_sweeps_for_stranded_pools_when_the_store_is_intact() {
        use alloy::sol_types::SolValue;

        let owner = Address::repeat_byte(1);
        let token = Address::repeat_byte(2);
        let existing = PoolId::from([9u8; 32]);
        let stranded_a = PoolId::from([0xAA; 32]);
        let stranded_b = PoolId::from([0xBB; 32]);

        let (contract, asserter) = mocked_pool_contract_with(vec![
            MockCall::Ok(vec![existing, stranded_a, stranded_b].abi_encode().into()),
            // Walked newest-first: `stranded_b`, `stranded_a`, then `existing`.
            // `existing` is listed open because that is the steady state this
            // test describes — the tracked pool is live and the other two are
            // deposits an earlier build left behind.
            MockCall::Ok(
                onchain_pool(owner, PaymentPool::Status::Open, 8_000)
                    .abi_encode()
                    .into(),
            ),
            MockCall::Ok(
                onchain_pool(owner, PaymentPool::Status::Open, 9_000)
                    .abi_encode()
                    .into(),
            ),
            MockCall::Ok(
                onchain_pool(owner, PaymentPool::Status::Open, 5_000)
                    .abi_encode()
                    .into(),
            ),
        ]);
        let store: Arc<dyn BuyerPoolStore> = Arc::new(MemoryBuyerPoolStore::new());
        store
            .record(&BuyerPoolState::new(
                existing,
                owner,
                token,
                U256::from(5_000u64),
            ))
            .unwrap();

        let metrics = metrics();
        assert!(!reconcile_owned_pool(&contract, &store, owner, token, &metrics).await);
        // Nothing adopted: the tracked row is untouched.
        assert_eq!(
            store.get_by_owner(owner).unwrap().unwrap().pool_id,
            existing
        );
        // The discriminating assertion. `Asserter` faults only on over-calls,
        // so an unspent queue is the only evidence the sweep ran at all — a
        // build that returned early on `AlreadyTracked` would satisfy both
        // assertions above and leave three responses behind.
        assert_eq!(
            asserter.read_q().len(),
            0,
            "the sweep must read every pool this owner holds"
        );
    }

    /// A row pointing at a pool the chain no longer lists as open is dropped,
    /// and another open pool is adopted in its place (#2078).
    ///
    /// This is the state the node-host `decdn pool close --pool` path leaves
    /// behind: it cannot write the daemon's store, so the row survives the
    /// close. Dropping it is what stops `adoption_applies` answering
    /// `AlreadyTracked` forever and `reuse_or_report` pinning every pull to a
    /// pool whose vouchers stop redeeming at its dispute deadline.
    #[tokio::test]
    async fn a_tracked_pool_the_chain_has_closed_is_dropped_and_replaced() {
        use alloy::sol_types::SolValue;

        let owner = Address::repeat_byte(1);
        let token = Address::repeat_byte(2);
        let closed = PoolId::from([9u8; 32]);
        let live = PoolId::from([0xAA; 32]);

        // `getPools` is append-only — it derives ids from `ownerPoolNonce` and
        // `closePool`/`reclaim` only change a pool's status — so the closed
        // pool is still listed, with `Status::Closed`. That listing, not its
        // absence, is what makes the row droppable. Walked newest-first, so
        // `live` is read before `closed`.
        let contract = mocked_pool_contract(vec![
            vec![closed, live].abi_encode().into(),
            onchain_pool(owner, PaymentPool::Status::Open, 9_000)
                .abi_encode()
                .into(),
            onchain_pool(owner, PaymentPool::Status::Closed, 5_000)
                .abi_encode()
                .into(),
        ]);
        let store: Arc<dyn BuyerPoolStore> = Arc::new(MemoryBuyerPoolStore::new());
        store
            .record(&BuyerPoolState::new(
                closed,
                owner,
                token,
                U256::from(5_000u64),
            ))
            .unwrap();

        assert!(
            reconcile_owned_pool(&contract, &store, owner, token, &metrics()).await,
            "a stale row must not block adoption of a pool this owner really holds"
        );
        assert_eq!(
            store.get_by_owner(owner).unwrap().unwrap().pool_id,
            live,
            "the stale row must be replaced by the live pool, not kept beside it"
        );
    }

    /// A tracked pool whose status read FAULTED keeps its row (#2078).
    ///
    /// This is the dangerous direction of the stale-row drop, and the reason
    /// `OwnedPools` keeps "unreadable" apart from "not open". `getPool` has no
    /// retry, so one rate-limited or timed-out call at bootstrap is enough. If
    /// absence from the open set were read as closure, that single fault would
    /// delete the node's only record of a funded pool and the next miss would
    /// escrow a second deposit — the #2072 failure, self-inflicted, and
    /// invisible because the stranded report cannot name a pool it failed to
    /// read either.
    #[tokio::test]
    async fn a_tracked_pool_whose_status_read_failed_keeps_its_row() {
        use alloy::sol_types::SolValue;

        let owner = Address::repeat_byte(1);
        let token = Address::repeat_byte(2);
        let tracked = PoolId::from([9u8; 32]);
        let other = PoolId::from([0xAA; 32]);

        // Walked newest-first, so `tracked` is read first — and faults.
        let (contract, asserter) = mocked_pool_contract_with(vec![
            MockCall::Ok(vec![other, tracked].abi_encode().into()),
            MockCall::Err,
            MockCall::Ok(
                onchain_pool(owner, PaymentPool::Status::Open, 9_000)
                    .abi_encode()
                    .into(),
            ),
        ]);
        let store: Arc<dyn BuyerPoolStore> = Arc::new(MemoryBuyerPoolStore::new());
        store
            .record(&BuyerPoolState::new(
                tracked,
                owner,
                token,
                U256::from(5_000u64),
            ))
            .unwrap();

        assert!(!reconcile_owned_pool(&contract, &store, owner, token, &metrics()).await);
        assert_eq!(
            store.get_by_owner(owner).unwrap().unwrap().pool_id,
            tracked,
            "a getPool fault must never cost the node its tracked pool"
        );
        assert_eq!(
            asserter.read_q().len(),
            0,
            "the walk must reach every queued call"
        );
    }

    /// A tracked id `getPools` does not list keeps its row too.
    ///
    /// `getPools` derives ids from `ownerPoolNonce` and `closePool`/`reclaim`
    /// only change a pool's status, so it is append-only and no on-chain event
    /// produces this state. An id missing from it is an anomaly — a wrong
    /// contract address, a re-orged chain — and anomalies must not delete
    /// records of escrowed funds.
    #[tokio::test]
    async fn a_tracked_pool_the_chain_does_not_list_keeps_its_row() {
        use alloy::sol_types::SolValue;

        let owner = Address::repeat_byte(1);
        let token = Address::repeat_byte(2);
        let tracked = PoolId::from([9u8; 32]);
        let other = PoolId::from([0xAA; 32]);

        let contract = mocked_pool_contract(vec![
            vec![other].abi_encode().into(),
            onchain_pool(owner, PaymentPool::Status::Open, 9_000)
                .abi_encode()
                .into(),
        ]);
        let store: Arc<dyn BuyerPoolStore> = Arc::new(MemoryBuyerPoolStore::new());
        store
            .record(&BuyerPoolState::new(
                tracked,
                owner,
                token,
                U256::from(5_000u64),
            ))
            .unwrap();

        assert!(!reconcile_owned_pool(&contract, &store, owner, token, &metrics()).await);
        assert_eq!(
            store.get_by_owner(owner).unwrap().unwrap().pool_id,
            tracked,
            "an id the chain does not list is unknown, not closed"
        );
    }

    /// The same stale row with nothing to replace it is still dropped, so the    /// The same stale row with nothing to replace it is still dropped, so the
    /// next miss opens a fresh pool rather than reusing the closed one.
    #[tokio::test]
    async fn a_stale_row_is_dropped_even_when_no_other_pool_is_open() {
        use alloy::sol_types::SolValue;

        let owner = Address::repeat_byte(1);
        let token = Address::repeat_byte(2);
        let closed = PoolId::from([9u8; 32]);

        // `getPools` still lists it, but its status is `Closed`, so it is not
        // in the open set.
        let contract = mocked_pool_contract(vec![
            vec![closed].abi_encode().into(),
            onchain_pool(owner, PaymentPool::Status::Closed, 5_000)
                .abi_encode()
                .into(),
        ]);
        let store: Arc<dyn BuyerPoolStore> = Arc::new(MemoryBuyerPoolStore::new());
        store
            .record(&BuyerPoolState::new(
                closed,
                owner,
                token,
                U256::from(5_000u64),
            ))
            .unwrap();

        assert!(!reconcile_owned_pool(&contract, &store, owner, token, &metrics()).await);
        assert!(
            store.get_by_owner(owner).unwrap().is_none(),
            "the row must be gone so the next miss opens a fresh pool"
        );
    }

    /// A failed enumeration on the already-tracked path is a warning, not an
    /// adoption failure. The counter's meaning — and the runbook's reading of
    /// it — is "about to open a second pool", which this path never is.
    #[tokio::test]
    async fn a_failed_stranded_sweep_is_not_counted_as_an_adoption_failure() {
        let owner = Address::repeat_byte(1);
        let token = Address::repeat_byte(2);
        let existing = PoolId::from([9u8; 32]);

        // Empty queue: the mocked transport errors the `getPools` call.
        let contract = mocked_pool_contract(vec![]);
        let store: Arc<dyn BuyerPoolStore> = Arc::new(MemoryBuyerPoolStore::new());
        store
            .record(&BuyerPoolState::new(
                existing,
                owner,
                token,
                U256::from(5_000u64),
            ))
            .unwrap();

        let metrics = metrics();
        assert!(!reconcile_owned_pool(&contract, &store, owner, token, &metrics).await);
        assert_eq!(
            adoption_failures(&metrics),
            0,
            "a stranded sweep that could not run is not an adoption failure"
        );
        assert_eq!(
            store.get_by_owner(owner).unwrap().unwrap().pool_id,
            existing,
            "a chain read that failed must never cost the node its tracked pool"
        );
    }

    /// A service wired to a mocked chain and an in-memory store, for the lane
    /// seed. Built field-wise rather than through `bootstrap`, which would spend
    /// mock responses on its `usdc()` self-check and approval.
    fn mocked_service(
        responses: Vec<alloy::primitives::Bytes>,
        store: Arc<dyn BuyerPoolStore>,
        signer: Arc<PrivateKeySigner>,
        owner: Address,
    ) -> BuyerPoolService<impl Provider + Clone + 'static> {
        BuyerPoolService {
            contract: mocked_pool_contract(responses),
            store,
            signer,
            voucher_domain: Eip712Domain::default(),
            token: Address::repeat_byte(2),
            owner,
            working_deposit: U256::from(10_000_000u64),
            open_in_flight: Arc::new(Mutex::new(None)),
            topup_in_flight: Arc::new(Mutex::new(None)),
            metrics: Arc::new(Metrics::new()),
            _reclaimer: AbortOnDrop(tokio::spawn(std::future::pending())),
        }
    }

    /// A lane this node has been paid on, but has no local record of, resumes
    /// from the chain's watermark.
    ///
    /// `PoolLedger` signs `prior + accrued`, so resuming from zero would put
    /// every cumulative at or below the contract's watermark, where
    /// `_applyVoucher` pays nothing — the node would stream real bytes and buy
    /// none of them (#2072).
    #[tokio::test]
    async fn a_lane_with_no_local_record_resumes_from_the_chain_watermark() {
        use alloy::sol_types::SolValue;

        let owner = Address::repeat_byte(1);
        let provider_addr = Address::repeat_byte(3);
        let pool_id = PoolId::from([7u8; 32]);
        let signer = signer();

        let store: Arc<dyn BuyerPoolStore> = Arc::new(MemoryBuyerPoolStore::new());
        let adopted = BuyerPoolState::new(
            pool_id,
            owner,
            Address::repeat_byte(2),
            U256::from(10_000_000u64),
        );
        store.record(&adopted).unwrap();

        // `getWatermark(...)` returns one `Lane`; the struct is a static tuple,
        // so its `SolValue` encoding equals the single-struct return.
        let service = mocked_service(
            vec![
                PaymentPool::Lane {
                    amount: 191_205,
                    bytesDelivered: 4_096,
                }
                .abi_encode()
                .into(),
            ],
            Arc::clone(&store),
            Arc::clone(&signer),
            owner,
        );

        let seeded = service
            .reseed_lane_from_chain(adopted, provider_addr)
            .await
            .expect("seed succeeds");
        let lane = LaneKey {
            pool_id,
            signer: signer.address(),
            provider: provider_addr,
        };
        let progress = seeded.lane_progress(lane).expect("lane seeded");
        assert_eq!(progress.last_amount, U256::from(191_205u64));
        assert_eq!(progress.last_bytes, U256::from(4_096u64));
        // And it is durable, so the next pull does not re-read the chain.
        assert!(
            store
                .get_by_owner(owner)
                .unwrap()
                .unwrap()
                .lane_progress(lane)
                .is_some()
        );
    }

    /// A lane the node already tracks locally is never re-read from the chain.
    ///
    /// The chain is stocked with a watermark ABOVE the local one, so a build
    /// that dropped the `lane_progress` guard would read it, advance the lane and
    /// fail the equality. An empty queue would not distinguish the two: the
    /// mocked transport errors an unexpected call, and the error path is now a
    /// refusal, which this test would then see as a different failure.
    #[tokio::test]
    async fn a_lane_with_local_progress_is_not_re_read_from_the_chain() {
        use alloy::sol_types::SolValue;

        let owner = Address::repeat_byte(1);
        let provider_addr = Address::repeat_byte(3);
        let signer = signer();
        let state = pool_with_lane(
            signer.address(),
            provider_addr,
            U256::from(8_192u64),
            U256::from(400_000u64),
        );

        let store: Arc<dyn BuyerPoolStore> = Arc::new(MemoryBuyerPoolStore::new());
        store.record(&state).unwrap();
        let service = mocked_service(
            vec![
                PaymentPool::Lane {
                    amount: 999_999,
                    bytesDelivered: 999_999,
                }
                .abi_encode()
                .into(),
            ],
            Arc::clone(&store),
            Arc::clone(&signer),
            owner,
        );

        let out = service
            .reseed_lane_from_chain(state.clone(), provider_addr)
            .await
            .expect("a tracked lane needs no chain read");
        assert_eq!(out, state);
    }

    /// A lane nobody has ever redeemed on reads `(0, 0)`, which is already the
    /// right resume point — so nothing is written and the state is unchanged.
    #[tokio::test]
    async fn a_never_paid_lane_is_left_at_zero() {
        use alloy::sol_types::SolValue;

        let owner = Address::repeat_byte(1);
        let signer = signer();
        let state = BuyerPoolState::new(
            PoolId::from([7u8; 32]),
            owner,
            Address::repeat_byte(2),
            U256::from(10_000_000u64),
        );
        // Recorded, so a build that dropped the `(0, 0)` guard would commit a
        // zero lane and raise `lane_count`, failing the assertion below. Against
        // an unrecorded state the write would be a no-op and pass vacuously.
        let store: Arc<dyn BuyerPoolStore> = Arc::new(MemoryBuyerPoolStore::new());
        store.record(&state).unwrap();
        let service = mocked_service(
            vec![
                PaymentPool::Lane {
                    amount: 0,
                    bytesDelivered: 0,
                }
                .abi_encode()
                .into(),
            ],
            Arc::clone(&store),
            Arc::clone(&signer),
            owner,
        );

        let out = service
            .reseed_lane_from_chain(state.clone(), Address::repeat_byte(3))
            .await
            .expect("a never-paid lane resumes at zero, it does not refuse");
        assert_eq!(out, state);
        assert_eq!(out.lane_count(), 0);
        assert_eq!(
            store.get_by_owner(owner).unwrap().unwrap().lane_count(),
            0,
            "a zero watermark must not be committed as a lane"
        );
    }

    /// An unreadable watermark REFUSES the pull, marked as this node's own fault.
    ///
    /// Resuming from zero is not a recoverable degradation: the pull would
    /// persist its own progress, give the lane a local row, and the
    /// `lane_progress` guard would then stop the reseed ever running for that
    /// provider again — stranding the lane below the chain watermark
    /// permanently. `LocalPullFault` is what makes the classifier exonerate the
    /// peer and refuse rather than report the blob absent.
    #[tokio::test]
    async fn an_unreadable_watermark_refuses_the_pull_as_a_local_fault() {
        let owner = Address::repeat_byte(1);
        let signer = signer();
        let state = BuyerPoolState::new(
            PoolId::from([7u8; 32]),
            owner,
            Address::repeat_byte(2),
            U256::from(10_000_000u64),
        );
        let store: Arc<dyn BuyerPoolStore> = Arc::new(MemoryBuyerPoolStore::new());
        // No queued response: the mocked transport errors the `getWatermark`.
        let service = mocked_service(Vec::new(), store, Arc::clone(&signer), owner);

        let err = service
            .reseed_lane_from_chain(state, Address::repeat_byte(3))
            .await
            .expect_err("an unreadable watermark must refuse, not resume from zero");
        assert!(
            err.downcast_ref::<LocalPullFault>().is_some(),
            "the refusal is this node's fault, not the upstream's"
        );
        assert!(
            err.downcast_ref::<OpenReported>().is_some(),
            "the raising site reports it, so the classifier must not restate it"
        );
    }

    /// A `BuyerPoolStore` whose every read and write faults, for the legs
    /// `MemoryBuyerPoolStore` cannot express.
    #[derive(Debug)]
    struct FailingStore;

    impl BuyerPoolStore for FailingStore {
        fn load_all(&self) -> std::result::Result<decdn_incentive::BuyerLoad, StoreError> {
            Err(StoreError::Backend("load_all faulted".into()))
        }
        fn record(&self, _state: &BuyerPoolState) -> std::result::Result<(), StoreError> {
            Err(StoreError::Backend("record faulted".into()))
        }
        fn forget(&self, _owner: Address) -> std::result::Result<(), StoreError> {
            Err(StoreError::Backend("forget faulted".into()))
        }
        fn get_by_pool_id(
            &self,
            _pool_id: PoolId,
        ) -> std::result::Result<Option<BuyerPoolState>, StoreError> {
            Err(StoreError::Backend("get_by_pool_id faulted".into()))
        }
        fn forget_if_pool(
            &self,
            _owner: Address,
            _pool_id: PoolId,
        ) -> std::result::Result<bool, StoreError> {
            Err(StoreError::Backend("forget_if_pool faulted".into()))
        }
        fn get_by_owner(
            &self,
            _owner: Address,
        ) -> std::result::Result<Option<BuyerPoolState>, StoreError> {
            Err(StoreError::Backend("get_by_owner faulted".into()))
        }
        fn advance_progress(
            &self,
            _owner: Address,
            _pool_id: PoolId,
            _lane: LaneKey,
            _bytes: U256,
            _amount: U256,
        ) -> std::result::Result<AdvanceOutcome, StoreError> {
            Err(StoreError::Backend("advance_progress faulted".into()))
        }
        fn add_deposit(
            &self,
            _owner: Address,
            _pool_id: PoolId,
            _additional: U256,
        ) -> std::result::Result<decdn_incentive::DepositOutcome, StoreError> {
            Err(StoreError::Backend("add_deposit faulted".into()))
        }
    }

    /// A fully-redeemed pool is still `Open` on chain, and adopting it would
    /// wedge buying for good: `reuse_or_report` would answer `Some` forever
    /// against a deposit that can fund no voucher, so no fresh pool would open.
    /// The walk passes it over for the solvent one behind it.
    #[tokio::test]
    async fn reconcile_skips_a_fully_redeemed_pool() {
        use alloy::sol_types::SolValue;

        let owner = Address::repeat_byte(1);
        let solvent = PoolId::from([0xAA; 32]);
        let drained = PoolId::from([0xBB; 32]);

        let mut spent = onchain_pool(owner, PaymentPool::Status::Open, 10_000_000);
        spent.totalRedeemed = 10_000_000;
        let contract = mocked_pool_contract(vec![
            vec![solvent, drained].abi_encode().into(),
            spent.abi_encode().into(),
            onchain_pool(owner, PaymentPool::Status::Open, 9_000_000)
                .abi_encode()
                .into(),
        ]);
        let store: Arc<dyn BuyerPoolStore> = Arc::new(MemoryBuyerPoolStore::new());

        assert!(
            reconcile_owned_pool(
                &contract,
                &store,
                owner,
                Address::repeat_byte(2),
                &metrics()
            )
            .await
        );
        assert_eq!(store.get_by_owner(owner).unwrap().unwrap().pool_id, solvent);
    }

    /// An unreadable store adopts nothing and counts the fault. Writing an
    /// adopted row into a store whose contents are unknown risks a second row
    /// beside one already there — the failure adoption exists to prevent — so
    /// `Unknown` must not be treated as `Applies`.
    #[tokio::test]
    async fn reconcile_counts_an_unreadable_store_and_adopts_nothing() {
        let owner = Address::repeat_byte(1);
        let store: Arc<dyn BuyerPoolStore> = Arc::new(FailingStore);
        let metrics = metrics();
        // Empty queue: reaching the chain at all would be the bug.
        let contract = mocked_pool_contract(Vec::new());

        assert_eq!(adoption_applies(&store, owner), AdoptionCheck::Unknown);
        assert!(
            !reconcile_owned_pool(&contract, &store, owner, Address::repeat_byte(2), &metrics)
                .await
        );
        assert_eq!(
            adoption_failures(&metrics),
            1,
            "an unreadable store is a counted adoption fault, not a quiet skip"
        );
    }

    /// A store whose reads answer normally and whose `record` faults: the
    /// escrowed-but-unpersisted leg, which `MemoryBuyerPoolStore` cannot express.
    #[derive(Debug)]
    struct WriteOnlyFault(MemoryBuyerPoolStore);
    impl BuyerPoolStore for WriteOnlyFault {
        fn load_all(&self) -> std::result::Result<decdn_incentive::BuyerLoad, StoreError> {
            self.0.load_all()
        }
        fn record(&self, _state: &BuyerPoolState) -> std::result::Result<(), StoreError> {
            Err(StoreError::Backend("disk full".into()))
        }
        fn forget(&self, owner: Address) -> std::result::Result<(), StoreError> {
            self.0.forget(owner)
        }
        fn get_by_pool_id(
            &self,
            pool_id: PoolId,
        ) -> std::result::Result<Option<BuyerPoolState>, StoreError> {
            self.0.get_by_pool_id(pool_id)
        }
        fn forget_if_pool(
            &self,
            owner: Address,
            pool_id: PoolId,
        ) -> std::result::Result<bool, StoreError> {
            self.0.forget_if_pool(owner, pool_id)
        }
        fn get_by_owner(
            &self,
            owner: Address,
        ) -> std::result::Result<Option<BuyerPoolState>, StoreError> {
            self.0.get_by_owner(owner)
        }
        fn advance_progress(
            &self,
            owner: Address,
            pool_id: PoolId,
            lane: LaneKey,
            bytes: U256,
            amount: U256,
        ) -> std::result::Result<AdvanceOutcome, StoreError> {
            self.0.advance_progress(owner, pool_id, lane, bytes, amount)
        }
        fn add_deposit(
            &self,
            owner: Address,
            pool_id: PoolId,
            additional: U256,
        ) -> std::result::Result<decdn_incentive::DepositOutcome, StoreError> {
            self.0.add_deposit(owner, pool_id, additional)
        }
    }

    /// A store that cannot persist the adopted row counts the fault, so an
    /// operator sees the state in which the node is about to escrow a second
    /// deposit rather than only a log line.
    #[tokio::test]
    async fn reconcile_counts_a_failed_persist() {
        use alloy::sol_types::SolValue;

        let owner = Address::repeat_byte(1);
        let store: Arc<dyn BuyerPoolStore> = Arc::new(WriteOnlyFault(MemoryBuyerPoolStore::new()));
        let metrics = metrics();
        let contract = mocked_pool_contract(vec![
            vec![PoolId::from([0xAA; 32])].abi_encode().into(),
            onchain_pool(owner, PaymentPool::Status::Open, 10_000_000)
                .abi_encode()
                .into(),
        ]);

        assert!(
            !reconcile_owned_pool(&contract, &store, owner, Address::repeat_byte(2), &metrics)
                .await
        );
        assert_eq!(adoption_failures(&metrics), 1);
    }

    /// Read `decdn_buyer_pool_adoption_failures_total` off an encoded registry.
    fn adoption_failures(metrics: &Arc<Metrics>) -> u64 {
        let text = metrics.encode().expect("encode metrics");
        text.lines()
            .find_map(|l| {
                l.strip_prefix("decdn_buyer_pool_adoption_failures_total")?
                    .strip_prefix(' ')?
                    .parse::<u64>()
                    .ok()
            })
            .unwrap_or_default()
    }

    /// A pool that is only unreachable — an RPC blip on `getPools` — leaves the
    /// store untouched and returns `false`, so the first miss falls through to
    /// the ordinary lazy open instead of buying being disabled for the process.
    #[tokio::test]
    async fn reconcile_leaves_the_store_untouched_when_the_chain_is_unreachable() {
        let owner = Address::repeat_byte(1);
        // No queued response: the mocked transport errors the `getPools` call.
        let contract = mocked_pool_contract(Vec::new());
        let store: Arc<dyn BuyerPoolStore> = Arc::new(MemoryBuyerPoolStore::new());

        assert!(
            !reconcile_owned_pool(
                &contract,
                &store,
                owner,
                Address::repeat_byte(2),
                &metrics()
            )
            .await
        );
        assert!(store.get_by_owner(owner).unwrap().is_none());
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

    /// One scripted step of a funding call: the claim the slot hands out, and the
    /// outcome of the `topUp` behind it.
    enum Step {
        /// The `topUp` lands with this `(new_deposit, credited)`.
        Lands(u64, u64),
        /// The `topUp` fails with a plain error.
        Fails(&'static str),
        /// The `topUp` mined but its deposit is escrowed and untracked.
        Untracked,
    }

    /// A scripted `join_or_spawn` for [`top_up_at_least`]: each call pops the next
    /// `(claim, step)` and records the amount it was asked for.
    fn scripted(
        script: Vec<(TopUpClaim, Step)>,
        asked: &std::cell::RefCell<Vec<U256>>,
    ) -> impl FnMut(U256) -> (futures_util::future::Ready<TopUpOutcome>, TopUpClaim) + '_ {
        let mut script = script.into_iter();
        move |amount| {
            asked.borrow_mut().push(amount);
            let (claim, step) = script.next().expect("script ran out of calls");
            let outcome = match step {
                Step::Lands(new_deposit, credited) => Ok(TopUpLanded {
                    new_deposit: U256::from(new_deposit),
                    added: U256::from(credited),
                }),
                Step::Fails(msg) => Err(Arc::new(anyhow::anyhow!(msg))),
                Step::Untracked => Err(Arc::new(
                    anyhow::anyhow!("row replaced").context(EscrowedUntracked),
                )),
            };
            (futures_util::future::ready(outcome), claim)
        }
    }

    fn u(v: u64) -> U256 {
        U256::from(v)
    }

    fn joined(claimed: u64, requested: u64) -> TopUpClaim {
        TopUpClaim::Joined {
            claimed: u(claimed),
            requested: u(requested),
        }
    }

    fn landed(new_deposit: u64, added: u64) -> TopUpLanded {
        TopUpLanded {
            new_deposit: u(new_deposit),
            added: u(added),
        }
    }

    const POOL: PoolId = PoolId::ZERO;

    #[tokio::test]
    async fn top_up_at_least_spawned_call_is_final() {
        let asked = std::cell::RefCell::new(Vec::new());
        let f = scripted(vec![(TopUpClaim::Spawned, Step::Lands(1_100, 100))], &asked);

        let got = top_up_at_least(POOL, u(100), f).await.unwrap();

        assert_eq!(got, landed(1_100, 100));
        assert_eq!(*asked.borrow(), vec![u(100)]);
    }

    /// A spawned `topUp` that credits less than requested is NOT retried: the
    /// shortfall is not from a join, and a retry escrows a second `topUp`.
    #[tokio::test]
    async fn top_up_at_least_short_spawn_is_not_retried() {
        let asked = std::cell::RefCell::new(Vec::new());
        let f = scripted(vec![(TopUpClaim::Spawned, Step::Lands(1_040, 40))], &asked);

        let got = top_up_at_least(POOL, u(100), f).await.unwrap();

        assert_eq!(got, landed(1_040, 40));
        assert_eq!(asked.borrow().len(), 1);
    }

    /// A join whose claim exactly covers the request needs no follow-up, however
    /// much the joined `topUp` itself raised the deposit.
    #[tokio::test]
    async fn top_up_at_least_exact_join_needs_no_follow_up() {
        let asked = std::cell::RefCell::new(Vec::new());
        let f = scripted(vec![(joined(100, 500), Step::Lands(1_500, 500))], &asked);

        let got = top_up_at_least(POOL, u(100), f).await.unwrap();

        assert_eq!(got, landed(1_500, 100));
        assert_eq!(asked.borrow().len(), 1);
    }

    /// The #2012 case: a join claims 40 of 100, so a follow-up asks for the other 60.
    #[tokio::test]
    async fn top_up_at_least_short_join_funds_the_remainder() {
        let asked = std::cell::RefCell::new(Vec::new());
        let f = scripted(
            vec![
                (joined(40, 40), Step::Lands(1_040, 40)),
                (TopUpClaim::Spawned, Step::Lands(1_100, 60)),
            ],
            &asked,
        );

        let got = top_up_at_least(POOL, u(100), f).await.unwrap();

        assert_eq!(got, landed(1_100, 100));
        assert_eq!(*asked.borrow(), vec![u(100), u(60)]);
    }

    /// Two concurrent reactive top-ups: the joiner claims none of the spawner's
    /// escrow, so it funds its whole request itself instead of counting the
    /// spawner's deposit growth as its own.
    #[tokio::test]
    async fn top_up_at_least_zero_claim_funds_the_whole_request() {
        let asked = std::cell::RefCell::new(Vec::new());
        let f = scripted(
            vec![
                (joined(0, 100), Step::Lands(1_100, 100)),
                (TopUpClaim::Spawned, Step::Lands(1_200, 100)),
            ],
            &asked,
        );

        let got = top_up_at_least(POOL, u(100), f).await.unwrap();

        assert_eq!(got, landed(1_200, 100));
        assert_eq!(*asked.borrow(), vec![u(100), u(100)]);
    }

    /// A short join followed by a join that covers the rest stops on the cumulative
    /// claim.
    #[tokio::test]
    async fn top_up_at_least_second_join_covers_the_rest() {
        let asked = std::cell::RefCell::new(Vec::new());
        let f = scripted(
            vec![
                (joined(30, 300), Step::Lands(1_300, 300)),
                (joined(70, 200), Step::Lands(1_500, 200)),
            ],
            &asked,
        );

        let got = top_up_at_least(POOL, u(100), f).await.unwrap();

        assert_eq!(got, landed(1_500, 100));
        assert_eq!(*asked.borrow(), vec![u(100), u(70)]);
    }

    /// A joined refill that asked for 100 but the chain credited only 50 (the
    /// contract credits the measured transfer). Claims of 60 and 40 were split out
    /// of the 100 before it landed, so they add up to more than landed. The join
    /// counts none of its claim and funds its whole need itself.
    #[tokio::test]
    async fn top_up_at_least_short_joined_topup_counts_no_claim() {
        let asked = std::cell::RefCell::new(Vec::new());
        let f = scripted(
            vec![
                (joined(60, 100), Step::Lands(1_050, 50)),
                (TopUpClaim::Spawned, Step::Lands(1_110, 60)),
            ],
            &asked,
        );

        let got = top_up_at_least(POOL, u(60), f).await.unwrap();

        assert_eq!(got, landed(1_110, 60));
        assert_eq!(*asked.borrow(), vec![u(60), u(60)]);
    }

    /// A failed joined `topUp` is another funder's failure: this caller funds the
    /// same amount again with a `topUp` of its own.
    #[tokio::test]
    async fn top_up_at_least_failed_join_funds_with_its_own_topup() {
        let asked = std::cell::RefCell::new(Vec::new());
        let f = scripted(
            vec![
                (joined(100, 100), Step::Fails("refill rpc error")),
                (TopUpClaim::Spawned, Step::Lands(1_100, 100)),
            ],
            &asked,
        );

        let got = top_up_at_least(POOL, u(100), f).await.unwrap();

        assert_eq!(got, landed(1_100, 100));
        assert_eq!(*asked.borrow(), vec![u(100), u(100)]);
    }

    /// An escrowed-but-untracked joined `topUp` stops the top-up: a second `topUp`
    /// against a row that cannot be credited strands a second deposit.
    #[tokio::test]
    async fn top_up_at_least_untracked_join_is_not_retried() {
        let asked = std::cell::RefCell::new(Vec::new());
        let f = scripted(vec![(joined(100, 100), Step::Untracked)], &asked);

        let err = top_up_at_least(POOL, u(100), f).await.unwrap_err();

        assert!(format!("{err:#}").contains("escrowed"), "{err:#}");
        assert_eq!(asked.borrow().len(), 1);
    }

    #[tokio::test]
    async fn top_up_at_least_spawned_error_propagates() {
        let asked = std::cell::RefCell::new(Vec::new());
        let f = scripted(
            vec![
                (joined(40, 40), Step::Lands(1_040, 40)),
                (TopUpClaim::Spawned, Step::Fails("chain rejected")),
            ],
            &asked,
        );

        let err = top_up_at_least(POOL, u(100), f).await.unwrap_err();

        assert!(format!("{err:#}").contains("chain rejected"), "{err:#}");
        assert_eq!(asked.borrow().len(), 2);
    }

    /// When the funding calls run out short, the top-up returns what landed rather
    /// than an error, so the pull keeps the headroom that is really escrowed.
    #[tokio::test]
    async fn top_up_at_least_returns_what_landed_after_repeated_short_joins() {
        let asked = std::cell::RefCell::new(Vec::new());
        let f = scripted(
            vec![
                (joined(10, 10), Step::Lands(1_010, 10)),
                (joined(10, 10), Step::Lands(1_020, 10)),
                (joined(10, 10), Step::Lands(1_030, 10)),
            ],
            &asked,
        );

        let got = top_up_at_least(POOL, u(100), f).await.unwrap();

        assert_eq!(got, landed(1_030, 30));
        assert_eq!(*asked.borrow(), vec![u(100), u(90), u(80)]);
        assert_eq!(asked.borrow().len(), MAX_TOPUP_CALLS as usize);
    }

    #[tokio::test]
    async fn top_up_at_least_errors_when_every_join_fails() {
        let asked = std::cell::RefCell::new(Vec::new());
        let f = scripted(
            vec![
                (joined(0, 100), Step::Fails("rpc down")),
                (joined(0, 100), Step::Fails("rpc down")),
                (joined(0, 100), Step::Fails("rpc still down")),
            ],
            &asked,
        );

        let err = top_up_at_least(POOL, u(100), f).await.unwrap_err();

        assert!(format!("{err:#}").contains("rpc still down"), "{err:#}");
    }

    fn in_flight(amount: u64, funder: TopUpFunder) -> InFlightTopUp {
        let fut: BoxFuture<'static, TopUpOutcome> =
            Box::pin(futures_util::future::ready(Ok(landed(0, amount))));
        InFlightTopUp::new(fut.shared(), u(amount), funder)
    }

    /// A refill's amount is claimable once: reactive joiners split it, and a claim
    /// never exceeds what is left.
    #[test]
    fn in_flight_refill_is_claimed_at_most_once() {
        let mut slot = in_flight(100, TopUpFunder::Refill);

        assert_eq!(slot.join(u(60), TopUpFunder::Reactive).1, joined(60, 100));
        assert_eq!(slot.join(u(60), TopUpFunder::Reactive).1, joined(40, 100));
        assert_eq!(slot.join(u(60), TopUpFunder::Reactive).1, joined(0, 100));
    }

    /// A reactive top-up's amount is its spawner's: a second reactive top-up that
    /// joins it claims nothing (#2012).
    #[test]
    fn in_flight_reactive_topup_leaves_nothing_to_claim() {
        let mut slot = in_flight(100, TopUpFunder::Reactive);

        assert_eq!(slot.join(u(100), TopUpFunder::Reactive).1, joined(0, 100));
    }

    /// A refill that joins claims nothing, so it cannot take headroom a reactive
    /// top-up could claim later.
    #[test]
    fn in_flight_refill_joiner_claims_nothing() {
        let mut slot = in_flight(100, TopUpFunder::Refill);

        assert_eq!(slot.join(u(100), TopUpFunder::Refill).1, joined(0, 100));
        assert_eq!(slot.join(u(100), TopUpFunder::Reactive).1, joined(100, 100));
    }

    #[test]
    fn refill_decision_no_topup_with_headroom() {
        // committed 100 of 10_000; working 10_000 → low-water 2_000; remaining huge.
        assert_eq!(
            refill_decision(
                U256::from(10_000u64),
                U256::from(100u64),
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
                U256::from(10_000u64)
            ),
            U256::from(9_900u64)
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

    use std::sync::atomic::{AtomicUsize, Ordering};

    #[tokio::test]
    async fn happy_path_attempts_topup_once_and_never_reads_allowance() {
        let attempts = AtomicUsize::new(0);
        let recovers = AtomicUsize::new(0);
        let out = top_up_recovering_allowance(
            || {
                attempts.fetch_add(1, Ordering::SeqCst);
                async { Ok(U256::from(500u64)) }
            },
            || {
                recovers.fetch_add(1, Ordering::SeqCst);
                async { Ok(()) }
            },
        )
        .await;
        assert_eq!(out.unwrap(), U256::from(500u64));
        assert_eq!(attempts.load(Ordering::SeqCst), 1, "one topUp");
        assert_eq!(
            recovers.load(Ordering::SeqCst),
            0,
            "zero allowance reads on the happy path"
        );
    }

    #[tokio::test]
    async fn bad_path_approves_then_retries_topup_once() {
        let attempts = AtomicUsize::new(0);
        let recovers = AtomicUsize::new(0);
        let out = top_up_recovering_allowance(
            || {
                let n = attempts.fetch_add(1, Ordering::SeqCst);
                async move {
                    if n == 0 {
                        Err(anyhow::Error::new(
                            crate::client_requester::buyer_pool::AllowanceShortfall,
                        ))
                    } else {
                        Ok(U256::from(700u64))
                    }
                }
            },
            || {
                recovers.fetch_add(1, Ordering::SeqCst);
                async { Ok(()) }
            },
        )
        .await;
        assert_eq!(out.unwrap(), U256::from(700u64));
        assert_eq!(
            attempts.load(Ordering::SeqCst),
            2,
            "topUp, then retry after approve"
        );
        assert_eq!(recovers.load(Ordering::SeqCst), 1, "exactly one approve");
    }

    #[tokio::test]
    async fn terminal_non_allowance_revert_is_not_retried() {
        let attempts = AtomicUsize::new(0);
        let recovers = AtomicUsize::new(0);
        let out: Result<U256> = top_up_recovering_allowance(
            || {
                attempts.fetch_add(1, Ordering::SeqCst);
                async { Err(anyhow::anyhow!("topUp reverted for pool: paused")) }
            },
            || {
                recovers.fetch_add(1, Ordering::SeqCst);
                async { Ok(()) }
            },
        )
        .await;
        assert!(out.is_err());
        assert_eq!(
            attempts.load(Ordering::SeqCst),
            1,
            "no retry for a non-allowance revert"
        );
        assert_eq!(
            recovers.load(Ordering::SeqCst),
            0,
            "no approve for a non-allowance revert"
        );
    }

    #[tokio::test]
    async fn retry_still_shortfall_stops_after_one_retry() {
        let attempts = AtomicUsize::new(0);
        let out: Result<U256> = top_up_recovering_allowance(
            || {
                attempts.fetch_add(1, Ordering::SeqCst);
                async {
                    Err(anyhow::Error::new(
                        crate::client_requester::buyer_pool::AllowanceShortfall,
                    ))
                }
            },
            || async { Ok(()) },
        )
        .await;
        assert!(out.is_err());
        assert_eq!(
            attempts.load(Ordering::SeqCst),
            2,
            "one retry only, then give up"
        );
    }
}
