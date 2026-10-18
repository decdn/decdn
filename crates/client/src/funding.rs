//! How a run's lanes fund their pool (ADR 003): open or reuse the buyer's own
//! pool, or adopt a delegated one from a capability; refill a reused pool
//! that runs low; and the funding recovery step the acquire loop asks for at
//! an exhausted candidate set ([`PoolFunder`]).
//!
//! The run's funding facts ([`RunFunding`]) are shared by every lane build and
//! funding recovery step: the pool's spend outside the run's lanes, the pool
//! the run pays from, a wallet too short of USDC to top it up, and the handle
//! that holds the command's unconfirmed and untracked `openPool`s and `topUp`s
//! ([`UnconfirmedEscrows`]).

use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock, PoisonError};

use alloy::dyn_abi::Eip712Domain;
use alloy::primitives::{Address, U256};
use alloy::signers::local::PrivateKeySigner;
use anyhow::Context as _;
use decdn_incentive::buyer_pool::{AdvanceOutcome, BuyerPoolState, BuyerPoolStore};
use decdn_incentive::payment_pool::{PaymentPool, newest_solvent_owned_pool};
use decdn_incentive::tx::RECEIPT_TIMEOUT;
use decdn_incentive::{CapabilityGrant, Deployment, LaneKey, PoolId, bind_node_id_domain};
use iroh::Endpoint;
use tokio_util::sync::CancellationToken;

use crate::buyer_pool::{
    LOW_WATER_DIVISOR, OpenedPool, OwnedPayer, PoolNotOpen, TopUpUnconfirmed, ToppedUpPool,
    UnconfirmedEscrows, UnrecordedEscrow, WalletShortfall, ensure_allowance,
    escrowed_but_untracked, grade_deposit_credit, open_pool, pool_accepts_funds, refill_amount,
    self_owned_lane_ctx, top_up_unless_ended, topped_up_effect,
};
use crate::lanes::LaneDeps;
use crate::session::{ChainAdoption, ChainConfig, DepositPolicy};
use crate::source::{Funder, SourceFuture};
use crate::{NoAffordableSource, PoolContext, PoolLedger, PoolReplaced, Recovery};

/// A pool status as the operator reads it.
const fn status_label(status: PaymentPool::Status) -> &'static str {
    match status {
        PaymentPool::Status::Open => "open",
        PaymentPool::Status::Closing => "closing",
        PaymentPool::Status::Closed => "closed",
        // `sol!` enums carry a hidden invalid variant.
        _ => "?",
    }
}

/// The persisted pool's settlement token, or `None` when the owner has no pool
/// row yet. The token is `PaymentPool.usdc()`, immutable per contract, so a
/// persisted [`BuyerPoolState::token`] equals a fresh on-chain read — letting a
/// repeat fetch skip the `usdc()` `eth_call`. Only a first-ever pool falls
/// through to the on-chain read.
///
/// "Immutable per contract" is the whole premise, so the row has to be on the
/// deployment being read: a row from another `PaymentPool` deployment caches
/// that deployment's `usdc()`, and answering with it would approve and price
/// against the wrong token. Such a row reads as absent and the caller pays the
/// round-trip.
pub fn cached_pool_token(
    store: &dyn BuyerPoolStore,
    self_address: Address,
    deployment: Deployment,
) -> anyhow::Result<Option<Address>> {
    Ok(store
        .get_by_owner(self_address)?
        .filter(|state| state.is_on(deployment))
        .map(|state| state.token))
}

/// What the lane builds of one run learn about funding its pool, shared by
/// every fetch of the run: one blob fetch, or every entry of a bundle pull.
///
/// It holds what the run's lane builds and recovery steps share. The first is
/// the pool's spend across every lane: a
/// baseline for the lanes the run does not drive, plus what each lane the run
/// built has committed since. The deposit gate reads it
/// ([`Funder::pool_spent`]), so a pool shared across many providers gates on its
/// true remaining deposit whichever lanes the acquire loop starts. The second
/// is a wallet that holds too little USDC to top the pool up: once seen, no
/// later lane build of the run tries the proactive refill again, a funding
/// recovery step fails without a transaction, and the command ends with one
/// `warning:` line naming it. It also names the pool the run's first built
/// lane pays from, which a funding recovery step tops up: every entry of a
/// bundle pull shares it, the entries that have built no lane yet included.
/// Every `openPool` and `topUp` a lane build or a recovery step sends is held
/// in its [`UnconfirmedEscrows`] until its local record or credit resolves
/// ([`UnrecordedEscrow`]). The run sends one
/// `topUp` at a time, and while the handle holds one that may have been
/// broadcast but is not known to have mined ([`TopUpUnconfirmed`]), every later
/// `topUp` sent through the same handle is refused with it: in this run, or in
/// a later pass or fetch that shares the handle. Once an `openPool` or `topUp`
/// of the run mines and its local record fails
/// ([`EscrowUntracked`](crate::buyer_pool::EscrowUntracked)), the handle holds
/// that escrow ([`UnconfirmedEscrows::untracked`]): every later `openPool` or
/// `topUp` of a run on the same handle is refused, fatally, before its
/// allowance read or send. A sibling lane build still in flight reads a row
/// that does not credit the escrow, and would escrow again. The hold lives on
/// the handle, so it outlives a dropped fetch, a dropped bundle entry and a
/// pass. A caller that keeps a clone of the handle names it when the command
/// ends.
#[derive(Debug, Default)]
pub struct RunFunding {
    spend: Mutex<RunSpend>,
    shortfall: Mutex<Option<String>>,
    pool_id: OnceLock<PoolId>,
    escrows: UnconfirmedEscrows,
    /// Held across each `topUp` of the run, from its refusal checks (an
    /// unconfirmed `topUp` or an untracked escrow in `escrows`) to the `topUp`'s
    /// outcome, so the checks and the send they allow are one step within this
    /// run. A handle shared with another live
    /// `RunFunding` is not serialized by it.
    top_up_serial: tokio::sync::Mutex<()>,
}

/// The refusal of an `openPool` or `topUp` once the run's handle holds an
/// escrow no local record credits
/// ([`EscrowUntracked`](crate::buyer_pool::EscrowUntracked)), from this run or
/// an earlier one on the handle. It holds the text of each held escrow. The
/// refused call escrows nothing, so it is not itself an untracked escrow. The
/// acquire loop classifies it as fatal to the command
/// ([`crate::Fault::Fatal`]): the run cannot fund a lane without escrowing
/// again.
#[derive(Debug)]
pub(crate) struct RefusedAfterUntracked(String);

impl std::fmt::Display for RefusedAfterUntracked {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "refusing an openPool or topUp: an earlier escrow of this command is untracked: {}",
            self.0
        )
    }
}

impl std::error::Error for RefusedAfterUntracked {}

/// The refusal of an `openPool` or `topUp` that a lane build reaches after the
/// fetch it serves has ended, or of a build still queued on the open or
/// `topUp` lock as it ends ([`crate::SourceProvider::connect`]): the lane
/// would never run, so the escrow would fund nothing this fetch needs. The
/// refused call escrows nothing. The build fails, and the drain of late lane
/// builds logs it at `debug`.
#[derive(Debug)]
pub(crate) struct FetchEnded;

impl std::fmt::Display for FetchEnded {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("the fetch ended before this lane build's openPool or topUp; it sends none")
    }
}

impl std::error::Error for FetchEnded {}

/// Refuse an `openPool` or `topUp` once `ended` fires ([`FetchEnded`]).
/// `None` is a caller outside any lane build, such as the funding recovery
/// step, which runs only while its fetch does.
pub(crate) fn refuse_after_end(ended: Option<&CancellationToken>) -> anyhow::Result<()> {
    if ended.is_some_and(CancellationToken::is_cancelled) {
        return Err(anyhow::Error::new(FetchEnded));
    }
    Ok(())
}

/// Take `lock`, or refuse with [`FetchEnded`] once `ended` fires, whichever
/// comes first. A lane build queued behind a sibling that holds the lock
/// through a receipt wait has sent nothing, so it ends at once rather than at
/// the drain's bound. A fired `ended` refuses even a free lock. `None` takes
/// the lock as a plain wait, as `refuse_after_end(None)` refuses nothing.
pub(crate) async fn lock_unless_ended<'a>(
    lock: &'a tokio::sync::Mutex<()>,
    ended: Option<&CancellationToken>,
) -> anyhow::Result<tokio::sync::MutexGuard<'a, ()>> {
    let Some(ended) = ended else {
        return Ok(lock.lock().await);
    };
    tokio::select! {
        biased;
        () = ended.cancelled() => Err(anyhow::Error::new(FetchEnded)),
        guard = lock.lock() => Ok(guard),
    }
}

/// What a lane build learned about the pool's spend, for
/// [`RunFunding::join_lane`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct LaneSpend {
    /// The pool's spend on every other lane, as the build saw it.
    pub(crate) outside: U256,
    /// The amount the lane resumes from.
    pub(crate) prior: U256,
    /// Whether the pool row recorded the lane before this build seeded it from
    /// chain, so that an earlier lane's `outside` counts its prior.
    pub(crate) recorded: bool,
    /// Whether `outside` includes redeemed spend no tracked lane accounts for
    /// ([`BuyerPoolState::redeemed_elsewhere`], from an adopted row's
    /// `totalRedeemed`), which counts the watermark of every lane the row has
    /// not seeded.
    pub(crate) from_chain: bool,
}

/// The pool's spend as a run's lanes join it.
#[derive(Debug, Default)]
struct RunSpend {
    /// The spend on lanes the run does not drive. `None` until the first lane
    /// joins.
    baseline: Option<U256>,
    /// Whether the first lane's `outside` came from the chain.
    from_chain: bool,
    /// Every ledger each joined lane has paid through. A lane rebuilt with a
    /// fresh ledger keeps its earlier ones: vouchers on a lane are cumulative,
    /// so the lane's spend is the largest of them.
    lanes: HashMap<LaneKey, Vec<Arc<PoolLedger>>>,
}

impl RunFunding {
    /// A run whose `openPool` and `topUp` transactions are held in `escrows`,
    /// a handle the caller keeps after the run's state is dropped. A caller
    /// that builds a `RunFunding` per pass passes the same handle to each, so
    /// a `topUp` one pass left unconfirmed refuses the next pass's, and an
    /// untracked escrow ([`UnconfirmedEscrows::untracked`]) refuses every later
    /// `openPool` and `topUp`.
    #[must_use]
    pub fn reporting_escrows_to(escrows: UnconfirmedEscrows) -> Self {
        Self {
            escrows,
            ..Self::default()
        }
    }

    /// The handle that holds the run's `openPool` and `topUp` transactions
    /// that no local record credits yet ([`UnconfirmedEscrows::pending`]), and those
    /// that escrowed with no local record crediting them
    /// ([`UnconfirmedEscrows::untracked`]).
    pub const fn escrows(&self) -> &UnconfirmedEscrows {
        &self.escrows
    }

    /// Name `pool_id` as the pool the run's lanes pay from. The first lane
    /// built names it; every later lane pays from the same pool.
    pub(crate) fn note_pool(&self, pool_id: PoolId) {
        let _ = self.pool_id.set(pool_id);
    }

    /// The pool the run's lanes pay from, once a lane is built.
    pub fn pool_id(&self) -> Option<PoolId> {
        self.pool_id.get().copied()
    }

    /// Record that `lane` joins the run, paying through `ledger`.
    ///
    /// The first lane sets the baseline to its `outside`. Each later lane
    /// moves its prior out of the baseline when the baseline counts it: when
    /// the row recorded the lane, or when the baseline came from the chain. Its
    /// ledger counts that amount from now on. A lane that has joined before
    /// only adds `ledger`, if it is a new one.
    pub(crate) fn join_lane(&self, lane: LaneKey, spend: LaneSpend, ledger: &Arc<PoolLedger>) {
        let mut run = self.spend.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(ledgers) = run.lanes.get_mut(&lane) {
            if !ledgers.iter().any(|known| Arc::ptr_eq(known, ledger)) {
                ledgers.push(Arc::clone(ledger));
            }
            return;
        }
        run.lanes.insert(lane, vec![Arc::clone(ledger)]);
        run.baseline = Some(match run.baseline {
            None => {
                run.from_chain = spend.from_chain;
                spend.outside
            }
            Some(baseline) if spend.recorded || run.from_chain => {
                baseline.saturating_sub(spend.prior)
            }
            Some(baseline) => baseline,
        });
    }

    /// The pool's spend across every lane: the baseline plus each joined
    /// lane's committed amount. `None` before any lane joins.
    pub(crate) fn pool_spent(&self) -> Option<U256> {
        let run = self.spend.lock().unwrap_or_else(PoisonError::into_inner);
        let baseline = run.baseline?;
        Some(run.lanes.values().fold(baseline, |total, ledgers| {
            let lane = ledgers
                .iter()
                .map(|ledger| ledger.committed().amount)
                .max()
                .unwrap_or(U256::ZERO);
            total.saturating_add(lane)
        }))
    }

    /// Refuse an `openPool` or `topUp` once the run's handle holds an
    /// untracked escrow ([`UnconfirmedEscrows::untracked`]), from this run or an
    /// earlier one on the same handle.
    ///
    /// A failed record is held ([`UnrecordedEscrow::untracked`]) with no
    /// `.await` after the failure, and the run's lane builds and recovery steps
    /// share one task. So a call that reaches this check after the failure sees
    /// the hold. A `topUp` checks under `top_up_serial`, which an earlier
    /// `topUp` holds until its outcome, so it always sees that `topUp`'s hold.
    /// An `openPool` that passed its check before another call's escrow went
    /// untracked still sends. When it also goes untracked, the handle holds
    /// both.
    ///
    /// # Errors
    ///
    /// [`RefusedAfterUntracked`], naming each held escrow.
    pub(crate) fn refuse_after_untracked(&self) -> anyhow::Result<()> {
        let held = self.escrows.untracked();
        if held.is_empty() {
            return Ok(());
        }
        let held: Vec<String> = held.into_iter().map(|escrow| escrow.text).collect();
        Err(anyhow::Error::new(RefusedAfterUntracked(held.join("; "))))
    }

    /// The warning for a wallet seen to hold too little USDC for a top-up in
    /// this run, if one was.
    pub fn shortfall(&self) -> Option<String> {
        self.shortfall
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// Diagnose a failed top-up leg (`err`) for `pool_id` by reading the
    /// wallet's USDC balance. When the wallet holds less than `additional`,
    /// which no retry fixes, record the shortfall and return its warning; it
    /// is logged at WARN the first time. Otherwise return `None`, after a WARN
    /// that names the failure, the balance, or why the balance could not be
    /// read.
    pub(crate) async fn check_wallet<P>(
        &self,
        rpc: &P,
        token: Address,
        owner: Address,
        pool_id: PoolId,
        additional: U256,
        err: &anyhow::Error,
    ) -> Option<String>
    where
        P: alloy::providers::Provider + Clone,
    {
        let error = decdn_common::redact::sanitize_err_chain(err);
        let balance = match decdn_incentive::Erc20::new(token, rpc.clone())
            .balanceOf(owner)
            .call()
            .await
        {
            Ok(balance) => balance,
            Err(read) => {
                let read = decdn_common::redact::sanitize_err_chain(&anyhow::Error::new(read));
                tracing::warn!(
                    %pool_id,
                    %additional,
                    %error,
                    wallet_usdc_error = %read,
                    "buyer pool top-up failed, and the wallet's USDC balance could not be read"
                );
                return None;
            }
        };
        if balance >= additional {
            tracing::warn!(
                %pool_id,
                %additional,
                wallet_usdc = %balance,
                %error,
                "buyer pool top-up failed although the wallet holds enough USDC"
            );
            return None;
        }
        let warning = format!(
            "wallet {owner} holds {balance} µUSDC, less than the {additional} µUSDC top-up \
             buyer pool {pool_id} needs; fund the wallet so the pool can be topped up"
        );
        let mut shortfall = self
            .shortfall
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if shortfall.is_none() {
            tracing::warn!(
                %pool_id,
                %additional,
                wallet_usdc = %balance,
                %error,
                "the wallet holds too little USDC to top up the buyer pool"
            );
            *shortfall = Some(warning.clone());
        }
        Some(warning)
    }
}

/// A self-owned pool's [`Funder`]: the funding recovery step (ADR 003 § Funding
/// recovery). A self-funded owner tops its current pool up toward the working
/// deposit through the same allowance → `topUp` → `add_deposit` path as the
/// proactive low-water refill (`escrow_top_up`, then `add_deposit`). Once a
/// `topUp` of the run is unconfirmed, the step sends none and fails with it,
/// and once the run's handle holds an untracked escrow, it sends no `topUp` or
/// `openPool`. A pool that is `Closing` or `Closed` reverts that `topUp` with
/// `PoolNotOpen`; the owner
/// then opens a new pool at the working deposit and records it as the current
/// pool in the buyer store, and the step reports [`Recovery::Replaced`]. The
/// old pool is not reclaimed here.
///
/// A delegated signer has no funder this command can ask, so its working
/// deposit is zero and every step reports [`Recovery::Unavailable`]: the fetch
/// ends "funding needed".
///
/// The acquire loop decides WHETHER to fund (its candidate set is exhausted
/// and the progress rule allows a step); this only executes the on-chain move.
/// Every failure returns to the loop, which ends the fetch "funding needed",
/// or fatally when an escrow of the run may be untracked: this step's, or an
/// earlier one that refuses this step's.
pub struct PoolFunder<'a, P> {
    pub(crate) contract: &'a PaymentPool::PaymentPoolInstance<P>,
    pub(crate) rpc: &'a P,
    pub(crate) store: &'a dyn BuyerPoolStore,
    pub(crate) owner: Address,
    /// The chain and `PaymentPool` a replacement pool opens on.
    pub(crate) deployment: Deployment,
    pub(crate) token: Address,
    pub(crate) payment_pool_addr: Address,
    /// The deposit a step tops up toward and a replacement pool opens at.
    /// Zero for a delegated signer, which has no funding path.
    pub(crate) working_deposit: U256,
    pub(crate) max_approve: bool,
    /// The run's funding facts: its outside spend, a wallet shortfall that
    /// makes a further top-up pointless, an unconfirmed `topUp` or an
    /// untracked escrow that makes any further escrow unsafe, and the pool to
    /// top up ([`RunFunding::pool_id`]).
    pub(crate) funding: &'a RunFunding,
}

impl<P> std::fmt::Debug for PoolFunder<'_, P> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PoolFunder")
            .field("owner", &self.owner)
            .field("deployment", &self.deployment)
            .field("working_deposit", &self.working_deposit)
            .finish_non_exhaustive()
    }
}

impl<P> Funder for PoolFunder<'_, P>
where
    P: alloy::providers::Provider + Clone,
{
    fn pool_spent(&self) -> Option<U256> {
        self.funding.pool_spent()
    }

    fn recover(&self, remaining: U256) -> SourceFuture<'_, Recovery> {
        Box::pin(async move {
            // A delegated signer owns no pool it could fund.
            if self.working_deposit.is_zero() {
                return Ok(Recovery::Unavailable);
            }
            let pool_id = self
                .funding
                .pool_id()
                .ok_or_else(|| anyhow::anyhow!("no payment lane is built, so no pool to top up"))?;
            let remaining = self.chain_remaining(pool_id, remaining).await;
            let additional = self.working_deposit.saturating_sub(remaining);
            if additional.is_zero() {
                return self.settle_or_replace(pool_id, remaining).await;
            }
            if let Some(shortfall) = self.funding.shortfall() {
                return Err(
                    anyhow::anyhow!("the wallet cannot fund a top-up: {shortfall}")
                        .context(WalletShortfall),
                );
            }
            let escrowed = escrow_top_up(
                self.funding,
                self.contract,
                self.token,
                self.owner,
                pool_id,
                additional,
                self.max_approve,
                None,
            )
            .await;
            // A failure before any receipt names its cause once: a wallet
            // short of USDC is recorded for the run's closing warning.
            let (ToppedUpPool { credited, tx, .. }, unrecorded) = match escrowed {
                Ok(topped_up) => topped_up,
                Err(err) if escrow_held(&err) => return Err(err),
                // The gas estimate reverted `PoolNotOpen`: the pool is closing
                // or closed, and a pool is never reopened. Open its successor.
                Err(err) if err.downcast_ref::<PoolNotOpen>().is_some() => {
                    tracing::info!(
                        %pool_id,
                        "the current buyer pool no longer accepts funds; opening a new pool"
                    );
                    let opened = self.open_replacement().await?;
                    return Ok(Recovery::Replaced(PoolReplaced {
                        closed: pool_id,
                        opened,
                    }));
                }
                Err(err) => {
                    let short = self
                        .funding
                        .check_wallet(self.rpc, self.token, self.owner, pool_id, additional, &err)
                        .await;
                    return Err(match short {
                        Some(_) => err.context(WalletShortfall),
                        None => err,
                    });
                }
            };
            // The escrow has moved: a credit that does not land names the tx
            // and the pool, and is fatal ([`grade_deposit_credit`]).
            let effect = topped_up_effect(pool_id, credited);
            let new_deposit = match grade_deposit_credit(
                self.store.add_deposit(self.owner, pool_id, credited),
                &effect,
                tx,
            ) {
                Ok(deposit) => {
                    unrecorded.recorded();
                    deposit
                }
                Err(err) => return Err(unrecorded.untracked(err)),
            };
            Ok(Recovery::ToppedUp(new_deposit))
        })
    }
}

impl<P> PoolFunder<'_, P>
where
    P: alloy::providers::Provider + Clone,
{
    /// The pool's remaining deposit for a recovery step: the run's `remaining`,
    /// capped at what the chain shows.
    ///
    /// Other signers share the pool, such as another data dir on the wallet
    /// ([`decdn_incentive::lane_signer`]). Their redemptions reach the run only
    /// from chain, and can land while the run is going. So the step reads
    /// `getPool` and counts at least `totalRedeemed` as spent
    /// ([`BuyerPoolState::reconcile_with_chain`]). Without it, a pool drained
    /// by another signer reads full here, the step only settles, and the run
    /// ends "funding needed" with a funded wallet. A failed read keeps the
    /// run's view.
    async fn chain_remaining(&self, pool_id: PoolId, remaining: U256) -> U256 {
        match self.contract.getPool(pool_id).call().await {
            Ok(pool) if pool.owner == self.owner => {
                let spent = self
                    .funding
                    .pool_spent()
                    .unwrap_or(U256::ZERO)
                    .max(U256::from(pool.totalRedeemed));
                remaining.min(U256::from(pool.deposit).saturating_sub(spent))
            }
            Ok(_) => remaining,
            Err(error) => {
                tracing::debug!(
                    %pool_id,
                    %error,
                    "could not read the buyer pool; the funding step uses the run's view"
                );
                remaining
            }
        }
    }

    /// The recovery step for a pool that already holds its working deposit,
    /// so a top-up has nothing to add. A `topUp` gas estimate (no transaction)
    /// tells a closing or closed pool, which a new pool replaces. An open pool
    /// is the nodes' stale view of a deposit they have not yet seen: the step
    /// settles, so the gate's settle window holds those nodes' refusals and
    /// asks them again ([`Recovery::ToppedUp`] at the current deposit).
    ///
    /// # Errors
    ///
    /// The replacement open fails.
    async fn settle_or_replace(
        &self,
        pool_id: PoolId,
        remaining: U256,
    ) -> anyhow::Result<Recovery> {
        if !pool_accepts_funds(self.contract, self.owner, pool_id).await {
            tracing::info!(
                %pool_id,
                "the current buyer pool holds its working deposit but accepts no more funds; \
                 opening a new pool"
            );
            let opened = self.open_replacement().await?;
            return Ok(Recovery::Replaced(PoolReplaced {
                closed: pool_id,
                opened,
            }));
        }
        // The settle step escrows nothing, so an estimate is safe here: it only
        // names the deposit the nodes are asked again at.
        let estimate = || remaining.saturating_add(self.funding.pool_spent().unwrap_or(U256::ZERO));
        let deposit = match self.store.get_by_pool_id(pool_id) {
            Ok(Some(row)) => row.deposit,
            Ok(None) => estimate(),
            Err(error) => {
                tracing::warn!(
                    %pool_id,
                    %error,
                    "could not read the buyer pool's row; estimating its deposit from the run"
                );
                estimate()
            }
        };
        tracing::info!(
            %pool_id,
            %deposit,
            "the buyer pool already holds its working deposit; waiting for the nodes to see it"
        );
        Ok(Recovery::ToppedUp(deposit))
    }

    /// Open a new pool at the working deposit and record it as the owner's
    /// current pool in the buyer store, where the next lane build reuses it.
    /// The closed pool's row stays in the store, so `decdn pool reclaim` and
    /// `pool list` still find it.
    ///
    /// # Errors
    ///
    /// The run's handle holds an untracked escrow ([`RefusedAfterUntracked`]:
    /// nothing sent), the allowance or `openPool` fails (nothing escrowed), or the
    /// `openPool` mined but the pool it opened cannot be named or recorded
    /// (escrowed and untracked: fatal).
    async fn open_replacement(&self) -> anyhow::Result<PoolId> {
        self.funding.refuse_after_untracked()?;
        let deposit = self.working_deposit;
        ensure_allowance(
            self.rpc,
            self.token,
            self.owner,
            self.payment_pool_addr,
            if self.max_approve {
                None
            } else {
                Some(deposit)
            },
        )
        .await?;
        let (opened, unrecorded) = open_pool(
            self.contract,
            self.deployment,
            self.token,
            self.owner,
            deposit,
            Some(self.funding.escrows()),
            Some(RECEIPT_TIMEOUT),
        )
        .await?;
        record_opened(self.store, &opened, unrecorded)?;
        tracing::info!(pool_id = %opened.state.pool_id, %deposit, "opened a new buyer pool");
        Ok(opened.state.pool_id)
    }
}

/// The keys one lane signs under, resolved when the lane is built.
pub(crate) enum LaneKeys<'p> {
    /// The buyer's own pool: the owner key owns it, the lane key signs.
    Owned(&'p OwnedPayer),
    /// A pool another owner funds: `signer` is the delegate `grant` authorizes.
    Delegated {
        /// The delegate key.
        signer: Arc<PrivateKeySigner>,
        /// The owner's capability for `signer`.
        grant: CapabilityGrant,
    },
}

impl LaneKeys<'_> {
    /// The key the lane's vouchers sign under.
    pub(crate) const fn lane_signer(&self) -> &Arc<PrivateKeySigner> {
        match self {
            Self::Owned(payer) => payer.lane(),
            Self::Delegated { signer, .. } => signer,
        }
    }
}

/// Select the pool context for one lane of `deps`' run: the delegated
/// adoption path for [`LaneKeys::Delegated`] (adopt the named pool, present
/// the owner's capability, no open), else the self-owned open-or-reuse path,
/// which sends no `openPool` or `topUp` once `ended` fires.
pub(crate) async fn build_ctx_for_fetch<P>(
    deps: &LaneDeps<'_, P>,
    keys: &LaneKeys<'_>,
    voucher_dom: &Eip712Domain,
    provider: Address,
    ended: &CancellationToken,
) -> anyhow::Result<(PoolContext, Option<LaneSpend>)>
where
    P: alloy::providers::Provider + Clone,
{
    match keys {
        LaneKeys::Delegated { signer, grant } => build_delegated_pool_ctx(
            deps.store.as_ref(),
            deps.contract,
            signer,
            voucher_dom,
            provider,
            deps.chain,
            deps.endpoint,
            grant,
        )
        .await
        .map(|ctx| (ctx, None)),
        LaneKeys::Owned(payer) => build_pool_ctx(deps, payer, provider, ended)
            .await
            .map(|(ctx, spend)| (ctx, Some(spend))),
    }
}

/// [`open_or_reuse_pool`] plus the ADR 005 client identity binding (#1115):
/// sign our OWN iroh `NodeId` with the lane key, so the serving node can prove
/// the request comes from the lane's signer and reactively pull a cache-missed
/// blob for it.
///
/// The bind domain's verifying contract is the `CapacityBond`; without one
/// configured we can't sign, so the request goes out unbound and the node serves
/// only content it already holds (a cache miss is refused). No warning is
/// emitted for that — it would fire on every successful cached fetch too,
/// training users to ignore it; the caller explains the refusal at the point
/// of failure.
async fn build_pool_ctx<P>(
    deps: &LaneDeps<'_, P>,
    payer: &OwnedPayer,
    provider: Address,
    ended: &CancellationToken,
) -> anyhow::Result<(PoolContext, LaneSpend)>
where
    P: alloy::providers::Provider + Clone,
{
    let (ctx, spend) = open_or_reuse_pool(
        deps.store.as_ref(),
        deps.contract,
        deps.rpc,
        deps.funding,
        payer,
        provider,
        deps.chain.deployment(),
        deps.deposit,
        ended,
    )
    .await?;
    Ok((
        attach_client_binding(ctx, deps.chain, deps.endpoint)?,
        spend,
    ))
}

/// Build a [`PoolContext`] that adopts a pool the caller does NOT own, from a
/// delegated [`CapabilityGrant`] (`decdn pool assign` → `--capability`).
///
/// Unlike [`build_pool_ctx`] this opens nothing on-chain and signs no
/// self-capability: it presents the OWNER's capability (rebuilt from the token)
/// so the serving node registers this client's signer on its first redemption.
/// `signer` is the delegate the capability authorizes: the caller takes both
/// from one [`crate::Credential`], which checks that they match.
///
/// The pool's on-chain row is read once for the informational `deposit` and to
/// confirm the pool exists (a zero-owner row means it was never opened on this
/// contract). The lane resumes from the store's delegate lane for
/// `(pool, this signer, provider)` on this deployment
/// ([`BuyerPoolStore::delegate_lanes`]); a delegate that has never streamed this
/// lane starts at zero. The ADR 005 client binding is attached the same as the
/// self-owned path: the delegate signs it, so the serving node can tie the
/// request to the delegate's lane.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn build_delegated_pool_ctx<P>(
    store: &dyn BuyerPoolStore,
    contract: &PaymentPool::PaymentPoolInstance<P>,
    signer: &Arc<PrivateKeySigner>,
    voucher_dom: &Eip712Domain,
    provider: Address,
    chain: &ChainConfig,
    endpoint: &Endpoint,
    grant: &CapabilityGrant,
) -> anyhow::Result<PoolContext>
where
    P: alloy::providers::Provider + Clone,
{
    let self_address = signer.address();
    let signed_capability = grant.to_signed_capability().map_err(|e| {
        anyhow::anyhow!("capability token carries a malformed owner signature: {e}")
    })?;

    let pool_id = grant.pool_id;
    let pool = contract.getPool(pool_id).call().await.map_err(|e| {
        anyhow::Error::new(e).context(format!("read on-chain pool {pool_id} for the capability"))
    })?;
    anyhow::ensure!(
        pool.owner != Address::ZERO,
        "pool {pool_id} named by the capability does not exist on this PaymentPool contract \
         ({}) — wrong --payment-pool-address/chain, or a stale token",
        chain.payment_pool,
    );

    // A store that cannot be read is this machine's fault: it fails the
    // command rather than retrying the lane build as a chain-side fault.
    let (prior_bytes, prior_amount) = store
        .delegate_lanes(chain.deployment(), pool_id, self_address)
        .map_err(|e| {
            anyhow::anyhow!(
                "read this key's delegate-lane watermarks for pool {pool_id} from the buyer \
                 store: {e}; the store needs repair before a delegated fetch can run"
            )
            .context(crate::LocalPullFault)
        })?
        .into_iter()
        .find(|(lane_provider, _)| *lane_provider == provider)
        .map_or((U256::ZERO, U256::ZERO), |(_, p)| {
            (p.last_bytes, p.last_amount)
        });

    // A transient state carrying the informational pool facts the context reads
    // (`pool_id`, `deposit`); it is not persisted (the delegate owns no pool
    // row, and its lane watermarks go to the delegate lanes instead).
    // The pool does not store its token — it is the contract's immutable
    // `usdc()`, so read it there rather than duplicating it per pool.
    let token = contract
        .usdc()
        .call()
        .await
        .map_err(|e| anyhow::Error::new(e).context("read PaymentPool.usdc()"))?;
    let state = BuyerPoolState::new(
        pool_id,
        chain.deployment(),
        pool.owner,
        token,
        U256::from(pool.deposit),
    );
    let ctx = PoolContext::for_pool(&state, Arc::clone(signer), voucher_dom.clone())
        .with_provider(provider, prior_bytes, prior_amount)
        .with_capability(signed_capability);
    attach_client_binding(ctx, chain, endpoint)
}

/// Attach the ADR 005 client identity binding to an already-built
/// [`PoolContext`] (#1115): sign our OWN iroh `NodeId` with the context's
/// `client_signer`, the key the lane's vouchers sign under
/// ([`PoolContext::bind_endpoint`]). The serving node keys the lane on the
/// binding's signer, so a binding signed by any other key names a lane the
/// vouchers do not pay on, and the node refuses the request. Separate from
/// [`build_pool_ctx`] so a bundle pull — which takes the `open_lock`
/// itself around [`open_or_reuse_pool`] — can attach the same binding outside
/// that critical section.
///
/// No binding when `chain.capacity_bond` is unset — see [`build_pool_ctx`]'s
/// docs for why that is silent.
pub(crate) fn attach_client_binding(
    ctx: PoolContext,
    chain: &ChainConfig,
    endpoint: &Endpoint,
) -> anyhow::Result<PoolContext> {
    let Some(capacity_bond) = chain.capacity_bond else {
        return Ok(ctx);
    };
    ctx.bind_endpoint(
        endpoint,
        &bind_node_id_domain(chain.chain_id, capacity_bond),
    )
}

/// Adopt the live pool `owner` already holds on `deployment`, recording it in
/// the client store, or return `None` if the chain lists no `Open` pool with
/// deposit left to spend. The adopted row carries the pool's `totalRedeemed`
/// as [`BuyerPoolState::redeemed_elsewhere`].
///
/// The node adopts by the same rule at bootstrap (the newest `Open`, solvent
/// pool); the client does it on demand, because a client is a one-shot process
/// with no bootstrap to hang it on. The two differ on failure, deliberately. The
/// node fails open: an unreadable enumeration leaves the first miss to open a
/// fresh pool, because a daemon that could not buy for the rest of its life is
/// worse than an occasional stranded deposit. The client fails closed: a read
/// that faults errors rather than answering `None`, because a one-shot fetch can
/// simply be rerun, and "could not tell" must not become "go open another pool".
/// Making the two consistent would mean weakening one of them.
async fn adopt_owned_pool<P>(
    store: &dyn BuyerPoolStore,
    contract: &PaymentPool::PaymentPoolInstance<P>,
    owner: Address,
    deployment: Deployment,
) -> anyhow::Result<Option<BuyerPoolState>>
where
    P: alloy::providers::Provider + Clone,
{
    let Some((pool_id, pool)) = newest_solvent_owned_pool(contract, owner).await.context(
        "could not ask the chain whether this wallet already owns a pool, so refusing to \
             open one blind — rerun, or check the RPC endpoint",
    )?
    else {
        return Ok(None);
    };
    let token =
        contract.usdc().call().await.with_context(|| {
            format!("read PaymentPool.usdc() while adopting live pool {pool_id}")
        })?;
    // The row has no lanes to account for what the pool already paid out, so it
    // keeps `totalRedeemed`, less what each lane it seeds takes over.
    let state = BuyerPoolState::adopt(
        pool_id,
        deployment,
        owner,
        token,
        U256::from(pool.deposit),
        U256::from(pool.totalRedeemed),
    );
    store.record(&state).with_context(|| {
        format!(
            "found live pool {pool_id} on chain but could not record it in the local buyer \
             store; nothing was escrowed, so rerunning retries the adoption"
        )
    })?;
    tracing::info!(
        %pool_id,
        deposit = pool.deposit,
        redeemed = pool.totalRedeemed,
        "adopted a live buyer pool this wallet already owns on chain, instead of opening a \
         second one"
    );
    Ok(Some(state))
}

/// The on-chain `(bytes, amount)` watermark for `lane` — what a lane with no
/// local record resumes from.
///
/// It reflects redeemed vouchers only, so a provider still holding one it has
/// not redeemed is ahead of it. That gap repairs itself inside the fetch: the
/// provider answers the first stale proof that its lane headroom cannot pay
/// with that watermark (the wallet-less resume, #1946), and the driver reseeds
/// from it — unless the resume budget is spent, where the fetch fails. Resuming below the chain
/// watermark has no such repair: every voucher at or below it redeems nothing
/// on chain, whatever the provider does.
async fn lane_watermark<P>(
    contract: &PaymentPool::PaymentPoolInstance<P>,
    lane: LaneKey,
) -> anyhow::Result<(U256, U256)>
where
    P: alloy::providers::Provider + Clone,
{
    let onchain = contract
        .getWatermark(lane.pool_id, lane.signer, lane.provider)
        .call()
        .await
        .context(
            "could not read this lane's on-chain watermark, so refusing to resume it from zero, \
             where it would stream bytes the provider cannot cash",
        )?;
    Ok((
        U256::from(onchain.bytesDelivered),
        U256::from(onchain.amount),
    ))
}

/// The pool this buy reuses, if any, and what that pool has already paid out
/// beyond what its row's lanes account for.
///
/// A tracked row is reused only if it is on `deployment` — the same chain and
/// the same `PaymentPool` address. A row from another deployment is not this
/// contract's pool, whatever its id says: `pool_id` is
/// `keccak256(owner, ownerPoolNonce)` and a redeploy restarts that nonce, so the
/// id alone will eventually name an existing, unrelated pool here, and its lane
/// progress would seed the first voucher at a cumulative this pool has never
/// redeemed against. It is treated as no row.
///
/// A tracked row is reused only while its pool is open. Only the owner closes a
/// pool, but the owner key can close it from another machine, and anyone can
/// reclaim it once its dispute window ends. Nodes answer a request on a closed
/// pool `NotFound`, the same as a missing blob, so a fetch on it never reaches
/// a funding recovery step. So the first lane build of a run reads the pool
/// once ([`still_open`]), and a pool the chain shows `Closing` or `Closed` is
/// treated as no row: the run adopts or opens another.
///
/// No row is not the same as no pool. The store loses rows — a reset data dir, a
/// new machine, a store-format change — while the pool they named is still open
/// on chain with deposit in it. Where `adoption` allows, the chain is asked
/// before anything is opened, so a lost row adopts the live pool instead of
/// escrowing a second deposit beside it, which reverts when the wallet's
/// remaining USDC cannot cover it.
///
/// An adopted row carries the pool's `totalRedeemed` as
/// [`BuyerPoolState::redeemed_elsewhere`], and keeps the part no seeded lane
/// has taken over on every later run.
/// The row has no lanes to account for what the pool paid out before
/// adoption, and without it the refill decision reads a pool other lanes
/// drained as a full deposit: no top-up fires, and the provider refuses the
/// fetch on a balance the client believes it has.
async fn pool_to_reuse<P>(
    store: &dyn BuyerPoolStore,
    contract: &PaymentPool::PaymentPoolInstance<P>,
    funding: &RunFunding,
    self_address: Address,
    deployment: Deployment,
    adoption: ChainAdoption,
) -> anyhow::Result<Option<BuyerPoolState>>
where
    P: alloy::providers::Provider + Clone,
{
    let tracked = match store.get_by_owner(self_address)? {
        Some(state) if state.is_on(deployment) => Some(state),
        Some(state) => {
            tracing::warn!(
                pool_id = %state.pool_id,
                foreign_payment_pool = %state.deployment.payment_pool,
                foreign_chain_id = state.deployment.chain_id,
                configured_payment_pool = %deployment.payment_pool,
                configured_chain_id = deployment.chain_id,
                "ignoring a tracked buyer pool from another PaymentPool deployment; its deposit, \
                 if any, is recoverable only against `foreign_payment_pool`"
            );
            None
        }
        None => None,
    };
    // A lane of this run already pays from the pool, so its first build read it.
    let tracked = match tracked {
        Some(state) if funding.pool_id() != Some(state.pool_id) => {
            let kept = still_open(contract, self_address, state).await;
            if let Some(state) = &kept {
                funding.note_pool(state.pool_id);
            }
            kept
        }
        other => other,
    };
    Ok(match (tracked, adoption) {
        (Some(state), _) => Some(state),
        (None, ChainAdoption::Refused) => None,
        (None, ChainAdoption::Allowed) => {
            adopt_owned_pool(store, contract, self_address, deployment).await?
        }
    })
}

/// `state` while the chain shows its pool open, or `None` once the chain shows
/// this owner's pool `Closing` or `Closed`. A failed read, or a pool the chain
/// holds no record of yet, keeps `state`: a lagging RPC can miss a pool opened
/// moments ago, and replacing it on that read would escrow a second deposit.
/// An open pool's `state` takes in its on-chain deposit and redeemed spend
/// ([`BuyerPoolState::reconcile_with_chain`]), so the run's funding counts what
/// other signers on the pool spent. This is a run's first lane build, so the
/// reconciled spend sets the run's baseline ([`RunFunding::join_lane`]).
async fn still_open<P>(
    contract: &PaymentPool::PaymentPoolInstance<P>,
    self_address: Address,
    state: BuyerPoolState,
) -> Option<BuyerPoolState>
where
    P: alloy::providers::Provider + Clone,
{
    match contract.getPool(state.pool_id).call().await {
        Ok(pool)
            if pool.owner == self_address && !matches!(pool.status, PaymentPool::Status::Open) =>
        {
            tracing::info!(
                pool_id = %state.pool_id,
                status = status_label(pool.status),
                "the tracked buyer pool no longer accepts work; a new pool replaces it"
            );
            None
        }
        // The pool is open: take in the top-ups and redemptions of the other
        // signers that share it, which reach this row only from chain.
        Ok(pool) if pool.owner == self_address => {
            let mut state = state;
            state.reconcile_with_chain(U256::from(pool.deposit), U256::from(pool.totalRedeemed));
            Some(state)
        }
        Ok(_) => Some(state),
        Err(error) => {
            tracing::warn!(
                pool_id = %state.pool_id,
                error = %error,
                "could not read the tracked buyer pool's status; reusing it"
            );
            Some(state)
        }
    }
}

/// Record `lane`'s on-chain watermark `(bytes, amount)` in the row before the
/// lane resumes from it ([`BuyerPoolState::seed_lane`]), and return the row
/// with it and the progress the lane resumes from.
///
/// The pool spend the refill and the run's funding read
/// ([`BuyerPoolState::pool_spend`]) then counts the watermark once. On an
/// adopted row it moves out of the redeemed spend no lane accounts for, so the
/// spend keeps counting the other lanes' redemptions as this lane advances. A
/// zero watermark has nothing to record. A row another run already moved past
/// the watermark wins: the lane resumes from the row's progress.
///
/// # Errors
///
/// The buyer store could not commit the seed, or the row is gone or names
/// another pool now (another run forgot or replaced it). Either way the build
/// fails and retries from a fresh read: going on would price the lane, or
/// escrow a refill, against a row the store no longer holds.
fn seed_lane(
    store: &dyn BuyerPoolStore,
    mut state: BuyerPoolState,
    lane: LaneKey,
    bytes: U256,
    amount: U256,
) -> anyhow::Result<(BuyerPoolState, U256, U256)> {
    if bytes.is_zero() && amount.is_zero() {
        return Ok((state, bytes, amount));
    }
    let pool_id = state.pool_id;
    let outcome = store
        .seed_progress(state.owner, pool_id, lane, bytes, amount)
        .with_context(|| {
            format!(
                "record the on-chain watermark of lane {} in pool {pool_id}",
                lane.provider
            )
        })?;
    match outcome {
        AdvanceOutcome::Advanced => {
            // The in-memory row has no record of the lane, so the seed takes.
            if let Err(err) = state.seed_lane(lane, bytes, amount) {
                tracing::debug!(error = %err, "lane seed did not advance the in-memory row");
            }
            Ok((state, bytes, amount))
        }
        AdvanceOutcome::Regressed(_) => {
            let row = store.get_by_pool_id(pool_id)?.ok_or_else(|| {
                anyhow::anyhow!("buyer row for pool {pool_id} vanished during a lane seed")
            })?;
            let progress = row.lane_progress(lane).ok_or_else(|| {
                anyhow::anyhow!(
                    "buyer row for pool {pool_id} lost lane {} during a seed",
                    lane.provider
                )
            })?;
            Ok((row, progress.last_bytes, progress.last_amount))
        }
        AdvanceOutcome::UnknownPool | AdvanceOutcome::PoolMismatch => Err(anyhow::anyhow!(
            "the buyer row for pool {pool_id} was forgotten or replaced while lane {} was built; \
             the build retries from the store",
            lane.provider
        )),
    }
}

/// The error for a pool with no unspent deposit whose wallet cannot fund a
/// top-up: [`NoAffordableSource`], which is fatal to the command even inside a
/// lane build. Nothing a retry or another provider does can pay for the fetch.
fn cannot_pay(state: &BuyerPoolState, shortfall: &str) -> anyhow::Error {
    anyhow::Error::new(NoAffordableSource {
        deposit: state.deposit,
    })
    .context(format!(
        "buyer pool {} has no unspent deposit, and the wallet cannot fund a top-up: \
         {shortfall}",
        state.pool_id
    ))
}

/// Record the pool `opened` bought in `store`, then resolve its escrow on the
/// run's handle. The deposit is escrowed on-chain, so a failed record leaves
/// it untracked (reconcile against the tx), held on the handle.
fn record_opened(
    store: &dyn BuyerPoolStore,
    opened: &OpenedPool,
    unrecorded: UnrecordedEscrow,
) -> anyhow::Result<()> {
    match store.record(&opened.state) {
        Ok(()) => {
            unrecorded.recorded();
            Ok(())
        }
        Err(e) => {
            Err(unrecorded.untracked(escrowed_but_untracked("buyer pool opened", opened.tx, e)))
        }
    }
}

/// Whether an [`escrow_top_up`] error is about an escrow the run holds: a
/// `topUp` that may still mine ([`TopUpUnconfirmed`]), or a refusal after an
/// untracked escrow ([`RefusedAfterUntracked`]). Either is fatal as it is, and no
/// wallet balance explains it.
fn escrow_held(err: &anyhow::Error) -> bool {
    err.downcast_ref::<TopUpUnconfirmed>().is_some()
        || err.downcast_ref::<RefusedAfterUntracked>().is_some()
}

/// Top `pool_id` up by `additional` from `owner`: ensure the pool's standing
/// USDC allowance covers it (unlimited under `max_approve`, else exactly
/// `additional`), since `topUp` pulls it via `transferFrom`, then send `topUp`
/// and read its receipt within [`RECEIPT_TIMEOUT`].
///
/// The proactive refill, under `open_lock`, and the funding recovery step,
/// under [`RecoveryGate`](crate::RecoveryGate)'s step lock, both escrow through
/// here. Those two locks do not exclude each other, so the call holds the
/// run's `topUp` lock in `funding` from its check to its outcome: a second
/// caller waits for the first `topUp` to resolve. A `topUp` that may have been
/// broadcast but is not known to have mined ([`TopUpUnconfirmed`]: a lapsed or
/// failed receipt read, a submit lost in transport, or a future dropped before
/// either) may still mine, so the run's [`UnconfirmedEscrows`] keeps it. Every
/// later call through the same handle refuses with it instead of escrowing a
/// second deposit, and the acquire loop classifies that as fatal to the
/// command ([`crate::Fault::Fatal`]). A `funding` whose handle holds an
/// untracked escrow ([`UnconfirmedEscrows::untracked`]) refuses too, with
/// [`RefusedAfterUntracked`]. Both refusals read the handle, so a later pass
/// on the same handle refuses the same way. A lane build's call refuses with
/// [`FetchEnded`] once `ended` fires: while it waits for the run's `topUp`
/// lock, before the allowance leg, and before each `topUp` send, a re-send
/// after a nonce collision too. A `topUp` already sent runs to its receipt.
///
/// Returns the [`ToppedUpPool`] with the [`UnrecordedEscrow`] the caller
/// resolves once its credit commits or fails.
///
/// # Errors
///
/// The run's handle holds an unconfirmed `topUp` or an untracked escrow,
/// `ended` fired before the `topUp` was sent ([`FetchEnded`]), or the
/// allowance or `topUp` leg fails.
#[allow(clippy::too_many_arguments)]
async fn escrow_top_up<P>(
    funding: &RunFunding,
    contract: &PaymentPool::PaymentPoolInstance<P>,
    token: Address,
    owner: Address,
    pool_id: PoolId,
    additional: U256,
    max_approve: bool,
    ended: Option<&CancellationToken>,
) -> anyhow::Result<(ToppedUpPool, UnrecordedEscrow)>
where
    P: alloy::providers::Provider + Clone,
{
    let _serial = lock_unless_ended(&funding.top_up_serial, ended).await?;
    if let Some(earlier) = funding.escrows.top_ups().first().copied() {
        return Err(anyhow::Error::new(earlier)
            .context("refusing a second topUp while an earlier one is unconfirmed"));
    }
    funding.refuse_after_untracked()?;
    ensure_allowance(
        contract.provider(),
        token,
        owner,
        *contract.address(),
        if max_approve { None } else { Some(additional) },
    )
    .await?;
    top_up_unless_ended(
        contract,
        owner,
        pool_id,
        additional,
        Some(&funding.escrows),
        Some(RECEIPT_TIMEOUT),
        ended,
    )
    .await
}

/// Restore `state`'s remaining deposit (`deposit - spent`) to `working_deposit`
/// once it falls below the low water — see [`refill_amount`] — and return the
/// row to build the lane on.
///
/// `spent` is the pool-wide spend from [`BuyerPoolState::pool_spend`]. The refill is an
/// optimisation, so a wallet that holds too little USDC for it does not fail
/// the lane while the pool can still pay (and ends the command, as
/// [`NoAffordableSource`], once it cannot): the shortfall is logged at WARN,
/// recorded on `funding` for the run's closing `warning:` line, and no later
/// lane build of the run tries the refill again. Any other failure of the
/// allowance or `topUp` leg fails the lane build, which the acquire loop
/// retries with backoff. A `topUp` that may have been broadcast but returned no
/// receipt ([`TopUpUnconfirmed`]) may have escrowed, and once `topUp` returns a
/// receipt the escrow has moved, so a credit that fails, or a re-read that
/// finds the row gone ([`escrowed_but_untracked`]), is fatal: the acquire loop
/// ends the command instead of retrying, because a retry escrows again. The
/// run's handle holds that escrow ([`UnrecordedEscrow::untracked`]). A refill
/// after an earlier `topUp` on the handle went unconfirmed sends nothing and
/// fails with that held [`TopUpUnconfirmed`] ([`escrow_top_up`]), and one
/// after an untracked escrow on the handle fails with
/// [`RefusedAfterUntracked`], each fatal the same way. A refill that `ended`
/// finds fired sends nothing and fails with [`FetchEnded`]: the lane would
/// never run.
#[allow(clippy::too_many_arguments)]
async fn refill_if_low<P>(
    store: &dyn BuyerPoolStore,
    contract: &PaymentPool::PaymentPoolInstance<P>,
    rpc: &P,
    funding: &RunFunding,
    state: BuyerPoolState,
    self_address: Address,
    spent: U256,
    working_deposit: U256,
    max_approve: bool,
    ended: &CancellationToken,
) -> anyhow::Result<BuyerPoolState>
where
    P: alloy::providers::Provider + Clone,
{
    let low_water = working_deposit / U256::from(LOW_WATER_DIVISOR);
    let additional = refill_amount(state.deposit, spent, working_deposit, low_water);
    if additional.is_zero() {
        return Ok(state);
    }
    let remaining = state.deposit.saturating_sub(spent);
    if let Some(shortfall) = funding.shortfall() {
        // The wallet could not fund a top-up earlier in this run; asking again
        // costs an allowance read and a reverting `topUp` estimate per lane.
        if remaining.is_zero() {
            return Err(cannot_pay(&state, &shortfall));
        }
        return Ok(state);
    }
    refuse_after_end(Some(ended))?;
    tracing::info!(
        "buyer pool {} low on deposit ({remaining} µUSDC remaining of {} deposited); \
         topping up {additional} µUSDC",
        state.pool_id,
        state.deposit,
    );
    let escrowed = escrow_top_up(
        funding,
        contract,
        state.token,
        self_address,
        state.pool_id,
        additional,
        max_approve,
        Some(ended),
    )
    .await;
    let err = match escrowed {
        Ok((ToppedUpPool { credited, tx, .. }, unrecorded)) => {
            // The USDC is escrowed the moment `topUp` mines. A local credit that
            // does not land leaves the deposit untracked, and continuing would
            // fetch on a `state.deposit` that understates the chain — so the
            // low-water check re-fires on every later fetch while nobody
            // reconciles the escrow. No bytes have been paid for on *this* entry
            // yet — a bundle pull reaches this once per entry, through
            // `open_or_reuse_pool`, so earlier entries may already be paid for and
            // written — and only the escrow moved, so failing here strands nothing
            // in flight. The funding recovery step takes the same disposition
            // (`PoolFunder::recover`).
            let effect = topped_up_effect(state.pool_id, credited);
            if let Err(err) = grade_deposit_credit(
                store.add_deposit(self_address, state.pool_id, credited),
                &effect,
                tx,
            ) {
                return Err(unrecorded.untracked(err));
            }
            // The credit committed, so the row must be there. A `None` here
            // means it vanished between the write and this read — the local
            // record did not survive, which is the same untracked-escrow
            // condition, not something to paper over with the pre-top-up
            // snapshot. A read that fails leaves the committed credit
            // standing.
            return match store.get_by_pool_id(state.pool_id) {
                Ok(Some(row)) => {
                    unrecorded.recorded();
                    Ok(row)
                }
                Ok(None) => Err(unrecorded.untracked(escrowed_but_untracked(
                    &effect,
                    tx,
                    "the credited row vanished",
                ))),
                Err(err) => {
                    unrecorded.recorded();
                    Err(err.into())
                }
            };
        }
        Err(err) => err,
    };
    if err.downcast_ref::<FetchEnded>().is_some() {
        tracing::info!(
            "buyer pool {} not topped up: the fetch ended before the topUp was sent",
            state.pool_id,
        );
        return Err(err);
    }
    // A refusal names its own cause; no wallet balance explains it.
    if escrow_held(&err) {
        return Err(err);
    }
    let Some(shortfall) = funding
        .check_wallet(
            rpc,
            state.token,
            self_address,
            state.pool_id,
            additional,
            &err,
        )
        .await
    else {
        return Err(err);
    };
    if remaining.is_zero() {
        return Err(cannot_pay(&state, &shortfall));
    }
    // `check_wallet` already logged the shortfall at WARN.
    tracing::info!(
        pool_id = %state.pool_id,
        %remaining,
        "continuing on the pool's remaining deposit (µUSDC) without a top-up"
    );
    Ok(state)
}

/// Reuse the caller's live pool (resuming `provider`'s lane watermark), or open
/// and persist a new one. A reused pool whose pool-wide remaining deposit has
/// run low gets an on-chain `topUp` before it is returned; a wallet too short
/// of USDC for it still returns the pool while it can pay — see
/// [`refill_if_low`]. The lane then joins `funding`, the run's view of the
/// pool's spend outside its lanes ([`RunFunding::join_lane`]). There is no
/// pool expiry (ADR 003), so
/// there is no replace-on-expiry branch: the same pool is reused for the
/// caller's whole lifetime, across every provider.
///
/// `deployment` is the chain and `PaymentPool` that `contract` talks to: a
/// tracked row is reused only on it, a new or adopted row carries it, and the
/// voucher EIP-712 domain derives from it. `deposit` is how the pool is
/// funded: the working deposit a new pool opens at and a low pool refills
/// toward, the allowance mode, and whether a lost row adopts from chain.
/// `ended` fires once the fetch the lane serves has ended.
///
/// # Errors
///
/// A chain read, allowance, `openPool` or `topUp` fails, the buyer store
/// cannot record a pool the chain escrowed (fatal: the escrow is untracked),
/// the run's handle holds an untracked escrow, so no `openPool` or `topUp`
/// is sent ([`RefusedAfterUntracked`], fatal), or `ended` fired before the
/// `openPool` or `topUp` was sent ([`FetchEnded`]).
#[allow(clippy::too_many_arguments)]
pub(crate) async fn open_or_reuse_pool<P>(
    store: &dyn BuyerPoolStore,
    contract: &PaymentPool::PaymentPoolInstance<P>,
    rpc: &P,
    funding: &RunFunding,
    payer: &OwnedPayer,
    provider: Address,
    deployment: Deployment,
    deposit: DepositPolicy,
    ended: &CancellationToken,
) -> anyhow::Result<(PoolContext, LaneSpend)>
where
    P: alloy::providers::Provider + Clone,
{
    let self_address = payer.owner().address();
    let DepositPolicy {
        working_deposit,
        max_approve,
        adoption,
    } = deposit;
    let payment_pool_addr = deployment.payment_pool;
    let tracked =
        pool_to_reuse(store, contract, funding, self_address, deployment, adoption).await?;
    if let Some(state) = tracked {
        let lane = LaneKey {
            pool_id: state.pool_id,
            signer: payer.lane().address(),
            provider,
        };
        let recorded = state.lane_progress(lane).is_some();
        let from_chain = !state.redeemed_elsewhere().is_zero();
        let (state, prior_bytes, prior_amount) = if let Some(p) = state.lane_progress(lane) {
            (state, p.last_bytes, p.last_amount)
        } else {
            // No local record of this lane — always so for a pool just adopted, and
            // for a tracked pool's first contact with a provider. The chain may
            // still hold a watermark for it, and a voucher at or below that
            // watermark redeems nothing — so resuming from zero would stream bytes
            // the provider can never cash. Resume from the chain.
            let (bytes, amount) = lane_watermark(contract, lane).await?;
            seed_lane(store, state, lane, bytes, amount)?
        };

        // Auto-refill a live pool whose remaining deposit has run low, so a
        // sustained series of fetches isn't stranded by a spent-down deposit.
        let spent = state.pool_spend();
        let state = refill_if_low(
            store,
            contract,
            rpc,
            funding,
            state,
            self_address,
            spent,
            working_deposit,
            max_approve,
            ended,
        )
        .await?;
        let lane_spend = LaneSpend {
            outside: spent.saturating_sub(prior_amount),
            prior: prior_amount,
            recorded,
            from_chain,
        };
        return self_owned_lane_ctx(
            &state,
            payer,
            &deployment.voucher_domain(),
            provider,
            prior_bytes,
            prior_amount,
        )
        .map(|ctx| (ctx, lane_spend));
    }

    funding.refuse_after_untracked()?;
    refuse_after_end(Some(ended))?;
    // Authoritative USDC token for the pool, from the contract itself.
    let token = contract
        .usdc()
        .call()
        .await
        .map_err(|e| anyhow::Error::new(e).context("read PaymentPool.usdc()"))?;
    // Escrowed as configured — there is no on-chain floor to clamp up to,
    // only a non-zero requirement (`openPool` reverts `ZeroAmount`). The shared
    // pool is fully withdrawable, so the buyer opens at the working deposit
    // directly rather than a smaller first-contact lock.
    let deposit = working_deposit;
    // `max_approve` opts into an unlimited standing allowance; otherwise approve
    // exactly the deposit being escrowed.
    let approve_amount = if max_approve { None } else { Some(deposit) };
    ensure_allowance(rpc, token, self_address, payment_pool_addr, approve_amount).await?;
    refuse_after_end(Some(ended))?;
    let (opened, unrecorded) = open_pool(
        contract,
        deployment,
        token,
        self_address,
        deposit,
        Some(funding.escrows()),
        Some(RECEIPT_TIMEOUT),
    )
    .await?;
    record_opened(store, &opened, unrecorded)?;
    // A fresh pool has spent nothing on any lane.
    let spend = LaneSpend {
        outside: U256::ZERO,
        prior: U256::ZERO,
        recorded: false,
        from_chain: false,
    };
    let ctx = self_owned_lane_ctx(
        &opened.state,
        payer,
        &deployment.voucher_domain(),
        provider,
        U256::ZERO,
        U256::ZERO,
    )?;
    Ok((ctx, spend))
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::indexing_slicing)]
mod tests;

/// `open_or_reuse_pool` against a mocked `PaymentPool`: what a client does when
/// its store has no row for a pool the chain says it owns.
#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::indexing_slicing)]
mod adoption_tests;
