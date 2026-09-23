//! Buyer-side `PaymentPool` open kernel, shared by the node's node-to-node
//! cache-miss buyer (#744) and the CLI (`decdn fetch`, #940).
//!
//! The genuinely duplication-prone part — the `openPool` transaction, the
//! authoritative `PoolOpened`-from-receipt decode, the self-owned capability the
//! single-user buyer signs for its own key, and the [`BuyerPoolState`] /
//! [`PoolContext`] construction for a self-owned lane
//! ([`self_owned_lane_ctx`](crate::buyer_pool::self_owned_lane_ctx)) —
//! lives here, with the rule that picks a lane's progress write
//! ([`ProgressWrite`](crate::buyer_pool::ProgressWrite)). Pool *reuse* is a thin
//! composition over [`decdn_incentive::BuyerPoolStore::get_by_owner`] that each
//! caller does directly: a one-shot CLI fetch needs neither the node service's
//! per-owner concurrency guard nor its background reclaim/reconcile machinery.

use std::sync::Arc;
use std::time::Duration;

use alloy::dyn_abi::Eip712Domain;
use alloy::primitives::{Address, B256, TxHash, U256};
use alloy::providers::Provider;
use alloy::signers::local::PrivateKeySigner;
use anyhow::{Context, Result};
use decdn_incentive::erc20::Erc20;
use decdn_incentive::payment_pool::{PaymentPool, to_pool_u64};
use decdn_incentive::{
    AdvanceOutcome, BuyerLaneProgress, BuyerPoolState, BuyerPoolStore, Capability, DepositOutcome,
    LaneKey, PoolId, PoolOpenFailureReason, SignedCapability, StoreError,
};
use tracing::{debug, error, info, warn};

use crate::{PoolContext, VoucherProgress};

/// Re-approve the `PaymentPool` spender for the *unlimited* case when the
/// standing USDC allowance has fallen below this floor. Half of `U256::MAX` so
/// one max approval covers effectively unlimited deposits and a re-run with the
/// approval already in place skips the redundant `approve`, while a
/// never-approved wallet (allowance `0`) trips it. Only consulted for the
/// unlimited mode; the exact mode compares against the requested amount instead
/// (see [`approve_decision`]).
fn approval_floor() -> U256 {
    U256::MAX >> 1
}

/// Decide, purely, what `approve` value (if any) to issue given the wallet's
/// `current` standing allowance and the requested approval `amount`:
///
/// - `amount == None` (unlimited): skip when `current >= approval_floor()`,
///   otherwise approve `U256::MAX` — the node/operator posture that avoids
///   re-approve churn.
/// - `amount == Some(needed)` (exact, the client default): skip when
///   `current >= needed`, otherwise approve exactly `needed`. Because the skip
///   threshold *is* `needed`, a wallet that previously granted an unlimited
///   (`U256::MAX`) allowance always skips — switching a client to exact never
///   re-approves the allowance *downward*.
fn approve_decision(current: U256, amount: Option<U256>) -> Option<U256> {
    let (threshold, approve_value) = match amount {
        None => (approval_floor(), U256::MAX),
        Some(needed) => (needed, needed),
    };
    if current >= threshold {
        None
    } else {
        Some(approve_value)
    }
}

/// Bound the wait for the one-time USDC `approve` receipt so a stuck or
/// underpriced tx can't wedge the buyer path (#1109). Sized well above a normal
/// inclusion window but short enough that the worst case is a few minutes. On
/// timeout the broadcast tx may still mine later; the allowance read at the top
/// of the next run makes the re-approve idempotent, so no funds are stranded.
const APPROVE_RECEIPT_TIMEOUT: Duration = Duration::from_mins(3);

/// A freshly opened buyer pool: the persistable [`BuyerPoolState`], a
/// ready-to-sign [`PoolContext`], the self-owned [`SignedCapability`] the buyer
/// registers on its first redemption, and the open transaction hash. The deposit
/// is on-chain the moment `openPool` mines, so a caller whose subsequent
/// `store.record` fails names that tx for manual reconciliation — an error in the
/// CLI, an `error!` in the daemon.
#[derive(Debug)]
#[must_use = "the open tx is the only handle to an escrowed-but-untracked deposit"]
pub struct OpenedPool {
    /// Persist this via `BuyerPoolStore::record`.
    pub state: BuyerPoolState,
    /// Use this to sign vouchers for the new pool. The fetch target's provider
    /// address and the lane priors are pinned per-pull via
    /// [`PoolContext::with_provider`].
    pub ctx: PoolContext,
    /// The owner-signed grant delegating spend on this pool to the buyer's own
    /// signing key, regenerated per open and never stored.
    pub capability: SignedCapability,
    /// The `openPool` transaction hash.
    pub tx: TxHash,
}

/// The outcome of a mined [`top_up`]: what the contract actually credited, and
/// the transaction that credited it.
///
/// The tx hash rides along for the same reason [`OpenedPool::tx`] does. The
/// funds are escrowed the moment this returns, so a caller whose local credit
/// then fails has to name the transaction an operator reconciles against — see
/// [`escrowed_but_untracked`].
#[derive(Debug, Clone, Copy)]
#[must_use = "the top-up tx is the only handle to an escrowed-but-untracked deposit"]
pub struct ToppedUpPool {
    /// The amount the contract credited, read back from the receipt.
    pub credited: U256,
    /// The `topUp` transaction hash.
    pub tx: TxHash,
}

/// Build the error for "the on-chain effect landed, the local record did not".
///
/// The funds have already moved when this is reached, so a non-zero exit is the
/// only outcome that guarantees anyone reconciles: a caller that logs and
/// returns success is indistinguishable from one that did the work, and a shell
/// wrapper (`if decdn pool top-up …; then mark_funded; fi`) records the money as
/// tracked when it is not.
///
/// `effect` names what landed on-chain in the operator's vocabulary, as a
/// past-tense clause that reads correctly with ` on-chain` appended
/// (`buyer pool opened`, `pool 0x… topped up by 5 µUSDC`). `tx` is the handle to
/// reconcile against, and `cause` is the local failure.
///
/// The message steers the operator away from the obvious response to a non-zero
/// exit: re-running an escrow that already landed escrows a second time.
#[must_use]
pub fn escrowed_but_untracked(
    effect: &str,
    tx: TxHash,
    cause: impl std::fmt::Display,
) -> anyhow::Error {
    anyhow::anyhow!(
        "{effect} on-chain (tx {tx}) but the local record did not survive; the deposit is \
         escrowed but untracked — reconcile against the tx before retrying, because a retry \
         escrows again: {cause}"
    )
}

/// The `effect` clause for a landed `topUp`, in the past-tense shape
/// [`escrowed_but_untracked`] expects.
///
/// Every caller that grades a top-up credit names the same effect, and the
/// amount and pool are what an operator reconciles the escrow against — so the
/// wording lives here rather than being written out per call site.
#[must_use]
pub fn topped_up_effect(pool_id: B256, credited: U256) -> String {
    format!("pool {pool_id} topped up by {credited} µUSDC")
}

/// Grade the local credit that follows a mined [`top_up`], turning every
/// not-credited outcome into an [`escrowed_but_untracked`] error.
///
/// [`decdn_incentive::BuyerPoolStore::add_deposit`] splits its failures across two channels by
/// design: a backend or codec fault is the `Err`, while a committed-row
/// mismatch (the row vanished, or now tracks a different pool) is a non-`Added`
/// `Ok`. For a caller standing over freshly escrowed USDC the distinction does
/// not change the disposition — either way the deposit moved on-chain and the
/// local row is short by `credited` — so both collapse here. `DepositOutcome`
/// is `#[must_use]` for exactly this reason: a dropped `PoolMismatch` looks
/// identical to a successful credit.
///
/// Returns the new committed deposit on success.
///
/// # Errors
///
/// Errors on any outcome other than [`DepositOutcome::Added`].
pub fn grade_deposit_credit(
    outcome: Result<DepositOutcome, StoreError>,
    effect: &str,
    tx: TxHash,
) -> Result<U256> {
    match outcome {
        Ok(DepositOutcome::Added(new_deposit)) => Ok(new_deposit),
        Ok(other) => Err(escrowed_but_untracked(
            effect,
            tx,
            format!("the local record was not updated: {other:?}"),
        )),
        Err(e) => Err(escrowed_but_untracked(effect, tx, e)),
    }
}

/// Sign a self-owned [`SignedCapability`]: the pool owner delegates spend on
/// `pool_id` to its OWN signing key, up to `spending_cap`, until `expiry`. This
/// is the single-user case — the owner and the voucher signer are the same key —
/// so the capability is regenerated from the key per connection and never stored.
///
/// The serving node registers it on the buyer's first on-chain redemption
/// (`redeemMany`'s `CapabilityReg`), then accepts vouchers from this signer up to
/// the cap (ADR 003 §Capability delegation).
///
/// # Errors
///
/// Propagates a signing error from the owner signer.
pub fn issue_self_capability(
    owner_signer: &PrivateKeySigner,
    pool_id: B256,
    spending_cap: u64,
    expiry: u64,
    voucher_domain: &Eip712Domain,
) -> Result<SignedCapability> {
    Capability {
        signer: owner_signer.address(),
        spending_cap,
        pool_id,
        expiry,
    }
    .sign(owner_signer, voucher_domain)
    .map_err(|e| anyhow::anyhow!("sign self capability: {e}"))
}

/// The [`PoolContext`] one lane of a self-owned pool signs under: `signer` pays
/// `provider` from `state`'s pool, resuming at the lane's prior cumulative
/// `(prior_bytes, prior_amount)`.
///
/// The context carries a freshly issued self-owned capability. The capability
/// is node-agnostic (valid at every provider this pool pays) and cheap to
/// regenerate, so a fresh open and a reused pool both present one without a
/// stored copy. An upstream registers the signer from it on the first on-chain
/// redemption.
///
/// The cap is [`SELF_CAPABILITY_CAP`], not the deposit. The delegate IS the
/// owner, so the pool deposit is the real spending bound. A cap at the deposit
/// would freeze the on-chain cap at the opening deposit (`_registerCapability` is
/// idempotent past the first redemption) and reject spend past a later `topUp`.
/// It is not `U256::MAX` either: the pool's `spendingCap` is a `uint64`, so a
/// wider cap hashes to a word the contract cannot reconstruct and every
/// redemption on the lane reverts.
///
/// The provider pin is mandatory: a voucher signed with `provider ==
/// Address::ZERO` fails at signing, so a context left at
/// [`PoolContext::for_pool`]'s zero provider cannot pay.
///
/// # Errors
///
/// Propagates a signing error from `signer` while issuing the capability.
pub fn self_owned_lane_ctx(
    state: &BuyerPoolState,
    signer: &Arc<PrivateKeySigner>,
    voucher_domain: &Eip712Domain,
    provider: Address,
    prior_bytes: U256,
    prior_amount: U256,
) -> Result<PoolContext> {
    let capability = issue_self_capability(
        signer.as_ref(),
        state.pool_id,
        SELF_CAPABILITY_CAP,
        SELF_CAPABILITY_EXPIRY,
        voucher_domain,
    )?;
    Ok(
        PoolContext::for_pool(state, Arc::clone(signer), voucher_domain.clone())
            .with_provider(provider, prior_bytes, prior_amount)
            .with_capability(capability),
    )
}

/// The buyer-store write that records one lane's [`VoucherProgress`], so a later
/// reuse or a restart resumes the lane at the right cumulative `bytes` /
/// `amount`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProgressWrite {
    /// The lane's ledger rebased DOWN to the upstream's authenticated watermark
    /// `anchor` (`PoolLedger::rebase`), and no write has recorded it yet. The
    /// record is overwritten with `anchor` and then advanced to `totals`. A
    /// monotone advance refuses a lower total, and the next reuse would then sign
    /// from the anchor the upstream already refused.
    Rebase {
        /// The upstream watermark the ledger rebased to.
        anchor: BuyerLaneProgress,
        /// The lane's cumulative totals after the rebase.
        totals: BuyerLaneProgress,
    },
    /// A monotone advance to `totals`.
    Advance {
        /// The lane's new cumulative totals.
        totals: BuyerLaneProgress,
    },
}

impl ProgressWrite {
    /// The write `progress` calls for, or `None` when it neither advanced past
    /// its seed nor carries a rebase anchor. A pending anchor is written even
    /// when the totals did not advance: the write exists to move the record
    /// down.
    #[must_use]
    pub fn of(progress: &VoucherProgress) -> Option<Self> {
        let (last_bytes, last_amount) = progress.totals();
        let totals = BuyerLaneProgress {
            last_amount,
            last_bytes,
        };
        match progress.rebase_anchor() {
            Some(anchor) => Some(Self::Rebase {
                anchor: BuyerLaneProgress {
                    last_amount: anchor.amount,
                    last_bytes: anchor.bytes,
                },
                totals,
            }),
            None => progress.advanced().map(|_| Self::Advance { totals }),
        }
    }

    /// The lane's cumulative totals after this write.
    #[must_use]
    pub const fn totals(&self) -> BuyerLaneProgress {
        match self {
            Self::Rebase { totals, .. } | Self::Advance { totals } => *totals,
        }
    }

    /// Apply this write to `lane` inside `owner`'s pool `pool_id`.
    ///
    /// # Errors
    ///
    /// Returns a [`StoreError`] only on a backend or codec fault. A write the
    /// store declines (an unknown or replaced pool, a regression) is the `Ok`
    /// [`AdvanceOutcome`].
    pub fn apply<S: BuyerPoolStore + ?Sized>(
        &self,
        store: &S,
        owner: Address,
        pool_id: PoolId,
        lane: LaneKey,
    ) -> Result<AdvanceOutcome, StoreError> {
        match *self {
            Self::Rebase { anchor, totals } => {
                store.rebase_progress(owner, pool_id, lane, anchor, totals)
            }
            Self::Advance { totals } => {
                store.advance_progress(owner, pool_id, lane, totals.last_bytes, totals.last_amount)
            }
        }
    }
}

/// Ensure `owner` holds a sufficient USDC allowance for the `PaymentPool`
/// `spender`, issuing at most one `approve` when the current allowance is below
/// what `amount` requires. `amount == None` grants an unlimited (`U256::MAX`)
/// allowance — the node/operator posture that avoids re-approve churn;
/// `amount == Some(deposit)` grants exactly `deposit` — the client default that
/// keeps the standing spend authority scoped to the deposit being escrowed. See
/// `approve_decision` for the skip logic. Idempotent across runs — a wallet
/// already at or above the required allowance skips the tx.
///
/// # Errors
///
/// Fails on the allowance read, the `approve` submission, or a reverted approve.
pub async fn ensure_allowance<P: Provider + Clone>(
    provider: &P,
    token: Address,
    owner: Address,
    spender: Address,
    amount: Option<U256>,
) -> Result<()> {
    let erc20 = Erc20::new(token, provider.clone());
    let current = erc20
        .allowance(owner, spender)
        .call()
        .await
        .context("read USDC allowance")?;
    let Some(approve_value) = approve_decision(current, amount) else {
        debug!(%current, "USDC allowance already sufficient; skipping approve");
        return Ok(());
    };
    let pending = erc20
        .approve(spender, approve_value)
        .send()
        .await
        .context("submit USDC approve")?;
    // Capture the hash before `get_receipt` consumes `pending`, so a timeout
    // error names the broadcast tx an operator needs to look up (it stays in the
    // mempool and may still mine after we give up).
    let approve_tx = *pending.tx_hash();
    let receipt = tokio::time::timeout(APPROVE_RECEIPT_TIMEOUT, pending.get_receipt())
        .await
        .map_err(|_| {
            anyhow::anyhow!(
                "USDC approve receipt timed out after {APPROVE_RECEIPT_TIMEOUT:?} \
                 (tx {approve_tx}; may still mine later)"
            )
        })?
        .context("await USDC approve receipt")?;
    if !receipt.status() {
        anyhow::bail!("USDC approve transaction reverted");
    }
    info!(
        %token,
        %spender,
        %approve_value,
        unlimited = amount.is_none(),
        "issued USDC approval for PaymentPool deposits"
    );
    Ok(())
}

/// Open a fresh `PaymentPool` deposit for `owner`, escrowing `deposit` USDC, and
/// decode the authoritative `poolId` from the receipt's own `PoolOpened` event.
///
/// `openPool` names no provider and no signer — a pool is bound to no payee at
/// open, and fans out to many `(signer, provider)` lanes off-chain (ADR 003).
/// The returned [`OpenedPool`] carries a self-owned capability the buyer signs
/// for its own key (`spending_cap == credited deposit`, no expiry), which the
/// serving node registers on the first redemption.
///
/// Decoding from the receipt (rather than a follow-up `getPool`) is atomic with
/// the open: a mined tx guarantees the event, so the caller can always persist —
/// a transient read failure can never leave the deposit orphaned. The event is
/// filtered on `owner == owner` to confirm we decoded our own open.
///
/// `deposit` is the final escrow amount — the caller applies any
/// `max(hint, default)` clamping before calling. The returned [`OpenedPool`] is
/// **not yet persisted**: the caller records [`OpenedPool::state`] in its
/// `BuyerPoolStore`, and on a record failure should log [`OpenedPool::tx`].
///
/// # Errors
///
/// Fails on `openPool` submit/receipt, a reverted tx, a missing `PoolOpened`
/// event in the receipt logs, or a capability-signing error. The classified
/// failure legs attach a [`PoolOpenFailureReason`] into the `anyhow` error chain
/// (recover it with `err.downcast_ref::<PoolOpenFailureReason>()`) so a caller
/// can bump the matching `decdn_pool_open_failures_{reason}_total` counter: a
/// deterministic revert (with ABI revert data, decoded against the
/// insufficient-deposit error selectors) is split from a transport/RPC fault (no
/// revert data) and a mined on-chain revert. The missing-`PoolOpened` leg carries
/// no reason — the deposit is escrowed but untracked, so it surfaces as an
/// unclassified error for manual reconciliation.
///
/// # The receipt wait is deliberately UNBOUNDED
///
/// `ensure_allowance`'s `approve` bounds its receipt wait (idempotent — giving up
/// costs nothing). `openPool` and `top_up` **escrow funds**: giving up on the
/// receipt does not cancel the tx, so a caller that then believes no open is in
/// flight would escrow a second deposit against a pool the first tx is still
/// going to mine. So while an `openPool` is outstanding, the only safe thing is
/// to keep waiting; the node calls this inside a detached task that holds the
/// owner's open slot for exactly as long as this future runs (#1143).
pub async fn open_pool<P: Provider + Clone>(
    contract: &PaymentPool::PaymentPoolInstance<P>,
    signer: Arc<PrivateKeySigner>,
    voucher_domain: &Eip712Domain,
    token: Address,
    owner: Address,
    deposit: U256,
) -> Result<OpenedPool> {
    let pending = match contract
        .openPool(to_pool_u64(deposit, "deposit")?)
        .send()
        .await
    {
        Ok(pending) => pending,
        Err(err) => {
            // A deterministic revert (a zero deposit, USDC balance/allowance too
            // low) is caught at gas estimation, so it surfaces here with ABI
            // revert data attached; a send error *without* revert data is a
            // transport/RPC fault.
            let reason = PoolOpenFailureReason::classify_revert_data(err.as_revert_data().as_ref());
            return Err(anyhow::Error::new(err))
                .context("submit openPool")
                .context(reason);
        }
    };
    // Unbounded by design — see the `# The receipt wait is deliberately UNBOUNDED`
    // section above.
    let receipt = pending.get_receipt().await.map_err(|err| {
        anyhow::Error::new(err)
            .context("await openPool receipt")
            .context(PoolOpenFailureReason::RpcError)
    })?;
    if !receipt.status() {
        // A mined revert: the reason is not recoverable from the receipt (no
        // trace), so it folds into `ContractRevert`.
        return Err(anyhow::anyhow!(
            "openPool reverted (owner {owner}, deposit {deposit}); check USDC balance/allowance"
        )
        .context(PoolOpenFailureReason::ContractRevert));
    }
    let tx = receipt.transaction_hash;

    let Some(opened) = receipt
        .inner
        .logs()
        .iter()
        .filter_map(|log| log.log_decode::<PaymentPool::PoolOpened>().ok())
        .map(|decoded| decoded.inner.data)
        .find(|ev| ev.owner == owner)
    else {
        // The deposit is escrowed on-chain and we cannot name the pool it bought.
        // Log it HERE rather than leaving it to the caller: the node's caller is a
        // detached task whose `Err` nobody may be waiting on (#1143).
        error!(
            %tx,
            %owner,
            %deposit,
            "openPool mined but its PoolOpened event is missing from the receipt logs; \
             the deposit is escrowed on-chain but UNTRACKED — reconcile manually against the tx"
        );
        anyhow::bail!(
            "PoolOpened event for owner {owner} not found in openPool receipt logs (tx {tx}); \
             the deposit is escrowed on-chain but untracked — reconcile manually"
        );
    };
    let pool_id = opened.poolId;
    // Record what the contract CREDITED, not what we asked it to transfer.
    // `openPool` sets the pool deposit to the measured balance delta and emits
    // that in `PoolOpened.deposit`, precisely so a fee-on-transfer token cannot
    // over-state a pool against the shared USDC balance.
    let credited = opened.deposit;
    let state = BuyerPoolState::new(pool_id, *contract.address(), owner, token, credited);
    // A self-owned capability delegates spend to the owner's OWN key, so the cap
    // bounds nothing a delegated capability would: the pool deposit is already
    // the real spending bound (redemption pays min(desired, cap-spent,
    // remaining), and `remaining` is the pool balance). Leave it at the widest
    // cap the pool's `uint64` field can hold, so a later `topUp` beyond the
    // opening deposit is redeemable too.
    let capability = issue_self_capability(
        signer.as_ref(),
        pool_id,
        SELF_CAPABILITY_CAP,
        SELF_CAPABILITY_EXPIRY,
        voucher_domain,
    )?;
    let ctx = PoolContext::for_pool(&state, signer, voucher_domain.clone());
    info!(
        %owner,
        %pool_id,
        requested = %deposit,
        credited = %credited,
        "opened buyer payment pool"
    );
    Ok(OpenedPool {
        state,
        ctx,
        capability,
        tx,
    })
}

/// Expiry stamped on the self-owned capability [`open_pool`] signs. The
/// single-user buyer owns both keys, so there is no delegation to time-box —
/// `u64::MAX` means "never expires", and the pool's own grace-window close is the
/// only lifecycle gate (there is no pool expiry, ADR 003).
const SELF_CAPABILITY_EXPIRY: u64 = u64::MAX;

/// Cap stamped on a self-owned capability — the widest value the pool's
/// `uint64 spendingCap` can hold, which is the effective "no
/// delegated ceiling" for an owner spending against its own pool. The real
/// bound is the deposit: redemption pays `min(desired, cap - spent, remaining)`.
/// A `u64` by type, matching the contract field exactly, so it can never sign a
/// cap the narrower on-chain field cannot reconstruct.
pub const SELF_CAPABILITY_CAP: u64 = u64::MAX;

/// Typed marker attached to a `top_up` submit error whose revert is an
/// `ERC20InsufficientAllowance` shortfall: the `PaymentPool`'s standing USDC
/// allowance is below the transfer amount. The node's `fund_pool` downcasts
/// on it to run a just-in-time `approve` and retry once; every other revert
/// stays terminal. CLI callers never downcast it, so it is invisible to them.
#[derive(Debug, Clone, Copy)]
pub struct AllowanceShortfall;

impl std::fmt::Display for AllowanceShortfall {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "topUp reverted: PaymentPool USDC allowance is below the transfer amount"
        )
    }
}

impl std::error::Error for AllowanceShortfall {}

/// How many times [`top_up`] re-sends a submit rejected as a nonce collision.
const TOPUP_NONCE_RETRIES: u32 = 3;

/// How long [`top_up`] waits before re-sending after a nonce collision: long
/// enough for the colliding transaction to reach the pending pool, so the next
/// pending-nonce read skips past it.
const TOPUP_NONCE_BACKOFF: Duration = Duration::from_millis(250);

/// Add `additional` USDC to the buyer pool `pool_id` on-chain and return the
/// credited amount and the mining transaction as a [`ToppedUpPool`] — the shared
/// mechanism behind the node's cache-miss buyer (#744) and the CLI fetch buyer's
/// auto-refill (#1103). `topUp` does not extend any lifecycle deadline; the caller
/// credits `credited` into its local [`BuyerPoolState`] via `add_deposit`, and
/// names `tx` if that credit fails — see [`escrowed_but_untracked`].
///
/// The amount credited is read back from `PoolToppedUp.additionalDeposit`, not
/// assumed to equal `additional`: the contract credits a measured balance delta,
/// so the two differ under a fee-on-transfer token and the local row must not
/// over-state the on-chain deposit. If the event is absent (an ABI skew), the
/// requested `additional` is returned as the best available estimate and the
/// fallback is logged — an over-stated local row is the mirror of the hazard
/// [`escrowed_but_untracked`] guards, so it must not be reached silently.
///
/// A submit rejected as a same-account nonce collision
/// ([`decdn_incentive::tx::is_nonce_collision`]) broadcast nothing, so it is
/// re-sent up to three more times, a quarter second apart.
/// A node signs its sellers' redemptions and its buyer top-ups with one key, so a
/// top-up can lose its nonce to a redemption sent at the same moment.
///
/// # Errors
///
/// Errors if the `topUp` transaction fails (submit, revert, or receipt).
pub async fn top_up<P: Provider + Clone>(
    contract: &PaymentPool::PaymentPoolInstance<P>,
    pool_id: B256,
    additional: U256,
) -> Result<ToppedUpPool> {
    let amount = to_pool_u64(additional, "top-up")?;
    let mut nonce_retries = 0u32;
    let pending = loop {
        let err = match contract.topUp(pool_id, amount).send().await {
            Ok(pending) => break pending,
            Err(err) => err,
        };
        if nonce_retries < TOPUP_NONCE_RETRIES && decdn_incentive::tx::is_nonce_collision(&err) {
            nonce_retries = nonce_retries.saturating_add(1);
            debug!(
                %pool_id, nonce_retries,
                "topUp lost its nonce to a concurrent transaction; re-sending"
            );
            tokio::time::sleep(TOPUP_NONCE_BACKOFF).await;
            continue;
        }
        // A deterministic allowance shortfall is caught at gas estimation and
        // surfaces here with ABI revert data attached. Match it so the node can
        // re-`approve` and retry once; every other revert stays terminal (an
        // `approve` cannot fix a balance shortfall or a paused pool).
        let allowance_short =
            decdn_incentive::is_erc20_allowance_shortfall(err.as_revert_data().as_ref());
        let submit_err = anyhow::Error::new(err).context("submit topUp");
        return Err(if allowance_short {
            submit_err.context(AllowanceShortfall)
        } else {
            submit_err
        });
    };
    let receipt = pending.get_receipt().await.context("await topUp receipt")?;
    if !receipt.status() {
        anyhow::bail!("topUp reverted for pool {pool_id}");
    }
    // Filtered on the emitting address, not just the topic: `topUp` transfers
    // BEFORE it emits, so a token with a transfer hook could plant a forged
    // `PoolToppedUp` earlier in the same receipt and `find` would prefer it.
    let credited = receipt
        .inner
        .logs()
        .iter()
        .filter(|log| log.address() == *contract.address())
        .filter_map(|log| log.log_decode::<PaymentPool::PoolToppedUp>().ok())
        .map(|decoded| decoded.inner.data)
        .find(|ev| ev.poolId == pool_id)
        .map_or_else(
            || {
                warn!(
                    %pool_id,
                    requested = %additional,
                    "topUp mined without a PoolToppedUp event (ABI skew); crediting the \
                     requested amount, which may over-state the on-chain deposit"
                );
                additional
            },
            |ev| ev.additionalDeposit,
        );
    let tx = receipt.transaction_hash;
    info!(%pool_id, %credited, %tx, "topped up buyer payment pool");
    Ok(ToppedUpPool { credited, tx })
}

/// Refill a reused buyer pool to `target_deposit` once its remaining spendable
/// deposit falls below `1/N` of the configured working deposit.
/// `5` → refill triggers below 20% remaining, then restores to a full deposit.
///
/// Shared by the CLI fetch auto-refill (#1103) and the node's node-to-node reuse
/// path (#1146), so the refill policy has a single source of truth.
pub const LOW_WATER_DIVISOR: u64 = 5;

/// Decide how much USDC to add to a reused pool so a sustained series of fetches
/// isn't stranded by a spent-down deposit (#1103).
///
/// Pure decision (no I/O) so the policy is unit-testable. `deposit` is the pool's
/// current on-chain deposit and `prior_amount` the cumulative amount already
/// vouchered across its lanes, so the remaining spendable is
/// `deposit - prior_amount`. When that remaining balance has fallen below
/// `low_water`, return the top-up that restores it to `target_deposit`; otherwise
/// return `U256::ZERO` (no refill). Hysteresis (`low_water < target_deposit`)
/// keeps a busy pool from topping up on every reuse.
///
/// The exact cost of the *next* fetch is not known at refill time (the per-MB
/// `rate` is only learned from the provider's probe / `StreamResponse`), so this
/// uses a rate-independent low-water refill: keep at least `low_water` of headroom,
/// and refill to a full `target_deposit` when it runs low.
#[must_use]
pub fn refill_amount(
    deposit: U256,
    prior_amount: U256,
    target_deposit: U256,
    low_water: U256,
) -> U256 {
    let remaining = deposit.saturating_sub(prior_amount);
    if remaining >= low_water {
        return U256::ZERO;
    }
    target_deposit.saturating_sub(remaining)
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic
)]
mod tests {
    use super::{
        AllowanceShortfall, LOW_WATER_DIVISOR, approval_floor, approve_decision,
        escrowed_but_untracked, grade_deposit_credit, issue_self_capability, open_pool,
        refill_amount, top_up,
    };
    use alloy::dyn_abi::Eip712Domain;
    use alloy::primitives::{Address, B256, TxHash, U256};
    use alloy::signers::local::PrivateKeySigner;
    use decdn_incentive::{DepositOutcome, StoreError};
    use decdn_incentive::{SignedCapability, voucher_domain};

    const CHAIN_ID: u64 = 421_614;

    fn domain() -> Eip712Domain {
        voucher_domain(CHAIN_ID, Address::repeat_byte(0xCC))
    }

    // ---- `open_pool` / `top_up` signatures -------------------------------

    /// Compile-time signature check that `open_pool` takes only `deposit` on the
    /// value axis (no provider, no `voucher_signer`): naming the monomorphized
    /// generic fn item as a value forces the compiler to check its parameter
    /// list. `open_pool` submits a real `openPool` tx and decodes the mined
    /// receipt, so end-to-end coverage lives in the anvil e2e.
    #[test]
    fn open_pool_signature_takes_deposit_only() {
        let _ = open_pool::<alloy::providers::RootProvider>;
    }

    /// Compile-time signature check that `top_up` takes `(contract, pool_id,
    /// additional)` — no store, no provider — mirroring the pool contract's own
    /// `topUp(poolId, additionalDeposit)`.
    #[test]
    fn top_up_signature_takes_pool_id_and_amount() {
        let _ = top_up::<alloy::providers::RootProvider>;
    }

    // ---- self-owned capability (#966 / ADR 003 §Capability delegation) ---

    /// The capability [`open_pool`] signs must recover to the OWNER — the buyer
    /// signs its own key as the delegate, so `recover_owner` returns the buyer's
    /// address.
    #[test]
    fn issue_self_capability_recovers_to_owner() -> anyhow::Result<()> {
        let owner = PrivateKeySigner::random();
        let pool_id = B256::repeat_byte(0x11);
        let cap: SignedCapability =
            issue_self_capability(&owner, pool_id, 1_000_000u64, u64::MAX, &domain())?;
        assert_eq!(cap.recover_owner(&domain())?, owner.address());
        assert_eq!(
            cap.capability.signer,
            owner.address(),
            "the delegated signer is the owner's own key"
        );
        assert_eq!(cap.capability.pool_id, pool_id);
        Ok(())
    }

    /// Regenerating the capability from the same key + params is deterministic:
    /// EIP-712 signing over a fixed digest is stable, so a per-connection
    /// regeneration (never stored) yields byte-identical signatures.
    #[test]
    fn regenerated_capability_is_deterministic_for_same_params() -> anyhow::Result<()> {
        let owner = PrivateKeySigner::random();
        let pool_id = B256::repeat_byte(0x22);
        let a = issue_self_capability(&owner, pool_id, 5_000u64, 1_900_000_000, &domain())?;
        let b = issue_self_capability(&owner, pool_id, 5_000u64, 1_900_000_000, &domain())?;
        assert_eq!(
            a.signature, b.signature,
            "regenerated signatures must match"
        );
        Ok(())
    }

    // ---- allowance decision -------------

    #[test]
    fn unlimited_zero_allowance_approves_max() {
        assert_eq!(approve_decision(U256::ZERO, None), Some(U256::MAX));
    }

    #[test]
    fn unlimited_sufficient_skips() {
        assert_eq!(approve_decision(approval_floor(), None), None);
        assert_eq!(approve_decision(U256::MAX, None), None);
    }

    #[test]
    fn unlimited_below_floor_reapproves_max() {
        let below = approval_floor() - U256::from(1);
        assert_eq!(approve_decision(below, None), Some(U256::MAX));
    }

    #[test]
    fn exact_zero_allowance_approves_deposit() {
        let deposit = U256::from(10_000_000u64);
        assert_eq!(approve_decision(U256::ZERO, Some(deposit)), Some(deposit));
    }

    #[test]
    fn exact_equal_allowance_skips() {
        let deposit = U256::from(10_000_000u64);
        assert_eq!(approve_decision(deposit, Some(deposit)), None);
    }

    #[test]
    fn exact_below_deposit_reapproves() {
        let deposit = U256::from(10_000_000u64);
        let current = deposit - U256::from(1);
        assert_eq!(approve_decision(current, Some(deposit)), Some(deposit));
    }

    #[test]
    fn exact_no_downgrade_from_unlimited() {
        let deposit = U256::from(10_000_000u64);
        assert_eq!(approve_decision(U256::MAX, Some(deposit)), None);
    }

    // ---- auto-refill decision (#1103, #1146) ----------------------------

    fn target() -> U256 {
        U256::from(10_000_000u64) // 10 USDC
    }
    fn low_water() -> U256 {
        target() / U256::from(LOW_WATER_DIVISOR) // 2 USDC (20%)
    }

    #[test]
    fn refill_amount_no_top_up_when_remaining_at_or_above_low_water() {
        assert_eq!(
            refill_amount(target(), U256::ZERO, target(), low_water()),
            U256::ZERO,
            "a full pool must not be topped up"
        );
        let prior = target() - low_water();
        assert_eq!(
            refill_amount(target(), prior, target(), low_water()),
            U256::ZERO,
            "remaining exactly at the low-water mark is still sufficient"
        );
    }

    #[test]
    fn refill_amount_restores_to_target_when_low() {
        let remaining = low_water() - U256::from(1u64);
        let prior = target() - remaining;
        assert_eq!(
            refill_amount(target(), prior, target(), low_water()),
            target() - remaining,
            "refill must restore the remaining deposit back up to the target"
        );
        let prior_drained = target() - U256::from(1u64);
        assert_eq!(
            refill_amount(target(), prior_drained, target(), low_water()),
            target() - U256::from(1u64),
        );
    }

    #[test]
    fn refill_amount_has_hysteresis_after_a_prior_top_up() {
        let deposit = target() * U256::from(2u64);
        let prior = deposit - low_water();
        assert_eq!(
            refill_amount(deposit, prior, target(), low_water()),
            U256::ZERO,
            "a topped-up pool with headroom must not refill on every reuse"
        );
    }

    #[test]
    fn refill_amount_saturates_and_never_underflows() {
        assert_eq!(
            refill_amount(target(), target() * U256::from(3u64), target(), low_water()),
            target(),
            "remaining saturates to zero, so refill is a full target"
        );
        let deposit = target() * U256::from(2u64);
        assert_eq!(
            refill_amount(deposit, U256::ZERO, target(), deposit),
            U256::ZERO,
            "remaining already >= target yields no top-up even below low-water"
        );
    }

    #[test]
    fn refill_targets_working_deposit_not_initial_open_size() {
        let initial = U256::from(500_000u64);
        let working = U256::from(10_000_000u64);
        let deposit = initial;
        let prior = U256::from(460_000u64); // remaining = 40_000
        let low_water = working / U256::from(LOW_WATER_DIVISOR);
        let add = refill_amount(deposit, prior, working, low_water);
        assert_eq!(add, working - (deposit - prior));
    }

    /// Pins the wire between `top_up`'s error tagging and the node's
    /// `top_up_recovering_allowance` downcast: the marker is attached with
    /// `.context(AllowanceShortfall)` on top of an already-wrapped
    /// `anyhow::Error` (the `submit topUp` context over the concrete submit
    /// error), exactly as `top_up` does. `anyhow::Error::downcast_ref` searches
    /// the context chain, so a context-attached marker is still recoverable —
    /// without this the whole daemon approve-and-retry path is silently dead.
    #[test]
    fn allowance_shortfall_context_is_downcastable_through_the_wrapping() {
        // Stand-in for the concrete `alloy` submit error `top_up` wraps first.
        #[derive(Debug)]
        struct SubmitError;
        impl std::fmt::Display for SubmitError {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "rpc submit failed")
            }
        }
        impl std::error::Error for SubmitError {}

        let submit_err = anyhow::Error::new(SubmitError).context("submit topUp");
        let tagged = submit_err.context(AllowanceShortfall);
        assert!(
            tagged.downcast_ref::<AllowanceShortfall>().is_some(),
            "the daemon retry gate downcasts on this marker; it must survive the \
             `.context()` wrapping `top_up` applies"
        );
    }

    // ---- escrowed-but-untracked grading -----------------------------------
    //
    // The hazard these guard is asymmetric: the on-chain half already
    // committed, so the only remaining lever is the exit code. A caller that
    // logs and returns success is indistinguishable from one that did the
    // work, and `if decdn pool top-up …; then mark_funded; fi` then records
    // money as tracked that nobody will reconcile.

    fn a_tx() -> TxHash {
        TxHash::from([0xab; 32])
    }

    #[test]
    fn escrowed_but_untracked_names_the_tx_and_the_action() {
        let err = escrowed_but_untracked("pool 0x01 topped up by 5 µUSDC", a_tx(), "disk full");
        let msg = format!("{err:#}");
        assert!(msg.contains("pool 0x01 topped up by 5 µUSDC"), "{msg}");
        assert!(
            msg.contains(&format!("{}", a_tx())),
            "the tx is the handle an operator reconciles against: {msg}"
        );
        assert!(msg.contains("reconcile against the tx"), "{msg}");
        assert!(
            msg.contains("a retry escrows again"),
            "re-running is the obvious response to a non-zero exit and the one that \
             double-spends: {msg}"
        );
        assert!(msg.contains("disk full"), "the cause must survive: {msg}");
    }

    #[test]
    fn a_credited_deposit_grades_to_the_new_total() {
        let new_deposit = grade_deposit_credit(
            Ok(DepositOutcome::Added(U256::from(140u64))),
            "topped up",
            a_tx(),
        )
        .expect("Added is the success path");
        assert_eq!(new_deposit, U256::from(140u64));
    }

    /// `add_deposit` splits its failures across two channels — a backend fault
    /// is the `Err`, a committed-row mismatch is a non-`Added` `Ok`. For a
    /// caller standing over escrowed USDC they mean the same thing, and the
    /// `Ok` half is the one that reads like success at a glance.
    #[test]
    fn every_uncredited_outcome_is_an_error_naming_the_tx() {
        for (outcome, cause) in [
            (Ok(DepositOutcome::UnknownPool), "UnknownPool"),
            (Ok(DepositOutcome::PoolMismatch), "PoolMismatch"),
            (
                Err(StoreError::Backend("commit (fsync): no space".into())),
                "no space",
            ),
        ] {
            let err = grade_deposit_credit(outcome, "pool 0x01 topped up by 5 µUSDC", a_tx())
                .expect_err("an uncredited deposit must not read as success");
            let msg = format!("{err:#}");
            assert!(msg.contains("escrowed but untracked"), "{msg}");
            assert!(msg.contains(&format!("{}", a_tx())), "{msg}");
            assert!(
                msg.contains("pool 0x01 topped up by 5 µUSDC"),
                "the amount and pool are what an operator reconciles the escrow against: {msg}"
            );
            assert!(
                msg.contains(cause),
                "each channel must keep its own diagnosis, not collapse to one string: {msg}"
            );
        }
    }

    fn cum(bytes: u64, amount: u64) -> crate::Cumulative {
        crate::Cumulative {
            bytes: U256::from(bytes),
            amount: U256::from(amount),
        }
    }

    fn lane_progress(bytes: u64, amount: u64) -> decdn_incentive::BuyerLaneProgress {
        decdn_incentive::BuyerLaneProgress {
            last_amount: U256::from(amount),
            last_bytes: U256::from(bytes),
        }
    }

    /// Progress that neither advanced past its seed nor rebased has nothing to
    /// write.
    #[test]
    fn progress_write_is_none_without_an_advance_or_an_anchor() {
        let progress = crate::VoucherProgress::from_cumulative(cum(500, 50), U256::from(50u64));
        assert_eq!(super::ProgressWrite::of(&progress), None);
    }

    /// An advance past the seed is a monotone advance to the totals.
    #[test]
    fn progress_write_advances_past_the_seed() {
        let progress = crate::VoucherProgress::from_cumulative(cum(600, 65), U256::from(50u64));
        assert_eq!(
            super::ProgressWrite::of(&progress),
            Some(super::ProgressWrite::Advance {
                totals: lane_progress(600, 65)
            })
        );
    }

    /// A pending anchor is written as a rebase even when the totals did not
    /// advance: the write exists to move the record down.
    #[test]
    fn progress_write_rebases_a_pending_anchor_without_an_advance() {
        let progress = crate::VoucherProgress::from_cumulative(cum(500, 50), U256::from(50u64))
            .with_rebase_anchor(Some(cum(400, 40)));
        assert_eq!(
            super::ProgressWrite::of(&progress),
            Some(super::ProgressWrite::Rebase {
                anchor: lane_progress(400, 40),
                totals: lane_progress(500, 50),
            })
        );
    }
}
