//! Buyer-side `PaymentPool` open kernel, shared by the node's node-to-node
//! cache-miss buyer (#744) and the CLI (`decdn get`, #940).
//!
//! The genuinely duplication-prone part — the `openPool` transaction, the
//! authoritative `PoolOpened`-from-receipt decode, the self-owned capability the
//! owner key signs for its lane key
//! ([`OwnedPayer`](crate::buyer_pool::OwnedPayer)), and the [`BuyerPoolState`] /
//! [`PoolContext`] construction for a self-owned lane
//! ([`self_owned_lane_ctx`](crate::buyer_pool::self_owned_lane_ctx)) —
//! lives here, with the rule that picks a lane's progress write
//! ([`ProgressWrite`](crate::buyer_pool::ProgressWrite)). Pool *reuse* is a thin
//! composition over [`decdn_incentive::BuyerPoolStore::get_by_owner`] that each
//! caller does directly: a one-shot CLI fetch needs neither the node service's
//! per-owner concurrency guard nor its background reclaim/reconcile machinery.

use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use alloy::dyn_abi::Eip712Domain;
use alloy::network::Ethereum;
use alloy::primitives::{Address, B256, TxHash, U256};
use alloy::providers::{PendingTransactionBuilder, Provider};
use alloy::signers::local::PrivateKeySigner;
use anyhow::{Context, Result};
use decdn_incentive::erc20::Erc20;
use decdn_incentive::lane_signer::derive_lane_signer;
use decdn_incentive::payment_pool::{PaymentPool, to_pool_u64};
use decdn_incentive::tx::{RECEIPT_TIMEOUT, await_receipt};
use decdn_incentive::{
    AdvanceOutcome, BuyerLaneProgress, BuyerPoolState, BuyerPoolStore, Capability, Deployment,
    DepositOutcome, LaneKey, PoolOpenFailureReason, SignedCapability, StoreError,
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

/// A freshly opened buyer pool: the persistable [`BuyerPoolState`] and the open
/// transaction hash. A lane on it signs under [`self_owned_lane_ctx`]. The
/// deposit is on-chain the moment `openPool` mines, so a caller whose subsequent
/// `store.record` fails names that tx for manual reconciliation — an error in the
/// CLI, an `error!` in the daemon.
#[derive(Debug)]
#[non_exhaustive]
#[must_use = "the open tx is the only handle to an escrowed-but-untracked deposit"]
pub struct OpenedPool {
    /// Persist this via `BuyerPoolStore::record`. A lane on the new pool
    /// signs under [`self_owned_lane_ctx`].
    pub state: BuyerPoolState,
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
#[non_exhaustive]
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
    anyhow::Error::new(EscrowUntracked {
        tx,
        text: format!(
            "{effect} on-chain (tx {tx}) but the local record did not survive; the deposit is \
             escrowed but untracked — reconcile against the tx before retrying, because a retry \
             escrows again: {cause}"
        ),
    })
}

/// The error [`escrowed_but_untracked`] builds, or an [`UnconfirmedEscrows`]
/// rebuilds from the escrow it holds: USDC moved on-chain and the local record
/// does not account for it. A retry escrows again, so the acquire
/// loop classifies it as fatal to the command ([`crate::Fault::Fatal`]) instead
/// of retrying the lane build.
#[derive(Debug)]
pub struct EscrowUntracked {
    /// The `openPool` or `topUp` transaction that escrowed.
    tx: TxHash,
    /// What landed, the tx, and the local failure.
    text: String,
}

impl EscrowUntracked {
    /// The `openPool` or `topUp` transaction that escrowed.
    #[must_use]
    pub const fn tx(&self) -> TxHash {
        self.tx
    }
}

impl std::fmt::Display for EscrowUntracked {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.text)
    }
}

impl std::error::Error for EscrowUntracked {}

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

/// The two keys a buyer pays its own pool with.
///
/// - The **owner** key owns the pool. It sends `openPool`, `topUp` and the
///   USDC approval, keys the buyer-store row, and signs the capability that
///   authorizes the lane key.
/// - The **lane** key signs the vouchers and the client identity binding, so
///   every lane of this payer is `(pool, lane key, provider)`.
///
/// A client data dir uses [`OwnedPayer::for_store`]: its lane key is derived
/// from the owner key and the store's salt ([`decdn_incentive::lane_signer`]),
/// so two data dirs on one wallet sign on disjoint lanes of one pool (ADR 003
/// § Capability delegation, ADR 012 § Ethereum Key). The node's upstream buyer
/// uses [`OwnedPayer::sole`]: one process per node data dir, so the node key
/// signs both roles.
#[derive(Clone)]
pub struct OwnedPayer {
    owner: Arc<PrivateKeySigner>,
    lane: Arc<PrivateKeySigner>,
}

impl OwnedPayer {
    /// The payer for `store`'s data dir: `owner` owns the pool, and the lane
    /// key derives from `owner` and the store's lane salt.
    ///
    /// # Errors
    ///
    /// The store cannot read or record its salt, marked [`crate::LocalPullFault`]
    /// because only a repair of the store clears it, or the derivation gives an
    /// invalid key.
    pub fn for_store(owner: Arc<PrivateKeySigner>, store: &dyn BuyerPoolStore) -> Result<Self> {
        let salt = store.lane_salt().map_err(|e| {
            anyhow::anyhow!(
                "read or record this data dir's lane salt in the buyer store: {e}; the store \
                 needs repair before a fetch on the wallet's own pool can run"
            )
            .context(crate::LocalPullFault)
        })?;
        let lane = derive_lane_signer(owner.as_ref(), &salt)?;
        Ok(Self {
            owner,
            lane: Arc::new(lane),
        })
    }

    /// The payer whose one key both owns the pool and signs its lanes. Use it
    /// only when one process ever signs for this owner, as the node does. Two
    /// processes on one sole key share every lane and fold over each other's
    /// watermarks, so a client data dir uses [`Self::for_store`].
    #[must_use]
    pub fn sole(owner: Arc<PrivateKeySigner>) -> Self {
        Self {
            lane: Arc::clone(&owner),
            owner,
        }
    }

    /// The key that owns the pool.
    #[must_use]
    pub const fn owner(&self) -> &Arc<PrivateKeySigner> {
        &self.owner
    }

    /// The key that signs this payer's vouchers.
    #[must_use]
    pub const fn lane(&self) -> &Arc<PrivateKeySigner> {
        &self.lane
    }
}

// Addresses only: the keys never reach a log.
impl std::fmt::Debug for OwnedPayer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OwnedPayer")
            .field("owner", &self.owner.address())
            .field("lane", &self.lane.address())
            .finish()
    }
}

/// Sign a self-owned [`SignedCapability`]: the pool owner delegates spend on
/// `pool_id` to the payer's lane key, up to `spending_cap`, until `expiry`.
/// The lane key derives from the owner key or is the owner key, so the
/// capability is regenerated per connection and never stored.
///
/// The serving node registers it on the buyer's first on-chain redemption
/// (`redeemMany`'s `CapabilityReg`), then accepts vouchers from the lane key up
/// to the cap (ADR 003 §Capability delegation).
///
/// # Errors
///
/// Propagates a signing error from the owner signer.
#[doc(hidden)]
pub fn issue_self_capability(
    payer: &OwnedPayer,
    pool_id: B256,
    spending_cap: u64,
    expiry: u64,
    voucher_domain: &Eip712Domain,
) -> Result<SignedCapability> {
    Capability {
        signer: payer.lane().address(),
        spending_cap,
        pool_id,
        expiry,
    }
    .sign(payer.owner().as_ref(), voucher_domain)
    .map_err(|e| anyhow::anyhow!("sign self capability: {e}"))
}

/// The [`PoolContext`] one lane of a self-owned pool signs under: `payer`'s
/// lane key pays `provider` from `state`'s pool, resuming at the lane's prior
/// cumulative `(prior_bytes, prior_amount)`.
///
/// The context carries a freshly issued self-owned capability. The capability
/// is node-agnostic (valid at every provider this pool pays) and cheap to
/// regenerate, so a fresh open and a reused pool both present one without a
/// stored copy. An upstream registers the lane key from it on the first
/// on-chain redemption.
///
/// The cap is [`SELF_CAPABILITY_CAP`], not the deposit. The lane key is the
/// owner key or derives from it, so the pool deposit is the real spending
/// bound. A cap at the deposit would freeze the on-chain cap at the opening
/// deposit (`_registerCapability` is idempotent past the first redemption) and
/// reject spend past a later `topUp`. It is not `U256::MAX` either: the pool's
/// `spendingCap` is a `uint64`, so a wider cap hashes to a word the contract
/// cannot reconstruct and every redemption on the lane reverts.
///
/// The provider pin is mandatory: a voucher signed with `provider ==
/// Address::ZERO` fails at signing, so a context left at
/// [`PoolContext::for_pool`]'s zero provider cannot pay.
///
/// # Errors
///
/// Propagates a signing error from the owner key while issuing the capability.
pub fn self_owned_lane_ctx(
    state: &BuyerPoolState,
    payer: &OwnedPayer,
    voucher_domain: &Eip712Domain,
    provider: Address,
    prior_bytes: U256,
    prior_amount: U256,
) -> Result<PoolContext> {
    let capability = issue_self_capability(
        payer,
        state.pool_id,
        SELF_CAPABILITY_CAP,
        SELF_CAPABILITY_EXPIRY,
        voucher_domain,
    )?;
    Ok(
        PoolContext::for_pool(state, Arc::clone(payer.lane()), voucher_domain.clone())
            .with_provider(provider, prior_bytes, prior_amount)
            .with_capability(capability),
    )
}

/// The buyer-store write that records one lane's [`VoucherProgress`], so a later
/// reuse or a restart resumes the lane at the right cumulative `bytes` /
/// `amount`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum ProgressWrite {
    /// The lane's ledger rebased to the upstream's authenticated watermark
    /// `anchor`, behind the record on `amount` (`PoolLedger::rebase`), and no write has recorded it yet. The
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

    /// Apply this write to `lane` in the record `row` names. A
    /// [`WatermarkRow::Owned`] write goes to the owner's pool row for
    /// `lane.pool_id`; a [`WatermarkRow::Delegated`] write goes to the delegate
    /// lane `lane` on the row's deployment.
    ///
    /// # Errors
    ///
    /// Returns a [`StoreError`] only on a backend or codec fault. A write the
    /// store declines (an unknown or replaced pool, a regression) is the `Ok`
    /// [`AdvanceOutcome`].
    pub fn apply<S: BuyerPoolStore + ?Sized>(
        &self,
        store: &S,
        row: WatermarkRow,
        lane: LaneKey,
    ) -> Result<AdvanceOutcome, StoreError> {
        match (row, *self) {
            (WatermarkRow::Owned(owner), Self::Rebase { anchor, totals }) => {
                store.rebase_progress(owner, lane.pool_id, lane, anchor, totals)
            }
            (WatermarkRow::Owned(owner), Self::Advance { totals }) => store.advance_progress(
                owner,
                lane.pool_id,
                lane,
                totals.last_bytes,
                totals.last_amount,
            ),
            (WatermarkRow::Delegated(deployment), Self::Rebase { anchor, totals }) => {
                store.rebase_delegate_progress(deployment, lane, anchor, totals)
            }
            (WatermarkRow::Delegated(deployment), Self::Advance { totals }) => {
                store.advance_delegate_progress(deployment, lane, totals)
            }
        }
    }
}

/// Which buyer-store record holds a lane's watermark.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum WatermarkRow {
    /// The pool row of this owner: a lane on the buyer's own pool.
    Owned(Address),
    /// The delegate lanes on this deployment: a lane a key signs under an
    /// owner-signed capability on a pool another owner funds
    /// ([`BuyerPoolStore::delegate_lanes`]).
    Delegated(Deployment),
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
/// Fails on the allowance read, the `approve` submission, a receipt that is not
/// read (a failed read, or none within [`RECEIPT_TIMEOUT`]; the error names the
/// tx, which may still mine), or a reverted approve. A submission that fails in
/// transport
/// ([`decdn_incentive::tx::send_broadcast_unknown`]) says the `approve` may have
/// been broadcast.
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
    let pending = match erc20.approve(spender, approve_value).send().await {
        Ok(pending) => pending,
        Err(err) => {
            // A transport failure may have reached the node and broadcast the
            // `approve` before the response was lost, with no hash returned.
            let note = if decdn_incentive::tx::send_broadcast_unknown(&err) {
                ": the request may have reached the node and broadcast the transaction — \
                 check this signer's pending and mined transactions before re-running"
            } else {
                ""
            };
            return Err(anyhow::Error::new(err).context(format!("submit USDC approve{note}")));
        }
    };
    // Capture the hash before the wait consumes `pending`, so a timeout error
    // names the broadcast tx an operator needs to look up: it stays in the
    // mempool and may still mine after the wait ends. The wait is bounded, so a
    // stuck or underpriced tx cannot wedge the buyer path, in the CLI or the
    // node (#1109). Ending the wait is safe: the allowance read at the top of
    // the next run makes the re-approve idempotent, so no funds are stranded.
    let approve_tx = *pending.tx_hash();
    let receipt = await_receipt(pending, Some(RECEIPT_TIMEOUT))
        .await
        .with_context(|| {
            format!("await USDC approve receipt (tx {approve_tx:#x}; it may still mine later)")
        })?;
    if !receipt.status() {
        anyhow::bail!("USDC approve transaction reverted (tx {approve_tx:#x})");
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
/// `deployment` is the chain and `PaymentPool` that `contract` talks to. It tags
/// the returned row and derives the voucher EIP-712 domain, so the row and every
/// voucher signed on it name the same deployment. A `deployment` whose address
/// is not `contract`'s is an error, before any transaction.
///
/// `openPool` names no provider and no signer — a pool is bound to no payee at
/// open, and fans out to many `(signer, provider)` lanes off-chain (ADR 003).
/// Build a lane on the new pool with [`self_owned_lane_ctx`], which signs the
/// self-owned capability the serving node registers on the first redemption.
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
/// Fails on the pending-nonce read (nothing sent, so no [`OpenUnconfirmed`]),
/// `openPool` submit/receipt, a reverted tx, a missing `PoolOpened`
/// event in the receipt logs, or a capability-signing error. The classified
/// failure legs attach a [`PoolOpenFailureReason`] into the `anyhow` error chain
/// (recover it with `err.downcast_ref::<PoolOpenFailureReason>()`) so a caller
/// can bump the matching `decdn_pool_open_failures_{reason}_total` counter: a
/// deterministic revert (with ABI revert data, decoded against the
/// insufficient-deposit error selectors) is split from a transport/RPC fault (no
/// revert data) and a mined on-chain revert. The missing-`PoolOpened` leg carries
/// no reason: the deposit is escrowed but untracked, so it surfaces as
/// [`EscrowUntracked`] for manual reconciliation, which the acquire loop
/// classifies as fatal to the command.
///
/// # The caller picks the receipt bound
///
/// `receipt_timeout` bounds the receipt wait when it is `Some`. `openPool`
/// **escrows funds**, and giving up on the receipt does not cancel the tx. A
/// caller that then believes no open is in flight would escrow a second deposit
/// against a pool the first tx is still going to mine. So a caller that bounds
/// the wait must resolve the unconfirmed open before it sends another one.
/// Both binaries pass [`RECEIPT_TIMEOUT`]. In the CLI a lapse ends the command
/// and names the tx (#2433). The node calls this inside a detached task that
/// holds the owner's open slot while this future runs, and after it while the
/// open is unconfirmed (#1143, #2415). That task settles an unconfirmed open
/// against the chain and re-sends one dropped from the mempool.
///
/// The wait ends without a receipt when the receipt read fails or the bound
/// lapses. That error names the broadcast tx, which may still mine and escrow the deposit.
/// The error is classified [`PoolOpenFailureReason::RpcError`] and carries an
/// [`OpenUnconfirmed`] with the hash and nonce. A submit that fails in
/// transport ([`decdn_incentive::tx::send_broadcast_unknown`]) carries one too,
/// with the nonce and no hash. The caller must resolve that transaction before
/// it sends another `openPool`, and hands a receipt it reads later to
/// [`opened_from_receipt`].
///
/// A caller can also drop this future: a Ctrl-C, or a fault that ends the
/// command. No error is returned then, and a caller can also drop an error a
/// sibling task returned. So `escrows` holds the `openPool` from before its
/// submit until its receipt is read: with no hash while the submit is in
/// flight or failed in transport, with the hash after. A caller that may drop
/// the future or its error passes `Some` and reads
/// [`UnconfirmedEscrows::pending`] when it ends, so it can still name the tx.
///
/// An `escrows` that already holds an unconfirmed open refuses a new one: the
/// call fails with that [`OpenUnconfirmed`] before any chain read, because the
/// earlier tx may still mine and a second open would escrow a second deposit.
/// A held `topUp` does not refuse an open.
/// A caller that serializes its opens sees this only after an earlier open
/// ended without a receipt.
///
/// # The nonce is explicit
///
/// `open_pool` reads `owner`'s pending nonce and sends the `openPool` with it,
/// so an unconfirmed open names the nonce it holds. The provider's own nonce
/// manager does not see that nonce, so `contract`'s provider must read the
/// pending nonce on every send (alloy's `with_simple_nonce_management`), as for
/// [`top_up`].
pub async fn open_pool<P: Provider + Clone>(
    contract: &PaymentPool::PaymentPoolInstance<P>,
    deployment: Deployment,
    token: Address,
    owner: Address,
    deposit: U256,
    escrows: Option<&UnconfirmedEscrows>,
    receipt_timeout: Option<Duration>,
) -> Result<OpenedPool> {
    anyhow::ensure!(
        *contract.address() == deployment.payment_pool,
        "openPool: contract {} is not the deployment's PaymentPool {}",
        contract.address(),
        deployment.payment_pool
    );
    let amount = to_pool_u64(deposit, "deposit")?;
    if let Some(earlier) = escrows.and_then(|escrows| escrows.opens().first().copied()) {
        return Err(anyhow::Error::new(earlier)
            .context("refusing a second openPool while an earlier one is unconfirmed"));
    }
    let nonce = contract
        .provider()
        .get_transaction_count(owner)
        .pending()
        .await
        .context("read the pending nonce for openPool")
        .context(PoolOpenFailureReason::RpcError)?;
    // A caller with no handle holds the entries in one nobody reads.
    let escrows = escrows.cloned().unwrap_or_default();
    let submitted = OpenUnconfirmed { tx: None, nonce };
    // Held across the submit: a drop while it is in flight leaves an
    // `openPool` the RPC node may have broadcast with no hash returned.
    escrows.insert(submitted);
    let sent = contract
        .openPool(amount)
        .from(owner)
        .nonce(nonce)
        .send()
        .await;
    let pending = match sent {
        Ok(pending) => pending,
        Err(err) => {
            // A deterministic revert (a zero deposit, USDC balance/allowance too
            // low) is caught at gas estimation, so it surfaces here with ABI
            // revert data attached; a send error *without* revert data is a
            // transport/RPC fault.
            let reason = PoolOpenFailureReason::classify_revert_data(err.as_revert_data().as_ref());
            // A transport failure may have reached the node and broadcast the
            // `openPool` before the response was lost.
            let broadcast_unknown = decdn_incentive::tx::send_broadcast_unknown(&err);
            let mut err = anyhow::Error::new(err).context("submit openPool");
            if broadcast_unknown {
                err = err.context(submitted);
            } else {
                escrows.remove(submitted);
            }
            return Err(err.context(reason));
        }
    };
    // Capture the hash before `get_receipt` consumes `pending`: past this point
    // the deposit may escrow, so a receipt failure names the tx to reconcile.
    let tx = *pending.tx_hash();
    info!(%tx, %owner, %deposit, "openPool broadcast; awaiting its receipt");
    let unconfirmed = OpenUnconfirmed {
        tx: Some(tx),
        nonce,
    };
    escrows.replace(submitted, unconfirmed);

    // Bounded only when the caller asks — see `# The caller picks the receipt
    // bound` above. Only a receipt releases the entry: a drop here, a failed
    // read or a lapse leaves it, which is the point.
    let receipt = await_receipt(pending, receipt_timeout)
        .await
        .map_err(|err| {
            err.context(format!(
                "await openPool receipt (tx {tx:#x}; it may still mine and escrow the deposit)"
            ))
            .context(unconfirmed)
            .context(PoolOpenFailureReason::RpcError)
        })?;
    escrows.remove(unconfirmed);
    opened_from_receipt(&receipt, tx, deployment, token, owner, deposit)
}

/// Build the [`OpenedPool`] an `openPool` tx bought, from its mined `receipt`.
///
/// [`open_pool`] calls this once its receipt arrives. A caller that holds an
/// [`OpenUnconfirmed`] calls it with the receipt it read later, so a pool
/// whose first receipt read failed is tracked like any other. `deposit` is the
/// amount the open requested; the returned state records what the contract
/// credited.
///
/// # Errors
///
/// The tx reverted ([`PoolOpenFailureReason::ContractRevert`]), or the receipt
/// holds no `PoolOpened` event for `owner`: the deposit is escrowed but
/// untracked ([`EscrowUntracked`], logged here), so a retry escrows again.
pub fn opened_from_receipt(
    receipt: &alloy::rpc::types::TransactionReceipt,
    tx: TxHash,
    deployment: Deployment,
    token: Address,
    owner: Address,
    deposit: U256,
) -> Result<OpenedPool> {
    if !receipt.status() {
        // A mined revert: the reason is not recoverable from the receipt (no
        // trace), so it folds into `ContractRevert`.
        return Err(anyhow::anyhow!(
            "openPool reverted (tx {tx:#x}, owner {owner}, deposit {deposit}); check USDC \
             balance/allowance"
        )
        .context(PoolOpenFailureReason::ContractRevert));
    }

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
        return Err(escrowed_but_untracked(
            "buyer pool opened",
            tx,
            format!("its receipt holds no PoolOpened event for owner {owner}"),
        ));
    };
    let pool_id = opened.poolId;
    // Record what the contract CREDITED, not what we asked it to transfer.
    // `openPool` sets the pool deposit to the measured balance delta and emits
    // that in `PoolOpened.deposit`, precisely so a fee-on-transfer token cannot
    // over-state a pool against the shared USDC balance.
    let credited = opened.deposit;
    let state = BuyerPoolState::new(pool_id, deployment, owner, token, credited);
    info!(
        %owner,
        %pool_id,
        requested = %deposit,
        credited = %credited,
        "opened buyer payment pool"
    );
    Ok(OpenedPool { state, tx })
}

/// Expiry stamped on the self-owned capability [`self_owned_lane_ctx`] signs.
/// The buyer holds the owner key, and the lane key is that key or derives from
/// it, so there is no delegation to time-box: `u64::MAX` means "never expires",
/// and the pool's own grace-window close is the only lifecycle gate (there is
/// no pool expiry, ADR 003).
const SELF_CAPABILITY_EXPIRY: u64 = u64::MAX;

/// Cap stamped on a self-owned capability — the widest value the pool's
/// `uint64 spendingCap` can hold, which is the effective "no
/// delegated ceiling" for an owner spending against its own pool. The real
/// bound is the deposit: redemption pays `min(desired, cap - spent, remaining)`.
/// A `u64` by type, matching the contract field exactly, so it can never sign a
/// cap the narrower on-chain field cannot reconstruct.
#[doc(hidden)]
pub const SELF_CAPABILITY_CAP: u64 = u64::MAX;

/// Typed marker on an [`open_pool`] error raised when the `openPool` may have
/// been broadcast but is not known to have mined: its submit failed in
/// transport, or it was broadcast and its receipt was not read (a failed read,
/// or none within the caller's bound). An [`UnconfirmedEscrows`] entry has the
/// same meaning. The transaction may still mine and escrow the deposit, so a
/// caller must resolve it before it sends another `openPool`. Recover it with
/// `err.downcast_ref::<OpenUnconfirmed>()`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OpenUnconfirmed {
    /// The broadcast `openPool` transaction. `None` when the submit returned
    /// no hash (it failed in transport, or was dropped in flight): the RPC
    /// node may have broadcast the transaction without returning its hash.
    pub tx: Option<TxHash>,
    /// The nonce the `openPool` was sent with. Once the account's confirmed
    /// nonce passes it, the `openPool` has either mined or can never mine.
    pub nonce: u64,
}

impl std::fmt::Display for OpenUnconfirmed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.tx {
            Some(tx) => write!(
                f,
                "openPool tx {tx} (nonce {n}) was broadcast but its receipt was not read, so it \
                 may have mined and escrowed the deposit. An open sent before the tx confirms \
                 or the account's nonce passes {n} escrows a second deposit; once it mines, \
                 `decdn pool list --all` shows its pool on chain",
                n = self.nonce
            ),
            None => write!(
                f,
                "openPool submit (nonce {n}) returned no tx hash, so the RPC node may have \
                 broadcast it and it may mine. An open sent before the account's nonce passes \
                 {n} escrows a second deposit; once it mines, `decdn pool list --all` shows its \
                 pool on chain",
                n = self.nonce
            ),
        }
    }
}

impl std::error::Error for OpenUnconfirmed {}

/// An `openPool` or `topUp` a command submitted whose receipt it has not read:
/// one entry of [`UnconfirmedEscrows`]. Either may still mine and escrow USDC.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EscrowUnconfirmed {
    /// An `openPool`, which may escrow a new pool's deposit.
    Open(OpenUnconfirmed),
    /// A `topUp`, which may add to a pool's deposit.
    TopUp(TopUpUnconfirmed),
}

impl From<OpenUnconfirmed> for EscrowUnconfirmed {
    fn from(open: OpenUnconfirmed) -> Self {
        Self::Open(open)
    }
}

impl From<TopUpUnconfirmed> for EscrowUnconfirmed {
    fn from(top_up: TopUpUnconfirmed) -> Self {
        Self::TopUp(top_up)
    }
}

impl std::fmt::Display for EscrowUnconfirmed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Open(open) => open.fmt(f),
            Self::TopUp(top_up) => top_up.fmt(f),
        }
    }
}

/// The `openPool` and `topUp` transactions a command submitted whose receipt
/// it has not read, and those that mined with no local record crediting them.
///
/// [`open_pool`] and [`top_up`] add an entry before their submit and remove it
/// once they read the receipt, or once a submit is rejected without a
/// broadcast. A future dropped mid-submit or mid-wait, or a receipt read that
/// fails, leaves its entry. A command that ends while an entry remains names
/// it, because the transaction may escrow after the command exits and a
/// re-run before it confirms escrows again.
///
/// The handle also holds every `openPool` and `topUp` that a run on it
/// ([`crate::funding::RunFunding`]) saw mine with no local record crediting it
/// ([`UnconfirmedEscrows::untracked`]). Their receipts were read, so they are
/// not pending, but a command that ends while the handle holds one names it
/// too: a re-run reads the uncredited row and escrows again. [`open_pool`] and
/// [`top_up`] alone hold none: their caller returns the error that names the
/// escrow.
///
/// A clone shares the same lists, so one handle can outlive the per-pass state
/// that the transactions run under.
#[derive(Debug, Default, Clone)]
pub struct UnconfirmedEscrows(Arc<Shared>);

/// What every clone of one [`UnconfirmedEscrows`] shares.
#[derive(Debug, Default)]
struct Shared {
    pending: Mutex<Vec<EscrowUnconfirmed>>,
    /// Oldest first, one per tx. An entry is never removed.
    untracked: Mutex<Vec<UntrackedEscrow>>,
}

/// An `openPool` or `topUp` that mined and escrowed USDC that no local record
/// credits ([`EscrowUntracked`]), as an [`UnconfirmedEscrows`] holds it for
/// its command to name when it ends. Displays its error chain.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct UntrackedEscrow {
    /// The transaction that escrowed.
    pub tx: TxHash,
    /// The sanitized error chain that reported it, which names the tx.
    pub text: String,
}

impl std::fmt::Display for UntrackedEscrow {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.text)
    }
}

impl UntrackedEscrow {
    /// The held escrow as the error that reported it ([`EscrowUntracked`]).
    pub(crate) fn into_error(self) -> anyhow::Error {
        anyhow::Error::new(EscrowUntracked {
            tx: self.tx,
            text: self.text,
        })
    }
}

impl UnconfirmedEscrows {
    /// The `openPool` and `topUp` transactions that escrowed USDC no local
    /// record credits, oldest first.
    #[must_use]
    pub fn untracked(&self) -> Vec<UntrackedEscrow> {
        self.0
            .untracked
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// Hold `err` when it is an untracked escrow ([`EscrowUntracked`]) whose
    /// tx the handle does not hold yet.
    pub(crate) fn hold_untracked(&self, err: &anyhow::Error) {
        let Some(escrow) = err.downcast_ref::<EscrowUntracked>() else {
            return;
        };
        let mut held = self
            .0
            .untracked
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if !held.iter().any(|known| known.tx == escrow.tx) {
            held.push(UntrackedEscrow {
                tx: escrow.tx,
                text: decdn_common::redact::sanitize_err_chain(err),
            });
        }
    }

    /// Hold `err` as a run on the handle holds it when it is an untracked
    /// escrow, for a test that builds the error with
    /// [`escrowed_but_untracked`].
    #[cfg(any(test, feature = "test-util"))]
    pub fn hold_untracked_for_test(&self, err: &anyhow::Error) {
        self.hold_untracked(err);
    }

    /// The transactions whose receipt was not read, oldest first.
    #[must_use]
    pub fn pending(&self) -> Vec<EscrowUnconfirmed> {
        self.0
            .pending
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// The `openPool` transactions whose receipt was not read, oldest first.
    #[must_use]
    pub(crate) fn opens(&self) -> Vec<OpenUnconfirmed> {
        self.pending()
            .into_iter()
            .filter_map(|held| match held {
                EscrowUnconfirmed::Open(open) => Some(open),
                EscrowUnconfirmed::TopUp(_) => None,
            })
            .collect()
    }

    /// The `topUp` transactions whose receipt was not read, oldest first.
    #[must_use]
    pub(crate) fn top_ups(&self) -> Vec<TopUpUnconfirmed> {
        self.pending()
            .into_iter()
            .filter_map(|held| match held {
                EscrowUnconfirmed::TopUp(top_up) => Some(top_up),
                EscrowUnconfirmed::Open(_) => None,
            })
            .collect()
    }

    /// Hold `escrow` from its submit.
    fn insert(&self, escrow: impl Into<EscrowUnconfirmed>) {
        self.0
            .pending
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(escrow.into());
    }

    /// Release `escrow` once its receipt is read, or its submit was rejected.
    /// Releases one entry: two calls that hold equal entries each release
    /// their own.
    fn remove(&self, escrow: impl Into<EscrowUnconfirmed>) {
        let escrow = escrow.into();
        let mut held = self
            .0
            .pending
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if let Some(at) = held.iter().position(|entry| *entry == escrow) {
            held.remove(at);
        }
    }

    /// Swap `submitted` for `broadcast` under one lock, once the submit
    /// returns a hash, so a reader never sees the transaction held as
    /// neither.
    fn replace(
        &self,
        submitted: impl Into<EscrowUnconfirmed>,
        broadcast: impl Into<EscrowUnconfirmed>,
    ) {
        let (submitted, broadcast) = (submitted.into(), broadcast.into());
        let mut held = self
            .0
            .pending
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        match held.iter_mut().find(|entry| **entry == submitted) {
            Some(entry) => *entry = broadcast,
            None => held.push(broadcast),
        }
    }
}

/// Send `openPool(deposit)` from `owner` at exactly `nonce`, and return its
/// hash without waiting for a receipt.
///
/// A caller resolving an [`OpenUnconfirmed`] whose nonce no transaction holds
/// re-sends with that same nonce. Two transactions at one nonce cannot both
/// mine, so the re-send and a first open that resurfaces escrow at most one
/// deposit between them. A receipt wait here would never end if the other
/// transaction took the nonce, so the caller reads receipts itself.
///
/// # Errors
///
/// The deposit overflows `uint64`, or the send fails. A send failure carries a
/// [`PoolOpenFailureReason`] classified from its revert data.
pub async fn send_open_pool<P: Provider + Clone>(
    contract: &PaymentPool::PaymentPoolInstance<P>,
    owner: Address,
    deposit: U256,
    nonce: u64,
) -> Result<TxHash> {
    let amount = to_pool_u64(deposit, "deposit")?;
    match contract
        .openPool(amount)
        .from(owner)
        .nonce(nonce)
        .send()
        .await
    {
        Ok(pending) => Ok(*pending.tx_hash()),
        Err(err) => {
            let reason = PoolOpenFailureReason::classify_revert_data(err.as_revert_data().as_ref());
            Err(anyhow::Error::new(err)
                .context(format!("re-send openPool at nonce {nonce}"))
                .context(reason))
        }
    }
}

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

/// Typed marker on a [`top_up`] error raised when the `topUp` may have been
/// broadcast but is not known to have mined: its submit failed in transport
/// (a timeout, a reset, an HTTP 5xx), or it was broadcast and its receipt was
/// not read (a failed read, or none within the caller's bound). An
/// [`UnconfirmedEscrows`] entry has the same meaning, and also holds a `topUp`
/// whose future was dropped before either. The transaction may still mine, so
/// the escrow may have moved while no local record credits it. A caller must
/// not treat it as "nothing escrowed"; the acquire loop classifies it as fatal
/// to the command ([`crate::Fault::Fatal`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TopUpUnconfirmed {
    /// The broadcast `topUp` transaction, which an operator reconciles
    /// against. `None` when the submit returned no hash (it failed in
    /// transport, or was dropped in flight): the RPC node may have broadcast
    /// the transaction without returning its hash.
    pub tx: Option<TxHash>,
    /// The nonce the `topUp` was sent with. Once the account's confirmed nonce
    /// passes it, the `topUp` has either mined or can never mine.
    pub nonce: u64,
}

impl std::fmt::Display for TopUpUnconfirmed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.tx {
            Some(tx) => write!(
                f,
                "topUp tx {tx} (nonce {}) was broadcast but its receipt was not read, so it \
                 may have mined; reconcile against the tx before retrying, because a retry \
                 escrows again",
                self.nonce
            ),
            None => write!(
                f,
                "topUp submit (nonce {}) returned no tx hash, so the RPC node may have \
                 broadcast it and it may mine; reconcile against the account's nonce before \
                 retrying, because a retry escrows again",
                self.nonce
            ),
        }
    }
}

impl std::error::Error for TopUpUnconfirmed {}

/// Typed marker on a failed top-up whose funder found the buyer's wallet
/// holding too little USDC for it. No retry fixes that, so the acquire loop
/// classifies it [`crate::Fault::Unaffordable`], and a funding recovery step
/// that fails with it ends the fetch "funding needed".
#[derive(Debug, Clone, Copy)]
pub struct WalletShortfall;

impl std::fmt::Display for WalletShortfall {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("the wallet holds too little USDC to top up the pool")
    }
}

impl std::error::Error for WalletShortfall {}

/// Typed marker on a [`top_up`] error whose gas estimate reverted
/// `PoolNotOpen`: the pool is `Closing` or `Closed`, so it accepts no funds and
/// is never reopened. The funding recovery step opens a new pool instead (ADR
/// 003 § Funding recovery).
#[derive(Debug, Clone, Copy)]
pub struct PoolNotOpen;

impl std::fmt::Display for PoolNotOpen {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("topUp reverted PoolNotOpen: the pool is closing or closed")
    }
}

impl std::error::Error for PoolNotOpen {}

/// Whether `pool_id` still accepts funds: the gas estimate of a one-unit
/// `topUp` from `owner`, with no transaction sent. A `PoolNotOpen` revert says
/// no. Any other estimate failure (an allowance or balance short of one unit,
/// a transport fault, a revert the decoder cannot read) says nothing about the
/// pool, so the pool's status is read instead: `Closing` or `Closed` says no.
/// When that read fails too, the pool reads as open, and the warning names
/// both failures.
///
/// The funding recovery step checks this when the pool already holds its
/// working deposit and there is nothing to add: a full pool that is `Closing`
/// or `Closed` still needs a replacement.
pub async fn pool_accepts_funds<P: Provider + Clone>(
    contract: &PaymentPool::PaymentPoolInstance<P>,
    owner: Address,
    pool_id: B256,
) -> bool {
    let estimate = match contract.topUp(pool_id, 1).from(owner).estimate_gas().await {
        Ok(_) => return true,
        Err(err) if decdn_incentive::is_pool_not_open(err.as_revert_data().as_ref()) => {
            return false;
        }
        Err(err) => err,
    };
    match contract.getPool(pool_id).call().await {
        Ok(pool) => !matches!(
            pool.status,
            PaymentPool::Status::Closing | PaymentPool::Status::Closed
        ),
        Err(read) => {
            tracing::warn!(
                %pool_id,
                error = %format_args!("topUp estimate: {estimate}; getPool: {read}"),
                "could not tell whether the buyer pool still accepts funds; treating it as open"
            );
            true
        }
    }
}

/// How many times [`top_up`] re-sends a submit rejected as a nonce collision.
const TOPUP_NONCE_RETRIES: u32 = 3;

/// How long [`top_up`] waits before re-sending after a nonce collision: long
/// enough for the colliding transaction to reach the pending pool, so the next
/// pending-nonce read skips past it.
const TOPUP_NONCE_BACKOFF: Duration = Duration::from_millis(250);

/// Send `topUp(pool_id, amount)` from `owner` at its pending nonce, re-sent on
/// a fresh nonce after a same-account nonce collision, and return the pending
/// transaction with the nonce it holds. The errors are [`top_up`]'s submit
/// errors.
///
/// `escrows` holds each submit while it is in flight, and keeps one that
/// failed in transport: either may have been broadcast with no hash returned.
/// A submit that returns a hash stays held for [`top_up`] to swap for the
/// broadcast `topUp`; a rejected one is released.
async fn send_top_up<P: Provider + Clone>(
    contract: &PaymentPool::PaymentPoolInstance<P>,
    owner: Address,
    pool_id: B256,
    amount: u64,
    escrows: &UnconfirmedEscrows,
) -> Result<(PendingTransactionBuilder<Ethereum>, u64)> {
    let mut nonce_retries = 0u32;
    loop {
        let nonce = contract
            .provider()
            .get_transaction_count(owner)
            .pending()
            .await
            .context("read the pending nonce for topUp")?;
        let call = contract.topUp(pool_id, amount).from(owner).nonce(nonce);
        let submitted = TopUpUnconfirmed { tx: None, nonce };
        // Held across the submit: a drop while it is in flight leaves a
        // `topUp` the RPC node may have broadcast with no hash returned.
        escrows.insert(submitted);
        let err = match call.send().await {
            Ok(pending) => return Ok((pending, nonce)),
            Err(err) => err,
        };
        if nonce_retries < TOPUP_NONCE_RETRIES && decdn_incentive::tx::is_nonce_collision(&err) {
            nonce_retries = nonce_retries.saturating_add(1);
            escrows.remove(submitted);
            debug!(
                %pool_id, nonce_retries,
                "topUp lost its nonce to a concurrent transaction; re-sending"
            );
            tokio::time::sleep(TOPUP_NONCE_BACKOFF).await;
            continue;
        }
        // A transport failure may have reached the node and broadcast the
        // `topUp` before the response was lost, so a retry may escrow twice.
        if decdn_incentive::tx::send_broadcast_unknown(&err) {
            return Err(anyhow::Error::new(err)
                .context("submit topUp")
                .context(submitted));
        }
        escrows.remove(submitted);
        return Err(top_up_submit_error(err, "submit topUp"));
    }
}

/// Wrap a failed `topUp` submit in `context`, marked by its revert.
///
/// A deterministic allowance shortfall is caught at gas estimation and
/// surfaces with ABI revert data attached. It is marked [`AllowanceShortfall`]
/// so the node can re-`approve` and retry once; every other revert stays
/// terminal (an `approve` cannot fix a balance shortfall or a paused pool). A
/// pool that is no longer open reverts `PoolNotOpen` the same way and is marked
/// [`PoolNotOpen`], which the funding recovery step answers with a new pool.
fn top_up_submit_error(err: alloy::contract::Error, context: impl Into<String>) -> anyhow::Error {
    let revert_data = err.as_revert_data();
    let allowance_short = decdn_incentive::is_erc20_allowance_shortfall(revert_data.as_ref());
    let not_open = decdn_incentive::is_pool_not_open(revert_data.as_ref());
    let submit_err = anyhow::Error::new(err).context(context.into());
    if allowance_short {
        submit_err.context(AllowanceShortfall)
    } else if not_open {
        submit_err.context(PoolNotOpen)
    } else {
        submit_err
    }
}

/// Send `topUp(pool_id, additional)` from `owner` at exactly `nonce`, and
/// return its hash without waiting for a receipt.
///
/// A caller resolving a [`TopUpUnconfirmed`] whose nonce no transaction holds
/// re-sends with that same nonce. Two transactions at one nonce cannot both
/// mine, so the re-send and a first `topUp` that resurfaces escrow at most one
/// top-up between them. A receipt wait here would never end if the other
/// transaction took the nonce, so the caller reads receipts itself
/// ([`topped_up_from_receipt`]).
///
/// # Errors
///
/// The amount overflows `uint64`, or the send fails. A revert carries the
/// markers [`top_up`]'s submit errors carry ([`AllowanceShortfall`],
/// [`PoolNotOpen`]). A transport failure is not marked [`TopUpUnconfirmed`]:
/// the caller already holds the nonce, and reads it again to learn the
/// outcome.
pub async fn send_top_up_at<P: Provider + Clone>(
    contract: &PaymentPool::PaymentPoolInstance<P>,
    owner: Address,
    pool_id: B256,
    additional: U256,
    nonce: u64,
) -> Result<TxHash> {
    let amount = to_pool_u64(additional, "top-up")?;
    match contract
        .topUp(pool_id, amount)
        .from(owner)
        .nonce(nonce)
        .send()
        .await
    {
        Ok(pending) => Ok(*pending.tx_hash()),
        Err(err) => Err(top_up_submit_error(
            err,
            format!("re-send topUp at nonce {nonce}"),
        )),
    }
}

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
/// `owner` is the pool owner, which signs the `topUp` (`topUp` is owner-only
/// on-chain). `top_up` reads `owner`'s pending nonce and sends the `topUp` with
/// it, so an unconfirmed `topUp` names the nonce it holds. The provider's own
/// nonce manager does not see that nonce, so `contract`'s provider must read
/// the pending nonce on every send (alloy's `with_simple_nonce_management`).
///
/// A submit rejected as a same-account nonce collision
/// ([`decdn_incentive::tx::is_nonce_collision`]) broadcast nothing, so it is
/// re-sent up to three more times, a quarter second apart, each time on a fresh
/// pending nonce.
/// A node signs its sellers' redemptions and its buyer top-ups with one key, so a
/// top-up can lose its nonce to a redemption sent at the same moment.
///
/// `receipt_timeout` bounds the receipt wait when it is `Some`, as for
/// [`open_pool`]. A caller that bounds the wait must not send another `topUp`
/// on the pool while this one is unconfirmed. The run funding of `decdn get`
/// refuses the rest while its `escrows` holds one
/// ([`crate::funding::RunFunding`]). The node keeps its top-up slot and settles
/// the unconfirmed `topUp` against the chain ([`send_top_up_at`],
/// [`topped_up_from_receipt`]).
///
/// `escrows` holds the `topUp` from before its submit until its receipt is
/// read, as [`open_pool`] holds an `openPool`: with no hash while the submit
/// is in flight or failed in transport, with the hash after. A drop, a failed
/// read or a lapse leaves the entry, so a caller that may drop the future or
/// its error passes `Some` and reads [`UnconfirmedEscrows::pending`] when it
/// ends. `top_up` itself refuses nothing on what `escrows` holds.
///
/// # Errors
///
/// Errors if the nonce read or the `topUp` transaction fails (submit, revert, or
/// receipt). A submit that fails in transport
/// ([`decdn_incentive::tx::send_broadcast_unknown`]) and a receipt that is not
/// read (a failed read, or none within `receipt_timeout`) both carry
/// [`TopUpUnconfirmed`], because the `topUp` may still mine.
pub async fn top_up<P: Provider + Clone>(
    contract: &PaymentPool::PaymentPoolInstance<P>,
    owner: Address,
    pool_id: B256,
    additional: U256,
    escrows: Option<&UnconfirmedEscrows>,
    receipt_timeout: Option<Duration>,
) -> Result<ToppedUpPool> {
    let amount = to_pool_u64(additional, "top-up")?;
    // A caller with no handle holds the entries in one nobody reads.
    let escrows = escrows.cloned().unwrap_or_default();
    let (pending, nonce) = send_top_up(contract, owner, pool_id, amount, &escrows).await?;
    // Capture the hash before the wait consumes `pending`: past this point the
    // transaction is broadcast and may mine, so every receipt failure names it
    // (see [`TopUpUnconfirmed`]). A lapse does not cancel the transaction.
    let tx = *pending.tx_hash();
    info!(%tx, %owner, %pool_id, %additional, nonce, "topUp broadcast; awaiting its receipt");
    let unconfirmed = TopUpUnconfirmed {
        tx: Some(tx),
        nonce,
    };
    escrows.replace(TopUpUnconfirmed { tx: None, nonce }, unconfirmed);
    // Only a receipt releases the entry: a drop here, a failed read or a
    // lapse leaves it.
    let receipt = await_receipt(pending, receipt_timeout)
        .await
        .map_err(|err| err.context("await topUp receipt").context(unconfirmed))?;
    escrows.remove(unconfirmed);
    topped_up_from_receipt(&receipt, *contract.address(), pool_id, additional)
}

/// Build the [`ToppedUpPool`] a `topUp` tx bought, from its mined `receipt`.
///
/// [`top_up`] calls this once its receipt arrives. A caller that holds a
/// [`TopUpUnconfirmed`] calls it with the receipt it read later, so a `topUp`
/// whose first receipt read failed is credited like any other. `payment_pool`
/// is the `PaymentPool` the `topUp` was sent to, and `additional` the amount it
/// requested. See [`top_up`] for how the credited amount is read.
///
/// # Errors
///
/// The tx reverted: nothing was escrowed.
pub fn topped_up_from_receipt(
    receipt: &alloy::rpc::types::TransactionReceipt,
    payment_pool: Address,
    pool_id: B256,
    additional: U256,
) -> Result<ToppedUpPool> {
    let tx = receipt.transaction_hash;
    if !receipt.status() {
        anyhow::bail!("topUp reverted for pool {pool_id} (tx {tx:#x})");
    }
    // Filtered on the emitting address, not just the topic: `topUp` transfers
    // BEFORE it emits, so a token with a transfer hook could plant a forged
    // `PoolToppedUp` earlier in the same receipt and `find` would prefer it.
    let credited = receipt
        .inner
        .logs()
        .iter()
        .filter(|log| log.address() == payment_pool)
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
    info!(%pool_id, %credited, %tx, "topped up buyer payment pool");
    Ok(ToppedUpPool { credited, tx })
}

/// Refill a reused buyer pool to `target_deposit` once its remaining spendable
/// deposit falls below `1/N` of the configured working deposit.
/// `5` → refill triggers below 20% remaining, then restores to a full deposit.
///
/// Shared by the CLI fetch auto-refill (#1103) and the node's node-to-node reuse
/// path (#1146), so the refill policy has a single source of truth.
#[doc(hidden)]
pub const LOW_WATER_DIVISOR: u64 = 5;

/// Decide how much USDC to add to a reused pool so a sustained series of fetches
/// isn't stranded by a spent-down deposit (#1103).
///
/// Pure decision (no I/O) so the policy is unit-testable. `deposit` is the pool's
/// current on-chain deposit and `prior_amount` the pool-wide cumulative amount
/// already vouchered — the sum across all its lanes, not the lane being built —
/// so the remaining spendable is
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
#[doc(hidden)]
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
mod tests;
