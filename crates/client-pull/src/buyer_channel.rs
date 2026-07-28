//! Buyer-side `PaymentChannel` open kernel, shared by the node's
//! `BuyerChannelService` (node-to-node miss pulls, #744) and the CLI (`decdn
//! fetch`, #940).
//!
//! The genuinely duplication-prone part — the `openChannel` transaction, the
//! authoritative `ChannelOpened`-from-receipt decode, and the
//! [`BuyerChannelState`] / [`ChannelContext`] construction — lives here. Channel
//! *reuse* and watermark *recording* are thin compositions over
//! [`decdn_incentive::BuyerChannelStore`] (`get_by_provider`, `advance_progress`)
//! that each caller does directly: a one-shot CLI fetch needs neither the node
//! service's per-provider concurrency guard nor its background reclaim/reconcile
//! machinery, so only the open kernel is shared.

use std::sync::Arc;
use std::time::Duration;

use alloy::dyn_abi::Eip712Domain;
use alloy::primitives::{Address, TxHash, U256};
use alloy::providers::Provider;
use alloy::signers::local::PrivateKeySigner;
use anyhow::{Context, Result};
use decdn_incentive::buyer_channel::{BuyerChannelStore, DepositOutcome};
use decdn_incentive::erc20::Erc20;
use decdn_incentive::payment_channel::PaymentChannel;
use decdn_incentive::{BuyerChannelState, ChannelOpenFailureReason};
use tracing::{debug, error, info, warn};

use crate::ChannelContext;

/// Re-approve the `PaymentChannel` spender for the *unlimited* case when the
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
/// underpriced tx can't wedge the buyer lane (#1109 — ~27 min observed on a live
/// node). Sized well above a normal inclusion window but short enough that the
/// worst case is a few minutes. On timeout the broadcast tx may still mine
/// later; the allowance read at the top of the next run makes the re-approve
/// idempotent, so no funds are stranded. Not config-tunable yet (YAGNI), like
/// the `RECONCILE_IDLE_SWEEPS` convention in the node's `buyer_channel`.
const APPROVE_RECEIPT_TIMEOUT: Duration = Duration::from_mins(3);

/// A freshly opened buyer channel: the persistable [`BuyerChannelState`], a
/// ready-to-sign [`ChannelContext`], and the open transaction hash (so a caller
/// whose subsequent `store.record` fails can log the escrowed-but-untracked tx
/// for manual reconciliation — the deposit is on-chain the moment `openChannel`
/// mines).
#[derive(Debug)]
pub struct OpenedChannel {
    /// Persist this via `BuyerChannelStore::record`.
    pub state: BuyerChannelState,
    /// Use this to sign vouchers for the new channel.
    pub ctx: ChannelContext,
    /// The `openChannel` transaction hash.
    pub tx: TxHash,
}

/// Ensure `owner` holds a sufficient USDC allowance for the `PaymentChannel`
/// `spender`, issuing at most one `approve` when the current allowance is below
/// what `amount` requires. `amount == None` grants an unlimited
/// (`U256::MAX`) allowance — the node/operator posture that avoids re-approve
/// churn; `amount == Some(deposit)` grants exactly `deposit` — the client
/// default that keeps the standing spend authority scoped to the deposit being
/// escrowed. See `approve_decision` for the skip logic. Idempotent across runs
/// — a wallet already at or above the required allowance skips the tx.
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
        "issued USDC approval for PaymentChannel deposits"
    );
    Ok(())
}

/// Open a fresh `PaymentChannel` against `provider_addr`, escrowing `deposit`
/// USDC, and decode the authoritative `channelId` + `expiresAt` from the
/// receipt's own `ChannelOpened` event.
///
/// Decoding from the receipt (rather than a follow-up `getChannel`) is atomic
/// with the open: a mined tx guarantees the event, so the caller can always
/// persist — a transient read failure can never leave the deposit orphaned
/// (opened-but-untracked). The event is filtered on `client == self_address`
/// and `provider == provider_addr` to confirm we decoded our own open.
///
/// `deposit` is the final escrow amount — the caller applies any
/// `max(hint, default, min_deposit)` clamping before calling. The returned
/// [`OpenedChannel`] is **not yet persisted**: the caller records
/// [`OpenedChannel::state`] in its `BuyerChannelStore`, and on a record failure
/// should log [`OpenedChannel::tx`] (the deposit is escrowed on-chain).
///
/// # Errors
///
/// Fails on `openChannel` submit/receipt, a reverted tx, or a missing
/// `ChannelOpened` event in the receipt logs. The classified failure legs
/// (submit, receipt wait, mined revert) attach a [`ChannelOpenFailureReason`]
/// into the `anyhow` error chain (recover it with
/// `err.downcast_ref::<ChannelOpenFailureReason>()`), so a caller can bump the
/// matching `decdn_channel_open_failures_{reason}_total` sibling counter
/// (a plain counter field carries no label dimension, so each class is its own counter) without
/// re-parsing the alloy error: a deterministic revert (with ABI revert data,
/// decoded against the insufficient-deposit error selectors) is split from a
/// transport/RPC fault (no revert data) and a mined on-chain revert. The
/// missing-`ChannelOpened` leg carries no reason — the deposit is escrowed but
/// untracked, so it surfaces as an unclassified error for manual reconciliation
/// rather than a metric bump.
///
/// # The receipt wait is deliberately UNBOUNDED
///
/// `ensure_allowance`'s `approve` bounds its receipt wait (`APPROVE_RECEIPT_TIMEOUT`).
/// `openChannel` must not — and neither does `top_up`, the module's third tx, which awaits
/// its receipt unbounded for exactly the reason below. The line is not "one tx is special";
/// it is ESCROWING vs idempotent, and it is load-bearing rather than an oversight (#1143).
///
/// `approve` is idempotent: giving up on its receipt costs nothing, because the
/// allowance read on the next run makes a re-approve a no-op. `openChannel` and `top_up`
/// **escrow funds**. Giving up on the receipt does not cancel the tx — it only
/// makes us stop watching a transfer of real USDC that is still in the mempool. The
/// caller then has no row, believes no open is in flight, and the next cache miss
/// escrows a **second** deposit against the same provider. When the first tx mines,
/// the boot reconcile scan finds a live row already covering that provider and
/// classifies the orphan `DeferredSecondOpen` — it declines to adopt it, and the
/// deposit is stranded for the channel's full expiry.
///
/// So: while an `openChannel` is outstanding, the only safe thing to do is keep
/// waiting. The node calls this inside a DETACHED task that holds the provider's
/// open slot for exactly as long as this future runs, which is what makes the wait
/// harmless — no caller is blocked by it (they time out on their own budget and get
/// `ChannelOpenPending`), and no second open can start behind it. It is also what
/// lets the boot scan do its job: with no second open, there is no live row, so an
/// orphan is `Rehydrate`d rather than deferred.
pub async fn open_channel<P: Provider + Clone>(
    contract: &PaymentChannel::PaymentChannelInstance<P>,
    signer: Arc<PrivateKeySigner>,
    voucher_domain: &Eip712Domain,
    token: Address,
    self_address: Address,
    provider_addr: Address,
    deposit: U256,
) -> Result<OpenedChannel> {
    // Self-signing open: a zero `voucherSigner` resolves on-chain to `msg.sender`,
    // which is this buyer's own key — the same behaviour as before the signer
    // split. Pinning a *delegate* signer here is deliberately out of scope: it
    // needs a publisher-pays UX (who holds the delegate key, how it is rotated,
    // how the funder authorizes it), not just an extra argument.
    let pending = match contract
        .openChannel(provider_addr, deposit, Address::ZERO)
        .send()
        .await
    {
        Ok(pending) => pending,
        Err(err) => {
            // A deterministic revert (deposit below `minDeposit`, USDC
            // balance/allowance too low, provider inactive, …) is caught at gas
            // estimation, so it surfaces here with ABI revert data attached;
            // a send error *without* revert data is a transport/RPC fault.
            let reason =
                ChannelOpenFailureReason::classify_revert_data(err.as_revert_data().as_ref());
            return Err(anyhow::Error::new(err))
                .context("submit openChannel")
                .context(reason);
        }
    };
    // Unbounded by design — see the `# The receipt wait is deliberately UNBOUNDED`
    // section above. A `tokio::time::timeout` here would be worse than no bound at
    // all: it abandons an escrowing tx that is still going to land.
    let receipt = pending
        .get_receipt()
        .await
        // A failed receipt wait is always a transport/RPC condition (the tx may
        // even have landed) — never a settlement decision.
        .map_err(|err| {
            anyhow::Error::new(err)
                .context("await openChannel receipt")
                .context(ChannelOpenFailureReason::RpcError)
        })?;
    if !receipt.status() {
        // A mined revert: the revert reason is not recoverable from the receipt
        // (no trace), so it is classified as a generic on-chain revert. Most
        // insufficient-deposit cases are caught at gas estimation above, but
        // because balance/allowance/`minDeposit` state can change between
        // estimation and mining, a mined revert *could* still be
        // insufficient-deposit — it just can't be distinguished here, so it
        // folds into `ContractRevert`.
        return Err(anyhow::anyhow!(
            "openChannel reverted (provider {provider_addr}, deposit {deposit}); check USDC \
             balance/allowance and that the provider is active"
        )
        .context(ChannelOpenFailureReason::ContractRevert));
    }
    let tx = receipt.transaction_hash;

    let Some(opened) = receipt
        .inner
        .logs()
        .iter()
        .filter_map(|log| log.log_decode::<PaymentChannel::ChannelOpened>().ok())
        .map(|decoded| decoded.inner.data)
        .find(|ev| ev.client == self_address && ev.provider == provider_addr)
    else {
        // The deposit is escrowed on-chain and we cannot name the channel it bought.
        // Log it HERE rather than leaving it to the caller: the node's caller is a
        // detached task whose `Err` nobody may be waiting on (#1143), so a bare
        // `bail!` could lose the only record of real, escrowed USDC.
        error!(
            %tx,
            provider = %provider_addr,
            %deposit,
            "openChannel mined but its ChannelOpened event is missing from the receipt logs; \
             the deposit is escrowed on-chain but UNTRACKED — reconcile manually against the tx"
        );
        anyhow::bail!(
            "ChannelOpened event for provider {provider_addr} not found in openChannel receipt \
             logs (tx {tx}); the deposit is escrowed on-chain but untracked — reconcile manually"
        );
    };
    let channel_id = opened.channelId;
    // `ChannelOpened.expiresAt` is `uint256`; clamp to `u64` (a too-far expiry
    // only ever means a reclaim sweep waits longer).
    let expires_at = u64::try_from(opened.expiresAt).unwrap_or(u64::MAX);

    // Self-signing open (see the `voucherSigner` comment above): the funder and
    // voucher signer are both this buyer's own key.
    let state = BuyerChannelState::new(
        channel_id,
        provider_addr,
        self_address,
        self_address,
        token,
        deposit,
        expires_at,
    );
    let ctx = ChannelContext::for_buyer_channel(&state, signer, voucher_domain.clone());
    info!(provider = %provider_addr, %channel_id, %deposit, expires_at, "opened buyer payment channel");
    Ok(OpenedChannel { state, ctx, tx })
}

/// Add `additional` USDC to the buyer channel tracked for `provider_addr` and
/// reconcile the persisted deposit — the shared mechanism behind the node's
/// cache-miss buyer (`BuyerChannelService::top_up`, #744) and the CLI fetch
/// buyer's auto-refill (#1103). `topUp` does not extend `expiresAt` (the
/// contract forbids it), so callers rotate a near-expiry channel rather than
/// top it up.
///
/// The `channelId` is read from the store *before* the RPC (it is needed both to
/// call `topUp` and as the channel-id guard on the post-RPC write). After the
/// receipt lands, the committed deposit is credited via
/// [`BuyerChannelStore::add_deposit`], which reads the deposit inside its own
/// write transaction (never this pre-call snapshot) and channel-id-guards it, so
/// a concurrent watermark advance or channel rotation during the RPC is not
/// clobbered. USDC is not fee-on-transfer, so the local `+= additional` matches
/// the contract's `+= received`.
///
/// # Returns
///
/// The [`DepositOutcome`] of crediting the local row: `Added` on the clean path,
/// or `UnknownProvider` / `ChannelMismatch` when the on-chain `topUp` landed but
/// the local record vanished or was replaced during the RPC (funds escrowed
/// on-chain, untracked locally — reconcile against the tx). The caller decides how
/// to grade those: they are `Ok` here (the deposit is safe on-chain), but a caller
/// that meters success separately should NOT count them as a clean top-up (#1146).
///
/// # Errors
///
/// Errors if no channel is tracked for `provider_addr` *before* the RPC, or if
/// the `topUp` transaction fails (submit, revert, or receipt). The
/// escrowed-but-untracked outcomes above are returned as `Ok`, not errors — the
/// funds are already escrowed on-chain against the topped-up channel, so failing
/// here would not unwind them.
pub async fn top_up<P, S>(
    contract: &PaymentChannel::PaymentChannelInstance<P>,
    store: &S,
    provider_addr: Address,
    additional: U256,
) -> Result<DepositOutcome>
where
    P: Provider + Clone,
    S: BuyerChannelStore + ?Sized,
{
    // Read the channel_id BEFORE the RPC — needed for `topUp` and as the
    // channel-id guard on the post-RPC write.
    let channel_id = store
        .get_by_provider(provider_addr)
        .context("look up buyer channel for top-up")?
        .with_context(|| format!("top_up for unknown provider {provider_addr}"))?
        .channel_id;
    let receipt = contract
        .topUp(channel_id, additional)
        .send()
        .await
        .context("submit topUp")?
        .get_receipt()
        .await
        .context("await topUp receipt")?;
    if !receipt.status() {
        anyhow::bail!("topUp reverted for channel {channel_id}");
    }
    let tx = receipt.transaction_hash;
    // Credit the *committed* deposit inside a write txn (never the pre-RPC
    // snapshot), channel-id-guarded so a concurrent advance/reuse during the RPC
    // is not clobbered.
    let outcome = store
        .add_deposit(provider_addr, channel_id, additional)
        .context("persist buyer channel top-up")?;
    match &outcome {
        DepositOutcome::Added(new_deposit) => {
            info!(
                provider = %provider_addr,
                %channel_id,
                %new_deposit,
                %tx,
                "topped up buyer channel"
            );
        }
        // The on-chain topUp already credited `channel_id`, but the local row
        // vanished during the RPC. Funds are escrowed on-chain with zero local
        // tracking — `error!` (matching the open path's escrowed-but-untracked
        // posture) and surface the tx for reconcile.
        DepositOutcome::UnknownChannel => {
            error!(
                provider = %provider_addr,
                %channel_id,
                %additional,
                %tx,
                "top_up: on-chain topUp landed but no local channel record exists to credit; \
                 deposit is escrowed on-chain and untracked — reconcile against the tx"
            );
        }
        // A row still exists (for a different channel), so the provider stays
        // reclaimable — less severe than `UnknownProvider`, hence `warn!`.
        DepositOutcome::ChannelMismatch => {
            warn!(
                provider = %provider_addr,
                %channel_id,
                %additional,
                %tx,
                "top_up: provider channel replaced during the topUp RPC; the on-chain deposit \
                 was credited to the topped-up channel but the local record now tracks a \
                 different channel — reconcile against the tx"
            );
        }
    }
    Ok(outcome)
}

/// Refill a reused buyer channel to `target_deposit` once its remaining spendable
/// deposit falls below `1/N` of the configured working deposit.
/// `5` → refill triggers below 20% remaining, then restores to a full deposit.
///
/// Shared by the CLI fetch auto-refill (#1103) and the node's node-to-node reuse
/// path (#1146), so the refill policy has a single source of truth.
pub const LOW_WATER_DIVISOR: u64 = 5;

/// Decide how much USDC to add to a reused channel so a sustained series of
/// fetches against one provider isn't stranded by a spent-down deposit (#1103).
///
/// Pure decision (no I/O) so the policy is unit-testable. `deposit` is the
/// channel's current on-chain deposit and `prior_amount` the cumulative amount
/// already vouchered, so the remaining spendable is `deposit - prior_amount`.
/// When that remaining balance has fallen below `low_water`, return the top-up
/// that restores it to `target_deposit` (the configured working deposit);
/// otherwise return `U256::ZERO` (no refill).
///
/// The exact cost of the *next* fetch is not known at refill time on either
/// caller — the per-MB `rate` is only learned from the provider's probe /
/// `StreamResponse` (and the CLI's explicit `--node-id` path does no probe; the
/// node fires this on channel reuse, before any `StreamResponse`) — so this uses a
/// rate-independent low-water refill: keep at least `low_water` of headroom, and
/// refill to a full `target_deposit` when it runs low. Hysteresis
/// (`low_water < target_deposit`) keeps a busy channel from topping up on every
/// reuse. `topUp` does not extend expiry, so an already-expired channel is replaced
/// (not refilled) by the caller.
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
    use super::{LOW_WATER_DIVISOR, approval_floor, approve_decision, refill_amount};
    use alloy::primitives::U256;

    #[test]
    fn unlimited_zero_allowance_approves_max() {
        assert_eq!(approve_decision(U256::ZERO, None), Some(U256::MAX));
    }

    #[test]
    fn unlimited_sufficient_skips() {
        // At exactly the floor the existing (max) approval is honored.
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
        // A wallet that previously granted an unlimited allowance never gets
        // re-approved downward when the client switches to exact mode.
        let deposit = U256::from(10_000_000u64);
        assert_eq!(approve_decision(U256::MAX, Some(deposit)), None);
    }

    // ---- auto-refill decision (#1103, #1146) ----------------------------

    // Configured working deposit + its derived low-water mark, mirroring what
    // the reuse paths pass (`low_water = target / LOW_WATER_DIVISOR`).
    fn target() -> U256 {
        U256::from(10_000_000u64) // 10 USDC
    }
    fn low_water() -> U256 {
        target() / U256::from(LOW_WATER_DIVISOR) // 2 USDC (20%)
    }

    #[test]
    fn refill_amount_no_top_up_when_remaining_at_or_above_low_water() {
        // Fresh channel (nothing spent): remaining == deposit == target.
        assert_eq!(
            refill_amount(target(), U256::ZERO, target(), low_water()),
            U256::ZERO,
            "a full channel must not be topped up"
        );
        // Spent down to exactly the low-water mark: still sufficient (>=).
        let prior = target() - low_water(); // remaining == low_water
        assert_eq!(
            refill_amount(target(), prior, target(), low_water()),
            U256::ZERO,
            "remaining exactly at the low-water mark is still sufficient"
        );
    }

    #[test]
    fn refill_amount_restores_to_target_when_low() {
        // Spent so remaining is just below the low-water mark.
        let remaining = low_water() - U256::from(1u64);
        let prior = target() - remaining;
        assert_eq!(
            refill_amount(target(), prior, target(), low_water()),
            target() - remaining,
            "refill must restore the remaining deposit back up to the target"
        );

        // Nearly drained: remaining ~0 → top up ~a full target's worth.
        let prior_drained = target() - U256::from(1u64); // remaining == 1
        assert_eq!(
            refill_amount(target(), prior_drained, target(), low_water()),
            target() - U256::from(1u64),
        );
    }

    #[test]
    fn refill_amount_has_hysteresis_after_a_prior_top_up() {
        // A channel that was already topped up (on-chain deposit == 2*target)
        // and has spent back down to just above low-water must NOT top up again.
        let deposit = target() * U256::from(2u64);
        let prior = deposit - low_water(); // remaining == low_water
        assert_eq!(
            refill_amount(deposit, prior, target(), low_water()),
            U256::ZERO,
            "a topped-up channel with headroom must not refill on every reuse"
        );
    }

    #[test]
    fn refill_amount_saturates_and_never_underflows() {
        // Pathological: prior_amount above deposit (never happens on-chain, but
        // the math must not panic under the anti-panic policy) → remaining 0.
        assert_eq!(
            refill_amount(target(), target() * U256::from(3u64), target(), low_water()),
            target(),
            "remaining saturates to zero, so refill is a full target"
        );
        // Low-water above target (misconfiguration): remaining below low-water
        // but at/above target → nothing to add (saturating).
        let deposit = target() * U256::from(2u64);
        assert_eq!(
            refill_amount(deposit, U256::ZERO, target(), deposit),
            U256::ZERO,
            "remaining already >= target yields no top-up even below low-water"
        );
    }
}
