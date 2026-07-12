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
use decdn_incentive::erc20::Erc20;
use decdn_incentive::payment_channel::PaymentChannel;
use decdn_incentive::{BuyerChannelState, ChannelOpenFailureReason};
use tracing::{debug, info};

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
/// (`iroh_metrics` has no label support, so each class is its own counter) without
/// re-parsing the alloy error: a deterministic revert (with ABI revert data,
/// decoded against the insufficient-deposit error selectors) is split from a
/// transport/RPC fault (no revert data) and a mined on-chain revert. The
/// missing-`ChannelOpened` leg carries no reason — the deposit is escrowed but
/// untracked, so it surfaces as an unclassified error for manual reconciliation
/// rather than a metric bump.
pub async fn open_channel<P: Provider + Clone>(
    contract: &PaymentChannel::PaymentChannelInstance<P>,
    signer: Arc<PrivateKeySigner>,
    voucher_domain: &Eip712Domain,
    token: Address,
    self_address: Address,
    provider_addr: Address,
    deposit: U256,
) -> Result<OpenedChannel> {
    let pending = match contract.openChannel(provider_addr, deposit).send().await {
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
        anyhow::bail!(
            "ChannelOpened event for provider {provider_addr} not found in openChannel receipt \
             logs (tx {tx}); the deposit is escrowed on-chain but untracked — reconcile manually"
        );
    };
    let channel_id = opened.channelId;
    // `ChannelOpened.expiresAt` is `uint256`; clamp to `u64` (a too-far expiry
    // only ever means a reclaim sweep waits longer).
    let expires_at = u64::try_from(opened.expiresAt).unwrap_or(u64::MAX);

    let state = BuyerChannelState::new(channel_id, provider_addr, token, deposit, expires_at);
    let ctx = ChannelContext::for_buyer_channel(&state, signer, voucher_domain.clone());
    info!(provider = %provider_addr, %channel_id, %deposit, expires_at, "opened buyer payment channel");
    Ok(OpenedChannel { state, ctx, tx })
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic
)]
mod tests {
    use super::{approval_floor, approve_decision};
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
}
