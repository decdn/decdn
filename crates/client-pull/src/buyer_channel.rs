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

use alloy::dyn_abi::Eip712Domain;
use alloy::primitives::{Address, TxHash, U256};
use alloy::providers::Provider;
use alloy::signers::local::PrivateKeySigner;
use anyhow::{Context, Result};
use decdn_incentive::BuyerChannelState;
use decdn_incentive::erc20::Erc20;
use decdn_incentive::payment_channel::PaymentChannel;
use tracing::{debug, info};

use crate::ChannelContext;

/// Re-approve the `PaymentChannel` spender when the standing USDC allowance has
/// fallen below this floor. Half of `U256::MAX` so one max approval covers
/// effectively unlimited deposits and a re-run with the approval already in
/// place skips the redundant `approve`, while a never-approved wallet (allowance
/// `0`) trips it.
fn approval_floor() -> U256 {
    U256::MAX >> 1
}

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

/// Ensure `owner` holds a standing USDC allowance for the `PaymentChannel`
/// `spender`, issuing a single `approve(spender, U256::MAX)` only when the
/// current allowance is below `approval_floor`. Idempotent across runs — a
/// wallet that has approved before skips the tx. Mirrors the node's one-time
/// approval at bring-up.
///
/// # Errors
///
/// Fails on the allowance read, the `approve` submission, or a reverted approve.
pub async fn ensure_allowance<P: Provider + Clone>(
    provider: &P,
    token: Address,
    owner: Address,
    spender: Address,
) -> Result<()> {
    let erc20 = Erc20::new(token, provider.clone());
    let current = erc20
        .allowance(owner, spender)
        .call()
        .await
        .context("read USDC allowance")?;
    if current >= approval_floor() {
        debug!(%current, "USDC allowance already sufficient; skipping approve");
        return Ok(());
    }
    let receipt = erc20
        .approve(spender, U256::MAX)
        .send()
        .await
        .context("submit USDC approve")?
        .get_receipt()
        .await
        .context("await USDC approve receipt")?;
    if !receipt.status() {
        anyhow::bail!("USDC approve transaction reverted");
    }
    info!(%token, %spender, "issued one-time max USDC approval for PaymentChannel deposits");
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
/// `ChannelOpened` event in the receipt logs.
pub async fn open_channel<P: Provider + Clone>(
    contract: &PaymentChannel::PaymentChannelInstance<P>,
    signer: Arc<PrivateKeySigner>,
    voucher_domain: &Eip712Domain,
    token: Address,
    self_address: Address,
    provider_addr: Address,
    deposit: U256,
) -> Result<OpenedChannel> {
    let receipt = contract
        .openChannel(provider_addr, deposit)
        .send()
        .await
        .context("submit openChannel")?
        .get_receipt()
        .await
        .context("await openChannel receipt")?;
    if !receipt.status() {
        anyhow::bail!(
            "openChannel reverted (provider {provider_addr}, deposit {deposit}); check USDC \
             balance/allowance and that the provider is active"
        );
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
