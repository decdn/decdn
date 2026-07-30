//! `decdn channel` — client-side payment-channel lifecycle.
//!
//! `coop-close` cooperatively settles the channel tracked for a provider
//! (#971): it dials the provider over `cdn/client/v1`, requests a
//! `CooperativeClose` waiver over the channel's final watermark, and submits
//! `cooperativeClose` on-chain — one tx, no 48h dispute window (ADR 003
//! §Cooperative close). The reusable request→verify→submit core lives in
//! [`decdn_client_pull::cooperative_close`]; this command is the thin
//! flag/store/endpoint wiring around it. The node→node buyer reconcile (#972)
//! drives the same core on a timer.
//!
//! If the provider has no channel / no accepted voucher it declines and the
//! channel must be closed the ordinary way (`closeChannel` → dispute window →
//! `settleChannel`); auto-fallback is out of scope here.

use std::io::Write;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use alloy::dyn_abi::Eip712Domain;
use alloy::primitives::{Address, B256, Bytes, U256};
use alloy::providers::Provider;
use alloy::signers::local::PrivateKeySigner;
use decdn_client_pull::buyer_channel::{ensure_allowance, open_channel};
use decdn_client_pull::cooperative_close::{
    AuthorizedWatermark, CooperativeCloseOutcome, cooperative_close,
};
use decdn_common::cli::{self, common::expand_tilde};
use decdn_common::config::{DEFAULT_CHAIN_ID, FileConfig, load_file_config};
use decdn_incentive::buyer_channel::{
    AdvanceOutcome, BuyerChannelState, BuyerChannelStore, BuyerLoad,
};
use decdn_incentive::buyer_channel_redb::RedbBuyerChannelStore;
use decdn_incentive::eth_identity::{self, PasswordSource, load_signer, read_password};
use decdn_incentive::payment_channel::PaymentChannel;
use decdn_incentive::{ChannelId, Voucher, voucher_domain};
use iroh::{EndpointAddr, PublicKey};
use serde::Serialize;

use super::chain_ctx;
use decdn_client_pull::endpoint as client_endpoint;
use decdn_client_pull::provider;

/// Dispatch `decdn channel <subcommand>`.
pub async fn channel_dispatch(
    args: &cli::ChannelArgs,
    config_path: Option<&Path>,
) -> anyhow::Result<()> {
    match &args.command {
        cli::ChannelCommand::List(a) => list(a, config_path),
        cli::ChannelCommand::CoopClose(a) => coop_close(a, config_path).await,
        cli::ChannelCommand::Close(a) => close(a, config_path).await,
        cli::ChannelCommand::Settle(a) => settle(a, config_path).await,
        cli::ChannelCommand::Clean(a) => clean(a, config_path).await,
        cli::ChannelCommand::Open(a) => open(a, config_path).await,
    }
}

/// Chain coordinates resolved flag > `[blockchain]`/`[identity]` config >
/// default. Pure (parse-only) so the precedence is unit-testable.
#[derive(Debug)]
struct ResolvedChain {
    rpc_url: String,
    payment_channel: Address,
    chain_id: u64,
    data_dir: PathBuf,
    keystore: PathBuf,
}

/// Resolve the buyer-store data dir: flag > `[identity]` config > client-scoped
/// `~/.decdn/client` default. Shared by the read-only `list` path (which needs
/// nothing else) and [`resolve_chain`].
fn resolve_data_dir(data_dir: Option<PathBuf>, file: &FileConfig) -> anyhow::Result<PathBuf> {
    // Mirror the fetch client's path resolution: an explicit data dir wins,
    // otherwise the client-scoped `~/.decdn/client` home.
    data_dir
        .or_else(|| file.identity.as_ref().and_then(|i| i.data_dir.clone()))
        .map(|p| expand_tilde(&p))
        .or_else(cli::default_client_data_dir)
        .ok_or_else(|| {
            anyhow::anyhow!("data_dir not set and no default available (pass --data-dir)")
        })
}

fn resolve_chain(args: &cli::ChannelChainArgs, file: &FileConfig) -> anyhow::Result<ResolvedChain> {
    let bc = file.blockchain.as_ref();
    let rpc_url = args
        .rpc_url
        .clone()
        .or_else(|| bc.and_then(|b| b.rpc_url.clone()))
        .ok_or_else(|| anyhow::anyhow!("rpc_url not set (--rpc-url or blockchain.rpc_url)"))?;
    let payment_channel_raw = args
        .payment_channel_address
        .clone()
        .or_else(|| bc.and_then(|b| b.payment_channel_address.clone()))
        .ok_or_else(|| {
            anyhow::anyhow!(
                "payment_channel_address not set (--payment-channel-address or \
                 blockchain.payment_channel_address)"
            )
        })?;
    let payment_channel =
        chain_ctx::parse_nonzero_address(&payment_channel_raw, "payment_channel_address")?;
    let chain_id = args
        .chain_id
        .or_else(|| bc.and_then(|b| b.chain_id))
        .unwrap_or(DEFAULT_CHAIN_ID);
    let data_dir = resolve_data_dir(args.data_dir.clone(), file)?;
    let keystore = args
        .keystore
        .clone()
        .or_else(|| bc.and_then(|b| b.eth_keystore.clone()))
        .map_or_else(
            || eth_identity::keystore_path(&data_dir),
            |p| expand_tilde(&p),
        );
    Ok(ResolvedChain {
        rpc_url,
        payment_channel,
        chain_id,
        data_dir,
        keystore,
    })
}

async fn coop_close(args: &cli::CoopCloseArgs, config_path: Option<&Path>) -> anyhow::Result<()> {
    let file = load_file_config(config_path)?;
    let chain = resolve_chain(&args.chain, &file)?;

    let node_id = PublicKey::from_str(&args.node_id)
        .map_err(|e| anyhow::anyhow!("invalid --node-id {:?}: {e}", args.node_id))?;
    let provider = chain_ctx::parse_address(&args.provider_address, "--provider-address")?;

    // The channel to close is the one tracked for this provider; its watermark is
    // exactly what this client paid, and the cap on what we'll sign away.
    let store = RedbBuyerChannelStore::open(&chain.data_dir)?;
    let state = store.get_by_provider(provider)?.ok_or_else(|| {
        anyhow::anyhow!("no buyer channel tracked for provider {provider} — nothing to close")
    })?;
    let authorized = AuthorizedWatermark {
        amount: state.last_amount,
        nonce: state.last_nonce,
        bytes_delivered: state.last_bytes_delivered,
    };

    // Buyer signer (the client voucher + the cooperativeClose tx).
    let signer = Arc::new(load_buyer_signer(&chain.keystore)?);
    let rpc = provider::build_provider(&chain.rpc_url, &signer)?;
    let contract = PaymentChannel::new(chain.payment_channel, rpc);
    let domain = voucher_domain(chain.chain_id, chain.payment_channel);

    let relays = client_endpoint::resolve_relays(args.relay_url.as_deref(), config_path)?;
    let disc = client_endpoint::client_discovery(config_path)?;
    let endpoint = client_endpoint::client_endpoint(&relays, &disc).await?;
    let mut target = EndpointAddr::new(node_id);
    if let Some(addr) = args.addr {
        target = target.with_ip_addr(addr);
    }
    if let Some(url) = relays.first() {
        target = target.with_relay_url(url.clone());
    }

    let outcome = cooperative_close(
        &endpoint,
        target,
        &contract,
        state.channel_id,
        provider,
        state.token,
        authorized,
        &signer,
        &domain,
        Duration::from_millis(args.timeout_ms),
    )
    .await?;

    match outcome {
        CooperativeCloseOutcome::Settled { reconciled } => {
            // Our persisted watermark lagged what we had actually signed, and the
            // provider proved it with our own signature (#1495). Persist the
            // healed value before forgetting the row, so a failed forget cannot
            // leave the store claiming less than was settled on-chain.
            persist_reconciled(&store, provider, state.channel_id, reconciled);
            // Deposit refunded on-chain; drop the now-terminal channel so a later
            // fetch opens a fresh one. A failed forget only risks a stale reuse
            // attempt (rejected on-chain), so warn rather than fail the close.
            if let Err(e) = store.forget_if_channel(provider, state.channel_id) {
                eprintln!(
                    "warning: channel {} settled on-chain but clearing it from the buyer store \
                     failed: {e}",
                    state.channel_id
                );
            }
            println!(
                "cooperatively closed channel {} (provider {provider}); deposit refunded",
                state.channel_id
            );
            Ok(())
        }
        CooperativeCloseOutcome::Declined => {
            anyhow::bail!(
                "provider {provider} declined the cooperative close (no channel or no voucher \
                 yet); close it the ordinary way with closeChannel"
            )
        }
        CooperativeCloseOutcome::Reverted { reconciled } => {
            // Persist the healed watermark here too: the fallback `closeChannel`
            // must submit the voucher we actually signed, not the stale one this
            // command started from.
            persist_reconciled(&store, provider, state.channel_id, reconciled);
            anyhow::bail!(
                "cooperativeClose reverted on-chain for channel {} (it may have raced a \
                 withdraw/close, or the watermark regressed); close it the ordinary way",
                state.channel_id
            )
        }
    }
}

/// Persist a watermark the close reconciled against the node's echo (#1495).
///
/// Best-effort: a failure here costs a stale local record, not money — the
/// on-chain settlement already stands — so it warns rather than failing the
/// close. `AdvanceOutcome` is `#[must_use]` precisely because a dropped
/// `UnknownChannel`/`ChannelMismatch` looks like success, so each variant is
/// reported rather than discarded.
fn persist_reconciled(
    store: &RedbBuyerChannelStore,
    provider: Address,
    channel_id: ChannelId,
    reconciled: Option<AuthorizedWatermark>,
) {
    let Some(healed) = reconciled else { return };
    match store.advance_progress(
        provider,
        channel_id,
        healed.nonce,
        healed.bytes_delivered,
        healed.amount,
    ) {
        Ok(AdvanceOutcome::Advanced) => {}
        Ok(other) => eprintln!(
            "warning: channel {channel_id} settled at the provider's watermark ({}, {}, {}) but \
             the local record was not advanced: {other:?}",
            healed.amount, healed.nonce, healed.bytes_delivered
        ),
        Err(e) => eprintln!(
            "warning: channel {channel_id} settled at the provider's watermark but persisting it \
             locally failed: {e}"
        ),
    }
}

/// Buyer signer for the on-chain `channel` commands (the client voucher +
/// close/settle/reclaim txs). Password from `KEYSTORE_PASSWORD_ENV`, else TTY.
fn load_buyer_signer(keystore: &Path) -> anyhow::Result<PrivateKeySigner> {
    let password = read_password(
        &[
            PasswordSource::Env(eth_identity::KEYSTORE_PASSWORD_ENV),
            PasswordSource::Prompt { confirm: false },
        ],
        "eth keystore password",
    )?;
    load_signer(keystore, &password)
}

/// Current Unix time (seconds) for the local pre-flight expiry / dispute-window
/// checks. Only a candidate filter — the contract's own `block.timestamp` gate
/// is authoritative, so a too-early attempt simply reverts (mirrors the node's
/// `payment_settlement::unix_now`). A clock before the epoch yields `0` (treat
/// everything as not-yet-ready — the safe direction).
fn unix_now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_secs())
}

/// Local mirror of the on-chain `PaymentChannel.Status`, decoupled from the
/// alloy-generated type so [`next_action`] stays a pure, table-testable fn.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ChannelStatus {
    Open,
    Closing,
    Closed,
}

/// Map the on-chain status. Any unexpected/invalid discriminant is treated as
/// `Closed` (terminal — drop the local record), the safe direction.
const fn map_status(s: PaymentChannel::Status) -> ChannelStatus {
    match s {
        PaymentChannel::Status::Open => ChannelStatus::Open,
        PaymentChannel::Status::Closing => ChannelStatus::Closing,
        _ => ChannelStatus::Closed,
    }
}

/// The next on-chain step to wind a channel down, from its current status and
/// the two deadlines. Pure so every branch is unit-testable without a chain.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CleanAction {
    /// Open and not yet expired: `closeChannel` with the latest voucher to open
    /// the dispute window.
    Close,
    /// Open but past `expiresAt`: `reclaimExpired` to refund the deposit.
    Reclaim,
    /// Closing and the dispute window has elapsed: `settleChannel` to finalize.
    Settle,
    /// Closing but the window is still open until this Unix deadline — re-run
    /// later; no tx now.
    PendingWindow(u64),
    /// Already `Closed`: nothing on-chain to do, drop the local record.
    AlreadyClosed,
}

/// `expires_at == 0` is the "untracked / never expires" sentinel (treated as not
/// expired); the dispute deadline uses `>=` to match the contract's
/// `block.timestamp >= disputeDeadline` finalize gate.
const fn next_action(
    status: ChannelStatus,
    expires_at: u64,
    dispute_deadline: u64,
    now: u64,
) -> CleanAction {
    match status {
        ChannelStatus::Open if expires_at != 0 && now >= expires_at => CleanAction::Reclaim,
        ChannelStatus::Open => CleanAction::Close,
        ChannelStatus::Closing if now >= dispute_deadline => CleanAction::Settle,
        ChannelStatus::Closing => CleanAction::PendingWindow(dispute_deadline),
        ChannelStatus::Closed => CleanAction::AlreadyClosed,
    }
}

/// Terminal outcome of a single close/settle/reclaim tx.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TxOutcome {
    /// Mined with a success receipt.
    Landed,
    /// Deterministically reverted (caught at estimation or a failing receipt) —
    /// non-fatal; the caller re-reads on-chain state and re-decides.
    Reverted,
}

/// Re-sign the client voucher over the persisted watermark. The buyer store
/// keeps only `(amount, nonce, bytes)` — never the signature — so `closeChannel`
/// re-produces it on demand, exactly as the node reconcile path and
/// `cooperative_close::prepare_close` do.
fn sign_client_voucher(
    state: &BuyerChannelState,
    signer: &PrivateKeySigner,
    domain: &Eip712Domain,
) -> anyhow::Result<Bytes> {
    let sig = Voucher {
        channel_id: state.channel_id,
        amount: state.last_amount,
        nonce: state.last_nonce,
        bytes_delivered: state.last_bytes_delivered,
        token: state.token,
    }
    .sign(signer, domain)
    .map_err(|e| anyhow::anyhow!("client voucher signing failed: {e}"))?
    .signature;
    Ok(Bytes::from(sig.as_bytes().to_vec()))
}

/// Whether the persisted watermark carries anything to claim, i.e. whether this
/// buyer holds a voucher worth presenting at close. A channel opened but never
/// drawn sits at all-zero; re-signing that as a voucher advances nothing the
/// contract has not already recorded.
///
/// This is only half of `submit_close`'s branch predicate — it says nothing
/// about whether the loaded key can actually *sign* a voucher this channel's
/// `voucherSigner` accepts. See [`can_sign_voucher`] for the other half.
fn has_claim_watermark(state: &BuyerChannelState) -> bool {
    !(state.last_amount.is_zero()
        && state.last_nonce.is_zero()
        && state.last_bytes_delivered.is_zero())
}

/// Whether `signer` is the address this channel's pinned `voucherSigner`
/// accepts. `BuyerChannelState::voucher_signer` (#1481) makes this a direct
/// comparison rather than the pre-#1481 proxy — a funder-run CLI holding a
/// non-zero watermark on a *delegated* channel (publisher-pays) does not hold
/// the delegate's key, so `signer.address() != state.voucher_signer` and this
/// correctly routes to `closeChannelWithoutVoucher` instead of reverting.
fn can_sign_voucher(state: &BuyerChannelState, signer: &PrivateKeySigner) -> bool {
    signer.address() == state.voucher_signer
}

/// Initiate close, opening the dispute window. Only when the loaded key can
/// sign this channel's voucher **and** the persisted watermark carries
/// something to claim does this take `closeChannel` over the re-signed latest
/// voucher; otherwise it is `closeChannelWithoutVoucher`, which reaches the
/// same close path without the signature check.
///
/// The `closeChannelWithoutVoucher` branch is not merely cheaper. `closeChannel`
/// treats a signature of any length as a voucher to verify, so a zero-amount
/// voucher is still checked against the channel's pinned `voucherSigner` — and
/// on a channel whose signer is a *delegate* the funder's own signature does
/// not recover to it and the close reverts. `closeChannelWithoutVoucher` is
/// exactly the escape hatch for a funder holding no voucher this channel's
/// signer would accept (no watermark to claim, or a delegated signer this
/// process does not hold).
async fn submit_close<P: Provider + Clone>(
    contract: &PaymentChannel::PaymentChannelInstance<P>,
    state: &BuyerChannelState,
    signer: &PrivateKeySigner,
    domain: &Eip712Domain,
) -> anyhow::Result<TxOutcome> {
    let sent = if can_sign_voucher(state, signer) && has_claim_watermark(state) {
        let sig = sign_client_voucher(state, signer, domain)?;
        contract
            .closeChannel(
                state.channel_id,
                state.last_amount,
                state.last_nonce,
                state.last_bytes_delivered,
                sig,
            )
            .send()
            .await
    } else {
        contract
            .closeChannelWithoutVoucher(state.channel_id)
            .send()
            .await
    };
    let pending = match sent {
        Ok(pending) => pending,
        Err(e) if e.as_revert_data().is_some() => return Ok(TxOutcome::Reverted),
        Err(e) => return Err(anyhow::anyhow!("channel close send failed: {e}")),
    };
    let receipt = pending
        .get_receipt()
        .await
        .map_err(|e| anyhow::anyhow!("channel close receipt failed: {e}"))?;
    Ok(receipt_outcome(receipt.status()))
}

/// `settleChannel` — finalize a closed channel past its dispute window.
async fn submit_settle<P: Provider + Clone>(
    contract: &PaymentChannel::PaymentChannelInstance<P>,
    channel_id: B256,
) -> anyhow::Result<TxOutcome> {
    let pending = match contract.settleChannel(channel_id).send().await {
        Ok(pending) => pending,
        Err(e) if e.as_revert_data().is_some() => return Ok(TxOutcome::Reverted),
        Err(e) => return Err(anyhow::anyhow!("settleChannel send failed: {e}")),
    };
    let receipt = pending
        .get_receipt()
        .await
        .map_err(|e| anyhow::anyhow!("settleChannel receipt failed: {e}"))?;
    Ok(receipt_outcome(receipt.status()))
}

/// `reclaimExpired` — refund the deposit of an expired, never-closed channel.
async fn submit_reclaim<P: Provider + Clone>(
    contract: &PaymentChannel::PaymentChannelInstance<P>,
    channel_id: B256,
) -> anyhow::Result<TxOutcome> {
    let pending = match contract.reclaimExpired(channel_id).send().await {
        Ok(pending) => pending,
        Err(e) if e.as_revert_data().is_some() => return Ok(TxOutcome::Reverted),
        Err(e) => return Err(anyhow::anyhow!("reclaimExpired send failed: {e}")),
    };
    let receipt = pending
        .get_receipt()
        .await
        .map_err(|e| anyhow::anyhow!("reclaimExpired receipt failed: {e}"))?;
    Ok(receipt_outcome(receipt.status()))
}

/// A mined-but-reverted tx returns `Ok(receipt)` with `status() == false`, so map
/// the receipt status to the outcome (mirrors the node settlement path).
const fn receipt_outcome(landed: bool) -> TxOutcome {
    if landed {
        TxOutcome::Landed
    } else {
        TxOutcome::Reverted
    }
}

/// Drop a terminal channel's local record. A failed clear only risks a later
/// stale-reuse attempt (rejected on-chain), so warn rather than fail.
fn forget_terminal(store: &RedbBuyerChannelStore, provider: Address, channel_id: B256) {
    if let Err(e) = store.forget_if_channel(provider, channel_id) {
        eprintln!(
            "warning: channel {channel_id} finalized on-chain but clearing it from the buyer \
             store failed: {e}"
        );
    }
}

/// `decdn channel close` (#1136): unilaterally close the channel tracked for a
/// provider with the latest client voucher, opening the on-chain dispute window.
/// No provider contact needed — use `channel coop-close` for the instant path.
async fn close(args: &cli::ChannelCloseArgs, config_path: Option<&Path>) -> anyhow::Result<()> {
    let file = load_file_config(config_path)?;
    let chain = resolve_chain(&args.chain, &file)?;
    let provider_addr = chain_ctx::parse_address(&args.provider_address, "--provider-address")?;

    let store = RedbBuyerChannelStore::open(&chain.data_dir)?;
    let state = store.get_by_provider(provider_addr)?.ok_or_else(|| {
        anyhow::anyhow!("no buyer channel tracked for provider {provider_addr} — nothing to close")
    })?;

    let signer = Arc::new(load_buyer_signer(&chain.keystore)?);
    let rpc = provider::build_provider(&chain.rpc_url, &signer)?;
    let contract = PaymentChannel::new(chain.payment_channel, rpc);
    let domain = voucher_domain(chain.chain_id, chain.payment_channel);

    let ch = contract
        .getChannel(state.channel_id)
        .call()
        .await
        .map_err(|e| anyhow::anyhow!("getChannel failed: {e}"))?;
    ensure_owned(ch.client, signer.address(), state.channel_id)?;

    match next_action(
        map_status(ch.status),
        ch.expiresAt,
        ch.disputeDeadline,
        unix_now(),
    ) {
        CleanAction::Close => {}
        CleanAction::Reclaim => anyhow::bail!(
            "channel {} expired while open — run `decdn channel settle` to reclaim the deposit",
            state.channel_id
        ),
        CleanAction::Settle | CleanAction::PendingWindow(_) => anyhow::bail!(
            "channel {} is already closing — run `decdn channel settle` to finalize it",
            state.channel_id
        ),
        CleanAction::AlreadyClosed => {
            // Terminal on-chain (and confirmed ours above): clear the stale record
            // and report success rather than erroring — consistent with `settle`.
            forget_terminal(&store, provider_addr, state.channel_id);
            println!(
                "channel {} is already closed; cleared local record",
                state.channel_id
            );
            return Ok(());
        }
    }

    match submit_close(&contract, &state, &signer, &domain).await? {
        TxOutcome::Landed => {
            // Always report the successful close; the settle-after deadline is only
            // known post-close, so read it best-effort and never print the
            // pre-close `0` if that read fails.
            let deadline_note = match contract.getChannel(state.channel_id).call().await {
                Ok(c) => format!("after Unix {}", c.disputeDeadline),
                Err(e) => {
                    format!("after the dispute window (couldn't read the exact deadline: {e})")
                }
            };
            println!(
                "closed channel {} (provider {provider_addr}); dispute window open — run \
                 `decdn channel settle --provider-address {provider_addr}` {deadline_note}",
                state.channel_id
            );
            Ok(())
        }
        TxOutcome::Reverted => anyhow::bail!(
            "closeChannel reverted on-chain for channel {} (it may have raced a withdraw/close, or \
             the voucher regressed against the on-chain claimed watermark)",
            state.channel_id
        ),
    }
}

/// Resolve `--voucher-signer`: absent means self-sign (`Address::ZERO`, which
/// resolves on-chain to the funder); present is parsed via
/// [`chain_ctx::parse_address`] (the zero address is a valid *explicit* input
/// too — it just means the same thing as omitting the flag).
fn resolve_voucher_signer(voucher_signer: Option<&str>) -> anyhow::Result<Address> {
    voucher_signer.map_or(Ok(Address::ZERO), |raw| {
        chain_ctx::parse_address(raw, "--voucher-signer")
    })
}

/// `decdn channel open` (#1481): open a payment channel against a provider,
/// optionally pinning a delegate as the channel's `voucherSigner` — the
/// publisher-pays case, where the caller (funder) escrows the deposit but a
/// wallet-less delegate signs vouchers for delivery. Mirrors the auto-open
/// path in `decdn fetch` ([`crate::commands::fetch::open_or_reuse`]): read
/// `usdc()` off the contract, ensure the allowance, then submit
/// `openChannel`.
async fn open(args: &cli::ChannelOpenArgs, config_path: Option<&Path>) -> anyhow::Result<()> {
    let file = load_file_config(config_path)?;
    let chain = resolve_chain(&args.chain, &file)?;
    let provider_addr = chain_ctx::parse_address(&args.provider_address, "--provider-address")?;
    let voucher_signer = resolve_voucher_signer(args.voucher_signer.as_deref())?;

    let store = RedbBuyerChannelStore::open(&chain.data_dir)?;
    let signer = Arc::new(load_buyer_signer(&chain.keystore)?);
    let self_address = signer.address();
    let rpc = provider::build_provider(&chain.rpc_url, &signer)?;
    let contract = PaymentChannel::new(chain.payment_channel, rpc.clone());
    let domain = voucher_domain(chain.chain_id, chain.payment_channel);

    let token = contract
        .usdc()
        .call()
        .await
        .map_err(|e| anyhow::anyhow!("read PaymentChannel.usdc(): {e}"))?;
    let deposit = U256::from(args.deposit_micro_usdc);

    ensure_allowance(
        &rpc,
        token,
        self_address,
        chain.payment_channel,
        Some(deposit),
    )
    .await?;

    let opened = open_channel(
        &contract,
        Arc::clone(&signer),
        &domain,
        token,
        self_address,
        provider_addr,
        deposit,
        voucher_signer,
    )
    .await?;

    // The deposit is escrowed on-chain; a failed local record leaves it
    // untracked (reconcile against the tx), matching `open_or_reuse`'s handling.
    store.record(&opened.state).map_err(|e| {
        anyhow::anyhow!(
            "buyer channel opened on-chain (tx {}) but persisting it failed; the deposit is \
             escrowed but untracked — reconcile manually: {e}",
            opened.tx
        )
    })?;

    let out = ChannelOpenJson {
        channel_id: format!("{:#x}", opened.state.channel_id),
    };
    serde_json::to_writer_pretty(std::io::stdout(), &out)?;
    println!();
    Ok(())
}

/// `decdn channel open` machine-readable output — mirrors the `list --json`
/// pattern ([`ChannelListJson`]).
#[derive(Serialize)]
struct ChannelOpenJson {
    #[serde(rename = "channelId")]
    channel_id: String,
}

/// `decdn channel settle` (#1136): finalize the channel tracked for a provider —
/// `settleChannel` once its dispute window has elapsed, or `reclaimExpired` if it
/// expired while still open. On success the local record is dropped.
async fn settle(args: &cli::ChannelSettleArgs, config_path: Option<&Path>) -> anyhow::Result<()> {
    let file = load_file_config(config_path)?;
    let chain = resolve_chain(&args.chain, &file)?;
    let provider_addr = chain_ctx::parse_address(&args.provider_address, "--provider-address")?;

    let store = RedbBuyerChannelStore::open(&chain.data_dir)?;
    let state = store.get_by_provider(provider_addr)?.ok_or_else(|| {
        anyhow::anyhow!("no buyer channel tracked for provider {provider_addr} — nothing to settle")
    })?;

    let signer = Arc::new(load_buyer_signer(&chain.keystore)?);
    let rpc = provider::build_provider(&chain.rpc_url, &signer)?;
    let contract = PaymentChannel::new(chain.payment_channel, rpc);

    let ch = contract
        .getChannel(state.channel_id)
        .call()
        .await
        .map_err(|e| anyhow::anyhow!("getChannel failed: {e}"))?;
    ensure_owned(ch.client, signer.address(), state.channel_id)?;

    match next_action(
        map_status(ch.status),
        ch.expiresAt,
        ch.disputeDeadline,
        unix_now(),
    ) {
        CleanAction::Settle => match submit_settle(&contract, state.channel_id).await? {
            TxOutcome::Landed => {
                forget_terminal(&store, provider_addr, state.channel_id);
                println!(
                    "settled channel {}; remaining balance refunded to {}",
                    state.channel_id,
                    signer.address()
                );
                Ok(())
            }
            TxOutcome::Reverted => anyhow::bail!(
                "settleChannel reverted for channel {} (already finalized, or the dispute window \
                 has not elapsed on-chain yet)",
                state.channel_id
            ),
        },
        CleanAction::Reclaim => match submit_reclaim(&contract, state.channel_id).await? {
            TxOutcome::Landed => {
                forget_terminal(&store, provider_addr, state.channel_id);
                println!(
                    "reclaimed expired channel {}; deposit refunded to {}",
                    state.channel_id,
                    signer.address()
                );
                Ok(())
            }
            TxOutcome::Reverted => anyhow::bail!(
                "reclaimExpired reverted for channel {} (not expired/open on-chain yet)",
                state.channel_id
            ),
        },
        CleanAction::AlreadyClosed => {
            forget_terminal(&store, provider_addr, state.channel_id);
            println!(
                "channel {} already closed; cleared local record",
                state.channel_id
            );
            Ok(())
        }
        CleanAction::Close => anyhow::bail!(
            "channel {} is still open — run `decdn channel close` first (or wait until it expires \
             to reclaim)",
            state.channel_id
        ),
        CleanAction::PendingWindow(deadline) => anyhow::bail!(
            "channel {} dispute window is still open — run `decdn channel settle` after Unix {deadline}",
            state.channel_id
        ),
    }
}

/// Reject a tracked record whose on-chain `client` isn't this keystore — a wrong
/// keystore/contract or a stale local record (a zero `client` means the channel
/// was never opened on this contract).
fn ensure_owned(on_chain_client: Address, ours: Address, channel_id: B256) -> anyhow::Result<()> {
    anyhow::ensure!(
        on_chain_client == ours,
        "channel {channel_id} on-chain client {on_chain_client} is not this keystore's address \
         {ours} — wrong keystore/contract, or a stale local record"
    );
    Ok(())
}

/// Terminal status of one channel after a `clean` pass, for reporting.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CleanStatus {
    /// Finalized via `settleChannel`.
    Settled,
    /// Refunded via `reclaimExpired`.
    Reclaimed,
    /// Just closed unilaterally; needs a later `settle` once the window elapses.
    /// `settle_after` is `None` when the post-close deadline read failed (the
    /// close still landed) — a re-run reports the real deadline.
    Closed { settle_after: Option<u64> },
    /// Already closing; dispute window still open until this Unix deadline.
    Pending { settle_after: u64 },
    /// Already closed on-chain; local record cleared.
    AlreadyClosed,
    /// The channel doesn't exist on-chain (zeroed `client`); stale local record
    /// cleared — safe, there's nothing to lose.
    ClearedStale,
    /// The channel is live on-chain but owned by a different `client` than this
    /// keystore — skipped *without* clearing, so a wrong-keystore run can't
    /// destroy a valid record.
    NotOwned,
    /// The on-chain tx reverted; a re-run re-reads state and re-decides.
    Reverted(&'static str),
}

impl CleanStatus {
    /// Whether the channel still needs a later `clean` run (a closed channel
    /// mid-window, or a revert to re-evaluate). Terminal outcomes return false.
    const fn incomplete(self) -> bool {
        matches!(
            self,
            Self::Closed { .. } | Self::Pending { .. } | Self::Reverted(_)
        )
    }

    fn label(self) -> String {
        match self {
            Self::Settled => "settled — balance refunded".to_string(),
            Self::Reclaimed => "reclaimed — expired deposit refunded".to_string(),
            Self::Closed {
                settle_after: Some(deadline),
            } => format!("closed — run `clean` again after Unix {deadline} to settle"),
            Self::Closed { settle_after: None } => {
                "closed — run `clean` again after the dispute window to settle".to_string()
            }
            Self::Pending { settle_after } => {
                format!("closing — dispute window open until Unix {settle_after}")
            }
            Self::AlreadyClosed => "already closed — cleared local record".to_string(),
            Self::ClearedStale => "not found on-chain — cleared stale local record".to_string(),
            Self::NotOwned => {
                "owned by a different keystore on-chain — skipped (record kept)".to_string()
            }
            Self::Reverted(op) => format!("{op} reverted — re-run to re-evaluate"),
        }
    }
}

/// Advance one tracked channel through its wind-down state machine. Transient
/// RPC/receipt failures surface as `Err` (the caller flags a needed re-run);
/// on-chain reverts are captured as [`CleanStatus::Reverted`], not errors.
async fn clean_one<P: Provider + Clone>(
    contract: &PaymentChannel::PaymentChannelInstance<P>,
    store: &RedbBuyerChannelStore,
    signer: &PrivateKeySigner,
    domain: &Eip712Domain,
    state: &BuyerChannelState,
    now: u64,
) -> anyhow::Result<CleanStatus> {
    let ch = contract
        .getChannel(state.channel_id)
        .call()
        .await
        .map_err(|e| anyhow::anyhow!("getChannel failed: {e}"))?;
    if ch.client != signer.address() {
        // A zeroed `client` means the channel doesn't exist on-chain (settled and
        // pruned, or never opened) — safe to drop the stale record. A *different*
        // non-zero client means the channel is live but belongs to another
        // keystore: never delete it, or a run with the wrong keystore would
        // destroy a valid record. Skip it instead.
        return if ch.client == Address::ZERO {
            forget_terminal(store, state.provider, state.channel_id);
            Ok(CleanStatus::ClearedStale)
        } else {
            Ok(CleanStatus::NotOwned)
        };
    }

    match next_action(map_status(ch.status), ch.expiresAt, ch.disputeDeadline, now) {
        CleanAction::Close => match submit_close(contract, state, signer, domain).await? {
            TxOutcome::Landed => {
                // The close landed; the real deadline is only known post-close. A
                // failed re-read must not fabricate the pre-close `0` — report it
                // as unknown (`None`) and let a re-run surface the true deadline.
                let settle_after = contract
                    .getChannel(state.channel_id)
                    .call()
                    .await
                    .ok()
                    .map(|c| c.disputeDeadline);
                Ok(CleanStatus::Closed { settle_after })
            }
            TxOutcome::Reverted => Ok(CleanStatus::Reverted("closeChannel")),
        },
        CleanAction::Reclaim => match submit_reclaim(contract, state.channel_id).await? {
            TxOutcome::Landed => {
                forget_terminal(store, state.provider, state.channel_id);
                Ok(CleanStatus::Reclaimed)
            }
            TxOutcome::Reverted => Ok(CleanStatus::Reverted("reclaimExpired")),
        },
        CleanAction::Settle => match submit_settle(contract, state.channel_id).await? {
            TxOutcome::Landed => {
                forget_terminal(store, state.provider, state.channel_id);
                Ok(CleanStatus::Settled)
            }
            TxOutcome::Reverted => Ok(CleanStatus::Reverted("settleChannel")),
        },
        CleanAction::PendingWindow(settle_after) => Ok(CleanStatus::Pending { settle_after }),
        CleanAction::AlreadyClosed => {
            forget_terminal(store, state.provider, state.channel_id);
            Ok(CleanStatus::AlreadyClosed)
        }
    }
}

/// `decdn channel clean` (#1136): reclaim USDC across every tracked channel by
/// driving each through the unilateral close → dispute-window → settle/reclaim
/// sweep. Idempotent and resumable — channels mid dispute-window are reported and
/// finalized by a later run. Processed sequentially (one signer nonce at a time).
async fn clean(args: &cli::ChannelCleanArgs, config_path: Option<&Path>) -> anyhow::Result<()> {
    let file = load_file_config(config_path)?;
    let chain = resolve_chain(&args.chain, &file)?;

    let store = RedbBuyerChannelStore::open(&chain.data_dir)?;
    let BuyerLoad {
        mut channels,
        mut skipped,
    } = store.load_all()?;
    channels.sort_by_key(|c| c.provider);
    skipped.sort_unstable();
    write_skipped_providers(&mut std::io::stderr().lock(), &skipped)?;
    if channels.is_empty() {
        write_clean_empty_status(&mut std::io::stdout().lock(), &skipped)?;
        return Ok(());
    }

    let signer = Arc::new(load_buyer_signer(&chain.keystore)?);
    let rpc = provider::build_provider(&chain.rpc_url, &signer)?;
    let contract = PaymentChannel::new(chain.payment_channel, rpc);
    let domain = voucher_domain(chain.chain_id, chain.payment_channel);
    let now = unix_now();

    let mut failed = 0usize;
    let mut incomplete = 0usize;
    for state in &channels {
        let provider_col = short_hex(&format!("{:#x}", state.provider));
        match clean_one(&contract, &store, &signer, &domain, state, now).await {
            Ok(status) => {
                if status.incomplete() {
                    incomplete += 1;
                }
                println!("{provider_col}  {}", status.label());
            }
            Err(e) => {
                failed += 1;
                println!("{provider_col}  needs-retry: {e}");
            }
        }
    }

    if incomplete > 0 {
        println!(
            "{incomplete} channel(s) still winding down — re-run `decdn channel clean` after their \
             dispute windows elapse to finish settling"
        );
    }
    anyhow::ensure!(
        failed == 0,
        "{failed} channel(s) hit a transient error; re-run `decdn channel clean` to retry"
    );
    Ok(())
}

/// `decdn channel list` / `status` (#1133): read-only dump of the tracked buyer
/// channels and their local voucher watermark. Reads only the buyer store — no
/// chain, keystore, or network access — so a client can see its own last nonce /
/// amount / bytes next to the deposit (e.g. to spot a voucher desync).
fn list(args: &cli::ChannelListArgs, config_path: Option<&Path>) -> anyhow::Result<()> {
    // Only the data dir is needed. An explicit `--data-dir` skips config loading
    // entirely so this read-only command can't fail on an unrelated `${VAR}` in
    // the config (`load_file_config` env-expands `blockchain.*` too); config is
    // read only for the `[identity].data_dir` fallback when the flag is absent.
    let data_dir = match args.data_dir.clone() {
        Some(dir) => expand_tilde(&dir),
        None => resolve_data_dir(None, &load_file_config(config_path)?)?,
    };

    let store = RedbBuyerChannelStore::open(&data_dir)?;
    let BuyerLoad {
        mut channels,
        mut skipped,
    } = store.load_all()?;
    // Stable output regardless of the store's internal key order.
    channels.sort_by_key(|c| c.provider);
    skipped.sort_unstable();
    write_skipped_providers(&mut std::io::stderr().lock(), &skipped)?;

    // Write to locked stdout so a `BrokenPipe` (e.g. piping to `head`) surfaces
    // as a propagated error rather than a `println!` panic, and the JSON isn't
    // buffered into one allocation.
    let mut out = std::io::stdout().lock();
    if args.json {
        let view = ChannelListJson {
            channels: channels.iter().map(ChannelJson::from).collect(),
            skipped: skipped.iter().map(|p| format!("{p:#x}")).collect(),
        };
        serde_json::to_writer_pretty(&mut out, &view)?;
        writeln!(out)?;
    } else {
        write_channels(&mut out, &channels, &skipped)?;
    }
    Ok(())
}

/// Warn about every undecodable buyer row on stderr. The `channel_id` (the
/// store's primary key) is the only available repair handle — the provider
/// lives inside the bytes that failed to decode.
fn write_skipped_providers(w: &mut impl Write, skipped: &[B256]) -> std::io::Result<()> {
    for channel_id in skipped {
        writeln!(
            w,
            "warning: buyer channel {channel_id:#x} could not be decoded; its deposit remains \
             escrowed but untracked and will not be auto-reclaimed until the record is repaired"
        )?;
    }
    Ok(())
}

/// Print the true-empty `clean` sentinel only when no row was skipped.
fn write_clean_empty_status(w: &mut impl Write, skipped: &[B256]) -> std::io::Result<()> {
    if skipped.is_empty() {
        writeln!(w, "no tracked channels to clean")?;
    }
    Ok(())
}

/// Render the tracked buyer channels as an aligned table. Pure (writes to any
/// sink) so the layout is unit-testable without a store. Mirrors the operator
/// side's `decdn node channels` style: a `key=value` summary line, a `(no ...)`
/// sentinel when truly empty, then fixed-width columns. Like the `clean`
/// sentinel, `(no tracked channels)` is suppressed when a row was skipped as
/// undecodable — an escrowed deposit still exists, so the store is not empty.
fn write_channels(
    w: &mut impl Write,
    channels: &[BuyerChannelState],
    skipped: &[B256],
) -> std::io::Result<()> {
    writeln!(w, "channels={}", channels.len())?;
    if channels.is_empty() {
        if skipped.is_empty() {
            writeln!(w, "(no tracked channels)")?;
        }
        return Ok(());
    }
    writeln!(
        w,
        "{:<14} {:<14} {:>8} {:>14} {:>14} {:>16} {:<14}",
        "PROVIDER", "CHANNEL", "NONCE", "LAST_AMOUNT", "DEPOSIT", "BYTES", "TOKEN"
    )?;
    for c in channels {
        writeln!(
            w,
            "{:<14} {:<14} {:>8} {:>14} {:>14} {:>16} {:<14}",
            short_hex(&format!("{:#x}", c.provider)),
            short_hex(&format!("{:#x}", c.channel_id)),
            c.last_nonce,
            format_usdc_u256(c.last_amount),
            format_usdc_u256(c.deposit),
            c.last_bytes_delivered,
            short_hex(&format!("{:#x}", c.token)),
        )?;
    }
    Ok(())
}

/// Top-level `--json` document. `channels` is the decoded rows; `skipped` lists
/// the `channel_id`s of undecodable rows (`{:#x}` hex) so a programmatic
/// consumer sees the escrowed-but-untracked deposits in-band, not only in the
/// stderr warning. The human table path surfaces the same split separately.
#[derive(Serialize)]
struct ChannelListJson {
    channels: Vec<ChannelJson>,
    skipped: Vec<String>,
}

/// Serializable view for `--json`. String-encodes the 256-bit fields (hex for
/// addresses / channel id, decimal for the counters) so the output shape is
/// stable and lossless regardless of the reader's integer width.
#[derive(Serialize)]
struct ChannelJson {
    provider: String,
    channel_id: String,
    token: String,
    deposit_usdc: String,
    last_amount_usdc: String,
    last_nonce: String,
    last_bytes_delivered: String,
    expires_at: u64,
}

impl From<&BuyerChannelState> for ChannelJson {
    fn from(c: &BuyerChannelState) -> Self {
        Self {
            provider: format!("{:#x}", c.provider),
            channel_id: format!("{:#x}", c.channel_id),
            token: format!("{:#x}", c.token),
            deposit_usdc: format_usdc_u256(c.deposit),
            last_amount_usdc: format_usdc_u256(c.last_amount),
            last_nonce: c.last_nonce.to_string(),
            last_bytes_delivered: c.last_bytes_delivered.to_string(),
            expires_at: c.expires_at,
        }
    }
}

/// Format a `U256` micro-USDC amount as `"N.NNNNNN"`. USDC has 6 decimals
/// (ADR 003), so 1 USDC == `1_000_000` micro-USDC; the six fractional places are
/// kept fixed-width so columns align and a parser sees a stable shape. Mirrors
/// `node.rs::format_usdc`, but over the store's `U256` amounts.
fn format_usdc_u256(micro: U256) -> String {
    let scale = U256::from(1_000_000u64);
    let whole = micro / scale;
    // `micro % scale < 1_000_000`, so the `u64` narrowing is always exact.
    let frac = (micro % scale).to::<u64>();
    format!("{whole}.{frac:06}")
}

/// Abbreviate a `0x`-prefixed hex string to `0x` + the first 10 nibbles + `…`
/// for table display; short inputs pass through unchanged. Uses `get(..)` to
/// stay within the workspace `indexing_slicing` clippy denial.
fn short_hex(hex: &str) -> String {
    match hex.get(..12) {
        Some(prefix) if hex.len() > 12 => format!("{prefix}…"),
        _ => hex.to_string(),
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    fn args() -> cli::CoopCloseArgs {
        cli::CoopCloseArgs {
            node_id: "n".into(),
            addr: None,
            relay_url: None,
            provider_address: "0x0000000000000000000000000000000000000001".into(),
            chain: cli::ChannelChainArgs {
                rpc_url: None,
                payment_channel_address: None,
                chain_id: None,
                keystore: None,
                data_dir: Some(PathBuf::from("/tmp/d")),
            },
            timeout_ms: 30_000,
        }
    }

    fn config(body: &str) -> FileConfig {
        toml::from_str(body).unwrap()
    }

    #[test]
    fn flags_override_config() {
        let mut a = args();
        a.chain.rpc_url = Some("http://flag:8545".into());
        a.chain.chain_id = Some(99);
        let pc = "0x1111111111111111111111111111111111111111";
        a.chain.payment_channel_address = Some(pc.into());
        let file = config(
            "[blockchain]\nrpc_url = \"http://config:8545\"\nchain_id = 1\n\
             payment_channel_address = \"0x3333333333333333333333333333333333333333\"\n",
        );
        let r = resolve_chain(&a.chain, &file).unwrap();
        assert_eq!(r.rpc_url, "http://flag:8545");
        assert_eq!(r.chain_id, 99);
        assert_eq!(r.payment_channel, Address::from_str(pc).unwrap());
    }

    #[test]
    fn config_fills_unset_flags_and_chain_id_defaults() {
        let file = config(
            "[blockchain]\nrpc_url = \"http://config:8545\"\n\
             payment_channel_address = \"0x3333333333333333333333333333333333333333\"\n",
        );
        let r = resolve_chain(&args().chain, &file).unwrap();
        assert_eq!(r.rpc_url, "http://config:8545");
        assert_eq!(r.chain_id, DEFAULT_CHAIN_ID);
        assert_eq!(
            r.keystore,
            eth_identity::keystore_path(&PathBuf::from("/tmp/d"))
        );
    }

    #[test]
    fn explicit_data_dir_not_client_scoped() {
        let file = config(
            "[blockchain]\nrpc_url = \"http://config:8545\"\n\
             payment_channel_address = \"0x3333333333333333333333333333333333333333\"\n",
        );
        let r = resolve_chain(&args().chain, &file).unwrap();
        assert_eq!(r.data_dir, PathBuf::from("/tmp/d"));
        assert_eq!(
            r.keystore,
            eth_identity::keystore_path(&PathBuf::from("/tmp/d"))
        );
    }

    #[test]
    fn missing_payment_channel_errors() {
        let file = config("[blockchain]\nrpc_url = \"http://config:8545\"\n");
        let err = resolve_chain(&args().chain, &file).unwrap_err();
        assert!(
            err.to_string().contains("payment_channel_address not set"),
            "{err}"
        );
    }

    /// A present-but-zero `payment_channel_address` fails fast via the shared
    /// `parse_nonzero_address` guard rather than as an opaque on-chain revert
    /// (#1213). The EOA `--provider-address` stays unguarded by design.
    #[test]
    fn rejects_zero_payment_channel() {
        let file = config(
            "[blockchain]\nrpc_url = \"http://config:8545\"\n\
             payment_channel_address = \"0x0000000000000000000000000000000000000000\"\n",
        );
        let err = resolve_chain(&args().chain, &file).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("payment_channel_address"), "{err}");
        assert!(msg.contains("must not be the zero address"), "{err}");
    }

    // ---- `decdn channel open --voucher-signer` resolution (#1481) --------

    #[test]
    fn voucher_signer_absent_resolves_to_zero_for_self_signing() {
        // Omitting the flag means self-sign: the on-chain `openChannel` call
        // resolves a zero `voucherSigner` to `msg.sender` (the funder).
        assert_eq!(resolve_voucher_signer(None).unwrap(), Address::ZERO);
    }

    #[test]
    fn voucher_signer_present_is_parsed() {
        let delegate = "0x2222222222222222222222222222222222222222";
        assert_eq!(
            resolve_voucher_signer(Some(delegate)).unwrap(),
            Address::from_str(delegate).unwrap()
        );
    }

    #[test]
    fn voucher_signer_unparseable_errors() {
        let err = resolve_voucher_signer(Some("not-an-address")).unwrap_err();
        assert!(err.to_string().contains("--voucher-signer"), "{err}");
    }

    /// Build a `BuyerChannelState` for the formatter tests. Fields chosen so the
    /// USDC scaling and hex abbreviation are both exercised.
    fn mk_state(provider_byte: u8, nonce: u64, deposit_micro: u64) -> BuyerChannelState {
        BuyerChannelState {
            channel_id: B256::repeat_byte(0xab),
            provider: Address::repeat_byte(provider_byte),
            funder: Address::repeat_byte(provider_byte),
            voucher_signer: Address::repeat_byte(provider_byte),
            token: Address::repeat_byte(0xcd),
            deposit: U256::from(deposit_micro),
            last_amount: U256::from(1_500_000u64),
            last_nonce: U256::from(nonce),
            last_bytes_delivered: U256::from(4096u64),
            expires_at: 0,
        }
    }

    #[test]
    fn write_channels_empty_emits_sentinel() {
        let mut buf = Vec::new();
        write_channels(&mut buf, &[], &[]).unwrap();
        let out = String::from_utf8(buf).unwrap();
        assert!(out.contains("channels=0"), "{out}");
        assert!(out.contains("(no tracked channels)"), "{out}");
    }

    #[test]
    fn write_channels_empty_sentinel_is_suppressed_when_a_row_was_skipped() {
        let mut buf = Vec::new();
        write_channels(&mut buf, &[], &[B256::repeat_byte(0x66)]).unwrap();
        let out = String::from_utf8(buf).unwrap();
        assert!(out.contains("channels=0"), "{out}");
        assert!(!out.contains("(no tracked channels)"), "{out}");
    }

    #[test]
    fn write_channels_renders_summary_header_and_rows() {
        let mut buf = Vec::new();
        write_channels(&mut buf, &[mk_state(0x11, 7, 2_000_000)], &[]).unwrap();
        let out = String::from_utf8(buf).unwrap();
        assert!(out.contains("channels=1"), "{out}");
        assert!(out.contains("PROVIDER"), "{out}");
        assert!(out.contains("DEPOSIT"), "{out}");
        // Deposit 2_000_000 micro-USDC renders as 2.000000, watermark as 1.500000.
        assert!(out.contains("2.000000"), "{out}");
        assert!(out.contains("1.500000"), "{out}");
        assert!(!out.contains("(no tracked channels)"), "{out}");
    }

    #[test]
    fn skipped_provider_warning_names_every_escrowed_row() {
        let skipped = [B256::repeat_byte(0x11), B256::repeat_byte(0x22)];
        let mut buf = Vec::new();
        write_skipped_providers(&mut buf, &skipped).unwrap();
        let out = String::from_utf8(buf).unwrap();
        for channel_id in skipped {
            assert!(out.contains(&format!("{channel_id:#x}")), "{out}");
        }
        assert!(out.contains("escrowed"), "{out}");
        assert!(out.contains("not be auto-reclaimed"), "{out}");
    }

    #[test]
    fn clean_empty_status_is_suppressed_when_a_row_was_skipped() {
        let mut clean = Vec::new();
        write_clean_empty_status(&mut clean, &[]).unwrap();
        assert_eq!(
            String::from_utf8(clean).unwrap(),
            "no tracked channels to clean\n"
        );

        let mut skipped = Vec::new();
        write_clean_empty_status(&mut skipped, &[B256::repeat_byte(0x33)]).unwrap();
        assert!(skipped.is_empty());
    }

    #[test]
    fn json_view_string_encodes_fields() {
        let view = ChannelJson::from(&mk_state(0x22, 3, 5_000_000));
        assert_eq!(view.deposit_usdc, "5.000000");
        assert_eq!(view.last_amount_usdc, "1.500000");
        assert_eq!(view.last_nonce, "3");
        assert_eq!(view.last_bytes_delivered, "4096");
        assert!(view.provider.starts_with("0x"), "{}", view.provider);
    }

    #[test]
    fn format_usdc_u256_pads_six_fractional_places() {
        assert_eq!(format_usdc_u256(U256::from(0u64)), "0.000000");
        assert_eq!(format_usdc_u256(U256::from(1u64)), "0.000001");
        assert_eq!(format_usdc_u256(U256::from(1_000_000u64)), "1.000000");
        assert_eq!(format_usdc_u256(U256::from(12_345_678u64)), "12.345678");
    }

    #[test]
    fn short_hex_abbreviates_only_long_input() {
        assert_eq!(short_hex("0x0102"), "0x0102");
        assert_eq!(
            short_hex("0x1111111111111111111111111111111111111111"),
            "0x1111111111…"
        );
    }

    /// The close-form selector: an all-zero watermark has nothing to present,
    /// so the close goes through `closeChannelWithoutVoucher`. Any non-zero
    /// component means a voucher exists worth re-signing. All three are checked
    /// independently — a watermark carrying bytes but no payment (or the
    /// reverse) is still a claim the contract has recorded.
    #[test]
    fn has_claim_watermark_is_false_only_for_an_all_zero_watermark() {
        let mut st = mk_state(0x11, 0, 10_000_000);
        st.last_amount = U256::ZERO;
        st.last_nonce = U256::ZERO;
        st.last_bytes_delivered = U256::ZERO;
        assert!(!has_claim_watermark(&st), "never-drawn channel");

        for field in [0usize, 1, 2] {
            let mut drawn = st.clone();
            match field {
                0 => drawn.last_amount = U256::from(1u64),
                1 => drawn.last_nonce = U256::from(1u64),
                _ => drawn.last_bytes_delivered = U256::from(1u64),
            }
            assert!(has_claim_watermark(&drawn), "field {field} is a claim");
        }
    }

    /// #1481: `can_sign_voucher` is the real predicate the old `has_claim_watermark`
    /// proxy could not check — whether the loaded keystore key equals the
    /// channel's pinned `voucher_signer`. A self-signed channel's own key
    /// passes; the funder's key on a *delegated* (publisher-pays) channel does
    /// not, even though the funder is the one who opened it.
    #[test]
    fn can_sign_voucher_checks_the_pinned_signer_not_the_funder() {
        let signer = PrivateKeySigner::random();
        let mut st = mk_state(0x11, 4, 10_000_000);

        // Self-signed: voucher_signer equals the loaded key.
        st.voucher_signer = signer.address();
        assert!(can_sign_voucher(&st, &signer), "self-signed channel");

        // Delegated: voucher_signer is some other address (a delegate this
        // process does not hold the key for) — the funder's own key must NOT
        // pass, or `submit_close` would take the reverting `closeChannel` branch.
        st.voucher_signer = Address::repeat_byte(0x99);
        assert!(
            st.voucher_signer != signer.address(),
            "fixture must pick a genuinely different address"
        );
        assert!(
            !can_sign_voucher(&st, &signer),
            "delegated channel: funder's key must not pass as the voucher signer"
        );
    }

    /// #1481: `submit_close`'s branch selector is `can_sign_voucher &&
    /// has_claim_watermark`, not `has_claim_watermark` alone. A delegated
    /// channel with a non-zero watermark must still route to
    /// `closeChannelWithoutVoucher` (the funder cannot produce a signature the
    /// pinned `voucherSigner` accepts), which is exactly the bug fixed here:
    /// the old code took `closeChannel` — and reverted — on this case.
    #[test]
    fn close_branch_selector_requires_both_predicates() {
        let signer = PrivateKeySigner::random();
        let mut drawn = mk_state(0x11, 4, 10_000_000); // non-zero watermark by construction

        drawn.voucher_signer = signer.address();
        assert!(
            can_sign_voucher(&drawn, &signer) && has_claim_watermark(&drawn),
            "self-signed + drawn must take the closeChannel branch"
        );

        drawn.voucher_signer = Address::repeat_byte(0x99);
        assert!(
            !(can_sign_voucher(&drawn, &signer) && has_claim_watermark(&drawn)),
            "delegated + drawn must NOT take the closeChannel branch (would revert)"
        );
    }

    #[test]
    fn next_action_open_not_expired_closes() {
        // expires_at = 0 (never) and a future expiry both stay Open -> Close.
        assert_eq!(
            next_action(ChannelStatus::Open, 0, 0, 1_000),
            CleanAction::Close
        );
        assert_eq!(
            next_action(ChannelStatus::Open, 2_000, 0, 1_000),
            CleanAction::Close
        );
    }

    #[test]
    fn next_action_open_expired_reclaims() {
        // Past expiry (and not the 0 sentinel) -> reclaim.
        assert_eq!(
            next_action(ChannelStatus::Open, 1_000, 0, 1_000),
            CleanAction::Reclaim
        );
        assert_eq!(
            next_action(ChannelStatus::Open, 900, 0, 1_000),
            CleanAction::Reclaim
        );
    }

    #[test]
    fn next_action_closing_gates_on_dispute_deadline() {
        // Window still open -> report; elapsed (>=) -> settle.
        assert_eq!(
            next_action(ChannelStatus::Closing, 0, 5_000, 4_999),
            CleanAction::PendingWindow(5_000)
        );
        assert_eq!(
            next_action(ChannelStatus::Closing, 0, 5_000, 5_000),
            CleanAction::Settle
        );
    }

    #[test]
    fn next_action_closed_is_terminal() {
        assert_eq!(
            next_action(ChannelStatus::Closed, 0, 0, 1_000),
            CleanAction::AlreadyClosed
        );
    }

    #[test]
    fn clean_status_incomplete_only_for_re_runnable() {
        assert!(
            CleanStatus::Closed {
                settle_after: Some(9)
            }
            .incomplete()
        );
        assert!(
            CleanStatus::Closed { settle_after: None }.incomplete(),
            "a landed close with an unread deadline still needs a re-run"
        );
        assert!(CleanStatus::Pending { settle_after: 9 }.incomplete());
        assert!(CleanStatus::Reverted("settleChannel").incomplete());
        assert!(!CleanStatus::Settled.incomplete());
        assert!(!CleanStatus::Reclaimed.incomplete());
        assert!(!CleanStatus::AlreadyClosed.incomplete());
        assert!(!CleanStatus::ClearedStale.incomplete());
        assert!(
            !CleanStatus::NotOwned.incomplete(),
            "a foreign channel is skipped, not retried with this keystore"
        );
    }

    #[test]
    fn clean_status_labels_are_descriptive() {
        assert!(CleanStatus::Settled.label().contains("settled"));
        assert!(
            CleanStatus::Closed {
                settle_after: Some(42)
            }
            .label()
            .contains("42")
        );
        assert!(
            CleanStatus::Closed { settle_after: None }
                .label()
                .contains("dispute window")
        );
        assert!(CleanStatus::NotOwned.label().contains("different keystore"));
        assert!(
            CleanStatus::Reverted("closeChannel")
                .label()
                .contains("closeChannel")
        );
    }
}
