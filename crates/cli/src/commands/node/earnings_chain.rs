//! `decdn node earnings --address`: the earnings table from the chain alone.
//!
//! No daemon, no lane state: every figure comes from `FeeRouter.Settled` logs
//! for the operator, scanned from the deployment's first L2 block to head in
//! windows the provider accepts ([`scan_logs`]). The operator's net per
//! settlement is reconstructed from the fee split in force at that log
//! (`SharesUpdated` history, then `getShares()` for the value after the last
//! change), the way `FeeRouter.routeSettlement` computes it: each bucket
//! floors, and the operator keeps the rounding dust. Period cutoffs are block
//! numbers found by a binary search over block timestamps, so the cost is a
//! few dozen header reads however many settlements there are.

use std::num::NonZeroU64;
use std::path::Path;

use alloy::eips::BlockId;
use alloy::primitives::{Address, U256};
use alloy::providers::Provider;
use alloy::rpc::types::{Filter, Log};
use alloy::sol_types::SolEvent;
use anyhow::Context;
use decdn_common::admin::{
    EarningsResponse, PERIOD_LAST_7D, PERIOD_LAST_24H, PERIOD_SINCE, PERIOD_SINCE_LEDGER_START,
};
use decdn_common::cli;
use decdn_common::config::{DEFAULT_CHAIN_ID, DEFAULT_GET_LOGS_MAX_BLOCK_SPAN};
use decdn_incentive::SettlementRecord;
use decdn_incentive::earnings::{last_settlement, sum_period};
use decdn_incentive::log_scan::scan_logs;
use decdn_incentive::payment_pool::{FeeRouter, PaymentPool};

use crate::commands::chain_ctx;
use crate::known_chains;

const DAY_SECS: u64 = 86_400;
const WEEK_SECS: u64 = 7 * DAY_SECS;
/// `FeeRouter.BPS_DENOMINATOR`.
const BPS_DENOMINATOR: u64 = 10_000;

/// Where the scan reads from, resolved flag > config file > known chain.
#[derive(Debug, PartialEq, Eq)]
pub(super) struct ChainCoordinates {
    pub(super) rpc_url: String,
    pub(super) payment_pool: Address,
    pub(super) from_block: u64,
    /// The first `eth_getLogs` window span; the scan halves it on a rejection.
    pub(super) span: NonZeroU64,
}

/// Resolve the scan's coordinates. The chain id (config, else the default
/// testnet) picks the known chain whose public RPC, manifest and deployment
/// block fill in whatever the flags and the file leave unset.
pub(super) fn resolve_coordinates(
    args: &cli::EarningsArgs,
    file: &chain_ctx::FileConfig,
) -> anyhow::Result<ChainCoordinates> {
    let chain_id = file.chain_id().unwrap_or(DEFAULT_CHAIN_ID);
    let known = known_chains::by_chain_id(chain_id);
    let rpc_url = args
        .rpc_url
        .clone()
        .or_else(|| file.rpc_url().map(str::to_owned))
        .or_else(|| known.map(|k| k.public_rpc.to_owned()))
        .ok_or_else(|| {
            anyhow::anyhow!(
                "rpc_url not set for chain {chain_id} (pass --rpc-url or set blockchain.rpc_url)"
            )
        })?;
    let payment_pool_raw = match args
        .payment_pool_address
        .clone()
        .or_else(|| file.payment_pool_address().map(str::to_owned))
    {
        Some(raw) => raw,
        None => {
            known
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "payment_pool_address not set for chain {chain_id} (pass \
                     --payment-pool-address or set blockchain.payment_pool_address)"
                    )
                })?
                .addresses()?
                .payment_pool
        }
    };
    let payment_pool = chain_ctx::parse_nonzero_address(&payment_pool_raw, "payment_pool_address")?;
    let from_block = args
        .from_block
        .or_else(|| known.map(|k| k.l2_start_block))
        .ok_or_else(|| {
            anyhow::anyhow!("no deployment block is known for chain {chain_id}; pass --from-block")
        })?;
    let span = NonZeroU64::new(
        file.get_logs_max_block_span()
            .unwrap_or(DEFAULT_GET_LOGS_MAX_BLOCK_SPAN),
    )
    .unwrap_or(NonZeroU64::MIN);
    Ok(ChainCoordinates {
        rpc_url,
        payment_pool,
        from_block,
        span,
    })
}

/// Build the [`EarningsResponse`] for `operator` from the chain. Progress goes
/// to stderr; the response has no `pending` section.
///
/// # Errors
///
/// Any RPC failure, including a provider that rejects a one-block
/// `eth_getLogs` window.
pub(super) async fn run(
    args: &cli::EarningsArgs,
    operator: Address,
    since_unix: Option<u64>,
    global_config: Option<&Path>,
) -> anyhow::Result<EarningsResponse> {
    let config_path = args.config.as_deref().or(global_config);
    let file = chain_ctx::load_optional_config(config_path)?;
    let coords = resolve_coordinates(args, &file)?;
    let provider = decdn_client::provider::build_read_provider(&coords.rpc_url)?;

    let router = PaymentPool::new(coords.payment_pool, &provider)
        .feeRouter()
        .call()
        .await
        .context("PaymentPool.feeRouter()")?;
    let head = provider
        .get_block(BlockId::latest())
        .await
        .context("failed to read the latest block")?
        .ok_or_else(|| anyhow::anyhow!("no latest block"))?;
    let (head_number, now) = (head.header.number, head.header.timestamp);
    let from = coords.from_block.min(head_number);
    let mut rows = scan_rows(&provider, router, operator, from, head_number, coords.span).await?;

    // The ledger-start and last-settlement times: one header read each.
    let ledger_since_unix = match rows.first_mut() {
        Some(first) => {
            first.landed_at_unix = block_timestamp(&provider, first.block_number).await?;
            Some(first.landed_at_unix)
        }
        None => None,
    };
    if let Some(last) = rows.last_mut()
        && last.landed_at_unix == 0
    {
        last.landed_at_unix = block_timestamp(&provider, last.block_number).await?;
    }

    // Period cutoffs as block numbers, by binary search over header times.
    let day = now.saturating_sub(DAY_SECS);
    let week = now.saturating_sub(WEEK_SECS);
    let day_block = block_at_or_after(&provider, day, from, head_number).await?;
    let week_block = block_at_or_after(&provider, week, from, head_number).await?;
    let landed_at_or_after = |block: u64| rows.iter().filter(move |row| row.block_number >= block);
    let mut periods = vec![
        sum_period(PERIOD_LAST_24H, day, landed_at_or_after(day_block)),
        sum_period(PERIOD_LAST_7D, week, landed_at_or_after(week_block)),
        sum_period(
            PERIOD_SINCE_LEDGER_START,
            ledger_since_unix.unwrap_or(0),
            rows.iter(),
        ),
    ];
    if let Some(since) = since_unix {
        let since_block = block_at_or_after(&provider, since, from, head_number).await?;
        periods.push(sum_period(
            PERIOD_SINCE,
            since,
            landed_at_or_after(since_block),
        ));
    }

    Ok(EarningsResponse {
        operator_address: Some(operator.to_string()),
        ledger_since_unix,
        periods,
        last_settlement: last_settlement(&rows),
        pending: None,
    })
}

/// Scan `[from, to]` for the operator's `Settled` logs and the router's fee
/// split history, and decode them into ledger rows (chain order,
/// `landed_at_unix` left `0`). Progress goes to stderr.
async fn scan_rows<P: Provider>(
    provider: &P,
    router: Address,
    operator: Address,
    from: u64,
    to: u64,
    span: NonZeroU64,
) -> anyhow::Result<Vec<SettlementRecord>> {
    eprintln!(
        "scanning FeeRouter.Settled for {operator} over blocks {from}..={to} \
         (windows of {span} blocks, shrinking if the provider rejects them)"
    );
    let settled = scan_logs(
        provider,
        &settled_filter(router, operator),
        from,
        to,
        span,
        |end, to| eprintln!("  scanned to block {end}/{to}"),
    )
    .await
    .context("scanning FeeRouter.Settled")?;
    let shares_logs = scan_logs(provider, &shares_filter(router), from, to, span, |_, _| {})
        .await
        .context("scanning FeeRouter.SharesUpdated")?;
    let current_shares = FeeRouter::new(router, provider)
        .getShares()
        .call()
        .await
        .context("FeeRouter.getShares()")?;
    let history = shares_history(&shares_logs);
    if let (Some(first_settled), Some((first_change, _))) =
        (settled.first().and_then(log_position), history.first())
        && first_settled < *first_change
    {
        eprintln!(
            "warning: a settlement precedes the first fee-split change in the scanned range; \
             its net uses the split set at block {}",
            first_change.0
        );
    }
    Ok(rows_from_logs(&settled, &history, current_shares))
}

/// `FeeRouter.Settled` logs for `operator`: the event signature as topic 0 and
/// the operator as topic 1, so the provider filters server-side.
fn settled_filter(router: Address, operator: Address) -> Filter {
    Filter::new()
        .address(router)
        .event_signature(FeeRouter::Settled::SIGNATURE_HASH)
        .topic1(operator.into_word())
}

/// `FeeRouter.SharesUpdated` logs: rare (the constructor and each governance
/// retune), with no indexed parameters.
fn shares_filter(router: Address) -> Filter {
    Filter::new()
        .address(router)
        .event_signature(FeeRouter::SharesUpdated::SIGNATURE_HASH)
}

/// A log's chain position, `(block_number, log_index)`. `None` for a pending
/// log, which a from-to scan never returns.
fn log_position(log: &Log) -> Option<(u64, u64)> {
    Some((log.block_number?, log.log_index?))
}

/// The fee split after each `SharesUpdated`, in chain order.
pub(super) fn shares_history(logs: &[Log]) -> Vec<((u64, u64), [U256; 3])> {
    let mut history: Vec<_> = logs
        .iter()
        .filter_map(|log| {
            let position = log_position(log)?;
            let event = FeeRouter::SharesUpdated::decode_log_data(&log.inner.data).ok()?;
            Some((position, event.newShares))
        })
        .collect();
    history.sort_by_key(|(position, _)| *position);
    history
}

/// The fee split in force at `position`: the latest `SharesUpdated` at or
/// before it. `current` when the history is empty; the first entry when every
/// entry comes later (the scan started after the split in force was set, and
/// the caller warns).
pub(super) fn shares_at(
    history: &[((u64, u64), [U256; 3])],
    current: [U256; 3],
    position: (u64, u64),
) -> [U256; 3] {
    let Some((first, _)) = history.first() else {
        return current;
    };
    if position < *first {
        return history.first().map_or(current, |(_, shares)| *shares);
    }
    history
        .iter()
        .take_while(|(at, _)| *at <= position)
        .last()
        .map_or(current, |(_, shares)| *shares)
}

/// The operator's net from a gross `Settled.amount` under `shares`
/// (`[operator, buyback, treasury]` bps), as `FeeRouter.routeSettlement`
/// computes it: the buyback and treasury buckets floor, and the operator
/// receives the rest. Saturates instead of underflowing on a malformed split.
pub(super) fn net_of(gross: U256, shares: [U256; 3]) -> U256 {
    let denominator = U256::from(BPS_DENOMINATOR);
    let bucket = |bps: U256| gross.saturating_mul(bps) / denominator;
    let [_, buyback, treasury] = shares;
    gross
        .saturating_sub(bucket(buyback))
        .saturating_sub(bucket(treasury))
}

/// The ledger rows of the operator's `Settled` logs, in chain order, with
/// `landed_at_unix` left `0` (the caller stamps the two it prints).
pub(super) fn rows_from_logs(
    settled: &[Log],
    history: &[((u64, u64), [U256; 3])],
    current: [U256; 3],
) -> Vec<SettlementRecord> {
    let mut rows: Vec<SettlementRecord> = settled
        .iter()
        .filter_map(|log| {
            let (block_number, log_index) = log_position(log)?;
            let event = FeeRouter::Settled::decode_log_data(&log.inner.data).ok()?;
            let shares = shares_at(history, current, (block_number, log_index));
            Some(SettlementRecord {
                block_number,
                log_index,
                tx_hash: log.transaction_hash?,
                landed_at_unix: 0,
                epoch: event.epoch,
                gross_micro_usdc: u64::try_from(event.amount).unwrap_or(u64::MAX),
                net_micro_usdc: u64::try_from(net_of(event.amount, shares)).unwrap_or(u64::MAX),
                bytes_delivered: u64::try_from(event.bytesDelivered).unwrap_or(u64::MAX),
            })
        })
        .collect();
    rows.sort_by_key(SettlementRecord::key);
    rows
}

/// The timestamp of `block`, Unix seconds.
async fn block_timestamp<P: Provider>(provider: &P, block: u64) -> anyhow::Result<u64> {
    provider
        .get_block(BlockId::number(block))
        .await
        .with_context(|| format!("failed to read block {block}"))?
        .map(|b| b.header.timestamp)
        .ok_or_else(|| anyhow::anyhow!("block {block} not found"))
}

/// The lowest block in `[lo, hi]` whose timestamp is at or after
/// `target_unix`, by binary search over block headers; `hi + 1` when even
/// `hi` is earlier (an empty period). About `log2(hi − lo)` header reads.
pub(super) async fn block_at_or_after<P: Provider>(
    provider: &P,
    target_unix: u64,
    lo: u64,
    hi: u64,
) -> anyhow::Result<u64> {
    if lo > hi {
        return Ok(hi.saturating_add(1));
    }
    if block_timestamp(provider, hi).await? < target_unix {
        return Ok(hi.saturating_add(1));
    }
    let (mut lo, mut hi) = (lo, hi);
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        if block_timestamp(provider, mid).await? < target_unix {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    Ok(lo)
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests;
