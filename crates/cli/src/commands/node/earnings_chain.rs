//! `decdn node earnings --address`: the earnings table from the chain alone.
//!
//! No daemon, no lane state: every figure comes from `FeeRouter.Settled` logs
//! for the operator, scanned from the deployment's first L2 block (or
//! `--from-block`) to head in windows the provider accepts ([`scan_logs`]).
//! The operator's net per settlement is reconstructed from the fee split in
//! force at that log: the `SharesUpdated` history in the scanned range, the
//! split read at the block before the first in-range change for settlements
//! that precede it, and `getShares()` when the range holds no change at all.
//! It is computed the way `FeeRouter.routeSettlement` computes it: each
//! bucket floors, and the operator keeps the rounding dust. Period cutoffs
//! are block numbers found by a binary search over block timestamps, so the
//! cost is a few dozen header reads however many settlements there are. A log
//! that does not decode fails the command: a clean table over a mis-decoded
//! scan would be a lie.
//!
//! The figures cover every settlement from the scan's first block on, so the
//! response reports that block's time as `covered_from_unix`.

use std::future::IntoFuture;
use std::num::NonZeroU64;
use std::path::Path;
use std::time::Duration;

use alloy::eips::BlockId;
use alloy::primitives::{Address, U256};
use alloy::providers::Provider;
use alloy::rpc::types::{Filter, Log};
use alloy::sol_types::SolEvent;
use anyhow::Context;
use decdn_common::admin::EarningsResponse;
use decdn_common::cli;
use decdn_common::config::{DEFAULT_CHAIN_ID, DEFAULT_GET_LOGS_MAX_BLOCK_SPAN};
use decdn_incentive::SettlementRecord;
use decdn_incentive::earnings::{last_settlement, sum_period};
use decdn_incentive::log_scan::{SCAN_WINDOW_RETRIES, ScanEvent, scan_logs};
use decdn_incentive::payment_pool::{FeeRouter, PaymentPool};

use crate::commands::chain_ctx;
use crate::known_chains;

const DAY_SECS: u64 = 86_400;
const WEEK_SECS: u64 = 7 * DAY_SECS;

/// How long one RPC read of the scan may take. The read-only provider sets no
/// request timeout of its own, and an endpoint that accepts the connection but
/// never answers would otherwise hang the command.
const RPC_CALL_TIMEOUT: Duration = Duration::from_secs(30);

/// Await `call` for at most [`RPC_CALL_TIMEOUT`]; an elapsed deadline is an
/// error naming `what`.
pub(crate) async fn bounded<T, E>(
    what: &str,
    call: impl IntoFuture<Output = Result<T, E>>,
) -> anyhow::Result<T>
where
    E: std::error::Error + Send + Sync + 'static,
{
    match tokio::time::timeout(RPC_CALL_TIMEOUT, call.into_future()).await {
        Ok(result) => result.with_context(|| what.to_owned()),
        Err(_) => anyhow::bail!(
            "{what} did not answer within {}s",
            RPC_CALL_TIMEOUT.as_secs()
        ),
    }
}

/// A `[operator, buyback, treasury]` fee split, in basis points.
type FeeSplit = [U256; 3];
/// One `SharesUpdated`: the chain position it took effect at and the split it
/// installed.
type SharesChange = ((u64, u64), FeeSplit);
/// `FeeRouter.BPS_DENOMINATOR`.
const BPS_DENOMINATOR: u64 = 10_000;

/// Where the scan reads from, resolved flag > config file > known chain.
#[derive(Debug, PartialEq, Eq)]
pub(super) struct ChainCoordinates {
    /// The chain the other coordinates belong to; checked against the RPC.
    pub(super) chain_id: u64,
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
    .ok_or_else(|| {
        anyhow::anyhow!(
            "blockchain.get_logs_max_block_span = 0 would scan one block per request; use at \
             least 1"
        )
    })?;
    Ok(ChainCoordinates {
        chain_id,
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
    let rpc_chain_id = bounded("eth_chainId", provider.get_chain_id()).await?;
    anyhow::ensure!(
        rpc_chain_id == coords.chain_id,
        "the RPC serves chain {rpc_chain_id}, but the addresses are for chain {}; pass \
         --rpc-url for that chain or set blockchain.chain_id",
        coords.chain_id
    );

    let router = bounded(
        "PaymentPool.feeRouter()",
        PaymentPool::new(coords.payment_pool, &provider)
            .feeRouter()
            .call(),
    )
    .await?;
    let head = bounded("the latest block", provider.get_block(BlockId::latest()))
        .await?
        .ok_or_else(|| anyhow::anyhow!("no latest block"))?;
    let (head_number, now) = (head.header.number, head.header.timestamp);
    let from = coords.from_block;
    anyhow::ensure!(
        from <= head_number,
        "--from-block {from} is past the chain head {head_number}"
    );
    // Read before the scan, so an RPC that cannot serve this header fails the
    // command before thousands of `eth_getLogs` windows, not after them.
    let from_unix = block_timestamp(&provider, from).await?;
    let rows = scan_rows(&provider, router, operator, from, head_number, coords.span).await?;
    let range = ScanRange {
        from,
        from_unix,
        head: head_number,
        head_unix: now,
    };
    response_from_rows(&provider, operator, rows, range, since_unix).await
}

/// The blocks a chain scan covered, with their times.
#[derive(Debug, Clone, Copy)]
struct ScanRange {
    /// The first scanned block.
    from: u64,
    /// `from`'s timestamp: the response's `covered_from_unix`.
    from_unix: u64,
    /// The head the scan ran to.
    head: u64,
    /// `head`'s timestamp: the "now" the periods count back from.
    head_unix: u64,
}

/// Sum the scanned `rows` into the [`EarningsResponse`] periods. Reads the
/// last row's header for the last-settlement time, then binary-searches the
/// headers in `range` for each period's first block.
///
/// # Errors
///
/// Any header read that fails.
async fn response_from_rows<P: Provider>(
    provider: &P,
    operator: Address,
    mut rows: Vec<SettlementRecord>,
    range: ScanRange,
    since_unix: Option<u64>,
) -> anyhow::Result<EarningsResponse> {
    let ScanRange {
        from,
        from_unix,
        head,
        head_unix,
    } = range;
    if let Some(last) = rows.last_mut() {
        last.landed_at_unix = block_timestamp(provider, last.block_number).await?;
    }

    // Period cutoffs as block numbers, by binary search over header times.
    let day = head_unix.saturating_sub(DAY_SECS);
    let week = head_unix.saturating_sub(WEEK_SECS);
    let day_block = block_at_or_after(provider, day, from, head).await?;
    let week_block = block_at_or_after(provider, week, from, head).await?;
    let landed_at_or_after = |block: u64| rows.iter().filter(move |row| row.block_number >= block);
    let since = match since_unix {
        Some(since) => {
            let since_block = block_at_or_after(provider, since, from, head).await?;
            Some(sum_period(since, landed_at_or_after(since_block)))
        }
        None => None,
    };

    Ok(EarningsResponse {
        operator_address: Some(operator.to_string()),
        covered_from_unix: from_unix,
        last_24h: sum_period(day, landed_at_or_after(day_block)),
        last_7d: sum_period(week, landed_at_or_after(week_block)),
        since_ledger_start: sum_period(from_unix, rows.iter()),
        since,
        last_settlement: last_settlement(&rows),
        pending: None,
    })
}

/// How many accepted windows pass between two progress lines; the final
/// window always prints. Against a 100-block cap a scan makes thousands of
/// requests, and a line per request would drown the terminal.
const PROGRESS_EVERY_WINDOWS: u32 = 50;

/// Scan `[from, to]` for the operator's `Settled` logs and the router's fee
/// split history, and decode them into ledger rows (chain order,
/// `landed_at_unix` left `0`; [`response_from_rows`] reads the last row's).
/// Progress goes to stderr.
///
/// # Errors
///
/// A scan the provider refuses, a log that does not decode, or a settlement
/// that precedes every in-range `SharesUpdated` on an RPC that cannot read
/// the split at an earlier block.
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
        settled_scan_progress(),
    )
    .await
    .with_context(|| scan_failed("FeeRouter.Settled", span))?;
    let shares_logs = scan_logs(provider, &shares_filter(router), from, to, span, |event| {
        shares_scan_progress(&event);
    })
    .await
    .with_context(|| scan_failed("FeeRouter.SharesUpdated", span))?;
    let fee_router = FeeRouter::new(router, provider);
    // Read at `to`, the block the scan ends at: a retune landing while the
    // scan runs is after every row and must not price them.
    let current_shares = bounded(
        "FeeRouter.getShares()",
        fee_router.getShares().block(BlockId::number(to)).call(),
    )
    .await?;
    let mut history = shares_history(&shares_logs)?;
    // A settlement before the first in-range change was paid under the split
    // that change replaced: read it at the block before the change. The
    // constructor emits `SharesUpdated`, so a scan from the deployment block
    // never needs this; `--from-block` past a retune does.
    if let (Some(first_settled), Some((first_change, _))) =
        (settled.first().and_then(log_position), history.first())
        && first_settled < *first_change
    {
        let change_block = first_change.0;
        let before = bounded(
            "FeeRouter.getShares() at the block before the first change",
            fee_router
                .getShares()
                .block(BlockId::number(change_block.saturating_sub(1)))
                .call(),
        )
        .await
        .with_context(|| {
            format!(
                "a settlement at block {} precedes the first fee-split change in the scanned \
                     range (block {change_block}), and the RPC cannot read the split in force \
                     before it; pass --from-block at or before the deployment block",
                first_settled.0
            )
        })?;
        history.insert(0, ((from, 0), before));
    }
    rows_from_logs(&settled, &history, current_shares)
}

/// Progress lines for the `Settled` scan: every
/// [`PROGRESS_EVERY_WINDOWS`]th accepted window and the last one, and each
/// retry with its cause and delay.
fn settled_scan_progress() -> impl FnMut(ScanEvent) {
    let mut windows = 0u32;
    move |event| match event {
        ScanEvent::Window { end, to } => {
            windows = windows.wrapping_add(1);
            if end == to || windows.is_multiple_of(PROGRESS_EVERY_WINDOWS) {
                eprintln!("  scanned to block {end}/{to}");
            }
        }
        ScanEvent::Retry { .. } => print_retry(&event),
    }
}

/// The context on a scan that gave up: which event, the window span in force
/// and the two levers. The inner error names the last scanned block. A
/// provider that refuses a wide window with wording the range-rejection test
/// does not know is retried like a transient fault and then fails here, so
/// the span is the first thing to try; `--from-block` resumes, but the table
/// then covers only the blocks from there.
pub(crate) fn scan_failed(event: &str, span: NonZeroU64) -> String {
    format!(
        "scanning {event} (windows of {span} blocks); if the provider caps eth_getLogs \
         windows, lower blockchain.get_logs_max_block_span in the config file; \
         --from-block <last scanned + 1> resumes, but covers only the rest"
    )
}

/// Progress lines for the `SharesUpdated` scan: its retries only.
fn shares_scan_progress(event: &ScanEvent) {
    if let ScanEvent::Retry { .. } = event {
        print_retry(event);
    }
}

/// The stderr line for a retried window: which window, why it failed, which
/// retry this is and how long the pause before it is, so a user watching a
/// stalled scan knows what the provider said.
pub(crate) fn print_retry(event: &ScanEvent) {
    if let ScanEvent::Retry {
        start,
        end,
        attempt,
        delay,
        error,
    } = event
    {
        let secs = delay.as_secs();
        eprintln!(
            "  window {start}..={end} failed ({error}); \
             retry {attempt}/{SCAN_WINDOW_RETRIES} in {secs}s"
        );
    }
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

/// Fail a scan on a log the RPC returned without a chain position.
fn positioned(log: &Log, what: &str) -> anyhow::Result<(u64, u64)> {
    log_position(log).ok_or_else(|| {
        anyhow::anyhow!("the RPC returned a {what} log without a block number or log index")
    })
}

/// The fee split after each `SharesUpdated`, in chain order.
///
/// # Errors
///
/// A log that does not decode as this binary's `SharesUpdated` ABI, or that
/// has no chain position.
pub(super) fn shares_history(logs: &[Log]) -> anyhow::Result<Vec<SharesChange>> {
    let mut history = Vec::with_capacity(logs.len());
    for log in logs {
        let position = positioned(log, "FeeRouter.SharesUpdated")?;
        let event =
            FeeRouter::SharesUpdated::decode_log_data(&log.inner.data).with_context(|| {
                format!(
                    "FeeRouter.SharesUpdated at block {} log {} does not decode as this binary's \
                     ABI; is the FeeRouter the one this build was made for?",
                    position.0, position.1
                )
            })?;
        history.push((position, event.newShares));
    }
    history.sort_by_key(|(position, _)| *position);
    Ok(history)
}

/// The fee split in force at `position`: the latest `SharesUpdated` at or
/// before it, else `current` (a range with no change holds one split
/// throughout, and `getShares()` is that split).
pub(super) fn shares_at(
    history: &[SharesChange],
    current: FeeSplit,
    position: (u64, u64),
) -> FeeSplit {
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
pub(super) fn net_of(gross: U256, shares: FeeSplit) -> U256 {
    let denominator = U256::from(BPS_DENOMINATOR);
    let bucket = |bps: U256| gross.saturating_mul(bps) / denominator;
    let [_, buyback, treasury] = shares;
    gross
        .saturating_sub(bucket(buyback))
        .saturating_sub(bucket(treasury))
}

/// The ledger rows of the operator's `Settled` logs, in chain order, with
/// `landed_at_unix` left `0` (the caller reads the last row's header, the only
/// time it prints).
///
/// # Errors
///
/// A log that does not decode as this binary's `Settled` ABI, or that has no
/// chain position or transaction hash. The scan filtered on the event's
/// topic, so such a log is an ABI drift, not noise, and a table that dropped
/// it would undercount.
pub(super) fn rows_from_logs(
    settled: &[Log],
    history: &[SharesChange],
    current: FeeSplit,
) -> anyhow::Result<Vec<SettlementRecord>> {
    let mut rows = Vec::with_capacity(settled.len());
    for log in settled {
        let (block_number, log_index) = positioned(log, "FeeRouter.Settled")?;
        let event = FeeRouter::Settled::decode_log_data(&log.inner.data).with_context(|| {
            format!(
                "FeeRouter.Settled at block {block_number} log {log_index} does not decode as \
                 this binary's ABI; is the FeeRouter the one this build was made for?"
            )
        })?;
        let tx_hash = log.transaction_hash.ok_or_else(|| {
            anyhow::anyhow!(
                "the RPC returned the FeeRouter.Settled log at block {block_number} log \
                 {log_index} without a transaction hash"
            )
        })?;
        let shares = shares_at(history, current, (block_number, log_index));
        rows.push(SettlementRecord {
            block_number,
            log_index,
            tx_hash,
            landed_at_unix: 0,
            epoch: event.epoch,
            gross_micro_usdc: u64::try_from(event.amount).unwrap_or(u64::MAX),
            net_micro_usdc: u64::try_from(net_of(event.amount, shares)).unwrap_or(u64::MAX),
            bytes_delivered: u64::try_from(event.bytesDelivered).unwrap_or(u64::MAX),
            operator: event.operator,
        });
    }
    rows.sort_by_key(SettlementRecord::key);
    Ok(rows)
}

/// The timestamp of `block`, Unix seconds.
async fn block_timestamp<P: Provider>(provider: &P, block: u64) -> anyhow::Result<u64> {
    bounded(
        &format!("block {block}"),
        provider.get_block(BlockId::number(block)),
    )
    .await?
    .map(|b| b.header.timestamp)
    .ok_or_else(|| anyhow::anyhow!("block {block} not found"))
}

/// The lowest block in `[lo, hi]` whose timestamp is at or after
/// `target_unix`, by binary search over block headers; `hi + 1` when even
/// `hi` is earlier (an empty period). About `log2(hi − lo)` header reads.
pub(crate) async fn block_at_or_after<P: Provider>(
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
