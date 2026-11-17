//! `decdn node earnings --address`: the earnings table from the chain alone.
//!
//! No daemon, no lane state. For each payment pool (every configured pool, or
//! `--pool`), the scan reads the pool's `usdc()` and `operatorBps()`, then
//! scans the token's `Transfer` logs from the pool to the operator, from the
//! pool's first block (or `--from-block`) to head, in windows the provider
//! accepts ([`scan_logs`]). Each transfer's transaction is a landed
//! `redeemMany`; its receipt decodes into one settlement row exactly as the
//! daemon's ledger decodes its own ([`settlement_from_receipt`]): the net is
//! the transfer, the gross is `ceil(net × 10000 / operatorBps)`, and the bytes
//! come from the operator's `PoolRedeemed` lanes in the same receipt. The
//! pool's `pending(operator)` and `minSweep()` give the protocol share that
//! waits for `decdn node sweep`.
//!
//! Period cutoffs are block numbers found per chain by a binary search over
//! block timestamps, so the cost is a few dozen header reads however many
//! settlements there are. A log or receipt that does not decode fails the
//! command: a clean table over a mis-decoded scan would be a lie.
//!
//! The figures cover every settlement from each scan's first block on, so the
//! response reports the earliest first block's time as `covered_from_unix`.
//! The header helpers here ([`bounded`], [`block_at_or_after`],
//! [`print_retry`], [`scan_failed`]) also serve the receipt and sweep commands'
//! chain reads.

use std::future::IntoFuture;
use std::num::NonZeroU64;
use std::path::Path;
use std::time::Duration;

use alloy::eips::BlockId;
use alloy::primitives::Address;
use alloy::providers::Provider;
use alloy::rpc::types::Filter;
use alloy::sol_types::SolEvent;
use anyhow::Context;
use decdn_client::session::PaymentPoolChain;
use decdn_common::admin::{EarningsResponse, PoolEarnings, ProtocolShare};
use decdn_common::cli;
use decdn_common::config::{DEFAULT_CHAIN_ID, DEFAULT_GET_LOGS_MAX_BLOCK_SPAN};
use decdn_incentive::SettlementRecord;
use decdn_incentive::earnings::{last_settlement, sum_period};
use decdn_incentive::erc20::Erc20;
use decdn_incentive::log_scan::{SCAN_WINDOW_RETRIES, ScanEvent, scan_logs};
use decdn_incentive::payment_pool::PaymentPool;
use decdn_incentive::settlement_receipt::{LedgerSource, settlement_from_receipt};

use crate::commands::chain_ctx;
use crate::commands::pool_select::{self, ConfiguredChain};
use crate::known_chains;

const DAY_SECS: u64 = 86_400;
const WEEK_SECS: u64 = 7 * DAY_SECS;

/// How long one RPC read may take. The read-only provider sets no request
/// timeout of its own, and an endpoint that accepts the connection but never
/// answers would otherwise hang the command.
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

/// One pool a chain scan reads, from the block it starts at.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct ScanTarget {
    /// The pool and its payment chain's RPC endpoint.
    pub(crate) pool: PaymentPoolChain,
    /// The first block to scan.
    pub(crate) from_block: u64,
}

/// The pools a chain scan reads: `--pool`, else every configured pool, else
/// (with no `[[blockchain.payment_chains]]` entry) the known target's. Each
/// starts at `from_block`, else at its known payment chain's first block.
///
/// # Errors
///
/// No pool resolves, a pool has no RPC endpoint, or no first block is known
/// for a pool's chain.
pub(crate) fn scan_targets(
    select: &cli::PoolSelectArgs,
    from_block: Option<u64>,
    file: &chain_ctx::FileConfig,
) -> anyhow::Result<Vec<ScanTarget>> {
    let governance_chain_id = file.chain_id().unwrap_or(DEFAULT_CHAIN_ID);
    let known_payment = known_chains::by_chain_id(governance_chain_id)
        .map(known_chains::KnownChain::payment_chains)
        .transpose()?
        .unwrap_or_default();
    let pools = if file.payment_chains().is_empty() {
        pool_select::resolve_pools(
            select,
            known_payment.iter().map(|known| ConfiguredChain {
                rpc_url: Some(known.public_rpc),
                chain_id: Some(known.chain_id),
                payment_pools: &known.payment_pools,
            }),
            governance_chain_id,
        )?
    } else {
        pool_select::resolve_pools(
            select,
            file.payment_chains().iter().map(Into::into),
            governance_chain_id,
        )?
    };
    pools
        .into_iter()
        .map(|pool| {
            let chain_id = pool.deployment.chain_id;
            let from_block = from_block
                .or_else(|| {
                    known_payment
                        .iter()
                        .find(|known| known.chain_id == chain_id)
                        .map(|known| known.start_block)
                })
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "no deployment block is known for payment chain {chain_id}; pass \
                         --from-block"
                    )
                })?;
            Ok(ScanTarget { pool, from_block })
        })
        .collect()
}

/// The first `eth_getLogs` window span: `blockchain.get_logs_max_block_span`,
/// else the default. A scan halves it on a rejection.
///
/// # Errors
///
/// The configured span is `0`.
pub(crate) fn scan_span(file: &chain_ctx::FileConfig) -> anyhow::Result<NonZeroU64> {
    NonZeroU64::new(
        file.get_logs_max_block_span()
            .unwrap_or(DEFAULT_GET_LOGS_MAX_BLOCK_SPAN),
    )
    .ok_or_else(|| {
        anyhow::anyhow!(
            "blockchain.get_logs_max_block_span = 0 would scan one block per request; use at \
             least 1"
        )
    })
}

/// Build the [`EarningsResponse`] for `operator` from the chain. Progress goes
/// to stderr; the response has no `pending` redeem section.
///
/// # Errors
///
/// Any RPC failure, including a provider that rejects a one-block
/// `eth_getLogs` window, and an RPC that serves another chain than the pool's.
pub(super) async fn run(
    args: &cli::EarningsArgs,
    operator: Address,
    since_unix: Option<u64>,
    global_config: Option<&Path>,
) -> anyhow::Result<EarningsResponse> {
    let config_path = args.config.as_deref().or(global_config);
    let file = chain_ctx::load_optional_config(config_path)?;
    let targets = scan_targets(&args.payment, args.from_block, &file)?;
    let span = scan_span(&file)?;
    let now_unix = super::unix_now();
    let cutoffs = Cutoffs {
        day: now_unix.saturating_sub(DAY_SECS),
        week: now_unix.saturating_sub(WEEK_SECS),
        since: since_unix,
    };
    let mut scans = Vec::with_capacity(targets.len());
    for target in &targets {
        scans.push(scan_pool(target, operator, cutoffs, span).await?);
    }
    Ok(aggregate(operator, &scans, now_unix, cutoffs))
}

/// The starts of the periods the response sums, Unix seconds.
#[derive(Debug, Clone, Copy)]
pub(super) struct Cutoffs {
    /// The start of the last 24 hours.
    pub(super) day: u64,
    /// The start of the last 7 days.
    pub(super) week: u64,
    /// The start of the `--since` period, when asked for.
    pub(super) since: Option<u64>,
}

/// What one pool's scan found.
#[derive(Debug, Default)]
pub(super) struct PoolScan {
    /// The pool, as a CAIP-10 account.
    pub(super) pool: String,
    /// The time of the scan's first block.
    pub(super) from_unix: u64,
    /// Every settlement row, in chain order. The last row carries its block
    /// time; the others carry `0`.
    pub(super) rows: Vec<SettlementRecord>,
    /// How many of `rows`, counted from the end, landed in the last 24 hours.
    pub(super) in_day: usize,
    /// How many of `rows`, counted from the end, landed in the last 7 days.
    pub(super) in_week: usize,
    /// How many of `rows`, counted from the end, landed in the `--since`
    /// period.
    pub(super) in_since: usize,
    /// The pending protocol share and `minSweep`.
    pub(super) share: Option<ProtocolShare>,
}

impl PoolScan {
    /// The rows of the last `n`, by chain order.
    fn tail(&self, n: usize) -> &[SettlementRecord] {
        self.rows
            .get(self.rows.len().saturating_sub(n)..)
            .unwrap_or_default()
    }
}

/// Sum the pool scans into one response at `now_unix`. Pure, so the per-pool
/// split and the totals are unit-testable. The totals span every pool; the
/// last settlement is the latest by block time across chains.
pub(super) fn aggregate(
    operator: Address,
    scans: &[PoolScan],
    now_unix: u64,
    cutoffs: Cutoffs,
) -> EarningsResponse {
    let covered_from_unix = scans
        .iter()
        .map(|scan| scan.from_unix)
        .min()
        .unwrap_or(now_unix);
    let rows_of = |pick: fn(&PoolScan) -> &[SettlementRecord]| -> Vec<&SettlementRecord> {
        scans.iter().flat_map(|scan| pick(scan).iter()).collect()
    };
    let all = rows_of(|scan| &scan.rows);
    let day = rows_of(|scan| scan.tail(scan.in_day));
    let week = rows_of(|scan| scan.tail(scan.in_week));
    let since = cutoffs.since.map(|from| {
        sum_period(
            from,
            scans
                .iter()
                .flat_map(|scan| scan.tail(scan.in_since).iter()),
        )
    });
    let latest = scans
        .iter()
        .filter_map(|scan| scan.rows.last())
        .max_by_key(|row| row.landed_at_unix);
    EarningsResponse {
        operator_address: Some(operator.to_string()),
        covered_from_unix,
        last_24h: sum_period(cutoffs.day, day),
        last_7d: sum_period(cutoffs.week, week),
        since_ledger_start: sum_period(covered_from_unix, all),
        since,
        last_settlement: last_settlement(latest),
        pending: None,
        pools: scans
            .iter()
            .map(|scan| PoolEarnings {
                pool: scan.pool.clone(),
                settled: sum_period(scan.from_unix, &scan.rows),
                protocol_share: scan.share.clone(),
            })
            .collect(),
    }
}

/// How many accepted windows pass between two progress lines; the final
/// window always prints. Against a 100-block cap a scan makes thousands of
/// requests, and a line per request would drown the terminal.
const PROGRESS_EVERY_WINDOWS: u32 = 50;

/// Scan one pool for `operator`'s settlements and read its protocol share.
///
/// # Errors
///
/// An RPC on another chain, a failed read or scan, or a receipt that does not
/// decode.
async fn scan_pool(
    target: &ScanTarget,
    operator: Address,
    cutoffs: Cutoffs,
    span: NonZeroU64,
) -> anyhow::Result<PoolScan> {
    let deployment = target.pool.deployment;
    let account = deployment.pool_account().to_string();
    let provider = decdn_client::provider::build_read_provider(&target.pool.rpc_url)?;
    let rpc_chain_id = bounded("eth_chainId", provider.get_chain_id()).await?;
    anyhow::ensure!(
        rpc_chain_id == deployment.chain_id,
        "the RPC serves chain {rpc_chain_id}, but pool {account} is on chain {}; pass \
         --pool-rpc-url for that chain",
        deployment.chain_id
    );
    let contract = PaymentPool::new(deployment.payment_pool, &provider);
    let usdc = bounded("PaymentPool.usdc()", contract.usdc().call()).await?;
    let operator_bps = bounded("PaymentPool.operatorBps()", contract.operatorBps().call())
        .await
        .and_then(|bps| {
            u16::try_from(bps).map_err(|_| anyhow::anyhow!("operatorBps {bps} is out of range"))
        })?;
    let head = bounded("the latest block", provider.get_block(BlockId::latest()))
        .await?
        .ok_or_else(|| anyhow::anyhow!("no latest block"))?
        .header
        .number;
    let from = target.from_block;
    anyhow::ensure!(
        from <= head,
        "--from-block {from} is past the head {head} of payment chain {}",
        deployment.chain_id
    );
    // Read before the scan, so an RPC that cannot serve this header fails the
    // command before thousands of `eth_getLogs` windows, not after them.
    let from_unix = block_timestamp(&provider, from).await?;

    eprintln!(
        "scanning USDC transfers from pool {} on chain {} to {operator} over blocks \
         {from}..={head} (windows of {span} blocks, shrinking if the provider rejects them)",
        deployment.payment_pool, deployment.chain_id
    );
    let filter = Filter::new()
        .address(usdc)
        .event_signature(Erc20::Transfer::SIGNATURE_HASH)
        .topic1(deployment.payment_pool.into_word())
        .topic2(operator.into_word());
    let transfers = scan_logs(&provider, &filter, from, head, span, scan_progress())
        .await
        .with_context(|| scan_failed("USDC Transfer", span))?;

    let source = LedgerSource {
        deployment,
        usdc,
        operator_bps,
    };
    let mut rows = settlement_rows(&provider, &transfers, operator, source).await?;
    if let Some(last) = rows.last_mut() {
        last.landed_at_unix = block_timestamp(&provider, last.block_number).await?;
    }
    let landed_from = |block: u64| rows.iter().filter(|row| row.block_number >= block).count();
    let in_day = landed_from(block_at_or_after(&provider, cutoffs.day, from, head).await?);
    let in_week = landed_from(block_at_or_after(&provider, cutoffs.week, from, head).await?);
    let in_since = match cutoffs.since {
        Some(since) => landed_from(block_at_or_after(&provider, since, from, head).await?),
        None => 0,
    };

    let pending = bounded(
        "PaymentPool.pending(operator)",
        contract.pending(operator).call(),
    )
    .await?;
    let min_sweep = bounded("PaymentPool.minSweep()", contract.minSweep().call()).await?;
    Ok(PoolScan {
        pool: account,
        from_unix,
        rows,
        in_day,
        in_week,
        in_since,
        share: Some(ProtocolShare {
            pending_micro_usdc: u64::try_from(pending.usdcAmount).unwrap_or(u64::MAX),
            pending_bytes: u64::try_from(pending.servedBytes).unwrap_or(u64::MAX),
            min_sweep_micro_usdc: min_sweep,
        }),
    })
}

/// The settlement row of each transaction among `transfers`, in chain order:
/// one receipt read per transaction, decoded as the daemon's ledger decodes
/// its own. `landed_at_unix` is left `0`.
///
/// # Errors
///
/// A failed or missing receipt, or one that does not decode.
async fn settlement_rows<P: Provider>(
    provider: &P,
    transfers: &[alloy::rpc::types::Log],
    operator: Address,
    source: LedgerSource,
) -> anyhow::Result<Vec<SettlementRecord>> {
    let mut seen = std::collections::HashSet::new();
    let mut rows = Vec::new();
    for log in transfers {
        let tx = log
            .transaction_hash
            .context("a Transfer log carries no transaction hash")?;
        if !seen.insert(tx) {
            continue;
        }
        let receipt = bounded(
            &format!("the receipt of {tx}"),
            provider.get_transaction_receipt(tx),
        )
        .await?
        .with_context(|| format!("no receipt for {tx}"))?;
        let block = receipt
            .block_number
            .with_context(|| format!("the receipt of {tx} names no block"))?;
        if let Some(row) = settlement_from_receipt(&receipt, operator, source, block, 0)
            .with_context(|| format!("decoding the receipt of {tx}"))?
        {
            rows.push(row);
        }
    }
    Ok(rows)
}

/// Progress lines for a scan: every [`PROGRESS_EVERY_WINDOWS`]th accepted
/// window and the last one, and each retry with its cause and delay.
pub(crate) fn scan_progress() -> impl FnMut(ScanEvent) {
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
