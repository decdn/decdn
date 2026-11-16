//! `decdn node earnings --address`: the earnings table from the chain alone.
//!
//! This build resolves the scan's coordinates on the payment chain (flag >
//! the config file's first `[[blockchain.payment_chains]]` entry > the known
//! target's first payment chain) and checks them against the RPC, then
//! reports that the chain scan is not available: a redemption's operator
//! income is the `PaymentPool` → operator USDC transfer in its receipt, and
//! this build decodes no such row. The daemon's own ledger
//! (`decdn node earnings` without `--address`) answers instead. The header
//! helpers here ([`bounded`], [`block_at_or_after`], [`print_retry`],
//! [`scan_failed`]) serve the receipt command's chain reads.

use std::future::IntoFuture;
use std::num::NonZeroU64;
use std::path::Path;
use std::time::Duration;

use alloy::eips::BlockId;
use alloy::primitives::Address;
use alloy::providers::Provider;
use anyhow::Context;
use decdn_common::admin::EarningsResponse;
use decdn_common::cli;
use decdn_common::config::{DEFAULT_CHAIN_ID, DEFAULT_GET_LOGS_MAX_BLOCK_SPAN};
use decdn_incentive::log_scan::{SCAN_WINDOW_RETRIES, ScanEvent};

use crate::commands::chain_ctx;
use crate::known_chains;

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

/// Where the scan reads from, resolved flag > config file > known target.
#[derive(Debug, PartialEq, Eq)]
pub(super) struct ChainCoordinates {
    /// The payment chain the other coordinates belong to; checked against the
    /// RPC.
    pub(super) chain_id: u64,
    pub(super) rpc_url: String,
    pub(super) payment_pool: Address,
    pub(super) from_block: u64,
    /// The first `eth_getLogs` window span; a scan halves it on a rejection.
    pub(super) span: NonZeroU64,
}

/// Resolve the scan's coordinates on the payment chain. The config file's
/// first `[[blockchain.payment_chains]]` entry fills whatever the flags leave
/// unset; with no entry, the known target of the governance chain id (config,
/// else the default testnet) supplies its first payment chain. The deployment
/// block comes from the flag, else from the known target when its first
/// payment chain is the resolved one.
pub(super) fn resolve_coordinates(
    args: &cli::EarningsArgs,
    file: &chain_ctx::FileConfig,
) -> anyhow::Result<ChainCoordinates> {
    let known = known_chains::by_chain_id(file.chain_id().unwrap_or(DEFAULT_CHAIN_ID));
    let known_payment = known
        .map(known_chains::KnownChain::payment_chains)
        .transpose()?
        .and_then(|chains| chains.into_iter().next());
    let configured = file.first_payment_chain();
    let chain_id = configured
        .and_then(|p| p.chain_id)
        .or_else(|| known_payment.as_ref().map(|p| p.chain_id))
        .unwrap_or(DEFAULT_CHAIN_ID);
    let rpc_url = args
        .rpc_url
        .clone()
        .or_else(|| configured.and_then(|p| p.rpc_url.clone()))
        .or_else(|| known_payment.as_ref().map(|p| p.public_rpc.to_owned()))
        .ok_or_else(|| {
            anyhow::anyhow!(
                "rpc_url not set for payment chain {chain_id} (pass --rpc-url or set a \
                 [[blockchain.payment_chains]] rpc_url)"
            )
        })?;
    let payment_pool_raw = args
        .payment_pool_address
        .clone()
        .or_else(|| configured.and_then(|p| p.payment_pools.first().cloned()))
        .or_else(|| {
            known_payment
                .as_ref()
                .and_then(|p| p.payment_pools.first().cloned())
        })
        .ok_or_else(|| {
            anyhow::anyhow!(
                "payment pool not set for payment chain {chain_id} (pass \
                 --payment-pool-address or set a [[blockchain.payment_chains]] payment_pools \
                 entry)"
            )
        })?;
    let payment_pool = chain_ctx::parse_nonzero_address(&payment_pool_raw, "payment_pool_address")?;
    let from_block = args
        .from_block
        .or_else(|| {
            known
                .zip(known_payment.as_ref())
                .filter(|(_, payment)| payment.chain_id == chain_id)
                .map(|(k, _)| k.l2_start_block)
        })
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

/// Resolve and check the coordinates for `operator`, then report that this
/// build has no chain scan of earnings.
///
/// # Errors
///
/// Always: an RPC failure or coordinate mismatch, else the unavailable scan.
pub(super) async fn run(
    args: &cli::EarningsArgs,
    operator: Address,
    since_unix: Option<u64>,
    global_config: Option<&Path>,
) -> anyhow::Result<EarningsResponse> {
    let _ = since_unix;
    let config_path = args.config.as_deref().or(global_config);
    let file = chain_ctx::load_optional_config(config_path)?;
    let coords = resolve_coordinates(args, &file)?;
    let provider = decdn_client::provider::build_read_provider(&coords.rpc_url)?;
    let rpc_chain_id = bounded("eth_chainId", provider.get_chain_id()).await?;
    anyhow::ensure!(
        rpc_chain_id == coords.chain_id,
        "the RPC serves chain {rpc_chain_id}, but the addresses are for payment chain {}; pass \
         --rpc-url for that chain",
        coords.chain_id
    );
    anyhow::bail!(
        "the chain scan of {operator}'s earnings on payment pool {} is not available in this \
         build; run `decdn node earnings` against the daemon instead",
        coords.payment_pool
    )
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
