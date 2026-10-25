//! Writes the settlement ledger (#2598): one [`SettlementRecord`] per landed
//! `redeemMany`, decoded from the transaction receipt.
//!
//! A `redeemMany` that paid anything routes its whole payout through
//! `FeeRouter.routeSettlement` once, so its receipt carries exactly one
//! `FeeRouter.Settled` for this operator (gross amount, bytes, epoch) and, when
//! the operator share is not zero, one USDC `Transfer` from the router to the
//! operator (the exact net payout). A `redeemMany` that paid nothing (every
//! voucher at or below its lane's watermark, as after a re-send of a batch
//! that had already landed) skips the router: it lands with no `Settled` and
//! no `PoolRedeemed`, and gets no row. Decoding the receipt costs no extra RPC;
//! the redeemer's one read is the block header, for the row's timestamp, only
//! when the receipt's logs do not carry it and only once the receipt has
//! decoded to a row. The watcher's backfill adds the receipt read itself.
//!
//! Two writers share [`SettlementRecorder`]: the redeemer, which has the
//! receipt in hand when its `redeemMany` lands, and the `PoolRedeemed` watcher,
//! which backfills a row for a transaction the redeemer never saw land (a
//! receipt wait that timed out, or a landing across a restart). Rows key on
//! `(block_number, log_index)`, so the two writers never duplicate a row.
//!
//! The ledger is observability, not a claim: every failure here is logged,
//! counted (`decdn_settlement_ledger_write_failures_total`,
//! `decdn_settlement_ledger_skipped_rows_total`) and dropped, and the
//! redemption itself stands.

use std::sync::Arc;
use std::time::Duration;

use alloy::eips::BlockId;
use alloy::primitives::{Address, B256};
use alloy::providers::Provider;
use alloy::rpc::types::TransactionReceipt;
use alloy::sol_types::SolEvent;
use decdn_common::redact::sanitize_err_chain;
use decdn_incentive::erc20::Erc20;
use decdn_incentive::payment_pool::{FeeRouter, PaymentPool};
use decdn_incentive::{SettlementLedger, SettlementRecord, StoreError};
use tracing::{debug, warn};

use crate::chain_events::timed;
use crate::metrics::Metrics;
use crate::payment_settlement::unix_now;

/// Why a landed receipt that routed a payout yields no ledger row.
#[derive(Debug)]
pub enum DecodeError {
    /// The receipt routed a payout (it carries a `Settled` or a
    /// `PoolRedeemed`) but no `Settled` names this operator.
    NoSettledForOperator {
        /// The operator the row was wanted for.
        operator: Address,
    },
    /// A `Settled`-shaped log does not decode as this binary's ABI: the router
    /// at the address is not the one this build was made for.
    UndecodableSettled(alloy::sol_types::Error),
    /// A `Transfer`-shaped log does not decode, so the net cannot be read.
    UndecodableTransfer(alloy::sol_types::Error),
    /// The `Settled` log carries no log index, so the row has no identity.
    MissingLogIndex,
}

impl std::fmt::Display for DecodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NoSettledForOperator { operator } => write!(
                f,
                "the receipt routed a payout but carries no FeeRouter.Settled for operator \
                 {operator}"
            ),
            Self::UndecodableSettled(err) => {
                write!(
                    f,
                    "FeeRouter.Settled log does not decode as this binary's ABI: {err}"
                )
            }
            Self::UndecodableTransfer(err) => {
                write!(
                    f,
                    "an ERC-20 Transfer log in the receipt does not decode: {err}"
                )
            }
            Self::MissingLogIndex => f.write_str("the FeeRouter.Settled log carries no log index"),
        }
    }
}

impl std::error::Error for DecodeError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::UndecodableSettled(err) | Self::UndecodableTransfer(err) => Some(err),
            Self::NoSettledForOperator { .. } | Self::MissingLogIndex => None,
        }
    }
}

/// Decode the ledger row for a landed `redeemMany` receipt in `block_number`.
///
/// `Ok(None)` is a `redeemMany` that paid nothing: no `Settled` and no
/// `PoolRedeemed` in the receipt, so nothing was routed and there is no row.
/// Otherwise the row is the `Settled` whose `operator` is `operator`; its
/// emitter is the `FeeRouter`, and the net payout is the sum of every
/// `Transfer`-shaped log from that emitter to `operator` in the same receipt
/// (`0` when the operator share is `0`; the emitting token is not checked,
/// because only USDC moves in a `redeemMany`).
///
/// Amounts narrow from `uint256` to `u64` micro-USDC with saturation. The
/// on-chain counters are `uint64`, so a real value never saturates.
///
/// # Errors
///
/// A receipt that routed a payout but yields no row: see [`DecodeError`].
pub fn settlement_from_receipt(
    receipt: &TransactionReceipt,
    operator: Address,
    block_number: u64,
    landed_at_unix: u64,
) -> Result<Option<SettlementRecord>, DecodeError> {
    let logs = receipt.inner.logs();
    let paid_out = logs.iter().any(|log| {
        log.topic0() == Some(&FeeRouter::Settled::SIGNATURE_HASH)
            || log.topic0() == Some(&PaymentPool::PoolRedeemed::SIGNATURE_HASH)
    });
    if !paid_out {
        return Ok(None);
    }
    let mut settled = None;
    for log in logs
        .iter()
        .filter(|log| log.topic0() == Some(&FeeRouter::Settled::SIGNATURE_HASH))
    {
        let event = FeeRouter::Settled::decode_log_data(&log.inner.data)
            .map_err(DecodeError::UndecodableSettled)?;
        if event.operator == operator {
            settled = Some((log, event));
            break;
        }
    }
    let Some((settled_log, settled)) = settled else {
        return Err(DecodeError::NoSettledForOperator { operator });
    };
    let log_index = settled_log.log_index.ok_or(DecodeError::MissingLogIndex)?;
    let router = settled_log.address();
    let mut net = 0u64;
    for log in logs
        .iter()
        .filter(|log| log.topic0() == Some(&Erc20::Transfer::SIGNATURE_HASH))
    {
        let transfer = Erc20::Transfer::decode_log_data(&log.inner.data)
            .map_err(DecodeError::UndecodableTransfer)?;
        if transfer.from == router && transfer.to == operator {
            net = net.saturating_add(u64::try_from(transfer.value).unwrap_or(u64::MAX));
        }
    }
    Ok(Some(SettlementRecord {
        block_number,
        log_index,
        tx_hash: receipt.transaction_hash,
        landed_at_unix,
        epoch: settled.epoch,
        gross_micro_usdc: u64::try_from(settled.amount).unwrap_or(u64::MAX),
        net_micro_usdc: net,
        bytes_delivered: u64::try_from(settled.bytesDelivered).unwrap_or(u64::MAX),
    }))
}

/// How many times [`landed_at`] retries a failed header read before it gives
/// up, and the pause before each retry.
const HEADER_READ_RETRIES: u32 = 2;
const HEADER_READ_RETRY_DELAY: Duration = Duration::from_secs(1);

/// The timestamp of `block`, Unix seconds: the one the receipt's logs carry
/// when the RPC fills it in, else one `eth_getBlockByNumber`, retried
/// [`HEADER_READ_RETRIES`] times. `None` when every read fails; the caller
/// decides between the wall clock and no row.
pub(crate) async fn landed_at<P: Provider>(
    provider: &P,
    receipt: &TransactionReceipt,
    block: u64,
) -> Option<u64> {
    if let Some(timestamp) = receipt
        .inner
        .logs()
        .iter()
        .find_map(|log| log.block_timestamp)
    {
        return Some(timestamp);
    }
    for attempt in 0..=HEADER_READ_RETRIES {
        if attempt > 0 {
            tokio::time::sleep(HEADER_READ_RETRY_DELAY).await;
        }
        match timed(
            None,
            "ledger block header",
            provider.get_block(BlockId::number(block)),
        )
        .await
        {
            Ok(Some(header)) => return Some(header.header.timestamp),
            Ok(None) => {
                warn!(block, attempt, "settlement block header not found");
            }
            Err(err) => {
                warn!(
                    error = %sanitize_err_chain(&err),
                    block,
                    attempt,
                    "settlement block header read failed"
                );
            }
        }
    }
    None
}

/// What to do with a row whose block time could not be read.
#[derive(Clone, Copy, Debug)]
enum TimeFallback {
    /// Stamp the wall clock: the redeemer writes within seconds of the
    /// landing, so the row lands in the right period in every case but a read
    /// that fails across a period boundary.
    WallClock,
    /// Write no row: the watcher's backfill can run as long after the landing
    /// as the node was down, and a wall-clock stamp would put an old
    /// settlement into "last 24h".
    Skip,
}

/// Writes ledger rows for one operator. Cheap to clone: `Arc`s over the
/// ledger and the metrics plus the operator address.
#[derive(Clone)]
pub struct SettlementRecorder {
    ledger: Arc<dyn SettlementLedger>,
    operator: Address,
    metrics: Arc<Metrics>,
}

impl std::fmt::Debug for SettlementRecorder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SettlementRecorder")
            .field("operator", &self.operator)
            .finish_non_exhaustive()
    }
}

impl SettlementRecorder {
    /// A recorder that writes rows for `operator` into `ledger` and counts
    /// its failures into `metrics`.
    #[must_use]
    pub fn new(
        ledger: Arc<dyn SettlementLedger>,
        operator: Address,
        metrics: Arc<Metrics>,
    ) -> Self {
        Self {
            ledger,
            operator,
            metrics,
        }
    }

    /// Record the row for a `redeemMany` receipt the redeemer just saw land.
    /// A block time the RPC cannot supply becomes the wall clock, which is
    /// seconds from the landing on this path.
    pub async fn record_landed<P: Provider>(&self, provider: &P, receipt: &TransactionReceipt) {
        self.record(provider, receipt, TimeFallback::WallClock)
            .await;
    }

    /// Record the row for a `redeemMany` the `PoolRedeemed` watcher saw in
    /// `block_number`, when the ledger has no row for `tx_hash` yet: one
    /// `eth_getTransactionReceipt`, then the same decoding as
    /// [`Self::record_landed`]. Covers a receipt wait that timed out and a
    /// landing across a restart. A transaction already in the ledger costs one
    /// ledger read of that block and no RPC. A block time the RPC cannot
    /// supply skips the row: this path can run long after the landing.
    pub async fn backfill<P: Provider>(&self, provider: &P, block_number: u64, tx_hash: B256) {
        if self.is_known(block_number, tx_hash).await != Some(false) {
            return;
        }
        if let Some(receipt) = self.fetch_receipt(provider, tx_hash).await {
            debug!(tx = %tx_hash, "backfilling a settlement ledger row from PoolRedeemed");
            self.record(provider, &receipt, TimeFallback::Skip).await;
        }
    }

    /// Whether the ledger already holds `tx_hash` in `block_number`; `None`
    /// after a failed read, which is counted and ends the backfill.
    async fn is_known(&self, block_number: u64, tx_hash: B256) -> Option<bool> {
        let known = self
            .on_ledger(move |ledger| ledger.has_settlement_tx(block_number, tx_hash))
            .await;
        match known {
            Ok(known) => Some(known),
            Err(error) => {
                warn!(error, tx = %tx_hash, block = block_number, "settlement ledger read failed; skipping backfill");
                self.metrics.settlement_ledger_skipped_row();
                None
            }
        }
    }

    /// The receipt of `tx_hash`, retried like the header read (a load-balanced
    /// provider whose backend lags head answers `null` for a moment), or
    /// `None` (counted) when every attempt fails.
    async fn fetch_receipt<P: Provider>(
        &self,
        provider: &P,
        tx_hash: B256,
    ) -> Option<TransactionReceipt> {
        let mut why = String::new();
        for attempt in 0..=HEADER_READ_RETRIES {
            if attempt > 0 {
                tokio::time::sleep(HEADER_READ_RETRY_DELAY).await;
            }
            let fetched = timed(
                None,
                "ledger backfill getTransactionReceipt",
                provider.get_transaction_receipt(tx_hash),
            )
            .await;
            why = match fetched {
                Ok(Some(receipt)) => return Some(receipt),
                Ok(None) => {
                    "PoolRedeemed names a transaction the RPC has no receipt for".to_owned()
                }
                Err(err) => format!("receipt read failed: {}", sanitize_err_chain(&err)),
            };
            debug!(tx = %tx_hash, attempt, why, "settlement ledger backfill receipt read missed");
        }
        self.skip(tx_hash, None, &why);
        None
    }

    /// Run one blocking ledger call on the blocking pool. A task that did not
    /// complete and a store error both come back as their message.
    async fn on_ledger<T, F>(&self, call: F) -> Result<T, String>
    where
        T: Send + 'static,
        F: FnOnce(&dyn SettlementLedger) -> Result<T, StoreError> + Send + 'static,
    {
        let ledger = Arc::clone(&self.ledger);
        tokio::task::spawn_blocking(move || call(ledger.as_ref()))
            .await
            .map_err(|join| join.to_string())
            .and_then(|result| result.map_err(|err| err.to_string()))
    }

    /// Log and count a landed redemption that gets no row.
    fn skip(&self, tx: B256, block: Option<u64>, why: &str) {
        warn!(%tx, block, why, "no settlement ledger row for a landed redemption");
        self.metrics.settlement_ledger_skipped_row();
    }

    async fn record<P: Provider>(
        &self,
        provider: &P,
        receipt: &TransactionReceipt,
        fallback: TimeFallback,
    ) {
        let tx = receipt.transaction_hash;
        let Some(block) = receipt.block_number else {
            self.skip(tx, None, "the receipt names no block");
            return;
        };
        // Decode first: a redemption that paid nothing gets no row and must
        // cost no header read, which can stall for the RPC timeout.
        let Some(mut row) = self.decode_or_skip(receipt, block) else {
            return;
        };
        let Some(landed_at_unix) = self
            .landed_at_or_skip(provider, receipt, block, fallback)
            .await
        else {
            return;
        };
        row.landed_at_unix = landed_at_unix;
        if row.net_micro_usdc > row.gross_micro_usdc {
            warn!(
                %tx,
                block,
                net_micro_usdc = row.net_micro_usdc,
                gross_micro_usdc = row.gross_micro_usdc,
                "receipt carries more router-to-operator USDC than the Settled amount"
            );
        }
        self.write(row).await;
    }

    /// The row's timestamp, or `None` when it cannot be read and `fallback`
    /// says to write no row.
    async fn landed_at_or_skip<P: Provider>(
        &self,
        provider: &P,
        receipt: &TransactionReceipt,
        block: u64,
        fallback: TimeFallback,
    ) -> Option<u64> {
        if let Some(at) = landed_at(provider, receipt, block).await {
            return Some(at);
        }
        let tx = receipt.transaction_hash;
        match fallback {
            TimeFallback::WallClock => {
                warn!(
                    %tx,
                    block,
                    "settlement block time unavailable; stamping the ledger row with the wall \
                     clock"
                );
                Some(unix_now())
            }
            TimeFallback::Skip => {
                self.skip(
                    tx,
                    Some(block),
                    "block time unavailable on a backfill (a wall-clock stamp could place an old \
                     settlement in the wrong period)",
                );
                None
            }
        }
    }

    /// The decoded row with `landed_at_unix` still `0`, or `None`: silently
    /// for a redemption that paid nothing, counted for a receipt that yields no
    /// row.
    fn decode_or_skip(&self, receipt: &TransactionReceipt, block: u64) -> Option<SettlementRecord> {
        let tx = receipt.transaction_hash;
        match settlement_from_receipt(receipt, self.operator, block, 0) {
            Ok(Some(row)) => Some(row),
            Ok(None) => {
                debug!(
                    %tx,
                    block,
                    "redeemMany landed but paid nothing (every voucher at or below its \
                     watermark); nothing routed, no ledger row"
                );
                None
            }
            Err(err) => {
                self.skip(tx, Some(block), &err.to_string());
                None
            }
        }
    }

    /// Write `row`, logging the outcome and counting a failure.
    async fn write(&self, row: SettlementRecord) {
        let written = self
            .on_ledger(move |ledger| ledger.record_settlement(&row))
            .await;
        match written {
            Ok(()) => debug!(
                tx = %row.tx_hash,
                block = row.block_number,
                net_micro_usdc = row.net_micro_usdc,
                gross_micro_usdc = row.gross_micro_usdc,
                bytes = row.bytes_delivered,
                "recorded settlement ledger row"
            ),
            Err(error) => {
                warn!(
                    error,
                    tx = %row.tx_hash,
                    block = row.block_number,
                    "settlement ledger write failed; `decdn node earnings` misses this redemption"
                );
                self.metrics.settlement_ledger_write_failure();
            }
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests;
