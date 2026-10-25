//! Writes the settlement ledger (#2598): one [`SettlementRecord`] per landed
//! `redeemMany`, decoded from the transaction receipt.
//!
//! A `redeemMany` routes its whole payout through `FeeRouter.routeSettlement`
//! once, so its receipt carries exactly one `FeeRouter.Settled` for this
//! operator (gross amount, bytes, epoch) and, when the operator share is not
//! zero, one USDC `Transfer` from the router to the operator (the exact net
//! payout). Decoding the receipt costs no extra RPC; the one read is the block
//! header, for the row's timestamp.
//!
//! Two writers share [`SettlementRecorder`]: the redeemer, which has the
//! receipt in hand when its `redeemMany` lands, and the `PoolRedeemed` watcher,
//! which backfills a row for a transaction the redeemer never saw land (a
//! receipt wait that timed out, or a landing across a restart). Rows key on
//! `(block_number, log_index)`, so the two writers never duplicate a row.

use std::sync::Arc;

use alloy::eips::BlockId;
use alloy::primitives::{Address, B256};
use alloy::providers::Provider;
use alloy::rpc::types::TransactionReceipt;
use alloy::sol_types::SolEvent;
use decdn_common::redact::sanitize_err_chain;
use decdn_incentive::erc20::Erc20;
use decdn_incentive::payment_pool::FeeRouter;
use decdn_incentive::{SettlementLedger, SettlementRecord};
use tracing::{debug, warn};

use crate::chain_events::timed;
use crate::payment_settlement::unix_now;

/// Decode the ledger row for a landed `redeemMany` from its receipt.
///
/// The row is the `Settled` log whose `operator` is `operator`; its emitter is
/// the `FeeRouter`, and the net payout is the sum of the USDC `Transfer` logs
/// from that emitter to `operator` in the same receipt (`0` when the operator
/// share is `0`). `None` when the receipt holds no such `Settled`, which a
/// landed `redeemMany` of this node cannot produce (a paused router reverts the
/// transaction); the caller logs it.
///
/// Amounts narrow from `uint256` to `u64` micro-USDC with saturation. The
/// on-chain counters are `uint64`, so a real value never saturates.
pub(crate) fn settlement_from_receipt(
    receipt: &TransactionReceipt,
    operator: Address,
    landed_at_unix: u64,
) -> Option<SettlementRecord> {
    let logs = receipt.inner.logs();
    let (settled_log, settled) = logs.iter().find_map(|log| {
        if log.topic0() != Some(&FeeRouter::Settled::SIGNATURE_HASH) {
            return None;
        }
        let event = FeeRouter::Settled::decode_log_data(&log.inner.data).ok()?;
        (event.operator == operator).then_some((log, event))
    })?;
    let router = settled_log.address();
    let net = logs
        .iter()
        .filter(|log| log.topic0() == Some(&Erc20::Transfer::SIGNATURE_HASH))
        .filter_map(|log| Erc20::Transfer::decode_log_data(&log.inner.data).ok())
        .filter(|transfer| transfer.from == router && transfer.to == operator)
        .fold(0u64, |sum, transfer| {
            sum.saturating_add(u64::try_from(transfer.value).unwrap_or(u64::MAX))
        });
    Some(SettlementRecord {
        block_number: receipt
            .block_number
            .or(settled_log.block_number)
            .unwrap_or_default(),
        log_index: settled_log.log_index.unwrap_or_default(),
        tx_hash: receipt.transaction_hash,
        landed_at_unix,
        epoch: settled.epoch,
        gross_micro_usdc: u64::try_from(settled.amount).unwrap_or(u64::MAX),
        net_micro_usdc: net,
        bytes_delivered: u64::try_from(settled.bytesDelivered).unwrap_or(u64::MAX),
    })
}

/// The timestamp of `block`, Unix seconds: one `eth_getBlockByNumber`. Falls
/// back to the wall clock, with a warning, when the header cannot be read; the
/// row then buckets into the period of the read, not of the landing, which is
/// the same period in every case but a read minutes late across a boundary.
pub(crate) async fn landed_at<P: Provider>(provider: &P, block: u64) -> u64 {
    match timed(
        None,
        "ledger block header",
        provider.get_block(BlockId::number(block)),
    )
    .await
    {
        Ok(Some(header)) => header.header.timestamp,
        Ok(None) => {
            warn!(
                block,
                "settlement block header not found; stamping the ledger row with the wall clock"
            );
            unix_now()
        }
        Err(err) => {
            warn!(
                error = %sanitize_err_chain(&err),
                block,
                "settlement block header read failed; stamping the ledger row with the wall clock"
            );
            unix_now()
        }
    }
}

/// Writes ledger rows for one operator. Cheap to clone: an `Arc` over the
/// ledger plus the operator address.
#[derive(Clone)]
pub(crate) struct SettlementRecorder {
    ledger: Arc<dyn SettlementLedger>,
    operator: Address,
}

impl std::fmt::Debug for SettlementRecorder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SettlementRecorder")
            .field("operator", &self.operator)
            .finish_non_exhaustive()
    }
}

impl SettlementRecorder {
    /// A recorder that writes rows for `operator` into `ledger`.
    pub(crate) fn new(ledger: Arc<dyn SettlementLedger>, operator: Address) -> Self {
        Self { ledger, operator }
    }

    /// Record the row for a `redeemMany` receipt that landed. A receipt
    /// without a `Settled` for the operator, and a ledger write that fails,
    /// are logged and dropped: the redemption itself landed, and the ledger is
    /// what `decdn node earnings` reads, not a claim.
    pub(crate) async fn record_landed<P: Provider>(
        &self,
        provider: &P,
        receipt: &TransactionReceipt,
    ) {
        let tx = receipt.transaction_hash;
        let Some(block) = receipt.block_number else {
            warn!(%tx, "landed redeemMany receipt names no block; no ledger row");
            return;
        };
        let landed_at_unix = landed_at(provider, block).await;
        let Some(row) = settlement_from_receipt(receipt, self.operator, landed_at_unix) else {
            warn!(
                %tx,
                block,
                "landed redeemMany receipt carries no FeeRouter.Settled for this operator; no \
                 ledger row"
            );
            return;
        };
        let ledger = Arc::clone(&self.ledger);
        let written = tokio::task::spawn_blocking(move || ledger.record_settlement(&row))
            .await
            .map_err(|join| join.to_string())
            .and_then(|write| write.map_err(|err| err.to_string()));
        report_write(written, &row);
    }

    /// Record the row for a `redeemMany` the `PoolRedeemed` watcher saw, when
    /// the ledger has no row for `tx_hash` yet: one `eth_getTransactionReceipt`,
    /// then [`Self::record_landed`]. Covers a receipt wait that timed out and a
    /// landing across a restart. A transaction already in the ledger costs one
    /// ledger read and no RPC.
    pub(crate) async fn backfill<P: Provider>(&self, provider: &P, tx_hash: B256) {
        let ledger = Arc::clone(&self.ledger);
        let known = tokio::task::spawn_blocking(move || ledger.has_settlement_tx(tx_hash))
            .await
            .map_err(|join| join.to_string())
            .and_then(|read| read.map_err(|err| err.to_string()));
        if known_or_warn(known, tx_hash) != Some(false) {
            return;
        }
        let fetched = timed(
            None,
            "ledger backfill getTransactionReceipt",
            provider.get_transaction_receipt(tx_hash),
        )
        .await;
        if let Some(receipt) = receipt_or_warn(fetched, tx_hash) {
            debug!(tx = %tx_hash, "backfilling a settlement ledger row from PoolRedeemed");
            self.record_landed(provider, &receipt).await;
        }
    }
}

/// Log a ledger write's outcome: the row at `debug`, a failure at `warn`.
fn report_write(written: Result<(), String>, row: &SettlementRecord) {
    match written {
        Ok(()) => debug!(
            tx = %row.tx_hash,
            block = row.block_number,
            net_micro_usdc = row.net_micro_usdc,
            gross_micro_usdc = row.gross_micro_usdc,
            bytes = row.bytes_delivered,
            "recorded settlement ledger row"
        ),
        Err(error) => warn!(
            error,
            tx = %row.tx_hash,
            block = row.block_number,
            "settlement ledger write failed; `decdn node earnings` misses this redemption"
        ),
    }
}

/// Whether the ledger holds `tx_hash`; `None`, after a warning, when the read
/// failed and the backfill must stop.
fn known_or_warn(known: Result<bool, String>, tx_hash: B256) -> Option<bool> {
    match known {
        Ok(known) => Some(known),
        Err(error) => {
            warn!(error, tx = %tx_hash, "settlement ledger read failed; skipping backfill");
            None
        }
    }
}

/// The receipt of `tx_hash`, or `None` after a warning naming why there is
/// none to record.
fn receipt_or_warn(
    fetched: anyhow::Result<Option<TransactionReceipt>>,
    tx_hash: B256,
) -> Option<TransactionReceipt> {
    match fetched {
        Ok(Some(receipt)) => Some(receipt),
        Ok(None) => {
            warn!(
                tx = %tx_hash,
                "PoolRedeemed names a transaction the RPC has no receipt for; no ledger row"
            );
            None
        }
        Err(err) => {
            warn!(
                error = %sanitize_err_chain(&err),
                tx = %tx_hash,
                "settlement ledger backfill receipt read failed; no ledger row"
            );
            None
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests;
