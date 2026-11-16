//! Writes the settlement ledger (#2598): one [`SettlementRecord`] per landed
//! `redeemMany`, decoded from the node's own transaction receipt by
//! [`settlement_from_receipt`].
//!
//! A `redeemMany` that paid anything pays the operator share in the same
//! transaction: one USDC `Transfer` from the pool to the operator, the exact
//! net income. The pool keeps the rest as the pending protocol share, which
//! `PaymentPool.pending(operator)` reports until a sweep. Each pool group that
//! paid emits one `PoolRedeemed` naming the provider and the bytes each lane
//! paid for. A `redeemMany` that paid nothing (every voucher at or below its
//! lane's watermark, as after a re-send of a batch that had already landed)
//! carries no transfer and gets no row. Decoding the receipt costs no extra
//! RPC; the redeemer's one read is the block header, for the row's timestamp,
//! only when the receipt's logs do not carry it and only once the receipt has
//! decoded to a row.
//!
//! Every `redeemMany` hash the redeemer gets back from its send is recorded
//! ([`SettlementRecorder::record_sent`]) before the redeemer waits for the
//! receipt, and dropped once the receipt is decoded. A receipt the redeemer
//! never saw (a wait that timed out, or a crash before the receipt) is fetched
//! by that hash on the next boot and at each periodic sweep
//! ([`SettlementRecorder::recover_sent`]). Rows key on `(deployment,
//! block_number, log_index)`, so a receipt decoded twice writes one row. The
//! hash reaches the store only after the RPC accepts the send, so a crash in
//! that one gap loses the row; `decdn node earnings --address` still has the
//! chain's record.
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
use decdn_incentive::payment_pool::PaymentPool;
use decdn_incentive::{Deployment, SettlementLedger, SettlementRecord, StoreError};
use tracing::{debug, warn};

use crate::chain_events::timed;
use crate::metrics::Metrics;
use crate::payment_settlement::unix_now;

/// Basis-point denominator of `PaymentPool.operatorBps()`.
const BPS_DENOMINATOR: u128 = 10_000;

/// The pool a recorder decodes receipts of: its deployment, the USDC token it
/// pays in (`PaymentPool.usdc()`), and its operator share
/// (`PaymentPool.operatorBps()`), which turns the net income back into the
/// gross payout.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LedgerSource {
    /// The pool.
    pub deployment: Deployment,
    /// The token the pool pays in.
    pub usdc: Address,
    /// The pool's operator share, in basis points.
    pub operator_bps: u16,
}

/// Why a landed receipt yields no ledger row.
#[derive(Debug)]
pub enum DecodeError {
    /// A `Transfer`-shaped log of the pool's token does not decode, so the net
    /// cannot be read.
    UndecodableTransfer(alloy::sol_types::Error),
    /// A `PoolRedeemed`-shaped log of the pool does not decode as this
    /// binary's ABI, so the bytes cannot be read.
    UndecodablePoolRedeemed(alloy::sol_types::Error),
    /// The operator transfer carries no log index, so the row has no identity.
    MissingLogIndex,
}

impl std::fmt::Display for DecodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UndecodableTransfer(err) => {
                write!(
                    f,
                    "an ERC-20 Transfer log in the receipt does not decode: {err}"
                )
            }
            Self::UndecodablePoolRedeemed(err) => write!(
                f,
                "a PaymentPool.PoolRedeemed log does not decode as this binary's ABI: {err}"
            ),
            Self::MissingLogIndex => {
                f.write_str("the operator's USDC transfer carries no log index")
            }
        }
    }
}

impl std::error::Error for DecodeError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::UndecodableTransfer(err) | Self::UndecodablePoolRedeemed(err) => Some(err),
            Self::MissingLogIndex => None,
        }
    }
}

/// The ledger row for `operator`'s landed `redeemMany` receipt on
/// `source.deployment`, in `block_number`.
///
/// - **Net:** the sum of every `Transfer` the pool's token (`source.usdc`)
///   emitted from the pool to `operator`. The row's log index is the first
///   such transfer's.
/// - **Gross:** the pool pays `floor(gross × operatorBps / 10000)` to the
///   operator, so the gross is the smallest payout that floors to the net:
///   `ceil(net × 10000 / operatorBps)`. It understates the true payout by less
///   than `10000 / operatorBps` micro-USDC (under 2 at a 60% share). The share
///   is the one the node seeded at startup; when that read fell back to the
///   floor share, the gross is approximate.
/// - **Bytes:** the sum of `bytesPaid` over every lane of every
///   `PoolRedeemed` the pool emitted for `operator` as provider.
///
/// `Ok(None)` is a receipt with no operator transfer: a `redeemMany` that paid
/// nothing, or one whose operator share floored to zero. A pool with a zero
/// `operatorBps` never pays one, so it yields no rows. Amounts narrow from
/// `uint256` to `u64` micro-USDC with saturation; the on-chain counters are
/// `uint64`, so a real value never saturates.
///
/// # Errors
///
/// An undecodable transfer or `PoolRedeemed`, or an operator transfer with no
/// log index: see [`DecodeError`].
pub fn settlement_from_receipt(
    receipt: &TransactionReceipt,
    operator: Address,
    source: LedgerSource,
    block_number: u64,
    landed_at_unix: u64,
) -> Result<Option<SettlementRecord>, DecodeError> {
    let pool = source.deployment.payment_pool;
    let logs = receipt.inner.logs();
    let mut net = 0u64;
    let mut first_transfer: Option<&alloy::rpc::types::Log> = None;
    for log in logs.iter().filter(|log| {
        log.address() == source.usdc && log.topic0() == Some(&Erc20::Transfer::SIGNATURE_HASH)
    }) {
        let transfer = Erc20::Transfer::decode_log_data(&log.inner.data)
            .map_err(DecodeError::UndecodableTransfer)?;
        if transfer.from == pool && transfer.to == operator {
            net = net.saturating_add(u64::try_from(transfer.value).unwrap_or(u64::MAX));
            first_transfer.get_or_insert(log);
        }
    }
    let Some(paid) = first_transfer else {
        return Ok(None);
    };
    if source.operator_bps == 0 {
        return Ok(None);
    }
    let log_index = paid.log_index.ok_or(DecodeError::MissingLogIndex)?;
    let mut bytes = 0u64;
    for log in logs.iter().filter(|log| {
        log.address() == pool && log.topic0() == Some(&PaymentPool::PoolRedeemed::SIGNATURE_HASH)
    }) {
        let redeemed = PaymentPool::PoolRedeemed::decode_log_data(&log.inner.data)
            .map_err(DecodeError::UndecodablePoolRedeemed)?;
        if redeemed.provider == operator {
            for lane in &redeemed.lanes {
                bytes = bytes.saturating_add(lane.bytesPaid);
            }
        }
    }
    let gross = u128::from(net)
        .saturating_mul(BPS_DENOMINATOR)
        .div_ceil(u128::from(source.operator_bps));
    Ok(Some(SettlementRecord {
        deployment: source.deployment,
        block_number,
        log_index,
        tx_hash: receipt.transaction_hash,
        landed_at_unix,
        gross_micro_usdc: u64::try_from(gross).unwrap_or(u64::MAX),
        net_micro_usdc: net,
        bytes_delivered: bytes,
        operator,
    }))
}

/// How many times a backfill retries a failed header read before it gives up,
/// and the pause before each retry. The redeemer's own path makes one attempt:
/// its fallback is the wall clock, seconds from the landing, and each retry
/// would hold the sweep for the call timeout before its next chunk.
const HEADER_READ_RETRIES: u32 = 2;
const HEADER_READ_RETRY_DELAY: Duration = Duration::from_secs(1);

/// The timestamp of `block`, Unix seconds: the one the receipt's logs carry
/// when the RPC fills it in, else one `eth_getBlockByNumber`, retried
/// `retries` times. `None` when every read fails; the caller decides between
/// the wall clock and no row.
pub(crate) async fn landed_at<P: Provider>(
    provider: &P,
    receipt: &TransactionReceipt,
    block: u64,
    retries: u32,
) -> Option<u64> {
    if let Some(timestamp) = receipt
        .inner
        .logs()
        .iter()
        .find_map(|log| log.block_timestamp)
    {
        return Some(timestamp);
    }
    for attempt in 0..=retries {
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
    source: LedgerSource,
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
    /// A recorder that writes rows for `operator`'s redemptions on `source`'s
    /// pool into `ledger` and counts its failures into `metrics`.
    #[must_use]
    pub fn new(
        ledger: Arc<dyn SettlementLedger>,
        operator: Address,
        source: LedgerSource,
        metrics: Arc<Metrics>,
    ) -> Self {
        Self {
            ledger,
            operator,
            source,
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

    /// Record the hash of a `redeemMany` the RPC accepted, before the
    /// redeemer waits for its receipt. A failed write is logged and counted:
    /// the redemption itself stands, and a receipt the wait does see still
    /// writes its row.
    pub async fn record_sent(&self, tx_hash: B256) {
        let deployment = self.source.deployment;
        if let Err(error) = self
            .on_ledger(move |ledger| ledger.record_sent_redeem(deployment, tx_hash))
            .await
        {
            warn!(error, tx = %tx_hash, "could not record a sent redeemMany hash");
            self.metrics.settlement_ledger_write_failure();
        }
    }

    /// Drop a recorded hash whose receipt is decoded.
    pub async fn forget_sent(&self, tx_hash: B256) {
        let deployment = self.source.deployment;
        if let Err(error) = self
            .on_ledger(move |ledger| ledger.forget_sent_redeem(deployment, tx_hash))
            .await
        {
            warn!(error, tx = %tx_hash, "could not drop a sent redeemMany hash");
        }
    }

    /// Fetch the receipt of every recorded `redeemMany` hash: a receipt the
    /// redeemer never decoded (a wait that timed out, or a crash before the
    /// receipt). A mined receipt writes its row and drops the hash; a hash the
    /// chain does not know (a dropped or replaced transaction) is dropped; a
    /// transaction still pending, or a failed read, keeps the hash for the next
    /// call. A block time the RPC cannot supply skips the row: this path can
    /// run long after the landing.
    pub async fn recover_sent<P: Provider>(&self, provider: &P) {
        let deployment = self.source.deployment;
        let sent = match self
            .on_ledger(move |ledger| ledger.sent_redeems(deployment))
            .await
        {
            Ok(sent) => sent,
            Err(error) => {
                warn!(error, "could not read the sent redeemMany hashes");
                return;
            }
        };
        for tx_hash in sent {
            self.recover_one(provider, tx_hash).await;
        }
    }

    /// Recover one sent hash: see [`Self::recover_sent`].
    async fn recover_one<P: Provider>(&self, provider: &P, tx_hash: B256) {
        let receipt = timed(
            None,
            "sent redeemMany getTransactionReceipt",
            provider.get_transaction_receipt(tx_hash),
        )
        .await;
        match receipt {
            Ok(Some(receipt)) => {
                debug!(tx = %tx_hash, "recovering a settlement ledger row by its sent hash");
                if receipt.status() {
                    self.record(provider, &receipt, TimeFallback::Skip).await;
                }
                self.forget_sent(tx_hash).await;
            }
            Ok(None) => self.drop_if_unknown(provider, tx_hash).await,
            Err(err) => debug!(
                tx = %tx_hash,
                error = %sanitize_err_chain(&err),
                "sent redeemMany receipt read failed; retrying later"
            ),
        }
    }

    /// Drop a sent hash with no receipt when the chain does not know the
    /// transaction at all; keep a pending one.
    async fn drop_if_unknown<P: Provider>(&self, provider: &P, tx_hash: B256) {
        let known = timed(
            None,
            "sent redeemMany getTransactionByHash",
            provider.get_transaction_by_hash(tx_hash),
        )
        .await;
        match known {
            Ok(None) => {
                debug!(tx = %tx_hash, "sent redeemMany is unknown to the chain; dropping its hash");
                self.forget_sent(tx_hash).await;
            }
            Ok(Some(_)) => {}
            Err(err) => debug!(
                tx = %tx_hash,
                error = %sanitize_err_chain(&err),
                "sent redeemMany lookup failed; retrying later"
            ),
        }
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
        let retries = match fallback {
            TimeFallback::WallClock => 0,
            TimeFallback::Skip => HEADER_READ_RETRIES,
        };
        if let Some(at) = landed_at(provider, receipt, block, retries).await {
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
        match settlement_from_receipt(receipt, self.operator, self.source, block, 0) {
            Ok(Some(row)) => Some(row),
            Ok(None) => {
                debug!(
                    %tx,
                    block,
                    "redeemMany landed but paid the operator nothing (every voucher at or \
                     below its watermark, or a zero operator share); no ledger row"
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
