//! Decodes a landed `redeemMany` receipt into one settlement ledger row
//! ([`settlement_from_receipt`]).
//!
//! A `redeemMany` that paid anything pays the operator share in the same
//! transaction: one USDC `Transfer` from the pool to the operator, the exact
//! net income. The pool keeps the rest as the pending protocol share, which
//! `PaymentPool.pending(operator)` reports until a sweep. Each pool group that
//! paid emits one `PoolRedeemed` naming the provider and the bytes each lane
//! paid for. The node's settlement ledger decodes its own receipts with it,
//! and `decdn node earnings --address` decodes the receipts of the transfers
//! it finds on chain, so the two tables agree.

use alloy::primitives::Address;
use alloy::rpc::types::TransactionReceipt;
use alloy::sol_types::SolEvent;

use crate::deployment::Deployment;
use crate::erc20::Erc20;
use crate::payment_pool::PaymentPool;
use crate::store::SettlementRecord;

/// Basis-point denominator of `PaymentPool.operatorBps()`.
pub const BPS_DENOMINATOR: u128 = 10_000;

/// The pool a receipt is decoded against: its deployment, the USDC token it
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
///   than `10000 / operatorBps` micro-USDC (under 2 at a 60% share).
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

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests;
