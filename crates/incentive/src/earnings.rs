//! Sums over the settlement ledger for `admin_v1_earnings` (#2598).
//!
//! Both producers of an [`EarningsResponse`] use these: the daemon over its
//! own [`SettlementRecord`] rows, and `decdn node earnings --address` over
//! rows it rebuilt from `FeeRouter.Settled` logs. One summer for both keeps the
//! two tables from drifting apart.
//!
//! [`EarningsResponse`]: decdn_common::admin::EarningsResponse

use decdn_common::admin::{EarningsPeriod, LastSettlement};

use crate::store::SettlementRecord;

/// Sum `rows` into one period starting at `from_unix`. The caller has already
/// chosen which rows fall in the period (by `landed_at_unix` on the daemon, by
/// block number in chain mode), so this only adds. Every sum saturates.
pub fn sum_period<'a>(
    from_unix: u64,
    rows: impl IntoIterator<Item = &'a SettlementRecord>,
) -> EarningsPeriod {
    let mut period = EarningsPeriod {
        from_unix,
        net_micro_usdc: 0,
        gross_micro_usdc: 0,
        bytes_settled: 0,
        settlements: 0,
    };
    for row in rows {
        period.net_micro_usdc = period.net_micro_usdc.saturating_add(row.net_micro_usdc);
        period.gross_micro_usdc = period.gross_micro_usdc.saturating_add(row.gross_micro_usdc);
        period.bytes_settled = period.bytes_settled.saturating_add(row.bytes_delivered);
        period.settlements = period.settlements.saturating_add(1);
    }
    period
}

/// The most recent row in chain order, `(block_number, log_index)`, or `None`
/// for no rows. Never by `landed_at_unix`: a row stamped with the wall clock
/// after a failed header read could sort out of order.
pub fn last_settlement<'a>(
    rows: impl IntoIterator<Item = &'a SettlementRecord>,
) -> Option<LastSettlement> {
    rows.into_iter()
        .max_by_key(|row| row.key())
        .map(|row| LastSettlement {
            at_unix: row.landed_at_unix,
            tx_hash: format!("{:#x}", row.tx_hash),
            block_number: row.block_number,
            net_micro_usdc: row.net_micro_usdc,
        })
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests;
