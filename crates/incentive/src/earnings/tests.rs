use alloy::primitives::B256;

use super::*;

fn row(
    block: u64,
    log_index: u64,
    landed_at: u64,
    net: u64,
    gross: u64,
    bytes: u64,
) -> SettlementRecord {
    SettlementRecord {
        block_number: block,
        log_index,
        tx_hash: B256::repeat_byte(u8::try_from(block % 256).unwrap_or(0)),
        landed_at_unix: landed_at,
        epoch: 1,
        gross_micro_usdc: gross,
        net_micro_usdc: net,
        bytes_delivered: bytes,
    }
}

#[test]
fn sum_period_adds_every_field_and_counts_rows() {
    let rows = [
        row(1, 0, 10, 60, 100, 1_000),
        row(2, 3, 20, 120, 200, 2_000),
    ];
    let period = sum_period(5, &rows);
    assert_eq!(period.from_unix, 5);
    assert_eq!(period.net_micro_usdc, 180);
    assert_eq!(period.gross_micro_usdc, 300);
    assert_eq!(period.bytes_settled, 3_000);
    assert_eq!(period.settlements, 2);
}

#[test]
fn sum_period_of_nothing_is_zero() {
    let period = sum_period(7, &[]);
    assert_eq!(period.settlements, 0);
    assert_eq!(period.net_micro_usdc, 0);
    assert_eq!(period.gross_micro_usdc, 0);
    assert_eq!(period.bytes_settled, 0);
}

#[test]
fn sum_period_saturates() {
    let rows = [
        row(1, 0, 10, u64::MAX, u64::MAX, u64::MAX),
        row(2, 0, 20, 1, 1, 1),
    ];
    let period = sum_period(0, &rows);
    assert_eq!(period.net_micro_usdc, u64::MAX);
    assert_eq!(period.gross_micro_usdc, u64::MAX);
    assert_eq!(period.bytes_settled, u64::MAX);
}

#[test]
fn last_settlement_orders_by_chain_position_not_wall_clock() {
    // The row at the higher block carries the lower (wall-clock fallback)
    // timestamp; chain order still wins.
    let rows = [
        row(10, 0, 500, 1, 1, 1),
        row(20, 0, 100, 2, 2, 2),
        row(20, 1, 50, 3, 3, 3),
    ];
    let last = last_settlement(&rows).expect("rows");
    assert_eq!(last.block_number, 20);
    assert_eq!(last.net_micro_usdc, 3);
    assert_eq!(last.at_unix, 50);
    assert_eq!(last.tx_hash, format!("{:#x}", B256::repeat_byte(20)));
}

#[test]
fn last_settlement_of_nothing_is_none() {
    assert_eq!(last_settlement(&[]), None);
}
