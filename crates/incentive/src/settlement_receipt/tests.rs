use alloy::primitives::{Address, B256, Bloom, Log as PrimitiveLog, U256};
use alloy::rpc::types::Log;
use alloy::sol_types::SolEvent;

use super::*;

const OPERATOR: Address = Address::repeat_byte(0x0E);
const USDC: Address = Address::repeat_byte(0xA0);
const POOL: Address = Address::repeat_byte(0xB0);
const DEPLOYMENT: Deployment = Deployment {
    chain_id: 31_337,
    payment_pool: POOL,
};
/// A 60% operator share, the reference deployment's.
const SOURCE: LedgerSource = LedgerSource {
    deployment: DEPLOYMENT,
    usdc: USDC,
    operator_bps: 6_000,
};

/// A confirmed receipt at `block` carrying `logs`. `TransactionReceipt` has
/// no constructor, so every field the decoder does not read is filler.
fn receipt(block: Option<u64>, logs: Vec<Log>) -> TransactionReceipt {
    TransactionReceipt {
        inner: alloy::consensus::ReceiptEnvelope::Eip1559(alloy::consensus::ReceiptWithBloom {
            receipt: alloy::consensus::Receipt {
                status: alloy::consensus::Eip658Value::Eip658(true),
                cumulative_gas_used: 0,
                logs,
            },
            logs_bloom: Bloom::ZERO,
        }),
        transaction_hash: B256::repeat_byte(0x55),
        transaction_index: Some(0),
        block_hash: None,
        block_number: block,
        gas_used: 0,
        effective_gas_price: 0,
        blob_gas_used: None,
        blob_gas_price: None,
        from: OPERATOR,
        to: None,
        contract_address: None,
    }
}

fn log(address: Address, data: alloy::primitives::LogData, log_index: u64) -> Log {
    Log {
        inner: PrimitiveLog { address, data },
        block_hash: None,
        block_number: Some(900),
        block_timestamp: None,
        transaction_hash: Some(B256::repeat_byte(0x55)),
        transaction_index: Some(0),
        log_index: Some(log_index),
        removed: false,
    }
}

fn transfer(token: Address, from: Address, to: Address, value: u64, log_index: u64) -> Log {
    let event = Erc20::Transfer {
        from,
        to,
        value: U256::from(value),
    };
    log(token, event.encode_log_data(), log_index)
}

/// A `PoolRedeemed` from `pool` for `provider`, one lane per `bytes` entry.
fn pool_redeemed(pool: Address, provider: Address, bytes: &[u64], log_index: u64) -> Log {
    let event = PaymentPool::PoolRedeemed {
        poolId: B256::repeat_byte(0x11),
        provider,
        lanes: bytes
            .iter()
            .map(|&bytes_paid| PaymentPool::LaneSettled {
                signer: Address::repeat_byte(0x5A),
                newPaidCumulative: 1_000,
                bytesPaid: bytes_paid,
            })
            .collect(),
    };
    log(pool, event.encode_log_data(), log_index)
}

/// Replace a log's data bytes with garbage, keeping its topics.
fn corrupt(mut log: Log) -> Log {
    log.inner.data = alloy::primitives::LogData::new_unchecked(
        log.inner.data.topics().to_vec(),
        alloy::primitives::Bytes::from(vec![0u8; 3]),
    );
    log
}

fn decode(logs: Vec<Log>) -> Result<Option<SettlementRecord>, DecodeError> {
    settlement_from_receipt(
        &receipt(Some(900), logs),
        OPERATOR,
        SOURCE,
        900,
        1_700_000_000,
    )
}

/// The operator's income is the pool's USDC `Transfer` to it; gross scales
/// it by `10000 / operatorBps`; bytes sum the pool's `PoolRedeemed` lanes for
/// the operator, across every pool group in the batch.
#[test]
fn a_redeem_receipt_decodes_net_gross_and_bytes() {
    let logs = vec![
        pool_redeemed(POOL, OPERATOR, &[1_024, 2_048], 0),
        pool_redeemed(POOL, OPERATOR, &[1_024], 1),
        transfer(USDC, POOL, OPERATOR, 600, 2),
    ];
    let row = decode(logs).expect("decodes").expect("a row");
    assert_eq!(row.deployment, DEPLOYMENT);
    assert_eq!(row.block_number, 900);
    assert_eq!(
        row.log_index, 2,
        "the row is keyed on the operator transfer"
    );
    assert_eq!(row.tx_hash, B256::repeat_byte(0x55));
    assert_eq!(row.landed_at_unix, 1_700_000_000);
    assert_eq!(row.net_micro_usdc, 600);
    assert_eq!(row.gross_micro_usdc, 1_000);
    assert_eq!(row.bytes_delivered, 4_096);
    assert_eq!(row.operator, OPERATOR);
}

/// The contract floors the operator share, so the gross is the smallest
/// payout that floors to the net: rounded up, never over the true payout.
#[test]
fn the_gross_rounds_up_to_the_smallest_payout_with_that_share() {
    // A payout of 1_001 at 60% pays floor(600.6) = 600 to the operator; the
    // smallest payout paying 600 is 1_000.
    let logs = vec![
        pool_redeemed(POOL, OPERATOR, &[10], 0),
        transfer(USDC, POOL, OPERATOR, 601, 1),
    ];
    let row = decode(logs).expect("decodes").expect("a row");
    // 601 × 10000 / 6000 = 1001.67, rounded up.
    assert_eq!(row.gross_micro_usdc, 1_002);
}

/// Transfers to someone else, from someone else, or of another token are not
/// the operator's income from this pool.
#[test]
fn foreign_transfers_and_other_tokens_are_ignored() {
    let other = Address::repeat_byte(0x99);
    let logs = vec![
        pool_redeemed(POOL, OPERATOR, &[512], 0),
        transfer(USDC, POOL, other, 300, 1),
        transfer(USDC, other, OPERATOR, 300, 2),
        transfer(Address::repeat_byte(0xC0), POOL, OPERATOR, 900, 3),
        transfer(USDC, POOL, OPERATOR, 600, 4),
    ];
    let row = decode(logs).expect("decodes").expect("a row");
    assert_eq!(row.net_micro_usdc, 600);
    assert_eq!(row.log_index, 4);
}

/// Another provider's lanes and another pool's events add no bytes.
#[test]
fn foreign_pool_redeemed_lanes_are_ignored() {
    let logs = vec![
        pool_redeemed(POOL, Address::repeat_byte(0x99), &[9_999], 0),
        pool_redeemed(Address::repeat_byte(0xB1), OPERATOR, &[9_999], 1),
        pool_redeemed(POOL, OPERATOR, &[100], 2),
        transfer(USDC, POOL, OPERATOR, 60, 3),
    ];
    let row = decode(logs).expect("decodes").expect("a row");
    assert_eq!(row.bytes_delivered, 100);
}

/// No operator transfer is no row: the batch paid nothing, or its operator
/// share floored to zero.
#[test]
fn a_receipt_without_an_operator_transfer_is_no_row() {
    assert!(matches!(decode(vec![]), Ok(None)));
    let logs = vec![
        pool_redeemed(POOL, OPERATOR, &[100], 0),
        transfer(Address::repeat_byte(0xC0), POOL, OPERATOR, 60, 1),
    ];
    assert!(matches!(decode(logs), Ok(None)));
}

/// A pool with no operator share pays the operator nothing in the
/// transaction, so it yields no row and no division by zero.
#[test]
fn a_zero_operator_share_is_no_row() {
    let source = LedgerSource {
        operator_bps: 0,
        ..SOURCE
    };
    let decoded = settlement_from_receipt(
        &receipt(
            Some(900),
            vec![
                pool_redeemed(POOL, OPERATOR, &[100], 0),
                transfer(USDC, POOL, OPERATOR, 60, 1),
            ],
        ),
        OPERATOR,
        source,
        900,
        0,
    );
    assert!(matches!(decoded, Ok(None)));
}

/// An undecodable event from the pool or the token is an error, never a zero.
#[test]
fn an_undecodable_event_is_an_error() {
    let logs = vec![
        corrupt(pool_redeemed(POOL, OPERATOR, &[100], 0)),
        transfer(USDC, POOL, OPERATOR, 60, 1),
    ];
    assert!(matches!(
        decode(logs),
        Err(DecodeError::UndecodablePoolRedeemed(_))
    ));
    let logs = vec![
        pool_redeemed(POOL, OPERATOR, &[100], 0),
        corrupt(transfer(USDC, POOL, OPERATOR, 60, 1)),
    ];
    assert!(matches!(
        decode(logs),
        Err(DecodeError::UndecodableTransfer(_))
    ));
}
