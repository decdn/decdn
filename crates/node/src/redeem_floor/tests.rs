use super::*;
use alloy::primitives::Bytes;
use alloy::providers::ProviderBuilder;
use alloy::providers::mock::Asserter;
use alloy::sol_types::SolValue;

/// 1 gwei.
const GWEI: u128 = 1_000_000_000;

/// A quote with no L1 component at `gas_price_wei`.
const fn l2_only(gas_price_wei: u128) -> GasQuote {
    GasQuote {
        gas_price_wei,
        l1_wei_per_byte: U256::ZERO,
        l1_wei_per_tx: U256::ZERO,
    }
}

/// Gas of one claim, execution and calldata.
const CLAIM_GAS: u64 = REDEEM_CLAIM_GAS + REDEEM_CLAIM_CALLDATA_BYTES * CALLDATA_GAS_PER_BYTE;

/// The floors price gas at the configured gas-token price: at 1 gwei and
/// 5,000 USDC per token, one claim costs `CLAIM_GAS × 1e9 wei × 5e9 µUSDC /
/// 1e18`.
#[test]
fn the_floors_price_gas_at_the_gas_token_price() {
    let floors = RedeemFloors::new(l2_only(GWEI), 5_000_000_000);
    let claim_micro = U256::from(CLAIM_GAS) * U256::from(5u64);
    let tx_micro = U256::from(REDEEM_TX_GAS) * U256::from(5u64);
    assert_eq!(
        floors.final_claim(),
        claim_micro * U256::from(FINAL_CLAIM_COST_MULTIPLE)
    );
    assert_eq!(
        floors.chunk(3),
        (tx_micro + claim_micro * U256::from(3u64)) * U256::from(CHUNK_COST_MULTIPLE)
    );
}

/// The floor follows the gas price: doubling the gas price doubles both
/// floors.
#[test]
fn the_floor_follows_the_gas_price() {
    let price = 5_000_000_000;
    let low = RedeemFloors::new(l2_only(GWEI), price);
    let high = RedeemFloors::new(l2_only(2 * GWEI), price);
    assert_eq!(high.final_claim(), low.final_claim() * U256::from(2u64));
    assert_eq!(high.chunk(4), low.chunk(4) * U256::from(2u64));
    assert!(high.chunk(1) > low.chunk(1));
}

/// The L1 data fee adds to the floor: per claim byte and per transaction.
#[test]
fn the_l1_fee_adds_to_the_floor() {
    let price = 1_000_000_000_000_000_000; // 1e18 µUSDC per token: 1 µUSDC per wei.
    let quote = GasQuote {
        gas_price_wei: 0,
        l1_wei_per_byte: U256::from(10u64),
        l1_wei_per_tx: U256::from(1_000u64),
    };
    let floors = RedeemFloors::new(quote, price);
    let claim = U256::from(REDEEM_CLAIM_CALLDATA_BYTES * 10);
    assert_eq!(
        floors.final_claim(),
        claim * U256::from(FINAL_CLAIM_COST_MULTIPLE)
    );
    assert_eq!(
        floors.chunk(2),
        (U256::from(1_000u64) + claim * U256::from(2u64)) * U256::from(CHUNK_COST_MULTIPLE)
    );
}

/// A cost below one `µUSDC` rounds up, so a nonzero cost never prices at 0.
#[test]
fn a_fraction_of_a_micro_usdc_rounds_up() {
    let floors = RedeemFloors::from_costs(U256::from(1u64), U256::from(1u64), 1);
    assert_eq!(floors.final_claim(), U256::from(1u64));
    assert_eq!(floors.chunk(5), U256::from(1u64));
}

/// One claim and a whole transaction's fixed gas cost less than the final
/// claim floor's multiple of one claim, so a final claim that redeems alone
/// pays for its transaction.
#[test]
fn a_lone_final_claim_pays_for_its_transaction() {
    const { assert!(REDEEM_TX_GAS + CLAIM_GAS < FINAL_CLAIM_COST_MULTIPLE * CLAIM_GAS) };
}

/// A chain with no `ArbGasInfo` code prices gas alone, and reads the code
/// once: a later quote reads only the gas price.
#[tokio::test(start_paused = true)]
async fn a_chain_without_arbgasinfo_prices_execution_gas_only() {
    let asserter = Asserter::new();
    asserter.push_success(&U256::from(GWEI));
    asserter.push_success(&Bytes::new());
    asserter.push_success(&U256::from(2 * GWEI));
    let provider = ProviderBuilder::new().connect_mocked_client(asserter.clone());
    let source = FloorSource::live(provider, 5_000_000_000);

    let first = source.floors().await;
    assert_eq!(first, Some(RedeemFloors::new(l2_only(GWEI), 5_000_000_000)));

    tokio::time::advance(QUOTE_TTL).await;
    let second = source.floors().await;
    assert_eq!(
        second,
        Some(RedeemFloors::new(l2_only(2 * GWEI), 5_000_000_000))
    );
    assert!(asserter.read_q().is_empty(), "every queued read was taken");
}

/// On Arbitrum the quote reads the L1 fee from `ArbGasInfo`.
#[tokio::test(start_paused = true)]
async fn an_arbitrum_quote_reads_the_l1_fee() {
    let asserter = Asserter::new();
    asserter.push_success(&U256::from(GWEI));
    asserter.push_success(&Bytes::from(vec![0xfe]));
    let prices = (
        U256::from(7u64),
        U256::from(3u64),
        U256::ZERO,
        U256::ZERO,
        U256::ZERO,
        U256::ZERO,
    );
    asserter.push_success(&Bytes::from(prices.abi_encode_params()));
    let provider = ProviderBuilder::new().connect_mocked_client(asserter);
    let source = FloorSource::live(provider, 5_000_000_000);

    let got = source.floors().await;
    let want = RedeemFloors::new(
        GasQuote {
            gas_price_wei: GWEI,
            l1_wei_per_byte: U256::from(3u64),
            l1_wei_per_tx: U256::from(7u64),
        },
        5_000_000_000,
    );
    assert_eq!(got, Some(want));
}

/// A quote younger than [`QUOTE_TTL`] is reused with no read.
#[tokio::test(start_paused = true)]
async fn a_fresh_quote_is_reused_without_a_read() {
    let asserter = Asserter::new();
    asserter.push_success(&U256::from(GWEI));
    asserter.push_success(&Bytes::new());
    let provider = ProviderBuilder::new().connect_mocked_client(asserter);
    let source = FloorSource::live(provider, 5_000_000_000);

    let first = source.floors().await;
    // The queue is empty: a second read would fail and fall back, but the
    // fresh quote needs none.
    let second = source.floors().await;
    assert!(first.is_some());
    assert_eq!(first, second);
}

/// A failed read falls back to the last quote at any age, and with no quote
/// yields no floors.
#[tokio::test(start_paused = true)]
async fn a_failed_read_falls_back_to_the_last_quote() {
    let asserter = Asserter::new();
    asserter.push_failure(alloy_json_rpc::ErrorPayload::internal_error());
    asserter.push_success(&U256::from(GWEI));
    asserter.push_success(&Bytes::new());
    asserter.push_failure(alloy_json_rpc::ErrorPayload::internal_error());
    let provider = ProviderBuilder::new().connect_mocked_client(asserter);
    let source = FloorSource::live(provider, 5_000_000_000);

    assert_eq!(source.floors().await, None, "no quote was ever read");
    let read = source.floors().await;
    assert!(read.is_some());
    tokio::time::advance(QUOTE_TTL * 10).await;
    assert_eq!(source.floors().await, read, "the stale quote stands in");
}
