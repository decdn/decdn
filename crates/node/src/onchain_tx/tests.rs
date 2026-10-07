use alloy::primitives::{Address, B256};
use alloy::providers::ProviderBuilder;
use alloy::providers::mock::Asserter;

use super::*;

const TX: TxHash = B256::repeat_byte(0x17);

/// A minimal mined receipt for `TX` with the given status.
fn receipt(status: bool) -> TransactionReceipt {
    TransactionReceipt {
        inner: alloy::consensus::ReceiptEnvelope::Eip1559(alloy::consensus::ReceiptWithBloom {
            receipt: alloy::consensus::Receipt {
                status: alloy::consensus::Eip658Value::Eip658(status),
                cumulative_gas_used: 0,
                logs: Vec::new(),
            },
            logs_bloom: alloy::primitives::Bloom::ZERO,
        }),
        transaction_hash: TX,
        transaction_index: Some(0),
        block_hash: Some(B256::repeat_byte(0x01)),
        block_number: Some(1),
        gas_used: 0,
        effective_gas_price: 0,
        blob_gas_used: None,
        blob_gas_price: None,
        from: Address::ZERO,
        to: None,
        contract_address: None,
    }
}

/// The receipt-wait error a load-balanced RPC returns when the backend that
/// answered has not seen the block yet.
fn unknown_block() -> WaitFailure {
    WaitFailure::Err(PendingTransactionError::TransportError(
        alloy::transports::TransportErrorKind::custom_str("error code 26: Unknown block"),
    ))
}

/// What one scripted `classify_wait` run produced.
struct Run {
    outcome: TxOutcome,
    /// The encoded metrics after `count_outcome`.
    encoded: String,
    /// Scripted RPC responses the run did not consume.
    unread: usize,
    /// Paused time the run spent, which is the by-hash backoff it slept.
    elapsed: Duration,
}

/// Classify `waited` against a provider scripted by `script`, then count it.
/// Call from a `start_paused` test so the backoff sleeps cost no real time.
async fn classify_and_count(
    waited: Result<TransactionReceipt, WaitFailure>,
    script: impl FnOnce(&Asserter),
) -> Run {
    let asserter = Asserter::new();
    script(&asserter);
    let provider = ProviderBuilder::new().connect_mocked_client(asserter.clone());
    let started = tokio::time::Instant::now();
    let (outcome, recovered) = classify_wait(waited, &provider, TX).await;
    let elapsed = started.elapsed();
    let metrics = Metrics::new();
    count_outcome(&outcome, recovered, &metrics);
    Run {
        outcome,
        encoded: metrics.encode().expect("metrics encode"),
        unread: asserter.read_q().len(),
        elapsed,
    }
}

fn has_line(encoded: &str, line: &str) -> bool {
    encoded.lines().any(|l| l == line)
}

#[tokio::test(start_paused = true)]
async fn failed_wait_resolves_to_landed_by_hash() {
    // The testnet case: the receipt wait fails with `Unknown block`, the
    // first by-hash fetch fails the same way, and the second finds the mined
    // receipt. The transaction landed, so it must count as landed — not as a
    // receipt failure — and as one recovered wait.
    let run = classify_and_count(Err(unknown_block()), |a| {
        a.push_failure_msg("Unknown block");
        a.push_success(&receipt(true));
    })
    .await;
    assert!(matches!(run.outcome, TxOutcome::Landed(_)));
    assert!(has_line(&run.encoded, "decdn_onchain_tx_landed_total 1"));
    assert!(has_line(
        &run.encoded,
        "decdn_onchain_tx_receipt_recovered_total 1"
    ));
    assert!(has_line(
        &run.encoded,
        "decdn_onchain_tx_receipt_failed_total 0"
    ));
}

#[tokio::test(start_paused = true)]
async fn empty_fetch_retries_until_the_receipt_appears() {
    // A lagging backend usually answers `null`, not an error. A `null`
    // must move on to the next fetch rather than end the lookup.
    let none: Option<TransactionReceipt> = None;
    let run = classify_and_count(Err(unknown_block()), |a| {
        a.push_success(&none);
        a.push_success(&receipt(true));
    })
    .await;
    assert!(matches!(run.outcome, TxOutcome::Landed(_)));
    assert!(has_line(
        &run.encoded,
        "decdn_onchain_tx_receipt_recovered_total 1"
    ));
    assert_eq!(run.unread, 0);
}

#[tokio::test(start_paused = true)]
async fn lapsed_wait_resolves_to_reverted_by_hash() {
    let run = classify_and_count(Err(WaitFailure::Lapsed), |a| {
        a.push_success(&receipt(false));
    })
    .await;
    assert!(matches!(run.outcome, TxOutcome::Reverted(_)));
    assert!(has_line(&run.encoded, "decdn_onchain_tx_reverted_total 1"));
    assert!(has_line(
        &run.encoded,
        "decdn_onchain_tx_receipt_recovered_total 1"
    ));
    assert!(has_line(&run.encoded, "decdn_onchain_tx_timeout_total 0"));
}

#[tokio::test(start_paused = true)]
async fn unresolved_wait_stays_unconfirmed() {
    // Every by-hash fetch comes back empty: the outcome keeps its wait
    // classification, nothing counts as recovered, every attempt ran on the
    // 1 s + 2 s + 4 s schedule, and the last miss reason reaches the caller.
    let none: Option<TransactionReceipt> = None;
    let run = classify_and_count(Err(unknown_block()), |a| {
        for _ in 0..RESOLVE_ATTEMPTS {
            a.push_success(&none);
        }
    })
    .await;
    assert!(matches!(
        &run.outcome,
        TxOutcome::ReceiptErr { tx_hash, last_lookup, .. }
            if *tx_hash == TX && last_lookup == "no receipt yet"
    ));
    assert!(has_line(
        &run.encoded,
        "decdn_onchain_tx_receipt_failed_total 1"
    ));
    assert!(has_line(
        &run.encoded,
        "decdn_onchain_tx_receipt_recovered_total 0"
    ));
    assert_eq!(run.unread, 0, "every attempt must run");
    assert_eq!(run.elapsed, Duration::from_secs(7));

    let run = classify_and_count(Err(WaitFailure::Lapsed), |a| {
        for _ in 0..RESOLVE_ATTEMPTS {
            a.push_failure_msg("Unknown block");
        }
    })
    .await;
    assert!(matches!(
        &run.outcome,
        TxOutcome::Timeout { tx_hash, last_lookup }
            if *tx_hash == TX && last_lookup.contains("Unknown block")
    ));
    assert!(has_line(&run.encoded, "decdn_onchain_tx_timeout_total 1"));
    assert!(has_line(
        &run.encoded,
        "decdn_onchain_tx_receipt_recovered_total 0"
    ));
    assert_eq!(run.unread, 0, "every attempt must run");
}

#[tokio::test(start_paused = true)]
async fn successful_wait_skips_the_hash_fetch() {
    // A reverted receipt waits in the queue as a sentinel. A stray fetch
    // would consume it and sleep the backoff; neither may happen.
    let run = classify_and_count(Ok(receipt(true)), |a| a.push_success(&receipt(false))).await;
    assert!(matches!(run.outcome, TxOutcome::Landed(_)));
    assert!(has_line(
        &run.encoded,
        "decdn_onchain_tx_receipt_recovered_total 0"
    ));
    assert_eq!(run.unread, 1, "no by-hash fetch on a successful wait");
    assert_eq!(run.elapsed, Duration::ZERO);
}

#[tokio::test]
async fn failed_send_classifies_as_send_err() {
    // A `.send()` that never issued a transaction must surface as `SendErr`
    // (so the caller ticks its send-failure bucket), never a receipt
    // outcome, and must not attempt a by-hash fetch: there is no hash.
    let send_err = alloy::transports::TransportErrorKind::custom_str("boom");
    let metrics = Metrics::new();
    let outcome = send_and_await_receipt(Err(send_err.into()), None, &metrics).await;
    assert!(matches!(outcome, TxOutcome::SendErr(_)));
    let encoded = metrics.encode().expect("metrics encode");
    assert!(has_line(&encoded, "decdn_onchain_tx_send_failed_total 1"));
    assert!(has_line(&encoded, "decdn_onchain_tx_landed_total 0"));
}
