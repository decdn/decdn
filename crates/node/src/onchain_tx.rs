//! Shared driver for the node's on-chain `PaymentPool` write ladders.
//!
//! Every state-changing call the node makes against `PaymentPool` (today
//! only the redeemer's `redeemMany`) follows the same micro-sequence: issue the transaction with `.send()`, await its receipt
//! (optionally under a timeout), and inspect `receipt.status()` to tell a
//! mined-and-succeeded transaction from a mined-and-reverted one. A receipt
//! wait that fails or lapses is not an outcome yet: the transaction was issued
//! and often mines, and a load-balanced RPC can fail the wait on a backend that
//! has not seen the block. The driver then fetches the receipt by hash a few
//! times before it settles on an unconfirmed outcome. What each
//! caller then *does* with that result — which metric it ticks, whether it
//! re-reads the pool to resolve a revert, what it returns — is domain state
//! that stays at the call site.
//!
//! `send_and_await_receipt` folds only that common plumbing into one place and
//! hands back a `TxOutcome`. It deliberately does **not** take the call builder
//! (each `redeem`/`redeemMany`/… builder is a distinct type, which would force a
//! generic or a closure hook); it takes the already issued `.send()` result, so
//! the concrete method call stays a visible call at every site.
//!
//! `send_and_await_receipt` is fixed to the `Ethereum` network — the whole
//! crate is Ethereum-only — which keeps its signature free of a `Provider` type
//! parameter and its `where` clause. The private by-hash helpers stay generic
//! over `Provider` so tests can drive them against a mocked client.

use std::time::Duration;

use alloy::contract::Error as ContractError;
use alloy::network::Ethereum;
use alloy::primitives::TxHash;
use alloy::providers::{PendingTransactionBuilder, PendingTransactionError, Provider};
use alloy::rpc::types::TransactionReceipt;
use decdn_common::redact::sanitize_rpc_display;
use tracing::{debug, info};

use crate::metrics::Metrics;

/// By-hash receipt fetches after a failed or lapsed receipt wait, before the
/// outcome counts as unconfirmed.
const RESOLVE_ATTEMPTS: u32 = 3;

/// Pause before the first by-hash fetch. It doubles before each later fetch
/// (1 s, 2 s, 4 s across the [`RESOLVE_ATTEMPTS`] fetches), so a lagging RPC
/// backend has time to see the block.
const RESOLVE_FIRST_BACKOFF: Duration = Duration::from_secs(1);

/// Bound on one by-hash fetch, so a hung RPC cannot hold the caller's tick.
/// With the backoff, the by-hash fallback adds at most 1 + 2 + 4 + 3 × 10 = 37 s
/// after a failed or lapsed wait.
const RESOLVE_CALL_TIMEOUT: Duration = Duration::from_secs(10);

/// Terminal outcome of one on-chain transaction: the `send` → `get_receipt` →
/// `status` sequence folded to a single value. Every arm past `.send()` carries
/// the transaction hash (inside the receipt, or beside the error) so callers can
/// log it: a transaction that is still unconfirmed may yet mine, and its hash is
/// the only way to find it. The two error arms keep their distinct concrete
/// types rather than being flattened.
pub(crate) enum TxOutcome {
    /// Mined and succeeded (`receipt.status() == true`).
    Landed(TransactionReceipt),
    /// Mined and reverted (`receipt.status() == false`).
    Reverted(TransactionReceipt),
    /// `.send()` failed (RPC / mempool rejection) — no transaction was issued.
    SendErr(ContractError),
    /// The transaction was issued, awaiting its receipt failed, and no by-hash
    /// fetch found a receipt either. The transaction is unconfirmed and may
    /// still mine.
    ReceiptErr {
        /// The receipt-wait failure.
        error: PendingTransactionError,
        /// Hash of the issued transaction.
        tx_hash: TxHash,
        /// Why the last by-hash fetch found nothing: no receipt yet, or the
        /// sanitized RPC error, or a fetch timeout. "No receipt" points at a
        /// dropped or replaced transaction; an error points at the RPC.
        last_lookup: String,
    },
    /// The receipt wait exceeded the caller-supplied timeout, and no by-hash
    /// fetch found a receipt either. Only reachable when `receipt_timeout` is
    /// `Some`; the transaction is unconfirmed and may still mine.
    Timeout {
        /// Hash of the issued transaction.
        tx_hash: TxHash,
        /// Why the last by-hash fetch found nothing (see
        /// [`TxOutcome::ReceiptErr::last_lookup`]).
        last_lookup: String,
    },
}

/// Drive an already-issued `.send()` to a classified [`TxOutcome`].
///
/// `sent` is the result of `contract.<method>(..).send().await`. When
/// `receipt_timeout` is `Some`, the receipt wait is bounded and a lapse yields
/// [`TxOutcome::Timeout`]; when `None`, the wait is unbounded. A wait that
/// fails or lapses falls back to fetching the receipt by hash, and a receipt
/// found there classifies as `Landed` / `Reverted` like one the wait returned.
/// Every outcome counts once into one of the five outcome counters
/// (`decdn_onchain_tx_{landed,reverted,send_failed,receipt_failed,timeout}_total`);
/// a wait the by-hash fetch resolved also counts into
/// `decdn_onchain_tx_receipt_recovered_total`, which sits beside that family.
pub(crate) async fn send_and_await_receipt(
    sent: Result<PendingTransactionBuilder<Ethereum>, ContractError>,
    receipt_timeout: Option<Duration>,
    metrics: &Metrics,
) -> TxOutcome {
    let (outcome, recovered) = await_receipt(sent, receipt_timeout).await;
    count_outcome(&outcome, recovered, metrics);
    outcome
}

/// Count one classified outcome into its outcome counter, plus the recovered
/// counter when a by-hash fetch resolved it.
fn count_outcome(outcome: &TxOutcome, recovered: bool, metrics: &Metrics) {
    if recovered {
        metrics.onchain_tx_receipt_recovered();
    }
    match outcome {
        TxOutcome::Landed(_) => metrics.onchain_tx_landed(),
        TxOutcome::Reverted(_) => metrics.onchain_tx_reverted(),
        TxOutcome::SendErr(_) => metrics.onchain_tx_send_failed(),
        TxOutcome::ReceiptErr { .. } => metrics.onchain_tx_receipt_failed(),
        TxOutcome::Timeout { .. } => metrics.onchain_tx_timeout(),
    }
}

/// Why a receipt wait ended without a receipt.
enum WaitFailure {
    /// The wait returned an error.
    Err(PendingTransactionError),
    /// The wait exceeded the caller-supplied timeout.
    Lapsed,
}

/// The classification behind [`send_and_await_receipt`], before it is counted.
/// The `bool` is `true` when a by-hash fetch resolved a failed or lapsed wait.
async fn await_receipt(
    sent: Result<PendingTransactionBuilder<Ethereum>, ContractError>,
    receipt_timeout: Option<Duration>,
) -> (TxOutcome, bool) {
    let pending = match sent {
        Ok(pending) => pending,
        Err(err) => return (TxOutcome::SendErr(err), false),
    };
    let tx_hash = *pending.tx_hash();
    let provider = pending.provider().clone();
    let waited = match receipt_timeout {
        Some(timeout) => match tokio::time::timeout(timeout, pending.get_receipt()).await {
            Ok(result) => result.map_err(WaitFailure::Err),
            Err(_elapsed) => Err(WaitFailure::Lapsed),
        },
        None => pending.get_receipt().await.map_err(WaitFailure::Err),
    };
    classify_wait(waited, &provider, tx_hash).await
}

/// Classify a finished receipt wait. A failed or lapsed wait first fetches the
/// receipt by hash ([`receipt_by_hash`]); only when that finds nothing does the
/// outcome stay `ReceiptErr` / `Timeout`.
async fn classify_wait<P: Provider>(
    waited: Result<TransactionReceipt, WaitFailure>,
    provider: &P,
    tx_hash: TxHash,
) -> (TxOutcome, bool) {
    let failure = match waited {
        Ok(receipt) => return (by_status(receipt), false),
        Err(failure) => failure,
    };
    let receipt = match receipt_by_hash(provider, tx_hash).await {
        Ok(receipt) => receipt,
        Err(last_lookup) => {
            let outcome = match failure {
                WaitFailure::Err(error) => TxOutcome::ReceiptErr {
                    error,
                    tx_hash,
                    last_lookup,
                },
                WaitFailure::Lapsed => TxOutcome::Timeout {
                    tx_hash,
                    last_lookup,
                },
            };
            return (outcome, false);
        }
    };
    let wait = match &failure {
        WaitFailure::Err(error) => sanitize_rpc_display(error),
        WaitFailure::Lapsed => "timed out".to_owned(),
    };
    info!(tx = %tx_hash, %wait, "receipt wait failed; found the receipt by hash");
    (by_status(receipt), true)
}

/// `Landed` or `Reverted` by the receipt's status.
const fn by_status(receipt: TransactionReceipt) -> TxOutcome {
    if receipt.status() {
        TxOutcome::Landed(receipt)
    } else {
        TxOutcome::Reverted(receipt)
    }
}

/// Fetch a transaction's receipt by hash, up to [`RESOLVE_ATTEMPTS`] times with
/// a doubling pause before each fetch. A fetch that errors, lapses or returns no
/// receipt moves on to the next attempt. When every attempt misses, the `Err`
/// carries the last miss's reason, so the caller's warning can tell a
/// transaction the chain does not know from an RPC that is failing.
async fn receipt_by_hash<P: Provider>(
    provider: &P,
    tx_hash: TxHash,
) -> Result<TransactionReceipt, String> {
    let mut backoff = RESOLVE_FIRST_BACKOFF;
    let mut last_miss = String::new();
    for attempt in 1..=RESOLVE_ATTEMPTS {
        tokio::time::sleep(backoff).await;
        backoff = backoff.saturating_mul(2);
        let fetch = provider.get_transaction_receipt(tx_hash);
        let miss = match tokio::time::timeout(RESOLVE_CALL_TIMEOUT, fetch).await {
            Ok(Ok(Some(receipt))) => return Ok(receipt),
            Ok(Ok(None)) => "no receipt yet".to_owned(),
            Ok(Err(error)) => sanitize_rpc_display(&error),
            Err(_elapsed) => "fetch timed out".to_owned(),
        };
        debug!(tx = %tx_hash, attempt, %miss, "receipt fetch by hash found nothing");
        last_miss = miss;
    }
    Err(last_miss)
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
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
}
