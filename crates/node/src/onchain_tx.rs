//! Shared driver for the node's on-chain `PaymentPool` write ladders.
//!
//! Every state-changing call the node makes against `PaymentPool`
//! (`redeem`, `redeemMany`, buyer-side `reclaim`, …) follows the same
//! micro-sequence: issue the transaction with `.send()`, await its receipt
//! (optionally under a timeout), and inspect `receipt.status()` to tell a
//! mined-and-succeeded transaction from a mined-and-reverted one. What each
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
//! The helper is fixed to the `Ethereum` network — the whole crate is
//! Ethereum-only — which keeps its signature free of a `Provider` type
//! parameter and its `where` clause.

use std::time::Duration;

use alloy::contract::Error as ContractError;
use alloy::network::Ethereum;
use alloy::primitives::TxHash;
use alloy::providers::{PendingTransactionBuilder, PendingTransactionError};
use alloy::rpc::types::TransactionReceipt;

use crate::metrics::Metrics;

/// Terminal outcome of one on-chain transaction: the `send` → `get_receipt` →
/// `status` sequence folded to a single value. Every arm past `.send()` carries
/// the transaction hash (inside the receipt, or beside the error) so callers can
/// log it: a transaction whose receipt wait failed or lapsed may still mine, and
/// its hash is the only way to find it. The two error arms keep their distinct
/// concrete types rather than being flattened.
pub(crate) enum TxOutcome {
    /// Mined and succeeded (`receipt.status() == true`).
    Landed(TransactionReceipt),
    /// Mined and reverted (`receipt.status() == false`).
    Reverted(TransactionReceipt),
    /// `.send()` failed (RPC / mempool rejection) — no transaction was issued.
    SendErr(ContractError),
    /// The transaction was issued but awaiting its receipt failed.
    ReceiptErr {
        /// The receipt-wait failure.
        error: PendingTransactionError,
        /// Hash of the issued transaction.
        tx_hash: TxHash,
    },
    /// The receipt wait exceeded the caller-supplied timeout. Only reachable
    /// when `receipt_timeout` is `Some`; the transaction may still mine later.
    Timeout {
        /// Hash of the issued transaction.
        tx_hash: TxHash,
    },
}

/// Drive an already-issued `.send()` to a classified [`TxOutcome`].
///
/// `sent` is the result of `contract.<method>(..).send().await`. When
/// `receipt_timeout` is `Some`, the receipt wait is bounded and a lapse yields
/// [`TxOutcome::Timeout`]; when `None`, the wait is unbounded. Every outcome
/// counts once into its `decdn_onchain_tx_*_total` sibling.
pub(crate) async fn send_and_await_receipt(
    sent: Result<PendingTransactionBuilder<Ethereum>, ContractError>,
    receipt_timeout: Option<Duration>,
    metrics: &Metrics,
) -> TxOutcome {
    let outcome = await_receipt(sent, receipt_timeout).await;
    match &outcome {
        TxOutcome::Landed(_) => metrics.onchain_tx_landed(),
        TxOutcome::Reverted(_) => metrics.onchain_tx_reverted(),
        TxOutcome::SendErr(_) => metrics.onchain_tx_send_failed(),
        TxOutcome::ReceiptErr { .. } => metrics.onchain_tx_receipt_failed(),
        TxOutcome::Timeout { .. } => metrics.onchain_tx_timeout(),
    }
    outcome
}

/// The classification behind [`send_and_await_receipt`], before it is counted.
async fn await_receipt(
    sent: Result<PendingTransactionBuilder<Ethereum>, ContractError>,
    receipt_timeout: Option<Duration>,
) -> TxOutcome {
    let pending = match sent {
        Ok(pending) => pending,
        Err(err) => return TxOutcome::SendErr(err),
    };
    let tx_hash = *pending.tx_hash();
    let receipt = match receipt_timeout {
        Some(timeout) => match tokio::time::timeout(timeout, pending.get_receipt()).await {
            Ok(Ok(receipt)) => receipt,
            Ok(Err(error)) => return TxOutcome::ReceiptErr { error, tx_hash },
            Err(_elapsed) => return TxOutcome::Timeout { tx_hash },
        },
        None => match pending.get_receipt().await {
            Ok(receipt) => receipt,
            Err(error) => return TxOutcome::ReceiptErr { error, tx_hash },
        },
    };
    if receipt.status() {
        TxOutcome::Landed(receipt)
    } else {
        TxOutcome::Reverted(receipt)
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn failed_send_classifies_as_send_err() {
        // The one classification branch of the core that needs no live provider:
        // a `.send()` that never issued a transaction must surface as `SendErr`
        // (so a ladder site ticks its send-failure bucket), never a receipt
        // outcome. The receipt/status branches are provider-bound and covered by
        // the anvil e2e.
        let send_err = alloy::transports::TransportErrorKind::custom_str("boom");
        let metrics = Metrics::new();
        let outcome = send_and_await_receipt(Err(send_err.into()), None, &metrics).await;
        assert!(matches!(outcome, TxOutcome::SendErr(_)));
        let encoded = metrics.encode().expect("metrics encode");
        assert!(
            encoded
                .lines()
                .any(|l| l == "decdn_onchain_tx_send_failed_total 1")
        );
        assert!(
            encoded
                .lines()
                .any(|l| l == "decdn_onchain_tx_landed_total 0")
        );
    }
}
