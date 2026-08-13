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
use alloy::providers::{PendingTransactionBuilder, PendingTransactionError};
use alloy::rpc::types::TransactionReceipt;

/// Terminal outcome of one on-chain transaction: the `send` → `get_receipt` →
/// `status` sequence folded to a single value. The `Landed`/`Reverted` receipts
/// are carried so callers can log the transaction hash; the two error arms keep
/// their distinct concrete types rather than being flattened.
pub(crate) enum TxOutcome {
    /// Mined and succeeded (`receipt.status() == true`).
    Landed(TransactionReceipt),
    /// Mined and reverted (`receipt.status() == false`).
    Reverted(TransactionReceipt),
    /// `.send()` failed (RPC / mempool rejection) — no transaction was issued.
    SendErr(ContractError),
    /// The transaction was issued but awaiting its receipt failed.
    ReceiptErr(PendingTransactionError),
    /// The receipt wait exceeded the caller-supplied timeout. Only reachable
    /// when `receipt_timeout` is `Some`; the transaction may still mine later.
    Timeout,
}

/// Drive an already-issued `.send()` to a classified [`TxOutcome`].
///
/// `sent` is the result of `contract.<method>(..).send().await`. When
/// `receipt_timeout` is `Some`, the receipt wait is bounded and a lapse yields
/// [`TxOutcome::Timeout`]; when `None`, the wait is unbounded.
pub(crate) async fn send_and_await_receipt(
    sent: Result<PendingTransactionBuilder<Ethereum>, ContractError>,
    receipt_timeout: Option<Duration>,
) -> TxOutcome {
    let pending = match sent {
        Ok(pending) => pending,
        Err(err) => return TxOutcome::SendErr(err),
    };
    let receipt = match receipt_timeout {
        Some(timeout) => match tokio::time::timeout(timeout, pending.get_receipt()).await {
            Ok(Ok(receipt)) => receipt,
            Ok(Err(err)) => return TxOutcome::ReceiptErr(err),
            Err(_elapsed) => return TxOutcome::Timeout,
        },
        None => match pending.get_receipt().await {
            Ok(receipt) => receipt,
            Err(err) => return TxOutcome::ReceiptErr(err),
        },
    };
    if receipt.status() {
        TxOutcome::Landed(receipt)
    } else {
        TxOutcome::Reverted(receipt)
    }
}

#[cfg(test)]
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
        let outcome = send_and_await_receipt(Err(send_err.into()), None).await;
        assert!(matches!(outcome, TxOutcome::SendErr(_)));
    }
}
