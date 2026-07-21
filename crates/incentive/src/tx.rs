//! One place to submit a transaction and resolve its outcome (#1355).
//!
//! Every on-chain write in this workspace needs the same three steps — send,
//! await the receipt, fail on a revert — and every hand-rolled copy of them got
//! the same detail wrong: the tx hash was read *after* `get_receipt()`, so a
//! receipt that could not be fetched left the caller with an error naming no
//! transaction at all. That is the one failure mode where the operator most
//! needs the hash, because the transaction is in flight and re-sending it is
//! what causes duplicate spends.
//!
//! Lives in `decdn-incentive` rather than the CLI so the swap venues in this
//! crate can share it: they spend USDC, which is the least recoverable write the
//! workspace makes.
//!
//! One caller is deliberately NOT migrated: `cli::commands::channel`'s
//! `submit_close` / `submit_settle` / `submit_reclaim` classify a revert as an
//! expected *outcome* (`TxOutcome::Reverted`, an `Ok`) rather than an error,
//! because a channel that someone else already closed is a normal race, not a
//! failure. Routing them through here would turn that into a hard error. Any
//! other hand-rolled send/receipt pair is a bug — it will lose the hash on a
//! receipt timeout.

use alloy::contract::{CallBuilder, CallDecoder};
use alloy::primitives::B256;
use alloy::providers::Provider;
use anyhow::Context;

/// Send one call, await its receipt, and fail on a revert.
///
/// `landed` is the caller's record of whether this step may have taken effect,
/// and is the reason an out-param exists rather than just a return value:
///
/// - send failed → untouched (nothing was broadcast)
/// - broadcast, receipt unreadable → `Some(hash)` **and** `Err` — the outcome is
///   UNKNOWN, and the caller must treat it as possibly-applied
/// - confirmed revert → `None` and `Err` (definitively no effect)
/// - confirmed success → `Some(hash)` and `Ok`
///
/// So `Some` means "may have taken effect", never merely "was broadcast", and an
/// `Err` from here does **not** imply nothing landed. Callers that report what
/// happened must read it that way; asserting "nothing was transferred" on an
/// `Err` is how an operator gets told to re-run a transaction that is still
/// pending.
///
/// `hint` is a per-step semantic diagnostic, attached to **both** the send and
/// revert arms deliberately. A wallet provider built with alloy's recommended
/// fillers runs `eth_estimateGas` inside `send()`, so a call that would revert
/// usually fails *there*, before it is ever broadcast, surfacing as a bare
/// `execution reverted` selector. A hint attached only to the receipt arm would
/// therefore almost never print.
///
/// # Errors
///
/// Fails if the call cannot be sent, if the receipt cannot be fetched, or if the
/// transaction reverted.
pub async fn send<C: CallDecoder, P: Provider>(
    call: CallBuilder<P, C>,
    label: &str,
    hint: Option<&str>,
    landed: &mut Option<B256>,
) -> anyhow::Result<B256> {
    send_for_receipt(call, label, hint, landed)
        .await
        .map(|r| r.transaction_hash)
}

/// [`send`], returning the whole receipt for callers that must decode event
/// logs from it (a contract's return value is not in a receipt, so an emitted
/// event is the only way to read one back).
///
/// # Errors
///
/// As [`send`].
pub async fn send_for_receipt<C: CallDecoder, P: Provider>(
    call: CallBuilder<P, C>,
    label: &str,
    hint: Option<&str>,
    landed: &mut Option<B256>,
) -> anyhow::Result<alloy::rpc::types::TransactionReceipt> {
    let suffix = hint.map_or_else(String::new, |h| format!("; {h}"));
    let pending = call.send().await.with_context(|| {
        format!(
            "{label} transaction failed to send. A pre-flight gas estimate rejecting the call \
             surfaces here rather than as a revert, as do transport and nonce errors — see \
             the cause below{suffix}"
        )
    })?;
    let hash = *pending.tx_hash();
    // Recorded BEFORE the receipt is awaited, which is the whole point of the
    // out-param. From here the transaction is broadcast and may take effect; if
    // the receipt cannot be read we still return `Err`, but the caller MUST be
    // able to see that a tx is outstanding.
    *landed = Some(hash);
    let receipt = pending.get_receipt().await.with_context(|| {
        format!("{label} sent (tx {hash:#x}) but the receipt could not be fetched")
    })?;
    // `status()` coerces a pre-Byzantium `PostState` receipt to success. Not
    // reachable on the L2s this targets; noted so the check isn't read as
    // exhaustive over every receipt shape.
    if !receipt.status() {
        // A confirmed revert had no effect, so clear the slot: `Some` must mean
        // "may have taken effect", never merely "was broadcast". The hash stays
        // in the message for lookup.
        *landed = None;
        anyhow::bail!("{label} reverted (tx {hash:#x}){suffix}");
    }
    Ok(receipt)
}

/// [`send`] for callers with no outcome to record — a step whose hash they do
/// not report. The in-flight hash still reaches the operator through the error
/// message; this only drops the structured record.
///
/// # Errors
///
/// As [`send`].
pub async fn send_unrecorded<C: CallDecoder, P: Provider>(
    call: CallBuilder<P, C>,
    label: &str,
    hint: Option<&str>,
) -> anyhow::Result<B256> {
    let mut ignored = None;
    send(call, label, hint, &mut ignored).await
}

/// Run `work`, then run `cleanup` on **every** path, and return `work`'s result
/// unchanged.
///
/// The swap venues both spend USDC through a multi-transaction sequence that
/// grants a standing allowance, and both must clear that allowance whether the
/// swap succeeded, reverted, or never sent — otherwise a failed onboarding
/// leaves a router able to pull USDC indefinitely. Rust has no `finally`, and
/// the shape that expresses it (`let r = async { … }.await; cleanup().await; r`)
/// is easy to write and easy to break: a stray `?` inside the outer scope skips
/// the cleanup silently, which is precisely the bug this guards.
///
/// Naming the pattern gives the two venues one definition to share and — the
/// reason it is a function at all — makes the property testable without a
/// chain. See the coverage note in `swap_uniswap`'s tests for why the
/// on-chain-revert version of this test is still deferred.
///
/// `cleanup` returns `()`, not a `Result`: it is best-effort by construction,
/// so there is no outcome for this to accidentally propagate in place of
/// `work`'s. Each reset helper logs its own failure.
///
/// `cleanup` is a closure returning a future, not a future, and that is a size
/// decision rather than a style one: a future passed by value is a live local
/// for the whole call, so the compiler cannot overlap its storage with `work`'s
/// and the combined state machine is the SUM of the two. `decdn`'s
/// `Command::Setup` future runs a swap, and taking both eagerly pushed it over
/// clippy's `large_futures` threshold. Constructing the cleanup future only
/// after `work` has finished lets the two share space.
///
/// # Errors
///
/// Returns `work`'s error verbatim. `cleanup` cannot introduce, mask, or
/// replace one.
pub async fn run_then_cleanup<T, F: Future<Output = ()>>(
    work: impl Future<Output = anyhow::Result<T>>,
    cleanup: impl FnOnce() -> F,
) -> anyhow::Result<T> {
    let result = work.await;
    cleanup().await;
    result
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use std::cell::Cell;

    use super::*;

    /// The whole point: an `Err` from `work` must not skip the cleanup. This is
    /// the failed-swap path, where a standing USDC allowance would otherwise
    /// survive.
    #[tokio::test]
    async fn cleanup_runs_when_work_fails() {
        let ran = Cell::new(0usize);
        let out = run_then_cleanup(
            async { anyhow::bail!("swap reverted") as anyhow::Result<u8> },
            || async {
                ran.set(ran.get() + 1);
            },
        )
        .await;

        assert_eq!(ran.get(), 1, "cleanup must run on the failure path");
        assert_eq!(
            format!("{}", out.unwrap_err()),
            "swap reverted",
            "and must not replace or wrap the swap's own error"
        );
    }

    /// The success path leaves `max_in − actual_in` of allowance behind, so it
    /// needs the cleanup just as much — and must still yield the tx hash.
    #[tokio::test]
    async fn cleanup_runs_when_work_succeeds_and_the_value_survives() {
        let ran = Cell::new(0usize);
        let out = run_then_cleanup(async { Ok(7u8) }, || async {
            ran.set(ran.get() + 1);
        })
        .await;

        assert_eq!(ran.get(), 1);
        assert_eq!(out.unwrap(), 7, "the work's value passes through");
    }

    /// Ordering is load-bearing, not incidental: resetting an allowance before
    /// the swap has finished with it would make the swap fail for lack of
    /// allowance.
    #[tokio::test]
    async fn cleanup_runs_after_work_not_before() {
        let log = Cell::new(String::new());
        let push = |c: char| {
            let mut s = log.take();
            s.push(c);
            log.set(s);
        };

        run_then_cleanup(
            async {
                push('w');
                Ok(())
            },
            || async {
                push('c');
            },
        )
        .await
        .unwrap();

        assert_eq!(log.take(), "wc");
    }
}
