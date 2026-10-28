//! One place to submit a transaction and resolve its outcome (#1355).
//!
//! Every production on-chain write needs the same three steps — send, await
//! the receipt, fail on a revert — and the tx hash must be read before
//! `get_receipt()` consumes the pending handle. A receipt that cannot be fetched
//! leaves the transaction's fate unknown: it may still mine, and re-sending it
//! is what causes duplicate spends. That is the one failure mode where the
//! operator most needs the hash, so the error must name it.
//!
//! [`send`] and [`send_for_receipt`] bound their receipt wait by
//! [`RECEIPT_TIMEOUT`] through [`await_receipt`]: a transaction dropped from the
//! mempool never yields a receipt, and an unbounded wait would hang the command.
//! A lapse ends in the same "receipt not read" outcome as a failed read, with
//! the hash in the error.
//!
//! Lives in `decdn-incentive` rather than the CLI so every on-chain write in
//! this crate shares one send/receipt path.
//!
//! `cli::commands::pool`'s `close_and_forget` / `reclaim_and_forget` hand-roll
//! the pair so they can word a revert as a likely race: a concurrent close by
//! the same owner, or a concurrent permissionless reclaim. Like this module,
//! they read the tx hash before the receipt wait and bound the wait with
//! [`await_receipt`], so a receipt-read error or a lapse names the
//! transaction. In production code, a send/receipt pair that reads the hash
//! after the receipt wait is a bug. The `decdn-e2e` fixtures are exempt: a
//! failed fixture tx fails the test, and no operator reconciles it.

mod gas_headroom;
mod send_leg;

pub use gas_headroom::{GAS_HEADROOM, GasHeadroomFiller, with_headroom};
pub use send_leg::{SendLegFailure, SendLegLayer, SendLegProvider};

use alloy::contract::{CallBuilder, CallDecoder, Error as ContractError};
use alloy::network::Ethereum;
use alloy::primitives::B256;
use alloy::providers::{PendingTransactionBuilder, Provider};
use alloy::rpc::types::TransactionReceipt;
use anyhow::Context;
use std::time::Duration;

/// The bound on a receipt wait: every CLI wait, and the node's `approve` and
/// `openPool` waits. It is well above a normal inclusion window, so a lapse
/// means the transaction is stuck or was dropped from the mempool. A lapse does
/// not cancel the transaction: it may still mine, so every caller reports it as
/// unconfirmed and names its hash.
pub const RECEIPT_TIMEOUT: Duration = Duration::from_mins(3);

/// Wait for `pending`'s receipt, for at most `timeout` when it is `Some`.
///
/// `get_receipt` consumes `pending`, so a caller reads the tx hash before this
/// call and names it in the error. `tokio::time::timeout` bounds the wait, not
/// alloy's `with_timeout`: alloy checks its own deadline against the wall clock,
/// which a paused test clock does not advance.
///
/// # Errors
///
/// Fails if the receipt cannot be read, or if no receipt arrives within
/// `timeout`. Either way the transaction may still mine.
pub async fn await_receipt(
    pending: PendingTransactionBuilder<Ethereum>,
    timeout: Option<Duration>,
) -> anyhow::Result<TransactionReceipt> {
    let Some(timeout) = timeout else {
        return Ok(pending.get_receipt().await?);
    };
    match tokio::time::timeout(timeout, pending.get_receipt()).await {
        Ok(receipt) => Ok(receipt?),
        Err(_elapsed) => anyhow::bail!("no receipt within {timeout:?}"),
    }
}

/// What a [`send_for_receipt`] call did on-chain, recorded through its out-param
/// independently of the `Result` it returns.
///
/// `send_for_receipt` still fails on every non-success path; this says *which*
/// non-success, and carries the tx hash on every arm that captured one — so a
/// machine-readable receipt need not scrape the hash out of the human error
/// text. That gap is the bug behind #1550 (a confirmed revert lost its hash) and
/// #1577 (a lost send response was reported as a definite "nothing happened").
///
/// The six states are mutually exclusive and cover every path:
///
/// - [`NotSent`](Self::NotSent) — the initial value; also what a dry run leaves.
/// - [`Rejected`](Self::Rejected) — `send()` failed before anything entered
///   the mempool: a JSON-RPC error response other than the three
///   [`send_broadcast_unknown`] reads as uncertain (a pre-flight gas-estimate
///   revert, an under-funded account), or a chain read before the send that
///   failed in transport, so nothing was signed.
/// - [`MaybeBroadcast`](Self::MaybeBroadcast) — the send request itself
///   failed with a transport-class error ([`SendLegFailure`]), or the node
///   answered that it already holds the transaction, counts its nonce as
///   spent, or holds another transaction of this key at that nonce. An
///   earlier copy of the request may have reached a node and broadcast the
///   transaction, but no hash was captured. The outcome is UNKNOWN.
/// - [`InFlight`](Self::InFlight) — broadcast, but the receipt could not be
///   read or did not arrive within [`RECEIPT_TIMEOUT`]. In flight, may still
///   confirm. Carries the hash.
/// - [`Reverted`](Self::Reverted) — broadcast and mined, but reverted: it burned
///   gas and had no effect. Carries the hash as a permanent on-chain handle.
/// - [`Confirmed`](Self::Confirmed) — confirmed success. Carries the hash.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum SendOutcome {
    /// No send has resolved into this slot — the initial value, and the value a
    /// dry run leaves.
    #[default]
    NotSent,
    /// Nothing entered the mempool: the node rejected the call (a JSON-RPC
    /// error response other than the three [`send_broadcast_unknown`] reads
    /// as uncertain), or a chain read before the send failed, so nothing was
    /// signed.
    Rejected,
    /// The send request itself failed with a transport-class error (timeout,
    /// connection reset, a 5xx from an RPC load balancer), or the node answered
    /// that it already holds the transaction (`already known`), already counts
    /// its nonce as spent (`nonce too low`), or holds another transaction of
    /// this key at that nonce (`replacement transaction underpriced`). The
    /// transaction may have been broadcast; no hash was captured, so the
    /// outcome is genuinely unknown and re-sending can double-submit.
    MaybeBroadcast,
    /// Broadcast; the receipt could not be read or did not arrive within
    /// [`RECEIPT_TIMEOUT`]. May still confirm.
    InFlight(B256),
    /// Broadcast and mined, but reverted. No effect; the hash is a handle for
    /// reconciling the gas spend.
    Reverted(B256),
    /// Confirmed success.
    Confirmed(B256),
}

impl SendOutcome {
    /// The tx hash for any arm that captured one — [`Confirmed`](Self::Confirmed),
    /// [`Reverted`](Self::Reverted), or [`InFlight`](Self::InFlight).
    ///
    /// [`MaybeBroadcast`](Self::MaybeBroadcast) has none by construction: the
    /// send request failed before returning a handle (a transport error, or an
    /// answer [`send_broadcast_unknown`] reads as uncertain), so there is no
    /// hash to give even though a transaction may exist.
    #[must_use]
    pub const fn tx(self) -> Option<B256> {
        match self {
            Self::Confirmed(h) | Self::Reverted(h) | Self::InFlight(h) => Some(h),
            Self::NotSent | Self::Rejected | Self::MaybeBroadcast => None,
        }
    }

    /// Whether the transaction may have taken effect on-chain.
    ///
    /// True for a confirmed success, an in-flight broadcast, and a send failure
    /// whose broadcast is unknown; false for a definitive rejection and a
    /// confirmed revert. Asserting "nothing was transferred" when this is true is
    /// how an operator gets told to re-run a transaction that is still pending.
    #[must_use]
    pub const fn maybe_effected(self) -> bool {
        matches!(
            self,
            Self::Confirmed(_) | Self::InFlight(_) | Self::MaybeBroadcast
        )
    }
}

/// Whether a `send()` failure leaves the transaction's fate genuinely unknown.
///
/// The question is whether the request could have reached the node and broadcast
/// the transaction before its response was lost. `send()` does its chain reads
/// first (the pending nonce, the gas estimate, the fees, the chain id), then
/// signs, then sends. Only the last step can broadcast, and only a failure of
/// that step is uncertain. [`SendLegLayer`] tells the two apart by structure:
/// it sits below the fillers and wraps a failure of the send request in
/// [`SendLegFailure`], so:
///
/// - a marked failure is the send itself failing without an answer: a timeout,
///   a connection reset, an HTTP 5xx from an RPC load balancer, a response body
///   that could not be deserialized. The transaction may already be in the
///   mempool, and `eth_sendRawTransaction` is not idempotent from the client's
///   view, so the caller must not report "nothing happened". Three marked
///   shapes are still definite: an HTTP 4xx (with or without a `Retry-After`),
///   where the gateway refused the request outright (a bad path, an expired
///   key, a rate limit) without forwarding it; a request that never connected
///   (a refused connection, a DNS or TLS failure) or could not be built; and a
///   client-side fault raised before the request left the process (an
///   unsupported feature, a local-usage error, a body that failed to
///   serialize);
/// - a JSON-RPC error *response* arrives unmarked whichever step it answers:
///   the node processed the request and rejected it (a pre-flight gas-estimate
///   revert, the modal `createNamespace` / `addOrigin` failure; an under-funded
///   account). Three answers are exceptions, because each can be the answer
///   to a gateway that retried the same signed bytes on a second backend after
///   it lost the first backend's response:
///   - `already known` / `known transaction`: the node already holds this
///     signed transaction, because an earlier copy of the send reached a
///     mempool;
///   - `nonce too low`: the node already counts the nonce as spent, and does
///     not say which transaction spent it. An earlier copy of the send that
///     was sequenced first spends it too. The answer is unmarked, so it may
///     also answer a gas estimate made at a pinned nonce; it is read as
///     uncertain either way, and the chain resolves it;
///   - `replacement transaction underpriced`: a different transaction from the
///     same account holds the nonce in this node's mempool, so this node
///     broadcast nothing of the send. The answer speaks for one backend only:
///     the backend that lost its response may still hold the first copy, and
///     that copy can win the nonce. A node's seller and buyer services sign
///     with one operator key, so a top-up and a redemption sent at the same
///     moment collide with no gateway at all: the later one gets this answer
///     from a mempool node, or `nonce too low` from a sequencer. The chain
///     then resolves the nonce as spent by the other transaction, and the
///     caller's settle records that this send escrowed nothing.
///
///   All three answers are uncertain, not rejections;
/// - any other unmarked failure happened before the send: a chain read that
///   failed in transport, a signing fault, a local fault. Nothing was signed
///   or sent, so re-running is safe.
///
/// The sending providers all carry the layer (`decdn_client::provider::build_provider`,
/// the node's wallet providers, sponsord's treasury provider). A provider
/// without it reports every transport failure as "nothing sent", so a
/// provider handed to a sender in this crate, `decdn-client`, or the node's
/// redeemer and reclaim sweep must install it.
#[must_use]
pub fn send_broadcast_unknown(err: &ContractError) -> bool {
    use alloy::transports::{RpcError, TransportErrorKind};

    if let Some(failure) = SendLegFailure::from_error(err) {
        return match failure.inner() {
            // The client failed before the request left: nothing reached the wire.
            RpcError::UnsupportedFeature(_)
            | RpcError::LocalUsageError(_)
            | RpcError::SerError(_) => false,
            // An HTTP 4xx (with or without a `Retry-After`) is the gateway
            // refusing the request (bad path, bad key, rate limit) before it
            // reaches the node; a 5xx may have broadcast before failing.
            RpcError::Transport(kind) if let Some(http) = kind.as_http_error() => {
                http.status >= 500
            }
            // A request that never connected, or that reqwest could not
            // build, put nothing on the wire.
            RpcError::Transport(TransportErrorKind::Custom(inner)) => !inner
                .downcast_ref::<alloy::transports::http::reqwest::Error>()
                .is_some_and(|e| e.is_connect() || e.is_builder()),
            // Timeouts, resets, a null response, an undeserializable body: the
            // request may have been processed before the answer was lost.
            _ => true,
        };
    }
    // The node already holds this signed tx, counts its nonce as spent, or
    // holds another tx of ours at that nonce: an earlier copy of the send may
    // have reached a mempool or been sequenced through another backend, so
    // the tx may still mine or may have mined. Every other answer, and every
    // failure before the send, broadcast nothing.
    let ContractError::TransportError(RpcError::ErrorResp(resp)) = err else {
        return false;
    };
    is_already_known(&resp.message)
        || is_nonce_too_low(&resp.message)
        || is_underpriced_replacement(&resp.message)
}

/// Whether a JSON-RPC rejection says the node already counts the nonce as
/// spent: `nonce too low`. The match ignores case.
fn is_nonce_too_low(message: &str) -> bool {
    message.to_ascii_lowercase().contains("nonce too low")
}

/// Whether a JSON-RPC rejection says a different transaction from the same
/// account holds the nonce in the node's mempool: `replacement transaction
/// underpriced`. The match ignores case. A sequencer chain (Arbitrum, anvil)
/// answers most same-key collisions with `nonce too low` instead.
fn is_underpriced_replacement(message: &str) -> bool {
    message
        .to_ascii_lowercase()
        .contains("replacement transaction underpriced")
}

/// Whether a JSON-RPC rejection says the node already holds this signed
/// transaction: `already known` (geth, reth, Nitro) or `known transaction`
/// (older geth). The match ignores case. `unknown transaction …` is not a
/// match, so the `known transaction` hit must not follow a letter.
fn is_already_known(message: &str) -> bool {
    let message = message.to_ascii_lowercase();
    message.contains("already known")
        || message.match_indices("known transaction").any(|(at, _)| {
            message
                .get(..at)
                .and_then(|before| before.chars().next_back())
                .is_none_or(|c| !c.is_ascii_alphabetic())
        })
}

/// Send one call, await its receipt, and fail on a revert.
///
/// `landed` is the caller's record of whether this step may have taken effect,
/// and is the reason an out-param exists rather than just a return value:
///
/// - rejected send → `None` (nothing was broadcast)
/// - a send failure whose broadcast is unknown ([`send_broadcast_unknown`]:
///   the send request failed in transport, or the node answered `already
///   known`, `nonce too low` or `replacement transaction underpriced`) → `None`,
///   but the error text warns that the tx MAY have been broadcast — the
///   outcome is unknown ([`SendOutcome`] carries this as
///   [`MaybeBroadcast`](SendOutcome::MaybeBroadcast) for callers that render
///   structured receipts)
/// - broadcast, receipt unread (a failed read, or none within
///   [`RECEIPT_TIMEOUT`]) → `Some(hash)` **and** `Err` — the outcome is
///   UNKNOWN, and the caller must treat it as possibly-applied
/// - confirmed revert → `None` and `Err` (definitively no effect)
/// - confirmed success → `Some(hash)` and `Ok`
///
/// So `Some` means "may have taken effect", never merely "was broadcast", and an
/// `Err` from here does **not** imply nothing landed. Callers that report what
/// happened must read it that way; asserting "nothing was transferred" on an
/// `Err` is how an operator gets told to re-run a transaction that is still
/// pending. Callers that need the full outcome — the revert hash, the
/// broadcast-unknown case — call [`send_for_receipt`] with a [`SendOutcome`]
/// out-param instead of this `Option<B256>` collapse.
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
/// Fails if the call cannot be sent, if the receipt read fails or no receipt
/// arrives within [`RECEIPT_TIMEOUT`], or if the transaction reverted.
pub async fn send<C: CallDecoder, P: Provider>(
    call: CallBuilder<P, C>,
    label: &str,
    hint: Option<&str>,
    landed: &mut Option<B256>,
) -> anyhow::Result<B256> {
    let mut outcome = SendOutcome::NotSent;
    let result = send_for_receipt(call, label, hint, &mut outcome).await;
    // Collapse the rich outcome to the historical `Option<B256>` contract:
    // `Some(hash)` iff the transaction may have taken effect. A revert clears it
    // (definitively no effect); a maybe-broadcast failure has no hash to give.
    *landed = outcome.tx().filter(|_| outcome.maybe_effected());
    result.map(|r| r.transaction_hash)
}

/// [`send`], recording the full [`SendOutcome`] and returning the whole receipt
/// for callers that must decode event logs from it (a contract's return value is
/// not in a receipt, so an emitted event is the only way to read one back).
///
/// `outcome` is set on every path, so a caller can render a machine-readable
/// receipt from it after the `Result` propagates: the revert hash it needs for
/// #1550, and the broadcast-unknown case it needs for #1577, are both here even
/// though the `Result` cannot carry them.
///
/// # Errors
///
/// Fails if the call cannot be sent, if the receipt read fails or no receipt
/// arrives within [`RECEIPT_TIMEOUT`], or if the transaction reverted.
pub async fn send_for_receipt<C: CallDecoder, P: Provider>(
    call: CallBuilder<P, C>,
    label: &str,
    hint: Option<&str>,
    outcome: &mut SendOutcome,
) -> anyhow::Result<alloy::rpc::types::TransactionReceipt> {
    let suffix = hint.map_or_else(String::new, |h| format!("; {h}"));
    let pending = match call.send().await {
        Ok(pending) => pending,
        Err(err) => {
            // A rejected send, and a chain read that failed before the send,
            // never reached the mempool. The send request failing in transport
            // may have broadcast the tx before its response was lost, an
            // `already known` answer means an earlier copy of it already
            // reached one, and a `nonce too low` or `replacement transaction
            // underpriced` answer may mean an earlier copy, retried by a
            // gateway on a second backend, spent or holds the nonce there.
            // None may be reported as a clean "nothing happened", since
            // re-sending is not idempotent (#1577, #2432, #2431, #2562,
            // #2565).
            let broadcast_unknown = send_broadcast_unknown(&err);
            *outcome = if broadcast_unknown {
                SendOutcome::MaybeBroadcast
            } else {
                SendOutcome::Rejected
            };
            let uncertainty = if broadcast_unknown {
                " The send's outcome is unknown: the RPC node may already hold this transaction, \
                 an earlier copy of it may hold or have spent its nonce on another backend, or \
                 the request may have reached the node and broadcast it before the response was \
                 lost. Check this signer's pending and mined transactions before re-running — \
                 re-sending is not idempotent and can double-submit."
            } else {
                ""
            };
            return Err(anyhow::Error::new(err).context(format!(
                "{label} transaction failed to send. A pre-flight gas estimate rejecting the \
                 call surfaces here rather than as a revert, as do transport and nonce errors — \
                 see the cause below{suffix}.{uncertainty}"
            )));
        }
    };
    let hash = *pending.tx_hash();
    // Recorded BEFORE the receipt is awaited, which is the whole point of the
    // out-param. From here the transaction is broadcast and may take effect; if
    // the receipt cannot be read or does not arrive within `RECEIPT_TIMEOUT`,
    // `outcome` stays `InFlight`, so the caller can still see that a tx is
    // outstanding.
    *outcome = SendOutcome::InFlight(hash);
    let receipt = await_receipt(pending, Some(RECEIPT_TIMEOUT))
        .await
        .with_context(|| {
            format!(
                "{label} sent (tx {hash:#x}) but its receipt was not read; it may still mine — \
                 check the tx before re-running"
            )
        })?;
    // `status()` coerces a pre-Byzantium `PostState` receipt to success. Not
    // reachable on the L2s this targets; noted so the check isn't read as
    // exhaustive over every receipt shape.
    if !receipt.status() {
        // A confirmed revert had no effect — but it DID mine, so keep the hash:
        // `Reverted` is what makes a gas-burning revert distinguishable from a
        // send that never broadcast (#1550). [`SendOutcome::maybe_effected`]
        // still reports `false` for it, so the `Option<B256>` collapse in
        // [`send`] clears the slot exactly as before.
        *outcome = SendOutcome::Reverted(hash);
        anyhow::bail!("{label} reverted (tx {hash:#x}){suffix}");
    }
    *outcome = SendOutcome::Confirmed(hash);
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

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests;
