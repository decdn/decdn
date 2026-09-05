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
//! Lives in `decdn-incentive` rather than the CLI so every on-chain write in
//! this crate shares one send/receipt path.
//!
//! One caller is deliberately NOT migrated: `cli::commands::channel`'s
//! `submit_close` / `submit_settle` / `submit_reclaim` classify a revert as an
//! expected *outcome* (`TxOutcome::Reverted`, an `Ok`) rather than an error,
//! because a channel that someone else already closed is a normal race, not a
//! failure. Routing them through here would turn that into a hard error. Any
//! other hand-rolled send/receipt pair is a bug — it will lose the hash on a
//! receipt timeout.

use alloy::contract::{CallBuilder, CallDecoder, Error as ContractError};
use alloy::primitives::B256;
use alloy::providers::Provider;
use anyhow::Context;

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
/// - [`Rejected`](Self::Rejected) — `send()` failed with a JSON-RPC error
///   response (a pre-flight gas-estimate revert, a stale nonce, an under-funded
///   account). The node answered, and nothing entered the mempool.
/// - [`MaybeBroadcast`](Self::MaybeBroadcast) — `send()` failed with a
///   transport-class error. The request may have reached the node and broadcast
///   the transaction, but no hash was captured. The outcome is UNKNOWN.
/// - [`InFlight`](Self::InFlight) — broadcast, but the receipt could not be
///   fetched. In flight, may still confirm. Carries the hash.
/// - [`Reverted`](Self::Reverted) — broadcast and mined, but reverted: it burned
///   gas and had no effect. Carries the hash as a permanent on-chain handle.
/// - [`Confirmed`](Self::Confirmed) — confirmed success. Carries the hash.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum SendOutcome {
    /// No send has resolved into this slot — the initial value, and the value a
    /// dry run leaves.
    #[default]
    NotSent,
    /// The node rejected the call before broadcast (a JSON-RPC error response).
    /// Nothing entered the mempool.
    Rejected,
    /// `send()` failed with a transport-class error (timeout, connection reset, a
    /// 5xx from an RPC load balancer). The transaction may have been broadcast;
    /// no hash was captured, so the outcome is genuinely unknown and re-sending
    /// can double-submit.
    MaybeBroadcast,
    /// Broadcast; the receipt could not be read. May still confirm.
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
    /// [`MaybeBroadcast`](Self::MaybeBroadcast) has none by construction: a
    /// transport error surfaces before `send()` returns a handle, so there is no
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
    /// True for a confirmed success, an in-flight broadcast, and a transport
    /// failure whose broadcast is unknown; false for a definitive rejection and a
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
/// the transaction before its response was lost. Two kinds of failure are
/// definite rejections that broadcast nothing:
///
/// - a JSON-RPC error *response* — the node processed the request and rejected
///   it (a pre-flight gas-estimate revert, the modal `createNamespace` /
///   `addOrigin` failure; a stale nonce; an under-funded account);
/// - a client-side fault raised before the request left the process (an
///   unsupported feature, a local-usage error, a body that failed to serialize),
///   or an HTTP 4xx where the gateway refused the request outright (a bad path,
///   an expired key) without forwarding it to the node.
///
/// Everything else is genuinely uncertain: a timeout, a connection reset, an HTTP
/// 5xx from an RPC load balancer, or a response body that could not be
/// deserialized. There the transaction may already be in the mempool, and
/// `eth_sendRawTransaction` is not idempotent from the client's view, so the
/// caller must not report "nothing happened".
///
/// The `as_error_resp().is_none()` shorthand is deliberately NOT used: it would
/// fold the local faults above into the uncertain bucket, warning about a
/// possibly-broadcast transaction on a bug that never touched the wire.
///
/// `const` because the workspace `missing_const_for_fn` clippy lint requires it —
/// the body is a pure classification with no non-const calls.
const fn send_broadcast_unknown(err: &ContractError) -> bool {
    use alloy::transports::{RpcError, TransportErrorKind};

    let ContractError::TransportError(rpc) = err else {
        // ABI, pending-transaction, and other contract-level faults are local —
        // nothing reached the wire.
        return false;
    };
    match rpc {
        // The node answered, or the client failed before sending: a definite
        // rejection that broadcast nothing.
        RpcError::ErrorResp(_)
        | RpcError::UnsupportedFeature(_)
        | RpcError::LocalUsageError(_)
        | RpcError::SerError(_) => false,
        // An HTTP 4xx is the gateway refusing the request (bad path, bad key)
        // before it reaches the node; a 5xx may have broadcast before failing.
        RpcError::Transport(TransportErrorKind::HttpError(h)) => h.status >= 500,
        // Timeouts, resets, a null response, an undeserializable body: the
        // request may have been processed before the answer was lost.
        _ => true,
    }
}

/// Send one call, await its receipt, and fail on a revert.
///
/// `landed` is the caller's record of whether this step may have taken effect,
/// and is the reason an out-param exists rather than just a return value:
///
/// - rejected send → `None` (nothing was broadcast)
/// - transport-class send failure → `None`, but the error text warns that the tx
///   MAY have been broadcast — the outcome is unknown ([`SendOutcome`] carries
///   this as [`MaybeBroadcast`](SendOutcome::MaybeBroadcast) for callers that
///   render structured receipts)
/// - broadcast, receipt unreadable → `Some(hash)` **and** `Err` — the outcome is
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
/// Fails if the call cannot be sent, if the receipt cannot be fetched, or if the
/// transaction reverted.
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
    // (definitively no effect); a transport-class failure has no hash to give.
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
/// Fails if the call cannot be sent, if the receipt cannot be fetched, or if the
/// transaction reverted.
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
            // A rejected send never reached the mempool; a transport-class
            // failure may have broadcast the tx before its response was lost.
            // The latter must not be reported as a clean "nothing happened",
            // since re-sending is not idempotent (#1577).
            let broadcast_unknown = send_broadcast_unknown(&err);
            *outcome = if broadcast_unknown {
                SendOutcome::MaybeBroadcast
            } else {
                SendOutcome::Rejected
            };
            let uncertainty = if broadcast_unknown {
                " This is a transport error, not a rejection: the request may have reached the \
                 node and broadcast the transaction before the response was lost. Check this \
                 signer's pending and mined transactions before re-running — re-sending is not \
                 idempotent and can double-submit."
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
    // the receipt cannot be read `outcome` stays `InFlight`, so the caller can
    // still see that a tx is outstanding.
    *outcome = SendOutcome::InFlight(hash);
    let receipt = pending.get_receipt().await.with_context(|| {
        format!("{label} sent (tx {hash:#x}) but the receipt could not be fetched")
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
mod tests {
    use super::*;

    /// A JSON-RPC error *response* — the node answered, rejecting the call.
    /// Built by deserializing the wire shape, since `ErrorPayload` is not
    /// re-exported through `alloy::transports` (only `RpcError` is).
    fn error_resp(code: i64, message: &str) -> ContractError {
        ContractError::TransportError(alloy::transports::RpcError::ErrorResp(
            serde_json::from_value(serde_json::json!({ "code": code, "message": message }))
                .unwrap(),
        ))
    }

    /// The tx hash reaches the receipt on every arm that has one, and only
    /// those; `maybe_effected` splits the "may be pending" arms from the
    /// definitively-nothing arms.
    #[test]
    fn send_outcome_hash_and_effect_are_total() {
        let h = B256::repeat_byte(0x55);
        assert_eq!(SendOutcome::Confirmed(h).tx(), Some(h));
        assert_eq!(SendOutcome::Reverted(h).tx(), Some(h));
        assert_eq!(SendOutcome::InFlight(h).tx(), Some(h));
        assert_eq!(SendOutcome::MaybeBroadcast.tx(), None);
        assert_eq!(SendOutcome::Rejected.tx(), None);
        assert_eq!(SendOutcome::NotSent.tx(), None);

        assert!(SendOutcome::Confirmed(h).maybe_effected());
        assert!(SendOutcome::InFlight(h).maybe_effected());
        // The #1577 case: a transport failure may have broadcast the tx, so it
        // must NOT be reported as "nothing happened".
        assert!(SendOutcome::MaybeBroadcast.maybe_effected());
        // A confirmed revert had no effect even though it kept its hash (#1550).
        assert!(!SendOutcome::Reverted(h).maybe_effected());
        assert!(!SendOutcome::Rejected.maybe_effected());
        assert!(!SendOutcome::NotSent.maybe_effected());
    }

    /// An HTTP transport error carrying `status`, for the 4xx/5xx split.
    fn http_error(status: u16) -> ContractError {
        ContractError::TransportError(alloy::transports::RpcError::Transport(
            alloy::transports::TransportErrorKind::HttpError(alloy::transports::HttpError {
                status,
                body: String::new(),
            }),
        ))
    }

    /// Failures that broadcast nothing: a JSON-RPC error *response* (the node
    /// answered and rejected), a client-side fault that never left the process,
    /// and an HTTP 4xx where the gateway refused the request. Re-running any of
    /// them is safe, so none may warn about a possibly-broadcast transaction.
    #[test]
    fn a_rejection_is_not_a_broadcast() {
        // The modal case: a pre-flight gas-estimate revert.
        assert!(!send_broadcast_unknown(&error_resp(
            3,
            "execution reverted"
        )));
        assert!(!send_broadcast_unknown(&error_resp(
            -32000,
            "nonce too low"
        )));
        // Local faults raised before the request leaves the client — these must
        // NOT be lumped into the uncertain bucket by an `as_error_resp()` shortcut.
        assert!(!send_broadcast_unknown(&ContractError::TransportError(
            alloy::transports::RpcError::UnsupportedFeature("batching")
        )));
        // A non-transport contract error is local too.
        assert!(!send_broadcast_unknown(&ContractError::ContractNotDeployed));
        // HTTP 4xx: the gateway refused the request (bad path, expired key).
        assert!(!send_broadcast_unknown(&http_error(401)));
        assert!(!send_broadcast_unknown(&http_error(404)));
    }

    /// A timeout / reset / 5xx / undeserializable body leaves the tx's fate
    /// unknown — it may be in the mempool. Classifying this as a clean rejection
    /// is the #1577 bug.
    #[test]
    fn a_transport_failure_leaves_the_broadcast_unknown() {
        let transport = ContractError::TransportError(
            alloy::transports::TransportErrorKind::custom_str("connection reset"),
        );
        assert!(send_broadcast_unknown(&transport));
        // A null/absent response is transport-shaped too: no answer came back.
        assert!(send_broadcast_unknown(&ContractError::TransportError(
            alloy::transports::RpcError::NullResp
        )));
        // HTTP 5xx: the node may have broadcast before the load balancer failed.
        assert!(send_broadcast_unknown(&http_error(502)));
        assert!(send_broadcast_unknown(&http_error(500)));
    }
}
