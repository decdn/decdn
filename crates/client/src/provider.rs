//! HTTP provider builders for the `decdn` CLI and SDK clients — a
//! wallet-filled builder that signs and sends transactions, and a wallet-free
//! builder for reads that run before a keystore is unlocked — plus the
//! retryable-vs-permanent classifier for a failed chain read
//! ([`is_permanent_contract_error`](crate::provider::is_permanent_contract_error),
//! [`is_permanent_rpc_error`](crate::provider::is_permanent_rpc_error)).
//!
//! It lives in the SDK so any client of the `cdn/client/v1` data plane — not
//! just the `decdn` CLI bin crate — can build the signing provider it opens and
//! settles payment pools with. The daemon's boot chain reads and the
//! client's registry discovery classify a typed chain error with the same
//! classifier.

use alloy::network::EthereumWallet;
use alloy::providers::{Provider, ProviderBuilder};
use alloy::signers::local::PrivateKeySigner;
use anyhow::Context;

/// Build a wallet-filled HTTP provider that signs and sends transactions as
/// `signer`. It reads the pending nonce on every send, so it stays correct
/// after a send that pins its own nonce ([`crate::buyer_pool::top_up`]) or a
/// transaction sent from elsewhere.
///
/// # Errors
///
/// Fails if `rpc_url` is not a valid URL. The error never echoes the value,
/// which can carry an API key.
// `use<>` pins the returned provider to capture *no* input lifetimes: it owns
// a clone of `signer` and the parsed URL, so it is genuinely `'static`. Without
// the precise-capturing bound, Rust 2024's RPIT rules over-capture `&signer`,
// which would stop callers (e.g. `setup`'s USDC swap venue) from handing the
// provider to APIs that need `P: 'static` (`DynProvider::erased`).
pub fn build_provider(
    rpc_url: &str,
    signer: &PrivateKeySigner,
) -> anyhow::Result<impl Provider + Clone + use<>> {
    Ok(ProviderBuilder::new()
        .with_simple_nonce_management()
        .wallet(EthereumWallet::from(signer.clone()))
        .connect_http(parse_rpc_url(rpc_url)?))
}

/// Build a wallet-free HTTP provider for read-only calls. It lets a command
/// read chain state before it unlocks a keystore, so a check that fails asks
/// for no password.
///
/// # Errors
///
/// Fails if `rpc_url` is not a valid URL. The error never echoes the value,
/// which can carry an API key.
pub fn build_read_provider(rpc_url: &str) -> anyhow::Result<impl Provider + Clone + use<>> {
    Ok(ProviderBuilder::new().connect_http(parse_rpc_url(rpc_url)?))
}

/// Parse the configured RPC endpoint into the type the caller's
/// `connect_http` takes. It is generic so that type is inferred at each call
/// site; naming `Url` would need `alloy::transports`, which this crate's alloy
/// feature set does not expose.
///
/// # Errors
///
/// Fails if `rpc_url` is not a valid URL. The value is never echoed into the
/// error: an `rpc_url` secret commonly lives in the path/query, which userinfo
/// redaction wouldn't scrub, so the value is hidden entirely (in the style of
/// `config validate`'s `<redacted> (N chars)`).
fn parse_rpc_url<U: std::str::FromStr>(rpc_url: &str) -> anyhow::Result<U>
where
    U::Err: std::error::Error + Send + Sync + 'static,
{
    rpc_url.parse().with_context(|| {
        format!(
            "rpc_url is not a valid URL (<redacted>, {} chars)",
            rpc_url.len()
        )
    })
}

/// Whether a failed contract call is deterministic — the same call fails the
/// same way however often it is repeated, so retrying it only spends a retry
/// budget before reporting the error it was always going to report.
///
/// Only a transport failure can be transient; [`is_permanent_rpc_error`]
/// decides those. Every other variant is a contract-level fault that repeats
/// identically: no contract at the configured address
/// ([`ZeroData`](alloy::contract::Error::ZeroData) — the typo'd-address case),
/// an ABI that does not match the binding, an unknown function or selector, a
/// failed deployment.
///
/// The classifier is for reads: it calls every non-transport failure permanent,
/// including a pending-transaction error, which a write path must judge on its
/// own.
#[must_use]
#[doc(hidden)]
pub fn is_permanent_contract_error(err: &alloy::contract::Error) -> bool {
    match err {
        alloy::contract::Error::TransportError(e) => is_permanent_rpc_error(e),
        _ => true,
    }
}

/// Whether a failed JSON-RPC request is deterministic, so a retry cannot
/// succeed.
///
/// This is a denylist — retry unless the failure is known to repeat. The
/// permanent failures are the ones a request carries with it:
///
/// - a JSON-RPC error response that is a revert (code `3`, or a message that
///   names a revert, in any case), or an invalid request, unknown method or
///   invalid params (`-32600`, `-32601`, `-32602`);
/// - an HTTP 4xx other than 408 and 429 — a wrong path, an unauthorized or
///   expired API key. An HTTP error whose body is a JSON-RPC error reaches
///   here as an error response and is judged by its code, not its status;
/// - a request that could not be serialized, or that the transport rejects
///   locally.
///
/// Every other failure is transient: refused connections, resets, timeouts,
/// truncated bodies, HTTP 5xx, a 4xx that carries `Retry-After`, and every other
/// JSON-RPC error response. Providers report rate limits and upstream outages
/// as JSON-RPC error responses over HTTP 200, with codes that differ by
/// provider — for example `1` "no available upstreams" and `19` "Temporary
/// internal error".
///
/// alloy's own retry checks are allowlists, and each misses a transient class.
/// `ErrorPayload::is_retry_err` names some providers' rate-limit codes, but not
/// outage codes such as `1` and `19`. `TransportErrorKind::is_retry_err` accepts
/// only a few transport kinds, so it rejects an ordinary refused connection or
/// reset.
#[must_use]
#[doc(hidden)]
pub fn is_permanent_rpc_error(err: &alloy::transports::TransportError) -> bool {
    use alloy::transports::{RpcError, TransportErrorKind};

    match err {
        // -32602..=-32600: invalid params, method not found, invalid request.
        RpcError::ErrorResp(resp) => {
            resp.code == 3
                || resp.message.to_ascii_lowercase().contains("revert")
                || (-32602..=-32600).contains(&resp.code)
        }
        RpcError::UnsupportedFeature(_) | RpcError::LocalUsageError(_) | RpcError::SerError(_) => {
            true
        }
        RpcError::Transport(TransportErrorKind::HttpError(h)) => {
            (400..500).contains(&h.status) && !matches!(h.status, 408 | 429)
        }
        // Everything else is transport-shaped and transient. That includes
        // `HttpErrorWithRetryAfter`: alloy reports an HTTP error that carries a
        // `Retry-After` header as its own variant, and a server that names when
        // to retry expects a retry, whatever the status.
        _ => false,
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests;
