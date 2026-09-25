//! Wallet-filled HTTP provider builder shared by the client commands
//! (`fetch`, `bundle pull`, `channel`) and the on-chain operator commands
//! (`register`, `bond`, `setup`), plus the retryable-vs-permanent classifier
//! for a failed chain read
//! ([`is_permanent_contract_error`](crate::provider::is_permanent_contract_error),
//! [`is_permanent_rpc_error`](crate::provider::is_permanent_rpc_error)).
//!
//! It lives in the SDK so any client of the `cdn/client/v1` data plane — not
//! just the `decdn` CLI bin crate — can build the signing provider it opens and
//! settles payment channels with. The daemon's boot chain reads and the
//! client's registry discovery classify a typed chain error with the same
//! classifier.

use alloy::network::EthereumWallet;
use alloy::providers::{Provider, ProviderBuilder};
use alloy::signers::local::PrivateKeySigner;
use anyhow::Context;

/// Build a wallet-filled HTTP provider that signs and sends transactions as
/// `signer`. The `Url` type is inferred from `connect_http`'s parameter;
/// naming it explicitly would need `alloy::transports`, which is not exposed
/// under this crate's alloy feature set.
///
/// # Errors
///
/// Fails if `rpc_url` is not a valid URL. The value is never echoed into the
/// error: an `rpc_url` secret commonly lives in the path/query, which userinfo
/// redaction wouldn't scrub, so the value is hidden entirely (matching
/// `config validate`'s `<redacted> (N chars)`).
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
        .wallet(EthereumWallet::from(signer.clone()))
        .connect_http(rpc_url.parse().with_context(|| {
            format!(
                "rpc_url is not a valid URL (<redacted>, {} chars)",
                rpc_url.len()
            )
        })?))
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
///   expired API key;
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
        _ => false,
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use alloy::transports::{RpcError, TransportErrorKind};

    /// A JSON-RPC error *response* — HTTP 200 with an error body, so the HTTP
    /// status check never sees it.
    ///
    /// Built by deserializing the wire shape: `ErrorPayload` is not re-exported
    /// through `alloy::transports` (only `RpcError` is), so the variant's own
    /// type inference is what names it here.
    fn error_resp(json: serde_json::Value) -> alloy::transports::TransportError {
        RpcError::ErrorResp(serde_json::from_value(json).unwrap())
    }

    fn resp(code: i64, message: &str) -> alloy::transports::TransportError {
        error_resp(serde_json::json!({ "code": code, "message": message }))
    }

    fn http(status: u16) -> alloy::transports::TransportError {
        RpcError::Transport(TransportErrorKind::HttpError(
            alloy::transports::HttpError {
                status,
                body: String::new(),
            },
        ))
    }

    #[test]
    fn provider_rate_limits_stay_retryable() {
        // Providers signal rate limiting as a JSON-RPC error response over HTTP
        // 200, so treating every `ErrorResp` as deterministic would skip the
        // retry for one of the most common transient failures there is.
        for (code, message) in [
            (429, "Too Many Requests"),
            (-32005, "exceeded project rate limit"),
            (-32016, "Your app has exceeded its rate limit"),
            (-32007, "100/second request limit reached"),
        ] {
            assert!(
                !is_permanent_rpc_error(&resp(code, message)),
                "JSON-RPC {code} ({message}) is a rate limit and must be retried"
            );
        }
    }

    #[test]
    fn provider_upstream_outages_stay_retryable() {
        // Providers report upstream outages with codes outside any rate-limit
        // allowlist; they must stay retryable.
        for (code, message) in [
            (1, "no available upstreams to process the request"),
            (19, "Temporary internal error. Please retry"),
            (-32603, "internal error"),
            (-32000, "header not found"),
        ] {
            assert!(
                !is_permanent_rpc_error(&resp(code, message)),
                "JSON-RPC {code} ({message}) is a provider outage and must be retried"
            );
        }
    }

    #[test]
    fn deterministic_responses_are_permanent() {
        for (code, message) in [
            (-32600, "invalid request"),
            (-32601, "method not found"),
            (-32602, "invalid params"),
            (3, "execution reverted"),
            (-32000, "execution reverted"),
            (-32015, "VM execution error: Reverted"),
        ] {
            assert!(
                is_permanent_rpc_error(&resp(code, message)),
                "JSON-RPC {code} ({message}) repeats on retry"
            );
        }
        let revert_with_data = error_resp(serde_json::json!({
            "code": 3,
            "message": "execution reverted: custom error",
            "data": "0x08c379a0",
        }));
        assert!(is_permanent_rpc_error(&revert_with_data));
    }

    #[test]
    fn http_client_errors_are_permanent_except_timeout_and_rate_limit() {
        for status in [400, 401, 403, 404] {
            assert!(is_permanent_rpc_error(&http(status)), "HTTP {status}");
        }
        for status in [408, 429, 500, 502, 503, 504] {
            assert!(!is_permanent_rpc_error(&http(status)), "HTTP {status}");
        }
    }

    #[test]
    fn transport_failures_are_transient_and_contract_faults_permanent() {
        assert!(!is_permanent_rpc_error(&RpcError::NullResp));
        assert!(!is_permanent_contract_error(
            &alloy::contract::Error::TransportError(RpcError::NullResp)
        ));
        assert!(is_permanent_contract_error(
            &alloy::contract::Error::UnknownFunction("usdc".to_string())
        ));
        assert!(is_permanent_contract_error(
            &alloy::contract::Error::ContractNotDeployed
        ));
        assert!(is_permanent_contract_error(
            &alloy::contract::Error::TransportError(resp(-32601, "method not found"))
        ));
    }
}
