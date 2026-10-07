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
    // A 4xx that names a retry time is retried.
    let with_retry_after = TransportErrorKind::http_error_with_retry_after(
        403,
        String::new(),
        Some(std::time::Duration::from_secs(30)),
    );
    assert!(!is_permanent_rpc_error(&with_retry_after));
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

/// Neither builder echoes an invalid `rpc_url` into its error: the value
/// commonly carries an API key in its path or query.
#[test]
fn invalid_rpc_url_is_redacted_from_both_builders() {
    let url = "not a url/SECRETKEY123";
    let errors = [
        build_read_provider(url).err().unwrap(),
        build_provider(url, &PrivateKeySigner::random())
            .err()
            .unwrap(),
    ];
    for err in errors {
        let msg = format!("{err:#}");
        assert!(msg.contains("<redacted>"), "{msg}");
        assert!(!msg.contains("SECRETKEY123"), "the URL leaked: {msg}");
    }
}
