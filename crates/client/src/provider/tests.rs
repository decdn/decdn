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

/// A fully specified request from `signer`: every filler is finished, so the
/// first request on the wire is the send itself.
fn specified_request(signer: &PrivateKeySigner) -> alloy::rpc::types::TransactionRequest {
    alloy::rpc::types::TransactionRequest {
        from: Some(signer.address()),
        to: Some(alloy::primitives::TxKind::Call(
            alloy::primitives::Address::repeat_byte(0x01),
        )),
        nonce: Some(0),
        gas: Some(21_000),
        chain_id: Some(1),
        max_fee_per_gas: Some(1),
        max_priority_fee_per_gas: Some(1),
        ..Default::default()
    }
}

/// The wallet-filled builder carries the send-leg marker: a send request
/// that fails in transport (nothing listens on the port) comes back as a
/// [`decdn_incentive::tx::SendLegFailure`], so the senders can tell it from a
/// chain read that failed before it (#2431).
#[tokio::test]
async fn wallet_builder_marks_a_failed_send_request() {
    let signer = PrivateKeySigner::random();
    let provider = build_provider("http://127.0.0.1:1", &signer).unwrap();
    let err = provider
        .send_transaction(specified_request(&signer))
        .await
        .unwrap_err();
    assert!(
        decdn_incentive::tx::SendLegFailure::from_transport_error(&err).is_some(),
        "{err}"
    );
}

/// The wallet-filled builder sends an estimated gas limit with
/// [`decdn_incentive::tx::GAS_HEADROOM`] on top. At the raw estimate, a
/// `declareMbps` estimated in the second of the operator's last checkpoint ran
/// out of gas when it mined in the next one (#2730).
#[tokio::test]
async fn wallet_builder_sends_an_estimate_with_headroom() {
    let estimate = 57_076;
    let stub = decdn_incentive::test_rpc::GasStub::spawn(estimate)
        .await
        .unwrap();
    let signer = PrivateKeySigner::random();
    let provider = build_provider(stub.url(), &signer).unwrap();
    let request = alloy::rpc::types::TransactionRequest {
        gas: None,
        ..specified_request(&signer)
    };
    let _pending = provider.send_transaction(request).await.unwrap();
    assert_eq!(
        stub.sent_gas_limits(),
        vec![decdn_incentive::tx::with_headroom(estimate)]
    );
}

/// reqwest is built with `rustls-no-provider` and panics in `Client::build`
/// with no process-wide rustls provider. An SDK host that installs none must
/// still get a chain client, so each builder installs ring itself. nextest
/// runs each test in its own process, so nothing installed one beforehand.
#[test]
fn read_builder_works_with_no_provider_installed() {
    assert!(build_read_provider("https://rpc.example").is_ok());
}

/// The wallet-filled builder installs the provider too; see
/// [`read_builder_works_with_no_provider_installed`].
#[test]
fn wallet_builder_works_with_no_provider_installed() {
    assert!(build_provider("https://rpc.example", &PrivateKeySigner::random()).is_ok());
}
