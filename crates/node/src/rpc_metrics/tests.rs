use super::*;
use alloy::providers::{Provider, ProviderBuilder};
use alloy::transports::HttpError;
use alloy_json_rpc::{Response, ResponsePayload};
use wiremock::matchers::method;
use wiremock::{Mock, MockServer, Request, ResponseTemplate};

fn error_payload(code: i64, message: &str) -> ErrorPayload {
    serde_json::from_value(serde_json::json!({ "code": code, "message": message })).unwrap()
}

fn response(id: u64, payload: Result<(), ErrorPayload>) -> alloy_json_rpc::Response {
    Response {
        id: Id::Number(id),
        payload: match payload {
            Ok(()) => ResponsePayload::Success(serde_json::value::to_raw_value(&1).unwrap()),
            Err(err) => ResponsePayload::Failure(err),
        },
    }
}

fn http(status: u16) -> TransportError {
    RpcError::Transport(TransportErrorKind::HttpError(HttpError {
        status,
        body: String::new(),
    }))
}

#[test]
fn classifies_single_replies() {
    let id = Id::Number(0);
    let single = |payload| Ok(ResponsePacket::Single(response(0, payload)));
    assert_eq!(classify(&single(Ok(())), &id), RpcOutcome::Ok);
    for (code, message, want) in [
        (3, "execution reverted", RpcOutcome::Reverted),
        (-32000, "Execution Reverted: bad", RpcOutcome::Reverted),
        (-32602, "invalid params", RpcOutcome::RpcError),
        (
            -32000,
            "query returned more than 10000 results",
            RpcOutcome::RpcError,
        ),
        (429, "slow down", RpcOutcome::RateLimited),
        (
            -32005,
            "project ID request rate exceeded",
            RpcOutcome::RateLimited,
        ),
        (-32000, "Too Many Requests", RpcOutcome::RateLimited),
    ] {
        assert_eq!(
            classify(&single(Err(error_payload(code, message))), &id),
            want,
            "{code} {message:?}"
        );
    }
}

#[test]
fn classifies_transport_failures() {
    let id = Id::Number(0);
    let err = |e: TransportError| Err::<ResponsePacket, _>(e);
    assert_eq!(classify(&err(http(429)), &id), RpcOutcome::RateLimited);
    let retry_after = TransportErrorKind::http_error_with_retry_after(
        503,
        String::new(),
        Some(Duration::from_secs(1)),
    );
    assert_eq!(classify(&err(retry_after), &id), RpcOutcome::RateLimited);
    assert_eq!(classify(&err(http(502)), &id), RpcOutcome::TransportError);
    assert_eq!(
        classify(
            &err(TransportErrorKind::custom_str("connection refused")),
            &id
        ),
        RpcOutcome::TransportError
    );
    assert_eq!(
        classify(&err(TransportErrorKind::backend_gone()), &id),
        RpcOutcome::TransportError
    );
    assert_eq!(
        classify(&err(RpcError::ErrorResp(error_payload(3, "reverted"))), &id),
        RpcOutcome::Reverted
    );
}

#[test]
fn batch_replies_are_matched_by_id() {
    let batch = Ok(ResponsePacket::Batch(vec![
        response(1, Err(error_payload(3, "execution reverted"))),
        response(0, Ok(())),
    ]));
    assert_eq!(classify(&batch, &Id::Number(0)), RpcOutcome::Ok);
    assert_eq!(classify(&batch, &Id::Number(1)), RpcOutcome::Reverted);
    assert_eq!(classify(&batch, &Id::Number(2)), RpcOutcome::TransportError);
}

#[test]
fn method_names_map_onto_the_closed_set() {
    for method in RpcMethod::ALL {
        if method != RpcMethod::Other {
            assert_eq!(RpcMethod::from_name(method.as_str()), method);
        }
    }
    for name in ["other", "debug_traceTransaction", "", "https://key@host"] {
        assert_eq!(RpcMethod::from_name(name), RpcMethod::Other, "{name:?}");
    }
}

fn requests_line(method: &str, outcome: &str) -> String {
    format!("decdn_rpc_requests_total{{method=\"{method}\",outcome=\"{outcome}\"}}")
}

fn count(text: &str, method: &str, outcome: &str) -> Option<u64> {
    let prefix = requests_line(method, outcome);
    text.lines()
        .find_map(|l| l.strip_prefix(&prefix)?.strip_prefix(' ')?.parse().ok())
}

/// Answer every JSON-RPC request with `body`, echoing the request's id.
fn echo_id(body: serde_json::Value) -> impl Fn(&Request) -> ResponseTemplate {
    move |req: &Request| {
        let id = serde_json::from_slice::<serde_json::Value>(&req.body)
            .ok()
            .and_then(|v| v.get("id").cloned())
            .unwrap_or_default();
        let mut reply = body.clone();
        reply["jsonrpc"] = "2.0".into();
        reply["id"] = id;
        ResponseTemplate::new(200).set_body_json(reply)
    }
}

async fn provider_against(
    template: impl wiremock::Respond + 'static,
) -> (MockServer, Arc<Metrics>, impl Provider) {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(template)
        .mount(&server)
        .await;
    let metrics = Arc::new(Metrics::new());
    let url: Url = server.uri().parse().unwrap();
    let provider = ProviderBuilder::new().connect_client(metered_http_client(url, &metrics));
    (server, metrics, provider)
}

/// End to end through a real HTTP transport: a served read counts as `ok`
/// under its method and records a duration sample; every other series
/// stays at zero.
#[tokio::test]
async fn a_served_request_counts_ok_under_its_method() {
    let (_server, metrics, provider) =
        provider_against(echo_id(serde_json::json!({ "result": "0x10" }))).await;
    assert_eq!(provider.get_block_number().await.unwrap(), 16);

    let text = metrics.encode().unwrap();
    assert_eq!(count(&text, "eth_blockNumber", "ok"), Some(1), "{text}");
    assert_eq!(count(&text, "eth_blockNumber", "transport_error"), Some(0));
    assert_eq!(count(&text, "eth_call", "ok"), Some(0));
    assert!(
        text.lines()
            .any(|l| l == "decdn_rpc_request_duration_seconds_count{method=\"eth_blockNumber\"} 1"),
        "{text}"
    );
}

#[tokio::test]
async fn a_revert_and_a_throttle_count_apart() {
    let (_server, metrics, provider) = provider_against(echo_id(
        serde_json::json!({ "error": { "code": 3, "message": "execution reverted" } }),
    ))
    .await;
    provider.get_chain_id().await.unwrap_err();
    let text = metrics.encode().unwrap();
    assert_eq!(count(&text, "eth_chainId", "reverted"), Some(1), "{text}");

    let (_server, metrics, provider) = provider_against(ResponseTemplate::new(429)).await;
    provider.get_block_number().await.unwrap_err();
    let text = metrics.encode().unwrap();
    assert_eq!(
        count(&text, "eth_blockNumber", "rate_limited"),
        Some(1),
        "{text}"
    );
}

/// A request whose caller gives up counts as `timeout` and records no
/// duration: the layer's drop guard, not the transport, sees it end.
#[tokio::test]
async fn a_dropped_request_counts_as_timeout() {
    let (_server, metrics, provider) =
        provider_against(ResponseTemplate::new(200).set_delay(Duration::from_secs(30))).await;
    let outcome =
        tokio::time::timeout(Duration::from_millis(50), provider.get_block_number()).await;
    assert!(outcome.is_err(), "the read should still be pending");

    let text = metrics.encode().unwrap();
    assert_eq!(
        count(&text, "eth_blockNumber", "timeout"),
        Some(1),
        "{text}"
    );
    assert!(
        text.lines()
            .any(|l| l == "decdn_rpc_request_duration_seconds_count{method=\"eth_blockNumber\"} 0"),
        "{text}"
    );
}
