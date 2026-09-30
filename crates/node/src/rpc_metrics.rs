//! Per-request JSON-RPC metrics: a tower layer on the alloy RPC client.
//!
//! Every provider the runtime builds goes through [`metered_http_client`], so
//! the layer sees every request the node sends to `blockchain.rpc_url` — reads,
//! filler lookups, and transaction sends alike — without touching a call site.
//! Each request counts once on `decdn_rpc_requests_total{method,outcome}`; see
//! [`RpcOutcome`] for the outcome set. The labels carry only the method and the
//! outcome, never the URL, which can hold an API key.
//!
//! The startup `net_version` reachability probe and its watchdog use a bare
//! `reqwest` client, not a provider, so they report on `decdn_rpc_healthy`
//! instead.

use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use alloy::rpc::client::{ClientBuilder, RpcClient};
use alloy::transports::http::reqwest::Url;
use alloy::transports::{RpcError, TransportError, TransportErrorKind, TransportFut};
use alloy_json_rpc::{ErrorPayload, Id, RequestPacket, ResponsePacket};
use tower::{Layer, Service};

use crate::metrics::{Metrics, RpcMethod, RpcOutcome};

/// Build an HTTP RPC client for `url` whose every request records into
/// `metrics`. The same client `ProviderBuilder::connect_http` builds, with
/// [`RpcMetricsLayer`] in front of the transport, so the localhost poll
/// default alloy derives from the URL is unchanged.
pub(crate) fn metered_http_client(url: Url, metrics: &Arc<Metrics>) -> RpcClient {
    ClientBuilder::default()
        .layer(RpcMetricsLayer::new(Arc::clone(metrics)))
        .http(url)
}

/// Whether a failed call is the provider's rate limit: HTTP 429, an HTTP error
/// that carries a `Retry-After` delay, or a JSON-RPC error response with code
/// 429 or rate-limit wording (Infura `-32005 "project ID request rate
/// exceeded"`).
pub(crate) fn is_rate_limit(err: &TransportError) -> bool {
    match err {
        RpcError::Transport(kind) => is_rate_limited_transport(kind),
        RpcError::ErrorResp(resp) => is_rate_limited_response(resp),
        _ => false,
    }
}

/// Words that mark a JSON-RPC error response as a rate limit. See
/// [`is_rate_limit`].
const RATE_LIMIT_WORDS: [&str; 3] = ["rate limit", "rate exceeded", "too many requests"];

fn is_rate_limited_transport(kind: &TransportErrorKind) -> bool {
    kind.retry_after().is_some() || kind.as_http_error().is_some_and(|h| h.status == 429)
}

fn is_rate_limited_response(resp: &ErrorPayload) -> bool {
    let message = resp.message.to_ascii_lowercase();
    resp.code == 429 || RATE_LIMIT_WORDS.iter().any(|word| message.contains(word))
}

/// Classify a JSON-RPC error response. A rate limit wins over a revert, since a
/// throttled call never ran.
fn classify_error_response(resp: &ErrorPayload) -> RpcOutcome {
    if is_rate_limited_response(resp) {
        RpcOutcome::RateLimited
    } else if resp.code == 3 || resp.message.to_ascii_lowercase().contains("revert") {
        RpcOutcome::Reverted
    } else {
        RpcOutcome::RpcError
    }
}

/// Classify a transport-level failure: the request produced no JSON-RPC reply
/// the layer can read.
fn classify_transport_error(err: &TransportError) -> RpcOutcome {
    match err {
        RpcError::ErrorResp(resp) => classify_error_response(resp),
        RpcError::Transport(kind) if is_rate_limited_transport(kind) => RpcOutcome::RateLimited,
        RpcError::Transport(TransportErrorKind::Custom(inner))
            if inner
                .downcast_ref::<reqwest::Error>()
                .is_some_and(reqwest::Error::is_timeout) =>
        {
            RpcOutcome::Timeout
        }
        _ => RpcOutcome::TransportError,
    }
}

/// Classify the request with id `id` from the transport's result. A single
/// reply answers the one request it was sent for; a batch reply is matched by
/// id, and a request the batch left out is a transport error.
fn classify(result: &Result<ResponsePacket, TransportError>, id: &Id) -> RpcOutcome {
    let packet = match result {
        Ok(packet) => packet,
        Err(err) => return classify_transport_error(err),
    };
    let response = match packet {
        ResponsePacket::Single(response) => Some(response),
        ResponsePacket::Batch(responses) => responses.iter().find(|r| r.id == *id),
    };
    match response.map(|r| r.payload.as_error()) {
        Some(None) => RpcOutcome::Ok,
        Some(Some(err)) => classify_error_response(err),
        None => RpcOutcome::TransportError,
    }
}

/// Tower layer that wraps an alloy transport in [`RpcMetricsService`].
#[derive(Debug, Clone)]
pub(crate) struct RpcMetricsLayer {
    metrics: Arc<Metrics>,
}

impl RpcMetricsLayer {
    pub(crate) const fn new(metrics: Arc<Metrics>) -> Self {
        Self { metrics }
    }
}

impl<S> Layer<S> for RpcMetricsLayer {
    type Service = RpcMetricsService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        RpcMetricsService {
            inner,
            metrics: Arc::clone(&self.metrics),
        }
    }
}

/// Transport wrapper that records every request of every packet it sends.
#[derive(Debug, Clone)]
pub(crate) struct RpcMetricsService<S> {
    inner: S,
    metrics: Arc<Metrics>,
}

impl<S> Service<RequestPacket> for RpcMetricsService<S>
where
    S: Service<
            RequestPacket,
            Response = ResponsePacket,
            Error = TransportError,
            Future = TransportFut<'static>,
        > + Send
        + Sync
        + Clone
        + 'static,
{
    type Response = ResponsePacket;
    type Error = TransportError;
    type Future = TransportFut<'static>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: RequestPacket) -> Self::Future {
        let mut in_flight = InFlight::new(Arc::clone(&self.metrics), &req);
        let fut = self.inner.call(req);
        Box::pin(async move {
            let result = fut.await;
            in_flight.finish(&result);
            result
        })
    }
}

/// The requests of one packet that have not been recorded yet. [`Self::finish`]
/// records each against the transport's result. A packet whose future is
/// dropped first records each request as [`RpcOutcome::Timeout`] on drop.
struct InFlight {
    metrics: Arc<Metrics>,
    calls: Vec<(RpcMethod, Id)>,
    started: Instant,
}

impl InFlight {
    fn new(metrics: Arc<Metrics>, req: &RequestPacket) -> Self {
        let calls = req
            .requests()
            .iter()
            .map(|r| (RpcMethod::from_name(r.method()), r.id().clone()))
            .collect();
        Self {
            metrics,
            calls,
            started: Instant::now(),
        }
    }

    fn finish(&mut self, result: &Result<ResponsePacket, TransportError>) {
        let elapsed: Duration = self.started.elapsed();
        for (method, id) in self.calls.drain(..) {
            self.metrics
                .rpc_request(method, classify(result, &id), Some(elapsed));
        }
    }
}

impl Drop for InFlight {
    fn drop(&mut self) {
        for (method, _) in self.calls.drain(..) {
            self.metrics.rpc_request(method, RpcOutcome::Timeout, None);
        }
    }
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]
mod tests {
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
                .any(|l| l
                    == "decdn_rpc_request_duration_seconds_count{method=\"eth_blockNumber\"} 1"),
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
                .any(|l| l
                    == "decdn_rpc_request_duration_seconds_count{method=\"eth_blockNumber\"} 0"),
            "{text}"
        );
    }
}
