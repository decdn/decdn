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
mod tests;
