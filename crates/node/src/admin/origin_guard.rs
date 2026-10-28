//! An HTTP layer that refuses every admin RPC request a browser sends.
//!
//! A browser stamps an `Origin` header on every cross-origin request and on
//! every same-origin `POST`. The admin CLI, the dashboard poller and the e2e
//! harness send none, so a request that carries one is a web page, and the
//! layer answers it `403` before the RPC server reads its body.

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use jsonrpsee::core::BoxError;
use jsonrpsee::server::http::response;
use jsonrpsee::server::{HttpRequest, HttpResponse};
use tower::{Layer, Service};

/// Tower layer that wraps the admin RPC service in [`RejectOrigin`].
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct RejectOriginLayer;

impl<S> Layer<S> for RejectOriginLayer {
    type Service = RejectOrigin<S>;

    fn layer(&self, inner: S) -> Self::Service {
        RejectOrigin { inner }
    }
}

/// Answers `403` to any request that carries an `Origin` header and passes
/// every other request to `inner`.
#[derive(Debug, Clone)]
pub(crate) struct RejectOrigin<S> {
    inner: S,
}

impl<S, B> Service<HttpRequest<B>> for RejectOrigin<S>
where
    S: Service<HttpRequest<B>, Response = HttpResponse>,
    S::Error: Into<BoxError> + 'static,
    S::Future: Send + 'static,
{
    type Response = HttpResponse;
    type Error = BoxError;
    type Future = Pin<Box<dyn Future<Output = Result<HttpResponse, BoxError>> + Send + 'static>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx).map_err(Into::into)
    }

    fn call(&mut self, request: HttpRequest<B>) -> Self::Future {
        if request.headers().contains_key(hyper::header::ORIGIN) {
            tracing::debug!("admin RPC: refused a request that carries an Origin header");
            return Box::pin(async { Ok(response::denied()) });
        }
        let call = self.inner.call(request);
        Box::pin(async move { call.await.map_err(Into::into) })
    }
}
