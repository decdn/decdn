//! The daemon's HTTP API: `POST /v1/capabilities`, `GET /v1/info`, and the
//! unauthenticated `GET /healthz` and `GET /metrics`. Every `/v1` route requires
//! `Authorization: Bearer <token>`, compared in constant time.

use std::sync::Arc;

use axum::extract::rejection::JsonRejection;
use axum::extract::{Request, State};
use axum::http::{HeaderValue, header};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use decdn_common::redact::sanitize_err_chain;
use sponsord_api::daemon::{Health, Info, IssueRequest, IssueResponse, routes};
use sponsord_api::secret::Secret;
use sponsord_api::time::{Clock, SystemClock};
use sponsord_api::{ErrorBody, ErrorCode};
use sponsord_core::{Sponsor, SponsorError, TermsError, TermsRequest};
use subtle::ConstantTimeEq;

use crate::metrics::Metrics;

/// What the HTTP handlers share.
#[derive(Clone)]
pub struct ApiState {
    /// Issues the capabilities and reports the keeper's status.
    pub sponsor: Arc<Sponsor>,
    /// Compared in constant time; held as a [`Secret`] so it is wiped on drop.
    pub api_token: Arc<Secret>,
    /// The time capabilities are issued at and registrations checked against.
    pub clock: Arc<dyn Clock>,
    /// Issue and error counters for `GET /metrics`.
    pub metrics: Arc<Metrics>,
}

// By hand: the clock is a trait object, and the token stays out.
impl std::fmt::Debug for ApiState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ApiState")
            .field("sponsor", &self.sponsor)
            .finish_non_exhaustive()
    }
}

impl ApiState {
    /// State on the system clock.
    #[must_use]
    pub fn new(sponsor: Arc<Sponsor>, api_token: Secret) -> Self {
        Self {
            sponsor,
            api_token: Arc::new(api_token),
            clock: Arc::new(SystemClock),
            metrics: Arc::default(),
        }
    }
}

/// The daemon's routes, with the bearer-token check on every `/v1` route.
pub fn router(state: ApiState) -> Router {
    let api = Router::new()
        .route(routes::CAPABILITIES, post(issue))
        .route(routes::INFO, get(info))
        .route_layer(middleware::from_fn_with_state(state.clone(), require_token));
    Router::new()
        .route(routes::HEALTHZ, get(healthz))
        .route(routes::METRICS, get(metrics))
        .merge(api)
        .with_state(state)
}

/// The token from an `Authorization` value with a case-insensitive `Bearer`
/// scheme (RFC 7235).
fn bearer_token(value: &str) -> Option<&str> {
    let (scheme, token) = value.split_once(' ')?;
    scheme
        .eq_ignore_ascii_case("bearer")
        .then_some(token.trim())
}

async fn require_token(State(state): State<ApiState>, req: Request, next: Next) -> Response {
    let presented = req
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(bearer_token);
    match presented {
        Some(t) if bool::from(t.as_bytes().ct_eq(state.api_token.expose().as_bytes())) => {
            next.run(req).await
        }
        _ => {
            state.metrics.error(ErrorCode::Unauthorized);
            ErrorCode::Unauthorized.into_response()
        }
    }
}

async fn healthz() -> Json<Health> {
    Json(Health { ok: true })
}

async fn metrics(State(state): State<ApiState>) -> Response {
    let body = state
        .metrics
        .render(&state.sponsor.keeper_status().snapshot());
    let mut resp = body.into_response();
    resp.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("text/plain; version=0.0.4; charset=utf-8"),
    );
    resp
}

async fn info(State(state): State<ApiState>) -> Json<Info> {
    Json(state.sponsor.info())
}

async fn issue(
    State(state): State<ApiState>,
    body: Result<Json<IssueRequest>, JsonRejection>,
) -> Result<Json<IssueResponse>, ErrorBody> {
    let result = issue_inner(&state, body).await;
    match &result {
        Ok(Json(issued)) => state.metrics.issued(issued.registered),
        Err(e) => state.metrics.error(e.error),
    }
    result
}

async fn issue_inner(
    state: &ApiState,
    body: Result<Json<IssueRequest>, JsonRejection>,
) -> Result<Json<IssueResponse>, ErrorBody> {
    let Ok(Json(req)) = body else {
        return Err(ErrorCode::BadRequest.into());
    };
    let now = state.clock.now_unix();
    let terms = TermsRequest {
        spending_cap: req.spending_cap,
        ttl_secs: req.ttl_secs,
    };
    match state.sponsor.issue(req.signer, &terms, now).await {
        Ok(issued) => Ok(Json(issued)),
        Err(SponsorError::Terms(TermsError::ExceedsMax { .. })) => {
            let i = state.sponsor.info();
            Err(ErrorBody {
                max_spending_cap: Some(i.max_spending_cap),
                max_ttl_secs: Some(i.max_ttl_secs),
                ..ErrorBody::new(ErrorCode::ExceedsMax)
            })
        }
        Err(SponsorError::Terms(TermsError::Zero { .. })) => Err(ErrorCode::Zero.into()),
        Err(SponsorError::SignerExpired { expiry }) => Err(ErrorBody {
            expiry: Some(expiry),
            ..ErrorBody::new(ErrorCode::SignerExpired)
        }),
        Err(SponsorError::Chain(e)) => {
            tracing::warn!(
                "signer authorization read failed: {}",
                sanitize_err_chain(&e)
            );
            Err(ErrorCode::ChainUnavailable.into())
        }
        Err(SponsorError::Sign(e)) => {
            tracing::error!("capability signing failed: {}", sanitize_err_chain(&e));
            Err(ErrorCode::Internal.into())
        }
    }
}
