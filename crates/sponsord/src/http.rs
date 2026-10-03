//! The daemon's HTTP API: `POST /v1/capabilities`, `GET /v1/info`, and an
//! unauthenticated `GET /healthz`. Every `/v1` route requires
//! `Authorization: Bearer <token>`, compared in constant time.

use std::sync::Arc;

use axum::extract::rejection::JsonRejection;
use axum::extract::{Request, State};
use axum::http::header;
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use sponsord_api::daemon::{Health, Info, IssueRequest, IssueResponse, routes};
use sponsord_api::time::{Clock, SystemClock};
use sponsord_api::{ErrorBody, ErrorCode, IssuedCapability, MicroUsdc};
use sponsord_core::issuer::TermsError;
use sponsord_core::{Sponsor, SponsorError};
use subtle::ConstantTimeEq;

#[derive(Clone)]
pub struct ApiState {
    pub sponsor: Arc<Sponsor>,
    pub api_token: Arc<str>,
    pub clock: Arc<dyn Clock>,
}

impl ApiState {
    /// State on the system clock.
    #[must_use]
    pub fn new(sponsor: Arc<Sponsor>, api_token: Arc<str>) -> Self {
        Self {
            sponsor,
            api_token,
            clock: Arc::new(SystemClock),
        }
    }
}

pub fn router(state: ApiState) -> Router {
    let api = Router::new()
        .route(routes::CAPABILITIES, post(issue))
        .route(routes::INFO, get(info))
        .route_layer(middleware::from_fn_with_state(state.clone(), require_token));
    Router::new()
        .route(routes::HEALTHZ, get(healthz))
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
        Some(t) if bool::from(t.as_bytes().ct_eq(state.api_token.as_bytes())) => {
            next.run(req).await
        }
        _ => ErrorCode::Unauthorized.into_response(),
    }
}

async fn healthz() -> Json<Health> {
    Json(Health { ok: true })
}

fn wire_info(sponsor: &Sponsor) -> Info {
    let i = sponsor.info();
    Info {
        chain_id: i.chain_id,
        payment_pool: i.payment_pool,
        max_spending_cap: MicroUsdc(i.max_spending_cap),
        max_ttl_secs: i.max_ttl_secs,
    }
}

async fn info(State(state): State<ApiState>) -> Json<Info> {
    Json(wire_info(&state.sponsor))
}

async fn issue(
    State(state): State<ApiState>,
    body: Result<Json<IssueRequest>, JsonRejection>,
) -> Result<Json<IssueResponse>, ErrorBody> {
    let Ok(Json(req)) = body else {
        return Err(ErrorCode::BadRequest.into());
    };
    let now = state.clock.now_unix();
    match state
        .sponsor
        .issue(req.signer, req.spending_cap.map(|c| c.0), req.ttl_secs, now)
        .await
    {
        Ok(i) => Ok(Json(IssueResponse {
            capability: IssuedCapability {
                token: i.token,
                spending_cap: MicroUsdc(i.spending_cap),
                expiry: i.expiry,
            },
            registered: i.registered,
        })),
        Err(SponsorError::Terms(TermsError::ExceedsMax { .. })) => {
            let i = wire_info(&state.sponsor);
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
            tracing::warn!("signer authorization read failed: {e}");
            Err(ErrorCode::ChainUnavailable.into())
        }
        Err(SponsorError::Sign(e)) => {
            tracing::error!("capability signing failed: {e}");
            Err(ErrorCode::Internal.into())
        }
    }
}
