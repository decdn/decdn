//! The daemon's HTTP API: `POST /v1/capabilities`, `GET /v1/info`, and an
//! unauthenticated `GET /healthz`. Every `/v1` route requires
//! `Authorization: Bearer <token>`, compared in constant time.

use std::str::FromStr;
use std::sync::Arc;

use alloy::primitives::Address;
use axum::extract::rejection::JsonRejection;
use axum::extract::{Request, State};
use axum::http::{StatusCode, header};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::Deserialize;
use serde_json::json;
use sponsord_core::issuer::TermsError;
use sponsord_core::{Sponsor, SponsorError};
use subtle::ConstantTimeEq;

#[derive(Clone)]
pub struct ApiState {
    pub sponsor: Arc<Sponsor>,
    pub api_token: Arc<str>,
}

pub fn router(state: ApiState) -> Router {
    let api = Router::new()
        .route("/v1/capabilities", post(issue))
        .route("/v1/info", get(info))
        .route_layer(middleware::from_fn_with_state(state.clone(), require_token));
    Router::new()
        .route("/healthz", get(healthz))
        .merge(api)
        .with_state(state)
}

fn err_json(status: StatusCode, code: &str) -> Response {
    (status, Json(json!({ "error": code }))).into_response()
}

fn now_unix() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
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
        _ => err_json(StatusCode::UNAUTHORIZED, "unauthorized"),
    }
}

async fn healthz() -> Json<serde_json::Value> {
    Json(json!({ "ok": true }))
}

async fn info(State(state): State<ApiState>) -> Json<serde_json::Value> {
    let i = state.sponsor.info();
    Json(json!({
        "chain_id": i.chain_id,
        "payment_pool": i.payment_pool.to_string(),
        "max_spending_cap": i.max_spending_cap,
        "max_ttl_secs": i.max_ttl_secs,
    }))
}

#[derive(Deserialize)]
struct IssueRequest {
    signer: String,
    spending_cap: Option<u64>,
    ttl_secs: Option<u64>,
}

async fn issue(
    State(state): State<ApiState>,
    body: Result<Json<IssueRequest>, JsonRejection>,
) -> Response {
    let Ok(Json(req)) = body else {
        return err_json(StatusCode::BAD_REQUEST, "bad_request");
    };
    let Ok(signer) = Address::from_str(&req.signer) else {
        return err_json(StatusCode::BAD_REQUEST, "bad_request");
    };
    match state
        .sponsor
        .issue(signer, req.spending_cap, req.ttl_secs, now_unix())
        .await
    {
        Ok(i) => Json(json!({
            "token": i.token,
            "spending_cap": i.spending_cap,
            "expiry": i.expiry,
            "registered": i.registered,
        }))
        .into_response(),
        Err(SponsorError::Terms(TermsError::ExceedsMax { .. })) => {
            let i = state.sponsor.info();
            (
                StatusCode::BAD_REQUEST,
                Json(json!({
                    "error": "exceeds_max",
                    "max_spending_cap": i.max_spending_cap,
                    "max_ttl_secs": i.max_ttl_secs,
                })),
            )
                .into_response()
        }
        Err(SponsorError::Terms(TermsError::Zero { .. })) => {
            err_json(StatusCode::BAD_REQUEST, "zero")
        }
        Err(SponsorError::SignerExpired { expiry }) => (
            StatusCode::CONFLICT,
            Json(json!({ "error": "signer_expired", "expiry": expiry })),
        )
            .into_response(),
        Err(SponsorError::Chain(e)) => {
            tracing::warn!("signer authorization read failed: {e}");
            err_json(StatusCode::SERVICE_UNAVAILABLE, "chain_unavailable")
        }
        Err(SponsorError::Sign(e)) => {
            tracing::error!("capability signing failed: {e}");
            err_json(StatusCode::INTERNAL_SERVER_ERROR, "internal")
        }
    }
}
