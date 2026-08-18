//! HTTP surface: `/healthz`, `/decdn.sh`, `/fund` (captcha page + issue),
//! `/capability` (poll for the issued token). Handlers live in the sibling
//! `fund`/`capability`/`installer` modules and share the helpers below.

pub mod capability;
pub mod fund;
pub mod installer;

use std::str::FromStr;

use alloy::primitives::Address;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use serde_json::json;

use crate::state::AppState;

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/healthz", get(healthz))
        .route("/decdn.sh", get(installer::get))
        .route("/fund", get(fund::page).post(fund::submit))
        .route("/capability", get(capability::get))
        .with_state(state)
}

async fn healthz() -> Json<serde_json::Value> {
    Json(json!({"ok": true}))
}

pub(crate) fn err_json(status: StatusCode, code: &str) -> Response {
    (status, Json(json!({"error": code}))).into_response()
}

pub(crate) fn now_unix() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Parse the `client` (0x signer address) shared by `/fund` and
/// `/capability`. Returns the mapped `400 bad_request` on failure.
pub(crate) fn parse_client(client: &str) -> Result<Address, Box<Response>> {
    Address::from_str(client)
        .map_err(|_| Box::new(err_json(StatusCode::BAD_REQUEST, "bad_request")))
}
