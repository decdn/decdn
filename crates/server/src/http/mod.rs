//! HTTP surface (Task 11): `/healthz`, `/fund`, `/channel`, `/topup`. See the
//! frozen contract in `.superpowers/sdd/task-11-brief.md` for exact status
//! codes and JSON shapes; handlers live in the sibling `fund`/`channel`/`topup`
//! modules and share the small helpers below.

pub mod channel;
pub mod fund;
pub mod topup;

use std::str::FromStr;

use alloy::primitives::{Address, B256};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde_json::json;

use crate::state::AppState;

/// Build the sponsor server's router over `state`.
pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/healthz", get(healthz))
        .route("/fund", get(fund::page).post(fund::submit))
        .route("/channel", get(channel::get))
        .route("/topup", post(topup::post))
        .with_state(state)
}

async fn healthz() -> Json<serde_json::Value> {
    Json(json!({"ok": true}))
}

/// `{"error": code}` at `status`.
pub(crate) fn err_json(status: StatusCode, code: &str) -> Response {
    (status, Json(json!({"error": code}))).into_response()
}

/// `{"error": code, "detail": detail}` at `status`, for the two contract
/// errors (`open_failed`, `topup_failed`) that carry a detail string.
pub(crate) fn err_json_detail(status: StatusCode, code: &str, detail: &str) -> Response {
    (status, Json(json!({"error": code, "detail": detail}))).into_response()
}

/// Current unix time in seconds. Falls back to `0` rather than panicking if
/// the clock is somehow before the epoch (anti-panic policy).
pub(crate) fn now_unix() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// `0x`-prefixed lowercase hex for a 32-byte channel id.
pub(crate) fn channel_id_hex(id: B256) -> String {
    format!("0x{}", hex::encode(id.0))
}

/// Parse the `client` (0x address) and `hash` (64-hex-char content hash)
/// query/body fields shared by `/fund` and `/channel`. Returns the mapped
/// `400 bad_request` response on any parse failure.
pub(crate) fn parse_client_hash(
    client: &str,
    hash: &str,
) -> Result<(Address, [u8; 32]), Box<Response>> {
    let client = Address::from_str(client)
        .map_err(|_| Box::new(err_json(StatusCode::BAD_REQUEST, "bad_request")))?;
    let bytes = hex::decode(hash.trim_start_matches("0x"))
        .map_err(|_| Box::new(err_json(StatusCode::BAD_REQUEST, "bad_request")))?;
    let hash: [u8; 32] = bytes
        .try_into()
        .map_err(|_| Box::new(err_json(StatusCode::BAD_REQUEST, "bad_request")))?;
    Ok((client, hash))
}

/// Parse a `0x`-prefixed 32-byte channel id. Returns the mapped
/// `400 bad_request` response on any parse failure.
pub(crate) fn parse_channel_id(s: &str) -> Result<B256, Box<Response>> {
    let bytes = hex::decode(s.trim_start_matches("0x"))
        .map_err(|_| Box::new(err_json(StatusCode::BAD_REQUEST, "bad_request")))?;
    let arr: [u8; 32] = bytes
        .try_into()
        .map_err(|_| Box::new(err_json(StatusCode::BAD_REQUEST, "bad_request")))?;
    Ok(B256::from(arr))
}
