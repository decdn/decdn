//! `GET /capability?client=<0xSIGNER>`: return the `dcap1:` token issued to
//! `client`, or `204` if none yet. The CLI polls this after the browser
//! captcha flow completes.

use axum::Json;
use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde::Deserialize;
use serde_json::json;

use crate::state::AppState;

use super::{err_json, parse_client};

#[derive(Debug, Deserialize)]
pub struct CapabilityQuery {
    pub client: String,
}

pub async fn get(State(state): State<AppState>, Query(q): Query<CapabilityQuery>) -> Response {
    let client = match parse_client(&q.client) {
        Ok(v) => v,
        Err(resp) => return *resp,
    };
    match state.store.get_grant(client) {
        Ok(Some(rec)) => Json(json!({ "token": rec.token })).into_response(),
        Ok(None) => StatusCode::NO_CONTENT.into_response(),
        Err(_) => err_json(StatusCode::INTERNAL_SERVER_ERROR, "internal"),
    }
}
