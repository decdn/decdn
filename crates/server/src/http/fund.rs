//! `GET /fund` (Turnstile page) and `POST /fund` (issue a capability for the
//! caller's signer, idempotent per signer).

use axum::Json;
use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::response::{Html, IntoResponse, Response};
use serde::Deserialize;
use serde_json::json;

use crate::state::AppState;
use crate::store::GrantRecord;

use super::{err_json, now_unix, parse_client};

#[derive(Debug, Deserialize)]
pub struct FundPageQuery {
    pub client: String,
}

const FUND_PAGE_TEMPLATE: &str = include_str!("../../assets/fund.html");

/// `true` iff `s` is `expected_len` hex digits, optional `0x` prefix — the
/// page's only injection guard (hex can't carry HTML metacharacters).
fn is_hex_of_len(s: &str, expected_len: usize) -> bool {
    let digits = s.strip_prefix("0x").unwrap_or(s);
    digits.len() == expected_len && digits.bytes().all(|b| b.is_ascii_hexdigit())
}

pub async fn page(State(state): State<AppState>, Query(q): Query<FundPageQuery>) -> Response {
    if !is_hex_of_len(&q.client, 40) {
        return err_json(StatusCode::BAD_REQUEST, "bad_request");
    }
    let html = FUND_PAGE_TEMPLATE
        .replace("{{SITEKEY}}", &state.cfg.turnstile_sitekey)
        .replace("{{CLIENT}}", &q.client);
    Html(html).into_response()
}

#[derive(Debug, Deserialize)]
pub struct FundRequest {
    pub client: String,
    pub turnstile_token: String,
}

pub async fn submit(State(state): State<AppState>, Json(req): Json<FundRequest>) -> Response {
    let client = match parse_client(&req.client) {
        Ok(v) => v,
        Err(resp) => return *resp,
    };

    match state.turnstile.verify(&req.turnstile_token, None).await {
        Ok(true) => {}
        Ok(false) => return err_json(StatusCode::FORBIDDEN, "captcha_failed"),
        Err(_) => return err_json(StatusCode::INTERNAL_SERVER_ERROR, "internal"),
    }

    // Idempotent: a signer that already holds a capability gets the same token.
    match state.store.get_grant(client) {
        Ok(Some(rec)) => return Json(json!({ "token": rec.token })).into_response(),
        Ok(None) => {}
        Err(_) => return err_json(StatusCode::INTERNAL_SERVER_ERROR, "internal"),
    }

    let now = now_unix();
    let (token, expiry) = match state.issuer.issue(client, now) {
        Ok(v) => v,
        Err(_) => return err_json(StatusCode::INTERNAL_SERVER_ERROR, "internal"),
    };
    let rec = GrantRecord {
        spending_cap: state.cfg.capability_cap.0,
        expiry,
        issued_unix: now,
        token: token.clone(),
    };
    if state.store.put_grant(client, &rec).is_err() {
        return err_json(StatusCode::INTERNAL_SERVER_ERROR, "internal");
    }
    Json(json!({ "token": token })).into_response()
}
