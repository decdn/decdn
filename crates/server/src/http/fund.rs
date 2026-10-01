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

    let now = now_unix();

    // Idempotent: a signer that already holds a still-valid capability gets
    // the same token back. A signer whose stored grant has expired gets a
    // fresh capability only while it is unregistered on-chain. `PaymentPool`
    // writes a signer's cap and expiry once, at its first redemption, and
    // ignores every later capability for it, so a re-issued token for a
    // redeemed signer would carry an expiry nodes accept but the contract
    // never honors: vouchers under it redeem to 0. A registered signer gets
    // `409 signer_registered` and must come back with a fresh key.
    //
    // Two concurrent `POST /fund` for the same signer can both miss this
    // lookup and both sign below. Both tokens are validly signed for the
    // same signer and pool, `put_grant` is last-writer-wins, and the client
    // reads back whatever `GET /capability` returns.
    match state.store.get_grant(client) {
        Ok(Some(rec)) if now < rec.expiry => {
            return Json(json!({ "token": rec.token })).into_response();
        }
        Ok(Some(_)) => match state
            .treasury
            .signer_registered(state.cfg.pool_id, client)
            .await
        {
            Ok(false) => {}
            Ok(true) => return err_json(StatusCode::CONFLICT, "signer_registered"),
            Err(_) => return err_json(StatusCode::INTERNAL_SERVER_ERROR, "internal"),
        },
        Ok(None) => {}
        Err(_) => return err_json(StatusCode::INTERNAL_SERVER_ERROR, "internal"),
    }

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
