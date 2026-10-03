//! `GET /fund` (Turnstile page) and `POST /fund` (get a capability for the
//! caller's signer from the daemon, idempotent per signer while the stored
//! grant is valid).

use axum::Json;
use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::response::{Html, IntoResponse, Response};
use serde::Deserialize;
use serde_json::json;

use crate::daemon::SourceError;
use crate::state::AppState;
use crate::store::GrantRecord;

use super::{err_json, now_unix, parse_client};

#[derive(Debug, Deserialize)]
pub struct FundPageQuery {
    pub client: String,
}

const FUND_PAGE_TEMPLATE: &str = include_str!("../../assets/fund.html");

/// `true` iff `s` is `expected_len` hex digits, optional `0x` prefix: the
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
    // the stored token back without a daemon call. An expired or missing
    // grant goes to the daemon, which hands back a registered signer's
    // existing capability or signs a fresh one.
    //
    // Two concurrent `POST /fund` for the same signer can both miss this
    // lookup and both call the daemon. That is benign: each token is validly
    // signed for the same signer and pool, `put_grant` is last-writer-wins,
    // and the client reads back whatever `GET /capability` returns.
    match state.store.get_grant(client) {
        Ok(Some(rec)) if now < rec.expiry => {
            return Json(json!({ "token": rec.token })).into_response();
        }
        Ok(_) => {}
        Err(_) => return err_json(StatusCode::INTERNAL_SERVER_ERROR, "internal"),
    }

    let grant = match state
        .source
        .issue(client, state.cfg.spending_cap, state.cfg.ttl_secs)
        .await
    {
        Ok(g) => g,
        Err(SourceError::SignerExpired(_)) => {
            return err_json(StatusCode::CONFLICT, "signer_expired");
        }
        Err(SourceError::Unavailable(e)) => {
            tracing::warn!("daemon unavailable: {e}");
            return err_json(StatusCode::BAD_GATEWAY, "upstream");
        }
        Err(SourceError::Misconfigured(e)) => {
            tracing::error!(
                "daemon rejected the issue request, check ONRAMP_DAEMON_TOKEN and terms: {e}"
            );
            return err_json(StatusCode::INTERNAL_SERVER_ERROR, "internal");
        }
    };
    let rec = GrantRecord {
        spending_cap: grant.spending_cap,
        expiry: grant.expiry,
        issued_unix: now,
        token: grant.token.clone(),
    };
    if state.store.put_grant(client, &rec).is_err() {
        return err_json(StatusCode::INTERNAL_SERVER_ERROR, "internal");
    }
    Json(json!({ "token": grant.token })).into_response()
}
