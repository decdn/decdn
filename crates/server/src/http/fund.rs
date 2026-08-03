//! `GET /fund` (placeholder Turnstile page) and `POST /fund` (open a payment
//! channel, or return the existing one idempotently).

use axum::Json;
use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::response::{Html, IntoResponse, Response};
use serde::Deserialize;
use serde_json::json;

use crate::cap::CapDecision;
use crate::money::month_bucket;
use crate::state::AppState;
use crate::store::ChannelRecord;

use super::{channel_id_hex, err_json, err_json_detail, now_unix, parse_client_hash};

#[derive(Debug, Deserialize)]
pub struct FundPageQuery {
    pub client: String,
    pub hash: String,
}

/// Minimal placeholder page: Task 17 fills in the real Turnstile widget.
/// Carries the sitekey, client, and hash through so that later task can wire
/// up the challenge without changing this handler's signature.
pub async fn page(State(state): State<AppState>, Query(q): Query<FundPageQuery>) -> Html<String> {
    Html(format!(
        "<!doctype html><html><body>\n\
         <div data-turnstile-sitekey=\"{}\" data-client=\"{}\" data-hash=\"{}\">\n\
         Verify you are not a robot to fund a payment channel.\n\
         </div></body></html>",
        state.cfg.turnstile_sitekey, q.client, q.hash
    ))
}

#[derive(Debug, Deserialize)]
pub struct FundRequest {
    pub client: String,
    pub hash: String,
    pub turnstile_token: String,
}

pub async fn submit(State(state): State<AppState>, Json(req): Json<FundRequest>) -> Response {
    let (client, hash) = match parse_client_hash(&req.client, &req.hash) {
        Ok(v) => v,
        Err(resp) => return *resp,
    };

    match state.turnstile.verify(&req.turnstile_token, None).await {
        Ok(true) => {}
        Ok(false) => return err_json(StatusCode::FORBIDDEN, "captcha_failed"),
        Err(_) => return err_json(StatusCode::INTERNAL_SERVER_ERROR, "internal"),
    }

    let pick = match state.discovery.resolve(hash).await {
        Ok(Some(p)) => p,
        Ok(None) => return err_json(StatusCode::NOT_FOUND, "no_provider"),
        Err(_) => return err_json(StatusCode::INTERNAL_SERVER_ERROR, "internal"),
    };

    match state.store.get_by_client_node(client, pick.node_id) {
        Ok(Some((id, rec))) => {
            return Json(json!({
                "channel_id": channel_id_hex(id),
                "node_id": hex::encode(pick.node_id),
                "provider": rec.provider.to_string(),
            }))
            .into_response();
        }
        Ok(None) => {}
        Err(_) => return err_json(StatusCode::INTERNAL_SERVER_ERROR, "internal"),
    }

    let now = now_unix();
    let decision =
        match state
            .cap
            .check_and_reserve(&state.store, client, now, state.cfg.initial_deposit)
        {
            Ok(d) => d,
            Err(_) => return err_json(StatusCode::INTERNAL_SERVER_ERROR, "internal"),
        };
    if matches!(decision, CapDecision::Exhausted { .. }) {
        return err_json(StatusCode::TOO_MANY_REQUESTS, "cap_exhausted");
    }

    match state
        .treasury
        .open(pick.provider, client, state.cfg.initial_deposit)
        .await
    {
        Ok(channel_id) => {
            let rec = ChannelRecord {
                client,
                provider: pick.provider,
                node_id: pick.node_id,
                deposit_micro: state.cfg.initial_deposit.0,
                opened_unix: now,
            };
            if let Err(e) = state.store.insert_channel(channel_id, &rec) {
                return err_json_detail(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "internal",
                    &e.to_string(),
                );
            }
            Json(json!({
                "channel_id": channel_id_hex(channel_id),
                "node_id": hex::encode(pick.node_id),
                "provider": pick.provider.to_string(),
            }))
            .into_response()
        }
        Err(e) => {
            let _ = state
                .store
                .cap_refund(client, month_bucket(now), state.cfg.initial_deposit);
            err_json_detail(StatusCode::BAD_GATEWAY, "open_failed", &e.to_string())
        }
    }
}
