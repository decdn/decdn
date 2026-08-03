//! `POST /topup`: authorize (by signature) and fund an existing channel back
//! up to `working_balance`.

use axum::Json;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde::Deserialize;
use serde_json::json;

use crate::cap::CapDecision;
use crate::money::{MicroUsdc, month_bucket};
use crate::state::AppState;
use crate::store::ChannelRecord;
use crate::topup_auth::{self, AuthError};

use super::{err_json, err_json_detail, now_unix, parse_channel_id};

#[derive(Debug, Deserialize)]
pub struct TopupRequest {
    pub channel_id: String,
    pub timestamp: u64,
    pub signature: String,
}

pub async fn post(State(state): State<AppState>, Json(req): Json<TopupRequest>) -> Response {
    let channel_id = match parse_channel_id(&req.channel_id) {
        Ok(v) => v,
        Err(resp) => return *resp,
    };

    let rec = match state.store.get_by_channel(channel_id) {
        Ok(Some(r)) => r,
        Ok(None) => return err_json(StatusCode::NOT_FOUND, "unknown_channel"),
        Err(_) => return err_json(StatusCode::INTERNAL_SERVER_ERROR, "internal"),
    };

    let now = now_unix();
    if let Err(e) = topup_auth::verify(
        channel_id,
        req.timestamp,
        now,
        &req.signature,
        rec.client,
        state.cfg.topup_max_skew_secs,
    ) {
        return match e {
            AuthError::Stale => err_json(StatusCode::REQUEST_TIMEOUT, "stale_timestamp"),
            AuthError::BadSignature => err_json(StatusCode::UNAUTHORIZED, "bad_signature"),
        };
    }

    let target = state.cfg.working_balance;
    let current = match state.treasury.deposit_of(channel_id).await {
        Ok(c) => c,
        Err(_) => return err_json(StatusCode::INTERNAL_SERVER_ERROR, "internal"),
    };
    if current.0 >= target.0 {
        return Json(json!({"ok": true, "deposit_micro_usdc": current.0})).into_response();
    }
    let delta = MicroUsdc(target.0 - current.0);

    let decision = match state
        .cap
        .check_and_reserve(&state.store, rec.client, now, delta)
    {
        Ok(d) => d,
        Err(_) => return err_json(StatusCode::INTERNAL_SERVER_ERROR, "internal"),
    };
    if matches!(decision, CapDecision::Exhausted { .. }) {
        return err_json(StatusCode::TOO_MANY_REQUESTS, "cap_exhausted");
    }

    match state
        .treasury
        .top_up_to(channel_id, rec.provider, target)
        .await
    {
        Ok(new_dep) => {
            let updated = ChannelRecord {
                deposit_micro: new_dep.0,
                ..rec
            };
            if let Err(e) = state.store.insert_channel(channel_id, &updated) {
                return err_json_detail(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "internal",
                    &e.to_string(),
                );
            }
            Json(json!({"ok": true, "deposit_micro_usdc": new_dep.0})).into_response()
        }
        Err(e) => {
            let _ = state.store.cap_refund(rec.client, month_bucket(now), delta);
            err_json_detail(StatusCode::BAD_GATEWAY, "topup_failed", &e.to_string())
        }
    }
}
