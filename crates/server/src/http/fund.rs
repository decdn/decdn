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

/// The Turnstile-widget page shipped in the binary (`include_str!`, not read
/// from disk at runtime).
const FUND_PAGE_TEMPLATE: &str = include_str!("../../assets/fund.html");

/// `true` iff `s` is `expected_len` hex digits, with an optional `0x` prefix.
/// `client`/`hash` are interpolated verbatim into [`FUND_PAGE_TEMPLATE`]
/// below, so this is the page's only injection guard: hex-only input can't
/// carry `<`, `>`, `"`, or any other HTML/script metacharacter.
fn is_hex_of_len(s: &str, expected_len: usize) -> bool {
    let digits = s.strip_prefix("0x").unwrap_or(s);
    digits.len() == expected_len && digits.bytes().all(|b| b.is_ascii_hexdigit())
}

/// Renders the Turnstile widget page: loads the Cloudflare script, shows the
/// challenge for `cfg.turnstile_sitekey`, and on token wires up a `POST
/// /fund` with `{client, hash, turnstile_token}` (see `assets/fund.html`).
/// `client`/`hash` are validated as hex before being interpolated into the
/// page (see `is_hex_of_len`); either failing to parse is a `400`.
pub async fn page(State(state): State<AppState>, Query(q): Query<FundPageQuery>) -> Response {
    if !is_hex_of_len(&q.client, 40) || !is_hex_of_len(&q.hash, 64) {
        return err_json(StatusCode::BAD_REQUEST, "bad_request");
    }
    let html = FUND_PAGE_TEMPLATE
        .replace("{{SITEKEY}}", &state.cfg.turnstile_sitekey)
        .replace("{{CLIENT}}", &q.client)
        .replace("{{HASH}}", &q.hash);
    Html(html).into_response()
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
            // `treasury.open` already spent real funds and escrowed
            // `channel_id` on-chain. If persisting the local
            // `(client,node)->channel_id` mapping fails, a client retry
            // would re-enter this handler, miss the idempotency lookup
            // above, and open a SECOND channel (double-spend). Retry the
            // insert a few times before giving up; if it still fails, do
            // NOT refund the cap (the escrow is real) and log everything
            // needed to recover the orphaned channel by hand.
            let mut last_err = None;
            let mut inserted = false;
            for _ in 0..3 {
                match state.store.insert_channel(channel_id, &rec) {
                    Ok(()) => {
                        inserted = true;
                        break;
                    }
                    Err(e) => last_err = Some(e),
                }
            }
            if !inserted {
                let channel_id_hex = channel_id_hex(channel_id);
                tracing::error!(
                    channel_id = %channel_id_hex,
                    client = %client,
                    provider = %pick.provider,
                    error = %last_err.as_ref().map(ToString::to_string).unwrap_or_default(),
                    "escrowed channel opened on-chain but failed to persist locally after retries; \
                     manual recovery needed to avoid a double-spend on client retry"
                );
                return err_json_detail(
                    StatusCode::BAD_GATEWAY,
                    "open_failed",
                    &channel_id_hex,
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
            if let Err(refund_err) =
                state
                    .store
                    .cap_refund(client, month_bucket(now), state.cfg.initial_deposit)
            {
                tracing::error!(
                    error = %refund_err,
                    client = %client,
                    "cap_refund failed after treasury.open error"
                );
            }
            err_json_detail(StatusCode::BAD_GATEWAY, "open_failed", &e.to_string())
        }
    }
}
