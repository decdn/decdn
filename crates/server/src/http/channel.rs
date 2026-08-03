//! `GET /channel`: look up an already-opened channel for `(client, hash)`
//! without opening a new one.

use axum::Json;
use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde::Deserialize;
use serde_json::json;

use crate::state::AppState;

use super::{channel_id_hex, err_json, parse_client_hash};

#[derive(Debug, Deserialize)]
pub struct ChannelQuery {
    pub client: String,
    pub hash: String,
}

pub async fn get(State(state): State<AppState>, Query(q): Query<ChannelQuery>) -> Response {
    let (client, hash) = match parse_client_hash(&q.client, &q.hash) {
        Ok(v) => v,
        Err(resp) => return *resp,
    };

    let pick = match state.discovery.resolve(hash).await {
        Ok(Some(p)) => p,
        Ok(None) => return StatusCode::NO_CONTENT.into_response(),
        Err(_) => return err_json(StatusCode::INTERNAL_SERVER_ERROR, "internal"),
    };

    match state.store.get_by_client_node(client, pick.node_id) {
        Ok(Some((id, rec))) => Json(json!({
            "channel_id": channel_id_hex(id),
            "node_id": hex::encode(pick.node_id),
            "provider": rec.provider.to_string(),
        }))
        .into_response(),
        Ok(None) => StatusCode::NO_CONTENT.into_response(),
        Err(_) => err_json(StatusCode::INTERNAL_SERVER_ERROR, "internal"),
    }
}
