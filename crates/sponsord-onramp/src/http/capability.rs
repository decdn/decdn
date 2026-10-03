//! `GET /v1/capability?client=<0xSIGNER>`: return the `dcap1:` token issued
//! to `client`, or `204` if none yet. The CLI polls this after the browser
//! gate flow completes.

use axum::Json;
use axum::extract::rejection::QueryRejection;
use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use sponsord_api::ErrorCode;
use sponsord_api::onramp::{CapabilityResponse, ClientQuery};

use crate::state::AppState;

pub async fn get(
    State(state): State<AppState>,
    query: Result<Query<ClientQuery>, QueryRejection>,
) -> Response {
    let Ok(Query(ClientQuery { client })) = query else {
        return ErrorCode::BadRequest.into_response();
    };
    match state.store.get_grant(client) {
        // An expired grant is treated as absent so the polling CLI
        // re-triggers the browser fund flow instead of latching onto a
        // token it can never redeem.
        Ok(Some(rec)) if state.clock.now_unix() >= rec.expiry => {
            StatusCode::NO_CONTENT.into_response()
        }
        Ok(Some(rec)) => Json(CapabilityResponse { token: rec.token }).into_response(),
        Ok(None) => StatusCode::NO_CONTENT.into_response(),
        Err(_) => ErrorCode::Internal.into_response(),
    }
}
