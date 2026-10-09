//! `GET /v1/capability?client=<0xSIGNER>`: return the `dcap1:` token issued
//! to `client`, or `204` if none yet. The CLI polls this while the person
//! passes the gate in the browser.

use axum::Json;
use axum::extract::rejection::QueryRejection;
use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use sponsord_api::ErrorCode;
use sponsord_api::onramp::{CapabilityResponse, ClientQuery};

use crate::net::ClientIp;
use crate::state::AppState;

/// `GET /v1/capability`: the token held for `client`, `204` if none is, or
/// `400` without a valid `client` query.
pub async fn get(
    State(state): State<AppState>,
    ClientIp(ip): ClientIp,
    query: Result<Query<ClientQuery>, QueryRejection>,
) -> Response {
    let now = state.clock.now_unix();
    if !state.poll_limit.allow(ip, now) {
        return ErrorCode::RateLimited.into_response();
    }
    let Ok(Query(ClientQuery { client })) = query else {
        return ErrorCode::BadRequest.into_response();
    };
    // An expired capability reads as absent, so the polling CLI sends the
    // person through the gate again instead of latching onto a token it can
    // never redeem.
    match state.grants.get(client, now) {
        Some(capability) => Json(CapabilityResponse {
            token: capability.token,
        })
        .into_response(),
        None => StatusCode::NO_CONTENT.into_response(),
    }
}
