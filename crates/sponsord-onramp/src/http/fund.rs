//! `GET /fund` (the gate page) and `POST /v1/fund` (pass the gate, then get
//! a capability for the client's signer from the daemon; idempotent per
//! signer while the held capability is valid).

use axum::Json;
use axum::extract::rejection::{JsonRejection, QueryRejection};
use axum::extract::{Query, State};
use axum::response::{Html, IntoResponse, Response};
use sponsord_api::client::DaemonError;
use sponsord_api::daemon::IssueRequest;
use sponsord_api::onramp::{CapabilityResponse, ClientQuery, FundRequest};
use sponsord_api::{ErrorBody, ErrorCode};

use crate::net::ClientIp;
use crate::state::AppState;

/// `GET /fund`: the gate page for `client`, or `400` without a valid
/// `client` query.
pub async fn page(
    State(state): State<AppState>,
    query: Result<Query<ClientQuery>, QueryRejection>,
) -> Response {
    let Ok(Query(ClientQuery { client })) = query else {
        return ErrorCode::BadRequest.into_response();
    };
    Html(state.gate.page(client)).into_response()
}

/// `POST /v1/fund`: check the gate's proof, then hand back the capability
/// held for the client or one newly issued by the daemon, and hold it for
/// the polling CLI.
pub async fn submit(
    State(state): State<AppState>,
    ClientIp(ip): ClientIp,
    body: Result<Json<FundRequest>, JsonRejection>,
) -> Result<Json<CapabilityResponse>, ErrorBody> {
    let now = state.clock.now_unix();
    if !state.fund_limit.allow(ip, now) {
        return Err(ErrorCode::RateLimited.into());
    }
    let Ok(Json(req)) = body else {
        return Err(ErrorCode::BadRequest.into());
    };
    let client = req.client;

    match state.gate.verify(client, &req.proof, ip).await {
        Ok(true) => {}
        Ok(false) => return Err(ErrorCode::GateFailed.into()),
        Err(e) => {
            tracing::warn!("gate check failed: {e:#}");
            return Err(ErrorCode::Internal.into());
        }
    }

    // Idempotent: a signer that already holds a still-valid capability gets
    // it back without a daemon call. Otherwise the daemon hands back a
    // registered signer's existing capability or signs a fresh one.
    //
    // Two concurrent requests for the same signer can both miss here and
    // both call the daemon. That is benign: each token is validly signed for
    // the same signer and pool, the later `put` wins, and the CLI reads back
    // whatever `GET /v1/capability` returns.
    if let Some(held) = state.grants.get(client, now) {
        return Ok(Json(CapabilityResponse { token: held.token }));
    }

    let issue = IssueRequest {
        signer: client,
        spending_cap: state.terms.spending_cap,
        ttl_secs: state.terms.ttl_secs,
    };
    let issued = match state.daemon.issue(&issue).await {
        Ok(r) => r.capability,
        Err(DaemonError::SignerExpired { .. }) => return Err(ErrorCode::SignerExpired.into()),
        Err(DaemonError::Unavailable(e)) => {
            tracing::warn!("daemon unavailable: {e}");
            return Err(ErrorCode::Upstream.into());
        }
        Err(e @ DaemonError::Rejected { .. }) => {
            tracing::error!("{e}; check ONRAMP_DAEMON_TOKEN and the configured terms");
            return Err(ErrorCode::Internal.into());
        }
    };
    let token = issued.token.clone();
    state.grants.put(client, issued, now);
    Ok(Json(CapabilityResponse { token }))
}
