//! `GET /fund` (the gate page) and `POST /v1/fund` (get a capability for the
//! caller's signer from the daemon, idempotent per signer while the stored
//! grant is valid).

use axum::Json;
use axum::extract::rejection::{JsonRejection, QueryRejection};
use axum::extract::{Query, State};
use axum::response::{Html, IntoResponse, Response};
use sponsord_api::client::DaemonError;
use sponsord_api::daemon::IssueRequest;
use sponsord_api::onramp::{CapabilityResponse, ClientQuery, FundRequest};
use sponsord_api::{ErrorBody, ErrorCode};

use crate::state::AppState;
use crate::store::GrantRecord;

const FUND_PAGE_TEMPLATE: &str = include_str!("../../assets/fund.html");

pub async fn page(
    State(state): State<AppState>,
    query: Result<Query<ClientQuery>, QueryRejection>,
) -> Response {
    let Ok(Query(ClientQuery { client })) = query else {
        return ErrorCode::BadRequest.into_response();
    };
    // `client` is a parsed address, so its text is hex: it can't carry HTML
    // metacharacters.
    let html = FUND_PAGE_TEMPLATE
        .replace("{{SITEKEY}}", &state.cfg.turnstile_sitekey)
        .replace("{{CLIENT}}", &client.to_string());
    Html(html).into_response()
}

pub async fn submit(
    State(state): State<AppState>,
    body: Result<Json<FundRequest>, JsonRejection>,
) -> Result<Json<CapabilityResponse>, ErrorBody> {
    let Ok(Json(req)) = body else {
        return Err(ErrorCode::BadRequest.into());
    };
    let client = req.client;

    match state.turnstile.verify(&req.proof, None).await {
        Ok(true) => {}
        Ok(false) => return Err(ErrorCode::GateFailed.into()),
        Err(_) => return Err(ErrorCode::Internal.into()),
    }

    let now = state.clock.now_unix();

    // Idempotent: a signer that already holds a still-valid capability gets
    // the stored token back without a daemon call. An expired or missing
    // grant goes to the daemon, which hands back a registered signer's
    // existing capability or signs a fresh one.
    //
    // Two concurrent `POST /v1/fund` for the same signer can both miss this
    // lookup and both call the daemon. That is benign: each token is validly
    // signed for the same signer and pool, `put_grant` is last-writer-wins,
    // and the client reads back whatever `GET /v1/capability` returns.
    match state.store.get_grant(client) {
        Ok(Some(rec)) if now < rec.expiry => {
            return Ok(Json(CapabilityResponse { token: rec.token }));
        }
        Ok(_) => {}
        Err(_) => return Err(ErrorCode::Internal.into()),
    }

    let issue = IssueRequest {
        signer: client,
        spending_cap: state.cfg.spending_cap,
        ttl_secs: state.cfg.ttl_secs,
    };
    let issued = match state.source.issue(&issue).await {
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
    let rec = GrantRecord {
        spending_cap: issued.spending_cap.0,
        expiry: issued.expiry,
        issued_unix: now,
        token: issued.token.clone(),
    };
    if state.store.put_grant(client, &rec).is_err() {
        return Err(ErrorCode::Internal.into());
    }
    Ok(Json(CapabilityResponse {
        token: issued.token,
    }))
}
