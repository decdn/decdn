//! HTTP surface: `/healthz`, `/decdn.sh` + `/decdn.ps1`, `/fund` (gate
//! page), `/v1/fund` (issue), `/v1/capability` (poll for the issued token),
//! `/v1/profile` (what the CLI runs `decdn` with).

pub mod capability;
pub mod fund;

use axum::extract::State;
use axum::http::{HeaderValue, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use sponsord_api::daemon::Health;
use sponsord_api::onramp::{Profile, routes};

use crate::state::AppState;

pub fn router(state: AppState) -> Router {
    Router::new()
        .route(routes::HEALTHZ, get(healthz))
        .route(routes::INSTALL_SH, get(install_sh))
        .route(routes::INSTALL_PS1, get(install_ps1))
        .route(routes::PROFILE, get(profile))
        .route(routes::FUND_PAGE, get(fund::page))
        .route(routes::FUND, post(fund::submit))
        .route(routes::CAPABILITY, get(capability::get))
        .with_state(state)
}

async fn healthz() -> Json<Health> {
    Json(Health { ok: true })
}

async fn profile(State(state): State<AppState>) -> Json<Profile> {
    Json(Profile::clone(&state.profile))
}

fn script(body: &str, content_type: &'static str) -> Response {
    let mut resp = body.to_owned().into_response();
    resp.headers_mut()
        .insert(header::CONTENT_TYPE, HeaderValue::from_static(content_type));
    resp
}

async fn install_sh(State(state): State<AppState>) -> Response {
    script(&state.installers.sh, "text/x-shellscript; charset=utf-8")
}

async fn install_ps1(State(state): State<AppState>) -> Response {
    script(&state.installers.ps1, "text/plain; charset=utf-8")
}
