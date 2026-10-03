//! HTTP surface: `/healthz`, `/decdn.sh` + `/decdn.ps1`, `/fund` (gate page),
//! `/v1/fund` (issue), `/v1/capability` (poll for the issued token). Handlers
//! live in the sibling `fund`/`capability`/`installer` modules.

pub mod capability;
pub mod fund;
pub mod installer;

use axum::routing::{get, post};
use axum::{Json, Router};
use sponsord_api::daemon::Health;
use sponsord_api::onramp::routes;

use crate::state::AppState;

pub fn router(state: AppState) -> Router {
    Router::new()
        .route(routes::HEALTHZ, get(healthz))
        .route(routes::INSTALL_SH, get(installer::sh))
        .route(routes::INSTALL_PS1, get(installer::ps1))
        .route(routes::FUND_PAGE, get(fund::page))
        .route(routes::FUND, post(fund::submit))
        .route(routes::CAPABILITY, get(capability::get))
        .with_state(state)
}

async fn healthz() -> Json<Health> {
    Json(Health { ok: true })
}
