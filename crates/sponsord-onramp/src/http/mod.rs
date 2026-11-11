//! HTTP surface: `/healthz`, `/decdn.sh` + `/decdn.ps1`, `/fund` (gate
//! page), `/v1/fund` (issue), `/v1/capability` (poll for the issued token).

pub mod capability;
pub mod fund;

use axum::extract::State;
use axum::http::{HeaderValue, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use sponsord_api::daemon::Health;
use sponsord_api::onramp::routes;

use crate::state::AppState;

/// The onramp's routes over `state`. Every response forbids framing:
/// `Content-Security-Policy: frame-ancestors 'none'` and
/// `X-Frame-Options: DENY`.
pub fn router(state: AppState) -> Router {
    Router::new()
        .route(routes::HEALTHZ, get(healthz))
        .route(routes::INSTALL_SH, get(install_sh))
        .route(routes::INSTALL_PS1, get(install_ps1))
        .route(routes::FUND_PAGE, get(fund::page))
        .route(routes::FUND, post(fund::submit))
        .route(routes::CAPABILITY, get(capability::get))
        .layer(axum::middleware::map_response(deny_framing))
        .with_state(state)
}

/// Forbid every site from framing `resp`: `Content-Security-Policy:
/// frame-ancestors 'none'`, and `X-Frame-Options: DENY` for browsers without
/// CSP. A framed gate page would let another site's visitors solve the gate
/// for keys that site chose.
async fn deny_framing(mut resp: Response) -> Response {
    let headers = resp.headers_mut();
    headers.insert(
        header::CONTENT_SECURITY_POLICY,
        HeaderValue::from_static("frame-ancestors 'none'"),
    );
    headers.insert(header::X_FRAME_OPTIONS, HeaderValue::from_static("DENY"));
    resp
}

async fn healthz() -> Json<Health> {
    Json(Health { ok: true })
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
