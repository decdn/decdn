//! `GET /decdn.sh` and `GET /decdn.ps1`: the templated installer scripts for
//! macOS/Linux and Windows. Each embeds its `assets/` file at compile time
//! (`include_str!`) and substitutes the `{{...}}` placeholders with values
//! from `ServerConfig`, so end users never set an env var themselves — the
//! contract addresses and RPC URL are baked in server-side.

use axum::extract::State;
use axum::http::{HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};

use crate::config::ServerConfig;
use crate::state::AppState;

/// The POSIX installer script, embedded at compile time.
const DECDN_SH_TEMPLATE: &str = include_str!("../../assets/decdn.sh");

/// The PowerShell installer script, embedded at compile time.
const DECDN_PS1_TEMPLATE: &str = include_str!("../../assets/decdn.ps1");

/// Substitute `cfg`'s values for the `{{GATEWAY_BASE}}`, `{{RPC_URL}}`,
/// `{{PAYMENT_POOL}}`, `{{CAPACITY_BOND}}`, and `{{CHAIN_ID}}` placeholders.
fn render(template: &str, cfg: &ServerConfig) -> String {
    template
        .replace("{{GATEWAY_BASE}}", &cfg.public_url)
        .replace("{{RPC_URL}}", &cfg.rpc_url)
        .replace("{{PAYMENT_POOL}}", &cfg.payment_pool.to_string())
        .replace("{{CAPACITY_BOND}}", &cfg.capacity_bond.to_string())
        .replace("{{CHAIN_ID}}", &cfg.chain_id.to_string())
}

fn script(body: String, content_type: &'static str) -> Response {
    let mut resp = (StatusCode::OK, body).into_response();
    resp.headers_mut()
        .insert(header::CONTENT_TYPE, HeaderValue::from_static(content_type));
    resp
}

pub async fn sh(State(state): State<AppState>) -> Response {
    script(
        render(DECDN_SH_TEMPLATE, &state.cfg),
        "text/x-shellscript; charset=utf-8",
    )
}

pub async fn ps1(State(state): State<AppState>) -> Response {
    script(
        render(DECDN_PS1_TEMPLATE, &state.cfg),
        "text/plain; charset=utf-8",
    )
}
