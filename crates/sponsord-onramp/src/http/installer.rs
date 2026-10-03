//! `GET /decdn.sh` and `GET /decdn.ps1`: the templated installer scripts for
//! macOS/Linux and Windows. Each embeds its `assets/` file at compile time
//! (`include_str!`) and substitutes the `{{...}}` placeholders with values
//! from `OnrampConfig` and the daemon's `/v1/info`, so end users never set an
//! env var themselves: the contract addresses and RPC URL are baked in
//! server-side.

use axum::extract::State;
use axum::http::{HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};

use crate::config::OnrampConfig;
use crate::state::AppState;
use sponsord_api::daemon::Info;

/// The POSIX installer script, embedded at compile time.
const DECDN_SH_TEMPLATE: &str = include_str!("../../assets/decdn.sh");

/// The PowerShell installer script, embedded at compile time.
const DECDN_PS1_TEMPLATE: &str = include_str!("../../assets/decdn.ps1");

/// Substitute config and daemon values for the `{{ONRAMP_URL}}`,
/// `{{RPC_URL}}`, `{{PAYMENT_POOL}}`, `{{CAPACITY_BOND}}`, `{{CHAIN_ID}}`,
/// and pinned release placeholders (`{{DECDN_RELEASE}}`,
/// `{{DECDN_SUMS_SHA256}}`, `{{CLI_RELEASE}}`, `{{CLI_SUMS_SHA256}}`).
/// The chain id and `PaymentPool` address come from the daemon, the
/// authority for the values its capabilities are signed against.
fn render(template: &str, cfg: &OnrampConfig, chain: &Info) -> String {
    template
        .replace("{{DECDN_RELEASE}}", &cfg.decdn_release.tag)
        .replace("{{DECDN_SUMS_SHA256}}", &cfg.decdn_release.sums_sha256)
        .replace("{{CLI_RELEASE}}", &cfg.cli_release.tag)
        .replace("{{CLI_SUMS_SHA256}}", &cfg.cli_release.sums_sha256)
        .replace("{{ONRAMP_URL}}", &cfg.public_url)
        .replace("{{RPC_URL}}", &cfg.rpc_url)
        .replace("{{PAYMENT_POOL}}", &chain.payment_pool.to_string())
        .replace("{{CAPACITY_BOND}}", &cfg.capacity_bond.to_string())
        .replace("{{CHAIN_ID}}", &chain.chain_id.to_string())
}

fn script(body: String, content_type: &'static str) -> Response {
    let mut resp = (StatusCode::OK, body).into_response();
    resp.headers_mut()
        .insert(header::CONTENT_TYPE, HeaderValue::from_static(content_type));
    resp
}

pub async fn sh(State(state): State<AppState>) -> Response {
    script(
        render(DECDN_SH_TEMPLATE, &state.cfg, &state.chain),
        "text/x-shellscript; charset=utf-8",
    )
}

pub async fn ps1(State(state): State<AppState>) -> Response {
    script(
        render(DECDN_PS1_TEMPLATE, &state.cfg, &state.chain),
        "text/plain; charset=utf-8",
    )
}
