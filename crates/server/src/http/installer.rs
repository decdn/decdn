//! `GET /decdn.sh`: the templated POSIX installer script. Embeds
//! `assets/decdn.sh` at compile time (`include_str!`) and substitutes the
//! `{{...}}` placeholders with values from `ServerConfig`, so end users never
//! set an env var themselves — the contract addresses and RPC URL are baked
//! in server-side.

use axum::extract::State;
use axum::http::{HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};

use crate::state::AppState;

/// The raw installer script, embedded at compile time.
const DECDN_SH_TEMPLATE: &str = include_str!("../../assets/decdn.sh");

/// Renders the installer script with `state.cfg`'s values substituted for
/// the `{{GATEWAY_BASE}}`, `{{RPC_URL}}`, `{{PAYMENT_CHANNEL}}`,
/// `{{CAPACITY_BOND}}`, and `{{CHAIN_ID}}` placeholders.
pub async fn get(State(state): State<AppState>) -> Response {
    let script = DECDN_SH_TEMPLATE
        .replace("{{GATEWAY_BASE}}", &state.cfg.public_url)
        .replace("{{RPC_URL}}", &state.cfg.rpc_url)
        .replace(
            "{{PAYMENT_CHANNEL}}",
            &state.cfg.payment_channel.to_string(),
        )
        .replace("{{CAPACITY_BOND}}", &state.cfg.capacity_bond.to_string())
        .replace("{{CHAIN_ID}}", &state.cfg.chain_id.to_string());

    let mut resp = (StatusCode::OK, script).into_response();
    resp.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("text/x-shellscript; charset=utf-8"),
    );
    resp
}
