//! `OpenAPI` 3.1 documents for both servers, built from the types in this
//! crate. The committed copies under `docs/openapi/` are checked against
//! these by `tests/openapi.rs`; regenerate them with
//! `UPDATE_OPENAPI=1 cargo test -p sponsord-api --test openapi`.
//!
//! The functions below exist only to carry each route's documentation.

#![allow(dead_code)]
#![expect(
    clippy::missing_const_for_fn,
    reason = "the route functions are empty; only `#[utoipa::path]` reads them"
)]

use utoipa::OpenApi;

use crate::IssuedCapability;
use crate::daemon::{Health, Info, IssueRequest, IssueResponse};
use crate::error::{ErrorBody, ErrorCode};
use crate::money::MicroUsdc;
use crate::onramp::{CapabilityResponse, ClientQuery, FundRequest, Profile};

#[derive(OpenApi)]
#[openapi(
    info(
        title = "sponsord",
        description = "The sponsord daemon: signs capped, expiring capabilities against the \
                       sponsor's PaymentPool for callers holding its bearer token.",
        license(name = "MIT OR Apache-2.0"),
    ),
    paths(issue, info, daemon_healthz, metrics),
    components(schemas(
        IssueRequest, IssueResponse, IssuedCapability, Info, Health, ErrorBody, ErrorCode,
        MicroUsdc
    )),
    modifiers(&BearerAuth),
)]
struct DaemonDoc;

#[derive(OpenApi)]
#[openapi(
    info(
        title = "sponsord-onramp",
        description = "The sponsord onramp: a gate in front of the daemon, the installers, and \
                       the API decdn-sponsored polls.",
        license(name = "MIT OR Apache-2.0"),
    ),
    paths(
        fund_page,
        fund,
        capability,
        profile,
        install_sh,
        install_ps1,
        onramp_healthz
    ),
    components(schemas(
        FundRequest,
        CapabilityResponse,
        Profile,
        Health,
        ErrorBody,
        ErrorCode,
        MicroUsdc
    ))
)]
struct OnrampDoc;

/// The daemon's `OpenAPI` document.
#[must_use]
pub fn daemon() -> utoipa::openapi::OpenApi {
    DaemonDoc::openapi()
}

/// The onramp's `OpenAPI` document.
#[must_use]
pub fn onramp() -> utoipa::openapi::OpenApi {
    OnrampDoc::openapi()
}

struct BearerAuth;

impl utoipa::Modify for BearerAuth {
    fn modify(&self, openapi: &mut utoipa::openapi::OpenApi) {
        use utoipa::openapi::security::{HttpAuthScheme, HttpBuilder, SecurityScheme};
        if let Some(components) = openapi.components.as_mut() {
            components.add_security_scheme(
                "bearer",
                SecurityScheme::Http(HttpBuilder::new().scheme(HttpAuthScheme::Bearer).build()),
            );
        }
    }
}

/// Issue a capability for `signer`. A registered, unexpired signer gets its
/// existing capability back whatever terms are asked; an expired one gets
/// `409 signer_expired`.
#[utoipa::path(
    post,
    path = "/v1/capabilities",
    request_body = IssueRequest,
    security(("bearer" = [])),
    responses(
        (status = 200, description = "The signed capability", body = IssueResponse),
        (status = 400, description = "`bad_request`, `exceeds_max` or `zero`", body = ErrorBody),
        (status = 401, description = "`unauthorized`", body = ErrorBody),
        (status = 409, description = "`signer_expired`", body = ErrorBody),
        (status = 500, description = "`internal`", body = ErrorBody),
        (status = 503, description = "`chain_unavailable`", body = ErrorBody),
    ),
)]
fn issue() {}

/// The chain and pool this daemon signs for, and its maximum terms.
#[utoipa::path(
    get,
    path = "/v1/info",
    security(("bearer" = [])),
    responses(
        (status = 200, description = "Chain, pool and maximum terms", body = Info),
        (status = 401, description = "`unauthorized`", body = ErrorBody),
    ),
)]
fn info() {}

/// Liveness.
#[utoipa::path(get, path = "/healthz", responses((status = 200, description = "Up", body = Health)))]
fn daemon_healthz() {}

/// Prometheus metrics (text exposition format).
#[utoipa::path(
    get,
    path = "/metrics",
    responses((status = 200, description = "Prometheus metrics", content_type = "text/plain; version=0.0.4", body = String)),
)]
fn metrics() {}

/// The gate page for `client` (HTML). It calls `POST /v1/fund` once the
/// gate's check passes.
#[utoipa::path(
    get,
    path = "/fund",
    params(ClientQuery),
    responses(
        (status = 200, description = "The gate page", content_type = "text/html", body = String),
        (status = 400, description = "`bad_request`", body = ErrorBody),
    ),
)]
fn fund_page() {}

/// Get a capability for `client`, given the gate's proof. Idempotent per
/// client while its capability is valid.
#[utoipa::path(
    post,
    path = "/v1/fund",
    request_body = FundRequest,
    responses(
        (status = 200, description = "The capability", body = CapabilityResponse),
        (status = 400, description = "`bad_request`", body = ErrorBody),
        (status = 403, description = "`gate_failed`", body = ErrorBody),
        (status = 409, description = "`signer_expired`: use a new key", body = ErrorBody),
        (status = 429, description = "`rate_limited`", body = ErrorBody),
        (status = 500, description = "`internal`", body = ErrorBody),
        (status = 502, description = "`upstream`", body = ErrorBody),
    ),
)]
fn fund() {}

/// The capability issued to `client`, or `204` while there is none.
#[utoipa::path(
    get,
    path = "/v1/capability",
    params(ClientQuery),
    responses(
        (status = 200, description = "The capability", body = CapabilityResponse),
        (status = 204, description = "No capability issued yet"),
        (status = 400, description = "`bad_request`", body = ErrorBody),
        (status = 429, description = "`rate_limited`", body = ErrorBody),
    ),
)]
fn capability() {}

/// What `decdn-sponsored` needs to run `decdn` against this onramp's pool.
#[utoipa::path(get, path = "/v1/profile", responses((status = 200, description = "The download profile", body = Profile)))]
fn profile() {}

/// The macOS/Linux installer: `curl -fsSL <onramp>/decdn.sh | sh`.
#[utoipa::path(
    get,
    path = "/decdn.sh",
    responses((status = 200, description = "The installer script", content_type = "text/x-shellscript", body = String)),
)]
fn install_sh() {}

/// The Windows installer: `irm <onramp>/decdn.ps1 | iex`.
#[utoipa::path(
    get,
    path = "/decdn.ps1",
    responses((status = 200, description = "The installer script", content_type = "text/plain", body = String)),
)]
fn install_ps1() {}

/// Liveness.
#[utoipa::path(get, path = "/healthz", responses((status = 200, description = "Up", body = Health)))]
fn onramp_healthz() {}
