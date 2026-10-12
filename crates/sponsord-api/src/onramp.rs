//! The sponsord onramp's public HTTP API: what the installer scripts and the
//! gate page in the browser call. No route needs a token.

use alloy_primitives::Address;
use serde::{Deserialize, Serialize};

/// Route paths.
pub mod routes {
    /// `GET ?client=0x..`: the gate page a person opens in the browser (HTML).
    pub const FUND_PAGE: &str = "/fund";
    /// `POST`: [`FundRequest`](super::FundRequest) →
    /// [`CapabilityResponse`](super::CapabilityResponse); the gate page calls
    /// it once its check passes.
    pub const FUND: &str = "/v1/fund";
    /// `GET ?client=0x..` → [`CapabilityResponse`](super::CapabilityResponse),
    /// or `204` while none is issued; the installer polls it.
    pub const CAPABILITY: &str = "/v1/capability";
    /// `GET`: the macOS/Linux installer (`sh`).
    pub const INSTALL_SH: &str = "/decdn.sh";
    /// `GET`: the Windows installer (PowerShell).
    pub const INSTALL_PS1: &str = "/decdn.ps1";
    /// `GET` → [`Health`](crate::daemon::Health).
    pub const HEALTHZ: &str = "/healthz";
}

/// The `?client=0x..` query of `GET /fund` and `GET /v1/capability`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::IntoParams))]
#[cfg_attr(feature = "openapi", into_params(parameter_in = Query))]
pub struct ClientQuery {
    /// The download key's address.
    #[cfg_attr(feature = "openapi", param(value_type = String))]
    pub client: Address,
}

/// `POST /v1/fund`: ask for a capability for `client`, with the gate's proof
/// that the request may have one (for Turnstile, the widget's token).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct FundRequest {
    /// The download key's address, as in the gate page's `?client=`.
    #[cfg_attr(feature = "openapi", schema(value_type = String))]
    pub client: Address,
    /// The gate's proof; for Turnstile, the widget's response token.
    pub proof: String,
}

/// The capability issued to a client.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct CapabilityResponse {
    /// The `dcap1:` token.
    pub token: String,
    /// Unix time (seconds) after which the capability is void.
    pub expiry: u64,
}
