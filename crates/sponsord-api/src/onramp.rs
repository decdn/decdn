//! The sponsord onramp's public HTTP API: what `decdn-sponsored` and the
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
    /// or `204` while none is issued; the CLI polls it.
    pub const CAPABILITY: &str = "/v1/capability";
    /// `GET` → [`Profile`](super::Profile): what the CLI needs to run `decdn`.
    pub const PROFILE: &str = "/v1/profile";
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
    #[cfg_attr(feature = "openapi", schema(value_type = String))]
    pub client: Address,
    pub proof: String,
}

/// The capability issued to a client.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct CapabilityResponse {
    /// The `dcap1:` token.
    pub token: String,
}

/// `GET /v1/profile`: the chain and contracts the CLI hands to `decdn`. The
/// CLI reads it on every run, so the onramp can change any of it without
/// users reinstalling.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct Profile {
    pub chain_id: u64,
    /// JSON-RPC endpoint `decdn` reads the chain through.
    pub rpc_url: String,
    /// The `PaymentPool` the capabilities are signed against.
    #[cfg_attr(feature = "openapi", schema(value_type = String))]
    pub payment_pool: Address,
    /// The `CapacityBond`, for node discovery.
    #[cfg_attr(feature = "openapi", schema(value_type = String))]
    pub capacity_bond: Address,
    /// The `SlashJudge`, when the onramp names one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "openapi", schema(value_type = Option<String>))]
    pub slash_judge: Option<Address>,
    /// Oldest `decdn-sponsored` this onramp works with; an older CLI tells
    /// the user to re-run the installer.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "openapi", schema(value_type = Option<String>, example = "0.2.0"))]
    pub min_cli_version: Option<semver::Version>,
}
