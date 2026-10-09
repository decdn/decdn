//! The sponsord daemon's HTTP API. Every `/v1` route needs
//! `Authorization: Bearer <SPONSORD_API_TOKEN>`; `/healthz` and `/metrics`
//! need none.

use alloy_primitives::Address;
use serde::{Deserialize, Serialize};

use crate::{IssuedCapability, MicroUsdc};

/// Route paths.
pub mod routes {
    /// `POST`: [`IssueRequest`](super::IssueRequest) →
    /// [`IssueResponse`](super::IssueResponse).
    pub const CAPABILITIES: &str = "/v1/capabilities";
    /// `GET` → [`Info`](super::Info).
    pub const INFO: &str = "/v1/info";
    /// `GET` → [`Health`](crate::daemon::Health), no token.
    pub const HEALTHZ: &str = "/healthz";
    /// `GET` → Prometheus text exposition, no token.
    pub const METRICS: &str = "/metrics";
}

/// `POST /v1/capabilities`: a capability for `signer`. An omitted term
/// defaults to the daemon's maximum.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct IssueRequest {
    /// The delegate key the capability authorizes to sign vouchers.
    #[cfg_attr(feature = "openapi", schema(value_type = String, example = "0x00000000000000000000000000000000000000aa"))]
    pub signer: Address,
    /// Most the signer may spend, in micro-USDC. Omitted: the daemon's
    /// maximum. 0 is refused (`zero`), above the maximum `exceeds_max`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub spending_cap: Option<MicroUsdc>,
    /// Seconds from now until the capability expires. Omitted: the daemon's
    /// maximum. 0 is refused (`zero`), above the maximum `exceeds_max`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ttl_secs: Option<u64>,
}

impl IssueRequest {
    /// A request for `signer` at the daemon's maximum terms.
    #[must_use]
    pub const fn new(signer: Address) -> Self {
        Self {
            signer,
            spending_cap: None,
            ttl_secs: None,
        }
    }
}

/// `POST /v1/capabilities` success. For a signer already registered on-chain
/// the capability carries its registered terms (`registered: true`),
/// whatever terms were requested.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct IssueResponse {
    /// The token and its terms, flattened into this object.
    #[serde(flatten)]
    pub capability: IssuedCapability,
    /// Whether the terms come from an existing on-chain registration.
    pub registered: bool,
}

/// `GET /v1/info`: the chain and pool this daemon signs for, and the
/// largest terms it grants.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct Info {
    /// EIP-155 id of the chain the pool is on.
    pub chain_id: u64,
    /// The `PaymentPool` contract.
    #[cfg_attr(feature = "openapi", schema(value_type = String))]
    pub payment_pool: Address,
    /// Largest `spending_cap` the daemon grants, in micro-USDC; also the
    /// default when a request omits it.
    pub max_spending_cap: MicroUsdc,
    /// Largest `ttl_secs` the daemon grants; also the default when a request
    /// omits it.
    pub max_ttl_secs: u64,
}

/// `GET /healthz`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct Health {
    /// Always `true`: the server is up and answering.
    pub ok: bool,
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests;
