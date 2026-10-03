//! The wire contracts of the sponsord daemon and the sponsord onramp, defined
//! once: request and response bodies, error codes, route paths, and (behind
//! the `client` feature) typed HTTP clients for both.
//!
//! - [`daemon`]: the daemon's bearer-token API. A gate of your own calls
//!   `POST /v1/capabilities` once its check passes ([`client::DaemonClient`]).
//! - [`onramp`]: the onramp's public JSON API, which `decdn-sponsored` calls
//!   ([`client::OnrampClient`]).
//! - [`error`]: the `{"error": "<code>"}` body both servers answer with.
//!
//! The `openapi` feature adds the OpenAPI documents (`openapi::daemon()`,
//! `openapi::onramp()`), committed under `docs/openapi/`.

pub mod daemon;
pub mod error;
pub mod money;
pub mod onramp;
pub mod time;

#[cfg(feature = "client")]
pub mod client;
#[cfg(feature = "openapi")]
pub mod openapi;
#[cfg(feature = "secret")]
pub mod secret;

pub use error::{ErrorBody, ErrorCode};
pub use money::MicroUsdc;

use serde::{Deserialize, Serialize};

/// A signed capability: the `dcap1:` token and the terms it carries.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct IssuedCapability {
    /// The `dcap1:` token handed to `decdn --capability-file`.
    #[cfg_attr(feature = "openapi", schema(example = "dcap1:..."))]
    pub token: String,
    /// Most the signer may spend against the pool under this capability.
    pub spending_cap: MicroUsdc,
    /// Unix time (seconds) after which the capability is void.
    pub expiry: u64,
}
