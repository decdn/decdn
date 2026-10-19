//! The error body both servers answer with: `{"error": "<code>"}`, plus the
//! fields some codes carry.

use serde::{Deserialize, Serialize};

use crate::MicroUsdc;

/// Every error code either server returns, with its HTTP status
/// ([`ErrorCode::http_status`]).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(rename_all = "snake_case")]
pub enum ErrorCode {
    /// 400: malformed body, query, or address.
    BadRequest,
    /// 400: a requested cap or TTL above the daemon's maximum; the body adds
    /// `max_spending_cap` and `max_ttl_secs`.
    ExceedsMax,
    /// 400: a requested cap or TTL of 0.
    Zero,
    /// 401: missing or wrong bearer token (daemon).
    Unauthorized,
    /// 403: the gate refused the request (onramp).
    GateFailed,
    /// 409: the signer is registered on-chain and its registration has
    /// expired; only a new signer can get a usable capability. The daemon's
    /// body adds `expiry`.
    SignerExpired,
    /// 429: too many requests from this client (onramp).
    RateLimited,
    /// 500: a server-side failure.
    Internal,
    /// 502: the daemon behind the onramp is unreachable or failing.
    Upstream,
    /// 503: the signer's on-chain registration could not be read (daemon).
    ChainUnavailable,
    /// A code this version does not know.
    #[serde(other)]
    Unknown,
}

impl ErrorCode {
    /// The HTTP status this code is sent with.
    #[must_use]
    pub const fn http_status(self) -> u16 {
        match self {
            Self::BadRequest | Self::ExceedsMax | Self::Zero => 400,
            Self::Unauthorized => 401,
            Self::GateFailed => 403,
            Self::SignerExpired => 409,
            Self::RateLimited => 429,
            Self::Internal | Self::Unknown => 500,
            Self::Upstream => 502,
            Self::ChainUnavailable => 503,
        }
    }

    /// The wire spelling, e.g. `"signer_expired"`.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::BadRequest => "bad_request",
            Self::ExceedsMax => "exceeds_max",
            Self::Zero => "zero",
            Self::Unauthorized => "unauthorized",
            Self::GateFailed => "gate_failed",
            Self::SignerExpired => "signer_expired",
            Self::RateLimited => "rate_limited",
            Self::Internal => "internal",
            Self::Upstream => "upstream",
            Self::ChainUnavailable => "chain_unavailable",
            Self::Unknown => "unknown",
        }
    }
}

impl std::fmt::Display for ErrorCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// An error response body.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct ErrorBody {
    /// What went wrong, e.g. `"signer_expired"`.
    pub error: ErrorCode,
    /// With `exceeds_max`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_spending_cap: Option<MicroUsdc>,
    /// With `exceeds_max`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_ttl_secs: Option<u64>,
    /// With `signer_expired` (daemon): when the registration expired.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expiry: Option<u64>,
}

impl ErrorBody {
    /// A body with `error` and none of the optional fields.
    #[must_use]
    pub const fn new(error: ErrorCode) -> Self {
        Self {
            error,
            max_spending_cap: None,
            max_ttl_secs: None,
            expiry: None,
        }
    }
}

impl From<ErrorCode> for ErrorBody {
    fn from(code: ErrorCode) -> Self {
        Self::new(code)
    }
}

#[cfg(feature = "axum")]
impl axum::response::IntoResponse for ErrorBody {
    fn into_response(self) -> axum::response::Response {
        let status = axum::http::StatusCode::from_u16(self.error.http_status())
            .unwrap_or(axum::http::StatusCode::INTERNAL_SERVER_ERROR);
        (status, axum::Json(self)).into_response()
    }
}

#[cfg(feature = "axum")]
impl axum::response::IntoResponse for ErrorCode {
    fn into_response(self) -> axum::response::Response {
        ErrorBody::new(self).into_response()
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests;
