//! Typed HTTP clients for the daemon ([`DaemonClient`]) and the onramp
//! ([`OnrampClient`]). Both take a caller-built `reqwest::Client`, so the
//! caller sets timeouts and the `User-Agent`.

use std::time::Duration;

use alloy_primitives::Address;

use crate::daemon::{self, Info, IssueRequest, IssueResponse};
use crate::error::{ErrorBody, ErrorCode};
use crate::onramp::{self, CapabilityResponse, Profile};
use crate::secret::Secret;

/// Why a daemon call failed.
#[derive(Debug, thiserror::Error)]
pub enum DaemonError {
    /// The signer's on-chain registration expired at `expiry`; only a new
    /// signer can get a usable capability.
    #[error("signer registration expired at {expiry}")]
    SignerExpired {
        /// Unix time (seconds) the registration expired at; 0 if the daemon
        /// left it out.
        expiry: u64,
    },
    /// The daemon rejected the request itself (`4xx`): a wrong token, or
    /// terms outside its maximums. Retrying will not help.
    #[error("daemon rejected the request: {status} {code}")]
    Rejected {
        /// The HTTP status, `400`–`499`.
        status: u16,
        /// The body's error code, or [`ErrorCode::Unknown`] if it had none.
        code: ErrorCode,
    },
    /// The daemon could not be reached, timed out, or failed on its side.
    #[error("daemon unavailable: {0}")]
    Unavailable(String),
}

/// Client for the daemon's bearer-token API. The token is held as a
/// [`Secret`]: `Debug` leaves it out, and it is wiped on drop.
#[derive(Clone)]
pub struct DaemonClient {
    base: String,
    token: Secret,
    http: reqwest::Client,
}

impl std::fmt::Debug for DaemonClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DaemonClient")
            .field("base", &self.base)
            .field("token", &"<redacted>")
            .finish_non_exhaustive()
    }
}

impl DaemonClient {
    /// `base` is the daemon's base URL (a trailing `/` is fine); `token` its
    /// `SPONSORD_API_TOKEN`.
    #[must_use]
    pub fn new(base: &str, token: Secret, http: reqwest::Client) -> Self {
        Self {
            base: base.trim_end_matches('/').to_owned(),
            token,
            http,
        }
    }

    /// The daemon's base URL, without a trailing `/`.
    #[must_use]
    pub fn base(&self) -> &str {
        &self.base
    }

    /// `POST /v1/capabilities`.
    ///
    /// # Errors
    ///
    /// See [`DaemonError`].
    pub async fn issue(&self, req: &IssueRequest) -> Result<IssueResponse, DaemonError> {
        let resp = self
            .http
            .post(format!("{}{}", self.base, daemon::routes::CAPABILITIES))
            .bearer_auth(self.token.expose())
            .json(req)
            .send()
            .await
            .map_err(unavailable)?;
        parse(resp).await
    }

    /// `GET /v1/info`.
    ///
    /// # Errors
    ///
    /// See [`DaemonError`].
    pub async fn info(&self) -> Result<Info, DaemonError> {
        let resp = self
            .http
            .get(format!("{}{}", self.base, daemon::routes::INFO))
            .bearer_auth(self.token.expose())
            .send()
            .await
            .map_err(unavailable)?;
        parse(resp).await
    }
}

fn unavailable(e: reqwest::Error) -> DaemonError {
    DaemonError::Unavailable(e.without_url().to_string())
}

async fn parse<T: serde::de::DeserializeOwned>(resp: reqwest::Response) -> Result<T, DaemonError> {
    let status = resp.status();
    if status.is_success() {
        return resp.json().await.map_err(unavailable);
    }
    let body: Option<ErrorBody> = resp.json().await.ok();
    let code = body.map_or(ErrorCode::Unknown, |b| b.error);
    match code {
        // The daemon sends `signer_expired` only with 409; the same code under
        // another status is a daemon fault, not a verdict on the signer.
        ErrorCode::SignerExpired if status == reqwest::StatusCode::CONFLICT => {
            Err(DaemonError::SignerExpired {
                expiry: body.and_then(|b| b.expiry).unwrap_or(0),
            })
        }
        _ if status.is_client_error() => Err(DaemonError::Rejected {
            status: status.as_u16(),
            code,
        }),
        _ => Err(DaemonError::Unavailable(format!("{status} {code}"))),
    }
}

/// Why an onramp call failed.
#[derive(Debug, thiserror::Error)]
pub enum OnrampError {
    /// The request could not be sent, or a `200` body did not parse.
    #[error("{0}")]
    Http(#[from] reqwest::Error),
    /// The onramp answered with an unexpected status.
    #[error("{route} failed: {status} {code}")]
    Status {
        /// The route path called, e.g. `/v1/profile`.
        route: &'static str,
        /// The HTTP status the onramp answered with.
        status: u16,
        /// The body's error code, or [`ErrorCode::Unknown`] if it had none.
        code: ErrorCode,
    },
    /// [`OnrampClient::poll_capability`] saw no capability within its timeout.
    #[error("timed out after {0:?} waiting for a capability")]
    Timeout(Duration),
}

/// Client for the onramp's public API.
#[derive(Clone, Debug)]
pub struct OnrampClient {
    base: String,
    http: reqwest::Client,
}

impl OnrampClient {
    /// `base` is the onramp's public base URL (a trailing `/` is fine).
    #[must_use]
    pub fn new(base: &str, http: reqwest::Client) -> Self {
        Self {
            base: base.trim_end_matches('/').to_owned(),
            http,
        }
    }

    /// `GET /v1/profile`.
    ///
    /// # Errors
    ///
    /// The request fails, or the onramp answers other than `200`.
    pub async fn profile(&self) -> Result<Profile, OnrampError> {
        let resp = self
            .http
            .get(format!("{}{}", self.base, onramp::routes::PROFILE))
            .send()
            .await?;
        if !resp.status().is_success() {
            return Err(status_error(onramp::routes::PROFILE, resp).await);
        }
        Ok(resp.json().await?)
    }

    /// `GET /v1/capability?client=..`: the token issued to `client`, or
    /// `None` (`204`) while there is none.
    ///
    /// # Errors
    ///
    /// The request fails, or the onramp answers other than `200`/`204`.
    pub async fn capability(&self, client: Address) -> Result<Option<String>, OnrampError> {
        let resp = self
            .http
            .get(format!("{}{}", self.base, onramp::routes::CAPABILITY))
            .query(&[("client", client.to_string())])
            .send()
            .await?;
        match resp.status() {
            reqwest::StatusCode::NO_CONTENT => Ok(None),
            reqwest::StatusCode::OK => Ok(Some(resp.json::<CapabilityResponse>().await?.token)),
            _ => Err(status_error(onramp::routes::CAPABILITY, resp).await),
        }
    }

    /// Poll [`capability`](Self::capability) every `every` until a token
    /// appears or `timeout` elapses. The timeout bounds the whole poll,
    /// including a request in flight.
    ///
    /// # Errors
    ///
    /// A poll fails, or `timeout` elapses with no capability.
    pub async fn poll_capability(
        &self,
        client: Address,
        every: Duration,
        timeout: Duration,
    ) -> Result<String, OnrampError> {
        let poll = async {
            loop {
                if let Some(token) = self.capability(client).await? {
                    return Ok(token);
                }
                tokio::time::sleep(every).await;
            }
        };
        tokio::time::timeout(timeout, poll)
            .await
            .map_err(|_| OnrampError::Timeout(timeout))?
    }

    /// The gate page for `client`, for a person to open in a browser.
    #[must_use]
    pub fn fund_url(&self, client: Address) -> String {
        format!("{}{}?client={client}", self.base, onramp::routes::FUND_PAGE)
    }
}

async fn status_error(route: &'static str, resp: reqwest::Response) -> OnrampError {
    let status = resp.status().as_u16();
    let code = resp
        .json::<ErrorBody>()
        .await
        .map_or(ErrorCode::Unknown, |b| b.error);
    OnrampError::Status {
        route,
        status,
        code,
    }
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]
mod tests;
