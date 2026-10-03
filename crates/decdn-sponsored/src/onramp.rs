//! Typed HTTP client for the onramp's JSON API.
//!
//! The CLI never calls `POST /fund` itself: a browser does that after
//! the captcha challenge. This client talks to `GET /capability` (poll for
//! a capability token the browser flow caused the server to issue) and
//! builds the `GET /fund` link as a string for the operator to open in a
//! browser. The JSON shape here MUST byte-match
//! `crates/sponsord-onramp/src/http/capability.rs`.

use std::time::Duration;

use alloy::primitives::Address;
use anyhow::bail;
use serde::Deserialize;

/// A capability the onramp has issued to `client`.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct CapabilityInfo {
    pub token: String,
}

/// Client for the onramp's HTTP API.
pub struct OnrampClient {
    pub base: String,
    pub http: reqwest::Client,
}

impl OnrampClient {
    /// Build a client pointed at `base` (no trailing slash expected).
    #[must_use]
    pub fn new(base: String) -> Self {
        Self {
            base,
            http: reqwest::Client::new(),
        }
    }

    /// `GET /capability?client=<0xADDR>`.
    ///
    /// `204 No Content` means no capability has been issued yet and maps to
    /// `Ok(None)`. `200` parses the body into `CapabilityInfo`. Any other
    /// status is an error carrying the response body.
    ///
    /// # Errors
    ///
    /// Returns an error if the request fails to send, the server returns a
    /// non-200/204 status, or a 200 body fails to parse as `CapabilityInfo`.
    pub async fn get_capability(&self, client: Address) -> anyhow::Result<Option<CapabilityInfo>> {
        let url = format!("{}/capability", self.base);
        let resp = self
            .http
            .get(&url)
            .query(&[("client", client.to_string())])
            .send()
            .await?;

        if resp.status() == reqwest::StatusCode::NO_CONTENT {
            return Ok(None);
        }
        if resp.status() != reqwest::StatusCode::OK {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            bail!("GET /capability failed: {status} {body}");
        }
        Ok(Some(resp.json().await?))
    }

    /// Poll `GET /capability` on a fixed backoff until a token appears or
    /// `timeout` elapses.
    ///
    /// # Errors
    ///
    /// Returns an error if any single poll fails, or if `timeout` elapses
    /// without a capability ever appearing.
    pub async fn poll_capability(
        &self,
        client: Address,
        timeout: Duration,
    ) -> anyhow::Result<CapabilityInfo> {
        const BACKOFF: Duration = Duration::from_secs(2);
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            if let Some(info) = self.get_capability(client).await? {
                return Ok(info);
            }
            if tokio::time::Instant::now() >= deadline {
                bail!("poll_capability: timed out after {timeout:?} waiting for a capability");
            }
            tokio::time::sleep(BACKOFF).await;
        }
    }

    /// The human-facing `GET /fund` link to print for the operator to open
    /// in a browser. The CLI never calls this itself.
    #[must_use]
    pub fn fund_url(&self, client: Address) -> String {
        format!("{}/fund?client={client}", self.base)
    }
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]
mod tests {
    use super::*;
    use alloy::primitives::address;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    #[tokio::test]
    async fn get_capability_maps_204_to_none() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/capability"))
            .respond_with(ResponseTemplate::new(204))
            .mount(&server)
            .await;
        let api = OnrampClient {
            base: server.uri(),
            http: reqwest::Client::new(),
        };
        let c = address!("00000000000000000000000000000000000000aa");
        assert!(api.get_capability(c).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn get_capability_parses_token() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/capability"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(serde_json::json!({ "token": "dcap1:AAAA" })),
            )
            .mount(&server)
            .await;
        let api = OnrampClient {
            base: server.uri(),
            http: reqwest::Client::new(),
        };
        let c = address!("00000000000000000000000000000000000000aa");
        let info = api.get_capability(c).await.unwrap().unwrap();
        assert_eq!(info.token, "dcap1:AAAA");
    }

    #[test]
    fn fund_url_has_no_hash() {
        let api = OnrampClient::new("https://s.example".to_string());
        let c = address!("00000000000000000000000000000000000000aa");
        let url = api.fund_url(c);
        assert!(url.contains("/fund?client="));
        assert!(!url.contains("hash"));
    }
}
