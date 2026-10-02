//! Client for the `sponsord` daemon's HTTP API, behind `CapabilitySource`
//! so the HTTP layer can take a fake in tests.

use alloy::primitives::Address;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

/// The daemon's `GET /v1/info`.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
pub struct DaemonInfo {
    pub chain_id: u64,
    pub payment_pool: Address,
    pub max_spending_cap: u64,
    pub max_ttl_secs: u64,
}

/// A capability the daemon handed out, with the terms it carries.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
pub struct DaemonGrant {
    pub token: String,
    pub spending_cap: u64,
    pub expiry: u64,
}

#[derive(Debug, thiserror::Error)]
pub enum SourceError {
    /// The signer's on-chain registration has expired; only a new signer
    /// can get a usable capability.
    #[error("signer registration expired at {0}")]
    SignerExpired(u64),
    /// The daemon could not be reached, timed out, or failed on its side.
    #[error("daemon unavailable: {0}")]
    Unavailable(String),
    /// The daemon rejected the request itself: a wrong token or terms
    /// outside its maximums.
    #[error("daemon rejected the request: {0}")]
    Misconfigured(String),
}

#[async_trait]
pub trait CapabilitySource: Send + Sync {
    async fn issue(
        &self,
        signer: Address,
        spending_cap: Option<u64>,
        ttl_secs: Option<u64>,
    ) -> Result<DaemonGrant, SourceError>;

    async fn info(&self) -> Result<DaemonInfo, SourceError>;
}

pub struct DaemonClient {
    base: String,
    token: String,
    http: reqwest::Client,
}

impl DaemonClient {
    #[must_use]
    pub fn new(base: &str, token: String, http: reqwest::Client) -> Self {
        Self {
            base: base.trim_end_matches('/').to_owned(),
            token,
            http,
        }
    }
}

#[derive(Serialize)]
struct IssueBody {
    signer: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    spending_cap: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    ttl_secs: Option<u64>,
}

#[derive(Default, Deserialize)]
struct ErrorBody {
    #[serde(default)]
    error: String,
    expiry: Option<u64>,
}

fn unavailable(e: reqwest::Error) -> SourceError {
    SourceError::Unavailable(e.without_url().to_string())
}

async fn error_from(resp: reqwest::Response) -> SourceError {
    let status = resp.status();
    let body: ErrorBody = resp.json().await.unwrap_or_default();
    if status.as_u16() == 409 && body.error == "signer_expired" {
        SourceError::SignerExpired(body.expiry.unwrap_or(0))
    } else if status.is_client_error() {
        SourceError::Misconfigured(format!("{status} {}", body.error))
    } else {
        SourceError::Unavailable(format!("{status} {}", body.error))
    }
}

#[async_trait]
impl CapabilitySource for DaemonClient {
    async fn issue(
        &self,
        signer: Address,
        spending_cap: Option<u64>,
        ttl_secs: Option<u64>,
    ) -> Result<DaemonGrant, SourceError> {
        let resp = self
            .http
            .post(format!("{}/v1/capabilities", self.base))
            .bearer_auth(&self.token)
            .json(&IssueBody {
                signer: signer.to_string(),
                spending_cap,
                ttl_secs,
            })
            .send()
            .await
            .map_err(unavailable)?;
        if !resp.status().is_success() {
            return Err(error_from(resp).await);
        }
        resp.json().await.map_err(unavailable)
    }

    async fn info(&self) -> Result<DaemonInfo, SourceError> {
        let resp = self
            .http
            .get(format!("{}/v1/info", self.base))
            .bearer_auth(&self.token)
            .send()
            .await
            .map_err(unavailable)?;
        if !resp.status().is_success() {
            return Err(error_from(resp).await);
        }
        resp.json().await.map_err(unavailable)
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
    use std::time::Duration;

    use super::*;
    use serde_json::json;
    use wiremock::matchers::{body_json, header, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    const SIGNER: Address = Address::repeat_byte(0xaa);

    fn client(base: &str) -> DaemonClient {
        DaemonClient::new(base, "tok".into(), reqwest::Client::new())
    }

    async fn mock_issue(status: u16, body: serde_json::Value) -> MockServer {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v1/capabilities"))
            .respond_with(ResponseTemplate::new(status).set_body_json(body))
            .mount(&server)
            .await;
        server
    }

    #[tokio::test]
    async fn issue_sends_bearer_and_terms_and_parses_the_grant() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v1/capabilities"))
            .and(header("authorization", "Bearer tok"))
            .and(body_json(json!({
                "signer": SIGNER.to_string(),
                "spending_cap": 1_000_000,
            })))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "token": "dcap1:X", "spending_cap": 1_000_000, "expiry": 99, "registered": false
            })))
            .mount(&server)
            .await;
        let g = client(&server.uri())
            .issue(SIGNER, Some(1_000_000), None)
            .await
            .unwrap();
        assert_eq!(
            g,
            DaemonGrant {
                token: "dcap1:X".into(),
                spending_cap: 1_000_000,
                expiry: 99
            }
        );
    }

    #[tokio::test]
    async fn trailing_slash_in_base_url_is_tolerated() {
        let server = mock_issue(
            200,
            json!({"token": "dcap1:X", "spending_cap": 1, "expiry": 2, "registered": true}),
        )
        .await;
        let base = format!("{}/", server.uri());
        assert!(client(&base).issue(SIGNER, None, None).await.is_ok());
    }

    #[tokio::test]
    async fn signer_expired_maps_to_its_own_error() {
        let server = mock_issue(409, json!({"error": "signer_expired", "expiry": 1_000})).await;
        let err = client(&server.uri())
            .issue(SIGNER, None, None)
            .await
            .err()
            .unwrap();
        assert!(matches!(err, SourceError::SignerExpired(1_000)));
    }

    #[tokio::test]
    async fn client_errors_are_misconfiguration() {
        for (status, code) in [(400, "exceeds_max"), (401, "unauthorized")] {
            let server = mock_issue(status, json!({"error": code})).await;
            let err = client(&server.uri())
                .issue(SIGNER, None, None)
                .await
                .err()
                .unwrap();
            assert!(matches!(err, SourceError::Misconfigured(_)), "{status}");
        }
    }

    #[tokio::test]
    async fn server_errors_and_refused_connections_are_unavailable() {
        let server = mock_issue(503, json!({"error": "chain_unavailable"})).await;
        let err = client(&server.uri())
            .issue(SIGNER, None, None)
            .await
            .err()
            .unwrap();
        assert!(matches!(err, SourceError::Unavailable(_)));

        let err = client("http://127.0.0.1:1")
            .issue(SIGNER, None, None)
            .await
            .err()
            .unwrap();
        assert!(matches!(err, SourceError::Unavailable(_)));
    }

    #[tokio::test]
    async fn a_hung_daemon_times_out_as_unavailable() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200).set_delay(Duration::from_secs(5)))
            .mount(&server)
            .await;
        let http = reqwest::Client::builder()
            .timeout(Duration::from_millis(200))
            .build()
            .unwrap();
        let c = DaemonClient::new(&server.uri(), "tok".into(), http);
        let err = c.issue(SIGNER, None, None).await.err().unwrap();
        assert!(matches!(err, SourceError::Unavailable(_)));
    }

    #[tokio::test]
    async fn info_parses() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v1/info"))
            .and(header("authorization", "Bearer tok"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "chain_id": 421_614,
                "payment_pool": Address::repeat_byte(0x22).to_string(),
                "max_spending_cap": 5_000_000,
                "max_ttl_secs": 172_800,
            })))
            .mount(&server)
            .await;
        assert_eq!(
            client(&server.uri()).info().await.unwrap(),
            DaemonInfo {
                chain_id: 421_614,
                payment_pool: Address::repeat_byte(0x22),
                max_spending_cap: 5_000_000,
                max_ttl_secs: 172_800,
            }
        );
    }
}
