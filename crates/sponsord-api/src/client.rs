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
    SignerExpired { expiry: u64 },
    /// The daemon rejected the request itself (`4xx`): a wrong token, or
    /// terms outside its maximums. Retrying will not help.
    #[error("daemon rejected the request: {status} {code}")]
    Rejected { status: u16, code: ErrorCode },
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
    #[error("{0}")]
    Http(#[from] reqwest::Error),
    /// The onramp answered with an unexpected status.
    #[error("{route} failed: {status} {code}")]
    Status {
        route: &'static str,
        status: u16,
        code: ErrorCode,
    },
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
mod tests {
    use serde_json::json;
    use wiremock::matchers::{body_json, header, method, path, query_param};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use super::*;
    use crate::{IssuedCapability, MicroUsdc};

    const SIGNER: Address = Address::repeat_byte(0xaa);

    fn daemon(base: &str) -> DaemonClient {
        DaemonClient::new(base, "tok".into(), reqwest::Client::new())
    }

    async fn mock_issue(status: u16, body: serde_json::Value) -> MockServer {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path(daemon::routes::CAPABILITIES))
            .respond_with(ResponseTemplate::new(status).set_body_json(body))
            .mount(&server)
            .await;
        server
    }

    #[tokio::test]
    async fn issue_sends_bearer_and_terms_and_parses_the_response() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path(daemon::routes::CAPABILITIES))
            .and(header("authorization", "Bearer tok"))
            .and(body_json(json!({
                "signer": SIGNER,
                "spending_cap": 1_000_000,
            })))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "token": "dcap1:X", "spending_cap": 1_000_000, "expiry": 99, "registered": false
            })))
            .mount(&server)
            .await;
        let req = IssueRequest {
            spending_cap: Some(MicroUsdc(1_000_000)),
            ..IssueRequest::new(SIGNER)
        };
        let resp = daemon(&server.uri()).issue(&req).await.unwrap();
        assert_eq!(
            resp,
            IssueResponse {
                capability: IssuedCapability {
                    token: "dcap1:X".into(),
                    spending_cap: MicroUsdc(1_000_000),
                    expiry: 99,
                },
                registered: false,
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
        assert!(
            daemon(&base)
                .issue(&IssueRequest::new(SIGNER))
                .await
                .is_ok()
        );
    }

    #[tokio::test]
    async fn signer_expired_maps_to_its_own_error() {
        let server = mock_issue(409, json!({"error": "signer_expired", "expiry": 1_000})).await;
        let err = daemon(&server.uri())
            .issue(&IssueRequest::new(SIGNER))
            .await
            .unwrap_err();
        assert!(matches!(err, DaemonError::SignerExpired { expiry: 1_000 }));
    }

    #[tokio::test]
    async fn signer_expired_outside_a_409_is_not_a_verdict_on_the_signer() {
        let server = mock_issue(500, json!({"error": "signer_expired", "expiry": 1_000})).await;
        let err = daemon(&server.uri())
            .issue(&IssueRequest::new(SIGNER))
            .await
            .unwrap_err();
        assert!(matches!(err, DaemonError::Unavailable(_)), "{err}");
    }

    #[test]
    fn debug_leaves_the_token_out() {
        let shown = format!("{:?}", daemon("http://d"));
        assert!(!shown.contains("tok\""), "{shown}");
        assert!(shown.contains("<redacted>"));
    }

    #[tokio::test]
    async fn client_errors_are_rejections() {
        for (status, code) in [(400, ErrorCode::ExceedsMax), (401, ErrorCode::Unauthorized)] {
            let server = mock_issue(status, json!({"error": code})).await;
            let err = daemon(&server.uri())
                .issue(&IssueRequest::new(SIGNER))
                .await
                .unwrap_err();
            assert!(
                matches!(err, DaemonError::Rejected { status: s, code: c } if s == status && c == code),
                "{err}"
            );
        }
    }

    #[tokio::test]
    async fn server_errors_and_refused_connections_are_unavailable() {
        let server = mock_issue(503, json!({"error": "chain_unavailable"})).await;
        let err = daemon(&server.uri())
            .issue(&IssueRequest::new(SIGNER))
            .await
            .unwrap_err();
        assert!(matches!(err, DaemonError::Unavailable(_)));

        let err = daemon("http://127.0.0.1:1")
            .issue(&IssueRequest::new(SIGNER))
            .await
            .unwrap_err();
        assert!(matches!(err, DaemonError::Unavailable(_)));
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
        let err = c.issue(&IssueRequest::new(SIGNER)).await.unwrap_err();
        assert!(matches!(err, DaemonError::Unavailable(_)));
    }

    #[tokio::test]
    async fn info_parses() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path(daemon::routes::INFO))
            .and(header("authorization", "Bearer tok"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "chain_id": 421_614,
                "payment_pool": Address::repeat_byte(0x22),
                "max_spending_cap": 5_000_000,
                "max_ttl_secs": 172_800,
            })))
            .mount(&server)
            .await;
        assert_eq!(
            daemon(&server.uri()).info().await.unwrap(),
            Info {
                chain_id: 421_614,
                payment_pool: Address::repeat_byte(0x22),
                max_spending_cap: MicroUsdc(5_000_000),
                max_ttl_secs: 172_800,
            }
        );
    }

    fn onramp(base: &str) -> OnrampClient {
        OnrampClient::new(base, reqwest::Client::new())
    }

    #[tokio::test]
    async fn capability_maps_204_to_none_and_200_to_the_token() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path(onramp::routes::CAPABILITY))
            .and(query_param("client", SIGNER.to_string()))
            .respond_with(ResponseTemplate::new(204))
            .up_to_n_times(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path(onramp::routes::CAPABILITY))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({"token": "dcap1:A"})))
            .mount(&server)
            .await;
        let c = onramp(&server.uri());
        assert_eq!(c.capability(SIGNER).await.unwrap(), None);
        assert_eq!(
            c.capability(SIGNER).await.unwrap().as_deref(),
            Some("dcap1:A")
        );
    }

    #[tokio::test]
    async fn poll_waits_for_the_token_and_times_out_without_one() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path(onramp::routes::CAPABILITY))
            .respond_with(ResponseTemplate::new(204))
            .mount(&server)
            .await;
        let err = onramp(&server.uri())
            .poll_capability(SIGNER, Duration::from_millis(10), Duration::from_millis(50))
            .await
            .unwrap_err();
        assert!(matches!(err, OnrampError::Timeout(_)));
    }

    #[tokio::test]
    async fn poll_timeout_bounds_a_request_in_flight() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path(onramp::routes::CAPABILITY))
            .respond_with(ResponseTemplate::new(204).set_delay(Duration::from_secs(30)))
            .mount(&server)
            .await;
        let started = std::time::Instant::now();
        let err = onramp(&server.uri())
            .poll_capability(
                SIGNER,
                Duration::from_millis(10),
                Duration::from_millis(200),
            )
            .await
            .unwrap_err();
        assert!(matches!(err, OnrampError::Timeout(_)));
        assert!(started.elapsed() < Duration::from_secs(5));
    }

    #[tokio::test]
    async fn other_statuses_carry_the_error_code() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path(onramp::routes::CAPABILITY))
            .respond_with(
                ResponseTemplate::new(429).set_body_json(json!({"error": "rate_limited"})),
            )
            .mount(&server)
            .await;
        let err = onramp(&server.uri()).capability(SIGNER).await.unwrap_err();
        assert!(matches!(
            err,
            OnrampError::Status {
                status: 429,
                code: ErrorCode::RateLimited,
                ..
            }
        ));
    }

    #[tokio::test]
    async fn profile_parses() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path(onramp::routes::PROFILE))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "chain_id": 421_614,
                "rpc_url": "https://rpc.example",
                "payment_pool": Address::repeat_byte(0x01),
                "capacity_bond": Address::repeat_byte(0x02),
                "min_cli_version": "0.2.0",
            })))
            .mount(&server)
            .await;
        let p = onramp(&server.uri()).profile().await.unwrap();
        assert_eq!(p.payment_pool, Address::repeat_byte(0x01));
        assert_eq!(p.slash_judge, None);
        assert_eq!(p.min_cli_version, Some(semver::Version::new(0, 2, 0)));
    }

    #[test]
    fn fund_url_names_the_client() {
        let c = onramp("https://s.example/");
        assert_eq!(
            c.fund_url(SIGNER),
            format!("https://s.example/fund?client={SIGNER}")
        );
    }
}
