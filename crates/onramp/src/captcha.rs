use async_trait::async_trait;
use serde::Deserialize;

/// Abstraction over "verify a captcha token" so the HTTP layer can inject a
/// fake in tests instead of calling out to Cloudflare. `Turnstile` is the
/// production implementation.
#[async_trait]
pub trait CaptchaVerifier: Send + Sync {
    async fn verify(&self, token: &str, remote_ip: Option<&str>) -> anyhow::Result<bool>;
}

pub struct Turnstile {
    secret: String,
    http: reqwest::Client,
    endpoint: String,
}

#[derive(Deserialize)]
struct SiteVerify {
    success: bool,
}

impl Turnstile {
    pub fn new(secret: String, http: reqwest::Client) -> Self {
        Self {
            secret,
            http,
            endpoint: "https://challenges.cloudflare.com/turnstile/v0/siteverify".into(),
        }
    }

    pub fn with_endpoint(secret: String, http: reqwest::Client, endpoint: String) -> Self {
        Self {
            secret,
            http,
            endpoint,
        }
    }

    pub async fn verify(&self, token: &str, remote_ip: Option<&str>) -> anyhow::Result<bool> {
        let mut form = vec![("secret", self.secret.as_str()), ("response", token)];
        if let Some(ip) = remote_ip {
            form.push(("remoteip", ip));
        }
        let resp: SiteVerify = self
            .http
            .post(&self.endpoint)
            .form(&form)
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        Ok(resp.success)
    }
}

#[async_trait]
impl CaptchaVerifier for Turnstile {
    async fn verify(&self, token: &str, remote_ip: Option<&str>) -> anyhow::Result<bool> {
        Turnstile::verify(self, token, remote_ip).await
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
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    #[tokio::test]
    async fn returns_true_on_success_json() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/siteverify"))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(serde_json::json!({"success": true})),
            )
            .mount(&server)
            .await;
        let ts = Turnstile::with_endpoint(
            "secret".into(),
            reqwest::Client::new(),
            format!("{}/siteverify", server.uri()),
        );
        assert!(ts.verify("token", None).await.unwrap());
    }

    #[tokio::test]
    async fn returns_false_on_failure_json() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/siteverify"))
            .respond_with(ResponseTemplate::new(200).set_body_json(
                serde_json::json!({"success": false, "error-codes": ["invalid-input-response"]}),
            ))
            .mount(&server)
            .await;
        let ts = Turnstile::with_endpoint(
            "secret".into(),
            reqwest::Client::new(),
            format!("{}/siteverify", server.uri()),
        );
        assert!(!ts.verify("bad", None).await.unwrap());
    }
}
