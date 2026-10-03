//! A Cloudflare Turnstile captcha gate.

use std::net::IpAddr;

use alloy::primitives::Address;
use async_trait::async_trait;
use serde::Deserialize;
use sponsord_api::secret::Secret;

use super::{Gate, html_escape};

/// The built-in page: the Turnstile widget, which posts its token as the
/// proof. `{{SITEKEY}}`, `{{CLIENT}}` and `{{BRAND_NAME}}` are substituted.
pub const DEFAULT_TEMPLATE: &str = include_str!("../../assets/turnstile.html");

const SITEVERIFY: &str = "https://challenges.cloudflare.com/turnstile/v0/siteverify";

pub struct TurnstileGate {
    secret: Secret,
    sitekey: String,
    brand_name: String,
    template: String,
    http: reqwest::Client,
    endpoint: String,
}

#[derive(Deserialize)]
struct SiteVerify {
    success: bool,
}

impl TurnstileGate {
    /// A gate verifying against Cloudflare with `secret`, rendering
    /// `template` (or [`DEFAULT_TEMPLATE`]). `sitekey` must be a plain token
    /// (the config checks it); `brand_name` is HTML-escaped.
    #[must_use]
    pub fn new(
        secret: Secret,
        sitekey: String,
        brand_name: String,
        template: Option<String>,
        http: reqwest::Client,
    ) -> Self {
        Self {
            secret,
            sitekey,
            brand_name,
            template: template.unwrap_or_else(|| DEFAULT_TEMPLATE.to_owned()),
            http,
            endpoint: SITEVERIFY.to_owned(),
        }
    }

    /// Verify against `endpoint` instead of Cloudflare's (for tests).
    #[must_use]
    pub fn with_endpoint(mut self, endpoint: String) -> Self {
        self.endpoint = endpoint;
        self
    }
}

#[async_trait]
impl Gate for TurnstileGate {
    fn page(&self, client: Address) -> String {
        // `client` is a parsed address, so its text is hex.
        self.template
            .replace("{{SITEKEY}}", &self.sitekey)
            .replace("{{BRAND_NAME}}", &html_escape(&self.brand_name))
            .replace("{{CLIENT}}", &client.to_string())
    }

    async fn verify(
        &self,
        _client: Address,
        proof: &str,
        client_ip: Option<IpAddr>,
    ) -> anyhow::Result<bool> {
        let ip = client_ip.map(|ip| ip.to_string());
        let mut form = vec![("secret", self.secret.expose()), ("response", proof)];
        if let Some(ip) = ip.as_deref() {
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

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]
mod tests {
    use wiremock::matchers::{body_string_contains, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use super::*;

    const CLIENT: Address = Address::repeat_byte(0xaa);

    fn gate(server: &MockServer) -> TurnstileGate {
        TurnstileGate::new(
            "secret".into(),
            "KEY".into(),
            "Acme <Models>".into(),
            None,
            reqwest::Client::new(),
        )
        .with_endpoint(format!("{}/siteverify", server.uri()))
    }

    #[tokio::test]
    async fn success_json_passes_and_sends_the_remote_ip() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/siteverify"))
            .and(body_string_contains("remoteip=203.0.113.7"))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(serde_json::json!({"success": true})),
            )
            .mount(&server)
            .await;
        let ip = Some("203.0.113.7".parse().unwrap());
        assert!(gate(&server).verify(CLIENT, "token", ip).await.unwrap());
    }

    #[tokio::test]
    async fn failure_json_refuses() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/siteverify"))
            .respond_with(ResponseTemplate::new(200).set_body_json(
                serde_json::json!({"success": false, "error-codes": ["invalid-input-response"]}),
            ))
            .mount(&server)
            .await;
        assert!(!gate(&server).verify(CLIENT, "bad", None).await.unwrap());
    }

    #[tokio::test]
    async fn page_carries_sitekey_client_and_escaped_brand() {
        let server = MockServer::start().await;
        let html = gate(&server).page(CLIENT);
        assert!(html.contains("data-sitekey=\"KEY\""));
        assert!(html.contains(&CLIENT.to_string()));
        assert!(html.contains("Acme &lt;Models&gt;"));
        assert!(!html.contains("{{"));
        // It posts to the fund route and tells an expired key apart.
        assert!(html.contains("fetch(\"/v1/fund\""));
        assert!(html.contains("id=\"expired\"") && html.contains("409"));
    }
}
