//! A Cloudflare Turnstile captcha gate.

use std::net::IpAddr;

use alloy_primitives::Address;
use async_trait::async_trait;
use serde::Deserialize;
use sponsord_api::secret::Secret;

use super::{Gate, html_escape};

/// The built-in page: the Turnstile widget, which posts its token as the
/// proof. `{{SITEKEY}}`, `{{CLIENT}}` and `{{BRAND_NAME}}` are substituted.
pub const DEFAULT_TEMPLATE: &str = include_str!("../../assets/turnstile.html");

const SITEVERIFY: &str = "https://challenges.cloudflare.com/turnstile/v0/siteverify";

/// A [`Gate`] that serves a Cloudflare Turnstile widget and checks its token
/// with Cloudflare's siteverify API.
pub struct TurnstileGate {
    secret: Secret,
    sitekey: String,
    brand_name: String,
    template: String,
    http: reqwest::Client,
    endpoint: String,
}

// By hand: the secret stays out, and the template is a whole HTML page.
impl std::fmt::Debug for TurnstileGate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TurnstileGate")
            .field("sitekey", &self.sitekey)
            .field("brand_name", &self.brand_name)
            .field("endpoint", &self.endpoint)
            .finish_non_exhaustive()
    }
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
        // `client` is a parsed address, so its text is hex and escaping it
        // changes nothing; the call is what CodeQL's rust/xss recognises.
        self.template
            .replace("{{SITEKEY}}", &self.sitekey)
            .replace("{{BRAND_NAME}}", &html_escape(&self.brand_name))
            .replace("{{CLIENT}}", &html_escape(&client.to_string()))
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
mod tests;
