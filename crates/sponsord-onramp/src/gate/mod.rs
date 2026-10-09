//! The gate: what a person must pass before the onramp asks the daemon for
//! a capability. A gate renders the page the CLI sends them to and checks
//! the proof that page posts back to `POST /v1/fund`.
//!
//! [`TurnstileGate`] (a Cloudflare Turnstile captcha) is the one built in.
//! Implement [`Gate`] for anything else: a login session, a signed token
//! from your own site, an allowlist.

pub mod turnstile;

use std::net::IpAddr;

use alloy_primitives::Address;
use async_trait::async_trait;

pub use turnstile::TurnstileGate;

#[async_trait]
pub trait Gate: Send + Sync {
    /// The HTML page for `client`. It must, once its check passes, `POST`
    /// `{"client": "<client>", "proof": "<proof>"}` to `/v1/fund`; a `409`
    /// answer means the key has expired and the download must restart.
    fn page(&self, client: Address) -> String;

    /// Whether `proof` lets `client` have a capability. `client_ip` is the
    /// requester's address when the onramp knows it. `Err` is the gate's own
    /// failure (its backend is down), answered `500`, not a refusal.
    async fn verify(
        &self,
        client: Address,
        proof: &str,
        client_ip: Option<IpAddr>,
    ) -> anyhow::Result<bool>;
}

/// Escape `s` for use in HTML text or a double-quoted attribute.
#[must_use]
pub fn html_escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&#39;"),
            c => out.push(c),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::html_escape;

    #[test]
    fn escapes_html_metacharacters() {
        assert_eq!(
            html_escape(r#"<a href="x">Tom & 'Jerry'</a>"#),
            "&lt;a href=&quot;x&quot;&gt;Tom &amp; &#39;Jerry&#39;&lt;/a&gt;"
        );
    }
}
