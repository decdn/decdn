//! Typed HTTP client for the sponsor server's backend contract.
//!
//! The wrapper never calls `POST /fund` itself — a browser does that after
//! the captcha challenge. This client only talks to the three endpoints the
//! wrapper needs after a channel may already exist: `GET /channel` (poll for
//! a channel the browser flow created), `POST /topup` (extend an existing
//! channel's deposit), and it builds the `GET /fund` link as a string for
//! the operator to open in a browser. The JSON shapes here MUST byte-match
//! `crates/server/src/http/{channel,topup}.rs` — see `.superpowers/sdd/`
//! task briefs 11 and 14 for the frozen contract.

use std::str::FromStr;
use std::time::Duration;

use alloy::primitives::{Address, B256};
use anyhow::{anyhow, bail};
use serde::{Deserialize, Deserializer};

/// A channel the sponsor server already opened for `(client, node)`.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct ChannelInfo {
    #[serde(deserialize_with = "deser_b256")]
    pub channel_id: B256,
    pub node_id: String,
    #[serde(deserialize_with = "deser_address")]
    pub provider: Address,
}

fn deser_b256<'de, D>(d: D) -> Result<B256, D::Error>
where
    D: Deserializer<'de>,
{
    let s = String::deserialize(d)?;
    B256::from_str(&s).map_err(serde::de::Error::custom)
}

fn deser_address<'de, D>(d: D) -> Result<Address, D::Error>
where
    D: Deserializer<'de>,
{
    let s = String::deserialize(d)?;
    Address::from_str(&s).map_err(serde::de::Error::custom)
}

#[derive(Debug, Deserialize)]
struct TopupOk {
    #[allow(dead_code)]
    ok: bool,
    deposit_micro_usdc: u64,
}

#[derive(Debug, Deserialize)]
struct ErrorBody {
    error: String,
}

/// Client for the sponsor server's backend HTTP surface.
pub struct Api {
    pub base: String,
    pub http: reqwest::Client,
}

impl Api {
    /// Build a client pointed at `base` (no trailing slash expected).
    #[must_use]
    pub fn new(base: String) -> Self {
        Self {
            base,
            http: reqwest::Client::new(),
        }
    }

    /// `GET /channel?client=<0xADDR>&hash=<HEX>`.
    ///
    /// `204 No Content` means no channel exists yet and maps to `Ok(None)`.
    /// `200` parses the body into `ChannelInfo`. Any other status is an
    /// error carrying the response body (or status line if the body can't
    /// be read).
    ///
    /// # Errors
    ///
    /// Returns an error if the request fails to send, the server returns a
    /// non-200/204 status, or a 200 body fails to parse as `ChannelInfo`.
    pub async fn get_channel(
        &self,
        client: Address,
        hash: &str,
    ) -> anyhow::Result<Option<ChannelInfo>> {
        let url = format!("{}/channel", self.base);
        let resp = self
            .http
            .get(&url)
            .query(&[("client", client.to_string()), ("hash", hash.to_string())])
            .send()
            .await?;

        if resp.status() == reqwest::StatusCode::NO_CONTENT {
            return Ok(None);
        }
        if resp.status() != reqwest::StatusCode::OK {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            bail!("GET /channel failed: {status} {body}");
        }
        let info: ChannelInfo = resp.json().await?;
        Ok(Some(info))
    }

    /// Poll `GET /channel` on a fixed backoff until a channel appears or
    /// `timeout` elapses.
    ///
    /// # Errors
    ///
    /// Returns an error if any single poll fails, or if `timeout` elapses
    /// without a channel ever appearing.
    pub async fn poll_channel(
        &self,
        client: Address,
        hash: &str,
        timeout: Duration,
    ) -> anyhow::Result<ChannelInfo> {
        const BACKOFF: Duration = Duration::from_secs(2);
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            if let Some(info) = self.get_channel(client, hash).await? {
                return Ok(info);
            }
            if tokio::time::Instant::now() >= deadline {
                bail!("poll_channel: timed out after {timeout:?} waiting for a channel");
            }
            tokio::time::sleep(BACKOFF).await;
        }
    }

    /// `POST /topup` with `{channel_id, timestamp, signature}`.
    ///
    /// Returns the new `deposit_micro_usdc` on success.
    ///
    /// # Errors
    ///
    /// Returns an error if the request fails to send, or the server
    /// returns a non-200 status (the parsed `{error}` body, if present, is
    /// included in the error message).
    pub async fn topup(
        &self,
        channel_id: B256,
        timestamp: u64,
        signature: &str,
    ) -> anyhow::Result<u64> {
        let url = format!("{}/topup", self.base);
        let resp = self
            .http
            .post(&url)
            .json(&serde_json::json!({
                "channel_id": format!("0x{}", hex::encode(channel_id.0)),
                "timestamp": timestamp,
                "signature": signature,
            }))
            .send()
            .await?;

        if resp.status() != reqwest::StatusCode::OK {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            let detail = serde_json::from_str::<ErrorBody>(&body)
                .map(|e| e.error)
                .unwrap_or(body);
            bail!("POST /topup failed: {status} {detail}");
        }
        let parsed: TopupOk = resp
            .json()
            .await
            .map_err(|e| anyhow!("bad /topup response: {e}"))?;
        Ok(parsed.deposit_micro_usdc)
    }

    /// The human-facing `GET /fund` link to print for the operator to open
    /// in a browser. The wrapper never calls this itself.
    #[must_use]
    pub fn fund_url(&self, client: Address, hash: &str) -> String {
        format!("{}/fund?client={client}&hash={hash}", self.base)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy::primitives::address;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    #[tokio::test]
    async fn get_channel_maps_204_to_none() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/channel"))
            .respond_with(ResponseTemplate::new(204))
            .up_to_n_times(1)
            .mount(&server)
            .await;
        let api = Api {
            base: server.uri(),
            http: reqwest::Client::new(),
        };
        let c = address!("00000000000000000000000000000000000000aa");
        assert!(
            api.get_channel(c, &"aa".repeat(32))
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn get_channel_parses_200_body() {
        let server = MockServer::start().await;
        let channel_id = "0x".to_string() + &"11".repeat(32);
        let provider = format!("0x{}bb", "00".repeat(19));
        Mock::given(method("GET"))
            .and(path("/channel"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "channel_id": channel_id,
                "node_id": "cc".repeat(32),
                "provider": provider,
            })))
            .mount(&server)
            .await;
        let api = Api {
            base: server.uri(),
            http: reqwest::Client::new(),
        };
        let c = address!("00000000000000000000000000000000000000aa");
        let info = api.get_channel(c, &"aa".repeat(32)).await.unwrap().unwrap();
        assert_eq!(info.channel_id, B256::from_str(&channel_id).unwrap());
        assert_eq!(info.node_id, "cc".repeat(32));
        assert_eq!(info.provider, Address::from_str(&provider).unwrap());
    }

    #[tokio::test]
    async fn topup_returns_deposit_micro_usdc() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/topup"))
            .respond_with(ResponseTemplate::new(200).set_body_json(
                serde_json::json!({"ok": true, "deposit_micro_usdc": 3_000_000_u64}),
            ))
            .mount(&server)
            .await;
        let api = Api {
            base: server.uri(),
            http: reqwest::Client::new(),
        };
        let channel_id = B256::from_str(&("0x".to_string() + &"11".repeat(32))).unwrap();
        let deposit = api
            .topup(channel_id, 1_700_000_000, "0xdeadbeef")
            .await
            .unwrap();
        assert_eq!(deposit, 3_000_000);
    }
}
