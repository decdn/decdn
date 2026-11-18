//! Circle's CCTP attestation service (Iris): the message and attestation of a
//! burn, by source domain and transaction hash.
//!
//! `GET {iris}/v2/messages/{sourceDomain}?transactionHash={tx}` answers
//! `{"messages": [{"message", "attestation", "status", …}]}`. A message is
//! ready once its `status` is `complete`; before that its attestation reads
//! `PENDING`. Iris answers `404` for a transaction it has not indexed yet,
//! which reads as pending too. A standard transfer waits for source-chain
//! finality, so the wait is minutes, and its message never expires.

use std::time::Duration;

use alloy::primitives::{B256, Bytes};
use anyhow::Context as _;
use serde::Deserialize;

/// How long to wait on a pending attestation: `attempts` reads, the first
/// pause `first`, each later pause doubled up to `max`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct IrisWait {
    /// The pause after the first pending answer.
    pub(crate) first: Duration,
    /// The longest pause.
    pub(crate) max: Duration,
    /// How many reads in all.
    pub(crate) attempts: u32,
}

impl Default for IrisWait {
    /// Five seconds doubling to a minute, forty reads: about half an hour,
    /// which covers source-chain finality.
    fn default() -> Self {
        Self {
            first: Duration::from_secs(5),
            max: Duration::from_secs(60),
            attempts: 40,
        }
    }
}

/// One attested CCTP message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Attested {
    /// The message, as `receiveMessage` takes it.
    pub(crate) message: Bytes,
    /// The attestation over it.
    pub(crate) attestation: Bytes,
}

/// What Iris says about a transaction's messages.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum MessageStatus {
    /// Every message of the transaction is attested.
    Complete(Vec<Attested>),
    /// Iris has not attested the transaction yet, after the whole wait.
    Pending,
}

/// The Iris answer, as far as it is read.
#[derive(Debug, Deserialize)]
struct MessagesResponse {
    #[serde(default)]
    messages: Vec<IrisMessage>,
}

#[derive(Debug, Deserialize)]
struct IrisMessage {
    message: Option<String>,
    attestation: Option<String>,
    status: Option<String>,
}

/// The HTTP client Iris is read with, with the process's rustls provider.
///
/// # Errors
///
/// The client does not build.
pub(crate) fn client() -> anyhow::Result<reqwest::Client> {
    decdn_common::tls::install_crypto_provider();
    reqwest::Client::builder()
        .user_agent(concat!("decdn/", env!("CARGO_PKG_VERSION")))
        .connect_timeout(Duration::from_secs(10))
        .timeout(Duration::from_secs(30))
        .build()
        .context("build the attestation service HTTP client")
}

/// Read the messages of `tx` from Iris at `base`, waiting per `wait` while
/// they are pending.
///
/// # Errors
///
/// A transport failure, an HTTP status other than success or `404`, or an
/// answer that does not decode.
pub(crate) async fn fetch_messages(
    client: &reqwest::Client,
    base: &str,
    source_domain: u32,
    tx: B256,
    wait: &IrisWait,
) -> anyhow::Result<MessageStatus> {
    let url = format!(
        "{}/v2/messages/{source_domain}?transactionHash={tx:#x}",
        base.trim_end_matches('/')
    );
    let mut pause = wait.first;
    for attempt in 1..=wait.attempts.max(1) {
        if let Some(messages) = read_once(client, &url).await? {
            return Ok(MessageStatus::Complete(messages));
        }
        if attempt < wait.attempts {
            eprintln!(
                "  sweep {tx:#x}: attestation pending ({attempt}/{}); next read in {}s",
                wait.attempts,
                pause.as_secs()
            );
            tokio::time::sleep(pause).await;
            pause = pause.saturating_mul(2).min(wait.max);
        }
    }
    Ok(MessageStatus::Pending)
}

/// One Iris read: `Some` when every message is attested, `None` while any is
/// pending or Iris does not know the transaction yet.
async fn read_once(client: &reqwest::Client, url: &str) -> anyhow::Result<Option<Vec<Attested>>> {
    let resp = client
        .get(url)
        .send()
        .await
        .context("reading the attestation service")?;
    if resp.status() == reqwest::StatusCode::NOT_FOUND {
        return Ok(None);
    }
    let status = resp.status();
    anyhow::ensure!(
        status.is_success(),
        "the attestation service answered HTTP {status}"
    );
    let body: MessagesResponse = resp
        .json()
        .await
        .context("decoding the attestation service answer")?;
    attested(body)
}

/// The attested messages of an answer, or `None` while any is pending.
fn attested(body: MessagesResponse) -> anyhow::Result<Option<Vec<Attested>>> {
    if body.messages.is_empty() {
        return Ok(None);
    }
    let mut out = Vec::with_capacity(body.messages.len());
    for m in body.messages {
        let complete = m.status.as_deref() == Some("complete");
        let (Some(message), Some(attestation)) = (m.message, m.attestation) else {
            return Ok(None);
        };
        if !complete || !attestation.starts_with("0x") {
            return Ok(None);
        }
        out.push(Attested {
            message: message
                .parse()
                .context("the attestation service sent a message that is not hex")?,
            attestation: attestation
                .parse()
                .context("the attestation service sent an attestation that is not hex")?,
        });
    }
    Ok(Some(out))
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::indexing_slicing)]
mod tests;
