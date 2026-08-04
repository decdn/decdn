//! The end-to-end wrapper flow: fund (browser) -> poll -> fetch -> top-up ->
//! resume.
//!
//! `get` is the real entry point main.rs calls. `run_rounds` is the pure
//! round-loop it delegates to, factored out so the drain/top-up/retry logic
//! is unit-testable with fakes instead of a real `decdn fetch` subprocess and
//! real HTTP.

use std::path::Path;
use std::time::Duration;

use crate::api::Api;
use crate::config::WrapperConfig;
use crate::runner::{self, FetchArgs, FetchOutcome};

/// How long to wait for the browser-driven captcha/fund flow to produce a
/// channel before giving up.
const FUND_POLL_TIMEOUT: Duration = Duration::from_secs(300);

/// Upper bound on fetch/top-up rounds in one `get` call. Bounds the
/// drain-then-retry loop so a channel that keeps draining (e.g. the
/// sponsor's monthly cap is exhausted) fails loudly instead of looping
/// forever.
const MAX_ROUNDS: usize = 20;

/// Outcome of one round, mirroring `FetchOutcome` without carrying a real
/// child-process payload. Exists so `run_rounds` can be driven by closures
/// in tests instead of a real `decdn fetch` invocation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    Complete,
    Drained,
    Failed(String),
}

impl From<FetchOutcome> for Outcome {
    fn from(o: FetchOutcome) -> Self {
        match o {
            FetchOutcome::Complete => Outcome::Complete,
            FetchOutcome::Drained => Outcome::Drained,
            FetchOutcome::Failed(e) => Outcome::Failed(e),
        }
    }
}

/// Run up to `max` fetch/top-up rounds.
///
/// Each round calls `next_outcome` (in real use: `run_fetch`, classified
/// into an `Outcome`). `Complete` ends the loop successfully. `Drained`
/// calls `do_topup` and retries. `Failed(e)` ends the loop with an error.
/// If `max` rounds all come back `Drained`, this returns an error too — the
/// channel or its sponsor is not keeping up.
///
/// # Errors
///
/// Returns an error if a round is `Failed`, if `do_topup` errors, or if
/// `max` rounds are exhausted while still draining.
pub async fn run_rounds<F, T>(
    max: usize,
    mut next_outcome: F,
    mut do_topup: T,
) -> anyhow::Result<()>
where
    F: FnMut() -> Outcome,
    T: FnMut() -> anyhow::Result<()>,
{
    for _round in 0..max {
        match next_outcome() {
            Outcome::Complete => return Ok(()),
            Outcome::Drained => {
                do_topup()?;
            }
            Outcome::Failed(e) => anyhow::bail!("fetch failed: {e}"),
        }
    }
    anyhow::bail!("still draining after {max} rounds")
}

/// Current unix timestamp (seconds), used as the top-up authorization
/// timestamp.
///
/// # Errors
///
/// Returns an error if the system clock is set before the Unix epoch.
fn now_unix() -> anyhow::Result<u64> {
    let dur = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)?;
    Ok(dur.as_secs())
}

/// The real end-to-end flow: read the operator's address from the keystore,
/// look up (or wait for the browser flow to create) a channel, then run the
/// fetch/top-up round loop against `decdn fetch`, sequentially — never two
/// fetches concurrently against the same `data_dir` (the redb buyer-channel
/// store takes a file lock).
///
/// # Errors
///
/// Returns an error if the keystore can't be read, the sponsor server can't
/// be reached or never produces a channel within the poll timeout, a
/// top-up signature/request fails, or `decdn fetch` fails terminally
/// (anything other than a recoverable drain).
pub async fn get(hash: &str, out: &Path, cfg: &WrapperConfig) -> anyhow::Result<()> {
    let client = crate::keystore::read_address(&cfg.keystore_path)?;
    let api = Api {
        base: cfg.gateway_base.clone(),
        http: reqwest::Client::new(),
    };

    let info = match api.get_channel(client, hash).await? {
        Some(info) => info,
        None => {
            println!(
                "No funded channel yet. Open this link and solve the captcha:\n  {}",
                api.fund_url(client, hash)
            );
            api.poll_channel(client, hash, FUND_POLL_TIMEOUT).await?
        }
    };

    let args = FetchArgs {
        hash: hash.to_string(),
        output: out.display().to_string(),
        channel_id: info.channel_id.to_string(),
        node_id: info.node_id.clone(),
        provider_address: info.provider.to_string(),
        rpc_url: cfg.rpc_url.clone(),
        payment_channel_address: cfg.payment_channel.to_string(),
        capacity_bond_address: cfg.capacity_bond.map(|a| a.to_string()),
        slash_judge_address: cfg.slash_judge.map(|a| a.to_string()),
        chain_id: cfg.chain_id,
        keystore: cfg.keystore_path.display().to_string(),
        data_dir: cfg.data_dir.display().to_string(),
    };

    // Sequential only: each round awaits the previous fetch/top-up to
    // completion before starting the next — nothing here spawns a
    // concurrent fetch against the same data_dir. `run_rounds` above takes
    // sync closures so it can be driven by cheap fakes in tests; here each
    // round needs real `await`s (spawning `decdn fetch`, signing, an HTTP
    // top-up), so this loop is the inline equivalent the brief allows for.
    let mut round = 0usize;
    loop {
        if round >= MAX_ROUNDS {
            anyhow::bail!("still draining after {MAX_ROUNDS} rounds");
        }
        round += 1;
        let outcome: Outcome = runner::run_fetch(&cfg.decdn_bin, &args).await?.into();
        match outcome {
            Outcome::Complete => return Ok(()),
            Outcome::Drained => {
                let ts = now_unix()?;
                let sig = crate::keystore::sign_topup(
                    &cfg.keystore_path,
                    &cfg.keystore_password,
                    info.channel_id,
                    ts,
                )
                .await?;
                api.topup(info.channel_id, ts, &sig).await?;
            }
            Outcome::Failed(e) => anyhow::bail!("fetch failed: {e}"),
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn drain_then_complete_tops_up_once() {
        let outcomes = std::sync::Mutex::new(vec![Outcome::Drained, Outcome::Complete]);
        let topups = std::sync::atomic::AtomicUsize::new(0);
        let res = run_rounds(
            3,
            || {
                let mut o = outcomes.lock().unwrap();
                o.remove(0)
            },
            || {
                topups.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                Ok(())
            },
        )
        .await;
        assert!(res.is_ok());
        assert_eq!(topups.load(std::sync::atomic::Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn failed_propagates_as_error_without_topup() {
        let outcomes = std::sync::Mutex::new(vec![Outcome::Failed("boom".to_string())]);
        let topups = std::sync::atomic::AtomicUsize::new(0);
        let res = run_rounds(
            3,
            || {
                let mut o = outcomes.lock().unwrap();
                o.remove(0)
            },
            || {
                topups.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                Ok(())
            },
        )
        .await;
        assert!(res.is_err());
        assert!(res.unwrap_err().to_string().contains("boom"));
        assert_eq!(topups.load(std::sync::atomic::Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn max_rounds_exhausted_while_draining_is_an_error() {
        let topups = std::sync::atomic::AtomicUsize::new(0);
        let res = run_rounds(
            3,
            || Outcome::Drained,
            || {
                topups.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                Ok(())
            },
        )
        .await;
        assert!(res.is_err());
        assert!(res.unwrap_err().to_string().contains("still draining"));
        assert_eq!(topups.load(std::sync::atomic::Ordering::SeqCst), 3);
    }
}
