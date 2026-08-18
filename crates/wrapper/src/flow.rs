//! The wrapper flow: get/poll a capability (browser captcha), then run one
//! `decdn fetch --capability`. There is no money top-up — an exhausted cap or
//! drained pool is terminal (issue a fresh capability with a new key).

use std::path::Path;
use std::time::Duration;

use crate::api::Api;
use crate::config::WrapperConfig;
use crate::runner::{self, FetchArgs, FetchOutcome};

/// How long to wait for the browser captcha flow to produce a capability.
const CAPABILITY_POLL_TIMEOUT: Duration = Duration::from_secs(300);

/// # Errors
/// Keystore unreadable, sponsor unreachable / no capability within the poll
/// timeout, or `decdn fetch` fails (including a terminal cap/pool exhaustion).
pub async fn get(hash: &str, out: &Path, cfg: &WrapperConfig) -> anyhow::Result<()> {
    let client = crate::keystore::read_address(&cfg.keystore_path)?;
    let api = Api {
        base: cfg.gateway_base.clone(),
        http: reqwest::Client::new(),
    };

    let info = match api.get_capability(client).await? {
        Some(info) => info,
        None => {
            println!(
                "No capability yet. Open this link and solve the captcha:\n  {}",
                api.fund_url(client)
            );
            api.poll_capability(client, CAPABILITY_POLL_TIMEOUT).await?
        }
    };

    let args = FetchArgs {
        hash: hash.to_string(),
        output: out.display().to_string(),
        capability: info.token,
        payment_pool_address: cfg.payment_pool.to_string(),
        rpc_url: cfg.rpc_url.clone(),
        capacity_bond_address: cfg.capacity_bond.map(|a| a.to_string()),
        slash_judge_address: cfg.slash_judge.map(|a| a.to_string()),
        chain_id: cfg.chain_id,
        keystore: cfg.keystore_path.display().to_string(),
        data_dir: cfg.data_dir.display().to_string(),
    };

    match runner::run_fetch(&cfg.decdn_bin, &args).await? {
        FetchOutcome::Complete => Ok(()),
        FetchOutcome::Exhausted => anyhow::bail!(
            "sponsored allowance exhausted (cap or pool drained). Generate a new keystore key \
             and request a fresh capability — an already-used key's cap cannot be raised on-chain."
        ),
        FetchOutcome::Failed(e) => anyhow::bail!("fetch failed: {e}"),
    }
}
