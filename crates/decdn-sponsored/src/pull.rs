//! The pull flow: read the onramp's profile, open this download's state,
//! make sure its throwaway key holds an unexpired capability (the browser
//! gate when it doesn't), then run `decdn bundle pull`. A successful pull
//! deletes the state; a failed one keeps it, so re-running the same command
//! resumes without passing the gate again.

use std::io::IsTerminal;
use std::num::NonZeroU64;
use std::path::Path;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::config::Config;
use crate::decdn::{self, PullArgs};
use crate::hash::normalize_hash;
use crate::onramp;
use crate::session::Session;
use sponsord_api::client::OnrampClient;
use sponsord_api::onramp::Profile;

/// How long to wait for the browser gate flow to produce a capability.
const CAPABILITY_POLL_TIMEOUT: Duration = Duration::from_secs(600);

/// How often to ask the onramp whether the capability is there yet.
const CAPABILITY_POLL_EVERY: Duration = Duration::from_secs(2);

/// A default decdn node's capability-expiry margin: it refuses vouchers once
/// `now + margin` reaches the capability's expiry. The node derives it as one
/// 300 s redeem interval plus 120 s for the redeem transaction to land.
pub const NODE_EXPIRY_MARGIN_SECS: u64 = 420;

/// Runway the download itself needs on top of the node's refusal window.
const DOWNLOAD_SLACK_SECS: u64 = 3600;

/// A saved capability this close to expiry is replaced before pulling.
const EXPIRY_MARGIN_SECS: u64 = NODE_EXPIRY_MARGIN_SECS + DOWNLOAD_SLACK_SECS;

/// # Errors
/// Invalid hash, state dir or key failure, sponsor unreachable / no
/// capability within the poll timeout, or `decdn bundle pull` failing.
pub async fn pull(
    hash: &str,
    output: &Path,
    namespace: Option<NonZeroU64>,
    cfg: &Config,
) -> anyhow::Result<()> {
    let hash = normalize_hash(hash)?;
    let api = onramp::client(&cfg.onramp_url)?;

    let mut session = Session::open(&cfg.data_dir, &hash)?;
    let profile = profile(&api, &session).await?;
    let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
    if session
        .capability()?
        .is_some_and(|grant| grant.expiry <= now.saturating_add(EXPIRY_MARGIN_SECS))
    {
        // A key's cap and expiry are frozen on-chain at its first
        // redemption, so a capability near expiry means a fresh key.
        session.discard()?;
        session = Session::open(&cfg.data_dir, &hash)?;
    }
    session.save_profile(&profile)?;

    let client = session.ensure_key()?;
    if session.capability()?.is_none() {
        let token = match api.capability(client).await? {
            Some(token) => token,
            None => {
                let url = api.fund_url(client);
                println!("Open this link to start the download:\n  {url}");
                open_in_browser(&url);
                api.poll_capability(client, CAPABILITY_POLL_EVERY, CAPABILITY_POLL_TIMEOUT)
                    .await?
            }
        };
        session.save_capability(&token)?;
    }

    let args = PullArgs {
        hash,
        output: output.to_path_buf(),
        namespace,
        capability_file: session.capability_path(),
        keystore: session.keystore_path(),
        password_file: session.password_path(),
        data_dir: session.dir().to_path_buf(),
        rpc_url: profile.rpc_url,
        payment_pool: profile.payment_pool,
        capacity_bond: Some(profile.capacity_bond),
        slash_judge: profile.slash_judge,
        chain_id: profile.chain_id,
    };

    let status = decdn::run_pull(&cfg.decdn_bin, &args).await?;
    if !status.success() {
        anyhow::bail!(
            "download did not finish. Re-run the same command to resume: downloaded \
             bytes are kept, and the allowance is reused while it is valid."
        );
    }
    session.discard()
}

/// The onramp's profile, refused if it requires a newer CLI. When the onramp
/// is unreachable, a download already under way resumes with the profile it
/// saved.
async fn profile(api: &OnrampClient, session: &Session) -> anyhow::Result<Profile> {
    let profile = match api.profile().await {
        Ok(profile) => profile,
        Err(e) => match session.profile()? {
            Some(saved) => {
                eprintln!(
                    "decdn-sponsored: onramp unreachable ({e}); resuming with saved settings"
                );
                saved
            }
            None => return Err(anyhow::anyhow!("read the onramp's profile: {e}")),
        },
    };
    check_version(&profile, env!("CARGO_PKG_VERSION"))?;
    Ok(profile)
}

fn check_version(profile: &Profile, ours: &str) -> anyhow::Result<()> {
    let Some(min) = &profile.min_cli_version else {
        return Ok(());
    };
    let ours = semver::Version::parse(ours)?;
    anyhow::ensure!(
        &ours >= min,
        "this onramp needs decdn-sponsored {min} or newer (this is {ours}); \
         re-run its installer to upgrade"
    );
    Ok(())
}

/// Best-effort: open `url` in the default browser when a user is at the
/// terminal. The link is always printed too, so a failure here is silent.
fn open_in_browser(url: &str) {
    if !std::io::stdout().is_terminal() {
        return;
    }
    // `rundll32 url.dll,FileProtocolHandler` hands the URL to the default
    // browser without passing it through `cmd`'s metacharacter parsing.
    let mut command = if cfg!(windows) {
        let mut c = std::process::Command::new("rundll32");
        c.args(["url.dll,FileProtocolHandler", url]);
        c
    } else {
        let mut c = std::process::Command::new(if cfg!(target_os = "macos") {
            "open"
        } else {
            "xdg-open"
        });
        c.arg(url);
        c
    };
    let _ = command
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn();
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use alloy::primitives::Address;

    use super::*;

    fn profile(min: Option<&str>) -> Profile {
        Profile {
            chain_id: 1,
            rpc_url: "https://rpc".into(),
            payment_pool: Address::repeat_byte(1),
            capacity_bond: Address::repeat_byte(2),
            slash_judge: None,
            min_cli_version: min.map(|m| m.parse().unwrap()),
        }
    }

    #[test]
    fn an_older_cli_is_told_to_upgrade() {
        assert!(check_version(&profile(None), "0.1.0").is_ok());
        assert!(check_version(&profile(Some("0.1.0")), "0.1.0").is_ok());
        let err = check_version(&profile(Some("0.2.0")), "0.1.9").unwrap_err();
        assert!(err.to_string().contains("re-run its installer"), "{err}");
    }
}
