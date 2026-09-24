//! The pull flow: open this download's state, make sure its throwaway key
//! holds an unexpired capability (browser captcha when it doesn't), then run
//! `decdn bundle pull`. A successful pull deletes the state; a failed one
//! keeps it, so re-running the same command resumes without a new captcha.

use std::io::IsTerminal;
use std::path::Path;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::api::Api;
use crate::config::WrapperConfig;
use crate::runner::{self, PullArgs};
use crate::session::{self, Session};

/// How long to wait for the browser captcha flow to produce a capability.
const CAPABILITY_POLL_TIMEOUT: Duration = Duration::from_secs(600);

/// A saved capability this close to expiry is replaced before pulling.
const EXPIRY_MARGIN_SECS: u64 = 300;

/// # Errors
/// Invalid hash, state dir or key failure, sponsor unreachable / no
/// capability within the poll timeout, or `decdn bundle pull` failing.
pub async fn pull(hash: &str, output: &Path, cfg: &WrapperConfig) -> anyhow::Result<()> {
    let hash = session::normalize_hash(hash)?;
    let api = Api::new(cfg.gateway_base.clone());

    let mut session = Session::open(&cfg.data_dir, &hash)?;
    let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
    if session
        .capability()?
        .is_some_and(|grant| grant.expiry <= now.saturating_add(EXPIRY_MARGIN_SECS))
    {
        // A key's cap and expiry are frozen on-chain at its first
        // redemption, so an expired capability means a fresh key.
        session.discard()?;
        session = Session::open(&cfg.data_dir, &hash)?;
    }

    let client = session.ensure_key()?;
    if session.capability()?.is_none() {
        let info = match api.get_capability(client).await? {
            Some(info) => info,
            None => {
                let url = api.fund_url(client);
                println!("Solve the captcha to start the download:\n  {url}");
                open_in_browser(&url);
                api.poll_capability(client, CAPABILITY_POLL_TIMEOUT).await?
            }
        };
        session.save_capability(&info.token)?;
    }

    let args = PullArgs {
        hash,
        output: output.to_path_buf(),
        capability_file: session.capability_path(),
        keystore: session.keystore_path(),
        password_file: session.password_path(),
        data_dir: session.dir().to_path_buf(),
        rpc_url: cfg.rpc_url.clone(),
        payment_pool_address: cfg.payment_pool.to_string(),
        capacity_bond_address: cfg.capacity_bond.map(|a| a.to_string()),
        slash_judge_address: cfg.slash_judge.map(|a| a.to_string()),
        chain_id: cfg.chain_id,
    };

    let status = runner::run_pull(&cfg.decdn_bin, &args).await?;
    if !status.success() {
        anyhow::bail!(
            "download did not finish. Re-run the same command to resume: downloaded \
             bytes are kept, and no new captcha is needed while the capability is valid."
        );
    }
    session.discard()
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
