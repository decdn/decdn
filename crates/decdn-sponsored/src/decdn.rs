//! Spawns `decdn bundle pull` against a sponsor-issued capability.
//!
//! `decdn` owns the pull end to end: discovery, payment, BLAKE3 verification,
//! progress, and resuming from `.partial` files. The child inherits all
//! stdio, so the user sees `decdn`'s own progress and error messages
//! unchanged; this module only builds its argument vector.

use std::ffi::OsString;
use std::num::NonZeroU64;
use std::path::PathBuf;
use std::process::ExitStatus;

use alloy::primitives::Address;

use tokio::process::Command;

/// `decdn` reads this before `--keystore-password-file`; it is removed from
/// the child's environment so the per-download password file always wins.
const PASSWORD_ENV: &str = "DECDN_KEYSTORE_PASSWORD";

/// Arguments for one `decdn bundle pull` invocation.
#[derive(Debug, Clone)]
pub struct PullArgs {
    /// `--hash`: BLAKE3 hash (64 hex) of the bundle manifest.
    pub hash: String,
    /// `-o`/`--output`: directory the bundle's files are written under.
    pub output: PathBuf,
    /// `--namespace`: the namespace the bundle is published under, so a
    /// cache-missing node can pull from its origins (ADR 002). Namespace 0
    /// means "no namespace" to `decdn`, so the type rules it out.
    pub namespace: Option<NonZeroU64>,
    /// `--capability-file`: the sponsor-issued `dcap1:` token, read from a
    /// file to keep it off the process table.
    pub capability_file: PathBuf,
    /// `--keystore`: this download's throwaway voucher-signing key.
    pub keystore: PathBuf,
    /// `--keystore-password-file`: that key's random password.
    pub password_file: PathBuf,
    /// `--data-dir`: the `decdn` data dir downloads share (peer store,
    /// buyer-channel store), or the download's own dir while another
    /// download holds the shared one. The key above stays per download.
    pub data_dir: PathBuf,
    /// `--rpc-url`: JSON-RPC endpoint for on-chain reads.
    pub rpc_url: String,
    /// `--payment-pool-address`: the sponsor's `PaymentPool` contract.
    pub payment_pool: Address,
    /// `--capacity-bond-address`: read for node auto-discovery.
    pub capacity_bond: Option<Address>,
    /// `--slash-judge-address`: `SlashJudge` contract address.
    pub slash_judge: Option<Address>,
    /// `--chain-id`: EIP-712 `chainId`.
    pub chain_id: u64,
}

impl PullArgs {
    /// The `decdn` argument vector, starting at the `bundle` subcommand.
    #[must_use]
    pub fn to_args(&self) -> Vec<OsString> {
        let mut args: Vec<OsString> = vec![
            "bundle".into(),
            "pull".into(),
            "--hash".into(),
            self.hash.clone().into(),
            "-o".into(),
            self.output.clone().into(),
            "--capability-file".into(),
            self.capability_file.clone().into(),
            "--keystore".into(),
            self.keystore.clone().into(),
            "--keystore-password-file".into(),
            self.password_file.clone().into(),
            "--data-dir".into(),
            self.data_dir.clone().into(),
            "--rpc-url".into(),
            self.rpc_url.clone().into(),
            "--payment-pool-address".into(),
            self.payment_pool.to_string().into(),
            "--chain-id".into(),
            self.chain_id.to_string().into(),
        ];
        if let Some(namespace) = self.namespace {
            args.push("--namespace".into());
            args.push(namespace.to_string().into());
        }
        if let Some(addr) = self.capacity_bond {
            args.push("--capacity-bond-address".into());
            args.push(addr.to_string().into());
        }
        if let Some(addr) = self.slash_judge {
            args.push("--slash-judge-address".into());
            args.push(addr.to_string().into());
        }
        args
    }
}

/// Run `decdn bundle pull` to completion with inherited stdio.
///
/// # Errors
///
/// Returns an error if `decdn_bin` cannot be spawned or awaited. A non-zero
/// exit is returned as the `ExitStatus`, not as an `Err`.
pub async fn run_pull(decdn_bin: &str, args: &PullArgs) -> anyhow::Result<ExitStatus> {
    let status = Command::new(decdn_bin)
        .args(args.to_args())
        .env_remove(PASSWORD_ENV)
        .status()
        .await
        .map_err(|e| anyhow::anyhow!("failed to run {decdn_bin}: {e}"))?;
    Ok(status)
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::indexing_slicing)]
mod tests;
