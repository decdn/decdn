//! Spawns `decdn fetch` as a child process and classifies its outcome.
//!
//! `decdn fetch` is the paying pull of a single content-addressed blob
//! (see `decdn/crates/common/src/cli/fetch.rs`). This wrapper runs it
//! unattended: stdout (the progress bar) is inherited so the operator still
//! sees liveness, stderr is captured so a cap/pool-exhaustion error can be
//! told apart from any other failure.
//!
//! Fetches draw against a sponsor-funded capability and its backing
//! `PaymentPool` (`--capability` / `--payment-pool-address`). When the
//! capability's cap or the pool's balance is exhausted mid-fetch, `decdn
//! fetch` cannot top up and fails terminally with an actionable message
//! (`decdn/crates/cli/src/commands/fetch.rs:907-909`). That is the one
//! failure mode this wrapper must tell apart from a generic error, because
//! the caller (Task 16's flow) responds to it by asking the sponsor to
//! raise the cap or refill the pool rather than treating the fetch as a
//! hard failure.

use std::process::{ExitStatus, Stdio};

use tokio::process::Command;

/// Outcome of one `decdn fetch` invocation.
#[derive(Debug)]
pub enum FetchOutcome {
    /// The fetch completed successfully; the blob is at `FetchArgs::output`.
    Complete,
    /// The capability's cap or the backing `PaymentPool`'s balance was
    /// exhausted mid-fetch. Terminal for this key — there is no top-up
    /// path from here; the caller must ask the sponsor to raise the cap or
    /// refill the pool.
    Exhausted,
    /// Any other non-zero exit. Carries the last stderr line, or a generic
    /// message if stderr was empty.
    Failed(String),
}

/// Stable substring of the terminal error `decdn fetch` prints when a
/// capability's cap or its backing `PaymentPool` is exhausted
/// (`decdn/crates/cli/src/commands/fetch.rs:907-909`).
const EXHAUSTED_MARKER: &str = "higher-cap capability";

/// Classify a finished `decdn fetch` child process from its exit status and
/// captured stderr. Pure — does no I/O — so it is unit-testable without
/// spawning a process.
#[must_use]
pub fn classify_exit(status: ExitStatus, stderr: &str) -> FetchOutcome {
    if status.success() {
        return FetchOutcome::Complete;
    }
    if stderr.contains(EXHAUSTED_MARKER) {
        return FetchOutcome::Exhausted;
    }
    FetchOutcome::Failed(stderr.lines().last().unwrap_or("fetch failed").to_string())
}

/// Arguments for one `decdn fetch` invocation. A plain data struct: this
/// task only builds and classifies the child process; Task 16's flow
/// populates and reuses these across fetches in a session.
#[derive(Debug, Clone)]
pub struct FetchArgs {
    /// `--hash`: BLAKE3 hash of the blob to fetch.
    pub hash: String,
    /// `-o`/`--output`: destination path for the fetched blob.
    pub output: String,
    /// `--capability`: the sponsor-issued capability id to draw the fetch's
    /// payment against.
    pub capability: String,
    /// `--rpc-url`: JSON-RPC endpoint for on-chain reads.
    pub rpc_url: String,
    /// `--payment-pool-address`: `PaymentPool` contract address backing the
    /// capability.
    pub payment_pool_address: String,
    /// `--capacity-bond-address`: `CapacityBond` contract address. Used by
    /// node auto-discovery under `--capability`: the fetch has no explicit
    /// node argument, so it reads `CapacityBond` to find nodes to fetch
    /// from.
    pub capacity_bond_address: Option<String>,
    /// `--slash-judge-address`: `SlashJudge` contract address.
    pub slash_judge_address: Option<String>,
    /// `--chain-id`: EIP-712 `chainId`.
    pub chain_id: u64,
    /// `--keystore`: path to the voucher-signing keystore.
    pub keystore: String,
    /// `--data-dir`: data dir holding the persistent buyer-channel store.
    pub data_dir: String,
}

impl FetchArgs {
    /// Build the `decdn fetch` argument vector (everything after the
    /// `fetch` subcommand).
    fn to_args(&self) -> Vec<String> {
        let mut args = vec![
            "fetch".to_string(),
            "--capability".to_string(),
            self.capability.clone(),
            "--hash".to_string(),
            self.hash.clone(),
            "-o".to_string(),
            self.output.clone(),
            "--rpc-url".to_string(),
            self.rpc_url.clone(),
            "--payment-pool-address".to_string(),
            self.payment_pool_address.clone(),
            "--chain-id".to_string(),
            self.chain_id.to_string(),
            "--keystore".to_string(),
            self.keystore.clone(),
            "--data-dir".to_string(),
            self.data_dir.clone(),
        ];
        if let Some(addr) = &self.capacity_bond_address {
            args.push("--capacity-bond-address".to_string());
            args.push(addr.clone());
        }
        if let Some(addr) = &self.slash_judge_address {
            args.push("--slash-judge-address".to_string());
            args.push(addr.clone());
        }
        args
    }
}

/// Spawn `decdn fetch` with `args`, letting stdout (progress bar) pass
/// through to the wrapper's own stdout while capturing stderr, then
/// classify the result.
///
/// # Errors
///
/// Returns an error if the child process cannot be spawned or awaited
/// (e.g. `decdn_bin` is not found). A non-zero exit from `decdn fetch`
/// itself is not an `Err` here — it is reported as `FetchOutcome::Exhausted`
/// or `FetchOutcome::Failed` so the caller can distinguish cap/pool
/// exhaustion from every other failure.
pub async fn run_fetch(decdn_bin: &str, args: &FetchArgs) -> anyhow::Result<FetchOutcome> {
    // `Command::output()` forces both stdout and stderr to piped, which
    // would swallow the progress bar the operator is meant to see. Spawn
    // with explicit per-stream stdio instead — stdout inherited, stderr
    // piped — then `wait_with_output` drains only the piped stream
    // (stdout comes back empty since it was never captured).
    let child = Command::new(decdn_bin)
        .args(args.to_args())
        .stdout(Stdio::inherit())
        .stderr(Stdio::piped())
        .spawn()?;
    let output = child.wait_with_output().await?;
    let stderr = String::from_utf8_lossy(&output.stderr);
    Ok(classify_exit(output.status, &stderr))
}
