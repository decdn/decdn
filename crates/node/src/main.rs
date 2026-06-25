//! deCDN node daemon — `decdn-node` binary entry point.
//!
//! Daemon-only: the single subcommand is `decdn-node run [--config <path>]`.
//! User-facing commands (probe, node admin, key-gen, config) live on the
//! `decdn` binary in `crates/cli/`.

use std::path::PathBuf;

use clap::{Parser, Subcommand};

use decdn_common::cli::{self, RunArgs};
use decdn_node::commands;

#[derive(Parser, Debug)]
#[command(
    name = "decdn-node",
    version,
    about = "deCDN cache node daemon",
    long_about = "Run a deCDN cache node. The daemon listens for QUIC \
                  client/peer connections, gossip announces, and \
                  loopback admin RPC. Use the `decdn` CLI for one-shot \
                  user/operator commands (probe, node admin, key-gen, \
                  config)."
)]
struct DaemonCli {
    /// Path to TOML config file [default: ~/.decdn/node.toml].
    #[arg(long, global = true, value_name = "PATH")]
    config: Option<PathBuf>,

    #[command(subcommand)]
    command: DaemonCommand,
}

#[derive(Subcommand, Debug)]
enum DaemonCommand {
    /// Run the deCDN node.
    Run(Box<RunArgs>),
}

/// Print a fatal bring-up failure through
/// [`decdn_common::redact::sanitize_err_chain`] rather than letting `anyhow`'s
/// `Termination` impl `Debug`-print the raw chain: an RPC-reachability failure's
/// source error carries the `rpc_url` (API keys live in its path/query). Mid-run
/// chain errors are logged via `tracing` and sanitized at their own call sites.
/// See issue #954.
#[tokio::main]
async fn main() -> std::process::ExitCode {
    let parsed = DaemonCli::parse();
    let config_path = parsed.config.map(|p| cli::common::expand_tilde(&p));
    let result = match parsed.command {
        // `Box::pin` the bring-up future: the runtime `run` state machine is
        // large (many sequential await points across endpoint/handler/task
        // setup) and crosses clippy's `large_futures` threshold. It runs once
        // per process, so heap-allocating it has no meaningful cost.
        DaemonCommand::Run(run_args) => {
            Box::pin(commands::run(config_path.as_deref(), &run_args)).await
        }
    };
    match result {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("Error: {}", decdn_common::redact::sanitize_err_chain(&e));
            std::process::ExitCode::FAILURE
        }
    }
}
