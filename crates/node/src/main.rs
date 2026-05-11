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

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let parsed = DaemonCli::parse();
    let config_path = parsed.config.map(|p| cli::common::expand_tilde(&p));
    match parsed.command {
        DaemonCommand::Run(run_args) => commands::run(config_path.as_deref(), &run_args).await,
    }
}
