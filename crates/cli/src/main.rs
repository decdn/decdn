//! deCDN user CLI — `decdn` binary entry point.
//!
//! Pairs with the `decdn-node` daemon: this binary carries every command
//! a human types in a terminal (probe, node admin, key-gen, config). The
//! `node` subcommand group talks to a running daemon over the loopback
//! admin RPC surface (ADR 025) and fails cleanly on a publisher's laptop
//! where no daemon is running.

use clap::Parser;

use decdn_cli::commands;
use decdn_common::cli::{self, Cli, Command, ConfigCommand};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    let config_path = cli.config.map(|p| cli::common::expand_tilde(&p));

    match cli.command {
        // The daemon-only `Run` subcommand lives on `decdn-node`. On the
        // user CLI it errors with a redirect rather than silently
        // unknown-subcommand, so an operator who muscle-memories `decdn
        // run` (the pre-split shape) gets a useful next step. Hard cut:
        // there's no compatibility shim — the daemon binary is the only
        // way to start the node.
        Command::Run(_) => Err(anyhow::anyhow!(
            "`decdn run` no longer exists. Run the daemon with `decdn-node run` instead.\n\
             See ADR appendix-binaries for the dockerd-style split."
        )),
        Command::KeyGen(args) => commands::key_gen::key_gen(&args),
        Command::Config(args) => match args.command {
            ConfigCommand::Init(init) => commands::config::config_init(&init),
            ConfigCommand::Validate(validate) => {
                commands::config::config_validate(config_path.as_deref(), &validate)
            }
        },
        Command::Probe(args) => commands::probe::probe(&args).await,
        Command::Node(args) => commands::node::node_dispatch(&args, config_path.as_deref()).await,
    }
}
