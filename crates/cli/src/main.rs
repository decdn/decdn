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
        Command::KeyGen(args) => commands::key_gen::key_gen(&args),
        Command::Config(args) => match args.command {
            ConfigCommand::Init(init) => commands::config::config_init(&init),
            ConfigCommand::Validate(validate) => {
                commands::config::config_validate(config_path.as_deref(), &validate)
            }
        },
        Command::Probe(args) => commands::probe::probe(&args).await,
        Command::Node(args) => commands::node::node_dispatch(&args, config_path.as_deref()).await,
        Command::Bundle(args) => commands::bundle::bundle_dispatch(&args).await,
    }
}
