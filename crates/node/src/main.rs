//! deCDN node — CLI entry point.

use clap::Parser;

use decdn_node::cli::{self, Cli, Command, ConfigCommand};
use decdn_node::commands;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    let config_path = cli.config.map(|p| cli::common::expand_tilde(&p));

    match cli.command {
        Command::Run(run_args) => commands::run(config_path.as_deref(), &run_args).await,
        Command::KeyGen(args) => commands::key_gen(&args),
        Command::Config(args) => match args.command {
            ConfigCommand::Init(init) => commands::config_init(&init),
            ConfigCommand::Validate(validate) => {
                commands::config_validate(config_path.as_deref(), &validate)
            }
        },
        Command::Probe(args) => commands::probe(&args).await,
        Command::Node(args) => commands::node_dispatch(&args, config_path.as_deref()).await,
    }
}
