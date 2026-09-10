//! deCDN user CLI — `decdn` binary entry point.
//!
//! Pairs with the `decdn-node` daemon: this binary carries every command
//! a human types in a terminal (probe, fetch, pools, publishing, node
//! admin). The `node` subcommand group talks to a running daemon over the
//! loopback admin RPC surface (`appendix-local-admin-http.md`) and fails
//! cleanly on a publisher's laptop where no daemon is running.
//!
//! Its one direct write is the top-level error boundary in `main`; everything
//! else prints from [`decdn_cli::commands`].

use clap::Parser;

use decdn_cli::commands;
use decdn_common::cli::{self, Cli, Command, ConfigCommand};

/// Print failures through [`decdn_common::redact::sanitize_err_chain`] rather
/// than letting `anyhow`'s `Termination` impl `Debug`-print the raw chain: a
/// chain-RPC failure's source error carries the `rpc_url` (API keys live in its
/// path/query), and this is the single boundary every CLI command propagates
/// to. See issue #954.
#[tokio::main]
#[expect(
    clippy::print_stderr,
    reason = "process exit boundary; the final user-facing error line, not a log event"
)]
async fn main() -> std::process::ExitCode {
    match run().await {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(e) => {
            if e.downcast_ref::<decdn_cli::commands::doctor::DoctorFailed>()
                .is_some()
            {
                // The report is already printed; exit nonzero without an Error: line.
                return std::process::ExitCode::FAILURE;
            }
            eprintln!("Error: {}", decdn_common::redact::sanitize_err_chain(&e));
            std::process::ExitCode::FAILURE
        }
    }
}

async fn run() -> anyhow::Result<()> {
    let cli = Cli::parse();

    // Opt-in only: with neither `--log-level` nor `RUST_LOG` this is a no-op and
    // the CLI stays a clean terminal UI. Installed before dispatch so it covers
    // every subcommand.
    decdn_cli::logging::init(cli.log_level);

    let config_path = cli.config.map(|p| cli::common::expand_tilde(&p));

    match cli.command {
        Command::KeyGen(args) => commands::key_gen::key_gen(&args),
        Command::Whoami(args) => commands::whoami::whoami(&args),
        Command::Config(args) => match args.command {
            ConfigCommand::Init(init) => commands::config::config_init(&init),
            ConfigCommand::Validate(validate) => {
                commands::config::config_validate(config_path.as_deref(), &validate)
            }
        },
        Command::Probe(args) => commands::probe::probe(&args, config_path.as_deref()).await,
        Command::Fetch(args) => commands::fetch::fetch(&args, config_path.as_deref()).await,
        Command::Setup(args) => commands::setup::run(&args, config_path.as_deref()).await,
        Command::Node(args) => commands::node::node_dispatch(&args, config_path.as_deref()).await,
        Command::Bundle(args) => {
            commands::bundle::bundle_dispatch(&args, config_path.as_deref()).await
        }
        // `origin` is offline, config-free local filesystem work — it never
        // reads the node config, so `config_path` is intentionally not passed.
        Command::Origin(args) => commands::origin::origin_dispatch(&args).await,
        Command::Pool(args) => commands::pool::pool_dispatch(&args, config_path.as_deref()).await,
        Command::Publish(args) => {
            commands::publish::publish_dispatch(&args, config_path.as_deref()).await
        }
        Command::Appeal(args) => match args.command {
            cli::AppealCommand::Slash(slash) => {
                commands::appeal::run(&slash, config_path.as_deref()).await
            }
        },
    }
}
