//! deCDN user CLI — `decdn` binary entry point.
//!
//! Pairs with the `decdn-node` daemon: this binary carries every command
//! a human types in a terminal (probe, get, pools, publishing, node
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
    // Before any reqwest client: chain RPC, `doctor` and `setup` probes.
    decdn_common::tls::install_crypto_provider();
    // `run()` dispatches every CLI command, so its future aggregates the
    // largest command's locals; box it to keep `main`'s future off the stack
    // (clippy::large_futures).
    match Box::pin(run()).await {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(e) => {
            if e.downcast_ref::<decdn_cli::commands::doctor::DoctorFailed>()
                .is_some()
            {
                // The report is already printed; exit nonzero without an Error: line.
                return std::process::ExitCode::FAILURE;
            }
            if let Some(line) = decdn_cli::commands::interrupt::stop_message(&e) {
                // Not an error to fix: an interrupt or a give-up, with any
                // `openPool` still in flight in place of the rerun advice.
                eprintln!("{line}");
            } else {
                eprintln!("Error: {}", decdn_common::redact::sanitize_err_chain(&e));
            }
            exit_code_for(&e)
        }
    }
}

/// The process exit code for a command's error: 130 for an interrupt, 75
/// (`EX_TEMPFAIL`) for a fetch that gave up for lack of progress with no
/// `openPool` in flight, 1 otherwise.
fn exit_code_for(err: &anyhow::Error) -> std::process::ExitCode {
    decdn_cli::commands::interrupt::stop_exit(err).map_or(
        std::process::ExitCode::FAILURE,
        std::process::ExitCode::from,
    )
}

async fn run() -> anyhow::Result<()> {
    let cli = Cli::parse();

    // Opt-in only: with no `-v`, `--log-level` or `RUST_LOG` this is a no-op and
    // the CLI stays a clean terminal UI. Installed before dispatch so it covers
    // every subcommand.
    decdn_cli::logging::init(decdn_cli::logging::requested_level(
        cli.log_level,
        cli.verbose,
    ));

    let config_path = cli.config.map(|p| cli::common::expand_tilde(&p));

    match cli.command {
        Command::KeyGen(args) => commands::key_gen::key_gen(&args),
        Command::Whoami(args) => commands::whoami::whoami(&args, config_path.as_deref()),
        Command::Config(args) => match args.command {
            ConfigCommand::Init(init) => commands::config::config_init(&init),
            ConfigCommand::Validate(validate) => {
                commands::config::config_validate(config_path.as_deref(), &validate)
            }
        },
        Command::Probe(args) => commands::probe::probe(&args, config_path.as_deref()).await,
        Command::Gateway(args) => commands::gateway::gateway(&args, config_path.as_deref()).await,
        Command::Get(args) => commands::get::get(&args, config_path.as_deref()).await,
        Command::Setup(args) => commands::setup::run(&args, config_path.as_deref()).await,
        Command::Node(args) => commands::node::node_dispatch(&args, config_path.as_deref()).await,
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

#[cfg(test)]
mod tests;
